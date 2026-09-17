from __future__ import annotations

import random
import hashlib
import json
import copy
import math
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

import torch
from torch import nn
from torch.utils.data import DataLoader

from .config import Settings
from .atr_calibration import ATR_ENCODING, prepare_atr_levels, validate_five_levels
from .dataset import INPUT_ALIGNMENT, StockSequenceDataset, subset
from .device import describe_device, move_targets_to_device, select_device
from .model import StockAutoregressiveModel
from .signals import CLASSES
from .paths import CHECKPOINT_DIR, FEATURES_DIR, ensure_runtime_dirs
from .universe import recent_symbols
from .storage import read_jsonl
from .replay import select_daily_sequences
from .provenance import fingerprint, file_fingerprint, sample_key, sample_versions
from .checkpoints import publish_checkpoint, require_finite, verify_checkpoint
from .trading_calendar import TradingCalendar


TARGET_NAMES = ("high_price", "low_price")
MODEL_OUTPUT_SCHEMA = "high_low_price_five_classes_v1"


def seed_everything(seed: int) -> None:
    random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def build_model(settings: Settings) -> StockAutoregressiveModel:
    return StockAutoregressiveModel(
        context_days=settings.context_days,
        d_model=settings.d_model,
        n_heads=settings.n_heads,
        n_layers=settings.n_layers,
        dropout=settings.dropout,
    )


def label_distribution(dataset: StockSequenceDataset) -> dict:
    result = {"count": len(dataset)}
    for field in TARGET_NAMES:
        counts = {str(c): 0 for c in CLASSES}
        for ref in dataset.refs:
            counts[str(dataset.rows_by_path[ref.feature_path][ref.end][field])] += 1
        result[f"{field}_counts"] = counts
        result[f"majority_{field}"] = max(CLASSES, key=lambda c: counts[str(c)])
    return result


def multiclass_class_weight(dataset: StockSequenceDataset, field: str) -> torch.Tensor | None:
    counts = label_distribution(dataset)[f"{field}_counts"]
    present = sum(count > 0 for count in counts.values())
    if present <= 1:
        return None
    # Missing classes receive zero weight; they have no training samples.
    return torch.tensor([len(dataset) / (present * counts[str(c)]) if counts[str(c)] else 0.0
                         for c in CLASSES], dtype=torch.float32)


class FocalLoss(nn.Module):
    """Multi-class focal loss: down-weights samples the model already classifies
    confidently, so gradient stays focused on hard positives."""

    def __init__(self, gamma: float = 2.0, weight: torch.Tensor | None = None):
        super().__init__()
        self.gamma = gamma
        self.weight = weight

    def forward(self, logits: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
        log_probs = torch.log_softmax(logits, dim=-1)
        target_log_prob = log_probs.gather(1, target.unsqueeze(1)).squeeze(1)
        focal_term = (1.0 - target_log_prob.exp()).clamp(min=0.0) ** self.gamma
        loss = -focal_term * target_log_prob
        if self.weight is not None:
            sample_weight = self.weight[target]
            return (loss * sample_weight).sum() / sample_weight.sum()
        return loss.mean()


def build_criteria(
    settings: Settings, dataset: StockSequenceDataset, device: torch.device,
) -> dict[str, nn.Module]:
    criteria: dict[str, nn.Module] = {}
    for field in TARGET_NAMES:
        weight = multiclass_class_weight(dataset, field)
        criteria[field] = FocalLoss(
            gamma=settings.focal_gamma,
            weight=weight.to(device) if weight is not None else None,
        )
        if weight is not None:
            print(
                f"class_weight {field}: {dict(zip(CLASSES, weight.tolist()))} "
                f"focal_gamma={settings.focal_gamma}"
            )
        else:
            print(f"class_weight {field}: 樣本只有單一類別，維持不加權 focal_gamma={settings.focal_gamma}")
    return criteria


def weighted_loss(
    outputs, targets, settings: Settings, criteria: dict[str, nn.Module],
) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
    """Returns the (weighted) loss used for backward(), plus the target's raw
    unweighted loss for logging/measurement."""
    components = {name: criteria[name](outputs[name], targets[name]) for name in TARGET_NAMES}
    combined = sum(getattr(settings, f"loss_{name}") * components[name] for name in TARGET_NAMES)
    return combined, components


def ensure_checkpoint_compatible(checkpoint: dict) -> None:
    heads = [checkpoint.get("model", {}).get(f"{name}_head.weight") for name in TARGET_NAMES]
    if (checkpoint.get("output_schema") != MODEL_OUTPUT_SCHEMA
            or tuple(checkpoint.get("target_names", ())) != TARGET_NAMES
            or any(head is None or head.ndim != 2 or head.shape[0] != 5 for head in heads)):
        raise RuntimeError("checkpoint 不是 high_price / low_price 雙目標五分類模型；請重新執行 train_initial")
    if ("atr_embedding.weight" not in checkpoint.get("model", {})
            or checkpoint.get("atr_encoding") != ATR_ENCODING):
        raise RuntimeError(
            "checkpoint 不是 ATR 五級離散模型；請重新執行 prepare_features 與 train_initial"
        )
    validate_five_levels(checkpoint.get("settings", {}).get("atr_boundaries_pct"))
    if not checkpoint.get("atr_calibration"):
        raise RuntimeError("checkpoint 缺少 ATR 刻度來源，請重新執行 train_initial")
    if any(key.startswith("close_head.") for key in checkpoint.get("model", {})):
        raise RuntimeError(
            "checkpoint 是舊版四目標模型，含 close_limit 輸出 head；"
            "請先重新執行 train_initial"
        )
    if any(key.startswith("price_head.") for key in checkpoint.get("model", {})):
        raise RuntimeError(
            "checkpoint 是舊版含 price 輸出 head 的模型；"
            "請先重新執行 train_initial"
        )
    if "night_futures_embedding.weight" not in checkpoint.get("model", {}):
        raise RuntimeError(
            "checkpoint 是舊版 6 輸入模型，缺少夜盤期指這項輸入；"
            "請先匯入 night_futures 資料、重新執行 prepare_features 與 train_initial"
        )
    if any(key.startswith("hit_down_head.") for key in checkpoint.get("model", {})):
        raise RuntimeError(
            "checkpoint 是舊版含 hit_down 輸出 head 的模型；"
            "請先重新執行 train_initial"
        )
    if "high_price_head.weight" not in checkpoint.get("model", {}):
        raise RuntimeError(
            "checkpoint 不是 high_price 輸出模型；"
            "請先重新執行 prepare_features 與 train_initial"
        )
    if (checkpoint.get("input_alignment") != INPUT_ALIGNMENT
            or any(f"{field}_embedding.weight" not in checkpoint.get("model", {})
                   for field in ("open_price", "high_price", "low_price", "close_price"))
            or "target_night_futures_embedding.weight" in checkpoint.get("model", {})):
        raise RuntimeError("checkpoint 不是含開高低收的十項序列模型；請重新執行 prepare_features 與 train_initial")



def _training_window_floor(
    feature_paths: list[Path], as_of: date, training_window_days: int | None,
) -> date | None:
    """Trailing-N-trading-day lower bound for training targets, derived from the
    actual feature calendar (not a calendar-day approximation), so a rolling
    window means exactly N trading days regardless of holidays."""
    if training_window_days is None:
        return None
    if training_window_days <= 0:
        raise ValueError("training_window_days 必須為正整數")
    cutoff = as_of.isoformat()
    dates: set[str] = set()
    for path in feature_paths:
        dates.update(row["date"] for row in read_jsonl(path) if row["date"] <= cutoff)
    if not dates:
        return None
    ordered = sorted(dates)
    floor_str = ordered[-training_window_days] if len(ordered) >= training_window_days else ordered[0]
    return date.fromisoformat(floor_str)


def _validation_split(
    dataset: StockSequenceDataset, validation_days: int,
) -> tuple[list, list]:
    """Hold out the trailing `validation_days` distinct target dates in
    `dataset` as a validation split, by date rather than by ref count, so every
    symbol's targets for a given day move together (no leakage of "today" into
    both train and validation through different symbols). Returns
    (train_refs, val_refs); val_refs is empty if there aren't enough distinct
    target dates to hold any out without emptying the train split."""
    target_dates = sorted({dataset.rows_by_path[ref.feature_path][ref.end]["date"] for ref in dataset.refs})
    if validation_days <= 0 or len(target_dates) <= validation_days:
        return list(dataset.refs), []
    val_dates = set(target_dates[-validation_days:])
    train_refs = [ref for ref in dataset.refs if dataset.rows_by_path[ref.feature_path][ref.end]["date"] not in val_dates]
    val_refs = [ref for ref in dataset.refs if dataset.rows_by_path[ref.feature_path][ref.end]["date"] in val_dates]
    return train_refs, val_refs


def validate_training_settings(settings):
    for name in ("epochs", "daily_epochs", "batch_size"):
        value = getattr(settings, name)
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} 必須是正整數")
    for name in ("learning_rate", "daily_learning_rate", "loss_high_price", "loss_low_price"):
        value = getattr(settings, name)
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} 必須是有限正數")
    if type(settings.focal_gamma) not in (int, float) or not math.isfinite(settings.focal_gamma) or settings.focal_gamma < 0:
        raise ValueError("focal_gamma 必須是有限非負數")


def train(
    settings: Settings,
    resume_path: Path | None = None,
    daily: bool = False,
    as_of: date | None = None,
    force_retrain: bool = False,
    training_window_days: int | None = None,
) -> Path:
    validate_training_settings(settings)
    ensure_runtime_dirs()
    seed_everything(settings.seed)
    as_of = as_of or date.today()
    TradingCalendar().require_session(as_of)
    if daily and resume_path is None:
        raise ValueError("每日續訓必須指定既有 checkpoint，不能重新擬合 ATR 刻度")
    checkpoint = None
    receipt_path = None
    if resume_path:
        checkpoint = torch.load(resume_path, map_location="cpu", weights_only=False)
        ensure_checkpoint_compatible(checkpoint)
        from .checkpoints import verify_training_result
        verify_training_result(checkpoint)
        previous_as_of = checkpoint.get("training_as_of")
        if previous_as_of and previous_as_of > as_of.isoformat():
            raise RuntimeError("checkpoint 訓練截止日晚於本次日期，不可用未來模型回訓歷史日期")
        if daily:
            if not previous_as_of:
                raise RuntimeError("checkpoint 缺少 training_as_of，無法區分新增與歷史序列")
            if "trained_sample_versions" not in checkpoint:
                raise RuntimeError("checkpoint 缺少樣本版本紀錄，請重新執行 train_initial 後再續訓")
    eligible_symbols = recent_symbols(as_of, settings.recent_universe_days)
    if not eligible_symbols:
        raise RuntimeError(
            f"找不到 {as_of.isoformat()} 往前 {settings.recent_universe_days} 天內的股票清單快照，"
            "請先執行 update_data 產生 universe snapshot，避免訓練誤用不在清單內的舊股票"
        )
    all_feature_paths = list(FEATURES_DIR.glob("*.jsonl"))
    feature_paths = sorted(
        path for path in all_feature_paths
        if path.stem in eligible_symbols
    )
    # Initial calibration uses stocks that can contribute training sequences.
    feature_paths = [path for path in feature_paths
                     if sum(row["date"] <= as_of.isoformat() for row in read_jsonl(path)) > settings.context_days]
    if not feature_paths:
        raise RuntimeError("沒有足夠的特徵序列可供訓練")
    window_floor = _training_window_floor(feature_paths, as_of, training_window_days)
    settings, atr_calibration = prepare_atr_levels(settings)
    dataset = StockSequenceDataset(
        feature_paths,
        settings.context_days,
        max_target_date=as_of,
        atr_boundaries_pct=settings.atr_boundaries_pct,
        min_target_date=window_floor,
    )
    if not dataset:
        raise RuntimeError("沒有足夠的特徵序列可供訓練")
    versions = sample_versions(dataset)
    trained_versions = dict(checkpoint.get("trained_sample_versions", {})) if checkpoint else {}
    if daily:
        key = fingerprint({"source": file_fingerprint(resume_path), "as_of": as_of.isoformat(),
                           "samples": versions, "settings": asdict(settings),
                           "training_window_days": training_window_days})
        receipt_path = CHECKPOINT_DIR / "daily_runs" / f"{key}.json"
        if receipt_path.exists() and not force_retrain:
            completed = Path(json.loads(receipt_path.read_text(encoding="utf-8"))["checkpoint"])
            if completed.exists():
                verify_checkpoint(completed)
                print(f"[SKIP] 相同來源、樣本版本與設定已完成: {completed}")
                return completed
    # Captured before `select_daily_sequences` below narrows dataset.refs to a
    # sampled training subset; the baseline should reflect the full window.
    naive_baseline = label_distribution(dataset)
    sampling = None
    seen_symbols = set(checkpoint.get("seen_symbols", checkpoint.get("symbols", []))) if checkpoint else set()
    if daily:
        all_refs = list(dataset.refs)
        sampling = select_daily_sequences(dataset, previous_as_of, as_of, settings, seen_symbols,
                                          trained_versions, versions)
        if force_retrain and not dataset.refs:
            dataset.refs = all_refs
            sampling["forced_full_window"] = True
        if not sampling["new_count"] and not force_retrain:
            print("[SKIP] 視窗內沒有未學習或已修正的樣本，不重複續訓")
            return resume_path
        if not dataset:
            print("[SKIP] 沒有選取的訓練序列")
            return resume_path
        print(
            f"daily_mode={sampling['mode']} new={sampling['new_count']} "
            f"replay={sampling['replay_count']} replay_hit_days={sampling['replay_hit_days']}"
        )

    # Held-out validation split, only for a full/reseed training run — a single
    # `daily_epochs` incremental step has nothing to early-stop from. Without
    # this, a fixed `epochs` count has no way to know it has started memorizing
    # the training window instead of learning anything that generalizes to the
    # next unseen day (see naive_baseline/log-loss comparisons in the backtest
    # harness, which is what surfaced this in the first place).
    train_dataset = dataset
    val_dataset = None
    use_validation = (not daily) and settings.validation_days > 0
    if use_validation:
        train_refs, val_refs = _validation_split(dataset, settings.validation_days)
        if train_refs and val_refs:
            train_dataset = subset(dataset, train_refs)
            val_dataset = subset(dataset, val_refs)
        else:
            use_validation = False
            print(f"[WARN] 可用交易日不足以切出 {settings.validation_days} 天驗證集，跳過 early stopping")

    device = select_device()
    use_cuda = device.type == "cuda"
    print(f"device={describe_device(device)}")
    criteria = build_criteria(settings, train_dataset, device)
    # Plain (unweighted, non-focal) cross-entropy — used only for measurement
    # (logged train loss, validation loss, early-stopping/epoch-selection),
    # never for the backward pass. `criteria`'s FocalLoss + inverse-frequency
    # class weighting is a legitimate training technique,
    # but its output is numerically on a different scale than the plain
    # -log(p_true) the walk-forward backtest scores real predictions with;
    # comparing the two directly (as earlier diagnostics here did) understates
    # how much of the in-sample/out-of-sample gap is real overfitting versus
    # just two different loss units.
    plain_criteria = {name: nn.CrossEntropyLoss() for name in TARGET_NAMES}
    loader = DataLoader(
        train_dataset,
        batch_size=settings.batch_size,
        shuffle=True,
        pin_memory=use_cuda,
    )
    val_loader = DataLoader(
        val_dataset, batch_size=settings.batch_size, shuffle=False, pin_memory=use_cuda,
    ) if use_validation else None
    model = build_model(settings).to(device)
    learning_rate = settings.daily_learning_rate if daily else settings.learning_rate
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    scaler = torch.amp.GradScaler("cuda", enabled=use_cuda)
    if checkpoint is not None:
        model.load_state_dict(checkpoint["model"])
        if "optimizer" in checkpoint:
            optimizer.load_state_dict(checkpoint["optimizer"])
            for group in optimizer.param_groups:
                group["lr"] = learning_rate
        if use_cuda and checkpoint.get("scaler"):
            scaler.load_state_dict(checkpoint["scaler"])

    epochs = settings.daily_epochs if daily else settings.epochs
    optimizer_steps = 0
    best_optimizer_steps = 0
    epochs_run = 0
    def record_step(optimizer, args, kwargs):
        nonlocal optimizer_steps
        optimizer_steps += 1
    optimizer.register_step_post_hook(record_step)
    model.train()
    final_loss = float("nan")
    final_component_loss = {name: float("nan") for name in TARGET_NAMES}
    monitored_targets = TARGET_NAMES
    best_val_loss = float("inf")
    best_epoch: int | None = None
    best_state: dict[str, torch.Tensor] | None = None
    best_optimizer_state = None
    best_scaler_state = None
    early_stopped = False
    stalled_targets: list[str] = []
    validation_history: list[dict] = []
    best_component_val = {name: float("inf") for name in monitored_targets}
    component_patience = {name: 0 for name in monitored_targets}
    for epoch in range(epochs):
        total_loss = 0.0
        total_component_loss = {name: 0.0 for name in TARGET_NAMES}
        for states, targets in loader:
            states = states.to(device, non_blocking=use_cuda)
            targets = move_targets_to_device(targets, device, non_blocking=use_cuda)
            optimizer.zero_grad(set_to_none=True)
            with torch.amp.autocast(
                device_type="cuda",
                dtype=torch.float16,
                enabled=use_cuda,
            ):
                outputs = model(states)
                loss, _ = weighted_loss(outputs, targets, settings, criteria)
                # Logged/saved per-target loss uses plain CE (see plain_criteria
                # above), not the FocalLoss components `loss` was built from.
                plain_components = {
                    name: plain_criteria[name](outputs[name], targets[name]) for name in monitored_targets
                }
            require_finite(loss, "training loss")
            require_finite(plain_components, "training high/low loss")
            scaler.scale(loss).backward()
            scaler.unscale_(optimizer)
            nn.utils.clip_grad_norm_(model.parameters(), 1.0, error_if_nonfinite=True)
            before_step = optimizer_steps
            scaler.step(optimizer)
            scaler.update()
            if optimizer_steps != before_step + 1:
                raise RuntimeError("optimizer 未完成權重更新，停止訓練，不發布模型或標記樣本已學習")
            batch_size = states.shape[0]
            total_loss += float(loss.detach()) * batch_size
            for name, value in plain_components.items():
                total_component_loss[name] += float(value.detach()) * batch_size
        final_loss = total_loss / len(train_dataset)
        final_component_loss = {name: value / len(train_dataset) for name, value in total_component_loss.items()}
        epochs_run = epoch + 1
        require_finite(final_loss, "epoch loss")
        require_finite(final_component_loss, "epoch high/low loss")
        log_line = (
            f"epoch={epoch + 1}/{epochs} loss={final_loss:.6f} (加權合計) | 未加權: "
            f"high_price={final_component_loss['high_price']:.6f}"
            f" low_price={final_component_loss['low_price']:.6f}"
        )

        if not use_validation:
            print(log_line)
            continue

        model.eval()
        val_total_component_loss = {name: 0.0 for name in TARGET_NAMES}
        with torch.no_grad():
            for states, targets in val_loader:
                states = states.to(device, non_blocking=use_cuda)
                targets = move_targets_to_device(targets, device, non_blocking=use_cuda)
                with torch.amp.autocast(
                    device_type="cuda",
                    dtype=torch.float16,
                    enabled=use_cuda,
                ):
                    outputs = model(states)
                    # Plain CE here too — see plain_criteria above.
                    plain_components = {
                        name: plain_criteria[name](outputs[name], targets[name]) for name in monitored_targets
                    }
                require_finite(plain_components, "validation high/low loss")
                batch_size = states.shape[0]
                for name, value in plain_components.items():
                    val_total_component_loss[name] += float(value.detach()) * batch_size
        model.train()
        val_component_loss = {name: value / len(val_dataset) for name, value in val_total_component_loss.items()}
        # Informational only (not used for any decision below): the same
        # weighted combination training optimizes, but built from the plain
        # per-target loss above rather than a second FocalLoss pass.
        val_loss = sum(getattr(settings, f"loss_{name}") * val_component_loss[name] for name in TARGET_NAMES)
        require_finite(val_loss, "validation loss")
        validation_history.append({
            "epoch": epoch + 1, "val_loss": val_loss, "val_components": val_component_loss,
            "train_loss": final_loss, "train_components": final_component_loss,
        })
        print(
            log_line
            + f" | validation: loss={val_loss:.6f} "
            + f"high_price={val_component_loss['high_price']:.6f}"
            + f" low_price={val_component_loss['low_price']:.6f}"
        )
        # Both heads contribute equally to checkpoint selection, in plain CE units.
        selection_loss = sum(val_component_loss.values()) / len(TARGET_NAMES)
        if selection_loss < best_val_loss - 1e-6:
            best_val_loss = selection_loss
            best_epoch = epoch + 1
            best_state = {key: value.detach().clone() for key, value in model.state_dict().items()}
            best_optimizer_state = copy.deepcopy(optimizer.state_dict())
            best_scaler_state = copy.deepcopy(scaler.state_dict())
            best_optimizer_steps = optimizer_steps

        for name in monitored_targets:
            if val_component_loss[name] < best_component_val[name] - 1e-6:
                best_component_val[name] = val_component_loss[name]
                component_patience[name] = 0
            else:
                component_patience[name] += 1

        stalled_targets = [name for name in monitored_targets if component_patience[name] >= settings.early_stopping_patience]
        if len(stalled_targets) == len(monitored_targets):
            early_stopped = True
            print(
                f"[EARLY STOP] {'/'.join(stalled_targets)} 連續 {settings.early_stopping_patience} 個 epoch "
                f"驗證 loss 沒有改善，停在 epoch {epoch + 1}；"
                f"採用 high/low 平均驗證 loss 最佳的 epoch {best_epoch}（selection_loss={best_val_loss:.6f}）"
            )
            break

    if use_validation and best_state is not None:
        model.load_state_dict(best_state)
        optimizer.load_state_dict(best_optimizer_state)
        if use_cuda:
            scaler.load_state_dict(best_scaler_state)
        final_loss, final_component_loss = next(
            (item["train_loss"], item["train_components"])
            for item in validation_history if item["epoch"] == best_epoch
        )
        print(
            f"採用驗證集最佳權重（依 high/low 平均 loss 挑選）："
            f"epoch={best_epoch}, selection_loss={best_val_loss:.6f}（訓練總 epoch 上限={epochs}）"
        )

    if optimizer_steps <= 0 or (use_validation and best_state is None):
        raise RuntimeError("沒有完成有效訓練，拒絕發布 checkpoint")
    require_finite(model.state_dict(), "model")
    require_finite(optimizer.state_dict(), "optimizer")
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    trained_versions.update({sample_key(train_dataset, ref): versions[sample_key(train_dataset, ref)]
                             for ref in train_dataset.refs})
    output = CHECKPOINT_DIR / f"stock_model_gpt_{stamp}.pt"
    publish_checkpoint(
        output,
        {
            "model": model.state_dict(),
            "optimizer": optimizer.state_dict(),
            "scaler": scaler.state_dict() if use_cuda else None,
            "settings": asdict(settings),
            "target_names": TARGET_NAMES,
            "output_schema": MODEL_OUTPUT_SCHEMA,
            "input_alignment": INPUT_ALIGNMENT,
            "atr_encoding": ATR_ENCODING,
            "atr_calibration": atr_calibration,
            "naive_baseline": naive_baseline,
            "loss": final_loss,
            "loss_components": final_component_loss,
            "training_progress": {"epochs_run": epochs_run,
                                  "optimizer_steps": best_optimizer_steps if use_validation else optimizer_steps,
                                  "executed_optimizer_steps": optimizer_steps},
            "validation": {
                "used": use_validation,
                "validation_days": settings.validation_days if use_validation else None,
                "epochs_run": len(validation_history) if use_validation else None,
                "selection_metric": "mean_high_low_price_val_loss",
                "best_epoch": best_epoch,
                "best_selection_loss": best_val_loss if best_epoch is not None else None,
                "early_stopped": early_stopped,
                "stalled_targets": stalled_targets,
                "best_component_val_loss": best_component_val if use_validation else None,
                "history": validation_history,
            },
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "training_as_of": as_of.isoformat(),
            "trained_sample_versions": trained_versions,
            "training_data_version": fingerprint(versions),
            "training_window_days": training_window_days,
            "symbols": [path.stem for path in feature_paths],
            "seen_symbols": sorted(seen_symbols | {ref.feature_path.stem for ref in dataset.refs}),
            "parent_checkpoint": str(resume_path.resolve()) if resume_path else None,
            "sampling": sampling,
        },
    )
    if receipt_path is not None:
        from .provenance import atomic_text
        atomic_text(receipt_path, json.dumps({"checkpoint": str(output.resolve()),
                                              "as_of": as_of.isoformat()}, indent=2) + "\n")
    return output
