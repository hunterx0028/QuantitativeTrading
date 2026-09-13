from __future__ import annotations

import random
import hashlib
import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

import torch
from torch import nn
from torch.utils.data import DataLoader

from .config import Settings
from .atr_calibration import ATR_ENCODING, prepare_atr_levels, validate_five_levels
from .dataset import StockSequenceDataset, subset
from .device import describe_device, move_targets_to_device, select_device
from .model import StockAutoregressiveModel
from .paths import CHECKPOINT_DIR, FEATURES_DIR, ensure_runtime_dirs
from .universe import recent_symbols
from .storage import read_jsonl
from .replay import select_daily_sequences


TARGET_NAMES = ("hit_up",)


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
    """Actual hit_up distribution among the dataset's own target rows (the
    same population it trains on), so a caller can score a trivial "always
    guess the majority class" baseline against real evaluation days and see
    whether the model beats doing nothing."""
    hit_up_counts = {"true": 0, "false": 0}
    for ref in dataset.refs:
        row = dataset.rows_by_path[ref.feature_path][ref.end]
        hit_up_counts["true" if row["hit_up"] else "false"] += 1
    total = len(dataset.refs)
    majority_hit_up = "true" if hit_up_counts["true"] > hit_up_counts["false"] else "false"
    return {
        "count": total,
        "hit_up_counts": hit_up_counts,
        "majority_hit_up": majority_hit_up,
    }


def binary_class_weight(dataset: StockSequenceDataset, field: str) -> torch.Tensor | None:
    """Inverse-frequency weight for a rare binary target (hit_up/hit_down)."""
    positives = sum(
        bool(dataset.rows_by_path[ref.feature_path][ref.end][field]) for ref in dataset.refs
    )
    total = len(dataset.refs)
    negatives = total - positives
    if positives == 0 or negatives == 0:
        return None
    return torch.tensor(
        [total / (2.0 * negatives), total / (2.0 * positives)], dtype=torch.float32,
    )


class FocalLoss(nn.Module):
    """Multi-class focal loss: down-weights samples the model already classifies
    confidently, so gradient stays focused on rare, hard hit_up/hit_down positives."""

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
    for field in ("hit_up",):
        weight = binary_class_weight(dataset, field)
        criteria[field] = FocalLoss(
            gamma=settings.focal_gamma,
            weight=weight.to(device) if weight is not None else None,
        )
        if weight is not None:
            print(
                f"class_weight {field}: negative={weight[0]:.4f} positive={weight[1]:.4f} "
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
    components = {
        "hit_up": criteria["hit_up"](outputs["hit_up"], targets["hit_up"]),
    }
    combined = settings.loss_hit_up * components["hit_up"]
    return combined, components


def ensure_checkpoint_compatible(checkpoint: dict) -> None:
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
            "只預測 hit_up 的模型請先重新執行 train_initial"
        )
    if "target_night_futures_embedding.weight" not in checkpoint.get("model", {}):
        raise RuntimeError(
            "checkpoint 缺少「被預測日當天盤前夜盤」這個獨立輸入"
            "（target_night_futures_embedding）；請先重新執行 train_initial"
        )


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


def train(
    settings: Settings,
    resume_path: Path | None = None,
    daily: bool = False,
    as_of: date | None = None,
    force_retrain: bool = False,
    training_window_days: int | None = None,
) -> Path:
    ensure_runtime_dirs()
    seed_everything(settings.seed)
    as_of = as_of or date.today()
    if daily and resume_path is None:
        raise ValueError("每日續訓必須指定既有 checkpoint，不能重新擬合 ATR 刻度")
    checkpoint = None
    receipt_path = None
    if resume_path:
        checkpoint = torch.load(resume_path, map_location="cpu", weights_only=False)
        ensure_checkpoint_compatible(checkpoint)
        previous_as_of = checkpoint.get("training_as_of")
        if previous_as_of and previous_as_of > as_of.isoformat():
            raise RuntimeError("checkpoint 訓練截止日晚於本次日期，不可用未來模型回訓歷史日期")
        if daily:
            if not previous_as_of:
                raise RuntimeError("checkpoint 缺少 training_as_of，無法區分新增與歷史序列")
            if previous_as_of == as_of.isoformat() and not force_retrain:
                print("[SKIP] 此 checkpoint 已完成該日期續訓")
                return resume_path
            key = hashlib.sha256(f"{resume_path.resolve()}|{as_of.isoformat()}".encode()).hexdigest()
            receipt_path = CHECKPOINT_DIR / "daily_runs" / f"{key}.json"
            if receipt_path.exists() and not force_retrain:
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                completed = Path(receipt["checkpoint"])
                if completed.exists():
                    print(f"[SKIP] 相同來源模型與截止日已完成: {completed}")
                    return completed
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
    # Captured before `select_daily_sequences` below narrows dataset.refs to a
    # sampled training subset; the baseline should reflect the full window.
    naive_baseline = label_distribution(dataset)
    sampling = None
    seen_symbols = set(checkpoint.get("seen_symbols", checkpoint.get("symbols", []))) if checkpoint else set()
    if daily:
        sampling = select_daily_sequences(dataset, previous_as_of, as_of, settings, seen_symbols)
        if not sampling["new_count"] and not force_retrain:
            print("[SKIP] 沒有新增目標序列，不重複訓練歷史資料")
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
    # class weighting for hit_up/hit_down is a legitimate training technique,
    # but its output is numerically on a different scale than the plain
    # -log(p_true) the walk-forward backtest scores real predictions with;
    # comparing the two directly (as earlier diagnostics here did) understates
    # how much of the in-sample/out-of-sample gap is real overfitting versus
    # just two different loss units.
    plain_criteria = {name: nn.CrossEntropyLoss() for name in ("hit_up",)}
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
    model.train()
    final_loss = float("nan")
    final_component_loss = {"hit_up": float("nan")}
    monitored_targets = ("hit_up",)
    best_val_loss = float("inf")
    best_epoch: int | None = None
    best_state: dict[str, torch.Tensor] | None = None
    early_stopped = False
    stalled_targets: list[str] = []
    validation_history: list[dict] = []
    best_component_val = {name: float("inf") for name in monitored_targets}
    component_patience = {name: 0 for name in monitored_targets}
    for epoch in range(epochs):
        total_loss = 0.0
        total_component_loss = {"hit_up": 0.0}
        for states, target_night_futures, targets in loader:
            states = states.to(device, non_blocking=use_cuda)
            target_night_futures = target_night_futures.to(device, non_blocking=use_cuda)
            targets = move_targets_to_device(targets, device, non_blocking=use_cuda)
            optimizer.zero_grad(set_to_none=True)
            with torch.amp.autocast(
                device_type="cuda",
                dtype=torch.float16,
                enabled=use_cuda,
            ):
                outputs = model(states, target_night_futures)
                loss, _ = weighted_loss(outputs, targets, settings, criteria)
                # Logged/saved per-target loss uses plain CE (see plain_criteria
                # above), not the FocalLoss components `loss` was built from.
                plain_components = {
                    name: plain_criteria[name](outputs[name], targets[name]) for name in monitored_targets
                }
            scaler.scale(loss).backward()
            scaler.unscale_(optimizer)
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            scaler.step(optimizer)
            scaler.update()
            batch_size = states.shape[0]
            total_loss += float(loss.detach()) * batch_size
            for name, value in plain_components.items():
                total_component_loss[name] += float(value.detach()) * batch_size
        final_loss = total_loss / len(train_dataset)
        final_component_loss = {name: value / len(train_dataset) for name, value in total_component_loss.items()}
        log_line = (
            f"epoch={epoch + 1}/{epochs} loss={final_loss:.6f} (加權合計) | 未加權: "
            f"hit_up={final_component_loss['hit_up']:.6f}"
        )

        if not use_validation:
            print(log_line)
            continue

        model.eval()
        val_total_component_loss = {"hit_up": 0.0}
        with torch.no_grad():
            for states, target_night_futures, targets in val_loader:
                states = states.to(device, non_blocking=use_cuda)
                target_night_futures = target_night_futures.to(device, non_blocking=use_cuda)
                targets = move_targets_to_device(targets, device, non_blocking=use_cuda)
                with torch.amp.autocast(
                    device_type="cuda",
                    dtype=torch.float16,
                    enabled=use_cuda,
                ):
                    outputs = model(states, target_night_futures)
                    # Plain CE here too — see plain_criteria above.
                    plain_components = {
                        name: plain_criteria[name](outputs[name], targets[name]) for name in monitored_targets
                    }
                batch_size = states.shape[0]
                for name, value in plain_components.items():
                    val_total_component_loss[name] += float(value.detach()) * batch_size
        model.train()
        val_component_loss = {name: value / len(val_dataset) for name, value in val_total_component_loss.items()}
        # Informational only (not used for any decision below): the same
        # weighted combination training optimizes, but built from the plain
        # per-target loss above rather than a second FocalLoss pass.
        val_loss = settings.loss_hit_up * val_component_loss["hit_up"]
        validation_history.append({
            "epoch": epoch + 1, "val_loss": val_loss, "val_components": val_component_loss,
            "train_loss": final_loss, "train_components": final_component_loss,
        })
        print(log_line + f" | validation: loss={val_loss:.6f} hit_up={val_component_loss['hit_up']:.6f}")
        # Which epoch's weights to keep is judged by hit_up's own validation loss.
        selection_loss = val_component_loss["hit_up"]
        if selection_loss < best_val_loss - 1e-6:
            best_val_loss = selection_loss
            best_epoch = epoch + 1
            best_state = {key: value.detach().clone() for key, value in model.state_dict().items()}

        for name in monitored_targets:
            if val_component_loss[name] < best_component_val[name] - 1e-6:
                best_component_val[name] = val_component_loss[name]
                component_patience[name] = 0
            else:
                component_patience[name] += 1

        stalled_targets = [name for name in monitored_targets if component_patience[name] >= settings.early_stopping_patience]
        if stalled_targets:
            early_stopped = True
            print(
                f"[EARLY STOP] {'/'.join(stalled_targets)} 連續 {settings.early_stopping_patience} 個 epoch "
                f"驗證 loss 沒有改善，停在 epoch {epoch + 1}；"
                f"採用 hit_up 驗證 loss 最佳的 epoch {best_epoch}（selection_loss={best_val_loss:.6f}）"
            )
            break

    if use_validation and best_state is not None:
        model.load_state_dict(best_state)
        final_loss, final_component_loss = next(
            (item["train_loss"], item["train_components"])
            for item in validation_history if item["epoch"] == best_epoch
        )
        print(
            f"採用驗證集最佳權重（依 hit_up 挑選）："
            f"epoch={best_epoch}, selection_loss={best_val_loss:.6f}（訓練總 epoch 上限={epochs}）"
        )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    output = CHECKPOINT_DIR / f"stock_model_gpt_{stamp}.pt"
    torch.save(
        {
            "model": model.state_dict(),
            "optimizer": optimizer.state_dict(),
            "scaler": scaler.state_dict() if use_cuda else None,
            "settings": asdict(settings),
            "target_names": TARGET_NAMES,
            "atr_encoding": ATR_ENCODING,
            "atr_calibration": atr_calibration,
            "naive_baseline": naive_baseline,
            "loss": final_loss,
            "loss_components": final_component_loss,
            "validation": {
                "used": use_validation,
                "validation_days": settings.validation_days if use_validation else None,
                "epochs_run": len(validation_history) if use_validation else None,
                "selection_metric": "hit_up_val_loss",
                "best_epoch": best_epoch,
                "best_selection_loss": best_val_loss if best_epoch is not None else None,
                "early_stopped": early_stopped,
                "stalled_targets": stalled_targets,
                "best_component_val_loss": best_component_val if use_validation else None,
                "history": validation_history,
            },
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "training_as_of": as_of.isoformat(),
            "symbols": [path.stem for path in feature_paths],
            "seen_symbols": sorted(seen_symbols | {ref.feature_path.stem for ref in dataset.refs}),
            "parent_checkpoint": str(resume_path.resolve()) if resume_path else None,
            "sampling": sampling,
        },
        output,
    )
    if receipt_path is not None:
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = receipt_path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"checkpoint": str(output.resolve()),
                                         "as_of": as_of.isoformat()}, indent=2) + "\n", encoding="utf-8")
        temporary.replace(receipt_path)
    return output
