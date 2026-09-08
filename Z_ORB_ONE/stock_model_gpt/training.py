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
from .dataset import StockSequenceDataset
from .device import describe_device, move_targets_to_device, select_device
from .model import StockAutoregressiveModel
from .paths import CHECKPOINT_DIR, FEATURES_DIR, ensure_runtime_dirs
from .universe import recent_symbols
from .storage import read_jsonl
from .replay import select_daily_sequences


TARGET_NAMES = ("price", "hit_up", "hit_down")


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
    criteria: dict[str, nn.Module] = {"price": nn.CrossEntropyLoss()}
    for field in ("hit_up", "hit_down"):
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
) -> torch.Tensor:
    return (
        settings.loss_price * criteria["price"](outputs["price"], targets["price"])
        + settings.loss_hit_up * criteria["hit_up"](outputs["hit_up"], targets["hit_up"])
        + settings.loss_hit_down * criteria["hit_down"](outputs["hit_down"], targets["hit_down"])
    )


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
            "三目標模型請先重新執行 train_initial"
        )


def train(
    settings: Settings,
    resume_path: Path | None = None,
    daily: bool = False,
    as_of: date | None = None,
    force_retrain: bool = False,
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
    settings, atr_calibration, atr_report_path = prepare_atr_levels(settings, feature_paths, as_of, checkpoint)
    dataset = StockSequenceDataset(
        feature_paths,
        settings.context_days,
        max_target_date=as_of,
        atr_boundaries_pct=settings.atr_boundaries_pct,
    )
    if not dataset:
        raise RuntimeError("沒有足夠的特徵序列可供訓練")
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
    device = select_device()
    use_cuda = device.type == "cuda"
    print(f"device={describe_device(device)}")
    criteria = build_criteria(settings, dataset, device)
    loader = DataLoader(
        dataset,
        batch_size=settings.batch_size,
        shuffle=True,
        pin_memory=use_cuda,
    )
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
    for epoch in range(epochs):
        total_loss = 0.0
        for states, targets in loader:
            states = states.to(device, non_blocking=use_cuda)
            targets = move_targets_to_device(targets, device, non_blocking=use_cuda)
            optimizer.zero_grad(set_to_none=True)
            with torch.amp.autocast(
                device_type="cuda",
                dtype=torch.float16,
                enabled=use_cuda,
            ):
                loss = weighted_loss(model(states), targets, settings, criteria)
            scaler.scale(loss).backward()
            scaler.unscale_(optimizer)
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            scaler.step(optimizer)
            scaler.update()
            total_loss += float(loss.detach()) * states.shape[0]
        final_loss = total_loss / len(dataset)
        print(f"epoch={epoch + 1}/{epochs} loss={final_loss:.6f}")

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
            "atr_report": str(atr_report_path) if atr_report_path else None,
            "loss": final_loss,
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
