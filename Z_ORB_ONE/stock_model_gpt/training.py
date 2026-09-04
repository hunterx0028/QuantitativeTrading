from __future__ import annotations

import random
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

import torch
from torch import nn
from torch.utils.data import DataLoader

from .config import Settings
from .dataset import StockSequenceDataset
from .model import StockAutoregressiveModel
from .paths import CHECKPOINT_DIR, FEATURES_DIR, ensure_runtime_dirs
from .universe import recent_symbols


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


def weighted_loss(outputs, targets, settings: Settings) -> torch.Tensor:
    criterion = nn.CrossEntropyLoss()
    return (
        settings.loss_price * criterion(outputs["price"], targets["price"])
        + settings.loss_hit_up * criterion(outputs["hit_up"], targets["hit_up"])
        + settings.loss_hit_down * criterion(outputs["hit_down"], targets["hit_down"])
        + settings.loss_close_limit * criterion(outputs["close_limit"], targets["close_limit"])
    )


def train(
    settings: Settings,
    resume_path: Path | None = None,
    daily: bool = False,
    as_of: date | None = None,
) -> Path:
    ensure_runtime_dirs()
    seed_everything(settings.seed)
    as_of = as_of or date.today()
    eligible_symbols = recent_symbols(as_of, settings.recent_universe_days)
    all_feature_paths = list(FEATURES_DIR.glob("*.jsonl"))
    feature_paths = sorted(
        path for path in all_feature_paths
        if not eligible_symbols or path.stem in eligible_symbols
    )
    dataset = StockSequenceDataset(
        feature_paths,
        settings.context_days,
        max_target_date=as_of,
    )
    if not dataset:
        raise RuntimeError("沒有足夠的特徵序列可供訓練")
    loader = DataLoader(dataset, batch_size=settings.batch_size, shuffle=True)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = build_model(settings).to(device)
    learning_rate = settings.daily_learning_rate if daily else settings.learning_rate
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    if resume_path:
        checkpoint = torch.load(resume_path, map_location=device, weights_only=False)
        previous_as_of = checkpoint.get("training_as_of")
        if previous_as_of and previous_as_of > as_of.isoformat():
            raise RuntimeError(
                f"checkpoint 訓練截止日 {previous_as_of} 晚於本次 {as_of.isoformat()}，"
                "不可用未來模型回訓歷史日期"
            )
        model.load_state_dict(checkpoint["model"])
        if "optimizer" in checkpoint:
            optimizer.load_state_dict(checkpoint["optimizer"])
            for group in optimizer.param_groups:
                group["lr"] = learning_rate

    epochs = settings.daily_epochs if daily else settings.epochs
    model.train()
    final_loss = float("nan")
    for epoch in range(epochs):
        total_loss = 0.0
        for states, targets in loader:
            states = states.to(device)
            targets = {key: value.to(device) for key, value in targets.items()}
            optimizer.zero_grad(set_to_none=True)
            loss = weighted_loss(model(states), targets, settings)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            total_loss += float(loss.detach()) * states.shape[0]
        final_loss = total_loss / len(dataset)
        print(f"epoch={epoch + 1}/{epochs} loss={final_loss:.6f}")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = CHECKPOINT_DIR / f"stock_model_gpt_{stamp}.pt"
    torch.save(
        {
            "model": model.state_dict(),
            "optimizer": optimizer.state_dict(),
            "settings": asdict(settings),
            "loss": final_loss,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "training_as_of": as_of.isoformat(),
            "symbols": [path.stem for path in feature_paths],
        },
        output,
    )
    return output
