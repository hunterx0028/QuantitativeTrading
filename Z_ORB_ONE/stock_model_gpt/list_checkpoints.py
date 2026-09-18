"""List checkpoints/*.pt with their authoritative training_as_of, sorted
chronologically.

A checkpoint filename's own timestamp is when training was *run* (wall
clock), which is not always the same as `training_as_of` (the trading-day
data cutoff that run actually trained up to) — e.g. a caught-up/backfilled
run, or a deliberately backdated `train_initial --as-of`. This reads the
authoritative `training_as_of` straight out of each checkpoint's saved
payload instead of guessing from the filename.
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import torch

from .paths import CHECKPOINT_DIR


def checkpoint_summaries(directory: Path = CHECKPOINT_DIR) -> list[dict]:
    """One entry per *.pt in `directory`, sorted by (training_as_of,
    created_at) — chronological by the data cutoff a checkpoint actually
    trained on, tie-broken by when it was produced. A checkpoint that fails
    to load is still listed (with an "error" key) rather than silently
    dropped or aborting the whole scan."""
    summaries = []
    for path in sorted(directory.glob("*.pt")):
        try:
            payload = torch.load(path, map_location="cpu", weights_only=False)
        except Exception as exc:
            summaries.append({"path": path, "training_as_of": None, "created_at": None, "error": str(exc)})
            continue
        summaries.append({
            "path": path,
            "training_as_of": payload.get("training_as_of"),
            "created_at": payload.get("created_at"),
            "parent_checkpoint": payload.get("parent_checkpoint"),
            "loss": payload.get("loss"),
        })
    summaries.sort(key=lambda item: (item["training_as_of"] or "", item["created_at"] or "", item["path"].name))
    return summaries


def latest_checkpoint_as_of(as_of: date, directory: Path = CHECKPOINT_DIR) -> Path:
    """Latest checkpoint whose training_as_of does not exceed `as_of` — the
    same "no future data" rule predict.run_prediction already enforces
    (training_as_of > universe_date is rejected there), surfaced here so a
    caller (e.g. backtest.py) can pick a valid starting checkpoint up front
    instead of guessing from filenames."""
    cutoff = as_of.isoformat()
    candidates = [item for item in checkpoint_summaries(directory)
                 if item.get("training_as_of") and item["training_as_of"] <= cutoff]
    if not candidates:
        raise RuntimeError(f"{directory} 內找不到 training_as_of 不晚於 {cutoff} 的 checkpoint")
    return candidates[-1]["path"]


def main() -> None:
    parser = argparse.ArgumentParser(description="列出 checkpoints 內每個模型的 training_as_of，依日期排序")
    parser.add_argument("--as-of", default=None, help="只列出 training_as_of 不晚於此日期的 checkpoint")
    parser.add_argument("--directory", default=None, help="預設為正式 checkpoints 目錄")
    args = parser.parse_args()
    directory = Path(args.directory) if args.directory else CHECKPOINT_DIR
    summaries = checkpoint_summaries(directory)
    if args.as_of:
        summaries = [item for item in summaries if item.get("training_as_of") and item["training_as_of"] <= args.as_of]
    if not summaries:
        print(f"{directory} 內沒有符合條件的 checkpoint")
        return
    for item in summaries:
        if item.get("error"):
            print(f"{item['path'].name}: 讀取失敗 - {item['error']}")
            continue
        print(f"{item['path'].name}  training_as_of={item['training_as_of']}  "
             f"created_at={item['created_at']}  loss={item['loss']}")


if __name__ == "__main__":
    main()
