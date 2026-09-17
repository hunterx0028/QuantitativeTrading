from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date
from pathlib import Path

from .config import Settings
from .paths import CHECKPOINT_DIR
from .training import train
from .checkpoints import current_checkpoint


def latest_checkpoint() -> Path:
    return current_checkpoint(CHECKPOINT_DIR)


from .runtime_lock import locked


@locked
def main() -> None:
    parser = argparse.ArgumentParser(description="從前一版本繼續每日訓練")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--settings", default=None)
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument("--training-mode", choices=("incremental_replay", "full_history"), default=None)
    parser.add_argument("--force-retrain", action="store_true", help="明確允許重跑同日期或只有歷史資料的訓練")
    parser.add_argument(
        "--training-window-days", type=int, default=None,
        help="只用截止日往前數的 N 個交易日續訓（滾動視窗，含重播樣本池）；預設不限制",
    )
    args = parser.parse_args()
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    if args.training_mode:
        settings = replace(settings, daily_training_mode=args.training_mode)
    source = Path(args.checkpoint) if args.checkpoint else latest_checkpoint()
    output = train(
        settings,
        resume_path=source,
        daily=True,
        as_of=date.fromisoformat(args.as_of),
        force_retrain=args.force_retrain,
        training_window_days=args.training_window_days,
    )
    print(f"來源模型: {source}")
    print(f"候選模型: {output}")


if __name__ == "__main__":
    main()
