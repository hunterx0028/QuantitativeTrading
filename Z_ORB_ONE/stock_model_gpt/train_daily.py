from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from .config import Settings
from .paths import CHECKPOINT_DIR
from .training import train


def latest_checkpoint() -> Path:
    paths = sorted(CHECKPOINT_DIR.glob("stock_model_gpt_*.pt"))
    if not paths:
        raise RuntimeError("找不到既有 checkpoint，請先執行 train_initial")
    return paths[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description="從前一版本繼續每日訓練")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--settings", default=None)
    parser.add_argument("--as-of", default=date.today().isoformat())
    args = parser.parse_args()
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    source = Path(args.checkpoint) if args.checkpoint else latest_checkpoint()
    output = train(
        settings,
        resume_path=source,
        daily=True,
        as_of=date.fromisoformat(args.as_of),
    )
    print(f"來源模型: {source}")
    print(f"候選模型: {output}")


if __name__ == "__main__":
    main()
