from __future__ import annotations

import argparse
from datetime import date

from .config import Settings
from .training import train


def main() -> None:
    parser = argparse.ArgumentParser(description="訓練初始 stock_model_gpt")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--as-of", default=date.today().isoformat())
    args = parser.parse_args()
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    output = train(settings, as_of=date.fromisoformat(args.as_of))
    print(f"模型已儲存: {output}")


if __name__ == "__main__":
    main()
