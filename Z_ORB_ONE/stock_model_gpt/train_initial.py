from __future__ import annotations

import argparse
from datetime import date

from .config import Settings
from .training import train


from .runtime_lock import locked


@locked
def main() -> None:
    parser = argparse.ArgumentParser(description="訓練初始 stock_model_gpt")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--as-of", required=True, help="訓練資料截止日；ATR 使用固定五級界線")
    parser.add_argument(
        "--training-window-days", type=int, default=None,
        help="只用截止日往前數的 N 個交易日作訓練目標（滾動視窗）；預設不限制，使用全部可用歷史",
    )
    args = parser.parse_args()
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    output = train(
        settings,
        as_of=date.fromisoformat(args.as_of),
        training_window_days=args.training_window_days,
    )
    print(f"模型已儲存: {output}")


if __name__ == "__main__":
    main()
