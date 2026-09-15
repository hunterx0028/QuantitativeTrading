from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date

from .night_futures import load_night_futures


def run(module: str, *arguments: str) -> None:
    subprocess.run([sys.executable, "-m", module, *arguments], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="每日資料更新、特徵產生與增量訓練流程")
    parser.add_argument("--as-of", required=True, help="今日完整日K日期 YYYY-MM-DD")
    parser.add_argument("--training-mode", choices=("incremental_replay", "full_history"), default=None)
    parser.add_argument("--force-retrain", action="store_true")
    parser.add_argument(
        "--training-window-days", type=int, default=None,
        help="只用截止日往前數的 N 個交易日續訓（滾動視窗）；預設不限制，使用全部可用歷史。"
             "要跟 walk_forward_backtest_v2.py 驗證過的設定一致，請明確指定（見 README）",
    )
    args = parser.parse_args()
    args.as_of = date.fromisoformat(args.as_of).isoformat()
    if load_night_futures().get(args.as_of) is None:
        print(f"[錯誤] 缺少 {args.as_of} 的夜盤資料，已中止每日更新與續訓。")
        raise SystemExit(1)
    run("Z_ORB_ONE.stock_model_gpt.update_data", "--as-of", args.as_of)
    run("Z_ORB_ONE.stock_model_gpt.prepare_features", "--as-of", args.as_of)
    training_args = ["--as-of", args.as_of]
    if args.training_mode:
        training_args.extend(["--training-mode", args.training_mode])
    if args.force_retrain:
        training_args.append("--force-retrain")
    if args.training_window_days is not None:
        training_args.extend(["--training-window-days", str(args.training_window_days)])
    run("Z_ORB_ONE.stock_model_gpt.train_daily", *training_args)


if __name__ == "__main__":
    main()
