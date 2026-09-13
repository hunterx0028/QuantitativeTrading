from __future__ import annotations

import argparse
import subprocess
import sys


def run(module: str, *arguments: str) -> None:
    subprocess.run([sys.executable, "-m", module, *arguments], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="每日資料、特徵、增量訓練與預測流程")
    parser.add_argument("--as-of", required=True, help="今日完整日K日期 YYYY-MM-DD")
    parser.add_argument("--prediction-date", required=True, help="下一交易日 YYYY-MM-DD")
    parser.add_argument("--training-mode", choices=("incremental_replay", "full_history"), default=None)
    parser.add_argument("--force-retrain", action="store_true")
    parser.add_argument(
        "--training-window-days", type=int, default=None,
        help="只用截止日往前數的 N 個交易日續訓（滾動視窗）；預設不限制，使用全部可用歷史。"
             "要跟 walk_forward_backtest_v2.py 驗證過的設定一致，請明確指定（見 README）",
    )
    parser.add_argument(
        "--skip-predict", action="store_true",
        help="只做 update_data/prepare_features/train_daily，不跑 predict。"
             "predict 需要 --prediction-date 當天開盤前的夜盤資料（通常隔天凌晨才有），"
             "收盤後沒有這筆資料時用這個旗標，之後另外單獨執行 predict.py（見 README 第 4、8 節）",
    )
    args = parser.parse_args()
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
    if args.skip_predict:
        print(
            "[SKIP] 已略過 predict；確認 night_futures.jsonl 已有 "
            f"{args.prediction_date} 這筆資料後，另外執行："
            f"python -m Z_ORB_ONE.stock_model_gpt.predict "
            f"--universe-date {args.as_of} --prediction-date {args.prediction_date}"
        )
        return
    run(
        "Z_ORB_ONE.stock_model_gpt.predict",
        "--prediction-date", args.prediction_date,
        "--universe-date", args.as_of,
    )


if __name__ == "__main__":
    main()
