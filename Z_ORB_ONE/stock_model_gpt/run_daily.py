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
    args = parser.parse_args()
    run("Z_ORB_ONE.stock_model_gpt.update_data", "--as-of", args.as_of)
    run("Z_ORB_ONE.stock_model_gpt.prepare_features", "--as-of", args.as_of)
    run("Z_ORB_ONE.stock_model_gpt.train_daily", "--as-of", args.as_of)
    run(
        "Z_ORB_ONE.stock_model_gpt.predict",
        "--prediction-date", args.prediction_date,
        "--universe-date", args.as_of,
    )


if __name__ == "__main__":
    main()
