from __future__ import annotations

import argparse
import importlib
import sys
from datetime import date

from .night_futures import load_night_futures
from .trading_calendar import TradingCalendar
from .checkpoint_gate import validation_queue, refresh_gate_for_prediction
from .config import Settings


def run(module: str, *arguments: str) -> None:
    previous_argv = sys.argv
    try:
        sys.argv = [module, *arguments]
        importlib.import_module(module).main()
    finally:
        sys.argv = previous_argv


from .runtime_lock import locked


@locked
def main() -> None:
    parser = argparse.ArgumentParser(description="每日資料更新、特徵產生與增量訓練流程")
    parser.add_argument("--as-of", required=True, help="今日完整日K日期 YYYY-MM-DD")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--training-mode", choices=("incremental_replay", "full_history"), default=None)
    parser.add_argument("--force-retrain", action="store_true")
    parser.add_argument(
        "--training-window-days", type=int, default=None,
        help="只用截止日往前數的 N 個交易日續訓（滾動視窗）；預設不限制，使用全部可用歷史。"
             "例如 --training-window-days 150（見 README）",
    )
    args = parser.parse_args()
    args.as_of = date.fromisoformat(args.as_of).isoformat()
    TradingCalendar().require_session(args.as_of)
    settings_args = ["--settings", args.settings] if args.settings else []
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    if load_night_futures().get(args.as_of) is None:
        print(f"[錯誤] 缺少 {args.as_of} 的夜盤資料，已中止每日更新與續訓。")
        raise SystemExit(1)
    run("Z_ORB_ONE.stock_model_gpt.update_data", "--as-of", args.as_of, "--require-complete", *settings_args)
    for prediction in validation_queue(settings, args.as_of):
        run("Z_ORB_ONE.stock_model_gpt.validate_predictions", "--prediction-date", prediction.stem,
            "--predictions", str(prediction.resolve()), *settings_args)
    status = refresh_gate_for_prediction(settings, args.as_of, require_fresh=False)
    print(f"每日 gate: {status['verdict']} — {status['reason']}")
    run("Z_ORB_ONE.stock_model_gpt.prepare_features", "--as-of", args.as_of, *settings_args)
    training_args = ["--as-of", args.as_of, *settings_args]
    if args.training_mode:
        training_args.extend(["--training-mode", args.training_mode])
    if args.force_retrain:
        training_args.append("--force-retrain")
    if args.training_window_days is not None:
        training_args.extend(["--training-window-days", str(args.training_window_days)])
    run("Z_ORB_ONE.stock_model_gpt.train_daily", *training_args)


if __name__ == "__main__":
    main()
