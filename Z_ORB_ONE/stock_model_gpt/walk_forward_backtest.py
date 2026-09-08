"""Walk-forward backtest harness (coarse-grained MVP).

For each expanding-window fold: train once at the fold's cutoff date, then let
that SAME checkpoint predict + validate forward for `--test-window-days`
trading days without any further (daily-incremental) training. This does not
simulate `train_daily`'s continuation loop, so it cannot show the effect of
mechanisms that only activate during daily continuation (e.g.
`daily_replay_hit_oversample`) — only of what a fresh `train_initial` learns.

All writes are isolated under `--output-dir` via the STOCK_MODEL_GPT_WRITE_ROOT
environment variable (see paths.py), so this never touches production
checkpoints, predictions, evaluations, universe snapshots, or ATR reports.
Candle/feature/corporate-action history is read-only and shared with
production — nothing here re-fetches or mutates it.

Known limitation (not fixed by this script): every simulated historical
universe snapshot is built from the CURRENT `stock_data.py` selected_stocks
list, not a period-appropriate one. Results carry a look-ahead/survivorship
bias relative to what a live deployment starting at that historical date
would actually have seen.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


PACKAGE = "Z_ORB_ONE.stock_model_gpt"


def _run(module: str, *arguments: str) -> None:
    subprocess.run([sys.executable, "-m", module, *arguments], check=True)


def _plan_folds(
    calendar: list[str], min_training_days: int, test_window_days: int, max_as_of: str | None,
) -> list[dict]:
    if max_as_of:
        calendar = [d for d in calendar if d <= max_as_of]
    folds = []
    cutoff_index = min_training_days - 1
    while cutoff_index + test_window_days < len(calendar):
        steps = [
            {"universe_date": calendar[cutoff_index + k], "prediction_date": calendar[cutoff_index + k + 1]}
            for k in range(test_window_days)
        ]
        folds.append({"cutoff_date": calendar[cutoff_index], "steps": steps})
        cutoff_index += test_window_days
    return folds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coarse-grained walk-forward backtest")
    parser.add_argument("--output-dir", required=True, help="隔離的回測輸出根目錄，不可跟正式環境共用")
    parser.add_argument("--test-window-days", type=int, default=20)
    parser.add_argument("--min-training-days", type=int, default=500, help="第一個 fold 的最少訓練期交易日數")
    parser.add_argument("--max-as-of", default=None, help="限制回測不使用晚於此日期的資料 YYYY-MM-DD")
    parser.add_argument("--settings", default=None, help="傳給 update_data/train_initial/validate_predictions 的 settings 路徑")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    # Must be set BEFORE importing anything from this package: paths.py reads
    # this env var at import time to redirect all write-side directories.
    # Imports below are deliberately deferred past this line — moving them
    # back to the top of the file would silently break the isolation this
    # whole script exists for (production data would be at risk).
    os.environ["STOCK_MODEL_GPT_WRITE_ROOT"] = str(output_dir)

    from .checkpoint_gate import pooled_recall_precision, signal_success_rate
    from .paths import CHECKPOINT_DIR, EVALUATIONS_DIR, FEATURES_DIR, ensure_runtime_dirs
    from .storage import read_jsonl
    from .universe import load_selected_stocks, write_universe_snapshot
    from datetime import date as date_cls

    ensure_runtime_dirs()
    settings_args = ["--settings", args.settings] if args.settings else []

    def write_snapshot(day: str) -> None:
        write_universe_snapshot(load_selected_stocks(), date_cls.fromisoformat(day))

    dates: set[str] = set()
    for path in FEATURES_DIR.glob("*.jsonl"):
        dates.update(row["date"] for row in read_jsonl(path))
    calendar = sorted(dates)

    folds = _plan_folds(calendar, args.min_training_days, args.test_window_days, args.max_as_of)
    if not folds:
        range_text = f"{calendar[0]} ~ {calendar[-1]}" if calendar else "無資料"
        raise RuntimeError(
            f"可用交易日（{len(calendar)} 天，{range_text}）不足以切出任何 fold，"
            f"至少需要 {args.min_training_days + args.test_window_days} 天"
        )
    print(
        f"trading_calendar={len(calendar)} 天（{calendar[0]} ~ {calendar[-1]}），"
        f"切出 {len(folds)} 個 fold，輸出目錄={output_dir}"
    )

    fold_reports = []
    for index, fold in enumerate(folds, start=1):
        cutoff_date = fold["cutoff_date"]
        print(f"=== fold {index}/{len(folds)}: cutoff={cutoff_date} ===")
        write_snapshot(cutoff_date)
        _run(f"{PACKAGE}.train_initial", "--as-of", cutoff_date, *settings_args)
        checkpoint = sorted(CHECKPOINT_DIR.glob("stock_model_gpt_*.pt"))[-1]

        for step in fold["steps"]:
            write_snapshot(step["universe_date"])
            _run(
                f"{PACKAGE}.predict",
                "--checkpoint", str(checkpoint),
                "--universe-date", step["universe_date"],
                "--prediction-date", step["prediction_date"],
            )
            _run(f"{PACKAGE}.validate_predictions", "--prediction-date", step["prediction_date"], *settings_args)

        fold_evaluations = [
            json.loads((EVALUATIONS_DIR / f"{step['prediction_date']}.json").read_text(encoding="utf-8"))
            for step in fold["steps"]
            if (EVALUATIONS_DIR / f"{step['prediction_date']}.json").exists()
        ]
        signal_count, success_rate = signal_success_rate(fold_evaluations)
        fold_reports.append({
            "fold": index,
            "cutoff_date": cutoff_date,
            "checkpoint": checkpoint.name,
            "evaluated_days": len(fold_evaluations),
            "signals": signal_count,
            "signal_success_rate": success_rate,
            "hit_up": pooled_recall_precision(fold_evaluations, "hit_up"),
            "hit_down": pooled_recall_precision(fold_evaluations, "hit_down"),
        })

    all_evaluations = [
        json.loads((EVALUATIONS_DIR / f"{step['prediction_date']}.json").read_text(encoding="utf-8"))
        for fold in folds
        for step in fold["steps"]
        if (EVALUATIONS_DIR / f"{step['prediction_date']}.json").exists()
    ]
    overall_signal_count, overall_success_rate = signal_success_rate(all_evaluations)
    report = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "trading_calendar_days": len(calendar),
        "min_training_days": args.min_training_days,
        "test_window_days": args.test_window_days,
        "fold_count": len(folds),
        "folds": fold_reports,
        "overall": {
            "signals": overall_signal_count,
            "signal_success_rate": overall_success_rate,
            "hit_up": pooled_recall_precision(all_evaluations, "hit_up"),
            "hit_down": pooled_recall_precision(all_evaluations, "hit_down"),
        },
        "caveat": (
            "每個歷史日期的股票清單快照，都是用現在的 selected_stocks 回頭套用，"
            "不是當時真正會用的清單，結果相對於「從那個時間點真的開始上線」"
            "會偏樂觀（look-ahead / survivorship bias）"
        ),
    }
    report_path = output_dir / "backtest_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"=== 完成，共 {len(folds)} 個 fold ===")
    print(f"整體訊號成功率: {overall_signal_count} 筆, {overall_success_rate}")
    print(f"報告已儲存: {report_path}")


if __name__ == "__main__":
    main()
