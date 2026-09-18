"""Walk-forward backtest: replays predict -> validate (-> optional daily
incremental train) across a historical date range, reusing run_prediction /
run_validation / train exactly as the real daily pipeline does (see their own
docstrings — they were already written to be reusable this way, to avoid
paying a fresh Python/torch interpreter startup cost per simulated day).

Output is fully isolated under STOCK_MODEL_GPT_WRITE_ROOT (see paths.py), so a
backtest never overwrites production predictions, checkpoints, evaluations,
universe snapshots, or gate_status.json. Candles/features/corporate_actions/
night_futures are shared, read-only production data throughout — a backtest
never mutates them.

`STOCK_MODEL_GPT_WRITE_ROOT` must be set *before* `paths.py` is imported
anywhere in the process (it reads the env var once, at import time). Since
this module's own imports below already pull in `paths` transitively, `main`
re-execs itself into a fresh process with the env var set rather than trying
to mutate an already-imported `paths` module.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

from . import paths
from .checkpoint_gate import compute_gate_status, pooled_recall_precision, signal_success_rate
from .classification_metrics import matrix_metrics, pool_matrices
from .config import Settings
from .list_checkpoints import latest_checkpoint_as_of
from .predict import add_prediction_signal_arguments, prediction_signal_thresholds, run_prediction
from .provenance import atomic_text
from .signals import SignalThresholds
from .trading_calendar import TradingCalendar
from .training import TARGET_NAMES, train
from .validate_predictions import run_validation


_PACKAGE_DIR = Path(__file__).resolve().parent


def _previous_session(calendar: TradingCalendar, day: date) -> date:
    current = day
    for _ in range(40):
        current -= timedelta(days=1)
        if calendar.is_session(current):
            return current
    raise ValueError(f"40 天內找不到 {day} 的前一交易日，請檢查交易日曆")


def pool_backtest_report(day_results: list[dict]) -> dict:
    """Pool per-day validate_predictions summaries the same way a daily gate
    computation pools evaluation files — sum tp/fp/fn and confusion matrices
    across days rather than averaging per-day rates (see
    checkpoint_gate.pooled_recall_precision's own rationale)."""
    targets = {}
    for target in TARGET_NAMES:
        rows = [day["targets"][target] for day in day_results if target in day.get("targets", {})]
        evaluated = sum(row.get("evaluated", 0) for row in rows)
        loss_sum = sum(row.get("log_loss_sum", {}).get(target) or 0.0 for row in rows)
        baseline_terms = [row.get("log_loss_sum", {}).get(f"baseline_{target}") for row in rows]
        baseline_sum = None if not rows or any(term is None for term in baseline_terms) else sum(baseline_terms)
        signal_count, signal_success, signal_rate = signal_success_rate(rows) if rows else (0, None, None)
        targets[target] = {
            "days": len(rows),
            "evaluated": evaluated,
            "classification": matrix_metrics(pool_matrices(
                [row["classification"]["confusion_matrix"] for row in rows])) if rows else None,
            "log_loss": loss_sum / evaluated if evaluated else None,
            "baseline_log_loss": baseline_sum / evaluated if (baseline_sum is not None and evaluated) else None,
            "signal": pooled_recall_precision(rows, target) if rows else None,
            "signal_count": signal_count,
            "signal_success_count": signal_success,
            "signal_success_rate": signal_rate,
        }
    return {"targets": targets}


def run_backtest(
    checkpoint_path: Path,
    start_date: date,
    end_date: date,
    settings: Settings,
    thresholds: SignalThresholds = SignalThresholds(),
    low_thresholds: SignalThresholds = SignalThresholds((-2, -1), 60),
    daily_train: bool = False,
    training_window_days: int | None = None,
    force_retrain: bool = False,
) -> dict:
    """Core walk-forward loop, reusable both by the CLI (`main`) and by tests
    without needing STOCK_MODEL_GPT_WRITE_ROOT — the caller is responsible for
    directing writes somewhere isolated before calling this (see
    `_main_isolated` below, or a test's monkeypatched `paths` attributes)."""
    if end_date < start_date:
        raise ValueError("end-date 必須不早於 start-date")
    calendar = TradingCalendar()
    calendar.require_session(start_date)
    calendar.require_session(end_date)
    current_checkpoint = Path(checkpoint_path)
    day_results: list[dict] = []
    failures: list[dict] = []
    prediction_date = start_date
    while prediction_date <= end_date:
        try:
            universe_date = _previous_session(calendar, prediction_date)
            prediction_path, _naive_baseline, _in_sample_loss = run_prediction(
                current_checkpoint, universe_date, prediction_date, thresholds, low_thresholds,
            )
            summary = run_validation(prediction_date.isoformat(), settings, prediction_path=prediction_path)
            day_results.append(summary)
            if daily_train:
                current_checkpoint = train(
                    settings, resume_path=current_checkpoint, daily=True, as_of=prediction_date,
                    force_retrain=force_retrain, training_window_days=training_window_days,
                )
        except RuntimeError as exc:
            failures.append({"date": prediction_date.isoformat(), "reason": str(exc)})
        prediction_date = date.fromisoformat(calendar.next_session(prediction_date))
    report = pool_backtest_report(day_results)
    report.update(
        start_date=start_date.isoformat(), end_date=end_date.isoformat(),
        days_run=len(day_results), days_failed=len(failures), failures=failures,
        initial_checkpoint=str(Path(checkpoint_path)), final_checkpoint=str(current_checkpoint),
        daily_train=daily_train,
    )
    if day_results:
        report["gate_status"] = compute_gate_status(settings)
    return report


def _seed_universe_snapshots() -> int:
    """Historical universe snapshots are shared, real production data (like
    candles/features), but paths.UNIVERSE_DIR is one of the write targets that
    get redirected under an isolated write root — so a fresh isolated
    directory starts with none. Copy in whatever the real production
    environment has already accumulated, without ever writing back to it."""
    source = paths.PACKAGE_DIR / "data" / "universe"
    if not source.exists():
        return 0
    paths.UNIVERSE_DIR.mkdir(parents=True, exist_ok=True)
    copied = 0
    for item in source.glob("*.json"):
        target = paths.UNIVERSE_DIR / item.name
        if not target.exists():
            shutil.copy2(item, target)
            copied += 1
    return copied


def _fmt(value, pct: bool = True) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2%}" if pct else f"{value:.4f}"


def _print_report(report: dict) -> None:
    print(f"回測區間: {report['start_date']} ~ {report['end_date']}，"
          f"完成 {report['days_run']} 天，失敗/跳過 {report['days_failed']} 天")
    if report["failures"]:
        for reason, count in Counter(item["reason"] for item in report["failures"]).most_common(5):
            print(f"  [SKIP x{count}] {reason}")
    for target, stats in report["targets"].items():
        if stats["days"] == 0:
            print(f"[{target}] 無可用資料")
            continue
        accuracy = stats["classification"]["accuracy"] if stats["classification"] else None
        print(f"[{target}] days={stats['days']} evaluated={stats['evaluated']} "
              f"accuracy={_fmt(accuracy)} log_loss={_fmt(stats['log_loss'], pct=False)} "
              f"baseline_log_loss={_fmt(stats['baseline_log_loss'], pct=False)}")
        signal = stats["signal"]
        if signal:
            print(f"  訊號 precision={_fmt(signal['precision'])} recall={_fmt(signal['recall'])} "
                  f"(tp={signal['tp']} fp={signal['fp']} fn={signal['fn']})")
    if "gate_status" in report:
        print(f"回測結束時 gate 狀態: {report['gate_status']['verdict']} — {report['gate_status']['reason']}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Walk-forward 回測：對歷史區間重跑 predict/validate（可選每日續訓），"
                    "輸出完全隔離在獨立目錄，不會動到正式 predictions/checkpoints/evaluations/universe"
    )
    parser.add_argument("--start-date", required=True, help="回測起始的『被預測日』YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="回測結束的『被預測日』YYYY-MM-DD（含）")
    parser.add_argument("--checkpoint", default=None,
                        help="回測起點使用的 checkpoint 路徑；省略時自動選用正式 checkpoints 內、"
                             "training_as_of 不晚於起始日前一交易日的最新一顆")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--write-root", default=None, help="隔離輸出目錄；預設在 backtests/<起訖日期>_<時間戳記> 底下")
    parser.add_argument("--daily-train", action="store_true",
                        help="每天預測、驗證後也模擬每日續訓；預設不訓練，只評估固定不變的起始模型")
    parser.add_argument("--training-window-days", type=int, default=None)
    parser.add_argument("--force-retrain", action="store_true")
    add_prediction_signal_arguments(parser)
    return parser


def _resolve_write_root(args: argparse.Namespace) -> Path:
    if args.write_root:
        return Path(args.write_root).resolve()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (_PACKAGE_DIR / "backtests" / f"{args.start_date}_{args.end_date}_{stamp}").resolve()


def main() -> None:
    args = _build_parser().parse_args()
    write_root = _resolve_write_root(args)
    if os.environ.get("STOCK_MODEL_GPT_WRITE_ROOT") != str(write_root):
        os.environ["STOCK_MODEL_GPT_WRITE_ROOT"] = str(write_root)
        forwarded = [*sys.argv[1:], "--write-root", str(write_root)]
        os.execv(sys.executable, [sys.executable, "-m", "Z_ORB_ONE.stock_model_gpt.backtest", *forwarded])
    _main_isolated(args, write_root)


from .runtime_lock import locked


@locked
def _main_isolated(args: argparse.Namespace, write_root: Path) -> None:
    paths.ensure_runtime_dirs()
    print(f"[隔離回測] 輸出目錄: {write_root}（不會動到正式 predictions/checkpoints/evaluations/universe）")
    copied = _seed_universe_snapshots()
    print(f"已從正式環境複製 {copied} 份股票清單快照(universe snapshot)供回測使用")
    start_date = date.fromisoformat(args.start_date)
    if args.checkpoint:
        checkpoint_path = Path(args.checkpoint)
    else:
        # Real, unredirected production checkpoints — paths.CHECKPOINT_DIR is
        # one of the write targets redirected under this isolated run's write
        # root, which starts out empty (see _seed_universe_snapshots' own
        # rationale for the same production-vs-isolated distinction).
        universe_date = _previous_session(TradingCalendar(), start_date)
        checkpoint_path = latest_checkpoint_as_of(universe_date, paths.PACKAGE_DIR / "checkpoints")
        print(f"未指定 --checkpoint，自動選用 {checkpoint_path.name}"
             f"（training_as_of 不晚於 {universe_date.isoformat()} 的最新一顆）")
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    thresholds = prediction_signal_thresholds(args, "high")
    low_thresholds = prediction_signal_thresholds(args, "low")
    report = run_backtest(
        checkpoint_path,
        start_date,
        date.fromisoformat(args.end_date),
        settings,
        thresholds,
        low_thresholds,
        daily_train=args.daily_train,
        training_window_days=args.training_window_days,
        force_retrain=args.force_retrain,
    )
    report_path = write_root / "backtest_report.json"
    atomic_text(report_path, json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    _print_report(report)
    print(f"完整報告: {report_path}")


if __name__ == "__main__":
    main()
