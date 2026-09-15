"""Day-by-day walk-forward backtest with a rolling training window (v2).

Design, in contrast to `walk_forward_backtest.py` / `walk_forward_backtest_resume.py`:

- No fixed-length "fold" with one static checkpoint predicting 20 blind days.
  Every trading day: train (reseed or incremental) -> predict tomorrow ->
  validate against tomorrow's actual outcome, exactly mirroring `run_daily.py`'s
  production loop. There is no boundary where the model stops learning.
- Training data for both the periodic full reseed and the daily incremental
  continuation is a rolling window of the most recent `--training-window-days`
  trading days (not an ever-expanding history back to whenever the feature
  files start), so old-regime data ages out on its own. See `training.py`'s
  `_training_window_floor` and `dataset.py`'s `StockSequenceDataset(min_target_date=...)`.
- Every `--reseed-interval-days` trading days, the model is retrained from
  scratch on that rolling window (fresh weights, fresh optimizer, fresh ATR
  calibration). Every day in between, it takes one small `train_daily`-style
  incremental step (inherited weights/optimizer/ATR, small learning rate,
  replay-sampled recent history) instead of standing still.
- Evaluation ignores the target-profit/max-adverse trade simulation entirely.
  It only pools `validate_predictions.py`'s `accuracy` and
  `signal_recall_precision` for high_price across days, rather than one
  blended "signal success rate". The model predicts the next day's high-price bucket across all five classes.

All predict/validate/train steps run in-process (direct function calls, not
`python -m module` subprocesses) because the day-by-day design multiplies the
call count roughly 20x versus the fold-based scripts; repeated interpreter and
torch import startup would otherwise dominate the runtime.

All writes are isolated under `--output-dir` via the STOCK_MODEL_GPT_WRITE_ROOT
environment variable (see paths.py), so this never touches production
checkpoints, predictions, evaluations, universe snapshots, or ATR reports.
Candle/feature/corporate-action history is read-only and shared with
production — nothing here re-fetches or mutates it.

Known limitation (not fixed by this script, same as the v1 scripts): every
simulated historical universe snapshot is built from the CURRENT
`stock_data.py` selected_stocks list, not a period-appropriate one. Results
carry a look-ahead/survivorship bias relative to what a live deployment
starting at that historical date would actually have seen.
"""
from __future__ import annotations

from .signals import OUTPUT_SCHEMA, add_signal_arguments
from .classification_metrics import matrix_metrics, pool_matrices

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any


PACKAGE = "Z_ORB_ONE.stock_model_gpt"
PROGRESS_FILENAME = "backtest_progress_v2.json"
REPORT_FILENAME = "backtest_report_v2.json"


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="逐日滾動視窗 walk-forward 回測（v2）")
    parser.add_argument("--output-dir", required=True, help="隔離的回測輸出根目錄，不可跟正式環境共用")
    parser.add_argument("--training-window-days", type=int, default=300,
                         help="每次訓練（重新校準或每日續訓）只用截止日往前數的 N 個交易日（滾動視窗）")
    parser.add_argument("--reseed-interval-days", type=int, default=20,
                         help="每隔幾個交易日做一次完整重新訓練（權重、optimizer、ATR 刻度全部重置）")
    parser.add_argument("--start-date", default=None, help="回測起始日 YYYY-MM-DD；預設用 --backtest-days 從最新資料往回推")
    parser.add_argument("--backtest-days", type=int, default=252,
                         help="未指定 --start-date 時，回測最近幾個交易日（預設約 1 年）")
    parser.add_argument("--max-as-of", default=None, help="限制回測不使用晚於此日期的資料 YYYY-MM-DD")
    parser.add_argument("--settings", default=None, help="傳給 train/predict/validate 的 settings 路徑")
    parser.add_argument("--restart", action="store_true", help="忽略既有進度，從第一天重跑；會覆寫 prediction/evaluation")
    add_signal_arguments(parser)
    return parser.parse_args()


def _pool(days: list[dict[str, Any]], field: str) -> dict[str, Any]:
    tp = sum(day[field]["tp"] for day in days)
    fp = sum(day[field]["fp"] for day in days)
    fn = sum(day[field]["fn"] for day in days)
    return {
        "tp": tp, "fp": fp, "fn": fn,
        "recall": tp / (tp + fn) if (tp + fn) else None,
        "precision": tp / (tp + fp) if (tp + fp) else None,
    }


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    # Must be set BEFORE importing anything from this package: paths.py reads
    # this env var at import time to redirect all write-side directories.
    os.environ["STOCK_MODEL_GPT_WRITE_ROOT"] = str(output_dir)

    from .config import Settings
    from .paths import CHECKPOINT_DIR, FEATURES_DIR, ensure_runtime_dirs
    from .predict import build_signal_thresholds, run_prediction
    from .storage import read_jsonl
    from .training import train
    from .universe import load_selected_stocks, write_universe_snapshot
    from .validate_predictions import run_validation
    from datetime import date as date_cls

    ensure_runtime_dirs()
    thresholds = build_signal_thresholds(args)
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    progress_path = output_dir / PROGRESS_FILENAME

    def write_snapshot(day: str) -> None:
        write_universe_snapshot(load_selected_stocks(), date_cls.fromisoformat(day))

    dates: set[str] = set()
    for path in FEATURES_DIR.glob("*.jsonl"):
        dates.update(row["date"] for row in read_jsonl(path))
    calendar = sorted(dates)
    if args.max_as_of:
        calendar = [d for d in calendar if d <= args.max_as_of]
    if len(calendar) < 2:
        raise RuntimeError(f"可用交易日（{len(calendar)} 天）不足以回測")

    if args.start_date:
        start_index = next((i for i, d in enumerate(calendar) if d >= args.start_date), None)
        if start_index is None:
            raise RuntimeError(f"起始日 {args.start_date} 晚於所有可用交易日")
    else:
        start_index = max(0, len(calendar) - 1 - args.backtest_days)

    if start_index >= len(calendar) - 1:
        raise RuntimeError(f"起始日 {calendar[start_index]} 之後沒有足夠的交易日可回測")

    steps = [
        {"index": k, "universe_date": calendar[i], "prediction_date": calendar[i + 1]}
        for k, i in enumerate(range(start_index, len(calendar) - 1))
    ]
    print(
        f"回測範圍: {steps[0]['universe_date']} ~ {steps[-1]['prediction_date']}，共 {len(steps)} 個交易日；"
        f"訓練滾動視窗={args.training_window_days} 天，重新校準間隔={args.reseed_interval_days} 天，"
        f"輸出目錄={output_dir}"
    )

    progress = None if args.restart else _read_json(progress_path)
    if progress and (progress.get("output_schema") != OUTPUT_SCHEMA
                     or progress.get("signal_thresholds") != thresholds.signal_values()):
        raise ValueError("回測進度的輸出版本或篩選條件不同，請使用新的 output-dir")
    days_done: list[dict[str, Any]] = list((progress or {}).get("days", []))
    resume_index = len(days_done)
    current_checkpoint = CHECKPOINT_DIR / days_done[-1]["checkpoint"] if days_done else None

    def write_progress(status: str) -> None:
        _atomic_write_json(progress_path, {
            "status": status,
            "output_schema": OUTPUT_SCHEMA,
            "signal_thresholds": thresholds.signal_values(),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "output_dir": str(output_dir),
            "training_window_days": args.training_window_days,
            "reseed_interval_days": args.reseed_interval_days,
            "start_date": steps[0]["universe_date"],
            "max_as_of": args.max_as_of,
            "total_days": len(steps),
            "completed_days": len(days_done),
            "days": days_done,
        })

    if resume_index >= len(steps):
        print("=== 所有交易日皆已完成（進度檔顯示），直接輸出報告 ===")
    else:
        write_progress("running")

    for step in steps[resume_index:]:
        index = step["index"]
        universe_date = step["universe_date"]
        prediction_date = step["prediction_date"]
        cutoff = date_cls.fromisoformat(universe_date)
        reseed = current_checkpoint is None or index % args.reseed_interval_days == 0

        write_snapshot(universe_date)
        if reseed:
            print(f"=== day {index + 1}/{len(steps)} [重新訓練] cutoff={universe_date} ===")
            checkpoint_path = train(settings, as_of=cutoff, training_window_days=args.training_window_days)
        else:
            print(f"=== day {index + 1}/{len(steps)} [每日續訓] cutoff={universe_date} ===")
            checkpoint_path = train(
                settings, resume_path=current_checkpoint, daily=True, as_of=cutoff,
                training_window_days=args.training_window_days,
            )
        current_checkpoint = checkpoint_path

        _, naive_baseline, in_sample_loss = run_prediction(
            checkpoint_path, cutoff, date_cls.fromisoformat(prediction_date), thresholds
        )
        summary = run_validation(prediction_date, settings, thresholds, update_gate=False)

        counts = summary["accuracy_counts"]
        actual_dist = summary["actual_distribution"]
        # Score a same-day, no-lookahead "always guess the majority class from
        # the model's own training window" baseline, so accuracy above can be
        # read against "better than guessing nothing" instead of in isolation.
        baseline_target = naive_baseline.get("majority_high_price") if naive_baseline else None
        baseline_target_hits = actual_dist["high_price_counts"].get(str(baseline_target), 0)


        days_done.append({
            "index": index,
            "universe_date": universe_date,
            "prediction_date": prediction_date,
            "reseeded": reseed,
            "checkpoint": checkpoint_path.name,
            "evaluated": counts["evaluated"],
            "classification": summary["classification"],
            "high_price_hits": counts["high_price"],
            "high_price": summary["signal_recall_precision"]["high_price"],
            "naive_baseline_high_price": baseline_target,
            "baseline_high_price_hits": baseline_target_hits,
            "log_loss_sum": summary["log_loss_sum"],
            "in_sample_loss": in_sample_loss,
        })
        write_progress("running")

    total_evaluated = sum(day["evaluated"] for day in days_done)
    total_target_hits = sum(day["high_price_hits"] for day in days_done)
    total_baseline_target_hits = sum(day["baseline_high_price_hits"] for day in days_done)

    def _mean_log_loss(field: str) -> float | None:
        total = sum(
            day["log_loss_sum"][field] for day in days_done
            if day["log_loss_sum"].get(field) is not None
        )
        weight = sum(
            day["evaluated"] for day in days_done
            if day["log_loss_sum"].get(field) is not None
        )
        return total / weight if weight else None

    log_loss = {
        "description": "每筆預測對「實際發生的那個類別」給的機率算 cross-entropy（-log(p)），"
                        "數字越小代表模型機率分佈跟實際狀況越吻合；能看出 argmax 準確率看不到的東西——"
                        "例如某天猜錯了，但如果模型當時給的機率本來就很不確定（不是很有信心地猜錯），"
                        "log-loss 不會像 accuracy 那樣直接算全錯。baseline 用的是同一次訓練視窗的固定"
                        "類別機率分佈（不看當天輸入），兩者可以直接比大小：模型如果比 baseline 低，"
                        "代表機率分佈上真的學到條件訊號；就算 accuracy 追不上 baseline，這裡贏了也算數。",
        "high_price": _mean_log_loss("high_price"),
        "baseline_high_price": _mean_log_loss("baseline_high_price"),
    }

    def _mean_in_sample_loss(field: str) -> float | None:
        values = [
            day["in_sample_loss"][field] for day in days_done
            if day.get("in_sample_loss") and day["in_sample_loss"].get(field) is not None
        ]
        return sum(values) / len(values) if values else None

    # Overfitting check: each day's checkpoint carries its own last-epoch,
    # in-sample (training-window) loss; compare it against the out-of-sample
    # log-loss above (same day-to-day scope, both unweighted-per-target cross-
    # entropy). In-sample much lower than out-of-sample is the classic
    # overfitting signature — the model fit the training window's noise
    # rather than anything that generalizes to the next unseen day.
    in_sample_loss = {
        "description": "每一天訓練當下、視窗內（in-sample）最後一個 epoch 的未加權 loss，"
                        "跟上面 log_loss 的 model 數字（隔天、out-of-sample）對照："
                        "如果 in-sample 壓得很低、out-of-sample 卻沒有跟著低，就是過擬合的訊號"
                        "——模型把訓練視窗裡的雜訊背起來，但沒有學到能類推到隔天的東西。",
        "high_price": _mean_in_sample_loss("high_price"),
    }
    report = {
        "output_schema": OUTPUT_SCHEMA,
        "signal_thresholds": thresholds.signal_values(),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "trading_calendar_days": len(calendar),
        "backtest_days": len(steps),
        "training_window_days": args.training_window_days,
        "reseed_interval_days": args.reseed_interval_days,
        "start_date": steps[0]["universe_date"],
        "end_date": steps[-1]["prediction_date"],
        "reseed_count": sum(1 for day in days_done if day["reseeded"]),
        "days": days_done,
        "overall": {
            "evaluated": total_evaluated,
            "high_price_accuracy": total_target_hits / total_evaluated if total_evaluated else None,
            "high_price": _pool(days_done, "high_price"),
            "classification": matrix_metrics(pool_matrices([day["classification"]["confusion_matrix"] for day in days_done])),
            "log_loss": log_loss,
            "in_sample_loss": in_sample_loss,
            "naive_baseline": {
                "description": "每日用該次訓練視窗的多數類別（high_price 多數類別）"
                                "當作固定猜測，跟真正模型的準確率做對照，藉此判斷模型是否"
                                "真的學到東西、還是連瞎猜多數類別都比不上。",
                "high_price_accuracy": total_baseline_target_hits / total_evaluated if total_evaluated else None,
            },
        },
        "caveat": (
            "每個歷史日期的股票清單快照，都是用現在的 selected_stocks 回頭套用，"
            "不是當時真正會用的清單，結果相對於「從那個時間點真的開始上線」"
            "會偏樂觀（look-ahead / survivorship bias）。滾動訓練視窗已避免「訓練資料無限往回累積」"
            "這個問題，但不會消除這條 universe 偏誤。"
            "此報告不含停損/停利交易模擬（target_profit/max_adverse），"
            "只比較 high_price 這項預測本身跟實際值（開高低收、hit_up、hit_down 留作歷史輸入特徵）。"
        ),
    }
    report_path = output_dir / REPORT_FILENAME
    _atomic_write_json(report_path, report)
    write_progress("completed")
    print(f"=== 完成，共 {len(steps)} 個交易日，{report['reseed_count']} 次重新訓練 ===")
    print(
        f"high_price 準確率: {total_target_hits}/{total_evaluated} = "
        f"{report['overall']['high_price_accuracy']} "
        f"(naive baseline={report['overall']['naive_baseline']['high_price_accuracy']})"
    )
    print(
        "high_price log-loss: "
        f"model={log_loss['high_price']} baseline={log_loss['baseline_high_price']}"
    )
    print(
        f"high_price in-sample loss={in_sample_loss['high_price']} "
        f"(跟上面 out-of-sample log-loss={log_loss['high_price']} 對照)"
    )
    print(f"報告已儲存: {report_path}")
    print(f"進度已儲存: {progress_path}")


if __name__ == "__main__":
    main()
