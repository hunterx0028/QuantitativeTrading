import argparse
import json
import math
import hashlib
from dataclasses import asdict
from datetime import date
from pathlib import Path

from .config import Settings
from .features import DailyState
from .state_pipeline import load_candle_states
from .paths import (
    ACTUAL_CANDLES_DIR,
    CANDLES_DIR,
    EVALUATIONS_DIR,
    PREDICTIONS_DIR,
    ensure_runtime_dirs,
)
from .checkpoint_gate import compute_gate_status, save_gate_status
from .meta_labeling import update_from_evaluation
from .signals import (CLASSES, OUTPUT_SCHEMA, SignalThresholds,
                      build_signal_thresholds, detect_signal, probabilities, predicted_class, sorted_signals)
from .classification_metrics import rates, matrix_metrics
from .storage import write_jsonl
from .provenance import file_fingerprint, atomic_text
from .trading_calendar import suspension_reason
from .evaluation_inputs import snapshot as actual_input_snapshot


def run_validation(
    prediction_date: str,
    settings: Settings,
    thresholds: SignalThresholds | None = None,
    prediction_path: Path | None = None,
    update_gate: bool = True,
    low_thresholds: SignalThresholds | None = None,
) -> dict:
    """Core validation step, reusable both by the CLI (`main`) and by in-process
    callers such as a walk-forward backtest that would otherwise pay a fresh
    Python/torch interpreter startup cost for every simulated trading day."""
    prediction_date = date.fromisoformat(prediction_date).isoformat()
    if prediction_path is None:
        prediction_path = PREDICTIONS_DIR / f"{prediction_date}.json"
    if not prediction_path.exists():
        raise RuntimeError(f"找不到預測檔: {prediction_path}")

    payload = json.loads(prediction_path.read_text(encoding="utf-8"))
    if payload.get("output_schema") != OUTPUT_SCHEMA:
        raise ValueError("預測檔不是 high_price 五分類版本，請重新預測")
    predictions = payload.get("predictions", [])
    if any(p.get("prediction_date") != prediction_date for p in predictions):
        raise ValueError("預測檔日期與 prediction-date 不一致")
    saved_high, saved_low = saved_thresholds(payload)
    thresholds = thresholds if thresholds is not None else saved_high
    low_thresholds = low_thresholds if low_thresholds is not None else saved_low
    overridden = thresholds != saved_high or low_thresholds != saved_low
    naive_baseline = payload.get("naive_baseline")
    actual_version = actual_input_snapshot([p["symbol"] for p in predictions], prediction_date, settings)
    actual_states, actual_candles = load_actuals(predictions, prediction_date, settings)
    if actual_version != actual_input_snapshot([p["symbol"] for p in predictions], prediction_date, settings):
        raise RuntimeError("驗證期間實際資料已變更，請重新驗證")
    write_actual_snapshot(prediction_date, actual_candles)
    summary = print_summary(
        predictions,
        actual_states,
        actual_candles,
        thresholds,
        naive_baseline=naive_baseline,
        low_thresholds=low_thresholds,
    )
    summary["prediction_date"] = prediction_date
    summary["actual_data_version"] = actual_version
    summary["conditions_overridden"] = overridden
    summary["prediction_id"] = payload.get("prediction_id")
    summary["prediction_content_sha256"] = file_fingerprint(prediction_path)
    summary["prediction_source"] = str(prediction_path.resolve())
    official = PREDICTIONS_DIR / f"{prediction_date}.json"
    summary["official_prediction"] = (official.exists() and
        file_fingerprint(official) == summary["prediction_content_sha256"])
    summary["mode"] = payload.get("mode", "official")
    for target in summary["targets"].values():
        target.update({key: summary[key] for key in (
            "prediction_date", "prediction_id", "prediction_content_sha256", "official_prediction", "mode")})
    write_evaluation(prediction_date, summary)
    if not overridden and summary["official_prediction"]:
        update_from_evaluation(summary, settings)
    if update_gate and not overridden and summary["official_prediction"]:
        gate_status = compute_gate_status(settings)
        save_gate_status(gate_status)
        print(f"checkpoint gate: {gate_status['verdict']} — {gate_status['reason']}")
    return summary


def saved_thresholds(payload):
    high = payload.get("high_signal_thresholds", payload.get("signal_thresholds"))
    low = payload.get("low_signal_thresholds", SignalThresholds((-2, -1), 60).signal_values())
    empty = argparse.Namespace(signal_classes=None, signal_threshold_pct=None)
    return build_signal_thresholds(empty, high), build_signal_thresholds(empty, low)


def add_validation_signal_arguments(parser):
    for target in ("high", "low"):
        parser.add_argument(f"--{target}-signal-classes", default=None,
                            help=f"覆寫 {target} 類別；省略時沿用預測檔，負數請用 =")
        parser.add_argument(f"--{target}-signal-threshold-pct", type=float, default=None,
                            help=f"覆寫 {target} 合計機率門檻；省略時沿用預測檔")


def resolve_thresholds(args, payload):
    saved = saved_thresholds(payload)
    return tuple(build_signal_thresholds(argparse.Namespace(
        signal_classes=getattr(args, f"{target}_signal_classes"),
        signal_threshold_pct=getattr(args, f"{target}_signal_threshold_pct"),
    ), defaults.signal_values()) for target, defaults in zip(("high", "low"), saved))


from .runtime_lock import locked


@locked
def main() -> None:
    parser = argparse.ArgumentParser(description="驗證 stock_model_gpt 預測結果")
    parser.add_argument("--prediction-date", required=True, help="要驗證的預測日期 YYYY-MM-DD")
    parser.add_argument("--predictions", default=None, help="預測 JSON 路徑，預設使用 predictions/<date>.json")
    parser.add_argument("--settings", default=None)
    add_validation_signal_arguments(parser)
    args = parser.parse_args()

    prediction_date = date.fromisoformat(args.prediction_date).isoformat()
    prediction_path = PREDICTIONS_DIR / f"{prediction_date}.json"
    if args.predictions is not None:
        provided_path = Path(args.predictions)
        prediction_path = provided_path if provided_path.is_absolute() else PREDICTIONS_DIR / provided_path

    payload = json.loads(prediction_path.read_text(encoding="utf-8"))
    thresholds, low_thresholds = resolve_thresholds(args, payload)
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    run_validation(
        prediction_date,
        settings,
        thresholds,
        prediction_path=prediction_path,
        low_thresholds=low_thresholds,
    )


def load_actuals(
    predictions: list[dict],
    prediction_date: str,
    settings: Settings,
) -> tuple[dict[str, DailyState], list[dict]]:
    actual_states: dict[str, DailyState] = {}
    actual_candles: list[dict] = []
    missing_symbols: list[str] = []

    for prediction in predictions:
        symbol = prediction["symbol"]
        rows, states = load_candle_states(
            CANDLES_DIR / f"{symbol}.jsonl", prediction_date, settings.warmup_days,
        )
        actual_candle = next((row for row in rows if row["date"] == prediction_date), None)
        if actual_candle is None:
            missing_symbols.append(symbol)
            continue
        actual_state = next((state for state in states if state.date == prediction_date), None)
        if actual_state is None:
            missing_symbols.append(symbol)
            continue
        actual_states[symbol] = actual_state
        actual_candles.append({"symbol": symbol, **actual_candle, "actual_state": asdict(actual_state)})

    if missing_symbols:
        preview = ", ".join(missing_symbols[:10])
        suffix = "..." if len(missing_symbols) > 10 else ""
        print(
            f"[WARN] {len(missing_symbols)} 支缺少 {prediction_date} 實際日K或不足以產生特徵: "
            f"{preview}{suffix}"
        )
    return actual_states, actual_candles


def write_actual_snapshot(prediction_date: str, actual_candles: list[dict]) -> None:
    ensure_runtime_dirs()
    if not actual_candles:
        print(f"沒有可儲存的 {prediction_date} 實際日K snapshot")
        return
    output = ACTUAL_CANDLES_DIR / f"{prediction_date}.jsonl"
    write_jsonl(output, sorted(actual_candles, key=lambda row: row["symbol"]))
    print(f"實際日K snapshot 已儲存: {output} ({len(actual_candles)}支)")


def _smoothed_probability(counts: dict[str, int], key: str, num_classes: int) -> float:
    """Laplace-smoothed empirical probability, so a baseline built from finite
    training-window counts never assigns literal zero probability to a class
    that simply never occurred there (which would make its log-loss infinite
    on the first miss instead of just large)."""
    total = sum(counts.values())
    return (counts.get(key, 0) + 1) / (total + num_classes)


_LOG_LOSS_FLOOR = 1e-9  # clamp so a near-zero predicted probability gives a large, finite loss, not -inf/nan


def _target_summary(
    predictions, actual_states, actual_candles, thresholds,
    naive_baseline=None,
    target="high_price",
):
    matrix = [[0] * 5 for _ in range(5)]
    distribution = {str(c): 0 for c in CLASSES}
    tp = fp = fn = 0
    loss = baseline_loss = 0.0
    signals = []
    pending = []
    pending_signals = []
    excluded = []
    counts = (naive_baseline or {}).get(f"{target}_counts")
    for prediction in predictions:
        values = probabilities(prediction, target)
        signal = detect_signal(prediction, thresholds, target)
        actual = actual_states.get(prediction["symbol"])
        if actual is None or getattr(actual, target, None) is None:
            suspension = suspension_reason(prediction["symbol"], prediction["prediction_date"])
            if suspension:
                excluded.append({"symbol": prediction["symbol"], "suspension": suspension})
                continue
            pending.append(prediction["symbol"])
            if signal:
                pending_signals.append(signal)
            continue
        label = getattr(actual, target)
        guess = predicted_class(values)
        matrix[CLASSES.index(label)][CLASSES.index(guess)] += 1
        distribution[str(label)] += 1
        loss -= math.log(max(values[str(label)], _LOG_LOSS_FLOOR))
        if counts is not None:
            baseline_loss -= math.log(_smoothed_probability(counts, str(label), 5))
        selected_actual = label in thresholds.classes
        tp += int(signal is not None and selected_actual)
        fp += int(signal is not None and not selected_actual)
        fn += int(signal is None and selected_actual)
        if signal:
            signals.append({**signal, f"actual_{target}": label, "success": selected_actual})
    evaluated = sum(distribution.values())
    metrics = matrix_metrics(matrix)
    selected = rates(tp, fp, fn)
    print(f"[{target}] 驗證筆數: {evaluated}/{len(predictions)}，待驗證: {len(pending)}")
    if pending:
        print("待驗證股票: " + ", ".join(pending))
    if excluded:
        print("已確認停牌，排除計分: " + ", ".join(item["symbol"] for item in excluded))
    print(f"{target} 五分類準確率: {_format_rate(metrics['accuracy'])}")
    for c, metric in metrics["per_class"].items():
        print(f"{target}={c}: precision={_format_rate(metric['precision'])} "
              f"recall={_format_rate(metric['recall'])} support={metric['support']}")
    print(f"所選刻度 {thresholds.classes}，合計門檻 {thresholds.threshold_pct:g}%: "
          f"precision={_format_rate(selected['precision'])} recall={_format_rate(selected['recall'])}")
    print(f"入選: {len(signals) + len(pending_signals)}，可驗證: {len(signals)}，成功: {tp}")
    print(f"log loss: {loss / evaluated if evaluated else 'N/A'}")
    return {
        "output_schema": OUTPUT_SCHEMA,
        "prediction_date": next((p["prediction_date"] for p in predictions), None),
        "evaluated": evaluated, "total_predictions": len(predictions),
        "signal_thresholds": thresholds.signal_values(),
        "accuracy": {target: metrics["accuracy"]},
        "accuracy_counts": {target: sum(matrix[i][i] for i in range(5)), "evaluated": evaluated},
        "classification": metrics,
        "actual_distribution": {"evaluated": evaluated, f"{target}_counts": distribution},
        "log_loss_sum": {target: loss, f"baseline_{target}": baseline_loss if counts is not None else None},
        "log_loss": loss / evaluated if evaluated else None,
        "signal_recall_precision": {target: selected},
        "pending_symbols": pending,
        "excluded_suspensions": excluded,
        "pending_signals": sorted_signals(pending_signals),
        "selected_count": len(signals) + len(pending_signals),
        "evaluated_signal_count": len(signals),
        "success_count": tp,
        "signals": sorted_signals(signals),
    }


def print_summary(predictions, actual_states, actual_candles, thresholds,
                  naive_baseline=None, low_thresholds=None):
    low_thresholds = low_thresholds if low_thresholds is not None else SignalThresholds((-2, -1), 60)
    high = _target_summary(predictions, actual_states, actual_candles, thresholds, naive_baseline)
    low_predictions = [p for p in predictions if "low_price" in p]
    low = _target_summary(low_predictions, actual_states, actual_candles, low_thresholds,
                          naive_baseline, "low_price")
    low["missing_prediction_symbols"] = [p["symbol"] for p in predictions if "low_price" not in p]
    low["status"] = "available" if low_predictions else "no_predictions"
    if not low_predictions:
        print("low_price：無預測資料（舊檔僅驗證 high）")
    # Preserve the existing high fields consumed by gates and pooled backtests.
    return {**high, "targets": {"high_price": high, "low_price": low},
            "high_signal_thresholds": thresholds.signal_values(),
            "low_signal_thresholds": low_thresholds.signal_values()}


def _recall_precision(tp: int, fp: int, fn: int) -> tuple[float | None, float | None]:
    recall = tp / (tp + fn) if (tp + fn) else None
    precision = tp / (tp + fp) if (tp + fp) else None
    return recall, precision


def _format_rate(value: float | None) -> str:
    return f"{value:.2%}" if value is not None else "N/A"


def write_evaluation(prediction_date: str, summary: dict) -> None:
    ensure_runtime_dirs()
    output = EVALUATIONS_DIR / f"{prediction_date}.json"
    content_hash = summary.get("prediction_content_sha256")
    if content_hash:
        conditions = {name: summary[f"{name}_signal_thresholds"] for name in ("high", "low")}
        digest = hashlib.sha256(json.dumps(conditions, sort_keys=True).encode()).hexdigest()[:16]
        archive = EVALUATIONS_DIR / "versions" / prediction_date / content_hash / f"{digest}.json"
        atomic_text(archive, json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        if not summary.get("official_prediction", False):
            print(f"非正式預測驗證已另存，不更新每日正式結果與 gate: {archive}")
            return
    if summary.get("conditions_overridden"):
        conditions = {name: summary[f"{name}_signal_thresholds"] for name in ("high", "low")}
        digest = hashlib.sha256(json.dumps(conditions, sort_keys=True).encode()).hexdigest()[:16]
        suffix = f"_{content_hash[:16]}" if content_hash else ""
        output = EVALUATIONS_DIR / "overrides" / f"{prediction_date}_{digest}{suffix}.json"
        output.parent.mkdir(parents=True, exist_ok=True)
    atomic_text(output, json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print(f"post-training evaluation 已儲存: {output}")


if __name__ == "__main__":
    main()
