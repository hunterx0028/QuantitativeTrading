import argparse
import json
import math
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
from .signals import (CLASSES, OUTPUT_SCHEMA, SignalThresholds, add_signal_arguments,
                      build_signal_thresholds, detect_signal, probabilities, predicted_class, sorted_signals)
from .classification_metrics import rates, matrix_metrics
from .storage import write_jsonl


def run_validation(
    prediction_date: str,
    settings: Settings,
    thresholds: SignalThresholds,
    prediction_path: Path | None = None,
    update_gate: bool = True,
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
    naive_baseline = payload.get("naive_baseline")
    actual_states, actual_candles = load_actuals(predictions, prediction_date, settings)
    write_actual_snapshot(prediction_date, actual_candles)
    summary = print_summary(
        predictions,
        actual_states,
        actual_candles,
        thresholds,
        naive_baseline=naive_baseline,
    )
    write_evaluation(prediction_date, summary)
    if update_gate:
        gate_status = compute_gate_status(settings)
        save_gate_status(gate_status)
        print(f"checkpoint gate: {gate_status['verdict']} — {gate_status['reason']}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="驗證 stock_model_gpt 預測結果")
    parser.add_argument("--prediction-date", required=True, help="要驗證的預測日期 YYYY-MM-DD")
    parser.add_argument("--predictions", default=None, help="預測 JSON 路徑，預設使用 predictions/<date>.json")
    parser.add_argument("--settings", default=None)
    add_signal_arguments(parser, saved_defaults=True)
    args = parser.parse_args()

    prediction_date = date.fromisoformat(args.prediction_date).isoformat()
    prediction_path = PREDICTIONS_DIR / f"{prediction_date}.json"
    if args.predictions is not None:
        provided_path = Path(args.predictions)
        prediction_path = provided_path if provided_path.is_absolute() else PREDICTIONS_DIR / provided_path

    payload = json.loads(prediction_path.read_text(encoding="utf-8"))
    thresholds = build_signal_thresholds(args, payload.get("signal_thresholds"))
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    run_validation(
        prediction_date,
        settings,
        thresholds,
        prediction_path=prediction_path,
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


def print_summary(
    predictions, actual_states, actual_candles, thresholds,
    naive_baseline=None,
):
    matrix = [[0] * 5 for _ in range(5)]
    distribution = {str(c): 0 for c in CLASSES}
    tp = fp = fn = 0
    loss = baseline_loss = 0.0
    signals = []
    for prediction in predictions:
        values = probabilities(prediction)
        actual = actual_states.get(prediction["symbol"])
        if actual is None:
            continue
        label = actual.high_price
        guess = predicted_class(values)
        matrix[CLASSES.index(label)][CLASSES.index(guess)] += 1
        distribution[str(label)] += 1
        loss -= math.log(max(values[str(label)], _LOG_LOSS_FLOOR))
        if naive_baseline:
            baseline_loss -= math.log(_smoothed_probability(naive_baseline["high_price_counts"], str(label), 5))
        signal = detect_signal(prediction, thresholds)
        selected_actual = label in thresholds.classes
        tp += int(signal is not None and selected_actual)
        fp += int(signal is not None and not selected_actual)
        fn += int(signal is None and selected_actual)
        if signal:
            signals.append({**signal, "actual_high_price": label, "success": selected_actual})
    evaluated = sum(distribution.values())
    metrics = matrix_metrics(matrix)
    selected = rates(tp, fp, fn)
    print(f"驗證筆數: {evaluated}/{len(predictions)}")
    print(f"high_price 五分類準確率: {_format_rate(metrics['accuracy'])}")
    for c, metric in metrics["per_class"].items():
        print(f"high_price={c}: precision={_format_rate(metric['precision'])} "
              f"recall={_format_rate(metric['recall'])} support={metric['support']}")
    print(f"所選刻度 {thresholds.classes}，合計門檻 {thresholds.threshold_pct:g}%: "
          f"precision={_format_rate(selected['precision'])} recall={_format_rate(selected['recall'])}")
    return {
        "output_schema": OUTPUT_SCHEMA,
        "prediction_date": next((p["prediction_date"] for p in predictions), None),
        "evaluated": evaluated, "total_predictions": len(predictions),
        "signal_thresholds": thresholds.signal_values(),
        "accuracy": {"high_price": metrics["accuracy"]},
        "accuracy_counts": {"high_price": sum(matrix[i][i] for i in range(5)), "evaluated": evaluated},
        "classification": metrics,
        "actual_distribution": {"evaluated": evaluated, "high_price_counts": distribution},
        "log_loss_sum": {"high_price": loss, "baseline_high_price": baseline_loss if naive_baseline else None},
        "signal_recall_precision": {"high_price": selected},
        "signals": sorted_signals(signals),
    }


def _recall_precision(tp: int, fp: int, fn: int) -> tuple[float | None, float | None]:
    recall = tp / (tp + fn) if (tp + fn) else None
    precision = tp / (tp + fp) if (tp + fp) else None
    return recall, precision


def _format_rate(value: float | None) -> str:
    return f"{value:.2%}" if value is not None else "N/A"


def write_evaluation(prediction_date: str, summary: dict) -> None:
    ensure_runtime_dirs()
    output = EVALUATIONS_DIR / f"{prediction_date}.json"
    output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"post-training evaluation 已儲存: {output}")


if __name__ == "__main__":
    main()
