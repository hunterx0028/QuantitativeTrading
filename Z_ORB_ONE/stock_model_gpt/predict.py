from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import torch

from .checkpoint_gate import load_gate_status
from .config import Settings
from .dataset import INPUT_ALIGNMENT, encode_sequence
from .device import describe_device, select_device
from .night_futures import load_night_futures
from .paths import CHECKPOINT_DIR, FEATURES_DIR, PREDICTIONS_DIR, SIGNAL_REPORTS_DIR, ensure_runtime_dirs
from .storage import read_jsonl
from .training import build_model, ensure_checkpoint_compatible
from .universe import load_universe_snapshot
from .paths import UNIVERSE_DIR


from .signals import (CLASSES, OUTPUT_SCHEMA, SignalThresholds, add_signal_arguments,
                      build_signal_thresholds, detect_signal, build_signal_report_lines,
                      predicted_class, sorted_signals)


def latest_checkpoint() -> Path:
    paths = sorted(list(CHECKPOINT_DIR.glob("stock_model_gpt_*.pt")))
    if not paths:
        raise RuntimeError("找不到 checkpoint")
    return paths[-1]


def select_checkpoint_for_prediction(thresholds: SignalThresholds = SignalThresholds()) -> Path:
    """Auto-select path only; an explicit --checkpoint always bypasses this gate."""
    candidate = latest_checkpoint()
    status = load_gate_status()
    if (status is not None and status.get("signal_thresholds") == thresholds.signal_values()
            and status["verdict"] == "DEGRADED"):
        raise RuntimeError(
            f"checkpoint gate 判定近期訊號表現明顯退化（{status['reason']}），"
            f"拒絕自動使用最新 checkpoint {candidate.name}；"
            "請先確認訓練或資料是否異常，或明確指定 --checkpoint 選用你確認過的模型"
        )
    return candidate


def select_prediction_inputs(
    active_symbols: set[str], universe_date: date, context_days: int,
) -> tuple[dict[str, list[dict]], list[dict]]:
    cutoff = universe_date.isoformat()
    eligible: dict[str, list[dict]] = {}
    skipped: list[dict] = []
    for symbol in sorted(active_symbols):
        path = FEATURES_DIR / f"{symbol}.jsonl"
        rows = sorted((row for row in read_jsonl(path) if row["date"] <= cutoff),
                      key=lambda row: row["date"])
        last_date = rows[-1]["date"] if rows else None
        reason = None
        if not path.exists():
            reason = "missing_features"
        elif not rows:
            reason = "no_features_as_of"
        elif last_date != cutoff:
            reason = "stale_features"
        elif len(rows) < context_days:
            reason = "insufficient_history"
        if reason:
            skipped.append({"symbol": symbol, "reason": reason,
                            "expected_date": cutoff, "input_last_date": last_date,
                            "history_days": len(rows)})
        else:
            eligible[symbol] = rows[-context_days:]
    return eligible, skipped


def run_prediction(
    checkpoint_path: Path,
    universe_date: date,
    prediction_date: date,
    thresholds: SignalThresholds,
) -> Path:
    """Core prediction step, reusable both by the CLI (`main`) and by in-process
    callers such as a walk-forward backtest that would otherwise pay a fresh
    Python/torch interpreter startup cost for every simulated trading day."""
    if prediction_date <= universe_date:
        raise ValueError("prediction-date 必須晚於 universe-date")
    ensure_runtime_dirs()
    device = select_device()
    print(f"device={describe_device(device)}")
    checkpoint = torch.load(checkpoint_path, map_location=device, weights_only=False)
    ensure_checkpoint_compatible(checkpoint)
    training_as_of = checkpoint.get("training_as_of")
    if training_as_of and training_as_of > universe_date.isoformat():
        raise RuntimeError(
            f"checkpoint 訓練截止日 {training_as_of} 晚於預測基準日 "
            f"{universe_date.isoformat()}，已拒絕可能洩漏未來資料的預測"
        )
    settings = Settings(**checkpoint["settings"])
    model = build_model(settings).to(device)
    model.load_state_dict(checkpoint["model"])
    model.eval()
    # Latest night data fills column ten of the final stock row.
    prediction_date_str = prediction_date.isoformat()
    night_by_date = load_night_futures()
    prediction_night_bucket = night_by_date.get(prediction_date_str)
    if prediction_night_bucket is None:
        raise RuntimeError(
            f"找不到 {prediction_date_str} 開盤前的夜盤資料；"
            f"請先用 set_night_futures.py（或 import_night_futures.py）匯入 {prediction_date_str} 這筆，"
            "再重新預測——這是被預測日當天的必要輸入，不能省略"
        )
    universe_path = UNIVERSE_DIR / f"{universe_date.isoformat()}.json"
    if not universe_path.exists():
        raise RuntimeError(f"找不到當日股票清單快照: {universe_path}")
    active_symbols = {stock.symbol for stock in load_universe_snapshot(universe_path)}
    eligible, skipped = select_prediction_inputs(active_symbols, universe_date, settings.context_days)
    coverage_lines = [f"預測資料覆蓋: {len(eligible)}/{len(active_symbols)} 支"]
    coverage_lines.extend(
        f"[SKIP] {item['symbol']} reason={item['reason']} "
        f"expected_date={item['expected_date']} input_last_date={item['input_last_date']} "
        f"history_days={item['history_days']}" for item in skipped
    )
    for line in coverage_lines:
        print(line)
    if not eligible:
        raise RuntimeError("沒有日期與歷史長度合格的股票，未產生預測")

    predictions: list[dict] = []
    signals: list[dict] = []
    with torch.no_grad():
        for symbol, rows in eligible.items():
            states = torch.tensor(
                [encode_sequence(rows, prediction_date_str, night_by_date, settings.atr_boundaries_pct)],
                dtype=torch.long,
                device=device,
            )
            outputs = model(states)
            probabilities = {key: torch.softmax(value, dim=-1)[0].cpu().tolist() for key, value in outputs.items()}
            prediction = {
                "symbol": symbol,
                "prediction_date": prediction_date.isoformat(),
                "input_last_date": rows[-1]["date"],
                "checkpoint": checkpoint_path.name,
                "high_price": _probabilities(probabilities["high_price"]),
                "predicted_class": CLASSES[max(range(5), key=lambda i: probabilities["high_price"][i])],
            }
            predictions.append(prediction)
            signal = detect_signal(prediction, thresholds)
            if signal:
                signals.append(signal)
    signals = sorted_signals(signals)
    naive_baseline = checkpoint.get("naive_baseline")
    # The checkpoint's own last-epoch, in-sample (training-window) unweighted
    # loss per target — surfaced alongside the prediction so a caller such as a
    # walk-forward backtest can compare it against tomorrow's actual
    # out-of-sample loss to check for overfitting (low in-sample, high
    # out-of-sample is the classic symptom).
    in_sample_loss = checkpoint.get("loss_components")
    output = PREDICTIONS_DIR / f"{prediction_date.isoformat()}.json"
    payload = {"created_at": datetime.now().isoformat(timespec="seconds"), "predictions": predictions,
               "universe_date": universe_date.isoformat(), "active_count": len(active_symbols),
               "predicted_count": len(predictions), "skipped": skipped,
               "signal_thresholds": thresholds.signal_values(),
               "atr_boundaries_pct": settings.atr_boundaries_pct,
               "night_futures_date": prediction_date_str,
               "night_futures_bucket": prediction_night_bucket,
               "input_alignment": INPUT_ALIGNMENT,
               "output_schema": OUTPUT_SCHEMA,
               "naive_baseline": naive_baseline,
               "in_sample_loss": in_sample_loss,
               "signals": signals}
    output.write_text(dumps_json_no_scientific(payload) + "\n", encoding="utf-8")
    print(f"預測已儲存: {output} ({len(predictions)}支)")
    report_lines = build_signal_report_lines(prediction_date, thresholds, signals)
    report_lines[1:1] = coverage_lines
    for line in report_lines:
        print(line)
    report_path = SIGNAL_REPORTS_DIR / f"{prediction_date.isoformat()}.txt"
    report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"訊號報告已儲存: {report_path}")
    return output, naive_baseline, in_sample_loss


def main() -> None:
    parser = argparse.ArgumentParser(description="預測下一交易日狀態機率")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--prediction-date", default=date.today().isoformat())
    parser.add_argument("--universe-date", default=date.today().isoformat())
    add_signal_arguments(parser)
    args = parser.parse_args()
    thresholds = build_signal_thresholds(args)
    universe_date = date.fromisoformat(args.universe_date)
    prediction_date = date.fromisoformat(args.prediction_date)
    ensure_runtime_dirs()
    checkpoint_path = Path(args.checkpoint) if args.checkpoint else select_checkpoint_for_prediction(thresholds)
    run_prediction(checkpoint_path, universe_date, prediction_date, thresholds)


def dumps_json_no_scientific(value, indent: int = 2) -> str:
    return _format_json_value(value, indent, 0)


def _format_json_value(value, indent: int, level: int) -> str:
    space = " " * (indent * level)
    child_space = " " * (indent * (level + 1))
    if isinstance(value, dict):
        if not value:
            return "{}"
        items = [
            f"{child_space}{json.dumps(str(key), ensure_ascii=False)}: "
            f"{_format_json_value(item, indent, level + 1)}"
            for key, item in value.items()
        ]
        return "{\n" + ",\n".join(items) + "\n" + space + "}"
    if isinstance(value, list):
        if not value:
            return "[]"
        items = [f"{child_space}{_format_json_value(item, indent, level + 1)}" for item in value]
        return "[\n" + ",\n".join(items) + "\n" + space + "]"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"JSON 不支援非有限浮點數: {value}")
        return format(Decimal(str(value)), "f")
    return json.dumps(value, ensure_ascii=False)


def _probabilities(values: list[float]) -> dict[str, float]:
    return {str(c): values[i] for i, c in enumerate(CLASSES)}


if __name__ == "__main__":
    main()
