import argparse
import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

from .config import Settings
from .features import DailyState, encode_candles
from .paths import (
    ACTUAL_CANDLES_DIR,
    CANDLES_DIR,
    EVALUATIONS_DIR,
    PREDICTIONS_DIR,
    ensure_runtime_dirs,
)
from .predict import SignalThresholds, build_signal_thresholds, detect_signal
from .storage import read_jsonl, write_jsonl


def main() -> None:
    parser = argparse.ArgumentParser(description="驗證 stock_model_gpt 預測結果")
    parser.add_argument("--prediction-date", required=True, help="要驗證的預測日期 YYYY-MM-DD")
    parser.add_argument("--predictions", default=None, help="預測 JSON 路徑，預設使用 predictions/<date>.json")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--signal-threshold", type=float, default=0.6)
    parser.add_argument("--long-hit-threshold", type=float, default=None)
    parser.add_argument("--long-price-threshold", type=float, default=None)
    parser.add_argument("--short-hit-threshold", type=float, default=None)
    parser.add_argument("--short-price-threshold", type=float, default=None)
    parser.add_argument("--target-profit-pct", type=float, default=3.0)
    parser.add_argument("--max-adverse-pct", type=float, default=2.0)
    args = parser.parse_args()
    thresholds = build_signal_thresholds(args)

    prediction_date = date.fromisoformat(args.prediction_date).isoformat()
    prediction_path = PREDICTIONS_DIR / f"{prediction_date}.json"
    if args.predictions is not None:
        provided_path = Path(args.predictions)
        prediction_path = provided_path if provided_path.is_absolute() else PREDICTIONS_DIR / provided_path
    if not prediction_path.exists():
        raise RuntimeError(f"找不到預測檔: {prediction_path}")

    settings = Settings.load(args.settings) if args.settings else Settings.load()
    payload = json.loads(prediction_path.read_text(encoding="utf-8"))
    predictions = payload.get("predictions", [])
    actual_states, actual_candles = load_actuals(predictions, prediction_date, settings)
    write_actual_snapshot(prediction_date, actual_candles)
    summary = print_summary(
        predictions,
        actual_states,
        actual_candles,
        thresholds,
        args.target_profit_pct,
        args.max_adverse_pct,
    )
    write_evaluation(prediction_date, summary)


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
        rows = [
            row for row in read_jsonl(CANDLES_DIR / f"{symbol}.jsonl")
            if row["date"] <= prediction_date
        ]
        actual_candle = next((row for row in rows if row["date"] == prediction_date), None)
        if actual_candle is None:
            missing_symbols.append(symbol)
            continue
        states = encode_candles(rows, warmup_days=settings.warmup_days)
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


def print_summary(
    predictions: list[dict],
    actual_states: dict[str, DailyState],
    actual_candles: list[dict],
    thresholds: SignalThresholds,
    target_profit_pct: float,
    max_adverse_pct: float,
) -> dict:
    evaluated = 0
    price_hits = 0
    hit_up_hits = 0
    hit_down_hits = 0
    signal_rows: list[tuple[dict, dict, DailyState, dict]] = []
    signal_results: list[dict] = []
    actual_candles_by_symbol = {row["symbol"]: row for row in actual_candles}

    for prediction in predictions:
        symbol = prediction["symbol"]
        actual = actual_states.get(symbol)
        if actual is None:
            continue
        evaluated += 1
        predicted_price = int(max(prediction["price"], key=prediction["price"].get))
        predicted_hit_up = prediction["hit_up"]["T"] >= prediction["hit_up"]["F"]
        predicted_hit_down = prediction["hit_down"]["T"] >= prediction["hit_down"]["F"]

        price_ok = predicted_price == actual.price
        hit_up_ok = predicted_hit_up == actual.hit_up
        hit_down_ok = predicted_hit_down == actual.hit_down
        price_hits += int(price_ok)
        hit_up_hits += int(hit_up_ok)
        hit_down_hits += int(hit_down_ok)

        signal = detect_signal(prediction, thresholds)
        if signal:
            candle = actual_candles_by_symbol[symbol]
            trade = evaluate_signal_trade(signal, candle, target_profit_pct, max_adverse_pct)
            signal_rows.append((prediction, signal, actual, trade))
            signal_results.append({**signal, **trade, "actual_price": actual.price, "actual_hit": trade["actual_hit"]})

    print(f"驗證筆數: {evaluated}/{len(predictions)}")
    if evaluated:
        print(f"price 命中率: {price_hits}/{evaluated} = {price_hits / evaluated:.2%}")
        print(f"hit_up 命中率: {hit_up_hits}/{evaluated} = {hit_up_hits / evaluated:.2%}")
        print(f"hit_down 命中率: {hit_down_hits}/{evaluated} = {hit_down_hits / evaluated:.2%}")

    print(
        "重點訊號 "
        f"long_hit>={thresholds.long_hit:.2f}, "
        f"long_price>={thresholds.long_price:.2f}, "
        f"short_hit>={thresholds.short_hit:.2f}, "
        f"short_price>={thresholds.short_price:.2f}, "
        f"target_profit >= {target_profit_pct:.2f}%, max_adverse <= {max_adverse_pct:.2f}%: "
        f"{len(signal_rows)}"
    )
    for prediction, signal, actual, trade in signal_rows:
        print(
            f"[SIGNAL {signal['side']}] {signal['symbol']} "
            f"reason={signal['reason']} "
            f"{signal['hit_key']}={signal['hit_probability']:.4f} "
            f"{signal['price_key']}={signal['price_probability']:.4f} | "
            f"actual_hit={trade['actual_hit']} actual_price={actual.price} | "
            f"O={trade['open']} H={trade['high']} L={trade['low']} C={trade['close']} | "
            f"best={trade['best_profit_pct']:.2f}% close={trade['close_profit_pct']:.2f}% "
            f"adverse={trade['adverse_pct']:.2f}% success={trade['success']}"
        )
    return {
        "prediction_date": next((item["prediction_date"] for item in predictions), None),
        "evaluated": evaluated,
        "total_predictions": len(predictions),
        "signal_thresholds": thresholds.__dict__,
        "target_profit_pct": target_profit_pct,
        "max_adverse_pct": max_adverse_pct,
        "accuracy": {
            "price": price_hits / evaluated if evaluated else None,
            "hit_up": hit_up_hits / evaluated if evaluated else None,
            "hit_down": hit_down_hits / evaluated if evaluated else None,
        },
        "signals": signal_results,
    }


def evaluate_signal_trade(
    signal: dict,
    candle: dict,
    target_profit_pct: float,
    max_adverse_pct: float,
) -> dict:
    open_price = float(candle["open"])
    high = float(candle["high"])
    low = float(candle["low"])
    close = float(candle["close"])
    if signal["side"] == "LONG":
        best_profit_pct = (high - open_price) / open_price * 100.0
        close_profit_pct = (close - open_price) / open_price * 100.0
        adverse_pct = (open_price - low) / open_price * 100.0
        actual_hit = bool(candle["actual_state"]["hit_up"])
    else:
        best_profit_pct = (open_price - low) / open_price * 100.0
        close_profit_pct = (open_price - close) / open_price * 100.0
        adverse_pct = (high - open_price) / open_price * 100.0
        actual_hit = bool(candle["actual_state"]["hit_down"])
    return {
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "best_profit_pct": best_profit_pct,
        "close_profit_pct": close_profit_pct,
        "adverse_pct": adverse_pct,
        "actual_hit": actual_hit,
        "success": best_profit_pct >= target_profit_pct and adverse_pct <= max_adverse_pct,
    }


def write_evaluation(prediction_date: str, summary: dict) -> None:
    ensure_runtime_dirs()
    output = EVALUATIONS_DIR / f"{prediction_date}.json"
    output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"post-training evaluation 已儲存: {output}")


if __name__ == "__main__":
    main()
