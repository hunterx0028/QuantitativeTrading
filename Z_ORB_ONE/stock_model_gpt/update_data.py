from __future__ import annotations

import argparse
import json
import time
from datetime import date, timedelta

from .config import Settings
from .finmind import update_corporate_actions
from .market_data import fetch_candles, load_sdk
from .paths import CONFIG_PATH, PREDICTIONS_DIR, ensure_runtime_dirs
from .storage import candle_path, merge_candles, read_jsonl
from .universe import load_selected_stocks, write_universe_snapshot


def missing_prediction_candles(as_of: date) -> dict[str, list[date]]:
    """Find due prediction dates missing local actual candles, including non-signals.

    An evaluation file can be partial, so its mere existence is not evidence
    that every predicted stock has actual data. Inspect candle dates instead.
    """
    requested: dict[str, set[date]] = {}
    for path in sorted(PREDICTIONS_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for prediction in payload.get("predictions", []):
            target = date.fromisoformat(prediction["prediction_date"])
            if target <= as_of:
                requested.setdefault(prediction["symbol"], set()).add(target)
    missing = {}
    for symbol, targets in requested.items():
        available = {date.fromisoformat(row["date"]) for row in read_jsonl(candle_path(symbol))}
        dates = sorted(targets - available)
        if dates:
            missing[symbol] = dates
    return missing


def main() -> None:
    parser = argparse.ArgumentParser(description="更新每日股票清單及日K快取")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="YYYY-MM-DD")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--config", default=str(CONFIG_PATH))
    args = parser.parse_args()
    as_of = date.fromisoformat(args.as_of)
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    ensure_runtime_dirs()
    stocks = load_selected_stocks()
    snapshot = write_universe_snapshot(stocks, as_of)
    print(f"股票清單快照: {snapshot} ({len(stocks)}支)")

    missing = missing_prediction_candles(as_of)
    active_symbols = {stock.symbol for stock in stocks}
    # The snapshot above contains only today's selected stocks. Supplementary
    # downloads never expand the prediction universe or modify stock_data.py.
    symbols = sorted(active_symbols | missing.keys())

    sdk = load_sdk(CONFIG_PATH if args.config == str(CONFIG_PATH) else args.config)
    rest_stock = sdk.rest_client.stock
    for index, symbol in enumerate(symbols):
        existing = read_jsonl(candle_path(symbol))
        from_date = (
            max(date.fromisoformat(row["date"]) for row in existing) + timedelta(days=1)
            if existing
            else date.fromisoformat(settings.earliest_date)
        )
        if symbol in missing:
            from_date = min(from_date, missing[symbol][0])
            dates_text = ", ".join(day.isoformat() for day in missing[symbol])
            print(f"[補抓驗證行情] {symbol}: {dates_text}")
        to_date = as_of if symbol in active_symbols else missing[symbol][-1]
        incoming = fetch_candles(rest_stock, symbol, from_date, to_date, settings)
        merged = merge_candles(symbol, incoming)
        actions = update_corporate_actions(symbol, to_date, settings)
        available = {row["date"] for row in merged}
        remaining = [day.isoformat() for day in missing.get(symbol, []) if day.isoformat() not in available]
        if remaining:
            print(f"[WARN] {symbol}: 補抓後仍缺驗證日K {', '.join(remaining)}，待資料可取得後重試")
        print(
            f"{symbol}: new_candles={len(incoming)} total_candles={len(merged)} "
            f"corporate_actions={len(actions)}"
        )
        if index + 1 < len(symbols):
            time.sleep(settings.request_interval_seconds)
    # 刻意不呼叫 sdk.logout()。


if __name__ == "__main__":
    main()
