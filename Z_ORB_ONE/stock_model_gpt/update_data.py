from __future__ import annotations

import argparse
import time
from datetime import date, timedelta

from .config import Settings
from .finmind import update_corporate_actions
from .market_data import fetch_candles, load_sdk
from .paths import CONFIG_PATH, ensure_runtime_dirs
from .storage import candle_path, merge_candles, read_jsonl
from .universe import load_selected_stocks, write_universe_snapshot


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

    sdk = load_sdk(CONFIG_PATH if args.config == str(CONFIG_PATH) else args.config)
    rest_stock = sdk.rest_client.stock
    for index, stock in enumerate(stocks):
        existing = read_jsonl(candle_path(stock.symbol))
        from_date = (
            date.fromisoformat(existing[-1]["date"]) + timedelta(days=1)
            if existing
            else date.fromisoformat(settings.earliest_date)
        )
        incoming = fetch_candles(rest_stock, stock.symbol, from_date, as_of, settings)
        merged = merge_candles(stock.symbol, incoming)
        actions = update_corporate_actions(stock.symbol, as_of, settings)
        print(
            f"{stock.symbol}: new_candles={len(incoming)} total_candles={len(merged)} "
            f"corporate_actions={len(actions)}"
        )
        if index + 1 < len(stocks):
            time.sleep(settings.request_interval_seconds)
    # 刻意不呼叫 sdk.logout()。


if __name__ == "__main__":
    main()
