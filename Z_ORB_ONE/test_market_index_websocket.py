# -*- coding: utf-8 -*-
import argparse
import json
import time
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from esun_marketdata import EsunMarketdata

from stock_data import market_previous_close_indices


MARKET_INDEX_STATE: Dict[str, Dict[str, Any]] = {}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def dump_payload(title: str, payload: Any):
    print(f"\n[{now_text()}] {title}")
    try:
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    except TypeError:
        print(repr(payload))


def build_symbol_to_market_key() -> dict[str, str]:
    return {
        str(info.get("symbol", "")): market_key
        for market_key, info in market_previous_close_indices.items()
        if info.get("symbol")
    }


def handle_index_message(message: Any, symbol_to_market_key: dict[str, str]):
    payload = json.loads(message) if isinstance(message, str) else message
    data = payload.get("data", payload) if isinstance(payload, dict) else {}
    index_symbol = str(data.get("symbol", ""))
    market_key = symbol_to_market_key.get(index_symbol)
    if not market_key:
        dump_payload("UNMATCHED_INDEX_MESSAGE", payload)
        return

    index_value = data.get("index")
    if index_value is None:
        dump_payload("INDEX_MESSAGE_WITHOUT_INDEX", payload)
        return

    index_float = float(index_value)
    previous_close = float(market_previous_close_indices[market_key]["previous_close"])
    gain_per = ((index_float - previous_close) / previous_close) * 100.0

    MARKET_INDEX_STATE[market_key] = {
        "symbol": index_symbol,
        "last_index": index_float,
        "previous_close": previous_close,
        "gain_per": gain_per,
        "time": data.get("time"),
        "last_updated": now_text(),
        "raw": data,
    }

    dump_payload("INDEX_MESSAGE", payload)
    print(
        f"[MARKET] {market_key} {index_symbol} index={index_float} "
        f"previous_close={previous_close} gain={gain_per:.4f}%"
    )


def start_market_index_stream(realtime_sdk: EsunMarketdata):
    symbol_to_market_key = build_symbol_to_market_key()
    if not symbol_to_market_key:
        raise ValueError("stock_data.market_previous_close_indices has no index symbols.")

    def on_message(message):
        try:
            handle_index_message(message, symbol_to_market_key)
        except Exception as exc:
            dump_payload("INDEX_MESSAGE_ERROR", exc)

    stock_ws = realtime_sdk.websocket_client.stock
    stock_ws.on("message", on_message)
    stock_ws.connect()

    for index_symbol, market_key in symbol_to_market_key.items():
        stock_ws.subscribe({
            "channel": "indices",
            "symbol": index_symbol,
        })
        print(f"[MARKET] subscribed {market_key} {index_symbol}")

    return stock_ws


def main():
    default_config_path = Path(__file__).with_name("config.ini")
    parser = argparse.ArgumentParser(description="Test E.SUN indices websocket used by v195.")
    parser.add_argument("--config", default=str(default_config_path))
    parser.add_argument("--seconds", type=int, default=300)
    args = parser.parse_args()

    config = ConfigParser()
    read_files = config.read(args.config, encoding="utf-8")
    if not read_files:
        raise FileNotFoundError(f"Unable to read config file: {args.config}")

    print(f"[{now_text()}] Login EsunMarketdata with {args.config}")
    realtime_sdk = EsunMarketdata(config)
    realtime_sdk.login()
    print(f"[{now_text()}] EsunMarketdata login success")

    print(f"[{now_text()}] Starting indices websocket")
    start_market_index_stream(realtime_sdk)

    print(f"[{now_text()}] Waiting {args.seconds} seconds for index messages. Press Ctrl+C to stop.")
    try:
        time.sleep(max(0, args.seconds))
    except KeyboardInterrupt:
        print("Interrupted by user.")

    dump_payload("FINAL_MARKET_INDEX_STATE", MARKET_INDEX_STATE)


if __name__ == "__main__":
    main()
