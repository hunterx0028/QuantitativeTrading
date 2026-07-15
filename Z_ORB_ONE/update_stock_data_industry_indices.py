# -*- coding: utf-8 -*-
"""
Subscribe industry index symbols through E.SUN WebSocket and update
stock_data.py market_previous_close_indices.

Run this after the close, or at the time you want to capture as the next
trading day's reference index value.
"""

import argparse
import importlib.util
import json
import os
import sys
import time
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from pprint import pformat
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, List, Tuple

from esun_marketdata import EsunMarketdata


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config.ini"
DEFAULT_STOCK_DATA_PATH = BASE_DIR / "stock_data.py"
DEFAULT_INDUSTRY_MAP_PATH = BASE_DIR / "industry_index_map.json"

StockTuple = Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]
IndexKey = Tuple[str, str]


INDEX_STATE: Dict[str, Dict[str, Any]] = {}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(message: str) -> None:
    print(message, flush=True)


def load_python_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"無法載入 Python 模組: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_config(path: Path) -> ConfigParser:
    config = ConfigParser()
    read_files = config.read(path, encoding="utf-8")
    if not read_files:
        raise FileNotFoundError(f"讀取 config.ini 失敗: {path}")
    return config


def get_exchange_for_stock(symbol_str: str) -> str:
    symbol_upper = str(symbol_str or "").upper()
    if symbol_upper.endswith(".TWO"):
        return "TPEX"
    return "TWSE"


def get_stock_name(symbol_str: str) -> str:
    return str(symbol_str).split(":", 1)[0]


def selected_industry_keys(stocks: Iterable[StockTuple]) -> List[IndexKey]:
    keys: List[IndexKey] = []
    seen: set[IndexKey] = set()
    for stock in stocks:
        symbol_str = stock[0]
        industry_code = str(stock[6]).zfill(2)
        key = (get_exchange_for_stock(symbol_str), industry_code)
        if key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def get_index_entry(
    industry_map: Dict[str, Any],
    exchange: str,
    industry_code: str,
) -> Dict[str, Any] | None:
    exchange_data = industry_map.get("exchanges", {}).get(exchange, {})
    industry_data = exchange_data.get("industries", {}).get(industry_code, {})
    index_data = industry_data.get("index")
    if not index_data:
        return None

    return {
        "exchange": exchange,
        "industry_code": industry_code,
        "industry_name": industry_data.get("industry_name"),
        "symbol": index_data.get("symbol"),
        "name": index_data.get("name"),
    }


def build_subscription_targets(
    stocks: Iterable[StockTuple],
    industry_map: Dict[str, Any],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[IndexKey, List[str]]]:
    targets: Dict[str, Dict[str, Any]] = {}
    missing_stocks: Dict[IndexKey, List[str]] = {}

    for stock in stocks:
        symbol_str = stock[0]
        exchange = get_exchange_for_stock(symbol_str)
        industry_code = str(stock[6]).zfill(2)
        key = (exchange, industry_code)
        index_entry = get_index_entry(industry_map, exchange, industry_code)

        if index_entry is None or not index_entry.get("symbol"):
            missing_stocks.setdefault(key, []).append(get_stock_name(symbol_str))
            continue

        map_key = f"{exchange}:{industry_code}"
        targets[map_key] = index_entry

    return targets, missing_stocks


def handle_index_message(message: Any, symbol_to_key: Dict[str, str]) -> None:
    payload = json.loads(message) if isinstance(message, str) else message
    data = payload.get("data", payload) if isinstance(payload, dict) else {}
    index_symbol = str(data.get("symbol", ""))
    map_key = symbol_to_key.get(index_symbol)
    if not map_key:
        return

    index_value = data.get("index")
    if index_value is None:
        return

    INDEX_STATE[map_key] = {
        "symbol": index_symbol,
        "previous_close": float(index_value),
        "time": data.get("time"),
        "last_updated": now_text(),
        "raw": data,
    }
    log(f"[{now_text()}] {map_key} {index_symbol} index={float(index_value)}")


def start_index_stream(
    realtime_sdk: EsunMarketdata,
    targets: Dict[str, Dict[str, Any]],
) -> Any:
    symbol_to_key = {
        str(info["symbol"]): map_key
        for map_key, info in targets.items()
        if info.get("symbol")
    }

    def on_message(message: Any) -> None:
        try:
            handle_index_message(message, symbol_to_key)
        except Exception as exc:
            log(f"[WARN] 處理指數訊息失敗: {exc}")

    stock_ws = realtime_sdk.websocket_client.stock
    stock_ws.on("message", on_message)
    log("[INFO] 正在連線 WebSocket")
    stock_ws.connect()
    log("[INFO] WebSocket 已連線")

    for map_key, info in targets.items():
        stock_ws.subscribe({
            "channel": "indices",
            "symbol": info["symbol"],
        })
        log(f"[MARKET] 已訂閱 {map_key} {info['symbol']} {info.get('name')}")

    return stock_ws


def close_index_stream(stock_ws: Any) -> None:
    if stock_ws is None:
        return

    for method_name in ("disconnect", "close", "stop"):
        method = getattr(stock_ws, method_name, None)
        if not callable(method):
            continue
        try:
            method()
            log(f"[INFO] WebSocket 已執行 {method_name}()")
            return
        except Exception as exc:
            log(f"[WARN] WebSocket {method_name}() 失敗: {exc}")


def build_market_previous_close_indices(
    targets: Dict[str, Dict[str, Any]],
    index_state: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for map_key in sorted(targets):
        target = targets[map_key]
        state = index_state[map_key]
        result[map_key] = {
            "exchange": target["exchange"],
            "industry_code": target["industry_code"],
            "industry_name": target.get("industry_name"),
            "symbol": target["symbol"],
            "name": target.get("name"),
            "previous_close": state["previous_close"],
            "time": state.get("time"),
            "last_updated": state.get("last_updated"),
        }
    return result


def write_stock_data(
    stock_data_path: Path,
    market_previous_close_indices: Dict[str, Dict[str, Any]],
    selected_stocks: List[StockTuple],
) -> None:
    lines = [
        "# 股票代碼、購買量、昨天開盤、昨天最高、昨天最低、昨天收盤、產業別代碼、真實平均波動幅度、(連漲天數, 連跌天數)\n"
    ]
    lines.append("market_previous_close_indices = ")
    lines.append(pformat(market_previous_close_indices, sort_dicts=False))
    lines.append("\n\n")
    lines.append("selected_stocks = [\n")
    for item in selected_stocks:
        lines.append(f"    {repr(item)},\n")
    lines.append("]\n")

    stock_data_path = stock_data_path.resolve()
    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=str(stock_data_path.parent),
        delete=False,
    ) as tmp_file:
        tmp_file.writelines(lines)
        tmp_file.flush()
        os.fsync(tmp_file.fileno())
        tmp_path = Path(tmp_file.name)

    os.replace(tmp_path, stock_data_path)


def print_missing_stocks(missing_stocks: Dict[IndexKey, List[str]]) -> None:
    if not missing_stocks:
        return

    log("[WARN] 下列候選股票產業沒有對應類股指數，未訂閱；之後可在策略初始化時排除：")
    for (exchange, industry_code), names in sorted(missing_stocks.items()):
        preview = ", ".join(names[:8])
        if len(names) > 8:
            preview += f"... 共 {len(names)} 檔"
        log(f"  {exchange}:{industry_code} -> {preview}")


def wait_for_indices(expected_keys: Iterable[str], seconds: int) -> List[str]:
    expected = set(expected_keys)
    deadline = time.time() + max(0, seconds)
    while time.time() < deadline:
        missing = sorted(expected - set(INDEX_STATE))
        if not missing:
            return []
        time.sleep(1)
    return sorted(expected - set(INDEX_STATE))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="訂閱候選股票產業類股指數，並更新 stock_data.py 的 previous_close。"
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--stock-data", type=Path, default=DEFAULT_STOCK_DATA_PATH)
    parser.add_argument("--industry-map", type=Path, default=DEFAULT_INDUSTRY_MAP_PATH)
    parser.add_argument("--seconds", type=int, default=120, help="等待所有指數回傳的秒數")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="逾時仍允許只用已收到的指數更新 stock_data.py",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只訂閱並顯示結果，不寫回 stock_data.py",
    )
    args = parser.parse_args()

    stock_module = load_python_module(args.stock_data.resolve(), "stock_data_for_index_update")
    selected_stocks: List[StockTuple] = list(stock_module.selected_stocks)
    industry_map = load_json(args.industry_map.resolve())

    targets, missing_stocks = build_subscription_targets(selected_stocks, industry_map)
    if not targets:
        raise ValueError("候選股票沒有任何可訂閱的產業類股指數。")

    log(f"[INFO] 候選股票涉及 {len(selected_industry_keys(selected_stocks))} 個交易所/產業組合")
    log(f"[INFO] 可訂閱產業類股指數 {len(targets)} 個")
    print_missing_stocks(missing_stocks)

    original_cwd = Path.cwd()
    stock_ws = None
    try:
        os.chdir(args.config.resolve().parent)
        config = load_config(args.config.resolve())
        realtime_sdk = EsunMarketdata(config)
        log(f"[INFO] 正在初始化 EsunMarketdata session: {args.config.resolve()}")
        realtime_sdk.login()
        log("[INFO] EsunMarketdata session 初始化完成")

        stock_ws = start_index_stream(realtime_sdk, targets)
        log(f"[INFO] 等待最多 {args.seconds} 秒收集指數資料")
        missing_keys = wait_for_indices(targets.keys(), args.seconds)

        if missing_keys:
            log(f"[WARN] 逾時仍未收到 {len(missing_keys)} 個指數: {', '.join(missing_keys)}")
            if not args.allow_partial:
                log("[WARN] 未寫回 stock_data.py；如要部分更新，請加 --allow-partial")
                return 2
        else:
            log("[INFO] 所有訂閱指數都已收到至少一筆資料")

        received_targets = {
            map_key: targets[map_key]
            for map_key in targets
            if map_key in INDEX_STATE
        }
        updated_indices = build_market_previous_close_indices(received_targets, INDEX_STATE)

        log(json.dumps(updated_indices, ensure_ascii=False, indent=2))
        if args.dry_run:
            log("[INFO] dry-run 模式，未寫回 stock_data.py")
            return 0

        write_stock_data(args.stock_data.resolve(), updated_indices, selected_stocks)
        log(f"[INFO] 已更新 {args.stock_data.resolve()}")
        return 0
    finally:
        close_index_stream(stock_ws)
        os.chdir(original_cwd)


if __name__ == "__main__":
    raise SystemExit(main())
