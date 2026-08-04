# -*- coding: utf-8 -*-
"""
Refresh E.SUN index mappings, fetch industry index closes through REST API,
and update stock_data.py market_previous_close_indices.

Run this after the close to capture today's close as the next trading day's
reference index value.
"""

import argparse
import importlib.util
import json
import os
import re
import sys
import time
from configparser import ConfigParser
from datetime import date, datetime, timedelta
from pathlib import Path
from pprint import pformat
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Iterable, List, Optional, Tuple

from esun_marketdata import EsunMarketdata


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config.ini"
DEFAULT_STOCK_DATA_PATH = BASE_DIR / "stock_data.py"
DEFAULT_INDUSTRY_MAP_PATH = BASE_DIR / "industry_index_map.json"
DEFAULT_TWSE_OUTPUT = BASE_DIR / "twse_indices.json"
DEFAULT_TPEX_OUTPUT = BASE_DIR / "tpex_indices.json"

StockTuple = Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]
IndexKey = Tuple[str, str]

RESERVE_MARKET_INDICES: Dict[str, Dict[str, Any]] = {
    "TWSE:MARKET": {
        "exchange": "TWSE",
        "industry_code": None,
        "industry_name": "上市",
        "symbol": "IX0001",
        "name": "發行量加權股價指數",
        "source": "historical.candles",
    },
    "TPEX:MARKET": {
        "exchange": "TPEX",
        "industry_code": None,
        "industry_name": "上櫃",
        "symbol": "IX0043",
        "name": "櫃買指數",
        "source": "historical.candles",
    },
}
RESERVE_MARKET_INDEX_KEYS = frozenset(RESERVE_MARKET_INDICES)

# Source: E.SUN MarketData Intraday Tickers "產業別代碼".
# Update this table manually if E.SUN changes the code list.
INDUSTRY_CODES: Dict[str, str] = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "14": "建材營造",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融保險",
    "18": "貿易百貨",
    "19": "綜合",
    "20": "其他",
    "21": "化學工業",
    "22": "生技醫療業",
    "23": "油電燃氣業",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "32": "文化創意業",
    "33": "農業科技業",
    "34": "電子商務",
    "35": "綠能環保",
    "36": "數位雲端",
    "37": "運動休閒",
    "38": "居家生活",
    "80": "管理股票",
}

INDEX_NAME_ALIASES: Dict[str, List[str]] = {
    "01": ["水泥"],
    "02": ["食品"],
    "03": ["塑膠"],
    "04": ["紡織纖維", "紡纖"],
    "05": ["電機機械", "機械"],
    "06": ["電器電纜"],
    "08": ["玻璃陶瓷"],
    "09": ["造紙"],
    "10": ["鋼鐵"],
    "11": ["橡膠"],
    "12": ["汽車"],
    "14": ["建材營造", "營建"],
    "15": ["航運"],
    "16": ["觀光餐旅"],
    "17": ["金融保險"],
    "18": ["貿易百貨"],
    "20": ["其他"],
    "21": ["化學", "化工"],
    "22": ["生技醫療"],
    "23": ["油電燃氣"],
    "24": ["半導體"],
    "25": ["電腦及週邊設備", "電腦及週邊"],
    "26": ["光電"],
    "27": ["通信網路"],
    "28": ["電子零組件"],
    "29": ["電子通路"],
    "30": ["資訊服務"],
    "31": ["其他電子"],
    "32": ["文化創意"],
    "35": ["綠能環保"],
    "36": ["數位雲端"],
    "37": ["運動休閒"],
    "38": ["居家生活"],
}


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


def fetch_index_list(sdk: EsunMarketdata, exchange: str) -> Dict[str, Any]:
    rest_stock = sdk.rest_client.stock
    return rest_stock.intraday.tickers(type="INDEX", exchange=exchange)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def normalize_name(name: str) -> str:
    normalized = str(name)
    normalized = normalized.replace("臺", "台")
    normalized = normalized.replace("櫃買", "")
    normalized = normalized.replace("業", "")
    normalized = normalized.replace("工業", "")
    normalized = normalized.replace("類", "")
    normalized = normalized.replace("指數", "")
    normalized = normalized.replace("及", "")
    normalized = normalized.replace("週邊", "周邊")
    return re.sub(r"\s+", "", normalized)


def iter_ix_indices(payload: Dict[str, Any]) -> Iterable[Dict[str, str]]:
    for item in payload.get("data", []):
        symbol = str(item.get("symbol", ""))
        name = str(item.get("name", ""))
        if symbol.startswith("IX") and name:
            yield {"symbol": symbol, "name": name}


def score_index_name(index_name: str, aliases: List[str]) -> int:
    normalized_index = normalize_name(index_name)
    best = 0
    for alias in aliases:
        normalized_alias = normalize_name(alias)
        if normalized_alias == normalized_index:
            best = max(best, 100)
        elif normalized_alias in normalized_index:
            best = max(best, 80)
    return best


def find_best_index(
    indices: Iterable[Dict[str, str]],
    industry_code: str,
) -> Optional[Dict[str, str]]:
    aliases = INDEX_NAME_ALIASES.get(industry_code)
    if not aliases:
        return None

    best_match: Optional[Dict[str, str]] = None
    best_score = 0
    for item in indices:
        score = score_index_name(item["name"], aliases)
        if score > best_score:
            best_score = score
            best_match = item

    if best_match is None:
        return None
    return {
        "symbol": best_match["symbol"],
        "name": best_match["name"],
        "match_score": best_score,
    }


def build_exchange_mapping(exchange: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    indices = list(iter_ix_indices(payload))
    industry_map: Dict[str, Any] = {}
    missing: List[str] = []

    for industry_code, industry_name in INDUSTRY_CODES.items():
        match = find_best_index(indices, industry_code)
        if match is None:
            industry_map[industry_code] = {
                "industry_name": industry_name,
                "index": None,
            }
            missing.append(industry_code)
            continue

        industry_map[industry_code] = {
            "industry_name": industry_name,
            "index": match,
        }

    return {
        "exchange": exchange,
        "source_date": payload.get("date"),
        "source_type": payload.get("type"),
        "industries": industry_map,
        "missing_industry_codes": missing,
    }


def build_industry_index_map(
    twse_payload: Dict[str, Any],
    tpex_payload: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "note": "industry code table is maintained in update_stock_data_industry_indices.py",
        "industry_codes": INDUSTRY_CODES,
        "market_indices": RESERVE_MARKET_INDICES,
        "exchanges": {
            "TWSE": build_exchange_mapping("TWSE", twse_payload),
            "TPEX": build_exchange_mapping("TPEX", tpex_payload),
        },
    }


def print_industry_map_summary(output: Dict[str, Any]) -> None:
    for exchange, exchange_data in output["exchanges"].items():
        missing = exchange_data["missing_industry_codes"]
        mapped_count = len(INDUSTRY_CODES) - len(missing)
        log(f"{exchange}: 已對應 {mapped_count} 個，未對應 {len(missing)} 個")
        if missing:
            missing_text = ", ".join(
                f"{code}:{INDUSTRY_CODES[code]}" for code in missing
            )
            log(f"{exchange} 未對應: {missing_text}")


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


def build_index_targets(
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

    market_indices = {**RESERVE_MARKET_INDICES, **(industry_map.get("market_indices") or {})}
    for map_key, index_entry in market_indices.items():
        if not index_entry.get("symbol") and map_key in RESERVE_MARKET_INDEX_KEYS:
            index_entry = RESERVE_MARKET_INDICES[map_key]
        if not index_entry.get("symbol"):
            continue
        targets[map_key] = {
            "exchange": index_entry.get("exchange"),
            "industry_code": index_entry.get("industry_code"),
            "industry_name": index_entry.get("industry_name"),
            "symbol": index_entry.get("symbol"),
            "name": index_entry.get("name"),
            "source": index_entry.get("source", "historical.candles"),
        }

    return targets, missing_stocks


def merge_relevant_indices(
    existing_indices: Dict[str, Dict[str, Any]],
    refreshed_indices: Dict[str, Dict[str, Any]],
    target_keys: Iterable[str],
) -> Dict[str, Dict[str, Any]]:
    relevant_keys = set(target_keys) | set(RESERVE_MARKET_INDEX_KEYS)
    result: Dict[str, Dict[str, Any]] = {}

    for map_key in sorted(relevant_keys):
        if map_key in refreshed_indices:
            result[map_key] = refreshed_indices[map_key]
        elif map_key in existing_indices:
            result[map_key] = existing_indices[map_key]

    return result


def parse_api_date(value: str | None) -> date | None:
    if not value:
        return None
    text = str(value)[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def extract_close_from_quote(payload: Dict[str, Any]) -> float | None:
    for key in ("closePrice", "lastPrice", "index"):
        value = payload.get(key)
        if value is not None:
            return float(value)
    return None


def extract_latest_candle_close(
    payload: Dict[str, Any],
    target_date: date,
    exact_date: bool,
) -> Tuple[float, str | None] | None:
    candles = payload.get("data", []) if isinstance(payload, dict) else []
    dated_rows: List[Tuple[date, str, Dict[str, Any]]] = []
    for row in candles:
        if not isinstance(row, dict):
            continue
        row_date = parse_api_date(row.get("date"))
        if row_date is None:
            continue
        if exact_date and row_date != target_date:
            continue
        if not exact_date and row_date > target_date:
            continue
        if row.get("close") is None:
            continue
        dated_rows.append((row_date, str(row.get("date")), row))

    if not dated_rows:
        return None

    dated_rows.sort(key=lambda item: item[1])
    row_date, row_time, row = dated_rows[-1]
    if exact_date and row_date != target_date:
        return None
    return float(row["close"]), row_time


def fetch_index_close_by_api(
    rest_stock: Any,
    map_key: str,
    target: Dict[str, Any],
    target_date: date,
    exact_date: bool,
) -> Dict[str, Any] | None:
    symbol = str(target["symbol"])
    from_date = target_date - timedelta(days=14)
    errors: List[str] = []

    try:
        response = rest_stock.historical.candles(
            **{
                "symbol": symbol,
                "from": from_date.strftime("%Y-%m-%d"),
                "to": target_date.strftime("%Y-%m-%d"),
            }
        )
        candle_close = extract_latest_candle_close(response, target_date, exact_date)
        if candle_close is not None:
            close_value, close_time = candle_close
            return {
                "symbol": symbol,
                "previous_close": close_value,
                "time": close_time,
                "last_updated": now_text(),
                "raw": response,
                "source": "historical.candles",
            }
        errors.append("historical.candles 無符合日期 close")
    except Exception as exc:
        errors.append(f"historical.candles: {exc}")

    try:
        response = rest_stock.intraday.candles(symbol=symbol)
        candle_close = extract_latest_candle_close(response, target_date, exact_date)
        if candle_close is not None:
            close_value, close_time = candle_close
            return {
                "symbol": symbol,
                "previous_close": close_value,
                "time": close_time,
                "last_updated": now_text(),
                "raw": response,
                "source": "intraday.candles",
            }
        errors.append("intraday.candles 無符合日期 close")
    except Exception as exc:
        errors.append(f"intraday.candles: {exc}")

    try:
        response = rest_stock.intraday.quote(symbol=symbol)
        close_value = extract_close_from_quote(response)
        if close_value is not None:
            return {
                "symbol": symbol,
                "previous_close": close_value,
                "time": response.get("time") or response.get("date"),
                "last_updated": now_text(),
                "raw": response,
                "source": "intraday.quote",
            }
        errors.append("intraday.quote 無 closePrice/lastPrice/index")
    except Exception as exc:
        errors.append(f"intraday.quote: {exc}")

    log(f"[WARN] {map_key} {symbol} API 取得收盤價失敗: {'; '.join(errors)}")
    return None


def fetch_indices_by_api(
    realtime_sdk: EsunMarketdata,
    targets: Dict[str, Dict[str, Any]],
    target_date: date,
    exact_date: bool,
    request_delay: float,
) -> Dict[str, Dict[str, Any]]:
    rest_stock = realtime_sdk.rest_client.stock
    state: Dict[str, Dict[str, Any]] = {}
    total = len(targets)
    for idx, (map_key, target) in enumerate(sorted(targets.items()), start=1):
        log(f"[API] ({idx}/{total}) 取得 {map_key} {target['symbol']} {target.get('name')}")
        index_state = fetch_index_close_by_api(
            rest_stock,
            map_key,
            target,
            target_date,
            exact_date,
        )
        if index_state is not None:
            state[map_key] = index_state
            log(
                f"[API] {map_key} {target['symbol']} close="
                f"{index_state['previous_close']} source={index_state.get('source')}"
            )
        if request_delay > 0 and idx < total:
            time.sleep(request_delay)
    return state


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
            "source": state.get("source", "rest_api"),
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

    log("[WARN] 下列候選股票產業沒有對應類股指數；之後可在策略初始化時排除：")
    for (exchange, industry_code), names in sorted(missing_stocks.items()):
        preview = ", ".join(names[:8])
        if len(names) > 8:
            preview += f"... 共 {len(names)} 檔"
        log(f"  {exchange}:{industry_code} -> {preview}")


def refresh_industry_map_files(
    sdk: EsunMarketdata,
    twse_output: Path,
    tpex_output: Path,
    industry_map_output: Path,
    raw_twse: bool,
) -> Dict[str, Any]:
    log("[INFO] 正在更新可訂閱指數清單與產業類股指數對應檔")
    twse_indices = fetch_index_list(sdk, "TWSE")
    tpex_indices = fetch_index_list(sdk, "TPEx")

    twse_payload = twse_indices if raw_twse else {"index_list": twse_indices}
    write_json(twse_output.resolve(), twse_payload)
    write_json(tpex_output.resolve(), tpex_indices)

    industry_map = build_industry_index_map(twse_indices, tpex_indices)
    write_json(industry_map_output.resolve(), industry_map)

    log(f"[INFO] 已寫入 {twse_output.resolve()}")
    log(f"[INFO] 已寫入 {tpex_output.resolve()}")
    log(f"[INFO] 已寫入 {industry_map_output.resolve()}")
    log(f"[INFO] TWSE 指數數量: {len(twse_indices.get('data', []))}")
    log(f"[INFO] TPEx 指數數量: {len(tpex_indices.get('data', []))}")
    print_industry_map_summary(industry_map)
    return industry_map


def main() -> int:
    parser = argparse.ArgumentParser(
        description="先刷新產業類股指數對應檔，再使用 REST API 更新候選股票產業類股指數 previous_close。"
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--stock-data", type=Path, default=DEFAULT_STOCK_DATA_PATH)
    parser.add_argument("--industry-map", type=Path, default=DEFAULT_INDUSTRY_MAP_PATH)
    parser.add_argument("--twse-output", type=Path, default=DEFAULT_TWSE_OUTPUT)
    parser.add_argument("--tpex-output", type=Path, default=DEFAULT_TPEX_OUTPUT)
    parser.add_argument(
        "--skip-refresh-industry-map",
        action="store_true",
        help="略過刷新指數清單與對應檔，直接讀取既有 --industry-map 檔案。",
    )
    parser.add_argument(
        "--raw-twse",
        action="store_true",
        help="TWSE 直接輸出 API 原始格式；未指定時維持目前 twse_indices.json 的 index_list 包裝格式。",
    )
    parser.add_argument(
        "--target-date",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d").date(),
        default=date.today(),
        help="要更新成哪一天的收盤價，格式 YYYY-MM-DD，預設今天",
    )
    parser.add_argument(
        "--allow-latest-before-target",
        action="store_true",
        help="API 若找不到 target-date 資料，允許使用 target-date 以前最新一筆 K 棒 close",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.3,
        help="API 每個指數請求間隔秒數，預設 0.3",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="API 未取得全部指數時，允許只用已取得的指數更新 stock_data.py",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只抓取並顯示結果，不寫回 stock_data.py",
    )
    args = parser.parse_args()

    stock_module = load_python_module(args.stock_data.resolve(), "stock_data_for_index_update")
    selected_stocks: List[StockTuple] = list(stock_module.selected_stocks)
    existing_indices = dict(getattr(stock_module, "market_previous_close_indices", {}))

    original_cwd = Path.cwd()
    try:
        os.chdir(args.config.resolve().parent)
        config = load_config(args.config.resolve())
        realtime_sdk = EsunMarketdata(config)
        log(f"[INFO] 正在初始化 EsunMarketdata session: {args.config.resolve()}")
        realtime_sdk.login()
        log("[INFO] EsunMarketdata session 初始化完成")

        if args.skip_refresh_industry_map:
            log(f"[INFO] 略過刷新，讀取既有產業類股指數對應檔: {args.industry_map.resolve()}")
            industry_map = load_json(args.industry_map.resolve())
        else:
            industry_map = refresh_industry_map_files(
                realtime_sdk,
                args.twse_output,
                args.tpex_output,
                args.industry_map,
                args.raw_twse,
            )

        targets, missing_stocks = build_index_targets(selected_stocks, industry_map)
        if not targets:
            raise ValueError("候選股票沒有任何可更新的產業類股指數。")

        log(f"[INFO] 候選股票涉及 {len(selected_industry_keys(selected_stocks))} 個交易所/產業組合")
        log(f"[INFO] 可更新產業類股指數 {len(targets)} 個")
        print_missing_stocks(missing_stocks)

        log(f"[INFO] 使用 REST API 取得 {args.target_date:%Y-%m-%d} 指數收盤價")
        index_state = fetch_indices_by_api(
            realtime_sdk,
            targets,
            args.target_date,
            not args.allow_latest_before_target,
            args.request_delay,
        )
        missing_keys = sorted(set(targets) - set(index_state))

        if missing_keys:
            log(f"[WARN] API 仍未取得 {len(missing_keys)} 個指數: {', '.join(missing_keys)}")
            if not args.allow_partial:
                log("[WARN] 未寫回 stock_data.py；如要部分更新，請加 --allow-partial")
                return 2
        else:
            log("[INFO] 所有指數都已取得至少一筆資料")

        received_targets = {
            map_key: targets[map_key]
            for map_key in targets
            if map_key in index_state
        }
        refreshed_indices = build_market_previous_close_indices(received_targets, index_state)
        updated_indices = merge_relevant_indices(existing_indices, refreshed_indices, targets)

        log(json.dumps(updated_indices, ensure_ascii=False, indent=2))
        if args.dry_run:
            log("[INFO] dry-run 模式，未寫回 stock_data.py")
            return 0

        write_stock_data(args.stock_data.resolve(), updated_indices, selected_stocks)
        log(f"[INFO] 已更新 {args.stock_data.resolve()}")
        return 0
    finally:
        os.chdir(original_cwd)


if __name__ == "__main__":
    raise SystemExit(main())
