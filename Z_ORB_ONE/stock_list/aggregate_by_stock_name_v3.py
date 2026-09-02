from __future__ import annotations

import ast
import configparser
import json
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Dict

from esun_marketdata import EsunMarketdata

REQUEST_INTERVAL_SEC = 1

ATR_PERIOD = 14

ST_DB_KEEP_RECENT_FILE_COUNT = 25 # st_db 最多保留最近 25 個日期檔案
ST_DB_CALCULATE_RECENT_FILE_COUNT = 15 # 本次取最近 12 個日期檔案寫入快取

CACHE_FILE_NAME = "aggregate_by_stock_cache.json"


def log(message: str) -> None:
    print(message, flush=True)


def parse_record(line: str):
    stripped = line.strip()
    if not stripped:
        return None

    if stripped.endswith(","):
        stripped = stripped[:-1]

    try:
        record = ast.literal_eval(stripped)
    except (SyntaxError, ValueError):
        return None

    if not isinstance(record, tuple) or not record:
        return None

    first = record[0]
    if not isinstance(first, str):
        return None

    stock_name = first.split(":", 1)[0]
    return stock_name, record


def normalize_config_paths(config: configparser.ConfigParser, config_file: Path) -> None:
    if not config.has_section("Cert"):
        return

    cert_path = config.get("Cert", "Path", fallback="").strip()
    if cert_path and not Path(cert_path).is_absolute():
        config.set("Cert", "Path", str((config_file.parent / cert_path).resolve()))


def init_rest_stock(config_dir: Path):
    config_file = config_dir / "config.ini"
    config = configparser.ConfigParser()
    config.read(config_file, encoding="utf-8")
    normalize_config_paths(config, config_file.resolve())

    log("[INFO] esun_marketdata 準備登入...")
    sdk = EsunMarketdata(config)
    sdk.login()
    log("[INFO] esun_marketdata 登入成功，可以使用")
    return sdk, sdk.rest_client.stock


def extract_symbol_from_stock_name(stock_name_with_code: str) -> str:
    # "世界:5347.TWO" -> "5347"
    right = stock_name_with_code.split(":", 1)[1]
    return right.split(".", 1)[0]


def apply_stats_price(record: tuple, stats_response: dict) -> tuple:
    source = stats_response.get("data", stats_response)
    open_price = source.get("openPrice")
    high_price = source.get("highPrice")
    low_price = source.get("lowPrice")
    close_price = source.get("closePrice")

    if None in (open_price, high_price, low_price, close_price):
        raise ValueError(f"stats 回傳缺少價格欄位: {stats_response}")

    updated = list(record)
    updated[2] = float(open_price)
    updated[3] = float(high_price)
    updated[4] = float(low_price)
    updated[5] = float(close_price)
    return tuple(updated)


def calculate_atr(response_data: Dict, period: int = ATR_PERIOD) -> float:
    bars = response_data.get("data", [])
    if len(bars) < 2:
        return 0.0

    tr_values = []
    usable_period = min(period, len(bars) - 1)
    for i in range(usable_period):
        curr = bars[i]
        prev = bars[i + 1]

        curr_h = float(curr["high"])
        curr_l = float(curr["low"])
        prev_c = float(prev["close"])

        tr = max(
            curr_h - curr_l,
            abs(curr_h - prev_c),
            abs(curr_l - prev_c),
        )
        tr_values.append(tr)

    return round(sum(tr_values) / len(tr_values), 4) if tr_values else 0.0


def normalize_candle_date(value) -> str:
    normalized = str(value or "").strip().replace("-", "").replace("/", "")
    return normalized[:8]


def build_daily_limit_states(response_data: Dict) -> list[dict]:
    """只記錄每個交易日是否收漲停／跌停，不在 API 程式累計天數。"""
    bars = response_data.get("data", [])
    if len(bars) < 2:
        return []

    dated_bars = []
    for bar in bars:
        candle_date = normalize_candle_date(bar.get("date"))
        if len(candle_date) != 8 or not candle_date.isdigit():
            raise ValueError(f"K 棒缺少有效日期: {bar}")
        dated_bars.append((candle_date, bar))
    dated_bars.sort(key=lambda item: item[0], reverse=True)

    states = []
    for index in range(len(dated_bars) - 1):
        candle_date, bar = dated_bars[index]
        _previous_date, previous_bar = dated_bars[index + 1]
        limit_up, limit_down = calculate_limit_prices(float(previous_bar["close"]))
        close_price = float(bar["close"])
        states.append(
            {
                "date": candle_date,
                "is_limit_up": abs(close_price - limit_up) < 1e-8,
                "is_limit_down": abs(close_price - limit_down) < 1e-8,
            }
        )
    return states


def get_symbol_historical_data(stock_id: str, sdk):
    code_num = stock_id.split(".")[0]
    rest_stock = sdk.rest_client.stock

    today = date.today()
    from_day = today - timedelta(days=30)

    response_data = rest_stock.historical.candles(
        **{
            "symbol": code_num,
            "from": from_day.strftime("%Y-%m-%d"),
            "to": today.strftime("%Y-%m-%d"),
        }
    )
    time.sleep(REQUEST_INTERVAL_SEC)

    daily_limit_states = build_daily_limit_states(response_data)
    atr = calculate_atr(response_data)
    return daily_limit_states, atr


def apply_atr(record: tuple, atr: float) -> tuple:
    updated = list(record)
    if len(updated) >= 2:
        updated[-2] = float(atr)
        updated[-1] = (0, 0)
    else:
        updated.append(float(atr))
        updated.append((0, 0))
    return tuple(updated)


def get_tick_size(price: float) -> float:
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.1
    if price < 500:
        return 0.5
    if price < 1000:
        return 1
    return 5


def round_price(price: float, tick: float) -> float:
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    rounded_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return float(rounded_units * tick_dec)


def floor_price_to_tick(price: float, tick: float) -> float:
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    floored_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_FLOOR)
    return float(floored_units * tick_dec)


def ceil_price_to_tick(price: float, tick: float) -> float:
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    ceiled_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return float(ceiled_units * tick_dec)


def calculate_limit_prices(prev_close: float):
    up_raw = prev_close * 1.10
    down_raw = prev_close * 0.90
    limit_up_tick = get_tick_size(up_raw)
    limit_down_tick = get_tick_size(down_raw)
    limit_up = floor_price_to_tick(up_raw, limit_up_tick)
    limit_down = ceil_price_to_tick(down_raw, limit_down_tick)
    return limit_up, limit_down


def cleanup_old_stock_db_files(stock_db_dir: Path, keep_count: int) -> None:
    dated_txt_files = sorted(
        (txt_file for txt_file in stock_db_dir.glob("*.txt") if txt_file.stem.isdigit()),
        key=lambda txt_file: txt_file.stem,
        reverse=True,
    )
    old_txt_files = dated_txt_files[keep_count:]
    for txt_file in old_txt_files:
        txt_file.unlink()

    if old_txt_files:
        log(f"[INFO] st_db 僅保留最近日期 {keep_count} 個檔案，已刪除 {len(old_txt_files)} 個舊檔")
    else:
        log(f"[INFO] st_db 日期檔案數量未超過 {keep_count}，不需刪除")


def validate_stock_db_file_counts(keep_count: int, calculate_count: int) -> None:
    if keep_count <= 0:
        raise ValueError("ST_DB_KEEP_RECENT_FILE_COUNT 必須大於 0")
    if calculate_count <= 0:
        raise ValueError("ST_DB_CALCULATE_RECENT_FILE_COUNT 必須大於 0")
    if calculate_count > keep_count:
        raise ValueError(
            "ST_DB_CALCULATE_RECENT_FILE_COUNT 不可大於 "
            "ST_DB_KEEP_RECENT_FILE_COUNT"
        )


def main() -> None:
    execution_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f"[INFO] 執行開始時間: {execution_start_time}")
    base_dir = Path(__file__).resolve().parent
    project_dir = base_dir.parent
    stock_db_dir = base_dir / "st_db"
    log("[INFO] 程式啟動")
    validate_stock_db_file_counts(
        ST_DB_KEEP_RECENT_FILE_COUNT,
        ST_DB_CALCULATE_RECENT_FILE_COUNT,
    )
    cleanup_old_stock_db_files(stock_db_dir, ST_DB_KEEP_RECENT_FILE_COUNT)
    dated_txt_files = sorted(
        (txt_file for txt_file in stock_db_dir.glob("*.txt") if txt_file.stem.isdigit()),
        key=lambda txt_file: txt_file.stem,
        reverse=True,
    )
    txt_files = sorted(
        dated_txt_files[:ST_DB_CALCULATE_RECENT_FILE_COUNT],
        key=lambda txt_file: txt_file.stem,
    )
    log(
        f"[INFO] 本次使用最新 {len(txt_files)} 個日期檔案統計"
        f"（設定值={ST_DB_CALCULATE_RECENT_FILE_COUNT}）"
    )

    first_record_by_stock: dict[str, tuple] = {}
    occurrence_dates_by_stock: dict[str, list[str]] = {}
    trading_dates = [txt_file.stem for txt_file in txt_files]

    for txt_file in txt_files:
        txt_name = txt_file.name.lower()
        if txt_name.startswith("aggregate") or txt_name.startswith("filter"):
            continue
        if txt_file.stem == "aggregated_by_stock_name":
            continue
        if not txt_file.stem.isdigit():
            continue

        for line in txt_file.read_text(encoding="utf-8").splitlines():
            parsed = parse_record(line)
            if parsed is None:
                continue

            stock_name, record = parsed
            occurrence_dates_by_stock.setdefault(stock_name, []).append(txt_file.stem)
            if stock_name not in first_record_by_stock:
                first_record_by_stock[stock_name] = record

    log(f"[INFO] 清單彙整完成，共 {len(first_record_by_stock)} 檔")
    log("[INFO] 正在初始化 SDK 並登入...")
    sdk, rest_stock = init_rest_stock(project_dir)
    log("[INFO] SDK 登入成功，開始逐檔更新")
    updated_by_stock: dict[str, tuple] = {}
    daily_limit_states_by_stock: dict[str, list[dict]] = {}
    stock_names = sorted(first_record_by_stock.keys())
    total = len(stock_names)
    success_count = 0
    api_failed_count = 0

    log(f"[INFO] 開始更新，共 {total} 檔")
    for idx, stock_name in enumerate(stock_names, start=1):
        original = first_record_by_stock[stock_name]
        symbol = extract_symbol_from_stock_name(original[0])
        progress = (idx / total) * 100 if total else 100.0
        log(f"[PROGRESS] {idx}/{total} ({progress:.1f}%) symbol:{symbol} {stock_name}")
        try:
            time.sleep(REQUEST_INTERVAL_SEC)
            stats_response = rest_stock.historical.stats(symbol=symbol)
            updated = apply_stats_price(original, stats_response)
            daily_limit_states, atr = get_symbol_historical_data(
                symbol,
                sdk,
            )
            updated = apply_atr(updated, atr)

            updated_by_stock[stock_name] = updated
            daily_limit_states_by_stock[stock_name] = daily_limit_states
            success_count += 1
        except Exception as exc:
            log(f"[WARN] {stock_name}({symbol}) API 更新失敗，本次排除: {exc}")
            api_failed_count += 1
            continue

    cache_dir = base_dir / "aggregate_json_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_path = cache_dir / CACHE_FILE_NAME
    cache = {
        "schema_version": 2,
        "generated_at": execution_start_time,
        "trading_dates": trading_dates,
        "stocks": [
            {
                "stock_name": stock_name,
                "occurrence_dates": occurrence_dates_by_stock.get(stock_name, []),
                "daily_limit_states": daily_limit_states_by_stock[stock_name],
                "record": updated_by_stock[stock_name],
            }
            for stock_name in sorted(updated_by_stock)
        ],
    }
    output_path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    log(f"done: {output_path}")
    log(f"stock_count={len(updated_by_stock)}")
    log(
        f"[SUMMARY] success={success_count}, "
        f"api_failed_excluded={api_failed_count}, "
        f"total={total}"
    )


if __name__ == "__main__":
    main()
