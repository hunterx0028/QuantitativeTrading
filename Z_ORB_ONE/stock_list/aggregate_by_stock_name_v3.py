from __future__ import annotations

import ast
import configparser
import time
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Dict

from esun_marketdata import EsunMarketdata

CONSOLE_RANKING_PREVIEW_COUNT = 100
REQUEST_INTERVAL_SEC = 1
MIN_ATR = 4.0
ATR_PERIOD = 14

MAX_LIMIT_UP_PRICE = 200.0
MIN_LIMIT_DOWN_PRICE = 50.0
TOP_RANK = 30
MIN_REPEAT_COUNT = 5
ST_DB_KEEP_RECENT_FILE_COUNT = 25

EXECUTION_START_TIME_PREFIX = "# [INFO] 執行開始時間:"
TOP_REPEAT_RESULT_HEADER_PREFIX = "# FILTER_RESULT"


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


def analyze_strict_streak(response_data: Dict) -> tuple[int, int, bool, bool, bool]:
    bars = response_data.get("data", [])
    if not bars:
        return 0, 0, False, False, False

    curr = bars[0]
    curr_h = float(curr["high"])
    curr_l = float(curr["low"])
    curr_c = float(curr["close"])
    curr_o = float(curr["open"])

    up_continue = 0
    if curr_c > curr_o:
        for bar in bars:
            bar_c = float(bar["close"])
            bar_o = float(bar["open"])
            if bar_c > bar_o:
                up_continue += 1
            else:
                break

    down_continue = 0
    if curr_c < curr_o:
        for bar in bars:
            bar_c = float(bar["close"])
            bar_o = float(bar["open"])
            if bar_c < bar_o:
                down_continue += 1
            else:
                break

    is_limit_up = curr_c == curr_h
    is_limit_down = curr_c == curr_l
    is_flat = curr_o == curr_c

    return up_continue, down_continue, is_limit_up, is_limit_down, is_flat


def symbol_historical_candles_continue_14(stock_id: str, sdk):
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

    up_continue, down_continue, is_limit_up, is_limit_down, is_flat = analyze_strict_streak(response_data)
    atr = calculate_atr(response_data)
    return up_continue, down_continue, is_limit_up, is_limit_down, is_flat, atr


def apply_atr_and_streak(record: tuple, atr: float, up_continue: int, down_continue: int) -> tuple:
    updated = list(record)
    if len(updated) >= 2:
        updated[-2] = float(atr)
        updated[-1] = (int(up_continue), int(down_continue))
    else:
        updated.append(float(atr))
        updated.append((int(up_continue), int(down_continue)))
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


def select_top_with_ties(ranked: list[tuple[str, int]], top_count: int) -> list[str]:
    if not ranked:
        return []
    if len(ranked) <= top_count:
        return [stock_name for stock_name, _ in ranked]

    threshold = ranked[top_count - 1][1]
    return [stock_name for stock_name, count in ranked if count >= threshold]


def select_by_min_repeat_count(ranked: list[tuple[str, int]], min_repeat_count: int) -> list[str]:
    return [stock_name for stock_name, count in ranked if count >= min_repeat_count]


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


def format_selected_stocks(records: list[tuple]) -> str:
    lines = ["selected_stocks = ["]
    lines.extend(f"    {record!r}," for record in records)
    lines.append("]")
    return "\n".join(lines)


def find_selected_stocks_assignment(source: str) -> tuple[int, int]:
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.Assign):
            target_names = [
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            ]
            if "selected_stocks" in target_names and node.end_lineno is not None:
                return node.lineno - 1, node.end_lineno
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "selected_stocks"
            and node.end_lineno is not None
        ):
            return node.lineno - 1, node.end_lineno

    raise ValueError("找不到 selected_stocks 宣告")


def update_selected_stocks_file(stock_data_path: Path, records: list[tuple]) -> None:
    source = stock_data_path.read_text(encoding="utf-8")
    lines = source.splitlines()
    start_line, end_line = find_selected_stocks_assignment(source)
    replacement = format_selected_stocks(records).splitlines()
    updated_source = "\n".join(lines[:start_line] + replacement + lines[end_line:]) + "\n"
    stock_data_path.write_text(updated_source, encoding="utf-8")


def main() -> None:
    execution_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f"[INFO] 執行開始時間: {execution_start_time}")
    base_dir = Path(__file__).resolve().parent
    project_dir = base_dir.parent
    stock_db_dir = base_dir / "st_db"
    log("[INFO] 程式啟動")
    cleanup_old_stock_db_files(stock_db_dir, ST_DB_KEEP_RECENT_FILE_COUNT)
    txt_files = sorted(stock_db_dir.glob("*.txt"))

    first_record_by_stock: dict[str, tuple] = {}
    stock_occurrence_counter: Counter[str] = Counter()

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
            stock_occurrence_counter[stock_name] += 1
            if stock_name not in first_record_by_stock:
                first_record_by_stock[stock_name] = record

    sorted_occurrences = sorted(
        stock_occurrence_counter.items(),
        key=lambda x: (-x[1], x[0]),
    )
    top_rank_stock_names = select_top_with_ties(sorted_occurrences, TOP_RANK)
    min_repeat_stock_names = select_by_min_repeat_count(sorted_occurrences, MIN_REPEAT_COUNT)

    log(f"[INFO] 清單彙整完成，共 {len(first_record_by_stock)} 檔")
    log("[INFO] 正在初始化 SDK 並登入...")
    sdk, rest_stock = init_rest_stock(project_dir)
    log("[INFO] SDK 登入成功，開始逐檔更新")
    updated_by_stock: dict[str, tuple] = {}
    stock_names = sorted(first_record_by_stock.keys())
    total = len(stock_names)
    success_count = 0
    skipped_count = 0
    fallback_count = 0

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
            up_continue, down_continue, is_limit_up, is_limit_down, is_flat, atr = symbol_historical_candles_continue_14(
                symbol,
                sdk,
            )
            updated = apply_atr_and_streak(updated, atr, up_continue, down_continue)

            if atr < MIN_ATR:
                log(f"{stock_name} 真實平均波動區間太小，跳過")
                skipped_count += 1
                continue

            tomorrow_limit_up, tomorrow_limit_down = calculate_limit_prices(updated[5])

            if tomorrow_limit_up > MAX_LIMIT_UP_PRICE:
                log(f"{stock_name} 漲停價位太高，跳過")
                skipped_count += 1
                continue

            if tomorrow_limit_down < MIN_LIMIT_DOWN_PRICE:
                log(f"{stock_name} 跌停價位太低，跳過")
                skipped_count += 1
                continue

            updated_by_stock[stock_name] = updated
            success_count += 1
        except Exception as exc:
            log(f"[WARN] {stock_name}({symbol}) 更新失敗，保留原值: {exc}")
            tomorrow_limit_up, tomorrow_limit_down = calculate_limit_prices(original[5])
            if tomorrow_limit_up > MAX_LIMIT_UP_PRICE:
                log(f"{stock_name} 漲停價位太高，跳過")
                skipped_count += 1
                continue
            if tomorrow_limit_down < MIN_LIMIT_DOWN_PRICE:
                log(f"{stock_name} 跌停價位太低，跳過")
                skipped_count += 1
                continue
            updated_by_stock[stock_name] = original
            fallback_count += 1

    output_path = base_dir / "aggregate_by_stock_name_v3_result.txt"
    stock_data_path = base_dir.parent.parent / "Z_ORB_ONE" / "stock_data.py"
    top_rank_updated_by_stock = {
        stock_name: updated_by_stock[stock_name]
        for stock_name in top_rank_stock_names
        if stock_name in updated_by_stock
    }
    min_repeat_updated_by_stock = {
        stock_name: updated_by_stock[stock_name]
        for stock_name in min_repeat_stock_names
        if stock_name in updated_by_stock
    }
    all_result_repeat_count_sort = [
        (stock_name, count)
        for stock_name, count in sorted_occurrences
        if stock_name in updated_by_stock
    ]

    with output_path.open("w", encoding="utf-8") as f:
        f.write(f"{EXECUTION_START_TIME_PREFIX} {execution_start_time}\n")
        f.write("# ALL_RESULT_REPEAT_COUNT_SORT\n")
        for stock_name, count in all_result_repeat_count_sort:
            f.write(f"{updated_by_stock[stock_name]},{count},\n")
        f.write("\n")
        f.write("# ALL_RESULT\n")
        for stock_name in sorted(updated_by_stock.keys()):
            f.write(f"{updated_by_stock[stock_name]},\n")
        f.write("\n")
        f.write(f"{TOP_REPEAT_RESULT_HEADER_PREFIX} (TOP_RANK={TOP_RANK})\n")
        for stock_name in sorted(top_rank_updated_by_stock.keys()):
            f.write(f"{top_rank_updated_by_stock[stock_name]},\n")
        f.write("\n")
        f.write(f"{TOP_REPEAT_RESULT_HEADER_PREFIX} (MIN_REPEAT_COUNT={MIN_REPEAT_COUNT})\n")
        for stock_name in sorted(min_repeat_updated_by_stock.keys()):
            f.write(f"{min_repeat_updated_by_stock[stock_name]},\n")

    selected_stock_records = [
        min_repeat_updated_by_stock[stock_name]
        for stock_name in sorted(min_repeat_updated_by_stock.keys())
    ]
    update_selected_stocks_file(stock_data_path, selected_stock_records)

    log("# ALL_RESULT_REPEAT_COUNT_SORT")
    for stock_name, count in all_result_repeat_count_sort[:CONSOLE_RANKING_PREVIEW_COUNT]:
        log(f"{updated_by_stock[stock_name]},{count},")

    log(f"{TOP_REPEAT_RESULT_HEADER_PREFIX} (TOP_RANK={TOP_RANK})")
    for stock_name in sorted(top_rank_updated_by_stock.keys()):
        log(f"{top_rank_updated_by_stock[stock_name]},")
    log(f"{TOP_REPEAT_RESULT_HEADER_PREFIX} (MIN_REPEAT_COUNT={MIN_REPEAT_COUNT})")
    for stock_name in sorted(min_repeat_updated_by_stock.keys()):
        log(f"{min_repeat_updated_by_stock[stock_name]},")

    log(f"done: {output_path}")
    log(f"updated selected_stocks: {stock_data_path}")
    log(f"stock_count={len(updated_by_stock)}")
    log(
        f"[SUMMARY] success={success_count}, "
        f"fallback={fallback_count}, skipped={skipped_count}, total={total}"
    )
    log(
        f"[SUMMARY] top_rank={TOP_RANK}, "
        f"top_result_count(rank)={len(top_rank_updated_by_stock)}, "
        f"min_repeat_count={MIN_REPEAT_COUNT}, "
        f"top_result_count(repeat_count)={len(min_repeat_updated_by_stock)}"
    )


if __name__ == "__main__":
    main()
