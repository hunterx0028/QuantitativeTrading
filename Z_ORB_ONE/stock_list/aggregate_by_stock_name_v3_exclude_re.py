"""只讀現有快取：最近 B 個交易日的股票，扣除截至最新日連續出現 A 次者。

價格、ATR、產業及 LIMIT 分流設定可在本檔獨立調整。
選股僅使用現有快取，不讀 st_db 或重新查詢行情 API。
執行後寫入獨立結果檔及 stock_data.py，再呼叫產業指數更新程式。
"""

from __future__ import annotations

import ast
import json
import math
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

MAX_LIMIT_UP_PRICE = 300.0
MIN_LIMIT_DOWN_PRICE = 50.0
MIN_ATR_PCT = 4.0  # 一般清單要求 ATR / 昨收價 * 100 >= 4%；LIMIT 名單沿用分流規則

LONG_LIMIT_UP_DAYS = [99]  # 分入 selected_limit_up_stocks 的實際連續漲停天數
SHORT_LIMIT_DOWN_DAYS = [99]  # 分入 selected_limit_down_stocks 的實際連續跌停天數
TOP_RANK = 30  # 出現次數的排名，僅供結果檔顯示，與寫入 stock_data.py 無關
EXCLUDED_INDUSTRY_CODES: list[str] = ["17"]  # 排除 17-金融保險
# 其他可選：20-其他、36-數位雲端、31-其他電子業、25-電腦及週邊設備業

# 原 MIN_REPEAT_COUNT 的條件由參數 A 取代，改為排除連續出現者。
EXCLUDE_MIN_REPEAT_COUNT = 3  # 參數 A：排除最近連續出現至少 A 個交易日的股票；0 表示不排除
RECENT_CACHE_DATE_COUNT = 5  # 參數 B：僅取現有快取中最近 B 個交易日出現過的股票
CACHE_FILE_NAME = "aggregate_by_stock_cache.json"
OUTPUT_RESULT_FILE_NAME = "aggregate_by_stock_name_v3_result_exclude_re.txt"

EXECUTION_START_TIME_PREFIX = "# [INFO] 執行開始時間:"
ALL_RESULT_REPEAT_COUNT_SORT_HEADER = "# ALL_RESULT_REPEAT_COUNT_SORT"
TOP_REPEAT_RESULT_HEADER_PREFIX = "# FILTER_RESULT"


def log(message: str) -> None:
    print(message, flush=True)


def lists_to_tuples(value):
    if isinstance(value, list):
        return tuple(lists_to_tuples(item) for item in value)
    return value


def load_cache(cache_path: Path) -> tuple[list[str], list[dict]]:
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    if cache.get("schema_version") != 2:
        raise ValueError(f"不支援的快取格式版本: {cache.get('schema_version')}")

    trading_dates = sorted({str(item) for item in cache.get("trading_dates", [])})
    stocks = cache.get("stocks")
    if not isinstance(stocks, list):
        raise ValueError("快取缺少 stocks 陣列")
    return trading_dates, stocks


def count_recent_consecutive_occurrences(
    occurrence_dates: list[str],
    trading_dates: list[str],
) -> int:
    """由快取最新交易日起，計算股票不中斷出現的交易日數。"""
    occurrences = {str(item) for item in occurrence_dates}
    count = 0
    for trading_date in reversed(trading_dates):
        if trading_date not in occurrences:
            break
        count += 1
    return count


def count_recent_consecutive_limit_days(
    daily_limit_states: list[dict],
    limit_trading_dates: list[str],
    state_name: str,
) -> int:
    """由 API 資料的最新交易日起累計，遇到缺日或不符合即停止。"""
    state_by_date = {
        str(state.get("date")): bool(state.get(state_name))
        for state in daily_limit_states
        if isinstance(state, dict) and state.get("date")
    }
    count = 0
    for trading_date in reversed(limit_trading_dates):
        if not state_by_date.get(trading_date, False):
            break
        count += 1
    return count


def apply_limit_counts(record: tuple, up_count: int, down_count: int) -> tuple:
    updated = list(record)
    if len(updated) >= 2:
        updated[-1] = (int(up_count), int(down_count))
    else:
        updated.append((int(up_count), int(down_count)))
    return tuple(updated)


def build_ranked_records(
    trading_dates: list[str],
    stocks: list[dict],
) -> tuple[list[tuple[tuple, int]], list[tuple[tuple, int]]]:
    total_ranked = []
    consecutive_ranked = []
    limit_trading_dates = sorted(
        {
            str(state.get("date"))
            for stock in stocks
            for state in stock.get("daily_limit_states", [])
            if isinstance(state, dict) and state.get("date")
        }
    )
    for stock in stocks:
        record = lists_to_tuples(stock.get("record"))
        if not isinstance(record, tuple):
            continue
        daily_limit_states = stock.get("daily_limit_states", [])
        if not isinstance(daily_limit_states, list):
            daily_limit_states = []
        up_count = count_recent_consecutive_limit_days(
            daily_limit_states,
            limit_trading_dates,
            "is_limit_up",
        )
        down_count = count_recent_consecutive_limit_days(
            daily_limit_states,
            limit_trading_dates,
            "is_limit_down",
        )
        record = apply_limit_counts(record, up_count, down_count)
        occurrence_dates = [str(item) for item in stock.get("occurrence_dates", [])]
        total_ranked.append((record, len(set(occurrence_dates))))
        consecutive_ranked.append(
            (
                record,
                count_recent_consecutive_occurrences(occurrence_dates, trading_dates),
            )
        )

    sort_key = lambda item: (-item[1], str(item[0][0]) if item[0] else "")
    total_ranked.sort(key=sort_key)
    consecutive_ranked.sort(key=sort_key)
    return total_ranked, consecutive_ranked


def build_result_lines(
    execution_start_time: str,
    ranked_records: list[tuple[tuple, int]],
    rank_records: list[tuple],
    rank_header: str,
    repeat_records: list[tuple],
    repeat_header: str,
) -> list[str]:
    lines = [f"{EXECUTION_START_TIME_PREFIX} {execution_start_time}\n"]
    lines.append(f"{ALL_RESULT_REPEAT_COUNT_SORT_HEADER}\n")
    lines.extend(f"{record},{count},\n" for record, count in ranked_records)
    lines.append("\n# ALL_RESULT\n")
    lines.extend(f"{record},\n" for record, _count in ranked_records)
    lines.append(f"\n{TOP_REPEAT_RESULT_HEADER_PREFIX} ({rank_header})\n")
    lines.extend(f"{record},\n" for record in rank_records)
    lines.append(f"\n{TOP_REPEAT_RESULT_HEADER_PREFIX} ({repeat_header})\n")
    lines.extend(f"{record},\n" for record in repeat_records)
    return lines


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


def calculate_limit_prices(prev_close: float) -> tuple[float, float]:
    up_raw = prev_close * 1.10
    down_raw = prev_close * 0.90
    limit_up_tick = get_tick_size(up_raw)
    limit_down_tick = get_tick_size(down_raw)
    limit_up = floor_price_to_tick(up_raw, limit_up_tick)
    limit_down = ceil_price_to_tick(down_raw, limit_down_tick)
    return limit_up, limit_down


def normalize_industry_code(industry_code) -> str:
    if industry_code is None:
        return ""
    return str(industry_code).strip()


def select_top_with_ties(ranked: list[tuple[tuple, int]], top_count: int) -> list[tuple]:
    if not ranked:
        return []
    if len(ranked) <= top_count:
        return [record for record, _ in ranked]

    threshold = ranked[top_count - 1][1]
    return [record for record, count in ranked if count >= threshold]


def format_stock_records_assignment(name: str, records: list[tuple]) -> str:
    lines = [f"{name} = ["]
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


def find_assignment(source: str, name: str) -> tuple[int, int] | None:
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.Assign):
            target_names = [
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            ]
            if name in target_names and node.end_lineno is not None:
                return node.lineno - 1, node.end_lineno
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
            and node.end_lineno is not None
        ):
            return node.lineno - 1, node.end_lineno

    return None


def ensure_selected_limit_stock_lists(source: str) -> str:
    lines = source.splitlines()
    missing_names = [
        name
        for name in ("selected_limit_up_stocks", "selected_limit_down_stocks")
        if find_assignment(source, name) is None
    ]
    if not missing_names:
        return source

    _start_line, end_line = find_selected_stocks_assignment(source)
    insert_lines = [""]
    insert_lines.extend(f"{name} = []" for name in missing_names)
    lines[end_line:end_line] = insert_lines
    return "\n".join(lines) + "\n"


def replace_stock_records_assignment(source: str, name: str, records: list[tuple]) -> str:
    lines = source.splitlines()
    assignment = find_assignment(source, name)
    if assignment is None:
        raise ValueError(f"找不到 {name} 宣告")

    start_line, end_line = assignment
    replacement = format_stock_records_assignment(name, records).splitlines()
    return "\n".join(lines[:start_line] + replacement + lines[end_line:]) + "\n"


def get_limit_repeat_counts(record: tuple) -> tuple[int, int]:
    repeat_counts = record[-1] if record else None
    if not isinstance(repeat_counts, tuple) or len(repeat_counts) < 2:
        return 0, 0

    return int(repeat_counts[0]), int(repeat_counts[1])


def update_selected_stocks_file(
    stock_data_path: Path,
    selected_records: list[tuple],
    limit_up_records: list[tuple],
    limit_down_records: list[tuple],
) -> None:
    source = stock_data_path.read_text(encoding="utf-8")
    source = ensure_selected_limit_stock_lists(source)
    source = replace_stock_records_assignment(source, "selected_stocks", selected_records)
    source = replace_stock_records_assignment(source, "selected_limit_up_stocks", limit_up_records)
    source = replace_stock_records_assignment(source, "selected_limit_down_stocks", limit_down_records)
    stock_data_path.write_text(source, encoding="utf-8")

def filter_records(ranked: list[tuple[tuple, int]]) -> list[tuple[tuple, int]]:
    """使用本檔設定篩選一般清單。"""
    excluded_codes = {
        normalize_industry_code(code) for code in EXCLUDED_INDUSTRY_CODES
    }
    filtered = []
    for record, count in ranked:
        if len(record) <= 7:
            continue
        try:
            close = float(record[5])
            atr = float(record[7])
        except (TypeError, ValueError):
            continue
        if not math.isfinite(close) or not math.isfinite(atr) or close <= 0 or atr < 0:
            continue
        limit_up, limit_down = calculate_limit_prices(close)
        if (
            limit_up <= MAX_LIMIT_UP_PRICE
            and limit_down >= MIN_LIMIT_DOWN_PRICE
            and atr / close * 100 >= MIN_ATR_PCT
            and normalize_industry_code(record[6]) not in excluded_codes
        ):
            filtered.append((record, count))
    return filtered


def split_records_by_limit_repeat(
    records: list[tuple],
) -> tuple[list[tuple], list[tuple], list[tuple]]:
    selected, limit_up, limit_down = [], [], []
    for record in records:
        up_count, down_count = get_limit_repeat_counts(record)
        if up_count in LONG_LIMIT_UP_DAYS:
            limit_up.append(record)
        elif down_count in SHORT_LIMIT_DOWN_DAYS:
            limit_down.append(record)
        else:
            selected.append(record)
    return selected, limit_up, limit_down


def build_recent_ranked_records(
    trading_dates: list[str],
    stocks: list[dict],
    exclude_min_repeat_count: int,
    recent_cache_date_count: int,
) -> tuple[list[str], list[tuple[tuple, int]], list[tuple[tuple, int]]]:
    """回傳 B 日期、B 區間排名、排除連續 A 次後的排名（次數均以 B 計）。"""
    if type(exclude_min_repeat_count) is not int or exclude_min_repeat_count < 0:
        raise ValueError("EXCLUDE_MIN_REPEAT_COUNT 必須是非負整數")
    if type(recent_cache_date_count) is not int or recent_cache_date_count < 1:
        raise ValueError("RECENT_CACHE_DATE_COUNT 必須是正整數")
    dates = sorted({str(item) for item in trading_dates})
    required_count = max(exclude_min_repeat_count, recent_cache_date_count)
    if len(dates) < required_count:
        raise ValueError(
            f"快取只有 {len(dates)} 個交易日，A={exclude_min_repeat_count}、"
            f"B={recent_cache_date_count} 至少需要 {required_count} 個；"
            "請調整參數或先執行 aggregate_by_stock_name_v3.py 產生足夠日期的快取"
        )

    recent_dates = dates[-recent_cache_date_count:]
    recent_date_set = set(recent_dates)
    # 使用完整快取計算漲跌停，避免 B 區間縮短 API 日期軸。
    all_ranked, _ = build_ranked_records(dates, stocks)
    counts_by_name = {}
    excluded_names = set()
    for stock in stocks:
        record = stock.get("record")
        if not isinstance(record, (list, tuple)) or not record:
            continue
        name = str(record[0])
        occurrence_dates = {str(item) for item in stock.get("occurrence_dates", [])}
        counts_by_name[name] = len(occurrence_dates & recent_date_set)
        # A 可以大於 B，連續次數必須從完整快取的最新日期往回判定。
        if exclude_min_repeat_count > 0 and count_recent_consecutive_occurrences(
            list(occurrence_dates), dates
        ) >= exclude_min_repeat_count:
            excluded_names.add(name)

    recent_ranked = [
        (record, counts_by_name[str(record[0])])
        for record, _ in all_ranked
        if record and counts_by_name.get(str(record[0]), 0) > 0
    ]
    recent_ranked.sort(key=lambda item: (-item[1], str(item[0][0])))
    remaining_ranked = [
        (record, count) for record, count in recent_ranked
        if str(record[0]) not in excluded_names
    ]
    return recent_dates, recent_ranked, remaining_ranked


def main() -> None:
    for name, days in (
        ("LONG_LIMIT_UP_DAYS", LONG_LIMIT_UP_DAYS),
        ("SHORT_LIMIT_DOWN_DAYS", SHORT_LIMIT_DOWN_DAYS),
    ):
        if (
            not isinstance(days, list)
            or any(type(day) is not int or day < 1 for day in days)
            or len(set(days)) != len(days)
        ):
            raise ValueError(f"{name} 必須是沒有重複值的正整數陣列（可為空）: {days}")
    if type(TOP_RANK) is not int or TOP_RANK < 1:
        raise ValueError("TOP_RANK 必須是正整數")
    execution_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    base_dir = Path(__file__).resolve().parent
    cache_path = base_dir / "aggregate_json_cache" / CACHE_FILE_NAME
    output_path = base_dir / OUTPUT_RESULT_FILE_NAME
    stock_data_path = base_dir.parent / "stock_data.py"
    trading_dates, stocks = load_cache(cache_path)
    recent_dates, recent_ranked, remaining_ranked = build_recent_ranked_records(
        trading_dates, stocks, EXCLUDE_MIN_REPEAT_COUNT, RECENT_CACHE_DATE_COUNT
    )
    filtered = filter_records(remaining_ranked)
    rank_records = select_top_with_ties(filtered, TOP_RANK)
    rank_header = f"TOP_RANK={TOP_RANK}"
    remaining_records = [record for record, _ in filtered]
    filter_header = (
        f"RECENT_CACHE_DATE_COUNT={RECENT_CACHE_DATE_COUNT}, "
        f"EXCLUDE_MIN_REPEAT_COUNT={EXCLUDE_MIN_REPEAT_COUNT}"
    )
    lines = build_result_lines(
        execution_start_time, recent_ranked, rank_records, rank_header,
        remaining_records, filter_header,
    )
    # LIMIT 沿用原本分流規則，但同樣必須先符合 B 區間及排除 A 的條件。
    selected, _, _ = split_records_by_limit_repeat(remaining_records)
    _, limit_up, limit_down = split_records_by_limit_repeat(
        [record for record, _ in remaining_ranked]
    )
    update_selected_stocks_file(stock_data_path, selected, limit_up, limit_down)
    output_path.write_text("".join(lines), encoding="utf-8")
    log(f"source={cache_path}")
    log(f"recent_trading_dates={recent_dates}")
    log(filter_header)
    log(f"recent_count={len(recent_ranked)}")
    log(f"excluded_consecutive_count={len(recent_ranked) - len(remaining_ranked)}")
    log(f"remaining_count={len(remaining_ranked)}")
    log(f"selected_stocks_count={len(selected)}")
    log(f"selected_limit_up_stocks_count={len(limit_up)}")
    log(f"selected_limit_down_stocks_count={len(limit_down)}")
    log(f"done: {output_path}")
    log(f"updated selected_stocks: {stock_data_path}")

    update_script_path = base_dir.parent / "update_stock_data_industry_indices.py"
    log(f"[INFO] 選股完成，開始更新產業指數: {update_script_path}")
    subprocess.run(
        [sys.executable, str(update_script_path), "--stock-data", str(stock_data_path)],
        cwd=update_script_path.parent,
        check=True,
    )
    log("[INFO] 產業指數更新完成")


if __name__ == "__main__":
    main()
