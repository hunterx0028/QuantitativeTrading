from __future__ import annotations

import ast
import json
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

MAX_LIMIT_UP_PRICE = 300.0
MIN_LIMIT_DOWN_PRICE = 50.0
MIN_ATR = 4.0 # ATR 低於此門檻的股票不寫入 stock_data.py

LONG_LIMIT_UP_DAYS = [99] # 分入 selected_limit_up_stocks 的實際連續漲停天數
SHORT_LIMIT_DOWN_DAYS = [99] # 分入 selected_limit_down_stocks 的實際連續跌停天數

TOP_RANK = 30 # 出現次數的排名，僅是打印用，和寫入 stock_data.py 無關

MIN_REPEAT_COUNT = 0 # 從快取最新交易日起，股票不中斷出現的最少交易日數；設為 0 時取消此條件，供擴大測試樣本使用

EXCLUDED_INDUSTRY_CODES: list[str] = ["17"] # 排除 17-金融保險, 20-其他, 36-數位雲端, 31-其他電子業, 25-電腦及週邊設備業
# "17", "20", "36", "31", "25"

CACHE_FILE_NAME = "aggregate_by_stock_cache.json"
OUTPUT_RESULT_FILE_NAME = "aggregate_by_stock_name_v3_result_re.txt"
EXECUTION_START_TIME_PREFIX = "# [INFO] 執行開始時間:"
ALL_RESULT_REPEAT_COUNT_SORT_HEADER = "# ALL_RESULT_REPEAT_COUNT_SORT"
TOP_REPEAT_RESULT_HEADER_PREFIX = "# FILTER_RESULT"
LEGACY_TOP_REPEAT_HEADER_PREFIX = "# TOP_REPEAT_RESULT"


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


def parse_rank_line(line: str):
    stripped = line.strip()
    if not stripped:
        return None

    # 原始格式: (tuple...),20,
    if not stripped.endswith(","):
        return None
    stripped = stripped[:-1]

    try:
        record_part, count_part = stripped.rsplit(",", 1)
        record = ast.literal_eval(record_part)
        count = int(count_part.strip())
    except (ValueError, SyntaxError):
        return None

    if not isinstance(record, tuple):
        return None
    return record, count


def extract_ranked_records(lines: list[str]) -> list[tuple[tuple, int]]:
    start_idx = -1
    for i, line in enumerate(lines):
        if line.strip() == ALL_RESULT_REPEAT_COUNT_SORT_HEADER:
            start_idx = i + 1
            break

    if start_idx == -1:
        raise ValueError(f"找不到區塊: {ALL_RESULT_REPEAT_COUNT_SORT_HEADER}")

    ranked: list[tuple[tuple, int]] = []
    for i in range(start_idx, len(lines)):
        current = lines[i]
        stripped = current.strip()
        if stripped.startswith("#"):
            break
        parsed = parse_rank_line(current)
        if parsed is not None:
            ranked.append(parsed)
    return ranked


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


def is_record_in_price_range(record: tuple) -> bool:
    if len(record) <= 5:
        return False

    try:
        close_price = float(record[5])
    except (TypeError, ValueError):
        return False

    tomorrow_limit_up, tomorrow_limit_down = calculate_limit_prices(close_price)
    return tomorrow_limit_up <= MAX_LIMIT_UP_PRICE and tomorrow_limit_down >= MIN_LIMIT_DOWN_PRICE


def filter_records_by_price_range(ranked: list[tuple[tuple, int]]) -> list[tuple[tuple, int]]:
    return [(record, count) for record, count in ranked if is_record_in_price_range(record)]


def is_record_atr_qualified(record: tuple) -> bool:
    if len(record) <= 7:
        return False
    try:
        atr_value = float(record[7])
    except (TypeError, ValueError):
        return False
    return atr_value >= MIN_ATR


def filter_records_by_min_atr(ranked: list[tuple[tuple, int]]) -> list[tuple[tuple, int]]:
    return [(record, count) for record, count in ranked if is_record_atr_qualified(record)]


def normalize_industry_code(industry_code) -> str:
    if industry_code is None:
        return ""
    return str(industry_code).strip()


def is_record_in_excluded_industry(record: tuple) -> bool:
    if len(record) <= 6:
        return False

    excluded_codes = {
        normalize_industry_code(industry_code)
        for industry_code in EXCLUDED_INDUSTRY_CODES
    }
    return normalize_industry_code(record[6]) in excluded_codes


def filter_records_by_excluded_industries(ranked: list[tuple[tuple, int]]) -> list[tuple[tuple, int]]:
    return [
        (record, count)
        for record, count in ranked
        if not is_record_in_excluded_industry(record)
    ]


def sum_previous_close_prices(records: list[tuple]) -> Decimal:
    total = Decimal("0")
    for record in records:
        if len(record) <= 5:
            continue
        total += Decimal(str(record[5]))
    return total


def select_top_with_ties(ranked: list[tuple[tuple, int]], top_count: int) -> list[tuple]:
    if not ranked:
        return []
    if len(ranked) <= top_count:
        return [record for record, _ in ranked]

    threshold = ranked[top_count - 1][1]
    return [record for record, count in ranked if count >= threshold]


def select_by_min_repeat_count(ranked: list[tuple[tuple, int]], min_repeat_count: int) -> list[tuple]:
    return [record for record, count in ranked if count >= min_repeat_count]


def select_records_for_rank(ranked: list[tuple[tuple, int]]) -> tuple[list[tuple], str]:
    records = select_top_with_ties(ranked, TOP_RANK)
    header_suffix = f"TOP_RANK={TOP_RANK}"
    return records, header_suffix


def select_records_for_repeat_count(ranked: list[tuple[tuple, int]]) -> tuple[list[tuple], str]:
    records = select_by_min_repeat_count(ranked, MIN_REPEAT_COUNT)
    header_suffix = f"MIN_REPEAT_COUNT={MIN_REPEAT_COUNT}"
    return records, header_suffix


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


def split_records_by_limit_repeat(
    records: list[tuple],
) -> tuple[list[tuple], list[tuple], list[tuple]]:
    selected_records = []
    limit_up_records = []
    limit_down_records = []
    for record in records:
        up_repeat_count, down_repeat_count = get_limit_repeat_counts(record)
        if up_repeat_count in LONG_LIMIT_UP_DAYS:
            limit_up_records.append(record)
        elif down_repeat_count in SHORT_LIMIT_DOWN_DAYS:
            limit_down_records.append(record)
        else:
            selected_records.append(record)

    return selected_records, limit_up_records, limit_down_records


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


def replace_top_repeat_section(lines: list[str], blocks: list[tuple[list[tuple], str]]) -> list[str]:
    new_block: list[str] = []
    for records, header_suffix in blocks:
        new_block.append(f"{TOP_REPEAT_RESULT_HEADER_PREFIX} ({header_suffix})\n")
        new_block.extend(f"{record},\n" for record in records)
        new_block.append("\n")
    if new_block and new_block[-1] == "\n":
        new_block.pop()

    start_idx = -1
    for i, line in enumerate(lines):
        if line.strip().startswith(TOP_REPEAT_RESULT_HEADER_PREFIX):
            start_idx = i
            break

    if start_idx == -1:
        if lines and lines[-1].strip() != "":
            lines.append("\n")
        return lines + new_block

    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        stripped = lines[i].strip()
        if not stripped.startswith("#"):
            continue
        if stripped.startswith(TOP_REPEAT_RESULT_HEADER_PREFIX):
            continue
        end_idx = i
        break

    return lines[:start_idx] + new_block + lines[end_idx:]


def remove_section_by_header_prefix(lines: list[str], header_prefix: str) -> list[str]:
    start_idx = -1
    for i, line in enumerate(lines):
        if line.strip().startswith(header_prefix):
            start_idx = i
            break

    if start_idx == -1:
        return lines

    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        if lines[i].strip().startswith("#"):
            end_idx = i
            break

    return lines[:start_idx] + lines[end_idx:]


def upsert_execution_start_time(lines: list[str], execution_start_time: str) -> list[str]:
    time_line = f"{EXECUTION_START_TIME_PREFIX} {execution_start_time}\n"
    if lines and lines[0].strip().startswith(EXECUTION_START_TIME_PREFIX):
        return [time_line] + lines[1:]
    return [time_line] + lines


def main() -> None:
    execution_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f"[INFO] 執行開始時間: {execution_start_time}")

    for setting_name, allowed_days in (
        ("LONG_LIMIT_UP_DAYS", LONG_LIMIT_UP_DAYS),
        ("SHORT_LIMIT_DOWN_DAYS", SHORT_LIMIT_DOWN_DAYS),
    ):
        if (
            not isinstance(allowed_days, list)
            or any(type(days) is not int or days < 1 for days in allowed_days)
            or len(set(allowed_days)) != len(allowed_days)
        ):
            raise ValueError(
                f"{setting_name} 必須是沒有重複值的正整數陣列（可為空）: {allowed_days}"
            )

    base_dir = Path(__file__).resolve().parent
    cache_path = base_dir / "aggregate_json_cache" / CACHE_FILE_NAME
    output_result_path = base_dir / OUTPUT_RESULT_FILE_NAME
    stock_data_path = base_dir.parent.parent / "Z_ORB_ONE" / "stock_data.py"

    if not cache_path.exists():
        raise FileNotFoundError(f"找不到快取檔案: {cache_path}")

    trading_dates, cached_stocks = load_cache(cache_path)
    ranked_records, consecutive_ranked_records = build_ranked_records(
        trading_dates,
        cached_stocks,
    )
    price_filtered_ranked_records = filter_records_by_price_range(ranked_records)
    atr_filtered_ranked_records = filter_records_by_min_atr(price_filtered_ranked_records)
    filtered_ranked_records = filter_records_by_excluded_industries(atr_filtered_ranked_records)
    consecutive_price_filtered = filter_records_by_price_range(consecutive_ranked_records)
    consecutive_atr_filtered = filter_records_by_min_atr(consecutive_price_filtered)
    consecutive_filtered = filter_records_by_excluded_industries(consecutive_atr_filtered)
    rank_records, rank_header = select_records_for_rank(filtered_ranked_records)
    repeat_records, repeat_header = select_records_for_repeat_count(consecutive_filtered)
    updated_lines = build_result_lines(
        execution_start_time,
        ranked_records,
        rank_records,
        rank_header,
        repeat_records,
        repeat_header,
    )
    output_result_path.write_text("".join(updated_lines), encoding="utf-8")
    # 一般名單仍須通過價位、ATR、產業與出現次數篩選；符合連續漲跌停天數者則直接從
    # 來源完整排名資料挑選，優先寫入 LIMIT 名單，不受上述篩選條件限制。
    selected_stock_records, _, _ = split_records_by_limit_repeat(repeat_records)
    all_ranked_stock_records = [record for record, _count in ranked_records]
    (
        _,
        selected_limit_up_stock_records,
        selected_limit_down_stock_records,
    ) = split_records_by_limit_repeat(all_ranked_stock_records)
    update_selected_stocks_file(
        stock_data_path,
        selected_stock_records,
        selected_limit_up_stock_records,
        selected_limit_down_stock_records,
    )

    log(f"done: {output_result_path}")
    log(f"updated selected_stocks: {stock_data_path}")
    log(f"source={cache_path}")
    log(f"trading_dates={trading_dates}")
    log(f"ranked_count={len(ranked_records)}")
    log(f"price_filtered_ranked_count={len(price_filtered_ranked_records)}")
    log(f"atr_filtered_ranked_count={len(atr_filtered_ranked_records)}")
    log(f"filtered_ranked_count={len(filtered_ranked_records)}")
    log(f"excluded_industry_codes={EXCLUDED_INDUSTRY_CODES}")
    log(f"top_rank={TOP_RANK}")
    log(f"min_repeat_count={MIN_REPEAT_COUNT}")
    log(f"long_limit_up_days={LONG_LIMIT_UP_DAYS}")
    log(f"short_limit_down_days={SHORT_LIMIT_DOWN_DAYS}")
    log(f"max_limit_up_price={MAX_LIMIT_UP_PRICE}")
    log(f"min_limit_down_price={MIN_LIMIT_DOWN_PRICE}")
    log(f"min_atr={MIN_ATR}")
    log(f"top_result_count(rank)={len(rank_records)}")
    log(f"top_result_count(repeat_count)={len(repeat_records)}")
    log(f"selected_stocks_count={len(selected_stock_records)}")
    log(f"selected_limit_up_stocks_count={len(selected_limit_up_stock_records)}")
    log(f"selected_limit_down_stocks_count={len(selected_limit_down_stock_records)}")
    log(f"selected_stocks_previous_close_total={sum_previous_close_prices(selected_stock_records)}")
    log(
        "selected_limit_up_stocks_previous_close_total="
        f"{sum_previous_close_prices(selected_limit_up_stock_records)}"
    )
    log(
        "selected_limit_down_stocks_previous_close_total="
        f"{sum_previous_close_prices(selected_limit_down_stock_records)}"
    )


if __name__ == "__main__":
    main()
