from __future__ import annotations

import ast
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

MAX_LIMIT_UP_PRICE = 200.0
MIN_LIMIT_DOWN_PRICE = 50.0
TOP_RANK = 30
MIN_REPEAT_COUNT = 5

RESULT_FILE_NAME = "aggregate_by_stock_name_v3_result.txt"
OUTPUT_RESULT_FILE_NAME = "aggregate_by_stock_name_v3_result_re.txt"
EXECUTION_START_TIME_PREFIX = "# [INFO] 執行開始時間:"
ALL_RESULT_REPEAT_COUNT_SORT_HEADER = "# ALL_RESULT_REPEAT_COUNT_SORT"
TOP_REPEAT_RESULT_HEADER_PREFIX = "# FILTER_RESULT"
LEGACY_TOP_REPEAT_HEADER_PREFIX = "# TOP_REPEAT_RESULT"


def log(message: str) -> None:
    print(message, flush=True)


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
    base_dir = Path(__file__).resolve().parent
    result_path = base_dir / RESULT_FILE_NAME
    output_result_path = base_dir / OUTPUT_RESULT_FILE_NAME

    if not result_path.exists():
        raise FileNotFoundError(f"找不到檔案: {result_path}")

    lines = result_path.read_text(encoding="utf-8").splitlines(keepends=True)
    ranked_records = extract_ranked_records(lines)
    filtered_ranked_records = filter_records_by_price_range(ranked_records)
    rank_records, rank_header = select_records_for_rank(filtered_ranked_records)
    repeat_records, repeat_header = select_records_for_repeat_count(filtered_ranked_records)
    updated_lines = upsert_execution_start_time(lines, execution_start_time)
    updated_lines = remove_section_by_header_prefix(updated_lines, LEGACY_TOP_REPEAT_HEADER_PREFIX)
    updated_lines = replace_top_repeat_section(
        updated_lines,
        [
            (rank_records, rank_header),
            (repeat_records, repeat_header),
        ],
    )
    output_result_path.write_text("".join(updated_lines), encoding="utf-8")

    log(f"done: {output_result_path}")
    log(f"source={result_path}")
    log(f"ranked_count={len(ranked_records)}")
    log(f"filtered_ranked_count={len(filtered_ranked_records)}")
    log(f"top_rank={TOP_RANK}")
    log(f"min_repeat_count={MIN_REPEAT_COUNT}")
    log(f"max_limit_up_price={MAX_LIMIT_UP_PRICE}")
    log(f"min_limit_down_price={MIN_LIMIT_DOWN_PRICE}")
    log(f"top_result_count(rank)={len(rank_records)}")
    log(f"top_result_count(repeat_count)={len(repeat_records)}")


if __name__ == "__main__":
    main()
