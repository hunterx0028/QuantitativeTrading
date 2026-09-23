"""Shared IX0001/IX0043 daily OHLC, compared with the previous session's same field."""
from __future__ import annotations

import math

from .input_schema import INDEX_SYMBOLS, OHLC_FIELDS
from .night_futures import night_futures_bucket
from .paths import INDEX_CANDLES_DIR
from .storage import read_jsonl
from .trading_calendar import shared_calendar


def index_path(symbol):
    if symbol not in INDEX_SYMBOLS:
        raise ValueError(f"不支援的指數: {symbol}")
    return INDEX_CANDLES_DIR / f"{symbol}.jsonl"


def index_features(candles, calendar):
    """Never bridge a missing session or substitute a neutral bucket."""
    result = {}
    rows = sorted(candles, key=lambda row: row["date"])
    if len({row["date"] for row in rows}) != len(rows):
        raise ValueError("指數日K日期重複")
    for row in rows:
        calendar.require_session(row["date"])
        values = [float(row[field]) for field in OHLC_FIELDS]
        o, h, l, c = values
        if not all(math.isfinite(v) and v > 0 for v in values) or not l <= min(o, c) <= max(o, c) <= h:
            raise ValueError(f"{row['date']}: 指數 OHLC 無效，請重新 update_data")
    for previous, row in zip(rows, rows[1:]):
        if calendar.next_session(previous["date"]) != row["date"]:
            continue
        result[row["date"]] = {
            field: night_futures_bucket((float(row[field]) / float(previous[field]) - 1) * 100)
            for field in OHLC_FIELDS
        }
    return result


def load_index_features(cutoff):
    calendar = shared_calendar()
    features = {}
    for symbol in INDEX_SYMBOLS:
        rows = [row for row in read_jsonl(index_path(symbol)) if row["date"] <= cutoff]
        if not rows:
            raise ValueError(f"缺少 {symbol} 歷史日K，請先執行 update_data")
        features[symbol] = index_features(rows, calendar)
    common_dates = set.intersection(*(set(rows) for rows in features.values()))
    return {
        day: {f"{symbol.lower()}_{field}": features[symbol][day][field]
              for symbol in INDEX_SYMBOLS for field in OHLC_FIELDS}
        for day in sorted(common_dates)
    }


def join_index_features(states, indices):
    """Stock-only states remain usable for historical target validation."""
    return [{**state.to_dict(), **indices[state.date]} for state in states if state.date in indices]
