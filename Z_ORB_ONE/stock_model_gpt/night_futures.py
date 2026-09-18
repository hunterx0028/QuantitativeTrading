"""TAIFEX TX (台指期貨) near-month night-session (盤後交易) data.

Source: https://www.taifex.com.tw/cht/3/futDailyMarketReport (latest day) and
its CSV export (history, limited to ~1 month per query; older data needs the
site's separate year-zip archives). The CSV is Big5-encoded and lists two rows
per contract per date — 交易時段="一般" (day session) and "盤後" (night
session) — for every open contract month, so multiple TX rows share one date.

TAIFEX's own 漲跌%/漲跌價 columns on a 盤後 row are already computed against
that *same contract's* immediately preceding 一般 (day session) close — i.e.
exactly "distance from yesterday's day-session close" — so this module reads
that column directly rather than re-deriving it by joining across dates.

Night trading (盤後) only exists from 2017-05-15 onward; dates before that
have no row here at all, not a zero-valued one — callers must treat a missing
date as "no data", not as "flat" (see `features.py:encode_candles`, which
skips any day without night-futures coverage rather than guessing a value).
"""
from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Iterable

from .paths import NIGHT_FUTURES_PATH
from .storage import read_jsonl, write_jsonl


NIGHT_FUTURES_CLASSES = (-2, -1, 0, 1, 2)
# Ascending boundaries between consecutive classes above, e.g. NIGHT_FUTURES_BOUNDARIES_PCT[0]
# is the -2/-1 cutoff. Must stay strictly increasing and one shorter than NIGHT_FUTURES_CLASSES.
NIGHT_FUTURES_BOUNDARIES_PCT = (-1.0, -0.5, 0.5, 1.0)


def night_futures_bucket(change_pct: float) -> int:
    """+2/+1/0/-1/-2 bucket for the night session's % change from the
    preceding day-session close, matching `features.py:price_bucket`'s
    boundary-snapping convention (a value that lands exactly on a boundary
    goes to the less extreme bucket, guarding against float noise)."""
    if not math.isfinite(change_pct):
        raise ValueError("夜盤漲跌幅必須是有限數值")
    for boundary, bucket in zip(NIGHT_FUTURES_BOUNDARIES_PCT, NIGHT_FUTURES_CLASSES):
        if change_pct < boundary and not math.isclose(change_pct, boundary, abs_tol=1e-9):
            return bucket
    return NIGHT_FUTURES_CLASSES[-1]


def parse_night_futures_csv(path: Path) -> list[dict]:
    """Parse one TAIFEX futDailyMarketReport CSV export. Returns one record
    per date — the TX near-month (smallest 到期月份 among that date's rows)
    盤後 (night-session) row — as {"date": "YYYY-MM-DD", "contract_month":
    "202609", "change_pct": 0.23, "bucket": 1}."""
    by_date: dict[str, dict] = {}
    with path.open(encoding="big5") as stream:
        reader = csv.DictReader(stream)
        for row in reader:
            # Large year-archive exports can end in a trailing DOS EOF marker
            # (0x1A) that CSV-parses as a short row with every other field
            # None — skip anything that isn't a real, fully-populated data row.
            contract = row.get("契約")
            session = row.get("交易時段")
            if contract is None or session is None:
                continue
            if contract.strip() != "TX" or session.strip() != "盤後":
                continue
            date_str = row["交易日期"].strip().replace("/", "-")
            contract_month = row["到期月份(週別)"].strip()
            existing = by_date.get(date_str)
            if existing is not None and contract_month >= existing["contract_month"]:
                continue  # keep the near month only
            change_field = row.get("漲跌%", "").strip().rstrip("%")
            if not change_field or change_field == "-":
                continue  # no trade that session; leave the date uncovered rather than guessing 0
            change_pct = float(change_field)
            by_date[date_str] = {
                "date": date_str,
                "contract_month": contract_month,
                "change_pct": change_pct,
                "bucket": night_futures_bucket(change_pct),
            }
    return sorted(by_date.values(), key=lambda item: item["date"])


def merge_night_futures(incoming: Iterable[dict]) -> list[dict]:
    by_date = {row["date"]: row for row in read_jsonl(NIGHT_FUTURES_PATH)}
    for row in incoming:
        by_date[row["date"]] = row
    merged = [by_date[key] for key in sorted(by_date)]
    write_jsonl(NIGHT_FUTURES_PATH, merged)
    return merged


def load_night_futures() -> dict[str, int]:
    """date -> bucket, for every date with known TX night-session data."""
    return {row["date"]: row["bucket"] for row in read_jsonl(NIGHT_FUTURES_PATH)}
