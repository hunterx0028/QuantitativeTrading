from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import torch
from torch.utils.data import Dataset

from .storage import read_jsonl
from .night_futures import load_night_futures
from .trading_calendar import assert_sequence_dates, shared_calendar


PRICE_TO_ID = {-2: 0, -1: 1, 0: 2, 1: 3, 2: 4}
VOLUME_TO_ID = {-2: 0, -1: 1, 0: 2, 1: 3, 2: 4, "X": 5}
CLOSE_TO_ID = {"D": 0, "N": 1, "U": 2}
NIGHT_FUTURES_TO_ID = {-2: 0, -1: 1, 0: 2, 1: 3, 2: 4}
INPUT_ALIGNMENT = "stock_nine_ohlc_next_trading_day_night_v2"
PRICE_FIELDS = ("open_price", "high_price", "low_price", "close_price")


def encode_sequence(rows: list[dict], prediction_date: str, night_by_date: dict[str, int],
                    atr_boundaries_pct=(1.0, 2.0, 3.0, 5.0)) -> list[list[int]]:
    """Pair each stock row with the following trading day's dated night bucket.

    Raw feature rows are never mutated. Night values are read by date from the
    current store so corrections take effect without rebuilding feature files.
    """
    dates = [row["date"] for row in rows] + [prediction_date]
    if any(left >= right for left, right in zip(dates, dates[1:])):
        raise ValueError("股票序列與預測日期必須嚴格遞增")
    assert_sequence_dates(dates)
    encoded = []
    for index, row in enumerate(rows):
        night_date = dates[index + 1]
        if index + 1 < len(rows) and rows[index + 1].get("previous_date") != row["date"]:
            raise ValueError("歷史特徵缺少連續交易日或 previous_date；請重新執行 prepare_features")
        if night_date not in night_by_date:
            raise ValueError(f"找不到 {night_date} 開盤前的夜盤資料，請先匯入")
        encoded.append(encode_state({**row, "night_futures": night_by_date[night_date]}, atr_boundaries_pct))
    return encoded


def encode_state(row: dict, atr_boundaries_pct=(1.0, 2.0, 3.0, 5.0)) -> list[int]:
    missing = [field for field in PRICE_FIELDS if field not in row]
    if missing:
        raise ValueError(f"特徵缺少開高低收欄位 {', '.join(missing)}；請重新執行 prepare_features")
    if "atr_ratio" not in row:
        raise ValueError("特徵缺少 ATR(14)，請先重新執行 prepare_features")
    if "night_futures" not in row:
        raise ValueError("特徵缺少夜盤期指，請先重新執行 prepare_features 並確認該日期已匯入 night_futures")
    atr_ratio = float(row["atr_ratio"])
    if not math.isfinite(atr_ratio) or atr_ratio < 0:
        raise ValueError("atr_ratio 必須是有限且非負的數值")
    return [
        *(PRICE_TO_ID[row[field]] for field in PRICE_FIELDS),
        int(bool(row["hit_up"])),
        int(bool(row["hit_down"])),
        CLOSE_TO_ID[row["close_limit"]],
        VOLUME_TO_ID[row["volume"]],
        bisect_right(atr_boundaries_pct, atr_ratio * 100),
        NIGHT_FUTURES_TO_ID[row["night_futures"]],
    ]


@dataclass(frozen=True)
class SequenceRef:
    feature_path: Path
    end: int


def _calendar_session_ok(calendar, day: str) -> bool:
    try:
        return calendar.is_session(day)
    except ValueError:
        return False


def _calendar_link_ok(calendar, previous_day: str, day: str) -> bool:
    try:
        return calendar.next_session(previous_day) == day
    except ValueError:
        return False


class StockSequenceDataset(Dataset):
    def __init__(
        self,
        feature_paths: list[Path],
        context_days: int,
        max_target_date: date | None = None,
        atr_boundaries_pct=(1.0, 2.0, 3.0, 5.0),
        min_target_date: date | None = None,
    ):
        self.context_days = context_days
        self.atr_boundaries_pct = atr_boundaries_pct
        self.night_by_date = load_night_futures()
        cutoff = max_target_date.isoformat() if max_target_date else None
        self.rows_by_path = {
            path: [
                row for row in read_jsonl(path)
                if cutoff is None or row["date"] <= cutoff
            ]
            for path in feature_paths
        }
        floor = min_target_date.isoformat() if min_target_date else None
        calendar = shared_calendar()
        self.refs: list[SequenceRef] = []
        for path, rows in self.rows_by_path.items():
            count = len(rows)
            if count <= context_days:
                continue
            if any("previous_date" not in row for row in rows[1:]):
                raise ValueError("特徵缺少 previous_date；請重新執行 prepare_features")
            # `chain_length[i]`: length of the run of rows ending at i that are each
            # a valid trading session and unbroken (by previous_date *and*
            # calendar-derived next_session, so a forged previous_date can't hide
            # a real gap) — computed once per row instead of once per (row,
            # window) pair, which made dataset construction O(days * context_days).
            chain_length = [0] * count
            chain_length[0] = 1 if _calendar_session_ok(calendar, rows[0]["date"]) else 0
            for i in range(1, count):
                if not _calendar_session_ok(calendar, rows[i]["date"]):
                    chain_length[i] = 0
                    continue
                linked = (chain_length[i - 1] > 0
                          and rows[i]["previous_date"] == rows[i - 1]["date"]
                          and _calendar_link_ok(calendar, rows[i - 1]["date"], rows[i]["date"]))
                chain_length[i] = chain_length[i - 1] + 1 if linked else 1
            # `night_gap[k]`: count of rows in rows[0:k] missing night-futures
            # coverage, for an O(1) range check below instead of re-scanning
            # each window.
            night_gap = [0] * (count + 1)
            for i in range(count):
                night_gap[i + 1] = night_gap[i] + (0 if rows[i]["date"] in self.night_by_date else 1)
            for end in range(context_days, count):
                if floor is not None and rows[end]["date"] < floor:
                    continue
                if chain_length[end] < context_days + 1:
                    continue
                start = end - context_days
                if night_gap[end + 1] - night_gap[start + 1] > 0:
                    continue
                self.refs.append(SequenceRef(path, end))

    def __len__(self) -> int:
        return len(self.refs)

    def __getitem__(self, index: int):
        ref = self.refs[index]
        rows = self.rows_by_path[ref.feature_path]
        inputs = torch.tensor(
            encode_sequence(rows[ref.end - self.context_days:ref.end], rows[ref.end]["date"],
                            self.night_by_date, self.atr_boundaries_pct),
            dtype=torch.long,
        )
        return inputs, {
            "high_price": torch.tensor(PRICE_TO_ID[rows[ref.end]["high_price"]], dtype=torch.long),
            "low_price": torch.tensor(PRICE_TO_ID[rows[ref.end]["low_price"]], dtype=torch.long),
        }


def subset(dataset: StockSequenceDataset, refs: list[SequenceRef]) -> StockSequenceDataset:
    """A read-only view over an existing dataset's already-loaded rows,
    restricted to `refs` (e.g. a train/validation split) — bypasses __init__ so
    it doesn't re-read any feature files from disk."""
    view = StockSequenceDataset.__new__(StockSequenceDataset)
    view.context_days = dataset.context_days
    view.atr_boundaries_pct = dataset.atr_boundaries_pct
    view.night_by_date = dataset.night_by_date
    view.rows_by_path = dataset.rows_by_path
    view.refs = refs
    return view
