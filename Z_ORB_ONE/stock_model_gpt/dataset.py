from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import torch
from torch.utils.data import Dataset

from .storage import read_jsonl


PRICE_TO_ID = {-2: 0, -1: 1, 0: 2, 1: 3, 2: 4}
VOLUME_TO_ID = {-2: 0, -1: 1, 0: 2, 1: 3, 2: 4, "X": 5}
CLOSE_TO_ID = {"D": 0, "N": 1, "U": 2}
NIGHT_FUTURES_TO_ID = {-2: 0, -1: 1, 0: 2, 1: 3, 2: 4}


def encode_state(row: dict, atr_boundaries_pct=(1.0, 2.0, 3.0, 5.0)) -> list[int]:
    if "atr_ratio" not in row:
        raise ValueError("特徵缺少 ATR(14)，請先重新執行 prepare_features")
    if "night_futures" not in row:
        raise ValueError("特徵缺少夜盤期指，請先重新執行 prepare_features 並確認該日期已匯入 night_futures")
    atr_ratio = float(row["atr_ratio"])
    if not math.isfinite(atr_ratio) or atr_ratio < 0:
        raise ValueError("atr_ratio 必須是有限且非負的數值")
    return [
        PRICE_TO_ID[row["price"]],
        int(bool(row["hit_up"])),
        int(bool(row["hit_down"])),
        CLOSE_TO_ID[row["close_limit"]],
        VOLUME_TO_ID[row["volume"]],
        bisect_right(atr_boundaries_pct, atr_ratio * 100),
        NIGHT_FUTURES_TO_ID[row["night_futures"]],
    ]


def target_intraday_up_1plus(row: dict) -> bool:
    if "intraday_up_1plus" not in row:
        raise ValueError(
            "特徵缺少 intraday_up_1plus，請先重新執行 prepare_features；"
            "此目標代表目標日盤中 high 曾達 price bucket 1 或 2"
        )
    return bool(row["intraday_up_1plus"])


@dataclass(frozen=True)
class SequenceRef:
    feature_path: Path
    end: int


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
        cutoff = max_target_date.isoformat() if max_target_date else None
        self.rows_by_path = {
            path: [
                row for row in read_jsonl(path)
                if cutoff is None or row["date"] <= cutoff
            ]
            for path in feature_paths
        }
        floor = min_target_date.isoformat() if min_target_date else None
        self.refs: list[SequenceRef] = []
        for path, rows in self.rows_by_path.items():
            for end in range(context_days, len(rows)):
                if floor is not None and rows[end]["date"] < floor:
                    continue
                self.refs.append(SequenceRef(path, end))

    def __len__(self) -> int:
        return len(self.refs)

    def __getitem__(self, index: int):
        ref = self.refs[index]
        rows = self.rows_by_path[ref.feature_path]
        inputs = torch.tensor(
            [encode_state(row, self.atr_boundaries_pct) for row in rows[ref.end - self.context_days:ref.end]],
            dtype=torch.long,
        )
        target = encode_state(rows[ref.end], self.atr_boundaries_pct)
        # target[6] is the target day's OWN night-futures bucket (the session
        # immediately before that day's own open) — known before that day's
        # market opens, not leakage, but not part of the context sequence
        # either since it belongs to the day being predicted, not a past day.
        target_night_futures = torch.tensor(target[6], dtype=torch.long)
        return inputs, target_night_futures, {
            "intraday_up_1plus": torch.tensor(target_intraday_up_1plus(rows[ref.end]), dtype=torch.long),
        }


def subset(dataset: StockSequenceDataset, refs: list[SequenceRef]) -> StockSequenceDataset:
    """A read-only view over an existing dataset's already-loaded rows,
    restricted to `refs` (e.g. a train/validation split) — bypasses __init__ so
    it doesn't re-read any feature files from disk."""
    view = StockSequenceDataset.__new__(StockSequenceDataset)
    view.context_days = dataset.context_days
    view.atr_boundaries_pct = dataset.atr_boundaries_pct
    view.rows_by_path = dataset.rows_by_path
    view.refs = refs
    return view
