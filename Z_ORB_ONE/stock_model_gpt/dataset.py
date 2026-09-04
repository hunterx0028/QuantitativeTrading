from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import torch
from torch.utils.data import Dataset

from .storage import read_jsonl


PRICE_TO_ID = {-2: 0, -1: 1, 0: 2, 1: 3, 2: 4}
VOLUME_TO_ID = {-2: 0, -1: 1, 0: 2, 1: 3, 2: 4, "X": 5}
CLOSE_TO_ID = {"D": 0, "N": 1, "U": 2}


def encode_state(row: dict) -> list[int]:
    return [
        PRICE_TO_ID[row["price"]],
        int(bool(row["hit_up"])),
        int(bool(row["hit_down"])),
        CLOSE_TO_ID[row["close_limit"]],
        VOLUME_TO_ID[row["volume"]],
    ]


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
    ):
        self.context_days = context_days
        cutoff = max_target_date.isoformat() if max_target_date else None
        self.rows_by_path = {
            path: [
                row for row in read_jsonl(path)
                if cutoff is None or row["date"] <= cutoff
            ]
            for path in feature_paths
        }
        self.refs: list[SequenceRef] = []
        for path, rows in self.rows_by_path.items():
            for end in range(context_days, len(rows)):
                self.refs.append(SequenceRef(path, end))

    def __len__(self) -> int:
        return len(self.refs)

    def __getitem__(self, index: int):
        ref = self.refs[index]
        rows = self.rows_by_path[ref.feature_path]
        inputs = torch.tensor(
            [encode_state(row) for row in rows[ref.end - self.context_days:ref.end]],
            dtype=torch.long,
        )
        target = encode_state(rows[ref.end])
        return inputs, {
            "price": torch.tensor(target[0], dtype=torch.long),
            "hit_up": torch.tensor(target[1], dtype=torch.long),
            "hit_down": torch.tensor(target[2], dtype=torch.long),
            "close_limit": torch.tensor(target[3], dtype=torch.long),
        }
