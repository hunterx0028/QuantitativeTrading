from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from .paths import CANDLES_DIR, CORPORATE_ACTIONS_DIR, FEATURES_DIR, ensure_runtime_dirs


def parse_api_date(value: str) -> date:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).date()


def candle_path(symbol: str) -> Path:
    return CANDLES_DIR / f"{symbol}.jsonl"


def feature_path(symbol: str) -> Path:
    return FEATURES_DIR / f"{symbol}.jsonl"


def corporate_action_path(symbol: str) -> Path:
    return CORPORATE_ACTIONS_DIR / f"{symbol}.jsonl"


def corporate_action_sync_path(symbol: str) -> Path:
    return CORPORATE_ACTIONS_DIR / f"{symbol}.sync.json"


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    ensure_runtime_dirs()
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def merge_candles(symbol: str, incoming: Iterable[dict]) -> list[dict]:
    by_date = {row["date"]: row for row in read_jsonl(candle_path(symbol))}
    for row in incoming:
        by_date[row["date"]] = row
    merged = [by_date[key] for key in sorted(by_date)]
    write_jsonl(candle_path(symbol), merged)
    return merged


def merge_corporate_actions(symbol: str, incoming: Iterable[dict]) -> list[dict]:
    path = corporate_action_path(symbol)
    by_key = {
        (row["date"], row["source"]): row
        for row in read_jsonl(path)
    }
    for row in incoming:
        by_key[(row["date"], row["source"])] = row
    merged = [by_key[key] for key in sorted(by_key)]
    write_jsonl(path, merged)
    return merged
