from __future__ import annotations

import importlib.util
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

from .paths import STOCK_DATA_PATH, UNIVERSE_DIR, ensure_runtime_dirs


@dataclass(frozen=True)
class StockIdentity:
    label: str
    symbol: str
    exchange: str


def parse_stock_label(label: str) -> StockIdentity:
    match = re.fullmatch(r"(.+):(\d+)\.(TW|TWO)", label.strip(), re.IGNORECASE)
    if not match:
        raise ValueError(f"股票格式錯誤: {label!r}")
    suffix = match.group(3).upper()
    return StockIdentity(
        label=label,
        symbol=match.group(2),
        exchange="TWSE" if suffix == "TW" else "TPEX",
    )


def _load_stock_data_module(path: Path = STOCK_DATA_PATH):
    spec = importlib.util.spec_from_file_location("stock_model_gpt_stock_data", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"無法載入股票清單: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_selected_stocks(path: Path = STOCK_DATA_PATH) -> list[StockIdentity]:
    module = _load_stock_data_module(path)
    identities: dict[str, StockIdentity] = {}
    for item in getattr(module, "selected_stocks", []):
        identity = parse_stock_label(item[0])
        identities[identity.symbol] = identity
    return sorted(identities.values(), key=lambda item: item.symbol)


def write_universe_snapshot(
    stocks: Iterable[StockIdentity], effective_date: date
) -> Path:
    ensure_runtime_dirs()
    path = UNIVERSE_DIR / f"{effective_date.isoformat()}.json"
    payload = {
        "effective_date": effective_date.isoformat(),
        "stocks": [asdict(stock) for stock in stocks],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_universe_snapshot(path: Path) -> list[StockIdentity]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [StockIdentity(**item) for item in payload["stocks"]]


def recent_symbols(as_of: date, trading_days: int) -> set[str]:
    """近似以 7/5 倍曆日讀取歷史清單；精確交易日由既有快照決定。"""
    start = as_of - timedelta(days=max(1, trading_days * 7 // 5 + 10))
    symbols: set[str] = set()
    for path in sorted(list(UNIVERSE_DIR.glob("*.json"))):
        try:
            snapshot_date = date.fromisoformat(path.stem)
        except ValueError:
            continue
        if start <= snapshot_date <= as_of:
            symbols.update(stock.symbol for stock in load_universe_snapshot(path))
    return symbols
