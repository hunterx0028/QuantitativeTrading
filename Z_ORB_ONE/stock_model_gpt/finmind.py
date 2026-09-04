from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta
from typing import Any

from .config import Settings
from .storage import (
    corporate_action_sync_path,
    merge_corporate_actions,
)


FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
FREE_DATASETS = ("TaiwanStockDividendResult",)
EXTENDED_DATASETS = (
    "TaiwanStockCapitalReductionReferencePrice",
    "TaiwanStockSplitPrice",
    "TaiwanStockParValueChange",
)

# 除權息結果需按個股查詢；其餘三種稀疏事件資料可查全市場。
# 同一查詢區間只抓一次，再由本機依 stock_id 過濾，讓121支股票的
# 首次同步維持在匿名額度內。
DATASET_ACCEPTS_DATA_ID = {
    "TaiwanStockDividendResult": True,
    "TaiwanStockCapitalReductionReferencePrice": False,
    "TaiwanStockSplitPrice": False,
    "TaiwanStockParValueChange": False,
}
_GLOBAL_DATASET_CACHE: dict[tuple[str, date, date, bool], list[dict]] = {}


def _number(row: dict[str, Any], *names: str) -> float | None:
    for name in names:
        value = row.get(name)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str) and value.strip():
            return float(value)
    return None


def _normalise_action(dataset: str, row: dict) -> dict:
    mappings = {
        "TaiwanStockDividendResult": {
            "reference": ("reference_price", "after_price"),
            "up": ("max_price",),
            "down": ("min_price",),
        },
        "TaiwanStockCapitalReductionReferencePrice": {
            "reference": ("OpeningReferencePrice", "PostReductionReferencePrice"),
            "up": ("LimitUp",),
            "down": ("LimitDown",),
        },
        "TaiwanStockSplitPrice": {
            "reference": ("after_price", "open_price"),
            "up": ("max_price",),
            "down": ("min_price",),
        },
        "TaiwanStockParValueChange": {
            "reference": ("after_ref_close", "after_ref_open"),
            "up": ("after_ref_max",),
            "down": ("after_ref_min",),
        },
    }
    fields = mappings[dataset]
    return {
        "date": str(row["date"])[:10],
        "symbol": str(row.get("stock_id", "")),
        "source": dataset,
        "reference_price": _number(row, *fields["reference"]),
        "limit_up": _number(row, *fields["up"]),
        "limit_down": _number(row, *fields["down"]),
        "raw": row,
    }


def fetch_dataset(
    dataset: str,
    symbol: str,
    start_date: date,
    end_date: date,
    token: str | None = None,
    timeout_seconds: float = 30.0,
) -> list[dict]:
    # FinMind 各資料集對 end_date 邊界的實作可能不同；多查一天後再本地嚴格過濾。
    query_end_date = end_date + timedelta(days=1)
    accepts_data_id = DATASET_ACCEPTS_DATA_ID[dataset]
    cache_key = (dataset, start_date, end_date, bool(token))
    if not accepts_data_id and cache_key in _GLOBAL_DATASET_CACHE:
        return [
            action for action in _GLOBAL_DATASET_CACHE[cache_key]
            if action["symbol"] == symbol
        ]

    query_parameters = {
        "dataset": dataset,
        "start_date": start_date.isoformat(),
        "end_date": query_end_date.isoformat(),
    }
    if accepts_data_id:
        query_parameters["data_id"] = symbol
    parameters = urllib.parse.urlencode(query_parameters)
    headers = {"Accept": "application/json", "User-Agent": "stock-model-gpt/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(f"{FINMIND_URL}?{parameters}", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"FinMind {dataset} HTTP {exc.code}: {detail[:300]}") from exc
    if payload.get("status") != 200:
        raise RuntimeError(
            f"FinMind {dataset} 查詢失敗: status={payload.get('status')} msg={payload.get('msg')}"
        )
    actions = [_normalise_action(dataset, row) for row in payload.get("data", [])]
    actions = [
        action for action in actions
        if start_date.isoformat() <= action["date"] <= end_date.isoformat()
    ]
    if not accepts_data_id:
        _GLOBAL_DATASET_CACHE[cache_key] = actions
        actions = [action for action in actions if action["symbol"] == symbol]
    return actions


def update_corporate_actions(
    symbol: str,
    as_of: date,
    settings: Settings,
    token: str | None = None,
) -> list[dict]:
    """增量取得影響歷史參考價的公司行動；Token 可省略。"""
    sync_path = corporate_action_sync_path(symbol)
    start_date = date.fromisoformat(settings.earliest_date)
    if sync_path.exists():
        payload = json.loads(sync_path.read_text(encoding="utf-8"))
        start_date = date.fromisoformat(payload["checked_through"]) + timedelta(days=1)
    if start_date > as_of:
        return []

    token = token or os.environ.get("FINMIND_TOKEN") or None
    incoming: list[dict] = []
    datasets = FREE_DATASETS + (
        EXTENDED_DATASETS if settings.finmind_extended_corporate_actions else ()
    )
    for index, dataset in enumerate(datasets):
        incoming.extend(fetch_dataset(dataset, symbol, start_date, as_of, token=token))
        if index + 1 < len(datasets):
            time.sleep(settings.finmind_request_interval_seconds)
    merged = merge_corporate_actions(symbol, incoming)
    sync_path.write_text(
        json.dumps(
            {"symbol": symbol, "checked_through": as_of.isoformat()},
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    return merged


def apply_corporate_actions(candles: list[dict], actions: list[dict]) -> list[dict]:
    """以 FinMind 公布值覆蓋公司行動日參考價與實際漲跌停價。"""
    action_by_date: dict[str, dict] = {}
    for action in actions:
        current = action_by_date.setdefault(action["date"], {})
        for field in ("reference_price", "limit_up", "limit_down"):
            if action.get(field) is not None:
                current[field] = action[field]
        current.setdefault("sources", []).append(action["source"])

    enriched: list[dict] = []
    for candle in candles:
        row = dict(candle)
        action = action_by_date.get(row["date"])
        if action:
            row.update({key: value for key, value in action.items() if key != "sources"})
            row["reference_price_source"] = "+".join(sorted(set(action["sources"])))
        enriched.append(row)
    return enriched
