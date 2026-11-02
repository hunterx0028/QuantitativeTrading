"""離線報表共用的歷史快取讀取介面。"""
from __future__ import annotations

import json
import re
from datetime import datetime
from functools import lru_cache


@lru_cache(maxsize=1)
def load_cache(path: str) -> dict:
    with open(path, encoding="utf-8-sig") as cache_file:
        return json.load(cache_file)


class CacheStock:
    """提供報表所需的 historical.candles 介面，全程只讀本機 JSON。"""

    is_history_cache = True

    def __init__(self, path: str):
        self.data = load_cache(path)
        self.historical = self

    def _rows(self, group: str, symbol: str) -> list[dict]:
        rows = []
        for label, values in self.data.get(group, {}).items():
            # 股票標籤如 世界:5347.TWO；指數 key 如 TWSE:24。
            code = label.rsplit(":", 1)[-1].split(".", 1)[0]
            if label == symbol or code == symbol:
                rows.extend(values)
        return rows

    def _minute_rows(self, symbol: str) -> list[dict]:
        if re.fullmatch(r"\d{4}", symbol):
            return self._rows("minute_raw_by_symbol", symbol)
        from Z_ORB_ONE.stock_data import market_previous_close_indices

        keys = {symbol}
        keys.update(
            key for key, meta in market_previous_close_indices.items()
            if str(meta.get("symbol", "")).upper() == symbol.upper()
        )
        if symbol.upper() == "IX0001":
            keys.add("TWSE:MARKET")
        elif symbol.upper() == "IX0043":
            keys.add("TPEX:MARKET")
        rows = []
        for key in keys:
            rows.extend(self.data.get("index_minute_raw_by_key", {}).get(key, []))
        return rows

    def candles(self, **params) -> dict:
        symbol = params["symbol"]
        start, end = params["from"], params["to"]
        minutes = [
            row for row in self._minute_rows(symbol)
            if start <= row["date"][:10] <= end
        ]
        if params.get("timeframe") == "1":
            rows = minutes
        else:
            # 指數沒有日 K 快取時，以當日所有分 K 聚合 OHLC 與成交量。
            by_day = {}
            for row in sorted(minutes, key=lambda item: item["date"]):
                dt = datetime.fromisoformat(row["date"])
                if not ("09:00" <= dt.strftime("%H:%M") <= "13:30"):
                    continue
                day = dt.strftime("%Y-%m-%d")
                if day not in by_day:
                    by_day[day] = dict(row, date=day, volume=0)
                candle = by_day[day]
                candle["high"] = max(candle["high"], row["high"])
                candle["low"] = min(candle["low"], row["low"])
                candle["close"] = row["close"]
                candle["volume"] += int(row.get("volume", 0) or 0)
            for row in self._rows("day_candles_by_symbol", symbol):
                by_day[row["date"][:10]] = row
            rows = list(by_day.values())
        return {"data": [row for row in rows if start <= row["date"][:10] <= end]}
