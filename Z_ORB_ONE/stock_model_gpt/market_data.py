from __future__ import annotations

import time
import math
from configparser import ConfigParser
from datetime import date, timedelta
from pathlib import Path

from esun_marketdata import EsunMarketdata

from .config import Settings
from .paths import CONFIG_PATH
from .storage import parse_api_date


class CandleBatch(list):
    """Valid candles plus rejected API records, retained for strict ingestion audits."""
    def __init__(self, rows, rejected):
        super().__init__(rows)
        self.rejected = rejected


def load_sdk(config_path: Path = CONFIG_PATH):
    """登入一次並回傳 SDK；依需求不呼叫 logout。"""
    config_path = Path(config_path).resolve()
    config = ConfigParser()
    config.read(config_path, encoding="utf-8")
    cert_path = config.get("Cert", "Path", fallback="").strip()
    if cert_path and not Path(cert_path).is_absolute():
        config.set("Cert", "Path", str((config_path.parent / cert_path).resolve()))
    sdk = EsunMarketdata(config)
    sdk.login()
    return sdk


def _normalise_candle(row: dict) -> dict | None:
    required_prices = ("open", "high", "low", "close")
    if any(row.get(field) is None for field in required_prices):
        return None
    try:
        open_price = float(row["open"])
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
    except (TypeError, ValueError):
        return None
    if (not all(math.isfinite(value) and value > 0 for value in (open_price, high, low, close))
            or not low <= min(open_price, close) <= max(open_price, close) <= high):
        return None
    change = float(row["change"]) if row.get("change") is not None else None
    reference = close - change if change is not None else row.get("reference_price")
    reference = float(reference) if reference is not None else None
    volume = int(row.get("volume", 0) or 0)
    factor = float(row.get("adjustment_factor", 1.0))
    if (volume < 0 or not math.isfinite(factor) or factor <= 0
            or (change is not None and not math.isfinite(change))
            or (reference is not None and (not math.isfinite(reference) or reference <= 0))):
        raise ValueError("成交量、調整因子、漲跌價或參考價無效")
    return {
        "date": parse_api_date(row["date"]).isoformat(),
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "change": change,
        # 原始價格永遠保留；調整因子可由後續的還原價格來源覆寫。
        "adjustment_factor": factor,
        "reference_price": reference,
        "reference_price_source": "esun_change" if change is not None else None,
    }


def fetch_candles(
    rest_stock,
    symbol: str,
    from_date: date,
    to_date: date,
    settings: Settings,
    *, index_ohlc: bool = False,
) -> list[dict]:
    """分段讀取可取得的完整日K範圍；呼叫者負責建立並登入 SDK。"""
    if from_date > to_date:
        return CandleBatch([], [])
    by_date: dict[str, dict] = {}
    rejected = []
    cursor = from_date
    while cursor <= to_date:
        chunk_to = min(
            to_date,
            cursor + timedelta(days=settings.request_chunk_calendar_days - 1),
        )
        response = rest_stock.historical.candles(
            **{
                "symbol": symbol,
                "from": cursor.isoformat(),
                "to": chunk_to.isoformat(),
                "timeframe": "D",
                "fields": "open,high,low,close" if index_ohlc else "open,high,low,close,volume,change",
            }
        )
        rows = response.get("data") if isinstance(response, dict) else None
        if not isinstance(rows, list):
            rejected.append({"date": None, "reason": "API 回應缺少有效 data 陣列",
                             "from": cursor.isoformat(), "to": chunk_to.isoformat()})
            rows = []
        for raw in rows:
            try:
                raw_date = parse_api_date(raw["date"]).isoformat()
                if not cursor.isoformat() <= raw_date <= chunk_to.isoformat():
                    continue
                candle = _normalise_candle(raw)
                if candle is None:
                    raise ValueError("OHLC 缺值、非有限正數或高低範圍不合理")
                by_date[candle["date"]] = candle
            except (KeyError, TypeError, ValueError, OverflowError, AttributeError) as exc:
                rejected.append({"date": str(raw.get("date")) if isinstance(raw, dict) else None,
                                 "reason": str(exc), "from": cursor.isoformat(), "to": chunk_to.isoformat()})
        cursor = chunk_to + timedelta(days=1)
        if cursor <= to_date:
            time.sleep(settings.request_interval_seconds)
    if rejected:
        print(f"[WARN] {symbol}: 拒絕 {len(rejected)} 筆行情／回應，已保留原因供驗收")
    return CandleBatch([by_date[key] for key in sorted(by_date)], rejected)
