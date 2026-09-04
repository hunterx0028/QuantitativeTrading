from __future__ import annotations

import time
from configparser import ConfigParser
from datetime import date, timedelta
from pathlib import Path

from esun_marketdata import EsunMarketdata

from .config import Settings
from .paths import CONFIG_PATH
from .storage import parse_api_date


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
    if min(open_price, high, low, close) <= 0 or high < low:
        return None
    change = float(row["change"]) if row.get("change") is not None else None
    return {
        "date": parse_api_date(row["date"]).isoformat(),
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": int(row.get("volume", 0) or 0),
        "change": change,
        # 原始價格永遠保留；調整因子可由後續的還原價格來源覆寫。
        "adjustment_factor": float(row.get("adjustment_factor", 1.0)),
        "reference_price": close - change if change is not None else row.get("reference_price"),
        "reference_price_source": "esun_change" if change is not None else None,
    }


def fetch_candles(
    rest_stock,
    symbol: str,
    from_date: date,
    to_date: date,
    settings: Settings,
) -> list[dict]:
    """分段讀取可取得的完整日K範圍；呼叫者負責建立並登入 SDK。"""
    if from_date > to_date:
        return []
    by_date: dict[str, dict] = {}
    skipped_invalid = 0
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
                "fields": "open,high,low,close,volume,change",
            }
        )
        for raw in response.get("data", []) or []:
            candle = _normalise_candle(raw)
            if candle is None:
                skipped_invalid += 1
                continue
            if cursor.isoformat() <= candle["date"] <= chunk_to.isoformat():
                by_date[candle["date"]] = candle
        cursor = chunk_to + timedelta(days=1)
        if cursor <= to_date:
            time.sleep(settings.request_interval_seconds)
    if skipped_invalid:
        print(f"[WARN] {symbol}: 略過 {skipped_invalid} 筆 OHLC 空值或無效的日K")
    return [by_date[key] for key in sorted(by_date)]
