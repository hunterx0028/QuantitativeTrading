from __future__ import annotations

import argparse
import json
import time
import math
from datetime import date, timedelta, datetime, timezone

from .config import Settings
from .finmind import update_corporate_actions, clear_query_cache
from .market_data import fetch_candles, load_sdk
from .paths import CONFIG_PATH, PREDICTIONS_DIR, DATA_CHECKS_DIR, ensure_runtime_dirs
from .storage import candle_path, merge_candles, read_jsonl, write_jsonl
from .input_schema import INDEX_SYMBOLS
from .market_indices import index_path
from .universe import load_selected_stocks, write_universe_snapshot
from .provenance import atomic_text, fingerprint
from .trading_calendar import TradingCalendar, suspension_reason


def valid_candle(row):
    try:
        o, h, l, c = (float(row[name]) for name in ("open", "high", "low", "close"))
        return (all(math.isfinite(value) and value > 0 for value in (o, h, l, c))
                and l <= min(o, c) <= max(o, c) <= h)
    except (KeyError, TypeError, ValueError):
        return False


def refresh_start(existing, as_of, earliest_date, refresh_days=7):
    dates = [date.fromisoformat(row["date"]) for row in existing if row["date"] <= as_of.isoformat()]
    if not dates:
        return date.fromisoformat(earliest_date)
    return max(date.fromisoformat(earliest_date), min(max(dates), as_of - timedelta(days=refresh_days - 1)))


def assert_after_close(as_of):
    now = datetime.now(timezone(timedelta(hours=8)))
    if as_of > now.date() or (as_of == now.date() and (now.hour, now.minute) < (15, 30)):
        raise ValueError("當日完整日K驗收須於台北時間 15:30 後執行，且不可指定未來日期")


def recent_gaps(symbol, rows, as_of, refresh_days, calendar):
    available = {row["date"] for row in rows if row["date"] <= as_of.isoformat()}
    if not available:
        return []
    day = max(date.fromisoformat(min(available)), as_of - timedelta(days=refresh_days - 1))
    gaps = []
    while day <= as_of:
        key = day.isoformat()
        if calendar.is_session(day) and key not in available and not suspension_reason(symbol, key):
            gaps.append(key)
        day += timedelta(days=1)
    return gaps


def missing_prediction_candles(as_of: date) -> dict[str, list[date]]:
    """Find due prediction dates missing local actual candles, including non-signals.

    An evaluation file can be partial, so its mere existence is not evidence
    that every predicted stock has actual data. Inspect candle dates instead.
    """
    requested: dict[str, set[date]] = {}
    paths = list(PREDICTIONS_DIR.glob("*.json"))
    for path in sorted(paths):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for prediction in payload.get("predictions", []):
            target = date.fromisoformat(prediction["prediction_date"])
            if target <= as_of:
                requested.setdefault(prediction["symbol"], set()).add(target)
    missing = {}
    for symbol, targets in requested.items():
        available = {date.fromisoformat(row["date"]) for row in read_jsonl(candle_path(symbol))}
        dates = sorted(day for day in targets - available if not suspension_reason(symbol, day.isoformat()))
        if dates:
            missing[symbol] = dates
    return missing


from .runtime_lock import locked


def update_indices(rest_stock, as_of, settings, refresh_days, calendar):
    audits = []
    for symbol in INDEX_SYMBOLS:
        path = index_path(symbol)
        existing = read_jsonl(path)
        start = refresh_start(existing, as_of, settings.earliest_date, refresh_days)
        # Even --refresh-days 1 must refresh the previous OHLC denominator.
        start = min(start, date.fromisoformat(calendar.previous_session(as_of)))
        incoming = fetch_candles(rest_stock, symbol, start, as_of, settings, index_ohlc=True)
        rejected = list(getattr(incoming, "rejected", []))
        rejected.extend({"date": row.get("date"), "reason": "指數 OHLC 驗收失敗"}
                        for row in incoming if not valid_candle(row))
        valid = [row for row in incoming if valid_candle(row)]
        merged = {row["date"]: row for row in existing}
        merged.update({row["date"]: row for row in valid})
        rows = [merged[day] for day in sorted(merged)]
        write_jsonl(path, rows)
        fresh_dates = {row["date"] for row in valid}
        required = {as_of.isoformat(), calendar.previous_session(as_of)}
        gaps = recent_gaps(symbol, rows, as_of, refresh_days, calendar)
        complete = required <= fresh_dates and not gaps and not any(row.get("date") for row in rejected)
        audits.append({"symbol": symbol, "status": "complete" if complete else "incomplete",
                       "missing_fresh_dates": sorted(required - fresh_dates),
                       "missing_session_dates": gaps, "rejected_records": rejected,
                       "data_version": fingerprint(rows)})
        print(f"[指數驗收] {symbol}: {audits[-1]['status']} total_candles={len(rows)}")
        time.sleep(settings.request_interval_seconds)
    return audits


@locked
def main() -> None:
    parser = argparse.ArgumentParser(description="更新每日股票清單及日K快取")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="YYYY-MM-DD")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--refresh-days", type=int, default=7, help="重抓最近日曆日，預設 7")
    parser.add_argument("--require-complete", action="store_true", help="驗收失敗時以錯誤狀態結束")
    args = parser.parse_args()
    clear_query_cache()
    as_of = date.fromisoformat(args.as_of)
    calendar = TradingCalendar()
    calendar.require_session(as_of)
    if args.refresh_days <= 0:
        parser.error("refresh-days 必須大於 0")
    if args.require_complete:
        assert_after_close(as_of)
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    ensure_runtime_dirs()
    stocks = load_selected_stocks()
    snapshot = write_universe_snapshot(stocks, as_of)
    print(f"股票清單快照: {snapshot} ({len(stocks)}支)")

    missing = missing_prediction_candles(as_of)
    active_symbols = {stock.symbol for stock in stocks}
    # The snapshot above contains only today's selected stocks. Supplementary
    # downloads never expand the prediction universe or modify stock_data.py.
    symbols = sorted(active_symbols | missing.keys())
    audit_rows = []
    download_rejections = {}

    sdk = load_sdk(CONFIG_PATH if args.config == str(CONFIG_PATH) else args.config)
    rest_stock = sdk.rest_client.stock
    index_audits = update_indices(rest_stock, as_of, settings, args.refresh_days, calendar)
    for index, symbol in enumerate(symbols):
        existing = read_jsonl(candle_path(symbol))
        from_date = refresh_start(existing, as_of, settings.earliest_date, args.refresh_days)
        if symbol in missing:
            from_date = min(from_date, missing[symbol][0])
            dates_text = ", ".join(day.isoformat() for day in missing[symbol])
            print(f"[補抓驗證行情] {symbol}: {dates_text}")
        to_date = as_of if symbol in active_symbols else missing[symbol][-1]
        incoming = fetch_candles(rest_stock, symbol, from_date, to_date, settings)
        rejected = list(getattr(incoming, "rejected", []))
        rejected.extend({"date": row.get("date"), "reason": "OHLC 驗收失敗"}
                        for row in incoming if not valid_candle(row))
        download_rejections[symbol] = rejected
        invalid_dates = [row["date"] for row in rejected if row.get("date")]
        incoming = [row for row in incoming if valid_candle(row)]
        merged = merge_candles(symbol, incoming)
        actions = update_corporate_actions(symbol, to_date, settings, refresh_days=args.refresh_days)
        available = {row["date"] for row in merged}
        if symbol in active_symbols:
            fresh = next((row for row in incoming if row["date"] == as_of.isoformat()), None)
            suspension = suspension_reason(symbol, as_of.isoformat())
            status = "complete" if fresh is not None else "suspended" if suspension else "missing_or_not_refreshed"
            gaps = recent_gaps(symbol, merged, as_of, args.refresh_days, calendar)
            if gaps and status == "complete":
                status = "history_gap"
            audit_rows.append({"symbol": symbol, "status": status, "suspension": suspension, "invalid_dates": invalid_dates,
                               "rejected_records": rejected,
                               "missing_session_dates": gaps,
                               "last_date": max(available) if available else None,
                               "data_version": fingerprint(merged)})
            print(f"[驗收] {symbol}: {status}")
        remaining = [day.isoformat() for day in missing.get(symbol, []) if day.isoformat() not in available]
        if remaining:
            print(f"[WARN] {symbol}: 補抓後仍缺驗證日K {', '.join(remaining)}，待資料可取得後重試")
        print(
            f"{symbol}: new_candles={len(incoming)} total_candles={len(merged)} "
            f"corporate_actions={len(actions)}"
        )
        if index + 1 < len(symbols):
            time.sleep(settings.request_interval_seconds)
    # 刻意不呼叫 sdk.logout()。
    blocking_download_rejections = {
        symbol: [row for row in rows if row.get("date")]
        for symbol, rows in download_rejections.items()
    }
    complete = all(row["status"] == "complete" for row in index_audits) and bool(active_symbols) and not any(blocking_download_rejections.values()) and all(row["status"] in ("complete", "suspended") and not row["invalid_dates"]
                                            and not row["missing_session_dates"]
                                            for row in audit_rows)
    report = {"as_of": as_of.isoformat(), "checked_at": datetime.now().astimezone().isoformat(),
              "complete": complete, "refresh_days": args.refresh_days, "stocks": audit_rows,
              "download_rejections": download_rejections, "indices": index_audits}
    report_path = DATA_CHECKS_DIR / f"{as_of.isoformat()}.json"
    atomic_text(report_path, json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(f"資料驗收報告: {report_path}")
    if args.require_complete and not complete:
        raise RuntimeError("行情驗收未通過，停止後續特徵產生與續訓；缺資料不自動當作停牌，補齊後重跑")


if __name__ == "__main__":
    main()
