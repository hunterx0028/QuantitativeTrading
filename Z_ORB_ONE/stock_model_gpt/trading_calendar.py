"""TWSE annual calendar with explicit, sourced exceptional closures/suspensions."""
import argparse
import json
import time
from calendar import monthrange
from functools import lru_cache
from datetime import date, datetime, timedelta, timezone
from urllib.request import Request, urlopen

from .paths import DATA_DIR
from .provenance import atomic_text

CALENDAR_DIR = DATA_DIR / "calendar"


def parse_schedule(year, payload):
    if str(payload.get("stat", "")).lower() != "ok" or not payload.get("data"):
        raise ValueError(f"{year} 交易日曆來源無有效資料")
    exceptions = {}
    for row in payload["data"]:
        day = date.fromisoformat(row[0])
        if day.year != year:
            raise ValueError("日曆回應年份不符")
        name = row[1]
        exceptions[day.isoformat()] = "開始交易日" in name or "最後交易日" in name
    days = {}
    current = date(year, 1, 1)
    while current.year == year:
        days[current.isoformat()] = exceptions.get(current.isoformat(), current.weekday() < 5)
        current += timedelta(days=1)
    return days


def fetch_calendar_json(url):
    with urlopen(Request(url, headers={"User-Agent": "stock-model-gpt/1.0"}), timeout=30) as response:
        return json.load(response)


def parse_index_month(year, month, payload):
    if str(payload.get("stat", "")).lower() != "ok" or not payload.get("data"):
        raise ValueError(f"{year}-{month:02} 官方歷史指數無交易日資料，不可猜測休市日期")
    sessions = {date(year, month, day).isoformat(): False for day in range(1, monthrange(year, month)[1] + 1)}
    for row in payload["data"]:
        roc_year, row_month, row_day = map(int, row[0].strip().split("/"))
        day = date(roc_year + 1911, row_month, row_day)
        if (day.year, day.month) != (year, month):
            raise ValueError("歷史指數回應年月不符")
        sessions[day.isoformat()] = True
    return sessions


def sync_year(year):
    today = datetime.now(timezone(timedelta(hours=8))).date()
    sources = []
    if year >= today.year:
        # queryYear is a response field; requests must use date=YYYY0101.
        url = f"https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule?response=json&date={year}0101"
        sessions = parse_schedule(year, fetch_calendar_json(url))
        sources.append(url)
    else:
        sessions = {}
    # Completed months use actual market-index sessions, including unexpected closures.
    last_month = 12 if year < today.year else today.month - 1 if year == today.year else 0
    for month in range(1, last_month + 1):
        url = f"https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date={year}{month:02}01"
        sessions.update(parse_index_month(year, month, fetch_calendar_json(url)))
        sources.append(url)
        time.sleep(0.3)
    result = {"year": year, "sources": sources, "updated_at": datetime.now(timezone.utc).isoformat(),
              "sessions": sessions}
    atomic_text(CALENDAR_DIR / f"{year}.json", json.dumps(result, ensure_ascii=False, indent=2) + "\n")


class TradingCalendar:
    def __init__(self):
        self.years = {}
        override_path = CALENDAR_DIR / "overrides.json"
        self.overrides = json.loads(override_path.read_text(encoding="utf-8")) if override_path.exists() else {}

    def is_session(self, day):
        day = date.fromisoformat(day) if isinstance(day, str) else day
        override = self.overrides.get(day.isoformat())
        if override is not None:
            if type(override.get("open")) is not bool or not override.get("reason"):
                raise ValueError(f"交易日例外紀錄無效: {day}")
            return override["open"]
        if day.year not in self.years:
            path = CALENDAR_DIR / f"{day.year}.json"
            if not path.exists():
                raise RuntimeError(f"缺少 {day.year} 交易日曆；請執行 python -m Z_ORB_ONE.stock_model_gpt.trading_calendar --start-year {day.year} --end-year {day.year}")
            self.years[day.year] = json.loads(path.read_text(encoding="utf-8"))["sessions"]
        value = self.years[day.year].get(day.isoformat())
        if type(value) is not bool:
            raise ValueError(f"交易日曆日期缺漏或無效: {day}")
        return value

    def require_session(self, day):
        if not self.is_session(day):
            raise ValueError(f"{day} 是休市日，不可更新、訓練或預測")

    @lru_cache(maxsize=20000)
    def next_session(self, day):
        current = date.fromisoformat(day) if isinstance(day, str) else day
        for _ in range(40):
            current += timedelta(days=1)
            if self.is_session(current):
                return current.isoformat()
        raise ValueError("40 日內找不到下一交易日，請檢查日曆")

    def previous_session(self, day):
        current = date.fromisoformat(day) if isinstance(day, str) else day
        for _ in range(40):
            current -= timedelta(days=1)
            if self.is_session(current):
                return current.isoformat()
        raise ValueError("40 日內找不到前一交易日，請檢查日曆")


@lru_cache(maxsize=1)
def shared_calendar():
    return TradingCalendar()


def assert_sequence_dates(dates):
    calendar = shared_calendar()
    for day in dates:
        calendar.require_session(day)
    for left, right in zip(dates, dates[1:]):
        if calendar.next_session(left) != right:
            raise ValueError(f"交易日序列缺漏：{left} 下一交易日應為 {calendar.next_session(left)}，不是 {right}")


def suspension_reason(symbol, day):
    path = CALENDAR_DIR / "suspensions.json"
    records = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    return records.get(f"{symbol}|{day}")


from .runtime_lock import locked


@locked
def main():
    parser = argparse.ArgumentParser(description="同步 TWSE 年度交易日曆，或記錄已確認的臨時休市／停牌")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--date")
    parser.add_argument("--closed", action="store_true")
    parser.add_argument("--open", action="store_true")
    parser.add_argument("--suspend-symbol")
    parser.add_argument("--reason", help="公告來源與原因，例外紀錄必填")
    args = parser.parse_args()
    if args.date:
        day = date.fromisoformat(args.date).isoformat()
        if not args.reason or sum((args.closed, args.open, bool(args.suspend_symbol))) != 1:
            parser.error("日期例外需選 closed、open 或 suspend-symbol 其中一項，並提供 reason")
        path = CALENDAR_DIR / ("suspensions.json" if args.suspend_symbol else "overrides.json")
        records = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        key = f"{args.suspend_symbol}|{day}" if args.suspend_symbol else day
        records[key] = {"reason": args.reason, "open": args.open, "recorded_at": datetime.now(timezone.utc).isoformat()}
        atomic_text(path, json.dumps(records, ensure_ascii=False, indent=2) + "\n")
    else:
        if args.start_year is None or args.end_year is None or not 2001 <= args.start_year <= args.end_year:
            parser.error("請指定有效的 start-year、end-year（2001 年起）")
        for year in range(args.start_year, args.end_year + 1):
            sync_year(year)
            print(f"交易日曆已更新: {year}")
    shared_calendar.cache_clear()
    TradingCalendar.next_session.cache_clear()


if __name__ == "__main__":
    main()
