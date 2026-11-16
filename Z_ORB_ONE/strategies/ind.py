"""IND：v40 完成分K確認、大盤一次判定、確認後即時報價入場。

本模組不連線、不送單；背景資料由共用流程提供，交易沿用主程式。
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import math


def positive(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def change_percent(price, previous_close):
    return (Decimal(str(price)) / Decimal(str(previous_close)) - 1) * 100


def completed_session(bars, now):
    cutoff = now.replace(second=0, microsecond=0)
    session = cutoff.replace(hour=9, minute=0)
    completed = [bar for bar in bars if session <= bar['dt'] < cutoff]
    times = [bar['dt'] for bar in completed]
    if times != sorted(set(times)) or any(dt.second or dt.microsecond for dt in times):
        raise ValueError('IND 分K時間重複、非整分鐘或順序異常，等待下一輪')
    return completed


@dataclass(frozen=True)
class IndConfig:
    market_check_time: tuple = (9, 15)
    stock_scan_start_time: tuple = (9, 16)
    entry_deadline: tuple = (12, 0)
    market_drop_percent: float = 1.5
    stock_max_drop_percent: float = 0.5
    enable_market_entry_recheck: bool = False
    market_quote_max_age_seconds: float = 15.0

    def validate(self):
        for clock in (self.market_check_time, self.stock_scan_start_time, self.entry_deadline):
            if (not isinstance(clock, tuple) or len(clock) != 2
                    or any(type(part) is not int for part in clock)
                    or not 0 <= clock[0] < 24 or not 0 <= clock[1] < 60):
                raise ValueError('IND 時間必須為合法 (時, 分)')
        if not ((9, 0) <= self.market_check_time <= self.entry_deadline
                and (9, 0) <= self.stock_scan_start_time <= self.entry_deadline):
            raise ValueError('IND 大盤檢查及個股起算須在09:00至入場截止之間')
        for value in (self.market_drop_percent, self.stock_max_drop_percent):
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value < 100:
                raise ValueError('IND 跌幅門檻必須介於0與100')
        if type(self.enable_market_entry_recheck) is not bool:
            raise ValueError('IND 大盤入場重查開關必須為布林值')
        if (type(self.market_quote_max_age_seconds) not in (int, float)
                or not math.isfinite(self.market_quote_max_age_seconds)
                or self.market_quote_max_age_seconds <= 0):
            raise ValueError('IND 大盤報價有效秒數必須為有限正數')

    def market_decision(self, bars, previous_close, now):
        if positive(previous_close) is None:
            raise ValueError('IND 大盤昨收缺失或無效')
        for bar in completed_session(bars, now):
            if (bar['dt'].hour, bar['dt'].minute) >= self.market_check_time:
                change = change_percent(bar['close'], previous_close)
                return dict(dt=bar['dt'], change=float(change),
                            passed=change <= -Decimal(str(self.market_drop_percent)))
        return None

    def find_candidate(self, bars, previous_close, market, now, limit_up, limit_down):
        """回傳 (候選欄位, 當日淘汰)。遞補只使用已完成且 average 有效的棒。"""
        if market is None:
            return None, False
        if not market['passed'] or positive(previous_close) is None:
            return None, True
        session = completed_session(bars, now)
        selection = next((bar for bar in session if bar['dt'] >= market['dt']
                          and positive(bar.get('average')) is not None), None)
        if selection is None:
            return None, False
        change = change_percent(selection['close'], previous_close)
        if change <= -Decimal(str(self.stock_max_drop_percent)):
            return None, True
        if any(bar['high'] >= limit_up or bar['low'] <= limit_down for bar in session):
            return None, True
        previous = None
        for bar in session:
            if ((bar['dt'].hour, bar['dt'].minute) < self.stock_scan_start_time
                    or positive(bar.get('average')) is None):
                continue
            if (previous is not None and previous['close'] >= previous['average']
                    and bar['close'] < bar['average'] and bar['dt'] >= selection['dt']):
                entry_time = bar['dt'] + timedelta(minutes=1)
                if (entry_time.hour, entry_time.minute) > self.entry_deadline:
                    return None, True
                return {
                    'ind_market_time': market['dt'].isoformat(),
                    'ind_market_change': market['change'],
                    'ind_selection_time': selection['dt'].isoformat(),
                    'ind_stock_change': float(change),
                    'ind_previous_bar_time': previous['dt'].isoformat(),
                    'ind_signal_time': bar['dt'].isoformat(),
                    'ind_signal_close': bar['close'], 'ind_signal_average': bar['average'],
                    'ind_entry_time': entry_time.isoformat(),
                }, False
            previous = bar
        return None, (now.hour, now.minute) > self.entry_deadline


@dataclass
class IndRuntime:
    decision: dict | None = None
    halted: bool = False
    quote: dict | None = None

    def reset(self):
        self.decision = None
        self.halted = False
        self.quote = None

    def confirm_market(self, config, bars, previous_close, now):
        if self.decision is None:
            self.decision = config.market_decision(bars, previous_close, now)
        return self.decision

    def entry_status(self, config, previous_close, now, monotonic):
        """僅真正競爭入場時檢查反彈；資料錯誤不觸發永久封鎖。"""
        if self.halted or not self.decision or not self.decision['passed']:
            return 'blocked'
        if not config.enable_market_entry_recheck:
            return 'allowed'
        quote = self.quote
        if (quote is None or positive(previous_close) is None
                or positive(quote.get('price')) is None):
            return 'unavailable'
        request_age = monotonic - quote['requested_at']
        market_age = (now - quote['market_time']).total_seconds()
        if not (0 <= request_age <= config.market_quote_max_age_seconds
                and 0 <= market_age <= config.market_quote_max_age_seconds):
            return 'unavailable'
        if change_percent(quote['price'], previous_close) > -Decimal(str(config.market_drop_percent)):
            self.halted = True
            return 'rebound'
        return 'allowed'


def parse_market_quote(response, expected_symbol, now, requested_at):
    """驗證標的與行情時間，拒用昨日或缺少時間的指數報價。"""
    if not isinstance(response, dict) or response.get('symbol') != expected_symbol:
        raise ValueError('IND 指數報價標的不符或格式錯誤')
    price = positive(response.get('lastPrice'))
    timestamp = response.get('lastUpdated')
    if price is None or timestamp is None or isinstance(timestamp, bool):
        raise ValueError('IND 指數報價缺少有效 lastPrice/lastUpdated')
    try:
        numeric = float(timestamp)
        if not math.isfinite(numeric) or numeric <= 0:
            raise ValueError('invalid timestamp')
        while numeric > 100_000_000_000:
            numeric /= 1000
        market_time = datetime.fromtimestamp(numeric, tz=now.tzinfo)
    except (TypeError, ValueError, OverflowError, OSError):
        market_time = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
        if market_time.tzinfo is None:
            market_time = market_time.replace(tzinfo=now.tzinfo)
        else:
            market_time = market_time.astimezone(now.tzinfo)
    if market_time.date() != now.date():
        raise ValueError('IND 指數報價不是當日資料')
    return dict(price=price, market_time=market_time, requested_at=requested_at)
