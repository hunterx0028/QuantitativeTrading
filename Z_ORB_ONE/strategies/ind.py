"""IND：v40 完成分K確認、大盤一次判定、確認後即時報價入場。

本模組不連線、不送單；背景資料由共用流程提供，交易沿用主程式。
"""
from dataclasses import dataclass
from datetime import timedelta
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
    market_slope_start_time: tuple = (9, 10)
    market_max_normalized_slope: float = math.tan(math.radians(-5))
    market_max_missing_slope_bars: int = 2
    stock_max_drop_percent: float = 0.5
    slope_cutoff_time: tuple = (9, 5)
    max_normalized_slope: float = math.tan(math.radians(40))
    max_missing_slope_bars: int = 2

    def validate(self):
        for clock in (self.market_check_time, self.stock_scan_start_time, self.entry_deadline,
                      self.slope_cutoff_time, self.market_slope_start_time):
            if (not isinstance(clock, tuple) or len(clock) != 2
                    or any(type(part) is not int for part in clock)
                    or not 0 <= clock[0] < 24 or not 0 <= clock[1] < 60):
                raise ValueError('IND 時間必須為合法 (時, 分)')
        if not ((9, 0) <= self.market_check_time <= self.entry_deadline
                and (9, 0) <= self.stock_scan_start_time <= self.entry_deadline):
            raise ValueError('IND 大盤檢查及個股起算須在09:00至入場截止之間')
        if not (9, 0) <= self.market_slope_start_time < self.market_check_time:
            raise ValueError('IND 大盤斜率起算須在09:00至大盤檢查時間之前')
        if (type(self.market_max_normalized_slope) not in (int, float)
                or not math.isfinite(self.market_max_normalized_slope)):
            raise ValueError('IND 大盤斜率門檻須為有限數值（%／分鐘），可為正、零或負')
        if (type(self.market_max_missing_slope_bars) is not int
                or self.market_max_missing_slope_bars < 0):
            raise ValueError('IND 大盤斜率缺棒容許數須為非負整數')
        for value in (self.market_drop_percent, self.stock_max_drop_percent):
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value < 100:
                raise ValueError('IND 跌幅門檻必須介於0與100')
        if not (9, 0) < self.slope_cutoff_time < self.stock_scan_start_time:
            raise ValueError('IND 斜率截止須晚於09:00，個股起算須晚於斜率截止分鐘')
        if (type(self.max_normalized_slope) not in (int, float)
                or not math.isfinite(self.max_normalized_slope) or self.max_normalized_slope < 0):
            raise ValueError('IND 正規化斜率門檻須為有限非負數（%／分鐘）')
        if type(self.max_missing_slope_bars) is not int or self.max_missing_slope_bars < 0:
            raise ValueError('IND 斜率缺棒容許數須為非負整數')

    def calculate_early_close_slope(self, session, previous_close, *, start_time=(9, 0), cutoff_time=None, max_missing=None):
        """與v40相同：close對實際分鐘回歸／昨收×100，不補缺棒。"""
        previous_close = positive(previous_close)
        if previous_close is None:
            return None
        cutoff_time = self.slope_cutoff_time if cutoff_time is None else cutoff_time
        max_missing = self.max_missing_slope_bars if max_missing is None else max_missing
        samples = []
        seen = set()
        expected = cutoff_time[0] * 60 + cutoff_time[1] - (start_time[0] * 60 + start_time[1]) + 1
        for bar in session:
            dt = bar['dt']
            if not start_time <= (dt.hour, dt.minute) <= cutoff_time:
                continue
            minute = dt.hour * 60 + dt.minute
            if dt.second or dt.microsecond or minute in seen:
                return None
            seen.add(minute)
            close = positive(bar.get('close'))
            if close is not None:
                samples.append((minute, close))
        if len(samples) < 2 or expected - len(samples) > max_missing:
            return None
        mean_x = math.fsum(x for x, _ in samples) / len(samples)
        mean_y = math.fsum(y for _, y in samples) / len(samples)
        slope = (math.fsum((x - mean_x) * (y - mean_y) for x, y in samples)
                 / math.fsum((x - mean_x) ** 2 for x, _ in samples))
        normalized = slope / previous_close * 100
        return normalized if math.isfinite(normalized) else None

    def market_decision(self, bars, previous_close, now):
        if positive(previous_close) is None:
            raise ValueError('IND 大盤昨收缺失或無效')
        session = completed_session(bars, now)
        for bar in session:
            if (bar['dt'].hour, bar['dt'].minute) >= self.market_check_time:
                change = change_percent(bar['close'], previous_close)
                slope = self.calculate_early_close_slope(
                    session, previous_close, start_time=self.market_slope_start_time,
                    cutoff_time=self.market_check_time, max_missing=self.market_max_missing_slope_bars)
                return dict(dt=bar['dt'], change=float(change), normalized_close_slope=slope,
                            passed=(change <= -Decimal(str(self.market_drop_percent))
                                    and slope is not None and slope < self.market_max_normalized_slope))
        return None

    def find_candidate(self, bars, previous_close, market, now, limit_up, limit_down):
        """回傳 (候選欄位, 當日淘汰)。遞補只使用已完成且 average 有效的棒。"""
        if market is None:
            return None, False
        if not market['passed'] or positive(previous_close) is None:
            return None, True
        session = completed_session(bars, now)
        slope_ready = now.replace(hour=self.slope_cutoff_time[0],
                                 minute=self.slope_cutoff_time[1], second=0, microsecond=0) + timedelta(minutes=1)
        if now < slope_ready:
            return None, False  # 截止棒尚未完成，等待而非淘汰。
        early_slope = self.calculate_early_close_slope(session, previous_close)
        if early_slope is None or early_slope > self.max_normalized_slope:
            return None, True  # 僅淘汰此股票當日IND資格。
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
                    'ind_early_close_slope_percent': early_slope,
                }, False
            previous = bar
        return None, (now.hour, now.minute) > self.entry_deadline


@dataclass
class IndRuntime:
    decision: dict | None = None

    def reset(self):
        self.decision = None

    def confirm_market(self, config, bars, previous_close, now):
        if self.decision is None:
            self.decision = config.market_decision(bars, previous_close, now)
        return self.decision
