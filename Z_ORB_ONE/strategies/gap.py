"""GAP: v36 昨日收漲、開盤跳空後衝高回落放空；單根K棒內完成訊號確認，下一根 open 入場。

前置資料沿用 vol_history_by_symbol，只驗證前一交易日成交量、昨收及來源日期，不要求5日均量。
回測的「昨收嚴格高於前一有資料交易日收盤」改用 stock_data 的 up_streak_days（連漲天數）
>=1 判斷。**重要依賴**：這個判斷成立的前提是 up_streak_days 代表「以T-1收盤為結尾、
連續上漲收盤的天數」（即 >=1 代表 T-1收盤 > T-2收盤），由 aggregate schema 3 提供。
"""
from dataclasses import dataclass
from bisect import bisect_left
from datetime import date, datetime, timedelta
from decimal import Decimal
import math


def positive(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


@dataclass(frozen=True)
class GapConfig:
    signal_start: tuple = (9, 5)
    entry_deadline: tuple = (10, 0)
    min_previous_turnover: float = 300_000_000
    min_open_gap_percent: float = 1.0
    min_runup_percent: float = 6.0
    pullback_percent: float = 2.0
    minute_volume_shares: int = 1000
    previous_turnover_top_percent: float = 20.0

    def validate(self):
        if (type(self.previous_turnover_top_percent) not in (int, float)
                or not math.isfinite(self.previous_turnover_top_percent)
                or not 0 < self.previous_turnover_top_percent <= 100):
            raise ValueError('GAP previous turnover top percent must be in (0, 100]')
        for value in (self.signal_start, self.entry_deadline):
            if (len(value) != 2 or any(type(x) is not int for x in value)
                    or not 0 <= value[0] < 24 or not 0 <= value[1] < 60):
                raise ValueError('GAP invalid time')
        if not (9, 0) <= self.signal_start <= self.entry_deadline < (12, 50):
            raise ValueError('GAP invalid entry window')
        for value in (self.minute_volume_shares,):
            if type(value) is not int or value <= 0:
                raise ValueError('GAP counts/units must be positive integers')
        for value in (self.min_previous_turnover, self.min_open_gap_percent, self.min_runup_percent):
            if not math.isfinite(value) or value < 0:
                raise ValueError('GAP thresholds must be finite and nonnegative')
        if not math.isfinite(self.pullback_percent) or not 0 < self.pullback_percent < 100:
            raise ValueError('GAP pullback must be between 0 and 100')

    def validate_history(self, history, previous_close, reference_date, now):
        """排名與候選共用昨日資料資格；不依賴 volume_dates 或多日均量。"""
        if not isinstance(history, dict) or not reference_date:
            return False
        try:
            volume = float(history['previous_volume_shares'])
            return (
                history['source'] == 'historical.minute_sum' and history['volume_unit'] == 'shares'
                and history['as_of_date'] == reference_date
                and date.fromisoformat(reference_date) < now.date()
                and positive(history['previous_close']) is not None
                and Decimal(str(history['previous_close'])) == Decimal(str(previous_close))
                and math.isfinite(volume) and volume >= 0
            )
        except (KeyError, TypeError, ValueError, OverflowError):
            return False

    def build_turnover_eligibility(self, rows, histories, reference_date, now):
        """Same ranking method as VolConfig.build_turnover_eligibility(); GAP 自己的百分比門檻。

        Match v36: count strictly lower turnovers, including ties in the denominator.
        """
        turnovers = {}
        for symbol, row in rows.items():
            history = histories.get(symbol)
            try:
                if not self.validate_history(history, row[5], reference_date, now):
                    continue
                close = positive(history.get('previous_close'))
                volume = float(history['previous_volume_shares'])
                turnovers[symbol] = Decimal(str(volume)) * Decimal(str(close))
            except (KeyError, TypeError, ValueError, IndexError, OverflowError):
                continue
        ordered = sorted(turnovers.values())
        required = (1 - Decimal(str(self.previous_turnover_top_percent)) / 100) * len(ordered)
        return {symbol: (turnover >= Decimal(str(self.min_previous_turnover))
                         and bisect_left(ordered, turnover) >= required)
                for symbol, turnover in turnovers.items()}

    def find_signal(self, completed, previous_close):
        """逐行對應回測 find_signal()：排名／昨收連漲判斷已由呼叫端處理完畢。"""
        if not completed:
            return None
        previous_close = Decimal(str(previous_close))
        day_open = Decimal(str(completed[0]['open']))
        if day_open <= previous_close * (1 + Decimal(str(self.min_open_gap_percent)) / 100):
            return None
        runup_price = day_open * (1 + Decimal(str(self.min_runup_percent)) / 100)
        pullback_factor = 1 - Decimal(str(self.pullback_percent)) / 100
        day_high = day_open
        for index, bar in enumerate(completed):
            bar_time = (bar['dt'].hour, bar['dt'].minute)
            if bar_time >= self.entry_deadline:
                break
            day_high = max(day_high, Decimal(str(bar['high'])))
            if bar_time < self.signal_start:
                continue
            trigger_price = day_high * pullback_factor
            if day_high >= runup_price and Decimal(str(bar['close'])) <= trigger_price:
                return index, {
                    'gap_day_open': float(day_open),
                    'gap_open_gap_percent': float((day_open / previous_close - 1) * 100),
                    'gap_day_high': float(day_high),
                    'gap_trigger_price': float(trigger_price),
                    'gap_signal_close': bar['close'],
                }
        return None

    def find_candidate(self, bars, previous_close, closed_up, history, reference_date, now, limit_up):
        """Return (candidate, disabled). Replaying completed bars is deterministic.

        closed_up: 對應回測 bars_by_date 讀前一有資料交易日收盤的判斷，
        由呼叫端以 stock_data 的 up_streak_days >= 1 提供。
        """
        if not self.validate_history(history, previous_close, reference_date, now):
            return None, True
        if (Decimal(str(history['previous_volume_shares'])) * Decimal(str(previous_close))
                < Decimal(str(self.min_previous_turnover))):
            return None, True
        if not closed_up:
            return None, True
        base = now.replace(second=0, microsecond=0)
        session = base.replace(hour=9, minute=0)
        completed = [bar for bar in bars if session <= bar['dt'] < base]
        times = [bar['dt'] for bar in completed]
        if times != sorted(set(times)) or any(t.second or t.microsecond for t in times):
            return None, True
        up = Decimal(str(limit_up))
        for bar in completed:
            if Decimal(str(bar['high'])) >= up:
                return None, True
        found = self.find_signal(completed, previous_close)
        if found is None:
            return None, (now.hour, now.minute) > self.entry_deadline
        index, fields = found
        signal_bar = completed[index]
        if (signal_bar['dt'].hour, signal_bar['dt'].minute) < self.signal_start:
            return None, True
        entry_time = signal_bar['dt'] + timedelta(minutes=1)
        if (entry_time.hour, entry_time.minute) > self.entry_deadline:
            return None, True
        turnover = Decimal(str(history['previous_volume_shares'])) * Decimal(str(previous_close))
        return {**fields, 'gap_previous_turnover': float(turnover),
                'gap_signal_time': signal_bar['dt'].isoformat(),
                'gap_entry_time': entry_time.isoformat()}, False
