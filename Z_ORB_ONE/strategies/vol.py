"""VOL: v35 completed-minute signals followed by a 15-second quote entry.

Historical volume uses minute candles, not daily volume (which can include
other sessions). All stored/computed volumes use shares; API minute lots x1000.
"""
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import math


def positive(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def build_volume_history(raw, expected_dates, prepared_at, days=5):
    """Use the same valid OHLC/volume rows as v35; require all expected days."""
    grouped = {}
    seen = set()
    for item in raw:
        try:
            dt = datetime.fromisoformat(str(item['date'])[:19])
            prices = {key: positive(item[key]) for key in ('open', 'high', 'low', 'close')}
            volume = float(item.get('volume', 0) or 0)
            if (any(value is None for value in prices.values())
                    or prices['low'] > min(prices['open'], prices['close'])
                    or prices['high'] < max(prices['open'], prices['close'])
                    or not math.isfinite(volume) or volume < 0):
                continue
            if dt in seen:
                raise ValueError('VOL historical minute timestamp duplicated')
            seen.add(dt)
            grouped.setdefault(dt.date().isoformat(), []).append((dt, volume * 1000, prices['close']))
        except (KeyError, TypeError, OverflowError):
            continue
    expected_dates = sorted(set(expected_dates))[-days:]
    if len(expected_dates) != days or any(day not in grouped for day in expected_dates):
        raise ValueError('VOL requires five complete historical minute dates')
    volumes = []
    for day in expected_dates:
        rows = sorted(grouped[day])
        # API may include a current partial session; never accept it as a full day.
        if rows[-1][0].time() < datetime.strptime('13:30', '%H:%M').time():
            raise ValueError(f'VOL incomplete minute session: {day}')
        volumes.append(sum(row[1] for row in rows))
    previous_close = sorted(grouped[expected_dates[-1]])[-1][2]
    if sum(volumes) <= 0:
        raise ValueError('VOL historical average volume must be positive')
    return {
        'as_of_date': expected_dates[-1], 'prepared_at': prepared_at,
        'volume_dates': expected_dates, 'daily_volumes_shares': volumes,
        'previous_close': previous_close, 'previous_volume_shares': volumes[-1],
        'average_daily_volume_shares': sum(volumes) / days,
        'volume_unit': 'shares', 'source': 'historical.minute_sum',
    }


@dataclass(frozen=True)
class VolConfig:
    signal_start: tuple = (9, 20)
    entry_deadline: tuple = (12, 0)
    min_previous_turnover: float = 1_000_000_000
    average_days: int = 5
    min_volume_ratio: float = 4.0
    session_minutes: int = 270
    min_runup_percent: float = 7.0
    pullback_percent: float = 3.0
    minute_volume_shares: int = 1000

    def validate(self):
        for value in (self.signal_start, self.entry_deadline):
            if (len(value) != 2 or any(type(x) is not int for x in value)
                    or not 0 <= value[0] < 24 or not 0 <= value[1] < 60):
                raise ValueError('VOL invalid time')
        if not (9, 0) <= self.signal_start <= self.entry_deadline < (12, 50):
            raise ValueError('VOL invalid entry window')
        for value in (self.average_days, self.session_minutes, self.minute_volume_shares):
            if type(value) is not int or value <= 0:
                raise ValueError('VOL counts/units must be positive integers')
        for value in (self.min_previous_turnover, self.min_volume_ratio, self.min_runup_percent):
            if not math.isfinite(value) or value < 0:
                raise ValueError('VOL thresholds must be finite and nonnegative')
        if not math.isfinite(self.pullback_percent) or not 0 < self.pullback_percent < 100:
            raise ValueError('VOL pullback must be between 0 and 100')

    def validate_history(self, history, previous_close, reference_date, now):
        """No guessing dates/units or silently using an old stock_data snapshot."""
        if not isinstance(history, dict) or not reference_date:
            return False
        try:
            days = history['volume_dates']
            return (
                history['source'] == 'historical.minute_sum' and history['volume_unit'] == 'shares'
                and len(days) == self.average_days and days == sorted(set(days))
                and days[-1] == history['as_of_date'] == reference_date
                and date.fromisoformat(reference_date) < now.date()
                and positive(history['previous_close']) is not None
                and Decimal(str(history['previous_close'])) == Decimal(str(previous_close))
                and positive(history['average_daily_volume_shares']) is not None
                and positive(history['previous_volume_shares']) is not None
            )
        except (KeyError, TypeError, ValueError):
            return False

    def find_candidate(self, bars, previous_close, history, reference_date, now, limit_up):
        """Return (candidate, disabled). Replaying completed bars is deterministic."""
        if not self.validate_history(history, previous_close, reference_date, now):
            return None, True
        if (Decimal(str(history['previous_volume_shares'])) * Decimal(str(previous_close))
                < Decimal(str(self.min_previous_turnover))):
            return None, True
        base = now.replace(second=0, microsecond=0)
        session = base.replace(hour=9, minute=0)
        deadline = base.replace(hour=self.entry_deadline[0], minute=self.entry_deadline[1])
        completed = [bar for bar in bars if session <= bar['dt'] < base]
        times = [bar['dt'] for bar in completed]
        if times != sorted(set(times)) or any(t.second or t.microsecond for t in times):
            return None, True
        if not completed:
            return None, False
        opening = Decimal(str(completed[0]['open']))
        high = Decimal('0')
        cumulative = 0.0
        previous_above = False
        signal = None
        candidate = None
        for bar in completed:
            if bar['dt'] > deadline:
                break
            if Decimal(str(bar['high'])) >= Decimal(str(limit_up)):
                return None, True
            if candidate is not None:
                continue  # Still inspect later completed bars for limit-up touches.
            if signal is not None:
                average = positive(bar.get('average'))
                if average is not None and Decimal(str(bar['low'])) < Decimal(str(average)):
                    candidate = {**signal, 'vol_average_break_time': bar['dt'].isoformat(),
                                 'vol_average_break_low': bar['low'], 'vol_average_break_average': average,
                                 'vol_entry_time': (bar['dt'] + timedelta(minutes=1)).isoformat()}
                continue
            volume = bar.get('volume')
            if volume is None or not math.isfinite(volume) or volume < 0:
                return None, True  # Do not invent zero volume for an incomplete response.
            high = max(high, Decimal(str(bar['high'])))
            cumulative += volume * self.minute_volume_shares
            trigger = high * (1 - Decimal(str(self.pullback_percent)) / 100)
            below = Decimal(str(bar['close'])) <= trigger
            crossed = previous_above and below
            previous_above = not below
            if not crossed or (bar['dt'].hour, bar['dt'].minute) < self.signal_start:
                continue
            if high < opening * (1 + Decimal(str(self.min_runup_percent)) / 100):
                continue
            elapsed = int((bar['dt'] - session).total_seconds() // 60) + 1
            ratio = cumulative / (history['average_daily_volume_shares'] * elapsed / self.session_minutes)
            if ratio < self.min_volume_ratio:
                continue
            signal = {'vol_signal_time': bar['dt'].isoformat(), 'vol_signal_close': bar['close'],
                      'vol_day_open': float(opening), 'vol_day_high': float(high),
                      'vol_trigger_price': float(trigger), 'vol_volume_ratio': ratio,
                      'vol_history_date': reference_date}
        if candidate is not None:
            if datetime.fromisoformat(candidate['vol_entry_time']) > deadline:
                return None, True
            return candidate, False
        return None, (now.hour, now.minute) > self.entry_deadline
