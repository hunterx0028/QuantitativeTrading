"""TOP: v41 昨日窄幅股，自當日高點回落後均價反壓放空；完成分K確認後以即時報價入場。

前一日資料由 aggregate v3 以已完成日K產生 top_history_by_symbol（不擴充股票 tuple）：
昨日振幅＝（昨高−昨低）／前日收盤×100，前日收盤為昨日的前一個已完成交易日收盤。
"""
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import math

TOP_HISTORY_SOURCE = 'historical.candles'


def positive(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _same_price(left, right):
    return Decimal(str(left)) == Decimal(str(right))


def build_top_history(daily_response, stats_response, record, prepared_at, now=None):
    """由 aggregate v3 已取得的日K及 stats 建立單檔 TOP 前一日資料。

    只使用已完成日K（同 VOL：今日 13:35 前不採用今日K）；昨高、昨低、昨收須與 stats
    寫入股票 tuple 的 record[3]、record[4]、record[5] 一致，否則拋出 ValueError，僅停用該檔 TOP。
    """
    now = now or datetime.now()
    today = now.date().isoformat()
    completed = {}
    for bar in daily_response.get('data', []) if isinstance(daily_response, dict) else []:
        day = str(bar.get('date', ''))[:10]
        if not day or not (day < today or (day == today and (now.hour, now.minute) >= (13, 35))):
            continue
        if day in completed:
            raise ValueError(f'TOP daily candle date duplicated: {day}')
        prices = {key: positive(bar.get(key)) for key in ('high', 'low', 'close')}
        if any(value is None for value in prices.values()) or prices['low'] > prices['high']:
            raise ValueError(f'TOP daily candle price invalid: {day}')
        completed[day] = prices
    days = sorted(completed)
    if len(days) < 2:
        raise ValueError('TOP requires two completed daily candles')
    as_of, day_before = days[-1], days[-2]
    stats = stats_response.get('data', stats_response) if isinstance(stats_response, dict) else {}
    stats_date = str(stats.get('date', stats_response.get('date', '')))[:10]
    if stats_date.replace('-', '') != as_of.replace('-', ''):
        raise ValueError('TOP stats date does not match latest completed daily candle')
    yesterday = completed[as_of]
    for key, index in (('high', 3), ('low', 4), ('close', 5)):
        if not _same_price(yesterday[key], record[index]):
            raise ValueError(f'TOP daily {key} does not match stats record')
    return {
        'as_of_date': as_of, 'prepared_at': prepared_at,
        'previous_high': float(record[3]), 'previous_low': float(record[4]),
        'previous_close': float(record[5]),
        'day_before_date': day_before, 'day_before_close': completed[day_before]['close'],
        'source': TOP_HISTORY_SOURCE,
    }


@dataclass(frozen=True)
class TopConfig:
    prev_range_max_percent: float = 3.2
    pullback_from_high_percent: float = 3.5
    signal_start: tuple = (9, 16)
    entry_deadline: tuple = (12, 0)

    def validate(self):
        for value in (self.signal_start, self.entry_deadline):
            if (not isinstance(value, tuple) or len(value) != 2 or any(type(x) is not int for x in value)
                    or not 0 <= value[0] < 24 or not 0 <= value[1] < 60):
                raise ValueError('TOP invalid time')
        if not (9, 0) < self.signal_start <= self.entry_deadline < (12, 50):
            raise ValueError('TOP invalid entry window')
        for value in (self.prev_range_max_percent, self.pullback_from_high_percent):
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value < 100:
                raise ValueError('TOP percentages must be between 0 and 100')

    def validate_history(self, history, previous_close, reference_date, now):
        """不猜日期、不沿用舊快照；昨收須與股票 tuple 的昨收一致。"""
        if not isinstance(history, dict) or not reference_date:
            return False
        try:
            values = [positive(history[key]) for key in
                      ('previous_high', 'previous_low', 'previous_close', 'day_before_close')]
            return (
                history['source'] == TOP_HISTORY_SOURCE
                and history['as_of_date'] == reference_date
                and date.fromisoformat(reference_date) < now.date()
                and str(history['day_before_date']) < str(history['as_of_date'])
                and all(value is not None for value in values)
                and values[1] <= values[0]
                and _same_price(history['previous_close'], previous_close)
            )
        except (KeyError, TypeError, ValueError, OverflowError):
            return False

    def previous_range_percent(self, history):
        return ((Decimal(str(history['previous_high'])) - Decimal(str(history['previous_low'])))
                / Decimal(str(history['day_before_close'])) * 100)

    def build_eligibility(self, rows, histories, reference_date, now):
        """開盤前固定名單：資料有效且昨日振幅 <= 門檻才保留 TOP；回傳 symbol -> (eligible, range)。"""
        result = {}
        for symbol, row in rows.items():
            history = histories.get(symbol) if isinstance(histories, dict) else None
            try:
                if not self.validate_history(history, row[5], reference_date, now):
                    result[symbol] = (False, None)
                    continue
                prev_range = self.previous_range_percent(history)
                result[symbol] = (prev_range <= Decimal(str(self.prev_range_max_percent)), float(prev_range))
            except (KeyError, TypeError, ValueError, IndexError, ArithmeticError):
                result[symbol] = (False, None)
        return result

    def find_candidate(self, bars, now, limit_up, limit_down):
        """回傳 (候選欄位, 當日淘汰)。每次重播已完成分K，結果可重現；分K時間異常拋出 ValueError。

        與 v41 相同：09:00 起累計最高價；相鄰兩根已完成分K average 皆有效才判斷；
        訊號棒 close 自最高價回落 >= 門檻，前一棒 close < average，本棒 high >= average 且
        close < average；入場時間為訊號棒下一分鐘，不得晚於入場截止。
        """
        base = now.replace(second=0, microsecond=0)
        session = base.replace(hour=9, minute=0)
        completed = [bar for bar in bars if session <= bar['dt'] < base]
        times = [bar['dt'] for bar in completed]
        if times != sorted(set(times)) or any(t.second or t.microsecond for t in times):
            # 與 IND 相同：單次回應異常不淘汰當日資格，由候選流程記錄後於下一輪重查。
            raise ValueError('TOP 分K時間重複、非整分鐘或順序異常，等待下一輪')
        limit_up, limit_down = Decimal(str(limit_up)), Decimal(str(limit_down))
        pullback_ratio = 1 - Decimal(str(self.pullback_from_high_percent)) / 100
        high = Decimal('0')
        previous = None
        for bar in completed:
            if (Decimal(str(bar['high'])) >= limit_up or Decimal(str(bar['low'])) <= limit_down):
                return None, True  # 入場前觸及漲跌停，當日不做（同回測框架）。
            high = max(high, Decimal(str(bar['high'])))
            prev_bar, previous = previous, bar
            if prev_bar is None or (bar['dt'].hour, bar['dt'].minute) < self.signal_start:
                continue
            average = positive(bar.get('average'))
            prev_average = positive(prev_bar.get('average'))
            if average is None or prev_average is None:
                continue
            close = Decimal(str(bar['close']))
            if not (Decimal(str(prev_bar['close'])) < Decimal(str(prev_average))
                    and Decimal(str(bar['high'])) >= Decimal(str(average))
                    and close < Decimal(str(average))
                    and close <= high * pullback_ratio):
                continue
            entry_time = bar['dt'] + timedelta(minutes=1)
            if (entry_time.hour, entry_time.minute) > self.entry_deadline:
                return None, True
            return {
                'top_signal_time': bar['dt'].isoformat(), 'top_previous_bar_time': prev_bar['dt'].isoformat(),
                'top_signal_close': bar['close'], 'top_signal_high': bar['high'],
                'top_signal_average': average, 'top_day_high': float(high),
                'top_pullback_percent': float((1 - close / high) * 100),
                'top_entry_time': entry_time.isoformat(),
            }, False
        return None, (now.hour, now.minute) > self.entry_deadline
