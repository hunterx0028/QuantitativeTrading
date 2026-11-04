"""BROKEN：昨收上方窗口成立後，等待連續完整分K跌破百分比門檻。"""
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
import math


@dataclass(frozen=True)
class BrokenConfig:
    signal_start: tuple
    signal_end: tuple
    signal_deadline: tuple
    drop_percent: float
    confirm_bars: int

    @property
    def entry_start(self):
        """確認棒自窗口結束的下一分鐘起算，不另設參數。"""
        return divmod(self.signal_end[0] * 60 + self.signal_end[1] + 1, 60)

    def validate(self):
        times = [self.signal_start, self.signal_end, self.signal_deadline]
        if any(len(t) != 2 or any(type(v) is not int for v in t)
               or not (0 <= t[0] <= 23 and 0 <= t[1] <= 59) for t in times):
            raise ValueError("BROKEN 時間格式錯誤")
        if not (self.signal_start <= self.signal_end < self.entry_start <= self.signal_deadline):
            raise ValueError("BROKEN 時間需滿足 起點 <= 確認 < 觸發起點 <= 訊號截止")
        if type(self.confirm_bars) is not int or self.confirm_bars < 1:
            raise ValueError("BROKEN_CONFIRM_BARS 必須為正整數")
        if not math.isfinite(self.drop_percent) or not 0 < self.drop_percent < 100:
            raise ValueError("BROKEN_DROP_PERCENT 必須介於 0 與 100")

    def find_candidate(self, bars, previous_close, now):
        """回傳 (訊號, 當日此策略已失效)。確認窗口結束後缺棒即當日失效。"""
        base = now.replace(second=0, microsecond=0)
        start = base.replace(hour=self.signal_start[0], minute=self.signal_start[1])
        end = base.replace(hour=self.signal_end[0], minute=self.signal_end[1])
        if now < end + timedelta(minutes=1):
            return None, False
        previous = Decimal(str(previous_close))
        if not previous.is_finite() or previous <= 0:
            return None, True
        completed = [bar for bar in bars if bar['dt'] < base]
        window = [bar for bar in completed if start <= bar['dt'] < end + timedelta(minutes=1)]
        expected = [start + timedelta(minutes=i) for i in range(int((end-start).total_seconds()//60)+1)]
        if [bar['dt'] for bar in window] != expected:
            return None, True
        if any(Decimal(str(bar['low'])) <= previous for bar in window):
            return None, True
        trigger = previous * (1 - Decimal(str(self.drop_percent))/100)
        entry_start = base.replace(hour=self.entry_start[0], minute=self.entry_start[1])
        deadline = base.replace(hour=self.signal_deadline[0], minute=self.signal_deadline[1])
        counts = Counter(bar['dt'] for bar in completed)
        count, last = 0, None
        for bar in completed:
            dt = bar['dt']
            if not entry_start <= dt <= deadline:
                continue
            if Decimal(str(bar['low'])) >= trigger:
                count, last = 0, None
                continue
            if dt.second or dt.microsecond or counts[dt] != 1:
                return None, True
            if last is None or dt != last + timedelta(minutes=1):
                count = 0
            count += 1
            last = dt
            if count >= self.confirm_bars:
                return {'broken_signal_time': dt.isoformat(),
                        'broken_activation_time': (end+timedelta(minutes=1)).isoformat(),
                        'broken_trigger_price': float(trigger)}, False
        return None, False
