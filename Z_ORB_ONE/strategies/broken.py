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
    max_signal_range_percent: float = 3.3

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
        if not math.isfinite(self.max_signal_range_percent) or self.max_signal_range_percent < 0:
            raise ValueError("BROKEN_MAX_SIGNAL_RANGE_PERCENT 必須為有限非負數")

    def check_window(self, bars, previous_close, now):
        """固定窗口僅需檢核一次；回傳 (窗口資料, 當日此策略已失效)。"""
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
        spread = (Decimal(str(max(bar['high'] for bar in window)))
                  - Decimal(str(min(bar['low'] for bar in window))))
        if spread > previous * Decimal(str(self.max_signal_range_percent)) / 100:
            return None, True
        return {'broken_activation_time': (end + timedelta(minutes=1)).isoformat(),
                'broken_trigger_price': float(previous * (1 - Decimal(str(self.drop_percent)) / 100)),
                'broken_signal_range_percent': float(spread / previous * 100)}, False

    def find_candidate(self, bars, previous_close, now, *, window_metrics=None):
        """已完成分K確認即保留資格；不要求次根 open，也不因跨分鐘失效。"""
        if window_metrics is None:
            window_metrics, rejected = self.check_window(bars, previous_close, now)
            if window_metrics is None:
                return None, rejected
        base = now.replace(second=0, microsecond=0)
        completed = [bar for bar in bars if bar['dt'] < base]
        # 使用昨收與參數重新建立 Decimal 門檻，不以快取 float 判斷邊界。
        trigger = Decimal(str(previous_close)) * (1 - Decimal(str(self.drop_percent))/100)
        entry_start = base.replace(hour=self.entry_start[0], minute=self.entry_start[1])
        deadline = base.replace(hour=self.signal_deadline[0], minute=self.signal_deadline[1])
        counts = Counter(bar['dt'] for bar in completed)
        count, last = 0, None
        for bar in completed:
            dt = bar['dt']
            if not entry_start <= dt <= deadline:
                continue
            if Decimal(str(bar['close'])) >= trigger:
                count, last = 0, None
                continue
            if dt.second or dt.microsecond or counts[dt] != 1:
                return None, True
            if last is None or dt != last + timedelta(minutes=1):
                count = 0
            count += 1
            last = dt
            if count >= self.confirm_bars:
                entry_time = dt + timedelta(minutes=1)
                return {'broken_signal_time': dt.isoformat(),
                        'broken_entry_time': entry_time.isoformat(),
                        **window_metrics}, False
        return None, False
