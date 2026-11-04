"""v24 連續完整分K確認與其後入場的離線測試。"""
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from test_v18_profit_protection import FUNCTIONS, load_functions

SOURCE = Path(__file__).resolve().parents[1] / 'analysis_strategy' / 'analysis_strategy_broken_each_stock_special_short_v24.py'


class ConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.api = load_functions(SOURCE, FUNCTIONS | {
            'get_signal_window', 'find_decline_signal', 'find_entry', 'price_below_previous_close',
        })
        self.api['ENTRY_CONFIRM_BARS'] = 2
        self.start = datetime(2026, 11, 4, *self.api['SIGNAL_START_TIME'])
        self.activation = self.start.replace(hour=self.api['SIGNAL_END_TIME'][0],
                                             minute=self.api['SIGNAL_END_TIME'][1]) + timedelta(minutes=1)
        count = int((self.activation - self.start).total_seconds() // 60)
        self.window = [dict(dt=self.start + timedelta(minutes=i), close=102-i*.2, low=101.9-i*.2)
                       for i in range(count)]

    def bar(self, minute, low):
        return dict(dt=self.activation + timedelta(minutes=minute), low=low, open=100)

    def find(self, *bars):
        return self.api['find_entry'](self.window + list(bars), 100)

    def test_single_deep_low_does_not_confirm(self):
        self.assertIsNone(self.find(self.bar(0, 95), self.bar(1, 100)))

    def test_two_bars_confirm_then_wait_for_following_bar(self):
        self.assertIsNone(self.find(self.bar(0, 99), self.bar(1, 99)))
        result = self.find(self.bar(0, 99), self.bar(1, 99), self.bar(2, 99.9))
        self.assertEqual((result[0]['dt'], result[1]), (self.activation + timedelta(minutes=2), 99.9))

    def test_missing_or_equal_low_resets_count(self):
        self.assertIsNone(self.find(self.bar(0, 99), self.bar(2, 99), self.bar(3, 100)))
        self.assertIsNone(self.find(self.bar(0, 99), self.bar(1, 100), self.bar(2, 99)))

    def test_confirmation_persists_until_later_entry(self):
        result = self.find(self.bar(0, 99), self.bar(1, 99), self.bar(2, 100), self.bar(5, 99.9))
        self.assertEqual(result[0]['dt'], self.activation + timedelta(minutes=5))

    def test_window_does_not_count_and_deadline_applies(self):
        for bar in self.window:
            bar['low'] = 95
        self.assertIsNone(self.find(self.bar(0, 99), self.bar(1, 100)))
        self.api['ENTRY_DEADLINE'] = (self.activation.hour, self.activation.minute+2)
        self.assertIsNone(self.find(self.bar(0, 99), self.bar(1, 99), self.bar(3, 99)))


if __name__ == '__main__':
    unittest.main()
