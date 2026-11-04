"""FALL 回測獲利保護：純函式與人工分K測試，不使用券商 SDK。"""
import ast
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from test_v18_profit_protection import load_functions


SOURCE = Path(__file__).resolve().parents[1] / 'analysis_strategy' / 'analysis_strategy_broken_each_stock_special_short_v24.py'


class FallProfitProtectionTests(unittest.TestCase):
    def setUp(self):
        self.api = load_functions(SOURCE)
        self.entry = datetime(2026, 11, 4, 10)

    def bar(self, minute, opening=100, high=101, low=99, average=105):
        return dict(dt=self.entry + timedelta(minutes=minute), open=opening,
                    high=high, low=low, close=opening, average=average)

    def evaluate(self, *following, entry_bar=None):
        bars = [entry_bar or self.bar(0), *following]
        candidate = self.api['build_trade_candidate'](
            'test', '01', self.entry.date(), bars[0], 100, bars,
            110, 90, self.api['STRATEGY_LOWER'], (12, 50))
        candidate.update(drop_percent=1.5, max_deviation_ratio=0.1, entry_intrabar=True)
        return self.api['evaluate_candidates']([candidate])[0][1]

    def test_parameters_match_live_fall_without_switch(self):
        live = SOURCE.parents[1] / 'execute_strategy_broken_high_falling_singleton_v17.py'
        constants = {}
        for node in ast.parse(live.read_text(encoding='utf-8')).body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.startswith('FALL_'):
                        try:
                            constants[target.id] = ast.literal_eval(node.value)
                        except (ValueError, TypeError):
                            pass
        for name in ('PROTECT_PROFIT_PER', 'PROTECT_LOSS_PER', 'PROFIT_BACK_PERCENT', 'PROFIT_TARGET_PERCENT'):
            self.assertEqual(self.api[name], constants['FALL_' + name])
        for path in (SOURCE, live, SOURCE.parents[1] / 'strategies' / 'fall.py'):
            self.assertNotIn('PROTECT_PROFIT_SWITCH', path.read_text(encoding='utf-8'))

    def test_protection_line_and_equal_touch(self):
        for high in (98.5, 99):
            with self.subTest(high=high):
                result = self.evaluate(self.bar(1, low=97.5), self.bar(2, opening=98, high=high, low=97.9))
                self.assertEqual((result['exit_reason'], result['exit_price']), ('protect_profit', 98.5))

    def test_five_percent_activates_trailing_not_fixed_exit(self):
        result = self.evaluate(self.bar(1, opening=98, high=99, low=95), self.bar(2, opening=95, high=96, low=94.9))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('trailing_profit', 95.5))

    def test_new_lines_only_apply_to_following_bars(self):
        result = self.evaluate(self.bar(1, high=101, low=95), self.bar(170, opening=96, high=104, low=94))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('close', 96))

    def test_trail_advance_and_old_line_priority(self):
        result = self.evaluate(self.bar(1, opening=98, high=99, low=95),
                               self.bar(2, opening=95, high=95.4, low=94),
                               self.bar(3, opening=94.2, high=94.5, low=94))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('trailing_profit', 94.5))
        result = self.evaluate(self.bar(1, low=97.5), self.bar(2, opening=98, high=99, low=94))
        self.assertEqual(result['exit_reason'], 'protect_profit')

    def test_gap_uses_open_for_each_exit_line(self):
        for low, opening, reason in ((97.5, 99, 'protect_profit_gap'), (95, 97, 'trailing_profit_gap')):
            with self.subTest(reason=reason):
                result = self.evaluate(self.bar(1, low=low), self.bar(2, opening=opening, high=opening + 1, low=opening - 1))
                self.assertEqual((result['exit_reason'], result['exit_price']), (reason, opening))

    def test_average_stop_excludes_entry_bar(self):
        result = self.evaluate(entry_bar=self.bar(0, opening=104, high=105, low=99, average=100))
        self.assertEqual(result['outcome'], 'incomplete')

    def test_intrabar_entry_low_can_activate_for_next_bar(self):
        result = self.evaluate(self.bar(1, opening=95, high=95.5, low=94.9),
                               entry_bar=self.bar(0, high=101, low=95))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('trailing_profit', 95.5))

    def test_stop_wins_over_same_bar_activation(self):
        result = self.evaluate(self.bar(1, high=103, low=95, average=102))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('average_stop', 102.5))

    def test_average_stop_strict_comparison_and_tick_bands(self):
        for average, expected in ((100, 100.5), (100.12, 100.5), (50, 50.1), (49.97, 50)):
            with self.subTest(average=average):
                result = self.evaluate(self.bar(1, opening=average, high=expected + 1,
                                               low=average - 0.1, average=average))
                self.assertEqual((result['exit_reason'], result['exit_price']), ('average_stop', expected))
        result = self.evaluate(self.bar(1, high=100, average=100), self.bar(170))
        self.assertEqual(result['exit_reason'], 'close')

    def test_average_gap_uses_tick_price_not_open(self):
        result = self.evaluate(self.bar(1, opening=104, high=105, low=103, average=100))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('average_stop', 100.5))

    def test_current_average_and_missing_average(self):
        result = self.evaluate(self.bar(1, high=101, average=101),
                               self.bar(2, high=101, average=100.6))
        self.assertEqual((result['exit_price'], result['exit_dt']), (101, self.entry + timedelta(minutes=2)))
        result = self.evaluate(self.bar(1, high=104, average=None), self.bar(2, high=103, average=102))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('average_stop', 102.5))
        result = self.evaluate(self.bar(1, low=97.5),
                               self.bar(2, opening=98, high=99, low=97.8, average=None))
        self.assertEqual(result['exit_reason'], 'protect_profit')

    def test_tighter_trigger_wins(self):
        result = self.evaluate(self.bar(1, low=97.5),
                               self.bar(2, opening=98, high=99, low=97.8, average=98.2))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('average_stop', 98.3))

    def test_missing_bars_and_settlement(self):
        result = self.evaluate(self.bar(1, low=97.5), self.bar(8, opening=98, high=99, low=97.8))
        self.assertEqual(result['exit_dt'], self.entry + timedelta(minutes=8))
        result = self.evaluate(self.bar(171, opening=99, high=105, low=94))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('close_delayed', 99))
        self.assertEqual(result['signed_pnl'], self.api['calculate_net_pnl_for_short_trade'](100, 99)[0])

    def test_incomplete_data_and_validation(self):
        result = self.evaluate(self.bar(1))
        self.assertEqual(result['outcome'], 'incomplete')
        self.api['validate_settings']()
        self.api['PROTECT_LOSS_PER'] = self.api['PROTECT_PROFIT_PER']
        with self.assertRaises(ValueError):
            self.api['validate_settings']()


if __name__ == '__main__':
    unittest.main()
