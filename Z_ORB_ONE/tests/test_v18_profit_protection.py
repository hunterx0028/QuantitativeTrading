"""離線測試 v18 出場規則；只載入純函式，不匯入券商 SDK。"""
import ast
import math
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

import numpy as np


SOURCE = Path(__file__).resolve().parents[1] / 'analysis_strategy' / 'analysis_strategy_broken_each_stock_special_short_v18.py'
FUNCTIONS = {
    'parse_average', 'get_tick_size', 'floor_price_to_tick', 'ceil_price_to_tick',
    'price_above_average',
    'get_profit_trail_tick_size', 'calculate_short_profit_trail',
    'calculate_stop_loss_amount_by_percent', 'calculate_take_profit_amount_by_percent',
    'should_skip_entry_by_limit_up', 'has_limit_price_touched_before_entry',
    'calculate_net_pnl_for_short_trade', 'build_outcome_result',
    'build_trade_candidate', 'evaluate_candidates', 'calculate_early_average_slope',
    'validate_settings', 'format_result_line',
}


def load_functions(source=SOURCE, functions=FUNCTIONS):
    tree = ast.parse(source.read_text(encoding='utf-8'))
    namespace = dict(math=math, np=np, Decimal=Decimal, ROUND_CEILING=ROUND_CEILING,
                     ROUND_FLOOR=ROUND_FLOOR, date=date, datetime=datetime, timedelta=timedelta)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    namespace[target.id] = value
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in functions]
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(source), 'exec'), namespace)
    return namespace


class ProfitProtectionTests(unittest.TestCase):
    def setUp(self):
        self.api = load_functions()
        self.entry = datetime(2026, 11, 4, 10)

    def bar(self, minute, *, opening=100, high=101, low=99):
        return dict(dt=self.entry + timedelta(minutes=minute), open=opening,
                    high=high, low=low, close=opening, average=100)

    def evaluate(self, *following, entry_bar=None):
        bars = [entry_bar or self.bar(0), *following]
        candidate = self.api['build_trade_candidate'](
            'test', '01', self.entry.date(), bars[0], 100, bars,
            110, 90, self.api['STRATEGY_LOWER'], (12, 50))
        return self.api['evaluate_candidates']([candidate])[0][1]

    def test_constants_match_live_avg(self):
        live = SOURCE.parents[1] / 'execute_strategy_broken_high_falling_singleton_v17.py'
        constants = {}
        for node in ast.parse(live.read_text(encoding='utf-8')).body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id.startswith('AVG_'):
                        try:
                            constants[target.id] = ast.literal_eval(node.value)
                        except (ValueError, TypeError):
                            pass
        for name in ('PROTECT_PROFIT_PER', 'PROTECT_LOSS_PER',
                     'PROFIT_BACK_PERCENT', 'PROFIT_TARGET_PERCENT'):
            self.assertEqual(self.api[name], constants['AVG_' + name])

    def test_protection_equal_trigger_and_exit_at_line_not_high(self):
        result = self.evaluate(self.bar(1, low=97.5),
                               self.bar(2, opening=98, high=98.8, low=97.8))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('protect_profit', 98.5))

    def test_protection_line_equal_high_triggers(self):
        result = self.evaluate(self.bar(1, low=97.5),
                               self.bar(2, opening=98, high=98.5, low=97.8))
        self.assertEqual(result['exit_reason'], 'protect_profit')

    def test_new_lines_do_not_exit_on_activation_bar(self):
        result = self.evaluate(self.bar(1, high=101, low=95),
                               self.bar(170, opening=96, high=105, low=94))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('close', 96))

    def test_five_percent_starts_trail_instead_of_fixed_exit(self):
        result = self.evaluate(self.bar(1, high=98, low=95, opening=98),
                               self.bar(2, opening=95.1, high=95.7, low=95))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('trailing_profit', 95.5))

    def test_trail_advances_only_on_next_target(self):
        result = self.evaluate(self.bar(1, high=98, low=95, opening=98),
                               self.bar(2, opening=95, high=95.4, low=94),
                               self.bar(3, opening=94.2, high=94.8, low=94))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('trailing_profit', 94.5))

    def test_low_above_next_target_does_not_move_trail(self):
        result = self.evaluate(self.bar(1, high=98, low=95, opening=98),
                               self.bar(2, opening=95, high=95.4, low=94.5),
                               self.bar(3, opening=95, high=95.5, low=94.9))
        self.assertEqual(result['exit_price'], 95.5)

    def test_prior_line_exit_precedes_favorable_new_low(self):
        result = self.evaluate(self.bar(1, low=97.5), self.bar(2, opening=98, high=99, low=94))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('protect_profit', 98.5))

    def test_gap_uses_open(self):
        for first, second, reason, price in (
            (self.bar(1), self.bar(2, opening=104, high=105, low=103), 'stop_gap', 104),
            (self.bar(1, low=97.5), self.bar(2, opening=99, high=100, low=98), 'protect_profit_gap', 99),
            (self.bar(1, low=95), self.bar(2, opening=97, high=98, low=96), 'trailing_profit_gap', 97),
        ):
            with self.subTest(reason=reason):
                result = self.evaluate(first, second)
                self.assertEqual((result['exit_reason'], result['exit_price']), (reason, price))

    def test_original_stop_wins_when_same_bar_hits_profit_threshold(self):
        result = self.evaluate(self.bar(1, high=103, low=95))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('stop', 102))

    def test_entry_bar_is_excluded(self):
        result = self.evaluate(self.bar(1, high=101, low=99), self.bar(170),
                               entry_bar=self.bar(0, high=105, low=94))
        self.assertEqual(result['exit_reason'], 'close')

    def test_protection_always_enabled_and_trailing_still_active(self):
        result = self.evaluate(self.bar(1, low=97.5),
                               self.bar(2, opening=98, high=99, low=97), self.bar(170))
        self.assertEqual(result['exit_reason'], 'protect_profit')
        result = self.evaluate(self.bar(1, low=95), self.bar(2, opening=95, high=96, low=94.9))
        self.assertEqual(result['exit_reason'], 'trailing_profit')

    def test_missing_bars_keep_protection_state(self):
        result = self.evaluate(self.bar(1, low=97.5),
                               self.bar(8, opening=98, high=99, low=97.8))
        self.assertEqual((result['exit_reason'], result['exit_dt']),
                         ('protect_profit', self.entry + timedelta(minutes=8)))

    def test_fixed_settlement_and_fees(self):
        result = self.evaluate(self.bar(1), self.bar(171, opening=99, high=105, low=94))
        self.assertEqual((result['exit_reason'], result['exit_price']), ('close', 99))
        expected = self.api['calculate_net_pnl_for_short_trade'](100, 99)[0]
        self.assertEqual(result['signed_pnl'], expected)

    def test_trail_steps_have_one_tick_minimum(self):
        self.api.update(PROFIT_TARGET_PERCENT=0.001, PROFIT_BACK_PERCENT=0.001)
        target, stop = self.api['calculate_short_profit_trail'](100, 90)
        self.assertEqual((target, stop), (99.9, 100.5))

    def test_parameter_validation(self):
        self.api['validate_settings']()
        self.api['PROTECT_LOSS_PER'] = self.api['PROTECT_PROFIT_PER']
        with self.assertRaises(ValueError):
            self.api['validate_settings']()
        for name in ('PROTECT_PROFIT_PER', 'PROTECT_LOSS_PER', 'PROFIT_BACK_PERCENT', 'PROFIT_TARGET_PERCENT'):
            with self.subTest(name=name):
                api = load_functions()
                api[name] = 0
                with self.assertRaises(ValueError):
                    api['validate_settings']()


if __name__ == '__main__':
    unittest.main()
