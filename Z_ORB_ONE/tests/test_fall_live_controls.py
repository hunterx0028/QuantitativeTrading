"""實機 FALL 控制流程的離線測試；只使用假行情、假交易服務。"""
import ast
import io
import math
import sys
import unittest
from concurrent.futures import Future
from contextlib import redirect_stdout, nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from strategies.candidate_flow import SharedCandidateFlow
from strategies.fall import FallConfig, find_fall_average_stop


def live_functions():
    path = ROOT / 'execute_strategy_broken_high_falling_singleton_v17.py'
    source = path.read_text(encoding='utf-8')
    compile(source, str(path), 'exec')
    tree = ast.parse(source)
    ns = dict(Dict=Dict, Any=Any, FallConfig=FallConfig, math=math)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            try:
                value = ast.literal_eval(node.value)
            except (TypeError, ValueError):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    ns[target.id] = value
    names = {'get_fall_config', 'validate_fall_config', 'is_fall_strategy',
             'get_optimize_loss_profit_percent', 'get_protect_loss_profit_percent',
             'get_tick_size', 'floor_price_to_tick_value', 'ceil_price_to_tick_value',
             'recalc_entry_position_prices', '_protect_profit_stop', 'reached_stop_to_flat',
             'try_close_position'}
    ns.update(is_avg_strategy=lambda state: bool(state and state.get('strategy_type') == 'AVG'),
              atomic_write_json=lambda *args: None, state_path=lambda symbol: symbol,
              time_tuple_to_minutes=lambda value, name: value[0] * 60 + value[1])
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), 'exec'), ns)
    return ns


class FakeExecutor:
    def __init__(self):
        self.submissions = []

    def submit(self, fn, *args):
        future = Future()
        self.submissions.append((fn, args, future))
        return future


class LiveFallTests(unittest.TestCase):
    def setUp(self):
        self.api = live_functions()
        self.entry = datetime(2026, 11, 4, 10, 0, 40, tzinfo=timezone(timedelta(hours=8)))
        self.now = self.entry.replace(minute=3, second=15)
        self.logs, self.exits = [], []
        self.executor = FakeExecutor()
        self.api.update(parse_avg_candles=lambda data, now: data, now_tpe=lambda: self.now,
                        trade_log=lambda event, **fields: self.logs.append((event, fields)),
                        try_close_position=lambda state, sdk: self.exits.append(state.copy()),
                        AVG_POSITION_QUOTE_EXECUTOR=self.executor,
                        force_close_time_reached=lambda state: False,
                        marketdata_priority=lambda priority: nullcontext())

    def state(self, **overrides):
        state = dict(symbol_code='1234', symbol_code_with_suf='1234.TW', strategy_type='FALL',
                     in_position=True, entry_fill_confirmed=True, entry_time=self.entry.isoformat(),
                     entry_first_fill_time=self.entry.isoformat(), yesterday_close_price=100,
                     limit_up_price=110, limit_down_price=90,
                     side='SHORT', entry_price=100, flat_price=0, last_price=99, symbol_name='test')
        state.update(overrides)
        return state

    def bar(self, minute, high=101, average=100):
        return dict(dt=self.entry.replace(second=0) + timedelta(minutes=minute), high=high, average=average)

    def test_config_validation(self):
        self.api['validate_fall_config']()
        self.api['get_fall_config']()
        self.assertNotIn('FALL_STOP_LOSS_PERCENT', self.api)

    def test_single_deep_low_cannot_confirm(self):
        for low in (99.7, 99.8, 95):
            with self.subTest(low=low):
                activation = self.now.replace(hour=9, minute=14, second=0)
                bars = [dict(dt=activation, low=low)]
                flow = SharedCandidateFlow(self.api)
                state = self.state(in_position=False)
                flow.fall_rows['1234.TW'] = []
                flow.confirm_fall_entry(state, {'data': bars}, activation + timedelta(minutes=1, seconds=15))
                self.assertNotIn('fall_entry_confirmed_time', state)

    def test_consecutive_confirmation(self):
        for previous, lows in ((100, [99.9, 99.9]), (50.1, [49.9, 49.9])):
            with self.subTest(previous=previous):
                start = self.now.replace(hour=9, minute=14, second=0)
                state = self.state(in_position=False, yesterday_close_price=previous)
                bars = [dict(dt=start + timedelta(minutes=i), low=low) for i, low in enumerate(lows)]
                SharedCandidateFlow(self.api).confirm_fall_entry(state, {'data': bars}, start + timedelta(minutes=3))
                self.assertIn('fall_entry_confirmed_time', state)

    def test_average_stop_excludes_entry_and_current_incomplete_bar(self):
        bars = [self.bar(0), self.bar(1, high=100), self.bar(3)]
        self.assertIsNone(find_fall_average_stop(bars, self.entry, self.now))
        signal = find_fall_average_stop(bars + [self.bar(2)], self.entry, self.now)
        self.assertEqual(signal['reference_price'], 100.5)
        self.assertEqual(signal['bar_time'], self.bar(2)['dt'].isoformat())

    def test_average_tick_boundaries_and_invalid_data(self):
        for average, expected in ((100, 100.5), (100.12, 100.5), (50, 50.1), (49.97, 50)):
            with self.subTest(average=average):
                signal = find_fall_average_stop([self.bar(1, high=expected, average=average)], self.entry, self.now)
                self.assertEqual(signal['reference_price'], expected)
        for average in (None, float('nan'), 0):
            self.assertIsNone(find_fall_average_stop([self.bar(1, average=average)], self.entry, self.now))
        duplicate = self.bar(1)
        self.assertIsNone(find_fall_average_stop([duplicate, duplicate], self.entry, self.now))

    def test_stop_scan_continues_after_entry_deadline_and_only_for_filled_fall(self):
        self.now = self.now.replace(hour=12)
        states = {'fall': self.state(), 'avg': self.state(strategy_type='AVG'),
                  'pending': self.state(entry_fill_confirmed=False), 'exiting': self.state(exit_order_pending=True)}
        flow = SharedCandidateFlow(self.api)
        flow.process_fall_average_stops(states, None, None, self.now)
        flow.process_fall_average_stops(states, None, None, self.now)
        self.assertEqual(len(self.executor.submissions), 1)
        self.assertEqual(set(flow.fall_stop_candles), {'fall'})

    def test_completed_scan_triggers_once_and_uses_existing_exit_path(self):
        flow, state = SharedCandidateFlow(self.api), self.state()
        future = Future()
        future.set_result(({'data': [self.bar(1)]}, self.now))
        flow.fall_stop_candles['1234.TW'] = future
        flow.process_fall_average_stops({'1234.TW': state}, None, object(), self.now)
        self.assertTrue(state['fall_average_stop_triggered'])
        self.assertEqual(state['fall_average_stop_price'], 100.5)
        self.assertEqual(len(self.exits), 1)
        flow.process_fall_average_stops({'1234.TW': state}, None, None, self.now)
        self.assertEqual(len(self.exits), 1)
        self.assertEqual(self.logs[-1][0], 'FALL_AVERAGE_STOP_TRIGGERED')

    def test_request_crossing_minute_does_not_use_partial_bar(self):
        flow, state = SharedCandidateFlow(self.api), self.state()
        future = Future()
        requested_at = self.entry.replace(minute=1, second=15)
        future.set_result(({'data': [self.bar(1)]}, requested_at))
        flow.fall_stop_candles['1234.TW'] = future
        flow.process_fall_average_stops({'1234.TW': state}, None, None, self.now)
        self.assertNotIn('fall_average_stop_triggered', state)

    def test_timeout_preserves_position_and_schedules_later_retry(self):
        flow, state = SharedCandidateFlow(self.api), self.state()
        future = Future()
        future.set_exception(TimeoutError('test timeout'))
        flow.fall_stop_candles['1234.TW'] = future
        flow.process_fall_average_stops({'1234.TW': state}, None, None, self.now)
        self.assertTrue(state['in_position'])
        self.assertEqual(self.logs[-1][0], 'FALL_AVERAGE_STOP_ERROR')
        self.assertEqual(len(self.executor.submissions), 1)

    def test_no_fixed_fall_stop_but_profit_protection_still_works(self):
        state = self.state()
        self.api['recalc_entry_position_prices'](state)
        self.assertEqual(state['flat_price'], 0)
        self.assertFalse(self.api['reached_stop_to_flat'](state))
        state['last_price'] = 97.5
        with redirect_stdout(io.StringIO()):
            self.api['_protect_profit_stop'](state)
        self.assertEqual(state['flat_price'], 98.5)
        self.api['recalc_entry_position_prices'](state)
        self.assertEqual(state['flat_price'], 98.5)
        state['last_price'] = 98.5
        self.assertTrue(self.api['reached_stop_to_flat'](state))
        avg = self.state(strategy_type='AVG')
        self.api['recalc_entry_position_prices'](avg)
        self.assertEqual(avg['flat_price'], 102)

    def test_average_stop_submit_respects_pending_and_retry_guards(self):
        api = live_functions()
        calls = []
        api.update(submit_exit_order=lambda state, sdk, reason: calls.append(reason),
                   reached_profitable_limit_price=lambda state: False, force_close_time_reached=lambda state: False)
        state = self.state(fall_average_stop_triggered=True)
        api['try_close_position'](state, None)
        self.assertEqual(calls, ['fall_average_stop'])
        state['exit_order_pending'] = True
        api['try_close_position'](state, None)
        self.assertEqual(len(calls), 1)
        state.update(exit_order_pending=False, exit_retry_reason='fall_average_stop')
        api['try_close_position'](state, None)
        self.assertEqual(calls, ['fall_average_stop', 'fall_average_stop'])


if __name__ == '__main__':
    unittest.main()
