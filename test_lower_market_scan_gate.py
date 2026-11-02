"""Offline checks for the LOWER pre-scan gate; never imports the trading entry point."""
import ast
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from Z_ORB_ONE.strategies.lower import LowerStrategy


class LowerMarketScanGateTests(unittest.TestCase):
    def setUp(self):
        self.strategy = object.__new__(LowerStrategy)
        self.strategy.services = SimpleNamespace(
            MARKET_GATE_INDEX_KEYS=('TWSE:MARKET', 'TPEX:MARKET'),
            get_critical_market_gate_snapshots=Mock(return_value={}),
            trade_log=Mock(),
            now_tpe=lambda: datetime(2026, 11, 2, 9, 30),
        )
        self.good = dict(index_fresh=True, rebound_level_maintained=True,
                         early_breakout_passed=True, previous_close_reversal_blocked=False,
                         symbol='IX0001', previous_close=100, last_index=98, lower_reason='')
        self.strategy.get_market_strategy_decision_gate_result = Mock(return_value=self.good)
        self.strategy.summarize_lower_strategy_decision_candidates = Mock(
            return_value={'passed': True, 'decline_percent': 40})

    def test_both_indices_pass_before_scanning(self):
        result = self.strategy.scan_lower_candidates_if_market_passes({}, None)
        self.assertTrue(result['passed'])
        self.assertEqual(self.strategy.get_market_strategy_decision_gate_result.call_count, 2)
        self.strategy.summarize_lower_strategy_decision_candidates.assert_called_once()

    def test_either_index_failure_prevents_all_stock_requests(self):
        for key, value in [('index_fresh', False), ('rebound_level_maintained', False),
                           ('early_breakout_passed', False), ('previous_close_reversal_blocked', True)]:
            for index in (0, 1):
                with self.subTest(key=key, index=index):
                    results = [dict(self.good), dict(self.good)]
                    results[index][key] = value
                    self.strategy.get_market_strategy_decision_gate_result.side_effect = results
                    result = self.strategy.scan_lower_candidates_if_market_passes({}, None)
                    self.assertTrue(result['scan_skipped'])
                    self.assertEqual(result['quote_request_count'], 0)
                    self.assertFalse(result['passed'])
        self.strategy.summarize_lower_strategy_decision_candidates.assert_not_called()

    def test_early_summary_refreshes_indices_after_scan(self):
        events = []
        self.strategy.scan_lower_candidates_if_market_passes = Mock(
            side_effect=lambda *args: events.append('scan') or {'passed': True})
        self.strategy.services.get_critical_market_gate_snapshots.side_effect = (
            lambda *args: events.append('snapshot') or {'latest': True})
        snapshots, _ = self.strategy.fetch_lower_early_summary({}, None)
        self.assertEqual(events, ['scan', 'snapshot'])
        self.assertEqual(snapshots, {'latest': True})

    def test_recovery_after_skipped_scan_cannot_activate_lower(self):
        self.strategy.config = SimpleNamespace(ENTRY_MODE_NO_TRADE=0,
            LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD=80)
        self.strategy.get_lower_strategy_windows = Mock(return_value=[
            ((9, 30), (9, 30), (9, 40)), ((9, 40), (9, 40), (9, 50))])
        mode, results = self.strategy.decide_entry_mode_by_market_gate(
            {}, market_snapshots={}, lower_decision_summary={
                'scan_skipped': True, 'skip_reason': 'stale', 'decline_percent': 0})
        self.assertEqual(mode, 0)
        self.assertTrue(all(r['decision_status'] == 'WAIT' for r in results))

    def test_entry_point_parses_without_importing_or_running_it(self):
        path = Path(__file__).parent / 'Z_ORB_ONE/execute_strategy_broken_high_falling_singleton_v17.py'
        ast.parse(path.read_text(encoding='utf-8-sig'))


if __name__ == '__main__':
    unittest.main()
