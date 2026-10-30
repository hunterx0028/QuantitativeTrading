"""Lower 策略。設定、當日狀態與共用服務由入口程式明確傳入。"""
from __future__ import annotations

import math
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Tuple, Optional
import requests


@dataclass(frozen=True)
class LowerConfig:
    ENTRY_MODE_LOWER: int
    ENTRY_MODE_NO_TRADE: int
    ENTRY_ORDER_QUANTITY_LOWER: int
    FORCE_CLOSE_TIME_LOWER: tuple[int, int]
    INDUSTRY_MARKET_FILTER_SHORT_PERCENT: int
    IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER: float
    IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER: float
    IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER: float
    IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER: float
    LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD: float
    LOWER_DECISION_DECLINE_PERCENT_THRESHOLD: float
    LOWER_ENTRY_RANGE_END_PERCENT: float
    LOWER_ENTRY_RANGE_START_PERCENT: float
    LOWER_EXIT_CHECK_INTERVAL_MINUTES: int
    LOWER_EXIT_PERCENT_THRESHOLD: float
    LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME: tuple[int, int]
    LOWER_STRATEGY_DECISION_1: tuple[int, int]
    LOWER_STRATEGY_DECISION_2: tuple[int, int]
    LOWER_STRATEGY_DECISION_3: tuple[int, int]
    LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE: tuple[int, int]
    MARKETDATA_PRIORITY_POSITION: int
    MARKET_INDEX_CRITICAL_WAIT_SECONDS: float
    MIN_MINUTE_BARS_BEFORE_0930: int
    STRATEGY_END_LOWER_1: tuple[int, int]
    STRATEGY_END_LOWER_2: tuple[int, int]
    STRATEGY_END_LOWER_3: tuple[int, int]
    STRATEGY_START_LOWER_1: tuple[int, int]
    STRATEGY_START_LOWER_2: tuple[int, int]
    STRATEGY_START_LOWER_3: tuple[int, int]


@dataclass
class LowerRuntime:
    session_state: dict = field(default_factory=lambda: {"activation_dt": None, "window_number": None, "baseline_prices": {}, "next_exit_check_dt": None, "stopped": False})
    minute_futures: dict = field(default_factory=dict)
    quote_futures: dict = field(default_factory=dict)


@dataclass(frozen=True)
class LowerServices:
    """共用行情、交易與持久化服務；不反向匯入主程式。"""
    AVG_POSITION_QUOTE_EXECUTOR: Any
    LOWER_DATA_EXECUTOR: Any
    LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_CHECK_ANNOUNCED_EVENT: Any
    LOWER_QUOTE_EXECUTOR: Any
    MARKETDATA_PRIORITY_CANDIDATE: Any
    MARKETDATA_STOP_EVENT: Any
    MARKET_GATE_INDEX_KEYS: Any
    MARKET_INDEX_FUTURE_TOLERANCE_SECONDS: Any
    MARKET_INDEX_REST_FATAL_EVENT: Any
    MARKET_INDEX_REST_STOP_EVENT: Any
    MARKET_INDEX_STALE_SECONDS: Any
    MARKET_INDEX_STATE: Any
    MARKET_INDEX_STATE_LOCK: Any
    ORDER_STATE_LOCK: Any
    TZ: Any
    atomic_write_json: Any
    calculate_entry_range_bounds: Any
    calculate_percent: Any
    ceil_price_to_tick_value: Any
    close_flat_position: Any
    fetch_fall_quote: Any
    finite_positive: Any
    format_market_gate_percent: Any
    format_market_gate_value: Any
    get_critical_market_gate_snapshots: Any
    get_entry_check_end_time: Any
    get_entry_check_start_time: Any
    get_entry_mode_text: Any
    get_fresh_market_index_value: Any
    get_fresh_market_index_value_from_snapshot: Any
    get_market_gate_snapshots: Any
    get_market_index_snapshot: Any
    get_market_index_snapshot_freshness: Any
    get_market_key_for_symbol: Any
    get_pure_symbol: Any
    get_stock_rest_client: Any
    is_avg_strategy: Any
    is_price_in_entry_range: Any
    market_gate_snapshots_are_fresh: Any
    market_previous_close_indices: Any
    now_tpe: Any
    state_path: Any
    state_symbol_fields: Any
    time_tuple_to_minutes: Any
    total_stocks: Any
    trade_log: Any


@dataclass
class LowerStrategy:
    config: LowerConfig
    runtime: LowerRuntime
    services: LowerServices

    def get_lower_strategy_windows(self) -> list[tuple[tuple[int, int], tuple[int, int], tuple[int, int]]]:
        return [
            (self.config.LOWER_STRATEGY_DECISION_1, self.config.STRATEGY_START_LOWER_1, self.config.STRATEGY_END_LOWER_1),
            (self.config.LOWER_STRATEGY_DECISION_2, self.config.STRATEGY_START_LOWER_2, self.config.STRATEGY_END_LOWER_2),
            (self.config.LOWER_STRATEGY_DECISION_3, self.config.STRATEGY_START_LOWER_3, self.config.STRATEGY_END_LOWER_3),
        ]

    def get_active_lower_window(self) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
        window_number = self.runtime.session_state.get("window_number")
        if window_number is None:
            raise ValueError("LOWER 尚未啟動，沒有可用的進場時間窗")
        return self.get_lower_strategy_windows()[window_number - 1]

    def activate_lower_session(self, window_number: int, decision_dt: datetime, summary: Dict[str, Any]) -> None:
        if self.runtime.session_state.get("activation_dt") is not None or self.runtime.session_state.get("stopped"):
            raise ValueError("LOWER 已啟動或已停止，不可再次啟動")
        self.runtime.session_state.update({
            "activation_dt": decision_dt,
            "window_number": window_number,
            "baseline_prices": dict(summary["prices"]),
            "next_exit_check_dt": decision_dt + timedelta(minutes=self.config.LOWER_EXIT_CHECK_INTERVAL_MINUTES),
        })

    def validate_market_reversal_time_config(self) -> None:
        if type(self.config.ENTRY_ORDER_QUANTITY_LOWER) is not int or self.config.ENTRY_ORDER_QUANTITY_LOWER != 1:
            raise ValueError("LOWER 目前每檔只允許一張")
        if self.config.MIN_MINUTE_BARS_BEFORE_0930 <= 0:
            raise ValueError("MIN_MINUTE_BARS_BEFORE_0930 必須大於 0")
        if self.config.LOWER_EXIT_CHECK_INTERVAL_MINUTES <= 0:
            raise ValueError("LOWER_EXIT_CHECK_INTERVAL_MINUTES 必須大於 0")
        previous_decision_hm = -1
        force_close_hm = self.services.time_tuple_to_minutes(self.config.FORCE_CLOSE_TIME_LOWER, "FORCE_CLOSE_TIME_LOWER")
        for window_number, (decision, start, end) in enumerate(self.get_lower_strategy_windows(), start=1):
            decision_hm = self.services.time_tuple_to_minutes(decision, f"LOWER_STRATEGY_DECISION_{window_number}")
            start_hm = self.services.time_tuple_to_minutes(start, f"STRATEGY_START_LOWER_{window_number}")
            end_hm = self.services.time_tuple_to_minutes(end, f"STRATEGY_END_LOWER_{window_number}")
            if not previous_decision_hm < decision_hm < start_hm < end_hm < force_close_hm:
                raise ValueError(f"LOWER 第{window_number}組需符合前組決策 < 決策 < 開始 < 截止 < 平倉時間")
            previous_decision_hm = decision_hm
        if not 0 <= self.config.LOWER_EXIT_PERCENT_THRESHOLD <= 100:
            raise ValueError("LOWER_EXIT_PERCENT_THRESHOLD 必須介於 0 與 100")
        lower_strategy_early_breakout_deadline_hm = (
            self.config.LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
            + self.config.LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
        )
        lower_market_previous_close_reversal_start_hm = (
            self.config.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
            + self.config.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
        )
        lower_strategy_decision_hm = self.config.LOWER_STRATEGY_DECISION_1[0] * 60 + self.config.LOWER_STRATEGY_DECISION_1[1]
        if not (0 <= lower_strategy_early_breakout_deadline_hm <= 23 * 60 + 59):
            raise ValueError(
                "LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE 設定錯誤，需介於 00:00~23:59"
            )
        if not (0 <= lower_market_previous_close_reversal_start_hm <= 23 * 60 + 59):
            raise ValueError(
                "LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 設定錯誤，需介於 00:00~23:59"
            )
        if lower_strategy_early_breakout_deadline_hm >= lower_strategy_decision_hm:
            raise ValueError(
                "LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE 必須早於 LOWER_STRATEGY_DECISION_1"
            )
        if lower_market_previous_close_reversal_start_hm > lower_strategy_early_breakout_deadline_hm:
            raise ValueError(
                "LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME "
                "不可晚於 LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE"
            )
        if lower_market_previous_close_reversal_start_hm >= lower_strategy_decision_hm:
            raise ValueError(
                "LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 必須早於 LOWER_STRATEGY_DECISION_1"
            )
        if not (
            0 <= self.config.LOWER_DECISION_DECLINE_PERCENT_THRESHOLD
            < self.config.LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD <= 100
        ):
            raise ValueError(
                "lower 股票比例門檻須符合 0 <= "
                "LOWER_DECISION_DECLINE_PERCENT_THRESHOLD < "
                "LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD <= 100"
            )
        if self.config.INDUSTRY_MARKET_FILTER_SHORT_PERCENT < 0:
            raise ValueError("INDUSTRY_MARKET_FILTER_SHORT_PERCENT 不可小於 0")
        self.services.time_tuple_to_minutes(
            self.config.STRATEGY_START_LOWER_1,
            "STRATEGY_START_LOWER_1",
        )

    def get_strategy_decision_drop_percent(self, index_key: str) -> float | None:
        if index_key == "TWSE:MARKET":
            return self.config.IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER
        if index_key == "TPEX:MARKET":
            return self.config.IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER
        return None

    def get_strategy_decision_rebound_percent(self, index_key: str) -> float | None:
        if index_key == "TWSE:MARKET":
            return self.config.IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER
        if index_key == "TPEX:MARKET":
            return self.config.IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER
        return None

    def update_market_strategy_decision_gate_state(self, market_key: str, index_value: float, event_time: Any) -> None:
        if market_key not in self.services.MARKET_GATE_INDEX_KEYS:
            return

        now_local = self.services.now_tpe()
        if (
            self.runtime.session_state.get("activation_dt") is not None
            or self.runtime.session_state.get("stopped")
            or (now_local.hour, now_local.minute) >= self.get_lower_strategy_windows()[-1][0]
        ):
            return

        index_config = self.services.market_previous_close_indices.get(market_key, {})
        drop_percent = self.get_strategy_decision_drop_percent(market_key)
        previous_close = index_config.get("previous_close")
        try:
            previous_close_float = float(previous_close)
        except (TypeError, ValueError):
            return
        if not self.services.finite_positive(previous_close_float) or not self.services.finite_positive(index_value):
            return

        now_hm = now_local.hour * 60 + now_local.minute
        drop_threshold = (
            previous_close_float * (1 - drop_percent / 100.0)
            if drop_percent is not None
            else None
        )
        lower_strategy_early_breakout_deadline_hm = (
            self.config.LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
            + self.config.LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
        )
        lower_market_previous_close_reversal_start_hm = (
            self.config.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
            + self.config.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
        )
        with self.services.MARKET_INDEX_STATE_LOCK:
            market_state = self.services.MARKET_INDEX_STATE.setdefault(market_key, {})
            if (now_local.hour, now_local.minute) >= self.config.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME:
                if not self.services.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_CHECK_ANNOUNCED_EVENT.is_set():
                    self.services.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_CHECK_ANNOUNCED_EVENT.set()
                    print(
                        f"⏰ {now_local.strftime('%H:%M:%S')} 已到市場指數上下穿越檢核時間，"
                        "開始檢核上市及上櫃指數是否穿越昨收"
                    )

                if index_value > previous_close_float:
                    market_state["previous_close_traded_above"] = True
                elif index_value < previous_close_float:
                    market_state["previous_close_traded_below"] = True

                if (
                    market_state.get("previous_close_traded_above")
                    and market_state.get("previous_close_traded_below")
                    and not market_state.get("previous_close_reversal_blocked")
                ):
                    market_state["previous_close_reversal_blocked"] = True
                    market_state["previous_close_reversal_time"] = event_time or now_local.isoformat()
                    print(
                        f"[MODE] {now_local.strftime('%H:%M:%S')} {market_key} 指數已檢核到上下穿越昨收，"
                        "將於下一次 LOWER 決策判定為 NO_TRADE"
                    )

            if (
                now_hm >= lower_market_previous_close_reversal_start_hm
                and now_hm <= lower_strategy_early_breakout_deadline_hm
                and not market_state.get("early_breakout_passed")
                and drop_threshold is not None
                and index_value < drop_threshold
            ):
                market_state["early_breakout_passed"] = True
                market_state["early_breakout_side"] = "DOWN"
                market_state["early_breakout_time"] = event_time or now_local.isoformat()
                market_state["early_breakout_index"] = index_value
                market_state["early_breakout_change_percent"] = (
                    (index_value - previous_close_float) / previous_close_float * 100.0
                )

    def get_market_strategy_decision_gate_result(self,
        index_key: str,
        market_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        index_config = self.services.market_previous_close_indices.get(index_key, {})
        drop_percent = self.get_strategy_decision_drop_percent(index_key)
        rebound_percent = self.get_strategy_decision_rebound_percent(index_key)
        previous_close = index_config.get("previous_close")
        market_state = self.services.get_market_index_snapshot(index_key) if market_state is None else dict(market_state)
        market_index_fresh, market_index_age = self.services.get_market_index_snapshot_freshness(index_key, market_state)

        result = {
            "index_key": index_key,
            "symbol": index_config.get("symbol"),
            "name": index_config.get("name"),
            "previous_close": previous_close,
            "last_index": market_state.get("last_index"),
            "current_change_percent": None,
            "provider_last_updated": market_state.get("last_updated"),
            "received_at": market_state.get("received_at"),
            "drop_threshold": None,
            "rebound_threshold": None,
            "previous_close_traded_above": bool(market_state.get("previous_close_traded_above")),
            "previous_close_traded_below": bool(market_state.get("previous_close_traded_below")),
            "previous_close_reversal_blocked": bool(market_state.get("previous_close_reversal_blocked")),
            "previous_close_reversal_time": market_state.get("previous_close_reversal_time"),
            "early_breakout_passed": bool(market_state.get("early_breakout_passed")),
            "early_breakout_side": market_state.get("early_breakout_side"),
            "early_breakout_time": market_state.get("early_breakout_time"),
            "index_fresh": market_index_fresh,
            "index_age_seconds": market_index_age,
            "rebound_level_maintained": False,
            "lower_reason": "",
        }

        if not index_config.get("symbol"):
            result["lower_reason"] = "stock_data.py 缺少市場指數設定"
            return result

        try:
            previous_close_float = float(previous_close)
        except (TypeError, ValueError):
            result["lower_reason"] = "previous_close 無效"
            return result

        if previous_close_float <= 0:
            result["lower_reason"] = "門檻設定無效"
            return result

        if drop_percent is not None:
            result["drop_threshold"] = previous_close_float * (1 - drop_percent / 100.0)
        if rebound_percent is not None:
            result["rebound_threshold"] = previous_close_float * (1 - rebound_percent / 100.0)
        try:
            raw_last_index_float = float(market_state.get("last_index"))
            result["current_change_percent"] = (
                (raw_last_index_float - previous_close_float) / previous_close_float * 100.0
            )
        except (TypeError, ValueError):
            pass

        last_index_float, market_index_age, market_index_error = self.services.get_fresh_market_index_value_from_snapshot(
            index_key,
            market_state,
        )
        result["index_age_seconds"] = market_index_age
        if last_index_float is None:
            result["lower_reason"] = market_index_error
            return result

        if drop_percent is None or rebound_percent is None:
            result["lower_reason"] = "LOWER 門檻設定無效"
        elif last_index_float >= result["rebound_threshold"]:
            result["lower_reason"] = "決策時已反彈至失效門檻"
        else:
            result["rebound_level_maintained"] = True
            result["lower_reason"] = "決策時仍維持在反彈失效門檻以下"

        return result

    def summarize_lower_strategy_decision_candidates(self,
        states: Dict[str, Dict[str, Any]],
        realtime_sdk: Any,
        completed_trade_matches: Optional[Dict[str, bool]] = None,
        current_prices: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """以 total_stocks 有效報價計算下降比例，並保留同一輪的啟動基準價格。"""
        candidate_count = 0
        decline_count = 0
        data_missing_count = 0

        seen_symbols = set()
        completed_trade_matches = completed_trade_matches or {}
        current_prices = current_prices or {}
        states_by_symbol = {state.get("symbol_code_with_suf"): state for state in states.values()}
        quote_request_count = 0
        prices = {}
        for stock_item in self.services.total_stocks:
            symbol_name = stock_item[0]
            _, symbol_code = self.services.get_pure_symbol(symbol_name)
            if symbol_code in seen_symbols:
                continue
            seen_symbols.add(symbol_code)
            if symbol_code in completed_trade_matches:
                candidate_count += 1
                decline_count += int(completed_trade_matches[symbol_code])
                continue

            try:
                yesterday_close = float(stock_item[5])
                state = states_by_symbol.get(symbol_code)
                if state is not None and state.get("limit_down_price"):
                    limit_down = float(state["limit_down_price"])
                else:
                    # 僅供 LOWER 比例判斷：依昨收九折並向上對齊 tick 推算跌停價。
                    limit_down = self.services.ceil_price_to_tick_value(yesterday_close * 0.90)
                if symbol_code in current_prices:
                    last_price = float(current_prices[symbol_code])
                else:
                    quote_request_count += 1
                    quote = self.services.get_stock_rest_client(realtime_sdk).intraday.quote(symbol=symbol_code.split(".")[0])
                    last_price = float(quote.get("lastPrice"))
            except Exception as exc:
                data_missing_count += 1
                print(f"[LOWER_PERCENT] total_stocks {symbol_name} 取價資料不足：{exc!r}")
                continue

            if not all(math.isfinite(value) and value > 0 for value in (yesterday_close, limit_down, last_price)):
                data_missing_count += 1
                continue
            candidate_count += 1
            prices[symbol_code] = last_price

            entry_lower_bound, entry_upper_bound = self.services.calculate_entry_range_bounds(
                yesterday_close,
                limit_down,
                self.config.LOWER_ENTRY_RANGE_START_PERCENT,
                self.config.LOWER_ENTRY_RANGE_END_PERCENT,
            )
            if self.services.is_price_in_entry_range(last_price, entry_lower_bound, entry_upper_bound):
                decline_count += 1

        valid_count = candidate_count
        decline_percent = self.services.calculate_percent(decline_count, candidate_count)
        return {
            "candidate_count": candidate_count,
            "valid_count": valid_count,
            "decline_count": decline_count,
            "decline_percent": decline_percent,
            "data_missing_count": data_missing_count,
            "source_count": len(seen_symbols),
            "quote_request_count": quote_request_count,
            "prices": prices,
            "threshold": self.config.LOWER_DECISION_DECLINE_PERCENT_THRESHOLD,
            "max_threshold": self.config.LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD,
            "passed": (
                candidate_count > 0
                and self.config.LOWER_DECISION_DECLINE_PERCENT_THRESHOLD
                < decline_percent
                < self.config.LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD
            ),
        }

    def summarize_lower_price_movements(self,
        states: Dict[str, Dict[str, Any]],
        realtime_sdk: Any,
        baseline_prices: Dict[str, float],
        current_prices: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """停利成交者排除、停損成交者算上漲，其餘與啟動價格比較。"""
        with self.services.ORDER_STATE_LOCK:
            completed_trade_matches = {
                state["symbol_code_with_suf"]: state.get("exit_reason") == "profit"
                for state in states.values()
                if (
                    state.get("exit_fully_filled")
                    and state.get("exit_fill_confirmed")
                    and state.get("exit_reason") in ("profit", "stop")
                )
            }
        up_count = flat_count = down_count = data_missing_count = quote_request_count = 0
        current_prices = current_prices or {}
        for symbol, baseline_price in baseline_prices.items():
            if self.services.MARKETDATA_STOP_EVENT.is_set():
                break
            if symbol in completed_trade_matches:
                if not completed_trade_matches[symbol]:
                    up_count += 1
                continue
            try:
                if symbol in current_prices:
                    price = float(current_prices[symbol])
                else:
                    quote_request_count += 1
                    quote = self.services.get_stock_rest_client(realtime_sdk).intraday.quote(symbol=symbol.split(".")[0])
                    price = float(quote.get("lastPrice"))
                if not math.isfinite(price) or price <= 0:
                    raise ValueError("報價無效")
            except Exception as exc:
                data_missing_count += 1
                self.services.trade_log("LOWER_PERCENT_QUOTE_ERROR", symbol=symbol, error_msg=repr(exc), action="exclude_from_denominator")
                continue
            if price > baseline_price:
                up_count += 1
            elif price < baseline_price:
                down_count += 1
            else:
                flat_count += 1
        total = up_count + flat_count + down_count
        return {
            "candidate_count": total,
            "up_count": up_count,
            "flat_count": flat_count,
            "down_count": down_count,
            "up_percent": self.services.calculate_percent(up_count, total),
            "flat_percent": self.services.calculate_percent(flat_count, total),
            "down_percent": self.services.calculate_percent(down_count, total),
            "data_missing_count": data_missing_count,
            "quote_request_count": quote_request_count,
        }

    def get_due_lower_exit_check(self, now_local: datetime) -> Optional[datetime]:
        next_check = self.runtime.session_state.get("next_exit_check_dt")
        if next_check is None or now_local < next_check:
            return None
        interval = timedelta(minutes=self.config.LOWER_EXIT_CHECK_INTERVAL_MINUTES)
        check_dt = next_check + ((now_local - next_check) // interval) * interval
        self.runtime.session_state["next_exit_check_dt"] = check_dt + interval
        if (check_dt.hour, check_dt.minute) >= self.config.FORCE_CLOSE_TIME_LOWER:
            return None
        return check_dt

    def check_lower_percent_exit(self,
        states: Dict[str, Dict[str, Any]],
        realtime_sdk: Any,
        current_prices: Optional[Dict[str, float]] = None,
        check_dt: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        summary = self.summarize_lower_price_movements(
            states, realtime_sdk, self.runtime.session_state["baseline_prices"], current_prices,
        )
        if not summary["candidate_count"] or summary["up_percent"] <= self.config.LOWER_EXIT_PERCENT_THRESHOLD:
            return None
        trigger_time = check_dt or self.services.now_tpe()
        summary.update({
            "exit_threshold": self.config.LOWER_EXIT_PERCENT_THRESHOLD,
            "trigger_time": trigger_time.isoformat(),
        })
        print(
            f'{trigger_time:%H:%M} [母數={summary["candidate_count"]}  '
            f'+{summary["up_percent"]:.2f}% {summary["flat_percent"]:.2f}% '
            f'-{summary["down_percent"]:.2f}%]-平倉'
        )
        return summary

    def handle_lower_percent_exit(self,
        states: Dict[str, Dict[str, Any]],
        mysdk,
        summary: Dict[str, Any],
    ) -> None:
        """封鎖當日 LOWER 新進場，待入場成交確認後平倉並持續重試失敗委託。"""
        self.runtime.session_state["stopped"] = True
        for state in states.values():
            if state.get("traded"):
                continue
            try:
                state.update({
                    "lower_percent_trigger_dt": summary["trigger_time"],
                    "lower_percent_trigger_percent": summary["up_percent"],
                    "lower_percent_exit_threshold": summary["exit_threshold"],
                })
                if state.get("entry_order_pending") or state.get("exit_order_pending"):
                    self.services.atomic_write_json(self.services.state_path(state.get("symbol_code_with_suf", "")), state)
                    continue
                if state.get("in_position"):
                    if state.get("entry_fill_confirmed"):
                        self.services.close_flat_position(state, mysdk, "lower_percent")
                else:
                    state["traded"] = True
                    state["exit_reason"] = "lower_percent"
                self.services.atomic_write_json(self.services.state_path(state.get("symbol_code_with_suf", "")), state)
            except Exception as exc:
                self.services.trade_log(
                    "LOWER_PERCENT_EXIT_ERROR", error=True,
                    **self.services.state_symbol_fields(state), error_msg=repr(exc), action="retry_next_loop",
                )

    def format_lower_decision_summary(self, summary: Dict[str, Any]) -> str:
        return (
            f"數量門檻={self.config.LOWER_DECISION_DECLINE_PERCENT_THRESHOLD:.2f}% < 比例 "
            f"< {self.config.LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD:.2f}% "
            f"候選={summary.get('candidate_count', 0)} "
            f"下降={summary.get('decline_count', 0)} "
            f"資料不足={summary.get('data_missing_count', 0)} "
            f"報價查詢={summary.get('quote_request_count', 0)} "
            f"下降比例={float(summary.get('decline_percent', 0.0)):.2f}%"
        )

    def decide_entry_mode_by_market_gate(self,
        states: Dict[str, Dict[str, Any]],
        market_snapshots: Optional[Dict[str, Dict[str, Any]]] = None,
        realtime_sdk: Optional[Any] = None,
        window_number: int = 1,
        lower_decision_summary: Optional[Dict[str, Any]] = None,
    ) -> tuple[int, list[Dict[str, Any]]]:
        if lower_decision_summary is None:
            # 保留同步工具呼叫介面；實機 monitor 僅傳入已完成的背景結果。
            lower_decision_summary = self.summarize_lower_strategy_decision_candidates(states, realtime_sdk)
            market_snapshots = self.services.get_critical_market_gate_snapshots(f"lower_decision_{window_number}_after_scan")
        elif market_snapshots is None:
            market_snapshots, _ = self.services.get_market_gate_snapshots()
        gate_results = [
            self.get_market_strategy_decision_gate_result(index_key, market_snapshots.get(index_key))
            for index_key in self.services.MARKET_GATE_INDEX_KEYS
        ]
        for result in gate_results:
            result["lower_decision_summary"] = lower_decision_summary
            result["window_number"] = window_number
            result["decision_status"] = "STOP"

        now_local = self.services.now_tpe()
        if (now_local.hour, now_local.minute) > self.get_lower_strategy_windows()[window_number - 1][2]:
            for result in gate_results:
                result["decision_status"] = "WAIT" if window_number < len(self.get_lower_strategy_windows()) else "STOP"
                result["decision_reason"] = "股票池掃描完成時已超過本組進場截止時間"
            return self.config.ENTRY_MODE_NO_TRADE, gate_results

        if lower_decision_summary["decline_percent"] > self.config.LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD:
            for result in gate_results:
                result["decision_reason"] = (
                    "原因=LOWER 股票下跌百分超出候選範圍 "
                    f"( > {self.config.LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD:g}%)"
                )
            return self.config.ENTRY_MODE_NO_TRADE, gate_results

        data_issue_results = [
            result
            for result in gate_results
            if (
                not result.get("symbol")
                or result.get("previous_close") in (None, "")
                or result.get("last_index") is None
                or not result.get("index_fresh")
            )
        ]
        if data_issue_results:
            for result in data_issue_results:
                print(
                    f"[MODE] {result['index_key']} 市場指數資料不足，"
                    f"lower_reason={result.get('lower_reason') or '--'}"
                )
            print("[MODE] 市場指數資料不足，保守判定為 NO_TRADE")
            return self.config.ENTRY_MODE_NO_TRADE, gate_results

        def fallback_no_trade(reason: str) -> tuple[int, list[Dict[str, Any]]]:
            print(f"[MODE] {reason}，判定為 NO_TRADE")
            return self.config.ENTRY_MODE_NO_TRADE, gate_results

        early_breakout_failed = [
            result for result in gate_results
            if not result.get("early_breakout_passed")
        ]
        if early_breakout_failed:
            return self.config.ENTRY_MODE_NO_TRADE, gate_results

        reversal_blocked = any(result["previous_close_reversal_blocked"] for result in gate_results)
        if reversal_blocked:
            decision_time = self.get_lower_strategy_windows()[window_number - 1][0]
            print(
                "[MODE] 上市或上櫃指數在指定時段曾位於昨收上下兩側 "
                f"({self.config.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0]:02d}:"
                f"{self.config.LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]:02d}~"
                f"{decision_time[0]:02d}:{decision_time[1]:02d})"
            )
            return fallback_no_trade("上市或上櫃指數在指定時段曾位於昨收上下兩側")

        lower_mode_passed = all(result["rebound_level_maintained"] for result in gate_results)

        if lower_mode_passed:
            if not lower_decision_summary["passed"]:
                decision_status = "WAIT" if window_number < len(self.get_lower_strategy_windows()) else "STOP"
                for result in gate_results:
                    result["decision_status"] = decision_status
                    result["decision_reason"] = (
                        "原因=LOWER 股票比例不符 "
                        f"({self.config.LOWER_DECISION_DECLINE_PERCENT_THRESHOLD:g}% < 比例 "
                        f"< {self.config.LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD:g}%)"
                    )
                print(
                    "[MODE] LOWER 個股池下降比例未落在允許範圍："
                    f"{self.format_lower_decision_summary(lower_decision_summary)}，"
                    + ("等待下次決策" if decision_status == "WAIT" else "本日無後續決策機會")
                )
                return self.config.ENTRY_MODE_NO_TRADE, gate_results
            for result in gate_results:
                result["decision_status"] = "ACTIVE"
            return self.config.ENTRY_MODE_LOWER, gate_results
        return fallback_no_trade("LOWER 未成立")

    def fetch_lower_early_summary(self, states_snapshot, realtime_sdk):
        snapshots = self.services.get_critical_market_gate_snapshots('lower_strategy_early_breakout_deadline_summary')
        return snapshots, self.summarize_lower_strategy_decision_candidates(states_snapshot, realtime_sdk)

    def lower_minute_checks_complete(self, states):
        return all(state.get('traded') or state.get('minute_bars_checked') for state in states.values())

    def poll_lower_decision_inputs(self, future, window_number, wait_started):
        """只消費完整掃描；新鮮大盤等待採非阻塞輪詢，保留原有最長等待時間。"""
        if not future.done():
            return None, wait_started
        summary = future.result()
        snapshots, _ = self.services.get_market_gate_snapshots()
        expired = (self.services.now_tpe().hour, self.services.now_tpe().minute) > self.get_lower_strategy_windows()[window_number - 1][2]
        if (expired or self.services.market_gate_snapshots_are_fresh(snapshots)
                or self.services.MARKET_INDEX_REST_FATAL_EVENT.is_set() or self.services.MARKET_INDEX_REST_STOP_EVENT.is_set()):
            return (summary, snapshots), None
        now_monotonic = time.monotonic()
        if wait_started is None:
            self.services.trade_log('LOWER_DECISION_WAIT_MARKET_DATA', window_number=window_number,
                      timeout_seconds=self.config.MARKET_INDEX_CRITICAL_WAIT_SECONDS)
            wait_started = now_monotonic
        if now_monotonic - wait_started >= self.config.MARKET_INDEX_CRITICAL_WAIT_SECONDS:
            return (summary, snapshots), None
        return None, wait_started

    def print_lower_strategy_early_breakout_deadline_summary(self,
        market_snapshots: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        """早盤突破截止分鐘結束後，列印並記錄兩指數的完整觸發結果。"""
        market_snapshots = market_snapshots or {}
        deadline_text = (
            f"{self.config.LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[0]:02d}:"
            f"{self.config.LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[1]:02d}"
        )
        print(f"[MODE] 早盤 LOWER 突破截止結果（截止分鐘 {deadline_text}，含）")

        for index_key in self.services.MARKET_GATE_INDEX_KEYS:
            index_config = self.services.market_previous_close_indices.get(index_key, {})
            market_state = dict(market_snapshots.get(index_key) or self.services.get_market_index_snapshot(index_key))
            drop_percent = self.get_strategy_decision_drop_percent(index_key)
            previous_close = index_config.get("previous_close")
            passed = bool(market_state.get("early_breakout_passed"))
            _, age_seconds, data_error = self.services.get_fresh_market_index_value_from_snapshot(
                index_key,
                market_state,
            )
            raw_last_index = market_state.get("last_index")

            try:
                previous_close_float = float(previous_close)
            except (TypeError, ValueError):
                previous_close_float = None

            threshold = None
            latest_change_percent = None
            if (
                previous_close_float is not None
                and previous_close_float > 0
                and drop_percent is not None
            ):
                threshold = previous_close_float * (1 - drop_percent / 100.0)
                try:
                    display_last_index = float(raw_last_index)
                except (TypeError, ValueError):
                    display_last_index = None
                if display_last_index is not None:
                    latest_change_percent = (
                        (display_last_index - previous_close_float) / previous_close_float * 100.0
                    )
            else:
                display_last_index = None

            print(
                f"[MODE] {index_key} {index_config.get('symbol') or ''} "
                f"價位門檻={self.services.format_market_gate_value(threshold)}"
                f"({self.services.format_market_gate_percent(drop_percent, threshold_drop=True)}) "
                f"當前值={self.services.format_market_gate_value(display_last_index)}"
                f"({self.services.format_market_gate_percent(latest_change_percent)}) "
                f"早盤突破={'YES' if passed else 'NO'}"
            )
            if data_error:
                self.services.trade_log(
                    "MARKET_INDEX_BREAKOUT_DATA_ERROR",
                    error=True,
                    index_key=index_key,
                    symbol=index_config.get("symbol"),
                    stored_index=raw_last_index,
                    provider_last_updated=market_state.get("last_updated"),
                    received_at=market_state.get("received_at"),
                    index_age_seconds=age_seconds,
                    stale_limit_seconds=self.services.MARKET_INDEX_STALE_SECONDS,
                    future_tolerance_seconds=self.services.MARKET_INDEX_FUTURE_TOLERANCE_SECONDS,
                    reason=data_error,
                    action="diagnostic_only_breakout_state_remains_fail_closed",
                )

    def print_entry_mode_decision(self,
        entry_mode: int,
        gate_results: list[Dict[str, Any]],
        states: Dict[str, Dict[str, Any]],
    ) -> None:
        mode_text = self.services.get_entry_mode_text(entry_mode)
        lower_decision_summary = next(
            (result["lower_decision_summary"] for result in gate_results if "lower_decision_summary" in result),
            None,
        )
        for result in gate_results:
            drop_percent = self.get_strategy_decision_drop_percent(result["index_key"])
            rebound_percent = self.get_strategy_decision_rebound_percent(result["index_key"])
            if not result.get("index_fresh"):
                rebound_maintained_text = "N/A"
            elif result.get("rebound_level_maintained"):
                rebound_maintained_text = "YES"
            else:
                rebound_maintained_text = "NO"
            print(
                f"[MODE] {result['index_key']} {result.get('symbol') or ''} "
                f"價位門檻={self.services.format_market_gate_value(result.get('drop_threshold'))}"
                f"({self.services.format_market_gate_percent(drop_percent, threshold_drop=True)}) "
                f"反彈門檻={self.services.format_market_gate_value(result.get('rebound_threshold'))}"
                f"({self.services.format_market_gate_percent(rebound_percent, threshold_drop=True)}) "
                f"當前值={self.services.format_market_gate_value(result.get('last_index'))}"
                f"({self.services.format_market_gate_percent(result.get('current_change_percent'))}) "
                f"早盤突破={'YES' if result.get('early_breakout_passed') else 'NO'} "
                f"決策維持={rebound_maintained_text}"
            )
            if not result.get("index_fresh") or result.get("last_index") is None:
                self.services.trade_log(
                    "MARKET_INDEX_DECISION_DATA_ERROR",
                    error=True,
                    index_key=result["index_key"],
                    symbol=result.get("symbol"),
                    stored_index=result.get("last_index"),
                    provider_last_updated=result.get("provider_last_updated"),
                    received_at=result.get("received_at"),
                    index_age_seconds=result.get("index_age_seconds"),
                    stale_limit_seconds=self.services.MARKET_INDEX_STALE_SECONDS,
                    future_tolerance_seconds=self.services.MARKET_INDEX_FUTURE_TOLERANCE_SECONDS,
                    reason=result.get("lower_reason") or "市場指數資料不足",
                    action="fail_closed",
                )
        if lower_decision_summary is not None:
            window_number = gate_results[0].get("window_number", 1)
            print(
                f'時間{window_number}:[下降={lower_decision_summary["decline_count"]}  '
                f'母數={lower_decision_summary["candidate_count"]}  '
                f'{lower_decision_summary["decline_percent"]:.2f}%]'
                + ("-進場" if entry_mode == self.config.ENTRY_MODE_LOWER else "")
            )
            reason = gate_results[0].get("decision_reason")
            if reason:
                print(reason)
        else:
            print("[MODE] LOWER 指數條件未通過，未查詢 total_stocks 比例")
        print(f'[MODE] 時間{gate_results[0].get("window_number", 1)} 模式判斷：{mode_text} 狀態={gate_results[0].get("decision_status", "STOP")}')

    def lower_industry_market_filter_pass(self, state: Dict[str, Any]) -> bool:
        market_key = state.get("market_index_key")
        if not market_key:
            market_key = self.services.get_market_key_for_symbol(
                state.get("symbol_code_with_suf", ""),
                state.get("industry_code", ""),
            )

        index_config = self.services.market_previous_close_indices.get(market_key, {})
        previous_close = index_config.get("previous_close")
        last_index_float, _age_seconds, stale_reason = self.services.get_fresh_market_index_value(market_key)
        if last_index_float is None:
            print(f"[{state['symbol_name']}] 產業別盤勢濾網等待 {market_key} 指數資料：{stale_reason}")
            return False

        try:
            previous_close_float = float(previous_close)
        except (TypeError, ValueError):
            print(f"[{state['symbol_name']}] 產業別盤勢濾網 {market_key} 昨收指數設定錯誤: {previous_close}")
            return False

        if previous_close_float <= 0:
            print(f"[{state['symbol_name']}] 產業別盤勢濾網 {market_key} 昨收指數設定錯誤: {previous_close}")
            return False

        threshold = previous_close_float * (1 - self.config.INDUSTRY_MARKET_FILTER_SHORT_PERCENT / 100.0)
        if last_index_float >= threshold:
            index_name = index_config.get("name", "")
            print(
                f"[{state['symbol_name']}] 產業別盤勢濾網未通過：{market_key} {index_name} "
                f"指數 {last_index_float:.2f} >= 門檻 {threshold:.2f}"
            )
            return False

        return True

    def format_industry_market_filter_pass_text(self, state: Dict[str, Any]) -> str:
        market_key = state.get("market_index_key")
        if not market_key:
            market_key = self.services.get_market_key_for_symbol(
                state.get("symbol_code_with_suf", ""),
                state.get("industry_code", ""),
            )

        index_config = self.services.market_previous_close_indices.get(market_key, {})
        previous_close = index_config.get("previous_close")
        last_index_float, _age_seconds, _stale_reason = self.services.get_fresh_market_index_value(market_key)
        if last_index_float is None:
            return ""

        try:
            previous_close_float = float(previous_close)
        except (TypeError, ValueError):
            return ""

        if not self.services.finite_positive(previous_close_float) or not self.services.finite_positive(last_index_float):
            return ""

        index_name = index_config.get("name", "")
        threshold = previous_close_float * (1 - self.config.INDUSTRY_MARKET_FILTER_SHORT_PERCENT / 100.0)
        operator = "<"

        return (
            f"（產業別盤勢濾網：{market_key} {index_name} "
            f"指數 {last_index_float:.2f} {operator} 門檻 {threshold:.2f}）"
        )

    def check_lower_minute_bar_eligibility(self,
        states: Dict[str, Dict[str, Any]], realtime_sdk: Any,
    ) -> None:
        """09:30 後背景取分K；只有主迴圈可套用資格結果，每檔最多一次在途。"""
        now_local = self.services.now_tpe()
        if (now_local.hour, now_local.minute) < (9, 30):
            return
        for symbol, (future, state) in list(self.runtime.minute_futures.items()):
            if not future.done():
                continue
            self.runtime.minute_futures.pop(symbol)
            if states.get(symbol) is not state or state.get('traded') or self.services.is_avg_strategy(state):
                continue
            try:
                count = future.result()
                error_msg = ''
            except Exception as exc:
                count, error_msg = 0, repr(exc)
            eligible = not error_msg and count >= self.config.MIN_MINUTE_BARS_BEFORE_0930
            state["minute_bars_checked"] = True
            state["minute_bars_eligible"] = bool(eligible)
            state["minute_bars_before_0930"] = count
            if not eligible:
                state["traded"] = True
                state["exit_reason"] = "insufficient_minute_bars_before_0930"
            self.services.atomic_write_json(self.services.state_path(state["symbol_code_with_suf"]), state)
            self.services.trade_log(
                "LOWER_MINUTE_BARS_CHECK", **self.services.state_symbol_fields(state),
                count=count, required=self.config.MIN_MINUTE_BARS_BEFORE_0930,
                action="eligible" if eligible else "block_today", error_msg=error_msg,
            )
        for symbol, state in states.items():
            if (state.get('minute_bars_checked') or state.get('traded') or self.services.is_avg_strategy(state)
                    or symbol in self.runtime.minute_futures):
                continue
            self.runtime.minute_futures[symbol] = (
                self.services.LOWER_DATA_EXECUTOR.submit(self.fetch_lower_minute_count, state['symbol_code'], realtime_sdk, now_local.date()), state)

    def fetch_lower_minute_count(self, symbol, realtime_sdk, target_date):
        response = self.services.get_stock_rest_client(realtime_sdk).intraday.candles(symbol=symbol)
        candles = response.get('data')
        if not isinstance(candles, list):
            raise ValueError('分K回應缺少 data 陣列')
        minute_keys = set()
        for candle in candles:
            try:
                dt = datetime.fromisoformat(str(candle['date']).replace('Z', '+00:00'))
                dt = self.services.TZ.localize(dt) if dt.tzinfo is None else dt.astimezone(self.services.TZ)
                if (dt.date() == target_date and (9, 0) <= (dt.hour, dt.minute) < (9, 30)
                        and all(self.services.finite_positive(candle[field]) for field in ('open', 'high', 'low', 'close'))):
                    minute_keys.add((dt.hour, dt.minute))
            except (KeyError, TypeError, ValueError):
                continue
        return len(minute_keys)

    def schedule_lower_quotes(self, states, realtime_sdk, *, request_quotes):
        for symbol, (future, owner) in list(self.runtime.quote_futures.items()):
            if states.get(symbol) is not owner or owner.get('traded') or self.services.is_avg_strategy(owner):
                future.cancel()
                self.runtime.quote_futures.pop(symbol)
        if not request_quotes:
            return
        for symbol, state in states.items():
            if state.get('traded') or self.services.is_avg_strategy(state) or symbol in self.runtime.quote_futures:
                continue
            priority = self.config.MARKETDATA_PRIORITY_POSITION if state.get('in_position') else self.services.MARKETDATA_PRIORITY_CANDIDATE
            executor = self.services.AVG_POSITION_QUOTE_EXECUTOR if state.get('in_position') else self.services.LOWER_QUOTE_EXECUTOR
            self.runtime.quote_futures[symbol] = (executor.submit(self.services.fetch_fall_quote, symbol, realtime_sdk, priority), state)

    def entry_lower_mode_price_check(self, state: Dict[str, Any]) -> bool | str:
        """
        lower 模式進場條件判斷；成立時以當下 last_price 寫入 entry_trigger_price。

        回傳值：
          True      — 條件成立，應進場（side 由呼叫端設定）
          'BLOCKED' — 驗證失敗，需永久封鎖本日進場（呼叫端負責設 traded=True）
          False     — 尚未觸發，繼續等待下一輪
        """
        now_local = self.services.now_tpe()
        if self.runtime.session_state.get("stopped") or self.runtime.session_state.get("activation_dt") is None:
            return False
        if not state.get("minute_bars_eligible"):
            return False
        if state.get("symbol_code_with_suf") not in self.runtime.session_state["baseline_prices"]:
            return False
        if (now_local.hour, now_local.minute) < self.services.get_entry_check_start_time():
            return False
        if (now_local.hour, now_local.minute) > self.services.get_entry_check_end_time(state):
            return False

        yesterday_close_price = state.get("yesterday_close_price")
        limit_down_price = state.get("limit_down_price")
        if yesterday_close_price is None or limit_down_price is None:
            return False

        last_price = state.get("last_price", 0)
        if last_price is None:
            return False

        try:
            yesterday_close = float(yesterday_close_price)
            limit_down = float(limit_down_price)
            last_px = float(last_price)
        except (TypeError, ValueError):
            return False

        if last_px <= 0:
            return False

        entry_lower_bound, entry_upper_bound = self.services.calculate_entry_range_bounds(
            yesterday_close,
            limit_down,
            self.config.LOWER_ENTRY_RANGE_START_PERCENT,
            self.config.LOWER_ENTRY_RANGE_END_PERCENT,
        )
        if not self.services.is_price_in_entry_range(last_px, entry_lower_bound, entry_upper_bound):
            return False

        if not self.lower_industry_market_filter_pass(state):
            return False

        state["entry_trigger_price"] = last_px
        return True

    def has_unfinished_lower_states(self, states: Dict[str, Dict[str, Any]]) -> bool:
        return any(
            not state.get("traded")
            for state in states.values()
        )

    def mark_unentered_lower_states_blocked(self, states: Dict[str, Dict[str, Any]], reason: str) -> None:
        for state in states.values():
            if (
                state.get("traded")
                or state.get("in_position")
                or state.get("entry_order_pending")
                or state.get("exit_order_pending")
            ):
                continue
            state["traded"] = True
            state["entry_time"] = self.services.now_tpe().isoformat()
            state["exit_reason"] = reason
            self.services.atomic_write_json(self.services.state_path(state.get("symbol_code_with_suf", "")), state)
