"""Avg 策略。設定、當日狀態與共用服務由入口程式明確傳入。"""
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
class AvgConfig:
    AVG_CANDLES_START_TIME: tuple[int, int]
    AVG_CANDLE_POLL_SECOND: int
    AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS: float
    AVG_ENTRY_DEADLINE: tuple[int, int]
    AVG_ENTRY_ORDER_QUANTITY: int
    AVG_ENTRY_START_TIME: tuple[int, int]
    AVG_FORCE_CLOSE_TIME: tuple[int, int]
    AVG_MAX_NORMALIZED_SLOPE: float
    AVG_MIN_MINUTE_BARS_BEFORE_0930: int
    AVG_MIN_PRE_ENTRY_RANGE_PERCENT: float
    AVG_PROFIT_BACK_PERCENT: float
    AVG_PROFIT_TARGET_PERCENT: float
    AVG_PROTECT_LOSS_PER: float
    AVG_PROTECT_PROFIT_PER: float
    AVG_SLOPE_CUTOFF_TIME: tuple[int, int]
    AVG_STAGE_1_AMPLITUDE_PERCENT: float
    AVG_STAGE_1_MIN_BARS: int
    AVG_STAGE_1_START_TIME: tuple[int, int]
    AVG_STAGE_2_AMPLITUDE_PERCENT: float
    AVG_STAGE_2_MIN_BARS: int
    AVG_STAGE_3_AMPLITUDE_PERCENT: float
    AVG_STAGE_3_MIN_BARS: int
    AVG_STAGE_4_MIN_BARS: int
    AVG_STOCK_DATA_SOURCE: str
    AVG_STOP_LOSS_PERCENT: float
    AVG_TAKE_PROFIT_PERCENT: float
    INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE: int
    MARKETDATA_PRIORITY_POSITION: int
    STRATEGY_AVG: str
    STRATEGY_START_LOWER_3: tuple[int, int]
    TRADE_SIDE_SHORT: str


@dataclass
class AvgRuntime:
    session_state: dict = field(default_factory=lambda: {"active": False, "last_scan_minute_key": None, "candidate_count": 0})
    next_request_monotonic: float = 0.0
    request_schedule_lock: Any = field(default_factory=threading.Lock)
    candle_pending_futures: dict = field(default_factory=dict)
    position_quote_futures: dict = field(default_factory=dict)


@dataclass(frozen=True)
class AvgServices:
    """共用行情、交易與持久化服務；不反向匯入主程式。"""
    AVG_CANDLE_EXECUTOR: Any
    AVG_POSITION_QUOTE_EXECUTOR: Any
    MARKET_INDEX_REST_FATAL_EVENT: Any
    TZ: Any
    TradeApiBusy: Any
    atomic_write_json: Any
    ceil_price_to_tick_value: Any
    floor_price_to_tick_value: Any
    get_realtime_price: Any
    get_stock_rest_client: Any
    initialize_states: Any
    now_tpe: Any
    selected_stocks: Any
    state_path: Any
    state_symbol_fields: Any
    time_tuple_to_minutes: Any
    total_stocks: Any
    trade_log: Any
    try_open_position: Any


@dataclass
class AvgStrategy:
    config: AvgConfig
    runtime: AvgRuntime
    services: AvgServices

    def get_avg_candidate_stocks(self) -> list:
        """不可將 LOWER 初始化過濾後的清單作為 AVG 股票池。"""
        if self.config.AVG_STOCK_DATA_SOURCE == "T":
            return list(self.services.total_stocks)
        if self.config.AVG_STOCK_DATA_SOURCE == "S":
            return list(self.services.selected_stocks)
        raise ValueError('AVG_STOCK_DATA_SOURCE 必須為 "T" 或 "S"')

    def validate_avg_config(self) -> None:
        if self.config.AVG_STOCK_DATA_SOURCE not in ("T", "S"):
            raise ValueError('AVG_STOCK_DATA_SOURCE 必須為 "T" 或 "S"')
        for name in ("AVG_STAGE_1_START_TIME", "AVG_CANDLES_START_TIME", "AVG_SLOPE_CUTOFF_TIME",
                     "AVG_ENTRY_START_TIME", "AVG_ENTRY_DEADLINE", "AVG_FORCE_CLOSE_TIME"):
            self.services.time_tuple_to_minutes(getattr(self.config, name), name)
        entry_start_minutes = self.services.time_tuple_to_minutes(self.config.AVG_ENTRY_START_TIME, "AVG_ENTRY_START_TIME")
        entry_deadline_minutes = self.services.time_tuple_to_minutes(self.config.AVG_ENTRY_DEADLINE, "AVG_ENTRY_DEADLINE")
        force_close_minutes = self.services.time_tuple_to_minutes(self.config.AVG_FORCE_CLOSE_TIME, "AVG_FORCE_CLOSE_TIME")
        candles_start_minutes = self.services.time_tuple_to_minutes(self.config.AVG_CANDLES_START_TIME, "AVG_CANDLES_START_TIME")
        lower_3_start_minutes = self.services.time_tuple_to_minutes(self.config.STRATEGY_START_LOWER_3, "STRATEGY_START_LOWER_3")
        stage_1_start_minutes = self.services.time_tuple_to_minutes(self.config.AVG_STAGE_1_START_TIME, "AVG_STAGE_1_START_TIME")
        slope_cutoff_minutes = self.services.time_tuple_to_minutes(self.config.AVG_SLOPE_CUTOFF_TIME, "AVG_SLOPE_CUTOFF_TIME")
        if not entry_start_minutes <= entry_deadline_minutes < force_close_minutes:
            raise ValueError("AVG 需符合最早入場 <= 截止入場 < 強制平倉")
        if not lower_3_start_minutes <= candles_start_minutes <= entry_start_minutes:
            raise ValueError("AVG 分K開始時間須介於第三組 LOWER 開始時間與 AVG 最早入場時間之間")
        if stage_1_start_minutes > entry_deadline_minutes or slope_cutoff_minutes > entry_deadline_minutes:
            raise ValueError("AVG 階段起算及斜率檢核時間不可晚於入場截止")
        for value in (self.config.AVG_STAGE_1_MIN_BARS, self.config.AVG_STAGE_2_MIN_BARS, self.config.AVG_STAGE_3_MIN_BARS,
                      self.config.AVG_STAGE_4_MIN_BARS, self.config.AVG_MIN_MINUTE_BARS_BEFORE_0930):
            if type(value) is not int or value <= 0:
                raise ValueError("AVG 分K根數必須為正整數")
        if type(self.config.AVG_ENTRY_ORDER_QUANTITY) is not int or self.config.AVG_ENTRY_ORDER_QUANTITY != 1:
            raise ValueError("AVG 目前每檔只允許一張")
        if type(self.config.AVG_CANDLE_POLL_SECOND) is not int or not 0 <= self.config.AVG_CANDLE_POLL_SECOND <= 59:
            raise ValueError("AVG_CANDLE_POLL_SECOND 必須介於 0~59")
        if (
            not math.isfinite(self.config.AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS)
            or self.config.AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS <= 0
            or len(self.get_avg_candidate_stocks()) * self.config.AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS
            >= 60.0
        ):
            raise ValueError("AVG 股票池無法在 60 秒輪詢週期內完成一次節流查詢")
        projected_candle_requests = len(self.get_avg_candidate_stocks())
        if projected_candle_requests > self.config.INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE - 20:
            raise ValueError(
                f"AVG 分K輪詢預估 {projected_candle_requests:.0f} 次／分鐘，"
                f"須低於行情 API 額度並保留 20 次／分鐘餘裕"
            )
        if not math.isfinite(self.config.AVG_MAX_NORMALIZED_SLOPE):
            raise ValueError("AVG 斜率門檻必須為有限數值")
        for value in (self.config.AVG_STOP_LOSS_PERCENT, self.config.AVG_TAKE_PROFIT_PERCENT, self.config.AVG_PROTECT_PROFIT_PER,
                      self.config.AVG_PROTECT_LOSS_PER, self.config.AVG_PROFIT_BACK_PERCENT, self.config.AVG_PROFIT_TARGET_PERCENT):
            if not math.isfinite(value) or not 0 < value < 100:
                raise ValueError("AVG 風控百分比必須介於 0 與 100")
        for value in (self.config.AVG_STAGE_1_AMPLITUDE_PERCENT, self.config.AVG_STAGE_2_AMPLITUDE_PERCENT,
                      self.config.AVG_STAGE_3_AMPLITUDE_PERCENT, self.config.AVG_MIN_PRE_ENTRY_RANGE_PERCENT):
            if not math.isfinite(value) or not 0 <= value < 100:
                raise ValueError("AVG 振幅百分比必須介於 0（含）與 100（不含）")

    def is_avg_strategy(self, state: Dict[str, Any] | None) -> bool:
        return bool(state and state.get("strategy_type") == self.config.STRATEGY_AVG)

    def parse_avg_candles(self, raw_candles: Any, now_local: datetime) -> list[Dict[str, Any]]:
        """解析當日 1 分K；策略階段只使用已完成 K 棒，下一根 open 僅用來確認入場時點。"""
        if not isinstance(raw_candles, list):
            raise ValueError("intraday.candles 回應缺少 data 陣列")
        bars = []
        for raw in raw_candles:
            try:
                candle_dt = datetime.fromisoformat(str(raw["date"]).replace("Z", "+00:00"))
                candle_dt = self.services.TZ.localize(candle_dt) if candle_dt.tzinfo is None else candle_dt.astimezone(self.services.TZ)
                if candle_dt.date() != now_local.date():
                    continue
                prices = {key: float(raw[key]) for key in ("open", "high", "low", "close")}
                try:
                    average = float(raw.get("average"))
                    if not math.isfinite(average) or average <= 0:
                        average = None
                except (TypeError, ValueError, OverflowError):
                    average = None
                if (
                    any(not math.isfinite(value) or value <= 0 for value in prices.values())
                    or prices["low"] > min(prices["open"], prices["close"])
                    or prices["high"] < max(prices["open"], prices["close"])
                ):
                    continue
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
            bars.append({"dt": candle_dt, **prices, "average": average})
        bars.sort(key=lambda bar: bar["dt"])
        return bars

    def calculate_avg_normalized_slope(self, bars: list[Dict[str, Any]], previous_close: float) -> Optional[float]:
        samples = []
        for bar in bars:
            dt = bar["dt"]
            if bar["average"] is None:
                continue
            if not (9 * 60 <= dt.hour * 60 + dt.minute < self.config.AVG_SLOPE_CUTOFF_TIME[0] * 60 + self.config.AVG_SLOPE_CUTOFF_TIME[1]):
                continue
            samples.append((dt.hour * 60 + dt.minute + dt.second / 60.0, bar["average"]))
        if len(samples) < 2 or not math.isfinite(previous_close) or previous_close <= 0:
            return None
        x_mean = sum(x for x, _ in samples) / len(samples)
        y_mean = sum(y for _, y in samples) / len(samples)
        denominator = sum((x - x_mean) ** 2 for x, _ in samples)
        if denominator <= 0:
            return None
        slope = sum((x - x_mean) * (y - y_mean) for x, y in samples) / denominator
        return slope / previous_close * 100.0

    def find_avg_entry_signal(self, bars: list[Dict[str, Any]], now_local: datetime) -> Optional[Dict[str, Any]]:
        """與 v18 find_entry 對齊：low>avg、high<avg、low>avg，之後連續 low<avg。"""
        current_minute = now_local.replace(second=0, microsecond=0)
        completed = [bar for bar in bars if bar["dt"] < current_minute]
        requirements = (self.config.AVG_STAGE_1_MIN_BARS, self.config.AVG_STAGE_2_MIN_BARS, self.config.AVG_STAGE_3_MIN_BARS)
        amplitudes = (self.config.AVG_STAGE_1_AMPLITUDE_PERCENT, self.config.AVG_STAGE_2_AMPLITUDE_PERCENT,
                      self.config.AVG_STAGE_3_AMPLITUDE_PERCENT)
        stage = count = 0
        amplitude_reached = False
        for index, bar in enumerate(completed):
            minute = (bar["dt"].hour, bar["dt"].minute)
            if minute < self.config.AVG_STAGE_1_START_TIME:
                continue
            if minute > self.config.AVG_ENTRY_DEADLINE:
                break
            average = bar["average"]
            if average is None:
                count = 0
                amplitude_reached = False
                continue
            if stage == 3:
                if minute < self.config.AVG_SLOPE_CUTOFF_TIME:
                    continue
                count = count + 1 if bar["low"] < average else 0
                if count >= self.config.AVG_STAGE_4_MIN_BARS:
                    if index + 1 >= len(bars):
                        return None
                    entry_bar = bars[index + 1]
                    entry_time = (entry_bar["dt"].hour, entry_bar["dt"].minute)
                    if not self.config.AVG_ENTRY_START_TIME <= entry_time <= self.config.AVG_ENTRY_DEADLINE:
                        return {"rejected": "first_signal_outside_entry_window", "signal_bar": bar}
                    return {"entry_bar": entry_bar, "signal_bar": bar}
                continue
            matched = bar["high"] < average if stage == 1 else bar["low"] > average
            if not matched:
                count = 0
                amplitude_reached = False
                continue
            count += 1
            avg_dec = Decimal(str(average))
            percent = Decimal(str(amplitudes[stage])) / Decimal("100")
            if stage == 1:
                reached = Decimal(str(bar["low"])) < avg_dec * (1 - percent)
            else:
                reached = Decimal(str(bar["high"])) > avg_dec * (1 + percent)
            amplitude_reached = amplitude_reached or reached
            if count >= requirements[stage] and amplitude_reached:
                stage += 1
                count = 0
                amplitude_reached = False
        return None

    def mark_avg_state_traded(self, state: Dict[str, Any], reason: str, *, error: bool = False, **fields: Any) -> None:
        state["traded"] = True
        state["entry_time"] = self.services.now_tpe().isoformat()
        state["exit_reason"] = reason
        self.services.atomic_write_json(self.services.state_path(state.get("symbol_code_with_suf", "")), state)
        self.services.trade_log("AVG_CANDIDATE_SKIPPED", error=error, **self.services.state_symbol_fields(state), reason=reason, **fields)

    def fetch_avg_candle_response(self, state: Dict[str, Any], realtime_sdk: Any) -> Dict[str, Any]:
        with self.runtime.request_schedule_lock:
            delay = self.runtime.next_request_monotonic - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            self.runtime.next_request_monotonic = time.monotonic() + self.config.AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS

        return self.services.get_stock_rest_client(realtime_sdk).intraday.candles(
            symbol=state["symbol_code"], timeframe="1", sort="asc",
        )

    def process_avg_candle_state(self,
        state: Dict[str, Any], response: Dict[str, Any], mysdk: Any, now_local: datetime,
    ) -> None:
        # AVG 進場委託送出後，成交及持倉風控交由主監控迴圈的即時報價處理。
        if state.get("in_position") or state.get("entry_order_pending") or state.get("exit_order_pending"):
            return
        if state.get("traded") and not (state.get("in_position") or state.get("entry_order_pending")):
            return
        try:
            bars = self.parse_avg_candles(response.get("data"), now_local)
            if not bars:
                raise ValueError("API 未回傳當日有效分K")
        except Exception as exc:
            self.services.trade_log("AVG_CANDLES_ERROR", error=True, **self.services.state_symbol_fields(state), error_msg=repr(exc))
            if not state.get("in_position") and not state.get("entry_order_pending"):
                self.mark_avg_state_traded(state, "avg_candles_unavailable", error=True, error_msg=repr(exc))
            return

        current = bars[-1]
        state["open_price"] = bars[0]["open"]
        state["high_price"] = max(bar["high"] for bar in bars)
        state["low_price"] = min(bar["low"] for bar in bars)
        state["close_price"] = current["close"]
        state["avg_price"] = current["average"]
        state["pre_last_price"] = state.get("last_price", 0)
        state["pre_last_price_time"] = state.get("last_price_time")
        state["last_price"] = current["close"]
        state["last_price_time"] = now_local.isoformat()

        before_0930 = sum(1 for bar in bars if (bar["dt"].hour, bar["dt"].minute) < (9, 30))
        state["minute_bars_before_0930"] = before_0930
        state["minute_bars_checked"] = True
        state["minute_bars_eligible"] = before_0930 >= self.config.AVG_MIN_MINUTE_BARS_BEFORE_0930

        if not state["minute_bars_eligible"]:
            self.mark_avg_state_traded(state, "insufficient_minute_bars_before_0930", count=before_0930)
            return
        slope = self.calculate_avg_normalized_slope(bars, float(state.get("yesterday_close_price", 0) or 0))
        if slope is None or not slope < self.config.AVG_MAX_NORMALIZED_SLOPE:
            self.mark_avg_state_traded(state, "avg_slope_rejected", normalized_slope=slope)
            return
        signal = self.find_avg_entry_signal(bars, now_local)
        if signal and signal.get("rejected"):
            self.mark_avg_state_traded(state, signal["rejected"])
            return
        if signal:
            if self.services.MARKET_INDEX_REST_FATAL_EVENT.is_set():
                self.mark_avg_state_traded(state, "market_index_rest_fatal")
                return
            entry_bar = signal["entry_bar"]
            signal_bar = signal["signal_bar"]
            entry_dt = entry_bar["dt"]
            entry_hm = (entry_dt.hour, entry_dt.minute)
            if not self.config.AVG_ENTRY_START_TIME <= entry_hm <= self.config.AVG_ENTRY_DEADLINE:
                self.mark_avg_state_traded(state, "first_signal_outside_entry_window", entry_time=entry_dt.isoformat())
                return
            if entry_dt != now_local.replace(second=0, microsecond=0):
                self.mark_avg_state_traded(state, "avg_entry_signal_stale", entry_time=entry_dt.isoformat())
                return
            entry_price = float(entry_bar["open"])
            prior_bars = [bar for bar in bars if bar["dt"] < entry_dt]
            previous_close = float(state.get("yesterday_close_price", 0) or 0)
            if not prior_bars or previous_close <= 0:
                self.mark_avg_state_traded(state, "avg_pre_entry_data_missing")
                return
            pre_entry_range_dec = Decimal(str(max(bar["high"] for bar in prior_bars))) - Decimal(
                str(min(bar["low"] for bar in prior_bars))
            )
            pre_entry_range = float(pre_entry_range_dec)
            if pre_entry_range_dec <= Decimal(str(previous_close)) * Decimal(str(self.config.AVG_MIN_PRE_ENTRY_RANGE_PERCENT)) / 100:
                self.mark_avg_state_traded(state, "avg_pre_entry_range_rejected", range=pre_entry_range)
                return
            limit_up = float(state.get("limit_up_price", 0) or 0)
            limit_down = float(state.get("limit_down_price", 0) or 0)
            if any(bar["high"] >= limit_up or bar["low"] <= limit_down for bar in prior_bars):
                self.mark_avg_state_traded(state, "limit_price_touched_before_entry")
                return
            raw_stop = entry_price * (1 + self.config.AVG_STOP_LOSS_PERCENT / 100.0)
            stop_price = min(self.services.ceil_price_to_tick_value(raw_stop), limit_up)
            if entry_price + entry_price * self.config.AVG_STOP_LOSS_PERCENT / 100.0 >= limit_up:
                self.mark_avg_state_traded(state, "avg_stop_loss_too_close_to_limit_up")
                return
            take_profit = max(self.services.floor_price_to_tick_value(entry_price * (1 - self.config.AVG_TAKE_PROFIT_PERCENT / 100.0)), limit_down)
            state["side"] = self.config.TRADE_SIDE_SHORT
            state["entry_signal_time"] = entry_dt.isoformat()
            state["entry_signal_price"] = entry_price
            state["entry_trigger_price"] = current["close"]
            state["last_price"] = current["close"]
            state["avg_signal_bar_time"] = signal_bar["dt"].isoformat()
            state["avg_normalized_slope"] = slope
            state["avg_pre_entry_range"] = pre_entry_range
            state["avg_initial_stop_price"] = stop_price
            state["avg_initial_take_profit_price"] = take_profit
            self.services.try_open_position(state, mysdk)
            return
        if (now_local.hour, now_local.minute) > self.config.AVG_ENTRY_DEADLINE:
            self.mark_avg_state_traded(state, "avg_entry_deadline_passed")
            return
        self.services.atomic_write_json(self.services.state_path(state.get("symbol_code_with_suf", "")), state)

    def process_avg_candle_round(self, states: Dict[str, Dict[str, Any]], realtime_sdk: Any, mysdk: Any) -> None:
        # 先處理已完成的請求；未完成請求留在背景執行，不阻塞即時報價與持倉風控。
        had_pending_requests = bool(self.runtime.candle_pending_futures)
        for future, state in list(self.runtime.candle_pending_futures.items()):
            if not future.done():
                continue
            self.runtime.candle_pending_futures.pop(future, None)
            try:
                self.process_avg_candle_state(state, future.result(), mysdk, self.services.now_tpe())
            except self.services.TradeApiBusy:
                # 未送出委託；保留完成的資料，當分鐘內下輪重試，不永久封鎖候選。
                self.runtime.candle_pending_futures[future] = state
            except requests.RequestException as exc:
                self.services.trade_log(
                    "AVG_CANDLES_REQUEST_RETRY",
                    error=True,
                    **self.services.state_symbol_fields(state),
                    error_msg=repr(exc),
                    action="keep_candidate_for_next_poll",
                )
            except Exception as exc:
                self.services.trade_log("AVG_CANDIDATE_PROCESS_ERROR", error=True, **self.services.state_symbol_fields(state), error_msg=repr(exc))
                if not state.get("in_position") and not state.get("entry_order_pending"):
                    self.mark_avg_state_traded(state, "avg_candidate_processing_error", error=True, error_msg=repr(exc))

        if self.runtime.candle_pending_futures:
            return

        # 整輪處理完才統計，讓本輪斜率剔除與其他候選狀態變化一併反映。
        if had_pending_requests:
            monitoring_count = sum(
                1 for state in states.values()
                if not state.get("traded")
                and not state.get("in_position")
                and not state.get("entry_order_pending")
                and not state.get("exit_order_pending")
            )
            print(
                f"[AVG_MONITORING_SUMMARY] time={self.services.now_tpe().strftime('%H:%M:%S')} "
                f"本輪分K處理完成，目前每分鐘監看候選股票={monitoring_count} 檔；"
                f"預估候選分K查詢約 {monitoring_count} 次／分鐘"
            )

        now_local = self.services.now_tpe()
        if (now_local.hour, now_local.minute) < self.config.AVG_CANDLES_START_TIME:
            return
        if now_local.second < self.config.AVG_CANDLE_POLL_SECOND:
            return
        scan_minute_key = now_local.strftime("%Y-%m-%d %H:%M")
        if self.runtime.session_state["last_scan_minute_key"] == scan_minute_key:
            return
        self.runtime.session_state["last_scan_minute_key"] = scan_minute_key
        pending_states = [
            state for state in states.values()
            if not state.get("traded")
            and not state.get("in_position")
            and not state.get("entry_order_pending")
            and not state.get("exit_order_pending")
        ]
        for state in pending_states:
            future = self.services.AVG_CANDLE_EXECUTOR.submit(self.fetch_avg_candle_response, state, realtime_sdk)
            self.runtime.candle_pending_futures[future] = state

    def prepare_avg_states(self, realtime_sdk):
        self.validate_avg_config()
        return self.services.initialize_states(self.get_avg_candidate_stocks(), realtime_sdk,
                                 strategy_type=self.config.STRATEGY_AVG, force_rebuild_state=True)

    def schedule_avg_position_quotes(self, states: Dict[str, Dict[str, Any]], realtime_sdk, *, request_quotes: bool) -> None:
        # 每檔最多保留一筆在途請求，慢回應時不重複排入工作池。
        for symbol, future in list(self.runtime.position_quote_futures.items()):
            state = states.get(symbol)
            if state is None or state.get("traded") or not state.get("in_position"):
                future.cancel()
                self.runtime.position_quote_futures.pop(symbol, None)
        if not request_quotes:
            return
        for symbol, state in states.items():
            if (not self.is_avg_strategy(state) or state.get("traded")
                    or not state.get("in_position") or symbol in self.runtime.position_quote_futures):
                continue
            self.runtime.position_quote_futures[symbol] = self.services.AVG_POSITION_QUOTE_EXECUTOR.submit(
                self.services.get_realtime_price, symbol, realtime_sdk, priority=self.config.MARKETDATA_PRIORITY_POSITION,
            )
