"""FALL 策略邏輯；行情服務與執行資源由主程式傳入。"""

import math
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal


@dataclass
class FallRuntime:
    """單次執行的 FALL 狀態；由主程式建立並管理執行緒生命週期。

    共享交易狀態的存取仍須使用主程式的 ORDER_STATE_LOCK。
    reset 只供啟動前使用，不負責取消在途工作或處理盤中重啟。
    """

    states: dict = field(default_factory=dict)
    candidate_symbols: set = field(default_factory=set)
    screen_pending_symbols: set = field(default_factory=set)
    screen_queue: list = field(default_factory=list)
    screen_futures: dict = field(default_factory=dict)
    quote_futures: dict = field(default_factory=dict)
    quote_snapshots: dict = field(default_factory=dict)
    quote_poller_stop_event: threading.Event = field(default_factory=threading.Event)
    last_quote_round: int | None = None
    session_state: dict = field(default_factory=lambda: {"screen_started": False, "screen_complete": False})

    def reset(self):
        for collection in (self.states, self.candidate_symbols, self.screen_pending_symbols,
                           self.screen_queue, self.screen_futures, self.quote_futures, self.quote_snapshots):
            collection.clear()
        self.quote_poller_stop_event.clear()
        self.last_quote_round = None
        self.session_state.update(screen_started=False, screen_complete=False)

    def initialize_universe(self, rows, get_pure_symbol):
        seen = set()
        for row in rows:
            symbol = get_pure_symbol(row[0])[1]
            if symbol not in seen:
                seen.add(symbol)
                self.screen_queue.append(row)
                self.screen_pending_symbols.add(symbol)
        return len(seen)

    def work_pending(self):
        return (not self.session_state["screen_complete"] or bool(self.screen_futures)
                or any(not s.get("traded") or s.get("in_position") or s.get("entry_order_pending")
                       or s.get("exit_order_pending") for s in list(self.states.values())))

    def register_candidate(
        self, row, signal, ticker, states, *, get_pure_symbol, all_order_states,
        finite_positive, build_initial_state, entry_order_quantity, strategy_type,
        trade_side_short, order_state_lock, refresh_active_order_states, persist_state, trade_log,
    ):
        """先保留股票，再判斷資格；以共用交易鎖註冊唯一交易狀態。"""
        symbol = get_pure_symbol(row[0])[1]
        self.candidate_symbols.add(symbol)
        trade_log("FALL_RESERVED", symbol=symbol, **signal)
        existing = all_order_states(states).get(symbol, {})
        eligibility = evaluate_fall_candidate(row, ticker, existing, finite_positive=finite_positive)
        if eligibility == "already_used":
            trade_log("FALL_ALREADY_USED", symbol=symbol, action="keep_existing_trade_no_fall_entry")
            return
        if eligibility == "ticker_error":
            trade_log("FALL_INELIGIBLE", error=True, symbol=symbol, reason=ticker.get("error"))
            return
        if eligibility == "ineligible":
            trade_log("FALL_INELIGIBLE", symbol=symbol, action="reserved_no_entry")
            return
        up, down, _can_trade, _disposition = ticker
        name, _qty, v1, v2, v3, v4, industry, _volatility, streak = row
        state = build_initial_state(name, entry_order_quantity, v1, v2, v3, v4,
                                    str(industry).zfill(2), "", up, down, *streak, strategy_type)
        state.update(signal, fall_candidate=True, minute_bars_eligible=True, side=trade_side_short)
        with order_state_lock:
            self.states[symbol] = state
            refresh_active_order_states(states)
        persist_state(symbol, state)

    def process_position_round(
        self, mysdk, *, now_local, entry_deadline, order_state_lock,
        monotonic, quote_round_interval_seconds, force_close_time_reached,
        close_position, check_open_status, persist_state, apply_quote, trade_log,
    ):
        """主監控執行緒每輪處理持倉與快照；交易和寫檔交由共用服務執行。"""
        for symbol, state in list(self.states.items()):
            try:
                if state.get("in_position") and (force_close_time_reached(state) or state.get("exit_retry_reason")):
                    close_position(state, mysdk)
                if state.get("traded"):
                    continue
                if ((now_local.hour, now_local.minute) > entry_deadline and check_open_status(state)):
                    state.update(traded=True, exit_reason="fall_entry_deadline_passed")
                    persist_state(symbol, state)
                    continue
                with order_state_lock:
                    snapshot = self.quote_snapshots.pop(symbol, None)
                if snapshot is not None:
                    quote, requested_at = snapshot
                    if monotonic() - requested_at <= quote_round_interval_seconds:
                        apply_quote(state, quote, mysdk)
            except Exception as exc:
                trade_log("FALL_MONITOR_ERROR", error=True, symbol=symbol, error_msg=repr(exc))

    def process_screen_round(
        self, states, realtime_sdk, *, now_local, config, get_pure_symbol,
        register_candidate, screen_executor, fetch_screen_result, trade_log,
    ):
        """排程篩選與消費結果；候選歸屬及交易狀態註冊仍由主程式負責。"""
        activation = now_local.replace(hour=config.signal_end_time[0], minute=config.signal_end_time[1],
                                       second=0, microsecond=0) + timedelta(
                                           minutes=1, seconds=config.candle_publish_delay_seconds)
        if now_local >= activation:
            self.session_state["screen_started"] = True
        for future, row in list(self.screen_futures.items()):
            if not future.done():
                continue
            self.screen_futures.pop(future)
            symbol = get_pure_symbol(row[0])[1]
            try:
                signal, ticker = future.result()
                if signal:
                    register_candidate(row, signal, ticker, states)
            except Exception as exc:
                trade_log("FALL_SCREEN_ERROR", error=True, symbol=symbol, error_msg=repr(exc), action="skip_no_signal")
            finally:
                self.screen_pending_symbols.discard(symbol)
        if (now_local.hour, now_local.minute) > config.entry_deadline:
            self.screen_queue.clear()
            for future in self.screen_futures:
                future.cancel()  # 已執行者保留，尚未開始的篩選不再發 API。
            self.screen_pending_symbols.intersection_update(
                get_pure_symbol(row[0])[1] for row in self.screen_futures.values())
        if self.session_state["screen_started"]:
            while self.screen_queue:
                row = self.screen_queue.pop(0)
                self.screen_futures[screen_executor.submit(fetch_screen_result, row, realtime_sdk)] = row
            if not self.screen_queue and not self.screen_futures and not self.session_state["screen_complete"]:
                self.session_state["screen_complete"] = True
                trade_log("FALL_SCREEN_COMPLETE", candidates=len(self.candidate_symbols), monitor_count=len(self.states))

    def poll_quotes_once(
        self, realtime_sdk, *, now, order_state_lock, entry_deadline,
        quote_round_interval_seconds, fetch_quote, position_executor, candidate_executor,
        position_priority, candidate_priority, trade_log,
    ):
        """消費行情結果並排程下一輪；不得在行情執行緒送單或修改交易狀態。"""
        for symbol, future in list(self.quote_futures.items()):
            if not future.done():
                continue
            self.quote_futures.pop(symbol)
            try:
                snapshot = future.result()
            except Exception as exc:
                trade_log("FALL_QUOTE_ERROR", error=True, symbol=symbol, error_msg=repr(exc))
            else:
                with order_state_lock:
                    self.quote_snapshots[symbol] = snapshot
        now_local = now()
        round_key = int((now_local.timestamp() - 5) // quote_round_interval_seconds)
        if round_key == self.last_quote_round:
            return
        self.last_quote_round = round_key
        with order_state_lock:
            candidates = list(self.states.items())
        for symbol, state in candidates:
            if state.get("traded") and not state.get("in_position"):
                continue
            if not state.get("in_position") and (now_local.hour, now_local.minute) > entry_deadline:
                continue
            if symbol not in self.quote_futures:
                priority = position_priority if state.get('in_position') else candidate_priority
                executor = position_executor if state.get('in_position') else candidate_executor
                self.quote_futures[symbol] = executor.submit(fetch_quote, symbol, realtime_sdk, priority)


@dataclass(frozen=True)
class FallConfig:
    """主程式傳入的 FALL 設定快照，不在匯入時讀取執行環境。"""

    stock_data_source: str
    signal_start_time: tuple[int, int]
    signal_end_time: tuple[int, int]
    min_drop_percent: float
    max_deviation_ratio: float
    entry_deadline: tuple[int, int]
    candle_publish_delay_seconds: float
    stop_loss_percent: float
    take_profit_percent: float
    force_close_time: tuple[int, int]
    entry_order_quantity: int
    protect_profit_switch: bool
    protect_profit_per: float
    protect_loss_per: float
    profit_back_percent: float
    profit_target_percent: float

    def validate(self, time_to_minutes):
        """時間格式沿用主程式的共用驗證器，保留既有驗證規則。"""
        start = time_to_minutes(self.signal_start_time, "FALL_SIGNAL_START_TIME")
        end = time_to_minutes(self.signal_end_time, "FALL_SIGNAL_END_TIME")
        deadline = time_to_minutes(self.entry_deadline, "FALL_ENTRY_DEADLINE")
        close = time_to_minutes(self.force_close_time, "FALL_FORCE_CLOSE_TIME")
        if not 540 <= start < end < end + 1 <= deadline < close <= 810:
            raise ValueError("FALL 需滿足 09:00 <= 起點 < 確認 < 監看起點 <= 截止 < 平倉 <= 13:30")
        if self.stock_data_source not in ("T", "S") or self.entry_order_quantity != 1:
            raise ValueError("FALL 股票池須為 T/S，每檔限一張")
        for value in (self.min_drop_percent, self.stop_loss_percent, self.take_profit_percent,
                      self.protect_profit_per, self.protect_loss_per,
                      self.profit_back_percent, self.profit_target_percent):
            if not math.isfinite(value) or not 0 < value < 100:
                raise ValueError("FALL 百分比必須介於 0 與 100")
        if not math.isfinite(self.max_deviation_ratio) or not 0 <= self.max_deviation_ratio < 1:
            raise ValueError("FALL 偏離比例須介於 0（含）與 1 之間")
        if not 0 <= self.candle_publish_delay_seconds < 60:
            raise ValueError("FALL 分K發布等待秒數須介於 0（含）與 60 之間")

    def find_signal(self, bars, now_local):
        return find_fall_signal(
            bars, now_local,
            signal_start_time=self.signal_start_time,
            signal_end_time=self.signal_end_time,
            min_drop_percent=self.min_drop_percent,
            max_deviation_ratio=self.max_deviation_ratio,
        )


def fetch_screen_result(
    row, realtime_sdk, *, get_pure_symbol, get_stock_rest_client,
    now, parse_candles, find_signal, get_up_down_price,
):
    """使用共用行情服務篩選；資格查詢失敗仍回傳已成立的 FALL 訊號。"""
    symbol = get_pure_symbol(row[0])[0]
    response = get_stock_rest_client(realtime_sdk).intraday.candles(symbol=symbol, timeframe="1", sort="asc")
    now_local = now()
    signal = find_signal(parse_candles(response.get("data"), now_local), now_local)
    if signal is None:
        return None, None
    try:
        ticker = get_up_down_price(get_pure_symbol(row[0])[1], realtime_sdk)
    except Exception as exc:
        return signal, {"error": repr(exc)}
    return signal, ticker


def fetch_quote_snapshot(symbol, realtime_sdk, priority, *, monotonic, get_realtime_price):
    """記錄請求起點，供主監控判斷報價快照是否過期。"""
    requested_at = monotonic()
    return get_realtime_price(symbol, realtime_sdk, priority=priority), requested_at


def evaluate_fall_quote(state, quote, *, now, finite_positive, check_open_status):
    """更新報價並回傳交易意圖；不送單、不寫檔。

    close 表示交由共用平倉流程檢查風控，並非每筆報價都要平倉。
    invalid 報價維持原狀；hold 報價有效但沒有新交易意圖。
    時鐘與共用資格檢查由主程式傳入，維持既有呼叫順序。
    """
    px, opening, high, low, close, average, bid, ask = quote
    if not all(finite_positive(value) for value in (px, opening, high, low)):
        return "invalid"
    state.update(pre_last_price=state.get("last_price"), pre_last_price_time=state.get("last_price_time"),
                 last_price=float(px), last_price_time=now().isoformat(), open_price=opening,
                 high_price=high, low_price=low, close_price=close, avg_price=average,
                 best_bid_price=bid, best_ask_price=ask)
    if state.get("in_position"):
        return "close"
    if check_open_status(state) and float(px) < float(state["yesterday_low_price"]):
        state["entry_trigger_price"] = float(px)
        return "open"
    return "hold"


def evaluate_fall_candidate(row, ticker, existing, *, finite_positive):
    """判斷候選資格；股票保留、紀錄與狀態寫入由主程式處理。

    保留原判斷順序：已有交易優先，其次查詢錯誤，最後才檢查資格。
    finite_positive 使用主程式共用的數值檢查規則。
    """
    if (existing.get("in_position") or existing.get("entry_order_pending")
            or existing.get("exit_order_pending") or existing.get("entry_order_no")
            or existing.get("entry_request_id")):
        return "already_used"
    if isinstance(ticker, dict):
        return "ticker_error"
    up, down, can_trade, disposition = ticker
    if not can_trade or disposition is None or disposition or not all(
            finite_positive(p) for p in (up, down, row[3], row[4])):
        return "ineligible"
    return "eligible"


def find_fall_signal(
    bars, now_local, *, signal_start_time, signal_end_time,
    min_drop_percent, max_deviation_ratio,
):
    """以首尾 close 連線及逐根最大偏離判斷，只使用完成分 K。"""
    start = now_local.replace(hour=signal_start_time[0], minute=signal_start_time[1], second=0, microsecond=0)
    end = start.replace(hour=signal_end_time[0], minute=signal_end_time[1])
    activation = end + timedelta(minutes=1)
    if now_local < activation:
        return None
    window = [bar for bar in bars if start <= bar["dt"] < activation]
    intervals = int((end - start).total_seconds() // 60)
    if [b["dt"] for b in window] != [start + timedelta(minutes=i) for i in range(intervals + 1)]:
        raise ValueError("FALL 確認窗口缺棒、重複或非整分鐘資料")
    first, last = Decimal(str(window[0]["close"])), Decimal(str(window[-1]["close"]))
    drop = first - last
    if drop <= 0 or drop * 100 < first * Decimal(str(min_drop_percent)):
        return None
    maximum = max(abs((Decimal(str(b["close"])) - first) * intervals + drop * i)
                  for i, b in enumerate(window))
    if maximum > drop * intervals * Decimal(str(max_deviation_ratio)):
        return None
    return {"fall_signal_time": end.isoformat(), "fall_activation_time": activation.isoformat(),
            "fall_drop_percent": float(drop / first * 100),
            "fall_max_deviation_ratio": float(maximum / (drop * intervals))}
