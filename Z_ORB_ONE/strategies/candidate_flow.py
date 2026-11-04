"""共用候選生命週期。背景工作只取資料；主監控決定歸屬與交易。"""
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from threading import Lock
from .fall import FallWindowError


class SharedCandidateFlow:
    def __init__(self, namespace):
        self.namespace = namespace
        self.rows = {}
        self.fall_rows = {}
        self.disabled = {}
        self.avg_checks = {}
        self.candles = {}
        self.quotes = {}
        self.last_scan = None
        self.last_quote_round = None
        self.initialized = False
        self.tickers = {}
        self.scan_timing = None
        self.scan_timing_lock = Lock()

    def record_scan_completion(self, batch, future):
        """背景完成時記錄時間；僅由主流程列印整輪摘要。"""
        a = self.api
        with self.scan_timing_lock:
            batch['finished_at'] = a.now_tpe()
            batch['finished_monotonic'] = a.time.monotonic()
            if future.cancelled() or future.exception() is not None:
                batch['failed'] += 1
            batch['completed'] += 1

    def print_scan_timing(self):
        if self.scan_timing is None or self.candles:
            return
        with self.scan_timing_lock:
            batch = self.scan_timing
            # Future.done() 可能早於完成 callback；等 callback 全部記錄後再印。
            if batch['completed'] != batch['count']:
                return
            elapsed = batch['finished_monotonic'] - batch['started_monotonic']
            print(f"[CANDLE_SCAN] 分K輪次={batch['minute']} 股票數={batch['count']} "
                  f"開始={batch['started_at'].strftime('%H:%M:%S.%f')[:-3]} "
                  f"結束={batch['finished_at'].strftime('%H:%M:%S.%f')[:-3]} "
                  f"耗時={elapsed:.3f}秒 成功={batch['count'] - batch['failed']} "
                  f"失敗={batch['failed']}")
            self.scan_timing = None

    @property
    def api(self):
        return SimpleNamespace(**self.namespace)

    def initialize(self):
        a = self.api
        supported = {a.STRATEGY_FALL, a.STRATEGY_AVG, a.STRATEGY_BROKEN}
        if len(a.STRATEGY_PRIORITY) != len(supported) or set(a.STRATEGY_PRIORITY) != supported:
            raise ValueError("STRATEGY_PRIORITY 必須各包含 FALL、AVG、BROKEN 一次")
        a.validate_broken_config()
        if a.STOCK_DATA_SOURCE not in ("T", "S"):
            raise ValueError("STOCK_DATA_SOURCE 必須為 T/S")
        for row in a.total_stocks if a.STOCK_DATA_SOURCE == "T" else a.selected_stocks:
            self.rows[a.get_pure_symbol(row[0])[1]] = row
        self.initialized = True

    def fetch_candles(self, row, sdk):
        a = self.api
        response = a.get_stock_rest_client(sdk).intraday.candles(
            symbol=a.get_pure_symbol(row[0])[0], timeframe="1", sort="asc")
        # 同一份完整分K同時供所有策略評估；資格查詢不介入策略先後。
        symbol = a.get_pure_symbol(row[0])[1]
        ticker = self.tickers.get(symbol)
        if ticker is None:
            ticker = a.get_up_down_price(symbol, sdk)
            self.tickers[symbol] = ticker
        return response, ticker

    def persist(self, state):
        a = self.api
        a.atomic_write_json(a.state_path(state["symbol_code_with_suf"]), state)

    def check_avg_eligibility(self, symbol, completed, previous_close, now, disabled):
        """固定時段條件各檢查一次；通過後保存結果，供後續型態檢核使用。"""
        a = self.api
        checks = self.avg_checks.setdefault(symbol, {})
        hm = now.hour, now.minute
        if 'slope' not in checks and hm >= a.AVG_SLOPE_CUTOFF_TIME:
            slope = a.get_avg_strategy().calculate_avg_normalized_slope(completed, previous_close)
            if slope is None or slope >= a.AVG_MAX_NORMALIZED_SLOPE:
                disabled.add(a.STRATEGY_AVG)
                a.trade_log("AVG_SLOPE_REJECTED", symbol=symbol, normalized_slope=slope)
                return checks
            checks['slope'] = slope
        if 'count' not in checks and hm >= (9, 30):
            count = sum((b['dt'].hour, b['dt'].minute) < (9, 30) for b in completed)
            if count < a.AVG_MIN_MINUTE_BARS_BEFORE_0930:
                disabled.add(a.STRATEGY_AVG)
                a.trade_log("AVG_MINUTE_BARS_REJECTED", symbol=symbol, count=count)
                return checks
            checks['count'] = count
        return checks

    def evaluate(self, symbol, response, ticker, states, now):
        a = self.api
        if symbol in self.fall_rows:
            self.confirm_fall_entry(states[symbol], response, now)
            return
        if symbol in states or symbol not in self.rows:
            return
        # 回傳可能跨過截止時間；先排除過期策略，再使用這輪分K評估。
        disabled = self.disabled.setdefault(symbol, set())
        for strategy, deadline in ((a.STRATEGY_FALL, a.FALL_ENTRY_DEADLINE),
                                   (a.STRATEGY_AVG, a.AVG_ENTRY_DEADLINE),
                                   (a.STRATEGY_BROKEN, a.BROKEN_ENTRY_DEADLINE)):
            if (now.hour, now.minute) > deadline:
                disabled.add(strategy)
        if len(disabled) == len(a.STRATEGY_PRIORITY):
            self.rows.pop(symbol)
            return
        row = self.rows[symbol]
        bars = a.parse_avg_candles(response.get("data"), now)
        completed = [b for b in bars if b["dt"] < now.replace(second=0, microsecond=0)]
        signals = []
        fall_end = now.replace(hour=a.FALL_SIGNAL_END_TIME[0], minute=a.FALL_SIGNAL_END_TIME[1], second=0, microsecond=0)
        # 即使其他策略稍後成立，也必須先確認較早的 FALL 窗口。
        if a.STRATEGY_FALL not in disabled and now >= fall_end + timedelta(minutes=1, seconds=a.FALL_CANDLE_PUBLISH_DELAY_SECONDS):
            try:
                signal = a.find_fall_signal(completed, now)
            except FallWindowError as exc:
                # 固定窗口無效只排除 FALL；不可中斷 AVG、BROKEN 的本輪檢核。
                disabled.add(a.STRATEGY_FALL)
                a.trade_log("FALL_WINDOW_REJECTED", symbol=symbol,
                            reason=str(exc), action="disable_fall_for_day")
                signal = None
            if signal:
                signals.append((fall_end, a.STRATEGY_FALL, signal))
            else:
                disabled.add(a.STRATEGY_FALL)
        if a.STRATEGY_AVG not in disabled:
            checks = self.check_avg_eligibility(symbol, completed, float(row[5]), now, disabled)
            if (a.STRATEGY_AVG not in disabled and 'slope' in checks and 'count' in checks
                    and (now.hour, now.minute) >= a.AVG_CANDLES_START_TIME):
                signal = a.get_avg_strategy().find_avg_entry_signal(completed, now, candidate_only=True)
                if signal:
                    dt = signal["signal_bar"]["dt"]
                    prior = [b for b in completed if b["dt"] <= dt]
                    spread = Decimal(str(max(b["high"] for b in prior))) - Decimal(str(min(b["low"] for b in prior)))
                    if spread <= Decimal(str(row[5])) * Decimal(str(a.AVG_MIN_PRE_ENTRY_RANGE_PERCENT)) / 100:
                        disabled.add(a.STRATEGY_AVG)
                    else:
                        signals.append((dt, a.STRATEGY_AVG, {
                            "avg_signal_bar_time": dt.isoformat(), "avg_normalized_slope": checks['slope'],
                            "avg_pre_entry_range": float(spread)}))
        if a.STRATEGY_BROKEN not in disabled:
            signal, rejected = a.get_broken_config().find_candidate(completed, row[5], now)
            if rejected:
                disabled.add(a.STRATEGY_BROKEN)
            if signal:
                dt = datetime.fromisoformat(signal['broken_signal_time'])
                signals.append((dt, a.STRATEGY_BROKEN, signal))
        if not signals:
            return
        dt, strategy, fields = min(signals, key=lambda item: (item[0], a.STRATEGY_PRIORITY.index(item[1])))
        up, down, can_trade, disposition = ticker
        state = a.build_initial_state(row[0], a.get_entry_order_quantity(strategy_type=strategy),
                                      *row[2:6], str(row[6]).zfill(2), "", up, down, *row[8], strategy)
        state.update(fields, candidate_time=dt.isoformat(), minute_bars_eligible=True,
                     side=a.TRADE_SIDE_SHORT, fall_candidate=strategy == a.STRATEGY_FALL)
        if not can_trade or disposition is None or disposition or not all(a.finite_positive(v) for v in (up, down, row[4], row[5])):
            state.update(traded=True, exit_reason="candidate_ineligible")
        with a.ORDER_STATE_LOCK:
            states[symbol] = state
            if strategy == a.STRATEGY_FALL:
                a.FALL_RUNTIME.candidate_symbols.add(symbol)
            a.refresh_active_order_states(states)
        self.rows.pop(symbol)
        if strategy == a.STRATEGY_FALL:
            self.fall_rows[symbol] = row
            self.confirm_fall_entry(state, response, now)
        self.persist(state)
        a.trade_log("STRATEGY_LOCKED", symbol=symbol, strategy=strategy, candidate_time=dt.isoformat())

    def confirm_fall_entry(self, state, response, now):
        """候選歸屬不變；持續使用完整分K確認連續跌破昨收。"""
        a = self.api
        symbol = state['symbol_code_with_suf']
        if state.get('fall_entry_confirmed_time') or state.get('traded') or state.get('in_position'):
            self.fall_rows.pop(symbol, None)
            return
        if (now.hour, now.minute) > a.FALL_ENTRY_DEADLINE:
            self.fall_rows.pop(symbol, None)
            return
        start = now.replace(hour=a.FALL_SIGNAL_END_TIME[0], minute=a.FALL_SIGNAL_END_TIME[1],
                            second=0, microsecond=0) + timedelta(minutes=1)
        completed = [bar for bar in a.parse_avg_candles(response.get('data'), now)
                     if start <= bar['dt'] < now.replace(second=0, microsecond=0)]
        counts = {}
        for bar in completed:
            counts[bar['dt']] = counts.get(bar['dt'], 0) + 1
        count, last = 0, None
        previous = Decimal(str(state['yesterday_close_price']))
        for bar in completed:
            dt = bar['dt']
            if (dt.second or dt.microsecond or counts[dt] != 1
                    or Decimal(str(bar['low'])) >= previous):
                count, last = 0, None
                continue
            if last is None or dt != last + timedelta(minutes=1):
                count = 0
            count += 1
            last = dt
            if count >= a.FALL_ENTRY_CONFIRM_BARS:
                state['fall_entry_confirmed_time'] = (dt + timedelta(minutes=1)).isoformat()
                self.fall_rows.pop(symbol, None)
                self.persist(state)
                a.trade_log('FALL_ENTRY_CONFIRMED', symbol=symbol, confirm_bars=count,
                            confirmed_time=state['fall_entry_confirmed_time'])
                return

    def apply_quote(self, state, quote, sdk, now):
        a = self.api
        px, opening, high, low, close, average, bid, ask = quote
        if not all(a.finite_positive(v) for v in (px, opening, high, low)):
            return
        state.update(pre_last_price=state.get("last_price"), pre_last_price_time=state.get("last_price_time"),
                     last_price=float(px), last_price_time=now.isoformat(), open_price=opening,
                     high_price=high, low_price=low, close_price=close, avg_price=average,
                     best_bid_price=bid, best_ask_price=ask)
        if state.get("in_position"):
            a.try_close_position(state, sdk)
        elif a.check_open_status(state):
            hm = now.hour, now.minute
            if hm > a.get_entry_check_end_time(state):
                state.update(traded=True, exit_reason="entry_deadline_passed")
            elif a.is_avg_strategy(state) and hm < a.AVG_ENTRY_START_TIME:
                state.update(traded=True, exit_reason="avg_before_entry_start")
            elif high >= state["limit_up_price"] or low <= state["limit_down_price"]:
                state.update(traded=True, exit_reason="limit_price_touched_before_entry")
            elif (a.is_avg_strategy(state) or a.is_broken_strategy(state)
                  or (state.get('fall_entry_confirmed_time')
                      and a.finite_positive(state.get("yesterday_close_price"))
                      and float(px) < float(state["yesterday_close_price"]))):
                state.update(entry_trigger_price=float(px), entry_signal_price=float(px), entry_signal_time=now.isoformat())
                a.try_open_position(state, sdk)
        self.persist(state)

    def process(self, states, sdk, trade_sdk):
        a = self.api
        now = a.now_tpe()
        for future, symbol in list(self.candles.items()):
            if not future.done():
                continue
            self.candles.pop(future)
            try:
                response, ticker = future.result()
                self.evaluate(symbol, response, ticker, states, now)
            except Exception as exc:
                a.trade_log("CANDIDATE_SCAN_ERROR", error=True, symbol=symbol, error_msg=repr(exc))
        self.print_scan_timing()
        minute = now.strftime("%Y-%m-%d %H:%M")
        start = min(a.AVG_CANDLES_START_TIME, a.AVG_SLOPE_CUTOFF_TIME, (9, 30),
                    divmod(a.FALL_SIGNAL_END_TIME[0] * 60 + a.FALL_SIGNAL_END_TIME[1] + 1, 60),
                    divmod(a.BROKEN_SIGNAL_END_TIME[0] * 60 + a.BROKEN_SIGNAL_END_TIME[1] + 1, 60))
        if (not self.candles and self.scan_timing is None
                and minute != self.last_scan and now.second >= a.AVG_CANDLE_POLL_SECOND
                and (now.hour, now.minute) >= start):
            self.last_scan = minute
            for symbol, row in list({**self.rows, **self.fall_rows}.items()):
                if symbol in self.fall_rows:
                    state = states[symbol]
                    if (state.get('traded') or state.get('in_position')
                            or state.get('fall_entry_confirmed_time')
                            or (now.hour, now.minute) > a.FALL_ENTRY_DEADLINE):
                        self.fall_rows.pop(symbol, None)
                        continue
                disabled = self.disabled.setdefault(symbol, set())
                for strategy, deadline in ((a.STRATEGY_FALL, a.FALL_ENTRY_DEADLINE),
                                           (a.STRATEGY_AVG, a.AVG_ENTRY_DEADLINE),
                                           (a.STRATEGY_BROKEN, a.BROKEN_ENTRY_DEADLINE)):
                    if (now.hour, now.minute) > deadline:
                        disabled.add(strategy)
                if len(disabled) == len(a.STRATEGY_PRIORITY):
                    self.rows.pop(symbol, None)
                else:
                    if self.scan_timing is None:
                        self.scan_timing = dict(minute=minute, started_at=a.now_tpe(),
                                                started_monotonic=a.time.monotonic(),
                                                count=0, completed=0, failed=0)
                    batch = self.scan_timing
                    future = a.AVG_CANDLE_EXECUTOR.submit(self.fetch_candles, row, sdk)
                    with self.scan_timing_lock:
                        batch['count'] += 1
                    self.candles[future] = symbol
                    future.add_done_callback(lambda done, batch=batch: self.record_scan_completion(batch, done))
        for symbol, future in list(self.quotes.items()):
            if not future.done():
                continue
            self.quotes.pop(symbol)
            try:
                quote, requested_at = future.result()
                if a.time.monotonic() - requested_at <= a.QUOTE_ROUND_INTERVAL_SECONDS:
                    self.apply_quote(states[symbol], quote, trade_sdk, now)
            except Exception as exc:
                a.trade_log("CANDIDATE_QUOTE_ERROR", error=True, symbol=symbol, error_msg=repr(exc))
        quote_round = int((now.timestamp() - 5) // a.QUOTE_ROUND_INTERVAL_SECONDS)
        request_quotes = quote_round != self.last_quote_round
        self.last_quote_round = quote_round
        for symbol, state in list(states.items()):
            try:
                if state.get("in_position") and (a.force_close_time_reached(state) or state.get("exit_retry_reason")):
                    a.try_close_position(state, trade_sdk)
                if state.get("traded"):
                    continue
                if a.check_open_status(state) and (now.hour, now.minute) > a.get_entry_check_end_time(state):
                    state.update(traded=True, exit_reason="entry_deadline_passed")
                    self.persist(state)
                    continue
                if request_quotes and symbol not in self.quotes:
                    priority = a.MARKETDATA_PRIORITY_POSITION if state.get("in_position") else a.MARKETDATA_PRIORITY_CANDIDATE
                    executor = a.AVG_POSITION_QUOTE_EXECUTOR if state.get("in_position") else a.FALL_QUOTE_EXECUTOR
                    self.quotes[symbol] = executor.submit(a.fetch_fall_quote, symbol, sdk, priority)
            except Exception as exc:
                a.trade_log("CANDIDATE_MONITOR_ERROR", error=True, symbol=symbol, error_msg=repr(exc))
