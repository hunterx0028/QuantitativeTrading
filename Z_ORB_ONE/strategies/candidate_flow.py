"""共用候選生命週期。背景工作只取資料；主監控決定歸屬與交易。"""
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from threading import Lock
from .fall import FallWindowError, find_fall_average_stop


class SharedCandidateFlow:
    def __init__(self, namespace):
        self.namespace = namespace
        self.rows = {}
        self.candidates = {}  # symbol -> strategy -> independent candidate state
        self.broken_windows = {}
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
        self.fall_stop_candles = {}
        self.last_fall_stop_scan = {}

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
        supported = {a.STRATEGY_FALL, a.STRATEGY_AVG}
        if hasattr(a, 'STRATEGY_BROKEN'):
            supported.add(a.STRATEGY_BROKEN)
        if len(a.STRATEGY_PRIORITY) != len(supported) or set(a.STRATEGY_PRIORITY) != supported:
            raise ValueError(f"STRATEGY_PRIORITY 必須各包含 {sorted(supported)} 一次")
        if a.STOCK_DATA_SOURCE not in ("T", "S"):
            raise ValueError("STOCK_DATA_SOURCE 必須為 T/S")
        if any(self.strategy_enabled(strategy) for strategy in a.STRATEGY_PRIORITY):
            for row in a.total_stocks if a.STOCK_DATA_SOURCE == "T" else a.selected_stocks:
                self.rows[a.get_pure_symbol(row[0])[1]] = row
        self.initialized = True

    def strategy_enabled(self, strategy):
        # 舊版入口未提供開關時，維持所有策略開啟的既有行為。
        check = self.namespace.get('is_strategy_enabled')
        return check(strategy) if check is not None else True

    def fetch_candles(self, row, sdk):
        a = self.api
        requested_at = a.now_tpe()
        response = a.get_stock_rest_client(sdk).intraday.candles(
            symbol=a.get_pure_symbol(row[0])[0], timeframe="1", sort="asc")
        # 同一份完整分K同時供所有策略評估；資格查詢不介入策略先後。
        symbol = a.get_pure_symbol(row[0])[1]
        ticker = self.tickers.get(symbol)
        if ticker is None:
            ticker = a.get_up_down_price(symbol, sdk)
            self.tickers[symbol] = ticker
        return response, ticker, requested_at

    def persist(self, state):
        a = self.api
        a.atomic_write_json(a.state_path(state["symbol_code_with_suf"]), state)

    def strategy_deadlines(self):
        a = self.api
        deadlines = [(a.STRATEGY_FALL, a.FALL_ENTRY_DEADLINE),
                     (a.STRATEGY_AVG, a.AVG_ENTRY_DEADLINE)]
        if hasattr(a, 'STRATEGY_BROKEN'):
            deadlines.append((a.STRATEGY_BROKEN, divmod(
                a.BROKEN_ENTRY_DEADLINE[0] * 60 + a.BROKEN_ENTRY_DEADLINE[1] + 1, 60)))
        return deadlines

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

    def prune(self, symbol, now):
        """只淘汰失效策略；全部失效或已有委託才停止該股票的候選掃描。"""
        a = self.api
        disabled = self.disabled.setdefault(symbol, set())
        disabled.update(strategy for strategy in a.STRATEGY_PRIORITY
                        if not self.strategy_enabled(strategy))
        for strategy, deadline in self.strategy_deadlines():
            if (now.hour, now.minute) > deadline:
                disabled.add(strategy)
        candidates = self.candidates.get(symbol, {})
        for strategy, state in list(candidates.items()):
            if strategy in disabled:
                candidates.pop(strategy, None)
        if set(a.STRATEGY_PRIORITY).issubset(disabled):
            self.rows.pop(symbol, None)
            self.candidates.pop(symbol, None)
        return disabled

    def add_candidate(self, symbol, strategy, fields, ticker, now):
        a = self.api
        candidates = self.candidates.setdefault(symbol, {})
        if strategy in candidates:
            return candidates[strategy]
        row = self.rows[symbol]
        up, down, _, _ = ticker
        state = a.build_initial_state(row[0], a.get_entry_order_quantity(strategy_type=strategy),
                                      *row[2:6], str(row[6]).zfill(2), '', up, down, *row[8], strategy)
        state.update(fields, minute_bars_eligible=True, side=a.TRADE_SIDE_SHORT,
                     fall_candidate=strategy == a.STRATEGY_FALL,
                     candidate_time=now.isoformat())
        candidates[strategy] = state
        a.trade_log('STRATEGY_CANDIDATE', symbol=symbol, strategy=strategy, **fields)
        return state

    def needs_candles(self, symbol):
        """只有仍待確認的策略需要續查分K；已確認候選直接等待即時價。"""
        a = self.api
        candidates = self.candidates.get(symbol, {})
        disabled = self.disabled.get(symbol, set())
        for strategy in a.STRATEGY_PRIORITY:
            if strategy in disabled or not self.strategy_enabled(strategy):
                continue
            state = candidates.get(strategy)
            if state is None or (strategy == a.STRATEGY_FALL and not state.get('fall_entry_confirmed_time')):
                return True
        return False

    def evaluate(self, symbol, response, ticker, states, now, *, requested_at=None):
        a = self.api
        if symbol in states or symbol not in self.rows:
            return
        disabled = self.prune(symbol, now)
        if symbol not in self.rows:
            return
        row = self.rows[symbol]
        up, down, can_trade, disposition = ticker
        if (not can_trade or disposition is None or disposition
                or not all(a.finite_positive(v) for v in (up, down, row[4], row[5]))):
            disabled.update(a.STRATEGY_PRIORITY)
            self.prune(symbol, now)
            a.trade_log('CANDIDATE_INELIGIBLE', symbol=symbol)
            return
        # 跨分鐘回傳仍可確認歷史訊號，但請求當時尚未完成的棒不能拿來確認。
        cutoff = min(requested_at or now, now)
        bars = a.parse_avg_candles(response.get('data'), cutoff)
        completed = [b for b in bars if b['dt'] < cutoff.replace(second=0, microsecond=0)]
        candidates = self.candidates.setdefault(symbol, {})
        fall_end = now.replace(hour=a.FALL_SIGNAL_END_TIME[0], minute=a.FALL_SIGNAL_END_TIME[1], second=0, microsecond=0)
        if a.STRATEGY_FALL not in disabled:
            if (a.STRATEGY_FALL not in candidates
                    and cutoff >= fall_end + timedelta(minutes=1, seconds=a.FALL_CANDLE_PUBLISH_DELAY_SECONDS)):
                try:
                    signal = a.find_fall_signal(completed, cutoff)
                except FallWindowError as exc:
                    a.trade_log('FALL_WINDOW_REJECTED', symbol=symbol, reason=str(exc))
                    signal = None
                if signal:
                    self.add_candidate(symbol, a.STRATEGY_FALL, signal, ticker, now)
                else:
                    disabled.add(a.STRATEGY_FALL)
            if a.STRATEGY_FALL in candidates:
                self.confirm_fall_entry(candidates[a.STRATEGY_FALL], response, cutoff)
        if a.STRATEGY_AVG not in disabled:
            checks = self.check_avg_eligibility(symbol, completed, float(row[5]), cutoff, disabled)
            if (a.STRATEGY_AVG not in disabled and a.STRATEGY_AVG not in candidates
                    and 'slope' in checks and 'count' in checks
                    and (now.hour, now.minute) >= a.AVG_CANDLES_START_TIME):
                signal = a.get_avg_strategy().find_avg_entry_signal(completed, cutoff, candidate_only=True)
                if signal:
                    dt = signal['signal_bar']['dt']
                    entry_time = dt + timedelta(minutes=1)
                    prior = [b for b in completed if b['dt'] <= dt]
                    spread = Decimal(str(max(b['high'] for b in prior))) - Decimal(str(min(b['low'] for b in prior)))
                    if (not a.AVG_ENTRY_START_TIME <= (entry_time.hour, entry_time.minute) <= a.AVG_ENTRY_DEADLINE
                            or spread <= Decimal(str(row[5])) * Decimal(str(a.AVG_MIN_PRE_ENTRY_RANGE_PERCENT)) / 100):
                        disabled.add(a.STRATEGY_AVG)
                        a.trade_log('AVG_ENTRY_REJECTED', symbol=symbol, reason='first_entry_outside_window_or_range')
                    else:
                        self.add_candidate(symbol, a.STRATEGY_AVG, {
                            'avg_signal_bar_time': dt.isoformat(), 'avg_normalized_slope': checks['slope'],
                            'avg_pre_entry_range': float(spread), 'avg_entry_time': entry_time.isoformat()}, ticker, now)
        if hasattr(a, 'STRATEGY_BROKEN') and a.STRATEGY_BROKEN not in disabled:
            config = a.get_broken_config()
            metrics = self.broken_windows.get(symbol)
            if metrics is None:
                metrics, rejected = config.check_window(bars, float(row[5]), cutoff)
                if rejected:
                    disabled.add(a.STRATEGY_BROKEN)
                    a.trade_log('BROKEN_WINDOW_REJECTED', symbol=symbol)
                elif metrics is not None:
                    self.broken_windows[symbol] = metrics
                    a.trade_log('BROKEN_WINDOW_PASSED', symbol=symbol, **metrics)
            if metrics is not None and a.STRATEGY_BROKEN not in candidates:
                signal, rejected = config.find_candidate(bars, float(row[5]), cutoff, window_metrics=metrics)
                if rejected:
                    disabled.add(a.STRATEGY_BROKEN)
                    a.trade_log('BROKEN_REJECTED', symbol=symbol, reason='invalid_or_expired_signal')
                elif signal:
                    self.add_candidate(symbol, a.STRATEGY_BROKEN, signal, ticker, now)
        self.prune(symbol, now)

    def confirm_fall_entry(self, state, response, now):
        """FALL 候選獨立確認跌破昨收，尚不取得交易歸屬。"""
        a = self.api
        symbol = state['symbol_code_with_suf']
        if state.get('fall_entry_confirmed_time') or state.get('traded') or state.get('in_position'):
            return
        if (now.hour, now.minute) > a.FALL_ENTRY_DEADLINE:
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
                a.trade_log('FALL_ENTRY_CONFIRMED', symbol=symbol, confirm_bars=count,
                            confirmed_time=state['fall_entry_confirmed_time'])
                return

    def fetch_fall_stop_candles(self, state, sdk):
        """持倉分K不查 ticker，沿用持倉優先序及共用 HTTP 逾時。"""
        a = self.api
        requested_at = a.now_tpe()
        with a.marketdata_priority(a.MARKETDATA_PRIORITY_POSITION):
            response = a.get_stock_rest_client(sdk).intraday.candles(
                symbol=state['symbol_code'], timeframe='1', sort='asc')
        return response, requested_at

    def process_fall_average_stops(self, states, sdk, trade_sdk, now):
        """確認成交後每分鐘取完整分K；主執行緒記錄觸發並走既有平倉流程。"""
        a = self.api
        for symbol, future in list(self.fall_stop_candles.items()):
            if not future.done():
                continue
            self.fall_stop_candles.pop(symbol)
            state = states.get(symbol)
            if (not state or not state.get('in_position') or not state.get('entry_fill_confirmed')
                    or state.get('traded') or state.get('exit_order_pending')
                    or state.get('fall_average_stop_triggered')):
                continue
            try:
                response, requested_at = future.result()
                # 請求跨分鐘時，當時仍未完成的棒不可當作完整棒使用。
                cutoff = min(requested_at, now)
                bars = a.parse_avg_candles(response.get('data'), cutoff)
                entry_time = datetime.fromisoformat(state.get('entry_first_fill_time') or state['entry_time'])
                if entry_time.tzinfo is None and now.tzinfo is not None:
                    entry_time = entry_time.replace(tzinfo=now.tzinfo)
                signal = find_fall_average_stop(bars, entry_time, cutoff)
                if signal:
                    state.update(fall_average_stop_triggered=True,
                                 fall_average_stop_bar_time=signal['bar_time'],
                                 fall_average_stop_price=signal['reference_price'])
                    self.persist(state)
                    a.trade_log('FALL_AVERAGE_STOP_TRIGGERED', symbol=symbol, **signal,
                                action='submit_market_exit')
                    a.try_close_position(state, trade_sdk)
            except Exception as exc:
                a.trade_log('FALL_AVERAGE_STOP_ERROR', error=True, symbol=symbol,
                            error_msg=repr(exc), action='retry_next_scan_quotes_continue')
        minute = now.strftime('%Y-%m-%d %H:%M')
        for symbol, state in list(states.items()):
            if (not a.is_fall_strategy(state) or not state.get('in_position')
                    or not state.get('entry_fill_confirmed') or state.get('traded')
                    or state.get('exit_order_pending') or state.get('fall_average_stop_triggered')
                    or a.force_close_time_reached(state)):
                continue
            if (symbol not in self.fall_stop_candles and self.last_fall_stop_scan.get(symbol) != minute
                    and now.second >= a.FALL_CANDLE_PUBLISH_DELAY_SECONDS):
                self.last_fall_stop_scan[symbol] = minute
                self.fall_stop_candles[symbol] = a.AVG_POSITION_QUOTE_EXECUTOR.submit(
                    self.fetch_fall_stop_candles, state, sdk)

    def update_quote(self, state, quote, now):
        px, opening, high, low, close, average, bid, ask = quote
        state.update(pre_last_price=state.get('last_price'), pre_last_price_time=state.get('last_price_time'),
                     last_price=float(px), last_price_time=now.isoformat(), open_price=opening,
                     high_price=high, low_price=low, close_price=close, avg_price=average,
                     best_bid_price=bid, best_ask_price=ask)

    def apply_quote(self, state, quote, sdk, now):
        """已送單的唯一狀態只管理委託與持倉，不重新尋找進場策略。"""
        a = self.api
        if not all(a.finite_positive(v) for v in quote[:4]):
            return
        self.update_quote(state, quote, now)
        if state.get('in_position'):
            a.try_close_position(state, sdk)
        self.persist(state)

    def apply_candidates_quote(self, symbol, quote, states, sdk, now):
        """同一份報價評估所有候選；只有完整符合進場條件才競爭交易歸屬。"""
        a = self.api
        if symbol in states or symbol not in self.rows:
            return
        self.prune(symbol, now)
        if symbol not in self.rows or not all(a.finite_positive(v) for v in quote[:4]):
            return
        candidates = self.candidates.get(symbol, {})
        px, _, high, low = quote[:4]
        for strategy in a.STRATEGY_PRIORITY:
            state = candidates.get(strategy)
            if state is None:
                continue
            self.update_quote(state, quote, now)
            if high >= state['limit_up_price'] or low <= state['limit_down_price']:
                self.disabled.setdefault(symbol, set()).update(a.STRATEGY_PRIORITY)
                self.prune(symbol, now)
                a.trade_log('CANDIDATE_LIMIT_TOUCHED', symbol=symbol)
                return
            if not a.check_open_status(state):
                continue
            if not a.get_entry_check_start_time(state) <= (now.hour, now.minute) <= a.get_entry_check_end_time(state):
                continue
            if a.is_fall_strategy(state):
                if (not state.get('fall_entry_confirmed_time')
                        or datetime.fromisoformat(state['fall_entry_confirmed_time']) > now
                        or Decimal(str(px)) >= Decimal(str(state['yesterday_close_price']))):
                    continue
            elif a.is_avg_strategy(state):
                if not state.get('avg_entry_time') or datetime.fromisoformat(state['avg_entry_time']) > now:
                    continue
            else:
                # BROKEN 的即時價門檻集中於此；不符時保留候選，等待後續報價。
                if (not state.get('broken_entry_time')
                        or datetime.fromisoformat(state['broken_entry_time']) > now
                        or Decimal(str(px)) >= Decimal(str(state['broken_trigger_price']))):
                    continue
            loss_percent, _ = a.get_optimize_loss_profit_percent(state)
            if not a.is_fall_strategy(state) and float(px) * (1 + loss_percent / 100) >= state['limit_up_price']:
                self.disabled.setdefault(symbol, set()).add(strategy)
                a.trade_log('CANDIDATE_STOP_SPACE_REJECTED', symbol=symbol, strategy=strategy)
                continue
            state.update(entry_trigger_price=float(px), entry_signal_price=float(px), entry_signal_time=now.isoformat())
            # SDK callback 與未知結果對帳必須在送單前即可找到唯一狀態。
            with a.ORDER_STATE_LOCK:
                if symbol in states:
                    return
                states[symbol] = state
                a.refresh_active_order_states(states)
            try:
                a.try_open_position(state, sdk)
            finally:
                attempted = bool(state.get('entry_submission_started') or state.get('entry_order_pending')
                                 or state.get('entry_order_no') or state.get('entry_request_id')
                                 or state.get('entry_outcome_unknown') or state.get('in_position'))
                if attempted:
                    self.rows.pop(symbol, None)
                    self.candidates.pop(symbol, None)
                    self.persist(state)
                    a.trade_log('STRATEGY_LOCKED', symbol=symbol, strategy=strategy,
                                reason='entry_submission', signal_time=state.get('entry_signal_time'))
                else:
                    with a.ORDER_STATE_LOCK:
                        if states.get(symbol) is state:
                            states.pop(symbol)
                        a.refresh_active_order_states(states)
                    if state.get('traded'):
                        self.disabled.setdefault(symbol, set()).add(strategy)
                    self.prune(symbol, now)
            # 同一報價只嘗試一個完整合格策略；Busy/暫停等下一輪重新仲裁。
            return

    def process(self, states, sdk, trade_sdk):
        a = self.api
        now = a.now_tpe()
        for future, symbol in list(self.candles.items()):
            if not future.done():
                continue
            self.candles.pop(future)
            try:
                response, ticker, requested_at = future.result()
                self.evaluate(symbol, response, ticker, states, now, requested_at=requested_at)
            except Exception as exc:
                a.trade_log('CANDIDATE_SCAN_ERROR', error=True, symbol=symbol, error_msg=repr(exc))
        self.print_scan_timing()
        self.process_fall_average_stops(states, sdk, trade_sdk, now)
        minute = now.strftime('%Y-%m-%d %H:%M')
        start = min(a.AVG_CANDLES_START_TIME, a.AVG_SLOPE_CUTOFF_TIME, (9, 30),
                    divmod(a.FALL_SIGNAL_END_TIME[0] * 60 + a.FALL_SIGNAL_END_TIME[1] + 1, 60))
        if hasattr(a, 'STRATEGY_BROKEN'):
            start = min(start, a.get_broken_config().entry_start)
        if (not self.candles and self.scan_timing is None
                and minute != self.last_scan and now.second >= a.AVG_CANDLE_POLL_SECOND
                and (now.hour, now.minute) >= start):
            self.last_scan = minute
            for symbol, row in list(self.rows.items()):
                self.prune(symbol, now)
                if symbol not in self.rows or symbol in states or not self.needs_candles(symbol):
                    continue
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
                now = a.now_tpe()
                if a.time.monotonic() - requested_at <= a.QUOTE_ROUND_INTERVAL_SECONDS:
                    if symbol in states:
                        self.apply_quote(states[symbol], quote, trade_sdk, now)
                    else:
                        self.apply_candidates_quote(symbol, quote, states, trade_sdk, now)
            except Exception as exc:
                a.trade_log('CANDIDATE_QUOTE_ERROR', error=True, symbol=symbol, error_msg=repr(exc))
        quote_round = int((now.timestamp() - 5) // a.QUOTE_ROUND_INTERVAL_SECONDS)
        request_quotes = quote_round != self.last_quote_round
        self.last_quote_round = quote_round
        for symbol in list(self.rows):
            self.prune(symbol, now)
        for symbol in list(set(states) | set(self.candidates)):
            try:
                state = states.get(symbol)
                if state:
                    if state.get('in_position') and (a.force_close_time_reached(state) or state.get('exit_retry_reason')
                                                     or state.get('fall_average_stop_triggered')):
                        a.try_close_position(state, trade_sdk)
                    if state.get('traded') and not state.get('in_position'):
                        continue
                elif not self.candidates.get(symbol):
                    continue
                if request_quotes and symbol not in self.quotes:
                    position = bool(state and state.get('in_position'))
                    priority = a.MARKETDATA_PRIORITY_POSITION if position else a.MARKETDATA_PRIORITY_CANDIDATE
                    executor = a.AVG_POSITION_QUOTE_EXECUTOR if position else a.FALL_QUOTE_EXECUTOR
                    self.quotes[symbol] = executor.submit(a.fetch_fall_quote, symbol, sdk, priority)
            except Exception as exc:
                a.trade_log('CANDIDATE_MONITOR_ERROR', error=True, symbol=symbol, error_msg=repr(exc))
