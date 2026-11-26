"""共用候選生命週期。背景工作只取資料；主監控決定歸屬與交易。"""
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from threading import Lock
from concurrent.futures import as_completed


class SharedCandidateFlow:
    def __init__(self, namespace):
        self.namespace = namespace
        self.rows = {}
        self.candidates = {}  # symbol -> strategy -> independent candidate state
        self.disabled = {}
        self.avg_checks = {}
        self.candles = {}
        self.quotes = {}
        self.last_scan = None
        self.last_quote_round = None
        self.initialized = False
        self.tickers = {}
        self.vol_turnover_eligibility = {}
        self.top_range_eligibility = {}
        self.entry_attempts = {}  # strategy -> 當日已嘗試送單筆數（含拒單／結果未知）
        self.scan_timing = None
        self.scan_timing_lock = Lock()
        self.ind_runtime = None
        self.ind_market_future = None
        self.ind_market_retry_at = 0.0

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
        supported = {a.STRATEGY_AVG}
        if hasattr(a, 'STRATEGY_VOL'):
            supported.add(a.STRATEGY_VOL)
        if hasattr(a, 'STRATEGY_IND'):
            supported.add(a.STRATEGY_IND)
            if self.strategy_enabled(a.STRATEGY_IND):
                self.ind_runtime = a.get_ind_runtime()
        if hasattr(a, 'STRATEGY_TOP'):
            supported.add(a.STRATEGY_TOP)
        if len(a.STRATEGY_PRIORITY) != len(supported) or set(a.STRATEGY_PRIORITY) != supported:
            raise ValueError(f"STRATEGY_PRIORITY 必須各包含 {sorted(supported)} 一次")
        if a.STOCK_DATA_SOURCE not in ("T", "S"):
            raise ValueError("STOCK_DATA_SOURCE 必須為 T/S")
        if any(self.strategy_enabled(strategy) for strategy in a.STRATEGY_PRIORITY):
            for row in a.total_stocks if a.STOCK_DATA_SOURCE == "T" else a.selected_stocks:
                self.rows[a.get_pure_symbol(row[0])[1]] = row
        if hasattr(a, 'STRATEGY_VOL') and self.strategy_enabled(a.STRATEGY_VOL):
            self.vol_turnover_eligibility = a.get_vol_config().build_turnover_eligibility(
                self.rows, a.vol_history_by_symbol, a.get_vol_reference_date(), a.now_tpe())
            for symbol in self.rows:
                if not self.vol_turnover_eligibility.get(symbol, False):
                    self.disabled.setdefault(symbol, set()).add(a.STRATEGY_VOL)
            a.trade_log('VOL_TURNOVER_FILTER', pool_count=len(self.rows),
                        ranked_count=len(self.vol_turnover_eligibility),
                        eligible_count=sum(self.vol_turnover_eligibility.values()))
        if hasattr(a, 'STRATEGY_TOP') and self.strategy_enabled(a.STRATEGY_TOP):
            # 開盤前固定名單；資料缺漏、日期不符或昨日振幅過大者只停用TOP。
            self.top_range_eligibility = a.get_top_config().build_eligibility(
                self.rows, a.top_history_by_symbol, a.get_top_reference_date(), a.now_tpe())
            for symbol in self.rows:
                if not self.top_range_eligibility.get(symbol, (False, None))[0]:
                    self.disabled.setdefault(symbol, set()).add(a.STRATEGY_TOP)
            a.trade_log('TOP_RANGE_FILTER', pool_count=len(self.rows),
                        valid_history_count=sum(value[1] is not None for value in self.top_range_eligibility.values()),
                        eligible_count=sum(value[0] for value in self.top_range_eligibility.values()),
                        eligible=','.join(sorted(symbol for symbol, value in self.top_range_eligibility.items()
                                                 if value[0])))
        self.initialized = True

    def prepare_tickers(self, sdk):
        """開盤前共用資格檢查；確定不合格即排除，欄位缺失不猜值。

        有限次重試後仍缺資料者保留待查，盤中取分K時重查，確認前不可建立候選。
        """
        a = self.api
        for attempt in range(a.PREOPEN_TICKER_ATTEMPTS):
            missing = [symbol for symbol in self.rows if symbol not in self.tickers]
            if not missing:
                break
            futures = {a.AVG_CANDLE_EXECUTOR.submit(a.get_up_down_price, symbol, sdk): symbol
                       for symbol in missing}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    ticker = future.result()
                    if self.ticker_missing_fields(ticker):
                        a.trade_log('PREOPEN_TICKER_INCOMPLETE', symbol=symbol, attempt=attempt + 1)
                        continue
                    up, down, can_trade, disposition = ticker
                    if not can_trade or disposition:
                        self.disabled.setdefault(symbol, set()).update(a.STRATEGY_PRIORITY)
                        self.rows.pop(symbol, None)
                        a.trade_log('PREOPEN_TICKER_REJECTED', symbol=symbol,
                                    can_day_trade=can_trade, disposition=disposition)
                        continue
                    self.tickers[symbol] = ticker
                except Exception as exc:
                    a.trade_log('PREOPEN_TICKER_ERROR', error=True, symbol=symbol,
                                attempt=attempt + 1, error_msg=repr(exc))
        a.trade_log('PREOPEN_TICKER_COMPLETE', pool_count=len(self.rows),
                    verified_count=len(self.tickers),
                    retry_pending_count=sum(symbol not in self.tickers for symbol in self.rows))

    def strategy_enabled(self, strategy):
        # 舊版入口未提供開關時，維持所有策略開啟的既有行為。
        check = self.namespace.get('is_strategy_enabled')
        return check(strategy) if check is not None else True

    def daily_trade_cap(self, strategy):
        """每日嘗試送單上限；未設定（或 None）代表不限。"""
        return self.namespace.get(f'{strategy}_MAX_TRADES_PER_DAY')

    def daily_cap_reached(self, strategy):
        cap = self.daily_trade_cap(strategy)
        return cap is not None and self.entry_attempts.get(strategy, 0) >= cap

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
            if not self.ticker_missing_fields(ticker):
                self.tickers[symbol] = ticker
        return response, ticker, requested_at

    def ticker_missing_fields(self, ticker):
        a = self.api
        up, down, can_trade, disposition = ticker
        return [key for key, valid in (
            ('limitUpPrice', not isinstance(up, bool) and a.finite_positive(up)),
            ('limitDownPrice', not isinstance(down, bool) and a.finite_positive(down)),
            ('canDayTrade', isinstance(can_trade, bool)),
            ('isDisposition', isinstance(disposition, bool))) if not valid]

    def persist(self, state):
        a = self.api
        a.atomic_write_json(a.state_path(state["symbol_code_with_suf"]), state)

    def strategy_deadlines(self):
        a = self.api
        deadlines = [(a.STRATEGY_AVG, a.AVG_ENTRY_DEADLINE)]
        if hasattr(a, 'STRATEGY_VOL'):
            deadlines.append((a.STRATEGY_VOL, a.VOL_ENTRY_DEADLINE))
        if hasattr(a, 'STRATEGY_IND'):
            deadlines.append((a.STRATEGY_IND, a.IND_ENTRY_DEADLINE))
        if hasattr(a, 'STRATEGY_TOP'):
            deadlines.append((a.STRATEGY_TOP, a.TOP_ENTRY_DEADLINE))
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
        return checks

    def prune(self, symbol, now):
        """只淘汰失效策略；全部失效或已有委託才停止該股票的候選掃描。"""
        a = self.api
        disabled = self.disabled.setdefault(symbol, set())
        disabled.update(strategy for strategy in a.STRATEGY_PRIORITY
                        if not self.strategy_enabled(strategy))
        if (self.ind_runtime is not None and self.ind_runtime.decision is not None
                and not self.ind_runtime.decision['passed']):
            disabled.add(a.STRATEGY_IND)
        for strategy, deadline in self.strategy_deadlines():
            if (now.hour, now.minute) > deadline:
                disabled.add(strategy)
        # 已達每日送單上限的策略停止新候選與分K掃描；既有委託及持倉不受影響。
        disabled.update(strategy for strategy in a.STRATEGY_PRIORITY if self.daily_cap_reached(strategy))
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
            if strategy == getattr(a, 'STRATEGY_IND', None) and (
                    self.ind_runtime is None or self.ind_runtime.decision is None):
                continue  # 大盤資格尚未確認，避免為IND重複掃描整個股票池。
            state = candidates.get(strategy)
            if state is None:
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
        missing = self.ticker_missing_fields(ticker)
        if missing:
            self.tickers.pop(symbol, None)
            a.trade_log('CANDIDATE_TICKER_INCOMPLETE', error=True, symbol=symbol,
                        missing_fields=','.join(missing), action='資格欄位缺失或無效，等待下一輪重查')
            return
        if (not can_trade or disposition
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
        if (self.ind_runtime is not None and a.STRATEGY_IND not in disabled
                and a.STRATEGY_IND not in candidates):
            signal, rejected = a.get_ind_config().find_candidate(
                bars, float(row[5]), self.ind_runtime.decision, cutoff, up, down)
            if rejected:
                disabled.add(a.STRATEGY_IND)
                a.trade_log('IND_REJECTED', symbol=symbol, reason='selection_limit_or_signal_invalid')
            elif signal:
                state = self.add_candidate(symbol, a.STRATEGY_IND, signal, ticker, now)
                state['ind_quote_not_before'] = a.time.monotonic()
        if (hasattr(a, 'STRATEGY_VOL') and a.STRATEGY_VOL not in disabled
                and a.STRATEGY_VOL not in candidates):
            signal, rejected = a.get_vol_config().find_candidate(
                bars, float(row[5]), a.vol_history_by_symbol.get(symbol),
                a.get_vol_reference_date(), cutoff, up)
            if rejected:
                disabled.add(a.STRATEGY_VOL)
                a.trade_log('VOL_REJECTED', symbol=symbol,
                            reason='history_turnover_limit_or_signal_invalid')
            elif signal:
                state = self.add_candidate(symbol, a.STRATEGY_VOL, signal, ticker, now)
                # Never enter using a quote requested before confirmation was available.
                state['vol_quote_not_before'] = a.time.monotonic()
        if (hasattr(a, 'STRATEGY_TOP') and a.STRATEGY_TOP not in disabled
                and a.STRATEGY_TOP not in candidates):
            signal, rejected = a.get_top_config().find_candidate(bars, cutoff, up, down)
            if rejected:
                disabled.add(a.STRATEGY_TOP)
                a.trade_log('TOP_REJECTED', symbol=symbol, reason='limit_touched_deadline_or_bars_invalid')
            elif signal:
                signal['top_prev_range_percent'] = self.top_range_eligibility.get(symbol, (False, None))[1]
                state = self.add_candidate(symbol, a.STRATEGY_TOP, signal, ticker, now)
                state['top_quote_not_before'] = a.time.monotonic()
        if a.STRATEGY_AVG not in disabled:
            checks = self.check_avg_eligibility(symbol, completed, float(row[5]), cutoff, disabled)
            if (a.STRATEGY_AVG not in disabled and a.STRATEGY_AVG not in candidates
                    and 'slope' in checks
                    and (now.hour, now.minute) >= a.AVG_ENTRY_START_TIME):
                signal = a.get_avg_strategy().find_avg_entry_signal(completed, cutoff, candidate_only=True)
                if signal and signal.get('rejected'):
                    disabled.add(a.STRATEGY_AVG)
                    a.trade_log('AVG_ENTRY_REJECTED', symbol=symbol, reason=signal['rejected'])
                elif signal:
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
        self.prune(symbol, now)

    def update_quote(self, state, quote, now):
        a = self.api
        quote = tuple(float(v) if not isinstance(v, bool) and a.finite_positive(v) else None for v in quote)
        px, opening, high, low, close, average, bid, ask = quote
        state.update(pre_last_price=state.get('last_price'), pre_last_price_time=state.get('last_price_time'),
                     last_price=px, last_price_time=now.isoformat() if px is not None else None, open_price=opening,
                     high_price=high, low_price=low, close_price=close, avg_price=average,
                     best_bid_price=bid, best_ask_price=ask)

    def apply_quote(self, state, quote, sdk, now):
        """已送單的唯一狀態只管理委託與持倉，不重新尋找進場策略。"""
        a = self.api
        self.update_quote(state, quote, now)
        if state.get('in_position'):
            a.try_close_position(state, sdk)
        self.persist(state)

    def apply_candidates_quote(self, symbol, quote, states, sdk, now, *, requested_at=None):
        """同一份報價評估所有候選；只有完整符合進場條件才競爭交易歸屬。"""
        a = self.api
        if symbol in states or symbol not in self.rows:
            return
        self.prune(symbol, now)
        if symbol not in self.rows:
            return
        missing = [key for key, value in zip(('lastPrice', 'openPrice', 'highPrice', 'lowPrice'), quote[:4])
                   if isinstance(value, bool) or not a.finite_positive(value)]
        if missing:
            for state in self.candidates.get(symbol, {}).values():
                self.update_quote(state, quote, now)
            a.trade_log('ENTRY_QUOTE_INCOMPLETE', error=True, symbol=symbol, missing_fields=','.join(missing),
                        action='必要欄位缺失或無效，跳過本輪進場，等待下一輪')
            return
        candidates = self.candidates.get(symbol, {})
        px, _, high, low = map(float, quote[:4])
        for strategy in a.STRATEGY_PRIORITY:
            state = candidates.get(strategy)
            if state is None:
                continue
            self.update_quote(state, quote, now)
            is_vol = strategy == getattr(a, 'STRATEGY_VOL', None)
            is_ind = strategy == getattr(a, 'STRATEGY_IND', None)
            is_top = strategy == getattr(a, 'STRATEGY_TOP', None)
            # 所有策略共用：入場前曾觸及漲跌停，當日不得進場。
            if high >= state['limit_up_price'] or low <= state['limit_down_price']:
                self.disabled.setdefault(symbol, set()).add(strategy)
                self.prune(symbol, now)
                a.trade_log('CANDIDATE_LIMIT_TOUCHED', symbol=symbol, strategy=strategy)
                continue
            if not a.check_open_status(state):
                continue
            if not a.get_entry_check_start_time(state) <= (now.hour, now.minute) <= a.get_entry_check_end_time(state):
                continue
            if is_ind:
                if (not state.get('ind_entry_time')
                        or datetime.fromisoformat(state['ind_entry_time']) > now
                        or requested_at is None
                        or requested_at < state.get('ind_quote_not_before', float('inf'))):
                    continue
            elif is_vol:
                if (not state.get('vol_entry_time')
                        or datetime.fromisoformat(state['vol_entry_time']) > now
                        or requested_at is None
                        or requested_at < state.get('vol_quote_not_before', float('inf'))):
                    continue
            elif is_top:
                if (not state.get('top_entry_time')
                        or datetime.fromisoformat(state['top_entry_time']) > now
                        or requested_at is None
                        or requested_at < state.get('top_quote_not_before', float('inf'))):
                    continue
            elif a.is_avg_strategy(state):
                if not state.get('avg_entry_time') or datetime.fromisoformat(state['avg_entry_time']) > now:
                    continue
            else:
                continue
            loss_percent, _ = a.get_optimize_loss_profit_percent(state)
            if float(px) * (1 + loss_percent / 100) >= state['limit_up_price']:
                self.disabled.setdefault(symbol, set()).add(strategy)
                a.trade_log('CANDIDATE_STOP_SPACE_REJECTED', symbol=symbol, strategy=strategy)
                continue
            if is_ind and not a.check_ind_entry_market(state, now):
                self.prune(symbol, now)
                continue
            if self.daily_cap_reached(strategy):
                # prune 已會移除達上限策略的候選；此處為送單前的第二道防線。
                self.disabled.setdefault(symbol, set()).add(strategy)
                self.prune(symbol, now)
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
                    self.entry_attempts[strategy] = self.entry_attempts.get(strategy, 0) + 1
                    a.trade_log('STRATEGY_LOCKED', symbol=symbol, strategy=strategy,
                                reason='entry_submission', signal_time=state.get('entry_signal_time'),
                                daily_attempts=self.entry_attempts[strategy],
                                daily_cap=self.daily_trade_cap(strategy))
                    if self.daily_cap_reached(strategy):
                        a.trade_log('STRATEGY_DAILY_CAP_REACHED', strategy=strategy,
                                    cap=self.daily_trade_cap(strategy), attempts=self.entry_attempts[strategy],
                                    action='停止此策略新候選與進場，既有委託及持倉照常管理')
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

    def process_ind_market(self, sdk, now):
        """指數分K背景查詢全股票共用；只有主監控更新一次性初始判定。"""
        if self.ind_runtime is None:
            return
        a = self.api
        config = a.get_ind_config()
        runtime = self.ind_runtime
        monotonic = a.time.monotonic()
        if self.ind_market_future is not None and self.ind_market_future.done():
            future, self.ind_market_future = self.ind_market_future, None
            try:
                response, requested_at = future.result()
                cutoff = min(requested_at, now)
                bars = a.parse_avg_candles(response.get('data'), cutoff)
                decision = runtime.confirm_market(config, bars,
                    a.get_ind_market_snapshot()['previous_close'], cutoff)
                if decision is not None:
                    a.trade_log('IND_MARKET_DECISION', passed=decision['passed'],
                                market_time=decision['dt'].isoformat(), change=decision['change'],
                                market_drop_percent=-config.market_drop_percent,
                                normalized_close_slope=decision.get('normalized_close_slope'),
                                market_max_normalized_slope=config.market_max_normalized_slope)
                    if decision['passed']:
                        self.last_scan = None  # 本分鐘先前可能因等待大盤而略過IND，立即允許首輪掃描。
            except Exception as exc:
                a.trade_log('IND_MARKET_CANDLE_ERROR', error=True, error_msg=repr(exc))
            self.ind_market_retry_at = monotonic + a.IND_MARKET_RETRY_SECONDS
        if (now.hour, now.minute) > config.entry_deadline:
            return
        decision_ready = now.replace(hour=config.market_check_time[0],
                                     minute=config.market_check_time[1], second=0, microsecond=0) + timedelta(minutes=1)
        if (runtime.decision is None and self.ind_market_future is None
                and now >= decision_ready and monotonic >= self.ind_market_retry_at):
            self.ind_market_future = a.MARKET_SCAN_EXECUTOR.submit(a.fetch_ind_market_candles, sdk)

    def process(self, states, sdk, trade_sdk):
        a = self.api
        now = a.now_tpe()
        self.process_ind_market(sdk, now)
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
        minute = now.strftime('%Y-%m-%d %H:%M')
        starts = []
        if self.strategy_enabled(a.STRATEGY_AVG):
            starts.append(min(a.AVG_ENTRY_START_TIME, a.AVG_SLOPE_CUTOFF_TIME))
        if hasattr(a, 'STRATEGY_VOL') and self.strategy_enabled(a.STRATEGY_VOL):
            starts.append(a.VOL_SIGNAL_START_TIME)
        if self.ind_runtime is not None and self.strategy_enabled(a.STRATEGY_IND):
            starts.append(min(a.IND_MARKET_CHECK_TIME, a.IND_STOCK_SCAN_START_TIME))
        if hasattr(a, 'STRATEGY_TOP') and self.strategy_enabled(a.STRATEGY_TOP):
            starts.append(a.TOP_SIGNAL_START_TIME)
        start = min(starts) if starts else (23, 59)
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
                        self.apply_candidates_quote(symbol, quote, states, trade_sdk, now,
                                                    requested_at=requested_at)
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
                    if state.get('in_position') and (a.force_close_time_reached(state) or state.get('exit_retry_reason')):
                        a.try_close_position(state, trade_sdk)
                    if state.get('traded') and not state.get('in_position'):
                        continue
                elif not self.candidates.get(symbol):
                    continue
                if request_quotes and symbol not in self.quotes:
                    position = bool(state and state.get('in_position'))
                    priority = a.MARKETDATA_PRIORITY_POSITION if position else a.MARKETDATA_PRIORITY_CANDIDATE
                    executor = a.AVG_POSITION_QUOTE_EXECUTOR if position else a.CANDIDATE_QUOTE_EXECUTOR
                    candidate = self.candidates.get(symbol, {}).get(getattr(a, 'STRATEGY_IND', None))
                    if candidate is not None and state is None:
                        self.quotes[symbol] = executor.submit(
                            a.fetch_candidate_quote, symbol, sdk, priority,
                            not_before=candidate['ind_quote_not_before'])
                    else:
                        self.quotes[symbol] = executor.submit(a.fetch_candidate_quote, symbol, sdk, priority)
            except Exception as exc:
                a.trade_log('CANDIDATE_MONITOR_ERROR', error=True, symbol=symbol, error_msg=repr(exc))
