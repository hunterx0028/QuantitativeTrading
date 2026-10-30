# -*- coding: utf-8 -*-
import os
import json
import time
import sys
import math
import shutil
import io
import threading
import uuid
import requests
from contextlib import contextmanager
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from decimal import Decimal
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta
import pytz
from tempfile import NamedTemporaryFile
from configparser import ConfigParser
from esun_trade.sdk import SDK
from esun_trade.order import OrderObject
from esun_trade.constant import (APCode, Trade, PriceFlag, Action)
from esun_marketdata import EsunMarketdata

from runtime_environment import RuntimeEnvironment

# Runtime data is loaded only after local/container preparation.
stock_data = None
selected_stocks = []
market_previous_close_indices = {}
total_stocks = []
from strategies.fall import FallConfig, FallRuntime, evaluate_fall_quote
from strategies.fall import fetch_screen_result, fetch_quote_snapshot
from strategies.avg import AvgConfig, AvgRuntime, AvgServices, AvgStrategy
from strategies.lower import LowerConfig, LowerRuntime, LowerServices, LowerStrategy


class TeeStream:
    """同時輸出到原始串流與記憶體緩衝。"""
    def __init__(self, original_stream, mirror_stream):
        self.original_stream = original_stream
        self.mirror_stream = mirror_stream

    def write(self, data):
        self.original_stream.write(data)
        self.mirror_stream.write(data)
        self.flush()
        return len(data)

    def flush(self):
        self.original_stream.flush()
        self.mirror_stream.flush()

    def isatty(self):
        return self.original_stream.isatty()

# ============ 參數/常數 ============
# 已知對齊原則與刻意差異：
# 1. 回測版以分 K 模擬策略，實機版以 REST 即時 quote 執行；資料粒度差異不視為策略不一致。
# 2. 保本與逐步獲利為實機版特有風控；回測維持固定停損/停利/收盤結算模型。
TZ = pytz.timezone("Asia/Taipei")
BASE_DIR = os.path.dirname(__file__)
STATE_DIR = os.path.join(BASE_DIR, "stock_state")  # 狀態檔目錄
# 部署前提：本程式不允許盤中補啟動；每日只會在 MAIN_START_TIME 前啟動一次。
# 若該次啟動失敗，當日不會再由排程重新啟動，因此不處理盤中恢復或補單情境。
MAIN_START_TIME = (8, 50)  # 主程序開始執行時間
FORCE_EXIT_TIME = (13, 30)  # 13:30 強制關閉程式

# 單日策略前提：每檔股票在當日只會有一個固定交易方向。
STRATEGY_LOWER = 'LOWER'
STRATEGY_AVG = 'AVG'
STRATEGY_FALL = 'FALL'
TRADE_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'LONG'

# 目前受交易規格與額度限制，每檔股票最多只交易一張；實際運行不會出現
# 多張委託中部分張數成功、部分張數失敗的情境。成交回報仍保留一般性防禦處理。
ENTRY_ORDER_QUANTITY_LOWER = 1 # lower 每次進場下單數量

PROTECT_PROFIT_SWITCH = True # 所有策略共用：False 不啟動獲利保護；True 啟動獲利保護
PROTECT_PROFIT_PER = 2.5 # 所有策略共用：觸發獲利保護百分比
PROTECT_LOSS_PER = 1.5 # 所有策略共用：獲利保護後的新停損百分比

PROFIT_BACK_PERCENT = 0.5 # 獲利後允許回撤百分比
PROFIT_TARGET_PERCENT = 1.0 # 逐步獲利目標百分比

REALTIME_QUOTE_START_TIME = (9, 3)  # 09:03 後才開始抓個股即時行情，避開開盤初期 quote 欄位不完整

LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME = (9, 6)  # LOWER 指數位於昨收兩側的 NO_TRADE 檢查起始時間（含）
LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE = (9, 21)  # IX0001/IX0043 早盤須先向下突破各自 LOWER 門檻的截止時間（含）
LOWER_STRATEGY_DECISION_1 = (9, 31)  # LOWER 市場模式判斷截止時間，不含此時間
STRATEGY_START_LOWER_1 = (9, 32)  # lower 進場檢核開始時間（含）
STRATEGY_END_LOWER_1 = (10, 1)  # lower 進場檢核截止時間（含）
LOWER_STRATEGY_DECISION_2 = (9, 41)
STRATEGY_START_LOWER_2 = (9, 42)
STRATEGY_END_LOWER_2 = (10, 11)
LOWER_STRATEGY_DECISION_3 = (9, 51)
STRATEGY_START_LOWER_3 = (9, 52)
STRATEGY_END_LOWER_3 = (10, 21)
MIN_MINUTE_BARS_BEFORE_0930 = 20
FORCE_CLOSE_TIME_LOWER = (12, 50)  # lower 收盤前強制平倉時間
OPTIMIZE_PROFIT_PER_LOWER = 5.0 # lower 停利百分比(%)，例如 5.0 代表入場價減去 5%
OPTIMIZE_LOSS_PER_LOWER = 2.0 # lower 停損百分比(%)，例如 3.0 代表入場價加上 3%

LOWER_ENTRY_RANGE_START_PERCENT = 10.0 # lower 入場價距昨收到跌停的起始百分比
LOWER_ENTRY_RANGE_END_PERCENT = 60.0 # lower 入場價距昨收到跌停的結束百分比
LOWER_DECISION_DECLINE_PERCENT_THRESHOLD = 35.0 # 各決策時點的股票下降比例須嚴格大於此值
LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD = 65.0 # 同一比例需嚴格小於此值，即下限 < 比例 < 上限
LOWER_EXIT_PERCENT_THRESHOLD = 40.0

IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.2 # IX0001 早盤啟動門檻：截止時間前即時值需低於前日最後 close 的百分比
IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.9 # IX0001 各決策時點的最新有效即時值須低於此門檻
IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.0 # IX0043 早盤啟動門檻：截止時間前即時值需低於前日最後 close 的百分比
IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.75 # IX0043 各決策時點的最新有效即時值須低於此門檻
# 產業盤勢過濾：LOWER 作空須嚴格低於昨收下跌門檻。
INDUSTRY_MARKET_FILTER_SHORT_PERCENT = 0 # LOWER 作空：產業指數需低於昨收下跌此百分比的門檻

# AVG 參數獨立於 LOWER。
AVG_STOCK_DATA_SOURCE = "T"  # T: total_stocks；S: selected_stocks，均使用獨立副本
AVG_CANDLES_START_TIME = STRATEGY_START_LOWER_3
AVG_STAGE_1_START_TIME = (9, 0)
AVG_STAGE_1_MIN_BARS = 8
AVG_STAGE_2_MIN_BARS = 8
AVG_STAGE_3_MIN_BARS = 5
AVG_STAGE_4_MIN_BARS = 2
AVG_STAGE_1_AMPLITUDE_PERCENT = 1.0
AVG_STAGE_2_AMPLITUDE_PERCENT = 1.0
AVG_STAGE_3_AMPLITUDE_PERCENT = 1.0
AVG_SLOPE_CUTOFF_TIME = (9, 20)
AVG_MAX_NORMALIZED_SLOPE = 0.06  # 相對昨收 %／分鐘，須嚴格小於此值
AVG_ENTRY_START_TIME = (10, 0)
AVG_ENTRY_DEADLINE = (11, 0)
# 不設秒數期限；僅接受當分鐘的入場棒，避免追補歷史訊號。
AVG_CANDLE_POLL_SECOND = 15  # 每分鐘於此秒數取一次完整分K，例如 09:33:15
AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS = 0.105  # 最多約 9.5 次請求／秒
AVG_MIN_PRE_ENTRY_RANGE_PERCENT = 5.0
AVG_MIN_MINUTE_BARS_BEFORE_0930 = 20
AVG_STOP_LOSS_PERCENT = 2.0
AVG_TAKE_PROFIT_PERCENT = 5.0
AVG_FORCE_CLOSE_TIME = (12, 50)
AVG_ENTRY_ORDER_QUANTITY = 1
AVG_PROTECT_PROFIT_SWITCH = True
AVG_PROTECT_PROFIT_PER = 2.5
AVG_PROTECT_LOSS_PER = 1.5
AVG_PROFIT_BACK_PERCENT = 0.5
AVG_PROFIT_TARGET_PERCENT = 1.0

# FALL 獨立於 LOWER/AVG 市場分流；候選當日專屬，不釋回其他策略。
FALL_STOCK_DATA_SOURCE = "T"
FALL_SIGNAL_START_TIME = (9, 0)
FALL_SIGNAL_END_TIME = (9, 10)  # 包含此根 close，09:11 後才能開始篩選
FALL_MIN_DROP_PERCENT = 1.0
FALL_MAX_DEVIATION_RATIO = 0.25
FALL_ENTRY_DEADLINE = (11, 50)  # 含此分鐘
FALL_CANDLE_PUBLISH_DELAY_SECONDS = 15
FALL_STOP_LOSS_PERCENT = 2.0
FALL_TAKE_PROFIT_PERCENT = 5.0
FALL_FORCE_CLOSE_TIME = (12, 50)
FALL_ENTRY_ORDER_QUANTITY = 1
FALL_PROTECT_PROFIT_SWITCH = True
FALL_PROTECT_PROFIT_PER = 2.5
FALL_PROTECT_LOSS_PER = 1.5
FALL_PROFIT_BACK_PERCENT = 0.5
FALL_PROFIT_TARGET_PERCENT = 1.0

ORDER_RESULTS_UPDATE_SECONDS = 10.0  # 成交價量校正查詢間隔，避免每筆下單後立即查詢造成交易 API 連線異常
ORDER_RESULTS_QUERY_START_TIME = (9, 0)  # 開盤前不查委託結果，09:00 後才補成交價量
ORDER_RESULTS_RATE_LIMIT_COOLDOWN_SECONDS = 60.0  # AGR0005 後依券商指示暫停查詢
MARKET_INDEX_REST_POLL_SECONDS = 5
MARKET_INDEX_REST_POLL_OFFSET_SECONDS = 1.0  # 避開供應商整 5 秒快照剛發布的邊界
MARKET_INDEX_REST_START_TIME = REALTIME_QUOTE_START_TIME
MARKET_INDEX_STALE_SECONDS = 17.0  # 容許三個 5 秒供應週期及少量網路/排程誤差
MARKET_INDEX_FUTURE_TOLERANCE_SECONDS = 1.0  # 容許供應商與本機最多 1 秒時鐘偏差
MARKET_INDEX_CRITICAL_WAIT_SECONDS = 13.0  # 關鍵判斷資料逾時時，最多等待既有背景輪次恢復為新鮮資料
INDUSTRY_INDEX_STALE_SECONDS = 20.0  # 產業指數配合每分鐘四輪 REST 查詢
QUOTE_ROUND_INTERVAL_SECONDS = 15.0
LOWER_EXIT_CHECK_INTERVAL_MINUTES = 10
INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE = 600  # 玉山行情官方日內行情額度
ORDER_UNKNOWN_ALERT_SECONDS = 30.0
ORDER_SUBMIT_WAIT_SECONDS = 5.0  # 主迴圈最多等待送單結果；逾時不代表拒單。
TRADE_API_DEGRADED_EVENT = threading.Event()  # 當日停止新進場；既有委託仍須對帳。
TRADE_CALL_STATE_LOCK = threading.Lock()
TRADE_CALL_STATE: Dict[str, Any] = {}
ORDER_SUBMISSION_FUTURES: Dict[str, Future] = {}
ENTRY_CANCEL_FUTURES: Dict[str, tuple] = {}

ACTIVE_ORDER_STATES: Dict[str, Dict[str, Any]] = {}
PENDING_DEALT_REPORTS: Dict[str, List[Dict[str, Any]]] = {}
DEALT_REPORT_LOCK = threading.Lock()
ORDER_STATE_LOCK = threading.RLock()
TRADE_API_LOCK = threading.Lock()  # native SDK 的查詢與送單不可並行。
SEEN_DEALT_REPORT_KEYS: set[tuple[str, str]] = set()
ORDER_RESULTS_COOLDOWN_UNTIL_MONOTONIC = 0.0

MARKET_INDEX_STATE: Dict[str, Dict[str, Any]] = {}
MARKET_INDEX_STATE_LOCK = threading.RLock()
MARKET_INDEX_ROUND_CONDITION = threading.Condition(MARKET_INDEX_STATE_LOCK)
MARKET_INDEX_ROUND_STATE: Dict[str, Any] = {
    "completed_round": 0,
    "completed_at": None,
    "snapshots": {},
}
MARKETDATA_STOCK_REST_CLIENT: Any = None
MARKET_GATE_INDEX_KEYS = ("TWSE:MARKET", "TPEX:MARKET")
MARKET_INDEX_REST_STOP_EVENT = threading.Event()
MARKET_INDEX_REST_FATAL_EVENT = threading.Event()
MARKET_REVERSAL_STOP_EVENT = threading.Event()
LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_CHECK_ANNOUNCED_EVENT = threading.Event()
ENTRY_MODE_NO_TRADE = 0
ENTRY_MODE_LOWER = 2
ENTRY_MODE_AVG = 3

AVG_CANDLE_EXECUTOR = ThreadPoolExecutor(max_workers=12, thread_name_prefix="avg-candles")

AVG_POSITION_QUOTE_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="avg-position-quote")

MARKET_SCAN_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="market-scan")
LOWER_DATA_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="lower-data")
LOWER_QUOTE_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="lower-quote")
INDEX_DATA_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="industry-data")


INDUSTRY_INDEX_FUTURES: Dict[str, Future] = {}
ORDER_RESULTS_PENDING_FUTURE: Optional[Future] = None
MARKETDATA_STOP_EVENT = threading.Event()

AVG_RUNTIME = AvgRuntime()
LOWER_RUNTIME = LowerRuntime()
FALL_RUNTIME = FallRuntime()
FALL_SCREEN_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="fall-screen")
FALL_QUOTE_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="fall-quote")
# 所有日內 stock REST 共用節流，保留 600/min 額度餘裕，避免兩套背景工作互相超額。
STOCK_REST_REQUEST_INTERVAL_SECONDS = 0.125
MARKETDATA_PRIORITY_POSITION = 0
MARKETDATA_PRIORITY_INDEX = 5
MARKETDATA_PRIORITY_CANDIDATE = 10
MARKETDATA_PRIORITY_SCAN = 50
MARKETDATA_REQUEST_CONTEXT = threading.local()
QUOTE_CACHE_LOCK = threading.Lock()
QUOTE_ROUND_CACHE: Dict[str, tuple] = {}

class MarketDataRequestGate:
    """統一發放行情請求時槽；等待期間釋放鎖，較高優先序可先取下一個時槽。"""
    def __init__(self, interval=STOCK_REST_REQUEST_INTERVAL_SECONDS):
        self.interval = interval
        self.condition = threading.Condition()
        self.waiting = []
        self.sequence = 0
        self.next_start = 0.0

    def acquire(self, priority):
        with self.condition:
            ticket = (priority, self.sequence)
            self.sequence += 1
            self.waiting.append(ticket)
            self.condition.notify_all()
            try:
                while True:
                    if MARKETDATA_STOP_EVENT.is_set():
                        raise RuntimeError("行情查詢已停止")
                    delay = self.next_start - time.monotonic()
                    if ticket == min(self.waiting) and delay <= 0:
                        self.next_start = time.monotonic() + self.interval
                        return
                    self.condition.wait(timeout=min(max(delay, 0.001), 0.1))
            finally:
                self.waiting.remove(ticket)
                self.condition.notify_all()


MARKETDATA_REQUEST_GATE = MarketDataRequestGate()


@contextmanager
def marketdata_priority(priority):
    previous = getattr(MARKETDATA_REQUEST_CONTEXT, 'priority', MARKETDATA_PRIORITY_SCAN)
    MARKETDATA_REQUEST_CONTEXT.priority = priority
    try:
        yield
    finally:
        MARKETDATA_REQUEST_CONTEXT.priority = previous


class RateLimitedIntraday:
    def __init__(self, client):
        self.client = client

    def __getattr__(self, name):
        callback = getattr(self.client, name)
        if not callable(callback):
            return callback

        def request(*args, **kwargs):
            priority = getattr(MARKETDATA_REQUEST_CONTEXT, 'priority', MARKETDATA_PRIORITY_SCAN)
            MARKETDATA_REQUEST_GATE.acquire(priority)
            return callback(*args, **kwargs)
        return request


class RateLimitedStockRest:
    def __init__(self, client):
        self.client = client
        self.intraday = RateLimitedIntraday(client.intraday)

    def __getattr__(self, name):
        return getattr(self.client, name)


def is_fall_strategy(state):
    return bool(state and state.get("strategy_type") == STRATEGY_FALL)


def all_order_states(states):
    """市場計算仍用原 states，委託與對帳採每檔唯一的交易擁有者。"""
    with ORDER_STATE_LOCK:
        return {**states, **FALL_RUNTIME.states}


def refresh_active_order_states(states):
    with ORDER_STATE_LOCK:
        ACTIVE_ORDER_STATES.clear()
        ACTIVE_ORDER_STATES.update(all_order_states(states))


def fall_blocks_other_entry(state):
    symbol = state.get("symbol_code_with_suf")
    return not is_fall_strategy(state) and (
        symbol in FALL_RUNTIME.candidate_symbols or symbol in FALL_RUNTIME.screen_pending_symbols)


def get_fall_config() -> FallConfig:
    """從現有調參入口建立設定快照，避免模組持有過期的參數副本。"""
    return FallConfig(
        stock_data_source=FALL_STOCK_DATA_SOURCE,
        signal_start_time=FALL_SIGNAL_START_TIME,
        signal_end_time=FALL_SIGNAL_END_TIME,
        min_drop_percent=FALL_MIN_DROP_PERCENT,
        max_deviation_ratio=FALL_MAX_DEVIATION_RATIO,
        entry_deadline=FALL_ENTRY_DEADLINE,
        candle_publish_delay_seconds=FALL_CANDLE_PUBLISH_DELAY_SECONDS,
        stop_loss_percent=FALL_STOP_LOSS_PERCENT,
        take_profit_percent=FALL_TAKE_PROFIT_PERCENT,
        force_close_time=FALL_FORCE_CLOSE_TIME,
        entry_order_quantity=FALL_ENTRY_ORDER_QUANTITY,
        protect_profit_switch=FALL_PROTECT_PROFIT_SWITCH,
        protect_profit_per=FALL_PROTECT_PROFIT_PER,
        protect_loss_per=FALL_PROTECT_LOSS_PER,
        profit_back_percent=FALL_PROFIT_BACK_PERCENT,
        profit_target_percent=FALL_PROFIT_TARGET_PERCENT,
    )


def validate_fall_config():
    get_fall_config().validate(time_tuple_to_minutes)


def initialize_fall_session():
    validate_fall_config()
    FALL_RUNTIME.reset()
    count = FALL_RUNTIME.initialize_universe(
        total_stocks if FALL_STOCK_DATA_SOURCE == "T" else selected_stocks,
        get_pure_symbol,
    )
    trade_log("FALL_UNIVERSE", source=FALL_STOCK_DATA_SOURCE, count=count)


def find_fall_signal(bars, now_local):
    """沿用主程式參數，將純訊號計算交給 FALL 模組。"""
    return get_fall_config().find_signal(bars, now_local)


def fetch_fall_screen_result(row, realtime_sdk):
    return fetch_screen_result(
        row, realtime_sdk, get_pure_symbol=get_pure_symbol,
        get_stock_rest_client=get_stock_rest_client, now=now_tpe,
        parse_candles=parse_avg_candles, find_signal=find_fall_signal,
        get_up_down_price=get_up_down_price,
    )


def register_fall_candidate(row, signal, ticker, states):
    FALL_RUNTIME.register_candidate(
        row, signal, ticker, states, get_pure_symbol=get_pure_symbol,
        all_order_states=all_order_states, finite_positive=finite_positive,
        build_initial_state=build_initial_state, entry_order_quantity=FALL_ENTRY_ORDER_QUANTITY,
        strategy_type=STRATEGY_FALL, trade_side_short=TRADE_SIDE_SHORT,
        order_state_lock=ORDER_STATE_LOCK, refresh_active_order_states=refresh_active_order_states,
        persist_state=lambda symbol, state: atomic_write_json(state_path(symbol), state),
        trade_log=trade_log,
    )


def fall_work_pending():
    return FALL_RUNTIME.work_pending()


def process_fall_round(states, realtime_sdk, mysdk, *, request_quotes):
    now_local = now_tpe()
    FALL_RUNTIME.process_screen_round(
        states, realtime_sdk, now_local=now_local, config=get_fall_config(),
        get_pure_symbol=get_pure_symbol, register_candidate=register_fall_candidate,
        screen_executor=FALL_SCREEN_EXECUTOR, fetch_screen_result=fetch_fall_screen_result,
        trade_log=trade_log,
    )

    # 獨立於市場分流、LOWER 停止及 AVG 分K循環，持倉/未知委託始終保留。
    FALL_RUNTIME.process_position_round(
        mysdk, now_local=now_local, entry_deadline=FALL_ENTRY_DEADLINE,
        order_state_lock=ORDER_STATE_LOCK, monotonic=time.monotonic,
        quote_round_interval_seconds=QUOTE_ROUND_INTERVAL_SECONDS,
        force_close_time_reached=force_close_time_reached, close_position=try_close_position,
        check_open_status=check_open_status,
        persist_state=lambda symbol, state: atomic_write_json(state_path(symbol), state),
        apply_quote=apply_fall_quote, trade_log=trade_log,
    )


def fetch_fall_quote(symbol, realtime_sdk, priority=MARKETDATA_PRIORITY_CANDIDATE):
    return fetch_quote_snapshot(
        symbol, realtime_sdk, priority, monotonic=time.monotonic,
        get_realtime_price=get_realtime_price,
    )


def poll_fall_quotes_once(realtime_sdk):
    """只抓行情與發布最新快照；不得在行情執行緒操作委託或交易狀態。"""
    FALL_RUNTIME.poll_quotes_once(
        realtime_sdk, now=now_tpe, order_state_lock=ORDER_STATE_LOCK,
        entry_deadline=FALL_ENTRY_DEADLINE, quote_round_interval_seconds=QUOTE_ROUND_INTERVAL_SECONDS,
        fetch_quote=fetch_fall_quote, position_executor=AVG_POSITION_QUOTE_EXECUTOR,
        candidate_executor=FALL_QUOTE_EXECUTOR, position_priority=MARKETDATA_PRIORITY_POSITION,
        candidate_priority=MARKETDATA_PRIORITY_CANDIDATE, trade_log=trade_log,
    )


def start_fall_quote_poller(realtime_sdk):
    def worker():
        while not FALL_RUNTIME.quote_poller_stop_event.is_set():
            try:
                poll_fall_quotes_once(realtime_sdk)
            except Exception as exc:
                trade_log("FALL_QUOTE_POLLER_ERROR", error=True, error_msg=repr(exc))
            FALL_RUNTIME.quote_poller_stop_event.wait(0.5)
    thread = threading.Thread(target=worker, name="fall-quotes", daemon=True)
    thread.start()
    return thread


def apply_fall_quote(state, quote, mysdk):
    action = evaluate_fall_quote(
        state, quote, now=now_tpe,
        finite_positive=finite_positive, check_open_status=check_open_status,
    )
    if action == "invalid":
        return
    if action == "close":
        try_close_position(state, mysdk)
    elif action == "open":
        try_open_position(state, mysdk)
    atomic_write_json(state_path(state["symbol_code_with_suf"]), state)


class OrderOutcomeUnknown(RuntimeError):
    """已嘗試送單，尚不能證明券商接受或拒絕；禁止直接補單。"""

    def __init__(self, request_id: str):
        super().__init__(f"委託結果未知，待對帳 request_id={request_id}")
        self.request_id = request_id


class TradeApiBusy(RuntimeError):
    """尚未送單；查詢佔用 SDK 時保留狀態，下一輪再嘗試。"""


def finite_positive(value: Any) -> bool:
    try:
        return math.isfinite(float(value)) and float(value) > 0
    except (TypeError, ValueError, OverflowError):
        return False


def _log_value(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value)
    if not text:
        return "-"
    text = text.replace("\n", "\\n")
    if any(ch.isspace() for ch in text) or "=" in text:
        return json.dumps(text, ensure_ascii=False)
    return text


def trade_log(event: str, *, error: bool = False, **fields: Any) -> None:
    parts = [
        f"[{event}]",
        f"time={datetime.now(TZ).strftime('%H:%M:%S')}",
    ]
    for key, value in fields.items():
        parts.append(f"{key}={_log_value(value)}")
    print(" ".join(parts), file=sys.stderr if error else sys.stdout)


def state_symbol_fields(state: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": state.get("symbol_code_with_suf"),
        "name": state.get("symbol_name"),
        "strategy": state.get("strategy_type") or get_entry_mode_text(),
        "side": state.get("side"),
    }



def get_avg_strategy() -> AvgStrategy:
    """使用目前參數與股票池建立策略視圖；同日狀態共用同一份。"""
    return AvgStrategy(
        config=AvgConfig(
            AVG_CANDLES_START_TIME=AVG_CANDLES_START_TIME,
            AVG_CANDLE_POLL_SECOND=AVG_CANDLE_POLL_SECOND,
            AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS=AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS,
            AVG_ENTRY_DEADLINE=AVG_ENTRY_DEADLINE,
            AVG_ENTRY_ORDER_QUANTITY=AVG_ENTRY_ORDER_QUANTITY,
            AVG_ENTRY_START_TIME=AVG_ENTRY_START_TIME,
            AVG_FORCE_CLOSE_TIME=AVG_FORCE_CLOSE_TIME,
            AVG_MAX_NORMALIZED_SLOPE=AVG_MAX_NORMALIZED_SLOPE,
            AVG_MIN_MINUTE_BARS_BEFORE_0930=AVG_MIN_MINUTE_BARS_BEFORE_0930,
            AVG_MIN_PRE_ENTRY_RANGE_PERCENT=AVG_MIN_PRE_ENTRY_RANGE_PERCENT,
            AVG_PROFIT_BACK_PERCENT=AVG_PROFIT_BACK_PERCENT,
            AVG_PROFIT_TARGET_PERCENT=AVG_PROFIT_TARGET_PERCENT,
            AVG_PROTECT_LOSS_PER=AVG_PROTECT_LOSS_PER,
            AVG_PROTECT_PROFIT_PER=AVG_PROTECT_PROFIT_PER,
            AVG_SLOPE_CUTOFF_TIME=AVG_SLOPE_CUTOFF_TIME,
            AVG_STAGE_1_AMPLITUDE_PERCENT=AVG_STAGE_1_AMPLITUDE_PERCENT,
            AVG_STAGE_1_MIN_BARS=AVG_STAGE_1_MIN_BARS,
            AVG_STAGE_1_START_TIME=AVG_STAGE_1_START_TIME,
            AVG_STAGE_2_AMPLITUDE_PERCENT=AVG_STAGE_2_AMPLITUDE_PERCENT,
            AVG_STAGE_2_MIN_BARS=AVG_STAGE_2_MIN_BARS,
            AVG_STAGE_3_AMPLITUDE_PERCENT=AVG_STAGE_3_AMPLITUDE_PERCENT,
            AVG_STAGE_3_MIN_BARS=AVG_STAGE_3_MIN_BARS,
            AVG_STAGE_4_MIN_BARS=AVG_STAGE_4_MIN_BARS,
            AVG_STOCK_DATA_SOURCE=AVG_STOCK_DATA_SOURCE,
            AVG_STOP_LOSS_PERCENT=AVG_STOP_LOSS_PERCENT,
            AVG_TAKE_PROFIT_PERCENT=AVG_TAKE_PROFIT_PERCENT,
            INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE=INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE,
            MARKETDATA_PRIORITY_POSITION=MARKETDATA_PRIORITY_POSITION,
            STRATEGY_AVG=STRATEGY_AVG,
            STRATEGY_START_LOWER_3=STRATEGY_START_LOWER_3,
            TRADE_SIDE_SHORT=TRADE_SIDE_SHORT,
        ),
        runtime=AVG_RUNTIME,
        services=AvgServices(
            AVG_CANDLE_EXECUTOR=AVG_CANDLE_EXECUTOR,
            AVG_POSITION_QUOTE_EXECUTOR=AVG_POSITION_QUOTE_EXECUTOR,
            MARKET_INDEX_REST_FATAL_EVENT=MARKET_INDEX_REST_FATAL_EVENT,
            TZ=TZ,
            TradeApiBusy=TradeApiBusy,
            atomic_write_json=atomic_write_json,
            ceil_price_to_tick_value=ceil_price_to_tick_value,
            floor_price_to_tick_value=floor_price_to_tick_value,
            get_realtime_price=get_realtime_price,
            get_stock_rest_client=get_stock_rest_client,
            initialize_states=initialize_states,
            now_tpe=now_tpe,
            selected_stocks=selected_stocks,
            state_path=state_path,
            state_symbol_fields=state_symbol_fields,
            time_tuple_to_minutes=time_tuple_to_minutes,
            total_stocks=total_stocks,
            trade_log=trade_log,
            try_open_position=try_open_position,
        ),
    )


def get_lower_strategy() -> LowerStrategy:
    """使用目前參數與股票池建立策略視圖；同日狀態共用同一份。"""
    return LowerStrategy(
        config=LowerConfig(
            ENTRY_MODE_LOWER=ENTRY_MODE_LOWER,
            ENTRY_MODE_NO_TRADE=ENTRY_MODE_NO_TRADE,
            ENTRY_ORDER_QUANTITY_LOWER=ENTRY_ORDER_QUANTITY_LOWER,
            FORCE_CLOSE_TIME_LOWER=FORCE_CLOSE_TIME_LOWER,
            INDUSTRY_MARKET_FILTER_SHORT_PERCENT=INDUSTRY_MARKET_FILTER_SHORT_PERCENT,
            IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER=IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER,
            IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER=IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER,
            IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER=IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER,
            IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER=IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER,
            LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD=LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD,
            LOWER_DECISION_DECLINE_PERCENT_THRESHOLD=LOWER_DECISION_DECLINE_PERCENT_THRESHOLD,
            LOWER_ENTRY_RANGE_END_PERCENT=LOWER_ENTRY_RANGE_END_PERCENT,
            LOWER_ENTRY_RANGE_START_PERCENT=LOWER_ENTRY_RANGE_START_PERCENT,
            LOWER_EXIT_CHECK_INTERVAL_MINUTES=LOWER_EXIT_CHECK_INTERVAL_MINUTES,
            LOWER_EXIT_PERCENT_THRESHOLD=LOWER_EXIT_PERCENT_THRESHOLD,
            LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME=LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME,
            LOWER_STRATEGY_DECISION_1=LOWER_STRATEGY_DECISION_1,
            LOWER_STRATEGY_DECISION_2=LOWER_STRATEGY_DECISION_2,
            LOWER_STRATEGY_DECISION_3=LOWER_STRATEGY_DECISION_3,
            LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE=LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE,
            MARKETDATA_PRIORITY_POSITION=MARKETDATA_PRIORITY_POSITION,
            MARKET_INDEX_CRITICAL_WAIT_SECONDS=MARKET_INDEX_CRITICAL_WAIT_SECONDS,
            MIN_MINUTE_BARS_BEFORE_0930=MIN_MINUTE_BARS_BEFORE_0930,
            STRATEGY_END_LOWER_1=STRATEGY_END_LOWER_1,
            STRATEGY_END_LOWER_2=STRATEGY_END_LOWER_2,
            STRATEGY_END_LOWER_3=STRATEGY_END_LOWER_3,
            STRATEGY_START_LOWER_1=STRATEGY_START_LOWER_1,
            STRATEGY_START_LOWER_2=STRATEGY_START_LOWER_2,
            STRATEGY_START_LOWER_3=STRATEGY_START_LOWER_3,
        ),
        runtime=LOWER_RUNTIME,
        services=LowerServices(
            AVG_POSITION_QUOTE_EXECUTOR=AVG_POSITION_QUOTE_EXECUTOR,
            LOWER_DATA_EXECUTOR=LOWER_DATA_EXECUTOR,
            LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_CHECK_ANNOUNCED_EVENT=LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_CHECK_ANNOUNCED_EVENT,
            LOWER_QUOTE_EXECUTOR=LOWER_QUOTE_EXECUTOR,
            MARKETDATA_PRIORITY_CANDIDATE=MARKETDATA_PRIORITY_CANDIDATE,
            MARKETDATA_STOP_EVENT=MARKETDATA_STOP_EVENT,
            MARKET_GATE_INDEX_KEYS=MARKET_GATE_INDEX_KEYS,
            MARKET_INDEX_FUTURE_TOLERANCE_SECONDS=MARKET_INDEX_FUTURE_TOLERANCE_SECONDS,
            MARKET_INDEX_REST_FATAL_EVENT=MARKET_INDEX_REST_FATAL_EVENT,
            MARKET_INDEX_REST_STOP_EVENT=MARKET_INDEX_REST_STOP_EVENT,
            MARKET_INDEX_STALE_SECONDS=MARKET_INDEX_STALE_SECONDS,
            MARKET_INDEX_STATE=MARKET_INDEX_STATE,
            MARKET_INDEX_STATE_LOCK=MARKET_INDEX_STATE_LOCK,
            ORDER_STATE_LOCK=ORDER_STATE_LOCK,
            TZ=TZ,
            atomic_write_json=atomic_write_json,
            calculate_entry_range_bounds=calculate_entry_range_bounds,
            calculate_percent=calculate_percent,
            ceil_price_to_tick_value=ceil_price_to_tick_value,
            close_flat_position=close_flat_position,
            fetch_fall_quote=fetch_fall_quote,
            finite_positive=finite_positive,
            format_market_gate_percent=format_market_gate_percent,
            format_market_gate_value=format_market_gate_value,
            get_critical_market_gate_snapshots=get_critical_market_gate_snapshots,
            get_entry_check_end_time=get_entry_check_end_time,
            get_entry_check_start_time=get_entry_check_start_time,
            get_entry_mode_text=get_entry_mode_text,
            get_fresh_market_index_value=get_fresh_market_index_value,
            get_fresh_market_index_value_from_snapshot=get_fresh_market_index_value_from_snapshot,
            get_market_gate_snapshots=get_market_gate_snapshots,
            get_market_index_snapshot=get_market_index_snapshot,
            get_market_index_snapshot_freshness=get_market_index_snapshot_freshness,
            get_market_key_for_symbol=get_market_key_for_symbol,
            get_pure_symbol=get_pure_symbol,
            get_stock_rest_client=get_stock_rest_client,
            is_avg_strategy=is_avg_strategy,
            is_price_in_entry_range=is_price_in_entry_range,
            market_gate_snapshots_are_fresh=market_gate_snapshots_are_fresh,
            market_previous_close_indices=market_previous_close_indices,
            now_tpe=now_tpe,
            state_path=state_path,
            state_symbol_fields=state_symbol_fields,
            time_tuple_to_minutes=time_tuple_to_minutes,
            total_stocks=total_stocks,
            trade_log=trade_log,
        ),
    )

# ============ 下單函式 ============
def start_serialized_trade_call(operation: str, callback, before_start=None) -> Future:
    """只允許一個 native 呼叫在途；逾時不可釋放其鎖或另開 SDK 補單。"""
    if not TRADE_API_LOCK.acquire(blocking=False):
        raise TradeApiBusy("交易 SDK 忙碌，尚未執行此操作")
    try:
        if before_start is not None:
            before_start()
    except Exception:
        TRADE_API_LOCK.release()
        raise  # 尚未啟動 native 呼叫；禁止在意圖記錄失敗時送單。
    future = Future()
    with TRADE_CALL_STATE_LOCK:
        TRADE_CALL_STATE.update(operation=operation, started=time.monotonic(), future=future, alerted=False)

    def worker():
        try:
            result = callback()
            # 送單、撤單共用節流，保持既有保守頻率。
            if operation != "get_order_results":
                time.sleep(0.1)
        except Exception as exc:
            TRADE_API_LOCK.release()
            future.set_exception(exc)
        else:
            TRADE_API_LOCK.release()
            future.set_result(result)

    try:
        threading.Thread(target=worker, name=f"trade-{operation}", daemon=True).start()
    except Exception:
        TRADE_API_LOCK.release()
        future.set_exception(RuntimeError("Unable to start SDK worker"))
        raise
    return future


def report_unsettled_trades(states: Dict[str, Dict[str, Any]], context: str) -> bool:
    states = all_order_states(states)
    with ORDER_STATE_LOCK:
        unsettled = [
            {key: state.get(key) for key in (
                "symbol_code_with_suf", "in_position", "qty", "entry_order_pending",
                "entry_order_no", "entry_request_id", "exit_order_pending", "exit_order_no",
                "exit_request_id", "exit_retry_reason", "entry_cancel_requested",
            )}
            for state in states.values()
            if state.get("in_position") or state.get("entry_order_pending") or state.get("exit_order_pending")
        ]
    attention = bool(unsettled) or TRADE_API_DEGRADED_EVENT.is_set()
    atomic_write_json(os.path.join(BASE_DIR, "trade_attention.json"), {
        "time": now_tpe().isoformat(), "context": context,
        "manual_attention_required": attention,
        "sdk_degraded": TRADE_API_DEGRADED_EVENT.is_set(), "unsettled": unsettled,
    })
    trade_log("MANUAL_ATTENTION_REQUIRED" if attention else "TRADE_STATUS_CLEAR",
              error=attention, context=context, unsettled=unsettled,
              sdk_degraded=TRADE_API_DEGRADED_EVENT.is_set())
    return attention


def check_trade_api_watchdog(states: Dict[str, Dict[str, Any]]) -> None:
    with TRADE_CALL_STATE_LOCK:
        call = TRADE_CALL_STATE
        if (not call or call["future"].done() or call["alerted"]
                or time.monotonic() - call["started"] < ORDER_UNKNOWN_ALERT_SECONDS):
            return
        call["alerted"] = True
        operation = call["operation"]
    TRADE_API_DEGRADED_EVENT.set()
    trade_log("TRADE_API_STALLED", error=True, operation=operation,
              action="no_new_entries_keep_native_lock_manual_broker_check_required")
    report_unsettled_trades(states, "trade_api_stalled")


def consume_late_order_responses(states: Dict[str, Dict[str, Any]]) -> None:
    for request_id, future in list(ORDER_SUBMISSION_FUTURES.items()):
        if not future.done():
            continue
        ORDER_SUBMISSION_FUTURES.pop(request_id, None)
        try:
            response = future.result()
        except Exception:
            continue  # 仍保留 unknown，由委託明細對帳，不直接補單。
        if not isinstance(response, dict):
            continue
        order_no = str(response.get("ord_no", "") or "").strip()
        ret_code = str(response.get("ret_code", "") or "").strip()
        rejected = bool(ret_code) and ret_code != "000000" and not order_no
        if not rejected and (ret_code != "000000" or not order_no):
            continue
        for state in states.values():
            for role in ("entry", "exit"):
                with ORDER_STATE_LOCK:
                    if state.get(f"{role}_request_id") != request_id or state.get(f"{role}_order_no"):
                        continue
                    if rejected:
                        if state.get(f"{role}_fill_confirmed"):
                            continue
                        state[f"{role}_order_pending"] = False
                        state[f"{role}_terminal_reason"] = "rejected"
                        if role == "entry":
                            state["traded"] = True
                            state["exit_reason"] = "entry_order_terminated_unfilled"
                        else:
                            state["exit_retry_reason"] = state.get("exit_reason_pending") or "stop"
                    else:
                        state[f"{role}_order_no"] = order_no
                    state[f"{role}_outcome_unknown"] = False
                apply_pending_dealt_reports(state)
                atomic_write_json(state_path(state["symbol_code_with_suf"]), dict(state))
                trade_log("LATE_ORDER_RESPONSE_RESOLVED", request_id=request_id, ord_no=order_no,
                          role=role, rejected=rejected)


# symbol: '2330' '0050'
# action_type: Action.Buy or Action.Sell
# trade_type: Trade.Cash or Trade.DayTradingSell
# price_flag: PriceFlag.Market or PriceFlag.LimitDown or PriceFlag.LimitUp or PriceFlag.Limit
def type_place_order(mysdk, symbol_code_with_suf, action_type, trade_type, quantity=1, price_flag=PriceFlag.Market, price=0.0) -> Optional[str]:
    if type(quantity) is not int or quantity != 1:
        raise ValueError("實機策略每檔委託只允許一張")
    request_id = "ORB" + uuid.uuid4().hex
    priceInfo = price

    if price_flag == PriceFlag.Market:  # 市價不需填價格
        price = ''

    if price_flag in (PriceFlag.LimitUp, PriceFlag.LimitDown):  # 漲停、跌停填None
        price = None

    if price_flag == PriceFlag.Limit:  # 限價預約平倉
        priceInfo = price

    orderCode = symbol_code_with_suf.split(".")[0]

    order = OrderObject(
        buy_sell=action_type,
        price_flag=price_flag,
        price=price,
        stock_no=orderCode,
        quantity=quantity,
        user_def=request_id,
        ap_code=APCode.Common,
        trade=trade_type
    )

    # Busy 表示尚未送出；與已開始執行後的例外／逾時必須區分。
    def record_intent():
        atomic_write_json(os.path.join(STATE_DIR, f"{request_id}.OrderIntent.json"), {
            "time": now_tpe().isoformat(), "request_id": request_id,
            "symbol": symbol_code_with_suf, "action": action_type,
            "trade": trade_type, "qty": quantity, "status": "prepared_outcome_requires_reconciliation",
        }, strict=True)
        trade_log("ORDER_INTENT", request_id=request_id, symbol=symbol_code_with_suf,
                  action=action_type, qty=quantity)

    future = start_serialized_trade_call("place_order", lambda: mysdk.place_order(order), record_intent)
    try:
        order_response = future.result(timeout=ORDER_SUBMIT_WAIT_SECONDS)
    except FutureTimeoutError as e:
        ORDER_SUBMISSION_FUTURES[request_id] = future
        TRADE_API_DEGRADED_EVENT.set()
        trade_log("ORDER_SUBMISSION_TIMEOUT", error=True, request_id=request_id,
                  symbol=symbol_code_with_suf, action="keep_pending_no_resubmit_manual_check_required")
        raise OrderOutcomeUnknown(request_id) from e
    except Exception as e:
        trade_log(
            "ORDER_OUTCOME_UNKNOWN",
            error=True,
            symbol=symbol_code_with_suf,
            action=action_type,
            trade=trade_type,
            price_flag=price_flag,
            price=priceInfo,
            qty=quantity,
            reason="place_order_exception",
            request_id=request_id,
            error_msg=repr(e),
        )
        raise OrderOutcomeUnknown(request_id) from e

    if not isinstance(order_response, dict):
        trade_log(
            "ORDER_OUTCOME_UNKNOWN",
            error=True,
            symbol=symbol_code_with_suf,
            action=action_type,
            trade=trade_type,
            price_flag=price_flag,
            price=priceInfo,
            qty=quantity,
            reason="invalid_response_type",
            request_id=request_id,
            response_type=type(order_response).__name__,
            response=repr(order_response),
        )
        raise OrderOutcomeUnknown(request_id)

    ret_code = str(order_response.get("ret_code", "") or "").strip()
    ret_msg = str(order_response.get("ret_msg", "") or "").strip()
    order_no = str(order_response.get("ord_no", "") or "").strip()
    if ret_code != "000000" or not order_no:
        # 僅「明確錯誤碼且無書號」視為拒單；成功但缺書號等格式異常須對帳。
        rejected = bool(ret_code) and ret_code != "000000" and not order_no
        trade_log(
            "ORDER_REJECTED" if rejected else "ORDER_OUTCOME_UNKNOWN",
            error=True,
            symbol=symbol_code_with_suf,
            action=action_type,
            trade=trade_type,
            price_flag=price_flag,
            price=priceInfo,
            qty=quantity,
            ret_code=ret_code,
            ret_msg=ret_msg,
            ord_no=order_no,
            request_id=request_id,
            response=repr(order_response),
        )
        if rejected:
            return None
        raise OrderOutcomeUnknown(request_id)

    trade_log(
        "ORDER_ACCEPTED",
        symbol=symbol_code_with_suf,
        action=action_type,
        trade=trade_type,
        price_flag=price_flag,
        price=priceInfo,
        qty=quantity,
        ord_no=order_no,
        request_id=request_id,
        ret_code=ret_code,
        ret_msg=ret_msg,
    )

    return order_no


def _find_state_by_order_no(order_no: str) -> tuple[Optional[Dict[str, Any]], str]:
    for state in ACTIVE_ORDER_STATES.values():
        if str(state.get("entry_order_no", "") or "").strip() == order_no:
            return state, "entry"
        if str(state.get("exit_order_no", "") or "").strip() == order_no:
            return state, "exit"
    return None, ""


def _apply_entry_dealt_report_to_state(state: Dict[str, Any], data: Dict[str, Any]) -> bool:
    """
    以主動成交回報校正入場價量；若已由另一來源接管則不覆寫。
    已知限制：目前實際只使用一張股票完整進出，部分成交或殘量拆單處理刻意延後。
    """
    try:
        mat_price = float(data.get("mat_price", 0) or 0)
        # DEALT_REPORT 實機回報 mat_qty 為股數；例如 mat_price=67.5, mat_qty=1000, pay_price=67500。
        mat_shares = int(data.get("mat_qty", 0) or 0)
    except (TypeError, ValueError):
        return False
    if mat_price <= 0 or mat_shares <= 0:
        return False

    with ORDER_STATE_LOCK:
        source = str(state.get("entry_price_source", "estimated") or "estimated")
        if source == "order_results" or state.get("entry_fully_filled"):
            return False
        try:
            entry_order_qty = int(state.get("entry_order_qty", state.get("qty", 0)) or 0)
        except (TypeError, ValueError):
            return False
        if entry_order_qty <= 0:
            return False

        accumulated_shares = int(state.get("dealt_report_filled_shares", 0) or 0) + mat_shares
        accumulated_value = float(state.get("dealt_report_filled_value", 0.0) or 0.0) + mat_price * mat_shares
        expected_shares = entry_order_qty * 1000
        filled_lots = min(accumulated_shares // 1000, entry_order_qty)

        state["dealt_report_filled_shares"] = accumulated_shares
        state["dealt_report_filled_value"] = accumulated_value
        state["entry_price"] = accumulated_value / accumulated_shares
        state["entry_filled_qty"] = filled_lots
        state["entry_fill_confirmed"] = True
        state["entry_fully_filled"] = accumulated_shares >= expected_shares
        state["entry_order_pending"] = not state["entry_fully_filled"]
        state["in_position"] = True
        state["entry_price_source"] = "dealt_report"
        state["entry_fill_update_time"] = now_tpe().isoformat()
        state["qty"] = max(filled_lots, 1)
        recalc_entry_position_prices(state)
        entry_price = state["entry_price"]
        entry_order_no = state.get("entry_order_no")
        entry_fully_filled = bool(state.get("entry_fully_filled"))
        state_snapshot = dict(state)
    atomic_write_json(state_path(state_snapshot.get("symbol_code_with_suf", "")), state_snapshot)

    fill_text = "完整成交" if entry_fully_filled else "部分成交"
    trade_log(
        "FILL_UPDATE",
        **state_symbol_fields(state),
        role="entry",
        source="DEALT_REPORT",
        fill_status=fill_text,
        ord_no=entry_order_no,
        avg_price=f"{entry_price:.4f}",
        filled_shares=accumulated_shares,
        expected_shares=expected_shares,
        filled_qty=filled_lots,
        expected_qty=entry_order_qty,
    )
    print_entry_position_prices(state_snapshot)
    return True


def _apply_exit_dealt_report_to_state(state: Dict[str, Any], data: Dict[str, Any]) -> bool:
    """
    以主動成交回報確認平倉；完整成交後才視為交易完成。
    已知限制：目前實際只使用一張股票完整進出，部分成交或殘量拆單處理刻意延後。
    """
    try:
        mat_price = float(data.get("mat_price", 0) or 0)
        # DEALT_REPORT 實機回報 mat_qty 為股數；需換算為張數後才和 exit_order_qty 比對。
        mat_shares = int(data.get("mat_qty", 0) or 0)
    except (TypeError, ValueError):
        return False
    if mat_price <= 0 or mat_shares <= 0:
        return False

    with ORDER_STATE_LOCK:
        if state.get("exit_fully_filled") or state.get("traded"):
            return False
        try:
            exit_order_qty = int(state.get("exit_order_qty", state.get("qty", 0)) or 0)
        except (TypeError, ValueError):
            return False
        if exit_order_qty <= 0:
            return False

        accumulated_shares = int(state.get("exit_dealt_report_filled_shares", 0) or 0) + mat_shares
        accumulated_value = float(state.get("exit_dealt_report_filled_value", 0.0) or 0.0) + mat_price * mat_shares
        expected_shares = exit_order_qty * 1000
        filled_lots = min(accumulated_shares // 1000, exit_order_qty)

        state["exit_dealt_report_filled_shares"] = accumulated_shares
        state["exit_dealt_report_filled_value"] = accumulated_value
        state["exit_price"] = accumulated_value / accumulated_shares
        state["exit_filled_qty"] = filled_lots
        state["exit_fill_confirmed"] = True
        state["exit_fully_filled"] = accumulated_shares >= expected_shares
        state["exit_price_source"] = "dealt_report"
        state["exit_fill_update_time"] = now_tpe().isoformat()

        if state["exit_fully_filled"]:
            state["exit_order_pending"] = False
            state["traded"] = True
            state["in_position"] = False
            if state.get("exit_reason_pending"):
                state["exit_reason"] = state.get("exit_reason_pending")
        exit_price = state["exit_price"]
        exit_order_no = state.get("exit_order_no")
        exit_reason = state.get("exit_reason_pending") or state.get("exit_reason")
        exit_fully_filled = bool(state.get("exit_fully_filled"))
        state_snapshot = dict(state)

    atomic_write_json(state_path(state_snapshot.get("symbol_code_with_suf", "")), state_snapshot)

    fill_text = "完整成交" if exit_fully_filled else "部分成交"
    trade_log(
        "FILL_UPDATE",
        **state_symbol_fields(state),
        role="exit",
        source="DEALT_REPORT",
        fill_status=fill_text,
        ord_no=exit_order_no,
        exit_reason=exit_reason,
        avg_price=f"{exit_price:.4f}",
        filled_shares=accumulated_shares,
        expected_shares=expected_shares,
        filled_qty=filled_lots,
        expected_qty=exit_order_qty,
    )
    return True


def _apply_dealt_report_to_state(state: Dict[str, Any], data: Dict[str, Any], order_role: str) -> bool:
    if order_role == "entry":
        return _apply_entry_dealt_report_to_state(state, data)
    if order_role == "exit":
        return _apply_exit_dealt_report_to_state(state, data)
    return False


def apply_pending_dealt_reports(state: Dict[str, Any]) -> None:
    order_pairs = [
        ("entry", str(state.get("entry_order_no", "") or "").strip()),
        ("exit", str(state.get("exit_order_no", "") or "").strip()),
    ]
    with DEALT_REPORT_LOCK:
        for order_role, order_no in order_pairs:
            if not order_no:
                continue
            reports = PENDING_DEALT_REPORTS.pop(order_no, [])
            for report in reports:
                _apply_dealt_report_to_state(state, report, order_role)


def start_trade_report_stream(sdk: SDK) -> threading.Thread:
    """註冊交易主動回報，並在背景執行阻塞式 connect_websocket()。"""

    @sdk.on("error")
    def on_trade_error(error):
        trade_log("TRADE_WS_ERROR", error=True, error_msg=repr(error))

    @sdk.on("dealt")
    def on_dealt(data):
        trade_log("DEALT_REPORT_RECEIVED", raw=repr(data))
        if not isinstance(data, dict):
            trade_log(
                "DEALT_REPORT_IGNORED",
                error=True,
                reason="invalid_payload_type",
                payload_type=type(data).__name__,
                raw=repr(data),
            )
            return
        order_no = str(data.get("ord_no", "") or "").strip()
        if not order_no:
            trade_log("DEALT_REPORT_IGNORED", error=True, reason="missing_ord_no", raw=repr(data))
            return
        report_identity = str(data.get("mkt_seq_num", "") or "").strip()
        if not report_identity:
            report_identity = "|".join(
                str(data.get(key, "") or "").strip()
                for key in ("mat_time", "mat_price", "mat_qty")
            )
        report_key = (order_no, report_identity)
        with DEALT_REPORT_LOCK:
            if report_key in SEEN_DEALT_REPORT_KEYS:
                trade_log("DEALT_REPORT_IGNORED", reason="duplicate", ord_no=order_no, identity=report_identity)
                return
            SEEN_DEALT_REPORT_KEYS.add(report_key)
            state, order_role = _find_state_by_order_no(order_no)
            if state is None:
                PENDING_DEALT_REPORTS.setdefault(order_no, []).append(dict(data))
                trade_log("DEALT_REPORT_PENDING", ord_no=order_no, identity=report_identity, raw=repr(data))
                return
            _apply_dealt_report_to_state(state, data, order_role)

    def websocket_worker():
        try:
            trade_log("TRADE_WS_CONNECTING")
            sdk.connect_websocket()
            trade_log("TRADE_WS_STOPPED", error=True, reason="connect_websocket_returned")
        except Exception as exc:
            trade_log("TRADE_WS_ERROR", error=True, reason="connect_websocket_exception", error_msg=repr(exc))

    websocket_thread = threading.Thread(
        target=websocket_worker,
        name="esun-trade-report-websocket",
        daemon=True,
    )
    websocket_thread.start()
    return websocket_thread


# ============ 工具函式 ============
def get_tick_size(price: float) -> float:
    """依台股價格區間回傳 tick size"""
    if price <= 10:
        return 0.01
    elif price <= 50:
        return 0.05
    elif price <= 100:
        return 0.1
    elif price <= 500:
        return 0.5
    elif price <= 1000:
        return 1.0
    else:
        return 5.0


def get_up_down_price(stock_id: str, realtime_sdk):
    code_num = stock_id.split(".")[0]
    stock = get_stock_rest_client(realtime_sdk)
    stock_intra_ticker = stock.intraday.ticker(symbol=code_num)
    time.sleep(0.2)  # 避免短時間過量 request
    limit_up_price = round(stock_intra_ticker.get('limitUpPrice', 0), 2)
    limit_down_price = round(stock_intra_ticker.get('limitDownPrice', 0), 2)

    symbol_can_day_trade = stock_intra_ticker.get('canDayTrade', False)
    symbol_is_disposition = stock_intra_ticker.get('isDisposition')

    return limit_up_price, limit_down_price, symbol_can_day_trade, symbol_is_disposition


def now_tpe() -> datetime:
    return datetime.now(TZ)


def wait_until_main_start_time() -> None:
    main_start_hm = MAIN_START_TIME[0] * 60 + MAIN_START_TIME[1]
    if not (0 <= main_start_hm <= 23 * 60 + 59):
        raise ValueError("MAIN_START_TIME 設定錯誤，需介於 00:00~23:59")

    now_local = now_tpe()
    target_time = now_local.replace(
        hour=MAIN_START_TIME[0],
        minute=MAIN_START_TIME[1],
        second=0,
        microsecond=0,
    )
    if now_local > target_time:
        trade_log(
            "STARTUP_ABORTED",
            error=True,
            reason="started_after_main_start_time",
            main_start_time=f"{MAIN_START_TIME[0]:02d}:{MAIN_START_TIME[1]:02d}:00",
            current_time=now_local.strftime("%H:%M:%S"),
        )
        sys.exit(0)

    if now_local < target_time:
        print(
            f"⏳ 主程序預定 {MAIN_START_TIME[0]:02d}:{MAIN_START_TIME[1]:02d} 開始，"
            f"目前時間：{now_local.strftime('%H:%M:%S')}，等待中"
        )
        while True:
            remaining_seconds = (target_time - now_tpe()).total_seconds()
            if remaining_seconds <= 0:
                break
            time.sleep(min(remaining_seconds, 30.0))

    print(f"⏰ 主程序開始執行！目前時間：{now_tpe().strftime('%H:%M:%S')}")


def time_tuple_to_minutes(value: tuple[int, int], name: str) -> int:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(type(part) is not int for part in value)
        or not (0 <= value[0] <= 23 and 0 <= value[1] <= 59)
    ):
        raise ValueError(f"{name} 必須是 (時, 分) tuple")
    minutes = value[0] * 60 + value[1]
    if not (0 <= minutes <= 23 * 60 + 59):
        raise ValueError(f"{name} 設定錯誤，需介於 00:00~23:59")
    return minutes


def get_lower_strategy_windows() -> list[tuple[tuple[int, int], tuple[int, int], tuple[int, int]]]:
    return get_lower_strategy().get_lower_strategy_windows()


def get_active_lower_window() -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
    return get_lower_strategy().get_active_lower_window()


def activate_lower_session(window_number: int, decision_dt: datetime, summary: Dict[str, Any]) -> None:
    return get_lower_strategy().activate_lower_session(window_number, decision_dt, summary)


def validate_market_reversal_time_config() -> None:
    return get_lower_strategy().validate_market_reversal_time_config()


def get_entry_mode_text(entry_mode: int | None = None) -> str:
    mode = get_current_entry_mode() if entry_mode is None else entry_mode
    if mode == ENTRY_MODE_NO_TRADE:
        return "NO_TRADE"
    if mode == ENTRY_MODE_LOWER:
        return "LOWER"
    if mode == ENTRY_MODE_AVG:
        return "AVG"
    return "UNKNOWN"


def get_avg_candidate_stocks() -> list:
    return get_avg_strategy().get_avg_candidate_stocks()


def validate_avg_config() -> None:
    return get_avg_strategy().validate_avg_config()


def resolve_session_entry_mode(now_local: datetime) -> Optional[int]:
    """LOWER 一旦啟動即固定；否則第三組開始時才最後分流。"""
    if LOWER_RUNTIME.session_state.get("activation_dt") is not None:
        return ENTRY_MODE_LOWER
    if (now_local.hour, now_local.minute) < STRATEGY_START_LOWER_3:
        return None
    if MARKET_INDEX_REST_FATAL_EVENT.is_set():
        return ENTRY_MODE_NO_TRADE
    return ENTRY_MODE_AVG


def is_avg_strategy(state: Dict[str, Any] | None) -> bool:
    return get_avg_strategy().is_avg_strategy(state)


def parse_avg_candles(raw_candles: Any, now_local: datetime) -> list[Dict[str, Any]]:
    return get_avg_strategy().parse_avg_candles(raw_candles, now_local)


def calculate_avg_normalized_slope(bars: list[Dict[str, Any]], previous_close: float) -> Optional[float]:
    return get_avg_strategy().calculate_avg_normalized_slope(bars, previous_close)


def find_avg_entry_signal(bars: list[Dict[str, Any]], now_local: datetime) -> Optional[Dict[str, Any]]:
    return get_avg_strategy().find_avg_entry_signal(bars, now_local)


def mark_avg_state_traded(state: Dict[str, Any], reason: str, *, error: bool=False, **fields: Any) -> None:
    return get_avg_strategy().mark_avg_state_traded(state, reason, error=error, **fields)


def fetch_avg_candle_response(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> Dict[str, Any]:
    return get_avg_strategy().fetch_avg_candle_response(state, realtime_sdk)


def process_avg_candle_state(state: Dict[str, Any], response: Dict[str, Any], mysdk: SDK, now_local: datetime) -> None:
    return get_avg_strategy().process_avg_candle_state(state, response, mysdk, now_local)


def process_avg_candle_round(states: Dict[str, Dict[str, Any]], realtime_sdk: EsunMarketdata, mysdk: SDK) -> None:
    return get_avg_strategy().process_avg_candle_round(states, realtime_sdk, mysdk)


def prepare_avg_states(realtime_sdk):
    return get_avg_strategy().prepare_avg_states(realtime_sdk)


def print_entry_position_prices(state: Dict[str, Any]) -> None:
    try:
        entry_price = float(state.get("entry_price", 0))
        flat_price = float(state.get("flat_price", 0))
        profit_price = float(state.get("profit_price", 0))
    except (TypeError, ValueError):
        return

    if is_protect_profit_enabled(state):
        _protect_loss_per, protect_profit_per = get_protect_loss_profit_percent(state)
        if state.get("side") == TRADE_SIDE_LONG:
            protect_profit_text = f"{entry_price * (1 + protect_profit_per / 100.0):.2f}"
        else:
            protect_profit_text = f"{entry_price * (1 - protect_profit_per / 100.0):.2f}"
    else:
        protect_profit_text = "停用"
    print(f"停損：{flat_price:.2f}，保本：{protect_profit_text}，停利：{profit_price:.2f}")


def recalc_entry_position_prices(state: Dict[str, Any]) -> bool:
    side = state.get("side")
    if side not in (TRADE_SIDE_SHORT, TRADE_SIDE_LONG):
        return False

    try:
        entry_price = float(state.get("entry_price", 0) or 0)
    except (TypeError, ValueError):
        return False

    if entry_price <= 0:
        return False

    optimize_loss_per, optimize_profit_per = get_optimize_loss_profit_percent(state)
    open_stop_loss = entry_price * (optimize_loss_per / 100.0)
    open_profit_target = entry_price * (optimize_profit_per / 100.0)

    if side == TRADE_SIDE_SHORT:
        if is_avg_strategy(state) or is_fall_strategy(state):
            state["profit_price"] = max(
                floor_price_to_tick_value(entry_price - open_profit_target),
                state.get("limit_down_price", 0),
            )
            state["flat_price"] = min(
                ceil_price_to_tick_value(entry_price + open_stop_loss),
                state.get("limit_up_price", 0),
            )
        else:
            state["profit_price"] = max(entry_price - open_profit_target, state.get("limit_down_price", 0))
            state["flat_price"] = min(entry_price + open_stop_loss, state.get("limit_up_price", 0))
    else:
        state["profit_price"] = min(
            entry_price + open_profit_target,
            state.get("limit_up_price", 0)
        )
        state["flat_price"] = max(
            entry_price - open_stop_loss,
            state.get("limit_down_price", 0)
        )

    return True


def get_realtime_price(stock_id: str, realtime_sdk, *, priority=MARKETDATA_PRIORITY_CANDIDATE):
    # LOWER/FALL 同輪共用一筆 quote；AVG 持倉亦沿用同一入口。
    round_key = int((time.time() - 5) // QUOTE_ROUND_INTERVAL_SECONDS)
    with QUOTE_CACHE_LOCK:
        cached = QUOTE_ROUND_CACHE.get(stock_id)
        if cached is not None and cached[0] == round_key:
            future, owner = cached[1], False
        else:
            future, owner = Future(), True
            QUOTE_ROUND_CACHE[stock_id] = (round_key, future)
    if owner:
        try:
            with marketdata_priority(priority):
                future.set_result(fetch_realtime_price_uncached(stock_id, realtime_sdk))
        except Exception as exc:
            future.set_exception(exc)
            with QUOTE_CACHE_LOCK:
                if QUOTE_ROUND_CACHE.get(stock_id) == (round_key, future):
                    QUOTE_ROUND_CACHE.pop(stock_id, None)
    return future.result()


def fetch_realtime_price_uncached(stock_id: str, realtime_sdk):
    code_num = stock_id.split(".")[0]
    stock = get_stock_rest_client(realtime_sdk)

    stock_intraday_quote = stock.intraday.quote(symbol=code_num)
    last_price = stock_intraday_quote['lastPrice']
    open_price = stock_intraday_quote['openPrice']
    high_price = stock_intraday_quote['highPrice']
    low_price = stock_intraday_quote['lowPrice']
    close_price = stock_intraday_quote['closePrice']
    avg_price = stock_intraday_quote['avgPrice']
    bids = stock_intraday_quote.get('bids') or []
    asks = stock_intraday_quote.get('asks') or []
    best_bid_price = bids[0].get('price') if bids else None
    best_ask_price = asks[0].get('price') if asks else None

    return last_price, open_price, high_price, low_price, close_price, avg_price, best_bid_price, best_ask_price


def schedule_avg_position_quotes(states: Dict[str, Dict[str, Any]], realtime_sdk, *, request_quotes: bool) -> None:
    return get_avg_strategy().schedule_avg_position_quotes(states, realtime_sdk, request_quotes=request_quotes)


def state_needs_entry_fill_update(state: Dict[str, Any]) -> bool:
    has_pending_entry = bool(state.get("entry_order_pending"))
    legacy_unconfirmed_entry = (
        bool(state.get("in_position"))
        and bool(state.get("entry_order_no"))
        and not state.get("entry_fill_confirmed")
    )
    if (not has_pending_entry and not legacy_unconfirmed_entry) or state.get("traded"):
        return False
    # DEALT_REPORT 已接管此筆委託後，由後續主動回報累計部分成交；
    # get_order_results 不再覆寫，也不再為這筆委託持續輪詢。
    if state.get("entry_price_source") == "dealt_report":
        return False
    try:
        entry_order_qty = int(state.get("entry_order_qty", state.get("qty", 0)) or 0)
        entry_filled_qty = int(state.get("entry_filled_qty", 0) or 0)
    except (TypeError, ValueError):
        return False
    return entry_order_qty > 0 and entry_filled_qty < entry_order_qty


def state_needs_exit_fill_update(state: Dict[str, Any]) -> bool:
    if not state.get("exit_order_pending") or state.get("traded"):
        return False
    if state.get("exit_price_source") == "dealt_report":
        return False
    try:
        exit_order_qty = int(state.get("exit_order_qty", state.get("qty", 0)) or 0)
        exit_filled_qty = int(state.get("exit_filled_qty", 0) or 0)
    except (TypeError, ValueError):
        return False
    return exit_order_qty > 0 and exit_filled_qty < exit_order_qty


def get_order_fill_info_by_results(order_no: str, order_results: List[Dict[str, Any]]) -> tuple[float, int]:
    order_no = str(order_no or "").strip()
    if not order_no:
        return 0.0, 0

    matched_results = []
    for item in order_results:
        item_order_no = str(item.get("ord_no", "") or "").strip()
        if item_order_no == order_no:
            matched_results.append(item)

    filled_results = []
    for item in matched_results:
        try:
            # get_order_results 的 mat_qty 為張數，與 DEALT_REPORT 的股數語意不同。
            mat_qty = int(item.get("mat_qty", 0) or 0)
        except (TypeError, ValueError):
            continue
        if mat_qty > 0:
            filled_results.append(item)

    if not filled_results:
        return 0.0, 0

    total_qty = 0
    total_value = 0.0
    for item in filled_results:
        try:
            # get_order_results 的 mat_qty 為張數，與 DEALT_REPORT 的股數語意不同。
            mat_qty = int(item.get("mat_qty", 0) or 0)
            avg_price = float(item.get("avg_price", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if mat_qty <= 0 or avg_price <= 0:
            continue
        total_qty += mat_qty
        total_value += avg_price * mat_qty

    avg_price = total_value / total_qty if total_qty > 0 else 0.0
    return avg_price, total_qty


def resolve_unknown_order(state: Dict[str, Any], role: str, results: list) -> None:
    request_id = state.get(f"{role}_request_id")
    if not request_id or state.get(f"{role}_order_no"):
        return
    matches = [row for row in results if isinstance(row, dict) and row.get("user_def") == request_id]
    expected_action = Action.Sell if (state.get("side") == TRADE_SIDE_SHORT) == (role == "entry") else Action.Buy
    expected_trade = Trade.DayTradingSell if expected_action == Action.Sell else Trade.Cash
    valid = [row for row in matches if (
        str(row.get("stock_no", "")) == state.get("symbol_code")
        and str(row.get("buy_sell", "")) == expected_action.value
        and str(row.get("trade", "")) == expected_trade.value
        and str(row.get("ap_code", "")) == APCode.Common.value
        and row.get("ord_no")
        and str(row.get("org_qty", "")) in ("1", "1.0")
    )]
    if len(matches) == len(valid) == 1:
        with ORDER_STATE_LOCK:
            state[f"{role}_order_no"] = str(valid[0]["ord_no"]).strip()
            state[f"{role}_outcome_unknown"] = False
        apply_pending_dealt_reports(state)
        trade_log("ORDER_UNKNOWN_RESOLVED", **state_symbol_fields(state), role=role,
                  request_id=request_id, ord_no=state[f"{role}_order_no"])
        atomic_write_json(state_path(state["symbol_code_with_suf"]), dict(state))
        return
    # 查不到不能證明原單不存在；不以逾時或空查詢結果解除 pending。
    started = state.get(f"{role}_unknown_since_monotonic", time.monotonic())
    if time.monotonic() - started >= ORDER_UNKNOWN_ALERT_SECONDS and not state.get(f"{role}_unknown_alerted"):
        state[f"{role}_unknown_alerted"] = True
        trade_log("ORDER_RECONCILE_REQUIRED", error=True, **state_symbol_fields(state),
                  role=role, request_id=request_id, matches=len(matches),
                  action="manual_check_required_keep_pending_no_resubmit")


def reconcile_terminal_order(state: Dict[str, Any], role: str, results: list) -> None:
    """一張委託：僅在確認零成交且全取消／拒單時解除 pending。"""
    order_no = str(state.get(f"{role}_order_no", "") or "")
    if not state.get(f"{role}_order_pending") or not order_no:
        return
    matches = [row for row in results if isinstance(row, dict) and str(row.get("ord_no", "")) == order_no]
    if len(matches) != 1:
        return
    row = matches[0]
    try:
        original = float(row["org_qty"])
        filled = float(row.get("mat_qty", 0))
        filled_shares = float(row.get("mat_qty_share", filled * 1000))
        cancelled = float(row.get("cel_qty", 0))
    except (KeyError, TypeError, ValueError, OverflowError):
        return
    if original != 1 or filled != 0 or filled_shares != 0 or cancelled not in (0, 1):
        return
    err_code = str(row.get("err_code", "") or "").strip()
    rejected = bool(err_code.strip("0")) and str(row.get("celable", "")) == "2"
    if cancelled < original and not rejected:
        return
    with ORDER_STATE_LOCK:
        # 成交回報可能在查詢之後抵達；不可覆寫已確認的成交。
        if state.get(f"{role}_fill_confirmed") or not state.get(f"{role}_order_pending"):
            return
        state[f"{role}_order_pending"] = False
        state[f"{role}_outcome_unknown"] = False
        state[f"{role}_terminal_reason"] = "rejected" if rejected else "cancelled"
        if role == "entry":
            state["traded"] = True
            state["exit_reason"] = "entry_order_terminated_unfilled"
        else:
            # 保留平倉意圖；即使價格已離開觸發點，下輪仍須重試平倉。
            state["exit_retry_reason"] = state.get("exit_reason_pending") or "stop"
    atomic_write_json(state_path(state["symbol_code_with_suf"]), dict(state))
    trade_log("ORDER_TERMINATED_UNFILLED", **state_symbol_fields(state), role=role,
              ord_no=order_no, err_code=err_code, cancelled=cancelled,
              action="block_entry_today" if role == "entry" else "retry_exit_next_loop")


def update_entry_fills_from_order_results(states: Dict[str, Dict[str, Any]], sdk, *, results=None, query_error=None) -> None:
    """
    用委託紀錄補成交價量。
    已知限制：目前實際只使用一張股票完整進出，部分成交或殘量拆單處理刻意延後。
    """
    global ORDER_RESULTS_COOLDOWN_UNTIL_MONOTONIC
    candidates = [st for st in states.values() if state_needs_entry_fill_update(st)]
    exit_candidates = [st for st in states.values() if state_needs_exit_fill_update(st)]
    if not candidates and not exit_candidates:
        return

    try:
        if query_error is not None:
            raise query_error
        # 此處只消費背景查詢結果；不得意外退回主執行緒同步查詢。
        order_results = results
    except Exception as exc:
        trade_log(
            "ORDER_RESULTS_ERROR",
            error=True,
            reason="get_order_results_exception",
            error_msg=repr(exc),
        )
        if "AGR0005" in str(exc):
            ORDER_RESULTS_COOLDOWN_UNTIL_MONOTONIC = (
                time.monotonic() + ORDER_RESULTS_RATE_LIMIT_COOLDOWN_SECONDS
            )
            trade_log(
                "ORDER_RESULTS_RATE_LIMIT",
                error=True,
                cooldown_seconds=f"{ORDER_RESULTS_RATE_LIMIT_COOLDOWN_SECONDS:.0f}",
            )
        return

    if not isinstance(order_results, list) or any(not isinstance(row, dict) for row in order_results):
        trade_log(
            "ORDER_RESULTS_ERROR",
            error=True,
            reason="invalid_response_type",
            response_type=type(order_results).__name__,
            response=repr(order_results),
        )
        return

    for state in candidates:
        resolve_unknown_order(state, "entry", order_results)
        avg_price, mat_qty = get_order_fill_info_by_results(state.get("entry_order_no", ""), order_results)
        if avg_price <= 0 or mat_qty <= 0:
            continue

        with ORDER_STATE_LOCK:
            # WebSocket 與輪詢共用同一把鎖；先成功接管的來源擁有本筆成交校正權。
            if state.get("entry_price_source") == "dealt_report":
                continue
            try:
                entry_order_qty = int(state.get("entry_order_qty", state.get("qty", 0)) or 0)
                previous_filled_qty = int(state.get("entry_filled_qty", 0) or 0)
            except (TypeError, ValueError):
                entry_order_qty = 0
                previous_filled_qty = 0

            if mat_qty <= previous_filled_qty and state.get("entry_price_source") == "order_results":
                continue

            state["entry_price"] = avg_price
            state["qty"] = mat_qty
            state["entry_filled_qty"] = mat_qty
            state["entry_fill_confirmed"] = True
            state["entry_fully_filled"] = entry_order_qty > 0 and mat_qty >= entry_order_qty
            state["entry_order_pending"] = not state["entry_fully_filled"]
            state["in_position"] = True
            state["entry_price_source"] = "order_results"
            state["entry_fill_update_time"] = now_tpe().isoformat()
            profit_tracking_active = bool(state.get("profit_tracking_active"))
            if not profit_tracking_active:
                recalc_entry_position_prices(state)
            entry_fully_filled = bool(state.get("entry_fully_filled"))
            entry_order_no = state.get("entry_order_no")
            state_snapshot = dict(state)

        if profit_tracking_active:
            print(f"[{state.get('symbol_name')}] 已啟動追蹤停利，成交價量校正不覆寫既有停利軌跡")

        atomic_write_json(state_path(state_snapshot.get("symbol_code_with_suf", "")), state_snapshot)
        fill_text = "完整成交" if entry_fully_filled else "部分成交"
        trade_log(
            "FILL_UPDATE",
            **state_symbol_fields(state),
            role="entry",
            source="get_order_results",
            fill_status=fill_text,
            ord_no=entry_order_no,
            avg_price=f"{avg_price:.4f}",
            filled_qty=mat_qty,
            expected_qty=entry_order_qty,
        )
        print_entry_position_prices(state_snapshot)

    for state in exit_candidates:
        resolve_unknown_order(state, "exit", order_results)
        avg_price, mat_qty = get_order_fill_info_by_results(state.get("exit_order_no", ""), order_results)
        if avg_price <= 0 or mat_qty <= 0:
            continue

        with ORDER_STATE_LOCK:
            if state.get("exit_price_source") == "dealt_report":
                continue
            try:
                exit_order_qty = int(state.get("exit_order_qty", state.get("qty", 0)) or 0)
                previous_filled_qty = int(state.get("exit_filled_qty", 0) or 0)
            except (TypeError, ValueError):
                exit_order_qty = 0
                previous_filled_qty = 0

            if mat_qty <= previous_filled_qty and state.get("exit_price_source") == "order_results":
                continue

            state["exit_price"] = avg_price
            state["exit_filled_qty"] = mat_qty
            state["exit_fill_confirmed"] = True
            state["exit_fully_filled"] = exit_order_qty > 0 and mat_qty >= exit_order_qty
            state["exit_price_source"] = "order_results"
            state["exit_fill_update_time"] = now_tpe().isoformat()

            if state["exit_fully_filled"]:
                state["exit_order_pending"] = False
                state["traded"] = True
                state["in_position"] = False
                if state.get("exit_reason_pending"):
                    state["exit_reason"] = state.get("exit_reason_pending")
            exit_fully_filled = bool(state.get("exit_fully_filled"))
            exit_order_no = state.get("exit_order_no")
            exit_reason = state.get("exit_reason_pending") or state.get("exit_reason")
            state_snapshot = dict(state)

        atomic_write_json(state_path(state_snapshot.get("symbol_code_with_suf", "")), state_snapshot)
        fill_text = "完整成交" if exit_fully_filled else "部分成交"
        trade_log(
            "FILL_UPDATE",
            **state_symbol_fields(state),
            role="exit",
            source="get_order_results",
            fill_status=fill_text,
            ord_no=exit_order_no,
            exit_reason=exit_reason,
            avg_price=f"{avg_price:.4f}",
            filled_qty=mat_qty,
            expected_qty=exit_order_qty,
        )

    for role, pending in (("entry", candidates), ("exit", exit_candidates)):
        for state in pending:
            reconcile_terminal_order(state, role, order_results)

    # 撤單只能依券商最新原始明細進行，不能依本機猜測的欄位組裝。
    if sdk is not None:
        request_expired_entry_cancellations(states, sdk, order_results)


def request_expired_entry_cancellations(states, sdk, order_results) -> None:
    now_local = now_tpe()
    for state in states.values():
        with ORDER_STATE_LOCK:
            if (not state.get("entry_order_pending") or state.get("entry_cancel_requested")
                    or (now_local.hour, now_local.minute) <= get_entry_check_end_time(state)):
                continue
            order_no = str(state.get("entry_order_no", "") or "").strip()
        rows = [row for row in order_results if str(row.get("ord_no", "") or "").strip() == order_no]
        if not order_no or len(rows) != 1:
            continue
        if time.monotonic() < state.get("entry_cancel_retry_after", 0):
            continue
        try:
            row = dict(rows[0])  # SDK recover_order_result 需使用完整原始欄位。
            future = start_serialized_trade_call("cancel_order", lambda row=row: sdk.cancel_order(row))
        except TradeApiBusy:
            return  # 不排隊，下一次已完成的委託查詢再嘗試。
        state["entry_cancel_requested"] = True
        ENTRY_CANCEL_FUTURES[order_no] = (future, state)
        atomic_write_json(state_path(state["symbol_code_with_suf"]), dict(state))
        trade_log("ENTRY_CANCEL_REQUESTED", ord_no=order_no,
                  action="past_entry_deadline_keep_pending_until_broker_confirmation")


def maintain_pending_orders(states) -> None:
    for order_no, (future, state) in list(ENTRY_CANCEL_FUTURES.items()):
        if not future.done():
            continue
        ENTRY_CANCEL_FUTURES.pop(order_no, None)
        try:
            response = future.result()
            if not isinstance(response, dict) or str(response.get("ret_code", "")) != "000000":
                raise RuntimeError(f"cancel response not confirmed: {response!r}")
            trade_log("ENTRY_CANCEL_ACKNOWLEDGED", ord_no=order_no,
                      action="await_terminal_order_results_or_fill")
        except Exception as exc:
            # 取消失敗不解除 entry pending，也不允許另一筆入場單。
            state["entry_cancel_requested"] = False
            state["entry_cancel_retry_after"] = time.monotonic() + ORDER_RESULTS_UPDATE_SECONDS
            trade_log("ENTRY_CANCEL_ERROR", error=True, ord_no=order_no, error_msg=repr(exc))
    attention_changed = False
    for state in states.values():
        for role in ("entry", "exit"):
            if not state.get(f"{role}_order_pending"):
                continue
            started = state.get(f"{role}_pending_since_monotonic", time.monotonic())
            expired_entry = role == "entry" and (now_tpe().hour, now_tpe().minute) > get_entry_check_end_time(state)
            if ((expired_entry or time.monotonic() - started >= ORDER_UNKNOWN_ALERT_SECONDS)
                    and not state.get(f"{role}_pending_alerted")):
                state[f"{role}_pending_alerted"] = True
                attention_changed = True
                trade_log("ORDER_PENDING_REQUIRES_ATTENTION", error=True,
                          **state_symbol_fields(state), role=role,
                          ord_no=state.get(f"{role}_order_no"),
                          request_id=state.get(f"{role}_request_id"),
                          action="keep_reconciling_no_blind_resubmit_manual_broker_check_required")
    if attention_changed or (TRADE_API_DEGRADED_EVENT.is_set() and not getattr(maintain_pending_orders, "degraded_reported", False)):
        maintain_pending_orders.degraded_reported = TRADE_API_DEGRADED_EVENT.is_set()
        report_unsettled_trades(states, "pending_order_watchdog")


def start_order_results_query(sdk) -> Future:
    return start_serialized_trade_call("get_order_results", sdk.get_order_results)


def get_exchange_key_for_symbol(symbol_code_with_suf: str) -> str:
    symbol_upper = str(symbol_code_with_suf or "").upper()
    if symbol_upper.endswith(".TWO"):
        return "TPEX"
    return "TWSE"


def get_market_key_for_symbol(symbol_code_with_suf: str, industry_code: str) -> str:
    exchange_key = get_exchange_key_for_symbol(symbol_code_with_suf)
    return f"{exchange_key}:{str(industry_code).zfill(2)}"


def get_industry_index_config(symbol_code_with_suf: str, industry_code: str) -> tuple[str, Dict[str, Any]]:
    market_key = get_market_key_for_symbol(symbol_code_with_suf, industry_code)
    return market_key, market_previous_close_indices.get(market_key, {})


def get_strategy_decision_drop_percent(index_key: str) -> float | None:
    return get_lower_strategy().get_strategy_decision_drop_percent(index_key)


def get_strategy_decision_rebound_percent(index_key: str) -> float | None:
    return get_lower_strategy().get_strategy_decision_rebound_percent(index_key)




def update_market_strategy_decision_gate_state(market_key: str, index_value: float, event_time: Any) -> None:
    return get_lower_strategy().update_market_strategy_decision_gate_state(market_key, index_value, event_time)


def get_market_index_snapshot(index_key: str) -> Dict[str, Any]:
    with MARKET_INDEX_STATE_LOCK:
        return dict(MARKET_INDEX_STATE.get(index_key, {}))


def get_market_gate_snapshots() -> tuple[Dict[str, Dict[str, Any]], int]:
    """在同一個 lock 內取得兩個大盤指數的同輪快照。"""
    with MARKET_INDEX_STATE_LOCK:
        completed_snapshots = MARKET_INDEX_ROUND_STATE.get("snapshots", {})
        snapshots = {
            index_key: dict(completed_snapshots.get(index_key, {}))
            for index_key in MARKET_GATE_INDEX_KEYS
        }
        completed_round = int(MARKET_INDEX_ROUND_STATE.get("completed_round", 0) or 0)
    return snapshots, completed_round


def get_market_index_snapshot_freshness(
    index_key: str,
    market_state: Dict[str, Any],
) -> tuple[bool, Optional[float]]:
    last_updated = market_state.get("last_updated")
    if not last_updated:
        return False, None
    try:
        last_updated_dt = datetime.fromisoformat(str(last_updated))
    except ValueError:
        return False, None
    if last_updated_dt.tzinfo is None:
        last_updated_dt = TZ.localize(last_updated_dt)
    age_seconds = (now_tpe() - last_updated_dt).total_seconds()
    if index_key in MARKET_GATE_INDEX_KEYS:
        return (
            -MARKET_INDEX_FUTURE_TOLERANCE_SECONDS
            <= age_seconds
            <= MARKET_INDEX_STALE_SECONDS
        ), age_seconds
    return 0 <= age_seconds <= INDUSTRY_INDEX_STALE_SECONDS, age_seconds


def market_gate_snapshots_are_fresh(snapshots: Dict[str, Dict[str, Any]]) -> bool:
    return all(
        get_market_index_snapshot_freshness(index_key, snapshots.get(index_key, {}))[0]
        for index_key in MARKET_GATE_INDEX_KEYS
    )


def market_gate_round_state_is_fresh_locked() -> bool:
    snapshots = MARKET_INDEX_ROUND_STATE.get("snapshots", {})
    return market_gate_snapshots_are_fresh({
        index_key: dict(snapshots.get(index_key, {}))
        for index_key in MARKET_GATE_INDEX_KEYS
    })


def market_gate_diagnostic_fields(snapshots: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    for index_key in MARKET_GATE_INDEX_KEYS:
        state = snapshots.get(index_key, {})
        _, age_seconds = get_market_index_snapshot_freshness(index_key, state)
        prefix = "twse" if index_key == "TWSE:MARKET" else "tpex"
        fields[f"{prefix}_index"] = state.get("last_index")
        fields[f"{prefix}_provider_last_updated"] = state.get("last_updated")
        fields[f"{prefix}_received_at"] = state.get("received_at")
        fields[f"{prefix}_age_seconds"] = age_seconds
    return fields


def get_critical_market_gate_snapshots(context: str) -> Dict[str, Dict[str, Any]]:
    """資料逾時時只等待既有背景輪次；不在主執行緒額外查 REST。"""
    snapshots, completed_round = get_market_gate_snapshots()
    if market_gate_snapshots_are_fresh(snapshots):
        return snapshots

    stale_indices = [
        index_key
        for index_key in MARKET_GATE_INDEX_KEYS
        if not get_market_index_snapshot_freshness(index_key, snapshots.get(index_key, {}))[0]
    ]
    wait_started = time.monotonic()
    trade_log(
        "MARKET_INDEX_CRITICAL_WAIT_START",
        context=context,
        stale_indices=",".join(stale_indices),
        completed_round=completed_round,
        timeout_seconds=MARKET_INDEX_CRITICAL_WAIT_SECONDS,
        action="wait_existing_background_round_no_extra_rest_request",
        **market_gate_diagnostic_fields(snapshots),
    )

    with MARKET_INDEX_ROUND_CONDITION:
        MARKET_INDEX_ROUND_CONDITION.wait_for(
            lambda: (
                market_gate_round_state_is_fresh_locked()
                or MARKET_INDEX_REST_FATAL_EVENT.is_set()
                or MARKET_INDEX_REST_STOP_EVENT.is_set()
            ),
            timeout=MARKET_INDEX_CRITICAL_WAIT_SECONDS,
        )

    snapshots, new_completed_round = get_market_gate_snapshots()
    fresh = market_gate_snapshots_are_fresh(snapshots)
    trade_log(
        "MARKET_INDEX_CRITICAL_WAIT_DONE",
        error=not fresh,
        context=context,
        waited_seconds=f"{time.monotonic() - wait_started:.3f}",
        previous_completed_round=completed_round,
        completed_round=new_completed_round,
        fresh=fresh,
        action="use_refreshed_snapshot" if fresh else "fail_closed_no_extra_rest_request",
        **market_gate_diagnostic_fields(snapshots),
    )
    return snapshots


def is_market_index_fresh(index_key: str) -> tuple[bool, Optional[float]]:
    return get_market_index_snapshot_freshness(index_key, get_market_index_snapshot(index_key))


def get_fresh_market_index_value_from_snapshot(
    index_key: str,
    market_state: Dict[str, Any],
) -> tuple[Optional[float], Optional[float], str]:
    last_index = market_state.get("last_index")
    if last_index is None:
        return None, None, "尚未收到 REST 指數資料"

    is_fresh, age_seconds = get_market_index_snapshot_freshness(index_key, market_state)
    if not is_fresh:
        age_text = "--" if age_seconds is None else f"{age_seconds:.1f}"
        if (
            index_key in MARKET_GATE_INDEX_KEYS
            and age_seconds is not None
            and age_seconds < -MARKET_INDEX_FUTURE_TOLERANCE_SECONDS
        ):
            return None, age_seconds, f"REST 指數供應商時間超前 age_seconds={age_text}"
        return None, age_seconds, f"REST 指數資料逾時 age_seconds={age_text}"

    try:
        return float(last_index), age_seconds, ""
    except (TypeError, ValueError):
        return None, age_seconds, "REST 指數資料格式錯誤"


def get_fresh_market_index_value(index_key: str) -> tuple[Optional[float], Optional[float], str]:
    return get_fresh_market_index_value_from_snapshot(index_key, get_market_index_snapshot(index_key))


def get_market_strategy_decision_gate_result(index_key: str, market_state: Optional[Dict[str, Any]]=None) -> Dict[str, Any]:
    return get_lower_strategy().get_market_strategy_decision_gate_result(index_key, market_state)


def calculate_percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def summarize_lower_strategy_decision_candidates(states: Dict[str, Dict[str, Any]], realtime_sdk: EsunMarketdata, completed_trade_matches: Optional[Dict[str, bool]]=None, current_prices: Optional[Dict[str, float]]=None) -> Dict[str, Any]:
    return get_lower_strategy().summarize_lower_strategy_decision_candidates(states, realtime_sdk, completed_trade_matches, current_prices)


def summarize_lower_price_movements(states: Dict[str, Dict[str, Any]], realtime_sdk: EsunMarketdata, baseline_prices: Dict[str, float], current_prices: Optional[Dict[str, float]]=None) -> Dict[str, Any]:
    return get_lower_strategy().summarize_lower_price_movements(states, realtime_sdk, baseline_prices, current_prices)


def get_due_lower_exit_check(now_local: datetime) -> Optional[datetime]:
    return get_lower_strategy().get_due_lower_exit_check(now_local)


def check_lower_percent_exit(states: Dict[str, Dict[str, Any]], realtime_sdk: EsunMarketdata, current_prices: Optional[Dict[str, float]]=None, check_dt: Optional[datetime]=None) -> Optional[Dict[str, Any]]:
    return get_lower_strategy().check_lower_percent_exit(states, realtime_sdk, current_prices, check_dt)


def handle_lower_percent_exit(states: Dict[str, Dict[str, Any]], mysdk, summary: Dict[str, Any]) -> None:
    return get_lower_strategy().handle_lower_percent_exit(states, mysdk, summary)


def format_lower_decision_summary(summary: Dict[str, Any]) -> str:
    return get_lower_strategy().format_lower_decision_summary(summary)


def decide_entry_mode_by_market_gate(states: Dict[str, Dict[str, Any]], market_snapshots: Optional[Dict[str, Dict[str, Any]]]=None, realtime_sdk: Optional[EsunMarketdata]=None, window_number: int=1, lower_decision_summary: Optional[Dict[str, Any]]=None) -> tuple[int, list[Dict[str, Any]]]:
    return get_lower_strategy().decide_entry_mode_by_market_gate(states, market_snapshots, realtime_sdk, window_number, lower_decision_summary)


def fetch_lower_early_summary(states_snapshot, realtime_sdk):
    return get_lower_strategy().fetch_lower_early_summary(states_snapshot, realtime_sdk)


def lower_minute_checks_complete(states):
    return get_lower_strategy().lower_minute_checks_complete(states)


def poll_lower_decision_inputs(future, window_number, wait_started):
    return get_lower_strategy().poll_lower_decision_inputs(future, window_number, wait_started)


def format_market_gate_value(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "N/A"


def format_market_gate_time(value: Any) -> str:
    if value in (None, ""):
        return "--"

    try:
        if isinstance(value, (int, float)):
            timestamp = float(value)
        else:
            text = str(value).strip()
            if not text:
                return "--"
            if text.replace(".", "", 1).isdigit():
                timestamp = float(text)
            else:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = TZ.localize(dt)
                else:
                    dt = dt.astimezone(TZ)
                return dt.strftime("%H:%M:%S")

        if timestamp > 10_000_000_000_000:
            timestamp /= 1_000_000
        elif timestamp > 10_000_000_000:
            timestamp /= 1_000
        dt = datetime.fromtimestamp(timestamp, TZ)
        return dt.strftime("%H:%M:%S")
    except (TypeError, ValueError, OSError):
        return str(value)


def format_market_gate_age(value: Any) -> str:
    if value is None:
        return "--"
    try:
        return f"{float(value):.1f}s"
    except (TypeError, ValueError):
        return str(value)


def format_market_gate_percent(value: Any, *, threshold_drop: bool = False) -> str:
    if value is None:
        return "N/A"
    try:
        percent = float(value)
        if threshold_drop and percent != 0:
            percent = -percent
        return f"{percent:+.2f}%"
    except (TypeError, ValueError):
        return str(value)


def print_lower_strategy_early_breakout_deadline_summary(market_snapshots: Optional[Dict[str, Dict[str, Any]]]=None) -> None:
    return get_lower_strategy().print_lower_strategy_early_breakout_deadline_summary(market_snapshots)


def print_entry_mode_decision(entry_mode: int, gate_results: list[Dict[str, Any]], states: Dict[str, Dict[str, Any]]) -> None:
    return get_lower_strategy().print_entry_mode_decision(entry_mode, gate_results, states)


def apply_entry_mode_to_states(states: Dict[str, Dict[str, Any]], entry_mode: int) -> None:
    persist_entry_mode_to_stock_data(entry_mode)
    for st in states.values():
        st["qty"] = get_entry_order_quantity()
        if entry_mode == ENTRY_MODE_LOWER:
            st["lower_window_number"] = LOWER_RUNTIME.session_state["window_number"]
            st["lower_activation_dt"] = LOWER_RUNTIME.session_state["activation_dt"].isoformat()
            st["lower_entry_start_time"] = get_entry_check_start_time()
            st["lower_entry_end_time"] = get_entry_check_end_time(st)
            if st.get("symbol_code_with_suf") not in LOWER_RUNTIME.session_state["baseline_prices"]:
                st["traded"] = True
                st["exit_reason"] = "missing_activation_price"
        st["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)


def provider_timestamp_to_datetime(value: Any) -> Optional[datetime]:
    try:
        timestamp = float(value)
        if timestamp > 10_000_000_000_000:
            timestamp /= 1_000_000
        elif timestamp > 10_000_000_000:
            timestamp /= 1_000
        return datetime.fromtimestamp(timestamp, TZ)
    except (TypeError, ValueError, OSError):
        return None


def get_stock_rest_client(realtime_sdk: EsunMarketdata):
    if MARKETDATA_STOCK_REST_CLIENT is not None:
        return MARKETDATA_STOCK_REST_CLIENT
    return RateLimitedStockRest(realtime_sdk.rest_client.stock)


def update_index_from_rest(realtime_sdk: EsunMarketdata, index_key: str) -> bool:
    index_config = market_previous_close_indices.get(index_key, {})
    index_symbol = str(index_config.get("symbol", "") or "").strip()
    if not index_symbol:
        trade_log("INDEX_REST_ERROR", error=True, index_key=index_key, reason="missing_symbol")
        return False

    started = time.monotonic()
    try:
        with marketdata_priority(MARKETDATA_PRIORITY_INDEX):
            quote = get_stock_rest_client(realtime_sdk).intraday.quote(symbol=index_symbol)
        index_value = float(quote["closePrice"])
        provider_time_raw = quote.get("lastUpdated")
        provider_time = provider_timestamp_to_datetime(provider_time_raw)
        if provider_time is None:
            raise ValueError(f"invalid lastUpdated={provider_time_raw!r}")
    except Exception as exc:
        with MARKET_INDEX_STATE_LOCK:
            market_state = MARKET_INDEX_STATE.setdefault(index_key, {})
            should_log_error = not market_state.get("rest_error_logged")
            market_state["rest_error_logged"] = True
        if should_log_error:
            trade_log(
                "INDEX_REST_ERROR", error=True, index_key=index_key, symbol=index_symbol,
                elapsed_seconds=f"{time.monotonic() - started:.3f}", error_msg=repr(exc),
                action="keep_previous_value_and_fail_closed_when_stale",
            )
        return False

    received_at = now_tpe()
    provider_age_seconds = (received_at - provider_time).total_seconds()
    stale_seconds = MARKET_INDEX_STALE_SECONDS if index_key in MARKET_GATE_INDEX_KEYS else INDUSTRY_INDEX_STALE_SECONDS
    minimum_age_seconds = (
        -MARKET_INDEX_FUTURE_TOLERANCE_SECONDS
        if index_key in MARKET_GATE_INDEX_KEYS
        else 0.0
    )
    is_fresh = minimum_age_seconds <= provider_age_seconds <= stale_seconds
    diagnostic_time_reached = (received_at.hour, received_at.minute) >= (9, 0)
    with MARKET_INDEX_STATE_LOCK:
        market_state = MARKET_INDEX_STATE.setdefault(index_key, {})
        recovered_from_error = bool(market_state.get("rest_error_logged"))
        market_state.update({
            "symbol": index_symbol,
            "last_index": index_value,
            "time": provider_time_raw,
            "last_updated": provider_time.isoformat(),
            "received_at": received_at.isoformat(),
            "rest_error_logged": False,
        })
        should_log_stale = diagnostic_time_reached and not is_fresh and not market_state.get("rest_stale_logged")
        market_state["rest_stale_logged"] = diagnostic_time_reached and not is_fresh
        if is_fresh:
            update_market_strategy_decision_gate_state(index_key, index_value, provider_time_raw)
    if recovered_from_error:
        trade_log("INDEX_REST_RECOVERED", index_key=index_key, symbol=index_symbol)
    if should_log_stale:
        trade_log(
            "INDEX_REST_STALE", error=True, index_key=index_key, symbol=index_symbol,
            provider_age_seconds=f"{provider_age_seconds:.1f}", stale_limit_seconds=stale_seconds,
            provider_last_updated=provider_time.isoformat(), received_at=received_at.isoformat(),
            index_value=index_value,
            action="fail_closed",
        )
    return True


def update_required_industry_indices(states: Dict[str, Dict[str, Any]], realtime_sdk: EsunMarketdata) -> None:
    for index_key, future in list(INDUSTRY_INDEX_FUTURES.items()):
        if future.done():
            INDUSTRY_INDEX_FUTURES.pop(index_key)
            try:
                future.result()
            except Exception as exc:
                trade_log('INDUSTRY_INDEX_ERROR', error=True, index_key=index_key, error_msg=repr(exc))
    for index_key in get_required_industry_index_keys(states):
        if index_key not in INDUSTRY_INDEX_FUTURES:
            INDUSTRY_INDEX_FUTURES[index_key] = INDEX_DATA_EXECUTOR.submit(update_index_from_rest, realtime_sdk, index_key)


def get_required_industry_index_keys(states: Dict[str, Dict[str, Any]]) -> list[str]:
    return sorted({
        str(state.get("market_index_key", "") or "").strip()
        for state in states.values()
        if (
            not state.get("traded")
            and not state.get("in_position")
            and not state.get("entry_order_pending")
            and not state.get("exit_order_pending")
            and state.get("market_index_key")
        )
    } - set(MARKET_GATE_INDEX_KEYS))


def print_rest_quote_rate_limit_estimate(states: Dict[str, Dict[str, Any]]) -> None:
    stock_count = sum(1 for state in states.values() if not state.get("traded"))
    industry_count = len(get_required_industry_index_keys(states))
    stock_and_industry_per_round = stock_count + industry_count
    stock_quote_rounds_per_minute = math.ceil(60 / QUOTE_ROUND_INTERVAL_SECONDS)
    stock_and_industry_per_minute = stock_and_industry_per_round * stock_quote_rounds_per_minute

    market_index_count = (
        0 if MARKET_INDEX_REST_STOP_EVENT.is_set() else len(MARKET_GATE_INDEX_KEYS)
    )
    market_index_rounds_per_minute = math.ceil(60 / MARKET_INDEX_REST_POLL_SECONDS)
    market_indices_per_minute = market_index_count * market_index_rounds_per_minute

    lower_pool_count = len({get_pure_symbol(stock[0])[1] for stock in total_stocks})
    lower_exit_quotes_per_minute = lower_pool_count
    total_per_minute = stock_and_industry_per_minute + market_indices_per_minute + lower_exit_quotes_per_minute + stock_count
    usage_percent = total_per_minute / INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE * 100

    print(
        f"個股與產業指數：{stock_and_industry_per_round} × {stock_quote_rounds_per_minute} "
        f"= {stock_and_industry_per_minute} 次／分鐘"
    )
    if market_index_count:
        print(
            f"大盤 IX0001、IX0043：{market_index_count} × {market_index_rounds_per_minute} "
            f"= {market_indices_per_minute} 次／分鐘"
        )
    print(f"LOWER 比例檢核：每 {LOWER_EXIT_CHECK_INTERVAL_MINUTES} 分鐘一次，檢核當分鐘最多額外 {lower_exit_quotes_per_minute} 次")
    print(f"09:30 分K篩選：最多額外 {stock_count} 次，當日僅一次")
    print(f"合計行情需求估算：約 {total_per_minute} 次／分鐘（尚未計入 FALL 候選）")
    print(f"共用節流：每 {STOCK_REST_REQUEST_INTERVAL_SECONDS:g} 秒最多啟動一筆，約 "
          f"{60 / STOCK_REST_REQUEST_INTERVAL_SECONDS:.0f} 次／分鐘；超額需求排隊，持倉優先")
    print(f"連線限制：每分鐘 {INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE} 次")
    print(
        f"使用率預做：{total_per_minute} ÷ {INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE} "
        f"= {usage_percent:.0f}%"
    )


def log_market_index_rest_watchdog_once(now_local: datetime) -> None:
    if MARKET_INDEX_REST_STOP_EVENT.is_set():
        return
    if (now_local.hour, now_local.minute) < MARKET_INDEX_REST_START_TIME:
        return

    for index_key in MARKET_GATE_INDEX_KEYS:
        with MARKET_INDEX_STATE_LOCK:
            live_state = MARKET_INDEX_STATE.setdefault(index_key, {})
            market_state = dict(live_state)
            reference_time = market_state.get("received_at") or market_state.get("polling_started_at")
            if not reference_time:
                continue
            try:
                reference_dt = datetime.fromisoformat(str(reference_time))
            except ValueError:
                continue
            if reference_dt.tzinfo is None:
                reference_dt = TZ.localize(reference_dt)
            age_seconds = (now_local - reference_dt).total_seconds()
            if age_seconds <= MARKET_INDEX_STALE_SECONDS:
                live_state["rest_watchdog_logged"] = False
                continue
            if live_state.get("rest_watchdog_logged"):
                continue
            live_state["rest_watchdog_logged"] = True

        trade_log(
            "MARKET_INDEX_REST_WATCHDOG_STALE",
            error=True,
            index_key=index_key,
            symbol=market_state.get("symbol") or market_previous_close_indices.get(index_key, {}).get("symbol"),
            seconds_since_success=f"{age_seconds:.1f}",
            stale_limit_seconds=MARKET_INDEX_STALE_SECONDS,
            last_received_at=market_state.get("received_at"),
            polling_started_at=market_state.get("polling_started_at"),
            action="diagnostic_only_no_restart_fail_closed_by_data_freshness",
        )


def start_market_index_rest_poller(realtime_sdk: EsunMarketdata) -> threading.Thread:
    def worker() -> None:
        try:
            now_local = now_tpe()
            start_at = now_local.replace(
                hour=MARKET_INDEX_REST_START_TIME[0],
                minute=MARKET_INDEX_REST_START_TIME[1],
                second=0,
                microsecond=0,
            ) + timedelta(seconds=MARKET_INDEX_REST_POLL_OFFSET_SECONDS)
            wait_seconds = max(0.0, (start_at - now_local).total_seconds())
            if wait_seconds > 0:
                trade_log(
                    "MARKET_INDEX_REST_WAITING",
                    start_time=start_at.strftime("%H:%M:%S.%f")[:-3],
                    wait_seconds=f"{wait_seconds:.1f}",
                )
                if MARKET_INDEX_REST_STOP_EVENT.wait(timeout=wait_seconds):
                    return

            trade_log(
                "MARKET_INDEX_REST_POLLING_START",
                symbols=",".join(str(market_previous_close_indices.get(key, {}).get("symbol", "")) for key in MARKET_GATE_INDEX_KEYS),
                interval_seconds=MARKET_INDEX_REST_POLL_SECONDS,
                offset_seconds=MARKET_INDEX_REST_POLL_OFFSET_SECONDS,
            )
            polling_started_at = now_tpe().isoformat()
            with MARKET_INDEX_STATE_LOCK:
                for index_key in MARKET_GATE_INDEX_KEYS:
                    MARKET_INDEX_STATE.setdefault(index_key, {})["polling_started_at"] = polling_started_at
            while not MARKET_INDEX_REST_STOP_EVENT.is_set():
                for index_key in MARKET_GATE_INDEX_KEYS:
                    if MARKET_INDEX_REST_STOP_EVENT.is_set():
                        return
                    update_index_from_rest(realtime_sdk, index_key)
                with MARKET_INDEX_ROUND_CONDITION:
                    MARKET_INDEX_ROUND_STATE["completed_round"] = (
                        int(MARKET_INDEX_ROUND_STATE.get("completed_round", 0) or 0) + 1
                    )
                    MARKET_INDEX_ROUND_STATE["completed_at"] = now_tpe().isoformat()
                    MARKET_INDEX_ROUND_STATE["snapshots"] = {
                        index_key: dict(MARKET_INDEX_STATE.get(index_key, {}))
                        for index_key in MARKET_GATE_INDEX_KEYS
                    }
                    MARKET_INDEX_ROUND_CONDITION.notify_all()
                now_local = now_tpe()
                poll_offset = timedelta(seconds=MARKET_INDEX_REST_POLL_OFFSET_SECONDS)
                next_poll = (
                    ceil_next_interval(
                        now_local - poll_offset,
                        MARKET_INDEX_REST_POLL_SECONDS,
                    )
                    + poll_offset
                )
                MARKET_INDEX_REST_STOP_EVENT.wait(timeout=max(0.2, (next_poll - now_local).total_seconds()))
        except Exception as exc:
            trade_log(
                "MARKET_INDEX_REST_POLLER_FATAL",
                error=True,
                error_msg=repr(exc),
                action="block_market_dependent_trading_no_restart",
            )
            MARKET_INDEX_REST_FATAL_EVENT.set()
        finally:
            with MARKET_INDEX_ROUND_CONDITION:
                MARKET_INDEX_ROUND_CONDITION.notify_all()
            if not MARKET_INDEX_REST_STOP_EVENT.is_set():
                trade_log(
                    "MARKET_INDEX_REST_POLLER_STOPPED",
                    error=True,
                    reason="unexpected_thread_exit",
                    action="block_market_dependent_trading_no_restart",
                )
                MARKET_INDEX_REST_FATAL_EVENT.set()

    thread = threading.Thread(target=worker, name="esun-market-index-rest-poller", daemon=True)
    thread.start()
    return thread


def calculate_entry_range_bounds(
    reference_price: float,
    limit_price: float,
    start_percent: float,
    end_percent: float,
) -> tuple[float, float]:
    """依參考價到漲跌停的百分比區間，回傳可入場價格上下界。"""
    start_price = reference_price + (limit_price - reference_price) * (start_percent / 100.0)
    end_price = reference_price + (limit_price - reference_price) * (end_percent / 100.0)
    return min(start_price, end_price), max(start_price, end_price)


def is_price_in_entry_range(price: float, lower_bound: float, upper_bound: float) -> bool:
    """判斷價格是否落在入場區間內，含上下界。"""
    return lower_bound <= price <= upper_bound


def lower_industry_market_filter_pass(state: Dict[str, Any]) -> bool:
    return get_lower_strategy().lower_industry_market_filter_pass(state)


def format_industry_market_filter_pass_text(state: Dict[str, Any]) -> str:
    return get_lower_strategy().format_industry_market_filter_pass_text(state)


def calculate_profit_trail_step(price: float, percent: float) -> float:
    """依目前價格計算追蹤停利距離，且至少保留 1 tick。"""
    return max(price * (percent / 100.0), get_tick_size(price))


def floor_price_to_tick_value(price: float) -> float:
    tick = get_tick_size(price)
    adjusted = math.floor(price / tick) * tick
    if adjusted < 100:
        return round(adjusted, 2)
    if adjusted < 1000:
        return round(adjusted, 1)
    return float(int(adjusted))


def ceil_price_to_tick_value(price: float) -> float:
    tick = get_tick_size(price)
    adjusted = math.ceil(price / tick - 1e-12) * tick
    if adjusted < 100:
        return round(adjusted, 2)
    if adjusted < 1000:
        return round(adjusted, 1)
    return float(int(adjusted))


def today_str_tpe() -> str:
    return now_tpe().date().isoformat()  # e.g. "2025-10-01"


def ceil_next_interval(t: datetime, interval_sec: int) -> datetime:
    """
    回傳 t 下一個 interval_sec 秒的時間點（對齊刻度）
    例如 interval_sec=30：
    10:00:07 → 10:00:30
    10:00:31 → 10:01:00
    """
    # 目前 timestamp（秒）
    ts = int(t.timestamp())

    # 算出下一個刻度
    next_ts = ((ts // interval_sec) + 1) * interval_sec

    return datetime.fromtimestamp(next_ts, tz=t.tzinfo)


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def clear_state_dir():
    # 實機版刻意採完整日內啟動模型：啟動時清空狀態，若盤中失敗則退出今日交易。
    # 不支援盤中重啟後接續既有委託/持倉狀態，該情境由外部或人工流程處理。
    abs_state_dir = os.path.abspath(STATE_DIR)
    if not os.path.isdir(abs_state_dir):
        os.makedirs(abs_state_dir, exist_ok=True)
        return

    for entry in os.listdir(abs_state_dir):
        entry_path = os.path.join(abs_state_dir, entry)
        try:
            if os.path.isdir(entry_path):
                shutil.rmtree(entry_path)
            else:
                os.remove(entry_path)
        except OSError as e:
            print(f"[WARN] 清空 stock_state 失敗: {entry_path} - {e}")


def state_path(symbol: str) -> str:
    ensure_dir(STATE_DIR)
    fname = f"{symbol}.SymbolState.json"
    return os.path.join(STATE_DIR, fname)


def persist_entry_mode_to_stock_data(entry_mode: int) -> None:
    stock_data.entry_mode = entry_mode


def atomic_write_json(path: str, data: Dict[str, Any], *, strict: bool = False):
    """
    原子寫入：先寫到臨時檔，fsync 後再 os.replace 覆蓋目標檔，降低檔案損壞風險。
    改進：
    - 轉為使用絕對目標路徑，確保 tmp 檔寫在相同目錄下（避免跨磁碟或路徑差異導致 os.replace 失敗）。
    - 對 Windows / OneDrive 可能的鎖定 (PermissionError) 做重試與 fallback（先嘗試刪除目標檔，再替換；最後備援以複製內容覆蓋）。
    - 確保臨時檔在任何情況下都會適當清理，避免殘留。
    """
    # FALL 與 LOWER/AVG 觀察狀態分檔，市場統計寫檔不得覆蓋 FALL 委託狀態。
    if is_fall_strategy(data) and path.endswith('.SymbolState.json'):
        path = path[:-len('.SymbolState.json')] + '.FALL.SymbolState.json'
    abs_path = os.path.abspath(path)
    ensure_dir(os.path.dirname(abs_path))

    tmp_path = None
    # 寫到與目標相同目錄下的暫存檔，確保同一檔案系統
    with NamedTemporaryFile("w", delete=False, dir=os.path.dirname(abs_path), suffix=".tmp", encoding="utf-8") as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = tmp.name

    # 嘗試以 os.replace 原子替換；若遇到 PermissionError（常見於 Windows/OneDrive 被鎖定），重試數次
    max_retries = 6
    for attempt in range(max_retries):
        try:
            os.replace(tmp_path, abs_path)
            return
        except PermissionError:
            # 可能是 OneDrive/防毒或其他程序短暫鎖定檔案，先等一會兒再重試
            time.sleep(0.2 * (attempt + 1))
            # 嘗試刪除目標檔再替換（若刪除失敗會在下一輪重試）
            try:
                if os.path.exists(abs_path):
                    os.remove(abs_path)
                    os.replace(tmp_path, abs_path)
                    return
            except Exception:
                pass
        except Exception as e:
            # 其他錯誤（如跨檔案系統），跳出重試
            print(f"[WARN] atomic_write_json replace failed: {e}")
            break

    # 最後備援：以非原子的方式讀寫內容（盡量確保目標檔被更新），並清理 tmp 檔
    try:
        with open(tmp_path, "rb") as src, open(abs_path, "wb") as dst:
            dst.write(src.read())
        try:
            os.remove(tmp_path)
        except Exception:
            pass
    except Exception as e:
        print(f"[ERROR] atomic_write_json final fallback failed: {e}")
        # 嘗試移除 tmp 檔，避免殘留
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        if strict:
            raise


def load_json_or_none(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] 無法讀取狀態檔 {path}：{e}")
        return None


def build_initial_state(
    symbol_str: str,
    qty: int,
    v1: float,
    v2: float,
    v3: float,
    v4: float,
    industry_code: str,
    market_index_key: str,
    limit_up_price: float,
    limit_down_price: float,
    up_streak_days: int = 0,
    down_streak_days: int = 0,
    strategy_type: str = "",
) -> Dict[str, Any]:
    code, code_with_suf = get_pure_symbol(symbol_str)

    return {
        "symbol_name": symbol_str, # 完整的股票名稱
        "symbol_code": code, # 只有四位數的股票代碼
        "symbol_code_with_suf": code_with_suf, # 包含.tw .two 的股票代碼
        "industry_code": str(industry_code).zfill(2), # 產業別代碼
        "market_index_key": market_index_key, # 產業別盤勢濾網 key，例如 TWSE:24
        "date": today_str_tpe(),  # 當前交易日（Asia/Taipei）
        "open_price": None,
        "high_price": None,
        "low_price": None,
        "close_price": None,
        "avg_price": None,
        "best_bid_price": None,
        "best_ask_price": None,
        "traded": False,
        "in_position": False,
        "strategy_type": strategy_type,
        "side": "",  # 'SHORT' or 'LONG'
        "entry_price": 0,
        "entry_time": None,
        # 已知限制：目前實際只下單一張並假設完整進出；部分成交、殘量、拆單平倉留待後續版本處理。
        "entry_order_pending": False,
        "entry_order_no": "", # 入場委託書號
        "entry_order_qty": qty, # 入場原始委託張數
        "entry_filled_qty": 0, # get_order_results 查得的入場成交張數
        "entry_fully_filled": False, # 入場成交張數是否已達原始委託張數
        "entry_fill_confirmed": False, # 是否已用 get_order_results 確認至少一筆成交
        "entry_price_source": "estimated", # estimated / order_results / dealt_report
        "dealt_report_filled_shares": 0,
        "dealt_report_filled_value": 0.0,
        "exit_order_pending": False,
        "exit_order_no": "",
        "exit_order_qty": 0,
        "exit_filled_qty": 0,
        "exit_fully_filled": False,
        "exit_fill_confirmed": False,
        "exit_price_source": "estimated",
        "exit_price": 0,
        "exit_reason_pending": "",
        "exit_dealt_report_filled_shares": 0,
        "exit_dealt_report_filled_value": 0.0,
        "flat_price": 0, # 強制平倉價格
        "stop_profit_price": limit_down_price, # 追蹤停利點（啟動後才有意義）
        "profit_price": 0, # 下一個獲利目標價
        "profit_tracking_active": False, # 追蹤停利是否已啟動
        "last_price": v4,  # 最近一次的收價（初始化為昨收，開盤後由即時行情覆蓋）
        "pre_last_price": 0, # 前一次的收價
        "pre_last_price_time": None,  # 前一次即時價格的時間；None 代表尚未累積前一筆即時價
        "last_price_time": None,  # 最近一次價格的時間
        "qty": qty,  # 預設下單數量
        "limit_up_price": limit_up_price,  # 漲停
        "limit_down_price": limit_down_price,  # 跌停
        "yesterday_open_price": v1,  # 昨開
        "yesterday_high_price": v2,  # 昨高
        "yesterday_low_price": v3,  # 昨低
        "yesterday_close_price": v4,  # 昨收
        "up_streak_days": up_streak_days,  # 連漲天數
        "down_streak_days": down_streak_days,  # 連跌天數
    }


def normalize_state(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    舊檔／缺欄位的容錯合併。
    """
    base = build_initial_state(
        d.get("symbol_name", "UNKNOWN"),
        d.get("qty", 1),
        d.get("yesterday_open_price", 0.0),
        d.get("yesterday_high_price", 0.0),
        d.get("yesterday_low_price", 0.0),
        d.get("yesterday_close_price", 0.0),
        d.get("industry_code", ""),
        d.get("market_index_key", ""),
        d.get("limit_up_price", 0.0),
        d.get("limit_down_price", 0.0),
        d.get("up_streak_days", 0),
        d.get("down_streak_days", 0),
        d.get("strategy_type", ""),
    )
    base.update(d)

    # 若舊檔沒有 date，補上今天
    if "date" not in base or not base["date"]:
        base["date"] = today_str_tpe()

    return base


def get_current_entry_mode() -> int:
    try:
        entry_mode = int(getattr(stock_data, "entry_mode", ENTRY_MODE_NO_TRADE))
    except (TypeError, ValueError):
        return ENTRY_MODE_NO_TRADE
    if entry_mode in (ENTRY_MODE_NO_TRADE, ENTRY_MODE_LOWER, ENTRY_MODE_AVG):
        return entry_mode
    return ENTRY_MODE_NO_TRADE


def is_entry_mode_enabled(entry_mode: int) -> bool:
    return entry_mode == ENTRY_MODE_LOWER


def is_lower_mode() -> bool:
    return get_current_entry_mode() == ENTRY_MODE_LOWER


def get_optimize_loss_profit_percent(state: Dict[str, Any]) -> tuple[float, float]:
    if is_fall_strategy(state):
        return FALL_STOP_LOSS_PERCENT, FALL_TAKE_PROFIT_PERCENT
    if is_avg_strategy(state):
        return AVG_STOP_LOSS_PERCENT, AVG_TAKE_PROFIT_PERCENT
    if is_lower_mode():
        return OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER
    return OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER


def get_protect_loss_profit_percent(state: Dict[str, Any] | None = None) -> tuple[float, float]:
    if is_fall_strategy(state):
        return FALL_PROTECT_LOSS_PER, FALL_PROTECT_PROFIT_PER
    if is_avg_strategy(state):
        return AVG_PROTECT_LOSS_PER, AVG_PROTECT_PROFIT_PER
    return PROTECT_LOSS_PER, PROTECT_PROFIT_PER


def is_protect_profit_enabled(state: Dict[str, Any] | None = None) -> bool:
    if is_fall_strategy(state):
        return FALL_PROTECT_PROFIT_SWITCH
    if is_avg_strategy(state):
        return AVG_PROTECT_PROFIT_SWITCH
    return PROTECT_PROFIT_SWITCH


def get_force_close_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if is_fall_strategy(state):
        return FALL_FORCE_CLOSE_TIME
    if is_avg_strategy(state):
        return AVG_FORCE_CLOSE_TIME
    if is_lower_mode():
        return FORCE_CLOSE_TIME_LOWER
    return FORCE_CLOSE_TIME_LOWER


def get_entry_order_quantity(state: Dict[str, Any] | None = None, strategy_type: str | None = None) -> int:
    if strategy_type == STRATEGY_FALL or is_fall_strategy(state):
        return FALL_ENTRY_ORDER_QUANTITY
    if strategy_type == STRATEGY_AVG or is_avg_strategy(state):
        return AVG_ENTRY_ORDER_QUANTITY
    if is_lower_mode():
        return ENTRY_ORDER_QUANTITY_LOWER
    return 0


def force_close_time_reached(state: Dict[str, Any] | None = None) -> bool:
    t = now_tpe()
    force_close_time = get_force_close_time(state)
    return (t.hour, t.minute) >= force_close_time


def realtime_quote_time_reached() -> bool:
    t = now_tpe()
    return (t.hour, t.minute) >= REALTIME_QUOTE_START_TIME


def time_reached(target_time: tuple[int, int]) -> bool:
    t = now_tpe()
    return (t.hour, t.minute) >= target_time


# ============ 訊號與狀態邏輯 ============
def check_open_status(state: Dict[str, Any]) -> bool:
    open_pass = False
    if (
        (not state.get("in_position"))
        and (not state.get("traded"))
        and (not state.get("entry_order_pending"))
        and (not state.get("exit_order_pending"))
    ):  # 未持倉、未交易、無委託中單
        open_pass = True
    return open_pass


def check_lower_minute_bar_eligibility(states: Dict[str, Dict[str, Any]], realtime_sdk: EsunMarketdata) -> None:
    return get_lower_strategy().check_lower_minute_bar_eligibility(states, realtime_sdk)


def fetch_lower_minute_count(symbol, realtime_sdk, target_date):
    return get_lower_strategy().fetch_lower_minute_count(symbol, realtime_sdk, target_date)


def schedule_lower_quotes(states, realtime_sdk, *, request_quotes):
    return get_lower_strategy().schedule_lower_quotes(states, realtime_sdk, request_quotes=request_quotes)


def get_entry_check_end_time(state: Dict[str, Any]) -> tuple[int, int]:
    if is_fall_strategy(state):
        return FALL_ENTRY_DEADLINE
    if is_avg_strategy(state):
        return AVG_ENTRY_DEADLINE
    if LOWER_RUNTIME.session_state.get("activation_dt") is not None:
        return get_active_lower_window()[2]
    return get_lower_strategy_windows()[-1][2]


def get_entry_check_start_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if is_fall_strategy(state):
        minutes = FALL_SIGNAL_END_TIME[0] * 60 + FALL_SIGNAL_END_TIME[1] + 1
        return divmod(minutes, 60)
    if is_avg_strategy(state) or get_current_entry_mode() == ENTRY_MODE_AVG:
        return AVG_ENTRY_START_TIME
    if LOWER_RUNTIME.session_state.get("activation_dt") is not None:
        return get_active_lower_window()[1]
    return STRATEGY_START_LOWER_1


def get_latest_entry_check_end_time(states: Dict[str, Dict[str, Any]] | None = None) -> tuple[int, int]:
    if states:
        return max(get_entry_check_end_time(state) for state in states.values())
    return get_entry_check_end_time({})


def entry_lower_mode_price_check(state: Dict[str, Any]) -> bool | str:
    return get_lower_strategy().entry_lower_mode_price_check(state)


def entry_price_check(state: Dict[str, Any]) -> bool | str:
    """
    依 entry_mode 分派進場條件判斷。
    """
    if MARKET_INDEX_REST_FATAL_EVENT.is_set():
        return 'BLOCKED'
    if get_current_entry_mode() == ENTRY_MODE_NO_TRADE:
        now_local = now_tpe()
        if not LOWER_RUNTIME.session_state.get("stopped"):
            return False
        print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} NO_TRADE 模式不追蹤")
        return 'BLOCKED'
    if is_lower_mode():
        return entry_lower_mode_price_check(state)
    return 'BLOCKED'


def try_open_position(state: Dict[str, Any], mysdk):
    # 最後共用送單入口再次檢查，任何策略不得繞過 FALL 專屬權或在途委託。
    if fall_blocks_other_entry(state):
        trade_log("ENTRY_RESERVED_FOR_FALL", **state_symbol_fields(state), action="skip_entry")
        return
    if not check_open_status(state):
        return
    if TRADE_API_DEGRADED_EVENT.is_set():
        return  # 當日停止新風險；保留已送出的委託與持倉對帳。
    if is_fall_strategy(state):
        if (state.get("symbol_code_with_suf") not in FALL_RUNTIME.candidate_symbols
                or not finite_positive(state.get("last_price"))
                or not finite_positive(state.get("yesterday_low_price"))
                or float(state["last_price"]) >= float(state["yesterday_low_price"])):
            return
    elif is_avg_strategy(state):
        if not AVG_RUNTIME.session_state.get("active"):
            return
    elif LOWER_RUNTIME.session_state.get("stopped") or LOWER_RUNTIME.session_state.get("activation_dt") is None:
        return
    now_local = now_tpe()
    if not get_entry_check_start_time(state) <= (now_local.hour, now_local.minute) <= get_entry_check_end_time(state):
        return
    if not state.get("minute_bars_eligible"):
        return
    if not (is_avg_strategy(state) or is_fall_strategy(state)) and state.get("symbol_code_with_suf") not in LOWER_RUNTIME.session_state["baseline_prices"]:
        return
    if MARKET_INDEX_REST_FATAL_EVENT.is_set() and not is_fall_strategy(state):
        state["traded"] = True
        state["exit_reason"] = "market_index_rest_fatal"
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        trade_log(
            "ENTRY_BLOCKED_MARKET_INDEX_REST_FATAL",
            error=True,
            **state_symbol_fields(state),
            action="no_new_entry_existing_positions_continue",
        )
        return

    last_px = state.get("last_price", 0.0) # 現價
    entry_ref_px = state.get("entry_trigger_price", last_px)  # 進場參考價：trigger price
    qty = state.get("qty", 1)

    # 入場安全條件：若入場前當日曾觸及任一漲跌停，立即取消當日交易。
    # 這也避免入場前留下的整日 high/low 在成交後誤觸發停利判斷。
    try:
        high_price = float(state.get("high_price"))
        low_price = float(state.get("low_price"))
        limit_up_price = float(state.get("limit_up_price"))
        limit_down_price = float(state.get("limit_down_price"))
    except (TypeError, ValueError):
        print(f"[{state['symbol_name']}] 無法取得當日高低價，暫不進場")
        return

    if high_price <= 0 or low_price <= 0 or limit_up_price <= 0 or limit_down_price <= 0:
        print(f"[{state['symbol_name']}] 當日高低價或漲跌停價無效，暫不進場")
        return

    if not is_avg_strategy(state) and (high_price >= limit_up_price or low_price <= limit_down_price):
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        state["exit_reason"] = "limit_price_touched_before_entry"
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(
            f"[{state['symbol_name']}] 入場前當日已觸及漲停或跌停，今日不交易 "
            f"high={high_price} limit_up={limit_up_price} "
            f"low={low_price} limit_down={limit_down_price}"
        )
        return

    optimize_loss_per, _optimize_profit_per = get_optimize_loss_profit_percent(state)
    open_stop_loss = entry_ref_px * (optimize_loss_per / 100.0)

    side = state.get("side")
    if side not in (TRADE_SIDE_SHORT, TRADE_SIDE_LONG):
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] side 無效，不執行")
        return

    if side == TRADE_SIDE_SHORT:
        limit_up_price = state.get("limit_up_price", 0)  # 漲停
        if (entry_ref_px + open_stop_loss) >= limit_up_price:
            state["traded"] = True
            state["entry_time"] = now_tpe().isoformat()
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            print(f"[{state['symbol_name']}] SHORT 停損超過漲停，空間太小，不執行")
            return
    else:
        limit_down_price = state.get("limit_down_price", 0)  # 跌停
        if (entry_ref_px - open_stop_loss) <= limit_down_price:
            state["traded"] = True
            state["entry_time"] = now_tpe().isoformat()
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            print(f"[{state['symbol_name']}] LONG 停損低於跌停，空間太小，不執行")
            return

    if check_open_status(state):
        unknown_request_id = ""
        try:
            action, trade = (Action.Sell, Trade.DayTradingSell) if side == TRADE_SIDE_SHORT else (Action.Buy, Trade.Cash)
            place_order_result = type_place_order(
                mysdk, state["symbol_code_with_suf"], action, trade,
                quantity=qty, price_flag=PriceFlag.Market, price=entry_ref_px,
            )
        except OrderOutcomeUnknown as exc:
            place_order_result = ""
            unknown_request_id = exc.request_id

        if place_order_result or unknown_request_id:
            state["entry_order_pending"] = True
            state["entry_pending_since_monotonic"] = time.monotonic()
            state["entry_pending_alerted"] = False
            state["in_position"] = False
            state["entry_order_qty"] = qty
            state["entry_filled_qty"] = 0
            state["entry_fully_filled"] = False
            state["entry_fill_confirmed"] = False
            state["entry_price_source"] = "estimated"
            state["entry_price"] = entry_ref_px
            state["dealt_report_filled_shares"] = 0
            state["dealt_report_filled_value"] = 0.0
            state["entry_request_id"] = unknown_request_id
            state["entry_outcome_unknown"] = bool(unknown_request_id)
            state["entry_unknown_since_monotonic"] = time.monotonic()
            state["entry_unknown_alerted"] = False

            industry_filter_text = "" if is_fall_strategy(state) else format_industry_market_filter_pass_text(state)
            recalc_entry_position_prices(state)
            if unknown_request_id:
                print(f"[{state['symbol_name']}] 入場送單結果未知，保留 pending 並查詢對帳，禁止重送")
            elif side == TRADE_SIDE_SHORT:
                print(f"[{state['symbol_name']}] 作空 已至入場時機，下單成功{industry_filter_text}")
            else:
                print(f"[{state['symbol_name']}] 作多 已至入場時機，下單成功{industry_filter_text}")

            state["profit_tracking_active"] = False  # 追蹤停利尚未啟動，等到首次觸及 profit_price 才開始
            # 最後才公開 ord_no，避免 callback 在估計欄位尚未初始化完成時搶先更新又遭覆寫。
            state["entry_order_no"] = str(place_order_result)
            apply_pending_dealt_reports(state)
            trade_log(
                "ORDER_PENDING",
                **state_symbol_fields(state),
                role="entry",
                ord_no=state.get("entry_order_no"),
                request_id=unknown_request_id,
                qty=state.get("entry_order_qty"),
                price=entry_ref_px,
                reason="entry_signal",
            )
            # 停損／保本／停利由成交確認流程輸出，避免重複印出或顯示估計價位。
        else:  # 下單失敗
            state["traded"] = True

        # 更新狀態
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def try_close_position(state: Dict[str, Any], mysdk):
    if not state.get("in_position"):
        return
    if state.get("exit_order_pending"):
        return
    if state.get("exit_retry_reason"):
        submit_exit_order(state, mysdk, state["exit_retry_reason"])
        return

    pl = reached_profitable_limit_price(state)
    if pl:
        if close_profit_position(state, mysdk):
            if state.get("side") == TRADE_SIDE_LONG:
                print(f"[{state['symbol_name']}] ✅ 作多已達漲停，立即停利平倉")
            else:
                print(f"[{state['symbol_name']}] ✅ 作空已達跌停，立即停利平倉")
        return

    ff = force_close_time_reached(state)
    if ff:
        if endtime_close_position(state, mysdk):
            strategy_label = state.get("strategy_type", "")
            if strategy_label:
                print(f"[{state['symbol_name']}] ✅ {strategy_label} 己達平倉時間")
            else:
                print(f"[{state['symbol_name']}] ✅ 己達平倉時間")
        return

    # 實機特有風控：保本與逐步獲利不納入回測對齊檢查。
    _protect_profit_stop(state)         # 獲利達標後，把停損推進到保住獲利的位置

    sl = reached_stop_to_flat(state)    # 至停損點
    rp = reached_resize_profit(state)   # 達到下一個獲利目標（純判斷，不修改 state）
    sp = reached_stop_to_profit(state)  # 追蹤停利反彈觸發

    if sl:
        if close_flat_position(state, mysdk):
            print(f"[{state['symbol_name']}] ✅ 已至停損價格 {state['flat_price']}")
    elif rp:
        _advance_profit_trail(state)  # 推進追蹤停利（更新 state 並寫檔）
        print(f"[{state['symbol_name']}] ✅ 動態調整停利價格 停利：{state['stop_profit_price']} 下個目標：{state['profit_price']}")
    elif sp:
        if close_profit_position(state, mysdk):
            print(f"[{state['symbol_name']}] ✅ 已至停利價格 {state['stop_profit_price']}")
    else:
        return  # 無須平倉


def mark_exit_order_pending(state: Dict[str, Any], order_no: str, exit_reason: str, request_id: str = "") -> None:
    with ORDER_STATE_LOCK:
        state["exit_order_pending"] = True
        state["exit_pending_since_monotonic"] = time.monotonic()
        state["exit_pending_alerted"] = False
        state["exit_order_qty"] = int(state.get("qty", 0) or 0)
        state["exit_filled_qty"] = 0
        state["exit_fully_filled"] = False
        state["exit_fill_confirmed"] = False
        state["exit_price_source"] = "estimated"
        state["exit_reason_pending"] = exit_reason
        state["exit_order_time"] = now_tpe().isoformat()
        state["exit_dealt_report_filled_shares"] = 0
        state["exit_dealt_report_filled_value"] = 0.0
        state["exit_request_id"] = request_id
        state["exit_outcome_unknown"] = bool(request_id)
        state["exit_unknown_since_monotonic"] = time.monotonic()
        state["exit_unknown_alerted"] = False
        state["exit_retry_reason"] = ""
        # 初始化完成後才公開書號，避免成交 callback 讀到未完成的欄位。
        state["exit_order_no"] = str(order_no)
    apply_pending_dealt_reports(state)
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
    trade_log(
        "ORDER_PENDING",
        **state_symbol_fields(state),
        role="exit",
        ord_no=state.get("exit_order_no"),
        request_id=request_id,
        qty=state.get("exit_order_qty"),
        exit_reason=exit_reason,
    )


def _protect_profit_stop(state: Dict[str, Any]):
    """獲利達標後，將 flat_price 推進到至少保住指定獲利的位置。"""
    if not is_protect_profit_enabled(state):
        return

    side = state.get("side")
    if side not in (TRADE_SIDE_SHORT, TRADE_SIDE_LONG):
        return

    try:
        entry_price = float(state.get("entry_price"))
        current_flat_price = float(state.get("flat_price"))
    except (TypeError, ValueError):
        return

    try:
        px = float(state.get("last_price"))
    except (TypeError, ValueError):
        return

    if entry_price <= 0 or px <= 0:
        return

    protect_loss_per, protect_profit_per = get_protect_loss_profit_percent(state)
    if side == TRADE_SIDE_LONG:
        protect_trigger_price = entry_price * (1 + protect_profit_per / 100.0)
        protected_flat_price = entry_price * (1 + protect_loss_per / 100.0)
        should_update = px >= protect_trigger_price and protected_flat_price > current_flat_price
    else:
        protect_trigger_price = entry_price * (1 - protect_profit_per / 100.0)
        protected_flat_price = entry_price * (1 - protect_loss_per / 100.0)
        should_update = px <= protect_trigger_price and protected_flat_price < current_flat_price

    if should_update:
        state["flat_price"] = protected_flat_price
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] ✅ 獲利保護啟動 停損調整為：{state['flat_price']}")


def reached_stop_to_flat(state: Dict[str, Any]) -> bool:

    if not state.get("entry_fill_confirmed"):
        return False

    side = state.get("side")
    try:
        px = float(state.get("last_price"))
        flat_price = float(state.get("flat_price"))
    except (TypeError, ValueError):
        return False

    if side == TRADE_SIDE_SHORT:
        return px >= flat_price # 只要價格大於停損點就平倉
    if side == TRADE_SIDE_LONG:
        return px <= flat_price
    return False


def reached_stop_to_profit(state: Dict[str, Any]) -> bool:
    if not state.get("profit_tracking_active"):
        return False  # 追蹤停利尚未啟動，不觸發

    side = state.get("side")
    try:
        px = float(state.get("last_price"))
        stop_profit_price = float(state.get("stop_profit_price"))
    except (TypeError, ValueError):
        return False

    if side == TRADE_SIDE_SHORT:
        return px >= stop_profit_price
    if side == TRADE_SIDE_LONG:
        return px <= stop_profit_price
    return False


def reached_resize_profit(state: Dict[str, Any]) -> bool:
    """純判斷：現價是否已達下一個獲利目標點（不修改 state）。"""
    side = state.get("side")

    try:
        px = float(state.get("last_price"))
        profit_price = float(state.get("profit_price"))
    except (TypeError, ValueError):
        return False

    if px <= 0:
        return False

    if side == TRADE_SIDE_SHORT:
        return px <= profit_price
    if side == TRADE_SIDE_LONG:
        return px >= profit_price
    return False


def reached_profitable_limit_price(state: Dict[str, Any]) -> bool:
    """確認入場成交後，作多曾達漲停、作空曾達跌停時立即平倉。"""
    if not state.get("entry_fill_confirmed"):
        return False

    side = state.get("side")

    if side == TRADE_SIDE_LONG:
        try:
            high_price = float(state.get("high_price"))
            limit_up_price = float(state.get("limit_up_price"))
        except (TypeError, ValueError):
            return False
        return high_price > 0 and limit_up_price > 0 and high_price >= limit_up_price

    if side == TRADE_SIDE_SHORT:
        try:
            low_price = float(state.get("low_price"))
            limit_down_price = float(state.get("limit_down_price"))
        except (TypeError, ValueError):
            return False
        return low_price > 0 and limit_down_price > 0 and low_price <= limit_down_price

    return False


def _advance_profit_trail(state: Dict[str, Any]):
    """啟動或推進追蹤停利：更新 profit_price、stop_profit_price，並設定追蹤旗標。"""
    try:
        px = float(state.get("last_price"))
    except (TypeError, ValueError):
        return

    side = state.get("side")
    target_percent = AVG_PROFIT_TARGET_PERCENT if is_avg_strategy(state) else PROFIT_TARGET_PERCENT
    back_percent = AVG_PROFIT_BACK_PERCENT if is_avg_strategy(state) else PROFIT_BACK_PERCENT
    if is_fall_strategy(state):
        target_percent, back_percent = FALL_PROFIT_TARGET_PERCENT, FALL_PROFIT_BACK_PERCENT
    target_step = calculate_profit_trail_step(px, target_percent)
    back_step = calculate_profit_trail_step(px, back_percent)
    if side == TRADE_SIDE_LONG:
        new_profit_price = ceil_price_to_tick_value(px + target_step)
        new_stop_profit_price = floor_price_to_tick_value(px - back_step)
        state["profit_price"] = min(new_profit_price, state.get("limit_up_price", 0))
    elif side == TRADE_SIDE_SHORT:
        new_profit_price = floor_price_to_tick_value(px - target_step)
        new_stop_profit_price = ceil_price_to_tick_value(px + back_step)
        state["profit_price"] = max(new_profit_price, state.get("limit_down_price", 0))
    else:
        return

    state["stop_profit_price"] = new_stop_profit_price
    state["profit_tracking_active"] = True
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def _exit_order_params(side: str, fallback: bool = False) -> Optional[tuple[Any, Any, Any]]:
    if side == TRADE_SIDE_SHORT:
        return Action.Buy, Trade.Cash, PriceFlag.LimitUp if fallback else PriceFlag.Market
    if side == TRADE_SIDE_LONG:
        return Action.Sell, Trade.DayTradingSell, PriceFlag.LimitDown if fallback else PriceFlag.Market
    return None


def submit_exit_order(
    state: Dict[str, Any],
    mysdk,
    exit_reason: str,
    *,
    fallback_success_message: str = "",
    fallback_failure_message: str = "",
) -> bool:
    if state.get("exit_order_pending") or not state.get("in_position") or state.get("traded"):
        return False

    side = state.get("side")
    market_params = _exit_order_params(side)
    if market_params is None:
        trade_log(
            "EXIT_ORDER_SKIPPED",
            error=True,
            **state_symbol_fields(state),
            reason="invalid_side",
            exit_reason=exit_reason,
        )
        return False

    def attempt(params) -> Optional[bool]:
        action_type, trade_type, price_flag = params
        try:
            order_no = type_place_order(
                mysdk, state["symbol_code_with_suf"], action_type, trade_type,
                quantity=state.get("qty", 0), price_flag=price_flag,
                price=state.get("last_price", 0.0),
            )
        except TradeApiBusy:
            state["exit_retry_reason"] = exit_reason
            return None
        except OrderOutcomeUnknown as exc:
            mark_exit_order_pending(state, "", exit_reason, request_id=exc.request_id)
            return True  # 結果未知也必須停止後續補單，留待查詢對帳。
        if not order_no:
            return False
        mark_exit_order_pending(state, order_no, exit_reason)
        return True

    first_attempt = attempt(market_params)
    if first_attempt is not False:
        return bool(first_attempt)

    fallback_params = _exit_order_params(side, fallback=True)
    if fallback_params is None:
        return False

    fallback_attempt = attempt(fallback_params)
    if fallback_attempt is None:
        return False
    if not fallback_attempt:
        state["exit_retry_reason"] = exit_reason
        if fallback_failure_message:
            print(fallback_failure_message)
        return False

    if fallback_success_message and not state.get("exit_outcome_unknown"):
        print(fallback_success_message)
    return True


def close_profit_position(state: Dict[str, Any], mysdk, exit_reason: str = "profit") -> bool:
    side = state.get("side")
    return submit_exit_order(
        state,
        mysdk,
        exit_reason,
        fallback_success_message=f'[{state.get("symbol_name")}] {side} 停利市價平倉失敗，已改掛預約平倉單',
        fallback_failure_message=f'[{state.get("symbol_name")}] {side} 停利市價平倉失敗且預約掛單失敗，須手動下單平倉',
    )


def close_flat_position(state: Dict[str, Any], mysdk, exit_reason: str = "stop") -> bool:
    side = state.get("side")
    return submit_exit_order(
        state,
        mysdk,
        exit_reason,
        fallback_success_message=f'[{state.get("symbol_name")}] {side} 市價平倉失敗，已改掛預約平倉單',
        fallback_failure_message=f'[{state.get("symbol_name")}] {side} 市價平倉失敗且預約掛單失敗，須手動下單平倉',
    )


def endtime_close_position(state: Dict[str, Any], mysdk, exit_reason: str = "force_close") -> bool:
    if state.get("traded") == True or state.get("in_position") == False:  # 已完成交易，不用平倉
        print(f'[{state.get("symbol_name")}] 已完成交易，不須強制平倉')
        state["traded"] = True
        state["in_position"] = False  # 確保持倉狀態為 False
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        return False  # 這裡直接返回，是因為若無建倉就下平倉單，反而變成另起一張訂單

    return submit_exit_order(
        state,
        mysdk,
        exit_reason,
        fallback_failure_message=f'[{state.get("symbol_name")}] 已至收盤時間，漲跌停平倉交易失敗，須手動下單平倉',
    )


# ============ 主監控流程 ============
def load_or_init_state(
    symbol: str,
    qty: int,
    v1: float,
    v2: float,
    v3: float,
    v4: float,
    industry_code: str,
    market_index_key: str,
    limit_up_price: float,
    limit_down_price: float,
    up_streak_days: int,
    down_streak_days: int,
    strategy_type: str = "",
    force_rebuild: bool = False,
) -> Dict[str, Any]:
    """
    若檔案存在就讀檔；若 date != 今天（Asia/Taipei），視為舊檔，直接刪除並用當日 v1/v2 重建。
    若不存在則用當日 v1/v2 建檔。
    """
    _, code_with_suf = get_pure_symbol(symbol)

    path = state_path(code_with_suf)
    existing = load_json_or_none(path)
    today_str = today_str_tpe()

    if existing and force_rebuild:
        st = build_initial_state(
            symbol,
            qty,
            v1,
            v2,
            v3,
            v4,
            industry_code,
            market_index_key,
            limit_up_price,
            limit_down_price,
            up_streak_days,
            down_streak_days,
            strategy_type,
        )
        atomic_write_json(path, st)
        return st

    if existing:
        st = normalize_state(existing)
        file_date = st.get("date")
        if file_date != today_str:
            # 舊日檔案：刪除並重建
            try:
                os.remove(path)
                print(f"[{symbol}] 發現舊日狀態檔（{file_date}），已刪除並重建。")
            except OSError:
                print(f"[{symbol}] 刪除舊檔失敗，將直接覆蓋重建。")
            st = build_initial_state(
                symbol,
                qty,
                v1,
                v2,
                v3,
                v4,
                industry_code,
                market_index_key,
                limit_up_price,
                limit_down_price,
                up_streak_days,
                down_streak_days,
                strategy_type,
            )
            atomic_write_json(path, st)
        st["industry_code"] = str(industry_code).zfill(2)
        st["market_index_key"] = market_index_key
        st["strategy_type"] = strategy_type
        return st
    else:
        st = build_initial_state(
            symbol,
            qty,
            v1,
            v2,
            v3,
            v4,
            industry_code,
            market_index_key,
            limit_up_price,
            limit_down_price,
            up_streak_days,
            down_streak_days,
            strategy_type,
        )
        atomic_write_json(path, st)
        return st


def get_pure_symbol(symbolStr: str) -> Tuple[str, str]:
    symbol_with_suf = symbolStr.split(":")[1]
    symbol = symbol_with_suf.split(".")[0]
    return symbol, symbol_with_suf


def all_traded(states: Dict[str, Dict[str, Any]]) -> bool:
    return bool(states) and all(s.get("traded", False) for s in states.values())


def has_unfinished_lower_states(states: Dict[str, Dict[str, Any]]) -> bool:
    return get_lower_strategy().has_unfinished_lower_states(states)


def mark_unentered_lower_states_blocked(states: Dict[str, Dict[str, Any]], reason: str) -> None:
    return get_lower_strategy().mark_unentered_lower_states_blocked(states, reason)


def mark_unentered_states_blocked_after_market_index_rest_fatal(
    states: Dict[str, Dict[str, Any]],
) -> int:
    blocked_count = 0
    for state in states.values():
        if (
            state.get("traded")
            or state.get("in_position")
            or state.get("entry_order_pending")
            or state.get("exit_order_pending")
        ):
            continue
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        state["exit_reason"] = "market_index_rest_fatal"
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        blocked_count += 1
    return blocked_count


def initialize_states(
    stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]],
    realtime_sdk: EsunMarketdata,
    strategy_type: str = "",
    force_rebuild_state: bool = False,
) -> Dict[str, Dict[str, Any]]:
    # 讀檔或初始化（程式啟動時先完成）
    states: Dict[str, Dict[str, Any]] = {}
    filtered_stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]] = []
    for symbolStr, qty, v1, v2, v3, v4, industry_code, volatility_value, streak_tuple in stocks:
        if MARKET_REVERSAL_STOP_EVENT.is_set() and not strategy_type:
            print("[MODE] 市場模式已封鎖，停止初始化一般策略個股資料")
            break

        _, code_with_suf = get_pure_symbol(symbolStr)
        normalized_industry_code = str(industry_code).zfill(2)
        if strategy_type == STRATEGY_AVG:
            market_index_key, market_index_config = "", {}
        else:
            market_index_key, market_index_config = get_industry_index_config(code_with_suf, normalized_industry_code)
        if not market_index_config.get("symbol") and strategy_type != STRATEGY_AVG:
            print(f"[{symbolStr}] ⚠️ 無對應之產業別指數代碼，排除")
            continue

        up_streak_days, down_streak_days = streak_tuple

        try:
            (
                limit_up_price,
                limit_down_price,
                symbol_can_day_trade,
                symbol_is_disposition,
            ) = get_up_down_price(code_with_suf, realtime_sdk)
        except Exception as e:
            trade_log(
                "INIT_API_ERROR",
                error=True,
                symbol=code_with_suf,
                raw_symbol=symbolStr,
                api="intraday.ticker",
                reason="limit_price_or_day_trade_check_failed",
                error_msg=repr(e),
            )
            continue

        if not symbol_can_day_trade:
            print(f"[{symbolStr}] ⚠️ 無法買賣現沖，排除")
            continue

        if symbol_is_disposition is None:
            print(f"[{symbolStr}] ⚠️ API 未回傳處置股狀態，為避免誤下單，排除")
            continue

        if symbol_is_disposition:
            print(f"[{symbolStr}] ⚠️ 處置股，排除")
            continue

        quantity = get_entry_order_quantity(strategy_type=strategy_type)

        st = load_or_init_state(
            symbolStr,
            quantity,
            v1,
            v2,
            v3,
            v4,
            normalized_industry_code,
            market_index_key,
            limit_up_price,
            limit_down_price,
            up_streak_days,
            down_streak_days,
            strategy_type,
            force_rebuild=force_rebuild_state,
        )
        st["industry_name"] = market_index_config.get("industry_name")
        st["market_index_symbol"] = market_index_config.get("symbol")
        st["market_index_name"] = market_index_config.get("name")
        states[code_with_suf] = st
        filtered_stocks.append((symbolStr, qty, v1, v2, v3, v4, normalized_industry_code, volatility_value, streak_tuple))

        st["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)

    stocks[:] = filtered_stocks

    return states


def monitor(states: Dict[str, Dict[str, Any]], mysdk: SDK, realtime_sdk: EsunMarketdata):
    global ORDER_RESULTS_PENDING_FUTURE
    update_status = False
    realtime_quote_start_announced = False
    lower_strategy_early_breakout_deadline_announced = False
    next_decision_index = 0
    entry_check_start_announced = False
    entry_check_end_announced = False
    entry_mode_decided = False
    market_index_rest_fatal_announced = False
    last_order_results_update_monotonic = 0.0
    lower_percent_exit_summary = None
    lower_exit_future = None
    order_query_started_monotonic = 0.0
    order_query_stall_announced = False
    avg_initialization_future = None
    early_summary_future = None
    lower_decision_future = None
    lower_decision_metadata = None
    lower_decision_wait_started = None
    while True:
        process_fall_round(states, realtime_sdk, mysdk, request_quotes=update_status)
        order_states = all_order_states(states)
        consume_late_order_responses(order_states)
        check_trade_api_watchdog(order_states)
        maintain_pending_orders(order_states)
        if MARKET_INDEX_REST_FATAL_EVENT.is_set():
            blocked_count = mark_unentered_states_blocked_after_market_index_rest_fatal(states)
            if not market_index_rest_fatal_announced:
                market_index_rest_fatal_announced = True
                active_positions = sum(1 for state in states.values() if state.get("in_position"))
                pending_orders = sum(
                    1 for state in states.values()
                    if state.get("entry_order_pending") or state.get("exit_order_pending")
                )
                trade_log(
                    "MARKET_INDEX_REST_FATAL_TRADING_POLICY",
                    error=True,
                    blocked_new_entries=blocked_count,
                    active_positions=active_positions,
                    pending_orders=pending_orders,
                    action="block_all_new_entries_continue_existing_position_and_order_monitoring",
                )

        if MARKET_REVERSAL_STOP_EVENT.is_set():
            if get_current_entry_mode() == ENTRY_MODE_AVG:
                print("[MODE] AVG 已切換，忽略 LOWER 專屬市場模式停止事件")
            else:
                mark_unentered_lower_states_blocked(states, "market_mode_blocked")
                LOWER_RUNTIME.session_state["stopped"] = True
                MARKET_INDEX_REST_STOP_EVENT.set()
            MARKET_REVERSAL_STOP_EVENT.clear()
            if get_current_entry_mode() != ENTRY_MODE_AVG:
                print("[MODE] LOWER 已封鎖，等待第三組開始時間再決定 AVG；不結束整個監控")

        now_local = now_tpe()
        log_market_index_rest_watchdog_once(now_local)
        if (
            not realtime_quote_start_announced
            and (now_local.hour, now_local.minute) >= REALTIME_QUOTE_START_TIME
        ):
            print(
                f"⏰ 個股即時行情取價開始時間！"
                f"目前時間：{now_local.strftime('%H:%M:%S')}"
            )
            realtime_quote_start_announced = True

        lower_strategy_early_breakout_deadline_hm = (
            LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
            + LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
        )
        now_hm = now_local.hour * 60 + now_local.minute
        if (
            not lower_strategy_early_breakout_deadline_announced
            and early_summary_future is None
            and now_hm > lower_strategy_early_breakout_deadline_hm
        ):
            early_summary_future = MARKET_SCAN_EXECUTOR.submit(
                fetch_lower_early_summary, {symbol: dict(st) for symbol, st in states.items()}, realtime_sdk,
            )
        if early_summary_future is not None and early_summary_future.done():
            try:
                market_snapshots, early_summary = early_summary_future.result()
                print_lower_strategy_early_breakout_deadline_summary(market_snapshots)
                print(f'check:[下降={early_summary["decline_count"]}  母數={early_summary["candidate_count"]}  '
                      f'{early_summary["decline_percent"]:.2f}%]')
            except Exception as exc:
                trade_log('LOWER_EARLY_SUMMARY_ERROR', error=True, error_msg=repr(exc))
            early_summary_future = None
            lower_strategy_early_breakout_deadline_announced = True

        if get_current_entry_mode() != ENTRY_MODE_AVG:
            check_lower_minute_bar_eligibility(states, realtime_sdk)
        decision_windows = get_lower_strategy_windows()
        decision_processed_this_round = False
        if lower_decision_future is not None:
            if LOWER_RUNTIME.session_state.get('stopped') or MARKET_INDEX_REST_FATAL_EVENT.is_set() or entry_mode_decided:
                lower_decision_future.cancel()
                lower_decision_future = None
                lower_decision_wait_started = None
            else:
                window_number, decision_dt = lower_decision_metadata
                try:
                    inputs, lower_decision_wait_started = poll_lower_decision_inputs(
                        lower_decision_future, window_number, lower_decision_wait_started)
                except Exception as exc:
                    trade_log('LOWER_DECISION_SCAN_ERROR', error=True, window_number=window_number, error_msg=repr(exc))
                    LOWER_RUNTIME.session_state['stopped'] = True
                    mark_unentered_lower_states_blocked(states, 'lower_decision_scan_failed')
                    MARKET_INDEX_REST_STOP_EVENT.set()
                    lower_decision_future = None
                    inputs = None
                if inputs is not None:
                    summary, market_snapshots = inputs
                    lower_decision_future = None
                    decision_processed_this_round = True
                    entry_mode, gate_results = decide_entry_mode_by_market_gate(
                        states, market_snapshots, realtime_sdk, window_number, lower_decision_summary=summary)
                    decision_status = gate_results[0]['decision_status']
                    if entry_mode == ENTRY_MODE_LOWER:
                        activate_lower_session(window_number, decision_dt, summary)
                        apply_entry_mode_to_states(states, entry_mode)
                        entry_mode_decided = True
                    print_entry_mode_decision(entry_mode, gate_results, states)
                    if decision_status == 'STOP':
                        LOWER_RUNTIME.session_state['stopped'] = True
                        mark_unentered_lower_states_blocked(states, 'market_mode_blocked')
                        MARKET_INDEX_REST_STOP_EVENT.set()
                    now_local = now_tpe()
        if (
            not LOWER_RUNTIME.session_state.get("stopped")
            and not MARKET_INDEX_REST_FATAL_EVENT.is_set()
            and not entry_mode_decided
            and not decision_processed_this_round
            and lower_decision_future is None
            and lower_strategy_early_breakout_deadline_announced
            and lower_minute_checks_complete(states)
            and next_decision_index < len(decision_windows)
            and (now_tpe().hour, now_tpe().minute) >= decision_windows[next_decision_index][0]
        ):
            decision_time = decision_windows[next_decision_index][0]
            decision_dt = now_tpe().replace(hour=decision_time[0], minute=decision_time[1], second=0, microsecond=0)
            window_number = next_decision_index + 1
            next_decision_index += 1
            lower_decision_metadata = window_number, decision_dt
            lower_decision_wait_started = None
            lower_decision_future = MARKET_SCAN_EXECUTOR.submit(
                summarize_lower_strategy_decision_candidates,
                {symbol: dict(st) for symbol, st in states.items()}, realtime_sdk,
            )
        if (not entry_mode_decided and lower_decision_future is None
                and (LOWER_RUNTIME.session_state.get('stopped') or MARKET_INDEX_REST_FATAL_EVENT.is_set()
                     or (lower_strategy_early_breakout_deadline_announced and lower_minute_checks_complete(states)))):
            final_mode = resolve_session_entry_mode(now_tpe())
            if final_mode == ENTRY_MODE_AVG:
                # 資格初始化含大量 REST 查詢，放背景執行以維持 FALL 報價與持倉風控。
                LOWER_RUNTIME.session_state["stopped"] = True
                MARKET_INDEX_REST_STOP_EVENT.set()
                avg_initialization_future = MARKET_SCAN_EXECUTOR.submit(prepare_avg_states, realtime_sdk)
                entry_mode_decided = True
            elif final_mode == ENTRY_MODE_NO_TRADE:
                persist_entry_mode_to_stock_data(ENTRY_MODE_NO_TRADE)
                mark_unentered_lower_states_blocked(states, "market_index_rest_fatal")
                entry_mode_decided = True
                print("[MODE] LOWER/AVG 停止新入場，FALL 與既有委託繼續監控")
        if avg_initialization_future is not None and avg_initialization_future.done():
            completed = avg_initialization_future
            avg_initialization_future = None
            try:
                avg_states = completed.result()
            except Exception as exc:
                persist_entry_mode_to_stock_data(ENTRY_MODE_NO_TRADE)
                mark_unentered_lower_states_blocked(states, "avg_initialization_failed")
                trade_log("AVG_INIT_ERROR", error=True, error_msg=repr(exc), action="fall_continues")
            else:
                persist_entry_mode_to_stock_data(ENTRY_MODE_AVG)
                states.clear()
                states.update(avg_states)
                refresh_active_order_states(states)
                order_states = all_order_states(states)
                started = now_tpe()
                AVG_RUNTIME.session_state.update(active=True, candidate_count=len(states),
                    last_scan_minute_key=(started.strftime("%Y-%m-%d %H:%M")
                                         if started.second >= AVG_CANDLE_POLL_SECOND else None))
                trade_log("AVG_MODE_SELECTED", source=AVG_STOCK_DATA_SOURCE,
                          candidate_count=len(states), action="fall_preserved_avg_started")
        entry_check_start_time = get_entry_check_start_time()
        current_entry_mode = get_current_entry_mode()
        if (
            entry_mode_decided
            and current_entry_mode != ENTRY_MODE_NO_TRADE
            and is_entry_mode_enabled(current_entry_mode)
            and has_unfinished_lower_states(states)
            and not entry_check_start_announced
            and (now_local.hour, now_local.minute) >= entry_check_start_time
        ):
            print(
                f"⏰ {get_entry_mode_text()} 進場檢核開始時間！"
                f"目前時間：{now_local.strftime('%H:%M:%S')}"
            )
            entry_check_start_announced = True

        latest_entry_check_end_time = get_latest_entry_check_end_time(states)
        if (not entry_check_end_announced) and ((now_local.hour, now_local.minute, now_local.second) >= (latest_entry_check_end_time[0], latest_entry_check_end_time[1], 0)):
            print(f"⏰ 進場檢核截止時間！目前時間：{now_local.strftime('%H:%M:%S')}")
            entry_check_end_announced = True

        pending_states = [st for st in states.values() if not st.get("traded")]
        all_force_close_time_reached = bool(pending_states) and all(
            force_close_time_reached(st)
            for st in pending_states
        )
        round_should_update_realtime = update_status
        if round_should_update_realtime and not realtime_quote_time_reached():
            round_should_update_realtime = False

        now_monotonic = time.monotonic()
        if (ORDER_RESULTS_PENDING_FUTURE is not None
                and not ORDER_RESULTS_PENDING_FUTURE.done()
                and now_monotonic - order_query_started_monotonic >= ORDER_UNKNOWN_ALERT_SECONDS
                and not order_query_stall_announced):
            order_query_stall_announced = True
            trade_log("ORDER_RESULTS_QUERY_STALLED", error=True,
                      action="manual_check_sdk_required_quotes_continue_order_submission_deferred")
        if ORDER_RESULTS_PENDING_FUTURE is not None and ORDER_RESULTS_PENDING_FUTURE.done():
            completed_query = ORDER_RESULTS_PENDING_FUTURE
            ORDER_RESULTS_PENDING_FUTURE = None
            try:
                results = completed_query.result()
            except Exception as exc:
                # 沿用原有查詢失敗／AGR0005 冷卻處理，不在主執行緒重查 SDK。
                update_entry_fills_from_order_results(order_states, mysdk, query_error=exc)
            else:
                update_entry_fills_from_order_results(order_states, mysdk, results=results)
        order_results_start_reached = (
            (now_local.hour, now_local.minute) >= ORDER_RESULTS_QUERY_START_TIME
        )
        order_results_cooldown_finished = (
            now_monotonic >= ORDER_RESULTS_COOLDOWN_UNTIL_MONOTONIC
        )
        if (
            order_results_start_reached
            and order_results_cooldown_finished
            and now_monotonic - last_order_results_update_monotonic >= ORDER_RESULTS_UPDATE_SECONDS
            and ORDER_RESULTS_PENDING_FUTURE is None
            and any(state_needs_entry_fill_update(st) or state_needs_exit_fill_update(st) for st in order_states.values())
        ):
            try:
                ORDER_RESULTS_PENDING_FUTURE = start_order_results_query(mysdk)
            except TradeApiBusy:
                pass  # 送單／撤單仍在途；不建立排隊查詢來佔用下一次平倉機會。
            else:
                order_query_started_monotonic = now_monotonic
                order_query_stall_announced = False
                last_order_results_update_monotonic = now_monotonic

        if lower_percent_exit_summary is not None:
            handle_lower_percent_exit(states, mysdk, lower_percent_exit_summary)

        # 已全數完成交易則收工
        if (entry_mode_decided and avg_initialization_future is None
                and not fall_work_pending() and (not states or all_traded(states))
                and not any(st.get("in_position") or st.get("entry_order_pending") or st.get("exit_order_pending")
                            for st in order_states.values())):
            if all_force_close_time_reached:
                print("=== 已達停止時間，今日所有標的已收工 ===")
            else:
                print("=== 已全部交易，今日所有標的已收工 ===")
            break

        if (
            current_entry_mode != ENTRY_MODE_AVG
            and LOWER_RUNTIME.session_state.get("activation_dt") is not None
            and lower_percent_exit_summary is None
        ):
            if lower_exit_future is not None and lower_exit_future.done():
                try:
                    lower_percent_exit_summary = lower_exit_future.result()
                except Exception as exc:
                    trade_log("LOWER_PERCENT_EXIT_CHECK_ERROR", error=True, error_msg=repr(exc))
                lower_exit_future = None
                if lower_percent_exit_summary is not None:
                    handle_lower_percent_exit(states, mysdk, lower_percent_exit_summary)
            if lower_exit_future is None and lower_percent_exit_summary is None:
                check_dt = get_due_lower_exit_check(now_tpe())
                if check_dt is not None:
                    lower_exit_future = MARKET_SCAN_EXECUTOR.submit(
                        check_lower_percent_exit, states, realtime_sdk, check_dt=check_dt,
                    )

        # 每分鐘四輪排程行情；主迴圈只取已完成結果，不等待網路。
        if round_should_update_realtime and current_entry_mode != ENTRY_MODE_AVG:
            update_required_industry_indices(states, realtime_sdk)

        if current_entry_mode == ENTRY_MODE_AVG:
            process_avg_candle_round(states, realtime_sdk, mysdk)
            # 先處理分K及入場，再排程持倉報價；主迴圈不等 SDK 網路回應。
            schedule_avg_position_quotes(
                states, realtime_sdk, request_quotes=round_should_update_realtime,
            )
        else:
            schedule_lower_quotes(states, realtime_sdk, request_quotes=round_should_update_realtime)

        # 輪詢
        for st in states.values():
            if st.get("traded"):
                # print(f"[{st['symbol_name']}] | 已完成交易，跳過")
                continue
            if is_avg_strategy(st) and not st.get("in_position"):
                # AVG 未成交持倉的入場訊號由分K輪詢處理；持倉則走下方即時報價風控。
                continue

            try:
                need_persist_at_end = False
                if lower_percent_exit_summary is not None:
                    continue

                if (force_close_time_reached(st) or st.get("exit_retry_reason")) and st.get("in_position") and not st.get("traded"):
                    try_close_position(st, mysdk)
                    continue

                quote_future = (
                    AVG_RUNTIME.position_quote_futures.get(st["symbol_code_with_suf"])
                    if is_avg_strategy(st) else
                    LOWER_RUNTIME.quote_futures.get(st["symbol_code_with_suf"], (None, None))[0]
                )
                quote_ready = quote_future is not None and quote_future.done()
                if quote_ready:
                    try:
                        # print("更新股價")
                        if is_avg_strategy(st):
                            AVG_RUNTIME.position_quote_futures.pop(st["symbol_code_with_suf"], None)
                            quote = quote_future.result()
                        else:
                            LOWER_RUNTIME.quote_futures.pop(st['symbol_code_with_suf'], None)
                            quote, requested_at = quote_future.result()
                            if time.monotonic() - requested_at > QUOTE_ROUND_INTERVAL_SECONDS:
                                continue
                        px, open_px, high_price, low_price, close_price, avg_price, best_bid_price, best_ask_price = quote
                    except Exception as e:
                        trade_log(
                            "QUOTE_ERROR",
                            error=True,
                            **state_symbol_fields(st),
                            api="intraday.quote",
                            reason="get_realtime_price_failed",
                            error_msg=repr(e),
                        )
                        continue

                    if (px is None) or (open_px is None):
                        trade_log(
                            "QUOTE_ERROR",
                            error=True,
                            **state_symbol_fields(st),
                            api="intraday.quote",
                            reason="missing_last_or_open_price",
                            last_price=px,
                            open_price=open_px,
                        )
                        continue

                    need_persist_at_end = True
                    st["open_price"] = open_px  # 開盤價(本日開盤價)
                    st["high_price"] = high_price # 最高價(目前為止的最高價)
                    st["low_price"] = low_price # 最低價(目前為止的最低價)
                    st["close_price"] = close_price # 收盤價(最近成交價)
                    st["avg_price"] = avg_price # 均價(即時API avgPrice)
                    st["best_bid_price"] = best_bid_price # 買一價
                    st["best_ask_price"] = best_ask_price # 賣一價
                    st["pre_last_price"] = st.get("last_price", 0)  # 本次更新前的前一筆即時價
                    st["pre_last_price_time"] = st.get("last_price_time")
                    st["last_price"] = px  # 最新價格
                    st["last_price_time"] = now_tpe().isoformat()
                    entry_check_end_time = get_entry_check_end_time(st)
                    if (
                        ((now_local.hour, now_local.minute) > entry_check_end_time)
                        and (not st.get("in_position"))
                        and (not st.get("entry_order_pending"))
                        and (not st.get("exit_order_pending"))
                        ):
                        st["traded"] = True
                        st["entry_time"] = now_tpe().isoformat()
                        atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)
                        reason = "檢核時間結束仍未進場，不追蹤"
                        print(f"[{st['symbol_name']}] {now_tpe().strftime('%H:%M:%S')} {reason}")
                        continue

                    if (st.get("in_position")) and (not st.get("traded")):  # 已持倉嘗試平倉
                        try_close_position(st, mysdk)
                    elif st.get("entry_order_pending") or st.get("exit_order_pending"):
                        continue
                    else:
                        entry_result = entry_price_check(st)
                        if entry_result is True:
                            trigger_price = st.get("entry_trigger_price")
                            if trigger_price is None:
                                st["traded"] = True
                                st["entry_time"] = now_tpe().isoformat()
                                atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)
                                print(f"[{st['symbol_name']}] {now_tpe().strftime('%H:%M:%S')} 無法取得進場觸發價，不追蹤")
                                continue
                            if not st.get("side"):
                                st["side"] = TRADE_SIDE_SHORT
                            st["entry_trigger_price"] = trigger_price
                            try_open_position(st, mysdk)
                        elif entry_result == 'BLOCKED':
                            st["traded"] = True
                            st["entry_time"] = now_tpe().isoformat()
                            atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)

                if need_persist_at_end:
                    atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)
            except Exception as e:
                print(f"[{st.get('symbol_name', 'UNKNOWN')}] ⚠️ 單檔監控處理失敗，略過本輪：{e}")
                continue

        # print("========= 下一輪監控等待中... =========")
        now_local = now_tpe()
        if (now_local.hour, now_local.minute) >= FORCE_EXIT_TIME:
            print(f"當前時間 {now_local.strftime('%Y-%m-%d %H:%M:%S')} >= {FORCE_EXIT_TIME[0]:02d}:{FORCE_EXIT_TIME[1]:02d}，程式結束。")
            sys.exit(2 if report_unsettled_trades(states, "force_exit_time") else 0)

        now = now_tpe()
        nxt = ceil_next_interval(now, 5) # 秒數在5的倍數輪巡
        sleep_sec = max(0.2, (nxt - now).total_seconds())
        if MARKET_REVERSAL_STOP_EVENT.wait(timeout=sleep_sec):
            continue
        # print(f"========= 開始下一輪 {nxt.strftime('%Y-%m-%d %H:%M:%S')} =========")

        update_status = False # 依指定秒點更新股票，太頻繁更新會有API call次數過多的問題
        if (nxt.second == 5) or (nxt.second == 20) or (nxt.second == 35) or (nxt.second == 50):
            update_status = True


# ============ 呼叫 ============
def main(*, runtime_mode: str | None = None):
    """本機與 Docker 共用入口；此程式只支援每日啟動一次。"""
    global stock_data, selected_stocks, market_previous_close_indices, total_stocks
    global MARKETDATA_STOCK_REST_CLIENT
    environment = RuntimeEnvironment(BASE_DIR, mode=runtime_mode or os.getenv("QUANT_RUNTIME_MODE", "local"))
    #print(10*get_tick_size(74.3))

    base_dir = os.path.dirname(__file__)
    execute_result_path = os.path.join(base_dir, "execute_strategy_result.txt")
    capture_buffer = io.StringIO()
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = TeeStream(original_stdout, capture_buffer)
    sys.stderr = TeeStream(original_stderr, capture_buffer)
    states = {}
    market_index_rest_thread = None
    fall_quote_thread = None
    realtime_sdk = None
    sdk = None

    try:
        stock_data = environment.prepare_stock_data()
        selected_stocks = stock_data.selected_stocks
        market_previous_close_indices = stock_data.market_previous_close_indices
        total_stocks = stock_data.total_stocks
        stock_data.entry_mode = ENTRY_MODE_NO_TRADE
        validate_market_reversal_time_config()
        validate_fall_config()
        wait_until_main_start_time()
        TRADE_API_DEGRADED_EVENT.clear()
        MARKET_REVERSAL_STOP_EVENT.clear()
        MARKET_INDEX_REST_STOP_EVENT.clear()
        MARKET_INDEX_REST_FATAL_EVENT.clear()
        LOWER_RUNTIME.session_state.update({
            "activation_dt": None, "window_number": None, "baseline_prices": {},
            "next_exit_check_dt": None, "stopped": False,
        })
        AVG_RUNTIME.session_state.update({"active": False, "last_scan_minute_key": None, "candidate_count": 0})
        AVG_RUNTIME.next_request_monotonic = 0.0
        LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_CHECK_ANNOUNCED_EVENT.clear()
        with MARKET_INDEX_ROUND_CONDITION:
            MARKET_INDEX_ROUND_STATE["completed_round"] = 0
            MARKET_INDEX_ROUND_STATE["completed_at"] = None
            MARKET_INDEX_ROUND_STATE["snapshots"] = {}
        clear_state_dir()
        persist_entry_mode_to_stock_data(ENTRY_MODE_NO_TRADE)

        # 登入以操作API
        config = ConfigParser()
        config.read(os.path.join(BASE_DIR, "config.ini"))

        realtime_sdk = EsunMarketdata(config)
        try:
            environment.login_sdk(realtime_sdk)
            trade_log("LOGIN_OK", api="marketdata")
        except Exception as exc:
            trade_log("LOGIN_ERROR", error=True, api="marketdata", error_msg=repr(exc))
            raise
        MARKETDATA_STOCK_REST_CLIENT = RateLimitedStockRest(realtime_sdk.rest_client.stock)
        market_index_rest_thread = start_market_index_rest_poller(realtime_sdk)

        sdk = SDK(config)
        try:
            environment.login_sdk(sdk)
            trade_log("LOGIN_OK", api="trade")
        except Exception as exc:
            trade_log("LOGIN_ERROR", error=True, api="trade", error_msg=repr(exc))
            raise

        # stock_data.py 的 selected_stocks 為 LOWER 候選名單。
        initialize_fall_session()
        candidate_symbols = list(selected_stocks)  # initialize_states 會原地過濾，勿修改股票池來源
        states = initialize_states(candidate_symbols, realtime_sdk)
        refresh_active_order_states(states)
        fall_quote_thread = start_fall_quote_poller(realtime_sdk)
        print_rest_quote_rate_limit_estimate(states)

        trade_report_thread = start_trade_report_stream(sdk)
        time.sleep(1.0)  # 先讓交易回報 WebSocket 建立連線
        if trade_report_thread.is_alive():
            trade_log("TRADE_WS_THREAD_OK", thread=trade_report_thread.name)
        else:
            trade_log(
                "TRADE_WS_THREAD_ERROR",
                error=True,
                thread=trade_report_thread.name,
                reason="thread_not_alive_after_start",
            )
        # 對齊到下一個 5 秒邊界，避免第一輪跨分鐘造成額外更新
        align_now = now_tpe()
        align_next = ceil_next_interval(align_now, 5)
        align_sleep_sec = max(0.2, (align_next - align_now).total_seconds())
        time.sleep(align_sleep_sec)

        # 開始正式作業
        monitor(states, sdk, realtime_sdk)
    finally:
        try:
            report_unsettled_trades(states, "shutdown")
        except Exception as exc:
            trade_log("SHUTDOWN_AUDIT_FAILED", error=True, error_msg=repr(exc))
        MARKETDATA_STOP_EVENT.set()
        FALL_RUNTIME.quote_poller_stop_event.set()
        MARKET_INDEX_REST_STOP_EVENT.set()
        for executor in (AVG_CANDLE_EXECUTOR, AVG_POSITION_QUOTE_EXECUTOR, MARKET_SCAN_EXECUTOR,
                         FALL_SCREEN_EXECUTOR, FALL_QUOTE_EXECUTOR, LOWER_DATA_EXECUTOR, LOWER_QUOTE_EXECUTOR,
                         INDEX_DATA_EXECUTOR):
            executor.shutdown(wait=False, cancel_futures=True)
        if market_index_rest_thread is not None:
            market_index_rest_thread.join(timeout=2.0)
        if fall_quote_thread is not None:
            fall_quote_thread.join(timeout=2.0)
        environment.safe_logout_sdk("Esun Trade SDK", sdk)
        environment.safe_logout_sdk("EsunMarketdata", realtime_sdk)
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        try:
            with open(execute_result_path, "w", encoding="utf-8") as f:
                f.write(capture_buffer.getvalue())
        except Exception as e:
            print(f"[WARN] 無法輸出 execute_strategy_result.txt：{e}", file=original_stderr)


if __name__ == "__main__":
    main()
