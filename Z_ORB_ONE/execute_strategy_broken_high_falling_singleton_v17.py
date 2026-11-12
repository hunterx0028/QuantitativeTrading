# -*- coding: utf-8 -*-
import os
import json
import copy
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
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
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
from marketdata_transport import install_intraday_timeout

# Runtime data is loaded only after local/container preparation.
stock_data = None
selected_stocks = []
market_previous_close_indices = {}
total_stocks = []
vol_history_by_symbol = {}
vol_history_reference_date = None
from strategies.avg import AvgConfig, AvgRuntime, AvgServices, AvgStrategy
from strategies.candidate_flow import SharedCandidateFlow
from strategies.vol import VolConfig


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
# 2. 所有策略共用獲利保護及逐步追蹤停利；啟動逐利的浮盈門檻依策略設定。
TZ = pytz.timezone("Asia/Taipei")
BASE_DIR = os.path.dirname(__file__)
STATE_DIR = os.path.join(BASE_DIR, "stock_state")  # 狀態檔目錄
# 部署前提：本程式不允許盤中補啟動；每日只會在 MAIN_START_TIME 前啟動一次。
# 若該次啟動失敗，當日不會再由排程重新啟動，因此不處理盤中恢復或補單情境。
MAIN_START_TIME = (8, 50)  # 主程序開始執行時間
FORCE_EXIT_TIME = (13, 30)  # 13:30 強制關閉程式

# 單日策略前提：每檔股票在當日只會有一個固定交易方向。
STRATEGY_AVG = 'AVG'
STRATEGY_VOL = 'VOL'

TRADE_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'LONG'

# 一筆入場委託可分批成交；出場意圖鎖定後，晚到成交亦須補平。
# 不主動撤銷入場委託；成交數量與有效回補殘量以券商回報及查詢共同校正。

ENABLE_PROFIT_PROTECTION = False  # 控制獲利保護，不影響固定停損及追蹤停利
PROTECT_PROFIT_PER = 2.5 # 所有策略共用：觸發獲利保護百分比
PROTECT_LOSS_PER = 1.5 # 所有策略共用：獲利保護後的新停損百分比

PROFIT_BACK_PERCENT = 0.5 # 獲利後允許回撤百分比
PROFIT_TARGET_PERCENT = 1.0 # 逐步獲利目標百分比

REALTIME_QUOTE_START_TIME = (9, 3)  # 09:03 後才開始抓個股即時行情，避開開盤初期 quote 欄位不完整

# 共用股票池；候選可並存，同輪完整進場條件成立依順位送單，送單後才固定歸屬。
STOCK_DATA_SOURCE = "T"
STRATEGY_PRIORITY = ["VOL", "AVG"]
# 每日啟動前設定；False 停止該策略的新候選與進場，既有持倉仍繼續風控。
ENABLE_AVG = True
ENABLE_VOL = True
# T: total_stocks；S: selected_stocks；共用候選流程使用副本。
AVG_CANDLES_START_TIME = (9, 52)  # AVG 型態檢核起點；斜率先檢查
AVG_STAGE_1_START_TIME = (9, 0)
AVG_STAGE_1_MIN_BARS = 8
AVG_STAGE_2_MIN_BARS = 8
AVG_STAGE_3_MIN_BARS = 5
AVG_STAGE_4_MIN_BARS = 2
AVG_STAGE_1_AMPLITUDE_PERCENT = 1.0
AVG_STAGE_2_AMPLITUDE_PERCENT = 1.0
AVG_STAGE_3_AMPLITUDE_PERCENT = 1.0
AVG_SLOPE_CUTOFF_TIME = (9, 20)
AVG_MAX_MISSING_SLOPE_BARS = 5  # 09:00（含）至斜率截止（不含）；無效 average 亦算缺棒，0 表示不可缺棒
AVG_MAX_NORMALIZED_SLOPE = 0.06  # 相對昨收 %／分鐘，須嚴格小於此值
AVG_ENTRY_START_TIME = (10, 0)
AVG_ENTRY_DEADLINE = (11, 0)
# 確認完成後保留資格至截止；首次預定入場早於開始時間則當日排除 AVG。
AVG_CANDLE_POLL_SECOND = 15  # 每分鐘於此秒數取一次完整分K，例如 09:33:15
AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS = 0.105  # 最多約 9.5 次請求／秒
AVG_MIN_PRE_ENTRY_RANGE_PERCENT = 5.0
AVG_STOP_LOSS_PERCENT = 2.0
AVG_TAKE_PROFIT_PERCENT = 5.0
AVG_FORCE_CLOSE_TIME = (12, 50)
AVG_ENTRY_ORDER_QUANTITY = 1

# VOL: v35 minute signals; completed close<average confirmation -> 15-second quote entry.
# Ticker eligibility replaces the backtest's 09:00-09:29 bar-count proxy.
VOL_SIGNAL_START_TIME = (9, 20)
VOL_ENTRY_DEADLINE = (12, 0)
VOL_PREVIOUS_TURNOVER_TOP_PERCENT = 30.0  # 共用股票池前日成交金額排名；100為不限排名
VOL_MIN_PREVIOUS_TURNOVER = 300_000_000  # 前日成交金額最低限額（元），另須通過排名
VOL_VOLUME_AVERAGE_DAYS = 5
VOL_MIN_VOLUME_RATIO = 4.0
VOL_REGULAR_SESSION_MINUTES = 270
VOL_MIN_RUNUP_PERCENT = 7.0
VOL_PULLBACK_PERCENT = 3.0
VOL_VOLUME_UNIT_SHARES = 1000
VOL_STOP_LOSS_PERCENT = 2.0
VOL_TAKE_PROFIT_PERCENT = 6.0
VOL_FORCE_CLOSE_TIME = (12, 50)
VOL_ENTRY_ORDER_QUANTITY = 1

ORDER_RESULTS_UPDATE_SECONDS = 10.0  # 成交價量校正查詢間隔，避免每筆下單後立即查詢造成交易 API 連線異常
ORDER_RESULTS_QUERY_START_TIME = (9, 0)  # 開盤前不查委託結果，09:00 後才補成交價量
ORDER_RESULTS_RATE_LIMIT_COOLDOWN_SECONDS = 60.0  # AGR0005 後依券商指示暫停查詢
QUOTE_ROUND_INTERVAL_SECONDS = 15.0
INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE = 600  # 玉山行情官方日內行情額度
ORDER_UNKNOWN_ALERT_SECONDS = 30.0
ORDER_SUBMIT_WAIT_SECONDS = 5.0  # 主迴圈最多等待送單結果；逾時不代表拒單。
TRADE_API_DEGRADED_EVENT = threading.Event()  # 當日停止新進場；既有委託仍須對帳。
TRADE_CALL_STATE_LOCK = threading.Lock()
TRADE_CALL_STATE: Dict[str, Any] = {}
ORDER_SUBMISSION_FUTURES: Dict[str, Future] = {}

ACTIVE_ORDER_STATES: Dict[str, Dict[str, Any]] = {}
PENDING_DEALT_REPORTS: Dict[str, List[Dict[str, Any]]] = {}
ORDER_STATE_LOCK = threading.RLock()
DEALT_REPORT_LOCK = ORDER_STATE_LOCK  # 回報、書號綁定與累計量使用同一把可重入鎖
TRADE_API_LOCK = threading.Lock()  # native SDK 的查詢與送單不可並行。
SEEN_DEALT_REPORT_KEYS: set[tuple[str, str]] = set()
ORDER_RESULTS_COOLDOWN_UNTIL_MONOTONIC = 0.0

MARKETDATA_STOCK_REST_CLIENT: Any = None
ENTRY_MODE_NO_TRADE = 0
ENTRY_MODE_AVG = 3

AVG_CANDLE_EXECUTOR = ThreadPoolExecutor(max_workers=12, thread_name_prefix="avg-candles")

AVG_POSITION_QUOTE_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="avg-position-quote")

MARKET_SCAN_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="market-scan")

ORDER_RESULTS_PENDING_FUTURE: Optional[Future] = None
MARKETDATA_STOP_EVENT = threading.Event()

AVG_RUNTIME = AvgRuntime()
CANDIDATE_QUOTE_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="candidate-quote")
# 所有日內 stock REST 共用節流，保留 600/min 額度餘裕，避免兩套背景工作互相超額。
STOCK_REST_REQUEST_INTERVAL_SECONDS = 0.125
MARKETDATA_HTTP_TIMEOUT_SECONDS = 5.0  # 日內行情 HTTP 逾時預算（秒）；分K、即時報價及 ticker 共用
# 連線耗時會扣除讀取等待預算，不含共用節流排隊，也不是整個請求的硬性總耗時上限。
# 原為 3 秒，先放寬至 5 秒容忍短暫網路延遲；設得越長，大量逾時時整輪掃描及報價更新可能越慢。
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
        self.intraday = RateLimitedIntraday(
            install_intraday_timeout(client.intraday, MARKETDATA_HTTP_TIMEOUT_SECONDS))

    def __getattr__(self, name):
        return getattr(self.client, name)


def is_vol_strategy(state):
    return bool(state and state.get('strategy_type') == STRATEGY_VOL)


def get_vol_config():
    return VolConfig(signal_start=VOL_SIGNAL_START_TIME, entry_deadline=VOL_ENTRY_DEADLINE,
                     min_previous_turnover=VOL_MIN_PREVIOUS_TURNOVER,
                     previous_turnover_top_percent=VOL_PREVIOUS_TURNOVER_TOP_PERCENT,
                     average_days=VOL_VOLUME_AVERAGE_DAYS, min_volume_ratio=VOL_MIN_VOLUME_RATIO,
                     session_minutes=VOL_REGULAR_SESSION_MINUTES, min_runup_percent=VOL_MIN_RUNUP_PERCENT,
                     pullback_percent=VOL_PULLBACK_PERCENT, minute_volume_shares=VOL_VOLUME_UNIT_SHARES)


def validate_vol_config():
    get_vol_config().validate()
    if type(VOL_ENTRY_ORDER_QUANTITY) is not int or VOL_ENTRY_ORDER_QUANTITY <= 0:
        raise ValueError('VOL 張數必須為正整數')
    for value in (VOL_STOP_LOSS_PERCENT, VOL_TAKE_PROFIT_PERCENT):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('VOL 風控百分比必須介於0與100')
    if not VOL_ENTRY_DEADLINE < VOL_FORCE_CLOSE_TIME <= (13, 30):
        raise ValueError('VOL 平倉時間必須晚於入場截止且不晚於13:30')


def get_vol_reference_date():
    # Industry data is independently refreshed after stock-list generation.
    # Reject inconsistent snapshots rather than reuse stale volumes silently.
    dates = [str(value.get('time', ''))[:10] for value in market_previous_close_indices.values()
             if isinstance(value, dict) and value.get('time')]
    if dates and max(dates) != vol_history_reference_date:
        return None
    return vol_history_reference_date


def all_order_states(states):
    """市場計算仍用原 states，委託與對帳採每檔唯一的交易擁有者。"""
    with ORDER_STATE_LOCK:
        return dict(states)


def refresh_active_order_states(states):
    with ORDER_STATE_LOCK:
        ACTIVE_ORDER_STATES.clear()
        ACTIVE_ORDER_STATES.update(all_order_states(states))


def fetch_candidate_quote(symbol, realtime_sdk, priority=MARKETDATA_PRIORITY_CANDIDATE):
    """共用候選行情工作只取資料，並保留請求開始時間。"""
    requested_at = time.monotonic()
    return get_realtime_price(symbol, realtime_sdk, priority=priority), requested_at


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
            ENABLE_PROFIT_PROTECTION=ENABLE_PROFIT_PROTECTION,
            AVG_CANDLES_START_TIME=AVG_CANDLES_START_TIME,
            AVG_CANDLE_POLL_SECOND=AVG_CANDLE_POLL_SECOND,
            AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS=AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS,
            AVG_ENTRY_DEADLINE=AVG_ENTRY_DEADLINE,
            AVG_ENTRY_ORDER_QUANTITY=AVG_ENTRY_ORDER_QUANTITY,
            AVG_ENTRY_START_TIME=AVG_ENTRY_START_TIME,
            AVG_FORCE_CLOSE_TIME=AVG_FORCE_CLOSE_TIME,
            AVG_MAX_NORMALIZED_SLOPE=AVG_MAX_NORMALIZED_SLOPE,
            AVG_MIN_PRE_ENTRY_RANGE_PERCENT=AVG_MIN_PRE_ENTRY_RANGE_PERCENT,
            AVG_PROFIT_BACK_PERCENT=PROFIT_BACK_PERCENT,
            AVG_PROFIT_TARGET_PERCENT=PROFIT_TARGET_PERCENT,
            AVG_PROTECT_LOSS_PER=PROTECT_LOSS_PER,
            AVG_PROTECT_PROFIT_PER=PROTECT_PROFIT_PER,
            AVG_SLOPE_CUTOFF_TIME=AVG_SLOPE_CUTOFF_TIME,
            AVG_MAX_MISSING_SLOPE_BARS=AVG_MAX_MISSING_SLOPE_BARS,
            AVG_STAGE_1_AMPLITUDE_PERCENT=AVG_STAGE_1_AMPLITUDE_PERCENT,
            AVG_STAGE_1_MIN_BARS=AVG_STAGE_1_MIN_BARS,
            AVG_STAGE_1_START_TIME=AVG_STAGE_1_START_TIME,
            AVG_STAGE_2_AMPLITUDE_PERCENT=AVG_STAGE_2_AMPLITUDE_PERCENT,
            AVG_STAGE_2_MIN_BARS=AVG_STAGE_2_MIN_BARS,
            AVG_STAGE_3_AMPLITUDE_PERCENT=AVG_STAGE_3_AMPLITUDE_PERCENT,
            AVG_STAGE_3_MIN_BARS=AVG_STAGE_3_MIN_BARS,
            AVG_STAGE_4_MIN_BARS=AVG_STAGE_4_MIN_BARS,
            AVG_STOCK_DATA_SOURCE=STOCK_DATA_SOURCE,
            AVG_STOP_LOSS_PERCENT=AVG_STOP_LOSS_PERCENT,
            AVG_TAKE_PROFIT_PERCENT=AVG_TAKE_PROFIT_PERCENT,
            INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE=INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE,
            MARKETDATA_PRIORITY_POSITION=MARKETDATA_PRIORITY_POSITION,
            STRATEGY_AVG=STRATEGY_AVG,
            TRADE_SIDE_SHORT=TRADE_SIDE_SHORT,
        ),
        runtime=AVG_RUNTIME,
        services=AvgServices(
            AVG_CANDLE_EXECUTOR=AVG_CANDLE_EXECUTOR,
            AVG_POSITION_QUOTE_EXECUTOR=AVG_POSITION_QUOTE_EXECUTOR,
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
                "symbol_code_with_suf", "symbol_name", "in_position", "qty", "entry_order_pending",
                "entry_order_no", "entry_request_id", "exit_order_pending", "exit_order_no",
                "exit_request_id", "exit_retry_reason", "exit_intent_reason",
                "entry_filled_shares", "exit_filled_shares", "net_position_shares",
                "exit_reserved_shares", "entry_execution", "exit_executions",
            )}
            for state in states.values()
             if state.get("in_position") or state.get("entry_order_pending") or state.get("exit_order_pending")
             or state.get('net_position_shares', 0) != 0
        ]
    attention = bool(unsettled) or TRADE_API_DEGRADED_EVENT.is_set()
    if unsettled:
        print(f'=== 未結清股票，請人工核對券商持倉與委託（{context}）===')
        for item in unsettled:
            net = item.get('net_position_shares')
            net_text = f'{net} 股' if net is not None else '尚未確認'
            print(f"[UNSETTLED_POSITION] 股票={item.get('symbol_code_with_suf')} "
                  f"名稱={item.get('symbol_name')} 已知淨持倉={net_text} "
                  f"入場未結={bool(item.get('entry_order_pending'))} "
                  f"回補未結={bool(item.get('exit_order_pending'))}")
            if item.get('entry_order_pending'):
                print(f"  入場書號={item.get('entry_order_no') or '未知'} "
                      f"request_id={item.get('entry_request_id') or ''}")
            for order in item.get('exit_executions') or []:
                if not order.get('terminal'):
                    print(f"  回補書號={order.get('order_no') or '未知'} "
                          f"request_id={order.get('request_id') or ''} "
                          f"委託={order.get('ordered_shares')} 股 已成交={order.get('filled_shares')} 股")
    atomic_write_json(os.path.join(BASE_DIR, "trade_attention.json"), {
        "time": now_tpe().isoformat(), "context": context,
        "manual_attention_required": attention,
        "sdk_degraded": TRADE_API_DEGRADED_EVENT.is_set(), "unsettled": unsettled,
    })
    trade_log("MANUAL_ATTENTION_REQUIRED" if attention else "TRADE_STATUS_CLEAR",
              error=attention, context=context, unsettled=unsettled,
              sdk_degraded=TRADE_API_DEGRADED_EVENT.is_set())
    return attention


def stop_at_force_exit_time(states):
    """停止時間是硬性退出點；未結清部位交由人工處理。"""
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) >= FORCE_EXIT_TIME:
        attention = report_unsettled_trades(states, 'force_exit_time')
        print(f"已達停止時間 {FORCE_EXIT_TIME[0]:02d}:{FORCE_EXIT_TIME[1]:02d}，程式結束。")
        sys.exit(2 if attention else 0)


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


def consume_late_order_responses(states):
    for request_id, future in list(ORDER_SUBMISSION_FUTURES.items()):
        if not future.done():
            continue
        ORDER_SUBMISSION_FUTURES.pop(request_id, None)
        try:
            response = future.result()
        except Exception:
            continue
        if not isinstance(response, dict):
            continue
        order_no = str(response.get('ord_no', '') or '').strip()
        code = str(response.get('ret_code', '') or '').strip()
        rejected = bool(code) and code != '000000' and not order_no
        if not rejected and (code != '000000' or not order_no):
            continue
        with ORDER_STATE_LOCK:
            for state in states.values():
                for role, order in execution_orders(state):
                    if order.get('request_id') != request_id or order.get('order_no'):
                        continue
                    if rejected:
                        order.update(terminal=True, request_id='')
                        state[role + '_outcome_unknown'] = False
                    else:
                        bind_execution_order(state, role, order, order_no)
                    refresh_execution_totals(state)
                    atomic_write_json(state_path(state['symbol_code_with_suf']), state)


def type_place_order(mysdk, symbol_code_with_suf, action_type, trade_type, quantity=1, price_flag=PriceFlag.Market, price=0.0) -> Optional[str]:
    if type(quantity) is not int or quantity <= 0:
        raise ValueError("委託張數必須為正整數")
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



def ensure_execution_ledger(state):
    """All ledger quantities are shares; API order quantities remain lots."""
    if 'entry_execution' not in state:
        state['entry_execution'] = dict(
            order_no=str(state.get('entry_order_no', '') or ''),
            request_id=state.get('entry_request_id', ''),
            ordered_shares=int(state.get('entry_order_qty', state.get('qty', 0))) * 1000,
            filled_shares=0, filled_value=0.0, ws_shares=0, ws_value=0.0,
            terminal=False, submitted_monotonic=time.monotonic())
    state.setdefault('exit_executions', [])
    return state['entry_execution']


def execution_orders(state):
    return [('entry', ensure_execution_ledger(state))] + [
        ('exit', order) for order in state['exit_executions']]


def order_result_shares(row, field='mat_qty'):
    """Reject malformed/fractional share counts instead of truncating them."""
    value = (Decimal(str(row['mat_qty_share'])) if field == 'mat_qty' and row.get('mat_qty_share') is not None
             else Decimal(str(row.get(field, 0))) * 1000)
    if not value.is_finite() or value < 0 or value != value.to_integral_value():
        raise ValueError('Invalid broker quantity')
    return int(value)


def refresh_execution_totals(state):
    """Called under ORDER_STATE_LOCK. Pending entry prevents early completion."""
    entry = ensure_execution_ledger(state)
    exits = state['exit_executions']
    entered = entry['filled_shares']
    exited = sum(o['filled_shares'] for o in exits)
    reserved = sum(max(0, o['ordered_shares'] - o['filled_shares'])
                   for o in exits if not o['terminal'])
    net = entered - exited
    state.update(entry_filled_shares=entered, exit_filled_shares=exited,
                 net_position_shares=net, exit_reserved_shares=reserved,
                 entry_filled_qty=entered // 1000, exit_filled_qty=exited // 1000,
                 entry_fill_confirmed=entered > 0, exit_fill_confirmed=exited > 0,
                 entry_fully_filled=entered >= entry['ordered_shares'],
                 entry_order_pending=not entry['terminal'],
                 exit_order_pending=any(not o['terminal'] for o in exits),
                 in_position=net > 0, qty=max(0, net // 1000))
    if entered:
        state['entry_average_price'] = entry['filled_value'] / entered
    if exited:
        state['exit_price'] = sum(o['filled_value'] for o in exits) / exited
    settled = entry['terminal'] and net == 0 and not state['exit_order_pending']
    state['traded'] = bool(settled)
    state['exit_fully_filled'] = bool(settled and entered > 0)
    if settled:
        state['exit_reason'] = state.get('exit_intent_reason') or 'entry_order_terminated_unfilled'
    if net < 0:
        TRADE_API_DEGRADED_EVENT.set()
        trade_log('EXECUTION_QUANTITY_MISMATCH', error=True, **state_symbol_fields(state),
                  entered=entered, exited=exited)
    return net


def apply_execution_fill(state, order, shares, value, source):
    """Merge two independent cumulative views by maximum, never addition."""
    if shares < 0 or shares > order['ordered_shares'] or (shares and not finite_positive(value)):
        trade_log('INVALID_FILL_QUANTITY', error=True, **state_symbol_fields(state), shares=shares)
        return False
    old = order['filled_shares']
    if shares > old:
        order.update(filled_shares=shares, filled_value=value)
    elif shares == old and shares and source == 'order_results':
        order['filled_value'] = value
    if order['filled_shares'] == order['ordered_shares']:
        order['terminal'] = True
    entry = ensure_execution_ledger(state)
    if order is entry and order['filled_shares'] and not state.get('risk_prices_initialized'):
        state['entry_price'] = order['filled_value'] / order['filled_shares']
        state['risk_entry_price'] = state['entry_price']
        recalc_entry_position_prices(state)
        state['risk_prices_initialized'] = True
        state['entry_first_fill_time'] = now_tpe().isoformat()
    refresh_execution_totals(state)
    if shares > old:
        trade_log('FILL_UPDATE', **state_symbol_fields(state), source=source,
                  role='entry' if order is entry else 'exit', ord_no=order.get('order_no'),
                  filled_shares=order['filled_shares'], entry_shares=state['entry_filled_shares'],
                  exit_shares=state['exit_filled_shares'], net_shares=state['net_position_shares'],
                  reserved_shares=state['exit_reserved_shares'])
    return shares > old


def apply_execution_report(state, data, role):
    with ORDER_STATE_LOCK:
        orders = [o for r, o in execution_orders(state) if r == role]
        order_no = str(data.get('ord_no', '') or '').strip()
        order = next((o for o in orders if o.get('order_no') == order_no), None)
        if order is None:
            return False
        try:
            shares = int(data.get('mat_qty', 0))
            price = float(data.get('mat_price', 0))
        except (TypeError, ValueError, OverflowError):
            return False
        if shares <= 0 or not finite_positive(price):
            return False
        order['ws_shares'] += shares
        order['ws_value'] += shares * price
        changed = apply_execution_fill(state, order, order['ws_shares'], order['ws_value'], 'DEALT_REPORT')
        atomic_write_json(state_path(state['symbol_code_with_suf']), state)
        return changed


def bind_execution_order(state, role, order, order_no):
    order['order_no'] = str(order_no).strip()
    order['request_id'] = ''
    if role == 'entry' or order is state['exit_executions'][-1]:
        state[role + '_order_no'] = order['order_no']
        state[role + '_outcome_unknown'] = False
    apply_pending_dealt_reports(state)


def _find_state_by_order_no(order_no):
    with ORDER_STATE_LOCK:
        for state in ACTIVE_ORDER_STATES.values():
            for role, order in execution_orders(state):
                if order.get('order_no') == order_no:
                    return state, role
    return None, ''


def _apply_entry_dealt_report_to_state(state, data):
    return apply_execution_report(state, data, 'entry')


def _apply_exit_dealt_report_to_state(state, data):
    return apply_execution_report(state, data, 'exit')


def _apply_dealt_report_to_state(state: Dict[str, Any], data: Dict[str, Any], order_role: str) -> bool:
    if order_role == "entry":
        return _apply_entry_dealt_report_to_state(state, data)
    if order_role == "exit":
        return _apply_exit_dealt_report_to_state(state, data)
    return False


def apply_pending_dealt_reports(state):
    with DEALT_REPORT_LOCK:
        for role, order in execution_orders(state):
            if order.get('order_no'):
                for report in PENDING_DEALT_REPORTS.pop(order['order_no'], []):
                    _apply_dealt_report_to_state(state, report, role)


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
def get_tick_size(price):
    """A boundary belongs to the price interval starting at that boundary."""
    price = Decimal(str(price))
    for upper, tick in (('10', '0.01'), ('50', '0.05'), ('100', '0.1'),
                        ('500', '0.5'), ('1000', '1')):
        if price < Decimal(upper):
            return float(tick)
    return 5.0


def floor_price_to_tick_value(price, tick=None):
    tick = Decimal(str(get_tick_size(price) if tick is None else tick))
    return float((Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick)


def ceil_price_to_tick_value(price, tick=None):
    tick = Decimal(str(get_tick_size(price) if tick is None else tick))
    return float((Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_CEILING) * tick)


def next_price_up(price):
    """Next legal price strictly above price, including at interval boundaries."""
    rounded = ceil_price_to_tick_value(price)
    if Decimal(str(rounded)) > Decimal(str(price)):
        return rounded
    return float(Decimal(str(rounded)) + Decimal(str(get_tick_size(rounded))))


def next_price_down(price):
    """Next legal price strictly below price; use the lower interval at a boundary."""
    rounded = floor_price_to_tick_value(price)
    if Decimal(str(rounded)) < Decimal(str(price)):
        return rounded
    value = Decimal(str(rounded))
    lower_ticks = {'10': '0.01', '50': '0.05', '100': '0.1', '500': '0.5', '1000': '1'}
    tick = next((step for boundary, step in lower_ticks.items()
                 if value == Decimal(boundary)), None)
    return float(value - Decimal(tick or str(get_tick_size(rounded))))


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


def get_entry_mode_text(entry_mode: int | None = None) -> str:
    mode = get_current_entry_mode() if entry_mode is None else entry_mode
    if mode == ENTRY_MODE_NO_TRADE:
        return "NO_TRADE"
    if mode == ENTRY_MODE_AVG:
        return "AVG"
    return "UNKNOWN"


def get_avg_candidate_stocks() -> list:
    return get_avg_strategy().get_avg_candidate_stocks()


def validate_avg_config() -> None:
    return get_avg_strategy().validate_avg_config()


def resolve_session_entry_mode(now_local: datetime) -> Optional[int]:
    """AVG 於自身監看時間啟動；各策略候選由共用流程獨立評估。"""
    if (now_local.hour, now_local.minute) < AVG_CANDLES_START_TIME:
        return None
    if (now_local.hour, now_local.minute) > AVG_ENTRY_DEADLINE:
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

    _protect_loss_per, protect_profit_per = get_protect_loss_profit_percent(state)
    if state.get("side") == TRADE_SIDE_LONG:
        protect_profit_text = f"{entry_price * (1 + protect_profit_per / 100.0):.2f}"
    else:
        protect_profit_text = f"{entry_price * (1 - protect_profit_per / 100.0):.2f}"
    stop_text = f"{flat_price:.2f}"
    print(f"停損：{stop_text}，保本：{protect_profit_text}，停利：{profit_price:.2f}")


def recalc_entry_position_prices(state: Dict[str, Any]) -> bool:
    if state.get('risk_prices_initialized'):
        return True
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
        if is_avg_strategy(state) or is_vol_strategy(state):
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
    # 候選與持倉使用同一報價入口與共用節流。
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


def state_needs_entry_fill_update(state):
    return bool(state.get('entry_order_pending'))


def state_needs_exit_fill_update(state):
    return bool(state.get('exit_order_pending'))


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


def resolve_unknown_order(state, role, results):
    with ORDER_STATE_LOCK:
        for order_role, order in execution_orders(state):
            if order_role != role or not order.get('request_id') or order.get('order_no'):
                continue
            matches = [row for row in results if row.get('user_def') == order['request_id']]
            action = Action.Sell if (state.get('side') == TRADE_SIDE_SHORT) == (role == 'entry') else Action.Buy
            trade = Trade.DayTradingSell if action == Action.Sell else Trade.Cash
            valid = [row for row in matches if (
                str(row.get('stock_no', '')) == state.get('symbol_code')
                and str(row.get('buy_sell', '')) == action.value
                and str(row.get('trade', '')) == trade.value
                and str(row.get('ap_code', '')) == APCode.Common.value
                and row.get('ord_no')
                and str(row.get('org_qty', '')) in (
                    str(order['ordered_shares'] // 1000), str(order['ordered_shares'] / 1000)))]
            if len(matches) == len(valid) == 1:
                bind_execution_order(state, role, order, valid[0]['ord_no'])
            elif time.monotonic() - order.get('submitted_monotonic', time.monotonic()) >= ORDER_UNKNOWN_ALERT_SECONDS:
                if not order.get('unknown_alerted'):
                    order['unknown_alerted'] = True
                    trade_log('ORDER_RECONCILE_REQUIRED', error=True, **state_symbol_fields(state),
                              role=role, request_id=order['request_id'], matches=len(matches))


def reconcile_terminal_order(state, role, results):
    """Release reservations only after fills and cancelled remainder reconcile."""
    with ORDER_STATE_LOCK:
        for order_role, order in execution_orders(state):
            if order_role != role or order['terminal'] or not order.get('order_no'):
                continue
            rows = [r for r in results if str(r.get('ord_no', '')).strip() == order['order_no']]
            if len(rows) != 1:
                continue
            row = rows[0]
            try:
                original = order_result_shares(row, 'org_qty')
                filled = order_result_shares(row)
                cancelled = order_result_shares(row, 'cel_qty')
            except (KeyError, ValueError, TypeError, OverflowError, InvalidOperation):
                continue
            if (original != order['ordered_shares'] or filled != order['filled_shares']
                    or filled + cancelled > original):
                continue  # A stale query cannot release quantity reserved by newer fills.
            err = str(row.get('err_code', '') or '').strip()
            rejected = bool(err.strip('0')) and str(row.get('celable', '')) == '2' and filled == 0
            if filled + cancelled >= original or rejected:
                order['terminal'] = True
        refresh_execution_totals(state)


def update_entry_fills_from_order_results(states, sdk, *, results=None, query_error=None):
    global ORDER_RESULTS_COOLDOWN_UNTIL_MONOTONIC
    if query_error is not None:
        if 'AGR0005' in str(query_error):
            ORDER_RESULTS_COOLDOWN_UNTIL_MONOTONIC = time.monotonic() + ORDER_RESULTS_RATE_LIMIT_COOLDOWN_SECONDS
        trade_log('ORDER_RESULTS_ERROR', error=True, error_msg=repr(query_error))
        return
    if not isinstance(results, list) or any(not isinstance(row, dict) for row in results):
        trade_log('ORDER_RESULTS_ERROR', error=True, reason='invalid_response_type')
        return
    for state in states.values():
        if not state_needs_entry_fill_update(state) and not state_needs_exit_fill_update(state):
            continue
        resolve_unknown_order(state, 'entry', results)
        resolve_unknown_order(state, 'exit', results)
        with ORDER_STATE_LOCK:
            for role, order in execution_orders(state):
                rows = [r for r in results if str(r.get('ord_no', '')).strip() == order.get('order_no') and order.get('order_no')]
                if len(rows) != 1:
                    continue  # Never sum ambiguous cumulative snapshots.
                row = rows[0]
                try:
                    shares = order_result_shares(row)
                    price = float(row.get('avg_price', 0) or 0)
                except (ValueError, TypeError, OverflowError, InvalidOperation):
                    continue
                if shares == 0 or finite_positive(price):
                    apply_execution_fill(state, order, shares, price * shares, 'order_results')
            reconcile_terminal_order(state, 'entry', results)
            reconcile_terminal_order(state, 'exit', results)
            atomic_write_json(state_path(state['symbol_code_with_suf']), state)


def request_expired_entry_cancellations(states, sdk, order_results):
    """Intentionally no cancellation, including after the entry deadline."""
    return


def maintain_pending_orders(states) -> None:
    attention_changed = False
    for state in states.values():
        if not state.get('entry_submission_started') or state.get('traded'):
            continue
        with ORDER_STATE_LOCK:
            for role, order in execution_orders(state):
                if order['terminal'] or order.get('pending_alerted'):
                    continue
                if time.monotonic() - order['submitted_monotonic'] >= ORDER_UNKNOWN_ALERT_SECONDS:
                    order['pending_alerted'] = True
                    attention_changed = True
                    trade_log('ORDER_PENDING_REQUIRES_ATTENTION', error=True,
                              **state_symbol_fields(state), role=role,
                              ord_no=order.get('order_no'), request_id=order.get('request_id'),
                              filled_shares=order['filled_shares'], ordered_shares=order['ordered_shares'])
    if attention_changed or (TRADE_API_DEGRADED_EVENT.is_set() and not getattr(maintain_pending_orders, "degraded_reported", False)):
        maintain_pending_orders.degraded_reported = TRADE_API_DEGRADED_EVENT.is_set()
        report_unsettled_trades(states, "pending_order_watchdog")


def start_order_results_query(sdk) -> Future:
    return start_serialized_trade_call("get_order_results", sdk.get_order_results)


def calculate_percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


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


def get_stock_rest_client(realtime_sdk: EsunMarketdata):
    if MARKETDATA_STOCK_REST_CLIENT is not None:
        return MARKETDATA_STOCK_REST_CLIENT
    return RateLimitedStockRest(realtime_sdk.rest_client.stock)


def print_rest_quote_rate_limit_estimate(states: Dict[str, Dict[str, Any]]) -> None:
    avg_count = len(get_avg_candidate_stocks())
    print(f"共用股票池 {avg_count} 檔；未歸屬股票每分鐘取分K，候選及持倉報價每 {QUOTE_ROUND_INTERVAL_SECONDS:g} 秒一輪")
    print(f"同棒策略優先順序：{STRATEGY_PRIORITY}；歸屬固定，持倉使用即時報價風控")
    print(f"共用行情節流：每 {STOCK_REST_REQUEST_INTERVAL_SECONDS:g} 秒最多啟動一筆，約 {60 / STOCK_REST_REQUEST_INTERVAL_SECONDS:.0f} 次／分鐘")
    print(f"日內行情額度：每分鐘 {INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE} 次；超額需求依優先序排隊")


def calculate_profit_trail_step(price: float, percent: float) -> float:
    """依目前價格計算追蹤停利距離，且至少保留 1 tick。"""
    return max(price * (percent / 100.0), get_tick_size(price))


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
    # 成交回報可同時新增/更新多筆委託；序列化使用一致的深層快照。
    with ORDER_STATE_LOCK:
        data = copy.deepcopy(data)
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
        # 原始委託張數不變；淨持倉與各筆回補委託另由 execution ledger 累計。
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
    if entry_mode in (ENTRY_MODE_NO_TRADE, ENTRY_MODE_AVG):
        return entry_mode
    return ENTRY_MODE_NO_TRADE


def get_optimize_loss_profit_percent(state: Dict[str, Any]) -> tuple[float, float]:
    if is_vol_strategy(state):
        return VOL_STOP_LOSS_PERCENT, VOL_TAKE_PROFIT_PERCENT
    if is_avg_strategy(state):
        return AVG_STOP_LOSS_PERCENT, AVG_TAKE_PROFIT_PERCENT
    raise ValueError("未支援的策略，無法取得停損停利設定")


def get_protect_loss_profit_percent(state: Dict[str, Any] | None = None) -> tuple[float, float]:
    return PROTECT_LOSS_PER, PROTECT_PROFIT_PER


def get_force_close_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if is_vol_strategy(state):
        return VOL_FORCE_CLOSE_TIME
    if is_avg_strategy(state):
        return AVG_FORCE_CLOSE_TIME
    return FORCE_EXIT_TIME


def get_entry_order_quantity(state: Dict[str, Any] | None = None, strategy_type: str | None = None) -> int:
    if strategy_type == STRATEGY_VOL or is_vol_strategy(state):
        return VOL_ENTRY_ORDER_QUANTITY
    if strategy_type == STRATEGY_AVG or is_avg_strategy(state):
        return AVG_ENTRY_ORDER_QUANTITY
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


def get_entry_check_end_time(state: Dict[str, Any]) -> tuple[int, int]:
    if is_vol_strategy(state):
        return VOL_ENTRY_DEADLINE
    if is_avg_strategy(state):
        return AVG_ENTRY_DEADLINE
    return AVG_ENTRY_DEADLINE


def get_entry_check_start_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if state is None:
        return min(AVG_ENTRY_START_TIME, VOL_SIGNAL_START_TIME)
    if is_vol_strategy(state):
        return VOL_SIGNAL_START_TIME
    if is_avg_strategy(state) or get_current_entry_mode() == ENTRY_MODE_AVG:
        return AVG_ENTRY_START_TIME
    return AVG_ENTRY_START_TIME


def get_latest_entry_check_end_time(states: Dict[str, Dict[str, Any]] | None = None) -> tuple[int, int]:
    # 尚未送單的候選不在 states；顯示截止時間仍須涵蓋所有策略。
    times = [AVG_ENTRY_DEADLINE, VOL_ENTRY_DEADLINE]
    times.extend(get_entry_check_end_time(state) for state in (states or {}).values())
    return max(times)


def is_strategy_enabled(strategy_type: str) -> bool:
    """策略開關僅控制新進場，不影響持倉平倉。"""
    return {
        STRATEGY_AVG: ENABLE_AVG,
        STRATEGY_VOL: ENABLE_VOL,
    }.get(strategy_type, False)


def validate_strategy_switches() -> None:
    if type(ENABLE_PROFIT_PROTECTION) is not bool:
        raise ValueError('ENABLE_PROFIT_PROTECTION 必須為布林值')
    for name in ('ENABLE_AVG', 'ENABLE_VOL'):
        if type(globals()[name]) is not bool:
            raise ValueError(f'{name} 必須為 True/False')


def try_open_position(state: Dict[str, Any], mysdk):
    if not is_strategy_enabled(state.get('strategy_type')):
        return
    # 候選可並存；共享流程在送單前登記唯一狀態，其他策略不得繞過交易歸屬。
    with ORDER_STATE_LOCK:
        owner = ACTIVE_ORDER_STATES.get(state.get('symbol_code_with_suf'))
        if owner is not state:
            return
    if state.get('entry_submission_started'):
        return
    if not check_open_status(state):
        return
    if TRADE_API_DEGRADED_EVENT.is_set():
        return  # 當日停止新風險；保留已送出的委託與持倉對帳。
    if is_vol_strategy(state):
        if (not state.get('vol_entry_time')
                or datetime.fromisoformat(state['vol_entry_time']) > now_tpe()
                or not finite_positive(state.get('last_price'))):
            return
    elif is_avg_strategy(state):
        if not AVG_RUNTIME.session_state.get("active"):
            return
        if not state.get('avg_entry_time') or datetime.fromisoformat(state['avg_entry_time']) > now_tpe():
            return
    else:
        trade_log("ENTRY_UNSUPPORTED_STRATEGY", error=True, **state_symbol_fields(state))
        return
    now_local = now_tpe()
    if not get_entry_check_start_time(state) <= (now_local.hour, now_local.minute) <= get_entry_check_end_time(state):
        return
    if not state.get("minute_bars_eligible"):
        return

    last_px = state.get("last_price", 0.0) # 現價
    entry_ref_px = state.get("entry_trigger_price", last_px)  # 進場參考價：trigger price
    qty = state.get("qty", 1)

    # 所有策略共用：入場前曾觸及漲跌停，當日不得進場。
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

    limit_touched = high_price >= limit_up_price or low_price <= limit_down_price
    if limit_touched:
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        state["exit_reason"] = "limit_price_touched_before_entry"
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(
            f"[{state['symbol_name']}] 入場前當日已觸及禁入價格，今日不交易 "
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

        # 已嘗試送單（包括拒單／結果未知），當日不交由另一策略再次送單。
        state['entry_submission_started'] = True
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

            recalc_entry_position_prices(state)
            if unknown_request_id:
                print(f"[{state['symbol_name']}] 入場送單結果未知，保留 pending 並查詢對帳，禁止重送")
            elif side == TRADE_SIDE_SHORT:
                print(f"[{state['symbol_name']}] 作空 已至入場時機，下單成功")
            else:
                print(f"[{state['symbol_name']}] 作多 已至入場時機，下單成功")

            state["profit_tracking_active"] = False  # 追蹤停利尚未啟動，等到首次觸及 profit_price 才開始
            # 最後才公開 ord_no，避免 callback 在估計欄位尚未初始化完成時搶先更新又遭覆寫。
            with ORDER_STATE_LOCK:
                state['entry_order_no'] = str(place_order_result or '')
                entry = ensure_execution_ledger(state)
                entry.update(order_no=state['entry_order_no'], request_id=unknown_request_id,
                             ordered_shares=qty * 1000, terminal=False)
                refresh_execution_totals(state)
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
    if state.get('exit_intent_reason') or state.get('exit_retry_reason'):
        submit_exit_order(state, mysdk, state.get('exit_intent_reason') or state['exit_retry_reason'])
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

    # 獲利保護及逐步追蹤停利仍使用即時報價。
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


def mark_exit_order_pending(state, order_no, exit_reason, request_id='', *, quantity=None):
    with ORDER_STATE_LOCK:
        quantity = quantity if quantity is not None else state['qty']
        order = dict(order_no=str(order_no or ''), request_id=request_id,
                     ordered_shares=quantity * 1000, filled_shares=0, filled_value=0.0,
                     ws_shares=0, ws_value=0.0, terminal=False,
                     submitted_monotonic=time.monotonic())
        state.setdefault('exit_executions', []).append(order)
        state.setdefault('exit_intent_reason', exit_reason)
        state.update(exit_order_no=order['order_no'], exit_request_id=request_id,
                     exit_order_qty=quantity, exit_reason_pending=state['exit_intent_reason'],
                     exit_outcome_unknown=bool(request_id), exit_retry_reason=state['exit_intent_reason'],
                     exit_pending_since_monotonic=time.monotonic(), exit_pending_alerted=False,
                     exit_unknown_since_monotonic=time.monotonic(), exit_unknown_alerted=False)
        refresh_execution_totals(state)
        apply_pending_dealt_reports(state)
        atomic_write_json(state_path(state['symbol_code_with_suf']), state)
    trade_log('ORDER_PENDING', **state_symbol_fields(state), role='exit',
              ord_no=order['order_no'], request_id=request_id, qty=quantity,
              exit_reason=state['exit_intent_reason'])


def _protect_profit_stop(state: Dict[str, Any]):
    """獲利達標後，將 flat_price 推進到至少保住指定獲利的位置。"""
    if not ENABLE_PROFIT_PROTECTION:
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
        return flat_price > 0 and px >= flat_price
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
    target_percent = PROFIT_TARGET_PERCENT
    back_percent = PROFIT_BACK_PERCENT
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


def submit_exit_order(state, mysdk, exit_reason, *, fallback_success_message='', fallback_failure_message=''):
    """Reserve each accepted/unknown exit. Never send more than unreserved net lots."""
    with ORDER_STATE_LOCK:
        state.setdefault('exit_intent_reason', exit_reason)
        state['exit_retry_reason'] = state['exit_intent_reason']
        net = refresh_execution_totals(state)
        available = net - state['exit_reserved_shares']
        if available < 1000 or state.get('exit_submission_busy'):
            return False
        quantity = available // 1000
        params = _exit_order_params(state.get('side'))
        if params is None:
            return False
        state['exit_submission_busy'] = True
        atomic_write_json(state_path(state['symbol_code_with_suf']), state)
    try:
        action, trade, price_flag = params
        try:
            order_no = type_place_order(mysdk, state['symbol_code_with_suf'], action, trade,
                                        quantity=quantity, price_flag=price_flag,
                                        price=state.get('last_price', 0))
        except TradeApiBusy:
            return False
        except OrderOutcomeUnknown as exc:
            mark_exit_order_pending(state, '', exit_reason, exc.request_id, quantity=quantity)
            return True
        if not order_no:
            # Definitive market rejection only: retain existing limit-price fallback.
            params = _exit_order_params(state.get('side'), fallback=True)
            if params is None:
                return False
            try:
                order_no = type_place_order(mysdk, state['symbol_code_with_suf'], *params[:2],
                                            quantity=quantity, price_flag=params[2],
                                            price=state.get('last_price', 0))
            except TradeApiBusy:
                return False
            except OrderOutcomeUnknown as exc:
                mark_exit_order_pending(state, '', exit_reason, exc.request_id, quantity=quantity)
                return True
        if not order_no:
            return False
        mark_exit_order_pending(state, order_no, exit_reason, quantity=quantity)
        return True
    finally:
        with ORDER_STATE_LOCK:
            state['exit_submission_busy'] = False
            atomic_write_json(state_path(state['symbol_code_with_suf']), state)


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
    # 暫時零持倉仍可能有入場殘單；只鎖定出場意圖，由 ledger 判斷可回補量。
    return submit_exit_order(
        state,
        mysdk,
        exit_reason,
        fallback_failure_message=f'[{state.get("symbol_name")}] 已至收盤時間，漲跌停平倉交易失敗，須手動下單平倉',
    )


# ============ 主監控流程 ============
def process_exit_intents(states, sdk):
    """Run every monitor iteration, including after query results and without quotes."""
    for state in states.values():
        with ORDER_STATE_LOCK:
            if not state.get('entry_submission_started') or state.get('traded'):
                continue
            if force_close_time_reached(state):
                if not state.get('exit_intent_reason'):
                    state['exit_intent_reason'] = 'force_close'
                    atomic_write_json(state_path(state['symbol_code_with_suf']), state)
            reason = state.get('exit_intent_reason')
        if reason:
            submit_exit_order(state, sdk, reason)


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


def initialize_states(
    stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]],
    realtime_sdk: EsunMarketdata,
    strategy_type: str = "",
    force_rebuild_state: bool = False,
) -> Dict[str, Dict[str, Any]]:
    if strategy_type != STRATEGY_AVG:
        raise ValueError("initialize_states 僅供 AVG")
    states: Dict[str, Dict[str, Any]] = {}
    filtered_stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]] = []
    for symbolStr, qty, v1, v2, v3, v4, industry_code, volatility_value, streak_tuple in stocks:
        _, code_with_suf = get_pure_symbol(symbolStr)
        normalized_industry_code = str(industry_code).zfill(2)
        market_index_key, market_index_config = "", {}

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
    realtime_quote_start_announced = False
    entry_mode_decided = False
    last_order_results_update_monotonic = 0.0
    order_query_started_monotonic = 0.0
    order_query_stall_announced = False
    candidate_flow = SharedCandidateFlow(globals())
    while True:
        stop_at_force_exit_time(states)
        order_states = all_order_states(states)
        consume_late_order_responses(order_states)
        check_trade_api_watchdog(order_states)
        maintain_pending_orders(order_states)
        now_local = now_tpe()
        if (not realtime_quote_start_announced
                and (now_local.hour, now_local.minute) >= REALTIME_QUOTE_START_TIME):
            print(f"⏰ 個股即時行情取價開始時間！目前時間：{now_local.strftime('%H:%M:%S')}")
            realtime_quote_start_announced = True

        if not candidate_flow.initialized:
            candidate_flow.initialize()
            AVG_RUNTIME.session_state["active"] = True
            persist_entry_mode_to_stock_data(ENTRY_MODE_AVG)
            entry_mode_decided = True
        candidate_flow.process(states, realtime_sdk, mysdk)
        order_states = all_order_states(states)
        pending_states = [st for st in states.values() if not st.get("traded")]
        all_force_close_time_reached = bool(pending_states) and all(
            force_close_time_reached(st)
            for st in pending_states
        )
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


        process_exit_intents(order_states, mysdk)

        # 已全數完成交易則收工
        if (entry_mode_decided
                and not candidate_flow.rows and not candidate_flow.candles
                and (not states or all_traded(states))
                and not any(st.get("in_position") or st.get("entry_order_pending") or st.get("exit_order_pending")
                            for st in order_states.values())):
            if all_force_close_time_reached:
                print("=== 已達停止時間，今日所有標的已收工 ===")
            else:
                print("=== 已全部交易，今日所有標的已收工 ===")
            break

        # print("========= 下一輪監控等待中... =========")
        stop_at_force_exit_time(states)

        now = now_tpe()
        nxt = ceil_next_interval(now, 5) # 秒數在5的倍數輪巡
        sleep_sec = max(0.2, (nxt - now).total_seconds())
        time.sleep(sleep_sec)
        # print(f"========= 開始下一輪 {nxt.strftime('%Y-%m-%d %H:%M:%S')} =========")


# ============ 呼叫 ============
def main(*, runtime_mode: str | None = None):
    """本機與 Docker 共用入口；此程式只支援每日啟動一次。"""
    global stock_data, selected_stocks, market_previous_close_indices, total_stocks
    global MARKETDATA_STOCK_REST_CLIENT
    global vol_history_by_symbol, vol_history_reference_date
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
    realtime_sdk = None
    sdk = None

    try:
        validate_strategy_switches()
        print(f'[PROFIT_PROTECTION] {"ON" if ENABLE_PROFIT_PROTECTION else "OFF"}')
        for strategy in STRATEGY_PRIORITY:
            print(f'[STRATEGY_SWITCH] {strategy}={"ON" if is_strategy_enabled(strategy) else "OFF"}')
        if not any(is_strategy_enabled(s) for s in STRATEGY_PRIORITY):
            print('所有策略已關閉，今日不啟動交易。')
            return
        stock_data = environment.prepare_stock_data()
        selected_stocks = stock_data.selected_stocks
        market_previous_close_indices = stock_data.market_previous_close_indices
        total_stocks = stock_data.total_stocks
        vol_history_by_symbol = getattr(stock_data, 'vol_history_by_symbol', {})
        vol_history_reference_date = getattr(stock_data, 'vol_history_reference_date', None)
        if not isinstance(vol_history_by_symbol, dict):
            vol_history_by_symbol = {}
        if ENABLE_VOL and not vol_history_by_symbol:
            print('[WARN] stock_data沒有VOL歷史量能，本日僅停用各檔VOL；請先重跑aggregate v3及v3_re')
        stock_data.entry_mode = ENTRY_MODE_NO_TRADE
        if ENABLE_AVG:
            validate_avg_config()
        if ENABLE_VOL:
            validate_vol_config()
        wait_until_main_start_time()
        TRADE_API_DEGRADED_EVENT.clear()
        AVG_RUNTIME.session_state.update({"active": False, "last_scan_minute_key": None, "candidate_count": 0})
        AVG_RUNTIME.next_request_monotonic = 0.0
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

        sdk = SDK(config)
        try:
            environment.login_sdk(sdk)
            trade_log("LOGIN_OK", api="trade")
        except Exception as exc:
            trade_log("LOGIN_ERROR", error=True, api="trade", error_msg=repr(exc))
            raise

        states = {}  # 候選條件成立後才建立固定策略的交易狀態。
        refresh_active_order_states(states)
        # SharedCandidateFlow 負責所有策略的背景行情排程。
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
        for executor in (AVG_CANDLE_EXECUTOR, AVG_POSITION_QUOTE_EXECUTOR, MARKET_SCAN_EXECUTOR,
                         CANDIDATE_QUOTE_EXECUTOR):
            executor.shutdown(wait=False, cancel_futures=True)
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
