# -*- coding: utf-8 -*-
import os
import json
import time
import sys
import math
import shutil
import io
import threading
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime
import pytz
from tempfile import NamedTemporaryFile
from configparser import ConfigParser
from esun_trade.sdk import SDK
from esun_trade.order import OrderObject
from esun_trade.constant import (APCode, Trade, PriceFlag, Action)
from esun_marketdata import EsunMarketdata

import stock_data
from stock_data import selected_stocks, selected_limit_up_stocks, selected_limit_down_stocks, market_previous_close_indices


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
# 1. 回測版以分 K 模擬策略，實機版以即時 quote/websocket 執行；資料粒度差異不視為策略不一致。
# 2. LIMIT_UP / LIMIT_DOWN 實機只交易當日名單，信任 selected_limit_up_stocks / selected_limit_down_stocks 已由前置流程產生。
# 3. LOWER 實機多了 best bid/ask 可成交性保護；回測分 K 無足夠委買委賣資料。
# 4. 保本與逐步獲利為實機版特有風控；回測維持固定停損/停利/收盤結算模型。
TZ = pytz.timezone("Asia/Taipei")
BASE_DIR = os.path.dirname(__file__)
STATE_DIR = os.path.join(BASE_DIR, "stock_state")  # 狀態檔目錄
MAIN_START_TIME = (8, 45)  # 主程序開始執行時間
FORCE_EXIT_TIME = (13, 30)  # 13:30 強制關閉程式

STRATEGY_LOWER = 'LOWER'
STRATEGY_LIMIT_DOWN = 'LIMIT_DOWN'
STRATEGY_LIMIT_UP = 'LIMIT_UP'
TRADE_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'LONG'

ENABLE_ENTRY_MODE_LOWER = True  # False 時，STRATEGY_DECISION 判定為 LOWER 後立即結束程序
ENABLE_LIMIT_UP_STRATEGY = False  # False 時，selected_limit_up_stocks 會強制視為空陣列
ENABLE_LIMIT_DOWN_STRATEGY = False  # False 時，selected_limit_down_stocks 會強制視為空陣列

OPTIMIZE_PROFIT_PER_LOWER = 5.0 # lower 停利百分比(%)，例如 5.0 代表入場價減去 5%
OPTIMIZE_LOSS_PER_LOWER = 2.0 # lower 停損百分比(%)，例如 3.0 代表入場價加上 3%

OPTIMIZE_PROFIT_PER_LIMIT_DOWN = 9.0 # limit down 停利百分比(%)
OPTIMIZE_LOSS_PER_LIMIT_DOWN = 2.0 # limit down 停損百分比(%)

OPTIMIZE_PROFIT_PER_LIMIT_UP = 10.0 # limit up 停利百分比(%)
OPTIMIZE_LOSS_PER_LIMIT_UP = 2.0 # limit up 停損百分比(%)

PROTECT_PROFIT_SWITCH_LOWER = False # False 時 lower 不啟動獲利保護；True 維持原本獲利保護
PROTECT_PROFIT_PER_LOWER = 2.5 # lower 觸發獲利保護百分比
PROTECT_LOSS_PER_LOWER = 1.5 # lower 獲利保護後的新停損百分比

PROTECT_PROFIT_SWITCH_LIMIT_UP = False # False 時 limit up 不啟動獲利保護；True 維持原本獲利保護
PROTECT_PROFIT_PER_LIMIT_UP = 5.0 # limit up 觸發獲利保護百分比
PROTECT_LOSS_PER_LIMIT_UP = 3.0 # limit up 獲利保護後的新停損百分比

PROTECT_PROFIT_SWITCH_LIMIT_DOWN = False # False 時 limit down 不啟動獲利保護；True 維持原本獲利保護
PROTECT_PROFIT_PER_LIMIT_DOWN = 4.5 # limit down 觸發獲利保護百分比
PROTECT_LOSS_PER_LIMIT_DOWN = 2.5 # limit down 獲利保護後的新停損百分比

REALTIME_QUOTE_START_TIME = (9, 3)  # 09:03 後才開始抓個股即時行情，避開開盤初期 quote 欄位不完整

MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME = (9, 6)  # 指數位於昨收兩側的 NO_TRADE 檢查起始時間（含）
STRATEGY_EARLY_BREAKOUT_DEADLINE = (9, 21)  # IX0001 早盤須先向下突破 LOWER 門檻的截止時間（含）
STRATEGY_DECISION = (9, 31)  # 市場模式判斷截止時間，不含此時間
ENTRY_CHECK_START_TIME_LOWER = (9, 32)  # lower 進場檢核開始時間（含）
ENTRY_CHECK_END_TIME_LOWER = (10, 1)  # lower 進場檢核截止時間（含）

FORCE_CLOSE_TIME_LOWER = (13, 0)  # lower 收盤前強制平倉時間
FORCE_CLOSE_TIME_LIMIT_DOWN = (13, 0)  # limit down 收盤前強制平倉時間
FORCE_CLOSE_TIME_LIMIT_UP = (13, 0)  # limit up 收盤前強制平倉時間

ENTRY_ORDER_QUANTITY_LOWER = 1 # lower 每次進場下單數量
ENTRY_ORDER_QUANTITY_LIMIT_DOWN = 1 # limit down 每次進場下單數量
ENTRY_ORDER_QUANTITY_LIMIT_UP = 1 # limit up 每次進場下單數量

LOWER_ENTRY_RANGE_START_PERCENT = 10.0 # lower 入場價距昨收到跌停的起始百分比
LOWER_ENTRY_RANGE_END_PERCENT = 60.0 # lower 入場價距昨收到跌停的結束百分比
LOWER_DECISION_DECLINE_PERCENT_THRESHOLD = 40.0 # STRATEGY_DECISION 時落入 lower 入場區間股票比例需嚴格大於此值，才成立 lower 模式

IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.2 # IX0001 啟動門檻：STRATEGY_DECISION 前（不含此時間）low 需低於前日最後 close 的百分比
IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.6 # IX0001 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內
IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.0 # IX0043 啟動門檻：STRATEGY_DECISION 前（不含此時間）low 需低於前日最後 close 的百分比
IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.0 # IX0043 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內

# 產業盤勢過濾：原策略入場條件成立後，產業指數當下價格不可與策略方向相反。
INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT = 0 # lower 入場條件成立後，產業指數即時值不可高於昨收指數上漲此百分比後的位置

PROFIT_BACK_PERCENT = 0.5 # 獲利後允許回撤百分比
PROFIT_TARGET_PERCENT = 1.0 # 逐步獲利目標百分比

ORDER_RESULTS_UPDATE_SECONDS = 10.0  # 成交價量校正查詢間隔，避免每筆下單後立即查詢造成交易 API 連線異常
ORDER_RESULTS_QUERY_START_TIME = (9, 0)  # 預掛單於開盤前不可能成交，09:00 前不查委託結果
ORDER_RESULTS_RATE_LIMIT_COOLDOWN_SECONDS = 60.0  # AGR0005 後依券商指示暫停查詢
MARKET_INDEX_STALE_SECONDS = 30.0  # 市場/產業指數 websocket 超過此秒數未更新時，入場判斷保守視為資料不足

ACTIVE_ORDER_STATES: Dict[str, Dict[str, Any]] = {}
PENDING_DEALT_REPORTS: Dict[str, List[Dict[str, Any]]] = {}
DEALT_REPORT_LOCK = threading.Lock()
SEEN_DEALT_REPORT_KEYS: set[tuple[str, str]] = set()
ORDER_RESULTS_COOLDOWN_UNTIL_MONOTONIC = 0.0

MARKET_INDEX_STATE: Dict[str, Dict[str, Any]] = {}
MARKET_GATE_INDEX_KEYS = ("TWSE:MARKET", "TPEX:MARKET")
MARKET_REVERSAL_STOP_EVENT = threading.Event()
MARKET_REVERSAL_CHECK_ANNOUNCED_EVENT = threading.Event()
POSITION_MARKET_REVERSAL_EVENT = threading.Event()
POSITION_MARKET_REVERSAL_LOCK = threading.Lock()
POSITION_MARKET_REVERSAL_STATE: Dict[str, Any] = {
    "triggered": False,
    "trigger_time": None,
    "index_key": None,
    "index_value": None,
    "threshold": None,
    "strategy_type": None,
}
ENTRY_MODE_NO_TRADE = 0
ENTRY_MODE_LOWER = 2
stock_data.entry_mode = ENTRY_MODE_NO_TRADE


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


# ============ 下單函式 ============
# symbol: '2330' '0050'
# action_type: Action.Buy or Action.Sell
# trade_type: Trade.Cash or Trade.DayTradingSell
# price_flag: PriceFlag.Market or PriceFlag.LimitDown or PriceFlag.LimitUp or PriceFlag.Limit
def type_place_order(mysdk, symbol_code_with_suf, action_type, trade_type, quantity=1, price_flag=PriceFlag.Market, price=0.0) -> Optional[str]:
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
        ap_code=APCode.Common,
        trade=trade_type
    )

    try:
        order_response = mysdk.place_order(order)
        time.sleep(0.1) # 交易 API 限制每秒委託含取消不可超過 20 筆，保守控制在約 10 筆/秒
    except Exception as e:
        trade_log(
            "ORDER_REJECTED",
            error=True,
            symbol=symbol_code_with_suf,
            action=action_type,
            trade=trade_type,
            price_flag=price_flag,
            price=priceInfo,
            qty=quantity,
            reason="place_order_exception",
            error_msg=repr(e),
        )
        return None

    if not isinstance(order_response, dict):
        trade_log(
            "ORDER_REJECTED",
            error=True,
            symbol=symbol_code_with_suf,
            action=action_type,
            trade=trade_type,
            price_flag=price_flag,
            price=priceInfo,
            qty=quantity,
            reason="invalid_response_type",
            response_type=type(order_response).__name__,
            response=repr(order_response),
        )
        return None

    ret_code = str(order_response.get("ret_code", "") or "").strip()
    ret_msg = str(order_response.get("ret_msg", "") or "").strip()
    order_no = str(order_response.get("ord_no", "") or "").strip()
    if ret_code != "000000" or not order_no:
        trade_log(
            "ORDER_REJECTED",
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
            response=repr(order_response),
        )
        return None

    trade_log(
        "ORDER_ACCEPTED",
        symbol=symbol_code_with_suf,
        action=action_type,
        trade=trade_type,
        price_flag=price_flag,
        price=priceInfo,
        qty=quantity,
        ord_no=order_no,
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
    source = str(state.get("entry_price_source", "estimated") or "estimated")
    if source == "order_results" or state.get("entry_fully_filled"):
        return False

    try:
        mat_price = float(data.get("mat_price", 0) or 0)
        # DEALT_REPORT 實機回報 mat_qty 為股數；例如 mat_price=67.5, mat_qty=1000, pay_price=67500。
        mat_shares = int(data.get("mat_qty", 0) or 0)
        entry_order_qty = int(state.get("entry_order_qty", state.get("qty", 0)) or 0)
    except (TypeError, ValueError):
        return False
    if mat_price <= 0 or mat_shares <= 0 or entry_order_qty <= 0:
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
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)

    fill_text = "完整成交" if state.get("entry_fully_filled") else "部分成交"
    trade_log(
        "FILL_UPDATE",
        **state_symbol_fields(state),
        role="entry",
        source="DEALT_REPORT",
        fill_status=fill_text,
        ord_no=state.get("entry_order_no"),
        avg_price=f"{state['entry_price']:.4f}",
        filled_shares=accumulated_shares,
        expected_shares=expected_shares,
        filled_qty=filled_lots,
        expected_qty=entry_order_qty,
    )
    print_entry_position_prices(state)
    return True


def _apply_exit_dealt_report_to_state(state: Dict[str, Any], data: Dict[str, Any]) -> bool:
    """
    以主動成交回報確認平倉；完整成交後才視為交易完成。
    已知限制：目前實際只使用一張股票完整進出，部分成交或殘量拆單處理刻意延後。
    """
    if state.get("exit_fully_filled") or state.get("traded"):
        return False

    try:
        mat_price = float(data.get("mat_price", 0) or 0)
        # DEALT_REPORT 實機回報 mat_qty 為股數；需換算為張數後才和 exit_order_qty 比對。
        mat_shares = int(data.get("mat_qty", 0) or 0)
        exit_order_qty = int(state.get("exit_order_qty", state.get("qty", 0)) or 0)
    except (TypeError, ValueError):
        return False
    if mat_price <= 0 or mat_shares <= 0 or exit_order_qty <= 0:
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

    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)

    fill_text = "完整成交" if state.get("exit_fully_filled") else "部分成交"
    trade_log(
        "FILL_UPDATE",
        **state_symbol_fields(state),
        role="exit",
        source="DEALT_REPORT",
        fill_status=fill_text,
        ord_no=state.get("exit_order_no"),
        exit_reason=state.get("exit_reason_pending") or state.get("exit_reason"),
        avg_price=f"{state['exit_price']:.4f}",
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
    stock = realtime_sdk.rest_client.stock
    stock_intra_ticker = stock.intraday.ticker(symbol=code_num)
    time.sleep(0.2)  # 避免短時間過量 request
    limit_up_price = round(stock_intra_ticker.get('limitUpPrice', 0), 2)
    limit_down_price = round(stock_intra_ticker.get('limitDownPrice', 0), 2)

    symbol_can_day_trade = stock_intra_ticker.get('canDayTrade', False)
    symbol_is_disposition = stock_intra_ticker.get('isDisposition')

    return limit_up_price, limit_down_price, symbol_can_day_trade, symbol_is_disposition


def now_tpe() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))


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


def validate_market_reversal_time_config() -> None:
    early_breakout_deadline_hm = (
        STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
        + STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
    )
    reversal_start_hm = (
        MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
        + MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
    )
    strategy_decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    if not (0 <= early_breakout_deadline_hm <= 23 * 60 + 59):
        raise ValueError(
            "STRATEGY_EARLY_BREAKOUT_DEADLINE 設定錯誤，需介於 00:00~23:59"
        )
    if not (0 <= reversal_start_hm <= 23 * 60 + 59):
        raise ValueError(
            "MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 設定錯誤，需介於 00:00~23:59"
        )
    if early_breakout_deadline_hm >= strategy_decision_hm:
        raise ValueError(
            "STRATEGY_EARLY_BREAKOUT_DEADLINE 必須早於 STRATEGY_DECISION"
        )
    if reversal_start_hm > early_breakout_deadline_hm:
        raise ValueError(
            "MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME "
            "不可晚於 STRATEGY_EARLY_BREAKOUT_DEADLINE"
        )
    if reversal_start_hm >= strategy_decision_hm:
        raise ValueError(
            "MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 必須早於 STRATEGY_DECISION"
        )
    if LOWER_DECISION_DECLINE_PERCENT_THRESHOLD < 0:
        raise ValueError(
            "LOWER_DECISION_DECLINE_PERCENT_THRESHOLD 不可小於 0"
        )


def get_entry_mode_text(entry_mode: int | None = None) -> str:
    mode = get_current_entry_mode() if entry_mode is None else entry_mode
    if mode == ENTRY_MODE_NO_TRADE:
        return "NO_TRADE"
    if mode == ENTRY_MODE_LOWER:
        return "LOWER"
    return "UNKNOWN"


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
        state["profit_price"] = max(
            entry_price - open_profit_target,
            state.get("limit_down_price", 0)
        )
        state["flat_price"] = min(
            entry_price + open_stop_loss,
            state.get("limit_up_price", 0)
        )
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


def get_realtime_price(stock_id: str, realtime_sdk):
    code_num = stock_id.split(".")[0]
    stock = realtime_sdk.rest_client.stock  # Stock REST API client

    stock_intraday_quote = stock.intraday.quote(symbol=code_num)
    last_price = stock_intraday_quote['lastPrice']
    open_price = stock_intraday_quote['openPrice']
    high_price = stock_intraday_quote['highPrice']
    low_price = stock_intraday_quote['lowPrice']
    close_price = stock_intraday_quote['closePrice']
    avg_price = stock_intraday_quote['avgPrice']
    bids = stock_intraday_quote.get('bids') or []
    best_bid_price = bids[0].get('price') if bids else None

    return last_price, open_price, high_price, low_price, close_price, avg_price, best_bid_price


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


def update_entry_fills_from_order_results(states: Dict[str, Dict[str, Any]], sdk) -> None:
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
        order_results = sdk.get_order_results()
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

    if not isinstance(order_results, list):
        trade_log(
            "ORDER_RESULTS_ERROR",
            error=True,
            reason="invalid_response_type",
            response_type=type(order_results).__name__,
            response=repr(order_results),
        )
        return

    for state in candidates:
        # 呼叫期間可能剛收到 DEALT_REPORT；先成功的來源擁有本筆校正權。
        if state.get("entry_price_source") == "dealt_report":
            continue
        avg_price, mat_qty = get_order_fill_info_by_results(state.get("entry_order_no", ""), order_results)
        if avg_price <= 0 or mat_qty <= 0:
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

        if not state.get("profit_tracking_active"):
            recalc_entry_position_prices(state)
        else:
            print(f"[{state.get('symbol_name')}] 已啟動追蹤停利，成交價量校正不覆寫既有停利軌跡")

        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        fill_text = "完整成交" if state.get("entry_fully_filled") else "部分成交"
        trade_log(
            "FILL_UPDATE",
            **state_symbol_fields(state),
            role="entry",
            source="get_order_results",
            fill_status=fill_text,
            ord_no=state.get("entry_order_no"),
            avg_price=f"{avg_price:.4f}",
            filled_qty=mat_qty,
            expected_qty=entry_order_qty,
        )
        print_entry_position_prices(state)

    for state in exit_candidates:
        if state.get("exit_price_source") == "dealt_report":
            continue
        avg_price, mat_qty = get_order_fill_info_by_results(state.get("exit_order_no", ""), order_results)
        if avg_price <= 0 or mat_qty <= 0:
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

        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        fill_text = "完整成交" if state.get("exit_fully_filled") else "部分成交"
        trade_log(
            "FILL_UPDATE",
            **state_symbol_fields(state),
            role="exit",
            source="get_order_results",
            fill_status=fill_text,
            ord_no=state.get("exit_order_no"),
            exit_reason=state.get("exit_reason_pending") or state.get("exit_reason"),
            avg_price=f"{avg_price:.4f}",
            filled_qty=mat_qty,
            expected_qty=exit_order_qty,
        )


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
    if index_key == "TWSE:MARKET":
        return IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER
    if index_key == "TPEX:MARKET":
        return IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER
    return None


def get_strategy_decision_rebound_percent(index_key: str) -> float | None:
    if index_key == "TWSE:MARKET":
        return IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER
    if index_key == "TPEX:MARKET":
        return IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER
    return None




def update_market_strategy_decision_gate_state(market_key: str, index_value: float, event_time: Any) -> None:
    if market_key not in MARKET_GATE_INDEX_KEYS:
        return

    now_local = now_tpe()
    if (now_local.hour, now_local.minute) >= STRATEGY_DECISION:
        return

    index_config = market_previous_close_indices.get(market_key, {})
    drop_percent = get_strategy_decision_drop_percent(market_key)
    rebound_percent = get_strategy_decision_rebound_percent(market_key)
    previous_close = index_config.get("previous_close")
    try:
        previous_close_float = float(previous_close)
    except (TypeError, ValueError):
        return
    if previous_close_float <= 0:
        return

    market_state = MARKET_INDEX_STATE.setdefault(market_key, {})
    now_hm = now_local.hour * 60 + now_local.minute
    if (now_local.hour, now_local.minute) >= MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME:
        if not MARKET_REVERSAL_CHECK_ANNOUNCED_EVENT.is_set():
            MARKET_REVERSAL_CHECK_ANNOUNCED_EVENT.set()
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
                "將於 STRATEGY_DECISION 判定為 NO_TRADE"
            )

    drop_threshold = (
        previous_close_float * (1 - drop_percent / 100.0)
        if drop_percent is not None
        else None
    )
    rebound_threshold = (
        previous_close_float * (1 - rebound_percent / 100.0)
        if rebound_percent is not None
        else None
    )
    early_breakout_deadline_hm = (
        STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
        + STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
    )
    early_breakout_start_hm = (
        MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
        + MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
    )
    if (
        market_key == "TWSE:MARKET"
        and now_hm >= early_breakout_start_hm
        and now_hm <= early_breakout_deadline_hm
        and not market_state.get("early_breakout_passed")
    ):
        if drop_threshold is not None and index_value < drop_threshold:
            market_state["early_breakout_passed"] = True
            market_state["early_breakout_side"] = "DOWN"
            market_state["early_breakout_time"] = event_time or now_local.isoformat()

    if drop_threshold is not None and index_value < drop_threshold:
        if (
            (not market_state.get("strategy_decision_broken"))
            or market_state.get("strategy_decision_rebound_blocked")
        ):
            market_state["strategy_decision_break_time"] = event_time or now_local.isoformat()
        market_state["strategy_decision_broken"] = True
        market_state["strategy_decision_rebound_blocked"] = False
        market_state.pop("strategy_decision_rebound_time", None)

    if (
        rebound_threshold is not None
        and market_state.get("strategy_decision_broken")
        and (not market_state.get("strategy_decision_rebound_blocked"))
        and index_value >= rebound_threshold
    ):
        market_state["strategy_decision_rebound_blocked"] = True
        market_state["strategy_decision_rebound_time"] = event_time or now_local.isoformat()

def update_position_market_reversal_state(
    market_key: str,
    index_value: float,
    event_time: Any,
) -> None:
    """模式確定後，以即時指數判斷是否須封鎖進場並平掉非獨立策略持倉。"""
    if market_key not in MARKET_GATE_INDEX_KEYS or POSITION_MARKET_REVERSAL_EVENT.is_set():
        return

    now_local = now_tpe()
    entry_mode = get_current_entry_mode()
    if entry_mode != ENTRY_MODE_LOWER:
        return
    strategy_type = STRATEGY_LOWER
    start_time = ENTRY_CHECK_START_TIME_LOWER
    end_time = FORCE_CLOSE_TIME_LOWER
    threshold_percent = get_strategy_decision_rebound_percent(market_key)
    comparison_text = ">="

    now_hm = (now_local.hour, now_local.minute)
    if now_hm < start_time or now_hm >= end_time or threshold_percent is None:
        return

    previous_close = market_previous_close_indices.get(market_key, {}).get("previous_close")
    try:
        previous_close_float = float(previous_close)
        index_value_float = float(index_value)
    except (TypeError, ValueError):
        return
    if previous_close_float <= 0:
        return

    threshold = previous_close_float * (1 - threshold_percent / 100.0)
    triggered = index_value_float >= threshold
    if not triggered:
        return

    with POSITION_MARKET_REVERSAL_LOCK:
        if POSITION_MARKET_REVERSAL_STATE["triggered"]:
            return
        trigger_time = event_time or now_local.isoformat()
        POSITION_MARKET_REVERSAL_STATE.update({
            "triggered": True,
            "trigger_time": trigger_time,
            "index_key": market_key,
            "index_value": index_value_float,
            "threshold": threshold,
            "strategy_type": strategy_type,
        })
        POSITION_MARKET_REVERSAL_EVENT.set()

    print(
        f"[MARKET_REVERSAL] {now_local.strftime('%H:%M:%S')} {market_key} "
        f"index={index_value_float:.2f} {comparison_text} threshold={threshold:.2f}，"
        "封鎖今日新進場並準備平掉非獨立策略持倉"
    )


def is_market_index_fresh(index_key: str) -> tuple[bool, Optional[float]]:
    market_state = MARKET_INDEX_STATE.get(index_key, {})
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
    return age_seconds <= MARKET_INDEX_STALE_SECONDS, age_seconds


def get_fresh_market_index_value(index_key: str) -> tuple[Optional[float], Optional[float], str]:
    market_state = MARKET_INDEX_STATE.get(index_key, {})
    last_index = market_state.get("last_index")
    if last_index is None:
        return None, None, "尚未收到 websocket 指數資料"

    is_fresh, age_seconds = is_market_index_fresh(index_key)
    if not is_fresh:
        age_text = "--" if age_seconds is None else f"{age_seconds:.1f}"
        return None, age_seconds, f"websocket 指數資料逾時 age_seconds={age_text}"

    try:
        return float(last_index), age_seconds, ""
    except (TypeError, ValueError):
        return None, age_seconds, "websocket 指數資料格式錯誤"


def get_market_strategy_decision_gate_result(index_key: str) -> Dict[str, Any]:
    index_config = market_previous_close_indices.get(index_key, {})
    drop_percent = get_strategy_decision_drop_percent(index_key)
    rebound_percent = get_strategy_decision_rebound_percent(index_key)
    previous_close = index_config.get("previous_close")
    market_state = MARKET_INDEX_STATE.get(index_key, {})
    market_index_fresh, market_index_age = is_market_index_fresh(index_key)

    result = {
        "index_key": index_key,
        "symbol": index_config.get("symbol"),
        "name": index_config.get("name"),
        "previous_close": previous_close,
        "last_index": market_state.get("last_index"),
        "drop_threshold": None,
        "rebound_threshold": None,
        "broken": bool(market_state.get("strategy_decision_broken")),
        "rebound_blocked": bool(market_state.get("strategy_decision_rebound_blocked")),
        "break_time": market_state.get("strategy_decision_break_time"),
        "rebound_time": market_state.get("strategy_decision_rebound_time"),
        "previous_close_traded_above": bool(market_state.get("previous_close_traded_above")),
        "previous_close_traded_below": bool(market_state.get("previous_close_traded_below")),
        "previous_close_reversal_blocked": bool(market_state.get("previous_close_reversal_blocked")),
        "previous_close_reversal_time": market_state.get("previous_close_reversal_time"),
        "early_breakout_passed": bool(market_state.get("early_breakout_passed")),
        "early_breakout_side": market_state.get("early_breakout_side"),
        "early_breakout_time": market_state.get("early_breakout_time"),
        "index_fresh": market_index_fresh,
        "index_age_seconds": market_index_age,
        "passed": False,
        "lower_passed": False,
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

    last_index_float, market_index_age, market_index_error = get_fresh_market_index_value(index_key)
    result["index_age_seconds"] = market_index_age
    if last_index_float is None:
        result["lower_reason"] = market_index_error
        return result

    if drop_percent is None or rebound_percent is None:
        result["lower_reason"] = "LOWER 門檻設定無效"
    elif not result["broken"]:
        result["lower_reason"] = "未跌破啟動門檻"
    elif last_index_float >= result["rebound_threshold"]:
        result["lower_reason"] = "決策時已反彈至失效門檻"
    else:
        result["lower_passed"] = True
        result["lower_reason"] = "通過"

    result["passed"] = result["lower_passed"]
    return result


def calculate_percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def summarize_lower_strategy_decision_candidates(
    states: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    candidate_count = 0
    decline_count = 0
    data_missing_count = 0

    for state in states.values():
        if is_independent_strategy(state):
            continue
        candidate_count += 1

        try:
            yesterday_close = float(state.get("yesterday_close_price"))
            limit_down = float(state.get("limit_down_price"))
            last_price = float(state.get("last_price"))
        except (TypeError, ValueError):
            data_missing_count += 1
            continue

        if yesterday_close <= 0 or limit_down <= 0 or last_price <= 0:
            data_missing_count += 1
            continue

        entry_lower_bound, entry_upper_bound = calculate_entry_range_bounds(
            yesterday_close,
            limit_down,
            LOWER_ENTRY_RANGE_START_PERCENT,
            LOWER_ENTRY_RANGE_END_PERCENT,
        )
        if is_price_in_entry_range(last_price, entry_lower_bound, entry_upper_bound):
            decline_count += 1

    decline_percent = calculate_percent(decline_count, candidate_count)
    return {
        "candidate_count": candidate_count,
        "decline_count": decline_count,
        "decline_percent": decline_percent,
        "data_missing_count": data_missing_count,
        "threshold": LOWER_DECISION_DECLINE_PERCENT_THRESHOLD,
        "passed": decline_percent > LOWER_DECISION_DECLINE_PERCENT_THRESHOLD,
    }


def format_lower_decision_summary(summary: Dict[str, Any]) -> str:
    return (
        f"候選={summary.get('candidate_count', 0)} "
        f"下降={summary.get('decline_count', 0)} "
        f"下降比例={float(summary.get('decline_percent', 0.0)):.2f}% "
        f"threshold={LOWER_DECISION_DECLINE_PERCENT_THRESHOLD:.2f}%"
    )


def decide_entry_mode_by_market_gate(
    states: Dict[str, Dict[str, Any]],
) -> tuple[int, list[Dict[str, Any]]]:
    gate_results = [
        get_market_strategy_decision_gate_result(index_key)
        for index_key in MARKET_GATE_INDEX_KEYS
    ]

    ix0001_result = next(
        (result for result in gate_results if result["index_key"] == "TWSE:MARKET"),
        None,
    )
    data_issue_results = [
        result
        for result in gate_results
        if (
            not result.get("symbol")
            or result.get("previous_close") in (None, "")
            or result.get("last_index") is None
        )
    ]
    if data_issue_results:
        for result in data_issue_results:
            print(
                f"[MODE] {result['index_key']} 市場指數資料不足，"
                f"lower_reason={result.get('lower_reason') or '--'}"
            )
        print("[MODE] 市場指數資料不足，保守判定為 NO_TRADE")
        return ENTRY_MODE_NO_TRADE, gate_results

    def fallback_no_trade(reason: str) -> tuple[int, list[Dict[str, Any]]]:
        print(f"[MODE] {reason}，判定為 NO_TRADE")
        return ENTRY_MODE_NO_TRADE, gate_results

    if ix0001_result is None or not ix0001_result.get("early_breakout_passed"):
        print(
            "[MODE] IX0001 未於早盤截止時間前向下突破 LOWER 門檻 "
            f"(截止 {STRATEGY_EARLY_BREAKOUT_DEADLINE[0]:02d}:"
            f"{STRATEGY_EARLY_BREAKOUT_DEADLINE[1]:02d})"
        )
        return fallback_no_trade("IX0001 未於早盤截止時間前向下突破 LOWER 門檻")

    reversal_blocked = any(result["previous_close_reversal_blocked"] for result in gate_results)
    if reversal_blocked:
        print(
            "[MODE] 上市或上櫃指數在指定時段曾位於昨收上下兩側 "
            f"({MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0]:02d}:"
            f"{MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]:02d}~"
            f"{STRATEGY_DECISION[0]:02d}:{STRATEGY_DECISION[1]:02d})"
        )
        return fallback_no_trade("上市或上櫃指數在指定時段曾位於昨收上下兩側")

    lower_mode_passed = all(result["lower_passed"] for result in gate_results)

    if lower_mode_passed:
        lower_decision_summary = summarize_lower_strategy_decision_candidates(states)
        if not lower_decision_summary["passed"]:
            print(
                "[MODE] LOWER 個股池下降比例不足："
                f"{format_lower_decision_summary(lower_decision_summary)}，"
                "判定為 NO_TRADE"
            )
            return ENTRY_MODE_NO_TRADE, gate_results
        return ENTRY_MODE_LOWER, gate_results
    return fallback_no_trade("LOWER 未成立")


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


def print_entry_mode_decision(
    entry_mode: int,
    gate_results: list[Dict[str, Any]],
    states: Dict[str, Dict[str, Any]],
) -> None:
    mode_text = get_entry_mode_text(entry_mode)
    print(f"[MODE] STRATEGY_DECISION 模式判斷：{mode_text}")
    lower_decision_summary = summarize_lower_strategy_decision_candidates(states)
    print(f"[MODE] LOWER 個股池：{format_lower_decision_summary(lower_decision_summary)}")
    for result in gate_results:
        if result["index_key"] != "TWSE:MARKET":
            continue
        print(
            f"[MODE] {result['index_key']} {result.get('symbol') or ''} "
            f"early_breakout_passed={result.get('early_breakout_passed')} "
            f"early_breakout_side={result.get('early_breakout_side') or '--'} "
            f"early_breakout_time={format_market_gate_time(result.get('early_breakout_time'))}"
        )
    for result in gate_results:
        print(
            f"[MODE] {result['index_key']} {result.get('symbol') or ''} "
            f"previous_close={format_market_gate_value(result.get('previous_close'))} "
            f"drop_threshold={format_market_gate_value(result.get('drop_threshold'))} "
            f"rebound_threshold={format_market_gate_value(result.get('rebound_threshold'))} "
            f"last_index={format_market_gate_value(result.get('last_index'))} "
            f"index_fresh={result.get('index_fresh')} "
            f"index_age={format_market_gate_age(result.get('index_age_seconds'))} "
            f"stale_limit={MARKET_INDEX_STALE_SECONDS:.1f}s "
            f"break_time={format_market_gate_time(result.get('break_time'))} "
            f"rebound_time={format_market_gate_time(result.get('rebound_time'))} "
            f"lower_passed={result.get('lower_passed')}"
        )
    for result in gate_results:
        print(
            f"[MODE] {result['index_key']} {result.get('symbol') or ''} "
            f"previous_close={format_market_gate_value(result.get('previous_close'))} "
            f"last_index={format_market_gate_value(result.get('last_index'))} "
            f"lower_reason={result.get('lower_reason') or '--'}"
        )


def apply_entry_mode_to_states(states: Dict[str, Dict[str, Any]], entry_mode: int) -> None:
    persist_entry_mode_to_stock_data(entry_mode)
    for st in states.values():
        if is_independent_strategy(st):
            continue
        st["qty"] = get_entry_order_quantity()
        st["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)


def start_market_index_stream(realtime_sdk: EsunMarketdata):
    symbol_to_market_key = {
        str(info.get("symbol", "")): market_key
        for market_key, info in market_previous_close_indices.items()
        if info.get("symbol")
    }
    if not symbol_to_market_key:
        print("[WARN] 未設定 market_previous_close_indices，盤勢濾網與 market gate 將等待指數資料")
        return None

    def handle_message(message):
        try:
            payload = json.loads(message) if isinstance(message, str) else message
            data = payload.get("data", payload) if isinstance(payload, dict) else {}
            index_symbol = str(data.get("symbol", ""))
            market_key = symbol_to_market_key.get(index_symbol)
            if not market_key:
                return

            index_value = data.get("index")
            if index_value is None:
                return

            index_float = float(index_value)
            market_state = MARKET_INDEX_STATE.setdefault(market_key, {})
            market_state["symbol"] = index_symbol
            market_state["last_index"] = index_float
            market_state["time"] = data.get("time")
            market_state["last_updated"] = now_tpe().isoformat()
            update_market_strategy_decision_gate_state(market_key, index_float, data.get("time"))
            update_position_market_reversal_state(market_key, index_float, data.get("time"))
        except Exception as e:
            trade_log(
                "MARKET_WS_MESSAGE_ERROR",
                error=True,
                reason="message_parse_or_update_failed",
                error_msg=repr(e),
                raw=repr(message),
            )

    try:
        stock_ws = realtime_sdk.websocket_client.stock
        stock_ws.on("message", handle_message)
        trade_log("MARKET_WS_CONNECTING", channel="indices", symbols=",".join(symbol_to_market_key.keys()))
        stock_ws.connect()
        for index_symbol in symbol_to_market_key:
            market_key = symbol_to_market_key[index_symbol]
            index_config = market_previous_close_indices.get(market_key, {})
            stock_ws.subscribe({
                "channel": "indices",
                "symbol": index_symbol,
            })
            trade_log(
                "MARKET_WS_SUBSCRIBED",
                channel="indices",
                market_key=market_key,
                symbol=index_symbol,
                name=index_config.get("name", ""),
            )
        return stock_ws
    except Exception as e:
        trade_log(
            "MARKET_WS_ERROR",
            error=True,
            reason="connect_or_subscribe_failed",
            error_msg=repr(e),
        )
        return None


def close_market_index_stream(stock_ws: Any) -> None:
    if stock_ws is None:
        return

    for method_name in ("disconnect", "close", "stop"):
        method = getattr(stock_ws, method_name, None)
        if not callable(method):
            continue
        try:
            method()
            print(f"[MARKET] WebSocket 已執行 {method_name}()")
            return
        except Exception as e:
            print(f"[WARN] 關閉盤勢指數 WebSocket {method_name}() 失敗: {e}")


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


def is_market_reversal_blocked_at_entry(state: Dict[str, Any], strategy_type: str) -> bool:
    """入場當下任一市場指數回到模式失效門檻即封鎖；缺資料亦封鎖。"""
    for index_key in MARKET_GATE_INDEX_KEYS:
        index_config = market_previous_close_indices.get(index_key, {})
        previous_close = index_config.get("previous_close")
        last_index_float, _age_seconds, stale_reason = get_fresh_market_index_value(index_key)
        if last_index_float is None:
            print(f"[{state['symbol_name']}] 市場模式入場檢查等待 {index_key} 指數資料：{stale_reason}")
            return True
        try:
            previous_close_float = float(previous_close)
        except (TypeError, ValueError):
            print(f"[{state['symbol_name']}] 市場模式入場檢查 {index_key} 昨收指數設定錯誤: {previous_close}")
            return True

        if previous_close_float <= 0:
            print(f"[{state['symbol_name']}] 市場模式入場檢查 {index_key} 昨收指數設定錯誤: {previous_close}")
            return True

        if strategy_type == STRATEGY_LOWER:
            rebound_percent = get_strategy_decision_rebound_percent(index_key)
            if rebound_percent is None:
                print(f"[{state['symbol_name']}] 市場模式入場檢查 {index_key} lower 反彈門檻設定錯誤")
                return True
            threshold = previous_close_float * (1 - rebound_percent / 100.0)
            if last_index_float >= threshold:
                print(
                    f"[{state['symbol_name']}] 市場模式入場檢查封鎖 LOWER："
                    f"{index_key} last_index={last_index_float:.2f} >= rebound_threshold={threshold:.2f}"
                )
                return True
        else:
            return True
    return False


def lower_industry_market_filter_pass(state: Dict[str, Any]) -> bool:
    market_key = state.get("market_index_key")
    if not market_key:
        market_key = get_market_key_for_symbol(
            state.get("symbol_code_with_suf", ""),
            state.get("industry_code", ""),
        )

    index_config = market_previous_close_indices.get(market_key, {})
    previous_close = index_config.get("previous_close")
    last_index_float, _age_seconds, stale_reason = get_fresh_market_index_value(market_key)
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

    threshold = previous_close_float * (1 + INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT / 100.0)
    if last_index_float >= threshold:
        index_name = index_config.get("name", "")
        print(
            f"[{state['symbol_name']}] 產業別盤勢濾網未通過：{market_key} {index_name} "
            f"指數 {last_index_float:.2f} >= 門檻 {threshold:.2f}"
        )
        return False

    return True




def format_industry_market_filter_pass_text(state: Dict[str, Any]) -> str:
    market_key = state.get("market_index_key")
    if not market_key:
        market_key = get_market_key_for_symbol(
            state.get("symbol_code_with_suf", ""),
            state.get("industry_code", ""),
        )

    index_config = market_previous_close_indices.get(market_key, {})
    previous_close = index_config.get("previous_close")
    last_index_float, _age_seconds, _stale_reason = get_fresh_market_index_value(market_key)
    if last_index_float is None:
        return ""

    try:
        previous_close_float = float(previous_close)
    except (TypeError, ValueError):
        return ""

    if previous_close_float <= 0:
        return ""

    index_name = index_config.get("name", "")
    threshold = previous_close_float * (1 + INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT / 100.0)
    operator = "<"

    return (
        f"（產業別盤勢濾網：{market_key} {index_name} "
        f"指數 {last_index_float:.2f} {operator} 門檻 {threshold:.2f}）"
    )


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


def atomic_write_json(path: str, data: Dict[str, Any]):
    """
    原子寫入：先寫到臨時檔，fsync 後再 os.replace 覆蓋目標檔，降低檔案損壞風險。
    改進：
    - 轉為使用絕對目標路徑，確保 tmp 檔寫在相同目錄下（避免跨磁碟或路徑差異導致 os.replace 失敗）。
    - 對 Windows / OneDrive 可能的鎖定 (PermissionError) 做重試與 fallback（先嘗試刪除目標檔，再替換；最後備援以複製內容覆蓋）。
    - 確保臨時檔在任何情況下都會適當清理，避免殘留。
    """
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
    if entry_mode in (ENTRY_MODE_NO_TRADE, ENTRY_MODE_LOWER):
        return entry_mode
    return ENTRY_MODE_NO_TRADE


def is_entry_mode_enabled(entry_mode: int) -> bool:
    if entry_mode == ENTRY_MODE_LOWER:
        return ENABLE_ENTRY_MODE_LOWER
    return False


def is_lower_mode() -> bool:
    return get_current_entry_mode() == ENTRY_MODE_LOWER


def is_limit_down_strategy(state: Dict[str, Any] | None) -> bool:
    if not state:
        return False
    return state.get("strategy_type") == STRATEGY_LIMIT_DOWN


def is_limit_up_strategy(state: Dict[str, Any] | None) -> bool:
    if not state:
        return False
    return state.get("strategy_type") == STRATEGY_LIMIT_UP


def is_independent_limit_strategy(state: Dict[str, Any] | None) -> bool:
    return is_limit_down_strategy(state) or is_limit_up_strategy(state)


def is_independent_strategy(state: Dict[str, Any] | None) -> bool:
    return is_independent_limit_strategy(state)


def get_optimize_loss_profit_percent(state: Dict[str, Any]) -> tuple[float, float]:
    if is_limit_down_strategy(state):
        return OPTIMIZE_LOSS_PER_LIMIT_DOWN, OPTIMIZE_PROFIT_PER_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return OPTIMIZE_LOSS_PER_LIMIT_UP, OPTIMIZE_PROFIT_PER_LIMIT_UP
    if is_lower_mode():
        return OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER
    return OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER


def get_protect_loss_profit_percent(state: Dict[str, Any] | None = None) -> tuple[float, float]:
    if is_limit_down_strategy(state):
        return PROTECT_LOSS_PER_LIMIT_DOWN, PROTECT_PROFIT_PER_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return PROTECT_LOSS_PER_LIMIT_UP, PROTECT_PROFIT_PER_LIMIT_UP
    return PROTECT_LOSS_PER_LOWER, PROTECT_PROFIT_PER_LOWER


def is_protect_profit_enabled(state: Dict[str, Any] | None = None) -> bool:
    if is_limit_down_strategy(state):
        return PROTECT_PROFIT_SWITCH_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return PROTECT_PROFIT_SWITCH_LIMIT_UP
    return PROTECT_PROFIT_SWITCH_LOWER


def get_force_close_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if is_limit_down_strategy(state):
        return FORCE_CLOSE_TIME_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return FORCE_CLOSE_TIME_LIMIT_UP
    if is_lower_mode():
        return FORCE_CLOSE_TIME_LOWER
    return FORCE_CLOSE_TIME_LOWER


def get_entry_order_quantity(state: Dict[str, Any] | None = None) -> int:
    if is_limit_down_strategy(state):
        return ENTRY_ORDER_QUANTITY_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return ENTRY_ORDER_QUANTITY_LIMIT_UP
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
    if is_limit_down_strategy(state):
        return FORCE_CLOSE_TIME_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return FORCE_CLOSE_TIME_LIMIT_UP
    if is_lower_mode():
        return ENTRY_CHECK_END_TIME_LOWER
    return STRATEGY_DECISION


def get_entry_check_start_time() -> tuple[int, int]:
    if is_lower_mode():
        return ENTRY_CHECK_START_TIME_LOWER
    return STRATEGY_DECISION


def get_latest_entry_check_end_time() -> tuple[int, int]:
    return max(
        ENTRY_CHECK_END_TIME_LOWER,
        FORCE_CLOSE_TIME_LIMIT_DOWN,
        FORCE_CLOSE_TIME_LIMIT_UP,
    )


def entry_lower_mode_price_check(state: Dict[str, Any]) -> bool | str:
    """
    lower 模式進場條件判斷；成立時以當下 last_price 寫入 entry_trigger_price。
    實機特有：best_bid 檢查用於降低即時價已觸發但委買未跟上的成交風險，回測分 K 不具備此資料。

    回傳值：
      True      — 條件成立，應進場（side 由呼叫端設定）
      'BLOCKED' — 驗證失敗，需永久封鎖本日進場（呼叫端負責設 traded=True）
      False     — 尚未觸發，繼續等待下一輪
    """
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME_LOWER:
        return False
    if (now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME_LOWER:
        return False

    yesterday_close_price = state.get("yesterday_close_price")
    limit_down_price = state.get("limit_down_price")
    if yesterday_close_price is None or limit_down_price is None:
        return False

    last_price = state.get("last_price", 0)
    if last_price is None:
        return False

    best_bid_price = state.get("best_bid_price")
    if best_bid_price is None:
        return False

    try:
        yesterday_close = float(yesterday_close_price)
        limit_down = float(limit_down_price)
        last_px = float(last_price)
        best_bid = float(best_bid_price)
    except (TypeError, ValueError):
        return False

    if last_px <= 0 or best_bid <= 0:
        return False

    entry_lower_bound, entry_upper_bound = calculate_entry_range_bounds(
        yesterday_close,
        limit_down,
        LOWER_ENTRY_RANGE_START_PERCENT,
        LOWER_ENTRY_RANGE_END_PERCENT,
    )
    if not is_price_in_entry_range(last_px, entry_lower_bound, entry_upper_bound):
        return False

    if best_bid > last_px:
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"現價已落入 lower 入場區間但最佳bid未跟上，暫不進場 "
            f"last_price={last_px} best_bid={best_bid} "
            f"entry_range={entry_lower_bound:.2f}~{entry_upper_bound:.2f}"
        )
        return False

    if is_market_reversal_blocked_at_entry(state, STRATEGY_LOWER):
        return 'BLOCKED'
    if not lower_industry_market_filter_pass(state):
        return False

    state["entry_trigger_price"] = last_px
    return True


def entry_limit_down_price_check(state: Dict[str, Any]) -> bool | str:
    """
    limit-down 獨立策略進場條件判斷；連續跌停條件已在狀態初始化前確認，直接以即時 quote openPrice 作空。
    """
    open_price = state.get("open_price")
    try:
        open_px = float(open_price)
    except (TypeError, ValueError):
        return False

    if open_px <= 0:
        return False

    state["entry_trigger_price"] = open_px
    state["side"] = TRADE_SIDE_SHORT
    return True


def entry_limit_up_price_check(state: Dict[str, Any]) -> bool | str:
    """
    limit-up 獨立策略進場條件判斷；連續漲停條件已在狀態初始化前確認，直接以即時 quote openPrice 作多。
    """
    open_price = state.get("open_price")
    try:
        open_px = float(open_price)
    except (TypeError, ValueError):
        return False

    if open_px <= 0:
        return False

    state["entry_trigger_price"] = open_px
    state["side"] = TRADE_SIDE_LONG
    return True


def entry_price_check(state: Dict[str, Any]) -> bool | str:
    """
    依 entry_mode 分派進場條件判斷。
    """
    if is_limit_down_strategy(state):
        return entry_limit_down_price_check(state)
    if is_limit_up_strategy(state):
        return entry_limit_up_price_check(state)
    if get_current_entry_mode() == ENTRY_MODE_NO_TRADE:
        now_local = now_tpe()
        if (now_local.hour, now_local.minute) < STRATEGY_DECISION:
            return False
        print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} NO_TRADE 模式不追蹤")
        return 'BLOCKED'
    if is_lower_mode():
        return entry_lower_mode_price_check(state)
    return 'BLOCKED'


def try_open_position(state: Dict[str, Any], mysdk):
    if POSITION_MARKET_REVERSAL_EVENT.is_set() and not is_independent_strategy(state):
        state["traded"] = True
        state["exit_reason"] = "market_reversal"
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] 大盤反轉已觸發，取消今日進場")
        return

    last_px = state.get("last_price", 0.0) # 現價
    entry_ref_px = state.get("entry_trigger_price", last_px)  # 進場參考價：trigger price
    qty = state.get("qty", 1)

    # LIMIT_UP / LIMIT_DOWN 為刻意預掛漲跌停價的獨立策略，不套用此入場排除。
    # 其餘策略若當日曾觸及任一漲跌停，則取消當日交易。
    if not is_independent_limit_strategy(state):
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

        if high_price >= limit_up_price or low_price <= limit_down_price:
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
        if side == TRADE_SIDE_SHORT: # SHORT 作空

            place_order_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Sell, Trade.DayTradingSell, quantity=qty, price_flag=PriceFlag.Market, price=entry_ref_px)
        else:
            place_order_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Buy, Trade.Cash, quantity=qty, price_flag=PriceFlag.Market, price=entry_ref_px)

        if place_order_result:  # 下單成功
            state["entry_order_pending"] = True
            state["in_position"] = False
            state["entry_order_qty"] = qty
            state["entry_filled_qty"] = 0
            state["entry_fully_filled"] = False
            state["entry_fill_confirmed"] = False
            state["entry_price_source"] = "estimated"
            state["entry_price"] = entry_ref_px
            state["dealt_report_filled_shares"] = 0
            state["dealt_report_filled_value"] = 0.0

            industry_filter_text = "" if is_independent_strategy(state) else format_industry_market_filter_pass_text(state)
            recalc_entry_position_prices(state)
            if side == TRADE_SIDE_SHORT:
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
                qty=state.get("entry_order_qty"),
                price=entry_ref_px,
                reason="entry_signal",
            )
            print_entry_position_prices(state)
        else:  # 下單失敗
            state["traded"] = True

        # 更新狀態
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def try_place_preopen_limit_order(state: Dict[str, Any], mysdk) -> bool:
    if not check_open_status(state):
        return False

    if is_limit_up_strategy(state):
        side = TRADE_SIDE_LONG
        action_type = Action.Buy
        trade_type = Trade.Cash
        price_flag = PriceFlag.LimitUp
        entry_ref_px = state.get("limit_up_price", 0)
        order_text = "預掛漲停買單"
    elif is_limit_down_strategy(state):
        side = TRADE_SIDE_SHORT
        action_type = Action.Sell
        trade_type = Trade.DayTradingSell
        price_flag = PriceFlag.LimitDown
        entry_ref_px = state.get("limit_down_price", 0)
        order_text = "預掛跌停賣單"
    else:
        return False

    try:
        entry_ref_px = float(entry_ref_px)
    except (TypeError, ValueError):
        entry_ref_px = 0.0

    if entry_ref_px <= 0:
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] 無法取得預掛委託參考價，不追蹤")
        return False

    qty = state.get("qty", 1)
    place_order_result = type_place_order(
        mysdk,
        state["symbol_code_with_suf"],
        action_type,
        trade_type,
        quantity=qty,
        price_flag=price_flag,
        price=entry_ref_px,
    )

    state["entry_time"] = now_tpe().isoformat()
    if not place_order_result:
        state["traded"] = True
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] {order_text}失敗，不追蹤")
        return False

    state["side"] = side
    state["entry_order_pending"] = True
    state["in_position"] = False
    state["entry_order_qty"] = qty
    state["entry_filled_qty"] = 0
    state["entry_fully_filled"] = False
    state["entry_fill_confirmed"] = False
    state["entry_trigger_price"] = entry_ref_px
    state["entry_price_source"] = "estimated"
    state["entry_price"] = entry_ref_px
    state["dealt_report_filled_shares"] = 0
    state["dealt_report_filled_value"] = 0.0
    state["profit_tracking_active"] = False
    recalc_entry_position_prices(state)
    # 最後才公開 ord_no，避免 callback 在估計欄位尚未初始化完成時搶先更新又遭覆寫。
    state["entry_order_no"] = str(place_order_result)
    apply_pending_dealt_reports(state)
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
    trade_log(
        "ORDER_PENDING",
        **state_symbol_fields(state),
        role="entry",
        ord_no=state.get("entry_order_no"),
        qty=state.get("entry_order_qty"),
        price=f"{entry_ref_px:.2f}",
        reason=order_text,
    )
    print_entry_position_prices(state)
    return True


def place_preopen_limit_orders(states: Dict[str, Dict[str, Any]], mysdk) -> None:
    # LIMIT_UP / LIMIT_DOWN 實機只交易當日輸入名單；連續漲跌停天數由前置選股流程負責。
    limit_states = [
        state
        for state in states.values()
        if is_limit_up_strategy(state) or is_limit_down_strategy(state)
    ]
    if not limit_states:
        return

    print(f"[PREOPEN] 發現 {len(limit_states)} 檔 LIMIT_UP/LIMIT_DOWN 標的，開始預掛委託")
    success_count = 0
    for state in limit_states:
        if try_place_preopen_limit_order(state, mysdk):
            success_count += 1
    print(f"[PREOPEN] 預掛委託完成：成功 {success_count}/{len(limit_states)}")


def try_close_position(state: Dict[str, Any], mysdk):
    if not state.get("in_position"):
        return
    if state.get("exit_order_pending"):
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


def mark_exit_order_pending(state: Dict[str, Any], order_no: str, exit_reason: str) -> None:
    state["exit_order_pending"] = True
    state["exit_order_no"] = str(order_no)
    state["exit_order_qty"] = int(state.get("qty", 0) or 0)
    state["exit_filled_qty"] = 0
    state["exit_fully_filled"] = False
    state["exit_fill_confirmed"] = False
    state["exit_price_source"] = "estimated"
    state["exit_reason_pending"] = exit_reason
    state["exit_order_time"] = now_tpe().isoformat()
    state["exit_dealt_report_filled_shares"] = 0
    state["exit_dealt_report_filled_value"] = 0.0
    apply_pending_dealt_reports(state)
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
    trade_log(
        "ORDER_PENDING",
        **state_symbol_fields(state),
        role="exit",
        ord_no=state.get("exit_order_no"),
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

    if is_limit_up_strategy(state) and side == TRADE_SIDE_LONG:
        trigger_price_field = "high_price"
    elif is_limit_down_strategy(state) and side == TRADE_SIDE_SHORT:
        trigger_price_field = "low_price"
    else:
        trigger_price_field = "last_price"
    try:
        px = float(state.get(trigger_price_field))
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
    if is_limit_up_strategy(state) and side == TRADE_SIDE_LONG:
        trigger_price_field = "high_price"
    elif is_limit_down_strategy(state) and side == TRADE_SIDE_SHORT:
        trigger_price_field = "low_price"
    else:
        trigger_price_field = "last_price"

    try:
        px = float(state.get(trigger_price_field))
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
    target_step = calculate_profit_trail_step(px, PROFIT_TARGET_PERCENT)
    back_step = calculate_profit_trail_step(px, PROFIT_BACK_PERCENT)
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
    if state.get("exit_order_pending"):
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

    action_type, trade_type, price_flag = market_params
    exit_order_no = type_place_order(
        mysdk,
        state["symbol_code_with_suf"],
        action_type,
        trade_type,
        quantity=state.get("qty", 0),
        price_flag=price_flag,
        price=state.get("last_price", 0.0),
    )
    if exit_order_no:
        mark_exit_order_pending(state, str(exit_order_no), exit_reason)
        return True

    fallback_params = _exit_order_params(side, fallback=True)
    if fallback_params is None:
        return False

    action_type, trade_type, price_flag = fallback_params
    reserve_order_no = type_place_order(
        mysdk,
        state["symbol_code_with_suf"],
        action_type,
        trade_type,
        quantity=state.get("qty", 0),
        price_flag=price_flag,
        price=0,
    )
    if not reserve_order_no:
        if fallback_failure_message:
            print(fallback_failure_message)
        return False

    if fallback_success_message:
        print(fallback_success_message)
    mark_exit_order_pending(state, str(reserve_order_no), exit_reason)
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


def apply_market_reversal_metadata(state: Dict[str, Any]) -> None:
    state["exit_reason"] = "market_reversal"
    state["market_reversal_trigger_time"] = POSITION_MARKET_REVERSAL_STATE.get("trigger_time")
    state["market_reversal_index_key"] = POSITION_MARKET_REVERSAL_STATE.get("index_key")
    state["market_reversal_index_value"] = POSITION_MARKET_REVERSAL_STATE.get("index_value")
    state["market_reversal_threshold"] = POSITION_MARKET_REVERSAL_STATE.get("threshold")
    state["market_reversal_strategy_type"] = POSITION_MARKET_REVERSAL_STATE.get("strategy_type")


def handle_position_market_reversal(
    states: Dict[str, Dict[str, Any]],
    mysdk,
) -> bool:
    """大盤反轉後送出非獨立策略平倉委託；成交確認後才視為完成。"""
    for state in states.values():
        if is_independent_strategy(state):
            continue

        apply_market_reversal_metadata(state)

        if state.get("traded"):
            continue

        if state.get("entry_order_pending"):
            continue

        if not state.get("in_position"):
            state["traded"] = True
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            continue

        close_flat_position(state, mysdk, "market_reversal")
        if state.get("exit_order_pending"):
            print(f"[{state.get('symbol_name')}] 大盤反轉平倉委託已送出")

    return all_traded({
        symbol: state
        for symbol, state in states.items()
        if not is_independent_strategy(state)
    })


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
) -> Dict[str, Any]:
    """
    若檔案存在就讀檔；若 date != 今天（Asia/Taipei），視為舊檔，直接刪除並用當日 v1/v2 重建。
    若不存在則用當日 v1/v2 建檔。
    """
    _, code_with_suf = get_pure_symbol(symbol)

    path = state_path(code_with_suf)
    existing = load_json_or_none(path)
    today_str = today_str_tpe()

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
    return all(s.get("traded", False) for s in states.values())


def has_unfinished_independent_states(states: Dict[str, Dict[str, Any]]) -> bool:
    return any(
        is_independent_strategy(state) and not state.get("traded")
        for state in states.values()
    )


def has_unfinished_non_independent_states(states: Dict[str, Dict[str, Any]]) -> bool:
    return any(
        (not is_independent_strategy(state)) and not state.get("traded")
        for state in states.values()
    )


def mark_non_independent_states_blocked(states: Dict[str, Dict[str, Any]], reason: str) -> None:
    for state in states.values():
        if (
            is_independent_strategy(state)
            or state.get("traded")
            or state.get("in_position")
            or state.get("entry_order_pending")
            or state.get("exit_order_pending")
        ):
            continue
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        state["exit_reason"] = reason
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def block_non_independent_states_for_disabled_entry_mode(
    states: Dict[str, Dict[str, Any]],
    entry_mode_name: str,
) -> bool:
    mark_non_independent_states_blocked(states, "entry_mode_disabled")
    if has_unfinished_independent_states(states):
        print(
            f"[MODE] {entry_mode_name} 已停用，非獨立策略今日不執行，"
            "獨立策略繼續監控"
        )
        return False
    print(
        f"[MODE] {entry_mode_name} 已停用，且無未完成獨立策略，"
        "結束監控"
    )
    return True


def initialize_states(
    stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]],
    realtime_sdk: EsunMarketdata,
    strategy_type: str = "",
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
        market_index_key, market_index_config = get_industry_index_config(code_with_suf, normalized_industry_code)
        if not market_index_config.get("symbol"):
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

        if strategy_type == STRATEGY_LIMIT_DOWN:
            quantity = ENTRY_ORDER_QUANTITY_LIMIT_DOWN
        elif strategy_type == STRATEGY_LIMIT_UP:
            quantity = ENTRY_ORDER_QUANTITY_LIMIT_UP
        else:
            quantity = get_entry_order_quantity()

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
        )
        if strategy_type == STRATEGY_LIMIT_DOWN:
            st["side"] = TRADE_SIDE_SHORT
            st["qty"] = ENTRY_ORDER_QUANTITY_LIMIT_DOWN
            st["entry_order_qty"] = ENTRY_ORDER_QUANTITY_LIMIT_DOWN
        elif strategy_type == STRATEGY_LIMIT_UP:
            st["side"] = TRADE_SIDE_LONG
            st["qty"] = ENTRY_ORDER_QUANTITY_LIMIT_UP
            st["entry_order_qty"] = ENTRY_ORDER_QUANTITY_LIMIT_UP
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
    update_status = False
    realtime_quote_start_announced = False
    strategy_decision_announced = False
    entry_check_start_announced = False
    entry_check_end_announced = False
    entry_mode_decided = False
    last_order_results_update_monotonic = 0.0
    while True:
        if POSITION_MARKET_REVERSAL_EVENT.is_set() and has_unfinished_non_independent_states(states):
            if handle_position_market_reversal(states, mysdk):
                if not has_unfinished_independent_states(states):
                    print("[MARKET_REVERSAL] 所有標的均已完成大盤反轉處理，結束監控")
                    return
                print("[MARKET_REVERSAL] 非獨立策略標的已完成大盤反轉處理，獨立策略繼續監控")

        if MARKET_REVERSAL_STOP_EVENT.is_set():
            mark_non_independent_states_blocked(states, "market_mode_blocked")
            if not has_unfinished_independent_states(states):
                print("[MODE] 市場模式已封鎖，維持 NO_TRADE 並結束監控")
                return

        now_local = now_tpe()
        if (
            not realtime_quote_start_announced
            and (now_local.hour, now_local.minute) >= REALTIME_QUOTE_START_TIME
        ):
            print(
                f"⏰ 個股即時行情取價開始時間！"
                f"目前時間：{now_local.strftime('%H:%M:%S')}"
            )
            realtime_quote_start_announced = True

        if (not strategy_decision_announced) and ((now_local.hour, now_local.minute) >= STRATEGY_DECISION):
            print(f"⏰ 模式判斷時間！目前時間：{now_local.strftime('%H:%M:%S')}")
            strategy_decision_announced = True
            if not entry_mode_decided:
                entry_mode, gate_results = decide_entry_mode_by_market_gate(states)
                apply_entry_mode_to_states(states, entry_mode)
                print_entry_mode_decision(entry_mode, gate_results, states)
                entry_mode_decided = True
                if entry_mode == ENTRY_MODE_LOWER and not ENABLE_ENTRY_MODE_LOWER:
                    print(
                        "[MODE] STRATEGY_DECISION 判定為 LOWER，但 "
                        "ENABLE_ENTRY_MODE_LOWER=False，今日不執行此模式，"
                        "非獨立策略將停止追蹤"
                    )
                    if block_non_independent_states_for_disabled_entry_mode(states, "LOWER"):
                        return
        entry_check_start_time = get_entry_check_start_time()
        current_entry_mode = get_current_entry_mode()
        if (
            entry_mode_decided
            and current_entry_mode != ENTRY_MODE_NO_TRADE
            and is_entry_mode_enabled(current_entry_mode)
            and has_unfinished_non_independent_states(states)
            and not entry_check_start_announced
            and (now_local.hour, now_local.minute) >= entry_check_start_time
        ):
            print(
                f"⏰ {get_entry_mode_text()} 進場檢核開始時間！"
                f"目前時間：{now_local.strftime('%H:%M:%S')}"
            )
            entry_check_start_announced = True

        latest_entry_check_end_time = get_latest_entry_check_end_time()
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
        ):
            update_entry_fills_from_order_results(states, mysdk)
            last_order_results_update_monotonic = now_monotonic

        # 已全數完成交易則收工
        if all_traded(states):
            if all_force_close_time_reached:
                print("=== 已達停止時間，今日所有標的已收工 ===")
            else:
                print("=== 已全部交易，今日所有標的已收工 ===")
            break

        # 輪詢
        for st in states.values():
            if st.get("traded"):
                # print(f"[{st['symbol_name']}] | 已完成交易，跳過")
                continue

            try:
                need_persist_at_end = False

                if force_close_time_reached(st) and st.get("in_position") and not st.get("traded"):
                    try_close_position(st, mysdk)
                    continue

                if round_should_update_realtime:
                    try:
                        # print("更新股價")
                        px, open_px, high_price, low_price, close_price, avg_price, best_bid_price = get_realtime_price(st.get("symbol_code_with_suf", ""), realtime_sdk)
                    except Exception as e:
                        trade_log(
                            "QUOTE_ERROR",
                            error=True,
                            **state_symbol_fields(st),
                            api="intraday.ticker",
                            reason="get_realtime_price_failed",
                            error_msg=repr(e),
                        )
                        continue

                    if (px is None) or (open_px is None):
                        trade_log(
                            "QUOTE_ERROR",
                            error=True,
                            **state_symbol_fields(st),
                            api="intraday.ticker",
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
                        print(f"[{st['symbol_name']}] {now_tpe().strftime('%H:%M:%S')} 檢核時間結束仍未進場，不追蹤")
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
                            if is_limit_down_strategy(st):
                                st["side"] = TRADE_SIDE_SHORT
                            elif is_limit_up_strategy(st):
                                st["side"] = TRADE_SIDE_LONG
                            else:
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
            sys.exit(0)

        now = now_tpe()
        nxt = ceil_next_interval(now, 5) # 秒數在5的倍數輪巡
        sleep_sec = max(0.2, (nxt - now).total_seconds())
        if MARKET_REVERSAL_STOP_EVENT.is_set() and has_unfinished_independent_states(states):
            time.sleep(sleep_sec)
        elif MARKET_REVERSAL_STOP_EVENT.wait(timeout=sleep_sec):
            continue
        # print(f"========= 開始下一輪 {nxt.strftime('%Y-%m-%d %H:%M:%S')} =========")

        update_status = False # 依指定秒點更新股票，太頻繁更新會有API call次數過多的問題
        if (nxt.second == 5) or (nxt.second == 20) or (nxt.second == 35) or (nxt.second == 50):
            update_status = True


# ============ 呼叫 ============
if __name__ == "__main__":
    #print(10*get_tick_size(74.3))

    base_dir = os.path.dirname(__file__)
    execute_result_path = os.path.join(base_dir, "execute_strategy_result.txt")
    capture_buffer = io.StringIO()
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sys.stdout = TeeStream(original_stdout, capture_buffer)
    sys.stderr = TeeStream(original_stderr, capture_buffer)
    market_index_ws = None

    try:
        validate_market_reversal_time_config()
        wait_until_main_start_time()
        MARKET_REVERSAL_STOP_EVENT.clear()
        MARKET_REVERSAL_CHECK_ANNOUNCED_EVENT.clear()
        clear_state_dir()
        persist_entry_mode_to_stock_data(ENTRY_MODE_NO_TRADE)

        # 登入以操作API
        config = ConfigParser()
        config.read('config.ini')

        realtime_sdk = EsunMarketdata(config)
        try:
            realtime_sdk.login()
            trade_log("LOGIN_OK", api="marketdata")
        except Exception as exc:
            trade_log("LOGIN_ERROR", error=True, api="marketdata", error_msg=repr(exc))
            raise
        market_index_ws = start_market_index_stream(realtime_sdk)

        sdk = SDK(config)
        try:
            sdk.login()
            trade_log("LOGIN_OK", api="trade")
        except Exception as exc:
            trade_log("LOGIN_ERROR", error=True, api="trade", error_msg=repr(exc))
            raise

        candidate_symbols = selected_stocks
        candidate_limit_up_symbols = selected_limit_up_stocks if ENABLE_LIMIT_UP_STRATEGY else []
        candidate_limit_down_symbols = selected_limit_down_stocks if ENABLE_LIMIT_DOWN_STRATEGY else []
        if not ENABLE_LIMIT_UP_STRATEGY:
            print("[CONFIG] ENABLE_LIMIT_UP_STRATEGY=False，selected_limit_up_stocks 強制視為空陣列")
        if not ENABLE_LIMIT_DOWN_STRATEGY:
            print("[CONFIG] ENABLE_LIMIT_DOWN_STRATEGY=False，selected_limit_down_stocks 強制視為空陣列")
        states = initialize_states(candidate_symbols, realtime_sdk)
        limit_up_states = initialize_states(
            candidate_limit_up_symbols,
            realtime_sdk,
            strategy_type=STRATEGY_LIMIT_UP,
        )
        states.update(limit_up_states)
        limit_down_states = initialize_states(
            candidate_limit_down_symbols,
            realtime_sdk,
            strategy_type=STRATEGY_LIMIT_DOWN,
        )
        states.update(limit_down_states)
        ACTIVE_ORDER_STATES.clear()
        ACTIVE_ORDER_STATES.update(states)

        trade_report_thread = start_trade_report_stream(sdk)
        time.sleep(1.0)  # 先讓交易回報 WebSocket 建立連線，再送出預掛單
        if trade_report_thread.is_alive():
            trade_log("TRADE_WS_THREAD_OK", thread=trade_report_thread.name)
        else:
            trade_log(
                "TRADE_WS_THREAD_ERROR",
                error=True,
                thread=trade_report_thread.name,
                reason="thread_not_alive_after_start",
            )
        place_preopen_limit_orders(states, sdk)

        # 對齊到下一個 5 秒邊界，避免第一輪跨分鐘造成額外更新
        align_now = now_tpe()
        align_next = ceil_next_interval(align_now, 5)
        align_sleep_sec = max(0.2, (align_next - align_now).total_seconds())
        time.sleep(align_sleep_sec)

        # 開始正式作業
        monitor(states, sdk, realtime_sdk)
    finally:
        close_market_index_stream(market_index_ws)
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        try:
            with open(execute_result_path, "w", encoding="utf-8") as f:
                f.write(capture_buffer.getvalue())
        except Exception as e:
            print(f"[WARN] 無法輸出 execute_strategy_result.txt：{e}", file=original_stderr)
