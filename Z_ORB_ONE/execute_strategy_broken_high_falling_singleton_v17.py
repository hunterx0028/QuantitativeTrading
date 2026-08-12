# -*- coding: utf-8 -*-
import os
import json
import time
import sys
import math
import shutil
import io
import threading
from pprint import pformat
from typing import List, Tuple, Optional, Dict, Any
from datetime import date, datetime, timedelta
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
TZ = pytz.timezone("Asia/Taipei")
BASE_DIR = os.path.dirname(__file__)
STATE_DIR = os.path.join(BASE_DIR, "stock_state")  # 狀態檔目錄
MAIN_START_TIME = (8, 55)  # 主程序開始執行時間；提早啟動時等待至此時間
FORCE_EXIT_TIME = (13, 30)  # 13:30 強制關閉程式

ENTRY_BLOCKED = 'ENTRY_BLOCKED'
GATE_LOWER_PASSED = 'LOWER_PASSED'
GATE_FOLLOW_PASSED = 'FOLLOW_PASSED'
GATE_CHANCE_PASSED = 'CHANCE_PASSED'
GATE_NOT_PASSED = 'NOT_PASSED'
GATE_NO_TRADE = 'NO_TRADE'
STRATEGY_LOWER = 'LOWER'
STRATEGY_FOLLOW = 'FOLLOW'
STRATEGY_CHANCE = 'CHANCE'
STRATEGY_VOLUME_DOWN = 'VOLUME_DOWN'
STRATEGY_NO_TRADE = 'NO_TRADE'
STRATEGY_LIMIT_DOWN = 'LIMIT_DOWN'
STRATEGY_LIMIT_UP = 'LIMIT_UP'
TRADE_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'LONG'

ENABLE_ENTRY_MODE_FOLLOW = True  # False 時，STRATEGY_DECISION 判定為 FOLLOW 後立即結束程序
ENABLE_ENTRY_MODE_LOWER = True  # False 時，STRATEGY_DECISION 判定為 LOWER 後立即結束程序
ENABLE_ENTRY_MODE_CHANCE = True  # False 時，STRATEGY_DECISION 判定為 CHANCE 後立即結束程序
ENABLE_VOLUME_DOWN_STRATEGY = True  # False 時，不取得分鐘量，也不執行 VOLUME_DOWN
ENABLE_LIMIT_UP_STRATEGY = True  # False 時，selected_limit_up_stocks 會強制視為空陣列
ENABLE_LIMIT_DOWN_STRATEGY = True  # False 時，selected_limit_down_stocks 會強制視為空陣列

OPTIMIZE_LOSS_PER_LOWER = 2.0 # lower 停損百分比(%)，例如 3.0 代表入場價加上 3%
OPTIMIZE_PROFIT_PER_LOWER = 6.0 # lower 停利百分比(%)，例如 5.0 代表入場價減去 5%

OPTIMIZE_LOSS_PER_FOLLOW = 2.0 # follow 停損百分比(%)
OPTIMIZE_PROFIT_PER_FOLLOW = 6.0 # follow 停利百分比(%)

OPTIMIZE_LOSS_PER_CHANCE = 2.0 # chance 停損百分比(%)
OPTIMIZE_PROFIT_PER_CHANCE = 5.0 # chance 停利百分比(%)

SHORT_VOLUME_SUMMARY_CANDLES = 5 # VOLUME_DOWN 計算開盤前 N 根一分鐘 K 棒交易量
SHORT_VOLUME_RATIO_THRESHOLD = 0.4 # 本日前 N 根量須小於或等於前一日前 N 根量的此倍數
MIN_MINUTE_BARS_BEFORE_0930 = 20 # 前一日 09:30 前至少須有此數量的一分鐘 K 棒
VOLUME_DOWN_EVALUATION_TIME = (9, 5, 10) # 稍晚取得 09:00~09:05 K 棒，避免 09:04 資料尚未完成
OPTIMIZE_LOSS_PER_VOLUME_DOWN = 2.0 # volume_down 停損百分比(%)
OPTIMIZE_PROFIT_PER_VOLUME_DOWN = 8.0 # volume_down 動態追蹤停利首次目標百分比(%)

OPTIMIZE_LOSS_PER_LIMIT_DOWN = 2.0 # limit down 停損百分比(%)
OPTIMIZE_PROFIT_PER_LIMIT_DOWN = 10.0 # limit down 停利百分比(%)
OPTIMIZE_LOSS_PER_LIMIT_UP = 2.0 # limit up 停損百分比(%)
OPTIMIZE_PROFIT_PER_LIMIT_UP = 10.0 # limit up 停利百分比(%)

PROTECT_LOSS_PER = 1.5 # 獲利保護後的新停損百分比
PROTECT_PROFIT_PER = 2.5 # 觸發調整停利百分比

REALTIME_QUOTE_START_TIME = (9, 3)  # 09:10 後才開始抓個股即時行情，避開開盤初期 quote 欄位不完整

MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME = (9, 6)  # 指數位於昨收兩側的 NO_TRADE 檢查起始時間（含）

STRATEGY_EARLY_BREAKOUT_DEADLINE = (9, 16)  # IX0001 早盤須先突破任一方向的截止時間（含）

STRATEGY_DECISION = (9, 43)  # 市場模式判斷截止時間，不含此時間

ENTRY_CHECK_START_TIME_LOWER = (9, 44)  # lower 進場檢核開始時間（含）
ENTRY_CHECK_START_TIME_FOLLOW = (9, 44)  # follow 進場檢核開始時間（含）
ENTRY_CHECK_START_TIME_CHANCE = (9, 44)  # chance 進場檢核開始時間（含）
ENTRY_CHECK_START_TIME_LIMIT_DOWN = (9, 40)  # limit down 進場檢核開始時間（含）
ENTRY_CHECK_START_TIME_LIMIT_UP = (9, 40)  # limit up 進場檢核開始時間（含）

ENTRY_CHECK_END_TIME_LOWER = (10, 1)  # lower 進場檢核截止時間（含）
ENTRY_CHECK_END_TIME_FOLLOW = (10, 1)  # follow 進場檢核截止時間（含）
ENTRY_CHECK_END_TIME_CHANCE = (10, 1)  # chance 進場檢核截止時間（含）
ENTRY_CHECK_END_TIME_LIMIT_DOWN = (10, 1)  # limit down 進場檢核截止時間（含）
ENTRY_CHECK_END_TIME_LIMIT_UP = (10, 1)  # limit up 進場檢核截止時間（含）

FORCE_CLOSE_TIME_LOWER = (13, 0)  # lower 收盤前強制平倉時間
FORCE_CLOSE_TIME_FOLLOW = (13, 0)  # follow 收盤前強制平倉時間
FORCE_CLOSE_TIME_CHANCE = (13, 0)  # chance 收盤前強制平倉時間
FORCE_CLOSE_TIME_VOLUME_DOWN = (13, 0) # volume_down 收盤前強制平倉時間
FORCE_CLOSE_TIME_LIMIT_DOWN = (13, 0)  # limit down 收盤前強制平倉時間
FORCE_CLOSE_TIME_LIMIT_UP = (13, 0)  # limit up 收盤前強制平倉時間

ENTRY_ORDER_QUANTITY_LOWER = 1 # lower 每次進場下單數量
ENTRY_ORDER_QUANTITY_FOLLOW = 1 # follow 每次進場下單數量
ENTRY_ORDER_QUANTITY_CHANCE = 1 # chance 每次進場下單數量
ENTRY_ORDER_QUANTITY_VOLUME_DOWN = 1 # volume_down 每次進場下單數量
ENTRY_ORDER_QUANTITY_LIMIT_DOWN = 1 # limit down 每次進場下單數量
ENTRY_ORDER_QUANTITY_LIMIT_UP = 1 # limit up 每次進場下單數量

LOWER_ENTRY_RANGE_START_PERCENT = 10.0 # lower 入場價距昨收到跌停的起始百分比
LOWER_ENTRY_RANGE_END_PERCENT = 60.0 # lower 入場價距昨收到跌停的結束百分比

FOLLOW_ENTRY_RANGE_START_PERCENT = 0.0 # follow 入場價距昨收到漲停的起始百分比
FOLLOW_ENTRY_RANGE_END_PERCENT = 25.0 # follow 入場價距昨收到漲停的結束百分比
LIMIT_UP_ENTRY_AVG_BELOW_PREVIOUS_CLOSE_PERCENT = 0.3 # limit up 入場時 avg 需低於昨收此百分比

IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.2 # IX0001 啟動門檻：STRATEGY_DECISION 前（不含此時間）low 需低於前日最後 close 的百分比
IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.6 # IX0001 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內
IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.0 # IX0043 啟動門檻：STRATEGY_DECISION 前（不含此時間）low 需低於前日最後 close 的百分比
IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.0 # IX0043 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內

IX0001_STRATEGY_DECISION_RAISE_PERCENT_FOLLOW = 1.2 # IX0001 啟動門檻：STRATEGY_DECISION 前 high 需高於前日最後 close 的百分比
IX0001_STRATEGY_DECISION_DECLINE_PERCENT_FOLLOW = 1.15 # IX0001 回跌失效門檻：突破後 low 不可回到前日最後 close 上方此百分比內
IX0043_STRATEGY_DECISION_RAISE_PERCENT_FOLLOW = 1.2 # IX0043 啟動門檻：STRATEGY_DECISION 前 high 需高於前日最後 close 的百分比
IX0043_STRATEGY_DECISION_DECLINE_PERCENT_FOLLOW = 0.8 # IX0043 回跌失效門檻：突破後 low 不可回到前日最後 close 上方此百分比內
IX0001_FOLLOW_REGRESSION_MIN_GRADIENT = 0 # IX0001 follow 成立門檻：STRATEGY_DECISION 前 close regression EMA 斜率需大於此值
EMA_SPAN_MULTIPLIER = 2.0 # regression EMA 權重參數，1.5 ~ 3.0 預設 2.0，值越大代表對於較近的價格不敏感

IX0001_CHANCE_MAX_RANGE_PERCENT = 1.0 # chance 門檻：IX0001 開盤至 STRATEGY_DECISION 前 high-low 價差不可超過昨收此百分比
CHANCE_PREVIOUS_CLOSE_CROSSING_LIMIT = 3 # chance 門檻：任一市場指數即時值上下穿越昨收達此次數即不交易

# 產業盤勢過濾：原策略入場條件成立後，產業指數當下價格不可與策略方向相反。
INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT = 0 # lower 入場條件成立後，產業指數即時值不可高於昨收指數上漲此百分比後的位置
INDUSTRY_MARKET_FILTER_MIN_DOWN_PERCENT = 0 # follow 入場條件成立後，產業指數即時值必須嚴格大於昨收指數下跌此百分比後的位置

PROFIT_BACK_PERCENT = 0.5 # 獲利後允許回撤百分比
PROFIT_TARGET_PERCENT = 1.0 # 逐步獲利目標百分比

BROKERAGE_FEE_RATE = 0.001425 # 台股手續費率，買賣雙邊皆收
SELL_TRANSACTION_TAX_RATE = 0.003 # 台股交易稅率，賣出時收

ORDER_RESULTS_UPDATE_SECONDS = 10.0  # 成交價量校正查詢間隔，避免每筆下單後立即查詢造成交易 API 連線異常

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
ENTRY_MODE_FOLLOW = 1
ENTRY_MODE_LOWER = 2
ENTRY_MODE_CHANCE = 3
stock_data.entry_mode = ENTRY_MODE_NO_TRADE

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
        print(f"[ERROR] {symbol_code_with_suf} : {priceInfo} {action_type} x {quantity} - {trade_type} - {e}")
        return None

    if not isinstance(order_response, dict):
        print(
            f"[ERROR] {symbol_code_with_suf} 下單回傳格式異常："
            f"{type(order_response)!r} {order_response!r}"
        )
        return None

    ret_code = str(order_response.get("ret_code", "") or "").strip()
    ret_msg = str(order_response.get("ret_msg", "") or "").strip()
    order_no = str(order_response.get("ord_no", "") or "").strip()
    if ret_code != "000000" or not order_no:
        print(
            f"[ERROR] {symbol_code_with_suf} 下單未成功："
            f"ret_code={ret_code!r} ret_msg={ret_msg!r} "
            f"ord_no={order_no!r} response={order_response!r}"
        )
        return None

    print(
        f"[ORDER] {symbol_code_with_suf} : {priceInfo} {action_type} x {quantity} "
        f"- {trade_type} - ord_no={order_no} response={order_response!r}"
    )

    return order_no


def start_trade_report_stream(sdk: SDK) -> threading.Thread:
    """註冊交易主動回報，並在背景執行阻塞式 connect_websocket()。"""

    @sdk.on("error")
    def on_trade_error(error):
        print(f"[TRADE_WS_ERROR] {error!r}", file=sys.stderr)

    @sdk.on("dealt")
    def on_dealt(data):
        # 目前先保留原始成交回報，尚不以它更新策略狀態。
        print(f"[DEALT_REPORT] {data!r}")

    def websocket_worker():
        try:
            sdk.connect_websocket()
            print("[WARN] 交易回報 WebSocket 已結束", file=sys.stderr)
        except Exception as exc:
            print(f"[TRADE_WS_ERROR] WebSocket 連線失敗：{exc}", file=sys.stderr)

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

    symbol_can_buy_day_trade = stock_intra_ticker.get('canBuyDayTrade', False)

    return limit_up_price, limit_down_price, symbol_can_buy_day_trade


def now_tpe() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))


def parse_event_time_or_now(event_time: Any, fallback_dt: datetime | None = None) -> datetime:
    fallback_dt = fallback_dt or now_tpe()
    if event_time in (None, ""):
        return fallback_dt

    try:
        if isinstance(event_time, (int, float)):
            timestamp = float(event_time)
        else:
            text = str(event_time).strip()
            if not text:
                return fallback_dt
            if text.replace(".", "", 1).isdigit():
                timestamp = float(text)
            else:
                event_dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if event_dt.tzinfo is None:
                    return TZ.localize(event_dt)
                return event_dt.astimezone(TZ)

        if timestamp > 10_000_000_000_000:
            timestamp /= 1_000_000
        elif timestamp > 10_000_000_000:
            timestamp /= 1_000
        return datetime.fromtimestamp(timestamp, TZ)
    except (TypeError, ValueError, OSError):
        return fallback_dt


def hhmm_text(hour: int, minute: int) -> str:
    return f"{hour:02d}:{minute:02d}"


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
    entry_start_chance_hm = ENTRY_CHECK_START_TIME_CHANCE[0] * 60 + ENTRY_CHECK_START_TIME_CHANCE[1]
    entry_end_chance_hm = ENTRY_CHECK_END_TIME_CHANCE[0] * 60 + ENTRY_CHECK_END_TIME_CHANCE[1]
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
    if not (0 <= entry_start_chance_hm <= 23 * 60 + 59):
        raise ValueError("ENTRY_CHECK_START_TIME_CHANCE 設定錯誤，需介於 00:00~23:59")
    if not (0 <= entry_end_chance_hm <= 23 * 60 + 59):
        raise ValueError("ENTRY_CHECK_END_TIME_CHANCE 設定錯誤，需介於 00:00~23:59")
    if entry_start_chance_hm >= entry_end_chance_hm:
        raise ValueError("ENTRY_CHECK_START_TIME_CHANCE 必須早於 ENTRY_CHECK_END_TIME_CHANCE")
    if entry_start_chance_hm < strategy_decision_hm:
        raise ValueError("ENTRY_CHECK_START_TIME_CHANCE 不可早於 STRATEGY_DECISION")
    if IX0001_CHANCE_MAX_RANGE_PERCENT < 0:
        raise ValueError("IX0001_CHANCE_MAX_RANGE_PERCENT 不可小於 0")
    if CHANCE_PREVIOUS_CLOSE_CROSSING_LIMIT <= 0:
        raise ValueError("CHANCE_PREVIOUS_CLOSE_CROSSING_LIMIT 必須大於 0")
    if EMA_SPAN_MULTIPLIER <= 0:
        raise ValueError("EMA_SPAN_MULTIPLIER 必須大於 0")
    if SHORT_VOLUME_SUMMARY_CANDLES <= 0:
        raise ValueError("SHORT_VOLUME_SUMMARY_CANDLES 必須大於 0")
    if SHORT_VOLUME_RATIO_THRESHOLD < 0:
        raise ValueError("SHORT_VOLUME_RATIO_THRESHOLD 不可小於 0")
    if MIN_MINUTE_BARS_BEFORE_0930 <= 0:
        raise ValueError("MIN_MINUTE_BARS_BEFORE_0930 必須大於 0")
    if len(VOLUME_DOWN_EVALUATION_TIME) != 3 or not (
        0 <= VOLUME_DOWN_EVALUATION_TIME[0] <= 23
        and 0 <= VOLUME_DOWN_EVALUATION_TIME[1] <= 59
        and 0 <= VOLUME_DOWN_EVALUATION_TIME[2] <= 59
    ):
        raise ValueError("VOLUME_DOWN_EVALUATION_TIME 設定錯誤，需為有效的 (時, 分, 秒)")


def get_entry_mode_text(entry_mode: int | None = None) -> str:
    mode = get_current_entry_mode() if entry_mode is None else entry_mode
    if mode == ENTRY_MODE_NO_TRADE:
        return "NO_TRADE"
    if mode == ENTRY_MODE_FOLLOW:
        return "FOLLOW"
    if mode == ENTRY_MODE_LOWER:
        return "LOWER"
    if mode == ENTRY_MODE_CHANCE:
        return "CHANCE"
    return "UNKNOWN"


def print_close_position_log(state: Dict[str, Any]) -> None:
    print(f'[{state.get("symbol_name")}] {now_tpe().strftime("%H:%M:%S")} 平倉')


def print_entry_position_prices(state: Dict[str, Any]) -> None:
    try:
        entry_price = float(state.get("entry_price", 0))
        flat_price = float(state.get("flat_price", 0))
        profit_price = float(state.get("profit_price", 0))
    except (TypeError, ValueError):
        return

    _protect_loss_per, protect_profit_per = get_protect_loss_profit_percent(state)
    if state.get("side") == TRADE_SIDE_LONG:
        protect_profit_price = entry_price * (1 + protect_profit_per / 100.0)
    else:
        protect_profit_price = entry_price * (1 - protect_profit_per / 100.0)
    print(f"停損：{flat_price:.2f}，保本：{protect_profit_price:.2f}，停利：{profit_price:.2f}")


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
    asks = stock_intraday_quote.get('asks') or []
    best_bid_price = bids[0].get('price') if bids else None
    best_ask_price = asks[0].get('price') if asks else None

    return last_price, open_price, high_price, low_price, close_price, avg_price, best_bid_price, best_ask_price


def parse_minute_candle_datetime(row: Dict[str, Any]) -> datetime | None:
    raw_value = row.get("date")
    if raw_value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(TZ).replace(tzinfo=None)
        return parsed
    except (TypeError, ValueError):
        return None


def get_opening_volume_candles(
    rows: List[Dict[str, Any]],
    target_date: date,
    summary_candles: int,
) -> List[Dict[str, Any]]:
    """取得 09:00 起、統計截止時間前具有有效成交量的分 K。"""
    opening_dt = datetime.combine(target_date, datetime.min.time()).replace(hour=9)
    summary_end_dt = opening_dt + timedelta(minutes=summary_candles)
    volume_rows = []
    for row in rows:
        parsed = parse_minute_candle_datetime(row)
        if parsed is None or not (opening_dt <= parsed < summary_end_dt):
            continue
        try:
            volume = float(row.get("volume"))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(volume) or volume < 0:
            continue
        volume_rows.append(row)
    return volume_rows


def count_minute_candles_before_0930(rows: List[Dict[str, Any]], target_date: date) -> int:
    minute_keys = set()
    for row in rows:
        parsed = parse_minute_candle_datetime(row)
        if parsed is None or parsed.date() != target_date:
            continue
        if (parsed.hour, parsed.minute) < (9, 30):
            minute_keys.add((parsed.hour, parsed.minute))
    return len(minute_keys)


def fetch_previous_minute_candles(stock_id: str, realtime_sdk) -> tuple[date | None, List[Dict[str, Any]]]:
    symbol = stock_id.split(".")[0]
    today = now_tpe().date()
    response = realtime_sdk.rest_client.stock.historical.candles(
        **{
            "symbol": symbol,
            "from": (today - timedelta(days=14)).strftime("%Y-%m-%d"),
            "to": today.strftime("%Y-%m-%d"),
            "timeframe": "1",
        }
    )
    rows = response.get("data", []) if isinstance(response, dict) else []
    dates = sorted({
        parsed.date()
        for row in rows
        if (parsed := parse_minute_candle_datetime(row)) is not None
        and parsed.date() < today
    })
    if not dates:
        return None, []
    previous_date = dates[-1]
    return previous_date, [
        row for row in rows
        if (parsed := parse_minute_candle_datetime(row)) is not None
        and parsed.date() == previous_date
    ]


def prepare_volume_down_yesterday_data(
    states: Dict[str, Dict[str, Any]],
    realtime_sdk,
) -> None:
    """開盤前準備一般個股的前一交易日開盤量；失敗者只放棄 VOLUME_DOWN。"""
    if not ENABLE_VOLUME_DOWN_STRATEGY:
        print("[CONFIG] ENABLE_VOLUME_DOWN_STRATEGY=False，不取得 VOLUME_DOWN 分鐘 K 資料")
        return

    for state in states.values():
        if state.get("strategy_type"):
            continue
        symbol_name = state.get("symbol_name", "UNKNOWN")
        try:
            previous_date, rows = fetch_previous_minute_candles(
                state.get("symbol_code_with_suf", ""),
                realtime_sdk,
            )
            if previous_date is None:
                raise ValueError("找不到前一交易日分鐘 K")
            before_0930_count = count_minute_candles_before_0930(rows, previous_date)
            if before_0930_count < MIN_MINUTE_BARS_BEFORE_0930:
                raise ValueError(
                    f"前一日 09:30 前僅 {before_0930_count} 根，"
                    f"少於 {MIN_MINUTE_BARS_BEFORE_0930} 根"
                )
            opening_rows = get_opening_volume_candles(
                rows,
                previous_date,
                SHORT_VOLUME_SUMMARY_CANDLES,
            )
            if not opening_rows:
                raise ValueError("前一日統計區間內無有效成交量 K")
            previous_volume = sum(float(row["volume"]) for row in opening_rows)
            if previous_volume <= 0:
                raise ValueError("前一日前 N 根交易量為 0")
            state["volume_down_yesterday_date"] = previous_date.isoformat()
            state["volume_down_yesterday_volume"] = previous_volume
            state["volume_down_yesterday_ready"] = True
            print(
                f"[{symbol_name}] VOLUME_DOWN 前日資料完成："
                f"日期={previous_date.isoformat()} 前{SHORT_VOLUME_SUMMARY_CANDLES}根量={previous_volume:,.0f}"
            )
        except Exception as exc:
            state["volume_down_yesterday_ready"] = False
            print(f"[{symbol_name}] ⚠️ VOLUME_DOWN 前日資料不可用：{exc}，將回到一般模式")
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        time.sleep(0.2)


def fetch_today_intraday_candles(stock_id: str, realtime_sdk) -> List[Dict[str, Any]]:
    symbol = stock_id.split(".")[0]
    response = realtime_sdk.rest_client.stock.intraday.candles(symbol=symbol)
    return response.get("data", []) if isinstance(response, dict) else []


def evaluate_volume_down_entries(
    states: Dict[str, Dict[str, Any]],
    mysdk,
    realtime_sdk,
) -> None:
    """於指定時間只執行一次 VOLUME_DOWN 判斷；符合者立即送出市價空單。"""
    if not ENABLE_VOLUME_DOWN_STRATEGY:
        return

    today = now_tpe().date()
    for state in states.values():
        if state.get("strategy_type") or state.get("traded") or state.get("in_position"):
            continue
        if state.get("volume_down_checked"):
            continue

        symbol_name = state.get("symbol_name", "UNKNOWN")
        state["volume_down_checked"] = True
        if not state.get("volume_down_yesterday_ready"):
            print(f"[{symbol_name}] VOLUME_DOWN 前日資料未通過，繼續等待一般模式")
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            continue

        try:
            rows = fetch_today_intraday_candles(
                state.get("symbol_code_with_suf", ""),
                realtime_sdk,
            )
            opening_rows = get_opening_volume_candles(
                rows,
                today,
                SHORT_VOLUME_SUMMARY_CANDLES,
            )
            if not opening_rows:
                raise ValueError("本日統計區間內無有效成交量 K")

            today_volume = sum(
                float(row["volume"])
                for row in opening_rows
            )
            yesterday_volume = float(state.get("volume_down_yesterday_volume", 0) or 0)
            if yesterday_volume <= 0:
                raise ValueError("前一日前 N 根交易量無效")
            volume_ratio = today_volume / yesterday_volume
            state["volume_down_today_volume"] = today_volume
            state["volume_down_volume_ratio"] = volume_ratio

            if today_volume > yesterday_volume * SHORT_VOLUME_RATIO_THRESHOLD:
                print(
                    f"[{symbol_name}] VOLUME_DOWN 不成立："
                    f"前日量={yesterday_volume:,.0f} 本日量={today_volume:,.0f} "
                    f"比例={volume_ratio:.4f} > 門檻={SHORT_VOLUME_RATIO_THRESHOLD:.4f}，"
                    "繼續等待一般模式"
                )
                atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
                continue

            try:
                entry_trigger_price = float(state.get("last_price"))
            except (TypeError, ValueError):
                entry_trigger_price = 0.0
            if not math.isfinite(entry_trigger_price) or entry_trigger_price <= 0:
                raise ValueError("無有效即時價格 last_price")

            state["strategy_type"] = STRATEGY_VOLUME_DOWN
            state["volume_down_qualified"] = True
            state["side"] = TRADE_SIDE_SHORT
            state["qty"] = ENTRY_ORDER_QUANTITY_VOLUME_DOWN
            state["entry_order_qty"] = ENTRY_ORDER_QUANTITY_VOLUME_DOWN
            state["entry_trigger_price"] = entry_trigger_price
            state["volume_down_entry_bar_time"] = now_tpe().isoformat()
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            print(
                f"[{symbol_name}] VOLUME_DOWN 成立："
                f"前日量={yesterday_volume:,.0f} 本日量={today_volume:,.0f} "
                f"比例={volume_ratio:.4f} <= 門檻={SHORT_VOLUME_RATIO_THRESHOLD:.4f}，"
                f"last_price={entry_trigger_price:.2f}，送出市價放空"
            )
            try_open_position(state, mysdk)
        except Exception as exc:
            if is_volume_down_strategy(state):
                print(
                    f"[{symbol_name}] ⚠️ VOLUME_DOWN 已成立但執行失敗：{exc}，"
                    "本日不轉入其他模式"
                )
            else:
                print(
                    f"[{symbol_name}] ⚠️ VOLUME_DOWN 本日資料不可用：{exc}，"
                    "繼續等待一般模式"
                )
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        time.sleep(0.2)


def state_needs_entry_fill_update(state: Dict[str, Any]) -> bool:
    if not state.get("in_position") or state.get("traded"):
        return False
    try:
        entry_order_qty = int(state.get("entry_order_qty", state.get("qty", 0)) or 0)
        entry_filled_qty = int(state.get("entry_filled_qty", 0) or 0)
    except (TypeError, ValueError):
        return False
    return entry_order_qty > 0 and entry_filled_qty < entry_order_qty


def get_order_fill_info_by_results(state: Dict[str, Any], order_results: List[Dict[str, Any]]) -> tuple[float, int]:
    symbol_code = str(state.get("symbol_code", "") or "").strip()
    if not symbol_code:
        return 0.0, 0

    matched_results = []
    for item in order_results:
        item_stock_no = str(item.get("stock_no", "") or "").strip()
        if item_stock_no == symbol_code:
            matched_results.append(item)

    filled_results = []
    for item in matched_results:
        try:
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
    candidates = [st for st in states.values() if state_needs_entry_fill_update(st)]
    if not candidates:
        return

    try:
        order_results = sdk.get_order_results()
    except Exception as exc:
        print(f"[WARN] get_order_results() 成交價量校正失敗：{exc}", file=sys.stderr)
        return

    if not isinstance(order_results, list):
        print(f"[WARN] get_order_results() 回傳格式異常：{type(order_results)!r} {order_results!r}", file=sys.stderr)
        return

    for state in candidates:
        avg_price, mat_qty = get_order_fill_info_by_results(state, order_results)
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
        state["entry_price_source"] = "order_results"
        state["entry_fill_update_time"] = now_tpe().isoformat()

        if not state.get("profit_tracking_active"):
            recalc_entry_position_prices(state)
        else:
            print(f"[{state.get('symbol_name')}] 已啟動追蹤停利，成交價量校正不覆寫既有停利軌跡")

        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        fill_text = "完整成交" if state.get("entry_fully_filled") else "部分成交"
        print(
            f"[{state.get('symbol_name')}] get_order_results() 更新入場成交："
            f"{fill_text} avg_price={avg_price} mat_qty={mat_qty}/{entry_order_qty}"
        )
        print_entry_position_prices(state)


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


def get_strategy_decision_raise_percent(index_key: str) -> float | None:
    if index_key == "TWSE:MARKET":
        return IX0001_STRATEGY_DECISION_RAISE_PERCENT_FOLLOW
    if index_key == "TPEX:MARKET":
        return IX0043_STRATEGY_DECISION_RAISE_PERCENT_FOLLOW
    return None


def get_strategy_decision_decline_percent(index_key: str) -> float | None:
    if index_key == "TWSE:MARKET":
        return IX0001_STRATEGY_DECISION_DECLINE_PERCENT_FOLLOW
    if index_key == "TPEX:MARKET":
        return IX0043_STRATEGY_DECISION_DECLINE_PERCENT_FOLLOW
    return None


def update_chance_previous_close_crossing_state(
    market_key: str,
    index_value: float,
    event_time: Any,
) -> None:
    """依 WebSocket 即時指數換邊次數，獨立更新 CHANCE 的昨收穿越限制。"""
    if market_key not in MARKET_GATE_INDEX_KEYS:
        return

    now_local = now_tpe()
    event_dt = parse_event_time_or_now(event_time, now_local)
    event_hm = event_dt.hour * 60 + event_dt.minute
    reversal_start_hm = (
        MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
        + MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
    )
    decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    if event_hm < reversal_start_hm or event_hm >= decision_hm:
        return

    previous_close = market_previous_close_indices.get(market_key, {}).get("previous_close")
    try:
        index_value_float = float(index_value)
        previous_close_float = float(previous_close)
    except (TypeError, ValueError):
        return
    if index_value_float <= 0 or previous_close_float <= 0:
        return

    # 等於昨收時視為中性過渡：不計數，也不改變最後明確側別。
    if index_value_float < previous_close_float:
        current_side = "BELOW"
    elif index_value_float > previous_close_float:
        current_side = "ABOVE"
    else:
        return

    market_state = MARKET_INDEX_STATE.setdefault(market_key, {})
    if market_state.get("chance_previous_close_crossing_blocked"):
        return

    previous_side = market_state.get("chance_previous_close_last_side")
    if previous_side is None:
        market_state["chance_previous_close_last_side"] = current_side
        market_state["chance_previous_close_last_side_time"] = event_time or event_dt.isoformat()
        return
    if current_side == previous_side:
        return

    crossing_count = int(market_state.get("chance_previous_close_crossing_count", 0)) + 1
    market_state["chance_previous_close_crossing_count"] = crossing_count
    market_state["chance_previous_close_last_side"] = current_side
    market_state["chance_previous_close_last_side_time"] = event_time or event_dt.isoformat()
    market_state["chance_previous_close_last_crossing_time"] = event_time or event_dt.isoformat()
    if crossing_count >= CHANCE_PREVIOUS_CLOSE_CROSSING_LIMIT:
        market_state["chance_previous_close_crossing_blocked"] = True
        market_state["chance_previous_close_crossing_blocked_time"] = event_time or event_dt.isoformat()
        print(
            f"[MODE] {now_local.strftime('%H:%M:%S')} {market_key} "
            f"即時指數上下穿越昨收已達 {crossing_count} 次，CHANCE 將不成立"
        )


def update_market_strategy_decision_gate_state(market_key: str, index_value: float, event_time: Any) -> None:
    if market_key not in MARKET_GATE_INDEX_KEYS:
        return

    now_local = now_tpe()
    if (now_local.hour, now_local.minute) >= STRATEGY_DECISION:
        return

    index_config = market_previous_close_indices.get(market_key, {})
    drop_percent = get_strategy_decision_drop_percent(market_key)
    rebound_percent = get_strategy_decision_rebound_percent(market_key)
    raise_percent = get_strategy_decision_raise_percent(market_key)
    decline_percent = get_strategy_decision_decline_percent(market_key)
    previous_close = index_config.get("previous_close")
    try:
        previous_close_float = float(previous_close)
    except (TypeError, ValueError):
        return
    if previous_close_float <= 0:
        return

    market_state = MARKET_INDEX_STATE.setdefault(market_key, {})
    now_hm = now_local.hour * 60 + now_local.minute
    decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    event_dt = parse_event_time_or_now(event_time, now_local)
    event_hm = event_dt.hour * 60 + event_dt.minute
    if (
        market_key == "TWSE:MARKET"
        and 9 * 60 <= event_hm < decision_hm
    ):
        market_state.setdefault("follow_regression_minute_closes", {})[event_hm] = {
            "dt": event_dt,
            "close": index_value,
        }

    if (
        market_key == "TWSE:MARKET"
        and 9 * 60 <= now_hm < decision_hm
    ):
        previous_chance_high = market_state.get("chance_decision_high")
        previous_chance_low = market_state.get("chance_decision_low")
        if previous_chance_high is None or index_value > float(previous_chance_high):
            market_state["chance_decision_high"] = index_value
            market_state["chance_decision_high_time"] = event_time or now_local.isoformat()
        if previous_chance_low is None or index_value < float(previous_chance_low):
            market_state["chance_decision_low"] = index_value
            market_state["chance_decision_low_time"] = event_time or now_local.isoformat()

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
                "將於 STRATEGY_DECISION fallback 檢查 CHANCE/NO_TRADE"
            )

    if (
        drop_percent is None
        or rebound_percent is None
        or raise_percent is None
        or decline_percent is None
    ):
        return

    drop_threshold = previous_close_float * (1 - drop_percent / 100.0)
    rebound_threshold = previous_close_float * (1 - rebound_percent / 100.0)
    raise_threshold = previous_close_float * (1 + raise_percent / 100.0)
    decline_threshold = previous_close_float * (1 + decline_percent / 100.0)

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
        if index_value > raise_threshold:
            market_state["early_breakout_passed"] = True
            market_state["early_breakout_side"] = "UP"
            market_state["early_breakout_time"] = event_time or now_local.isoformat()
        elif index_value < drop_threshold:
            market_state["early_breakout_passed"] = True
            market_state["early_breakout_side"] = "DOWN"
            market_state["early_breakout_time"] = event_time or now_local.isoformat()

    if index_value < drop_threshold:
        if (
            (not market_state.get("strategy_decision_broken"))
            or market_state.get("strategy_decision_rebound_blocked")
        ):
            market_state["strategy_decision_break_time"] = event_time or now_local.isoformat()
        market_state["strategy_decision_broken"] = True
        market_state["strategy_decision_rebound_blocked"] = False
        market_state.pop("strategy_decision_rebound_time", None)

    if (
        market_state.get("strategy_decision_broken")
        and (not market_state.get("strategy_decision_rebound_blocked"))
        and index_value >= rebound_threshold
    ):
        market_state["strategy_decision_rebound_blocked"] = True
        market_state["strategy_decision_rebound_time"] = event_time or now_local.isoformat()

    if index_value > raise_threshold:
        if not market_state.get("strategy_decision_raised"):
            market_state["strategy_decision_raise_time"] = event_time or now_local.isoformat()
        market_state["strategy_decision_raised"] = True

    if (
        market_state.get("strategy_decision_raised")
        and (not market_state.get("strategy_decision_decline_blocked"))
        and index_value <= decline_threshold
    ):
        market_state["strategy_decision_decline_blocked"] = True
        market_state["strategy_decision_decline_time"] = event_time or now_local.isoformat()
        print(
            f"[MODE] {now_local.strftime('%H:%M:%S')} {market_key} 指數 FOLLOW 突破後回跌，"
            "FOLLOW 模式不成立；其餘模式將於判斷時間依原規則決定"
        )


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
    if entry_mode == ENTRY_MODE_LOWER:
        strategy_type = STRATEGY_LOWER
        start_time = ENTRY_CHECK_START_TIME_LOWER
        end_time = FORCE_CLOSE_TIME_LOWER
        threshold_percent = get_strategy_decision_rebound_percent(market_key)
        comparison_text = ">="
    elif entry_mode == ENTRY_MODE_FOLLOW:
        strategy_type = STRATEGY_FOLLOW
        start_time = ENTRY_CHECK_START_TIME_FOLLOW
        end_time = FORCE_CLOSE_TIME_FOLLOW
        threshold_percent = get_strategy_decision_decline_percent(market_key)
        comparison_text = "<="
    else:
        return

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

    if strategy_type == STRATEGY_LOWER:
        threshold = previous_close_float * (1 - threshold_percent / 100.0)
        triggered = index_value_float >= threshold
    else:
        threshold = previous_close_float * (1 + threshold_percent / 100.0)
        triggered = index_value_float <= threshold
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


def get_market_strategy_decision_gate_result(index_key: str) -> Dict[str, Any]:
    index_config = market_previous_close_indices.get(index_key, {})
    drop_percent = get_strategy_decision_drop_percent(index_key)
    rebound_percent = get_strategy_decision_rebound_percent(index_key)
    raise_percent = get_strategy_decision_raise_percent(index_key)
    decline_percent = get_strategy_decision_decline_percent(index_key)
    previous_close = index_config.get("previous_close")
    market_state = MARKET_INDEX_STATE.get(index_key, {})

    result = {
        "index_key": index_key,
        "symbol": index_config.get("symbol"),
        "name": index_config.get("name"),
        "previous_close": previous_close,
        "last_index": market_state.get("last_index"),
        "drop_threshold": None,
        "rebound_threshold": None,
        "raise_threshold": None,
        "decline_threshold": None,
        "broken": bool(market_state.get("strategy_decision_broken")),
        "rebound_blocked": bool(market_state.get("strategy_decision_rebound_blocked")),
        "raised": bool(market_state.get("strategy_decision_raised")),
        "decline_blocked": bool(market_state.get("strategy_decision_decline_blocked")),
        "break_time": market_state.get("strategy_decision_break_time"),
        "rebound_time": market_state.get("strategy_decision_rebound_time"),
        "raise_time": market_state.get("strategy_decision_raise_time"),
        "decline_time": market_state.get("strategy_decision_decline_time"),
        "previous_close_traded_above": bool(market_state.get("previous_close_traded_above")),
        "previous_close_traded_below": bool(market_state.get("previous_close_traded_below")),
        "previous_close_reversal_blocked": bool(market_state.get("previous_close_reversal_blocked")),
        "previous_close_reversal_time": market_state.get("previous_close_reversal_time"),
        "early_breakout_passed": bool(market_state.get("early_breakout_passed")),
        "early_breakout_side": market_state.get("early_breakout_side"),
        "early_breakout_time": market_state.get("early_breakout_time"),
        "chance_decision_high": market_state.get("chance_decision_high"),
        "chance_decision_low": market_state.get("chance_decision_low"),
        "chance_decision_high_time": market_state.get("chance_decision_high_time"),
        "chance_decision_low_time": market_state.get("chance_decision_low_time"),
        "chance_previous_close_crossing_count": int(
            market_state.get("chance_previous_close_crossing_count", 0)
        ),
        "chance_previous_close_crossing_blocked": bool(
            market_state.get("chance_previous_close_crossing_blocked")
        ),
        "chance_previous_close_crossing_blocked_time": market_state.get(
            "chance_previous_close_crossing_blocked_time"
        ),
        "follow_regression_gradient": None,
        "follow_regression_min_gradient": (
            IX0001_FOLLOW_REGRESSION_MIN_GRADIENT
            if index_key == "TWSE:MARKET"
            else None
        ),
        "follow_regression_count": 0,
        "passed": False,
        "lower_passed": False,
        "follow_passed": False,
        "chance_passed": False,
        "final_above_previous_close": False,
        "final_below_previous_close": False,
        "lower_reason": "",
        "follow_reason": "",
        "chance_reason": "",
    }

    if not index_config.get("symbol"):
        result["lower_reason"] = "stock_data.py 缺少市場指數設定"
        result["follow_reason"] = "stock_data.py 缺少市場指數設定"
        return result

    try:
        previous_close_float = float(previous_close)
    except (TypeError, ValueError):
        result["lower_reason"] = "previous_close 無效"
        result["follow_reason"] = "previous_close 無效"
        return result

    if (
        previous_close_float <= 0
        or drop_percent is None
        or rebound_percent is None
        or raise_percent is None
        or decline_percent is None
    ):
        result["lower_reason"] = "門檻設定無效"
        result["follow_reason"] = "門檻設定無效"
        return result

    result["drop_threshold"] = previous_close_float * (1 - drop_percent / 100.0)
    result["rebound_threshold"] = previous_close_float * (1 - rebound_percent / 100.0)
    result["raise_threshold"] = previous_close_float * (1 + raise_percent / 100.0)
    result["decline_threshold"] = previous_close_float * (1 + decline_percent / 100.0)

    if result["last_index"] is None:
        result["lower_reason"] = "尚未收到 websocket 指數資料"
        result["follow_reason"] = "尚未收到 websocket 指數資料"
        return result

    last_index_float = float(result["last_index"])
    result["final_above_previous_close"] = last_index_float > previous_close_float
    result["final_below_previous_close"] = last_index_float < previous_close_float
    if index_key == "TWSE:MARKET":
        minute_close_map = market_state.get("follow_regression_minute_closes", {})
        regression_bars = [
            row
            for _minute_hm, row in sorted(minute_close_map.items())
            if row.get("close") is not None
        ]
        result["follow_regression_count"] = len(regression_bars)
        result["follow_regression_gradient"] = calculate_regression_gradient(regression_bars)

    if not result["broken"]:
        result["lower_reason"] = "未跌破啟動門檻"
    elif last_index_float >= result["rebound_threshold"]:
        result["lower_reason"] = "決策時已反彈至失效門檻"
    elif not result["final_below_previous_close"]:
        result["lower_reason"] = "決策時未在昨收下方"
    else:
        result["lower_passed"] = True
        result["lower_reason"] = "通過"

    if not result["raised"]:
        result["follow_reason"] = "未突破啟動門檻"
    elif result["decline_blocked"]:
        result["follow_reason"] = "突破後曾回跌至失效門檻"
    elif last_index_float <= result["decline_threshold"]:
        result["follow_reason"] = "決策時已回跌至失效門檻"
    elif not result["final_above_previous_close"]:
        result["follow_reason"] = "決策時未在昨收上方"
    elif (
        index_key == "TWSE:MARKET"
        and (
            result["follow_regression_gradient"] is None
            or result["follow_regression_gradient"] <= IX0001_FOLLOW_REGRESSION_MIN_GRADIENT
        )
    ):
        result["follow_reason"] = (
            "IX0001 regression EMA 斜率未達門檻 "
            f"gradient={result['follow_regression_gradient']} "
            f"> {IX0001_FOLLOW_REGRESSION_MIN_GRADIENT}"
        )
    else:
        result["follow_passed"] = True
        result["follow_reason"] = "通過"

    result["passed"] = result["lower_passed"]
    return result


def get_chance_gate_result(gate_results: list[Dict[str, Any]]) -> tuple[bool, str]:
    crossing_blocked_result = next(
        (
            result
            for result in gate_results
            if result.get("chance_previous_close_crossing_blocked")
        ),
        None,
    )
    if crossing_blocked_result is not None:
        return (
            False,
            f"{crossing_blocked_result['index_key']} 即時指數上下穿越昨收 "
            f"{crossing_blocked_result['chance_previous_close_crossing_count']} 次",
        )

    ix0001_result = next(
        (result for result in gate_results if result["index_key"] == "TWSE:MARKET"),
        None,
    )
    if ix0001_result is None:
        return False, "缺少 IX0001 gate 結果"

    try:
        previous_close = float(ix0001_result.get("previous_close"))
        chance_high = float(ix0001_result.get("chance_decision_high"))
        chance_low = float(ix0001_result.get("chance_decision_low"))
    except (TypeError, ValueError):
        return False, "IX0001 chance 資料不足"

    if previous_close <= 0:
        return False, "IX0001 昨收無效"

    max_allowed_range = previous_close * (IX0001_CHANCE_MAX_RANGE_PERCENT / 100.0)
    actual_range = chance_high - chance_low
    if actual_range > max_allowed_range:
        return (
            False,
            f"IX0001 決策前波動過大 range={actual_range:.2f} > {max_allowed_range:.2f}",
        )

    return True, "通過"


def decide_entry_mode_by_market_gate() -> tuple[int, list[Dict[str, Any]]]:
    gate_results = [
        get_market_strategy_decision_gate_result(index_key)
        for index_key in MARKET_GATE_INDEX_KEYS
    ]

    ix0001_result = next(
        (result for result in gate_results if result["index_key"] == "TWSE:MARKET"),
        None,
    )
    def fallback_chance_or_no_trade(reason: str) -> tuple[int, list[Dict[str, Any]]]:
        chance_passed, chance_reason = get_chance_gate_result(gate_results)
        if ix0001_result is not None:
            ix0001_result["chance_passed"] = chance_passed
            ix0001_result["chance_reason"] = chance_reason
        if chance_passed:
            print(f"[MODE] {reason}，fallback 判定為 CHANCE")
            return ENTRY_MODE_CHANCE, gate_results
        print(f"[MODE] {reason}，CHANCE 不成立：{chance_reason}")
        return ENTRY_MODE_NO_TRADE, gate_results

    if ix0001_result is None or not ix0001_result.get("early_breakout_passed"):
        print(
            "[MODE] IX0001 未於早盤截止時間前突破任一方向 "
            f"(截止 {STRATEGY_EARLY_BREAKOUT_DEADLINE[0]:02d}:"
            f"{STRATEGY_EARLY_BREAKOUT_DEADLINE[1]:02d})"
        )
        return fallback_chance_or_no_trade("IX0001 未於早盤截止時間前突破任一方向")

    reversal_blocked = any(result["previous_close_reversal_blocked"] for result in gate_results)
    if reversal_blocked:
        print(
            "[MODE] 上市或上櫃指數在指定時段曾位於昨收上下兩側 "
            f"({MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0]:02d}:"
            f"{MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]:02d}~"
            f"{STRATEGY_DECISION[0]:02d}:{STRATEGY_DECISION[1]:02d})"
        )
        return fallback_chance_or_no_trade("上市或上櫃指數在指定時段曾位於昨收上下兩側")

    follow_decline_blocked = any(result["decline_blocked"] for result in gate_results)
    if follow_decline_blocked:
        return fallback_chance_or_no_trade("上市或上櫃指數 FOLLOW 突破後曾回跌")

    follow_mode_passed = all(result["follow_passed"] for result in gate_results)
    lower_mode_passed = all(result["lower_passed"] for result in gate_results)

    if follow_mode_passed and lower_mode_passed:
        print("[WARN] STRATEGY_DECISION 同時符合 FOLLOW 與 LOWER，視為資料異常，今日 NO_TRADE")
        return ENTRY_MODE_NO_TRADE, gate_results
    if follow_mode_passed:
        return ENTRY_MODE_FOLLOW, gate_results
    if lower_mode_passed:
        return ENTRY_MODE_LOWER, gate_results
    return fallback_chance_or_no_trade("LOWER/FOLLOW 未成立")


def stop_if_ix0001_early_breakout_missing() -> bool:
    return False


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


def print_entry_mode_decision(entry_mode: int, gate_results: list[Dict[str, Any]]) -> None:
    mode_text = get_entry_mode_text(entry_mode)
    print(f"[MODE] STRATEGY_DECISION 模式判斷：{mode_text}")
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
            f"break_time={format_market_gate_time(result.get('break_time'))} "
            f"rebound_time={format_market_gate_time(result.get('rebound_time'))} "
            f"lower_passed={result.get('lower_passed')}"
        )
    for result in gate_results:
        print(
            f"[MODE] {result['index_key']} {result.get('symbol') or ''} "
            f"previous_close={format_market_gate_value(result.get('previous_close'))} "
            f"raise_threshold={format_market_gate_value(result.get('raise_threshold'))} "
            f"decline_threshold={format_market_gate_value(result.get('decline_threshold'))} "
            f"last_index={format_market_gate_value(result.get('last_index'))} "
            f"raise_time={format_market_gate_time(result.get('raise_time'))} "
            f"decline_time={format_market_gate_time(result.get('decline_time'))} "
            f"follow_regression_gradient={result.get('follow_regression_gradient') if result['index_key'] == 'TWSE:MARKET' else '--'} "
            f"follow_regression_min={result.get('follow_regression_min_gradient') if result['index_key'] == 'TWSE:MARKET' else '--'} "
            f"follow_regression_count={result.get('follow_regression_count') if result['index_key'] == 'TWSE:MARKET' else '--'} "
            f"follow_passed={result.get('follow_passed')} "
            f"follow_reason={result.get('follow_reason') or '--'}"
        )
    for result in gate_results:
        if result["index_key"] != "TWSE:MARKET":
            continue
        print(
            f"[MODE] {result['index_key']} {result.get('symbol') or ''} "
            f"chance_high={format_market_gate_value(result.get('chance_decision_high'))} "
            f"chance_low={format_market_gate_value(result.get('chance_decision_low'))} "
            f"chance_high_time={format_market_gate_time(result.get('chance_decision_high_time'))} "
            f"chance_low_time={format_market_gate_time(result.get('chance_decision_low_time'))} "
            f"chance_passed={result.get('chance_passed')} "
            f"chance_reason={result.get('chance_reason') or '--'}"
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
            update_chance_previous_close_crossing_state(market_key, index_float, data.get("time"))
            update_market_strategy_decision_gate_state(market_key, index_float, data.get("time"))
            update_position_market_reversal_state(market_key, index_float, data.get("time"))
        except Exception as e:
            print(f"[WARN] 處理盤勢指數訊息失敗: {e}")

    try:
        stock_ws = realtime_sdk.websocket_client.stock
        stock_ws.on("message", handle_message)
        stock_ws.connect()
        for index_symbol in symbol_to_market_key:
            market_key = symbol_to_market_key[index_symbol]
            index_config = market_previous_close_indices.get(market_key, {})
            stock_ws.subscribe({
                "channel": "indices",
                "symbol": index_symbol,
            })
            print(
                f"[MARKET] 已訂閱盤勢指數 {market_key} "
                f"{index_symbol} {index_config.get('name', '')}"
            )
        return stock_ws
    except Exception as e:
        print(f"[WARN] 啟動盤勢指數 WebSocket 失敗，盤勢濾網將等待指數資料: {e}")
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


def round_half_away_from_zero(value: float) -> int:
    if value >= 0:
        return math.floor(value + 0.5)
    return math.ceil(value - 0.5)


def _extract_close_value(candle: Any) -> float:
    if isinstance(candle, dict):
        return float(candle.get("close", 0.0) or 0.0)
    return float(getattr(candle, "close"))


def calculate_regression_gradient(candles: list[Any]) -> int | None:
    current_window = len(candles)
    if current_window < 2:
        return None

    closes = [_extract_close_value(candle) for candle in candles]
    base_close = closes[0]
    if math.isclose(base_close, 0.0, abs_tol=1e-12):
        return 0

    normalized_closes = [(close / base_close - 1.0) * 100.0 for close in closes]
    x_values = list(range(current_window))
    ema_span = current_window * EMA_SPAN_MULTIPLIER
    alpha = 2.0 / (ema_span + 1.0)
    decay = 1.0 - alpha
    weights = [decay ** (current_window - 1 - i) for i in range(current_window)]
    weight_sum = sum(weights)
    if math.isclose(weight_sum, 0.0, abs_tol=1e-12):
        return 0

    x_avg = sum(w * x for w, x in zip(weights, x_values)) / weight_sum
    y_avg = sum(w * y for w, y in zip(weights, normalized_closes)) / weight_sum

    numerator = sum(w * (x - x_avg) * (y - y_avg) for w, x, y in zip(weights, x_values, normalized_closes))
    denominator = sum(w * (x - x_avg) ** 2 for w, x in zip(weights, x_values))
    if math.isclose(denominator, 0.0, abs_tol=1e-12):
        return 0

    slope = numerator / denominator
    angle = math.degrees(math.atan(slope))
    return round_half_away_from_zero(angle)


def is_market_reversal_blocked_at_entry(state: Dict[str, Any], strategy_type: str) -> bool:
    """入場當下任一市場指數回到模式失效門檻即封鎖；缺資料亦封鎖。"""
    for index_key in MARKET_GATE_INDEX_KEYS:
        index_config = market_previous_close_indices.get(index_key, {})
        previous_close = index_config.get("previous_close")
        last_index = MARKET_INDEX_STATE.get(index_key, {}).get("last_index")
        try:
            previous_close_float = float(previous_close)
            last_index_float = float(last_index)
        except (TypeError, ValueError):
            print(f"[{state['symbol_name']}] 市場模式入場檢查等待 {index_key} 指數資料")
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
        elif strategy_type == STRATEGY_FOLLOW:
            decline_percent = get_strategy_decision_decline_percent(index_key)
            if decline_percent is None:
                print(f"[{state['symbol_name']}] 市場模式入場檢查 {index_key} follow 回跌門檻設定錯誤")
                return True
            threshold = previous_close_float * (1 + decline_percent / 100.0)
            if last_index_float <= threshold:
                print(
                    f"[{state['symbol_name']}] 市場模式入場檢查封鎖 FOLLOW："
                    f"{index_key} last_index={last_index_float:.2f} <= decline_threshold={threshold:.2f}"
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
    market_state = MARKET_INDEX_STATE.get(market_key, {})
    last_index = market_state.get("last_index")

    try:
        previous_close_float = float(previous_close)
        last_index_float = float(last_index)
    except (TypeError, ValueError):
        print(f"[{state['symbol_name']}] 產業別盤勢濾網等待 {market_key} 指數資料")
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


def follow_industry_market_filter_pass(state: Dict[str, Any]) -> bool:
    market_key = state.get("market_index_key")
    if not market_key:
        market_key = get_market_key_for_symbol(
            state.get("symbol_code_with_suf", ""),
            state.get("industry_code", ""),
        )

    index_config = market_previous_close_indices.get(market_key, {})
    previous_close = index_config.get("previous_close")
    market_state = MARKET_INDEX_STATE.get(market_key, {})
    last_index = market_state.get("last_index")

    try:
        previous_close_float = float(previous_close)
        last_index_float = float(last_index)
    except (TypeError, ValueError):
        print(f"[{state['symbol_name']}] 產業別盤勢濾網等待 {market_key} 指數資料")
        return False

    if previous_close_float <= 0:
        print(f"[{state['symbol_name']}] 產業別盤勢濾網 {market_key} 昨收指數設定錯誤: {previous_close}")
        return False

    threshold = previous_close_float * (1 - INDUSTRY_MARKET_FILTER_MIN_DOWN_PERCENT / 100.0)
    if last_index_float <= threshold:
        index_name = index_config.get("name", "")
        print(
            f"[{state['symbol_name']}] 產業別盤勢濾網未通過：{market_key} {index_name} "
            f"指數 {last_index_float:.2f} <= 門檻 {threshold:.2f}"
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
    market_state = MARKET_INDEX_STATE.get(market_key, {})
    last_index = market_state.get("last_index")

    try:
        previous_close_float = float(previous_close)
        last_index_float = float(last_index)
    except (TypeError, ValueError):
        return ""

    if previous_close_float <= 0:
        return ""

    index_name = index_config.get("name", "")
    if is_follow_mode():
        threshold = previous_close_float * (1 - INDUSTRY_MARKET_FILTER_MIN_DOWN_PERCENT / 100.0)
        operator = ">"
    else:
        threshold = previous_close_float * (1 + INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT / 100.0)
        operator = "<"

    return (
        f"（產業別盤勢濾網：{market_key} {index_name} "
        f"指數 {last_index_float:.2f} {operator} 門檻 {threshold:.2f}）"
    )


def adjust_price(price: float, trade_strategy: str) -> float:
    """
    根據 side ('Buy' 或 'Sell') 及價格，自動進位或捨去到符合 tick_size。
    Sell → 無條件捨去
    """
    tick = get_tick_size(price)
    if trade_strategy == "SHORT":
        adjusted = math.floor(price / tick) * tick  # 無條件捨去
    else:
        adjusted = round(price / tick) * tick  # 四捨五入（預設行為）

    if price < 100:
        return round(adjusted, 2)
    elif price < 1000:
        return round(adjusted, 1)
    else:
        return int(adjusted)


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


def calculate_range_fraction_prices(entry_price: float, pre_close_price: float, trade_strategy: str) -> Tuple[float, float]:
    """
    依起點、終點與方向，回傳區間的 2/4 與 3/4 價位，並調整為符合台股 tick size。

    範例：
    - calculate_range_fraction_prices(100, 60, "SHORT") -> (80, 70)
    """

    price_gap = pre_close_price - entry_price
    second_quartile = entry_price + price_gap * 0.5
    third_quartile = entry_price + price_gap * 0.75

    return (
        adjust_price(second_quartile, trade_strategy),
        adjust_price(third_quartile, trade_strategy),
    )


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


def persist_selected_stocks_to_stock_data(
    stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]]
):
    """
    實機執行時不回寫 stock_data.py。
    selected_stocks 本身會在 initialize_states() 透過 stocks[:] 更新為過濾後名單。
    """
    return


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
        "best_ask_price": None,
        "chance_effective_yesterday_low_price": v3,
        "chance_effective_yesterday_low_time": None,
        "traded": False,
        "in_position": False,
        "strategy_type": strategy_type,
        "side": "",  # 'SHORT' or 'LONG'
        "entry_price": 0,
        "entry_time": None,
        "entry_order_no": "", # 入場委託書號
        "entry_order_qty": qty, # 入場原始委託張數
        "entry_filled_qty": 0, # get_order_results 查得的入場成交張數
        "entry_fully_filled": False, # 入場成交張數是否已達原始委託張數
        "entry_fill_confirmed": False, # 是否已用 get_order_results 確認至少一筆成交
        "entry_price_source": "estimated", # estimated / order_results
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
        "volume_down_checked": False,
        "volume_down_qualified": False,
        "volume_down_yesterday_date": None,
        "volume_down_yesterday_volume": None,
        "volume_down_yesterday_ready": False,
        "volume_down_today_volume": None,
        "volume_down_volume_ratio": None,
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
    if entry_mode in (ENTRY_MODE_NO_TRADE, ENTRY_MODE_FOLLOW, ENTRY_MODE_LOWER, ENTRY_MODE_CHANCE):
        return entry_mode
    return ENTRY_MODE_NO_TRADE


def is_follow_mode() -> bool:
    return get_current_entry_mode() == ENTRY_MODE_FOLLOW


def is_lower_mode() -> bool:
    return get_current_entry_mode() == ENTRY_MODE_LOWER


def is_chance_mode() -> bool:
    return get_current_entry_mode() == ENTRY_MODE_CHANCE


def is_limit_down_strategy(state: Dict[str, Any] | None) -> bool:
    if not state:
        return False
    return state.get("strategy_type") == STRATEGY_LIMIT_DOWN


def is_limit_up_strategy(state: Dict[str, Any] | None) -> bool:
    if not state:
        return False
    return state.get("strategy_type") == STRATEGY_LIMIT_UP


def is_volume_down_strategy(state: Dict[str, Any] | None) -> bool:
    if not state:
        return False
    return state.get("strategy_type") == STRATEGY_VOLUME_DOWN


def is_independent_limit_strategy(state: Dict[str, Any] | None) -> bool:
    return is_limit_down_strategy(state) or is_limit_up_strategy(state)


def is_independent_strategy(state: Dict[str, Any] | None) -> bool:
    return is_independent_limit_strategy(state) or is_volume_down_strategy(state)


def get_optimize_loss_profit_percent(state: Dict[str, Any]) -> tuple[float, float]:
    if is_limit_down_strategy(state):
        return OPTIMIZE_LOSS_PER_LIMIT_DOWN, OPTIMIZE_PROFIT_PER_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return OPTIMIZE_LOSS_PER_LIMIT_UP, OPTIMIZE_PROFIT_PER_LIMIT_UP
    if is_volume_down_strategy(state):
        return OPTIMIZE_LOSS_PER_VOLUME_DOWN, OPTIMIZE_PROFIT_PER_VOLUME_DOWN
    if is_follow_mode():
        return OPTIMIZE_LOSS_PER_FOLLOW, OPTIMIZE_PROFIT_PER_FOLLOW
    if is_chance_mode():
        return OPTIMIZE_LOSS_PER_CHANCE, OPTIMIZE_PROFIT_PER_CHANCE
    if is_lower_mode():
        return OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER
    return OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER


def get_protect_loss_profit_percent(state: Dict[str, Any] | None = None) -> tuple[float, float]:
    return PROTECT_LOSS_PER, PROTECT_PROFIT_PER


def get_force_close_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if is_limit_down_strategy(state):
        return FORCE_CLOSE_TIME_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return FORCE_CLOSE_TIME_LIMIT_UP
    if is_volume_down_strategy(state):
        return FORCE_CLOSE_TIME_VOLUME_DOWN
    if is_follow_mode():
        return FORCE_CLOSE_TIME_FOLLOW
    if is_chance_mode():
        return FORCE_CLOSE_TIME_CHANCE
    if is_lower_mode():
        return FORCE_CLOSE_TIME_LOWER
    return FORCE_CLOSE_TIME_LOWER


def get_entry_order_quantity(state: Dict[str, Any] | None = None) -> int:
    if is_limit_down_strategy(state):
        return ENTRY_ORDER_QUANTITY_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return ENTRY_ORDER_QUANTITY_LIMIT_UP
    if is_volume_down_strategy(state):
        return ENTRY_ORDER_QUANTITY_VOLUME_DOWN
    if is_follow_mode():
        return ENTRY_ORDER_QUANTITY_FOLLOW
    if is_chance_mode():
        return ENTRY_ORDER_QUANTITY_CHANCE
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
    if (not state.get("in_position")) and (not state.get("traded")):  #未持倉, 未交易
        open_pass = True
    return open_pass


def has_none_in_entry_k_data(state: Dict[str, Any]) -> bool:
    """
    檢查 entry_price_check 會使用的分K欄位是否存在 None。
    """
    required_k_fields = (
        "yesterday_low_price",
        "last_price",
    )
    return any(state.get(field) is None for field in required_k_fields)


def should_skip_entry_by_limit_down_zone(
    entry_price: float,
    true_yesterday_low: float,
    limit_down_price: float,
) -> bool:
    """進場價若低於昨低到跌停三分之一位置，略過本次進場。"""
    threshold = true_yesterday_low - ((true_yesterday_low - limit_down_price) / 3.0)
    return entry_price <= threshold


def should_skip_entry_by_limit_up_zone(
    entry_price: float,
    yesterday_close: float,
    limit_up_price: float,
) -> bool:
    """進場價若高於昨收到漲停三分之一位置，略過本次進場。"""
    threshold = yesterday_close + ((limit_up_price - yesterday_close) / 3.0)
    return entry_price >= threshold


def get_entry_check_end_time(state: Dict[str, Any]) -> tuple[int, int]:
    if is_limit_down_strategy(state):
        return ENTRY_CHECK_END_TIME_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return ENTRY_CHECK_END_TIME_LIMIT_UP
    if is_follow_mode():
        return ENTRY_CHECK_END_TIME_FOLLOW
    if is_chance_mode():
        return ENTRY_CHECK_END_TIME_CHANCE
    if is_lower_mode():
        return ENTRY_CHECK_END_TIME_LOWER
    return STRATEGY_DECISION


def get_entry_check_start_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if is_limit_down_strategy(state):
        return ENTRY_CHECK_START_TIME_LIMIT_DOWN
    if is_limit_up_strategy(state):
        return ENTRY_CHECK_START_TIME_LIMIT_UP
    if is_follow_mode():
        return ENTRY_CHECK_START_TIME_FOLLOW
    if is_chance_mode():
        return ENTRY_CHECK_START_TIME_CHANCE
    if is_lower_mode():
        return ENTRY_CHECK_START_TIME_LOWER
    return STRATEGY_DECISION


def get_latest_entry_check_end_time() -> tuple[int, int]:
    return max(
        ENTRY_CHECK_END_TIME_FOLLOW,
        ENTRY_CHECK_END_TIME_LOWER,
        ENTRY_CHECK_END_TIME_CHANCE,
        ENTRY_CHECK_END_TIME_LIMIT_DOWN,
        ENTRY_CHECK_END_TIME_LIMIT_UP,
    )


def get_entry_trigger_reference_price(state: Dict[str, Any]) -> float | None:
    if is_follow_mode():
        reference_price = state.get("yesterday_close_price")
    else:
        reference_price = state.get("yesterday_low_price")
    try:
        return float(reference_price)
    except (TypeError, ValueError):
        return None


def get_entry_trigger_price(state: Dict[str, Any]) -> float | None:
    reference_price = get_entry_trigger_reference_price(state)
    if reference_price is None:
        return None
    if is_follow_mode():
        return adjust_price(reference_price + get_tick_size(reference_price), TRADE_SIDE_LONG)
    if is_lower_mode():
        return adjust_price(reference_price - get_tick_size(reference_price), TRADE_SIDE_SHORT)
    return None


def entry_follow_mode_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    follow 模式進場條件判斷；成立時以當下 last_price 寫入 entry_trigger_price。
    """
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME_FOLLOW:
        return False
    if (now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME_FOLLOW:
        return False

    yesterday_close_price = state.get("yesterday_close_price")
    limit_up_price = state.get("limit_up_price")
    if yesterday_close_price is None or limit_up_price is None:
        return False

    pre_last_price = state.get("pre_last_price")
    pre_last_price_time = state.get("pre_last_price_time")
    last_price = state.get("last_price", 0)
    best_ask_price = state.get("best_ask_price")
    if (
        pre_last_price is None
        or pre_last_price_time is None
        or last_price is None
        or best_ask_price is None
    ):
        return False

    try:
        yesterday_close = float(yesterday_close_price)
        limit_up = float(limit_up_price)
        pre_last_px = float(pre_last_price)
        last_px = float(last_price)
        best_ask = float(best_ask_price)
    except (TypeError, ValueError):
        return False

    if pre_last_px <= 0 or last_px <= 0 or best_ask <= 0:
        return False

    entry_lower_bound, entry_upper_bound = calculate_entry_range_bounds(
        yesterday_close,
        limit_up,
        FOLLOW_ENTRY_RANGE_START_PERCENT,
        FOLLOW_ENTRY_RANGE_END_PERCENT,
    )
    if not is_price_in_entry_range(last_px, entry_lower_bound, entry_upper_bound):
        return False

    if best_ask < last_px:
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"現價已落入 follow 入場區間但最佳ask未跟上，暫不進場 "
            f"last_price={last_px} best_ask={best_ask} "
            f"entry_range={entry_lower_bound:.2f}~{entry_upper_bound:.2f}"
        )
        return False

    if is_market_reversal_blocked_at_entry(state, STRATEGY_FOLLOW):
        return 'BLOCKED'
    if not follow_industry_market_filter_pass(state):
        return False

    state["entry_trigger_price"] = last_px
    return True


def entry_lower_mode_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    lower 模式進場條件判斷；成立時以當下 last_price 寫入 entry_trigger_price。

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


def entry_chance_mode_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    chance 模式進場條件判斷；成立時以當下 last_price 寫入 entry_trigger_price。
    """
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME_CHANCE:
        return False
    if (now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME_CHANCE:
        return False

    effective_low_price = state.get("chance_effective_yesterday_low_price")
    limit_down_price = state.get("limit_down_price")
    last_price = state.get("last_price", 0)
    best_bid_price = state.get("best_bid_price")
    if effective_low_price is None or limit_down_price is None or last_price is None or best_bid_price is None:
        return False

    try:
        effective_low = float(effective_low_price)
        limit_down = float(limit_down_price)
        last_px = float(last_price)
        best_bid = float(best_bid_price)
    except (TypeError, ValueError):
        return False

    if effective_low <= 0 or limit_down <= 0 or last_px <= 0 or best_bid <= 0:
        return False

    entry_tick = get_tick_size(effective_low)
    entry_probe_price = max(effective_low - entry_tick, 0.0)
    if should_skip_entry_by_limit_down_zone(
        entry_probe_price,
        effective_low,
        limit_down,
    ):
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"CHANCE 跌破有效昨低後將過於接近跌停，今日不進場 "
            f"effective_low={effective_low:.2f} limit_down={limit_down:.2f}"
        )
        return 'BLOCKED'

    if last_px >= effective_low:
        return False

    if should_skip_entry_by_limit_down_zone(
        last_px,
        effective_low,
        limit_down,
    ):
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"CHANCE 觸發價過於接近跌停，今日不進場 "
            f"last_price={last_px:.2f} effective_low={effective_low:.2f} limit_down={limit_down:.2f}"
        )
        return 'BLOCKED'

    if best_bid > last_px:
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"現價已跌破 chance 有效昨低但最佳bid未跟上，暫不進場 "
            f"last_price={last_px} best_bid={best_bid} effective_low={effective_low}"
        )
        return False

    if not lower_industry_market_filter_pass(state):
        return False

    state["entry_trigger_price"] = last_px
    return True


def entry_limit_down_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    limit-down 獨立策略進場條件判斷；即時價由昨收下方/等於昨收向上穿越昨收時作空。
    """
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME_LIMIT_DOWN:
        return False
    if (now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME_LIMIT_DOWN:
        return False

    yesterday_close_price = state.get("yesterday_close_price")
    pre_last_price = state.get("pre_last_price")
    pre_last_price_time = state.get("pre_last_price_time")
    last_price = state.get("last_price")
    best_bid_price = state.get("best_bid_price")
    if (
        yesterday_close_price is None
        or pre_last_price is None
        or pre_last_price_time is None
        or last_price is None
        or best_bid_price is None
    ):
        return False

    try:
        yesterday_close = float(yesterday_close_price)
        pre_last_px = float(pre_last_price)
        last_px = float(last_price)
        best_bid = float(best_bid_price)
    except (TypeError, ValueError):
        return False

    if yesterday_close <= 0 or pre_last_px <= 0 or last_px <= 0 or best_bid <= 0:
        return False

    if not (pre_last_px <= yesterday_close < last_px):
        return False

    if best_bid > last_px:
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"LIMIT_DOWN 現價已向上穿越昨收但最佳bid未跟上，暫不進場 "
            f"pre_last_price={pre_last_px} last_price={last_px} "
            f"best_bid={best_bid} yesterday_close={yesterday_close}"
        )
        return False

    state["entry_trigger_price"] = last_px
    state["side"] = TRADE_SIDE_SHORT
    return True


def entry_limit_up_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    limit-up 獨立策略進場條件判斷；即時價由昨收下方/等於昨收向上穿越昨收時作多。
    """
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME_LIMIT_UP:
        return False
    if (now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME_LIMIT_UP:
        return False

    yesterday_close_price = state.get("yesterday_close_price")
    pre_last_price = state.get("pre_last_price")
    pre_last_price_time = state.get("pre_last_price_time")
    last_price = state.get("last_price")
    avg_price = state.get("avg_price")
    best_ask_price = state.get("best_ask_price")
    if (
        yesterday_close_price is None
        or pre_last_price is None
        or pre_last_price_time is None
        or last_price is None
        or avg_price is None
        or best_ask_price is None
    ):
        return False

    try:
        yesterday_close = float(yesterday_close_price)
        pre_last_px = float(pre_last_price)
        last_px = float(last_price)
        avg_px = float(avg_price)
        best_ask = float(best_ask_price)
    except (TypeError, ValueError):
        return False

    if yesterday_close <= 0 or pre_last_px <= 0 or last_px <= 0 or avg_px <= 0 or best_ask <= 0:
        return False

    if not (pre_last_px <= yesterday_close < last_px):
        return False

    avg_gap_threshold = yesterday_close * (LIMIT_UP_ENTRY_AVG_BELOW_PREVIOUS_CLOSE_PERCENT / 100.0)
    if not ((yesterday_close - avg_px) > avg_gap_threshold):
        return False

    if best_ask < last_px:
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"LIMIT_UP 現價已向上穿越昨收但最佳ask未跟上，暫不進場 "
            f"pre_last_price={pre_last_px} last_price={last_px} "
            f"best_ask={best_ask} yesterday_close={yesterday_close}"
        )
        return False

    state["entry_trigger_price"] = last_px
    state["side"] = TRADE_SIDE_LONG
    return True


def entry_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    依 entry_mode 分派進場條件判斷。
    """
    if is_limit_down_strategy(state):
        return entry_limit_down_price_check(state, realtime_sdk)
    if is_limit_up_strategy(state):
        return entry_limit_up_price_check(state, realtime_sdk)
    if get_current_entry_mode() == ENTRY_MODE_NO_TRADE:
        now_local = now_tpe()
        if (now_local.hour, now_local.minute) < STRATEGY_DECISION:
            return False
        print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} NO_TRADE 模式不追蹤")
        return 'BLOCKED'
    if is_follow_mode():
        return entry_follow_mode_price_check(state, realtime_sdk)
    if is_chance_mode():
        return entry_chance_mode_price_check(state, realtime_sdk)
    if is_lower_mode():
        return entry_lower_mode_price_check(state, realtime_sdk)
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

    '''
    limit_up_price = state.get("limit_up_price")# 漲停
    limit_down_price = state.get("limit_down_price") # 跌停

    high_price = state.get("high_price")
    low_price = state.get("low_price")

    # 已經漲停或跌停就不再追蹤了
    if (high_price == limit_up_price) or (low_price == limit_down_price) or (last_px == limit_up_price) or (last_px == limit_down_price):
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] 已達漲停或跌停，不追蹤")
        return
    '''

    '''
    up_streak_days = state.get("up_streak_days", 0)  # 連漲天數
    if up_streak_days >= 3:
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] 處在連漲狀態，不執行")
        return
    '''

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
            state["in_position"] = True
            state["entry_order_no"] = str(place_order_result)
            state["entry_order_qty"] = qty
            state["entry_filled_qty"] = 0
            state["entry_fully_filled"] = False
            state["entry_fill_confirmed"] = False
            state["entry_price_source"] = "estimated"
            state["entry_price"] = entry_ref_px

            industry_filter_text = "" if is_independent_strategy(state) else format_industry_market_filter_pass_text(state)
            recalc_entry_position_prices(state)
            if side == TRADE_SIDE_SHORT:
                print(f"[{state['symbol_name']}] 作空 已至入場時機，下單成功{industry_filter_text}")
            else:
                print(f"[{state['symbol_name']}] 作多 已至入場時機，下單成功{industry_filter_text}")

            state["profit_tracking_active"] = False  # 追蹤停利尚未啟動，等到首次觸及 profit_price 才開始
            print_entry_position_prices(state)
        else:  # 下單失敗
            state["traded"] = True

        # 更新狀態
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def try_close_position(state: Dict[str, Any], mysdk):
    if not state.get("in_position"):
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

    if is_independent_limit_strategy(state):
        sl = reached_stop_to_flat(state)
        rp = reached_resize_profit(state)
        strategy_label = state.get("strategy_type", "LIMIT")

        if sl:
            if close_flat_position(state, mysdk):
                print(f"[{state['symbol_name']}] ✅ {strategy_label} 已至停損價格 {state['flat_price']}")
        elif rp:
            if close_profit_position(state, mysdk):
                print(f"[{state['symbol_name']}] ✅ {strategy_label} 已至停利價格 {state['profit_price']}")
        return

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


def _protect_profit_stop(state: Dict[str, Any]):
    """獲利達標後，將 flat_price 推進到至少保住指定獲利的位置。"""
    side = state.get("side")
    if side not in (TRADE_SIDE_SHORT, TRADE_SIDE_LONG):
        return

    try:
        px = float(state.get("last_price"))
        entry_price = float(state.get("entry_price"))
        current_flat_price = float(state.get("flat_price"))
    except (TypeError, ValueError):
        return

    if entry_price <= 0:
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

    if side == TRADE_SIDE_SHORT:
        return px <= profit_price
    if side == TRADE_SIDE_LONG:
        return px >= profit_price
    return False


def reached_profitable_limit_price(state: Dict[str, Any]) -> bool:
    """作多達漲停、作空達跌停時，當沖獲利已到當日極限，應立即平倉。"""
    side = state.get("side")
    try:
        px = float(state.get("last_price"))
    except (TypeError, ValueError):
        return False

    if px <= 0:
        return False

    if side == TRADE_SIDE_LONG:
        try:
            limit_up_price = float(state.get("limit_up_price"))
        except (TypeError, ValueError):
            return False
        return limit_up_price > 0 and px >= limit_up_price

    if side == TRADE_SIDE_SHORT:
        try:
            limit_down_price = float(state.get("limit_down_price"))
        except (TypeError, ValueError):
            return False
        return limit_down_price > 0 and px <= limit_down_price

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


def close_profit_position(state: Dict[str, Any], mysdk) -> bool:
    last_px = state.get("last_price", 0.0)
    side = state.get("side")
    exit_place_result = False
    close_order_sent = False

    if side == TRADE_SIDE_SHORT:
        exit_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Buy,
            Trade.Cash,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.Market,
            price=last_px
        )
        close_order_sent = bool(exit_place_result)
    elif side == TRADE_SIDE_LONG:
        exit_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Sell,
            Trade.DayTradingSell,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.Market,
            price=last_px
        )
        close_order_sent = bool(exit_place_result)

    if not exit_place_result and side == TRADE_SIDE_SHORT:
        reserve_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Buy,
            Trade.Cash,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.LimitUp,
            price=0
        )
        if not reserve_place_result:
            print(f'[{state.get("symbol_name")}] SHORT 停利市價平倉失敗且預約掛單失敗，須手動下單平倉')
            return False
        print(f'[{state.get("symbol_name")}] SHORT 停利市價平倉失敗，已改掛預約平倉單')
        close_order_sent = True
    elif not exit_place_result and side == TRADE_SIDE_LONG:
        reserve_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Sell,
            Trade.DayTradingSell,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.LimitDown,
            price=0
        )
        if not reserve_place_result:
            print(f'[{state.get("symbol_name")}] LONG 停利市價平倉失敗且預約掛單失敗，須手動下單平倉')
            return False
        print(f'[{state.get("symbol_name")}] LONG 停利市價平倉失敗，已改掛預約平倉單')
        close_order_sent = True

    if not close_order_sent:
        return False

    print_close_position_log(state)
    state["traded"] = True
    state["in_position"] = False  # 確保持倉狀態為 False
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
    return True


def close_flat_position(state: Dict[str, Any], mysdk) -> bool:

    last_px = state.get("last_price", 0.0)
    side = state.get("side")
    exit_place_result = False
    close_order_sent = False

    # SHORT：先嘗試市價買回平倉
    if side == TRADE_SIDE_SHORT:
        exit_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Buy,
            Trade.Cash,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.Market,
            price=last_px
        )
        close_order_sent = bool(exit_place_result)
    elif side == TRADE_SIDE_LONG:
        exit_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Sell,
            Trade.DayTradingSell,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.Market,
            price=last_px
        )
        close_order_sent = bool(exit_place_result)

    # 集合競價等情境若無法市價，改掛預約單
    if not exit_place_result and side == TRADE_SIDE_SHORT:
        reserve_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Buy,
            Trade.Cash,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.LimitUp,
            price=0
        )

        if not reserve_place_result:
            print(f'[{state.get("symbol_name")}] SHORT 市價平倉失敗且預約掛單失敗，須手動下單平倉')
            return False

        print(f'[{state.get("symbol_name")}] SHORT 市價平倉失敗，已改掛預約平倉單')
        close_order_sent = True
    elif not exit_place_result and side == TRADE_SIDE_LONG:
        reserve_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Sell,
            Trade.DayTradingSell,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.LimitDown,
            price=0
        )

        if not reserve_place_result:
            print(f'[{state.get("symbol_name")}] LONG 市價平倉失敗且預約掛單失敗，須手動下單平倉')
            return False

        print(f'[{state.get("symbol_name")}] LONG 市價平倉失敗，已改掛預約平倉單')
        close_order_sent = True

    if not close_order_sent:
        return False

    print_close_position_log(state)
    state["traded"] = True
    state["in_position"] = False  # 確保持倉狀態為 False
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
    return True


def endtime_close_position(state: Dict[str, Any], mysdk) -> bool:

    if state.get("traded") == True or state.get("in_position") == False:  # 已完成交易，不用平倉
        print(f'[{state.get("symbol_name")}] 已完成交易，不須強制平倉')
        state["traded"] = True
        state["in_position"] = False  # 確保持倉狀態為 False
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        return False  # 這裡直接返回，是因為若無建倉就下平倉單，反而變成另起一張訂單

    # 是否有順利出場
    exit_place_result = False
    # 強制平倉
    last_px = state.get("last_price", 0.0)
    side = state.get("side")

    if side == TRADE_SIDE_SHORT:
        exit_place_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Buy, Trade.Cash,
                                             quantity=state.get("qty", 0),
                                             price_flag=PriceFlag.Market, price=last_px)
    elif side == TRADE_SIDE_LONG:
        exit_place_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Sell, Trade.DayTradingSell,
                                             quantity=state.get("qty", 0),
                                             price_flag=PriceFlag.Market, price=last_px)
    if exit_place_result: # 有成功平倉
        # 更新狀態
        print_close_position_log(state)
        state["traded"] = True
        state["in_position"] = False  # 確保持倉狀態為 False
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        return True

    # 強制以漲跌停平倉結果
    limit_order_result = False

    # 市價平倉失敗的話，改用漲停價回補
    if not exit_place_result:
        if side == TRADE_SIDE_SHORT:
            limit_order_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Buy, Trade.Cash,
                                                  quantity=state.get("qty", 0), price_flag=PriceFlag.LimitUp, price=0)
        elif side == TRADE_SIDE_LONG:
            limit_order_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Sell, Trade.DayTradingSell,
                                                  quantity=state.get("qty", 0), price_flag=PriceFlag.LimitDown, price=0)
        if limit_order_result:
            print_close_position_log(state)
            state["traded"] = True
            state["in_position"] = False  # 確保持倉狀態為 False
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            return True

    if limit_order_result == False:
        print(f'[{state.get("symbol_name")}] 已至收盤時間，漲跌停平倉交易失敗，須手動下單平倉')
    return False


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
    """沿用原平倉機制：平倉委託回傳成功即視為交易完成。"""
    for state in states.values():
        if is_independent_strategy(state):
            continue

        apply_market_reversal_metadata(state)

        if state.get("traded"):
            continue

        if not state.get("in_position"):
            state["traded"] = True
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            continue

        close_flat_position(state, mysdk)
        if state.get("traded"):
            print(f"[{state.get('symbol_name')}] 大盤反轉平倉委託成功")

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
        if is_independent_strategy(state) or state.get("traded") or state.get("in_position"):
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
            limit_up_price, limit_down_price, symbol_can_buy_day_trade = get_up_down_price(code_with_suf, realtime_sdk)
        except Exception as e:
            print(f"[{symbolStr}] ⚠️ 取得漲跌停/當沖資格失敗，排除：{e}")
            continue

        '''
        if down_streak_days >= 4:
            print(f"[{symbolStr}] ⚠️ 已連跌多日，排除")
            continue
        '''

        if not symbol_can_buy_day_trade:
            print(f"[{symbolStr}] ⚠️ 無法當沖，排除")
            continue

        if symbolStr.split(":")[1].split(".")[0] in []: # in ["1597", "8064"]
            print(f"[{symbolStr}] ⚠️ 高失敗率，排除")
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

    # 將過濾後名單回寫至原始 stocks（例如 selected_stocks）
    stocks[:] = filtered_stocks
    persist_selected_stocks_to_stock_data(filtered_stocks)

    return states


def monitor(states: Dict[str, Dict[str, Any]], mysdk: SDK, realtime_sdk: EsunMarketdata):
    update_status = False
    realtime_quote_start_announced = False
    strategy_decision_announced = False
    entry_check_start_announced = False
    entry_check_end_announced = False
    entry_mode_decided = False
    volume_down_evaluation_done = not ENABLE_VOLUME_DOWN_STRATEGY
    last_order_results_update_monotonic = 0.0
    while True:
        if POSITION_MARKET_REVERSAL_EVENT.is_set() and has_unfinished_non_independent_states(states):
            if handle_position_market_reversal(states, mysdk):
                if not has_unfinished_independent_states(states):
                    print("[MARKET_REVERSAL] 所有標的均已完成大盤反轉處理，結束監控")
                    return
                print("[MARKET_REVERSAL] 非獨立策略標的已完成大盤反轉處理，獨立策略繼續監控")
            else:
                time.sleep(5.0)
                continue

        if MARKET_REVERSAL_STOP_EVENT.is_set():
            mark_non_independent_states_blocked(states, "market_mode_blocked")
            if not has_unfinished_independent_states(states):
                print("[MODE] 市場模式已封鎖，維持 NO_TRADE 並結束監控")
                return

        # round_has_market_update = False
        now_local = now_tpe()
        if (
            not volume_down_evaluation_done
            and (now_local.hour, now_local.minute, now_local.second) >= VOLUME_DOWN_EVALUATION_TIME
        ):
            print(
                f"⏰ VOLUME_DOWN 判斷時間！目前時間：{now_local.strftime('%H:%M:%S')}"
            )
            volume_down_evaluation_done = True
            evaluate_volume_down_entries(states, mysdk, realtime_sdk)

        if stop_if_ix0001_early_breakout_missing():
            return

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
                entry_mode, gate_results = decide_entry_mode_by_market_gate()
                apply_entry_mode_to_states(states, entry_mode)
                print_entry_mode_decision(entry_mode, gate_results)
                entry_mode_decided = True
                if entry_mode == ENTRY_MODE_FOLLOW and not ENABLE_ENTRY_MODE_FOLLOW:
                    print(
                        "[MODE] STRATEGY_DECISION 判定為 FOLLOW，但 "
                        "ENABLE_ENTRY_MODE_FOLLOW=False，今日不執行此模式，"
                        "非獨立策略將停止追蹤"
                    )
                    if block_non_independent_states_for_disabled_entry_mode(states, "FOLLOW"):
                        return
                if entry_mode == ENTRY_MODE_LOWER and not ENABLE_ENTRY_MODE_LOWER:
                    print(
                        "[MODE] STRATEGY_DECISION 判定為 LOWER，但 "
                        "ENABLE_ENTRY_MODE_LOWER=False，今日不執行此模式，"
                        "非獨立策略將停止追蹤"
                    )
                    if block_non_independent_states_for_disabled_entry_mode(states, "LOWER"):
                        return
                if entry_mode == ENTRY_MODE_CHANCE and not ENABLE_ENTRY_MODE_CHANCE:
                    print(
                        "[MODE] STRATEGY_DECISION 判定為 CHANCE，但 "
                        "ENABLE_ENTRY_MODE_CHANCE=False，今日不執行此模式，"
                        "非獨立策略將停止追蹤"
                    )
                    if block_non_independent_states_for_disabled_entry_mode(states, "CHANCE"):
                        return

        entry_check_start_time = get_entry_check_start_time()
        if (
            entry_mode_decided
            and get_current_entry_mode() != ENTRY_MODE_NO_TRADE
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
        if now_monotonic - last_order_results_update_monotonic >= ORDER_RESULTS_UPDATE_SECONDS:
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
                        px, open_px, high_price, low_price, close_price, avg_price, best_bid_price, best_ask_price = get_realtime_price(st.get("symbol_code_with_suf", ""), realtime_sdk)
                    except Exception as e:
                        print(f"[{st['symbol_name']}] ⚠️ 取價失敗：{e}，略過本輪重試")
                        continue

                    if (px is None) or (open_px is None):
                        print(f"[{st['symbol_name']}] ⚠️ 無開盤價及最新價資訊，略過本輪重試")
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
                    if (now_local.hour, now_local.minute) <= MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME:
                        try:
                            low_price_float = float(low_price)
                        except (TypeError, ValueError):
                            low_price_float = 0.0
                        if low_price_float > 0:
                            previous_effective_low = st.get("chance_effective_yesterday_low_price")
                            try:
                                previous_effective_low_float = float(previous_effective_low)
                            except (TypeError, ValueError):
                                previous_effective_low_float = float(st.get("yesterday_low_price", 0) or 0)
                            if (
                                previous_effective_low_float <= 0
                                or low_price_float < previous_effective_low_float
                            ):
                                st["chance_effective_yesterday_low_price"] = low_price_float
                                st["chance_effective_yesterday_low_time"] = now_tpe().isoformat()
                    # round_has_market_update = True

                    entry_check_end_time = get_entry_check_end_time(st)
                    if (
                        ((now_local.hour, now_local.minute) > entry_check_end_time)
                        and (not st.get("in_position"))
                        ):
                        st["traded"] = True
                        st["entry_time"] = now_tpe().isoformat()
                        atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)
                        print(f"[{st['symbol_name']}] {now_tpe().strftime('%H:%M:%S')} 檢核時間結束仍未進場，不追蹤")
                        continue

                    if (st.get("in_position")) and (not st.get("traded")):  # 已持倉嘗試平倉
                        try_close_position(st, mysdk)
                    else:
                        entry_result = entry_price_check(st, realtime_sdk)
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
                            elif is_follow_mode():
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

        # 只有本輪有成功更新行情資訊才打印
        '''
        if round_has_market_update:
            round_end_time = now_tpe().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[ROUND_END] {round_end_time}")
        '''

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
    #print(calculate_range_fraction_prices(74.3, 72.1, "SHORT"))
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
        realtime_sdk.login()
        market_index_ws = start_market_index_stream(realtime_sdk)

        sdk = SDK(config)
        sdk.login()
        trade_report_thread = start_trade_report_stream(sdk)
        time.sleep(1.0)  # 先讓交易回報 WebSocket 建立連線，再進入策略監控
        if trade_report_thread.is_alive():
            print("[TRADE_WS] 委託／成交主動回報背景執行緒已啟動")
        else:
            print(
                "[WARN] 交易回報背景執行緒未持續運作；"
                "下單仍會檢查 place_order() 同步回傳",
                file=sys.stderr,
            )

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

        prepare_volume_down_yesterday_data(states, realtime_sdk)

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
