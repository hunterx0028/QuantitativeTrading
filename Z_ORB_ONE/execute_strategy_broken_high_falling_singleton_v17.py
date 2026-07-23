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
from datetime import datetime
import pytz
from tempfile import NamedTemporaryFile
from configparser import ConfigParser
from esun_trade.sdk import SDK
from esun_trade.order import OrderObject
from esun_trade.constant import (APCode, Trade, PriceFlag, Action)
from esun_marketdata import EsunMarketdata

import stock_data
from stock_data import selected_stocks, market_previous_close_indices


class TeeStream:
    """同時輸出到原始串流與記憶體緩衝。"""
    def __init__(self, original_stream, mirror_stream):
        self.original_stream = original_stream
        self.mirror_stream = mirror_stream

    def write(self, data):
        self.original_stream.write(data)
        self.mirror_stream.write(data)
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
FORCE_EXIT_TIME = (13, 30)  # 13:30 強制關閉程式
MAIN_START_TIME = (8, 55)  # 主程序開始執行時間；提早啟動時等待至此時間
REALTIME_QUOTE_START_TIME = (9, 10)  # 09:10 後才開始抓個股即時行情，避開開盤初期 quote 欄位不完整

ENTRY_BLOCKED = 'ENTRY_BLOCKED'
GATE_LOWER_PASSED = 'LOWER_PASSED'
GATE_FOLLOW_PASSED = 'FOLLOW_PASSED'
GATE_NOT_PASSED = 'NOT_PASSED'
GATE_NO_TRADE = 'NO_TRADE'
STRATEGY_LOWER = 'LOWER'
STRATEGY_FOLLOW = 'FOLLOW'
STRATEGY_NO_TRADE = 'NO_TRADE'
TRADE_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'LONG'

OPTIMIZE_LOSS_PER_LOWER = 2.0 # lower 停損百分比(%)，例如 3.0 代表入場價加上 3%
OPTIMIZE_PROFIT_PER_LOWER = 6.0 # lower 停利百分比(%)，例如 5.0 代表入場價減去 5%

OPTIMIZE_LOSS_PER_FOLLOW = 2.0 # follow 停損百分比(%)
OPTIMIZE_PROFIT_PER_FOLLOW = 6.0 # follow 停利百分比(%)

PROTECT_LOSS_PER_LOWER = 1.5 # lower 獲利保護後的新停損百分比
PROTECT_PROFIT_PER_LOWER = 2.5 # lower 觸發調整停利百分比

STRATEGY_DECISION = (9, 41)  # 市場模式判斷截止時間，不含此時間
MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME = (9, 6)  # 指數位於昨收兩側的 NO_TRADE 檢查起始時間（含）

ENTRY_CHECK_START_TIME_LOWER = (9, 46)  # lower 進場檢核開始時間（含）
ENTRY_CHECK_START_TIME_FOLLOW = (9, 46)  # follow 進場檢核開始時間（含）

ENTRY_CHECK_END_TIME_LOWER = (10, 1)  # lower 進場檢核截止時間（含）
ENTRY_CHECK_END_TIME_FOLLOW = (10, 1)  # follow 進場檢核截止時間（含）

FORCE_CLOSE_TIME_LOWER = (13, 0)  # lower 收盤前強制平倉時間
FORCE_CLOSE_TIME_FOLLOW = (13, 0)  # follow 收盤前強制平倉時間

ENTRY_ORDER_QUANTITY_LOWER = 1 # lower 每次進場下單數量
ENTRY_ORDER_QUANTITY_FOLLOW = 1 # follow 每次進場下單數量

IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.2 # IX0001 啟動門檻：STRATEGY_DECISION 前（不含此時間）low 需低於前日最後 close 的百分比
IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.8 # IX0001 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內

IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.0 # IX0043 啟動門檻：STRATEGY_DECISION 前（不含此時間）low 需低於前日最後 close 的百分比
IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.75 # IX0043 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內

IX0001_STRATEGY_DECISION_RAISE_PERCENT_FOLLOW = 1.2 # IX0001 啟動門檻：STRATEGY_DECISION 前 high 需高於前日最後 close 的百分比
IX0001_STRATEGY_DECISION_DECLINE_PERCENT_FOLLOW = 0.8 # IX0001 回跌失效門檻：突破後 low 不可回到前日最後 close 上方此百分比內

IX0043_STRATEGY_DECISION_RAISE_PERCENT_FOLLOW = 1.0 # IX0043 啟動門檻：STRATEGY_DECISION 前 high 需高於前日最後 close 的百分比
IX0043_STRATEGY_DECISION_DECLINE_PERCENT_FOLLOW = 0.75 # IX0043 回跌失效門檻：突破後 low 不可回到前日最後 close 上方此百分比內

BROKERAGE_FEE_RATE = 0.001425 # 台股手續費率，買賣雙邊皆收
SELL_TRANSACTION_TAX_RATE = 0.003 # 台股交易稅率，賣出時收

# 產業盤勢過濾：原策略入場條件成立後，產業指數當下價格不可與策略方向相反。
INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT = 0 # lower 入場條件成立後，產業指數即時值不可高於昨收指數上漲此百分比後的位置
INDUSTRY_MARKET_FILTER_MIN_DOWN_PERCENT = 0 # follow 入場條件成立後，產業指數即時值必須嚴格大於昨收指數下跌此百分比後的位置

PROFIT_BIG_BACK_STEP = 0.5 # 獲利後允許回撤多少
PROFIT_BIG_TARGET_STEP = 1.0 # 逐步獲利

PROFIT_SMALL_BACK_STEP = 0.2 # 獲利後允許回撤多少
PROFIT_SMALL_TARGET_STEP = 0.3 # 逐步獲利

MAX_LIMIT_UP_PRICE = 200 # 漲停不可超過的價格
MIN_LIMIT_DOWN_PRICE = 50 # 跌停不可超過的價格

MARKET_INDEX_STATE: Dict[str, Dict[str, Any]] = {}
MARKET_GATE_INDEX_KEYS = ("TWSE:MARKET", "TPEX:MARKET")
MARKET_REVERSAL_STOP_EVENT = threading.Event()
MARKET_REVERSAL_CHECK_ANNOUNCED_EVENT = threading.Event()
ENTRY_MODE_NO_TRADE = 0
ENTRY_MODE_FOLLOW = 1
ENTRY_MODE_LOWER = 2

# ============ 下單函式 ============
# symbol: '2330' '0050'
# action_type: Action.Buy or Action.Sell
# trade_type: Trade.Cash or Trade.DayTradingSell
# price_flag: PriceFlag.Market or PriceFlag.LimitDown or PriceFlag.LimitUp or PriceFlag.Limit
def type_place_order(mysdk, symbol_code_with_suf, action_type, trade_type, quantity=1, price_flag=PriceFlag.Market, price=0.0) -> Optional[bool]:
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
        mysdk.place_order(order)
        time.sleep(0.1) # 交易 API 限制每秒委託含取消不可超過 20 筆，保守控制在約 10 筆/秒
    except Exception as e:
        print(f"[ERROR] {symbol_code_with_suf} : {priceInfo} {action_type} x {quantity} - {trade_type} - {e}")
        return False

    print(f"[ORDER] {symbol_code_with_suf} : {priceInfo} {action_type} x {quantity} - {trade_type}")
    return True


def get_order_fill_info(symbol: str, sdk) -> tuple[float, int]:
    """
    依股票代號查詢委託結果，回傳 (加權平均成交價, 總成交量)。

    若查無成交資料，回傳 (0.0, 0)。
    若同一股票有多筆成交（委託被拆單），以加權平均計算成交價。
    """
    try:
        order_results = sdk.get_order_results()
    except Exception as exc:
        print(f'[ERROR] 取得委託結果失敗: {exc}', file=sys.stderr)
        return 0.0, 0

    matched_results = [
        item for item in order_results
        if str(item.get('stock_no', '')).strip() == str(symbol).strip()
    ]

    filled_results = [
        item for item in matched_results
        if int(item.get('mat_qty', 0) or 0) > 0
    ]

    if not filled_results:
        return 0.0, 0

    total_qty = sum(int(item.get('mat_qty', 0) or 0) for item in filled_results)
    total_value = sum(
        float(item.get('avg_price', 0.0) or 0.0) * int(item.get('mat_qty', 0) or 0)
        for item in filled_results
    )
    avg_price = total_value / total_qty if total_qty > 0 else 0.0
    return avg_price, total_qty


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
    reversal_start_hm = (
        MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
        + MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
    )
    strategy_decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    if not (0 <= reversal_start_hm <= 23 * 60 + 59):
        raise ValueError(
            "MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 設定錯誤，需介於 00:00~23:59"
        )
    if reversal_start_hm >= strategy_decision_hm:
        raise ValueError(
            "MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 必須早於 STRATEGY_DECISION"
        )


def get_entry_mode_text(entry_mode: int | None = None) -> str:
    mode = get_current_entry_mode() if entry_mode is None else entry_mode
    if mode == ENTRY_MODE_NO_TRADE:
        return "NO_TRADE"
    if mode == ENTRY_MODE_FOLLOW:
        return "FOLLOW"
    if mode == ENTRY_MODE_LOWER:
        return "LOWER"
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
            MARKET_REVERSAL_STOP_EVENT.set()
            print(
                f"[MODE] {now_local.strftime('%H:%M:%S')} {market_key} 指數已檢核到上下穿越昨收，"
                "今日 NO_TRADE，準備停止取價、關閉 WebSocket 並結束程序"
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
        if (
            (not market_state.get("strategy_decision_raised"))
            or market_state.get("strategy_decision_decline_blocked")
        ):
            market_state["strategy_decision_raise_time"] = event_time or now_local.isoformat()
        market_state["strategy_decision_raised"] = True
        market_state["strategy_decision_decline_blocked"] = False
        market_state.pop("strategy_decision_decline_time", None)

    if (
        market_state.get("strategy_decision_raised")
        and (not market_state.get("strategy_decision_decline_blocked"))
        and index_value <= decline_threshold
    ):
        market_state["strategy_decision_decline_blocked"] = True
        market_state["strategy_decision_decline_time"] = event_time or now_local.isoformat()


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
        "passed": False,
        "lower_passed": False,
        "follow_passed": False,
        "final_above_previous_close": False,
        "final_below_previous_close": False,
        "lower_reason": "",
        "follow_reason": "",
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
    elif last_index_float <= result["decline_threshold"]:
        result["follow_reason"] = "決策時已回跌至失效門檻"
    elif not result["final_above_previous_close"]:
        result["follow_reason"] = "決策時未在昨收上方"
    else:
        result["follow_passed"] = True
        result["follow_reason"] = "通過"

    result["passed"] = result["lower_passed"]
    return result


def decide_entry_mode_by_market_gate() -> tuple[int, list[Dict[str, Any]]]:
    gate_results = [
        get_market_strategy_decision_gate_result(index_key)
        for index_key in MARKET_GATE_INDEX_KEYS
    ]

    reversal_blocked = any(result["previous_close_reversal_blocked"] for result in gate_results)
    if reversal_blocked:
        print(
            "[MODE] 上市或上櫃指數在指定時段曾位於昨收上下兩側，今日 NO_TRADE "
            f"({MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0]:02d}:"
            f"{MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]:02d}~"
            f"{STRATEGY_DECISION[0]:02d}:{STRATEGY_DECISION[1]:02d})"
        )
        return ENTRY_MODE_NO_TRADE, gate_results

    follow_mode_passed = all(result["follow_passed"] for result in gate_results)
    lower_mode_passed = all(result["lower_passed"] for result in gate_results)

    if follow_mode_passed and lower_mode_passed:
        print("[WARN] STRATEGY_DECISION 同時符合 FOLLOW 與 LOWER，視為資料異常，今日 NO_TRADE")
        return ENTRY_MODE_NO_TRADE, gate_results
    if follow_mode_passed:
        return ENTRY_MODE_FOLLOW, gate_results
    if lower_mode_passed:
        return ENTRY_MODE_LOWER, gate_results
    return ENTRY_MODE_NO_TRADE, gate_results


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
        print(
            f"[MODE] {result['index_key']} {result.get('symbol') or ''} {result.get('name') or ''} "
            f"previous_close={format_market_gate_value(result.get('previous_close'))} "
            f"last_index={format_market_gate_value(result.get('last_index'))} "
            f"previous_close_traded_above={result.get('previous_close_traded_above')} "
            f"previous_close_traded_below={result.get('previous_close_traded_below')} "
            f"previous_close_reversal_blocked={result.get('previous_close_reversal_blocked')} "
            f"previous_close_reversal_time={format_market_gate_time(result.get('previous_close_reversal_time'))} "
            f"drop_threshold={format_market_gate_value(result.get('drop_threshold'))} "
            f"break_time={format_market_gate_time(result.get('break_time'))} "
            f"rebound_threshold={format_market_gate_value(result.get('rebound_threshold'))} "
            f"rebound_time={format_market_gate_time(result.get('rebound_time'))} "
            f"lower_passed={result.get('lower_passed')} "
            f"lower_reason={result.get('lower_reason')}"
        )
        print(
            f"[MODE] {result['index_key']} {result.get('symbol') or ''} {result.get('name') or ''} "
            f"previous_close={format_market_gate_value(result.get('previous_close'))} "
            f"last_index={format_market_gate_value(result.get('last_index'))} "
            f"raise_threshold={format_market_gate_value(result.get('raise_threshold'))} "
            f"raise_time={format_market_gate_time(result.get('raise_time'))} "
            f"decline_threshold={format_market_gate_value(result.get('decline_threshold'))} "
            f"decline_time={format_market_gate_time(result.get('decline_time'))} "
            f"follow_passed={result.get('follow_passed')} "
            f"follow_reason={result.get('follow_reason')}"
        )


def apply_entry_mode_to_states(states: Dict[str, Dict[str, Any]], entry_mode: int) -> None:
    persist_entry_mode_to_stock_data(entry_mode)
    for st in states.values():
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
    if last_index_float > threshold:
        index_name = index_config.get("name", "")
        print(
            f"[{state['symbol_name']}] 產業別盤勢濾網未通過：{market_key} {index_name} "
            f"指數 {last_index_float:.2f} > 門檻 {threshold:.2f}"
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
        print(f"[{state['symbol_name']}] follow 產業別盤勢濾網等待 {market_key} 指數資料")
        return False

    if previous_close_float <= 0:
        print(f"[{state['symbol_name']}] follow 產業別盤勢濾網 {market_key} 昨收指數設定錯誤: {previous_close}")
        return False

    threshold = previous_close_float * (1 - INDUSTRY_MARKET_FILTER_MIN_DOWN_PERCENT / 100.0)
    if last_index_float <= threshold:
        index_name = index_config.get("name", "")
        print(
            f"[{state['symbol_name']}] follow 產業別盤勢濾網未通過：{market_key} {index_name} "
            f"指數 {last_index_float:.2f} <= 門檻 {threshold:.2f}"
        )
        return False

    return True


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
    stock_data_path = os.path.join(os.path.dirname(__file__), "stock_data.py")

    lines = ["# 股票代碼、購買量、昨天開盤、昨天最高、昨天最低、昨天收盤、產業別代碼、真實平均波動幅度、(連漲天數, 連跌天數)\n"]
    lines.append(f"entry_mode = {get_current_entry_mode()}  # 0=no_trade, 1=follow, 2=lower\n\n")
    lines.append("market_previous_close_indices = ")
    lines.append(pformat(market_previous_close_indices, sort_dicts=False))
    lines.append("\n\n")
    lines.append("selected_stocks = [\n")

    for item in stocks:
        lines.append(f"    {repr(item)},\n")

    lines.append("]\n")

    try:
        with open(stock_data_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
    except Exception as e:
        print(f"[WARN] 回寫 stock_data.py 失敗: {e}")


def persist_entry_mode_to_stock_data(entry_mode: int) -> None:
    stock_data.entry_mode = entry_mode
    persist_selected_stocks_to_stock_data(selected_stocks)


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
        "side": "",  # 'SHORT' or 'LONG'
        "entry_price": 0,
        "entry_time": None,
        "flat_price": 0, # 強制平倉價格
        "stop_profit_price": limit_down_price, # 追蹤停利點（啟動後才有意義）
        "profit_price": 0, # 下一個獲利目標價
        "profit_tracking_active": False, # 追蹤停利是否已啟動
        "last_price": v4,  # 最近一次的收價（初始化為昨收，開盤後由即時行情覆蓋）
        "pre_last_price": 0, # 前一次的收價
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
    if entry_mode in (ENTRY_MODE_NO_TRADE, ENTRY_MODE_FOLLOW, ENTRY_MODE_LOWER):
        return entry_mode
    return ENTRY_MODE_NO_TRADE


def is_follow_mode() -> bool:
    return get_current_entry_mode() == ENTRY_MODE_FOLLOW


def is_lower_mode() -> bool:
    return get_current_entry_mode() == ENTRY_MODE_LOWER


def get_optimize_loss_profit_percent(state: Dict[str, Any]) -> tuple[float, float]:
    if is_follow_mode():
        return OPTIMIZE_LOSS_PER_FOLLOW, OPTIMIZE_PROFIT_PER_FOLLOW
    if is_lower_mode():
        return OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER
    return OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER


def get_protect_loss_profit_percent(state: Dict[str, Any] | None = None) -> tuple[float, float]:
    if is_lower_mode():
        return PROTECT_LOSS_PER_LOWER, PROTECT_PROFIT_PER_LOWER
    return PROTECT_LOSS_PER_LOWER, PROTECT_PROFIT_PER_LOWER


def get_force_close_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if is_follow_mode():
        return FORCE_CLOSE_TIME_FOLLOW
    if is_lower_mode():
        return FORCE_CLOSE_TIME_LOWER
    return FORCE_CLOSE_TIME_LOWER


def get_entry_order_quantity(state: Dict[str, Any] | None = None) -> int:
    if is_follow_mode():
        return ENTRY_ORDER_QUANTITY_FOLLOW
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
    if is_follow_mode():
        return ENTRY_CHECK_END_TIME_FOLLOW
    if is_lower_mode():
        return ENTRY_CHECK_END_TIME_LOWER
    return STRATEGY_DECISION


def get_entry_check_start_time(state: Dict[str, Any] | None = None) -> tuple[int, int]:
    if is_follow_mode():
        return ENTRY_CHECK_START_TIME_FOLLOW
    if is_lower_mode():
        return ENTRY_CHECK_START_TIME_LOWER
    return STRATEGY_DECISION


def get_latest_entry_check_end_time() -> tuple[int, int]:
    return max(ENTRY_CHECK_END_TIME_FOLLOW, ENTRY_CHECK_END_TIME_LOWER)


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
    follow 模式進場條件判斷（純函式，不修改 state）。
    """
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME_FOLLOW:
        return False
    if (now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME_FOLLOW:
        return False

    yesterday_close_price = state.get("yesterday_close_price")
    limit_up_price = state.get("limit_up_price")
    if yesterday_close_price is None:
        return False

    last_price = state.get("last_price", 0)
    best_ask_price = state.get("best_ask_price")
    if last_price is None or best_ask_price is None:
        return False

    try:
        yesterday_close = float(yesterday_close_price)
        last_px = float(last_price)
        best_ask = float(best_ask_price)
    except (TypeError, ValueError):
        return False

    trigger_price = get_entry_trigger_price(state)
    if trigger_price is None:
        return False

    if best_ask >= trigger_price and last_px >= trigger_price:
        if not follow_industry_market_filter_pass(state):
            return False

        try:
            limit_up = float(limit_up_price)
        except (TypeError, ValueError):
            return False

        if should_skip_entry_by_limit_up_zone(trigger_price, yesterday_close, limit_up):
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 進場價高於昨收到漲停三分之一位置，不追蹤。")
            return 'BLOCKED'

        return True

    if last_px >= trigger_price and best_ask < trigger_price:
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"現價已突破但最佳ask未突破，暫不進場 "
            f"last_price={last_px} best_ask={best_ask} trigger_price={trigger_price}"
        )

    return False


def entry_lower_mode_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    lower 模式進場條件判斷（純函式，不修改 state）。

    回傳值：
      True      — 條件成立，應進場（side / entry_trigger_price 由呼叫端設定）
      'BLOCKED' — 驗證失敗，需永久封鎖本日進場（呼叫端負責設 traded=True）
      False     — 尚未觸發，繼續等待下一輪
    """
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME_LOWER:
        return False
    if (now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME_LOWER:
        return False

    yesterday_low_price = state.get("yesterday_low_price")
    limit_down_price = state.get("limit_down_price")
    if yesterday_low_price is None:
        return False

    last_price = state.get("last_price", 0)
    if last_price is None:
        return False

    best_bid_price = state.get("best_bid_price")
    if best_bid_price is None:
        return False

    try:
        true_ylow = float(yesterday_low_price)
        last_px = float(last_price)
        best_bid = float(best_bid_price)
    except (TypeError, ValueError):
        return False

    trigger_price = get_entry_trigger_price(state)
    if trigger_price is None:
        return False

    if best_bid <= trigger_price and last_px <= trigger_price:  # 買一與成交價皆跌破真實昨低下一檔才進場
        if not lower_industry_market_filter_pass(state):
            return False

        if should_skip_entry_by_limit_down_zone(trigger_price, true_ylow, limit_down_price):
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 進場價低於真實昨低到跌停三分之一位置，不追蹤。")
            return 'BLOCKED'

        return True

    if last_px <= trigger_price and best_bid > trigger_price:
        print(
            f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} "
            f"現價已跌破但最佳bid未跌破，暫不進場 "
            f"last_price={last_px} best_bid={best_bid} trigger_price={trigger_price}"
        )

    return False


def entry_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    依 entry_mode 分派進場條件判斷。
    """
    if get_current_entry_mode() == ENTRY_MODE_NO_TRADE:
        now_local = now_tpe()
        if (now_local.hour, now_local.minute) < STRATEGY_DECISION:
            return False
        print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} NO_TRADE 模式不追蹤")
        return 'BLOCKED'
    if is_follow_mode():
        return entry_follow_mode_price_check(state, realtime_sdk)
    if is_lower_mode():
        return entry_lower_mode_price_check(state, realtime_sdk)
    return 'BLOCKED'


def try_open_position(state: Dict[str, Any], mysdk):

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

    optimize_loss_per, optimize_profit_per = get_optimize_loss_profit_percent(state)
    open_stop_loss = entry_ref_px * (optimize_loss_per / 100.0)
    open_profit_target = entry_ref_px * (optimize_profit_per / 100.0)

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

            avg_price, mat_qty = get_order_fill_info(state["symbol_code"], mysdk)
            if (avg_price != 0) and (mat_qty == state.get("qty", 0)):
                state["entry_price"] = avg_price
            else:
                state["entry_price"] = entry_ref_px

            if side == TRADE_SIDE_SHORT:
                state["profit_price"] = max(
                    state.get("entry_price", 0) - open_profit_target,
                    state.get("limit_down_price", 0)
                )
                state["flat_price"] = min(
                    state.get("entry_price", 0) + open_stop_loss,
                    state.get("limit_up_price", 0)
                )
                print(f"[{state['symbol_name']}] 作空 已至入場時機，下單成功")
            else:
                state["profit_price"] = min(
                    state.get("entry_price", 0) + open_profit_target,
                    state.get("limit_up_price", 0)
                )
                state["flat_price"] = max(
                    state.get("entry_price", 0) - open_stop_loss,
                    state.get("limit_down_price", 0)
                )
                print(f"[{state['symbol_name']}] 作多 已至入場時機，下單成功")

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

    _protect_profit_stop(state)         # 獲利達標後，把停損推進到保住獲利的位置

    sl = reached_stop_to_flat(state)    # 至停損點
    rp = reached_resize_profit(state)   # 達到下一個獲利目標（純判斷，不修改 state）
    sp = reached_stop_to_profit(state)  # 追蹤停利反彈觸發
    ff = force_close_time_reached(state)     # 已至收盤

    if sl:
        close_flat_position(state, mysdk)
        print(f"[{state['symbol_name']}] ✅ 已至停損價格 {state['flat_price']}")
    elif rp:
        _advance_profit_trail(state)  # 推進追蹤停利（更新 state 並寫檔）
        print(f"[{state['symbol_name']}] ✅ 動態調整停利價格 停利：{state['stop_profit_price']} 下個目標：{state['profit_price']}")
    elif sp:
        close_profit_position(state, mysdk)
        print(f"[{state['symbol_name']}] ✅ 已至停利價格 {state['stop_profit_price']}")
    elif ff:
        endtime_close_position(state, mysdk)
        print(f"[{state['symbol_name']}] ✅ 己達平倉時間")
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


def _advance_profit_trail(state: Dict[str, Any]):
    """啟動或推進追蹤停利：更新 profit_price、stop_profit_price，並設定追蹤旗標。"""
    try:
        px = float(state.get("last_price"))
    except (TypeError, ValueError):
        return

    side = state.get("side")
    if side == TRADE_SIDE_LONG:
        if px >= 100:
            new_profit_price = px + PROFIT_BIG_TARGET_STEP
            new_stop_profit_price = px - PROFIT_BIG_BACK_STEP
        else:
            new_profit_price = px + PROFIT_SMALL_TARGET_STEP
            new_stop_profit_price = px - PROFIT_SMALL_BACK_STEP
        state["profit_price"] = min(new_profit_price, state.get("limit_up_price", 0))
    else:
        if px >= 100:
            new_profit_price = px - PROFIT_BIG_TARGET_STEP
            new_stop_profit_price = px + PROFIT_BIG_BACK_STEP
        else:
            new_profit_price = px - PROFIT_SMALL_TARGET_STEP
            new_stop_profit_price = px + PROFIT_SMALL_BACK_STEP
        state["profit_price"] = max(new_profit_price, state.get("limit_down_price", 0))

    state["stop_profit_price"] = new_stop_profit_price
    state["profit_tracking_active"] = True
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def close_profit_position(state: Dict[str, Any], mysdk):
    last_px = state.get("last_price", 0.0)
    side = state.get("side")
    exit_place_result = False

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

    # 停利時若市價失敗，保留倉位等待下一輪以更佳價格或條件再平倉
    if not exit_place_result:
        print(f'[{state.get("symbol_name")}] 停利市價平倉失敗，略過本輪等待下一輪')
        return

    print_close_position_log(state)
    state["traded"] = True
    state["in_position"] = False  # 確保持倉狀態為 False
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def close_flat_position(state: Dict[str, Any], mysdk):

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
            return

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
            return

        print(f'[{state.get("symbol_name")}] LONG 市價平倉失敗，已改掛預約平倉單')
        close_order_sent = True

    if not close_order_sent:
        return

    print_close_position_log(state)
    state["traded"] = True
    state["in_position"] = False  # 確保持倉狀態為 False
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def endtime_close_position(state: Dict[str, Any], mysdk):

    if state.get("traded") == True or state.get("in_position") == False:  # 已完成交易，不用平倉
        print(f'[{state.get("symbol_name")}] 已完成交易，不須強制平倉')
        state["traded"] = True
        state["in_position"] = False  # 確保持倉狀態為 False
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        return  # 這裡直接返回，是因為若無建倉就下平倉單，反而變成另起一張訂單

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
        return

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
            return

    if limit_order_result == False:
        print(f'[{state.get("symbol_name")}] 已至收盤時間，漲跌停平倉交易失敗，須手動下單平倉')


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
            )
            atomic_write_json(path, st)
        st["industry_code"] = str(industry_code).zfill(2)
        st["market_index_key"] = market_index_key
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
        )
        atomic_write_json(path, st)
        return st


def get_pure_symbol(symbolStr: str) -> Tuple[str, str]:
    symbol_with_suf = symbolStr.split(":")[1]
    symbol = symbol_with_suf.split(".")[0]
    return symbol, symbol_with_suf


def all_traded(states: Dict[str, Dict[str, Any]]) -> bool:
    return all(s.get("traded", False) for s in states.values())


def initialize_states(
    stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]],
    realtime_sdk: EsunMarketdata,
) -> Dict[str, Dict[str, Any]]:
    # 讀檔或初始化（程式啟動時先完成）
    states: Dict[str, Dict[str, Any]] = {}
    filtered_stocks: List[Tuple[str, int, float, float, float, float, str, float, Tuple[int, int]]] = []
    for symbolStr, qty, v1, v2, v3, v4, industry_code, volatility_value, streak_tuple in stocks:
        if MARKET_REVERSAL_STOP_EVENT.is_set():
            print("[MODE] 已觸發市場指數上下穿越，停止初始化個股資料")
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

        if limit_up_price > MAX_LIMIT_UP_PRICE:
            print(f"[{symbolStr}] ⚠️ 本日漲停價位太高，排除")
            continue

        if limit_down_price < MIN_LIMIT_DOWN_PRICE:
            print(f"[{symbolStr}] ⚠️ 本日跌停價位太低，排除")
            continue

        if not symbol_can_buy_day_trade:
            print(f"[{symbolStr}] ⚠️ 無法當沖，排除")
            continue

        if symbolStr.split(":")[1].split(".")[0] in []: # in ["1597", "8064"]
            print(f"[{symbolStr}] ⚠️ 高失敗率，排除")
            continue

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
        )
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
    while True:
        if MARKET_REVERSAL_STOP_EVENT.is_set():
            print("[MODE] 市場指數上下穿越已觸發，維持 NO_TRADE 並結束監控")
            return

        # round_has_market_update = False
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
                entry_mode, gate_results = decide_entry_mode_by_market_gate()
                apply_entry_mode_to_states(states, entry_mode)
                print_entry_mode_decision(entry_mode, gate_results)
                entry_mode_decided = True

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

        for st in states.values():
            if force_close_time_reached(st) and st.get("in_position") and not st.get("traded"):  # 仍有持倉且未交易
                # 強制平倉
                endtime_close_position(st, mysdk)
                # cancel_orders_and_close_position(st, mysdk)

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
                    st["last_price"] = px  # 最新價格
                    st["last_price_time"] = now_tpe().isoformat()
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
                            trigger_price = get_entry_trigger_price(st)
                            if trigger_price is None:
                                st["traded"] = True
                                st["entry_time"] = now_tpe().isoformat()
                                atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)
                                print(f"[{st['symbol_name']}] {now_tpe().strftime('%H:%M:%S')} 無法取得進場觸發價，不追蹤")
                                continue
                            if is_follow_mode():
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
        if MARKET_REVERSAL_STOP_EVENT.wait(timeout=sleep_sec):
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

        candidate_symbols = selected_stocks
        states = initialize_states(candidate_symbols, realtime_sdk)

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
