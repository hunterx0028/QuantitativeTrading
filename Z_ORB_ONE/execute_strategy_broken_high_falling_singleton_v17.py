# -*- coding: utf-8 -*-
import os
import json
import time
import sys
import math
import shutil
import io
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
STOP_MONITOR_TIME = (13, 25)  # 13:25 停止監控時間
FORCE_EXIT_TIME = (13, 30)  # 13:30 強制關閉程式

OPTIMIZE_LOSS_PER = 3.0 # 停損百分比(%)，例如 2.5 代表入場價加上 2.5%

OPTIMIZE_PROFIT_PER = 6.0 # 停利百分比(%)，例如 6.0 代表入場價減去 6%

PROTECT_LOSS_PER = 1.0 # 新的停損

PROTECT_PROFIT_PER = 3.0 # 觸發調整停利

BUFFER_LOW_CHECK_END_TIME = (9, 5) # 可以調降昨低的時間

ENTRY_CHECK_START_TIME = (9, 41)  # 進場檢核開始時間（含）
ENTRY_CHECK_END_TIME = (10, 11)  # 進場檢核截止時間（含）

FORCE_CLOSE_TIME = (13, 0)  # 13:21 收盤前強制平倉時間

MAX_INTRADAY_RANGE_BEFORE_TRIGGER_PER = 5.0 # 觸發棒前當日高低價差上限(%)，以上一日最低價為基準

PREV_LOW_BARS_REQUIRED = 5  # 跌破昨低前，需連續幾根分K low >= 昨低

MAX_ENTRY_SLIPPAGE_TICKS = 3 # 跌破昨低達幾檔後不追空

ENTRY_ORDER_QUANTITY = 2 # 每次進場下單數量

PROFIT_BIG_BACK_STEP = 0.0 # 獲利後允許回撤多少
PROFIT_BIG_TARGET_STEP = 0.5 # 逐步獲利

PROFIT_SMALL_BACK_STEP = 0.0 # 獲利後允許回撤多少
PROFIT_SMALL_TARGET_STEP = 0.2 # 逐步獲利

MAX_LIMIT_UP_PRICE = 200 # 漲停不可超過的價格
MIN_LIMIT_DOWN_PRICE = 50 # 跌停不可超過的價格

ENABLE_MARKET_TREND_FILTER = True # 是否啟用盤勢濾網
MAX_MARKET_GAIN_PER = 0.3 # 指數相對昨日收盤漲幅超過此百分比，不允許放空

MARKET_INDEX_STATE: Dict[str, Dict[str, Any]] = {}

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
        # time.sleep(0.1) # 避免下單頻率過快
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


def get_market_key_for_symbol(symbol_code_with_suf: str) -> str:
    symbol_upper = str(symbol_code_with_suf or "").upper()
    if symbol_upper.endswith(".TWO"):
        return "TPEX"
    return "TWSE"


def start_market_index_stream(realtime_sdk: EsunMarketdata):
    if not ENABLE_MARKET_TREND_FILTER:
        return None

    symbol_to_market_key = {
        str(info.get("symbol", "")): market_key
        for market_key, info in market_previous_close_indices.items()
        if info.get("symbol")
    }
    if not symbol_to_market_key:
        print("[WARN] 未設定 market_previous_close_indices，盤勢濾網將等待指數資料")
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
        except Exception as e:
            print(f"[WARN] 處理盤勢指數訊息失敗: {e}")

    try:
        stock_ws = realtime_sdk.websocket_client.stock
        stock_ws.on("message", handle_message)
        stock_ws.connect()
        for index_symbol in symbol_to_market_key:
            stock_ws.subscribe({
                "channel": "indices",
                "symbol": index_symbol,
            })
            print(f"[MARKET] 已訂閱盤勢指數 {index_symbol}")
        return stock_ws
    except Exception as e:
        print(f"[WARN] 啟動盤勢指數 WebSocket 失敗，盤勢濾網將等待指數資料: {e}")
        return None


def market_trend_filter_pass(state: Dict[str, Any]) -> bool:
    if not ENABLE_MARKET_TREND_FILTER:
        return True

    market_key = get_market_key_for_symbol(state.get("symbol_code_with_suf", ""))
    index_config = market_previous_close_indices.get(market_key, {})
    previous_close = index_config.get("previous_close")
    market_state = MARKET_INDEX_STATE.get(market_key, {})
    last_index = market_state.get("last_index")

    try:
        previous_close_float = float(previous_close)
        last_index_float = float(last_index)
    except (TypeError, ValueError):
        print(f"[{state['symbol_name']}] 盤勢濾網等待 {market_key} 指數資料")
        return False

    if previous_close_float <= 0:
        print(f"[{state['symbol_name']}] 盤勢濾網 {market_key} 昨收指數設定錯誤: {previous_close}")
        return False

    market_gain_per = ((last_index_float - previous_close_float) / previous_close_float) * 100.0
    if market_gain_per > MAX_MARKET_GAIN_PER:
        print(
            f"[{state['symbol_name']}] 盤勢濾網未通過：{market_key} 指數漲幅 "
            f"{market_gain_per:.2f}% > {MAX_MARKET_GAIN_PER:.2f}%"
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
        "side": "",  # 'SHORT'
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
        "original_yesterday_low_price": v3,  # 原始昨低
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
        d.get("limit_up_price", 0.0),
        d.get("limit_down_price", 0.0),
        d.get("up_streak_days", 0),
        d.get("down_streak_days", 0),
    )
    base.update(d)

    # 若舊檔沒有 date，補上今天
    if "date" not in base or not base["date"]:
        base["date"] = today_str_tpe()

    if "original_yesterday_low_price" not in base or base["original_yesterday_low_price"] is None:
        base["original_yesterday_low_price"] = d.get("yesterday_low_price", base.get("yesterday_low_price"))

    return base


def force_close_time_reached() -> bool:
    t = now_tpe()
    return (t.hour, t.minute) >= FORCE_CLOSE_TIME


def force_monitor_time_reached() -> bool:
    t = now_tpe()
    return (t.hour, t.minute) >= STOP_MONITOR_TIME


# ============ 訊號與狀態邏輯 ============
def update_latest_candle_at_second_n(state: Dict[str, Any], realtime_sdk) -> list[Dict[str, Any]]:
    """
    取得今日分K，回傳排除最新一根（尚未完整）的其餘K棒。
    """
    try:
        code_num = state.get("symbol_code", "")
        rest_stock = realtime_sdk.rest_client.stock
        stock_intraday_candles = rest_stock.intraday.candles(symbol=code_num)
        all_candles = stock_intraday_candles.get("data", [])
    except Exception as e:
        print(f"[{state['symbol_name']}] ⚠️ 取得最新分K失敗：{e}")
        return []

    if not all_candles:
        print(f"[{state['symbol_name']}] ⚠️ 查無最新分K資料")
        return []

    normalized_all_candles = [
        {
            "open": candle.get("open"),
            "high": candle.get("high"),
            "low": candle.get("low"),
            "close": candle.get("close"),
            "date": candle.get("date"),
            "average": candle.get("average"),
            "volume": candle.get("volume"),
        }
        for candle in all_candles
    ]
    normalized_all_candles.sort(key=lambda candle: candle.get("date") or "")

    today_prefix = now_tpe().date().isoformat()
    today_candles = [
        candle for candle in normalized_all_candles
        if str(candle.get("date") or "").startswith(today_prefix)
    ]

    if len(today_candles) <= 1:
        return []
    return today_candles[:-1]


def get_latest_complete_candle_average_and_volume(
    completed_candles: list[Dict[str, Any]]
) -> tuple[float | None, float | None]:
    """
    從已完成分K清單中，取得最新一根分K的 average 與 volume。
    回傳 (average, volume)；若資料不足則回傳 (None, None)。
    """
    if not completed_candles:
        return None, None

    latest_complete_candle = completed_candles[-1]
    average_value = latest_complete_candle.get("average")
    volume_value = latest_complete_candle.get("volume")

    try:
        average_value = float(average_value) if average_value is not None else None
    except (TypeError, ValueError):
        average_value = None

    try:
        volume_value = float(volume_value) if volume_value is not None else None
    except (TypeError, ValueError):
        volume_value = None

    return average_value, volume_value


def are_recent_lows_above_or_equal_yesterday_low(
    completed_candles: list[Dict[str, Any]],
    state: Dict[str, Any],
) -> bool:
    """
    檢查 completed_candles 最後 PREV_LOW_BARS_REQUIRED 根分K的 low，是否皆 >= 昨低。
    completed_candles 預期為已排除觸發K棒後的K棒資料。
    """
    if PREV_LOW_BARS_REQUIRED <= 0:
        return False

    yesterday_low_price = state.get("yesterday_low_price")
    if yesterday_low_price is None:
        return False

    if len(completed_candles) < PREV_LOW_BARS_REQUIRED:
        return False

    try:
        yesterday_low = float(yesterday_low_price)
    except (TypeError, ValueError):
        return False

    recent_candles = completed_candles[-PREV_LOW_BARS_REQUIRED:]
    for candle in recent_candles:
        low_value = candle.get("low")
        try:
            if float(low_value) < yesterday_low:
                return False
        except (TypeError, ValueError):
            return False

    return True


def is_intraday_range_within_threshold_before_trigger(
    completed_candles: list[Dict[str, Any]],
    yesterday_low_price: float,
) -> bool:
    """
    檢查觸發棒前（已完成分K）的當日高低價差是否在允許範圍內。
    回傳 True 代表可繼續進場檢核；False 代表超過門檻或資料異常。
    """
    if not completed_candles:
        return True

    highs: list[float] = []
    lows: list[float] = []
    for candle in completed_candles:
        high_value = candle.get("high")
        low_value = candle.get("low")
        try:
            highs.append(float(high_value))
            lows.append(float(low_value))
        except (TypeError, ValueError):
            return False

    if not highs or not lows:
        return False

    intraday_range = max(highs) - min(lows)
    max_intraday_range_before_trigger = float(yesterday_low_price) * (MAX_INTRADAY_RANGE_BEFORE_TRIGGER_PER / 100.0)
    return intraday_range <= max_intraday_range_before_trigger


def is_intraday_range_within_threshold_by_realtime_prices(
    yesterday_low_price: float,
    high_price: float,
    low_price: float,
) -> bool:
    try:
        intraday_range = float(high_price) - float(low_price)
    except (TypeError, ValueError):
        return False

    max_intraday_range_before_trigger = float(yesterday_low_price) * (MAX_INTRADAY_RANGE_BEFORE_TRIGGER_PER / 100.0)
    return intraday_range <= max_intraday_range_before_trigger


def adjust_buffer_yesterday_low_for_states(
    states: Dict[str, Dict[str, Any]],
    realtime_sdk: EsunMarketdata,
):
    """
    等待 BUFFER_LOW_CHECK_END_TIME 後，用已完成分K調降有效昨低。
    調整範圍排除第一根K棒，且只取時間 <= BUFFER_LOW_CHECK_END_TIME 的分K。
    """
    target_hour, target_minute = BUFFER_LOW_CHECK_END_TIME
    target_second = 0

    print(f"正在等待時間到 {target_hour:02d}:{target_minute:02d}:{target_second:02d}，並於緩衝期間調降昨低 ...")

    while True:
        now_local = now_tpe()
        if (now_local.hour, now_local.minute, now_local.second) > (target_hour, target_minute, target_second):
            print(f"⏰ 時間到！目前時間：{now_local.strftime('%H:%M:%S')}")
            break
        time.sleep(5)

    for st in states.values():
        if st.get("traded"):
            continue

        completed_candles = update_latest_candle_at_second_n(st, realtime_sdk)
        if len(completed_candles) <= 1:
            print(f"[{st['symbol_name']}] 緩衝期分K資料不足，昨低不調整")
            continue

        buffer_lows: list[float] = []
        for candle in completed_candles[1:]:
            date_text = str(candle.get("date") or "")
            if len(date_text) < 16:
                continue
            try:
                candle_hour = int(date_text[11:13])
                candle_minute = int(date_text[14:16])
            except ValueError:
                continue

            if (candle_hour, candle_minute) > BUFFER_LOW_CHECK_END_TIME:
                continue

            low_value = candle.get("low")
            try:
                buffer_lows.append(float(low_value))
            except (TypeError, ValueError):
                continue

        if not buffer_lows:
            print(f"[{st['symbol_name']}] 緩衝期無可用分K，昨低不調整")
            continue

        buffer_low = min(buffer_lows)
        yesterday_low_price = st.get("yesterday_low_price")
        try:
            yesterday_low = float(yesterday_low_price)
        except (TypeError, ValueError):
            continue

        if buffer_low >= yesterday_low:
            continue

        symbol_key = st.get("symbol_code_with_suf", "")
        st["yesterday_low_price"] = buffer_low
        st["entry_time"] = now_tpe().isoformat()
        limit_down_price = st.get("limit_down_price")
        entry_price = adjust_price(buffer_low - get_tick_size(buffer_low), "SHORT")
        if should_skip_entry_by_limit_down_zone(entry_price, buffer_low, limit_down_price):
            st["traded"] = True
            st["in_position"] = False
            atomic_write_json(state_path(symbol_key), st)
            print(f"[{st['symbol_name']}] 緩衝期調降昨低至 {buffer_low}，進場價低於昨低到跌停三分之一位置，不追蹤。")
            continue

        atomic_write_json(state_path(symbol_key), st)
        print(f"[{st['symbol_name']}] 緩衝期調降昨低：{yesterday_low} -> {buffer_low}")


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


def entry_price_check(state: Dict[str, Any], realtime_sdk: EsunMarketdata) -> bool | str:
    """
    進場條件判斷（純函式，不修改 state）。

    回傳值：
      True      — 條件成立，應進場（side / entry_trigger_price 由呼叫端設定）
      'BLOCKED' — 驗證失敗，需永久封鎖本日進場（呼叫端負責設 traded=True）
      False     — 尚未觸發，繼續等待下一輪
    """
    now_local = now_tpe()
    if (now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME:
        return False
    if (now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME:
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
        ylow = float(yesterday_low_price)
        last_px = float(last_price)
        best_bid = float(best_bid_price)
    except (TypeError, ValueError):
        return False

    ylow_tick_size = get_tick_size(ylow)
    trigger_price = adjust_price(ylow - ylow_tick_size, "SHORT")
    max_slippage_price = adjust_price(ylow - (ylow_tick_size * MAX_ENTRY_SLIPPAGE_TICKS), "SHORT")

    if best_bid <= trigger_price and last_px <= trigger_price:  # 買一與成交價皆跌破昨低下一檔才進場
        if last_px <= max_slippage_price:
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 已跌破昨低達 {MAX_ENTRY_SLIPPAGE_TICKS} 檔，不追蹤")
            return 'BLOCKED'

        if not market_trend_filter_pass(state):
            return False

        high_price = state.get("high_price")
        original_yesterday_low_price = state.get("original_yesterday_low_price")
        try:
            if float(high_price) <= float(original_yesterday_low_price):
                print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 入場前最高價未大於原始昨低，突破失敗，不追蹤")
                return 'BLOCKED'
        except (TypeError, ValueError):
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 無法檢核原始昨低與最高價，突破失敗，不追蹤")
            return 'BLOCKED'

        completed_candles = update_latest_candle_at_second_n(state, realtime_sdk)
        if not completed_candles:
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} K棒資料不足，突破失敗，不追蹤")
            return 'BLOCKED'

        if not is_intraday_range_within_threshold_before_trigger(completed_candles, float(yesterday_low_price)):
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 已跌破昨低，但觸發前高低價差過大，不追蹤")
            return 'BLOCKED'

        if not are_recent_lows_above_or_equal_yesterday_low(completed_candles, state):
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 之前K棒未能保持在昨低之上，突破失敗，不追蹤")
            return 'BLOCKED'

        latest_average, _latest_volume = get_latest_complete_candle_average_and_volume(completed_candles)
        if (latest_average is None) or (latest_average <= float(yesterday_low_price)):
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 均價小於昨低，突破失敗，不追蹤")
            return 'BLOCKED'

        if should_skip_entry_by_limit_down_zone(last_px, ylow, limit_down_price):
            print(f"[{state['symbol_name']}] {now_local.strftime('%H:%M:%S')} 進場價低於昨低到跌停三分之一位置，不追蹤。")
            return 'BLOCKED'

        return True

    # 高頻路徑：entry 條件尚未觸發，繼續等待
    return False


def try_open_position(state: Dict[str, Any], mysdk):

    last_px = state.get("last_price", 0.0) # 現價
    entry_ref_px = state.get("entry_trigger_price", last_px)  # 進場參考價：昨低-1tick
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

    open_stop_loss = entry_ref_px * (OPTIMIZE_LOSS_PER / 100.0)
    open_profit_target = entry_ref_px * (OPTIMIZE_PROFIT_PER / 100.0)

    if state.get("side") != "SHORT":
        state["traded"] = True
        state["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] 僅允許 SHORT，不執行")
        return

    if state.get("side") == "SHORT":
        limit_up_price = state.get("limit_up_price", 0)  # 漲停
        if (entry_ref_px + open_stop_loss) >= limit_up_price:
            state["traded"] = True
            state["entry_time"] = now_tpe().isoformat()
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            print(f"[{state['symbol_name']}] SHORT 停損超過漲停，空間太小，不執行")
            return

    if check_open_status(state):
        if state.get("side") == "SHORT": # SHORT 作空

            place_order_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Sell, Trade.DayTradingSell, quantity=qty, price_flag=PriceFlag.Market, price=entry_ref_px)
            if place_order_result:  # 下單成功
                state["in_position"] = True

                avg_price, mat_qty = get_order_fill_info(state["symbol_code"], mysdk)
                if (avg_price != 0) and (mat_qty == state.get("qty", 0)):
                    state["entry_price"] = avg_price
                else:
                    state["entry_price"] = entry_ref_px

                candidate_profit = max(state.get("entry_price", 0) - open_profit_target, state.get("limit_down_price", 0))

                state["profit_price"] = candidate_profit
                state["profit_tracking_active"] = False  # 追蹤停利尚未啟動，等到首次觸及 profit_price 才開始
                state["flat_price"] = min(
                    state.get("entry_price", 0) + open_stop_loss,
                    state.get("limit_up_price", 0)
                )

                print(f"[{state['symbol_name']}] SHORT 已至入場時機，下單成功")
                # type_place_order(mysdk, state["symbol_code_with_suf"], Action.Buy, Trade.Cash, quantity=qty, price_flag=PriceFlag.LimitDown, price=0) # 舊方案註解：目前不預掛，避免增加額度。
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
    ff = force_close_time_reached()     # 已至收盤

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
    """SHORT 獲利達標後，將 flat_price 下修到至少保住指定獲利的位置。"""
    if state.get("side") != "SHORT":
        return

    try:
        px = float(state.get("last_price"))
        entry_price = float(state.get("entry_price"))
        current_flat_price = float(state.get("flat_price"))
    except (TypeError, ValueError):
        return

    if entry_price <= 0:
        return

    protect_trigger_price = entry_price * (1 - PROTECT_PROFIT_PER / 100.0)
    protected_flat_price = entry_price * (1 - PROTECT_LOSS_PER / 100.0)

    if px <= protect_trigger_price and protected_flat_price < current_flat_price:
        state["flat_price"] = protected_flat_price
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        print(f"[{state['symbol_name']}] ✅ 獲利保護啟動 停損調整為：{state['flat_price']}")


def reached_stop_to_flat(state: Dict[str, Any]) -> bool:

    side = state.get("side")
    px = state.get("last_price")
    flat_price = state.get("flat_price")

    if side == "SHORT":
        return px >= flat_price # 只要價格大於停損點就平倉
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

    if side == "SHORT":
        return px >= stop_profit_price
    return False


def reached_resize_profit(state: Dict[str, Any]) -> bool:
    """純判斷：現價是否已達下一個獲利目標點（不修改 state）。"""
    side = state.get("side")
    try:
        px = float(state.get("last_price"))
        profit_price = float(state.get("profit_price"))
    except (TypeError, ValueError):
        return False

    if side == "SHORT":
        return px <= profit_price
    return False


def _advance_profit_trail(state: Dict[str, Any]):
    """啟動或推進追蹤停利：更新 profit_price、stop_profit_price，並設定追蹤旗標。"""
    try:
        px = float(state.get("last_price"))
    except (TypeError, ValueError):
        return

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

    if side == "SHORT":
        exit_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Buy,
            Trade.Cash,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.Market,
            price=last_px
        )

    # 停利時若市價失敗，保留倉位等待下一輪以更佳價格或條件再平倉
    if not exit_place_result:
        print(f'[{state.get("symbol_name")}] 停利市價平倉失敗，略過本輪等待下一輪')
        return

    state["traded"] = True
    state["in_position"] = False  # 確保持倉狀態為 False
    atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)


def close_flat_position(state: Dict[str, Any], mysdk):

    last_px = state.get("last_price", 0.0)
    side = state.get("side")
    exit_place_result = False

    # SHORT：先嘗試市價買回平倉
    if side == "SHORT":
        exit_place_result = type_place_order(
            mysdk,
            state["symbol_code_with_suf"],
            Action.Buy,
            Trade.Cash,
            quantity=state.get("qty", 0),
            price_flag=PriceFlag.Market,
            price=last_px
        )

    # 集合競價等情境若無法市價，改掛預約單
    if not exit_place_result and side == "SHORT":
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

    if side == "SHORT":
        exit_place_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Buy, Trade.Cash,
                                             quantity=state.get("qty", 0),
                                             price_flag=PriceFlag.Market, price=last_px)
    if exit_place_result: # 有成功平倉
        # 更新狀態
        state["traded"] = True
        state["in_position"] = False  # 確保持倉狀態為 False
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        return

    # 強制以漲跌停平倉結果
    limit_order_result = False

    # 市價平倉失敗的話，改用漲停價回補
    if not exit_place_result:
        if side == "SHORT":
            limit_order_result = type_place_order(mysdk, state["symbol_code_with_suf"], Action.Buy, Trade.Cash,
                                                  quantity=state.get("qty", 0), price_flag=PriceFlag.LimitUp, price=0)
        if limit_order_result:
            state["traded"] = True
            state["in_position"] = False  # 確保持倉狀態為 False
            atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
            return

    if limit_order_result == False:
        print(f'[{state.get("symbol_name")}] 已至收盤時間，漲跌停平倉交易失敗，須手動下單平倉')


def close_monitor(state: Dict[str, Any]):
    if state.get("in_position") or state.get("traded"): # 已持倉或已交易就不處理
        return
    else:
        state["traded"] = True
        state["in_position"] = False  # 確保持倉狀態為 False
        atomic_write_json(state_path(state.get("symbol_code_with_suf", "")), state)
        return


# ============ 主監控流程 ============
def load_or_init_state(
    symbol: str,
    qty: int,
    v1: float,
    v2: float,
    v3: float,
    v4: float,
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
                limit_up_price,
                limit_down_price,
                up_streak_days,
                down_streak_days,
            )
            atomic_write_json(path, st)
        return st
    else:
        st = build_initial_state(
            symbol,
            qty,
            v1,
            v2,
            v3,
            v4,
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
        _, code_with_suf = get_pure_symbol(symbolStr)
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

        quantity = ENTRY_ORDER_QUANTITY # 這次進場的數量

        st = load_or_init_state(
            symbolStr,
            quantity,
            v1,
            v2,
            v3,
            v4,
            limit_up_price,
            limit_down_price,
            up_streak_days,
            down_streak_days,
        )
        states[code_with_suf] = st
        filtered_stocks.append((symbolStr, qty, v1, v2, v3, v4, industry_code, volatility_value, streak_tuple))

        st["entry_time"] = now_tpe().isoformat()
        atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)

    # 將過濾後名單回寫至原始 stocks（例如 selected_stocks）
    stocks[:] = filtered_stocks
    persist_selected_stocks_to_stock_data(filtered_stocks)

    return states


def monitor(states: Dict[str, Dict[str, Any]], mysdk: SDK, realtime_sdk: EsunMarketdata):
    update_status = False
    entry_check_start_announced = False
    entry_check_end_announced = False
    stop_monitor_processed = False
    while True:
        # round_has_market_update = False
        now_local = now_tpe()
        if (not entry_check_start_announced) and ((now_local.hour, now_local.minute) >= ENTRY_CHECK_START_TIME):
            print(f"⏰ 進場檢核開始時間！目前時間：{now_local.strftime('%H:%M:%S')}")
            entry_check_start_announced = True

        if (not entry_check_end_announced) and ((now_local.hour, now_local.minute, now_local.second) >= (ENTRY_CHECK_END_TIME[0], ENTRY_CHECK_END_TIME[1], 0)):
            print(f"⏰ 進場檢核截止時間！目前時間：{now_local.strftime('%H:%M:%S')}")
            entry_check_end_announced = True

        monitor_time_reached = force_monitor_time_reached()
        close_time_reached = force_close_time_reached()
        round_should_update_realtime = update_status

        if monitor_time_reached and not stop_monitor_processed:
            for state in states.values():
                close_monitor(state)
                print(f"[{state['symbol_name']}] ✅ 停止 己達停止追蹤時間")
            stop_monitor_processed = True

        if close_time_reached:
            for st in states.values():
                if st.get("in_position") and not st.get("traded"):  # 仍有持倉且未交易
                    # 強制平倉
                    endtime_close_position(st, mysdk)
                    # cancel_orders_and_close_position(st, mysdk)

        # 已全數完成交易則收工
        if all_traded(states):
            if close_time_reached:
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

                    yesterday_low_price = st.get("yesterday_low_price")
                    if (
                        ((now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME)
                        and (yesterday_low_price is not None)
                        and (high_price is not None)
                        and (low_price is not None)
                        and (not is_intraday_range_within_threshold_by_realtime_prices(float(yesterday_low_price), float(high_price), float(low_price)))
                    ):
                        st["traded"] = True
                        st["entry_time"] = now_tpe().isoformat()
                        atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)
                        print(f"[{st['symbol_name']}] {now_tpe().strftime('%H:%M:%S')} 觸發前高低價差過大，不追蹤")
                        continue

                    if (
                        ((now_local.hour, now_local.minute) < ENTRY_CHECK_START_TIME)
                        and ((now_local.hour, now_local.minute) > BUFFER_LOW_CHECK_END_TIME)
                        and (yesterday_low_price is not None)
                        and (low_price is not None)
                        and (low_price < yesterday_low_price)
                        ):
                        st["traded"] = True
                        st["entry_time"] = now_tpe().isoformat()
                        atomic_write_json(state_path(st.get("symbol_code_with_suf", "")), st)
                        print(f"[{st['symbol_name']}] {now_tpe().strftime('%H:%M:%S')} 檢核時間前已先突破昨低，不追蹤")
                        continue

                    if (
                        ((now_local.hour, now_local.minute) > ENTRY_CHECK_END_TIME)
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
                            ylow = float(st.get("yesterday_low_price", 0))
                            st["side"] = "SHORT"
                            st["entry_trigger_price"] = adjust_price(ylow - get_tick_size(ylow), "SHORT")
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
        time.sleep(sleep_sec)
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

    try:
        clear_state_dir()

        # 登入以操作API
        config = ConfigParser()
        config.read('config.ini')

        realtime_sdk = EsunMarketdata(config)
        realtime_sdk.login()
        start_market_index_stream(realtime_sdk)

        sdk = SDK(config)
        sdk.login()

        candidate_symbols = selected_stocks
        states = initialize_states(candidate_symbols, realtime_sdk)

        adjust_buffer_yesterday_low_for_states(states, realtime_sdk)

        # 對齊到下一個 5 秒邊界，避免第一輪跨分鐘造成額外更新
        align_now = now_tpe()
        align_next = ceil_next_interval(align_now, 5)
        align_sleep_sec = max(0.2, (align_next - align_now).total_seconds())
        time.sleep(align_sleep_sec)

        # 開始正式作業
        monitor(states, sdk, realtime_sdk)
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        try:
            with open(execute_result_path, "w", encoding="utf-8") as f:
                f.write(capture_buffer.getvalue())
        except Exception as e:
            print(f"[WARN] 無法輸出 execute_strategy_result.txt：{e}", file=original_stderr)
