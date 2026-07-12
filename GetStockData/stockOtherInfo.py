#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time
import requests
import re
import math
import numpy as np
from pathlib import Path
from fractions import Fraction
import pytz
from datetime import date, timedelta, datetime
import json
from typing import Dict, Any, Deque, List

from pandas.core.dtypes.inference import is_float

from globals import start_min_number

MIS_ENDPOINT = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://mis.twse.com.tw/stock/index.jsp",
}

LINE_RE = re.compile(r'^\s*(.*?)\s*\(\s*([^)]+)\s*\)\s*$')

stockNameBracketsCodePath = "../papers/stock_name_list.txt"
def load_code_name_map() -> dict[str, str]:
    path = Path(stockNameBracketsCodePath)
    mapping: dict[str, str] = {}
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            m = LINE_RE.match(line)
            if not m:
                continue
            name, code = m.group(1), m.group(2)
            mapping[code] = name
    return mapping

def lookup_name_by_code(code: str):
    """輸入如 '2409.tw' 回傳對應名稱（如 '友達'）；找不到回傳 None。"""
    return load_code_name_map().get(code.strip())

def normalize(symbol: str):
    """回傳 (純代碼, 交易所)；若未帶 .tw/.two 直接丟錯。"""
    s = (symbol or "").strip().lower()
    if s.endswith(".two"):
        return s[:-4], "otc"
    if s.endswith(".tw"):
        return s[:-3], "tse"
    raise ValueError("參數必須帶市場尾碼：'.tw'（上市）或 '.two'（上櫃），例如 2330.tw / 8411.two")

def query_once(code: str, exch: str):
    """exch = 'tse' 或 'otc'；查不到回傳 None（不做備援）。"""
    ex_ch = f"{exch}_{code}.tw"
    r = requests.get(MIS_ENDPOINT,
                     params={"ex_ch": ex_ch, "json": "1", "delay": "0"},
                     headers=HEADERS, timeout=3)
    r.raise_for_status()
    arr = (r.json().get("msgArray") or [])
    if not arr:
        return None
    rec = arr[0]

    def to_int(x):
        x = (x or "").replace(",", "").strip()
        return 0 if (x == "" or x == "-") else int(float(x))

    return {
        "stock_no": code,
        "exchange": rec.get("ex") or exch,   # tse / otc
        "volume": to_int(rec.get("v")),      # 累積成交量
        "tick_volume": to_int(rec.get("tv")),# 當盤成交量
        "last_price": (rec.get("z") or "").strip(),
        "last_time": (rec.get("t") or "").strip(),
    }

def get_current_volume(symbol: str):
    code, exch = normalize(symbol)  # 若未帶尾碼，這裡直接丟 ValueError
    return query_once(code, exch)   # 只查指定市場，不做雙邊

def has_volume(stockId: str):
    try:
        info = get_current_volume(stockId)
    except ValueError as e:
        print(f"參數錯誤：{e}")
        sys.exit(2)
    except requests.RequestException as e:
        print(f"連線失敗：{e}")
        sys.exit(3)

    if not info:
        print(f"查無資料（可能代碼/市場不符或暫時無回應）：{stockId}")
        sys.exit(4)

    traded = info["volume"] > 0
    return traded


def calculateCumret(returns: np.ndarray):
    """
    向量化版本：用 cumprod 計算累積報酬，起始淨值=1。
    傳回值仍維持「累積報酬」序列（= 累積淨值 - 1），與你現有呼叫相容。
    """
    r = np.asarray(returns, dtype=float)
    # 避免 NaN 傳染；沒有訊號的日子當 0 報酬
    r = np.nan_to_num(r, nan=0.0)
    wealth = np.cumprod(1.0 + r)         # 累積淨值曲線（起始=1）
    return wealth - 1.0                   # 回傳累積報酬（與你原本介面一致）


def calculateMaxDD(cumret: np.ndarray):
    """
    標準回撤：(HWM - Wealth) / HWM
    - Wealth = 1 + cumret
    - maxDD 介於 [0, 1]，不會失真放大；maxDDD 為最長回撤持續「天數/Bar數」
    """
    # 轉回淨值曲線
    wealth = 1.0 + np.asarray(cumret, dtype=float)
    wealth = np.nan_to_num(wealth, nan=1.0)

    # 高水位線（逐步累積最大值）
    hwm = np.maximum.accumulate(wealth)

    # 標準回撤：相對峰值的跌幅百分比
    # 避免極端數值（理論上 hwm 不會是 0，這裡加保險）
    dd = (hwm - wealth) / np.where(hwm == 0.0, np.nan, hwm)
    dd = np.nan_to_num(dd, nan=0.0)

    # 回撤持續時間（連續低於 HWM 的期數）
    duration = np.zeros_like(wealth, dtype=int)
    run = 0
    for i in range(len(wealth)):
        if wealth[i] >= hwm[i] - 1e-12:   # 容忍極小誤差
            run = 0
        else:
            run += 1
        duration[i] = run

    maxDD  = float(dd.max()) if dd.size else 0.0
    maxDDD = int(duration.max()) if duration.size else 0
    return maxDD, maxDDD, dd, duration


def units_from_beta_prices(beta: float, price_y: float, price_x: float,
                           lot_y: int = 1000, lot_x: int = 1000,
                           max_leg_units: int = 50,
                           min_ratio: float = 1e-6):
    """
    依金額中性： qX * Px ≈ |beta| * qY * Py
    r = |beta| * Py / Px ≈ (qX/qY) * (lot_x/lot_y)

    回傳: (qY_shares, qX_shares, ratio_target, ratio_actual)
    """
    r = abs(beta) * (price_y / price_x)
    if not np.isfinite(r) or r < min_ratio:
        # 幾乎純單邊；可選擇直接跳過
        return (lot_y, 0, r, 0.0)

    # 先嘗試小整數近似
    frac = Fraction(r).limit_denominator(max_leg_units)
    num, den = frac.numerator, frac.denominator

    if num == 0:
        # Fallback：小腿 1 lot，大腿 ≈ 1/r lots（再做上限裁切）
        qx_lots = 1
        qy_lots = max(1, min(int(round(1.0 / r)), max_leg_units))
    else:
        # 正常情況：用小整數分數（注意：這是「lot 的比」）
        qx_lots = max(1, num)
        qy_lots = max(1, den)

    # 轉成股數
    qY = qy_lots * lot_y
    qX = qx_lots * lot_x

    # 實際 lot 比（拿來對照 r；理論上要比較 qx_lots/qy_lots ≈ r * (lot_y/lot_x)）
    ratio_target = r
    ratio_actual = (qX / qY) * (lot_y / lot_x)  # = qx_lots / qy_lots

    return (qY, qX, ratio_target, ratio_actual)


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

def adjust_price(price: float, trade_strategy: str) -> float:
    """
    根據 side ('Buy' 或 'Sell') 及價格，自動進位或捨去到符合 tick_size。
    Buy → 無條件進位
    Sell → 無條件捨去
    """
    tick = get_tick_size(price)
    if trade_strategy == "LONG":
        adjusted = math.ceil(price / tick) * tick  # 無條件進位
    elif trade_strategy == "SHORT":
        adjusted = math.floor(price / tick) * tick  # 無條件捨去
    else:
        adjusted = round(price / tick) * tick  # 四捨五入（預設行為）

    if price < 100:
        return round(adjusted, 2)
    elif price < 1000:
        return round(adjusted, 1)
    else:
        return int(adjusted)

# ============ 即時抓價（TW/TWO） ============
def get_realtime_price_totalvol(stock_id: str, realtime_sdk):
    codeNum = stock_id.split(".")[0]
    stock = realtime_sdk.rest_client.stock  # Stock REST API client

    stock_intraday_quote = stock.intraday.quote(symbol=codeNum)
    last_price = stock_intraday_quote['lastPrice']
    total_vol = stock_intraday_quote['total']['tradeVolume']

    return last_price, total_vol

def get_realtime_price(stock_id: str, realtime_sdk):
    codeNum = stock_id.split(".")[0]
    stock = realtime_sdk.rest_client.stock  # Stock REST API client

    stock_intraday_quote = stock.intraday.quote(symbol=codeNum)
    last_price = stock_intraday_quote['lastPrice']
    open_price = stock_intraday_quote['openPrice']
    high_price = stock_intraday_quote['highPrice']
    low_price = stock_intraday_quote['lowPrice']

    return last_price, open_price, high_price, low_price

def get_up_down_price(stock_id: str, realtime_sdk):
    codeNum = stock_id.split(".")[0]
    stock = realtime_sdk.rest_client.stock
    stock_intrady_tricker = stock.intraday.ticker(symbol=codeNum)
    time.sleep(0.5)  # 避免短時間過量 request
    limit_up_price = round(stock_intrady_tricker.get('limitUpPrice', 0), 2)
    limit_down_price = round(stock_intrady_tricker.get('limitDownPrice', 0), 2)
    return limit_up_price, limit_down_price

def symbol_intraday_ticker_info(stock_id: str, realtime_sdk):
    codeNum = stock_id.split(".")[0]
    stock = realtime_sdk.rest_client.stock
    stock_intrady_tricker = stock.intraday.ticker(symbol=codeNum)
    time.sleep(0.5)  # 避免短時間過量 request

    symbol_can_day_trade = stock_intrady_tricker.get('canDayTrade', False)
    symbol_can_buy_day_trade = stock_intrady_tricker.get('canBuyDayTrade', False)

    security_type = stock_intrady_tricker.get('securityType', '')

    industry = stock_intrady_tricker.get('industry', '')

    limit_up_price = stock_intrady_tricker.get('limitUpPrice', 0)
    limit_down_price = stock_intrady_tricker.get('limitDownPrice', 0)

    previousClose = stock_intrady_tricker.get('previousClose', 0)

    return symbol_can_day_trade, symbol_can_buy_day_trade, security_type, industry, limit_up_price, limit_down_price, previousClose

def analyze_strict_streak(responseData: Dict) -> tuple[int, int, bool, bool, bool]:

    bars = responseData.get("data", [])
    if len(bars) < 3:
        return 0, 0, False, False, False

    #print(bars)
    # 依日期由舊到新排序
    #bars_sorted = sorted(bars, key=lambda x: x["date"])
    #print(bars_sorted)

    up_ok = True
    down_ok = True

    curr = bars[0]
    prev = bars[1]
    pre_prev = bars[2]
    pre_pre_prev = bars[3]

    #print(f"分析最近K棒: 前前根 {preprev['date']} | 開={preprev['open']} 高={preprev['high']} 低={preprev['low']} 收={preprev['close']} || 前一根 {prev['date']} | 開={prev['open']} 高={prev['high']} 低={prev['low']} 收={prev['close']} || 目前根 {curr['date']} | 開={curr['open']} 高={curr['high']} 低={curr['low']} 收={curr['close']}")

    curr_h = float(curr["high"])
    curr_l = float(curr["low"])

    curr_c = float(curr["close"])
    curr_o = float(curr["open"])

    prev_c = float(prev["close"])
    prev_o = float(prev["open"])

    pre_prev_c = float(pre_prev["close"])
    pre_prev_o = float(pre_prev["open"])

    pre_pre_prev_c = float(pre_pre_prev["close"])
    pre_pre_prev_o = float(pre_pre_prev["open"])

    up_continue = 0
    if (curr_c > curr_o) and (prev_c > prev_o) and (pre_prev_c > pre_prev_o) and (pre_pre_prev_c > pre_pre_prev_o):
        up_continue = 4
    elif (curr_c > curr_o) and (prev_c > prev_o) and (pre_prev_c > pre_prev_o):
        up_continue = 3
    elif (curr_c > curr_o) and (prev_c > prev_o):
        up_continue = 2
    elif curr_c > curr_o:
        up_continue = 1

    down_continue = 0
    if (curr_c < curr_o) and (prev_c < prev_o) and (pre_prev_c < pre_prev_o) and (pre_pre_prev_c < pre_pre_prev_o):
        down_continue = 4
    elif (curr_c < curr_o) and (prev_c < prev_o) and (pre_prev_c < pre_prev_o):
        down_continue = 3
    elif (curr_c < curr_o) and (prev_c < prev_o) :
        down_continue = 2
    elif curr_c < curr_o:
        down_continue = 1

    #漲停或跌停
    is_limit_up = False
    is_limit_down = False
    if curr_c == curr_h:
        is_limit_up = True
    if curr_c == curr_l:
        is_limit_down = True

    is_flat = False
    if curr_o == curr_c:
        is_flat = True

    return up_continue, down_continue, is_limit_up, is_limit_down, is_flat


def symbol_historical_candles_continue_14(stock_id: str, realtime_sdk):
    codeNum = stock_id.split(".")[0]
    rest_stock = realtime_sdk.rest_client.stock

    today = date.today()
    yesterday = today - timedelta(days=16)  # 多抓幾天以確保有足夠的 K 棒資料


    responseData = rest_stock.historical.candles(
        **{
            "symbol": codeNum,
            "from": yesterday.strftime("%Y-%m-%d"),
            "to": today.strftime("%Y-%m-%d"),
        }
    )

    #print(f'symbol:{codeNum} from:{yesterday.strftime("%Y-%m-%d")} to:{today.strftime("%Y-%m-%d")} responseData:{responseData}')
    time.sleep(0.5)  # 避免短時間過量 request

    return analyze_strict_streak(responseData)

def now_tpe() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))

def reached_stop_to_mid(state: Dict[str, Any], px: float, avg_vol: float) -> bool:
    side = state.get("side")
    mid = state.get("mid")
    last_volume = state.get("last_volume")

    if side == "LONG":
        return (px < mid) # and (last_volume > (1.2 * avg_vol)) # 不考量成交量，只要價格到達停損點就平倉
    elif side == "SHORT":
        return (px > mid) # and (last_volume > (1.2 * avg_vol)) # 不考量成交量，只要價格到達停損點就平倉
    else:
        return False

def recent_avg_volume_from_deque(stock_volumn_dict: Dict[str, Deque], stock_code_suf: str) -> float:
    volumn_deque = stock_volumn_dict.get(stock_code_suf, None)
    if len(volumn_deque) < start_min_number:
        avg_volume = float('inf')  # 若成交量資料不足，回傳無限大，避免觸發條件
    else:
        avg_volume = sum(volumn_deque) / len(volumn_deque)
    return avg_volume

def now_over_timedelta(timestamp_str: str) -> timedelta:
     # now_over_timedelta("2025-10-23T09:42:00.123646+08:00") >= timedelta(minutes=24)
    timenow = now_tpe().replace(second=0, microsecond=0)
    timpeinputStrFormat = datetime.fromisoformat(timestamp_str)
    timeinput = timpeinputStrFormat.replace(second=0, microsecond=0)
    return timenow - timeinput

def save_candidate_symbols(candidate_symbols):
    filename = "candidate_symbols.txt"

    with open(filename, "w", encoding="utf-8") as f:  # 'w' = 覆蓋舊檔
        for item in candidate_symbols:
            f.write(str(item) + "," + "\n")  # 每筆一行

    print(f"已寫入 {filename}")

def get_stock_codes():
    # 以 stock_name_list.txt 取回股票代碼陣列
    stockNamePath = "../papers/stock_name_list.txt"

    pattern = re.compile(r"\(([^()]+)\)")  # 擷取括號中的內容

    stock_codes = []

    with open(stockNamePath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue  # 跳過空行

            match = pattern.search(line)
            if match:
                stock_codes.append(match.group(1))
            else:
                raise ValueError(f"無法解析股票代碼：{line}")

    return tuple(stock_codes)  # ✅ 與原本 JSON 版完全一致

def get_pairs_datas(): # 以 pairs_data.json 這個檔案取回配對好的股票清單
    pairsDataPath = "../papers/pairs_data.json"  # 儲存已篩選出的配對清單
    with open(pairsDataPath, "r", encoding="utf-8") as f:
        return tuple(json.load(f))  # 用 tuple 方便快取

def main():
    # 華榮:1608.tw <-> 野村全球航運龍頭:00960.tw

    symbol = '00960.tw'  # 可改這裡測試其他股票
    try:
        info = get_current_volume(symbol)
    except ValueError as e:
        print(f"參數錯誤：{e}")
        sys.exit(2)
    except requests.RequestException as e:
        print(f"連線失敗：{e}")
        sys.exit(3)

    if not info:
        print(f"查無資料（可能代碼/市場不符或暫時無回應）：{symbol}")
        sys.exit(4)

    traded = info["volume"] > 0
    print(
        f"[{info['exchange'].upper()}] {info['stock_no']} | "
        f"v={info['volume']:,} | tv={info['tick_volume']:,} | "
        f"z={info['last_price']} | t={info['last_time']} | 已有成交？{traded}"
    )

if __name__ == "__main__":
    main()

