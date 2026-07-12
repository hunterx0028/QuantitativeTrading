# -*- coding: utf-8 -*-

import builtins
import time
from datetime import date, timedelta
from esun_marketdata import EsunMarketdata
from configparser import ConfigParser
from pathlib import Path
from typing import Dict, List
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING

ATR_PERIOD = 14

ETF_CODE = ["24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "45", "46", "47"]
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = CURRENT_DIR.parent
OUTPUT_FILE = CURRENT_DIR / "yesterday_selector_result.txt"
CONFIG_PATH = PROJECT_DIR / "config.ini"
PRINT_BUFFER: List[str] = []


def reset_output_file() -> None:
    PRINT_BUFFER.clear()
    OUTPUT_FILE.write_text("", encoding="utf-8")


def print(*args, **kwargs):
    builtins.print(*args, **kwargs)

    target_file = kwargs.get("file")
    if target_file is not None:
        return

    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "\n")
    PRINT_BUFFER.append(sep.join(map(str, args)) + end)
    OUTPUT_FILE.write_text("".join(PRINT_BUFFER), encoding="utf-8")


def normalize_config_paths(config: ConfigParser, config_file: Path) -> None:
    if not config.has_section("Cert"):
        return

    cert_path = config.get("Cert", "Path", fallback="").strip()
    if cert_path and not Path(cert_path).is_absolute():
        config.set("Cert", "Path", str((config_file.parent / cert_path).resolve()))

def fmt_bool(b: bool) -> str:
    return "✅ True" if b else "❌ False"


def calculate_atr(responseData: Dict, period: int = ATR_PERIOD) -> float:
    bars = responseData.get("data", [])
    if len(bars) < 2:
        return 0.0

    tr_values = []
    usable_period = min(period, len(bars) - 1)
    for i in range(usable_period):
        curr = bars[i]
        prev = bars[i + 1]

        curr_h = float(curr["high"])
        curr_l = float(curr["low"])
        prev_c = float(prev["close"])

        tr = max(
            curr_h - curr_l,
            abs(curr_h - prev_c),
            abs(curr_l - prev_c),
        )
        tr_values.append(tr)

    return round(sum(tr_values) / len(tr_values), 4) if tr_values else 0.0


def get_tick_size(price: float) -> float:
    if price < 10:
        return 0.01
    elif price < 50:
        return 0.05
    elif price < 100:
        return 0.1
    elif price < 500:
        return 0.5
    elif price < 1000:
        return 1
    else:
        return 5


def round_price(price: float, tick: float) -> float:
    """
    以 tick 為單位做四捨五入（.5 一律進位，ROUND_HALF_UP）。
    """
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    rounded_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return float(rounded_units * tick_dec)


def floor_price_to_tick(price: float, tick: float) -> float:
    """將價格無條件捨去到合法 tick。"""
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    floored_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_FLOOR)
    return float(floored_units * tick_dec)


def ceil_price_to_tick(price: float, tick: float) -> float:
    """將價格無條件進位到合法 tick。"""
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    ceiled_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return float(ceiled_units * tick_dec)


def calculate_limit_prices(prev_close: float):
    # 原始價格（未調整）
    up_raw = prev_close * 1.10
    down_raw = prev_close * 0.90

    # 台股漲跌停價:
    # 漲停價用「無條件捨去」，跌停價用「無條件進位」；
    # tick 依各自價位區間判斷。
    limit_up_tick = get_tick_size(up_raw)
    limit_down_tick = get_tick_size(down_raw)
    limit_up = floor_price_to_tick(up_raw, limit_up_tick)
    limit_down = ceil_price_to_tick(down_raw, limit_down_tick)

    return limit_up, limit_down


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

def get_symbol_recent_ohlc(stock_str: str, realtime_sdk):
    code_num = stock_str.split(".")[0].split(":")[1]
    stock = realtime_sdk.rest_client.stock
    stock_intraday_quote = stock.intraday.quote(symbol=code_num)
    time.sleep(0.5)  # 避免短時間過量 request

    open_price = stock_intraday_quote.get('openPrice')
    high_price = stock_intraday_quote.get('highPrice')
    low_price = stock_intraday_quote.get('lowPrice')
    close_price = stock_intraday_quote.get('closePrice')

    return stock_str, open_price, high_price, low_price, close_price

def symbol_historical_candles_continue_14(stock_id: str, realtime_sdk):
    codeNum = stock_id.split(".")[0]
    rest_stock = realtime_sdk.rest_client.stock

    today = date.today()
    from_day = today - timedelta(days=30)  # 預留長假與休市空窗，確保有足夠的交易日 K 棒資料


    responseData = rest_stock.historical.candles(
        **{
            "symbol": codeNum,
            "from": from_day.strftime("%Y-%m-%d"),
            "to": today.strftime("%Y-%m-%d"),
        }
    )

    #print(f'symbol:{codeNum} from:{from_day.strftime("%Y-%m-%d")} to:{today.strftime("%Y-%m-%d")} responseData:{responseData}')
    time.sleep(0.5)  # 避免短時間過量 request

    up_continue, down_continue, is_limit_up, is_limit_down, is_flat = analyze_strict_streak(responseData)
    atr = calculate_atr(responseData)
    return up_continue, down_continue, is_limit_up, is_limit_down, is_flat, atr

def analyze_strict_streak(responseData: Dict) -> tuple[int, int, bool, bool, bool]:

    bars = responseData.get("data", [])
    if not bars:
        return 0, 0, False, False, False

    curr = bars[0]

    curr_h = float(curr["high"])
    curr_l = float(curr["low"])

    curr_c = float(curr["close"])
    curr_o = float(curr["open"])

    up_continue = 0
    if curr_c > curr_o:
        for bar in bars:
            bar_c = float(bar["close"])
            bar_o = float(bar["open"])
            if bar_c > bar_o:
                up_continue += 1
            else:
                break

    down_continue = 0
    if curr_c < curr_o:
        for bar in bars:
            bar_c = float(bar["close"])
            bar_o = float(bar["open"])
            if bar_c < bar_o:
                down_continue += 1
            else:
                break

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

def check_orb_filters_for_symbols(realtime_sdk: EsunMarketdata, symbols) -> List[tuple]:

    order_target_arr = []
    for symbol, v1, v2, v3, v4 in symbols:
        _, code = symbol.split(':', 1)
        try:

            tomorrow_limit_up, tomorrow_limit_down = calculate_limit_prices(v4) # v4 是收盤價

            if tomorrow_limit_up > 200: # 價位太高
                print(f'symbol:{symbol} 漲停價位太高，跳過')
                continue

            if tomorrow_limit_down < 50: # 價位太低
                print(f'symbol:{symbol} 跌停價位太低，跳過')
                continue

            symbol_can_day_trade, symbol_can_buy_day_trade, security_type, industry, _, _, _ = symbol_intraday_ticker_info(code, realtime_sdk)

            if security_type in ETF_CODE:  # 型態:"00" -> ETF - 0.8
                print(f'symbol:{symbol} 不考慮ETF，跳過')
                continue # 目前不考慮ETF

            '''
            if not symbol_can_buy_day_trade:
                print(f'symbol:{symbol} 無法當沖，跳過')
                continue
            '''

            '''
            if industry in EXCLUDE_INDUSTRY_CODE:
                print(f'symbol:{symbol} 不考慮 15航運業 17金融保險業 10鋼鐵工業，跳過')
                continue # 目前不考慮的產業
            '''

            # 股票漲跌連續天數
            up_continue, down_continue, is_limit_up, is_limit_down, is_flat, atr = symbol_historical_candles_continue_14(code, realtime_sdk)

            if atr < 4:
                print(f'symbol:{symbol} 真實平均波動區間太小，跳過')
                continue

            qty = 1

            order_target_arr.append((symbol, qty, v1, v2, v3, v4, industry, atr, (up_continue, down_continue)))

        except Exception as e:
            print(f'symbol:{symbol} check_orb_filters_for_symbols {e}')

    print("=" * 72)
    for target in order_target_arr:
        print(f"{target},")
    return order_target_arr


def get_top_volume_symbols(realtime_sdk):
    rest_stock = realtime_sdk.rest_client.stock

    TSE_LIMIT = 200
    OTC_LIMIT = 100

    """
    取得台股成交量前 N 名股票清單
    輸出格式：['台積電:2330.TW', '緯創:3231.TW', ...]
    """
    resp_tse = rest_stock.snapshot.actives(market='TSE', trade='volume')
    time.sleep(0.5)  # 避免短時間過量 request

    print(f'上市 snapshot.actives:{resp_tse["date"]} {resp_tse["time"]}')  # 印出時間戳記，確認資料是最新的

    # 安全檢查
    if not resp_tse or 'data' not in resp_tse:
        result_tse = []
    else:
        data_tse = resp_tse['data'][:TSE_LIMIT]  # 取前 N 筆
        result_tse = [(f"{item_tse['name']}:{item_tse['symbol']}.TW", item_tse['openPrice'], item_tse['highPrice'], item_tse['lowPrice'], item_tse['closePrice']) for item_tse in data_tse]

    """
        取得上櫃成交量前 N 名股票清單
        輸出格式：['聯光通:4903.TWO', '凱崴:8498.TWO', ...]
    """

    resp_otc = rest_stock.snapshot.actives(market='OTC', trade='volume')
    time.sleep(0.5)  # 避免短時間過量 request

    print(f'上櫃 snapshot.actives:{resp_otc["date"]} {resp_otc["time"]}')  # 印出時間戳記，確認資料是最新的

    # 安全檢查
    if not resp_otc or 'data' not in resp_otc:
        result_otc = []
    else:
        data_otc = resp_otc['data'][:OTC_LIMIT]  # 取前 N 筆
        result_otc = [(f"{item_otc['name']}:{item_otc['symbol']}.TWO", item_otc['openPrice'], item_otc['highPrice'], item_otc['lowPrice'], item_otc['closePrice']) for item_otc in data_otc]

    result = result_tse + result_otc
    return result

# === 範例呼叫（請依你的情境帶入日期區間）===
if __name__ == "__main__":
    reset_output_file()

    # 登入以操作API
    config = ConfigParser()
    config.read(CONFIG_PATH, encoding="utf-8")
    normalize_config_paths(config, CONFIG_PATH)

    realtime_sdk = EsunMarketdata(config)
    realtime_sdk.login()

    # symbols = ['南亞科:2408.TW', '旺宏:2337.TW', '主動統一台股增長:00981A.TW']

    # symbols = safe_fetch_yahoo_volume(30)

    symbols = get_top_volume_symbols(realtime_sdk)
    # print(symbols)

    orderTargets = check_orb_filters_for_symbols(realtime_sdk, symbols)
    #print(orderTargets)

    pre_symbol= [

    ]

    order_target_symbols = {target[0] for target in orderTargets}
    missing_symbols = [symbol_info[0] for symbol_info in pre_symbol if symbol_info[0] not in order_target_symbols]

    missing_symbol_prices = []
    for symbol_name in missing_symbols:
        try:
            missing_symbol_prices.append(get_symbol_recent_ohlc(symbol_name, realtime_sdk))
        except Exception as e:
            print(f'symbol:{symbol_name} get_symbol_recent_ohlc {e}')

    missingSymbolTargets = check_orb_filters_for_symbols(realtime_sdk, missing_symbol_prices)
