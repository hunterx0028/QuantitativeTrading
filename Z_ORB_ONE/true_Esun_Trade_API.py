from configparser import ConfigParser
from esun_trade.sdk import SDK
from esun_trade.order import OrderObject
from esun_trade.constant import (APCode, Trade, PriceFlag, BSFlag, Action)
from esun_marketdata import EsunMarketdata
import time
from typing import Optional
config = ConfigParser()
config.read('config.ini')
realtime_sdk = EsunMarketdata(config)
realtime_sdk.login()
sdk = SDK(config)
sdk.login()
rest_stock = realtime_sdk.rest_client.stock

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
        time.sleep(1) # 避免下單頻率過快
    except Exception as e:
        print(f"[ERROR] {symbol_code_with_suf} : {priceInfo} {action_type} x {quantity} - {trade_type} - {e}")
        return False

    print(f"[ORDER] {symbol_code_with_suf} : {priceInfo} {action_type} x {quantity} - {trade_type}")
    return True


'''
# 歷史分K線，時間距離無法指定，應該是固定一個月
responseData = rest_stock.historical.candles(**{"symbol": "2891", 'timeframe': '1'})
responseData = sdk.get_order_results()
print(responseData)
'''

'''
# 交易額度及權限
responseData = sdk.get_trade_status()
print(responseData)
'''


'''
# 取得近 52 週股價數據
responseHistoricalStats = rest_stock.historical.stats(symbol = "00687B")
print(responseHistoricalStats)
'''


'''
# 今日即時1分K線
responseTodayCandles = rest_stock.intraday.candles(symbol="2476")
print(responseTodayCandles)
'''


'''
# 今日5分K線
responseTodayCandles = rest_stock.intraday.candles(symbol="1785", timeframe=5)
print(responseTodayCandles)
'''


'''
# 即時報價
stock_intraday_quote = rest_stock.intraday.quote(symbol="2316")
print(stock_intraday_quote)
'''


'''
# 歷史分K線
responseData = rest_stock.historical.candles(**{"symbol": "8064", "from": "2026-03-08", "to": "2026-04-24", 'timeframe': '1'})
print(responseData)
'''

'''
# 股票資訊
responseData = rest_stock.intraday.ticker(symbol="8096")
print(responseData)
'''


'''
type_place_order(sdk, "3006", Action.Sell, Trade.DayTradingSell, quantity=1, price_flag=PriceFlag.Limit, price=150)
'''


'''
# 歷史日K線
responseData = rest_stock.historical.candles(**{"symbol": "8064", "from": "2026-03-08", "to": "2026-04-22"})
print(responseData)
'''

'''
# 測試有確實登入
responseTodayCandles = rest_stock.intraday.ticker(symbol="8096")
print(responseTodayCandles)
type_place_order(sdk, "3006", Action.Sell, Trade.DayTradingSell, quantity=1, price_flag=PriceFlag.Limit, price=150)
'''

'''
# 即時報價
stock_intraday_quote = rest_stock.intraday.quote(symbol="2316")
print(stock_intraday_quote)
'''

# 今日即時1分K線
responseTodayCandles = rest_stock.intraday.candles(symbol="6770")
print(responseTodayCandles)


# 歷史日K線
# responseData = rest_stock.historical.candles(**{"symbol": "8096", "from": "2026-04-14", "to": "2026-04-14", 'timeframe': '1'})
# print(responseData)