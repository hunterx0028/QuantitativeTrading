from configparser import ConfigParser
from esun_marketdata import EsunMarketdata

config = ConfigParser()
config.read('config.ini')
sdk = EsunMarketdata(config)

print("準備登入")
sdk.login()

print("準備取回rest_client")
rest_stock = sdk.rest_client.stock

print("準備取回intraday.candles")
responseTodayCandles = rest_stock.intraday.candles(symbol='2324')
print(responseTodayCandles.get("data")[-1])
print(responseTodayCandles.get("data")[-1].get("high"))

#responseYesterdayCandles = rest_stock.historical.candles(**{"symbol": "2324", "timeframe": "1"})
#print(responseYesterdayCandles)

'''
# —— 使用示例 —— #
# 假設你拿到的兩個回應物件如下（以你的範例結構）：
today_resp = rest_stock.intraday.candles(symbol='2324')
history_resp = rest_stock.historical.candles(**{"symbol": "2324", "timeframe": "1"})

df_1m = build_1m_dataframe(today_resp, history_resp)
day_str = today_resp.get("date") # 建議用回應裡的 'date'
result = robust_orb_width(df_1m, day=day_str, product_type="stock_large", lookback_days=20, use_ema=True)
print(result)
# -> {'W_today_raw': ..., 'W_today_eff': ..., 'W_avg': ..., 'alpha_used': ..., 'ATR': ..., 'W_final': ...}
'''




#print(match_today_summary_by_stock_and_qty('2836', 270, '2836', 270, sdk))
#print(match_otherday_summary_by_stock_and_qty('2836', 270, '2836', 270, sdk))

