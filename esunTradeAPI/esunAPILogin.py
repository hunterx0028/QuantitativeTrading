from configparser import ConfigParser
from esun_marketdata import EsunMarketdata
from esun_trade.sdk import SDK

config = ConfigParser()
config.read('config.ini')

realtime_sdk = EsunMarketdata(config)
try:
    realtime_sdk.logout()
except Exception as e:
    print(f"略過 realtime_sdk logout：{e}")
print("esun_marketdata 準備登入...")
realtime_sdk.login()
print("esun_marketdata 登入成功，可以使用")

sdk = SDK(config)
try:
    sdk.logout()
except Exception as e:
    print(f"略過 sdk logout：{e}")
print("esun_trade 準備登入...")
sdk.login()
print("esun_trade 登入成功，可以使用")
