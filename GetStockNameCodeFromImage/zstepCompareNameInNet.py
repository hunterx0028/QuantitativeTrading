import json
import time
from configparser import ConfigParser
from esun_marketdata import EsunMarketdata
from globals import ETF_CODE

# === 初始化設定 ===
config = ConfigParser()
config.read("config.ini")

sdk = EsunMarketdata(config)
sdk.login()

rest_stock = sdk.rest_client.stock

# === 輸入文字檔 ===
input_file = "../papers/temp_code_buffer.txt"

with open(input_file, "r", encoding="utf-8") as f:
    stock_codes = [line.strip() for line in f if line.strip()]

stock_code_list = []
stock_name_list = []

for code in stock_codes:
    try:
        # resp = rest_stock.historical.stats(symbol=code)
        # time.sleep(1)

        resp = rest_stock.intraday.ticker(symbol=code)
        time.sleep(0.1)  # 避免短時間過量 request

        if not resp or "name" not in resp or "market" not in resp:
            print(f"⚠️ 無法取得股票資料：{code}")
            continue

        name = resp.get('name', '')
        market = resp.get('market', '')

        security_type = resp.get('securityType', '')
        if security_type in ETF_CODE:  # 是否留 ETF, 在這裡控制
            print(f'股票:{name} 型態：{security_type} ETF')
            continue

        '''
        industry = resp.get('industry', '')
        INDUSTRY_CODE = "24"
        # 24 半導體  25 電腦及週邊設備業  26 光電業  27 通信網路業  28 電子零組件業  29 電子通路業  30 資訊服務業  31 其他電子業
        if industry != INDUSTRY_CODE:
            print(f'股票:{name} 產業別：{industry} 產業別不符合{INDUSTRY_CODE}，所以跳過')
            continue
        '''

        # 決定後綴
        if market == "TSE":
            full_code = f"{code}.tw"
        elif market == "OTC":
            full_code = f"{code}.two"
        else:
            full_code = code

        stock_code_list.append(full_code)
        stock_name_list.append(f"{name}({full_code})")       

        print(f"✅ {name} → {full_code} 證券別：{security_type}")

    except Exception as e:
        print(f"❌ 查詢 {code} 發生錯誤: {e}")

# === 輸出 JSON ===

# with open("../papers/stock_code_list.json", "w", encoding="utf-8") as jf:
#    json.dump(stock_code_list, jf, ensure_ascii=False, indent=2)


# === 輸出 TXT ===
with open("../papers/stock_name_list.txt", "w", encoding="utf-8") as tf:
    for line in stock_name_list:
        tf.write(line + "\n")

print("\n✅ 已輸出：")
# print("  - stock_code_list.json")
print("  - stock_name_list.txt")
