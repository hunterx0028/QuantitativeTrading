# -*- coding: utf-8 -*-
"""
fetchStockDataYf_Parquet.py
用 yfinance 抓取台股歷史資料，並直接保存為 parquet 檔案
"""

import yfinance as yf
import time
import os
import pandas as pd
from globals import during_start_data, during_end_data
from GetStockData.stockOtherInfo import get_stock_codes


def fetch_stock_data(stock_code, start_date, end_date):
    """
    下載股票歷史資料並保存為 parquet 檔案。
    """
    # 抓資料
    stock_data = yf.download(stock_code, auto_adjust=False, start=start_date, end=end_date, progress=False)

    if stock_data.empty:
        print(f"❌ 無法取得股票代碼 {stock_code} 在 {start_date} 到 {end_date} 之間的資料。")
        return

    # 選擇必要欄位並統一命名
    stock_data = stock_data[['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']]
    stock_data.columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']

    # 日期欄位化
    stock_data.reset_index(inplace=True)
    stock_data['Date'] = pd.to_datetime(stock_data['Date'])
    stock_data.rename(columns={'Date': 'Date'}, inplace=True)

    # 確保資料夾存在
    os.makedirs("../stocks", exist_ok=True)

    # 儲存為 parquet
    output_filename = f"../stocks/{stock_code}.parquet"
    try:
        stock_data.to_parquet(output_filename, index=False)
        print(f"✅ 股票資料已保存為 {output_filename}")
    except Exception as e:
        print(f"⚠️ 無法保存 {stock_code} 為 parquet 檔案：{e}")


if __name__ == '__main__':

    #stock_data_list = ['5460.TWO', '2634.TW']
    stock_data_list = get_stock_codes()
    total = len(stock_data_list)

    for idx, stock_code in enumerate(stock_data_list, start=1):
        progress = idx / total * 100
        print(f"[{idx}/{total}] ({progress:.2f}%) Processing {stock_code}")

        fetch_stock_data(stock_code, during_start_data, during_end_data)
        time.sleep(5)
