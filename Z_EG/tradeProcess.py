from configparser import ConfigParser

from esun_trade.sdk import SDK
from esun_trade.order import OrderObject
from esun_trade.constant import (APCode, Trade, PriceFlag, Action)

from GetStockData.stockOtherInfo import has_volume, get_pairs_datas
from toStockZscore import calculate_zscore_quantity  # Route B：支援 alpha 參數
from datetime import datetime

import schedule, time, logging
from threading import Lock

from globals import get_trade_date, RED, GREEN, RESET

from esun_marketdata import EsunMarketdata

# 讀取設定檔
config = ConfigParser()
config.read('config.ini')

realtime_sdk = EsunMarketdata(config)
realtime_sdk.login()

sdk = SDK(config)
sdk.login()

def getInventories(mySDK = sdk):
    inventories = mySDK.get_inventories()
    return inventories

def placeOrder(stock_id, stock_id_rsuffix, trade, buy_sell, quantity, ap_code = APCode.Common, price_flag = PriceFlag.Market, price = None, user_def = "", mySDK = sdk):
    stockRunning = has_volume(stock_id)
    if stockRunning:
        price_flag = PriceFlag.Market
        print(f"市價交易 {price_flag}")
    else:
        price_flag = PriceFlag.Flat
        print(f"平盤交易 {price_flag}")

    # 建立委託物件
    order = OrderObject(
      ap_code = ap_code, # APCode.Common 一般委託 APCode.IntradayOdd 零股委託
      buy_sell = buy_sell, # Action.Buy or Action.Sell
      trade = trade, # Trade.Cash Cash:"0"現股買賣 Trade.Margin:"3"融資 Trade.Short:"4"融券
      price_flag = price_flag, # PriceFlag.LimitDown Limit:"0"限價 Flat:"1"平盤(用於APCode.Flat) LimitDown:"2"跌停 LimitUp:"3"漲停 Market:"4"市價(用於APCode.Common)
      price = price,
      stock_no = stock_id_rsuffix,
      quantity = quantity,
      user_def = user_def
    )
    try:
        mySDK.place_order(order)
    except Exception as e:
        print(f"Failed to place order for stock {stock_id_rsuffix}: {e}")
        return
    print(f"Your order for stock {stock_id_rsuffix} has been placed successfully.")

def find_one_item_in_inventories(stockInventories, target_stk_no, onlyToday = False):
    stk_qty_result = 0
    if onlyToday == True: # 只找今天的庫存
        for item in stockInventories:
            if item.get('stk_no') == target_stk_no:
                for detail in item.get('stk_dats', []):
                    if detail.get('t_date') == get_trade_date().replace("-", ""): # 20250910
                        stk_qty = int(detail.get('qty')) // 1000
                        stk_qty_result = stk_qty_result + stk_qty
        return stk_qty_result
    elif onlyToday == False: # 找不是今天的庫存
        for item in stockInventories:
            if item.get('stk_no') == target_stk_no:
                for detail in item.get('stk_dats', []):
                    if detail.get('t_date') != get_trade_date().replace("-", ""):  # 20250910
                        stk_qty = int(detail.get('qty')) // 1000
                        stk_qty_result = stk_qty_result + stk_qty
        return stk_qty_result

def quantitativeTradeing(mySDK = sdk, rtSDK = realtime_sdk):
    condidate_data = get_pairs_datas()

    # 取得目前庫存
    stockInventories = mySDK.get_inventories()

    for item in condidate_data:
        xStockName = item['nameOne']
        yStockName = item['nameTwo']
        xStockId = item['nameOneCode']
        yStockId = item['nameTwoCode']
        spreadMean = float(item['spreadMean'])
        spreadStd = float(item['spreadStd'])
        alpha = float(item.get('alpha', 0.0))  # ★ Route B：
        beta = float(item.get('beta', 0.0))
        xStockQty = int(item['nameOneQty'])
        yStockQty = int(item['nameTwoQty'])
        zscoretop = float(item['ztop'])
        zscoredown = float(item['zdown'])

        status = item.get('status', 'A')

        xStockIdRSuffix = xStockId.split('.', 1)[0]
        yStockIdRSuffix = yStockId.split('.', 1)[0]

        title = f"{xStockName}:{xStockId}<->{yStockName}:{yStockId}"
        userDef = xStockIdRSuffix+'-'+yStockIdRSuffix

        # 計算 zscore（Route B：傳入 alpha）
        zscoreAndQuantity = calculate_zscore_quantity(xStockId, yStockId, spreadMean, spreadStd, alpha, beta, rtSDK)
        zscore, _, _, _, _, _ = zscoreAndQuantity

        if zscore is None:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {title} - 無法計算 z-score（價格或參數有誤） ( {zscoretop:.2f} - {zscoredown:.2f} ) qty:{xStockQty}/{yStockQty} status:{status}")
            continue

        color = RESET
        if abs(zscore) >= abs(zscoretop):
            color = RED
        elif abs(zscore) <= abs(zscoredown):
            color = GREEN
        else:
            color = RESET

        # 列印及顯示 zscore 結果
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {title} - z-score:{color}{zscore:.4f}{RESET} ( {zscoretop:.2f} - {zscoredown:.2f} ) qty:{xStockQty:,}/{yStockQty:,} status:{status}")

        # 根據 zscore 決定交易行動（與你原本方向一致）
        # 進場訊號 zscore > ztop → x 券賣、y 資買
        if zscore > zscoretop and status in ['A','B']:
            # 做空 xStockId (融券)
            print(f"準備下單 Executed trade for {title} {xStockName}:{xStockId}-券賣:{xStockQty}")
            placeOrder(xStockId, xStockIdRSuffix, Trade.Short, Action.Sell, xStockQty, APCode.Common, PriceFlag.Market,
                       None, userDef, mySDK)
            print(f"準備下單 Executed trade for {title} {yStockName}:{yStockId}-資買:{yStockQty}")
            placeOrder(yStockId, yStockIdRSuffix, Trade.Margin, Action.Buy, yStockQty, APCode.Common, PriceFlag.Market,
                       None, userDef, mySDK)
        # 進場訊號 zscore < -ztop → x 資買、y 券賣
        if zscore < -zscoretop and status in ['A','B']:
            print(f"準備下單 Executed trade for {title} {xStockName}:{xStockId}-資買:{xStockQty}")
            placeOrder(xStockId, xStockIdRSuffix, Trade.Margin, Action.Buy, xStockQty, APCode.Common, PriceFlag.Market,
                       None, userDef, mySDK)
            print(f"準備下單 Executed trade for {title} {yStockName}:{yStockId}-券賣:{yStockQty}")
            placeOrder(yStockId, yStockIdRSuffix, Trade.Short, Action.Sell, yStockQty, APCode.Common, PriceFlag.Market,
                       None, userDef, mySDK)
        # 進行平倉（回到區間內）
        if (-zscoredown) < zscore < zscoredown and status in ['A','S']:
            # 當沖：只平今天持倉（x 先前券賣 → 資買平倉；y 先前資買 → 券賣平倉）
            stockOneQtyToday = find_one_item_in_inventories(stockInventories, xStockIdRSuffix, True)
            if stockOneQtyToday > 0:
                print(f"準備平倉 Executed trade for {title} {xStockName}:{xStockId}-資買:{stockOneQtyToday}")
                placeOrder(xStockId, xStockIdRSuffix, Trade.Margin, Action.Buy, stockOneQtyToday, APCode.Common, PriceFlag.Market, None, userDef, mySDK)
            stockTwoQtyToday = find_one_item_in_inventories(stockInventories, yStockIdRSuffix, True)
            if stockTwoQtyToday > 0:
                print(f"準備平倉 Executed trade for {title} {yStockName}:{yStockId}-券賣:{stockTwoQtyToday}")
                placeOrder(yStockId, yStockIdRSuffix, Trade.Short, Action.Sell, stockTwoQtyToday, APCode.Common, PriceFlag.Market, None, userDef, mySDK)
            # 非當沖：平舊倉（x 先前券賣 → 券買；y 先前資買 → 資賣）
            stockOneQtyNotToday = find_one_item_in_inventories(stockInventories, xStockIdRSuffix, False)
            if stockOneQtyNotToday > 0:
                print(f"準備平倉 Executed trade for {title} {xStockName}:{xStockId}-券買:{stockOneQtyNotToday}")
                placeOrder(xStockId, xStockIdRSuffix, Trade.Short, Action.Buy, stockOneQtyNotToday, APCode.Common, PriceFlag.Market, None, userDef, mySDK)
            stockTwoQtyNotToday = find_one_item_in_inventories(stockInventories, yStockIdRSuffix, False)
            if stockTwoQtyNotToday > 0:
                print(f"準備平倉 Executed trade for {title} {yStockName}:{yStockId}-資賣:{stockTwoQtyNotToday}")
                placeOrder(yStockId, yStockIdRSuffix, Trade.Margin, Action.Sell, stockTwoQtyNotToday, APCode.Common, PriceFlag.Market, None, userDef, mySDK)

# 使用鎖來防止重複執行
lock = Lock()

def job():
    if not lock.acquire(blocking=False):
        print("上一輪尚未結束，跳過這次。")
        return
    try:
        # 你的交易主程序
        quantitativeTradeing(mySDK=sdk)
        print("=======================================================================================================")
    except Exception:
        logging.exception("quantitativeTradeing 執行失敗")
    finally:
        lock.release()

if __name__ == "__main__":
    # 先執行一次
    job()

    # 之後每 N 分鐘執行一次（改 10 分鐘就把 N 改 10）
    schedule.every(3).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
