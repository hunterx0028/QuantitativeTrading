import schedule
import time
from datetime import datetime
from globals import RED, GREEN, BLUE, RESET, YELLOW
from GetStockData.stockOtherInfo import units_from_beta_prices, get_pairs_datas
from esun_marketdata import EsunMarketdata
from configparser import ConfigParser

# ============================
# 即時抓價（TW/TWO）
# ============================
def fetch_realtime_price(stock_id: str, realtime_sdk: EsunMarketdata):
    code = stock_id.split(".")[0]
    stock = realtime_sdk.rest_client.stock  # Stock REST API client

    quote = stock.intraday.quote(symbol=code)
    try:
        # 若 'lastPrice' 不存在，就自動用 'previousClose' 當替代
        lastPrice = quote.get('lastPrice', quote.get('previousClose'))
    except:
        print(stock.intraday.quote(symbol=code))
        return None
    return lastPrice

# ============================
# 本輪快取：同一輪只抓一次價
# ============================

def fetch_price_cached(stock_id: str, cache: dict, realtime_sdk: EsunMarketdata):
    if stock_id in cache:
        return cache[stock_id]

    px = fetch_realtime_price(stock_id, realtime_sdk)
    cache[stock_id] = px  # 成功/失敗都記，避免本輪重複打
    return px


# ============================
# EG Route B：z = ((price1 - α) - β·price2 − mean) / std
# ============================

def calculate_zscore_quantity(xStockId: str, yStockId: str, spreadMean: float, spreadStd: float,
                              alpha: float = 0.0, beta: float = 0.0, realtime_sdk: EsunMarketdata = None,
                              price_cache: dict | None = None):
    # 用快取抓價
    if price_cache is None:
        xPrice = fetch_realtime_price(xStockId, realtime_sdk)
        yPrice = fetch_realtime_price(yStockId, realtime_sdk)
    else:
        xPrice = fetch_price_cached(xStockId, price_cache, realtime_sdk)
        yPrice = fetch_price_cached(yStockId, price_cache, realtime_sdk)

    if xPrice is None or yPrice is None or spreadStd == 0:
        return None

    spread_now = (xPrice - alpha) - beta * yPrice
    zscore = (spread_now - spreadMean) / spreadStd

    rt = abs(beta) * (yPrice/xPrice)

    qtyOne, qtyTwo, ratio_target, ratio_actual = units_from_beta_prices(beta, xPrice, yPrice)
    return (zscore, qtyOne, qtyTwo, xPrice, yPrice, rt)


def print_zscore_result(title: str, xStockId: str, yStockId: str, spreadMean: float, spreadStd: float,
                        addinfo: str, ztop: float = 0.0, zdown: float = 0.0,
                        alpha: float = 0.0, beta: float = 0.0, realtime_sdk: EsunMarketdata = None,
                        price_cache: dict | None = None):
    out = calculate_zscore_quantity(xStockId, yStockId, spreadMean, spreadStd, alpha, beta, realtime_sdk, price_cache=price_cache)
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if out is None:
        print(f"{ts} - {title} - 無法計算 z-score（價格/參數異常） {addinfo}")
        return

    z, qtyOne, qtyTwo, xPrice, yPrice, rt = out

    zcolor = RESET
    if abs(z) >= abs(ztop):
        zcolor = RED
    elif abs(z) <= abs(zdown):
        zcolor = GREEN

    rcolor = RESET
    if rt>0.2 and rt<5:
        rcolor = BLUE

    if z > 0:
        print(f"{ts} - {title} ({xPrice} | {yPrice}) (融券:{qtyOne:,}-融資:{qtyTwo:,}) {rcolor}rt:{rt:.4f}{RESET} {zcolor}z-score:{z:.4f}{RESET}{addinfo}")
    elif z < 0:
        print(f"{ts} - {title} ({xPrice} | {yPrice}) (融資:{qtyOne:,}-融券:{qtyTwo:,}) {rcolor}rt:{rt:.4f}{RESET} {zcolor}z-score:{z:.4f}{RESET}{addinfo}")
    else:
        print(f"{ts} - {title} ({xPrice} | {yPrice}) {rcolor}rt:{rt:.4f}{RESET} {zcolor}z-score:{z:.4f}{RESET} {addinfo}")


def print_line():
    print("=" * 120)


# ============================
# 單一排程工作：每輪建立一次快取，跑完整輪後丟棄
# ============================

def one_cycle_printzscore():
    condidate_data = get_pairs_datas()
    price_cache: dict[str, float | None] = {}  # 本輪快取

    config = ConfigParser()
    config.read('config.ini')
    realtime_sdk = EsunMarketdata(config)
    realtime_sdk.login()

    print_line()
    for item in condidate_data:
        xStockId = item['nameOneCode']  # 與離線一致：spread = (nameOne − α) − β·nameTwo
        yStockId = item['nameTwoCode']

        spreadMean = float(item['spreadMean'])
        spreadStd = float(item['spreadStd'])
        alpha = float(item.get('alpha', 0.0))
        beta = float(item.get('beta', 0.0))

        ztop = item.get('ztop', 0.0)
        zdown = item.get('zdown', 0.0)
        status = item.get('status', '')

        comment = item.get('comment', '')
        c1 = c2 = c3 = ''
        if comment:
            parts = comment.split(',')
            if len(parts) > 1: c1 = parts[1]
            if len(parts) > 2: c2 = parts[2]
            if len(parts) > 3: c3 = parts[3]
        if status != 'C':
            statusColor = f"{YELLOW}status:{status}{RESET}"
        else:
            statusColor = f"status:{status}"

        addinfo = f"( {ztop}<->{zdown} | {zdown}<->{ztop} ) {statusColor} {xStockId}-{yStockId} {c1} {c2} {c3}"
        title = f"{item['nameOne']}:{xStockId} | {item['nameTwo']}:{yStockId}"

        print_zscore_result(
            title, xStockId, yStockId, spreadMean, spreadStd,
            addinfo, ztop, zdown, alpha, beta, realtime_sdk,
            price_cache=price_cache
        )


def every_n_minutes_printzscore(nmin: int = 3):
    # 先跑一次
    one_cycle_printzscore()
    # 之後每 n 分鐘跑一次「整輪」（本輪內共享快取）
    schedule.every(nmin).minutes.do(one_cycle_printzscore)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    every_n_minutes_printzscore(3)
