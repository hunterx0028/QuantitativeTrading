"""v18、v24、v31 共用的市場及產業指數快取補齊流程。"""

import json
from pathlib import Path


def get_required_index_symbols(stocks):
    from Z_ORB_ONE.stock_data import market_previous_close_indices

    symbols = {'TWSE:MARKET': 'IX0001', 'TPEX:MARKET': 'IX0043'}
    index_map = None
    for item in stocks:
        if len(item) <= 6:
            continue
        suffix = item[0].rsplit('.', 1)[-1].upper()
        exchange = {'TW': 'TWSE', 'TWO': 'TPEX'}.get(suffix)
        if exchange is None:
            continue
        industry_code = str(item[6]).zfill(2)
        key = f'{exchange}:{industry_code}'
        symbol = market_previous_close_indices.get(key, {}).get('symbol')
        if not symbol:
            if index_map is None:
                map_path = Path(__file__).resolve().parents[1] / 'industry_index_map.json'
                index_map = json.loads(map_path.read_text(encoding='utf-8'))
            industry = (index_map or {}).get('exchanges', {}).get(exchange, {}).get('industries', {}).get(industry_code, {})
            symbol = (industry.get('index') or {}).get('symbol')
        if isinstance(symbol, str) and symbol:
            symbols[key] = symbol
        else:
            print(f'[WARN] 缺少產業指數 mapping：{key}，略過此指數')
    return symbols


def complete_index_cache(stocks, index_raw, target_date, fetch_candles, get_rest_stock):
    """保留既有指數；僅抓取缺少或空白的必要指數，回傳是否有補抓。"""
    symbols = get_required_index_symbols(stocks)
    missing = [(key, symbol) for key, symbol in sorted(symbols.items())
               if not isinstance(index_raw.get(key), list) or not index_raw[key]]
    if not missing:
        return False
    rest_stock = get_rest_stock()
    for i, (key, symbol) in enumerate(missing, 1):
        print(f'指數API抓取進度：{i}/{len(missing)} {key} {symbol}')
        index_raw[key] = fetch_candles(rest_stock, symbol, target_date)
    return True
