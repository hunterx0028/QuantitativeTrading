"""Cloud 多空回測：selected_stocks，結束日往前 40 個日曆天。
早盤只觸發單邊資格才交易，每股每日最多一筆；分 K 同棒停損優先。
--to YYYY-MM-DD 指定結束日；--cache-only 只讀快取。
沿用原程式固定門檻外一 tick 成交模型（含跳空）。
"""

import argparse
import configparser
import json
import math
import sys
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = Path(__file__).resolve().parent

DEFAULT_CONFIG_PATH = BASE_DIR.parent / 'config.ini'
CACHE_DIR = BASE_DIR / 'analysis_json_cache' / 'cloud'
OUTPUT_FILE = BASE_DIR / 'analysis_strategy_signal_cloud_result.txt'

# 百分比常數：3 代表 3%。早盤振幅須嚴格大於所選方向的門檻。
CLOUD_STOP_LOSS_PERCENT = 3.0
CLOUD_TAKE_PROFIT_PERCENT = 5.0
CLOUD_ENTRY_START_TIME = (9, 55)
CLOUD_ENTRY_END_TIME = (10, 30)
CLOUD_LONG_EARLY_BREAKOUT_POINT = -1  # 0 昨收、1 昨高、-1 昨低；向下跌破
CLOUD_LONG_BREAKOUT_POINT = 1  # 0 昨收、1 昨高、-1 昨低；向上突破
CLOUD_SHORT_EARLY_BREAKOUT_POINT = 1  # 0 昨收、1 昨高、-1 昨低；向上突破
CLOUD_SHORT_BREAKOUT_POINT = -1  # 0 昨收、1 昨高、-1 昨低；向下跌破
FORCE_EXIT_TIME = (12, 50)
CLOUD_LONG_BREAKOUT_PERCENT = 2.5
CLOUD_SHORT_BREAKOUT_PERCENT = 2.5
LOOKBACK_DAYS = 40  # 日曆天；預設以今日為結束日

# 共用設定
TRADE_SHARES = 1000  # 每筆交易股數，預設一張；收益金額 = 每股損益 × 股數
BROKERAGE_FEE_RATE = 0.001425
SELL_TRANSACTION_TAX_RATE = 0.003
API_REQUEST_DELAY_SEC = 1.0


def side_settings(side):
    if side not in ('LONG', 'SHORT'):
        raise ValueError(f'未知方向: {side}')
    point = CLOUD_LONG_BREAKOUT_POINT if side == 'LONG' else CLOUD_SHORT_BREAKOUT_POINT
    return (CLOUD_ENTRY_START_TIME, CLOUD_ENTRY_END_TIME, FORCE_EXIT_TIME,
            point, CLOUD_STOP_LOSS_PERCENT, CLOUD_TAKE_PROFIT_PERCENT)


def validate_settings():
    if type(TRADE_SHARES) is not int or TRADE_SHARES <= 0:
        raise ValueError('TRADE_SHARES 必須為正整數')
    if type(LOOKBACK_DAYS) is not int or LOOKBACK_DAYS <= 0:
        raise ValueError('LOOKBACK_DAYS 必須為正整數')
    for point in (CLOUD_LONG_EARLY_BREAKOUT_POINT, CLOUD_SHORT_EARLY_BREAKOUT_POINT,
                  CLOUD_LONG_BREAKOUT_POINT, CLOUD_SHORT_BREAKOUT_POINT):
        if type(point) is not int or point not in (-1, 0, 1):
            raise ValueError('突破點必須為 -1、0 或 1')
    for hm in (CLOUD_ENTRY_START_TIME, CLOUD_ENTRY_END_TIME, FORCE_EXIT_TIME):
        if (not isinstance(hm, tuple) or len(hm) != 2
                or any(type(v) is not int for v in hm)
                or not (0 <= hm[0] < 24 and 0 <= hm[1] < 60)):
            raise ValueError('時間必須為有效的 (時, 分)')
    if not CLOUD_ENTRY_START_TIME <= CLOUD_ENTRY_END_TIME < FORCE_EXIT_TIME:
        raise ValueError('須符合開始時間 <= 停止時間 < 強制離場時間')
    for value in (CLOUD_STOP_LOSS_PERCENT, CLOUD_TAKE_PROFIT_PERCENT):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('停損停利須大於 0 且小於 100')
    for value in (CLOUD_LONG_BREAKOUT_PERCENT, CLOUD_SHORT_BREAKOUT_PERCENT,
                  BROKERAGE_FEE_RATE, SELL_TRANSACTION_TAX_RATE, API_REQUEST_DELAY_SEC):
        if not math.isfinite(value) or value < 0:
            raise ValueError('振幅、費率及延遲須為有限非負數')


def point_price(yesterday, point):
    return yesterday[{-1: 'low', 0: 'close', 1: 'high'}[point]]


def limit_prices(close):
    prices = []
    for factor, rounding in (('1.10', ROUND_FLOOR), ('0.90', ROUND_CEILING)):
        raw = Decimal(str(close)) * Decimal(factor)
        tick = Decimal('5')
        for boundary, unit in [('10', '.01'), ('50', '.05'), ('100', '.1'),
                               ('500', '.5'), ('1000', '1')]:
            if raw < Decimal(boundary):
                tick = Decimal(unit)
                break
        prices.append(float((raw / tick).to_integral_value(rounding=rounding) * tick))
    return tuple(prices)

def parse_minutes(raw, target):
    bars = {}
    for item in raw:
        dt = datetime.fromisoformat(item['date'][:19])
        if dt.date() != target:
            continue
        bar = {'dt': dt, **{k: float(item[k]) for k in ('open', 'high', 'low', 'close')}}
        if any(not math.isfinite(bar[k]) or bar[k] <= 0 for k in ('open', 'high', 'low', 'close')):
            raise ValueError(f'無效分 K 價格: {dt}')
        if bar['low'] > min(bar['open'], bar['close']) or bar['high'] < max(bar['open'], bar['close']):
            raise ValueError(f'無效分 K OHLC: {dt}')
        bars[dt] = bar
    return sorted(bars.values(), key=lambda b: b['dt'])


def breakout_entry_price(threshold, side):
    """普通股門檻上/下方下一個合法價位；跨級距使用對應方向的 tick。

    升降單位依 TWSE 一般股票規則：
    https://www.twse.com.tw/downloads/zh/trading/introduce/introduce004.pdf
    """
    price = Decimal(str(threshold))
    if price <= 0 or not price.is_finite():
        raise ValueError('突破門檻須為有限正數')
    if side not in ('LONG', 'SHORT'):
        raise ValueError(f'未知方向: {side}')
    levels = [('10', '0.01'), ('50', '0.05'), ('100', '0.1'),
              ('500', '0.5'), ('1000', '1')]
    tick = Decimal('5')
    for boundary, unit in levels:
        if price < Decimal(boundary) or (side == 'SHORT' and price == Decimal(boundary)):
            tick = Decimal(unit)
            break
    if side == 'LONG':
        result = ((price / tick).to_integral_value(rounding=ROUND_FLOOR) + 1) * tick
    else:
        result = ((price / tick).to_integral_value(rounding=ROUND_CEILING) - 1) * tick
    if result <= 0:
        raise ValueError('門檻下方沒有有效正價位')
    return float(result)


def trade_result(side, entry, entry_price, exit_bar, exit_price, reason, threshold, yesterday):
    sell_price = exit_price if side == 'LONG' else entry_price
    costs = (entry_price + exit_price) * BROKERAGE_FEE_RATE + sell_price * SELL_TRANSACTION_TAX_RATE
    gross = (exit_price - entry_price) * (1 if side == 'LONG' else -1)
    return {'status': 'TRADE', 'side': side, 'entry_dt': entry['dt'],
            'exit_dt': exit_bar['dt'], 'entry_price': entry_price, 'exit_price': exit_price,
            'exit_reason': reason, 'threshold': threshold, 'previous_date': yesterday['date'],
            'gross_pnl': gross, 'cost': costs, 'net_pnl': gross - costs,
            'shares': TRADE_SHARES, 'entry_amount': entry_price * TRADE_SHARES,
            'gross_amount': gross * TRADE_SHARES, 'cost_amount': costs * TRADE_SHARES,
            'net_amount': (gross - costs) * TRADE_SHARES,
            'return_percent': (gross - costs) / entry_price * 100}


def backtest(bars, yesterday):
    hm = lambda b: (b['dt'].hour, b['dt'].minute)
    early = [b for b in bars if (9, 0) <= hm(b) < CLOUD_ENTRY_START_TIME]
    if not early:
        return {'status': 'SKIP', 'reason': '缺少開始進場時間前的分 K'}
    high = max(b['high'] for b in early)
    low = min(b['low'] for b in early)
    limit_up, limit_down = limit_prices(yesterday['close'])
    if high >= limit_up or low <= limit_down:
        return {'status': 'SKIP', 'reason': '開始進場時間前曾觸及漲停或跌停'}
    long_hit = low < point_price(yesterday, CLOUD_LONG_EARLY_BREAKOUT_POINT)
    short_hit = high > point_price(yesterday, CLOUD_SHORT_EARLY_BREAKOUT_POINT)
    if long_hit and short_hit:
        return {'status': 'SKIP', 'reason': '開始進場時間前多空早盤條件皆觸發'}
    if not long_hit and not short_hit:
        return {'status': 'SKIP', 'reason': '開始進場時間前多空早盤條件皆未觸發'}
    side = 'LONG' if long_hit else 'SHORT'
    amplitude = (high - low) / yesterday['close'] * 100
    required = CLOUD_LONG_BREAKOUT_PERCENT if side == 'LONG' else CLOUD_SHORT_BREAKOUT_PERCENT
    if amplitude <= required:
        return {'status': 'SKIP', 'side': side,
                'reason': f'早盤振幅 {amplitude:g}% 未大於 {required:g}%'}
    result = backtest_direction(side, bars, yesterday)
    result.update(side=side, early_range_percent=amplitude)
    return result


def backtest_direction(side, bars, yesterday):
    start, end, force, point, loss, profit = side_settings(side)
    threshold = point_price(yesterday, point)
    hm = lambda b: (b['dt'].hour, b['dt'].minute)
    by_dt = {b['dt']: b for b in bars}
    entry_idx = None
    for idx, bar in enumerate(bars):
        if not start <= hm(bar) <= end:
            continue
        previous = by_dt.get(bar['dt'] - timedelta(minutes=1))
        if previous is None:
            continue
        crossed = (bar['high'] > threshold and previous['high'] < threshold if side == 'LONG'
                   else bar['low'] < threshold and previous['low'] > threshold)
        if crossed:
            entry_idx = idx
            break
    if entry_idx is None:
        return {'status': 'SKIP', 'reason': '進場區間內未出現再突破入場點'}
    entry = bars[entry_idx]
    entry_price = breakout_entry_price(threshold, side)
    sign = 1 if side == 'LONG' else -1
    stop = entry_price * (1 - sign * loss / 100)
    take = entry_price * (1 + sign * profit / 100)
    for bar in bars[entry_idx:]:
        if hm(bar) >= force:
            return trade_result(side, entry, entry_price, bar, bar['open'], 'force_exit', threshold, yesterday)
        # 入場棒的 open 發生在突破前，不用它判斷出場；後續棒維持跳空出場處理。
        stop_gap = bar['open'] <= stop if side == 'LONG' else bar['open'] >= stop
        take_gap = bar['open'] >= take if side == 'LONG' else bar['open'] <= take
        if bar is not entry and (stop_gap or take_gap):
            return trade_result(side, entry, entry_price, bar, bar['open'],
                                'stop_loss' if stop_gap else 'take_profit', threshold, yesterday)
        hit_stop = bar['low'] <= stop if side == 'LONG' else bar['high'] >= stop
        hit_take = bar['high'] >= take if side == 'LONG' else bar['low'] <= take
        if hit_stop or hit_take:
            return trade_result(side, entry, entry_price, bar, stop if hit_stop else take,
                                'stop_loss' if hit_stop else 'take_profit', threshold, yesterday)
    return {'status': 'INCOMPLETE', 'reason': '已進場，但缺少強制離場及後續分 K，收益不納入統計',
            'entry_dt': entry['dt'], 'entry_price': entry_price}


def init_sdk(config_path):
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from esun_marketdata import EsunMarketdata
    config_file = Path(config_path).resolve()
    config = configparser.ConfigParser()
    if not config.read(config_file, encoding='utf-8'):
        raise FileNotFoundError(f'找不到設定檔: {config_file}')
    if config.has_section('Cert'):
        cert = config.get('Cert', 'Path', fallback='').strip()
        if cert and not Path(cert).is_absolute():
            config.set('Cert', 'Path', str((config_file.parent / cert).resolve()))
    sdk = EsunMarketdata(config)
    sdk.login()
    return sdk, sdk.rest_client.stock


def save_cache(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
    temporary.replace(path)


def print_summary(emit, label, results):
    trades = [r for r in results if r['status'] == 'TRADE']
    capital = sum(r['entry_price'] for r in trades)
    net = sum(r['net_pnl'] for r in trades)
    wins = sum(r['net_pnl'] > 0 for r in trades)
    emit(f'{label}: 筆數={len(trades)} 勝率={wins / len(trades) * 100 if trades else 0:.2f}% '
         f'總入場金額={sum(r["entry_amount"] for r in trades):,.2f}元 '
         f'淨收益金額={sum(r["net_amount"] for r in trades):+,.2f}元 '
         f'總報酬率={net / capital * 100 if capital else 0:.4f}% '
         f'（每筆{TRADE_SHARES:,}股；SKIP={sum(r["status"] == "SKIP" for r in results)} '
         f'資料不足={sum(r["status"] == "INCOMPLETE" for r in results)}）')




def load_range(symbol, target, rest_getter, cache_only=False):
    start = target - timedelta(days=LOOKBACK_DAYS)
    path = CACHE_DIR / f'{start}_{target}' / f'{symbol}.json'
    raw = None
    if path.exists():
        payload = json.loads(path.read_text(encoding='utf-8'))
        if (payload.get('symbol') == symbol and payload.get('from') == start.isoformat()
                and payload.get('to') == target.isoformat()):
            raw = payload['minute_raw']
    source = 'cache'
    if raw is None:
        if cache_only:
            raise ValueError('缺少有效 JSON 快取（cache-only）')
        time.sleep(API_REQUEST_DELAY_SEC)
        response = rest_getter().historical.candles(**{
            'symbol': symbol, 'from': start.isoformat(), 'to': target.isoformat(), 'timeframe': '1'})
        raw = response.get('data', [])
        if not raw:
            raise ValueError('缺少回測區間分 K')
        save_cache(path, {'symbol': symbol, 'from': start.isoformat(),
                          'to': target.isoformat(), 'minute_raw': raw})
        source = 'api'
    dates = sorted({date.fromisoformat(item['date'][:10]) for item in raw
                    if start.isoformat() <= item['date'][:10] <= target.isoformat()})
    grouped = {day: parse_minutes(raw, day) for day in dates}
    # 與 v17 相同：最早資料日供下一交易日計算昨高、昨低、昨收。
    return grouped, source


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--to', dest='to_date', type=date.fromisoformat, default=date.today())
    parser.add_argument('--config', default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument('--output', type=Path, default=OUTPUT_FILE)
    parser.add_argument('--cache-only', action='store_true')
    args = parser.parse_args(argv)
    validate_settings()
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from Z_ORB_ONE.stock_data import selected_stocks
    symbols = list(dict.fromkeys(item[0].split(':')[-1].split('.')[0] for item in selected_stocks))
    lines, results, connection = [], [], []

    def emit(line):
        print(line)
        lines.append(line)

    def rest_getter():
        if not connection:
            connection.extend(init_sdk(args.config))
        return connection[1]

    try:
        emit(f'開始時間: {datetime.now():%Y-%m-%d %H:%M:%S}')
        emit(f'資料區間={args.to_date - timedelta(days=LOOKBACK_DAYS)} ~ {args.to_date}；候選股={len(symbols)}')
        emit('最早資料日僅供前一交易日基準；每股每日最多一筆。候選名單為目前 selected_stocks。')
        emit(f'開始/停止/強制離場={CLOUD_ENTRY_START_TIME}/{CLOUD_ENTRY_END_TIME}/{FORCE_EXIT_TIME}')
        emit(f'停損={CLOUD_STOP_LOSS_PERCENT}% 停利={CLOUD_TAKE_PROFIT_PERCENT}% 每筆={TRADE_SHARES}股')
        emit(f'手續費率={BROKERAGE_FEE_RATE} 賣出交易稅率={SELL_TRANSACTION_TAX_RATE}')
        emit(f'LONG 早盤點={CLOUD_LONG_EARLY_BREAKOUT_POINT} 入場點={CLOUD_LONG_BREAKOUT_POINT} 振幅須大於={CLOUD_LONG_BREAKOUT_PERCENT}%')
        emit(f'SHORT 早盤點={CLOUD_SHORT_EARLY_BREAKOUT_POINT} 入場點={CLOUD_SHORT_BREAKOUT_POINT} 振幅須大於={CLOUD_SHORT_BREAKOUT_PERCENT}%')
        emit('前一根須為上一分鐘；固定門檻外一 tick 成交（含跳空）；同棒停損優先。')
        for symbol in symbols:
            try:
                grouped, source = load_range(symbol, args.to_date, rest_getter, args.cache_only)
                dates = sorted(grouped)
                if len(dates) < 2:
                    raise ValueError('不足兩個交易日資料，無法取得前一交易日基準')
                for previous, current in zip(dates, dates[1:]):
                    previous_bars = grouped[previous]
                    yesterday = {'date': previous.isoformat(), 'close': previous_bars[-1]['close'],
                                 'high': max(b['high'] for b in previous_bars),
                                 'low': min(b['low'] for b in previous_bars)}
                    result = backtest(grouped[current], yesterday)
                    result.update(symbol=symbol, date=current.isoformat())
                    results.append(result)
                    side = result.get('side', 'UNDECIDED')
                    if result['status'] == 'TRADE':
                        emit(f'{current} {symbol} {side} 昨日={previous} 門檻={result["threshold"]:g} '
                             f'入場={result["entry_dt"]:%H:%M} @{result["entry_price"]:g} '
                             f'離場={result["exit_dt"]:%H:%M} @{result["exit_price"]:g} '
                             f'原因={result["exit_reason"]} 淨收益金額={result["net_amount"]:+,.2f}元 '
                             f'報酬={result["return_percent"]:.4f}% 資料={source}')
                    else:
                        emit(f'{current} {symbol} {side} [{result["status"]}] {result["reason"]}')
            except Exception as exc:
                results.append({'status': 'INCOMPLETE', 'symbol': symbol, 'reason': str(exc)})
                emit(f'{symbol} [INCOMPLETE] {exc}')
        for current in sorted({r['date'] for r in results if 'date' in r}):
            print_summary(emit, f'{current} 合計', [r for r in results if r.get('date') == current])
        for side in ('LONG', 'SHORT'):
            print_summary(emit, side, [r for r in results if r.get('side') == side])
        print_summary(emit, '全部合計', results)
        return 1 if any(r['status'] == 'INCOMPLETE' for r in results) else 0
    finally:
        emit(f'結束時間: {datetime.now():%Y-%m-%d %H:%M:%S}')
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        print(f'TXT 報表: {args.output}')


if __name__ == '__main__':
    sys.exit(main())

