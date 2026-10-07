"""依 signal_reports 日期名單，以日 K 門檻及分 K 再突破回測。

直接執行：回測全部日期報表。--from / --to：指定日期區間。
--cache-only：只讀快取，不登入行情 API。每股每日每方向最多一筆。
前一根指上一分鐘；缺少上一分鐘不判定該棒進場。
分 K 無法判斷同棒價格先後，同棒停損停利皆觸及時採停損優先。
每股損益另依 TRADE_SHARES 換算收益金額；總報酬率並非資金複利。
"""

import argparse
import configparser
import json
import math
import re
import sys
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = Path(__file__).resolve().parent
SIGNAL_REPORTS_DIR = BASE_DIR.parent / 'stock_model_gpt' / 'signal_reports'
DEFAULT_CONFIG_PATH = BASE_DIR.parent / 'config.ini'
CACHE_DIR = BASE_DIR / 'analysis_json_cache' / 'signal_reports_long_short'
OUTPUT_FILE = BASE_DIR / 'analysis_strategy_signal_reports_long_short_result.txt'

# 百分比常數：3 代表 3%，5 代表 5%。
# 做多設定
LONG_STOP_LOSS_PERCENT = 3.0
LONG_TAKE_PROFIT_PERCENT = 5.0
LONG_ENTRY_START_TIME = (10, 0)
LONG_ENTRY_END_TIME = (10, 30)
LONG_BREAKOUT_POINT = 1  # 0: 昨收，1: 昨高
LONG_EARLY_BREAKOUT_POINT = 1  # 開始進場前曾突破：0 昨收、1 昨高、9 不須突破
LONG_FORCE_EXIT_TIME = (12, 50)

# 做空設定
SHORT_STOP_LOSS_PERCENT = 3.0
SHORT_TAKE_PROFIT_PERCENT = 5.0
SHORT_ENTRY_START_TIME = (10, 0)
SHORT_ENTRY_END_TIME = (10, 30)
SHORT_BREAKOUT_POINT = -1  # 0: 昨收，-1: 昨低
SHORT_EARLY_BREAKOUT_POINT = 9  # 開始進場前曾跌破：0 昨收、-1 昨低、9 不須跌破
SHORT_FORCE_EXIT_TIME = (12, 50)

# 共用設定
TRADE_SHARES = 1000  # 每筆交易股數，預設一張；收益金額 = 每股損益 × 股數
BROKERAGE_FEE_RATE = 0.001425
SELL_TRANSACTION_TAX_RATE = 0.003
API_REQUEST_DELAY_SEC = 1.0


def side_settings(side):
    if side == 'LONG':
        return (LONG_ENTRY_START_TIME, LONG_ENTRY_END_TIME, LONG_FORCE_EXIT_TIME,
                LONG_BREAKOUT_POINT, LONG_STOP_LOSS_PERCENT, LONG_TAKE_PROFIT_PERCENT)
    if side == 'SHORT':
        return (SHORT_ENTRY_START_TIME, SHORT_ENTRY_END_TIME, SHORT_FORCE_EXIT_TIME,
                SHORT_BREAKOUT_POINT, SHORT_STOP_LOSS_PERCENT, SHORT_TAKE_PROFIT_PERCENT)
    raise ValueError(f'未知方向: {side}')


def validate_settings():
    if type(TRADE_SHARES) is not int or TRADE_SHARES <= 0:
        raise ValueError('TRADE_SHARES 必須是正整數股數')
    for side in ('LONG', 'SHORT'):
        early_point = LONG_EARLY_BREAKOUT_POINT if side == 'LONG' else SHORT_EARLY_BREAKOUT_POINT
        if type(early_point) is not int or early_point not in ((0, 1, 9) if side == 'LONG' else (0, -1, 9)):
            raise ValueError(f'{side} 早盤突破點設定不合法: {early_point!r}')
        start, end, force, point, loss, profit = side_settings(side)
        for value in (start, end, force):
            if (not isinstance(value, tuple) or len(value) != 2
                    or any(type(v) is not int for v in value)
                    or not (0 <= value[0] < 24 and 0 <= value[1] < 60)):
                raise ValueError(f'{side} 時間須為有效的 (時, 分): {value}')
        if not start <= end < force:
            raise ValueError(f'{side} 須符合開始時間 <= 停止時間 < 強制離場時間')
        if type(point) is not int or point not in ((0, 1) if side == 'LONG' else (0, -1)):
            raise ValueError(f'{side} 突破點設定不合法: {point}')
        if any(not math.isfinite(v) or not 0 < v < 100 for v in (loss, profit)):
            raise ValueError('停損停利百分比須大於 0 且小於 100')
    if any(not math.isfinite(v) or v < 0 for v in
           (BROKERAGE_FEE_RATE, SELL_TRANSACTION_TAX_RATE, API_REQUEST_DELAY_SEC)):
        raise ValueError('手續費、交易稅、API 延遲須為有限非負數')


def parse_signal_report(path):
    """只讀 high/low 區段的股票行，不將 SKIP、條件及機率類別當股票。"""
    result = {'LONG': [], 'SHORT': []}
    section = None
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        line = line.strip()
        if line.startswith('prediction_date='):
            if line.split('=', 1)[1].strip() != path.stem:
                raise ValueError(f'{path.name}: prediction_date 與檔名不一致')
        if line.startswith('['):
            section = {'[high 符合清單]': 'LONG', '[low 符合清單]': 'SHORT'}.get(line)
            continue
        match = re.match(r'^(\d{4,6})\s+[-+\d.]+%', line)
        if section and match and match[1] not in result[section]:
            result[section].append(match[1])
    return result


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


def previous_day(raw, target):
    candidates = [item for item in raw if item['date'][:10] < target.isoformat()]
    if not candidates:
        raise ValueError('缺少前一交易日日 K')
    item = max(candidates, key=lambda b: b['date'][:10])
    result = {'date': item['date'][:10], **{k: float(item[k]) for k in ('close', 'high', 'low')}}
    if any(not math.isfinite(result[k]) or result[k] <= 0 for k in ('close', 'high', 'low')):
        raise ValueError('前一交易日日 K 價格無效')
    return result


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


def backtest(side, bars, yesterday):
    start, end, force, point, loss, profit = side_settings(side)
    field = 'close' if point == 0 else ('high' if side == 'LONG' else 'low')
    threshold = yesterday[field]
    early_point = LONG_EARLY_BREAKOUT_POINT if side == 'LONG' else SHORT_EARLY_BREAKOUT_POINT
    hm = lambda b: (b['dt'].hour, b['dt'].minute)
    early = [b for b in bars if hm(b) < start]
    early_threshold = (None if early_point == 9 else yesterday[
        'close' if early_point == 0 else ('high' if side == 'LONG' else 'low')])
    qualified = early_point == 9 or any(
        b['high'] > early_threshold if side == 'LONG' else b['low'] < early_threshold for b in early)
    if not qualified:
        return {'status': 'SKIP', 'reason': f'開始進場時間前未曾{"突破" if side == "LONG" else "跌破"}早盤門檻 {early_threshold:g}（選項={early_point}）'}
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


def load_data(symbol, target, rest_getter, cache_only=False):
    path = CACHE_DIR / target.isoformat() / f'{symbol}.json'
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
            if payload.get('version') in (1, 2) and payload.get('symbol') == symbol and payload.get('date') == target.isoformat():
                yesterday = previous_day(payload['day_raw'], target)
                bars = parse_minutes(payload['minute_raw'], target)
                if bars:
                    compact = compact_cache(symbol, target, payload['day_raw'], payload['minute_raw'])
                    if payload != compact:
                        save_cache(path, compact)
                    return bars, yesterday, 'cache'
        except (ValueError, KeyError, TypeError):
            pass
    if cache_only:
        raise ValueError('缺少有效 JSON 快取（cache-only）')
    rest = rest_getter()
    # 逐日往前找有日 K 的日期；週末不查，連假回傳空資料則繼續。
    # 一年仍無資料時停止，避免新上市或錯誤代碼造成無限查詢。
    day_raw = []
    for offset in range(1, 367):
        previous = target - timedelta(days=offset)
        if previous.weekday() >= 5:
            continue
        time.sleep(API_REQUEST_DELAY_SEC)
        raw = rest.historical.candles(
            **{'symbol': symbol, 'from': previous.isoformat(), 'to': previous.isoformat()}
        ).get('data', [])
        day_raw = [item for item in raw if item['date'][:10] == previous.isoformat()]
        if day_raw:
            break
    yesterday = previous_day(day_raw, target)
    time.sleep(API_REQUEST_DELAY_SEC)
    minute_raw = rest.historical.candles(
        **{'symbol': symbol, 'from': target.isoformat(), 'to': target.isoformat(), 'timeframe': '1'}
    ).get('data', [])
    bars = parse_minutes(minute_raw, target)
    if not bars:
        raise ValueError('缺少回測當日分 K')
    save_cache(path, compact_cache(symbol, target, day_raw, minute_raw))
    return bars, yesterday, 'api'


def compact_cache(symbol, target, day_raw, minute_raw):
    """僅保留前一交易日日 K 及目標日分 K，兼容縮減舊快取。"""
    yesterday = previous_day(day_raw, target)
    latest = next(item for item in day_raw if item['date'][:10] == yesterday['date'])
    return {'version': 2, 'date': target.isoformat(), 'symbol': symbol,
            'day_raw': [latest],
            'minute_raw': [item for item in minute_raw if item['date'][:10] == target.isoformat()]}


def print_summary(emit, label, results):
    trades = [r for r in results if r['status'] == 'TRADE']
    capital = sum(r['entry_price'] for r in trades)
    gross = sum(r['gross_pnl'] for r in trades)
    costs = sum(r['cost'] for r in trades)
    net = sum(r['net_pnl'] for r in trades)
    wins = sum(r['net_pnl'] > 0 for r in trades)
    emit(f'{label}: 筆數={len(trades)} 勝率={wins / len(trades) * 100 if trades else 0:.2f}% '
         f'每股毛收益合計={gross:.4f} 每股成本合計={costs:.4f} 每股淨收益合計={net:.4f} '
         f'總入場金額={sum(r["entry_amount"] for r in trades):,.2f}元 '
         f'毛收益金額={sum(r["gross_amount"] for r in trades):+,.2f}元 '
         f'交易成本金額={sum(r["cost_amount"] for r in trades):,.2f}元 '
         f'淨收益金額={sum(r["net_amount"] for r in trades):+,.2f}元 '
         f'總報酬率={net / capital * 100 if capital else 0:.4f}% '
         f'（每筆{TRADE_SHARES:,}股；SKIP={sum(r["status"] == "SKIP" for r in results)} '
         f'資料不足={sum(r["status"] == "INCOMPLETE" for r in results)}）')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--from', dest='from_date', type=date.fromisoformat)
    parser.add_argument('--to', dest='to_date', type=date.fromisoformat)
    parser.add_argument('--config', default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument('--reports-dir', type=Path, default=SIGNAL_REPORTS_DIR)
    parser.add_argument('--output', type=Path, default=OUTPUT_FILE)
    parser.add_argument('--cache-only', action='store_true')
    args = parser.parse_args(argv)
    validate_settings()
    if args.from_date and args.to_date and args.from_date > args.to_date:
        parser.error('--from 不可晚於 --to')
    if not args.reports_dir.is_dir():
        parser.error(f'找不到報表資料夾: {args.reports_dir}')
    reports = []
    for path in sorted(args.reports_dir.glob('*.txt')):
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', path.stem):
            continue
        target = date.fromisoformat(path.stem)
        if (not args.from_date or target >= args.from_date) and (not args.to_date or target <= args.to_date):
            reports.append((target, path))
    lines, results, connection = [], [], []

    def emit(line):
        print(line)
        lines.append(line)

    def rest_getter():
        if not connection:
            connection.extend(init_sdk(args.config))
        return connection[1]

    try:
        emit(f'開始時間: {datetime.now():%Y-%m-%d %H:%M:%S}；日期報表數={len(reports)}')
        emit(f'手續費率={BROKERAGE_FEE_RATE} 賣出交易稅率={SELL_TRANSACTION_TAX_RATE}')
        emit(f'每筆股數={TRADE_SHARES:,}股；金額按股數換算，成本沿用費率模型。總入場金額為各筆加總。')
        for side in ('LONG', 'SHORT'):
            emit(f'{side}: 開始/停止/強制離場/突破點/停損%/停利%={side_settings(side)}')
            early_point = LONG_EARLY_BREAKOUT_POINT if side == 'LONG' else SHORT_EARLY_BREAKOUT_POINT
            emit(f'{side}: 早盤突破點={early_point!r}（9 代表免除早盤突破資格）')
        emit('前一根須為上一分鐘；本根 high/low 突破，以門檻上/下方 1 tick 固定入場（含跳空）。')
        emit('入場棒以 high/low 檢查風控；後續棒跳空停損停利以 open 成交；同棒雙觸及採停損優先。')
        if not reports:
            emit('指定範圍內沒有日期報表。')
        for target, path in reports:
            picks = parse_signal_report(path)
            emit(f'\n日期={target} high={picks["LONG"]} low={picks["SHORT"]}')
            day_results = []
            for symbol in dict.fromkeys(picks['LONG'] + picks['SHORT']):
                try:
                    bars, yesterday, source = load_data(symbol, target, rest_getter, args.cache_only)
                    data_error = None
                except Exception as exc:
                    data_error = str(exc)
                for side in ('LONG', 'SHORT'):
                    if symbol not in picks[side]:
                        continue
                    result = ({'status': 'INCOMPLETE', 'reason': data_error} if data_error
                              else backtest(side, bars, yesterday))
                    result.update(symbol=symbol, date=target.isoformat(), side=side)
                    day_results.append(result)
                    if result['status'] == 'TRADE':
                        emit(f'{symbol} {side} 昨日={result["previous_date"]} 門檻={result["threshold"]:g} '
                             f'入場={result["entry_dt"]:%H:%M} @{result["entry_price"]:g} '
                             f'離場={result["exit_dt"]:%H:%M} @{result["exit_price"]:g} '
                             f'原因={result["exit_reason"]} 每股成本={result["cost"]:.4f} '
                             f'每股淨收益={result["net_pnl"]:+.4f} 股數={result["shares"]:,} '
                             f'入場金額={result["entry_amount"]:,.2f}元 '
                             f'毛收益金額={result["gross_amount"]:+,.2f}元 '
                             f'交易成本金額={result["cost_amount"]:,.2f}元 '
                             f'淨收益金額={result["net_amount"]:+,.2f}元 '
                             f'報酬={result["return_percent"]:.4f}% 資料={source}')
                    else:
                        emit(f'{symbol} {side} [{result["status"]}] {result["reason"]}')
            results.extend(day_results)
            print_summary(emit, f'{target} 合計', day_results)
        emit('')
        for side in ('LONG', 'SHORT'):
            print_summary(emit, side, [r for r in results if r['side'] == side])
        print_summary(emit, '全部合計', results)
        return 1 if any(r['status'] == 'INCOMPLETE' for r in results) else 0
    finally:
        emit(f'結束時間: {datetime.now():%Y-%m-%d %H:%M:%S}')
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text('\n'.join(lines) + '\n', encoding='utf-8')
        print(f'TXT 報表: {args.output}')


if __name__ == '__main__':
    sys.exit(main())
