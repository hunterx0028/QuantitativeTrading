"""v23：統計至 10:30（含）各棒相對自身 API average 的位置，10:31 open 入場。

資料、cache、tick、費用設定沿用 v18；不使用其階段或斜率訊號。
"""
import math
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from Z_ORB_ONE.analysis_strategy import analysis_strategy_broken_each_stock_special_short_v18 as base

SIGNAL_TIME = (10, 30)  # 統計包含此分鐘；下一分鐘 open 入場
LONG_COUNT_RATIO = 30.0
SHORT_COUNT_RATIO = 20.0
LONG_STOP_LOSS_PERCENT = 2.0
LONG_TAKE_PROFIT_PERCENT = 5.0
SHORT_STOP_LOSS_PERCENT = 2.0
SHORT_TAKE_PROFIT_PERCENT = 5.0
MIN_PRE_ENTRY_RANGE_PERCENT = 5.0  # 0 停用；正數時須嚴格超過昨收的此百分比
INTRADAY_COMPARE_END_LONG = (12, 50)
INTRADAY_COMPARE_END_SHORT = (12, 50)
MIN_MINUTE_BARS_BEFORE_0930 = 20
STOCK_DATA_SOURCE = 'T'
STOCK_LIST = []
HISTORICAL_CACHE_DATE = ''  # 空字串、YYYYMMDD 或 xxxxxxxx；同 v18
BROKERAGE_FEE_RATE = 0.001425
SELL_TRANSACTION_TAX_RATE = 0.003
EACH_STOCK_OUTPUT_FILE = Path(__file__).with_name(
    'analysis_strategy_broken_each_stock_special_short_v23_result.txt')


def count_average_positions(bars):
    counts = dict(above_count=0, below_count=0, neutral_count=0, invalid_count=0)
    for bar in bars:
        if not (9, 0) <= (bar['dt'].hour, bar['dt'].minute) <= SIGNAL_TIME:
            continue
        average = base.parse_average(bar.get('average'))
        if average is None:
            key = 'invalid_count'
        elif bar['low'] > average:
            key = 'above_count'
        elif bar['high'] < average:
            key = 'below_count'
        else:
            key = 'neutral_count'
        counts[key] += 1
    return counts


def select_trade_side(above, below):
    if above > 0 and Decimal(above) >= Decimal(below) * Decimal(str(LONG_COUNT_RATIO)):
        return 'LONG'
    if below > 0 and Decimal(below) >= Decimal(above) * Decimal(str(SHORT_COUNT_RATIO)):
        return 'SHORT'
    return None


def has_enough_pre_entry_range(bars, entry_dt, previous_close):
    if MIN_PRE_ENTRY_RANGE_PERCENT == 0:
        return True
    prior = [b for b in bars if (9, 0) <= (b['dt'].hour, b['dt'].minute) and b['dt'] < entry_dt]
    if not prior:
        return False
    spread = Decimal(str(max(b['high'] for b in prior))) - Decimal(str(min(b['low'] for b in prior)))
    return spread > Decimal(str(previous_close)) * Decimal(str(MIN_PRE_ENTRY_RANGE_PERCENT)) / 100


def calculate_exit_prices(price, side, limit_up, limit_down):
    if not limit_down < price < limit_up:
        return None
    p = Decimal(str(price))
    if side == 'LONG':
        raw_stop = float(p * (1 - Decimal(str(LONG_STOP_LOSS_PERCENT)) / 100))
        raw_target = float(p * (1 + Decimal(str(LONG_TAKE_PROFIT_PERCENT)) / 100))
        stop = base.floor_price_to_tick(raw_stop, base.get_tick_size(raw_stop))
        target = min(base.ceil_price_to_tick(raw_target, base.get_tick_size(raw_target)), limit_up)
        if stop <= limit_down:
            return None
    elif side == 'SHORT':
        raw_stop = float(p * (1 + Decimal(str(SHORT_STOP_LOSS_PERCENT)) / 100))
        raw_target = float(p * (1 - Decimal(str(SHORT_TAKE_PROFIT_PERCENT)) / 100))
        stop = base.ceil_price_to_tick(raw_stop, base.get_tick_size(raw_stop))
        target = max(base.floor_price_to_tick(raw_target, base.get_tick_size(raw_target)), limit_down)
        if stop >= limit_up:
            return None
    else:
        raise ValueError(f'未知交易方向：{side}')
    return stop, target


def collect_trade_candidates(name, bars_by_date, target_date):
    candidates = []
    for day in sorted(bars_by_date):
        current_date = base.date.fromisoformat(day)
        if current_date > target_date:
            continue
        today, yesterday = base.get_target_and_yesterday(bars_by_date, current_date)
        early_count = sum((9, 0) <= (b['dt'].hour, b['dt'].minute) < (9, 30) for b in today)
        if not yesterday or early_count < MIN_MINUTE_BARS_BEFORE_0930:
            continue
        signal_dt = datetime.combine(current_date, datetime.min.time()).replace(
            hour=SIGNAL_TIME[0], minute=SIGNAL_TIME[1])
        entry_dt = signal_dt + timedelta(minutes=1)
        entry = next((b for b in today if b['dt'] == entry_dt), None)
        if entry is None:
            continue  # 缺少指定入場分鐘不往後延
        counts = count_average_positions(today)
        side = select_trade_side(counts['above_count'], counts['below_count'])
        previous_close = base.compute_yesterday_stats(yesterday)['close']
        if side is None or not has_enough_pre_entry_range(today, entry_dt, previous_close):
            continue
        limit_up, limit_down = base.calculate_limit_prices(previous_close)
        prior = [b for b in today if (9, 0) <= (b['dt'].hour, b['dt'].minute) and b['dt'] < entry_dt]
        if any(b['high'] >= limit_up or b['low'] <= limit_down for b in prior):
            continue
        prices = calculate_exit_prices(float(entry['open']), side, limit_up, limit_down)
        if prices is None:
            continue
        candidates.append(dict(name=name, date_str=day, entry_dt=entry_dt,
                               entry_price=float(entry['open']), trade_side=side,
                               stop_loss_price=prices[0], take_profit_price=prices[1],
                               today_bars=today, **counts))
    return candidates


def build_outcome_result(reason, entry, exit_price, side, exit_dt):
    buy, sell = (entry, exit_price) if side == 'LONG' else (exit_price, entry)
    cost = (buy + sell) * BROKERAGE_FEE_RATE + sell * SELL_TRANSACTION_TAX_RATE
    pnl = round(sell - buy - cost, 4)
    return dict(exit_reason=reason, exit_price=exit_price, exit_dt=exit_dt,
                signed_pnl=pnl, total_cost=round(cost, 4), is_success=pnl > 0)


def evaluate_candidates(candidates):
    results = []
    for candidate in candidates:
        side = candidate['trade_side']
        stop, target = candidate['stop_loss_price'], candidate['take_profit_price']
        end = INTRADAY_COMPARE_END_LONG if side == 'LONG' else INTRADAY_COMPARE_END_SHORT
        bars = [b for b in candidate['today_bars'] if b['dt'] >= candidate['entry_dt']]
        result = None
        for bar in bars:  # 包含入場棒；同根同時觸及採停損優先
            if (bar['dt'].hour, bar['dt'].minute) >= end:
                break
            if side == 'LONG':
                open_stop, open_target = bar['open'] <= stop, bar['open'] >= target
                hit_stop, hit_target = bar['low'] <= stop, bar['high'] >= target
            else:
                open_stop, open_target = bar['open'] >= stop, bar['open'] <= target
                hit_stop, hit_target = bar['high'] >= stop, bar['low'] <= target
            if open_stop or open_target:
                reason, price = ('stop' if open_stop else 'target'), bar['open']
            elif hit_stop:
                reason, price = 'stop', stop
            elif hit_target:
                reason, price = 'target', target
            else:
                continue
            result = build_outcome_result(reason, candidate['entry_price'], price, side, bar['dt'])
            break
        if result is None:
            settlement = next((b for b in bars if (b['dt'].hour, b['dt'].minute) >= end), None)
            bar = settlement if settlement is not None else bars[-1]
            # 缺少平倉時間之後資料時，以最後 close 結算，不回到已檢查過的 open。
            price = bar['open'] if settlement is not None else bar['close']
            result = build_outcome_result('close', candidate['entry_price'], price, side, bar['dt'])
        signal = {k: v for k, v in candidate.items() if k != 'today_bars'}
        results.append((signal, result))
    return results


def validate_settings():
    for value in (SIGNAL_TIME, INTRADAY_COMPARE_END_LONG, INTRADAY_COMPARE_END_SHORT):
        if (len(value) != 2 or any(type(x) is not int for x in value)
                or not 0 <= value[0] <= 23 or not 0 <= value[1] <= 59):
            raise ValueError('時間需為合法的 (時, 分)')
    if SIGNAL_TIME < (9, 0):
        raise ValueError('訊號時間不可早於 09:00')
    for end in (INTRADAY_COMPARE_END_LONG, INTRADAY_COMPARE_END_SHORT):
        if SIGNAL_TIME[0] * 60 + SIGNAL_TIME[1] + 1 >= end[0] * 60 + end[1]:
            raise ValueError('入場時間須早於強制平倉時間')
    for ratio in (LONG_COUNT_RATIO, SHORT_COUNT_RATIO):
        if not math.isfinite(ratio) or ratio <= 1:
            raise ValueError('多空倍數須為大於 1 的有限數值')
    for value in (LONG_STOP_LOSS_PERCENT, LONG_TAKE_PROFIT_PERCENT,
                  SHORT_STOP_LOSS_PERCENT, SHORT_TAKE_PROFIT_PERCENT):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('停損停利百分比須介於 0 與 100 之間')
    if not math.isfinite(MIN_PRE_ENTRY_RANGE_PERCENT) or MIN_PRE_ENTRY_RANGE_PERCENT < 0:
        raise ValueError('振幅百分比須為有限非負數')
    if type(MIN_MINUTE_BARS_BEFORE_0930) is not int or MIN_MINUTE_BARS_BEFORE_0930 <= 0:
        raise ValueError('早盤最低根數須為正整數')


def format_result_line(name, day, signal, result):
    return (
        f'{day} {name} {signal["trade_side"]} | '
        f'上方={signal["above_count"]} 下方={signal["below_count"]} '
        f'中性={signal["neutral_count"]} 無效={signal["invalid_count"]} | '
        f'{signal["entry_dt"]:%H:%M} {signal["entry_price"]:.2f} -> '
        f'{result["exit_dt"]:%H:%M} {result["exit_price"]:.2f} {result["exit_reason"]} | '
        f'停損={signal["stop_loss_price"]:.2f} 停利={signal["take_profit_price"]:.2f} '
        f'每股淨損益={result["signed_pnl"]:+.4f}')


def main():
    validate_settings()
    import argparse
    import json
    from datetime import date
    parser = argparse.ArgumentParser(description='v23 average 多空數量比例回測')
    parser.add_argument('--to', default=None, metavar='YYYY-MM-DD')
    parser.add_argument('--config', default=str(base.DEFAULT_CONFIG_PATH), metavar='PATH')
    parse_args = parser.parse_args
    resolve_historical_cache = base.resolve_historical_cache
    get_api_cache_path = base.get_api_cache_path
    parse_bars = base.parse_bars
    init_sdk = base.init_sdk
    fetch_minute_candles = base.fetch_minute_candles
    extract_symbol = base.extract_symbol
    save_shared_api_cache = base.save_shared_api_cache
    args = parse_args()
    if HISTORICAL_CACHE_DATE:
        cache_path, target_date = resolve_historical_cache(HISTORICAL_CACHE_DATE)
    else:
        target_date = date.fromisoformat(args.to) if args.to else date.today()
        cache_path = get_api_cache_path(target_date)
    payload = None
    if cache_path.exists():
        try:
            payload = json.loads(cache_path.read_text(encoding='utf-8'))
        except (ValueError, OSError):
            if HISTORICAL_CACHE_DATE:
                raise
    if HISTORICAL_CACHE_DATE:
        if not isinstance(payload, dict) or not payload.get('stock_names'):
            raise ValueError(f'歷史 cache 缺失或無股票清單：{cache_path}')
        stock_names = list(dict.fromkeys(payload['stock_names']))
        raw = payload.get('minute_raw_by_symbol', {})
        if not isinstance(raw, dict) or any(not isinstance(raw.get(name), list) for name in stock_names):
            raise ValueError('歷史 cache 分K不完整；不回退 API')
        if target_date is None:
            cached_dates = [date.fromisoformat(day)
                            for name in stock_names for day in parse_bars(raw[name])]
            if not cached_dates:
                raise ValueError('xxxxxxxx cache 沒有可用分K，無法判定回測截止日')
            target_date = max(cached_dates)
    else:
        from Z_ORB_ONE.stock_data import selected_stocks, total_stocks
        if STOCK_DATA_SOURCE not in ('S', 'T'):
            raise ValueError('STOCK_DATA_SOURCE 必須為 S 或 T')
        stocks = STOCK_LIST or (selected_stocks if STOCK_DATA_SOURCE == 'S' else total_stocks)
        stock_names = list(dict.fromkeys(item[0] for item in stocks))
        raw = payload.get('minute_raw_by_symbol', {}) if isinstance(payload, dict) else {}
        if not isinstance(raw, dict):
            raw = {}
        missing = [name for name in stock_names if not isinstance(raw.get(name), list)]
        if missing:
            _, rest_stock = init_sdk(args.config)
            fetched = {}
            for i, name in enumerate(missing, 1):
                print(f'API抓取進度：{i}/{len(missing)} {name}')
                fetched[name] = fetch_minute_candles(rest_stock, extract_symbol(name), target_date)
            raw = {**raw, **fetched}
            save_shared_api_cache(cache_path, payload, raw)
    minute = SIGNAL_TIME[0] * 60 + SIGNAL_TIME[1] + 1
    lines = [f'v23 average 多空數量比例回測，目標日期={target_date}',
             f'統計=09:00～{SIGNAL_TIME[0]:02d}:{SIGNAL_TIME[1]:02d}（含） '
             f'入場={minute // 60:02d}:{minute % 60:02d} open '
             f'多方倍數={LONG_COUNT_RATIO:g} 空方倍數={SHORT_COUNT_RATIO:g}',
             f'多方停損={LONG_STOP_LOSS_PERCENT:g}% 停利={LONG_TAKE_PROFIT_PERCENT:g}% '
             f'空方停損={SHORT_STOP_LOSS_PERCENT:g}% 停利={SHORT_TAKE_PROFIT_PERCENT:g}%',
             ('入場前振幅篩選=停用' if MIN_PRE_ENTRY_RANGE_PERCENT == 0 else
              f'入場前高低價差須大於昨收的{MIN_PRE_ENTRY_RANGE_PERCENT:g}%')]
    results = []
    for name in stock_names:
        results.extend(evaluate_candidates(
            collect_trade_candidates(name, parse_bars(raw[name]), target_date)))
    results.sort(key=lambda pair: (pair[0]['entry_dt'], pair[0]['name']))
    for signal, result in results:
        lines.append(format_result_line(signal['name'], signal['date_str'], signal, result))
    groups = [(day, [r for s, r in results if s['date_str'] == day])
              for day in sorted({s['date_str'] for s, _ in results})]
    groups += [(side, [r for s, r in results if s['trade_side'] == side])
               for side in ('LONG', 'SHORT')]
    groups.append(('總計', [r for _, r in results]))
    for label, rows in groups:
        lines.append(f'{label} 交易數={len(rows)} 獲利筆數={sum(r["is_success"] for r in rows)} '
                     f'每筆一股淨損益合計={sum(r["signed_pnl"] for r in rows):+.4f}')
    output = '\n'.join(lines) + '\n'
    EACH_STOCK_OUTPUT_FILE.write_text(output, encoding='utf-8')
    print(output, end='')


if __name__ == '__main__':
    main()
