import argparse
import configparser
import json
import sys
import time
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from esun_marketdata import EsunMarketdata
from Z_ORB_ONE.stock_data import selected_stocks


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
API_REQUEST_DELAY_SEC = 1
FETCH_BUFFER_DAYS = 14
DEFAULT_SCAN_DAYS = 40
SIGNAL_BAR_TIME = datetime_time(9, 9)
ENTRY_BAR_TIME = datetime_time(9, 10)
EXIT_BAR_TIME = datetime_time(13, 25)
BROKERAGE_FEE_RATE = 0.001425
DAY_TRADE_TAX_RATE = 0.0015
STOP_LOSS_PERCENT = 8.0
CACHE_VERSION = 1


def get_api_cache_path(scan_from: date, scan_to: date) -> Path:
    cache_dir = Path(__file__).resolve().parent / 'analysis_json_cache'
    return cache_dir / (
        f'analysis_two_day_price_limits_api_cache_'
        f'{scan_from:%Y%m%d}_{scan_to:%Y%m%d}.json'
    )


def load_api_cache(
    cache_path: Path,
    stock_list: list[tuple],
    fetch_from: date,
    fetch_to: date,
) -> tuple[dict[str, list], dict[str, list]] | None:
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding='utf-8'))
        current_names = [item[0] for item in stock_list]
        if payload.get('cache_version') != CACHE_VERSION:
            return None
        if payload.get('stock_names') != current_names:
            return None
        if payload.get('fetch_from') != fetch_from.isoformat():
            return None
        if payload.get('fetch_to') != fetch_to.isoformat():
            return None
        daily_raw_by_symbol = payload.get('daily_raw_by_symbol')
        minute_raw_by_symbol = payload.get('minute_raw_by_symbol')
        if not isinstance(daily_raw_by_symbol, dict) or not isinstance(minute_raw_by_symbol, dict):
            return None
        if any(
            stock_name not in daily_raw_by_symbol or stock_name not in minute_raw_by_symbol
            for stock_name in current_names
        ):
            return None
        return daily_raw_by_symbol, minute_raw_by_symbol
    except Exception:
        return None


def save_api_cache(
    cache_path: Path,
    stock_list: list[tuple],
    fetch_from: date,
    fetch_to: date,
    daily_raw_by_symbol: dict[str, list],
    minute_raw_by_symbol: dict[str, list],
) -> None:
    payload = {
        'cache_version': CACHE_VERSION,
        'stock_names': [item[0] for item in stock_list],
        'fetch_from': fetch_from.isoformat(),
        'fetch_to': fetch_to.isoformat(),
        'daily_raw_by_symbol': daily_raw_by_symbol,
        'minute_raw_by_symbol': minute_raw_by_symbol,
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding='utf-8',
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='找出連續兩個交易日收漲停或跌停，並列出次一交易日收盤價。'
    )
    parser.add_argument(
        '--from',
        dest='from_date',
        default=(date.today() - timedelta(days=DEFAULT_SCAN_DAYS)).isoformat(),
        metavar='YYYY-MM-DD',
        help=f'掃描起始日（預設今日往前 {DEFAULT_SCAN_DAYS} 個日曆日）',
    )
    parser.add_argument(
        '--to',
        dest='to_date',
        default=date.today().isoformat(),
        metavar='YYYY-MM-DD',
        help='掃描結束日（預設今日）',
    )
    parser.add_argument(
        '--config',
        default=str(DEFAULT_CONFIG_PATH),
        metavar='PATH',
        help=f'config.ini 路徑（預設 {DEFAULT_CONFIG_PATH}）',
    )
    return parser.parse_args()


def parse_date(value: str, option_name: str) -> date:
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError as exc:
        raise ValueError(f'{option_name} 日期格式錯誤，請使用 YYYY-MM-DD: {value}') from exc


def normalize_config_paths(config: configparser.ConfigParser, config_file: Path) -> None:
    config_dir = config_file.parent
    if config.has_section('Cert'):
        cert_path = config.get('Cert', 'Path', fallback='').strip()
        if cert_path and not Path(cert_path).is_absolute():
            config.set('Cert', 'Path', str((config_dir / cert_path).resolve()))


def init_rest_stock(config_path: str):
    config_file = Path(config_path).resolve()
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    normalize_config_paths(config, config_file)
    sdk = EsunMarketdata(config)
    sdk.login()
    return sdk.rest_client.stock


def extract_symbol(stock_name: str) -> str:
    symbol_text = stock_name.split(':', 1)[-1]
    return symbol_text.split('.', 1)[0]


def get_industry_code(stock_item: tuple) -> str:
    if len(stock_item) <= 6 or stock_item[6] is None:
        return ''
    return str(stock_item[6]).zfill(2)


def get_tick_size(price: float) -> float:
    if price < 10:
        return 0.01
    if price < 50:
        return 0.05
    if price < 100:
        return 0.1
    if price < 500:
        return 0.5
    if price < 1000:
        return 1.0
    return 5.0


def round_price_to_tick(price: float, tick: float, rounding: str) -> float:
    price_decimal = Decimal(str(price))
    tick_decimal = Decimal(str(tick))
    units = (price_decimal / tick_decimal).quantize(Decimal('1'), rounding=rounding)
    return float(units * tick_decimal)


def calculate_limit_prices(previous_close: float) -> tuple[float, float]:
    up_raw = previous_close * 1.10
    down_raw = previous_close * 0.90
    limit_up = round_price_to_tick(up_raw, get_tick_size(up_raw), ROUND_FLOOR)
    limit_down = round_price_to_tick(down_raw, get_tick_size(down_raw), ROUND_CEILING)
    return limit_up, limit_down


def fetch_daily_candles(
    rest_stock: Any,
    symbol: str,
    from_date: date,
    to_date: date,
) -> list[dict]:
    time.sleep(API_REQUEST_DELAY_SEC)
    response = rest_stock.historical.candles(
        **{
            'symbol': symbol,
            'from': from_date.isoformat(),
            'to': to_date.isoformat(),
            'timeframe': 'D',
        }
    )
    return response.get('data', [])


def fetch_minute_candles(
    rest_stock: Any,
    symbol: str,
    from_date: date,
    to_date: date,
) -> list[dict]:
    time.sleep(API_REQUEST_DELAY_SEC)
    response = rest_stock.historical.candles(
        **{
            'symbol': symbol,
            'from': from_date.isoformat(),
            'to': to_date.isoformat(),
            'timeframe': '1',
        }
    )
    return response.get('data', [])


def normalize_daily_candles(raw_candles: list[dict]) -> list[dict]:
    candles_by_date: dict[date, dict] = {}
    for item in raw_candles:
        try:
            trading_date = datetime.strptime(str(item['date'])[:10], '%Y-%m-%d').date()
            candles_by_date[trading_date] = {
                'date': trading_date,
                'close': float(item['close']),
            }
        except (KeyError, TypeError, ValueError):
            continue
    return [candles_by_date[key] for key in sorted(candles_by_date)]


def is_same_price(left: float, right: float) -> bool:
    return abs(left - right) < 1e-8


def find_two_day_limit_events(
    candles: list[dict],
    scan_from: date,
    scan_to: date,
) -> list[tuple[dict, dict, dict, str]]:
    events: list[tuple[dict, dict, dict, str]] = []
    for index in range(1, len(candles) - 2):
        first = candles[index]
        second = candles[index + 1]
        next_day = candles[index + 2]

        if not (scan_from <= first['date'] <= scan_to):
            continue
        if not (scan_from <= second['date'] <= scan_to):
            continue

        first_up, first_down = calculate_limit_prices(candles[index - 1]['close'])
        second_up, second_down = calculate_limit_prices(first['close'])
        if is_same_price(first['close'], first_up) and is_same_price(second['close'], second_up):
            events.append((first, second, next_day, '漲停'))
        elif is_same_price(first['close'], first_down) and is_same_price(second['close'], second_down):
            events.append((first, second, next_day, '跌停'))
    return events


def format_event(stock_item: tuple, event: tuple[dict, dict, dict, str]) -> str:
    first, second, next_day, _direction = event
    stock_name = stock_item[0]
    industry_code = get_industry_code(stock_item)
    dates = ' '.join(
        candle['date'].strftime('%m%d') for candle in (first, second, next_day)
    )
    closes = '|'.join(
        f"{candle['close']:.2f}" for candle in (first, second, next_day)
    )
    return f'{stock_name} {industry_code} {dates} [{closes}]'


def is_event_successful(event: tuple[dict, dict, dict, str]) -> bool:
    _first, second, next_day, direction = event
    if direction == '漲停':
        return next_day['close'] > second['close']
    return next_day['close'] < second['close']


def normalize_minute_candles(raw_candles: list[dict]) -> dict[date, dict[datetime_time, dict]]:
    candles_by_date: dict[date, dict[datetime_time, dict]] = {}
    for item in raw_candles:
        try:
            bar_dt = datetime.strptime(str(item['date'])[:19], '%Y-%m-%dT%H:%M:%S')
            bar = {
                'dt': bar_dt,
                'open': float(item['open']),
                'high': float(item['high']),
                'low': float(item['low']),
                'close': float(item['close']),
            }
        except (KeyError, TypeError, ValueError):
            continue
        candles_by_date.setdefault(bar_dt.date(), {})[bar_dt.time()] = bar
    return candles_by_date


def calculate_net_return_percent(direction: str, entry_price: float, exit_price: float) -> float:
    if direction == '漲停':
        net_value = (
            exit_price * (1 - BROKERAGE_FEE_RATE - DAY_TRADE_TAX_RATE)
            - entry_price * (1 + BROKERAGE_FEE_RATE)
        )
    else:
        net_value = (
            entry_price * (1 - BROKERAGE_FEE_RATE - DAY_TRADE_TAX_RATE)
            - exit_price * (1 + BROKERAGE_FEE_RATE)
        )
    return net_value / entry_price * 100


def analyze_intraday_event(
    event: tuple[dict, dict, dict, str],
    minute_candles_by_date: dict[date, dict[datetime_time, dict]],
) -> dict:
    _first, second, next_day, direction = event
    bars_by_time = minute_candles_by_date.get(next_day['date'], {})
    signal_bar = bars_by_time.get(SIGNAL_BAR_TIME)
    entry_bar = bars_by_time.get(ENTRY_BAR_TIME)
    if signal_bar is None or entry_bar is None:
        return {'status': 'data_incomplete', 'direction': direction}

    signal_confirmed = (
        signal_bar['close'] > second['close']
        if direction == '漲停'
        else signal_bar['close'] < second['close']
    )
    if not signal_confirmed:
        return {
            'status': 'rejected',
            'direction': direction,
            'signal_close': signal_bar['close'],
        }

    eligible_times = sorted(
        bar_time
        for bar_time in bars_by_time
        if ENTRY_BAR_TIME <= bar_time <= EXIT_BAR_TIME
    )
    if not eligible_times:
        return {'status': 'data_incomplete', 'direction': direction}

    trade_bars = [bars_by_time[bar_time] for bar_time in eligible_times]
    entry_price = entry_bar['open']
    if entry_price <= 0:
        return {'status': 'data_incomplete', 'direction': direction}

    exit_bar = trade_bars[-1]
    exit_price = exit_bar['close']
    exit_reason = '收盤'
    analyzed_bars = trade_bars
    if direction == '跌停':
        stop_loss_price = entry_price * (1 + STOP_LOSS_PERCENT / 100)
        for bar_index, bar in enumerate(trade_bars):
            if bar['high'] >= stop_loss_price:
                exit_bar = bar
                exit_price = stop_loss_price
                exit_reason = '停損'
                # 分K無法得知同一根內 high/low 的先後；停損棒不計入 MFE，採保守估計。
                analyzed_bars = trade_bars[:bar_index]
                break

    if direction == '漲停':
        gross_return = (exit_price - entry_price) / entry_price * 100
        mfe = (max(bar['high'] for bar in analyzed_bars) - entry_price) / entry_price * 100
        mae = (entry_price - min(bar['low'] for bar in analyzed_bars)) / entry_price * 100
    else:
        gross_return = (entry_price - exit_price) / entry_price * 100
        if analyzed_bars:
            mfe = (entry_price - min(bar['low'] for bar in analyzed_bars)) / entry_price * 100
            mae = (max(bar['high'] for bar in analyzed_bars) - entry_price) / entry_price * 100
        else:
            mfe = 0.0
            mae = 0.0
        if exit_reason == '停損':
            mae = STOP_LOSS_PERCENT

    return {
        'status': 'entered',
        'direction': direction,
        'signal_close': signal_bar['close'],
        'entry_price': entry_price,
        'exit_price': exit_price,
        'exit_time': exit_bar['dt'].time(),
        'exit_reason': exit_reason,
        'gross_return': gross_return,
        'net_return': calculate_net_return_percent(direction, entry_price, exit_price),
        'mfe': mfe,
        'mae': mae,
    }


def format_intraday_result(result: dict) -> str:
    if result['status'] == 'data_incomplete':
        return '  分K資料不足，未納入當沖統計'
    if result['status'] == 'rejected':
        return f"  09:04 close={result['signal_close']:.2f}，方向未延續，不進場"
    side = '做多' if result['direction'] == '漲停' else '放空'
    return (
        f"  {side} 09:04 close={result['signal_close']:.2f} "
        f"09:05進場={result['entry_price']:.2f} "
        f"{result['exit_time'].strftime('%H:%M')}{result['exit_reason']}={result['exit_price']:.2f} "
        f"毛報酬={result['gross_return']:.2f}% 淨報酬={result['net_return']:.2f}% "
        f"MFE={result['mfe']:.2f}% MAE={result['mae']:.2f}%"
    )


def print_intraday_summary(results: list[dict]) -> None:
    print(
        f'當沖作空統計（09:04確認、09:05進場、停損{STOP_LOSS_PERCENT:g}%、最晚13:25出場）'
    )
    entered = [result for result in results if result['status'] == 'entered']
    rejected = sum(result['status'] == 'rejected' for result in results)
    incomplete = sum(result['status'] == 'data_incomplete' for result in results)
    wins = sum(result['net_return'] > 0 for result in entered)
    losses = len(entered) - wins
    stopped = sum(result['exit_reason'] == '停損' for result in entered)
    print(
        f'連跌二天放空: 候選={len(results)} 訊號成立={len(entered)} '
        f'未成立={rejected} 資料不足={incomplete} 停損={stopped} '
        f'淨利成功={wins} 淨利失敗={losses}'
    )
    if entered:
        average_gross = sum(result['gross_return'] for result in entered) / len(entered)
        average_net = sum(result['net_return'] for result in entered) / len(entered)
        average_mfe = sum(result['mfe'] for result in entered) / len(entered)
        average_mae = sum(result['mae'] for result in entered) / len(entered)
        print(
            f'淨勝率={wins / len(entered) * 100:.2f}% '
            f'平均毛報酬={average_gross:.2f}% 平均淨報酬={average_net:.2f}% '
            f'平均MFE={average_mfe:.2f}% 平均MAE={average_mae:.2f}%'
        )


def main() -> None:
    args = parse_args()
    try:
        scan_from = parse_date(args.from_date, '--from')
        scan_to = parse_date(args.to_date, '--to')
        if scan_from > scan_to:
            raise ValueError('--from 不可晚於 --to')
    except ValueError as exc:
        print(f'[ERROR] {exc}', file=sys.stderr)
        sys.exit(2)

    fetch_from = scan_from - timedelta(days=FETCH_BUFFER_DAYS)
    fetch_to = min(scan_to + timedelta(days=FETCH_BUFFER_DAYS), date.today())

    cache_path = get_api_cache_path(scan_from, scan_to)
    cached = load_api_cache(
        cache_path,
        selected_stocks,
        fetch_from,
        fetch_to,
    )
    if cached is not None:
        daily_raw_by_symbol, minute_raw_by_symbol = cached
        rest_stock = None
        print(f'已載入API快取: {cache_path.name}')
    else:
        daily_raw_by_symbol: dict[str, list] = {}
        minute_raw_by_symbol: dict[str, list] = {}
        try:
            rest_stock = init_rest_stock(args.config)
        except Exception as exc:
            print(f'[ERROR] SDK 初始化失敗: {exc}', file=sys.stderr)
            sys.exit(1)

    intraday_results: list[dict] = []
    total = len(selected_stocks)
    for index, stock_item in enumerate(selected_stocks, start=1):
        stock_name = stock_item[0]
        progress_label = '快取分析進度' if cached is not None else 'API抓取進度'
        print(f'\r{progress_label}: {index}/{total} - {stock_name}', end='', flush=True)
        try:
            if cached is not None:
                raw_candles = daily_raw_by_symbol.get(stock_name, [])
            else:
                raw_candles = fetch_daily_candles(
                    rest_stock,
                    extract_symbol(stock_name),
                    fetch_from,
                    fetch_to,
                )
                daily_raw_by_symbol[stock_name] = raw_candles
            candles = normalize_daily_candles(raw_candles)
            events = [
                event
                for event in find_two_day_limit_events(candles, scan_from, scan_to)
                if event[3] == '跌停'
            ]
            minute_candles_by_date: dict[date, dict[datetime_time, dict]] = {}
            if events:
                if cached is not None:
                    raw_minute_candles = minute_raw_by_symbol.get(stock_name, [])
                else:
                    event_dates = [event[2]['date'] for event in events]
                    try:
                        raw_minute_candles = fetch_minute_candles(
                            rest_stock,
                            extract_symbol(stock_name),
                            min(event_dates),
                            max(event_dates),
                        )
                    except Exception as exc:
                        print(f'\r[ERROR] 取得 {stock_name} 分K失敗: {exc}', file=sys.stderr)
                        raw_minute_candles = []
                    minute_raw_by_symbol[stock_name] = raw_minute_candles
                minute_candles_by_date = normalize_minute_candles(raw_minute_candles)
            elif cached is None:
                minute_raw_by_symbol[stock_name] = []

            for event in events:
                intraday_result = analyze_intraday_event(event, minute_candles_by_date)
                intraday_results.append(intraday_result)
                if intraday_result['status'] == 'entered':
                    print(f'\r{format_event(stock_item, event)}')
                    print(format_intraday_result(intraday_result))
        except Exception as exc:
            print(f'\r[ERROR] 取得或分析 {stock_name} 日K失敗: {exc}', file=sys.stderr)
            if cached is None:
                daily_raw_by_symbol.setdefault(stock_name, [])
                minute_raw_by_symbol.setdefault(stock_name, [])

    if total:
        print()
    if cached is None:
        save_api_cache(
            cache_path,
            selected_stocks,
            fetch_from,
            fetch_to,
            daily_raw_by_symbol,
            minute_raw_by_symbol,
        )
        print(f'已儲存API快取: {cache_path.name}')
    print_intraday_summary(intraday_results)


if __name__ == '__main__':
    main()
