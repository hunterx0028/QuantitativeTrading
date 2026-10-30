"""v28 broken 開盤破低放空回測。

以分K時間為分鐘起點：訊號K完成後，於下一根可用分K open 放空。
歷史 cache、費用與 tick 規則沿用 v18；不使用 average 訊號。
"""
import argparse
import configparser
import json
import math
import re
import sys
import time
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# IDE 直接執行時在此調整；時間標籤視為該分鐘起點。
MARKET_OPEN_TIME = (9, 0)
SIGNAL_START_TIME = (9, 30)  # 包含此分鐘的訊號K
ENTRY_DEADLINE = (12, 0)  # 實際進場時間，包含此分鐘
OPENING_DROP_PERCENT = 3.0  # 首根 low 嚴格低於昨收此百分比
IX0001_OPENING_DROP_PERCENT = 0.5  # 首根 close 相對 open，嚴格跌逾此值則排除 .TW
IX0043_OPENING_DROP_PERCENT = 0.5  # 同上，對應 .TWO
MARKET_INDEX_RULES = {
    '.TW': ('TWSE:MARKET', 'IX0001'),
    '.TWO': ('TPEX:MARKET', 'IX0043'),
}

OPTIMIZE_LOSS_PER_LOWER = 2.0  # 固定停損百分比，亦用於漲停入場限制
OPTIMIZE_PROFIT_PER_LOWER = 5.0  # %

INTRADAY_COMPARE_END_LOWER = (12, 30)
STOCK_DATA_SOURCE = "T"  # S: selected_stocks；T: total_stocks
STOCK_LIST = []  # 非空時優先
# 空字串：使用 stock_data.py；YYYYMMDD：讀取對應日期 cache，優先於 --to。
# 字面值 "xxxxxxxx"：讀取 analysis_json_cache/analysis_strategy_broken_each_stock_special_short_api_cache_xxxxxxxx.json，
# 以檔內最新有效分K日期為截止日，回測檔內全部可用歷史日期；不連 API、不讀 stock_data.py。
HISTORICAL_CACHE_DATE = "xxxxxxxx"
BROKERAGE_FEE_RATE = 0.001425
SELL_TRANSACTION_TAX_RATE = 0.003
API_REQUEST_DELAY_SEC = 1
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
EACH_STOCK_OUTPUT_FILE = Path(__file__).with_name(
    'analysis_strategy_broken_each_stock_special_short_v28_result.txt'
)
STRATEGY_LOWER = 'broken'
TRADE_SIDE_SHORT = 'SHORT'


def parse_average(value):
    """保留共用資料格式；average 不參與 v28 訊號判斷。"""
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def get_tick_size(price: float) -> float:
    if price < 10:
        return 0.01
    elif price < 50:
        return 0.05
    elif price < 100:
        return 0.1
    elif price < 500:
        return 0.5
    elif price < 1000:
        return 1
    else:
        return 5


def floor_price_to_tick(price: float, tick: float) -> float:
    """將價格無條件捨去到合法 tick。"""
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    floored_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_FLOOR)
    return float(floored_units * tick_dec)


def ceil_price_to_tick(price: float, tick: float) -> float:
    """將價格無條件進位到合法 tick。"""
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    ceiled_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return float(ceiled_units * tick_dec)


def price_below_previous_low(previous_low: float) -> float:
    """昨低以下第一個合法報價；跨價位區間時使用下方區間的 tick。"""
    price = Decimal(str(previous_low))
    tick = Decimal(str(get_tick_size(float(price - Decimal('0.001')))))
    return float(((price / tick).to_integral_value(rounding=ROUND_CEILING) - 1) * tick)


def calculate_limit_prices(prev_close: float):
    # 原始價格（未調整）
    up_raw = prev_close * 1.10
    down_raw = prev_close * 0.90

    # 台股漲跌停價:
    # 漲停價用「無條件捨去」，跌停價用「無條件進位」；
    # tick 依各自價位區間判斷。
    limit_up_tick = get_tick_size(up_raw)
    limit_down_tick = get_tick_size(down_raw)
    limit_up = floor_price_to_tick(up_raw, limit_up_tick)
    limit_down = ceil_price_to_tick(down_raw, limit_down_tick)

    return limit_up, limit_down


def calculate_stop_loss_amount_by_percent(entry_price: float, stop_loss_percent: float) -> float:
    """依入場價與停損百分比計算停損價差。"""
    return entry_price * (stop_loss_percent / 100.0)


def calculate_take_profit_amount_by_percent(entry_price: float, take_profit_percent: float) -> float:
    """依入場價與停利百分比計算停利價差。"""
    return entry_price * (take_profit_percent / 100.0)


def should_skip_entry_by_limit_up(entry_price: float, stop_loss: float, limit_up_price: float) -> bool:
    """若作空進場價加停損差價已達漲停，略過本次交易。"""
    return (entry_price + stop_loss) >= limit_up_price


def has_limit_price_touched_before_entry(
    high_values: np.ndarray,
    low_values: np.ndarray,
    entry_idx: int,
    limit_up_price: float,
    limit_down_price: float,
) -> bool:
    """檢查入場分 K 之前是否曾觸及漲停或跌停。"""
    if entry_idx <= 0:
        return False
    prior_high_values = high_values[:entry_idx]
    prior_low_values = low_values[:entry_idx]
    return (
        np.max(prior_high_values) >= limit_up_price
        or np.min(prior_low_values) <= limit_down_price
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='v28 broken 開盤破低放空回測')
    parser.add_argument(
        '--to',
        metavar='YYYY-MM-DD',
        default=None,
        help='分析目標日期（預設今日）',
    )
    parser.add_argument(
        '--config',
        metavar='PATH',
        default=str(DEFAULT_CONFIG_PATH),
        help=f'config.ini 路徑（預設 {DEFAULT_CONFIG_PATH}）',
    )
    return parser.parse_args()


def normalize_config_paths(config: configparser.ConfigParser, config_file: Path) -> None:
    """將 config.ini 內的相對路徑轉成以設定檔所在目錄為基準的絕對路徑。"""
    config_dir = config_file.parent

    if config.has_section('Cert'):
        cert_path = config.get('Cert', 'Path', fallback='').strip()
        if cert_path and not Path(cert_path).is_absolute():
            config.set('Cert', 'Path', str((config_dir / cert_path).resolve()))


def init_sdk(config_path: str):
    """初始化 SDK，回傳 (sdk, rest_stock)；失敗時印出錯誤並結束。"""
    from esun_marketdata import EsunMarketdata

    config_file = Path(config_path).resolve()
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    normalize_config_paths(config, config_file)
    try:
        sdk = EsunMarketdata(config)
        sdk.login()
        rest_stock = sdk.rest_client.stock
        return sdk, rest_stock
    except Exception as exc:
        print(f'[ERROR] SDK 初始化失敗: {exc}', file=sys.stderr)
        sys.exit(1)


def extract_symbol(stock_name: str) -> str:
    """從「名稱:代碼.TW」格式中擷取股票代碼。"""
    m = re.search(r':(\d+)\.', stock_name)
    if not m:
        raise ValueError(f'無法從股票名稱擷取代碼: {stock_name!r}')
    return m.group(1)


def fetch_minute_candles(rest_stock, symbol: str, target_date: date) -> list:
    """呼叫 SDK 取得 1-minute K棒，回傳 data 陣列（最新在前）。"""
    from_date = target_date - timedelta(days=40)
    from_str = from_date.strftime('%Y-%m-%d')
    to_str = target_date.strftime('%Y-%m-%d')
    try:
        time.sleep(API_REQUEST_DELAY_SEC)
        response = rest_stock.historical.candles(
            **{'symbol': symbol, 'from': from_str, 'to': to_str, 'timeframe': '1'}
        )
        data = response.get('data', [])
        if data:
            return data
        print(f'[ERROR] 取得 {symbol} K棒失敗: 回傳資料為空', file=sys.stderr)
    except Exception as exc:
        print(f'[ERROR] 取得 {symbol} K棒失敗: {exc}', file=sys.stderr)
    return []


def parse_bars(raw_data: list) -> dict:
    """
    將 SDK 回傳的 data 陣列解析並依日期分組。
    回傳 dict[YYYY-MM-DD -> list[bar_dict]]，每個日期內按時間升序排列。
    bar_dict = {dt, open, high, low, close, volume, average}
    """
    bars_by_date: dict[str, list] = {}
    invalid_count = 0
    invalid_example = None
    for item in raw_data:
        # API 可能回傳缺少 OHLC 的資料列；不補造價格，也不讓單筆異常中斷整批。
        try:
            date_raw = item['date'][:19]  # '2026-04-02T09:00:00'
            dt = datetime.strptime(date_raw, '%Y-%m-%dT%H:%M:%S')
            prices = {key: float(item[key]) for key in ('open', 'high', 'low', 'close')}
            if any(not math.isfinite(value) or value <= 0 for value in prices.values()):
                raise ValueError('OHLC 必須為有限正數')
            if prices['low'] > min(prices['open'], prices['close']) or prices['high'] < max(prices['open'], prices['close']):
                raise ValueError('OHLC 高低價範圍不一致')
        except (KeyError, TypeError, ValueError) as exc:
            invalid_count += 1
            if invalid_example is None:
                invalid_example = f'{item!r}；原因={exc}'
            continue
        if not MARKET_OPEN_TIME <= (dt.hour, dt.minute) <= (13, 30):
            continue
        date_key = dt.strftime('%Y-%m-%d')
        bar = {
            'dt': dt,
            **prices,
            'volume': float(item.get('volume', 0) or 0),
            'average': parse_average(item.get('average')),
        }
        bars_by_date.setdefault(date_key, []).append(bar)

    if invalid_count:
        print(f'[WARN] 分 K 解析略過 {invalid_count}/{len(raw_data)} 筆無效資料（未補價）；首筆={invalid_example}', file=sys.stderr)

    # 每個日期內按時間升序
    for key in bars_by_date:
        bars_by_date[key].sort(key=lambda b: b['dt'])

    return bars_by_date


def get_target_and_yesterday(bars_by_date: dict, target_date: date):
    """
    找出目標日期的 K棒列表，以及目標日期前最近一個有 K棒的日期（前一交易日）。
    回傳 (today_bars, yesterday_bars)；任一不存在則回傳 ([], [])。
    """
    target_key = target_date.strftime('%Y-%m-%d')
    today_bars = bars_by_date.get(target_key, [])
    if not today_bars:
        return [], []

    # 找前一交易日：所有日期中小於 target_key 的最大者
    previous_dates = sorted(k for k in bars_by_date if k < target_key)
    if not previous_dates:
        return today_bars, []

    yesterday_key = previous_dates[-1]
    yesterday_bars = bars_by_date.get(yesterday_key, [])
    return today_bars, yesterday_bars


def compute_yesterday_stats(yesterday_bars: list) -> dict:
    """計算前一交易日的高/低/開/收。"""
    return {
        'high': max(b['high'] for b in yesterday_bars),
        'low': min(b['low'] for b in yesterday_bars),
        'open': yesterday_bars[0]['open'],
        'close': yesterday_bars[-1]['close'],
    }


def calculate_net_pnl_for_short_trade(entry_price: float, exit_price: float) -> tuple[float, float]:
    """計算放空交易每股淨損益與總交易成本。"""
    sell_side_cost = entry_price * (BROKERAGE_FEE_RATE + SELL_TRANSACTION_TAX_RATE)
    buy_side_cost = exit_price * BROKERAGE_FEE_RATE
    total_cost = sell_side_cost + buy_side_cost
    gross_pnl = entry_price - exit_price
    net_pnl = gross_pnl - total_cost
    return round(net_pnl, 4), round(total_cost, 4)


def build_outcome_result(
    exit_reason: str,
    entry_price: float,
    exit_price: float,
) -> dict:
    """根據進出場價格建立結果，成功/失敗以淨損益正負判斷。"""
    net_pnl, total_cost = calculate_net_pnl_for_short_trade(entry_price, exit_price)
    is_success = net_pnl > 0
    return {
        'outcome': 'success' if is_success else 'fail',
        'is_success': is_success,
        'exit_reason': exit_reason,
        'pnl': round(abs(net_pnl), 4),
        'signed_pnl': net_pnl,
        'net_return_percent': round(net_pnl / entry_price * 100, 6),
        'exit_price': round(exit_price, 4),
        'total_cost': total_cost,
    }


def build_trade_candidate(
    stock_name: str,
    industry_code: str,
    target_date: date,
    entry_bar: dict,
    entry_price: float,
    today_bars: list,
    limit_up_price: float,
    limit_down_price: float,
    strategy_type: str,
    intraday_compare_end: tuple[int, int],
) -> dict:
    """建立候選交易資料，供不同參數重複評估。"""
    dt_values = [bar['dt'] for bar in today_bars]
    open_values = np.array([float(bar['open']) for bar in today_bars], dtype=np.float64)
    high_values = np.array([float(bar['high']) for bar in today_bars], dtype=np.float64)
    low_values = np.array([float(bar['low']) for bar in today_bars], dtype=np.float64)
    return {
        'name': stock_name,
        'industry_code': industry_code,
        'date_str': target_date.strftime('%Y-%m-%d'),
        'entry_dt': entry_bar['dt'],
        'entry_price': entry_price,
        'dt_values': dt_values,
        'open_values': open_values,
        'high_values': high_values,
        'low_values': low_values,
        'limit_up_price': limit_up_price,
        'limit_down_price': limit_down_price,
        'strategy_type': strategy_type,
        'intraday_compare_end': intraday_compare_end,
        'trade_side': TRADE_SIDE_SHORT,
    }


def evaluate_candidates(
    candidates: list,
    print_results: bool = False,
) -> list:
    """對候選交易套用指定參數並回傳結果。"""
    all_results = []
    for candidate in candidates:
        name = candidate['name']
        industry_code = candidate.get('industry_code', '')
        date_str = candidate['date_str']
        entry_dt = candidate['entry_dt']
        entry_price = candidate['entry_price']
        dt_values = candidate['dt_values']
        high_values = candidate['high_values']
        low_values = candidate['low_values']
        open_values = candidate['open_values']
        limit_up_price = candidate['limit_up_price']
        limit_down_price = candidate['limit_down_price']
        strategy_type = candidate.get('strategy_type')
        intraday_compare_end = candidate.get('intraday_compare_end')
        if strategy_type != STRATEGY_LOWER:
            continue
        optimize_profit_percent = OPTIMIZE_PROFIT_PER_LOWER

        try:
            entry_idx = dt_values.index(entry_dt)
        except ValueError:
            continue

        effective_stop_loss = calculate_stop_loss_amount_by_percent(entry_price, OPTIMIZE_LOSS_PER_LOWER)
        effective_profit = calculate_take_profit_amount_by_percent(entry_price, optimize_profit_percent)
        # 只檢查入場分 K 之前的歷史 K 棒；若當日已曾觸及
        # 任一漲跌停，當日不進場。刻意不含 entry_idx，避免使用入場後資訊。
        if has_limit_price_touched_before_entry(
            high_values,
            low_values,
            entry_idx,
            limit_up_price,
            limit_down_price,
        ):
            continue

        if should_skip_entry_by_limit_up(
            entry_price,
            effective_stop_loss,
            limit_up_price,
        ):
            continue

        raw_take_profit_price = entry_price - effective_profit
        raw_stop_loss_price = entry_price + effective_stop_loss
        # 作空停利向下取 tick，固定停損向上取 tick。
        adjusted_take_profit_price = floor_price_to_tick(
            raw_take_profit_price,
            get_tick_size(raw_take_profit_price),
        )
        take_profit_price = max(adjusted_take_profit_price, limit_down_price)
        stop_loss_price = min(ceil_price_to_tick(
            raw_stop_loss_price, get_tick_size(raw_stop_loss_price),
        ), limit_up_price)

        signal = {
            'name': name,
            'activation_dt': candidate.get('activation_dt'),
            'industry_code': industry_code,
            'date_str': date_str,
            'entry_dt': entry_dt,
            'entry_price': entry_price,
            'strategy_type': strategy_type,
            'trade_side': TRADE_SIDE_SHORT,
            'take_profit_price': take_profit_price,
            'effective_profit': effective_profit,
            'opening_drop_percent': candidate['opening_drop_percent'],
            'opening_low': candidate['opening_low'],
            'previous_close': candidate['previous_close'],
            'signal_dt': candidate['signal_dt'],
            'effective_stop_loss': effective_stop_loss,
            'stop_loss_price': stop_loss_price,
        }
        result = None
        settlement_dt = entry_dt.replace(hour=intraday_compare_end[0],
                                         minute=intraday_compare_end[1], second=0, microsecond=0)
        for i in range(entry_idx, len(dt_values)):
            bar_time = dt_values[i]
            # 強制平倉先於該分鐘內的 high/low；缺棒則在其後第一根 open 平倉。
            if bar_time >= settlement_dt:
                reason = 'close' if bar_time == settlement_dt else 'close_delayed'
                result = build_outcome_result(reason, entry_price, float(open_values[i]))
            elif (i > entry_idx or not candidate.get('entry_intrabar', False)) and open_values[i] >= stop_loss_price:
                result = build_outcome_result('stop_gap', entry_price, float(open_values[i]))
            elif (i > entry_idx or not candidate.get('entry_intrabar', False)) and open_values[i] <= take_profit_price:
                # 停利委託以目標價成交，跳空不額外假設獲得較佳價格。
                result = build_outcome_result('target', entry_price, take_profit_price)
            elif high_values[i] >= stop_loss_price:
                # 盤中跌破入場的當根 high 可能早於入場；無逐筆資料，保守視為停損。
                # 同根觸及兩者時亦停損優先；當根 open 不可用作入場後跳空出場。
                result = build_outcome_result('stop', entry_price, stop_loss_price)
            elif low_values[i] <= take_profit_price:
                result = build_outcome_result('target', entry_price, take_profit_price)
            if result is not None:
                result['exit_dt'] = bar_time
                break
        if result is None:
            # 仍持倉但沒有平倉時段資料，不回推至過去 open，也不計入已完成績效。
            result = {
                'outcome': 'incomplete', 'is_success': False,
                'exit_reason': 'data_insufficient', 'exit_dt': None,
                'exit_price': None, 'signed_pnl': None, 'net_return_percent': None,
            }
        if print_results:
            line = format_result_line(name, date_str, signal, result)
            if line:
                print(line)
        all_results.append((signal, result))

    return all_results


def find_entry(today_bars, previous_close):
    """首根跌幅達門檻後，首次破首根 low 的訊號，下一根 open 入場。"""
    if not today_bars:
        return None
    first = today_bars[0]
    opening_low = Decimal(str(first['low']))
    previous = Decimal(str(previous_close))
    if opening_low >= previous * (1 - Decimal(str(OPENING_DROP_PERCENT)) / 100):
        return None
    start = first['dt'].replace(hour=SIGNAL_START_TIME[0], minute=SIGNAL_START_TIME[1])
    deadline = start.replace(hour=ENTRY_DEADLINE[0], minute=ENTRY_DEADLINE[1])
    for i, bar in enumerate(today_bars[:-1]):
        if bar['dt'] < start:
            continue
        if bar['dt'] >= deadline:
            break
        if Decimal(str(bar['low'])) < opening_low:
            entry_bar = today_bars[i + 1]
            # 分K標籤是分鐘起點，訊號必須先完成；不使用重複時間成交。
            if entry_bar['dt'] < bar['dt'] + timedelta(minutes=1):
                return None
            if entry_bar['dt'] > deadline:
                return None
            return entry_bar, float(entry_bar['open']), {
                'activation_dt': bar['dt'] + timedelta(minutes=1),
                'signal_dt': bar['dt'], 'opening_low': float(opening_low),
                'previous_close': previous_close,
                'opening_drop_percent': float((previous - opening_low) / previous * 100),
                'entry_intrabar': False,
            }
    return None


def count_testable_stock_dates(bars_by_date, target_date):
    """有前日資料及當日分K的股票－日期數，不套用訊號。"""
    count = 0
    has_previous = False
    for day in sorted(bars_by_date):
        if date.fromisoformat(day) > target_date:
            break
        bars = bars_by_date[day]
        if bars:
            if has_previous:
                count += 1
            has_previous = True
    return count


def collect_trade_candidates(stock_name, bars_by_date, target_date):
    """每檔每日僅取首次破首根 low 的訊號，最多一筆交易。"""
    candidates = []
    for date_key in sorted(bars_by_date):
        current_date = date.fromisoformat(date_key)
        if current_date > target_date:
            continue
        today, yesterday = get_target_and_yesterday(bars_by_date, current_date)
        if not today or not yesterday:
            continue
        previous = compute_yesterday_stats(yesterday)
        found = find_entry(today, previous['close'])
        if found is None:
            continue
        entry_bar, entry_price, metrics = found
        previous_close = previous['close']
        limit_up, limit_down = calculate_limit_prices(previous_close)
        candidate = build_trade_candidate(
            stock_name, '', current_date, entry_bar, entry_price, today,
            limit_up, limit_down, STRATEGY_LOWER, INTRADAY_COMPARE_END_LOWER,
        )
        candidate.update(metrics)
        candidates.append(candidate)
    return candidates


def filter_candidates_by_market(candidates, index_bars, counts):
    """各市場獨立檢查當日首根 close/open；不使用訊號完成之後的資料。"""
    accepted = []
    for candidate in candidates:
        suffix = Path(candidate['name'].upper()).suffix
        rule = MARKET_INDEX_RULES.get(suffix)
        if rule is None:
            counts['unknown_market'] = counts.get('unknown_market', 0) + 1
            continue
        key, label = rule
        bars = index_bars.get(key, {}).get(candidate['date_str'], [])
        if not bars or bars[0]['dt'] + timedelta(minutes=1) > candidate['activation_dt']:
            reason = label + '_missing'
            counts[reason] = counts.get(reason, 0) + 1
            continue
        first = bars[0]
        threshold = (IX0001_OPENING_DROP_PERCENT if suffix == '.TW'
                     else IX0043_OPENING_DROP_PERCENT)
        opening = Decimal(str(first['open']))
        close = Decimal(str(first['close']))
        if close < opening * (1 - Decimal(str(threshold)) / 100):
            reason = label + '_drop'
            counts[reason] = counts.get(reason, 0) + 1
            continue
        candidate['market_index'] = label
        candidate['market_opening_drop_percent'] = float((opening - close) / opening * 100)
        accepted.append(candidate)
    return accepted


def get_api_cache_path(target_date):
    """與 v17 共用原始 API cache；不要求舊策略的指數資料。"""
    return Path(__file__).parent / 'analysis_json_cache' / (
        f'analysis_strategy_broken_each_stock_special_short_api_cache_{target_date:%Y%m%d}.json'
    )


def resolve_historical_cache(value):
    """xxxxxxxx 為字面檔名尾碼；日期模式仍採 YYYYMMDD。"""
    if value == 'xxxxxxxx':
        return get_api_cache_path(date(2000, 1, 1)).with_name(
            'analysis_strategy_broken_each_stock_special_short_api_cache_xxxxxxxx.json'
        ), None
    if not re.fullmatch(r'\d{8}', value):
        raise ValueError('歷史 cache 設定須為 YYYYMMDD 或字面值 xxxxxxxx')
    target_date = datetime.strptime(value, '%Y%m%d').date()
    return get_api_cache_path(target_date), target_date


def save_shared_api_cache(cache_path, payload, raw):
    """與 v17 共用檔名；補資料時保留原有股票、指數與其他欄位。"""
    updated = dict(payload) if isinstance(payload, dict) else {}
    updated['stock_names'] = list(dict.fromkeys([*updated.get('stock_names', []), *raw]))
    updated['minute_raw_by_symbol'] = raw
    updated.setdefault('index_minute_raw_by_key', {})
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(updated, ensure_ascii=False), encoding='utf-8')
def validate_settings():
    times = (MARKET_OPEN_TIME, SIGNAL_START_TIME, ENTRY_DEADLINE, INTRADAY_COMPARE_END_LOWER)
    for value in times:
        if (len(value) != 2 or any(type(part) is not int for part in value)
                or not 0 <= value[0] <= 23 or not 0 <= value[1] <= 59):
            raise ValueError('時間需為合法的 (時, 分)')
    opening, start, deadline, close = [h * 60 + m for h, m in times]
    if not 9 * 60 <= opening <= start < deadline < close <= 13 * 60 + 30:
        raise ValueError('需滿足 09:00 <= 開盤 <= 訊號起點 < 最後進場 < 強制平倉 <= 13:30')
    for value in (OPENING_DROP_PERCENT, IX0001_OPENING_DROP_PERCENT,
                  IX0043_OPENING_DROP_PERCENT, OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('跌幅及停損停利百分比必須介於 0 與 100 之間')


def format_result_line(stock_name, date_str, signal, result):
    prefix = (
        f'{date_str} {stock_name} broken 首根low跌幅={signal["opening_drop_percent"]:.4f}% '
        f'昨收={signal["previous_close"]:.2f} 首根low={signal["opening_low"]:.2f} '
        f'訊號={signal["signal_dt"]:%H:%M} | '
        f'{signal["entry_dt"]:%H:%M} {signal["entry_price"]:.2f} -> '
    )
    if result['outcome'] == 'incomplete':
        return prefix + 'data_insufficient 資料不足，未計入已完成交易績效'
    return (prefix + f'{result["exit_dt"]:%H:%M} {result["exit_price"]:.2f} '
            f'{result["exit_reason"]} | 每股淨損益={result["signed_pnl"]:+.4f} '
            f'淨報酬率={result["net_return_percent"]:+.4f}%')


def summarize_results(label, pairs):
    completed = [result for _, result in pairs if result['outcome'] != 'incomplete']
    incomplete = len(pairs) - len(completed)
    return (f'{label} 已完成交易數={len(completed)} 資料不足筆數={incomplete} '
            f'獲利筆數={sum(r["is_success"] for r in completed)} '
            f'勝率={(sum(r["is_success"] for r in completed) / len(completed) * 100 if completed else 0):.2f}% '
            f'平均淨報酬率={(sum(r["net_return_percent"] for r in completed) / len(completed) if completed else 0):+.4f}% '
            f'停損={sum(r["exit_reason"] in ("stop", "stop_gap") for r in completed)} '
            f'停利={sum(r["exit_reason"] == "target" for r in completed)} '
            f'強平={sum(r["exit_reason"] in ("close", "close_delayed") for r in completed)} '
            f'每筆一股淨損益合計={sum(r["signed_pnl"] for r in completed):+.4f}')


def main():
    validate_settings()
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
    index_raw = payload.get('index_minute_raw_by_key', {}) if isinstance(payload, dict) else {}
    if not isinstance(index_raw, dict):
        index_raw = {}
    if not HISTORICAL_CACHE_DATE:
        missing_indices = [(key, symbol) for key, symbol in MARKET_INDEX_RULES.values()
                           if not isinstance(index_raw.get(key), list) or not index_raw[key]]
        if missing_indices:
            _, rest_stock = init_sdk(args.config)
            index_raw = dict(index_raw)
            for key, symbol in missing_indices:
                index_raw[key] = fetch_minute_candles(rest_stock, symbol, target_date)
            updated_payload = dict(payload) if isinstance(payload, dict) else {}
            updated_payload['index_minute_raw_by_key'] = index_raw
            save_shared_api_cache(cache_path, updated_payload, raw)
    index_bars = {key: parse_bars(index_raw.get(key, []))
                  for key, _ in MARKET_INDEX_RULES.values()
                  if isinstance(index_raw.get(key, []), list)}
    filter_counts = {}
    lines = [f'v28 broken 開盤破低放空回測，目標日期={target_date}',
             f'首根low跌幅嚴格大於={OPENING_DROP_PERCENT:g}% 訊號起點={SIGNAL_START_TIME}（含） '
             f'最後進場={ENTRY_DEADLINE}（含，依實際進場K） '
             f'停損={OPTIMIZE_LOSS_PER_LOWER:g}% 停利={OPTIMIZE_PROFIT_PER_LOWER:g}% '
             f'強制平倉={INTRADAY_COMPARE_END_LOWER} open',
             '首根為開盤後最早可用分K；昨收為前一個有分K日期的最後close。',
             f'市場過濾：IX0001 首根 close/open 下跌嚴格超過 {IX0001_OPENING_DROP_PERCENT:g}% 排除 .TW；'
             f'IX0043 嚴格超過 {IX0043_OPENING_DROP_PERCENT:g}% 排除 .TWO；等於門檻不排除。',
             '指數使用當日最早可用分K，須於股票訊號完成前可知；缺指數資料略過，不在歷史模式回退API。',
             '訊號K low 嚴格低於首根low，於下一根可用分K open 放空，每檔每日最多一筆。',
             '缺棒則進場及強平順延；進場不得超過最後進場時間。入場前觸及漲跌停或停損達漲停不入場。',
             '入場當根開始檢查停損停利，同根兩者皆觸及則停損優先；強平分鐘優先以open平倉。',
             '淨損益扣除既定手續費及交易稅，未含滑價、借券費；每筆一股合計非投資組合報酬率。']
    results = []
    testable_count = 0
    for name in stock_names:
        bars = parse_bars(raw[name])
        if HISTORICAL_CACHE_DATE:
            testable_count += count_testable_stock_dates(bars, target_date)
        candidates = collect_trade_candidates(name, bars, target_date)
        candidates = filter_candidates_by_market(candidates, index_bars, filter_counts)
        results.extend(evaluate_candidates(candidates))
    if HISTORICAL_CACHE_DATE:
        lines.insert(0, f'歷史 cache 模式：{target_date}，使用 cache 全部股票，可檢驗股票－日期數={testable_count:,}')
    results.sort(key=lambda pair: (pair[0]['entry_dt'], pair[0]['name']))
    for signal, result in results:
        lines.append(format_result_line(signal['name'], signal['date_str'], signal, result))
    for day in sorted({signal['date_str'] for signal, _ in results}):
        lines.append(summarize_results(day, [pair for pair in results if pair[0]['date_str'] == day]))
    lines.append('市場過濾候選筆數（套用漲跌停過濾前）=' + json.dumps(filter_counts, ensure_ascii=False, sort_keys=True))
    for suffix, (_, label) in MARKET_INDEX_RULES.items():
        lines.append(summarize_results(label + ' ' + suffix,
                                      [pair for pair in results if pair[0]['name'].upper().endswith(suffix)]))
    lines.append(summarize_results('總計', results))
    output = '\n'.join(lines) + '\n'
    EACH_STOCK_OUTPUT_FILE.write_text(output, encoding='utf-8')
    print(output, end='')


if __name__ == '__main__':
    main()
