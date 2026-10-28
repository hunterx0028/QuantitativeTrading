"""v18 三階段 average 放空回測。資料取得、費用、tick 與出場沿用 v17。

依序完成 low > average、high < average、low > average 的連續根數，
每階段同一段連續分K內另須至少一根達到設定的 average 偏離百分比，
STAGE_4 須連續設定根數 low < average，確認後以緊接下一根 open 入場。
average 僅採 API 欄位，不補算。
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
from esun_marketdata import EsunMarketdata

SLOPE_CUTOFF_TIME = (9, 20)  # 此時間完成開盤至此前（不含）的 average 斜率檢核
MAX_NORMALIZED_SLOPE = 0.06  # %／分鐘；昨收正規化斜率須嚴格小於此值才允許入場

# IDE 直接執行時在此調整；三階段各自獨立設定。
STAGE_1_START_TIME = (9, 0)  # STAGE_1 起算時間，包含此分鐘
STAGE_1_MIN_BARS = 8
STAGE_2_MIN_BARS = 8
STAGE_3_MIN_BARS = 5
STAGE_4_MIN_BARS = 2  # 第三階段完成後連續 low < average，下一根 open 入場

STAGE_1_AMPLITUDE_PERCENT = 1.0  # 同段至少一根 high > 該根 average × (1 + 2%)
STAGE_2_AMPLITUDE_PERCENT = 1.0  # 同段至少一根 low < 該根 average × (1 - 2%)
STAGE_3_AMPLITUDE_PERCENT = 1.0  # 同段至少一根 high > 該根 average × (1 + 2%)
MIN_PRE_ENTRY_RANGE_PERCENT = 5.0  # 入場前當日高低價差須嚴格大於昨收的此百分比

ENTRY_START_TIME = (10, 0)  # 包含此分鐘；首次入場時間不在區間內，當日直接排除
ENTRY_DEADLINE = (11, 0)  # 包含此分鐘

OPTIMIZE_LOSS_PER_LOWER = 2.0  # 固定停損百分比，亦用於漲停入場限制
OPTIMIZE_PROFIT_PER_LOWER = 5.0  # %

INTRADAY_COMPARE_END_LOWER = (12, 50)
MIN_MINUTE_BARS_BEFORE_0930 = 20
STOCK_DATA_SOURCE = "T"  # S: selected_stocks；T: total_stocks
STOCK_LIST = []  # 非空時優先
# 空字串：使用 stock_data.py；YYYYMMDD：讀取對應日期 cache，優先於 --to。
# 字面值 "xxxxxxxx"：讀取 analysis_json_cache/analysis_strategy_broken_each_stock_special_short_api_cache_xxxxxxxx.json，
# 以檔內最新有效分K日期為截止日，回測檔內全部可用歷史日期；不連 API、不讀 stock_data.py。
HISTORICAL_CACHE_DATE = ""
BROKERAGE_FEE_RATE = 0.001425
SELL_TRANSACTION_TAX_RATE = 0.003
API_REQUEST_DELAY_SEC = 1
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
EACH_STOCK_OUTPUT_FILE = Path(__file__).with_name(
    'analysis_strategy_broken_each_stock_special_short_v18_result.txt'
)
STRATEGY_LOWER = 'AVERAGE_THREE_STAGE'
TRADE_SIDE_SHORT = 'SHORT'


def parse_average(value):
    """無效 average 不補價，會中斷當前階段的連續計數。"""
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
    parser = argparse.ArgumentParser(description='v18 三階段 average 放空回測')
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
        'exit_price': round(exit_price, 4),
        'total_cost': total_cost,
    }


def has_enough_minute_bars_before_0930(today_bars: list) -> bool:
    """09:30 前分 K 少於設定根數者視為延遲撮合股票，排除當日交易。"""
    before_0930_count = sum(
        1
        for bar in today_bars
        if (dtv := bar.get('dt')) is not None
        and (dtv.hour, dtv.minute) < (9, 30)
    )
    return before_0930_count >= MIN_MINUTE_BARS_BEFORE_0930


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
        'early_average_slope': calculate_early_average_slope(today_bars),
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
            'early_average_slope': candidate.get('early_average_slope'),
            'early_average_slope_percent': candidate.get('early_average_slope_percent'),
            'effective_stop_loss': effective_stop_loss,
            'stop_loss_price': stop_loss_price,
        }
        result = None
        first_exit_check_idx = entry_idx + 1
        for i in range(first_exit_check_idx, len(dt_values)):
            bar_time = dt_values[i]
            if (
                bar_time.hour > intraday_compare_end[0]
                or (
                    bar_time.hour == intraday_compare_end[0]
                    and bar_time.minute >= intraday_compare_end[1]
                )
            ):
                break
            if high_values[i] >= stop_loss_price:
                result = build_outcome_result('stop', entry_price, stop_loss_price)
                result['exit_dt'] = dt_values[i]
                break
            if low_values[i] <= take_profit_price:
                result = build_outcome_result('target', entry_price, take_profit_price)
                result['exit_dt'] = dt_values[i]
                break
        if result is None:
            settlement_idx = None
            for i, dtv in enumerate(dt_values):
                if dtv.hour == intraday_compare_end[0] and dtv.minute == intraday_compare_end[1]:
                    settlement_idx = i
                    break
            if settlement_idx is None:
                for i, dtv in enumerate(dt_values):
                    if (
                        dtv.hour > intraday_compare_end[0]
                        or (dtv.hour == intraday_compare_end[0] and dtv.minute > intraday_compare_end[1])
                    ):
                        settlement_idx = i
                        break
            if settlement_idx is None:
                settlement_idx = len(dt_values) - 1
            result = build_outcome_result(
                'close',
                entry_price,
                float(open_values[settlement_idx]),
            )
            result['exit_dt'] = dt_values[settlement_idx]
        if print_results:
            line = format_result_line(name, date_str, signal, result)
            if line:
                print(line)
        all_results.append((signal, result))

    return all_results


def find_entry(today_bars):
    """階段完成即鎖定；下一根才可開始下一階段或觸發入場。"""
    requirements = (STAGE_1_MIN_BARS, STAGE_2_MIN_BARS, STAGE_3_MIN_BARS)
    amplitudes = (STAGE_1_AMPLITUDE_PERCENT, STAGE_2_AMPLITUDE_PERCENT,
                  STAGE_3_AMPLITUDE_PERCENT)
    stage = count = 0
    amplitude_reached = False
    for index, bar in enumerate(today_bars):
        if (bar['dt'].hour, bar['dt'].minute) < STAGE_1_START_TIME:
            continue
        if (bar['dt'].hour, bar['dt'].minute) > ENTRY_DEADLINE:
            break
        average = parse_average(bar.get('average'))
        if average is None:
            count = 0
            amplitude_reached = False
            continue
        if stage == 3:
            if (bar['dt'].hour, bar['dt'].minute) < SLOPE_CUTOFF_TIME:
                continue
            count = count + 1 if bar['low'] < average else 0
            if count >= STAGE_4_MIN_BARS:
                if index + 1 >= len(today_bars):
                    return None
                entry_bar = today_bars[index + 1]
                if not ENTRY_START_TIME <= (entry_bar['dt'].hour, entry_bar['dt'].minute) <= ENTRY_DEADLINE:
                    return None
                return entry_bar, float(entry_bar['open'])
            continue
        matched = bar['high'] < average if stage == 1 else bar['low'] > average
        if not matched:
            count = 0
            amplitude_reached = False
            continue
        count += 1
        # 每根使用自己的 API average；用 Decimal 確保等於 2% 時不算達標。
        avg_dec = Decimal(str(average))
        percent = Decimal(str(amplitudes[stage])) / Decimal('100')
        if stage == 1:
            reached = Decimal(str(bar['low'])) < avg_dec * (1 - percent)
        else:
            reached = Decimal(str(bar['high'])) > avg_dec * (1 + percent)
        amplitude_reached = amplitude_reached or reached
        if count >= requirements[stage] and amplitude_reached:
            stage += 1
            count = 0
            amplitude_reached = False
    return None


def has_enough_pre_entry_range(today_bars, entry_dt, previous_close):
    """只使用入場前已完成分K；等於門檻不通過。"""
    prior_bars = [bar for bar in today_bars if bar['dt'] < entry_dt]
    if not prior_bars:
        return False
    # Decimal 避免相減的浮點誤差使剛好等於門檻者通過。
    price_range = (Decimal(str(max(bar['high'] for bar in prior_bars)))
                   - Decimal(str(min(bar['low'] for bar in prior_bars))))
    threshold = Decimal(str(previous_close)) * Decimal(str(MIN_PRE_ENTRY_RANGE_PERCENT)) / Decimal('100')
    return price_range > threshold


def count_testable_stock_dates(bars_by_date, target_date):
    """計算有前日資料且早盤根數足夠的股票－日期組合，不套用策略訊號。"""
    count = 0
    has_previous = False
    for day in sorted(bars_by_date):
        if date.fromisoformat(day) > target_date:
            break
        bars = bars_by_date[day]
        if bars:
            if has_previous and has_enough_minute_bars_before_0930(bars):
                count += 1
            has_previous = True
    return count


def collect_trade_candidates(stock_name, bars_by_date, target_date):
    """每日每檔只取第一個觸發訊號；交易限制未通過時該日不交易。"""
    candidates = []
    for date_key in sorted(bars_by_date):
        current_date = date.fromisoformat(date_key)
        if current_date > target_date:
            continue
        today, yesterday = get_target_and_yesterday(bars_by_date, current_date)
        if not today or not yesterday or not has_enough_minute_bars_before_0930(today):
            continue
        previous_close = compute_yesterday_stats(yesterday)['close']
        early_slope = calculate_early_average_slope(today)
        normalized_slope = normalize_slope_by_previous_close(early_slope, previous_close)
        if not is_normalized_slope_allowed(normalized_slope):
            continue
        found = find_entry(today)
        if found is None:
            continue
        entry_bar, entry_price = found
        if not has_enough_pre_entry_range(today, entry_bar['dt'], previous_close):
            continue
        limit_up, limit_down = calculate_limit_prices(previous_close)
        candidate = build_trade_candidate(
            stock_name, '', current_date, entry_bar, entry_price, today,
            limit_up, limit_down, STRATEGY_LOWER, INTRADAY_COMPARE_END_LOWER,
        )
        candidate['early_average_slope_percent'] = normalized_slope
        candidates.append(candidate)
    return candidates


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


def calculate_early_average_slope(today_bars):
    """09:00 起、截止時間之前的 API average 對實際分鐘數作最小平方法回歸。"""
    samples = []
    for bar in today_bars:
        dt = bar['dt']
        if not (9, 0) <= (dt.hour, dt.minute) < SLOPE_CUTOFF_TIME:
            continue
        average = parse_average(bar.get('average'))
        if average is not None:
            samples.append((dt.hour * 60 + dt.minute + dt.second / 60, average))
    if len(samples) < 2:
        return None
    x = np.array([sample[0] for sample in samples], dtype=float)
    y = np.array([sample[1] for sample in samples], dtype=float)
    centered_x = x - x.mean()
    denominator = float(np.dot(centered_x, centered_x))
    if denominator == 0:
        return None
    return float(np.dot(centered_x, y - y.mean()) / denominator)


def normalize_slope_by_previous_close(slope, previous_close):
    """原始元／分鐘除以昨收，轉為相對昨收的 %／分鐘。"""
    previous_close = parse_average(previous_close)
    if slope is None or previous_close is None or not math.isfinite(slope):
        return None
    return slope / previous_close * 100


def is_normalized_slope_allowed(slope):
    return slope is not None and math.isfinite(slope) and slope < MAX_NORMALIZED_SLOPE


def validate_settings():
    if not math.isfinite(MAX_NORMALIZED_SLOPE):
        raise ValueError('正規化斜率門檻必須為有限數值')
    for value in (STAGE_1_AMPLITUDE_PERCENT, STAGE_2_AMPLITUDE_PERCENT,
                  STAGE_3_AMPLITUDE_PERCENT):
        if not math.isfinite(value) or not 0 <= value < 100:
            raise ValueError('階段振幅百分比必須為 0（含）至 100（不含）的有限數值')
    if not math.isfinite(MIN_PRE_ENTRY_RANGE_PERCENT) or MIN_PRE_ENTRY_RANGE_PERCENT < 0:
        raise ValueError('入場前最低振幅百分比必須為有限非負數')
    for value in (STAGE_1_MIN_BARS, STAGE_2_MIN_BARS, STAGE_3_MIN_BARS, STAGE_4_MIN_BARS,
                  MIN_MINUTE_BARS_BEFORE_0930):
        if type(value) is not int or value <= 0:
            raise ValueError('連續根數及早盤最低根數必須為正整數')
    for value in (STAGE_1_START_TIME, SLOPE_CUTOFF_TIME, ENTRY_START_TIME, ENTRY_DEADLINE, INTRADAY_COMPARE_END_LOWER):
        if (len(value) != 2 or any(type(part) is not int for part in value)
                or not 0 <= value[0] <= 23 or not 0 <= value[1] <= 59):
            raise ValueError('時間需為合法的 (時, 分)')
    if ENTRY_DEADLINE >= INTRADAY_COMPARE_END_LOWER:
        raise ValueError('入場截止時間必須早於強制平倉時間')
    if ENTRY_START_TIME > ENTRY_DEADLINE:
        raise ValueError('最早入場時間不可晚於入場截止時間')
    if STAGE_1_START_TIME > ENTRY_DEADLINE:
        raise ValueError('STAGE_1 起算時間不可晚於入場截止時間')
    if SLOPE_CUTOFF_TIME > ENTRY_DEADLINE:
        raise ValueError('斜率檢核時間不可晚於入場截止時間')
    for value in (OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('停損停利百分比必須介於 0 與 100 之間')


def format_result_line(stock_name, date_str, signal, result):
    normalized = signal.get('early_average_slope_percent')
    normalized_text = '無法計算' if normalized is None else f'{normalized:+.6f}'
    return (
        f'{date_str} {stock_name} {normalized_text} | '
        f'{signal["entry_dt"]:%H:%M} {signal["entry_price"]:.2f} -> '
        f'{result["exit_dt"]:%H:%M} {result["exit_price"]:.2f} '
        f'{result["exit_reason"]} | 每股淨損益={result["signed_pnl"]:+.4f}'
    )


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
        if any(not isinstance(raw.get(name), list) for name in stock_names):
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
    lines = [f'v18 三階段 average 放空回測，目標日期={target_date}',
             f'STAGE_1起算={STAGE_1_START_TIME[0]:02d}:{STAGE_1_START_TIME[1]:02d} '
             f'根數={STAGE_1_MIN_BARS}/{STAGE_2_MIN_BARS}/{STAGE_3_MIN_BARS} '
             f'階段振幅門檻={STAGE_1_AMPLITUDE_PERCENT:g}%/'
             f'{STAGE_2_AMPLITUDE_PERCENT:g}%/{STAGE_3_AMPLITUDE_PERCENT:g}% '
             f'最早入場={ENTRY_START_TIME[0]:02d}:{ENTRY_START_TIME[1]:02d} '
             f'入場截止={ENTRY_DEADLINE[0]:02d}:{ENTRY_DEADLINE[1]:02d} '
             f'停損={OPTIMIZE_LOSS_PER_LOWER:g}% 停利={OPTIMIZE_PROFIT_PER_LOWER:g}% '
             f'入場前高低價差須大於昨收的{MIN_PRE_ENTRY_RANGE_PERCENT:g}% '
             f'正規化斜率須<{MAX_NORMALIZED_SLOPE:+g}%/分鐘 '
             f'STAGE_4=連續{STAGE_4_MIN_BARS}根low低於average，下一根open入場']
    results = []
    testable_count = 0
    for name in stock_names:
        bars = parse_bars(raw[name])
        if HISTORICAL_CACHE_DATE:
            testable_count += count_testable_stock_dates(bars, target_date)
        results.extend(evaluate_candidates(collect_trade_candidates(name, bars, target_date)))
    if HISTORICAL_CACHE_DATE:
        lines.insert(0, f'歷史 cache 模式：{target_date}，使用 cache 全部股票  {testable_count:,}')
    results.sort(key=lambda pair: (pair[0]['entry_dt'], pair[0]['name']))
    for signal, result in results:
        lines.append(format_result_line(signal['name'], signal['date_str'], signal, result))
    for day in sorted({signal['date_str'] for signal, _ in results}):
        day_results = [result for signal, result in results if signal['date_str'] == day]
        lines.append(f'{day} 交易數={len(day_results)} '
                     f'獲利筆數={sum(r["is_success"] for r in day_results)} '
                     f'每筆一股淨損益合計={sum(r["signed_pnl"] for r in day_results):+.4f}')
    lines.append(f'總交易數={len(results)} '
                 f'獲利筆數={sum(r["is_success"] for _, r in results)} '
                 f'每筆一股淨損益合計={sum(r["signed_pnl"] for _, r in results):+.4f}')
    output = '\n'.join(lines) + '\n'
    EACH_STOCK_OUTPUT_FILE.write_text(output, encoding='utf-8')
    print(output, end='')


if __name__ == '__main__':
    main()
