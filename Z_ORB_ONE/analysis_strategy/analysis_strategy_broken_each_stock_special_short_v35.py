"""v35 爆量急拉後跌破轉弱價放空（衝高回落）。

選股（皆以訊號棒當下已知資料判斷）：
1. 前一有資料交易日成交金額（日成交量×昨收）達 MIN_PREVIOUS_TURNOVER。
2. 當日最高價（09:00起，含訊號棒）不低於當日開盤價的 (1 + MIN_RUNUP_PERCENT%)。
3. 當日累計成交量（09:00起，含訊號棒）不低於
   前 VOLUME_AVERAGE_DAYS 個有資料交易日均量 × 已過分鐘比例 × MIN_VOLUME_RATIO。
進場：轉弱價＝當日最高價 × (1 − PULLBACK_PERCENT%)。
SIGNAL_START_TIME 起，分K close 由轉弱價上方（前一根有效分K close 嚴格高於當時轉弱價）
跌至轉弱價（含）以下，且上述條件於該棒成立，即為轉弱訊號棒；
其後等待分K low 嚴格低於該根 average，於跌破 average 棒的下一根有效分K open 放空。
實際入場不得晚於 ENTRY_DEADLINE（含）；每檔每日最多一筆。
09:00起至入場前任一有效分K high曾達漲停，當日不入場；入場棒僅檢查open。
出場：固定停損、固定停利（同根停損優先），停損出場另計滑價；12:50 open 強制平倉，缺棒順延。
使用原始 average 欄位；缺失或無效時該棒不確認跌破，不補算。缺棒以前後有效分K接續判斷。
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

# vol 模式的回測

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# IDE 直接執行時在此調整；時間標籤視為該分鐘起點。
SIGNAL_START_TIME = (9, 20)  # 訊號棒最早時間；實際入場為其後下一根有效分K open
ENTRY_DEADLINE = (12, 0)  # 實際open入場截止（含），不允許截止次分鐘入場
MIN_MINUTE_BARS_BEFORE_0930 = 20  # 09:00～09:29 不足此根數，排除當日交易（當日分析前先判斷）
MIN_PREVIOUS_TURNOVER = 1_000_000_000  # 前一有資料交易日成交金額下限（元）＝日成交量×昨收
VOLUME_UNIT_SHARES = 1000  # 分K volume 單位為張；換算成交金額用
VOLUME_AVERAGE_DAYS = 5  # 均量採前幾個有資料交易日；不足則當日不交易
MIN_VOLUME_RATIO = 4.0  # 累計量須達「均量×已過分鐘比例」的倍數
REGULAR_SESSION_MINUTES = 270  # 09:00～13:30；已過分鐘數＝訊號棒距09:00分鐘數+1
MIN_RUNUP_PERCENT = 7.0  # 當日最高價相對當日開盤價的最低漲幅百分比
PULLBACK_PERCENT = 3.0  # 轉弱價＝當日最高價向下回落的百分比
OPTIMIZE_LOSS_PER_LOWER = 2.0  # 固定停損百分比，亦用於漲停入場限制
OPTIMIZE_PROFIT_PER_LOWER = 6.0  # 固定停利百分比，觸及即平倉
STOP_SLIPPAGE_TICKS = 1  # 停損（含跳空停損）出場價向上加計的tick數；0為不計滑價

INTRADAY_COMPARE_END_LOWER = (12, 50)
STOCK_DATA_SOURCE = "T"  # 僅 HISTORICAL_CACHE_DATE 為空時使用；S: selected_stocks；T: total_stocks
# 空字串：交易當日回測，依 STOCK_DATA_SOURCE 使用 stock_data.py 的股票清單。
# 非空：只使用指定 cache 內全部股票與分 K，不讀 stock_data.py、不回退 API。
# YYYYMMDD：讀取對應日期 cache，優先於 --to。
# 字面值 "xxxxxxxx"：讀取 analysis_json_cache/analysis_strategy_broken_each_stock_special_short_api_cache_xxxxxxxx.json，
# 以檔內最新有效分K日期為截止日，回測檔內全部可用歷史日期；不連 API、不讀 stock_data.py。
HISTORICAL_CACHE_DATE = "xxxxxxxx"
BROKERAGE_FEE_RATE = 0.001425
SELL_TRANSACTION_TAX_RATE = 0.003
API_REQUEST_DELAY_SEC = 1
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
EACH_STOCK_OUTPUT_FILE = Path(__file__).with_name(
    'analysis_strategy_broken_each_stock_special_short_v35_result.txt'
)
STRATEGY_LOWER = 'VOLUME_RUNUP_THEN_PULLBACK_BREAK'
TRADE_SIDE_SHORT = 'SHORT'


def parse_positive_number(value):
    """須為有限正數；無效值不補算。"""
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


def apply_stop_slippage(price: float, limit_up_price: float) -> float:
    """停損回補價向上加計 STOP_SLIPPAGE_TICKS 個tick（逐tick依價位區間），不超過漲停價。"""
    price_dec = Decimal(str(price))
    for _ in range(STOP_SLIPPAGE_TICKS):
        price_dec += Decimal(str(get_tick_size(float(price_dec))))
    return min(float(price_dec), limit_up_price)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='v35 爆量急拉後跌破轉弱價放空回測')
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
    bar_dict = {dt, open, high, low, close, volume}
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
            volume = float(item.get('volume', 0) or 0)
            if not math.isfinite(volume) or volume < 0:
                raise ValueError('volume 必須為有限非負數')
        except (KeyError, TypeError, ValueError) as exc:
            invalid_count += 1
            if invalid_example is None:
                invalid_example = f'{item!r}；原因={exc}'
            continue
        date_key = dt.strftime('%Y-%m-%d')
        bar = {
            'dt': dt,
            **prices,
            'average': parse_positive_number(item.get('average')),
            'volume': volume,
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
    """計算前一交易日的高/低/開/收/量。"""
    return {
        'high': max(b['high'] for b in yesterday_bars),
        'low': min(b['low'] for b in yesterday_bars),
        'open': yesterday_bars[0]['open'],
        'close': yesterday_bars[-1]['close'],
        'volume': sum(b['volume'] for b in yesterday_bars),
    }


def compute_average_daily_volume(bars_by_date: dict, target_date: date):
    """前 VOLUME_AVERAGE_DAYS 個有資料交易日的日成交量平均（分K加總）；不足或為零回傳 None。"""
    target_key = target_date.strftime('%Y-%m-%d')
    previous_dates = sorted(k for k in bars_by_date if k < target_key and bars_by_date[k])
    if len(previous_dates) < VOLUME_AVERAGE_DAYS:
        return None
    volumes = [sum(bar['volume'] for bar in bars_by_date[key])
               for key in previous_dates[-VOLUME_AVERAGE_DAYS:]]
    average = sum(volumes) / VOLUME_AVERAGE_DAYS
    return average if average > 0 else None


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


def get_signal_window(today_bars):
    """搜尋時段內缺棒不限；重複、非整分鐘或順序異常則排除。"""
    if not today_bars:
        return None
    start = today_bars[0]['dt'].replace(
        hour=SIGNAL_START_TIME[0], minute=SIGNAL_START_TIME[1], second=0, microsecond=0)
    deadline = start.replace(hour=ENTRY_DEADLINE[0], minute=ENTRY_DEADLINE[1])
    bars = [bar for bar in today_bars if start <= bar['dt'] < deadline + timedelta(minutes=1)]
    times = [bar['dt'] for bar in bars]
    if (not times or times != sorted(set(times))
            or any(dt.second or dt.microsecond for dt in times)):
        return None
    return bars


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
    """對候選交易套用固定停損／停利及強制平倉並回傳結果。"""
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

        try:
            entry_idx = dt_values.index(entry_dt)
        except ValueError:
            continue

        effective_stop_loss = calculate_stop_loss_amount_by_percent(entry_price, OPTIMIZE_LOSS_PER_LOWER)
        effective_profit = calculate_take_profit_amount_by_percent(entry_price, OPTIMIZE_PROFIT_PER_LOWER)
        if should_skip_entry_by_limit_up(
            entry_price,
            effective_stop_loss,
            limit_up_price,
        ):
            continue

        raw_take_profit_price = entry_price - effective_profit
        raw_stop_loss_price = entry_price + effective_stop_loss
        # 停利價向下取 tick，停損價向上取 tick。
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
            'industry_code': industry_code,
            'date_str': date_str,
            'entry_dt': entry_dt,
            'entry_price': entry_price,
            'strategy_type': strategy_type,
            'trade_side': TRADE_SIDE_SHORT,
            'take_profit_price': take_profit_price,
            'effective_profit': effective_profit,
            'previous_close': candidate['previous_close'],
            'signal_dt': candidate['signal_dt'],
            'signal_close': candidate['signal_close'],
            'average_break_dt': candidate['average_break_dt'],
            'average_break_low': candidate['average_break_low'],
            'average_break_price': candidate['average_break_price'],
            'day_open': candidate['day_open'],
            'day_high': candidate['day_high'],
            'trigger_price': candidate['trigger_price'],
            'volume_ratio': candidate['volume_ratio'],
            'effective_stop_loss': effective_stop_loss,
            'stop_loss_price': stop_loss_price,
        }
        result = None
        settlement_dt = entry_dt.replace(hour=intraday_compare_end[0],
                                         minute=intraday_compare_end[1], second=0, microsecond=0)
        for i in range(entry_idx, len(dt_values)):
            bar_time = dt_values[i]
            bar_open = float(open_values[i])
            # 強制平倉先於該分鐘內的 high/low；缺棒則在其後第一根 open 平倉。
            if bar_time >= settlement_dt:
                reason = 'close' if bar_time == settlement_dt else 'close_delayed'
                result = build_outcome_result(reason, entry_price, bar_open)
            elif i > entry_idx and bar_open >= stop_loss_price:
                result = build_outcome_result(
                    'stop_gap', entry_price, apply_stop_slippage(bar_open, limit_up_price))
            elif high_values[i] >= stop_loss_price:
                # open入場後，同根同時觸及停損及停利時停損優先。
                result = build_outcome_result(
                    'stop', entry_price, apply_stop_slippage(stop_loss_price, limit_up_price))
            elif i > entry_idx and bar_open <= take_profit_price:
                # 跳空越過停利價，以 open 回補。
                result = build_outcome_result('take_profit_gap', entry_price, bar_open)
            elif low_values[i] <= take_profit_price:
                result = build_outcome_result('take_profit', entry_price, take_profit_price)
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


def find_entry(today_bars, previous_close, average_daily_volume):
    """轉弱訊號後，等待後續分K low低於該根average，再於下一根有效分K open入場。"""
    window = get_signal_window(today_bars)
    if window is None:
        return None
    if parse_positive_number(previous_close) is None or not average_daily_volume:
        return None
    start = window[0]['dt'].replace(
        hour=SIGNAL_START_TIME[0], minute=SIGNAL_START_TIME[1], second=0, microsecond=0)
    deadline = start.replace(hour=ENTRY_DEADLINE[0], minute=ENTRY_DEADLINE[1])
    session_open = start.replace(hour=9, minute=0)
    # 當日開盤、最高價與累計量自09:00起計，含搜尋起點前的早盤。
    bars = [bar for bar in today_bars
            if bar['dt'].date() == start.date() and bar['dt'] >= session_open
            and not bar['dt'].second and not bar['dt'].microsecond]
    if not bars:
        return None
    day_open = Decimal(str(bars[0]['open']))
    runup_price = day_open * (1 + Decimal(str(MIN_RUNUP_PERCENT)) / 100)
    pullback_factor = 1 - Decimal(str(PULLBACK_PERCENT)) / 100
    limit_up_price, _ = calculate_limit_prices(previous_close)
    limit_up = Decimal(str(limit_up_price))
    day_high = None
    cumulative_volume = 0.0
    previous_above = False  # 前一根有效分K close 是否嚴格高於當時轉弱價
    signal = None
    for bar in bars:
        if signal is not None:
            # 轉弱訊號成立後保留資格；後續棒low低於該根average後，才於再下一根open進場。
            if bar['dt'] > deadline:
                return None
            if 'average_break_dt' in signal:
                # 此時只知道入場棒open，不使用其後才形成的high。
                if Decimal(str(bar['open'])) >= limit_up:
                    return None
                return bar, float(bar['open']), signal
            if Decimal(str(bar['high'])) >= limit_up:
                return None
            average = parse_positive_number(bar.get('average'))
            if average is not None and Decimal(str(bar['low'])) < Decimal(str(average)):
                signal['average_break_dt'] = bar['dt']
                signal['average_break_low'] = bar['low']
                signal['average_break_price'] = average
            continue
        if bar['dt'] > deadline:
            return None
        high = Decimal(str(bar['high']))
        if high >= limit_up:
            return None
        day_high = high if day_high is None else max(day_high, high)
        cumulative_volume += bar['volume']
        trigger_price = day_high * pullback_factor
        close = Decimal(str(bar['close']))
        is_below = close <= trigger_price
        crossed = previous_above and is_below
        previous_above = not is_below
        if not crossed or bar['dt'] < start:
            continue
        if day_high < runup_price:
            continue
        elapsed_minutes = int((bar['dt'] - session_open).total_seconds() // 60) + 1
        expected_volume = average_daily_volume * elapsed_minutes / REGULAR_SESSION_MINUTES
        volume_ratio = cumulative_volume / expected_volume
        if volume_ratio < MIN_VOLUME_RATIO:
            continue
        signal = {
            'signal_dt': bar['dt'],
            'signal_close': bar['close'],
            'previous_close': previous_close,
            'day_open': float(day_open),
            'day_high': float(day_high),
            'trigger_price': float(trigger_price),
            'volume_ratio': volume_ratio,
        }
    return None


def has_enough_minute_bars_before_0930(today_bars):
    """以早盤資料密度排除疑似延遲撮合股票；於當日分析前先判斷，不限制最早入場時間。"""
    minutes = {bar['dt'] for bar in today_bars
               if (9, 0) <= (bar['dt'].hour, bar['dt'].minute) < (9, 30)
               and not bar['dt'].second and not bar['dt'].microsecond}
    return len(minutes) >= MIN_MINUTE_BARS_BEFORE_0930


def count_testable_stock_dates(bars_by_date, target_date):
    """有足夠前日資料、通過早盤根數篩選且搜尋時段有有效分K的股票－日期數。"""
    count = 0
    previous_days = 0
    for day in sorted(bars_by_date):
        if date.fromisoformat(day) > target_date:
            break
        bars = bars_by_date[day]
        if bars:
            if (previous_days >= VOLUME_AVERAGE_DAYS and has_enough_minute_bars_before_0930(bars)
                    and get_signal_window(bars) is not None):
                count += 1
            previous_days += 1
    return count


def collect_trade_candidates(stock_name, bars_by_date, target_date):
    """每檔每日取首次轉弱訊號，等待後續low低於該根average，再於下一根有效分K open放空。"""
    candidates = []
    for date_key in sorted(bars_by_date):
        current_date = date.fromisoformat(date_key)
        if current_date > target_date:
            continue
        today, yesterday = get_target_and_yesterday(bars_by_date, current_date)
        if not today or not yesterday:
            continue
        if not has_enough_minute_bars_before_0930(today):
            continue
        previous = compute_yesterday_stats(yesterday)
        previous_close = previous['close']
        previous_turnover = previous['volume'] * VOLUME_UNIT_SHARES * previous_close
        if previous_turnover < MIN_PREVIOUS_TURNOVER:
            continue
        average_daily_volume = compute_average_daily_volume(bars_by_date, current_date)
        if average_daily_volume is None:
            continue
        found = find_entry(today, previous_close, average_daily_volume)
        if found is None:
            continue
        entry_bar, entry_price, metrics = found
        limit_up, limit_down = calculate_limit_prices(previous_close)
        candidate = build_trade_candidate(
            stock_name, '', current_date, entry_bar, entry_price, today,
            limit_up, limit_down, STRATEGY_LOWER, INTRADAY_COMPARE_END_LOWER,
        )
        candidate.update(metrics)
        candidate['previous_turnover'] = previous_turnover
        candidates.append(candidate)
    return candidates


def get_api_cache_path(target_date):
    """與 v17 共用原始 API cache；僅補齊股票資料。"""
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
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(updated, ensure_ascii=False), encoding='utf-8')


def validate_settings():
    if type(MIN_MINUTE_BARS_BEFORE_0930) is not int or MIN_MINUTE_BARS_BEFORE_0930 <= 0:
        raise ValueError('MIN_MINUTE_BARS_BEFORE_0930 必須為正整數')
    for name, value in (('VOLUME_AVERAGE_DAYS', VOLUME_AVERAGE_DAYS),
                        ('VOLUME_UNIT_SHARES', VOLUME_UNIT_SHARES),
                        ('REGULAR_SESSION_MINUTES', REGULAR_SESSION_MINUTES)):
        if type(value) is not int or value <= 0:
            raise ValueError(f'{name} 必須為正整數')
    if type(STOP_SLIPPAGE_TICKS) is not int or STOP_SLIPPAGE_TICKS < 0:
        raise ValueError('STOP_SLIPPAGE_TICKS 必須為非負整數')
    for name, value in (('MIN_PREVIOUS_TURNOVER', MIN_PREVIOUS_TURNOVER),
                        ('MIN_VOLUME_RATIO', MIN_VOLUME_RATIO),
                        ('MIN_RUNUP_PERCENT', MIN_RUNUP_PERCENT)):
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError(f'{name} 必須為有限非負數')
    for value in (SIGNAL_START_TIME, ENTRY_DEADLINE, INTRADAY_COMPARE_END_LOWER):
        if (len(value) != 2 or any(type(part) is not int for part in value)
                or not 0 <= value[0] <= 23 or not 0 <= value[1] <= 59):
            raise ValueError('時間需為合法的 (時, 分)')
    start = SIGNAL_START_TIME[0] * 60 + SIGNAL_START_TIME[1]
    deadline = ENTRY_DEADLINE[0] * 60 + ENTRY_DEADLINE[1]
    close = INTRADAY_COMPARE_END_LOWER[0] * 60 + INTRADAY_COMPARE_END_LOWER[1]
    if not 9 * 60 <= start <= deadline < close <= 13 * 60 + 30:
        raise ValueError('需滿足 09:00 <= 起點 <= 入場截止 < 平倉 <= 13:30')
    for value in (OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER, PULLBACK_PERCENT):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('停損、停利及回落百分比必須介於 0 與 100 之間')


def format_result_line(stock_name, date_str, signal, result):
    prefix = (
        f'{date_str} 放空 {stock_name} 昨收={signal["previous_close"]:.2f} '
        f'開盤={signal["day_open"]:.2f} 日高={signal["day_high"]:.2f} '
        f'轉弱價={signal["trigger_price"]:.4f} 量比={signal["volume_ratio"]:.2f} '
        f'跌破確認={signal["signal_dt"]:%H:%M} close={signal["signal_close"]:.2f} '
        f'跌破average={signal["average_break_dt"]:%H:%M} low={signal["average_break_low"]:.2f} average={signal["average_break_price"]:.4f} '
        f'停損={signal["stop_loss_price"]:.2f} 停利={signal["take_profit_price"]:.2f} | '
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
    average_return = (sum(r['net_return_percent'] for r in completed) / len(completed)
                      if completed else 0.0)
    return (f'{label} 已完成交易數={len(completed)} 資料不足筆數={incomplete} '
            f'獲利筆數={sum(r["is_success"] for r in completed)} '
            f'每筆一股淨損益合計={sum(r["signed_pnl"] for r in completed):+.4f} '
            f'平均淨報酬率={average_return:+.4f}%')


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
        stocks = selected_stocks if STOCK_DATA_SOURCE == 'S' else total_stocks
        stock_names = list(dict.fromkeys(item[0] for item in stocks))
        raw = payload.get('minute_raw_by_symbol', {}) if isinstance(payload, dict) else {}
        if not isinstance(raw, dict):
            raw = {}
        rest_stock = None

        def get_rest_stock():
            nonlocal rest_stock
            if rest_stock is None:
                _, rest_stock = init_sdk(args.config)
            return rest_stock

        missing = [name for name in stock_names if not isinstance(raw.get(name), list)]
        if missing:
            fetched = {}
            for i, name in enumerate(missing, 1):
                print(f'API抓取進度：{i}/{len(missing)} {name}')
                fetched[name] = fetch_minute_candles(get_rest_stock(), extract_symbol(name), target_date)
            raw = {**raw, **fetched}
        if missing:
            save_shared_api_cache(cache_path, payload, raw)

    lines = [f'v35 爆量急拉後跌破轉弱價放空回測，目標日期={target_date}',
             f'訊號起點={SIGNAL_START_TIME} 最後入場={ENTRY_DEADLINE}（含） '
             f'強制平倉={INTRADAY_COMPARE_END_LOWER} open',
             f'早盤篩選：09:00～09:29 至少 {MIN_MINUTE_BARS_BEFORE_0930} 根有效且不重複分K，'
             '不足則排除當日交易；於當日分析前先判斷，不限制最早入場至09:30。',
             f'停損={OPTIMIZE_LOSS_PER_LOWER:g}%（向上取tick，出場加計{STOP_SLIPPAGE_TICKS}個tick滑價，不超過漲停） '
             f'停利={OPTIMIZE_PROFIT_PER_LOWER:g}%（向下取tick，觸及即平倉）；同根停損優先，開盤跳空以open出場。',
             f'選股：前一有資料交易日成交金額（日量×{VOLUME_UNIT_SHARES}×昨收）至少{MIN_PREVIOUS_TURNOVER:,}元；'
             f'當日最高價不低於當日開盤價的{100 + MIN_RUNUP_PERCENT:g}%；'
             f'09:00起累計量不低於前{VOLUME_AVERAGE_DAYS}個有資料交易日均量×（已過分鐘數÷{REGULAR_SESSION_MINUTES}）'
             f'的{MIN_VOLUME_RATIO:g}倍，前日資料不足{VOLUME_AVERAGE_DAYS}日則不交易。',
             f'進場：轉弱價＝當日最高價×{100 - PULLBACK_PERCENT:g}%。訊號起點後，前一根有效分K close嚴格高於當時轉弱價、'
             '本根close小於或等於轉弱價，且選股條件於本根成立，即為轉弱訊號棒；'
             '其後等待分K low嚴格低於該根average，於跌破average棒的下一根有效分K open放空。'
             '等待期間保留轉弱訊號資格，不重新檢查量比或因最高價更新而重設，但觸及漲停即取消當日交易。'
             '跌破當下條件未成立則不交易，須待重新站回轉弱價上方後再次跌破。'
             '最晚入場為截止時間（含），每檔每日最多一筆，資格不跨日。',
             '當日開盤、最高價及累計量自09:00起計；缺棒以前後有效分K接續判斷；使用原始average欄位，缺失或無效時該棒不確認跌破、不補算。'
             '昨收為前一有資料交易日最後一根close；重複、非整分鐘或順序異常分K排除。'
             '09:00起至入場前任一有效分K high曾達漲停則當日不交易；入場棒僅檢查open是否達漲停，不使用該棒high。'
             '入場價加停損價差達漲停價則不交易。',
             '淨損益扣除既定手續費及交易稅，停損含上述滑價，未含借券費；每筆一股合計非投資組合報酬率。']
    results = []
    testable_count = 0
    for name in stock_names:
        bars = parse_bars(raw[name])
        if HISTORICAL_CACHE_DATE:
            testable_count += count_testable_stock_dates(bars, target_date)
        results.extend(evaluate_candidates(collect_trade_candidates(name, bars, target_date)))
    if HISTORICAL_CACHE_DATE:
        lines.insert(0, f'歷史 cache 模式：{target_date}，使用 cache 全部股票，可檢驗股票－日期數={testable_count:,}')
    results.sort(key=lambda pair: (pair[0]['entry_dt'], pair[0]['name']))
    for signal, result in results:
        lines.append(format_result_line(signal['name'], signal['date_str'], signal, result))
    for day in sorted({signal['date_str'] for signal, _ in results}):
        lines.append(summarize_results(day, [pair for pair in results if pair[0]['date_str'] == day]))
    lines.append(summarize_results('總計', results))
    output = '\n'.join(lines) + '\n'
    EACH_STOCK_OUTPUT_FILE.write_text(output, encoding='utf-8')
    print(output, end='')


if __name__ == '__main__':
    main()
