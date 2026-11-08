"""v24 四關依序確認放空回測。
鎖跌停5根、low高於昨收10根、close低於昨收2根、open低於昨收入場。
缺棒保留計數，不符合只重計當前關；通過不回溯，下一根才開始下一關。
最後入場11:30（含）；本根low啟動的獲利保護及追蹤線下一根生效。
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

# fall 模式的回測

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# IDE 直接執行時在此調整；時間標籤視為該分鐘起點。
SIGNAL_START_TIME = (9, 0)
ENTRY_DEADLINE = (11, 30)  # 實際open入場截止（含），不允許截止次分鐘入場
MIN_MINUTE_BARS_BEFORE_0930 = 20  # 09:00～09:29 不足此根數，排除當日交易
STAGE_1_MIN_BARS = 5  # 第一關最少符合根數：high、low 皆等於跌停價（鎖跌停）
STAGE_2_MIN_BARS = 8  # 第二關最少符合根數：low 高於昨收價
STAGE_2_MAX_PRICE_SCALE = 40  # 第二關 high 上限刻度：昨收為 0、漲停為 100；40 表示昨收到漲停價差的 40%
STAGE_3_MIN_BARS = 1  # 第三關最少符合根數：close 低於昨收價
STAGE_4_MIN_BARS = 1  # 第四關最少符合根數：open 低於昨收價，達標時以該根 open 放空入場
# 每關條件：(欄位, 比較方式, 基準價)，同一關所有條件須同時成立。
# 欄位：open/high/low/close；比較：==/>/<；基準：previous_close/limit_down。
# 第四關僅能使用open，避免以入場後的high/low/close決定入場。
STAGE_1_CONDITIONS = (("high", "==", "limit_down"), ("low", "==", "limit_down"))
STAGE_2_CONDITIONS = (("low", ">", "previous_close"),)
STAGE_3_CONDITIONS = (("close", "<", "previous_close"),)
STAGE_4_CONDITIONS = (("open", "<", "previous_close"),)
OPTIMIZE_LOSS_PER_LOWER = 1.5  # 固定停損百分比，亦用於漲停入場限制
OPTIMIZE_PROFIT_PER_LOWER = 5.0  # 浮盈達此百分比啟動追蹤停利，不直接平倉
PROTECT_PROFIT_PER = 2.5  # 浮盈達此百分比，固定啟動獲利保護
PROTECT_LOSS_PER = 1.5  # 保護停損鎖住的毛利百分比（相對入場價，尚未扣費用）
PROFIT_BACK_PERCENT = 0.5  # 追蹤出場線：當次啟動／推進價格向上回撤百分比
PROFIT_TARGET_PERCENT = 1.0  # 下次推進目標：當次啟動／推進價格再向下的百分比

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
    'analysis_strategy_broken_each_stock_special_short_v24_result.txt'
)
STRATEGY_LOWER = 'LIMIT_DOWN_RECOVERY_BREAKDOWN'
TRADE_SIDE_SHORT = 'SHORT'


def parse_average(value):
    """保留共用資料格式；average 不參與 v24 訊號判斷。"""
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


def get_profit_trail_tick_size(price: float) -> float:
    """與 v18、v24 的追蹤停利 tick 邊界規則一致。"""
    for upper, tick in ((10, 0.01), (50, 0.05), (100, 0.1), (500, 0.5), (1000, 1)):
        if price <= upper:
            return tick
    return 5


def calculate_short_profit_trail(price: float, limit_down_price: float) -> tuple[float, float]:
    """以當次 low 計算下一個目標及反彈出場線，各價差至少一 tick。"""
    price_dec = Decimal(str(price))
    tick = Decimal(str(get_profit_trail_tick_size(price)))
    target_step = max(price_dec * Decimal(str(PROFIT_TARGET_PERCENT)) / 100, tick)
    back_step = max(price_dec * Decimal(str(PROFIT_BACK_PERCENT)) / 100, tick)
    raw_target = float(price_dec - target_step)
    raw_stop = float(price_dec + back_step)
    target = max(floor_price_to_tick(raw_target, get_profit_trail_tick_size(raw_target)), limit_down_price)
    stop = ceil_price_to_tick(raw_stop, get_profit_trail_tick_size(raw_stop))
    return target, stop


def calculate_take_profit_amount_by_percent(entry_price: float, take_profit_percent: float) -> float:
    """依入場價與停利百分比計算停利價差。"""
    return entry_price * (take_profit_percent / 100.0)


def should_skip_entry_by_limit_up(entry_price: float, stop_loss: float, limit_up_price: float) -> bool:
    """若作空進場價加停損差價已達漲停，略過本次交易。"""
    return (entry_price + stop_loss) >= limit_up_price





def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='v24 四關依序確認放空回測')
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


def get_stage_settings():
    return ((STAGE_1_MIN_BARS, STAGE_1_CONDITIONS),
            (STAGE_2_MIN_BARS, STAGE_2_CONDITIONS),
            (STAGE_3_MIN_BARS, STAGE_3_CONDITIONS),
            (STAGE_4_MIN_BARS, STAGE_4_CONDITIONS))


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
        if should_skip_entry_by_limit_up(
            entry_price,
            effective_stop_loss,
            limit_up_price,
        ):
            continue

        raw_take_profit_price = entry_price - effective_profit
        raw_stop_loss_price = entry_price + effective_stop_loss
        # 首個追蹤啟動目標向下取 tick，固定停損向上取 tick。
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
            'previous_close': candidate['previous_close'],
            'signal_dt': candidate['signal_dt'],
            'trigger_price': candidate['trigger_price'],
            'stage_completed_dts': candidate['stage_completed_dts'],
            'stage_2_max_scale': candidate['stage_2_max_scale'],
            'effective_stop_loss': effective_stop_loss,
            'stop_loss_price': stop_loss_price,
        }
        result = None
        protection_active = False
        profit_tracking_active = False
        trailing_stop_price = None
        next_profit_price = take_profit_price
        protect_trigger = Decimal(str(entry_price)) * (1 - Decimal(str(PROTECT_PROFIT_PER)) / 100)
        raw_protected_stop = float(Decimal(str(entry_price)) * (1 - Decimal(str(PROTECT_LOSS_PER)) / 100))
        protected_stop = ceil_price_to_tick(raw_protected_stop, get_profit_trail_tick_size(raw_protected_stop))
        settlement_dt = entry_dt.replace(hour=intraday_compare_end[0],
                                         minute=intraday_compare_end[1], second=0, microsecond=0)
        for i in range(entry_idx, len(dt_values)):
            bar_time = dt_values[i]
            # 只使用本根之前已生效的出場線；放空以較低反彈價位優先。
            exit_price = stop_loss_price
            exit_reason = 'protect_profit' if protection_active else 'stop'
            if profit_tracking_active and trailing_stop_price < exit_price:
                exit_price, exit_reason = trailing_stop_price, 'trailing_profit'
            # 強制平倉先於該分鐘內的 high/low；缺棒則在其後第一根 open 平倉。
            if bar_time >= settlement_dt:
                reason = 'close' if bar_time == settlement_dt else 'close_delayed'
                result = build_outcome_result(reason, entry_price, float(open_values[i]))
            elif (i > entry_idx or not candidate.get('entry_intrabar', False)) and open_values[i] >= exit_price:
                result = build_outcome_result(exit_reason + '_gap', entry_price, float(open_values[i]))
            elif high_values[i] >= exit_price:
                # open入場後，同根同時觸及停損及獲利條件時停損優先。
                result = build_outcome_result(exit_reason, entry_price, exit_price)
            if result is not None:
                result['exit_dt'] = bar_time
                break
            # 本根未出場才以 low 啟動／推進保護，新價位下一根可用分K生效。
            low = float(low_values[i])
            if (not protection_active and Decimal(str(low)) <= protect_trigger
                    and protected_stop < stop_loss_price):
                stop_loss_price = protected_stop
                protection_active = True
            if low <= next_profit_price:
                next_profit_price, new_trailing_stop = calculate_short_profit_trail(low, limit_down_price)
                trailing_stop_price = (min(trailing_stop_price, new_trailing_stop)
                                       if profit_tracking_active else new_trailing_stop)
                profit_tracking_active = True
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
    """四關依序鎖定；缺棒保留計數，第二關超過刻度上限則排除當日交易。"""
    bars = get_signal_window(today_bars)
    if bars is None:
        return None
    limit_up, limit_down = calculate_limit_prices(previous_close)
    references = {'previous_close': Decimal(str(previous_close)),
                  'limit_down': Decimal(str(limit_down))}
    stage_2_max_price = (references['previous_close']
                         + (Decimal(str(limit_up)) - references['previous_close'])
                         * Decimal(str(STAGE_2_MAX_PRICE_SCALE)) / 100)
    settings = get_stage_settings()
    stage = count = 0
    stage_2_high = None
    stage_2_max_scale = None
    completed = []
    for bar in bars:
        required, conditions = settings[stage]
        matched = True
        for field, comparison, reference in conditions:
            value, target = Decimal(str(bar[field])), references[reference]
            matched = matched and {'==': value == target, '>': value > target,
                                   '<': value < target}[comparison]
        if stage == 1:
            # 保留第二關整段等待期間的最高價，包含條件不符及計數重置前的分K。
            high = Decimal(str(bar['high']))
            stage_2_high = high if stage_2_high is None else max(stage_2_high, high)
            # 第二關回升超過上限，當日直接排除，不再等待重新符合。
            if high > stage_2_max_price:
                return None
        count = count + 1 if matched else 0
        if count < required:
            continue
        if stage == 1:
            # 與排除條件使用相同範圍：進入第二關至通過為止，不含後續關卡。
            stage_2_max_scale = float(
                (stage_2_high - references['previous_close'])
                / (Decimal(str(limit_up)) - references['previous_close']) * 100)
        completed.append(bar['dt'])
        stage += 1
        count = 0
        if stage < len(settings):
            continue  # 本根不可重複用於下一關
        return bar, float(bar['open']), {
            'activation_dt': bars[0]['dt'],
            'signal_dt': completed[-2],
            'stage_completed_dts': tuple(completed),
            'stage_2_max_scale': stage_2_max_scale,
            'previous_close': previous_close,
            'trigger_price': previous_close,
            'entry_intrabar': False,
        }
    return None


def has_enough_minute_bars_before_0930(today_bars):
    """以早盤資料密度排除疑似延遲撮合股票；歷史篩選不限制最早入場時間。"""
    minutes = {bar['dt'] for bar in today_bars
               if (9, 0) <= (bar['dt'].hour, bar['dt'].minute) < (9, 30)
               and not bar['dt'].second and not bar['dt'].microsecond}
    return len(minutes) >= MIN_MINUTE_BARS_BEFORE_0930


def count_testable_stock_dates(bars_by_date, target_date):
    """有前日資料、通過早盤根數篩選且搜尋時段有有效分K的股票－日期數。"""
    count = 0
    has_previous = False
    for day in sorted(bars_by_date):
        if date.fromisoformat(day) > target_date:
            break
        bars = bars_by_date[day]
        if bars:
            if (has_previous and has_enough_minute_bars_before_0930(bars)
                    and get_signal_window(bars) is not None):
                count += 1
            has_previous = True
    return count


def collect_trade_candidates(stock_name, bars_by_date, target_date):
    """每檔每日僅取首次完成四關的open放空。"""
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
    if type(STAGE_2_MAX_PRICE_SCALE) is not int or not 0 <= STAGE_2_MAX_PRICE_SCALE <= 100:
        raise ValueError('STAGE_2_MAX_PRICE_SCALE 必須為 0 到 100 的整數刻度')
    if type(MIN_MINUTE_BARS_BEFORE_0930) is not int or MIN_MINUTE_BARS_BEFORE_0930 <= 0:
        raise ValueError('MIN_MINUTE_BARS_BEFORE_0930 必須為正整數')
    for value in (PROTECT_PROFIT_PER, PROTECT_LOSS_PER, PROFIT_BACK_PERCENT, PROFIT_TARGET_PERCENT):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('獲利保護及追蹤停利百分比必須介於 0 與 100 之間')
    if PROTECT_LOSS_PER >= PROTECT_PROFIT_PER:
        raise ValueError('保護毛利百分比必須小於獲利保護啟動百分比')
    for required, conditions in get_stage_settings():
        if type(required) is not int or required < 1:
            raise ValueError('各關根數必須為正整數')
        if not conditions:
            raise ValueError('各關至少需要一個比較條件')
        for field, comparison, reference in conditions:
            if (field not in ('open', 'high', 'low', 'close')
                    or comparison not in ('==', '>', '<')
                    or reference not in ('previous_close', 'limit_down')):
                raise ValueError('關卡欄位、比較方式或基準價無效')
    if any(field != 'open' for field, _, _ in STAGE_4_CONDITIONS):
        raise ValueError('第四關只能使用open，以避免使用入場後資訊')
    for value in (SIGNAL_START_TIME, ENTRY_DEADLINE, INTRADAY_COMPARE_END_LOWER):
        if (len(value) != 2 or any(type(part) is not int for part in value)
                or not 0 <= value[0] <= 23 or not 0 <= value[1] <= 59):
            raise ValueError('時間需為合法的 (時, 分)')
    start = SIGNAL_START_TIME[0] * 60 + SIGNAL_START_TIME[1]
    deadline = ENTRY_DEADLINE[0] * 60 + ENTRY_DEADLINE[1]
    close = INTRADAY_COMPARE_END_LOWER[0] * 60 + INTRADAY_COMPARE_END_LOWER[1]
    if not 9 * 60 <= start <= deadline < close <= 13 * 60 + 30:
        raise ValueError('需滿足 09:00 <= 起點 <= 入場截止 < 平倉 <= 13:30')
    for value in (OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('停損停利百分比必須介於 0 與 100 之間')


def format_result_line(stock_name, date_str, signal, result):
    prefix = (
        f'{date_str} 第二關最高刻度={signal["stage_2_max_scale"]:.2f} {stock_name} '
        f'昨收={signal["previous_close"]:.2f} '
        f'門檻={signal["trigger_price"]:.4f} 訊號={signal["signal_dt"]:%H:%M} | '
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

    lines = [f'v24 四關依序確認放空回測，目標日期={target_date}',
             f'第二關最高刻度上限={STAGE_2_MAX_PRICE_SCALE}（昨收=0，漲停=100）；'
             '最高刻度包含第二關計數重置前的分K，超過上限即排除當日交易。',
             f'起點={SIGNAL_START_TIME} 最後入場={ENTRY_DEADLINE}（含） '
             f'強制平倉={INTRADAY_COMPARE_END_LOWER} open',
             f'早盤篩選：09:00～09:29 至少 {MIN_MINUTE_BARS_BEFORE_0930} 根有效且不重複分K，'
             '不足則排除當日交易；使用完整早盤歷史資料篩選，不限制最早入場至09:30。',
             f'停損={OPTIMIZE_LOSS_PER_LOWER:g}% 追蹤停利啟動={OPTIMIZE_PROFIT_PER_LOWER:g}% '
             f'獲利保護：浮盈{PROTECT_PROFIT_PER:g}%後保住{PROTECT_LOSS_PER:g}%毛利；'
             f'追蹤反彈={PROFIT_BACK_PERCENT:g}% 下次向下目標步幅={PROFIT_TARGET_PERCENT:g}%',
             '缺棒保留計數，不補價；第二關high超過刻度上限則排除當日交易；其餘不符合只重計當前關卡，完成關卡不退回；下一根有效分K才開始下一關。'
             '第四關符合時以open放空，每檔每日最多一筆，資格不跨日。昨收為前一有資料交易日最後一根close。',
             '取消固定整理窗口及入場前觸及漲跌停排除。重複、非整分鐘或順序異常分K排除。'
             '本根low啟動的保護及追蹤線下一根有效分K生效；同根停損優先。',
             '淨損益扣除既定手續費及交易稅，未含滑價、借券費；每筆一股合計非投資組合報酬率。']
    for index, (required, conditions) in enumerate(get_stage_settings(), 1):
        lines.append(f'第{index}關：' + ' 且 '.join(
            f'{field} {comparison} {reference}' for field, comparison, reference in conditions)
                     + f'，連續有效分K {required} 根')
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
