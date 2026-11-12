"""v36 昨日收漲、開盤跳空後衝高回落放空；沿用 v0 共用回測流程。"""

import argparse
import bisect
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

# ======================================================================
# 【策略參數】新策略的參數加在這裡
# ======================================================================
STRATEGY_LOWER = 'GAP_UP_RUNUP_PULLBACK_V36'
PREVIOUS_TURNOVER_TOP_PERCENT = 20.0  # 沿用 v35：昨日成交值排名前此百分比
MIN_PREVIOUS_TURNOVER = 300_000_000  # 昨日成交值保底（元），非多日平均成交值
VOLUME_UNIT_SHARES = 1000  # 分K成交量單位為張
VOLUME_AVERAGE_DAYS = 5  # 排名母體需前此數個有資料交易日齊備且均量大於零
MIN_OPEN_GAP_PERCENT = 1.0  # 今日開盤相對昨收漲幅須嚴格大於此值
MIN_RUNUP_PERCENT = 6.0  # 累計最高價至少為開盤價上漲此百分比
PULLBACK_PERCENT = 2.0  # 收盤價小於等於累計最高價回落此百分比

# ======================================================================
# 共用參數；IDE 直接執行時在此調整。時間標籤視為該分鐘起點。
# ======================================================================
SIGNAL_START_TIME = (9, 5)  # 訊號棒最早時間；實際入場為其後下一根有效分K open
ENTRY_DEADLINE = (10, 0)  # 實際open入場截止（含），不允許截止次分鐘入場

OPTIMIZE_LOSS_PER_LOWER = 2.5  # 固定停損百分比，亦用於漲停入場限制
OPTIMIZE_PROFIT_PER_LOWER = 6.0  # 浮盈達此百分比啟動追蹤停利，不直接平倉

ENABLE_PROFIT_PROTECTION = False  # True才啟用PROTECT_PROFIT_PER、PROTECT_LOSS_PER；不影響追蹤停利
PROTECT_PROFIT_PER = 2.5  # 浮盈達此百分比啟動獲利保護
PROTECT_LOSS_PER = 1.5  # 保護停損鎖住的毛利百分比，尚未扣費用

PROFIT_BACK_PERCENT = 0.5  # 追蹤出場線相對當次low向上回撤百分比
PROFIT_TARGET_PERCENT = 1.0  # 下一個追蹤目標相對當次low再向下的百分比

STOP_SLIPPAGE_TICKS = 0  # 停損（含跳空停損）出場價向上加計的tick數；0為不計滑價

INTRADAY_COMPARE_END_LOWER = (12, 50)  # 強制平倉時間，以該分K open 平倉
MIN_MINUTE_BARS_BEFORE_0930 = 20  # 09:00～09:29 不足此根數，排除當日交易（當日分析前先判斷）
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
API_FETCH_DAYS = 40  # API 自目標日期往前抓取的日曆天數
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
EACH_STOCK_OUTPUT_FILE = Path(__file__).with_name(
    'analysis_strategy_broken_each_stock_special_short_v36_result.txt'
)
TRADE_SIDE_SHORT = 'SHORT'


# ======================================================================
# 【策略】插入點
# ======================================================================
def find_signal(session_bars, day_info):
    """只讀截至訊號棒的資料；同棒可同時衝高與轉弱，下一根有效分K open 入場。"""
    if not session_bars:
        return None
    stock_name = day_info['stock_name']
    date_key = day_info['date_key']
    context = day_info['context']
    if not is_previous_turnover_qualified(stock_name, date_key, context):
        return None
    previous_dates = sorted(key for key, bars in day_info['bars_by_date'].items()
                            if key < date_key and bars)
    if len(previous_dates) < 2:
        return None
    prior_close = day_info['bars_by_date'][previous_dates[-2]][-1]['close']
    previous_close = Decimal(str(day_info['previous_close']))
    if previous_close <= Decimal(str(prior_close)):
        return None
    day_open = Decimal(str(session_bars[0]['open']))
    if day_open <= previous_close * (1 + Decimal(str(MIN_OPEN_GAP_PERCENT)) / 100):
        return None
    runup_price = day_open * (1 + Decimal(str(MIN_RUNUP_PERCENT)) / 100)
    pullback_factor = 1 - Decimal(str(PULLBACK_PERCENT)) / 100
    day_high = day_open
    for index, bar in enumerate(session_bars):
        bar_time = (bar['dt'].hour, bar['dt'].minute)
        if bar_time >= ENTRY_DEADLINE:
            break
        day_high = max(day_high, Decimal(str(bar['high'])))
        if bar_time < SIGNAL_START_TIME:
            continue
        trigger_price = day_high * pullback_factor
        if day_high >= runup_price and Decimal(str(bar['close'])) <= trigger_price:
            return index, {
                'previous_turnover': context[date_key]['by_stock'][stock_name],
                'prior_close': prior_close,
                'day_open': float(day_open),
                'open_gap_percent': float((day_open / previous_close - 1) * 100),
                'day_high': float(day_high),
                'trigger_price': float(trigger_price),
                'signal_close': bar['close'],
            }
    return None


def build_market_context(bars_by_stock, target_date):
    """沿用 v35 昨日成交值排名及排名母體，不加入量比或 average 入場條件。"""
    return build_previous_turnover_table(bars_by_stock, target_date)


def validate_strategy_settings():
    for name, value in (('VOLUME_UNIT_SHARES', VOLUME_UNIT_SHARES),
                        ('VOLUME_AVERAGE_DAYS', VOLUME_AVERAGE_DAYS)):
        if type(value) is not int or value <= 0:
            raise ValueError(f'{name} 必須為正整數')
    for name, value in (('MIN_PREVIOUS_TURNOVER', MIN_PREVIOUS_TURNOVER),
                        ('MIN_OPEN_GAP_PERCENT', MIN_OPEN_GAP_PERCENT),
                        ('MIN_RUNUP_PERCENT', MIN_RUNUP_PERCENT)):
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError(f'{name} 必須為有限非負數')
    for name, value in (('PREVIOUS_TURNOVER_TOP_PERCENT', PREVIOUS_TURNOVER_TOP_PERCENT),
                        ('PULLBACK_PERCENT', PULLBACK_PERCENT)):
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 100:
            raise ValueError(f'{name} 必須大於 0 且不超過 100')
    if PULLBACK_PERCENT == 100:
        raise ValueError('PULLBACK_PERCENT 必須小於 100')


def strategy_header_lines():
    return [
        '策略：v36 昨日收漲、開盤跳空、衝高回落放空。',
        f'成交值沿用v35：昨日成交量×{VOLUME_UNIT_SHARES}×昨收，'
        f'排名前{PREVIOUS_TURNOVER_TOP_PERCENT:g}%且至少{MIN_PREVIOUS_TURNOVER:,}元；'
        f'排名母體須前{VOLUME_AVERAGE_DAYS}個有資料交易日齊備且均量大於零，當日無分K者不列入；'
        f'金額嚴格較低股票占比至少{100 - PREVIOUS_TURNOVER_TOP_PERCENT:g}%。',
        f'選股：昨日收盤嚴格高於前一有資料交易日收盤；今日開盤相對昨收跳空嚴格大於{MIN_OPEN_GAP_PERCENT:g}%。',
        f'訊號：09:00起累計最高價（含訊號棒）≥開盤價×{1 + MIN_RUNUP_PERCENT / 100:g}，'
        f'且訊號棒close≤累計最高價×{1 - PULLBACK_PERCENT / 100:g}；'
        '允許同棒成立，不要求前一根close在轉弱價上方。最高價只使用截至訊號棒的資料。',
        '訊號棒須早於最後入場時間，下一根有效分K open入場且不得晚於截止時間；'
        '今日開盤取09:00起第一根有效分K open。',
        '保留早盤根數篩選：於09:30前入場時，仍以09:00～09:29完整資料事後判斷資格。',
    ]


def format_strategy_text(signal):
    fields = signal['strategy_fields']
    return (f'昨成交值={fields["previous_turnover"]:,.0f} '
            f'跳空={fields["open_gap_percent"]:.2f}% '
            f'開盤={fields["day_open"]:.2f} 最高={fields["day_high"]:.2f} '
            f'轉弱價={fields["trigger_price"]:.2f} 訊號收盤={fields["signal_close"]:.2f}')


def build_previous_turnover_table(bars_by_stock: dict, target_date: date) -> dict:
    """各交易日清單內所有股票的前一有資料交易日成交金額。
    回傳 dict[YYYY-MM-DD -> {'by_stock': {股票: 金額}, 'sorted': 由小到大金額}]；
    當日無分K的股票不列入。
    排名母體與「當日可交易」的條件一致（對齊實機 VOL）：之前有資料的交易日不足
    VOLUME_AVERAGE_DAYS 日，或這幾日均量為零的股票，當日本來就不會交易，也不列入排名。
    僅使用當日之前的資料，當日開盤前即可算出。"""
    target_key = target_date.strftime('%Y-%m-%d')
    table: dict[str, dict] = {}
    for stock_name, bars_by_date in bars_by_stock.items():
        previous_bars = None
        previous_volumes = []  # 之前各有資料交易日的日量，由舊到新
        for date_key in sorted(bars_by_date):
            if date_key > target_key:
                break
            bars = bars_by_date[date_key]
            if not bars:
                continue
            recent_volumes = previous_volumes[-VOLUME_AVERAGE_DAYS:]
            if len(recent_volumes) == VOLUME_AVERAGE_DAYS and sum(recent_volumes) > 0:
                turnover = recent_volumes[-1] * VOLUME_UNIT_SHARES * previous_bars[-1]['close']
                table.setdefault(date_key, {'by_stock': {}})['by_stock'][stock_name] = turnover
            previous_bars = bars
            previous_volumes.append(sum(bar['volume'] for bar in bars))
    for entry in table.values():
        entry['sorted'] = sorted(entry['by_stock'].values())
    return table


def is_previous_turnover_qualified(stock_name: str, date_key: str, turnover_table: dict) -> bool:
    """成交金額不低於保底，且清單內金額嚴格較低的股票占比不低於 (100 − 排名百分比)%。"""
    entry = turnover_table.get(date_key)
    if not entry or stock_name not in entry['by_stock']:
        return False
    turnover = entry['by_stock'][stock_name]
    if turnover < MIN_PREVIOUS_TURNOVER:
        return False
    lower_count = bisect.bisect_left(entry['sorted'], turnover)
    required_ratio = 1 - Decimal(str(PREVIOUS_TURNOVER_TOP_PERCENT)) / 100
    return Decimal(lower_count) >= required_ratio * len(entry['sorted'])


# ======================================================================
# 以下為共用程式
# ======================================================================
def parse_positive_number(value):
    """須為有限正數；無效值不補算。"""
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0 else None


# ---------------------------------------------------------------- tick 與價格
def get_tick_size(price):
    """A boundary belongs to the price interval starting at that boundary."""
    price = Decimal(str(price))
    for upper, tick in (('10', '0.01'), ('50', '0.05'), ('100', '0.1'),
                        ('500', '0.5'), ('1000', '1')):
        if price < Decimal(upper):
            return float(tick)
    return 5.0


def floor_price_to_tick(price, tick=None):
    tick = Decimal(str(get_tick_size(price) if tick is None else tick))
    return float((Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick)


def ceil_price_to_tick(price, tick=None):
    tick = Decimal(str(get_tick_size(price) if tick is None else tick))
    return float((Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_CEILING) * tick)


def next_price_up(price):
    """Next legal price strictly above price, including at interval boundaries."""
    rounded = ceil_price_to_tick(price)
    if Decimal(str(rounded)) > Decimal(str(price)):
        return rounded
    return float(Decimal(str(rounded)) + Decimal(str(get_tick_size(rounded))))


def next_price_down(price):
    """Next legal price strictly below price; use the lower interval at a boundary."""
    rounded = floor_price_to_tick(price)
    if Decimal(str(rounded)) < Decimal(str(price)):
        return rounded
    value = Decimal(str(rounded))
    lower_ticks = {'10': '0.01', '50': '0.05', '100': '0.1', '500': '0.5', '1000': '1'}
    tick = next((step for boundary, step in lower_ticks.items()
                 if value == Decimal(boundary)), None)
    return float(value - Decimal(tick or str(get_tick_size(rounded))))


def calculate_limit_prices(prev_close: float):
    """以昨收計算漲跌停價。

    限制：除權息日的漲跌停基準為參考價而非昨收，資料無參考價欄位，該日漲跌停價會有誤差。
    """
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


# Compatibility name; all price calculations now share one grid definition.
get_profit_trail_tick_size = get_tick_size


def calculate_short_profit_trail(price: float, limit_down_price: float) -> tuple[float, float]:
    """以當次low計算下一目標與反彈出場線，各價差至少一tick。"""
    price_dec = Decimal(str(price))
    tick = Decimal(str(get_profit_trail_tick_size(price)))
    target_step = max(price_dec * Decimal(str(PROFIT_TARGET_PERCENT)) / 100, tick)
    back_step = max(price_dec * Decimal(str(PROFIT_BACK_PERCENT)) / 100, tick)
    raw_target = float(price_dec - target_step)
    raw_stop = float(price_dec + back_step)
    target = max(floor_price_to_tick(raw_target, get_profit_trail_tick_size(raw_target)), limit_down_price)
    stop = ceil_price_to_tick(raw_stop, get_profit_trail_tick_size(raw_stop))
    return target, stop


# ---------------------------------------------------------------- 入場限制
def should_skip_entry_by_limit_up(entry_price: float, stop_loss: float, limit_up_price: float) -> bool:
    """若作空進場價加停損差價已達漲停（無完整停損空間），略過本次交易。"""
    return (entry_price + stop_loss) >= limit_up_price


def has_limit_price_touched_before_entry(
    dt_values: list,
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    entry_idx: int,
    limit_up_price: float,
    limit_down_price: float,
) -> bool:
    """09:00起至入場前任一有效分K high達漲停或low達跌停，或入場棒open已達漲跌停，回傳True。

    入場當下只知道入場棒open，不使用該棒其後才形成的high/low。
    09:00前或非整分鐘的分K不列入判斷。
    """
    limit_up = Decimal(str(limit_up_price))
    limit_down = Decimal(str(limit_down_price))
    session_open = dt_values[entry_idx].replace(hour=9, minute=0, second=0, microsecond=0)
    for i in range(entry_idx):
        bar_time = dt_values[i]
        if bar_time < session_open or bar_time.second or bar_time.microsecond:
            continue
        if (Decimal(str(float(high_values[i]))) >= limit_up
                or Decimal(str(float(low_values[i]))) <= limit_down):
            return True
    entry_open = Decimal(str(float(open_values[entry_idx])))
    return entry_open >= limit_up or entry_open <= limit_down


def apply_stop_slippage(price: float, limit_up_price: float) -> float:
    """停損回補價向上加計 STOP_SLIPPAGE_TICKS 個tick（逐tick依價位區間），不超過漲停價。"""
    price_dec = Decimal(str(price))
    for _ in range(STOP_SLIPPAGE_TICKS):
        price_dec = Decimal(str(next_price_up(price_dec)))
    return min(float(price_dec), limit_up_price)


# ---------------------------------------------------------------- 參數與 SDK
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='v36 昨日收漲、跳空衝高回落放空回測')
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
    from esun_marketdata import EsunMarketdata  # 用到才匯入；歷史 cache 模式不需安裝 SDK

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
    from_date = target_date - timedelta(days=API_FETCH_DAYS)
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


# ---------------------------------------------------------------- 分K資料
def parse_bars(raw_data: list) -> dict:
    """
    將 SDK 回傳的 data 陣列解析並依日期分組。
    回傳 dict[YYYY-MM-DD -> list[bar_dict]]，每個日期內按時間升序排列。
    bar_dict = {dt, open, high, low, close, average, volume}
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


def has_enough_minute_bars_before_0930(today_bars):
    """以早盤資料密度排除疑似處置或延遲撮合股票；只計 09:00～09:29 不重複的整分鐘分K。"""
    minutes = {bar['dt'] for bar in today_bars
               if (9, 0) <= (bar['dt'].hour, bar['dt'].minute) < (9, 30)
               and not bar['dt'].second and not bar['dt'].microsecond}
    return len(minutes) >= MIN_MINUTE_BARS_BEFORE_0930


def get_signal_window(today_bars):
    """SIGNAL_START_TIME～ENTRY_DEADLINE（含）的分K；缺棒不限，重複、非整分鐘或順序異常則回傳 None。"""
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


def get_session_bars(today_bars):
    """當日09:00起的整分鐘分K，供策略判斷訊號；當日開盤、最高價、累計量皆應由此起算。"""
    return [bar for bar in today_bars
            if (bar['dt'].hour, bar['dt'].minute) >= (9, 0)
            and not bar['dt'].second and not bar['dt'].microsecond]


# ---------------------------------------------------------------- 損益
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


# ---------------------------------------------------------------- 進場與出場
def resolve_entry(session_bars, signal_index):
    """訊號棒的下一根有效分K open 入場；不符時間限制回傳 None（當日不交易）。"""
    if isinstance(signal_index, bool) or not isinstance(signal_index, (int, np.integer)):
        raise ValueError(f'find_signal 回傳的訊號棒索引必須為整數：{signal_index!r}')
    signal_index = int(signal_index)
    if not 0 <= signal_index < len(session_bars):
        raise ValueError(f'find_signal 回傳的訊號棒索引超出範圍：{signal_index}')
    if signal_index + 1 >= len(session_bars):
        return None  # 訊號棒之後已無分K
    signal_bar = session_bars[signal_index]
    entry_bar = session_bars[signal_index + 1]
    if (signal_bar['dt'].hour, signal_bar['dt'].minute) < SIGNAL_START_TIME:
        return None
    if (entry_bar['dt'].hour, entry_bar['dt'].minute) > ENTRY_DEADLINE:
        return None
    return entry_bar, float(entry_bar['open'])


def build_trade_candidate(
    stock_name: str,
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


def evaluate_candidates(candidates: list) -> list:
    """套用入場限制、固定停損、獲利保護、追蹤停利及強制平倉。"""
    all_results = []
    for candidate in candidates:
        name = candidate['name']
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

        # 09:00起至入場前曾觸及任一漲跌停，或入場棒open已達漲跌停，當日不進場。
        if has_limit_price_touched_before_entry(
            dt_values,
            open_values,
            high_values,
            low_values,
            entry_idx,
            limit_up_price,
            limit_down_price,
        ):
            continue

        effective_stop_loss = calculate_stop_loss_amount_by_percent(entry_price, OPTIMIZE_LOSS_PER_LOWER)
        effective_profit = calculate_take_profit_amount_by_percent(entry_price, OPTIMIZE_PROFIT_PER_LOWER)
        # 放空入場須保留完整停損空間。
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
            'date_str': date_str,
            'signal_dt': candidate.get('signal_dt'),
            'entry_dt': entry_dt,
            'entry_price': entry_price,
            'strategy_type': strategy_type,
            'trade_side': TRADE_SIDE_SHORT,
            'previous_close': candidate.get('previous_close'),
            'take_profit_price': take_profit_price,
            'effective_profit': effective_profit,
            'effective_stop_loss': effective_stop_loss,
            'stop_loss_price': stop_loss_price,
            'strategy_fields': candidate.get('strategy_fields', {}),
        }
        result = None
        protection_active = False
        profit_tracking_active = False
        trailing_stop_price = None
        next_profit_price = take_profit_price
        protect_trigger = protected_stop = None
        if ENABLE_PROFIT_PROTECTION:
            protect_trigger = Decimal(str(entry_price)) * (1 - Decimal(str(PROTECT_PROFIT_PER)) / 100)
            raw_protected_stop = float(Decimal(str(entry_price)) * (1 - Decimal(str(PROTECT_LOSS_PER)) / 100))
            protected_stop = ceil_price_to_tick(raw_protected_stop, get_profit_trail_tick_size(raw_protected_stop))
        settlement_dt = entry_dt.replace(hour=intraday_compare_end[0],
                                         minute=intraday_compare_end[1], second=0, microsecond=0)
        # 入場棒 open 已成交，該棒即須檢查固定停損。
        for i in range(entry_idx, len(dt_values)):
            bar_time = dt_values[i]
            bar_open = float(open_values[i])
            # 僅使用前一根結束時已生效的出場線，較低反彈價位優先。
            exit_price = stop_loss_price
            exit_reason = 'protect_profit' if protection_active else 'stop'
            if profit_tracking_active and trailing_stop_price < exit_price:
                exit_price, exit_reason = trailing_stop_price, 'trailing_profit'
            # 強制平倉先於該分鐘內的 high/low；缺棒則在其後第一根 open 平倉。
            if bar_time >= settlement_dt:
                reason = 'close' if bar_time == settlement_dt else 'close_delayed'
                result = build_outcome_result(reason, entry_price, bar_open)
            elif bar_open >= exit_price:
                fill_price = (apply_stop_slippage(bar_open, limit_up_price)
                              if exit_reason == 'stop' else bar_open)
                result = build_outcome_result(exit_reason + '_gap', entry_price, fill_price)
            elif high_values[i] >= exit_price:
                fill_price = (apply_stop_slippage(exit_price, limit_up_price)
                              if exit_reason == 'stop' else exit_price)
                result = build_outcome_result(exit_reason, entry_price, fill_price)
            if result is not None:
                result['exit_dt'] = bar_time
                break
            # 本根未出場才以low啟動／推進，新出場線下一根生效。
            low = float(low_values[i])
            if (ENABLE_PROFIT_PROTECTION and not protection_active and Decimal(str(low)) <= protect_trigger
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
        all_results.append((signal, result))

    return all_results


def count_testable_stock_dates(bars_by_date, target_date):
    """有前日資料、通過早盤根數篩選且搜尋時段有有效分K的股票－日期數；不套用策略條件。"""
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


def collect_trade_candidates(stock_name, bars_by_date, target_date, context):
    """每檔每日只呼叫一次 find_signal，最多產生一筆候選交易。"""
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
        if get_signal_window(today) is None:
            continue
        previous = compute_yesterday_stats(yesterday)
        previous_close = previous['close']
        limit_up, limit_down = calculate_limit_prices(previous_close)
        session_bars = get_session_bars(today)
        found = find_signal(session_bars, {
            'stock_name': stock_name,
            'date_key': date_key,
            'previous_close': previous_close,
            'yesterday': previous,
            'limit_up': limit_up,
            'limit_down': limit_down,
            'bars_by_date': bars_by_date,
            'context': context,
        })
        if found is None:
            continue
        signal_index, strategy_fields = found
        entry = resolve_entry(session_bars, signal_index)
        if entry is None:
            continue
        entry_bar, entry_price = entry
        candidate = build_trade_candidate(
            stock_name, current_date, entry_bar, entry_price, today,
            limit_up, limit_down, STRATEGY_LOWER, INTRADAY_COMPARE_END_LOWER,
        )
        candidate['previous_close'] = previous_close
        candidate['signal_dt'] = session_bars[int(signal_index)]['dt']
        candidate['strategy_fields'] = dict(strategy_fields or {})
        candidates.append(candidate)
    return candidates


# ---------------------------------------------------------------- cache
def get_api_cache_path(target_date):
    """各版本共用原始 API cache；僅補齊股票資料。"""
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
    """各版本共用檔名；補資料時保留原有股票、指數與其他欄位。"""
    updated = dict(payload) if isinstance(payload, dict) else {}
    updated['stock_names'] = list(dict.fromkeys([*updated.get('stock_names', []), *raw]))
    updated['minute_raw_by_symbol'] = raw
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(updated, ensure_ascii=False), encoding='utf-8')


# ---------------------------------------------------------------- 設定檢核與輸出
def validate_settings():
    if type(ENABLE_PROFIT_PROTECTION) is not bool:
        raise ValueError('ENABLE_PROFIT_PROTECTION 必須為布林值')
    protection_values = (PROTECT_PROFIT_PER, PROTECT_LOSS_PER) if ENABLE_PROFIT_PROTECTION else ()
    for value in (*protection_values, PROFIT_BACK_PERCENT, PROFIT_TARGET_PERCENT):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('獲利保護及追蹤停利百分比必須介於 0 與 100 之間')
    if ENABLE_PROFIT_PROTECTION and PROTECT_LOSS_PER >= PROTECT_PROFIT_PER:
        raise ValueError('保護毛利百分比必須小於獲利保護啟動百分比')
    for name, value in (('MIN_MINUTE_BARS_BEFORE_0930', MIN_MINUTE_BARS_BEFORE_0930),
                        ('API_FETCH_DAYS', API_FETCH_DAYS)):
        if type(value) is not int or value <= 0:
            raise ValueError(f'{name} 必須為正整數')
    if type(STOP_SLIPPAGE_TICKS) is not int or STOP_SLIPPAGE_TICKS < 0:
        raise ValueError('STOP_SLIPPAGE_TICKS 必須為非負整數')
    for name, value in (('API_REQUEST_DELAY_SEC', API_REQUEST_DELAY_SEC),
                        ('BROKERAGE_FEE_RATE', BROKERAGE_FEE_RATE),
                        ('SELL_TRANSACTION_TAX_RATE', SELL_TRANSACTION_TAX_RATE)):
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
        raise ValueError('需滿足 09:00 <= 訊號起點 <= 入場截止 < 平倉 <= 13:30')
    for value in (OPTIMIZE_LOSS_PER_LOWER, OPTIMIZE_PROFIT_PER_LOWER):
        if not math.isfinite(value) or not 0 < value < 100:
            raise ValueError('停損及停利百分比必須介於 0 與 100 之間')
    validate_strategy_settings()


def format_result_line(stock_name, date_str, signal, result):
    display_name = re.sub(r'\.TW(?:O)?$', '', stock_name)
    strategy_text = format_strategy_text(signal)
    prefix = (
        f'{date_str} {display_name} ' + (f'{strategy_text} ' if strategy_text else '')
        + f'| {signal["entry_dt"]:%H:%M} {signal["entry_price"]:.2f} -> '
    )
    if result['outcome'] == 'incomplete':
        return prefix + 'data_insufficient 資料不足，未計入已完成交易績效'
    return (prefix + f'{result["exit_dt"]:%H:%M} {result["exit_price"]:.2f} '
            f'{result["exit_reason"]} | 每股淨損益={result["signed_pnl"]:+.4f}')


def summarize_results(label, pairs):
    completed = [result for _, result in pairs if result['outcome'] != 'incomplete']
    incomplete = len(pairs) - len(completed)
    return (f'{label} 已完成交易數={len(completed)} 資料不足筆數={incomplete} '
            f'獲利筆數={sum(r["is_success"] for r in completed)} '
            f'每筆一股淨損益合計={sum(r["signed_pnl"] for r in completed):+.4f}')


def common_header_lines():
    """結果檔表頭的共用規則說明。"""
    def hhmm(value):
        return f'{value[0]:02d}:{value[1]:02d}'

    return [
        f'訊號起點={hhmm(SIGNAL_START_TIME)} 最後入場={hhmm(ENTRY_DEADLINE)}（含） '
        f'強制平倉={hhmm(INTRADAY_COMPARE_END_LOWER)} open',
        f'早盤篩選：09:00～09:29 至少 {MIN_MINUTE_BARS_BEFORE_0930} 根不重複的整分鐘分K，'
        '不足則排除當日交易；於當日分析前先判斷。',
        '進場：訊號棒的下一根有效分K open 放空，每檔每日最多一筆。'
        '09:00起至入場前任一有效分K high曾達漲停或low曾達跌停則當日不交易；'
        '入場棒僅檢查open是否達漲跌停，不使用該棒high/low。入場價加停損價差達漲停價則不交易。',
        f'停損={OPTIMIZE_LOSS_PER_LOWER:g}%（向上取tick，出場加計{STOP_SLIPPAGE_TICKS}個tick滑價，不超過漲停） '
        f'追蹤停利啟動={OPTIMIZE_PROFIT_PER_LOWER:g}%（向下取tick，達標不直接平倉）；'
        + (f'獲利保護啟用（浮盈{PROTECT_PROFIT_PER:g}%後保住{PROTECT_LOSS_PER:g}%毛利），'
           if ENABLE_PROFIT_PROTECTION else '獲利保護關閉，')
        + f'追蹤回撤={PROFIT_BACK_PERCENT:g}% 下次目標步幅={PROFIT_TARGET_PERCENT:g}%；'
        '先檢查既有出場線，本根low更新的出場線下一根生效；跳空以open出場，僅固定停損加計滑價。',
        '強制平倉缺棒順延至其後第一根open（close_delayed）；其後完全無分K記為data_insufficient，不計入已完成績效。',
        '昨收為前一有資料交易日最後一根close；重複、非整分鐘或順序異常分K排除；average採原始欄位，無效不補算。',
        '淨損益扣除既定手續費及交易稅，停損含上述滑價，未含借券費；每筆一股合計非投資組合報酬率。',
    ]


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

        # cache 內沒有分K（含既有的空清單）者視為未抓過，每次執行都重抓。
        missing = [name for name in stock_names
                   if not isinstance(raw.get(name), list) or not raw[name]]
        if missing:
            fetched = {}
            for i, name in enumerate(missing, 1):
                print(f'API抓取進度：{i}/{len(missing)} {name}')
                data = fetch_minute_candles(get_rest_stock(), extract_symbol(name), target_date)
                if data:  # 抓取失敗或無資料不寫入 cache，下次執行再抓
                    fetched[name] = data
            if fetched:
                raw = {**raw, **fetched}
                save_shared_api_cache(cache_path, payload, raw)
            skipped = [name for name in missing if name not in fetched]
            if skipped:
                print(f'[WARN] {len(skipped)} 檔抓取失敗或無資料，本次略過且未寫入 cache：'
                      f'{"、".join(skipped)}', file=sys.stderr)
        # 本次無分K的股票不參與回測。
        stock_names = [name for name in stock_names if isinstance(raw.get(name), list)]

    lines = [f'{STRATEGY_LOWER} 放空回測，目標日期={target_date}',
             *strategy_header_lines(),
             *common_header_lines()]
    results = []
    testable_count = 0
    # 先備齊清單內全部股票的分K（跨股票資料需要），再逐檔分析。
    bars_by_stock = {name: parse_bars(raw[name]) for name in stock_names}
    context = build_market_context(bars_by_stock, target_date)
    for name in stock_names:
        bars = bars_by_stock[name]
        if HISTORICAL_CACHE_DATE:
            testable_count += count_testable_stock_dates(bars, target_date)
        results.extend(evaluate_candidates(
            collect_trade_candidates(name, bars, target_date, context)))
    if HISTORICAL_CACHE_DATE:
        lines.insert(0, f'歷史 cache 模式：{target_date}，使用 cache 全部股票，可檢驗股票－日期數={testable_count:,}')
    results.sort(key=lambda pair: (pair[0]['entry_dt'], pair[0]['name']))
    for signal, result in results:
        lines.append(format_result_line(signal['name'], signal['date_str'], signal, result))
    for day in sorted({signal['date_str'] for signal, _ in results}):
        lines.append(summarize_results(day, [pair for pair in results if pair[0]['date_str'] == day]))
    monthly_pnl = {}
    monthly_trade_counts = {}
    for signal, result in results:
        if result['outcome'] == 'incomplete':
            continue
        month = signal['date_str'][:7]
        monthly_pnl[month] = monthly_pnl.get(month, 0.0) + result['signed_pnl']
        monthly_trade_counts[month] = monthly_trade_counts.get(month, 0) + 1
    for month, pnl in sorted(monthly_pnl.items()):
        lines.append(f'{month} 已完成交易數={monthly_trade_counts[month]} 每月每筆一股淨損益合計={pnl:+.4f}')
    lines.append(summarize_results('總計', results))
    output = '\n'.join(lines) + '\n'
    EACH_STOCK_OUTPUT_FILE.write_text(output, encoding='utf-8')
    print(output, end='')


if __name__ == '__main__':
    main()
