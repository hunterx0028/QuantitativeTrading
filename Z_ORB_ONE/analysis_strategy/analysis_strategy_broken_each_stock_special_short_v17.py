import builtins
import argparse
import configparser
import json
import re
import sys
import time
import numpy as np
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from esun_marketdata import EsunMarketdata
from Z_ORB_ONE.stock_data import market_previous_close_indices, selected_stocks

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
EACH_STOCK_OUTPUT_FILE = Path(__file__).with_name('analysis_strategy_broken_each_stock_special_short_v17_result.txt')
OUTPUT_BUFFER: list[str] = []
ENTRY_BLOCKED = 'ENTRY_BLOCKED'
GATE_LOWER_PASSED = 'LOWER_PASSED'
GATE_REBOUND_BLOCKED = 'REBOUND_BLOCKED'
GATE_NOT_PASSED = 'NOT_PASSED'
STRATEGY_CHANCE = 'CHANCE'
STRATEGY_LOWER = 'LOWER'
STRATEGY_DELAY = 'DELAY'

# ---------------------------------------------------------------------------
# IDE 直接執行時可在此調整策略參數, 此版本不會跳過前一日非營業日的狀況
# ---------------------------------------------------------------------------

OPTIMIZE_LOSS_PER_CHANCE = 2.5 # v17 停損百分比(%)，例如 3.0 代表入場價加上 3%
OPTIMIZE_PROFIT_PER_CHANCE = 3.5 # v17 停利百分比(%)，例如 5.0 代表入場價減去 5%

OPTIMIZE_LOSS_PER_LOWER = 2.0 # lower 停損百分比(%)，例如 3.0 代表入場價加上 3%
OPTIMIZE_PROFIT_PER_LOWER = 6.0 # lower 停利百分比(%)，例如 5.0 代表入場價減去 5%

OPTIMIZE_LOSS_PER_DELAY = 2.5 # delay 停損百分比(%)，預設沿用 chance
OPTIMIZE_PROFIT_PER_DELAY = 3.5 # delay 停利百分比(%)，預設沿用 chance

STRATEGY_DECISION = (9, 41) # 市場模式判斷截止分K棒的(時, 分)，不包含此時間

STRATEGY_START_CHANCE = (9, 41) # chance 個股進場開始分K棒的(時, 分)，包含此時間
STRATEGY_START_LOWER = (9, 46) # lower 個股進場開始分K棒的(時, 分)，包含此時間
STRATEGY_START_DELAY = (9, 51) # delay 個股進場開始分K棒的(時, 分)，包含此時間

STRATEGY_END_CHANCE = (10, 1) # v17 策略結束分k棒的(時, 分)
STRATEGY_END_LOWER = (10, 1) # lower 策略可進場截止分k棒的(時, 分)，包含此時間
STRATEGY_END_DELAY = (10, 1) # delay 策略可進場截止分k棒的(時, 分)，預設沿用 chance

PRE_STRATEGY_LOW_BUFFER_END_CHANCE = (9, 5) # v17 策略開始前昨低緩衝截止分k棒的(時, 分)，含此時間

INTRADAY_COMPARE_END_CHANCE = (13, 0)  # v17 盤中停損/停利比對截止(時, 分)
INTRADAY_COMPARE_END_LOWER = (13, 0)  # lower 盤中停損/停利比對截止(時, 分)
INTRADAY_COMPARE_END_DELAY = (13, 0)  # delay 盤中停損/停利比對截止(時, 分)，預設沿用 chance

MAX_INTRADAY_RANGE_BEFORE_TRIGGER_PER = 6.0 # 觸發棒前當日高低價差上限(%)，以上一日最低價為基準

PREV_LOW_BARS_REQUIRED_CHANCE = 5 # v17 跌破昨低前，需連續幾根分K low >= 昨低

IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.2 # IX0001 啟動門檻：STRATEGY_DECISION 前最後 low 需低於前日最後 close 的百分比
IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.6 # IX0001 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內

IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.0 # IX0043 啟動門檻：STRATEGY_DECISION 前最後 low 需低於前日最後 close 的百分比
IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.5 # IX0043 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內

ENABLE_CHANCE = True # 在回測版開啟，在實測版關閉

BROKERAGE_FEE_RATE = 0.001425 # 台股手續費率，買賣雙邊皆收
SELL_TRANSACTION_TAX_RATE = 0.003 # 台股交易稅率，賣出時收

# 產業盤勢過濾：原策略入場條件成立後，產業指數當下 high 必須嚴格小於前一營業日最後一根分K close。
INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT = 0 # 百分比表示方式，正值代表要高於昨收指數，負值代表要低於昨收指數
RESERVE_MARKET_INDICES = {
    'TWSE:MARKET': {
        'exchange': 'TWSE',
        'industry_code': None,
        'industry_name': '上市',
        'symbol': 'IX0001',
        'name': '發行量加權股價指數',
        'source': 'historical.candles',
    },
    'TPEX:MARKET': {
        'exchange': 'TPEX',
        'industry_code': None,
        'industry_name': '上櫃',
        'symbol': 'IX0043',
        'name': '櫃買指數',
        'source': 'historical.candles',
    },
}
MARKET_INDEX_METADATA = {**market_previous_close_indices, **RESERVE_MARKET_INDICES}

# 額外API的配置
API_REQUEST_DELAY_SEC = 1 # 每次 API 查詢前的延遲

def get_api_cache_path(target_date: date) -> Path:
    """回傳 API 快取檔路徑（json_cache 資料夾）。"""
    return Path(__file__).resolve().parent / 'analysis_json_cache' / f'analysis_strategy_broken_each_stock_special_short_api_cache_{target_date:%Y%m%d}.json'


def load_api_cache(cache_path: Path, stock_list: list[tuple]) -> tuple[dict[str, list], dict[str, dict[str, list]], dict[str, dict[str, list]]] | None:
    """載入 API 快取；若不存在或格式不符則回傳 None。"""
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding='utf-8'))
        cached_names = payload.get('stock_names', [])
        current_names = [item[0] for item in stock_list]
        if cached_names != current_names:
            return None
        day_candles_by_symbol = payload.get('day_candles_by_symbol', {})
        raw_minute_by_symbol = payload.get('minute_raw_by_symbol', {})
        raw_index_minute_by_key = payload.get('index_minute_raw_by_key', {})
        required_index_keys = set(RESERVE_MARKET_INDICES) | get_required_industry_index_keys(stock_list)
        if any(
            key not in raw_index_minute_by_key
            for key in required_index_keys
        ):
            return None
        minute_bars_by_symbol = {
            stock_name: parse_bars(raw_minute_by_symbol.get(stock_name, []))
            for stock_name in current_names
        }
        index_minute_bars_by_key = {
            index_key: parse_bars(raw_index_minute_by_key.get(index_key, []))
            for index_key in required_index_keys
        }
        return day_candles_by_symbol, minute_bars_by_symbol, index_minute_bars_by_key
    except Exception:
        return None


def save_api_cache(
    cache_path: Path,
    stock_list: list[tuple],
    day_candles_by_symbol: dict[str, list],
    minute_raw_by_symbol: dict[str, list],
    index_minute_raw_by_key: dict[str, list],
) -> None:
    """儲存 API 快取。"""
    payload = {
        'stock_names': [item[0] for item in stock_list],
        'day_candles_by_symbol': day_candles_by_symbol,
        'minute_raw_by_symbol': minute_raw_by_symbol,
        'index_minute_raw_by_key': index_minute_raw_by_key,
    }
    cache_path.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding='utf-8',
    )


def flush_each_stock_output_file() -> None:
    """在流程結束後一次性覆蓋寫入 each-stock 結果檔。"""
    EACH_STOCK_OUTPUT_FILE.write_text(''.join(OUTPUT_BUFFER), encoding='utf-8')


def print(*args, **kwargs):
    file_obj = kwargs.get('file', sys.stdout)
    sep = kwargs.get('sep', ' ')
    end = kwargs.get('end', '\n')
    if sep is None:
        sep = ' '
    if end is None:
        end = '\n'

    builtins.print(*args, **kwargs)
    if file_obj in (None, sys.stdout, sys.stderr):
        text = sep.join(str(arg) for arg in args) + end
        OUTPUT_BUFFER.append(text)


def print_progress(current: int, total: int, stock_name: str) -> None:
    """僅輸出到終端機的進度列，不寫入結果檔。"""
    builtins.print(f'\r策略運算進度: {current}/{total} - {stock_name}', end='', flush=True)


def print_api_progress(current: int, total: int, stock_name: str) -> None:
    """僅輸出 API 抓取進度，不寫入結果檔。"""
    builtins.print(f'\rAPI抓取進度: {current}/{total} - {stock_name}', end='', flush=True)


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


def calculate_stop_loss_price(entry_price: float, stop_loss: float, limit_up_price: float) -> float:
    """計算停損價：停損上限使用（漲停價 - 1 tick，tick 依進場價位）。"""
    # tick = get_tick_size(entry_price)
    # adjusted_limit_up_price = max(limit_up_price - tick, 0.0)
    # return min(entry_price + stop_loss, adjusted_limit_up_price)

    return min(entry_price + stop_loss, limit_up_price)


def calculate_stop_loss_amount_by_percent(entry_price: float, stop_loss_percent: float) -> float:
    """依入場價與停損百分比計算停損價差。"""
    return entry_price * (stop_loss_percent / 100.0)


def calculate_take_profit_amount_by_percent(entry_price: float, take_profit_percent: float) -> float:
    """依入場價與停利百分比計算停利價差。"""
    return entry_price * (take_profit_percent / 100.0)


def should_skip_entry_by_limit_down_zone(
    entry_price: float,
    true_yesterday_low: float,
    limit_down_price: float,
) -> bool:
    """進場價若低於昨低到跌停三分之一位置，略過本次進場。"""
    threshold = true_yesterday_low - ((true_yesterday_low - limit_down_price) / 3.0)
    return entry_price <= threshold

# ---------------------------------------------------------------------------
# 股票清單 (tuple 格式：第一個元素為「名稱:代碼.TW」)
# ---------------------------------------------------------------------------
STOCK_LIST = [

]


# ---------------------------------------------------------------------------
# Phase 1 / Phase 5 — CLI 與設定檔
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='ORB 作空策略掃描程式')
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


def load_config(config_path: str) -> configparser.ConfigParser:
    config_file = Path(config_path).resolve()
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    normalize_config_paths(config, config_file)
    return config


# ---------------------------------------------------------------------------
# Phase 2 — SDK 連線與資料擷取
# ---------------------------------------------------------------------------

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


def get_exchange_for_stock(stock_name: str) -> str | None:
    """依股票代碼後綴判斷上市(TWSE)或上櫃(TPEX)。"""
    symbol_text = stock_name.split(':', 1)[-1].upper()
    if symbol_text.endswith('.TWO'):
        return 'TPEX'
    if symbol_text.endswith('.TW'):
        return 'TWSE'
    return None


def get_industry_index_key(stock_item: tuple) -> str | None:
    """回傳 market_previous_close_indices 的 key，例如 TWSE:26。"""
    if len(stock_item) <= 6:
        return None
    stock_name = stock_item[0]
    exchange = get_exchange_for_stock(stock_name)
    if exchange is None:
        return None
    industry_code = str(stock_item[6]).zfill(2)
    index_key = f'{exchange}:{industry_code}'
    index_meta = market_previous_close_indices.get(index_key)
    if not index_meta or not index_meta.get('symbol'):
        return None
    return index_key


def get_required_industry_index_keys(stock_list: list[tuple]) -> set[str]:
    """取得本次股票清單會用到的產業指數 key。"""
    required_keys: set[str] = set()
    for stock_item in stock_list:
        index_key = get_industry_index_key(stock_item)
        if index_key is not None:
            required_keys.add(index_key)
    return required_keys


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


def fetch_index_minute_candles(rest_stock, index_key: str, target_date: date) -> list:
    """呼叫 SDK 取得產業指數 1-minute K棒，回傳 data 陣列（最新在前）。"""
    index_meta = MARKET_INDEX_METADATA.get(index_key, {})
    symbol = index_meta.get('symbol')
    if not symbol:
        print(f'[ERROR] 找不到產業指數代碼: {index_key}', file=sys.stderr)
        return []
    return fetch_minute_candles(rest_stock, symbol, target_date)


def fetch_day_candles(stock_item: tuple, target_date: date, rest_stock) -> list:
    """呼叫 SDK 取得日 K 棒，回傳 data 陣列（最新在前）。"""
    stock_name = stock_item[0]
    symbol = extract_symbol(stock_name)
    from_date = target_date - timedelta(days=40)
    from_str = from_date.strftime('%Y-%m-%d')
    to_str = target_date.strftime('%Y-%m-%d')
    try:
        time.sleep(API_REQUEST_DELAY_SEC)
        response = rest_stock.historical.candles(
            **{'symbol': symbol, 'from': from_str, 'to': to_str}
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
    for item in raw_data:
        date_raw = item['date'][:19]  # '2026-04-02T09:00:00'
        dt = datetime.strptime(date_raw, '%Y-%m-%dT%H:%M:%S')
        date_key = dt.strftime('%Y-%m-%d')
        bar = {
            'dt': dt,
            'open': float(item['open']),
            'high': float(item['high']),
            'low': float(item['low']),
            'close': float(item['close']),
            'volume': float(item.get('volume', 0) or 0),
            'average': float(item['average']) if item.get('average') is not None else (
                float(item['turnover']) / float(item['volume'])
                if item.get('turnover') is not None and float(item.get('volume', 0)) > 0
                else None
            ),
        }
        bars_by_date.setdefault(date_key, []).append(bar)

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


def find_bar_at_or_before(bars: list, target_dt: datetime) -> dict | None:
    """找出 target_dt 同時間或之前最近一根K棒。"""
    matched_bar = None
    for bar in bars:
        dtv = bar.get('dt')
        if dtv is None:
            continue
        if dtv <= target_dt:
            matched_bar = bar
        else:
            break
    return matched_bar


def get_previous_trading_day_last_close(bars_by_date: dict, target_date: date) -> float | None:
    """取 target_date 前最近一個有資料日的最後一根分K close。"""
    target_key = target_date.strftime('%Y-%m-%d')
    previous_dates = sorted(k for k in bars_by_date if k < target_key)
    if not previous_dates:
        return None
    yesterday_bars = bars_by_date.get(previous_dates[-1], [])
    if not yesterday_bars:
        return None
    return float(yesterday_bars[-1]['close'])


def get_previous_trading_day_low(bars_by_date: dict, target_date: date) -> float | None:
    """取 target_date 前最近一個有資料日的最低 low。"""
    target_key = target_date.strftime('%Y-%m-%d')
    previous_dates = sorted(k for k in bars_by_date if k < target_key)
    if not previous_dates:
        return None
    yesterday_bars = bars_by_date.get(previous_dates[-1], [])
    if not yesterday_bars:
        return None
    return min(float(bar['low']) for bar in yesterday_bars)


def find_strategy_decision_threshold_break(
    bars_by_date: dict,
    target_date: date,
    threshold: float,
) -> tuple[float, datetime] | None:
    """找出 STRATEGY_DECISION 前第一個 low 跌破 threshold 的分K。"""
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    valid_bars = [
        bar
        for bar in today_bars
        if bar.get('dt') is not None and bar.get('low') is not None
        and (bar['dt'].hour * 60 + bar['dt'].minute) < decision_hm
    ]
    if not valid_bars:
        return None
    for bar in sorted(valid_bars, key=lambda item: item['dt']):
        bar_low = float(bar['low'])
        if bar_low < threshold:
            return bar_low, bar['dt']
    return None


def find_strategy_decision_rebound_to_threshold(
    bars_by_date: dict,
    target_date: date,
    break_dt: datetime,
    rebound_threshold: float,
) -> tuple[float, datetime] | None:
    """找出跌破後下一根起到 STRATEGY_DECISION 前第一個 high >= 反彈門檻的分K。"""
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    valid_bars = [
        bar
        for bar in today_bars
        if bar.get('dt') is not None and bar.get('high') is not None
        and break_dt < bar['dt']
        and (bar['dt'].hour * 60 + bar['dt'].minute) < decision_hm
    ]
    for bar in sorted(valid_bars, key=lambda item: item['dt']):
        bar_high = float(bar['high'])
        if bar_high >= rebound_threshold:
            return bar_high, bar['dt']
    return None


def build_market_index_daily_low_summary(
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> list[str]:
    """建立每日彙總用的 IX0001/IX0043 前日低點與啟動低點文字。"""
    rows = []
    for index_key, label in (
        ('TWSE:MARKET', 'IX0001'),
        ('TPEX:MARKET', 'IX0043'),
    ):
        bars_by_date = index_minute_bars_by_key.get(index_key, {})
        previous_low = get_previous_trading_day_low(bars_by_date, target_date)
        previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
        drop_percent = get_strategy_decision_drop_percent(index_key)
        rebound_percent = get_strategy_decision_rebound_percent(index_key)
        previous_low_text = f'{previous_low:.2f}' if previous_low is not None else 'N/A'
        previous_close_text = f'{previous_close:.2f}' if previous_close is not None else 'N/A'
        if previous_close is None or drop_percent is None or rebound_percent is None:
            threshold_text = 'N/A'
            break_low_text = 'N/A'
            break_percent_text = 'N/A'
            break_time_text = '--:--:--'
            rebound_threshold_text = 'N/A'
            rebound_high_text = 'N/A'
            rebound_percent_text = 'N/A'
            rebound_time_text = '--:--:--'
        else:
            threshold = previous_close * (1 - drop_percent / 100.0)
            rebound_threshold = previous_close * (1 - rebound_percent / 100.0)
            threshold_text = f'{threshold:.2f}'
            rebound_threshold_text = f'{rebound_threshold:.2f}'
            break_low = find_strategy_decision_threshold_break(
                bars_by_date,
                target_date,
                threshold,
            )
            if break_low is None:
                break_low_text = 'N/A'
                break_percent_text = 'N/A'
                break_time_text = '--:--:--'
                rebound_high_text = 'N/A'
                rebound_percent_text = 'N/A'
                rebound_time_text = '--:--:--'
            else:
                break_low_value, break_low_dt = break_low
                break_percent = ((previous_close - break_low_value) / previous_close) * 100.0
                break_low_text = f'{break_low_value:.2f}'
                break_percent_text = f'{break_percent:.2f}%'
                break_time_text = break_low_dt.strftime('%H:%M:%S')
                rebound = find_strategy_decision_rebound_to_threshold(
                    bars_by_date,
                    target_date,
                    break_low_dt,
                    rebound_threshold,
                )
                if rebound is None:
                    rebound_high_text = 'N/A'
                    rebound_percent_text = 'N/A'
                    rebound_time_text = '--:--:--'
                else:
                    rebound_high, rebound_dt = rebound
                    rebound_percent_value = ((rebound_high - break_low_value) / break_low_value) * 100.0
                    rebound_high_text = f'{rebound_high:.2f}'
                    rebound_percent_text = f'{rebound_percent_value:.2f}%'
                    rebound_time_text = rebound_dt.strftime('%H:%M:%S')
        rows.append(
            f'{label} 前日昨低={previous_low_text}  '
            f'昨收={previous_close_text}  '
            f'門檻={threshold_text}  '
            f'跌破low={break_low_text}  '
            f'跌幅={break_percent_text}  '
            f'時間={break_time_text}  '
            f'反彈門檻={rebound_threshold_text}  '
            f'反彈high={rebound_high_text}  '
            f'反彈漲幅={rebound_percent_text}  '
            f'反彈時間={rebound_time_text}'
        )
    return rows


def has_strategy_market_rebound_block(
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> bool:
    """回傳市場 gate 是否因跌破後反彈到反彈門檻而被擋下。"""
    for index_key in ('TWSE:MARKET', 'TPEX:MARKET'):
        bars_by_date = index_minute_bars_by_key.get(index_key, {})
        previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
        drop_percent = get_strategy_decision_drop_percent(index_key)
        rebound_percent = get_strategy_decision_rebound_percent(index_key)
        if previous_close is None or drop_percent is None or rebound_percent is None:
            continue

        threshold = previous_close * (1 - drop_percent / 100.0)
        threshold_break = find_strategy_decision_threshold_break(
            bars_by_date,
            target_date,
            threshold,
        )
        if threshold_break is None:
            continue

        _, break_dt = threshold_break
        rebound_threshold = previous_close * (1 - rebound_percent / 100.0)
        if find_strategy_decision_rebound_to_threshold(
            bars_by_date,
            target_date,
            break_dt,
            rebound_threshold,
        ) is not None:
            return True
    return False


def get_strategy_decision_drop_percent(index_key: str) -> float | None:
    """回傳大盤啟動 gate 對應指數的跌幅百分比門檻。"""
    if index_key == 'TWSE:MARKET':
        return IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER
    if index_key == 'TPEX:MARKET':
        return IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER
    return None


def get_strategy_decision_rebound_percent(index_key: str) -> float | None:
    """回傳大盤啟動 gate 對應指數的反彈百分比門檻。"""
    if index_key == 'TWSE:MARKET':
        return IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER
    if index_key == 'TPEX:MARKET':
        return IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER
    return None


def get_market_index_strategy_decision_gate_status(
    index_key: str,
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> str:
    """回傳單一市場指數的策略啟動 gate 狀態。"""
    drop_percent = get_strategy_decision_drop_percent(index_key)
    rebound_percent = get_strategy_decision_rebound_percent(index_key)
    if drop_percent is None or rebound_percent is None:
        return GATE_NOT_PASSED

    bars_by_date = index_minute_bars_by_key.get(index_key, {})
    if not bars_by_date:
        return GATE_NOT_PASSED

    previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
    if previous_close is None:
        return GATE_NOT_PASSED

    threshold = previous_close * (1 - drop_percent / 100.0)
    rebound_threshold = previous_close * (1 - rebound_percent / 100.0)
    threshold_break = find_strategy_decision_threshold_break(
        bars_by_date,
        target_date,
        threshold,
    )
    if threshold_break is None:
        return GATE_NOT_PASSED

    _, break_dt = threshold_break
    if find_strategy_decision_rebound_to_threshold(
        bars_by_date,
        target_date,
        break_dt,
        rebound_threshold,
    ) is not None:
        return GATE_REBOUND_BLOCKED
    return GATE_LOWER_PASSED


def get_strategy_market_decision_gate_status(
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> str:
    """全體策略決策 gate：兩指數都 lower 才 lower，都未跌破才 chance，其餘 delay。"""
    statuses = [
        get_market_index_strategy_decision_gate_status(
            index_key,
            target_date,
            index_minute_bars_by_key,
        )
        for index_key in ('TWSE:MARKET', 'TPEX:MARKET')
    ]
    if all(status == GATE_LOWER_PASSED for status in statuses):
        return GATE_LOWER_PASSED
    if all(status == GATE_NOT_PASSED for status in statuses):
        return GATE_NOT_PASSED
    return GATE_REBOUND_BLOCKED


def is_industry_market_filter_passed(
    stock_item: tuple,
    target_date: date,
    entry_dt: datetime,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> bool:
    """產業指數當下 high 必須嚴格小於前一營業日最後一根分K close。"""
    index_key = get_industry_index_key(stock_item)
    if index_key is None:
        return False

    index_bars_by_date = index_minute_bars_by_key.get(index_key, {})
    if not index_bars_by_date:
        return False

    previous_close = get_previous_trading_day_last_close(index_bars_by_date, target_date)
    if previous_close is None:
        return False

    today_key = target_date.strftime('%Y-%m-%d')
    today_index_bars = index_bars_by_date.get(today_key, [])
    if not today_index_bars:
        return False

    entry_index_bar = find_bar_at_or_before(today_index_bars, entry_dt)
    if entry_index_bar is None:
        return False

    threshold = previous_close * (1 + INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT / 100.0)
    return float(entry_index_bar['high']) < threshold


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


def build_outcome_result(exit_reason: str, entry_price: float, exit_price: float) -> dict:
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


# ---------------------------------------------------------------------------
# Phase 3 (US1) — 進場條件判斷
# ---------------------------------------------------------------------------

def find_first_bar(today_bars: list):
    """找當日最早時間的第一根K棒；不存在回傳 (None, -1)。"""
    if not today_bars:
        return None, -1
    first_idx = -1
    first_bar = None
    first_dt = None
    for idx, bar in enumerate(today_bars):
        dtv = bar.get('dt')
        if dtv is None:
            continue
        if first_dt is None or dtv < first_dt:
            first_dt = dtv
            first_idx = idx
            first_bar = bar
    if first_bar is None:
        return None, -1
    return first_bar, first_idx


def is_one_minute_bars(today_bars: list) -> bool:
    """以第 2~4 根K棒判斷是否為 1 分K；若像 5 分K則排除。"""
    dt_values = [
        bar.get('dt')
        for bar in today_bars
        if bar.get('dt') is not None
    ]
    if len(dt_values) < 4:
        return False
    dt_values.sort()
    second_to_third = dt_values[2] - dt_values[1]
    third_to_fourth = dt_values[3] - dt_values[2]
    return not (
        second_to_third >= timedelta(minutes=5)
        and third_to_fourth >= timedelta(minutes=5)
    )


def scan_entry_signal_lower(
    today_bars: list,
    ystats: dict,
):
    """
    作空進場訊號：
    1) 取前一營業日最低價作為 trigger_low
    2) 進場檢查時間為 STRATEGY_START_LOWER 到 STRATEGY_END_LOWER（含起訖）
    3) 若任一根K棒 low < trigger_low，則可進場
    4) 進場價 = trigger_low - 1 tick
    5) 若進場價 <= 昨低 - (昨低 - 跌停價) / 3，則不進場
    回傳：
    - (entry_bar, entry_price): 條件成立
    - ENTRY_BLOCKED: 首次跌破但檢核失敗（當日封單）
    - None: 未形成 trigger_low 或未出現進場跌破
    """
    start_hm = STRATEGY_START_LOWER[0] * 60 + STRATEGY_START_LOWER[1]
    end_hm = STRATEGY_END_LOWER[0] * 60 + STRATEGY_END_LOWER[1]
    true_yesterday_low = float(ystats['low'])
    max_intraday_range_before_trigger = true_yesterday_low * (MAX_INTRADAY_RANGE_BEFORE_TRIGGER_PER / 100.0)
    _, limit_down_price = calculate_limit_prices(float(ystats['close']))

    indexed_bars = []
    for idx, bar in enumerate(today_bars):
        dtv = bar.get('dt')
        if dtv is None:
            continue
        hm = dtv.hour * 60 + dtv.minute
        indexed_bars.append((idx, bar, hm))
    indexed_bars.sort(key=lambda item: item[1]['dt'])

    if len(indexed_bars) < 2:
        return None

    trigger_low = true_yesterday_low

    for original_idx, bar, hm in indexed_bars:
        bar_low = float(bar.get('low', 0) or 0.0)

        if not (start_hm <= hm <= end_hm):
            continue

        if bar_low < trigger_low:
            entry_tick = get_tick_size(trigger_low)
            entry_price = max(trigger_low - entry_tick, 0.0)
            if should_skip_entry_by_limit_down_zone(
                entry_price,
                true_yesterday_low,
                limit_down_price,
            ):
                return ENTRY_BLOCKED

            prior_bars = today_bars[:original_idx]
            if prior_bars:
                prior_day_high = max(float(item['high']) for item in prior_bars)
                prior_day_low = min(float(item['low']) for item in prior_bars)
                if (prior_day_high - prior_day_low) > max_intraday_range_before_trigger:
                    return ENTRY_BLOCKED

            return bar, entry_price
    return None


def scan_entry_signal_chance(
    today_bars: list,
    first_bar_idx: int,
    ystats: dict,
):
    """
    v17 作空進場訊號：
    1) 排除第一根K棒，PRE_STRATEGY_LOW_BUFFER_END_CHANCE 前若跌破昨低，取最低 low 作為有效昨低
    2) 自 STRATEGY_START_CHANCE ~ STRATEGY_END_CHANCE 監控分K
    3) 任一根分K low < 有效昨低，且該棒前 PREV_LOW_BARS_REQUIRED_CHANCE 根分K的 low 皆 >= 有效昨低
    """
    buffer_end_hm = PRE_STRATEGY_LOW_BUFFER_END_CHANCE[0] * 60 + PRE_STRATEGY_LOW_BUFFER_END_CHANCE[1]
    start_hm = STRATEGY_START_CHANCE[0] * 60 + STRATEGY_START_CHANCE[1]
    end_hm = STRATEGY_END_CHANCE[0] * 60 + STRATEGY_END_CHANCE[1]
    yesterday_low = float(ystats['low'])
    true_yesterday_low = yesterday_low

    for idx, bar in enumerate(today_bars):
        if idx == first_bar_idx:
            continue
        dtv = bar.get('dt')
        if dtv is None:
            continue
        hm = dtv.hour * 60 + dtv.minute
        if hm > buffer_end_hm:
            continue
        bar_low = bar.get('low')
        if bar_low is None:
            continue
        yesterday_low = min(yesterday_low, float(bar_low))

    entry_tick = get_tick_size(yesterday_low)
    entry_price = max(yesterday_low - entry_tick, 0.0)
    max_intraday_range_before_trigger = true_yesterday_low * (MAX_INTRADAY_RANGE_BEFORE_TRIGGER_PER / 100.0)
    _, limit_down_price = calculate_limit_prices(float(ystats['close']))
    if should_skip_entry_by_limit_down_zone(
        entry_price,
        yesterday_low,
        limit_down_price,
    ):
        return ENTRY_BLOCKED

    for idx, bar in enumerate(today_bars):
        if idx == first_bar_idx:
            continue
        dtv = bar.get('dt')
        if dtv is None:
            continue
        hm = dtv.hour * 60 + dtv.minute
        if hm <= buffer_end_hm:
            continue
        if hm >= start_hm:
            continue
        if float(bar.get('low', 0) or 0.0) < yesterday_low:
            return ENTRY_BLOCKED

    time_indexed = []
    for idx, bar in enumerate(today_bars):
        dtv = bar.get('dt')
        if dtv is None:
            continue
        hm = dtv.hour * 60 + dtv.minute
        if hm < start_hm or hm > end_hm:
            continue
        time_indexed.append((idx, bar, hm))

    for original_idx, bar, _ in time_indexed:
        if original_idx == first_bar_idx:
            continue
        bar_low = float(bar['low'])
        if bar_low >= yesterday_low:
            continue

        if should_skip_entry_by_limit_down_zone(
            entry_price,
            yesterday_low,
            limit_down_price,
        ):
            return ENTRY_BLOCKED

        prior_bars = today_bars[:original_idx]
        if prior_bars:
            if all(float(item['high']) <= true_yesterday_low for item in prior_bars):
                return ENTRY_BLOCKED

            prior_day_high = max(float(item['high']) for item in prior_bars)
            prior_day_low = min(float(item['low']) for item in prior_bars)
            if (prior_day_high - prior_day_low) > max_intraday_range_before_trigger:
                return ENTRY_BLOCKED

        prev_lows_valid = False
        prev_bar_average = (
            float(today_bars[original_idx - 1].get('average', 0) or 0.0)
            if original_idx > 0
            else 0.0
        )
        if original_idx >= PREV_LOW_BARS_REQUIRED_CHANCE:
            prev_lows = [
                float(today_bars[original_idx - offset].get('low', 0) or 0.0)
                for offset in range(1, PREV_LOW_BARS_REQUIRED_CHANCE + 1)
            ]
            prev_lows_valid = all(prev_low >= yesterday_low for prev_low in prev_lows)
        if prev_lows_valid and prev_bar_average > yesterday_low:
            return bar, entry_price
        return ENTRY_BLOCKED
    return None


def scan_entry_signal_delay(
    today_bars: list,
    first_bar_idx: int,
    ystats: dict,
):
    """
    delay 作空進場訊號：
    1) 排除第一根K棒，PRE_STRATEGY_LOW_BUFFER_END_CHANCE 前若跌破昨低，取最低 low 作為有效昨低
    2) 自 STRATEGY_START_DELAY ~ STRATEGY_END_DELAY 監控分K
    3) 任一根分K low < 有效昨低，且該棒前 PREV_LOW_BARS_REQUIRED_CHANCE 根分K的 low 皆 >= 有效昨低
    """
    buffer_end_hm = PRE_STRATEGY_LOW_BUFFER_END_CHANCE[0] * 60 + PRE_STRATEGY_LOW_BUFFER_END_CHANCE[1]
    start_hm = STRATEGY_START_DELAY[0] * 60 + STRATEGY_START_DELAY[1]
    end_hm = STRATEGY_END_DELAY[0] * 60 + STRATEGY_END_DELAY[1]
    yesterday_low = float(ystats['low'])
    true_yesterday_low = yesterday_low

    for idx, bar in enumerate(today_bars):
        if idx == first_bar_idx:
            continue
        dtv = bar.get('dt')
        if dtv is None:
            continue
        hm = dtv.hour * 60 + dtv.minute
        if hm > buffer_end_hm:
            continue
        bar_low = bar.get('low')
        if bar_low is None:
            continue
        yesterday_low = min(yesterday_low, float(bar_low))

    entry_tick = get_tick_size(yesterday_low)
    entry_price = max(yesterday_low - entry_tick, 0.0)
    max_intraday_range_before_trigger = true_yesterday_low * (MAX_INTRADAY_RANGE_BEFORE_TRIGGER_PER / 100.0)
    _, limit_down_price = calculate_limit_prices(float(ystats['close']))
    if should_skip_entry_by_limit_down_zone(
        entry_price,
        yesterday_low,
        limit_down_price,
    ):
        return ENTRY_BLOCKED

    for idx, bar in enumerate(today_bars):
        if idx == first_bar_idx:
            continue
        dtv = bar.get('dt')
        if dtv is None:
            continue
        hm = dtv.hour * 60 + dtv.minute
        if hm <= buffer_end_hm:
            continue
        if hm >= start_hm:
            continue
        if float(bar.get('low', 0) or 0.0) < yesterday_low:
            return ENTRY_BLOCKED

    time_indexed = []
    for idx, bar in enumerate(today_bars):
        dtv = bar.get('dt')
        if dtv is None:
            continue
        hm = dtv.hour * 60 + dtv.minute
        if hm < start_hm or hm > end_hm:
            continue
        time_indexed.append((idx, bar, hm))

    for original_idx, bar, _ in time_indexed:
        if original_idx == first_bar_idx:
            continue
        bar_low = float(bar['low'])
        if bar_low >= yesterday_low:
            continue

        if should_skip_entry_by_limit_down_zone(
            entry_price,
            yesterday_low,
            limit_down_price,
        ):
            return ENTRY_BLOCKED

        prior_bars = today_bars[:original_idx]
        if prior_bars:
            if all(float(item['high']) <= true_yesterday_low for item in prior_bars):
                return ENTRY_BLOCKED

            prior_day_high = max(float(item['high']) for item in prior_bars)
            prior_day_low = min(float(item['low']) for item in prior_bars)
            if (prior_day_high - prior_day_low) > max_intraday_range_before_trigger:
                return ENTRY_BLOCKED

        prev_lows_valid = False
        prev_bar_average = (
            float(today_bars[original_idx - 1].get('average', 0) or 0.0)
            if original_idx > 0
            else 0.0
        )
        if original_idx >= PREV_LOW_BARS_REQUIRED_CHANCE:
            prev_lows = [
                float(today_bars[original_idx - offset].get('low', 0) or 0.0)
                for offset in range(1, PREV_LOW_BARS_REQUIRED_CHANCE + 1)
            ]
            prev_lows_valid = all(prev_low >= yesterday_low for prev_low in prev_lows)
        if prev_lows_valid and prev_bar_average > yesterday_low:
            return bar, entry_price
        return ENTRY_BLOCKED
    return None


def get_day_streaks(
    stock_name: str,
    target_date: date,
    day_candles_by_symbol: dict[str, list],
) -> tuple[int, int]:
    """
    回傳 (連漲天數, 連跌天數)。
    規則使用 target_date 前最近三根日K（非營業日會自動跳過）。
    """
    raw_day_bars = day_candles_by_symbol.get(stock_name, [])
    if not raw_day_bars:
        return 0, 0

    # 以日期去重，避免重複資料干擾
    daily_map: dict[date, dict] = {}
    for item in raw_day_bars:
        try:
            day_dt = datetime.strptime(str(item.get('date', ''))[:10], '%Y-%m-%d').date()
            if day_dt >= target_date:
                continue
            daily_map[day_dt] = {
                'open': float(item['open']),
                'high': float(item['high']),
                'low': float(item['low']),
                'close': float(item['close']),
            }
        except Exception:
            continue

    previous_dates = sorted(daily_map.keys(), reverse=True)[:4]
    if len(previous_dates) < 4:
        return 0, 0

    d1 = daily_map[previous_dates[0]]  # target_date 前第 1 根
    d2 = daily_map[previous_dates[1]]  # target_date 前第 2 根
    d3 = daily_map[previous_dates[2]]  # target_date 前第 3 根
    d4 = daily_map[previous_dates[3]]  # target_date 前第 3 根

    up_streak = 0
    if (
        d1['close'] > d1['open']
        and d2['close'] > d2['open']
        and d3['close'] > d3['open']
        and d4['close'] > d4['open']
    ):
        up_streak = 4
    elif (
        d1['close'] > d1['open']
        and d2['close'] > d2['open']
        and d3['close'] > d3['open']
    ):
        up_streak = 3
    elif (
        d1['close'] > d1['open']
        and d2['close'] > d2['open']
    ):
        up_streak = 2
    elif (
        d1['close'] > d1['open']
    ):
        up_streak = 1

    down_streak = 0
    if (
            d1['close'] < d1['open']
            and d2['close'] < d2['open']
            and d3['close'] < d3['open']
            and d4['close'] < d4['open']
    ):
        down_streak = 4
    elif (
        d1['close'] < d1['open']
        and d2['close'] < d2['open']
        and d3['close'] < d3['open']
    ):
        down_streak = 3
    elif (
        d1['close'] < d1['open']
        and d2['close'] < d2['open']
    ):
        down_streak = 2
    elif (
        d1['close'] < d1['open']
    ):
        down_streak = 1

    return up_streak, down_streak


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
        'entry_bar_average': float(entry_bar.get('average', entry_price)),
        'today_bars': today_bars,
        'dt_values': dt_values,
        'open_values': open_values,
        'high_values': high_values,
        'low_values': low_values,
        'limit_up_price': limit_up_price,
        'limit_down_price': limit_down_price,
        'strategy_type': strategy_type,
        'intraday_compare_end': intraday_compare_end,
    }


def should_skip_entry_by_limit_up(entry_price: float, stop_loss: float, limit_up_price: float) -> bool:
    #return False # 先關閉判斷

    # 若進場價 + 停損差價 >= 漲停價，則跳過本次交易。
    return (entry_price + stop_loss) >= limit_up_price


# ---------------------------------------------------------------------------
# Phase 4 (US2) — 策略結果評估
# ---------------------------------------------------------------------------

def evaluate_outcome(today_bars: list, entry_dt: datetime,
                     entry_price: float, take_profit_price: float, stop_loss: float,
                     limit_up_price: float) -> dict | None:
    """
    在進場K棒之後逐根掃描至指定截止時間，判斷停損/獲利；
    若未觸及則以 13:21 K棒（或最後一根）收盤價結算。

    停損優先：同根K棒同時觸及時停損優先。

    若停損價高於（本日漲停價 - 1 tick），則以上述價格作為停損價。

    回傳 {outcome, pnl}。
    outcome 僅代表成功或失敗；停損/停利/結算原因記錄在 exit_reason。
    """
    stop_loss_price = calculate_stop_loss_price(entry_price, stop_loss, limit_up_price)

    # 找進場K棒索引
    entry_idx = next(
        (i for i, b in enumerate(today_bars) if b['dt'] == entry_dt),
        None,
    )
    if entry_idx is None:
        # 找不到進場K棒，fallback 到收盤結算
        settlement_bar = today_bars[-1]
        close = settlement_bar['close']
        result = build_outcome_result('close', entry_price, close)
        result['exit_dt'] = settlement_bar.get('dt')
        return result

    post_entry_bars = today_bars[entry_idx + 1:]

    # 逐根掃描至截止時間前一根 K棒（截止時間點改以開盤價結算）
    for bar in post_entry_bars:
        bar_time = bar['dt']
        # 到達或超過截止時間停止掃描（之後交給結算邏輯）
        if (
            bar_time.hour > INTRADAY_COMPARE_END_CHANCE[0]
            or (
                bar_time.hour == INTRADAY_COMPARE_END_CHANCE[0]
                and bar_time.minute >= INTRADAY_COMPARE_END_CHANCE[1]
            )
        ):
            break

        # 停損優先
        if bar['high'] >= stop_loss_price:
            result = build_outcome_result('stop', entry_price, stop_loss_price)
            result['exit_dt'] = bar.get('dt')
            return result

        if bar['low'] <= take_profit_price:
            result = build_outcome_result('target', entry_price, take_profit_price)
            result['exit_dt'] = bar.get('dt')
            return result

    # 未觸及 — 以截止時間 K棒開盤價結算；若無該時間，改用其後第一根開盤價
    settlement_bar = next(
        (
            b for b in today_bars
            if (
                b['dt'].hour == INTRADAY_COMPARE_END_CHANCE[0]
                and b['dt'].minute == INTRADAY_COMPARE_END_CHANCE[1]
            )
        ),
        None,
    )
    if settlement_bar is None:
        settlement_bar = next(
            (
                b for b in today_bars
                if (
                    b['dt'].hour > INTRADAY_COMPARE_END_CHANCE[0]
                    or (
                        b['dt'].hour == INTRADAY_COMPARE_END_CHANCE[0]
                        and b['dt'].minute > INTRADAY_COMPARE_END_CHANCE[1]
                    )
                )
            ),
            today_bars[-1],
        )
    close = settlement_bar['open']
    result = build_outcome_result('close', entry_price, close)
    result['exit_dt'] = settlement_bar.get('dt')
    return result


def format_result_line(stock_name: str, date_str: str,
                       signal: dict | None, result: dict | None) -> str:
    """格式化單支股票的輸出行。"""
    if signal is None or result is None:
        return ''

    outcome = result['outcome']
    exit_reason = result.get('exit_reason')
    signed = result.get('signed_pnl', signed_pnl(result))
    status_label = '成功' if outcome == 'success' else '失敗'

    if exit_reason == 'target':
        return f'{stock_name} / {date_str} / {status_label}(已達獲利, 淨損益: {signed:+.2f})'
    if exit_reason == 'stop':
        return f'{stock_name} / {date_str} / {status_label}(已達停損, 淨損益: {signed:+.2f})'
    if exit_reason == 'close':
        return f'{stock_name} / {date_str} / {status_label}(收盤結算, 淨損益: {signed:+.2f})'

    return ''


def signed_pnl(result: dict | None) -> float:
    """將策略結果轉成帶方向的損益值。"""
    if not result:
        return 0.0

    if 'signed_pnl' in result:
        return result['signed_pnl']

    if 'pnl' not in result:
        return 0.0

    return result['pnl'] if result.get('outcome') == 'success' else -result['pnl']


def format_entry_datetime(entry_dt: datetime | None) -> str:
    """格式化入場時間，無資料時回傳空字串。"""
    if entry_dt is None:
        return ''
    return entry_dt.strftime('%Y-%m-%d %H:%M:%S')


def format_entry_time(entry_dt: datetime | None) -> str:
    """格式化入場時間（僅時分秒），無資料時回傳空字串。"""
    if entry_dt is None:
        return ''
    return entry_dt.strftime('%H:%M:%S')


def format_date_with_weekday(date_key: str) -> str:
    """將 YYYY-MM-DD 轉為 YYYY-MM-DD(一~日)。"""
    try:
        dt = datetime.strptime(date_key, '%Y-%m-%d')
    except ValueError:
        return date_key
    weekday_labels = ['一', '二', '三', '四', '五', '六', '日']
    return f'{date_key}({weekday_labels[dt.weekday()]})'


def format_monitoring_time_range() -> str:
    """回傳目前策略的監控時間區間字串。"""
    session_start = datetime.combine(date.today(), datetime.min.time()).replace(hour=9, minute=0)
    monitor_end = session_start.replace(
        hour=INTRADAY_COMPARE_END_CHANCE[0],
        minute=INTRADAY_COMPARE_END_CHANCE[1],
    )
    return f'監控時間：{session_start:%H:%M} ~ {monitor_end:%H:%M}'


def format_optimize_parameter_text(loss_percent: float) -> str:
    """格式化停損百分比顯示。"""
    return f'停損={loss_percent:.1f}%'


def format_optimize_profit_parameter_text(profit_percent: float) -> str:
    """格式化停利百分比顯示。"""
    return f'停利={profit_percent:.1f}%'


def format_industry_market_filter_text() -> str:
    """格式化產業盤勢過濾設定。"""
    return (
        '產業盤勢過濾='
        f'(entry當下產業指數high < 前一營業日最後close * '
        f'{1 + INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT / 100.0:.4f})'
    )


def format_strategy_market_decision_gate_text() -> str:
    """格式化全體市場決策 gate 設定。"""
    return (
        '策略決策gate=IX0001與IX0043於STRATEGY_DECISION前判斷 '
        f'(IX0001曾low < 前日最後close * {1 - IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER / 100.0:.4f}, '
        f'IX0043曾low < 前日最後close * {1 - IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER / 100.0:.4f}, '
        f'跌破後下一根至STRATEGY_DECISION前IX0001 high不可 >= 前日最後close * {1 - IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER / 100.0:.4f}, '
        f'IX0043跌破後下一根至STRATEGY_DECISION前 high不可 >= 前日最後close * {1 - IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER / 100.0:.4f})'
    )


def print_daily_optimization_results(
    all_results: list,
    total_pnl: float,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    market_start_gate_cache: dict[date, str],
) -> None:
    """印出固定參數下依日期彙總的進出場明細。"""
    print(
        f'損益已納入交易成本: 手續費率={BROKERAGE_FEE_RATE:.6f}, '
        f'賣出交易稅率={SELL_TRANSACTION_TAX_RATE:.6f}'
    )
    print(format_strategy_market_decision_gate_text())
    print(format_industry_market_filter_text())
    print(f'ENABLE_CHANCE={ENABLE_CHANCE}')
    print('')

    summary = summarize_results(all_results)
    gate_status_by_date_key = {
        current_date.strftime('%Y-%m-%d'): gate_status
        for current_date, gate_status in market_start_gate_cache.items()
    }
    strategy_type_by_gate_status = {
        GATE_LOWER_PASSED: STRATEGY_LOWER,
        GATE_REBOUND_BLOCKED: STRATEGY_DELAY,
        GATE_NOT_PASSED: STRATEGY_CHANCE,
    }
    lower_gate_date_keys = {
        current_date.strftime('%Y-%m-%d')
        for current_date, gate_status in market_start_gate_cache.items()
        if gate_status == GATE_LOWER_PASSED
    }
    rebound_blocked_date_keys = {
        current_date.strftime('%Y-%m-%d')
        for current_date, gate_status in market_start_gate_cache.items()
        if gate_status == GATE_REBOUND_BLOCKED
    }
    if summary['total'] == 0 and not lower_gate_date_keys and not rebound_blocked_date_keys:
        print('固定參數下沒有交易結果。')
        return

    grouped_results: dict[str, list[tuple[str, datetime | None, datetime | None, float, float, float, bool]]] = {}
    for signal, result in all_results:
        if not signal or not result:
            continue
        date_key = signal['date_str']
        grouped_results.setdefault(date_key, []).append((
            signal['name'],
            signal.get('entry_dt'),
            result.get('exit_dt'),
            signal['entry_price'],
            result['exit_price'],
            signed_pnl(result),
            result.get('outcome') == 'success',
        ))

    daily_date_keys = set(grouped_results) | lower_gate_date_keys | rebound_blocked_date_keys
    for date_key in sorted(daily_date_keys, reverse=True):
        day_rows = grouped_results.get(date_key, [])
        day_total = sum(row[5] for row in day_rows)
        day_successes = sum(1 for row in day_rows if row[6])
        day_failures = len(day_rows) - day_successes
        strategy_type = strategy_type_by_gate_status.get(
            gate_status_by_date_key.get(date_key),
            STRATEGY_CHANCE,
        )
        print(
            f'{format_date_with_weekday(date_key)} '
            f'模式={strategy_type}  '
            f'筆數={len(day_rows)}  '
            f'成功={day_successes}  '
            f'失敗={day_failures}  '
            f'總收益={day_total:+.2f}'
        )
        target_date = datetime.strptime(date_key, '%Y-%m-%d').date()
        if date_key in lower_gate_date_keys or date_key in rebound_blocked_date_keys:
            for index_summary in build_market_index_daily_low_summary(
                target_date,
                index_minute_bars_by_key,
            ):
                print(index_summary)
        for stock_name, entry_dt, exit_dt, entry_price, exit_price, pnl_value, _ in sorted(
            day_rows,
            key=lambda row: (format_entry_time(row[1]), row[0]),
        ):
            print(
                f'{stock_name} {format_entry_time(entry_dt)} {format_entry_time(exit_dt)} '
                f'[{entry_price:.2f}|{exit_price:.2f}|{pnl_value:.2f}]'
            )
        print('')

    print(
        f'有結果總筆數={summary["total"]}  '
        f'成功總數={summary["successes"]}  '
        f'失敗總數={summary["failures"]}  '
        f'總收益統計={summary["total_pnl"]:+.2f}'
    )
    print(
        f'CHANCE_LOSS_PER={OPTIMIZE_LOSS_PER_CHANCE:.1f}%  '
        f'CHANCE_PROFIT_PER={OPTIMIZE_PROFIT_PER_CHANCE:.1f}%  '
        f'LOWER_LOSS_PER={OPTIMIZE_LOSS_PER_LOWER:.1f}%  '
        f'LOWER_PROFIT_PER={OPTIMIZE_PROFIT_PER_LOWER:.1f}%  '
        f'DELAY_LOSS_PER={OPTIMIZE_LOSS_PER_DELAY:.1f}%  '
        f'DELAY_PROFIT_PER={OPTIMIZE_PROFIT_PER_DELAY:.1f}%  '
        f'ENABLE_CHANCE={ENABLE_CHANCE}'
    )
    print(
        f'STRATEGY_DECISION={STRATEGY_DECISION[0]:02d}:{STRATEGY_DECISION[1]:02d}    '
        f'CHANCE進場時間窗={STRATEGY_START_CHANCE[0]:02d}:{STRATEGY_START_CHANCE[1]:02d}~{STRATEGY_END_CHANCE[0]:02d}:{STRATEGY_END_CHANCE[1]:02d}    '
        f'昨低緩衝截止={PRE_STRATEGY_LOW_BUFFER_END_CHANCE[0]:02d}:{PRE_STRATEGY_LOW_BUFFER_END_CHANCE[1]:02d}    '
        f'CHANCE出場時間窗={INTRADAY_COMPARE_END_CHANCE[0]:02d}:{INTRADAY_COMPARE_END_CHANCE[1]:02d}'
    )
    print(
        f'LOWER進場時間窗={STRATEGY_START_LOWER[0]:02d}:{STRATEGY_START_LOWER[1]:02d}~{STRATEGY_END_LOWER[0]:02d}:{STRATEGY_END_LOWER[1]:02d}    '
        f'LOWER出場時間窗={INTRADAY_COMPARE_END_LOWER[0]:02d}:{INTRADAY_COMPARE_END_LOWER[1]:02d}'
    )
    print(
        f'DELAY進場時間窗={STRATEGY_START_DELAY[0]:02d}:{STRATEGY_START_DELAY[1]:02d}~{STRATEGY_END_DELAY[0]:02d}:{STRATEGY_END_DELAY[1]:02d}    '
        f'DELAY出場時間窗={INTRADAY_COMPARE_END_DELAY[0]:02d}:{INTRADAY_COMPARE_END_DELAY[1]:02d}'
    )
    print(format_strategy_market_decision_gate_text())
    print(format_industry_market_filter_text())


# ---------------------------------------------------------------------------
# Core orchestration — analyze_stock
# ---------------------------------------------------------------------------

def find_trade_candidate_on_date(
    stock_item: tuple,
    target_date: date,
    bars_by_date: dict,
    day_candles_by_symbol: dict[str, list],
    index_minute_bars_by_key: dict[str, dict[str, list]],
    strategy_type: str,
):
    """找出單日候選交易；無訊號則回傳 None。"""
    stock_name = stock_item[0]
    today_bars, yesterday_bars = get_target_and_yesterday(bars_by_date, target_date)
    if not today_bars or not yesterday_bars:
        return None

    ystats = compute_yesterday_stats(yesterday_bars)

    first_bar, first_bar_idx = find_first_bar(today_bars)
    if first_bar_idx < 0:
        return None

    if not is_one_minute_bars(today_bars):
        return None

    if first_bar_idx + 1 >= len(today_bars):
        return None

    if strategy_type == STRATEGY_LOWER:
        pair = scan_entry_signal_lower(
            today_bars,
            ystats,
        )
        intraday_compare_end = INTRADAY_COMPARE_END_LOWER
    elif strategy_type == STRATEGY_DELAY:
        pair = scan_entry_signal_delay(
            today_bars,
            first_bar_idx,
            ystats,
        )
        intraday_compare_end = INTRADAY_COMPARE_END_DELAY
    else:
        pair = scan_entry_signal_chance(
            today_bars,
            first_bar_idx,
            ystats,
        )
        intraday_compare_end = INTRADAY_COMPARE_END_CHANCE
    if pair is None:
        return None
    if pair == ENTRY_BLOCKED:
        return None

    entry_bar, entry_price = pair
    if not is_industry_market_filter_passed(
        stock_item,
        target_date,
        entry_bar['dt'],
        index_minute_bars_by_key,
    ):
        return None

    limit_up_price, limit_down_price = calculate_limit_prices(ystats['close'])
    return build_trade_candidate(
        stock_name,
        target_date,
        entry_bar,
        entry_price,
        today_bars,
        limit_up_price,
        limit_down_price,
        strategy_type,
        intraday_compare_end,
    )


def collect_trade_candidates(
    stock_item: tuple,
    target_date: date,
    minute_bars_by_symbol: dict[str, dict[str, list]],
    day_candles_by_symbol: dict[str, list],
    index_minute_bars_by_key: dict[str, dict[str, list]],
    market_start_gate_cache: dict[date, str],
) -> list:
    """蒐集單支股票自 target_date 起往前的所有候選交易。"""
    stock_name = stock_item[0]
    bars_by_date = minute_bars_by_symbol.get(stock_name, {})
    if not bars_by_date:
        return []
    available_dates = sorted(
        (
            datetime.strptime(date_key, '%Y-%m-%d').date()
            for date_key in bars_by_date
            if datetime.strptime(date_key, '%Y-%m-%d').date() <= target_date
        ),
        reverse=True,
    )

    candidates = []
    for current_date in available_dates:
        if current_date not in market_start_gate_cache:
            market_start_gate_cache[current_date] = get_strategy_market_decision_gate_status(
                current_date,
                index_minute_bars_by_key,
            )
        gate_status = market_start_gate_cache[current_date]
        if gate_status == GATE_LOWER_PASSED:
            strategy_type = STRATEGY_LOWER
        elif gate_status == GATE_NOT_PASSED:
            strategy_type = STRATEGY_CHANCE
        else:
            strategy_type = STRATEGY_DELAY
        if strategy_type == STRATEGY_CHANCE and not ENABLE_CHANCE:
            continue

        candidate = find_trade_candidate_on_date(
            stock_item,
            current_date,
            bars_by_date,
            day_candles_by_symbol,
            index_minute_bars_by_key,
            strategy_type,
        )
        if candidate is not None:
            candidates.append(candidate)

    return candidates


def summarize_results(all_results: list) -> dict:
    """彙整結果供最佳化比較。"""
    total = sum(1 for _, result in all_results if result)
    successes = sum(
        1 for _, result in all_results
        if result and result['outcome'] == 'success'
    )
    failures = sum(
        1 for _, result in all_results
        if result and result['outcome'] == 'fail'
    )
    total_pnl = sum(signed_pnl(result) for _, result in all_results)
    used_big_count = sum(
        1 for signal, result in all_results
        if signal and result and signal.get('entry_price', 0) >= 100
    )
    used_small_count = sum(
        1 for signal, result in all_results
        if signal and result and signal.get('entry_price', 0) < 100
    )
    return {
        'total': total,
        'successes': successes,
        'failures': failures,
        'total_pnl': total_pnl,
        'used_big_count': used_big_count,
        'used_small_count': used_small_count,
    }


def evaluate_candidates(
    candidates: list,
    print_results: bool = False,
) -> list:
    """對候選交易套用指定參數並回傳結果。"""
    all_results = []
    for candidate in candidates:
        name = candidate['name']
        date_str = candidate['date_str']
        entry_dt = candidate['entry_dt']
        entry_price = candidate['entry_price']
        entry_bar_average = candidate['entry_bar_average']
        today_bars = candidate['today_bars']
        dt_values = candidate['dt_values']
        high_values = candidate['high_values']
        low_values = candidate['low_values']
        open_values = candidate['open_values']
        limit_up_price = candidate['limit_up_price']
        limit_down_price = candidate['limit_down_price']
        strategy_type = candidate.get('strategy_type', STRATEGY_CHANCE)
        intraday_compare_end = candidate.get('intraday_compare_end', INTRADAY_COMPARE_END_CHANCE)
        if strategy_type == STRATEGY_LOWER:
            optimize_loss_percent = OPTIMIZE_LOSS_PER_LOWER
            optimize_profit_percent = OPTIMIZE_PROFIT_PER_LOWER
        elif strategy_type == STRATEGY_DELAY:
            optimize_loss_percent = OPTIMIZE_LOSS_PER_DELAY
            optimize_profit_percent = OPTIMIZE_PROFIT_PER_DELAY
        else:
            optimize_loss_percent = OPTIMIZE_LOSS_PER_CHANCE
            optimize_profit_percent = OPTIMIZE_PROFIT_PER_CHANCE

        effective_stop_loss = calculate_stop_loss_amount_by_percent(entry_price, optimize_loss_percent)
        effective_profit = calculate_take_profit_amount_by_percent(entry_price, optimize_profit_percent)
        raw_take_profit_price = entry_price - effective_profit
        take_profit_price = max(raw_take_profit_price, limit_down_price)
        if should_skip_entry_by_limit_up(
            entry_price, effective_stop_loss, limit_up_price
        ):
            continue
        try:
            entry_idx = dt_values.index(entry_dt)
        except ValueError:
            continue

        signal = {
            'name': name,
            'date_str': date_str,
            'entry_dt': entry_dt,
            'entry_price': entry_price,
            'strategy_type': strategy_type,
            'take_profit_price': take_profit_price,
            'effective_profit': effective_profit,
            'effective_stop_loss': effective_stop_loss,
            'stop_loss_price': calculate_stop_loss_price(
                entry_price, effective_stop_loss, limit_up_price
            ),
        }
        stop_loss_price = calculate_stop_loss_price(entry_price, effective_stop_loss, limit_up_price)
        result = None
        for i in range(entry_idx + 1, len(dt_values)):
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
            result = build_outcome_result('close', entry_price, float(open_values[settlement_idx]))
            result['exit_dt'] = dt_values[settlement_idx]
        if print_results:
            line = format_result_line(name, date_str, signal, result)
            if line:
                print(line)
        all_results.append((signal, result))

    return all_results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    OUTPUT_BUFFER.clear()
    builtins.print(f'開始時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    builtins.print()
    try:
        args = parse_args()
        raw_stock_list = STOCK_LIST or selected_stocks
        stock_list = raw_stock_list

        # 目標日期
        if args.to:
            try:
                target_date = datetime.strptime(args.to, '%Y-%m-%d').date()
            except ValueError:
                print(f'[ERROR] 日期格式錯誤，請使用 YYYY-MM-DD: {args.to}', file=sys.stderr)
                sys.exit(1)
        else:
            target_date = date.today()

        cache_path = get_api_cache_path(target_date)
        cached = load_api_cache(cache_path, stock_list)
        if cached is not None:
            day_candles_by_symbol, minute_bars_by_symbol, index_minute_bars_by_key = cached
            print(f'已載入API快取: {cache_path.name}')
        else:
            # 初始化 SDK
            _, rest_stock = init_sdk(args.config)

            # 先蒐集日K
            day_candles_by_symbol: dict[str, list] = {}
            # 先蒐集分K（raw + parsed）
            minute_raw_by_symbol: dict[str, list] = {}
            minute_bars_by_symbol: dict[str, dict[str, list]] = {}
            index_minute_raw_by_key: dict[str, list] = {}
            index_minute_bars_by_key: dict[str, dict[str, list]] = {}
            total_stocks = len(stock_list)
            for idx, stock_item in enumerate(stock_list, start=1):
                stock_name = stock_item[0]
                print_api_progress(idx, total_stocks, stock_name)
                day_candles_by_symbol[stock_name] = fetch_day_candles(stock_item, target_date, rest_stock)
                try:
                    symbol = extract_symbol(stock_name)
                except ValueError as exc:
                    print(f'[ERROR] {exc}', file=sys.stderr)
                    minute_raw_by_symbol[stock_name] = []
                    minute_bars_by_symbol[stock_name] = {}
                    continue
                raw_data = fetch_minute_candles(rest_stock, symbol, target_date)
                minute_raw_by_symbol[stock_name] = raw_data if raw_data else []
                minute_bars_by_symbol[stock_name] = parse_bars(raw_data) if raw_data else {}
            if total_stocks > 0:
                builtins.print()

            required_index_keys = sorted(set(RESERVE_MARKET_INDICES) | get_required_industry_index_keys(stock_list))
            total_indices = len(required_index_keys)
            for idx, index_key in enumerate(required_index_keys, start=1):
                index_meta = MARKET_INDEX_METADATA.get(index_key, {})
                index_label = f'{index_key} {index_meta.get("name", "")}'.strip()
                print_api_progress(idx, total_indices, index_label)
                raw_index_data = fetch_index_minute_candles(rest_stock, index_key, target_date)
                index_minute_raw_by_key[index_key] = raw_index_data if raw_index_data else []
                index_minute_bars_by_key[index_key] = parse_bars(raw_index_data) if raw_index_data else {}
            if total_indices > 0:
                builtins.print()

            save_api_cache(
                cache_path,
                stock_list,
                day_candles_by_symbol,
                minute_raw_by_symbol,
                index_minute_raw_by_key,
            )
            print(f'已儲存API快取: {cache_path.name}')

        total_stocks = len(stock_list)

        def evaluate_one_window() -> dict:
            nonlocal total_stocks

            all_candidates: list = []
            market_start_gate_cache: dict[date, str] = {}
            for idx, stock_item in enumerate(stock_list, start=1):
                stock_name = stock_item[0]
                print_progress(
                    idx,
                    total_stocks,
                    f'{stock_name} [C {STRATEGY_START_CHANCE[0]:02d}:{STRATEGY_START_CHANCE[1]:02d}~{STRATEGY_END_CHANCE[0]:02d}:{STRATEGY_END_CHANCE[1]:02d} / '
                    f'L {STRATEGY_START_LOWER[0]:02d}:{STRATEGY_START_LOWER[1]:02d}~{STRATEGY_END_LOWER[0]:02d}:{STRATEGY_END_LOWER[1]:02d} / '
                    f'D {STRATEGY_START_DELAY[0]:02d}:{STRATEGY_START_DELAY[1]:02d}~{STRATEGY_END_DELAY[0]:02d}:{STRATEGY_END_DELAY[1]:02d}]',
                )
                all_candidates.extend(
                    collect_trade_candidates(
                        stock_item,
                        target_date,
                        minute_bars_by_symbol,
                        day_candles_by_symbol,
                        index_minute_bars_by_key,
                        market_start_gate_cache,
                    )
                )

            results = evaluate_candidates(
                all_candidates,
                print_results=False,
            )
            evaluated_summary = summarize_results(results)

            return {
                'best_total_pnl': evaluated_summary['total_pnl'],
                'best_total_failures': evaluated_summary['failures'],
                'best_results': results,
                'market_start_gate_cache': market_start_gate_cache,
            }

        strategy_decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
        strategy_start_chance_hm = STRATEGY_START_CHANCE[0] * 60 + STRATEGY_START_CHANCE[1]
        strategy_start_lower_hm = STRATEGY_START_LOWER[0] * 60 + STRATEGY_START_LOWER[1]
        strategy_start_delay_hm = STRATEGY_START_DELAY[0] * 60 + STRATEGY_START_DELAY[1]
        strategy_end_chance_hm = STRATEGY_END_CHANCE[0] * 60 + STRATEGY_END_CHANCE[1]
        strategy_end_lower_hm = STRATEGY_END_LOWER[0] * 60 + STRATEGY_END_LOWER[1]
        strategy_end_delay_hm = STRATEGY_END_DELAY[0] * 60 + STRATEGY_END_DELAY[1]
        pre_strategy_low_buffer_end_chance_hm = (
            PRE_STRATEGY_LOW_BUFFER_END_CHANCE[0] * 60 + PRE_STRATEGY_LOW_BUFFER_END_CHANCE[1]
        )
        if not (0 <= strategy_decision_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_DECISION 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if not (0 <= strategy_start_chance_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_START_CHANCE 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if not (0 <= strategy_start_lower_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_START_LOWER 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if not (0 <= strategy_start_delay_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_START_DELAY 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if not (0 <= strategy_end_chance_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_END_CHANCE 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if not (0 <= strategy_end_lower_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_END_LOWER 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if not (0 <= strategy_end_delay_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_END_DELAY 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if pre_strategy_low_buffer_end_chance_hm >= strategy_start_chance_hm:
            print('[ERROR] PRE_STRATEGY_LOW_BUFFER_END_CHANCE 必須早於 STRATEGY_START_CHANCE', file=sys.stderr)
            sys.exit(1)
        if pre_strategy_low_buffer_end_chance_hm >= strategy_start_delay_hm:
            print('[ERROR] PRE_STRATEGY_LOW_BUFFER_END_CHANCE 必須早於 STRATEGY_START_DELAY', file=sys.stderr)
            sys.exit(1)
        if strategy_start_chance_hm >= strategy_end_chance_hm:
            print('[ERROR] STRATEGY_START_CHANCE 必須早於 STRATEGY_END_CHANCE', file=sys.stderr)
            sys.exit(1)
        if strategy_start_lower_hm >= strategy_end_lower_hm:
            print('[ERROR] STRATEGY_START_LOWER 必須早於 STRATEGY_END_LOWER', file=sys.stderr)
            sys.exit(1)
        if strategy_start_delay_hm >= strategy_end_delay_hm:
            print('[ERROR] STRATEGY_START_DELAY 必須早於 STRATEGY_END_DELAY', file=sys.stderr)
            sys.exit(1)
        if IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER < 0:
            print('[ERROR] IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER 不可小於 0', file=sys.stderr)
            sys.exit(1)
        if IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER < 0:
            print('[ERROR] IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER 不可小於 0', file=sys.stderr)
            sys.exit(1)
        if IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER < 0:
            print('[ERROR] IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER 不可小於 0', file=sys.stderr)
            sys.exit(1)
        if IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER < 0:
            print('[ERROR] IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER 不可小於 0', file=sys.stderr)
            sys.exit(1)

        best_window_result = evaluate_one_window()
        builtins.print()
        best_total_pnl = best_window_result['best_total_pnl']
        best_results = best_window_result['best_results']
        market_start_gate_cache = best_window_result['market_start_gate_cache']

        print_daily_optimization_results(
            best_results,
            best_total_pnl,
            index_minute_bars_by_key,
            market_start_gate_cache,
        )
    finally:
        builtins.print(f'結束時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        flush_each_stock_output_file()


if __name__ == '__main__':
    main()
