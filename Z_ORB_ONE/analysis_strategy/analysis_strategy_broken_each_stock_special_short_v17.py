"""
已知對齊原則與刻意差異
1. 回測版以分 K 模擬策略，實機版以即時 quote/websocket 執行；逐項比對時不把資料粒度差異視為策略不一致。
2. LIMIT_UP / LIMIT_DOWN 回測自行用日 K 驗證連續漲跌停天數；實機只交易當日名單，信任 selected_limit_up_stocks / selected_limit_down_stocks 已由前置流程產生。
3. 實機 LOWER 多了 best bid/ask 可成交性保護，回測分 K 無足夠委買委賣資料，因此不納入回測。
4. 保本與逐步獲利為實機版特有風控；回測維持固定停損/停利/收盤結算模型。

LOWER 模式成立條件
1. IX0001 在 09:06～09:16（含）曾發生早盤突破。
2. 09:06～09:42 期間，IX0001、IX0043 都不能上下穿越。
3. IX0001、IX0043 均曾在 09:43 前跌破各自的 LOWER 啟動門檻且在判別模式時仍維持住。

LIMIT_UP 策略成立條件
1. 該股票連漲停符合指定次數

LIMIT_DOWN 策略成立條件
1. 該股票連跌停符合指定次數

------------------------------------------------

LOWER 模式個股入場條件
1. 於 STRATEGY_START_LOWER～STRATEGY_END_LOWER（含）尋找第一根 low 落在「昨收到跌停價」之 LOWER_ENTRY_RANGE_START_PERCENT～LOWER_ENTRY_RANGE_END_PERCENT 區間內的分 K。
2. 以上述分 K 的 low 作為放空入場價。
3. 入場當下 IX0001、IX0043 均仍須維持 LOWER，且個股所屬產業指數不得高於 LOWER 允許門檻；任一資料不足或條件不符即不入場。

非 LIMIT_UP / LIMIT_DOWN 策略共同入場保護
1. 入場分 K 之前若當日已觸及漲停或跌停，當日不進場。
2. 放空時若入場價加停損價差已達漲停價，或做多時若入場價減停損價差已達跌停價，皆不進場。

LIMIT_UP 策略個股入場條件
1. 該股票截至前一交易日的連續收漲停天數，必須恰好存在 LONG_LIMIT_UP_DAYS 中，當日直接以第一根分 K 的 open 做多進場。

LIMIT_DOWN 策略個股入場條件
1. 該股票截至前一交易日的連續收跌停天數，必須恰好存在 SHORT_LIMIT_DOWN_DAYS 中，當日直接以第一根分 K 的 open 放空進場。

"""



import builtins
import argparse
import ast
import configparser
import json
import math
import re
import sys
import time
import numpy as np
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from datetime import date, datetime, timedelta
from pathlib import Path
from pprint import pformat
from tempfile import NamedTemporaryFile

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from esun_marketdata import EsunMarketdata
from Z_ORB_ONE.stock_data import (
    market_previous_close_indices,
    selected_limit_down_stocks,
    selected_limit_up_stocks,
    selected_stocks,
)

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
STOCK_DATA_PATH = Path(__file__).resolve().parents[1] / 'stock_data.py'
INDUSTRY_INDEX_MAP_PATH = Path(__file__).resolve().parents[1] / 'industry_index_map.json'
EACH_STOCK_OUTPUT_FILE = Path(__file__).with_name('analysis_strategy_broken_each_stock_special_short_v17_result.txt')
OUTPUT_BUFFER: list[str] = []
DAILY_CANDLE_DATA_ISSUES: set[tuple[str, str, str]] = set()
GATE_LOWER_PASSED = 'LOWER_PASSED'
GATE_NOT_PASSED = 'NOT_PASSED'
GATE_NO_TRADE = 'NO_TRADE'
GATE_DATA_INCOMPLETE = 'DATA_INCOMPLETE'
STRATEGY_LOWER = 'LOWER'
STRATEGY_LIMIT_DOWN = 'LIMIT_DOWN'
STRATEGY_LIMIT_UP = 'LIMIT_UP'
STRATEGY_NO_TRADE = 'NO_TRADE'
TRADE_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'LONG'

INCLUDE_LOWER_IN_PRINT_STATS = True
INCLUDE_LIMIT_UP_IN_PRINT_STATS = True
INCLUDE_LIMIT_DOWN_IN_PRINT_STATS = True

# ---------------------------------------------------------------------------
# IDE 直接執行時可在此調整策略參數, 此版本不會跳過前一日非營業日的狀況
# ---------------------------------------------------------------------------

# 本日於 09:30 前至少須有此數量的分鐘 K 棒；
# 少於此數量視為延遲撮合股票，不納入當日策略。
MIN_MINUTE_BARS_BEFORE_0930 = 20

OPTIMIZE_PROFIT_PER_LOWER = 5.0 # lower 停利百分比(%)，例如 5.0 代表入場價減去 5%
OPTIMIZE_LOSS_PER_LOWER = 2.0 # lower 停損百分比(%)，例如 3.0 代表入場價加上 3%

OPTIMIZE_PROFIT_PER_LIMIT_DOWN = 9.0 # limit down 停利百分比(%)
OPTIMIZE_LOSS_PER_LIMIT_DOWN = 2.0 # limit down 停損百分比(%)

OPTIMIZE_PROFIT_PER_LIMIT_UP = 10.0 # limit up 停利百分比(%)
OPTIMIZE_LOSS_PER_LIMIT_UP = 2.0 # limit up 停損百分比(%)

LOWER_ENTRY_RANGE_START_PERCENT = 10.0 # lower 入場價距昨收到跌停的起始百分比，可以為 0
LOWER_ENTRY_RANGE_END_PERCENT = 60.0 # lower 入場價距昨收到跌停的結束百分比，可以為 70

LONG_LIMIT_UP_DAYS = [2] # limit up 策略允許的「實際」連續收漲停天數
SHORT_LIMIT_DOWN_DAYS = [1] # limit down 策略允許的「實際」連續收跌停天數

MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME = (9, 6) # 指數昨收兩側檢查起始時間，包含此時間
STRATEGY_EARLY_BREAKOUT_DEADLINE = (9, 15) # IX0001 早盤須先向下突破 LOWER 門檻的截止分K棒，包含此時間
STRATEGY_DECISION = (9, 43) # 市場模式判斷截止分K棒的(時, 分)，不包含此時間
STRATEGY_START_LOWER = (9, 44) # lower 個股進場開始分K棒的(時, 分)，包含此時間
STRATEGY_END_LOWER = (10, 1) # lower 策略可進場截止分k棒的(時, 分)，包含此時間

INTRADAY_COMPARE_END_LOWER = (13, 0)  # lower 盤中停損/停利比對截止(時, 分)
INTRADAY_COMPARE_END_LIMIT_DOWN = (13, 0) # limit down 盤中停損/停利比對截止(時, 分)
INTRADAY_COMPARE_END_LIMIT_UP = (13, 0) # limit up 盤中停損/停利比對截止(時, 分)

IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.2 # IX0001 啟動門檻：STRATEGY_DECISION 前最後 low 需低於前日最後 close 的百分比
IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.6 # IX0001 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內
IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.0 # IX0043 啟動門檻：STRATEGY_DECISION 前最後 low 需低於前日最後 close 的百分比
IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.0 # IX0043 反彈失效門檻：跌破後 high 不可回到前日最後 close 下方此百分比內

# 產業盤勢過濾：原策略入場條件成立後，產業指數當下價格不可與策略方向相反。
INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT = 0 # lower 入場條件成立後，產業指數當下 close 不可高於昨收指數上漲此百分比後的位置

BROKERAGE_FEE_RATE = 0.001425 # 台股手續費率，買賣雙邊皆收
SELL_TRANSACTION_TAX_RATE = 0.003 # 台股交易稅率，賣出時收

EXCLUDED_INDUSTRY_CODES: list[str] = [] # 排除 17-金融保險, 20-其他, 36-數位雲端, 31-其他電子業, 25-電腦及週邊設備業
# "17", "20", "36", "31", "25"

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


def find_assignment(source: str, name: str) -> tuple[int, int]:
    """回傳指定頂層 assignment 的起訖行；寫回 stock_data.py 時使用。"""
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.Assign):
            target_names = [
                target.id
                for target in node.targets
                if isinstance(target, ast.Name)
            ]
            if name in target_names and node.end_lineno is not None:
                return node.lineno - 1, node.end_lineno
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == name
            and node.end_lineno is not None
        ):
            return node.lineno - 1, node.end_lineno
    raise ValueError(f'找不到 {name} 宣告')


def write_stock_data_market_indices(indices: dict[str, dict]) -> None:
    """只更新 stock_data.py 的 market_previous_close_indices assignment。"""
    source = STOCK_DATA_PATH.read_text(encoding='utf-8')
    lines = source.splitlines()
    start_line, end_line = find_assignment(source, 'market_previous_close_indices')
    replacement = (
        'market_previous_close_indices = '
        + pformat(indices, sort_dicts=False)
    ).splitlines()
    updated_source = '\n'.join(lines[:start_line] + replacement + lines[end_line:]) + '\n'

    with NamedTemporaryFile(
        'w',
        encoding='utf-8',
        dir=str(STOCK_DATA_PATH.parent),
        delete=False,
    ) as tmp_file:
        tmp_file.write(updated_source)
        tmp_file.flush()
        tmp_path = Path(tmp_file.name)

    tmp_path.replace(STOCK_DATA_PATH)


def get_required_industry_targets(stock_list: list[tuple]) -> set[tuple[str, str]]:
    """依股票清單回傳需要的 (exchange, industry_code)。"""
    targets: set[tuple[str, str]] = set()
    for stock_item in stock_list:
        if len(stock_item) <= 6:
            continue
        exchange = get_exchange_for_stock(stock_item[0])
        if exchange is None:
            continue
        industry_code = str(stock_item[6]).zfill(2)
        targets.add((exchange, industry_code))
    return targets


def build_industry_index_entry(
    exchange: str,
    industry_code: str,
    index_map: dict,
) -> dict | None:
    """從 update_stock_data_industry_indices.py 產出的 mapping 建立產業指數 metadata。"""
    exchange_map = index_map.get('exchanges', {}).get(exchange, {})
    industry = exchange_map.get('industries', {}).get(industry_code, {})
    index_info = industry.get('index')
    if not index_info:
        return None

    symbol = index_info.get('symbol')
    name = index_info.get('name')
    if not symbol or not name:
        return None

    industry_codes = index_map.get('industry_codes', {})
    industry_name = industry.get('industry_name') or industry_codes.get(industry_code)
    return {
        'exchange': exchange,
        'industry_code': industry_code,
        'industry_name': industry_name,
        'symbol': symbol,
        'name': name,
        'previous_close': None,
        'time': None,
        'last_updated': None,
        'source': 'historical.candles',
    }


def ensure_backtest_industry_index_metadata(stock_list: list[tuple]) -> None:
    """
    回測程式專用：啟動時檢查 stock_data.py 是否具備足夠產業別指數 metadata。
    只補齊產業指數結構與代碼，不更新最新 previous_close；回測會另外抓歷史分 K。
    """
    required_targets = get_required_industry_targets(stock_list)
    missing_targets = [
        (exchange, industry_code)
        for exchange, industry_code in sorted(required_targets)
        if not market_previous_close_indices.get(f'{exchange}:{industry_code}', {}).get('symbol')
    ]
    if not missing_targets:
        return

    if not INDUSTRY_INDEX_MAP_PATH.exists():
        missing_text = ', '.join(f'{exchange}:{industry_code}' for exchange, industry_code in missing_targets)
        raise FileNotFoundError(
            f'缺少產業指數 metadata: {missing_text}；且找不到 {INDUSTRY_INDEX_MAP_PATH}'
        )

    index_map = json.loads(INDUSTRY_INDEX_MAP_PATH.read_text(encoding='utf-8'))
    updated_indices = dict(market_previous_close_indices)
    unresolved: list[str] = []
    added_keys: list[str] = []
    for exchange, industry_code in missing_targets:
        index_key = f'{exchange}:{industry_code}'
        entry = build_industry_index_entry(exchange, industry_code, index_map)
        if entry is None:
            unresolved.append(index_key)
            continue
        updated_indices[index_key] = entry
        added_keys.append(index_key)

    if unresolved:
        raise ValueError(
            'industry_index_map.json 找不到以下產業指數 mapping: '
            + ', '.join(unresolved)
        )

    write_stock_data_market_indices(updated_indices)
    market_previous_close_indices.clear()
    market_previous_close_indices.update(updated_indices)
    MARKET_INDEX_METADATA.clear()
    MARKET_INDEX_METADATA.update({**market_previous_close_indices, **RESERVE_MARKET_INDICES})
    print(
        '回測啟動檢查：已補齊 stock_data.py 產業別指數 metadata '
        f'({len(added_keys)}): {", ".join(added_keys)}'
    )


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
        cached_name_set = set(cached_names)
        if any(stock_name not in cached_name_set for stock_name in current_names):
            return None
        day_candles_by_symbol = payload.get('day_candles_by_symbol', {})
        raw_minute_by_symbol = payload.get('minute_raw_by_symbol', {})
        raw_index_minute_by_key = payload.get('index_minute_raw_by_key', {})
        if any(
            stock_name not in day_candles_by_symbol
            or stock_name not in raw_minute_by_symbol
            for stock_name in current_names
        ):
            return None
        required_index_keys = set(RESERVE_MARKET_INDICES) | get_required_industry_index_keys(stock_list)
        if any(
            key not in raw_index_minute_by_key
            for key in required_index_keys
        ):
            return None
        day_candles_by_symbol = {
            stock_name: day_candles_by_symbol.get(stock_name, [])
            for stock_name in current_names
        }
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


def calculate_stop_loss_amount_by_percent(entry_price: float, stop_loss_percent: float) -> float:
    """依入場價與停損百分比計算停損價差。"""
    return entry_price * (stop_loss_percent / 100.0)


def calculate_take_profit_amount_by_percent(entry_price: float, take_profit_percent: float) -> float:
    """依入場價與停利百分比計算停利價差。"""
    return entry_price * (take_profit_percent / 100.0)


def should_skip_entry_by_limit_up(entry_price: float, stop_loss: float, limit_up_price: float) -> bool:
    """若作空進場價加停損差價已達漲停，略過本次交易。"""
    return (entry_price + stop_loss) >= limit_up_price


def should_skip_entry_by_limit_down(entry_price: float, stop_loss: float, limit_down_price: float) -> bool:
    """若作多進場價減停損差價已達跌停，略過本次交易。"""
    return (entry_price - stop_loss) <= limit_down_price


def is_independent_limit_strategy(strategy_type: str | None) -> bool:
    """LIMIT_UP / LIMIT_DOWN 為獨立預掛策略，不套用一般入場保護。"""
    return strategy_type in (STRATEGY_LIMIT_UP, STRATEGY_LIMIT_DOWN)


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


def calculate_entry_range_bounds(
    reference_price: float,
    limit_price: float,
    start_percent: float,
    end_percent: float,
) -> tuple[float, float]:
    """依參考價到漲跌停的百分比區間，回傳可入場價格上下界。"""
    start_price = reference_price + (limit_price - reference_price) * (start_percent / 100.0)
    end_price = reference_price + (limit_price - reference_price) * (end_percent / 100.0)
    return min(start_price, end_price), max(start_price, end_price)


def is_price_in_entry_range(price: float, lower_bound: float, upper_bound: float) -> bool:
    """判斷價格是否落在入場區間內，含上下界。"""
    return lower_bound <= price <= upper_bound


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


def get_exchange_suffix_for_stock(stock_name: str) -> str:
    """取得結果輸出用的市場別尾碼。"""
    symbol_text = stock_name.split(':', 1)[-1].upper()
    if symbol_text.endswith('.TWO'):
        return 'TWO'
    if symbol_text.endswith('.TW'):
        return 'TW'
    return ''


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


def get_stock_industry_code(stock_item: tuple) -> str:
    """取得股票產業別代碼，輸出用。"""
    if len(stock_item) <= 6 or stock_item[6] is None:
        return ''
    return str(stock_item[6]).zfill(2)


def normalize_excluded_industry_code(industry_code) -> str:
    """排除清單比對用；不補零，輸入什麼就比對什麼。"""
    if industry_code is None:
        return ''
    return str(industry_code).strip()


def is_stock_in_excluded_industry(stock_item: tuple) -> bool:
    """判斷股票是否屬於要排除的產業別。"""
    if len(stock_item) <= 6:
        return False
    excluded_codes = {
        normalize_excluded_industry_code(industry_code)
        for industry_code in EXCLUDED_INDUSTRY_CODES
    }
    return normalize_excluded_industry_code(stock_item[6]) in excluded_codes


def filter_stocks_by_excluded_industries(stock_list: list[tuple]) -> list[tuple]:
    """排除指定產業別的股票。"""
    return [
        stock_item
        for stock_item in stock_list
        if not is_stock_in_excluded_industry(stock_item)
    ]


def dedupe_stock_list(stock_lists: list[list[tuple]]) -> list[tuple]:
    """依股票名稱去重，保留第一次出現的 tuple。"""
    seen_names: set[str] = set()
    deduped: list[tuple] = []
    for stock_list in stock_lists:
        for stock_item in stock_list:
            stock_name = stock_item[0]
            if stock_name in seen_names:
                continue
            seen_names.add(stock_name)
            deduped.append(stock_item)
    return deduped


def build_stock_strategy_assignments(
    stock_list: list[tuple],
    limit_up_stock_list: list[tuple],
    limit_down_stock_list: list[tuple],
) -> list[tuple[tuple, bool, bool, bool]]:
    """依股票名稱整合清單，回傳股票與一般/LIMIT_UP/LIMIT_DOWN 執行權限。"""
    assignments: dict[str, list] = {}

    def register(stock_item: tuple, mode_index: int) -> None:
        stock_name = stock_item[0]
        assignment = assignments.setdefault(
            stock_name,
            [stock_item, False, False, False],
        )
        assignment[mode_index] = True

    for stock_item in stock_list:
        register(stock_item, 1)
        if INCLUDE_LIMIT_UP_IN_PRINT_STATS:
            register(stock_item, 2)
        if INCLUDE_LIMIT_DOWN_IN_PRINT_STATS:
            register(stock_item, 3)
    for stock_item in limit_up_stock_list:
        register(stock_item, 2)
    for stock_item in limit_down_stock_list:
        register(stock_item, 3)

    return [tuple(assignment) for assignment in assignments.values()]


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
            f'{label} 昨低={previous_low_text}  '
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




def build_market_open_summary_at_entry(
    target_date: date,
    entry_dt: datetime | None,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> str:
    """建立日彙總用的 IX0001 入場分鐘 open 與昨收比較文字。"""
    if entry_dt is None:
        return 'N/A N/A N/A'
    bars_by_date = index_minute_bars_by_key.get('TWSE:MARKET', {})
    previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    if previous_close is None or previous_close == 0 or not today_bars:
        return 'N/A N/A N/A'
    entry_bar = next(
        (
            bar
            for bar in today_bars
            if bar.get('dt') == entry_dt and bar.get('open') is not None
        ),
        None,
    )
    if entry_bar is None:
        return 'N/A N/A N/A'
    try:
        market_open = float(entry_bar['open'])
    except (TypeError, ValueError):
        return 'N/A N/A N/A'
    diff = market_open - previous_close
    diff_percent = (diff / previous_close) * 100.0
    return f'{market_open:.2f} {diff:+.2f} {diff_percent:+.1f}%'




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


def has_market_index_crossed_both_sides_of_previous_close(
    index_key: str,
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> bool:
    """指定起始時間至決策時間前，指數若曾嚴格位於昨收上下兩側即視為反轉。"""
    bars_by_date = index_minute_bars_by_key.get(index_key, {})
    previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
    if previous_close is None:
        return False

    reversal_start_hm = (
        MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
        + MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
    )
    decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    has_traded_above = False
    has_traded_below = False

    for bar in today_bars:
        bar_dt = bar.get('dt')
        if bar_dt is None:
            continue
        bar_hm = bar_dt.hour * 60 + bar_dt.minute
        if bar_hm < reversal_start_hm or bar_hm >= decision_hm:
            continue

        high = bar.get('high')
        low = bar.get('low')
        if high is not None and float(high) > previous_close:
            has_traded_above = True
        if low is not None and float(low) < previous_close:
            has_traded_below = True
        if has_traded_above and has_traded_below:
            return True

    return False


def has_complete_market_decision_data(
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> bool:
    """兩個市場指數在模式判斷區間內都需具備完整 OHLC 分 K，否則本日不交易。"""
    decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    for index_key in ('TWSE:MARKET', 'TPEX:MARKET'):
        bars_by_date = index_minute_bars_by_key.get(index_key, {})
        if get_previous_trading_day_last_close(bars_by_date, target_date) is None:
            return False

        minute_keys_with_required_fields = set()
        today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
        for bar in today_bars:
            bar_dt = bar.get('dt')
            if bar_dt is None:
                continue
            bar_hm = bar_dt.hour * 60 + bar_dt.minute
            if bar_hm >= decision_hm:
                continue
            if any(bar.get(field) is None for field in ('open', 'high', 'low', 'close')):
                continue
            minute_keys_with_required_fields.add(bar_hm)

        if set(range(9 * 60, decision_hm)) - minute_keys_with_required_fields:
            return False

    return True




def get_market_index_strategy_decision_gate_detail(
    index_key: str,
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> tuple[str, datetime | None]:
    """依是否曾跌破及決策時間前最後 close 回傳單一指數的 lower 狀態。"""
    drop_percent = get_strategy_decision_drop_percent(index_key)
    rebound_percent = get_strategy_decision_rebound_percent(index_key)
    if drop_percent is None or rebound_percent is None:
        return GATE_NOT_PASSED, None

    bars_by_date = index_minute_bars_by_key.get(index_key, {})
    if not bars_by_date:
        return GATE_NOT_PASSED, None

    previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
    if previous_close is None:
        return GATE_NOT_PASSED, None

    threshold = previous_close * (1 - drop_percent / 100.0)
    rebound_threshold = previous_close * (1 - rebound_percent / 100.0)
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
    valid_bars = [
        bar
        for bar in today_bars
        if bar.get('dt') is not None
        and (bar['dt'].hour * 60 + bar['dt'].minute) < decision_hm
    ]
    sorted_bars = sorted(valid_bars, key=lambda item: item['dt'])
    close_bars = [bar for bar in sorted_bars if bar.get('close') is not None]
    if not close_bars:
        return GATE_NOT_PASSED, None

    break_bars = [
        bar for bar in sorted_bars
        if bar.get('low') is not None and float(bar['low']) < threshold
    ]
    if not break_bars:
        return GATE_NOT_PASSED, None

    final_close = float(close_bars[-1]['close'])
    if final_close < rebound_threshold:
        return GATE_LOWER_PASSED, break_bars[0]['dt']
    return GATE_NOT_PASSED, break_bars[0]['dt']


def has_ix0001_early_lower_breakout(
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> bool:
    """IX0001 在早盤截止時間（含）前曾嚴格向下突破 LOWER 門檻即回傳 True。"""
    index_key = 'TWSE:MARKET'
    bars_by_date = index_minute_bars_by_key.get(index_key, {})
    previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
    if previous_close is None:
        return False

    drop_percent = get_strategy_decision_drop_percent(index_key)
    if drop_percent is None:
        return False

    drop_threshold = previous_close * (1 - drop_percent / 100.0)
    start_hm = (
        MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
        + MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
    )
    deadline_hm = (
        STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
        + STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
    )
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    for bar in today_bars:
        bar_dt = bar.get('dt')
        if bar_dt is None:
            continue
        bar_hm = bar_dt.hour * 60 + bar_dt.minute
        if bar_hm < start_hm or bar_hm > deadline_hm:
            continue

        low = bar.get('low')
        if low is not None and float(low) < drop_threshold:
            return True
    return False


def get_strategy_market_decision_gate_status(
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> str:
    """依兩指數歷史觸發與 STRATEGY_DECISION 前最後 close 決定最終模式。"""
    if not has_complete_market_decision_data(
        target_date,
        index_minute_bars_by_key,
    ):
        return GATE_DATA_INCOMPLETE

    if not has_ix0001_early_lower_breakout(
        target_date,
        index_minute_bars_by_key,
    ):
        return GATE_NO_TRADE

    if any(
        has_market_index_crossed_both_sides_of_previous_close(
            index_key,
            target_date,
            index_minute_bars_by_key,
        )
        for index_key in ('TWSE:MARKET', 'TPEX:MARKET')
    ):
        return GATE_NO_TRADE

    lower_details = [
        get_market_index_strategy_decision_gate_detail(
            index_key,
            target_date,
            index_minute_bars_by_key,
        )
        for index_key in ('TWSE:MARKET', 'TPEX:MARKET')
    ]
    both_ever_dropped = all(break_dt is not None for _, break_dt in lower_details)
    if not both_ever_dropped:
        return GATE_NO_TRADE
    if all(status == GATE_LOWER_PASSED for status, _ in lower_details):
        return GATE_LOWER_PASSED
    return GATE_NO_TRADE


def is_industry_market_filter_passed(
    stock_item: tuple,
    target_date: date,
    entry_dt: datetime,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    strategy_type: str,
) -> bool:
    """依策略方向檢查 entry 當下產業指數與前一營業日收盤價的相對位置。"""
    index_key = get_industry_index_key(stock_item)
    if index_key is None:
        return False

    index_bars_by_date = index_minute_bars_by_key.get(index_key, {})
    if not index_bars_by_date:
        return False

    previous_reference = get_previous_trading_day_last_close(index_bars_by_date, target_date)
    if previous_reference is None:
        return False

    today_key = target_date.strftime('%Y-%m-%d')
    today_index_bars = index_bars_by_date.get(today_key, [])
    if not today_index_bars:
        return False

    entry_index_bar = find_bar_at_or_before(today_index_bars, entry_dt)
    if entry_index_bar is None:
        return False

    try:
        entry_index_close = float(entry_index_bar['close'])
    except (KeyError, TypeError, ValueError):
        return False

    if strategy_type != STRATEGY_LOWER:
        return False
    threshold = previous_reference * (1 + INDUSTRY_MARKET_FILTER_MAX_UP_PERCENT / 100.0)
    return entry_index_close < threshold


def is_market_reversal_blocked_at_entry(
    target_date: date,
    entry_dt: datetime,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    strategy_type: str,
) -> bool:
    """入場當下任一市場指數 close 回到模式失效門檻即封鎖；缺資料亦封鎖。"""
    for index_key in ('TWSE:MARKET', 'TPEX:MARKET'):
        bars_by_date = index_minute_bars_by_key.get(index_key, {})
        previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
        today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
        entry_bar = find_bar_at_or_before(today_bars, entry_dt)
        if previous_close is None or entry_bar is None or entry_bar.get('close') is None:
            return True

        index_close = float(entry_bar['close'])
        if strategy_type != STRATEGY_LOWER:
            return True
        rebound_percent = get_strategy_decision_rebound_percent(index_key)
        if rebound_percent is None:
            return True
        rebound_threshold = previous_close * (1 - rebound_percent / 100.0)
        if index_close >= rebound_threshold:
            return True
    return False


def find_market_reversal_trigger_dt(
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    strategy_type: str,
) -> datetime | None:
    """
    自模式進場時間起至盤中比較截止前，找出任一市場指數首次觸及失效門檻的時間。
    每分鐘須同時具備上市及上櫃指數所需欄位，資料不完整的分鐘不判定。
    """
    if strategy_type != STRATEGY_LOWER:
        return None
    start_time = STRATEGY_START_LOWER
    end_time = INTRADAY_COMPARE_END_LOWER
    value_field = 'high'

    bars_by_index: dict[str, dict[tuple[int, int], dict]] = {}
    previous_close_by_index: dict[str, float] = {}
    today_key = target_date.strftime('%Y-%m-%d')
    start_hm = start_time[0] * 60 + start_time[1]
    end_hm = end_time[0] * 60 + end_time[1]

    for index_key in ('TWSE:MARKET', 'TPEX:MARKET'):
        bars_by_date = index_minute_bars_by_key.get(index_key, {})
        previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
        if previous_close is None:
            return None
        previous_close_by_index[index_key] = previous_close

        minute_bars = {}
        for bar in bars_by_date.get(today_key, []):
            bar_dt = bar.get('dt')
            if bar_dt is None or bar.get(value_field) is None:
                continue
            bar_hm = bar_dt.hour * 60 + bar_dt.minute
            if start_hm <= bar_hm < end_hm:
                minute_bars[(bar_dt.hour, bar_dt.minute)] = bar
        bars_by_index[index_key] = minute_bars

    common_minutes = sorted(
        set(bars_by_index['TWSE:MARKET']) & set(bars_by_index['TPEX:MARKET'])
    )
    for minute_key in common_minutes:
        for index_key in ('TWSE:MARKET', 'TPEX:MARKET'):
            bar = bars_by_index[index_key][minute_key]
            previous_close = previous_close_by_index[index_key]
            index_value = float(bar[value_field])
            rebound_percent = get_strategy_decision_rebound_percent(index_key)
            if rebound_percent is None:
                continue
            threshold = previous_close * (1 - rebound_percent / 100.0)
            triggered = index_value >= threshold
            if triggered:
                return bar['dt']
    return None


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


def calculate_net_pnl_for_long_trade(entry_price: float, exit_price: float) -> tuple[float, float]:
    """計算作多交易每股淨損益與總交易成本。"""
    buy_side_cost = entry_price * BROKERAGE_FEE_RATE
    sell_side_cost = exit_price * (BROKERAGE_FEE_RATE + SELL_TRANSACTION_TAX_RATE)
    total_cost = buy_side_cost + sell_side_cost
    gross_pnl = exit_price - entry_price
    net_pnl = gross_pnl - total_cost
    return round(net_pnl, 4), round(total_cost, 4)


def build_outcome_result(
    exit_reason: str,
    entry_price: float,
    exit_price: float,
    trade_side: str = TRADE_SIDE_SHORT,
) -> dict:
    """根據進出場價格建立結果，成功/失敗以淨損益正負判斷。"""
    if trade_side == TRADE_SIDE_LONG:
        net_pnl, total_cost = calculate_net_pnl_for_long_trade(entry_price, exit_price)
    else:
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


def has_enough_minute_bars_before_0930(today_bars: list) -> bool:
    """09:30 前分 K 少於設定根數者視為延遲撮合股票，排除當日交易。"""
    before_0930_count = sum(
        1
        for bar in today_bars
        if (dtv := bar.get('dt')) is not None
        and (dtv.hour, dtv.minute) < (9, 30)
    )
    return before_0930_count >= MIN_MINUTE_BARS_BEFORE_0930


def scan_entry_signal_lower(
    today_bars: list,
    ystats: dict,
):
    """
    作空進場訊號：
    1) 進場檢查時間為 STRATEGY_START_LOWER 到 STRATEGY_END_LOWER（含起訖）
    2) 取時間窗內第一根 low 落在入場區間的 K 棒作為進場 K 棒
    3) 進場價 = 進場分K棒 low
    回傳：
    - (entry_bar, entry_price): 條件成立
    - None: 未出現符合入場區間的 K 棒
    """
    start_hm = STRATEGY_START_LOWER[0] * 60 + STRATEGY_START_LOWER[1]
    end_hm = STRATEGY_END_LOWER[0] * 60 + STRATEGY_END_LOWER[1]
    yesterday_close = float(ystats['close'])
    _, limit_down_price = calculate_limit_prices(yesterday_close)
    entry_lower_bound, entry_upper_bound = calculate_entry_range_bounds(
        yesterday_close,
        limit_down_price,
        LOWER_ENTRY_RANGE_START_PERCENT,
        LOWER_ENTRY_RANGE_END_PERCENT,
    )

    indexed_bars = []
    for idx, bar in enumerate(today_bars):
        dtv = bar.get('dt')
        if dtv is None:
            continue
        hm = dtv.hour * 60 + dtv.minute
        indexed_bars.append((idx, bar, hm))
    indexed_bars.sort(key=lambda item: item[1]['dt'])

    for original_idx, bar, hm in indexed_bars:
        if bar.get('low') is None:
            continue

        if not (start_hm <= hm <= end_hm):
            continue

        entry_price = float(bar['low'])
        if not is_price_in_entry_range(entry_price, entry_lower_bound, entry_upper_bound):
            continue

        return bar, entry_price
    return None




def normalize_daily_candles(raw_day_bars: list, stock_name: str) -> dict[date, dict]:
    """將日 K 轉為 date -> OHLC，供漲跌停序列判斷使用。"""
    daily_map: dict[date, dict] = {}
    for item in raw_day_bars:
        raw_date = str(item.get('date', 'N/A')) if isinstance(item, dict) else 'N/A'
        try:
            day_dt = datetime.strptime(str(item.get('date', ''))[:10], '%Y-%m-%d').date()
            daily_map[day_dt] = {
                'open': float(item['open']),
                'high': float(item['high']),
                'low': float(item['low']),
                'close': float(item['close']),
            }
        except Exception as exc:
            reason = f'{type(exc).__name__}: {exc}'
            DAILY_CANDLE_DATA_ISSUES.add((stock_name, raw_date, reason))
            continue
    return daily_map


def is_same_price(left: float, right: float) -> bool:
    return math.isclose(float(left), float(right), abs_tol=1e-9)


def has_limit_sequence_before_date(
    stock_name: str,
    target_date: date,
    day_candles_by_symbol: dict[str, list],
    allowed_days: list[int],
    direction: str,
) -> bool:
    """判斷 target_date 前的實際連續漲跌停天數是否恰好在允許清單內。"""
    if not isinstance(allowed_days, list):
        raise ValueError(f'{direction}連續天數必須是陣列: {allowed_days}')
    if any(type(days) is not int or days < 1 for days in allowed_days):
        raise ValueError(f'{direction}連續天數只能包含正整數: {allowed_days}')
    if not allowed_days:
        return False

    daily_map = normalize_daily_candles(
        day_candles_by_symbol.get(stock_name, []),
        stock_name,
    )
    previous_dates = sorted(day_key for day_key in daily_map if day_key < target_date)
    if len(previous_dates) < 2:
        return False

    actual_days = 0
    for index in range(len(previous_dates) - 1, 0, -1):
        current_date = previous_dates[index]
        previous_date = previous_dates[index - 1]
        current = daily_map[current_date]
        previous = daily_map[previous_date]
        limit_up_price, limit_down_price = calculate_limit_prices(previous['close'])
        limit_price = limit_up_price if direction == STRATEGY_LIMIT_UP else limit_down_price
        if not is_same_price(current['close'], limit_price):
            break
        actual_days += 1
    return actual_days in allowed_days


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
    trade_side: str,
    market_reversal_trigger_dt: datetime | None,
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
        'trade_side': trade_side,
        'market_reversal_trigger_dt': market_reversal_trigger_dt,
    }


# ---------------------------------------------------------------------------
# Phase 4 (US2) — 策略結果評估
# ---------------------------------------------------------------------------

def format_result_line(stock_name: str, date_str: str,
                       signal: dict | None, result: dict | None) -> str:
    """格式化單支股票的輸出行。"""
    if signal is None or result is None:
        return ''

    stock_label = format_stock_label(stock_name, signal.get('industry_code'))
    outcome = result['outcome']
    exit_reason = result.get('exit_reason')
    signed = result.get('signed_pnl', signed_pnl(result))
    status_label = '成功' if outcome == 'success' else '失敗'

    if exit_reason == 'target':
        return f'{stock_label} / {date_str} / {status_label}(已達獲利, 淨損益: {signed:+.2f})'
    if exit_reason == 'stop':
        return f'{stock_label} / {date_str} / {status_label}(已達停損, 淨損益: {signed:+.2f})'
    if exit_reason == 'close':
        return f'{stock_label} / {date_str} / {status_label}(收盤結算, 淨損益: {signed:+.2f})'

    return ''


def format_stock_label(stock_name: str, industry_code: str | None) -> str:
    """股票名稱後加上產業別代碼。"""
    if not industry_code:
        return stock_name
    return f"{stock_name} '{industry_code}'"


def summarize_industry_results(all_results: list) -> list[tuple[str, str, int, int, float]]:
    """依市場別與產業別代碼統計成功/失敗與損益。"""
    grouped: dict[tuple[str, str], dict[str, float | int]] = {}
    for signal, result in all_results:
        if not signal or not result:
            continue
        exchange_suffix = get_exchange_suffix_for_stock(signal['name'])
        industry_code = signal.get('industry_code', '')
        if not exchange_suffix or not industry_code:
            continue
        key = (exchange_suffix, industry_code)
        bucket = grouped.setdefault(
            key,
            {'successes': 0, 'failures': 0, 'total_pnl': 0.0},
        )
        if result.get('outcome') == 'success':
            bucket['successes'] += 1
        elif result.get('outcome') == 'fail':
            bucket['failures'] += 1
        bucket['total_pnl'] += signed_pnl(result)

    market_order = {'TWO': 0, 'TW': 1}
    return [
        (
            exchange_suffix,
            industry_code,
            int(values['successes']),
            int(values['failures']),
            float(values['total_pnl']),
        )
        for (exchange_suffix, industry_code), values in sorted(
            grouped.items(),
            key=lambda item: (
                market_order.get(item[0][0], 99),
                item[0][1],
            ),
        )
    ]


def signed_pnl(result: dict | None) -> float:
    """將策略結果轉成帶方向的損益值。"""
    if not result:
        return 0.0

    if 'signed_pnl' in result:
        return result['signed_pnl']

    if 'pnl' not in result:
        return 0.0

    return result['pnl'] if result.get('outcome') == 'success' else -result['pnl']


def should_include_strategy_in_print_stats(strategy_type: str | None) -> bool:
    """依打印統計開關判斷策略模式是否納入輸出。"""
    if strategy_type == STRATEGY_LOWER:
        return INCLUDE_LOWER_IN_PRINT_STATS
    if strategy_type == STRATEGY_LIMIT_DOWN:
        return INCLUDE_LIMIT_DOWN_IN_PRINT_STATS
    if strategy_type == STRATEGY_LIMIT_UP:
        return INCLUDE_LIMIT_UP_IN_PRINT_STATS
    return True


def filter_results_for_print_stats(all_results: list) -> list:
    """回傳納入打印統計的交易結果。"""
    return [
        (signal, result)
        for signal, result in all_results
        if signal
        and result
        and should_include_strategy_in_print_stats(signal.get('strategy_type'))
    ]


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


def print_daily_candle_data_issues() -> None:
    """在每日交易結果前集中印出被跳過的異常日 K。"""
    if not DAILY_CANDLE_DATA_ISSUES:
        return

    print('========== 日K資料異常 ==========')
    for stock_name, raw_date, reason in sorted(DAILY_CANDLE_DATA_ISSUES):
        print(
            f'股票={stock_name}  原始日期={raw_date}  原因={reason}  '
            '影響=該筆日K未納入 LIMIT_UP / LIMIT_DOWN 連續天數判斷'
        )
    print('========== 日K資料異常結束 ==========')
    print('')


def print_daily_optimization_results(
    all_results: list,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    market_start_gate_cache: dict[date, str],
) -> None:
    """印出固定參數下依日期彙總的進出場明細。"""
    print_daily_candle_data_issues()
    print_results = filter_results_for_print_stats(all_results)
    print(
        f'損益已納入交易成本: 手續費率={BROKERAGE_FEE_RATE:.6f}, '
        f'賣出交易稅率={SELL_TRANSACTION_TAX_RATE:.6f}'
    )
    print('')

    summary = summarize_results(print_results)
    gate_status_by_date_key = {
        current_date.strftime('%Y-%m-%d'): gate_status
        for current_date, gate_status in market_start_gate_cache.items()
    }
    strategy_type_by_gate_status = {
        GATE_LOWER_PASSED: STRATEGY_LOWER,
        GATE_NO_TRADE: STRATEGY_NO_TRADE,
        GATE_DATA_INCOMPLETE: STRATEGY_NO_TRADE,
    }
    lower_gate_date_keys = {
        current_date.strftime('%Y-%m-%d')
        for current_date, gate_status in market_start_gate_cache.items()
        if gate_status == GATE_LOWER_PASSED and INCLUDE_LOWER_IN_PRINT_STATS
    }
    if summary['total'] == 0 and not lower_gate_date_keys:
        print('固定參數下沒有交易結果。')
        return

    grouped_results: dict[tuple[str, str], list[tuple[str, str, datetime | None, datetime | None, float, float, float, bool, list[float], float | None]]] = {}
    for signal, result in print_results:
        if not signal or not result:
            continue
        date_key = signal['date_str']
        strategy_type = signal.get('strategy_type', STRATEGY_NO_TRADE)
        grouped_results.setdefault((date_key, strategy_type), []).append((
            signal['name'],
            signal.get('industry_code', ''),
            signal.get('entry_dt'),
            result.get('exit_dt'),
            signal['entry_price'],
            result['exit_price'],
            signed_pnl(result),
            result.get('outcome') == 'success',
            signal.get('previous_close'),
        ))

    normal_gate_group_keys = {
        (
            date_key,
            strategy_type_by_gate_status.get(
                gate_status_by_date_key.get(date_key),
                STRATEGY_NO_TRADE,
            ),
        )
        for date_key in lower_gate_date_keys
    }
    daily_group_keys = set(grouped_results) | normal_gate_group_keys
    daily_date_keys = {date_key for date_key, _ in daily_group_keys}
    strategy_order = {
        STRATEGY_LOWER: 0,
        STRATEGY_LIMIT_DOWN: 1,
        STRATEGY_LIMIT_UP: 1,
        STRATEGY_NO_TRADE: 2,
    }
    for date_key in sorted(daily_date_keys, reverse=True):
        date_group_keys = sorted(
            (key for key in daily_group_keys if key[0] == date_key),
            key=lambda key: (strategy_order.get(key[1], 99), key[1]),
        )
        for _, strategy_type in date_group_keys:
            day_rows = grouped_results.get((date_key, strategy_type), [])
            day_total_cost = sum(row[4] for row in day_rows)
            day_total = sum(row[6] for row in day_rows)
            day_successes = sum(1 for row in day_rows if row[7])
            day_failures = len(day_rows) - day_successes
            target_date = datetime.strptime(date_key, '%Y-%m-%d').date()
            market_open_text = ''
            if strategy_type in (
                STRATEGY_LIMIT_DOWN,
                STRATEGY_LIMIT_UP,
            ):
                entry_dt_values = [row[2] for row in day_rows if row[2] is not None]
                market_open_entry_dt = min(entry_dt_values) if entry_dt_values else None
                market_open_text = (
                    build_market_open_summary_at_entry(
                        target_date,
                        market_open_entry_dt,
                        index_minute_bars_by_key,
                    )
                    + '  '
                )
            print(
                f'{format_date_with_weekday(date_key)} '
                f'模式={strategy_type}  '
                f'{market_open_text}'
                f'筆數={len(day_rows)}  '
                f'成功={day_successes}  '
                f'失敗={day_failures}  '
                f'總支出={day_total_cost:.2f}  '
                f'總收益={day_total:+.2f}'
            )
            if strategy_type == STRATEGY_LOWER and date_key in lower_gate_date_keys:
                for index_summary in build_market_index_daily_low_summary(
                    target_date,
                    index_minute_bars_by_key,
                ):
                    print(index_summary)
            for stock_name, industry_code, entry_dt, exit_dt, entry_price, exit_price, pnl_value, _, previous_close in sorted(
                day_rows,
                key=lambda row: (format_entry_time(row[2]), row[0]),
            ):
                previous_close_text = ''
                if strategy_type in (
                    STRATEGY_LIMIT_DOWN,
                    STRATEGY_LIMIT_UP,
                ):
                    if previous_close is not None:
                        previous_close_text = f' {previous_close:.2f}'
                print(
                    f'{format_stock_label(stock_name, industry_code)} '
                    f'{format_entry_time(entry_dt)} {format_entry_time(exit_dt)}{previous_close_text} '
                    f'[{entry_price:.2f}|{exit_price:.2f}|{pnl_value:.2f}]'
                )
            print('')

    for exchange_suffix, industry_code, successes, failures, pnl_value in summarize_industry_results(print_results):
        print(
            f'{exchange_suffix} {industry_code} '
            f'成功數={successes}  '
            f'失敗數={failures} '
            f'收益統計={pnl_value:+.2f}'
        )
    print('')

    print(
        f'有結果總筆數={summary["total"]}  '
        f'成功總數={summary["successes"]}  '
        f'失敗總數={summary["failures"]}  '
        f'總收益統計={summary["total_pnl"]:+.2f}'
    )
    print(
        f'LIMIT_DOWN_LOSS_PER={OPTIMIZE_LOSS_PER_LIMIT_DOWN:.1f}%  '
        f'LIMIT_DOWN_PROFIT_PER={OPTIMIZE_PROFIT_PER_LIMIT_DOWN:.1f}%  '
        f'LIMIT_UP_LOSS_PER={OPTIMIZE_LOSS_PER_LIMIT_UP:.1f}%  '
        f'LIMIT_UP_PROFIT_PER={OPTIMIZE_PROFIT_PER_LIMIT_UP:.1f}%  '
        f'LOWER_LOSS_PER={OPTIMIZE_LOSS_PER_LOWER:.1f}%  '
        f'LOWER_PROFIT_PER={OPTIMIZE_PROFIT_PER_LOWER:.1f}%'
    )
    print(
        f'INCLUDE_LOWER_IN_PRINT_STATS={INCLUDE_LOWER_IN_PRINT_STATS}  '
        f'INCLUDE_LIMIT_DOWN_IN_PRINT_STATS={INCLUDE_LIMIT_DOWN_IN_PRINT_STATS}  '
        f'INCLUDE_LIMIT_UP_IN_PRINT_STATS={INCLUDE_LIMIT_UP_IN_PRINT_STATS}'
    )
    print(
        f'LOWER_ENTRY_RANGE={LOWER_ENTRY_RANGE_START_PERCENT:.1f}%~{LOWER_ENTRY_RANGE_END_PERCENT:.1f}%'
    )
    print(
        f'MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME='
        f'{MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0]:02d}:'
        f'{MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]:02d}  '
        f'STRATEGY_EARLY_BREAKOUT_DEADLINE='
        f'{STRATEGY_EARLY_BREAKOUT_DEADLINE[0]:02d}:'
        f'{STRATEGY_EARLY_BREAKOUT_DEADLINE[1]:02d}  '
        f'STRATEGY_DECISION={STRATEGY_DECISION[0]:02d}:{STRATEGY_DECISION[1]:02d}'
    )
    print(
        f'LOWER進場時間窗={STRATEGY_START_LOWER[0]:02d}:{STRATEGY_START_LOWER[1]:02d}~{STRATEGY_END_LOWER[0]:02d}:{STRATEGY_END_LOWER[1]:02d}    '
        f'LOWER出場時間窗={INTRADAY_COMPARE_END_LOWER[0]:02d}:{INTRADAY_COMPARE_END_LOWER[1]:02d}'
    )
    print(
        f'LIMIT_DOWN允許的實際連續跌停天數={SHORT_LIMIT_DOWN_DAYS}  '
        f'LIMIT_DOWN進場=第一根分K open    '
        f'LIMIT_DOWN出場時間窗={INTRADAY_COMPARE_END_LIMIT_DOWN[0]:02d}:{INTRADAY_COMPARE_END_LIMIT_DOWN[1]:02d}'
    )
    print(
        f'LIMIT_UP允許的實際連續漲停天數={LONG_LIMIT_UP_DAYS}  '
        f'LIMIT_UP進場=第一根分K open    '
        f'LIMIT_UP出場時間窗={INTRADAY_COMPARE_END_LIMIT_UP[0]:02d}:{INTRADAY_COMPARE_END_LIMIT_UP[1]:02d}'
    )


# ---------------------------------------------------------------------------
# Core orchestration — analyze_stock
# ---------------------------------------------------------------------------

def find_limit_candidate_on_date(
    stock_item: tuple,
    target_date: date,
    bars_by_date: dict,
    day_candles_by_symbol: dict[str, list],
    strategy_type: str,
    sequence_already_confirmed: bool = False,
):
    """判斷連續漲跌停後次一交易日的 limit 獨立策略候選交易。"""
    stock_name = stock_item[0]
    if strategy_type == STRATEGY_LIMIT_UP:
        if not sequence_already_confirmed and not has_limit_sequence_before_date(
            stock_name,
            target_date,
            day_candles_by_symbol,
            LONG_LIMIT_UP_DAYS,
            STRATEGY_LIMIT_UP,
        ):
            return None
    elif strategy_type == STRATEGY_LIMIT_DOWN:
        if not sequence_already_confirmed and not has_limit_sequence_before_date(
            stock_name,
            target_date,
            day_candles_by_symbol,
            SHORT_LIMIT_DOWN_DAYS,
            STRATEGY_LIMIT_DOWN,
        ):
            return None
    else:
        return None

    today_bars, yesterday_bars = get_target_and_yesterday(bars_by_date, target_date)
    if not today_bars or not yesterday_bars:
        return None
    if not has_enough_minute_bars_before_0930(today_bars):
        return None

    today_ordered = sorted(
        (bar for bar in today_bars if bar.get('dt') is not None),
        key=lambda bar: bar['dt'],
    )
    ystats = compute_yesterday_stats(yesterday_bars)
    previous_close = float(ystats['close'])
    limit_up_price, limit_down_price = calculate_limit_prices(ystats['close'])

    first_bar, _ = find_first_bar(today_ordered)
    pair = None

    if strategy_type == STRATEGY_LIMIT_UP:
        if first_bar is not None and first_bar.get('open') is not None:
            pair = first_bar, float(first_bar['open'])
        intraday_compare_end = INTRADAY_COMPARE_END_LIMIT_UP
        trade_side = TRADE_SIDE_LONG
    else:
        if first_bar is not None and first_bar.get('open') is not None:
            pair = first_bar, float(first_bar['open'])
        intraday_compare_end = INTRADAY_COMPARE_END_LIMIT_DOWN
        trade_side = TRADE_SIDE_SHORT

    if pair is None:
        return None

    entry_bar, entry_price = pair
    candidate = build_trade_candidate(
        stock_name,
        get_stock_industry_code(stock_item),
        target_date,
        entry_bar,
        entry_price,
        today_ordered,
        limit_up_price,
        limit_down_price,
        strategy_type,
        intraday_compare_end,
        trade_side,
        None,
    )
    candidate['previous_close'] = previous_close
    return candidate


def find_trade_candidate_on_date(
    stock_item: tuple,
    target_date: date,
    bars_by_date: dict,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    strategy_type: str,
    market_reversal_trigger_dt: datetime | None,
):
    """找出單日候選交易；無訊號則回傳 None。"""
    stock_name = stock_item[0]
    today_bars, yesterday_bars = get_target_and_yesterday(bars_by_date, target_date)
    if not today_bars or not yesterday_bars:
        return None

    ystats = compute_yesterday_stats(yesterday_bars)
    limit_up_price, limit_down_price = calculate_limit_prices(ystats['close'])

    if not has_enough_minute_bars_before_0930(today_bars):
        return None

    if strategy_type == STRATEGY_LOWER:
        pair = scan_entry_signal_lower(
            today_bars,
            ystats,
        )
        intraday_compare_end = INTRADAY_COMPARE_END_LOWER
        trade_side = TRADE_SIDE_SHORT
    else:
        return None
    if pair is None:
        return None

    entry_bar, entry_price = pair
    if (
        market_reversal_trigger_dt is not None
        and market_reversal_trigger_dt <= entry_bar['dt']
    ):
        return None
    if is_market_reversal_blocked_at_entry(
        target_date,
        entry_bar['dt'],
        index_minute_bars_by_key,
        strategy_type,
    ):
        return None
    if not is_industry_market_filter_passed(
        stock_item,
        target_date,
        entry_bar['dt'],
        index_minute_bars_by_key,
        strategy_type,
    ):
        return None

    return build_trade_candidate(
        stock_name,
        get_stock_industry_code(stock_item),
        target_date,
        entry_bar,
        entry_price,
        today_bars,
        limit_up_price,
        limit_down_price,
        strategy_type,
        intraday_compare_end,
        trade_side,
        market_reversal_trigger_dt,
    )


def collect_trade_candidates(
    stock_item: tuple,
    target_date: date,
    minute_bars_by_symbol: dict[str, dict[str, list]],
    day_candles_by_symbol: dict[str, list],
    index_minute_bars_by_key: dict[str, dict[str, list]],
    market_start_gate_cache: dict[date, str],
    market_reversal_cache: dict[tuple[date, str], datetime | None],
    enable_general_strategies: bool = True,
    enable_limit_up_strategy: bool = False,
    enable_limit_down_strategy: bool = False,
) -> list:
    """逐日先執行強制 LIMIT 策略；未符合 LIMIT 序列時才執行一般策略。"""
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
        limit_up_forced = enable_limit_up_strategy and has_limit_sequence_before_date(
            stock_name,
            current_date,
            day_candles_by_symbol,
            LONG_LIMIT_UP_DAYS,
            STRATEGY_LIMIT_UP,
        )
        limit_down_forced = enable_limit_down_strategy and has_limit_sequence_before_date(
            stock_name,
            current_date,
            day_candles_by_symbol,
            SHORT_LIMIT_DOWN_DAYS,
            STRATEGY_LIMIT_DOWN,
        )
        if limit_up_forced or limit_down_forced:
            forced_strategy_type = (
                STRATEGY_LIMIT_UP if limit_up_forced else STRATEGY_LIMIT_DOWN
            )
            limit_candidate = find_limit_candidate_on_date(
                stock_item,
                current_date,
                bars_by_date,
                day_candles_by_symbol,
                forced_strategy_type,
                sequence_already_confirmed=True,
            )
            if limit_candidate is not None:
                candidates.append(limit_candidate)
            # LIMIT 屬強制策略；序列成立後即使沒有進場訊號，也不再執行其他策略。
            continue

        if not enable_general_strategies:
            continue

        if current_date not in market_start_gate_cache:
            market_start_gate_cache[current_date] = get_strategy_market_decision_gate_status(
                current_date,
                index_minute_bars_by_key,
            )
        gate_status = market_start_gate_cache[current_date]
        if gate_status != GATE_LOWER_PASSED:
            continue
        strategy_type = STRATEGY_LOWER

        reversal_cache_key = (current_date, strategy_type)
        if reversal_cache_key not in market_reversal_cache:
            market_reversal_cache[reversal_cache_key] = find_market_reversal_trigger_dt(
                current_date,
                index_minute_bars_by_key,
                strategy_type,
            )
        market_reversal_trigger_dt = market_reversal_cache[reversal_cache_key]

        candidate = find_trade_candidate_on_date(
            stock_item,
            current_date,
            bars_by_date,
            index_minute_bars_by_key,
            strategy_type,
            market_reversal_trigger_dt,
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
        trade_side = candidate.get('trade_side', TRADE_SIDE_SHORT)
        market_reversal_trigger_dt = candidate.get('market_reversal_trigger_dt')
        if strategy_type == STRATEGY_LOWER:
            optimize_loss_percent = OPTIMIZE_LOSS_PER_LOWER
            optimize_profit_percent = OPTIMIZE_PROFIT_PER_LOWER
        elif strategy_type == STRATEGY_LIMIT_DOWN:
            optimize_loss_percent = OPTIMIZE_LOSS_PER_LIMIT_DOWN
            optimize_profit_percent = OPTIMIZE_PROFIT_PER_LIMIT_DOWN
        elif strategy_type == STRATEGY_LIMIT_UP:
            optimize_loss_percent = OPTIMIZE_LOSS_PER_LIMIT_UP
            optimize_profit_percent = OPTIMIZE_PROFIT_PER_LIMIT_UP
        else:
            continue

        try:
            entry_idx = dt_values.index(entry_dt)
        except ValueError:
            continue

        effective_stop_loss = calculate_stop_loss_amount_by_percent(entry_price, optimize_loss_percent)
        effective_profit = calculate_take_profit_amount_by_percent(entry_price, optimize_profit_percent)
        if not is_independent_limit_strategy(strategy_type):
            if has_limit_price_touched_before_entry(
                high_values,
                low_values,
                entry_idx,
                limit_up_price,
                limit_down_price,
            ):
                continue
            if trade_side == TRADE_SIDE_LONG and should_skip_entry_by_limit_down(
                entry_price,
                effective_stop_loss,
                limit_down_price,
            ):
                continue
            if trade_side == TRADE_SIDE_SHORT and should_skip_entry_by_limit_up(
                entry_price,
                effective_stop_loss,
                limit_up_price,
            ):
                continue

        if trade_side == TRADE_SIDE_LONG:
            raw_take_profit_price = entry_price + effective_profit
            raw_stop_loss_price = entry_price - effective_stop_loss
            # 作多：停利須至少達標，向上取 tick；停損觸發後以較保守的向下 tick 出場。
            adjusted_take_profit_price = ceil_price_to_tick(
                raw_take_profit_price,
                get_tick_size(raw_take_profit_price),
            )
            adjusted_stop_loss_price = floor_price_to_tick(
                raw_stop_loss_price,
                get_tick_size(raw_stop_loss_price),
            )
            take_profit_price = min(adjusted_take_profit_price, limit_up_price)
            stop_loss_price = max(adjusted_stop_loss_price, limit_down_price)
        else:
            raw_take_profit_price = entry_price - effective_profit
            raw_stop_loss_price = entry_price + effective_stop_loss
            # 作空：停利須至少達標，向下取 tick；停損觸發後以較保守的向上 tick 回補。
            adjusted_take_profit_price = floor_price_to_tick(
                raw_take_profit_price,
                get_tick_size(raw_take_profit_price),
            )
            adjusted_stop_loss_price = ceil_price_to_tick(
                raw_stop_loss_price,
                get_tick_size(raw_stop_loss_price),
            )
            take_profit_price = max(adjusted_take_profit_price, limit_down_price)
            stop_loss_price = min(adjusted_stop_loss_price, limit_up_price)

        signal = {
            'name': name,
            'industry_code': industry_code,
            'date_str': date_str,
            'entry_dt': entry_dt,
            'entry_price': entry_price,
            'strategy_type': strategy_type,
            'trade_side': trade_side,
            'previous_close': candidate.get('previous_close'),
            'take_profit_price': take_profit_price,
            'effective_profit': effective_profit,
            'effective_stop_loss': effective_stop_loss,
            'stop_loss_price': stop_loss_price,
        }
        result = None
        first_exit_check_idx = (
            entry_idx
            if is_independent_limit_strategy(strategy_type)
            else entry_idx + 1
        )
        for i in range(first_exit_check_idx, len(dt_values)):
            bar_time = dt_values[i]
            if (
                market_reversal_trigger_dt is not None
                and bar_time >= market_reversal_trigger_dt
            ):
                market_exit_idx = next(
                    (
                        exit_idx
                        for exit_idx in range(i, len(dt_values))
                        if dt_values[exit_idx] > market_reversal_trigger_dt
                    ),
                    None,
                )
                if market_exit_idx is not None:
                    result = build_outcome_result(
                        'market_reversal',
                        entry_price,
                        float(open_values[market_exit_idx]),
                        trade_side,
                    )
                    result['exit_dt'] = dt_values[market_exit_idx]
                    break
                # 已觸發市場反轉但沒有後續個股 K 棒：交由既有結算邏輯處理。
                break
            if (
                bar_time.hour > intraday_compare_end[0]
                or (
                    bar_time.hour == intraday_compare_end[0]
                    and bar_time.minute >= intraday_compare_end[1]
                )
            ):
                break
            if trade_side == TRADE_SIDE_LONG:
                if low_values[i] <= stop_loss_price:
                    result = build_outcome_result('stop', entry_price, stop_loss_price, trade_side)
                    result['exit_dt'] = dt_values[i]
                    break
                if high_values[i] >= take_profit_price:
                    result = build_outcome_result('target', entry_price, take_profit_price, trade_side)
                    result['exit_dt'] = dt_values[i]
                    break
            else:
                if high_values[i] >= stop_loss_price:
                    result = build_outcome_result('stop', entry_price, stop_loss_price, trade_side)
                    result['exit_dt'] = dt_values[i]
                    break
                if low_values[i] <= take_profit_price:
                    result = build_outcome_result('target', entry_price, take_profit_price, trade_side)
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
            exit_reason = (
                'market_reversal'
                if market_reversal_trigger_dt is not None
                and market_reversal_trigger_dt > entry_dt
                else 'close'
            )
            result = build_outcome_result(
                exit_reason,
                entry_price,
                float(open_values[settlement_idx]),
                trade_side,
            )
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
    DAILY_CANDLE_DATA_ISSUES.clear()
    builtins.print(f'開始時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    builtins.print()
    try:
        args = parse_args()
        raw_stock_list = STOCK_LIST or selected_stocks
        stock_list = filter_stocks_by_excluded_industries(raw_stock_list)
        limit_up_stock_list = filter_stocks_by_excluded_industries(
            selected_limit_up_stocks if INCLUDE_LIMIT_UP_IN_PRINT_STATS else []
        )
        limit_down_stock_list = filter_stocks_by_excluded_industries(
            selected_limit_down_stocks if INCLUDE_LIMIT_DOWN_IN_PRINT_STATS else []
        )
        analysis_stock_list = dedupe_stock_list([
            stock_list,
            limit_up_stock_list,
            limit_down_stock_list,
        ])
        ensure_backtest_industry_index_metadata(analysis_stock_list)
        excluded_stock_count = len(raw_stock_list) - len(stock_list)
        if EXCLUDED_INDUSTRY_CODES:
            print(
                f'已排除產業別: {EXCLUDED_INDUSTRY_CODES} '
                f'排除股票數={excluded_stock_count} '
                f'剩餘股票數={len(stock_list)}'
            )
        print(
            f'一般模式股票數={len(stock_list)}  '
            f'LIMIT_UP股票數={len(limit_up_stock_list)}  '
            f'LIMIT_DOWN股票數={len(limit_down_stock_list)}  '
            f'API去除重複股票後={len(analysis_stock_list)}'
        )

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
        cached = load_api_cache(cache_path, analysis_stock_list)
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
            total_stocks = len(analysis_stock_list)
            for idx, stock_item in enumerate(analysis_stock_list, start=1):
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

            required_index_keys = sorted(set(RESERVE_MARKET_INDICES) | get_required_industry_index_keys(analysis_stock_list))
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
                analysis_stock_list,
                day_candles_by_symbol,
                minute_raw_by_symbol,
                index_minute_raw_by_key,
            )
            print(f'已儲存API快取: {cache_path.name}')

        stock_strategy_assignments = build_stock_strategy_assignments(
            stock_list,
            limit_up_stock_list,
            limit_down_stock_list,
        )
        total_stocks = len(stock_strategy_assignments)

        def evaluate_one_window() -> dict:
            all_candidates: list = []
            market_start_gate_cache: dict[date, str] = {}
            market_reversal_cache: dict[tuple[date, str], datetime | None] = {}
            progress_idx = 0
            for (
                stock_item,
                enable_general_strategies,
                enable_limit_up_strategy,
                enable_limit_down_strategy,
            ) in stock_strategy_assignments:
                progress_idx += 1
                stock_name = stock_item[0]
                enabled_strategy_labels = []
                if enable_general_strategies:
                    enabled_strategy_labels.append('GENERAL')
                if enable_limit_up_strategy:
                    enabled_strategy_labels.append(STRATEGY_LIMIT_UP)
                if enable_limit_down_strategy:
                    enabled_strategy_labels.append(STRATEGY_LIMIT_DOWN)
                print_progress(
                    progress_idx,
                    total_stocks,
                    f'{stock_name} [{"/".join(enabled_strategy_labels)}]',
                )
                all_candidates.extend(
                    collect_trade_candidates(
                        stock_item,
                        target_date,
                        minute_bars_by_symbol,
                        day_candles_by_symbol,
                        index_minute_bars_by_key,
                        market_start_gate_cache,
                        market_reversal_cache,
                        enable_general_strategies,
                        enable_limit_up_strategy,
                        enable_limit_down_strategy,
                    )
                )

            results = evaluate_candidates(
                all_candidates,
                print_results=False,
            )
            return {
                'best_results': results,
                'market_start_gate_cache': market_start_gate_cache,
            }

        market_reversal_start_hm = (
            MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
            + MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
        )
        early_breakout_deadline_hm = (
            STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
            + STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
        )
        strategy_decision_hm = STRATEGY_DECISION[0] * 60 + STRATEGY_DECISION[1]
        strategy_start_lower_hm = STRATEGY_START_LOWER[0] * 60 + STRATEGY_START_LOWER[1]
        strategy_end_lower_hm = STRATEGY_END_LOWER[0] * 60 + STRATEGY_END_LOWER[1]
        if MIN_MINUTE_BARS_BEFORE_0930 <= 0:
            print('[ERROR] MIN_MINUTE_BARS_BEFORE_0930 必須大於 0', file=sys.stderr)
            sys.exit(1)
        if (
            not isinstance(SHORT_LIMIT_DOWN_DAYS, list)
            or any(type(days) is not int or days < 1 for days in SHORT_LIMIT_DOWN_DAYS)
            or len(set(SHORT_LIMIT_DOWN_DAYS)) != len(SHORT_LIMIT_DOWN_DAYS)
        ):
            print('[ERROR] SHORT_LIMIT_DOWN_DAYS 必須是沒有重複值的正整數陣列（可為空）', file=sys.stderr)
            sys.exit(1)
        if (
            not isinstance(LONG_LIMIT_UP_DAYS, list)
            or any(type(days) is not int or days < 1 for days in LONG_LIMIT_UP_DAYS)
            or len(set(LONG_LIMIT_UP_DAYS)) != len(LONG_LIMIT_UP_DAYS)
        ):
            print('[ERROR] LONG_LIMIT_UP_DAYS 必須是沒有重複值的正整數陣列（可為空）', file=sys.stderr)
            sys.exit(1)
        if not (0 <= strategy_decision_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_DECISION 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if not (0 <= market_reversal_start_hm <= 23 * 60 + 59):
            print(
                '[ERROR] MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 設定錯誤，需介於 00:00~23:59',
                file=sys.stderr,
            )
            sys.exit(1)
        if not (0 <= early_breakout_deadline_hm <= 23 * 60 + 59):
            print(
                '[ERROR] STRATEGY_EARLY_BREAKOUT_DEADLINE 設定錯誤，需介於 00:00~23:59',
                file=sys.stderr,
            )
            sys.exit(1)
        if early_breakout_deadline_hm >= strategy_decision_hm:
            print(
                '[ERROR] STRATEGY_EARLY_BREAKOUT_DEADLINE 必須早於 STRATEGY_DECISION',
                file=sys.stderr,
            )
            sys.exit(1)
        if market_reversal_start_hm > early_breakout_deadline_hm:
            print(
                '[ERROR] MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME '
                '不可晚於 STRATEGY_EARLY_BREAKOUT_DEADLINE',
                file=sys.stderr,
            )
            sys.exit(1)
        if market_reversal_start_hm >= strategy_decision_hm:
            print(
                '[ERROR] MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 必須早於 STRATEGY_DECISION',
                file=sys.stderr,
            )
            sys.exit(1)
        if not (0 <= strategy_start_lower_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_START_LOWER 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if not (0 <= strategy_end_lower_hm <= 23 * 60 + 59):
            print('[ERROR] STRATEGY_END_LOWER 設定錯誤，需介於 00:00~23:59', file=sys.stderr)
            sys.exit(1)
        if strategy_start_lower_hm >= strategy_end_lower_hm:
            print('[ERROR] STRATEGY_START_LOWER 必須早於 STRATEGY_END_LOWER', file=sys.stderr)
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
        best_results = best_window_result['best_results']
        market_start_gate_cache = best_window_result['market_start_gate_cache']

        print_daily_optimization_results(
            best_results,
            index_minute_bars_by_key,
            market_start_gate_cache,
        )
    finally:
        builtins.print(f'結束時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        flush_each_stock_output_file()


if __name__ == '__main__':
    main()
