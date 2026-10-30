"""
LOWER 回測策略

回測以分 K 模擬策略，使用固定停損、停利與結算時間。
市場條件：IX0001、IX0043 須通過早盤突破、昨收兩側檢查與反彈失效門檻；
LOWER 依序在三個決策時點檢核；啟動後每十分鐘比較股票池相對啟動價格的漲跌家數。
時間及百分比門檻以本檔常數設定為準。

個股入場條件
1. 於當日啟動組別的 STRATEGY_START_LOWER_n～STRATEGY_END_LOWER_n（含）逐根尋找 low 落在
   「昨收到跌停價」之 LOWER_ENTRY_RANGE_START_PERCENT～LOWER_ENTRY_RANGE_END_PERCENT 區間內的分 K。
2. 以上述分 K 的下一根分 K low 作為放空入場價。
3. LOWER 模式成立後不再檢查 IX0001、IX0043 反彈門檻；若產業指數未通過，繼續檢查後續分 K。
4. 入場分 K 之前若當日已觸及漲停或跌停，當日不進場。
5. 入場價加停損價差已達漲停價時不進場。
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
# 空字串維持原流程；YYYYMMDD 固定讀取歷史 cache，優先於 --to。
HISTORICAL_CACHE_DATE = ""

if not HISTORICAL_CACHE_DATE:
    from Z_ORB_ONE.stock_data import (
        market_previous_close_indices,
        selected_stocks,
        total_stocks as lower_decision_stocks,
    )
else:
    market_previous_close_indices = {}
    selected_stocks = []
    lower_decision_stocks = []

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
STOCK_DATA_PATH = Path(__file__).resolve().parents[1] / 'stock_data.py'
INDUSTRY_INDEX_MAP_PATH = Path(__file__).resolve().parents[1] / 'industry_index_map.json'
EACH_STOCK_OUTPUT_FILE = Path(__file__).with_name('analysis_strategy_broken_each_stock_special_short_v17_result.txt')
OUTPUT_BUFFER: list[str] = []
GATE_LOWER_PASSED = 'LOWER_PASSED'
GATE_LOWER_PERCENT_REJECTED = 'LOWER_PERCENT_REJECTED'
GATE_LOWER_CANDIDATE_RANGE_REJECTED = 'LOWER_CANDIDATE_RANGE_REJECTED'
GATE_NO_TRADE = 'NO_TRADE'
GATE_DATA_INCOMPLETE = 'DATA_INCOMPLETE'
STRATEGY_LOWER = 'LOWER'
STRATEGY_NO_TRADE = 'NO_TRADE'
TRADE_SIDE_SHORT = 'SHORT'

INCLUDE_LOWER_IN_PRINT_STATS = True

# ---------------------------------------------------------------------------
# IDE 直接執行時可在此調整策略參數, 此版本不會跳過前一日非營業日的狀況
# ---------------------------------------------------------------------------

STOCK_DATA_SOURCE = "S" # "S": selected_stocks；"T": total_stocks；非空 STOCK_LIST 仍優先使用

# 本日於 09:30 前至少須有此數量的分鐘 K 棒；
# 少於此數量視為延遲撮合股票，不納入當日策略。
MIN_MINUTE_BARS_BEFORE_0930 = 20

OPTIMIZE_PROFIT_PER_LOWER = 5.0 # lower 停利百分比(%)，例如 5.0 代表入場價減去 5%
OPTIMIZE_LOSS_PER_LOWER = 2.0 # lower 停損百分比(%)，例如 3.0 代表入場價加上 3%


LOWER_ENTRY_RANGE_START_PERCENT = 10.0 # lower 入場價距昨收到跌停的起始百分比，可以為 0
LOWER_ENTRY_RANGE_END_PERCENT = 60.0 # lower 入場價距昨收到跌停的結束百分比，可以為 70
LOWER_DECISION_DECLINE_PERCENT_THRESHOLD = 35.0 # 各啟動檢核時點的區間股票比例須嚴格大於此值
LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD = 65.0 # 同一比例須嚴格小於此值，即下限 < 比例 < 上限
LOWER_EXIT_PERCENT_THRESHOLD = 40.0 # 啟動後每十分鐘的上漲家數比例嚴格超過此值時全平倉，當日不再入場


LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME = (9, 6) # LOWER 指數昨收兩側檢查起始時間，包含此時間
LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE = (9, 21) # IX0001/IX0043 早盤須先向下突破各自 LOWER 門檻的截止分K棒，包含此時間
LOWER_STRATEGY_DECISION_1 = (9, 31) # 第一組市場模式判斷截止分K棒，不包含此時間
STRATEGY_START_LOWER_1 = (9, 32) # 第一組個股進場訊號開始分K棒，包含此時間
STRATEGY_END_LOWER_1 = (10, 1) # 第一組實際進場截止分K棒，包含此時間

LOWER_STRATEGY_DECISION_2 = (9, 41)
STRATEGY_START_LOWER_2 = (9, 42)
STRATEGY_END_LOWER_2 = (10, 11)

LOWER_STRATEGY_DECISION_3 = (9, 51)
STRATEGY_START_LOWER_3 = (9, 52)
STRATEGY_END_LOWER_3 = (10, 21)

INTRADAY_COMPARE_END_LOWER = (12, 50)  # lower 盤中停損/停利比對截止(時, 分)

IX0001_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.2 # IX0001 早盤向下突破昨收的百分比門檻
IX0001_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.9 # IX0001 各決策時點前最後 close 須低於昨收下方此百分比的門檻
IX0043_STRATEGY_DECISION_DROP_PERCENT_LOWER = 1.0 # IX0043 早盤向下突破昨收的百分比門檻
IX0043_STRATEGY_DECISION_REBOUND_PERCENT_LOWER = 0.75 # IX0043 各決策時點前最後 close 須低於昨收下方此百分比的門檻

# 產業盤勢過濾：作空須嚴格低於昨收下跌門檻。
INDUSTRY_MARKET_FILTER_SHORT_PERCENT = 0 # LOWER 作空：產業指數需低於昨收下跌此百分比的門檻

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


def ensure_backtest_industry_index_metadata(stock_list: list[tuple]) -> list[tuple]:
    """
    回測程式專用：啟動時檢查 stock_data.py 是否具備足夠產業別指數 metadata。
    只補齊產業指數結構與代碼，不更新最新 previous_close；回測會另外抓歷史分 K。
    mapping 無對應指數的股票列入報告並排除，回傳可回測的股票清單。
    """
    required_targets = get_required_industry_targets(stock_list)
    missing_targets = [
        (exchange, industry_code)
        for exchange, industry_code in sorted(required_targets)
        if not market_previous_close_indices.get(f'{exchange}:{industry_code}', {}).get('symbol')
    ]
    if not missing_targets:
        return stock_list

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

    if added_keys:
        write_stock_data_market_indices(updated_indices)
    market_previous_close_indices.clear()
    market_previous_close_indices.update(updated_indices)
    MARKET_INDEX_METADATA.clear()
    MARKET_INDEX_METADATA.update({**market_previous_close_indices, **RESERVE_MARKET_INDICES})
    if added_keys:
        print(
            '回測啟動檢查：已補齊 stock_data.py 產業別指數 metadata '
            f'({len(added_keys)}): {", ".join(added_keys)}'
        )
    eligible_stocks = []
    for stock_item in stock_list:
        if get_industry_index_key(stock_item) is None:
            exchange = get_exchange_for_stock(stock_item[0])
            industry_code = get_stock_industry_code(stock_item)
            print(
                f'[排除回測] {stock_item[0]}：缺少產業指數 mapping '
                f'{exchange}:{industry_code}，無法驗證產業入場條件'
            )
        else:
            eligible_stocks.append(stock_item)
    if unresolved or len(eligible_stocks) != len(stock_list):
        print(
            f'缺少產業指數 mapping：排除股票數={len(stock_list) - len(eligible_stocks)} '
            f'剩餘回測股票數={len(eligible_stocks)}'
        )
    return eligible_stocks


def get_api_cache_path(target_date: date) -> Path:
    """回傳 API 快取檔路徑（json_cache 資料夾）。"""
    return Path(__file__).resolve().parent / 'analysis_json_cache' / f'analysis_strategy_broken_each_stock_special_short_api_cache_{target_date:%Y%m%d}.json'


def load_api_cache(cache_path: Path, stock_list: list[tuple], decision_stock_list: list[tuple] | None = None) -> tuple[dict[str, dict[str, list]], dict[str, dict[str, list]]] | None:
    """載入分 K 快取；相容含日 K 欄位的舊快取，資料不完整則回傳 None。"""
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding='utf-8'))
        cached_names = payload.get('stock_names', [])
        minute_names = [item[0] for item in dedupe_stock_list([stock_list, decision_stock_list or []])]
        cached_name_set = set(cached_names)
        if any(stock_name not in cached_name_set for stock_name in minute_names):
            return None
        raw_minute_by_symbol = payload.get('minute_raw_by_symbol', {})
        raw_index_minute_by_key = payload.get('index_minute_raw_by_key', {})
        if any(stock_name not in raw_minute_by_symbol for stock_name in minute_names):
            return None
        required_index_keys = set(RESERVE_MARKET_INDICES) | get_required_industry_index_keys(stock_list)
        if any(
            key not in raw_index_minute_by_key
            for key in required_index_keys
        ):
            return None
        minute_bars_by_symbol = {
            stock_name: parse_bars(raw_minute_by_symbol.get(stock_name, []))
            for stock_name in minute_names
        }
        index_minute_bars_by_key = {
            index_key: parse_bars(raw_index_minute_by_key.get(index_key, []))
            for index_key in required_index_keys
        }
        return minute_bars_by_symbol, index_minute_bars_by_key
    except Exception:
        return None


def load_historical_api_cache(cache_path: Path) -> tuple[list[tuple], dict, dict]:
    """僅讀取歷史 cache；失敗報錯，不回退 API，不使用當天股票資料。"""
    try:
        payload = json.loads(cache_path.read_text(encoding='utf-8'))
        names = payload['stock_names']
        if not isinstance(names, list) or not names or any(
            not isinstance(name, str) or not name for name in names
        ):
            raise ValueError('stock_names 必須是非空的股票名稱清單')
        stocks = dedupe_stock_list([[(name,) for name in names]])
        raw_stocks = payload['minute_raw_by_symbol']
        raw_indices = payload['index_minute_raw_by_key']
        for name, in stocks:
            if not isinstance(raw_stocks[name], list):
                raise ValueError(f'{name} 分 K 必須是陣列')
        for key in RESERVE_MARKET_INDICES:
            if not isinstance(raw_indices[key], list):
                raise ValueError(f'{key} 分 K 必須是陣列')
        return (
            stocks,
            {name: parse_bars(raw_stocks[name]) for name, in stocks},
            {key: parse_bars(raw_indices[key]) for key in RESERVE_MARKET_INDICES},
        )
    except Exception as exc:
        raise ValueError(f'歷史 cache 載入失敗: {cache_path}；{exc}') from exc


def save_api_cache(
    cache_path: Path,
    stock_list: list[tuple],
    minute_raw_by_symbol: dict[str, list],
    index_minute_raw_by_key: dict[str, list],
) -> None:
    """儲存 API 快取。"""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'stock_names': [item[0] for item in stock_list],
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
            'average': float(item['average']) if item.get('average') is not None else (
                float(item['turnover']) / float(item['volume'])
                if item.get('turnover') is not None and float(item.get('volume', 0)) > 0
                else None
            ),
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
    """找出第一組決策前第一個 low 跌破 threshold 的分K。"""
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    decision_hm = LOWER_STRATEGY_DECISION_1[0] * 60 + LOWER_STRATEGY_DECISION_1[1]
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
    """找出跌破後下一根起到第一組決策前第一個 high >= 反彈門檻的分K。"""
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    decision_hm = LOWER_STRATEGY_DECISION_1[0] * 60 + LOWER_STRATEGY_DECISION_1[1]
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
    decision_count: int = 3,
) -> list[str]:
    """建立每日指數摘要；跌幅採截止棒及決策前一棒 close，相對昨收計算。"""
    rows = []
    for index_key, label in (
        ('TWSE:MARKET', 'IX0001'),
        ('TPEX:MARKET', 'IX0043'),
    ):
        bars_by_date = index_minute_bars_by_key.get(index_key, {})
        previous_low = get_previous_trading_day_low(bars_by_date, target_date)
        previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
        drop_percent = get_strategy_decision_drop_percent(index_key)
        previous_low_text = f'{previous_low:.2f}' if previous_low is not None else 'N/A'
        previous_close_text = f'{previous_close:.2f}' if previous_close is not None else 'N/A'
        decline_texts = []
        today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
        for time_hm, exclude_time in (
            (LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE, False),
            *((decision_hm, True) for decision_hm, _, _ in get_lower_strategy_windows()[:decision_count]),
        ):
            bar_dt = datetime.combine(target_date, datetime.min.time()).replace(
                hour=time_hm[0], minute=time_hm[1],
            )
            if exclude_time:
                bar_dt -= timedelta(minutes=1)
            time_bar = next((bar for bar in today_bars if bar.get('dt') == bar_dt), None)
            close = time_bar.get('close') if time_bar is not None else None
            decline_texts.append(
                f'{(previous_close - float(close)) / previous_close * 100:.2f}%'
                if previous_close is not None and previous_close > 0 and close is not None
                else 'N/A'
            )
        if previous_close is None or drop_percent is None:
            threshold_text = 'N/A'
        else:
            threshold = previous_close * (1 - drop_percent / 100.0)
            threshold_text = f'{threshold:.2f}'
        rows.append(
            f'{label} 昨低={previous_low_text}  '
            f'昨收={previous_close_text}  '
            f'門檻={threshold_text}  '
            f'check={decline_texts[0]}  '
            + '  '.join(
                f'時間{time_number}={decline_text}'
                for time_number, decline_text in enumerate(decline_texts[1:], start=1)
            )
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
    decision_dt: datetime | None = None,
) -> bool:
    """指定起始時間至決策時間前，指數若曾嚴格位於昨收上下兩側即視為反轉。"""
    bars_by_date = index_minute_bars_by_key.get(index_key, {})
    previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
    if previous_close is None:
        return False

    lower_market_previous_close_reversal_start_hm = (
        LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
        + LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
    )
    decision_hm = (decision_dt.hour * 60 + decision_dt.minute) if decision_dt else (
        LOWER_STRATEGY_DECISION_1[0] * 60 + LOWER_STRATEGY_DECISION_1[1]
    )
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    has_traded_above = False
    has_traded_below = False

    for bar in today_bars:
        bar_dt = bar.get('dt')
        if bar_dt is None:
            continue
        bar_hm = bar_dt.hour * 60 + bar_dt.minute
        if bar_hm < lower_market_previous_close_reversal_start_hm or bar_hm >= decision_hm:
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
    decision_dt: datetime | None = None,
) -> bool:
    """兩個市場指數在模式判斷區間內都需具備完整 OHLC 分 K，否則本日不交易。"""
    decision_hm = (decision_dt.hour * 60 + decision_dt.minute) if decision_dt else (
        LOWER_STRATEGY_DECISION_1[0] * 60 + LOWER_STRATEGY_DECISION_1[1]
    )
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


def is_market_index_rebound_level_maintained_at_strategy_decision(
    index_key: str,
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    decision_dt: datetime | None = None,
) -> bool:
    """只檢查決策前最後 close 是否低於反彈門檻，不檢查期間內是否曾觸及。"""
    rebound_percent = get_strategy_decision_rebound_percent(index_key)
    if rebound_percent is None:
        return False

    bars_by_date = index_minute_bars_by_key.get(index_key, {})
    if not bars_by_date:
        return False

    previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
    if previous_close is None:
        return False

    rebound_threshold = previous_close * (1 - rebound_percent / 100.0)
    today_bars = bars_by_date.get(target_date.strftime('%Y-%m-%d'), [])
    decision_hm = (decision_dt.hour * 60 + decision_dt.minute) if decision_dt else (
        LOWER_STRATEGY_DECISION_1[0] * 60 + LOWER_STRATEGY_DECISION_1[1]
    )
    valid_bars = [
        bar
        for bar in today_bars
        if bar.get('dt') is not None
        and (bar['dt'].hour * 60 + bar['dt'].minute) < decision_hm
    ]
    sorted_bars = sorted(valid_bars, key=lambda item: item['dt'])
    close_bars = [bar for bar in sorted_bars if bar.get('close') is not None]
    if not close_bars:
        return False

    final_close = float(close_bars[-1]['close'])
    return final_close < rebound_threshold


def has_market_index_early_lower_breakout(
    index_key: str,
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
) -> bool:
    """指數在早盤截止時間（含）前曾嚴格向下突破各自 LOWER 門檻即回傳 True。"""
    bars_by_date = index_minute_bars_by_key.get(index_key, {})
    previous_close = get_previous_trading_day_last_close(bars_by_date, target_date)
    if previous_close is None:
        return False

    drop_percent = get_strategy_decision_drop_percent(index_key)
    if drop_percent is None:
        return False

    drop_threshold = previous_close * (1 - drop_percent / 100.0)
    start_hm = (
        LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
        + LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
    )
    deadline_hm = (
        LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
        + LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
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
    minute_bars_by_symbol: dict[str, dict[str, list]],
    stock_list: list[tuple],
    decision_dt: datetime | None = None,
) -> str:
    """依兩指數早盤跌破與指定決策前反彈水準決定模式。"""
    if not has_complete_market_decision_data(
        target_date,
        index_minute_bars_by_key,
        decision_dt,
    ):
        return GATE_DATA_INCOMPLETE

    if not all(
        has_market_index_early_lower_breakout(
            index_key,
            target_date,
            index_minute_bars_by_key,
        )
        for index_key in ('TWSE:MARKET', 'TPEX:MARKET')
    ):
        return GATE_NO_TRADE

    if any(
        has_market_index_crossed_both_sides_of_previous_close(
            index_key,
            target_date,
            index_minute_bars_by_key,
            decision_dt,
        )
        for index_key in ('TWSE:MARKET', 'TPEX:MARKET')
    ):
        return GATE_NO_TRADE

    if not all(
        is_market_index_rebound_level_maintained_at_strategy_decision(
            index_key,
            target_date,
            index_minute_bars_by_key,
            decision_dt,
        )
        for index_key in ('TWSE:MARKET', 'TPEX:MARKET')
    ):
        return GATE_NO_TRADE

    candidate_count, decline_count = summarize_lower_strategy_decision_candidates(
        lower_decision_stocks,
        target_date,
        minute_bars_by_symbol,
        decision_dt=decision_dt,
    )
    decline_percent = calculate_percent(decline_count, candidate_count)
    if not (
        LOWER_DECISION_DECLINE_PERCENT_THRESHOLD
        < decline_percent
        < LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD
    ):
        return GATE_LOWER_PERCENT_REJECTED
    return GATE_LOWER_PASSED


def get_lower_strategy_windows() -> list[tuple[tuple[int, int], tuple[int, int], tuple[int, int]]]:
    """依序回傳三組獨立的決策、訊號開始與實際進場截止時間。"""
    return [
        (LOWER_STRATEGY_DECISION_1, STRATEGY_START_LOWER_1, STRATEGY_END_LOWER_1),
        (LOWER_STRATEGY_DECISION_2, STRATEGY_START_LOWER_2, STRATEGY_END_LOWER_2),
        (LOWER_STRATEGY_DECISION_3, STRATEGY_START_LOWER_3, STRATEGY_END_LOWER_3),
    ]


def get_lower_entry_window(activation_dt: datetime | None) -> tuple[tuple[int, int], tuple[int, int]]:
    """依實際啟動時點選擇進場區間；未指定時使用第一組。"""
    for decision_hm, start_hm, end_hm in get_lower_strategy_windows():
        if activation_dt is None or (activation_dt.hour, activation_dt.minute) == decision_hm:
            return start_hm, end_hm
    raise ValueError(f'找不到啟動時間 {activation_dt:%H:%M} 對應的 LOWER 進場區間')


def validate_lower_strategy_windows() -> None:
    """檢查三組時間有效、決策依序遞增，且每組決策早於進場區間。"""
    previous_decision_hm = -1
    settlement_hm = INTRADAY_COMPARE_END_LOWER[0] * 60 + INTRADAY_COMPARE_END_LOWER[1]
    for window_number, window in enumerate(get_lower_strategy_windows(), start=1):
        for time_name, time_hm in zip(
            ('LOWER_STRATEGY_DECISION', 'STRATEGY_START_LOWER', 'STRATEGY_END_LOWER'), window,
        ):
            if not (0 <= time_hm[0] <= 23 and 0 <= time_hm[1] <= 59):
                raise ValueError(f'{time_name}_{window_number} 設定錯誤，需介於 00:00~23:59')
        decision_hm, start_hm, end_hm = (hour * 60 + minute for hour, minute in window)
        if not previous_decision_hm < decision_hm < start_hm < end_hm < settlement_hm:
            raise ValueError(
                f'LOWER 第{window_number}組需符合：前組決策 < 本組決策 < 開始 < 截止 < 結算時間'
            )
        previous_decision_hm = decision_hm


def get_lower_decision_times(target_date: date) -> list[datetime]:
    """依序回傳三組設定的決策時點。"""
    return [
        datetime.combine(target_date, datetime.min.time()).replace(hour=decision_hm[0], minute=decision_hm[1])
        for decision_hm, _, _ in get_lower_strategy_windows()
    ]


def get_strategy_market_activation(
    target_date: date,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    minute_bars_by_symbol: dict[str, dict[str, list]],
    stock_list: list[tuple],
) -> tuple[str, datetime | None]:
    """時間1起下降比例嚴格超過入場上限即停止，其餘依原入場條件檢核。"""
    for decision_dt in get_lower_decision_times(target_date):
        candidate_count, decline_count = summarize_lower_strategy_decision_candidates(
            lower_decision_stocks, target_date, minute_bars_by_symbol, decision_dt=decision_dt,
        )
        if calculate_percent(decline_count, candidate_count) > LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD:
            return GATE_LOWER_CANDIDATE_RANGE_REJECTED, None
        gate_status = get_strategy_market_decision_gate_status(
            target_date, index_minute_bars_by_key, minute_bars_by_symbol,
            stock_list, decision_dt,
        )
        if gate_status == GATE_LOWER_PASSED:
            return gate_status, decision_dt
        if gate_status != GATE_LOWER_PERCENT_REJECTED:
            return gate_status, None
    return GATE_LOWER_PERCENT_REJECTED, None


def is_industry_market_filter_passed(
    stock_item: tuple,
    target_date: date,
    entry_dt: datetime,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    strategy_type: str,
) -> bool:
    """LOWER 入場前，產業指數前一根已完成分 K 須嚴格低於作空門檻。"""
    if HISTORICAL_CACHE_DATE:
        return True  # cache 無個股產業對照，依歷史模式設定略過。
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
    completed_bars = sorted(
        (
            bar
            for bar in today_index_bars
            if bar.get('dt') is not None
            and bar['dt'] < entry_dt
            and bar.get('close') is not None
        ),
        key=lambda bar: bar['dt'],
    )
    if not completed_bars:
        return False

    try:
        industry_close = float(completed_bars[-1]['close'])
        previous_reference = float(previous_reference)
    except (TypeError, ValueError):
        return False

    if strategy_type != STRATEGY_LOWER:
        return False
    threshold = previous_reference * (1 - INDUSTRY_MARKET_FILTER_SHORT_PERCENT / 100.0)
    return industry_close < threshold


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


def find_first_bar_at_or_after_time(today_bars: list, target_time: tuple[int, int]):
    """找指定時間後最早的分 K；不存在回傳 (None, -1)。"""
    target_minutes = target_time[0] * 60 + target_time[1]
    best_idx = -1
    best_bar = None
    best_dt = None
    for idx, bar in enumerate(today_bars):
        dtv = bar.get('dt')
        if dtv is None:
            continue
        bar_minutes = dtv.hour * 60 + dtv.minute
        if bar_minutes < target_minutes:
            continue
        if best_dt is None or dtv < best_dt:
            best_dt = dtv
            best_idx = idx
            best_bar = bar
    if best_bar is None:
        return None, -1
    return best_bar, best_idx


def find_previous_bar_with_close(today_bars: list, entry_idx: int):
    """從入場 K 往前找第一根具備 close 的分 K；不存在回傳 None。"""
    for idx in range(entry_idx - 1, -1, -1):
        bar = today_bars[idx]
        if bar.get('close') is not None:
            return bar
    return None


def has_enough_minute_bars_before_0930(today_bars: list) -> bool:
    """09:30 前分 K 少於設定根數者視為延遲撮合股票，排除當日交易。"""
    before_0930_count = sum(
        1
        for bar in today_bars
        if (dtv := bar.get('dt')) is not None
        and (dtv.hour, dtv.minute) < (9, 30)
    )
    return before_0930_count >= MIN_MINUTE_BARS_BEFORE_0930


def iter_entry_signal_lower_candidates(
    today_bars: list,
    ystats: dict,
    activation_dt: datetime | None = None,
):
    """
    作空進場候選訊號：
    1) 進場訊號與實際進場時間，均須在啟動組別的開始到截止時間內
    2) 逐根回傳 low 落在入場區間的 K 棒
    3) 候選進場價 = 訊號分 K 下一根 K 棒 low
    """
    start_time, end_time = get_lower_entry_window(activation_dt)
    start_hm = start_time[0] * 60 + start_time[1]
    end_hm = end_time[0] * 60 + end_time[1]
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

    for sorted_idx, (_original_idx, bar, hm) in enumerate(indexed_bars):
        if bar.get('low') is None:
            continue

        if not (start_hm <= hm <= end_hm):
            continue

        signal_price = float(bar['low'])
        if not is_price_in_entry_range(signal_price, entry_lower_bound, entry_upper_bound):
            continue

        if sorted_idx + 1 >= len(indexed_bars):
            continue
        _next_original_idx, entry_bar, _entry_hm = indexed_bars[sorted_idx + 1]
        if _entry_hm > end_hm or (activation_dt is not None and bar['dt'] <= activation_dt):
            continue
        if entry_bar.get('low') is None:
            continue

        entry_price = float(entry_bar['low'])
        yield entry_bar, entry_price


def scan_entry_signal_lower(
    today_bars: list,
    ystats: dict,
):
    """
    回傳第一個 lower 價格候選；保留給需要單一訊號的呼叫端使用。
    """
    return next(iter_entry_signal_lower_candidates(today_bars, ystats), None)


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
    if exit_reason == 'lower_percent':
        return f'{stock_label} / {date_str} / {status_label}(LOWER 比例不足平倉, 淨損益: {signed:+.2f})'
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


def get_completed_trade_matches(day_results: list, check_dt: datetime) -> dict[str, bool]:
    """只採計指定分鐘前已實際停利或停損的交易。"""
    return {
        signal['name']: result['exit_reason'] == 'target'
        for signal, result in day_results
        if signal['entry_dt'] <= result['exit_dt'] <= check_dt
        and result.get('exit_reason') in ('target', 'stop')
    }


def summarize_lower_strategy_decision_candidates(
    stock_list: list[tuple],
    target_date: date,
    minute_bars_by_symbol: dict[str, dict[str, list]],
    decision_dt: datetime | None = None,
    completed_trade_matches: dict[str, bool] | None = None,
) -> tuple[int, int]:
    """缺資料不計分母；已停利算符合、已停損算不符，其餘依指定分鐘 close 判斷。"""
    candidate_count = 0
    decline_count = 0
    decision_dt = decision_dt or datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        LOWER_STRATEGY_DECISION_1[0],
        LOWER_STRATEGY_DECISION_1[1],
    )

    for stock_item in stock_list:
        stock_name = stock_item[0]
        if completed_trade_matches is not None and stock_name in completed_trade_matches:
            candidate_count += 1
            decline_count += int(completed_trade_matches[stock_name])
            continue
        bars_by_date = minute_bars_by_symbol.get(stock_name, {})
        today_bars, yesterday_bars = get_target_and_yesterday(bars_by_date, target_date)
        if not today_bars or not yesterday_bars:
            continue

        decision_bar = find_bar_at_or_before(today_bars, decision_dt)
        if decision_bar is None or decision_bar.get('close') is None:
            continue

        try:
            ystats = compute_yesterday_stats(yesterday_bars)
            decision_close = float(decision_bar['close'])
        except (KeyError, TypeError, ValueError):
            continue

        _, limit_down_price = calculate_limit_prices(ystats['close'])
        entry_lower_bound, entry_upper_bound = calculate_entry_range_bounds(
            ystats['close'],
            limit_down_price,
            LOWER_ENTRY_RANGE_START_PERCENT,
            LOWER_ENTRY_RANGE_END_PERCENT,
        )
        candidate_count += 1
        if is_price_in_entry_range(decision_close, entry_lower_bound, entry_upper_bound):
            decline_count += 1

    return candidate_count, decline_count


def calculate_percent(numerator: int, denominator: int) -> float:
    """安全計算百分比。"""
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def get_stock_close_at(
    stock_name: str,
    check_dt: datetime,
    minute_bars_by_symbol: dict[str, dict[str, list]],
) -> float | None:
    """取得該分鐘或之前最後一根有效收盤價。"""
    bars_by_date = minute_bars_by_symbol.get(stock_name, {})
    today_bars, yesterday_bars = get_target_and_yesterday(bars_by_date, check_dt.date())
    if not today_bars or not yesterday_bars:
        return None
    time_bar = find_bar_at_or_before(today_bars, check_dt)
    try:
        close = float(time_bar['close']) if time_bar is not None else None
    except (KeyError, TypeError, ValueError):
        return None
    return close if close is not None and math.isfinite(close) and close > 0 else None


def summarize_lower_price_movements(
    baseline_prices: dict[str, float],
    check_dt: datetime,
    minute_bars_by_symbol: dict[str, dict[str, list]],
    completed_trade_matches: dict[str, bool],
) -> tuple[int, int, int, int]:
    """回傳有效母數、上漲、持平、下跌家數；停利排除，停損固定算上漲。"""
    up_count = flat_count = down_count = 0
    for stock_name, baseline_price in baseline_prices.items():
        if stock_name in completed_trade_matches:
            if not completed_trade_matches[stock_name]:
                up_count += 1
            continue
        close = get_stock_close_at(stock_name, check_dt, minute_bars_by_symbol)
        if close is None:
            continue
        if close > baseline_price:
            up_count += 1
        elif close < baseline_price:
            down_count += 1
        else:
            flat_count += 1
    return up_count + flat_count + down_count, up_count, flat_count, down_count


def build_lower_tracking_snapshots(
    activation_dt: datetime,
    day_results: list,
    minute_bars_by_symbol: dict[str, dict[str, list]],
) -> list[dict]:
    """以 selected_stocks（歷史模式用 cache 全部股票）追蹤，於全平倉或最後交易平倉時停止。"""
    if not day_results:
        return []
    baseline_prices = {}
    tracking_stocks = lower_decision_stocks if HISTORICAL_CACHE_DATE else selected_stocks
    for stock_item in tracking_stocks:
        close = get_stock_close_at(stock_item[0], activation_dt, minute_bars_by_symbol)
        if close is not None:
            baseline_prices[stock_item[0]] = close
    final_exit_dt = max(result['exit_dt'] for _, result in day_results)
    settlement_dt = activation_dt.replace(
        hour=INTRADAY_COMPARE_END_LOWER[0], minute=INTRADAY_COMPARE_END_LOWER[1],
    )
    snapshots = []
    check_dt = activation_dt + timedelta(minutes=10)
    while check_dt < settlement_dt and check_dt < final_exit_dt:
        total, up_count, flat_count, down_count = summarize_lower_price_movements(
            baseline_prices, check_dt, minute_bars_by_symbol,
            get_completed_trade_matches(day_results, check_dt),
        )
        up_percent = calculate_percent(up_count, total)
        snapshots.append({
            'dt': check_dt,
            'total': total,
            'up_percent': up_percent,
            'flat_percent': calculate_percent(flat_count, total),
            'down_percent': calculate_percent(down_count, total),
            'triggered': total > 0 and up_percent > LOWER_EXIT_PERCENT_THRESHOLD,
        })
        if snapshots[-1]['triggered']:
            break
        check_dt += timedelta(minutes=10)
    return snapshots


def print_daily_optimization_results(
    all_results: list,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    minute_bars_by_symbol: dict[str, dict[str, list]],
    stock_list: list[tuple],
    market_start_gate_cache: dict[date, tuple[str, datetime | None]],
    tracking_snapshots_by_date: dict[str, list[dict]] | None = None,
) -> None:
    """印出固定參數下依日期彙總的進出場明細。"""
    print_results = filter_results_for_print_stats(all_results)
    print(
        f'損益已納入交易成本: 手續費率={BROKERAGE_FEE_RATE:.6f}, '
        f'賣出交易稅率={SELL_TRANSACTION_TAX_RATE:.6f}'
    )
    print('')

    summary = summarize_results(print_results)
    gate_status_by_date_key = {
        current_date.strftime('%Y-%m-%d'): gate_status
        for current_date, (gate_status, _) in market_start_gate_cache.items()
    }
    strategy_type_by_gate_status = {
        GATE_LOWER_PASSED: STRATEGY_LOWER,
        GATE_LOWER_PERCENT_REJECTED: STRATEGY_NO_TRADE,
        GATE_LOWER_CANDIDATE_RANGE_REJECTED: STRATEGY_NO_TRADE,
        GATE_NO_TRADE: STRATEGY_NO_TRADE,
        GATE_DATA_INCOMPLETE: STRATEGY_NO_TRADE,
    }
    lower_gate_date_keys = {
        current_date.strftime('%Y-%m-%d')
        for current_date, (gate_status, _) in market_start_gate_cache.items()
        if gate_status in (GATE_LOWER_PASSED, GATE_LOWER_PERCENT_REJECTED, GATE_LOWER_CANDIDATE_RANGE_REJECTED)
        and INCLUDE_LOWER_IN_PRINT_STATS
    }
    if summary['total'] == 0 and not lower_gate_date_keys:
        print('固定參數下沒有交易結果。')
        return

    grouped_results: dict[tuple[str, str], list[tuple[str, str, datetime | None, datetime | None, float, float, float, bool]]] = {}
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
        STRATEGY_NO_TRADE: 2,
    }
    for date_key in sorted(daily_date_keys, reverse=True):
        day_results = [
            (signal, result)
            for signal, result in all_results
            if signal and result and signal['date_str'] == date_key
        ]
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
            lower_decision_text = ''
            candidate_range_rejected = gate_status_by_date_key.get(date_key) == GATE_LOWER_CANDIDATE_RANGE_REJECTED
            checked_decision_count = 3
            percent_rejected = (
                strategy_type == STRATEGY_NO_TRADE
                and gate_status_by_date_key.get(date_key) in (
                    GATE_LOWER_PERCENT_REJECTED, GATE_LOWER_CANDIDATE_RANGE_REJECTED,
                )
            )
            if strategy_type == STRATEGY_LOWER or percent_rejected:
                _, activation_dt = market_start_gate_cache.get(target_date, (GATE_NO_TRADE, None))
                decision_times = get_lower_decision_times(target_date)
                early_dt = datetime.combine(target_date, datetime.min.time()).replace(
                    hour=LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[0],
                    minute=LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[1],
                )
                for time_number, snapshot_dt in enumerate([early_dt] + decision_times):
                    if activation_dt is not None and snapshot_dt > activation_dt:
                        break
                    snapshot_candidate_count, snapshot_decline_count = summarize_lower_strategy_decision_candidates(
                        lower_decision_stocks,
                        target_date,
                        minute_bars_by_symbol,
                        decision_dt=snapshot_dt,
                        completed_trade_matches=get_completed_trade_matches(day_results, snapshot_dt),
                    )
                    snapshot_decline_percent = calculate_percent(snapshot_decline_count, snapshot_candidate_count)
                    time_label = 'check' if time_number == 0 else f'時間{time_number}'
                    lower_decision_text += (
                        f'{time_label}:[母數={snapshot_candidate_count}  '
                        f'下降={snapshot_decline_count}  {snapshot_decline_percent:.2f}%]'
                    )
                    lower_decision_text += '-進場  ' if snapshot_dt == activation_dt else '  '
                    if (
                        candidate_range_rejected and time_number >= 1
                        and snapshot_decline_percent > LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD
                    ):
                        checked_decision_count = time_number
                        break
                if activation_dt is not None:
                    snapshots = (
                        tracking_snapshots_by_date.get(date_key, [])
                        if tracking_snapshots_by_date is not None
                        else build_lower_tracking_snapshots(activation_dt, day_results, minute_bars_by_symbol)
                    )
                    for snapshot in snapshots:
                        if not snapshot['triggered']:
                            continue
                        lower_decision_text += (
                            f'{snapshot["dt"]:%H:%M} [母數={snapshot["total"]}  '
                            f'+{snapshot["up_percent"]:.2f}% '
                            f'{snapshot["flat_percent"]:.2f}% '
                            f'-{snapshot["down_percent"]:.2f}%]-平倉  '
                        )
                if candidate_range_rejected:
                    lower_decision_text += (
                        '原因=LOWER股票下跌百分超出候選範圍'
                        f'(>{LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD:g}%)  '
                    )
                elif percent_rejected:
                    lower_decision_text += (
                        '原因=LOWER 股票比例不符 '
                        f'({LOWER_DECISION_DECLINE_PERCENT_THRESHOLD:g}% < 比例 '
                        f'< {LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD:g}%)  '
                    )
            print(
                f'{format_date_with_weekday(date_key)} '
                f'模式={strategy_type}  '
                f'{market_open_text}'
                f'{lower_decision_text}'
                f'筆數={len(day_rows)}  '
                f'成功={day_successes}  '
                f'失敗={day_failures}  '
                f'總支出={day_total_cost:.2f}  '
                f'總收益={day_total:+.2f}'
            )
            if (strategy_type == STRATEGY_LOWER or percent_rejected) and date_key in lower_gate_date_keys:
                for index_summary in build_market_index_daily_low_summary(
                    target_date,
                    index_minute_bars_by_key,
                    decision_count=checked_decision_count,
                ):
                    print(index_summary)
            for stock_name, industry_code, entry_dt, exit_dt, entry_price, exit_price, pnl_value, _ in sorted(
                day_rows,
                key=lambda row: (format_entry_time(row[2]), row[0]),
            ):
                print(
                    f'{format_stock_label(stock_name, industry_code)} '
                    f'{format_entry_time(entry_dt)} {format_entry_time(exit_dt)} '
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
        f'LOWER_LOSS_PER={OPTIMIZE_LOSS_PER_LOWER:.1f}%  '
        f'LOWER_PROFIT_PER={OPTIMIZE_PROFIT_PER_LOWER:.1f}%'
    )
    print(
        f'INCLUDE_LOWER_IN_PRINT_STATS={INCLUDE_LOWER_IN_PRINT_STATS}'
    )
    print(
        f'LOWER_ENTRY_RANGE={LOWER_ENTRY_RANGE_START_PERCENT:.1f}%~{LOWER_ENTRY_RANGE_END_PERCENT:.1f}%'
    )
    print(
        f'LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME='
        f'{LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0]:02d}:'
        f'{LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]:02d}  '
        f'LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE='
        f'{LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[0]:02d}:'
        f'{LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[1]:02d}'
    )
    for window_number, (decision_hm, start_hm, end_hm) in enumerate(get_lower_strategy_windows(), start=1):
        print(
            f'LOWER_STRATEGY_DECISION_{window_number}={decision_hm[0]:02d}:{decision_hm[1]:02d}  '
            f'LOWER進場時間窗_{window_number}={start_hm[0]:02d}:{start_hm[1]:02d}'
            f'~{end_hm[0]:02d}:{end_hm[1]:02d}'
        )
    print(
        f'LOWER出場時間窗={INTRADAY_COMPARE_END_LOWER[0]:02d}:{INTRADAY_COMPARE_END_LOWER[1]:02d}'
    )


# ---------------------------------------------------------------------------
# Core orchestration — analyze_stock
# ---------------------------------------------------------------------------


def find_trade_candidate_on_date(
    stock_item: tuple,
    target_date: date,
    bars_by_date: dict,
    index_minute_bars_by_key: dict[str, dict[str, list]],
    strategy_type: str,
    activation_dt: datetime | None = None,
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

    today_ordered = sorted(
        (bar for bar in today_bars if bar.get('dt') is not None),
        key=lambda bar: bar['dt'],
    )

    if strategy_type == STRATEGY_LOWER:
        entry_candidates = iter_entry_signal_lower_candidates(today_ordered, ystats, activation_dt)
        intraday_compare_end = INTRADAY_COMPARE_END_LOWER
    else:
        return None

    for entry_bar, entry_price in entry_candidates:
        if not is_industry_market_filter_passed(
            stock_item,
            target_date,
            entry_bar['dt'],
            index_minute_bars_by_key,
            strategy_type,
        ):
            continue

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
        )
        candidate['activation_dt'] = activation_dt
        return candidate

    return None


def collect_trade_candidates(
    stock_item: tuple,
    stock_list: list[tuple],
    target_date: date,
    minute_bars_by_symbol: dict[str, dict[str, list]],
    index_minute_bars_by_key: dict[str, dict[str, list]],
    market_start_gate_cache: dict[date, tuple[str, datetime | None]],
) -> list:
    """逐日依 LOWER 市場條件尋找候選交易。"""
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
            market_start_gate_cache[current_date] = get_strategy_market_activation(
                current_date,
                index_minute_bars_by_key,
                minute_bars_by_symbol,
                stock_list,
            )
        gate_status, activation_dt = market_start_gate_cache[current_date]
        if gate_status != GATE_LOWER_PASSED:
            continue
        strategy_type = STRATEGY_LOWER

        candidate = find_trade_candidate_on_date(
            stock_item,
            current_date,
            bars_by_date,
            index_minute_bars_by_key,
            strategy_type,
            activation_dt,
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
        if strategy_type != STRATEGY_LOWER:
            continue
        optimize_loss_percent = OPTIMIZE_LOSS_PER_LOWER
        optimize_profit_percent = OPTIMIZE_PROFIT_PER_LOWER

        try:
            entry_idx = dt_values.index(entry_dt)
        except ValueError:
            continue

        effective_stop_loss = calculate_stop_loss_amount_by_percent(entry_price, optimize_loss_percent)
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
            'activation_dt': candidate.get('activation_dt'),
            'industry_code': industry_code,
            'date_str': date_str,
            'entry_dt': entry_dt,
            'entry_price': entry_price,
            'strategy_type': strategy_type,
            'trade_side': TRADE_SIDE_SHORT,
            'take_profit_price': take_profit_price,
            'effective_profit': effective_profit,
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def apply_lower_percent_exit(
    all_results: list,
    minute_bars_by_symbol: dict[str, dict[str, list]],
    tracking_snapshots_by_date: dict[str, list[dict]] | None = None,
) -> list:
    """啟動後每十分鐘檢核上漲家數比例，超標則下一根 open 全平倉。"""
    results_by_date: dict[str, list] = {}
    for signal, result in all_results:
        results_by_date.setdefault(signal['date_str'], []).append((signal, result))
    adjusted_results = []
    for date_key, day_results in results_by_date.items():
        first_entry = min(signal['entry_dt'] for signal, _ in day_results)
        activation_dt = day_results[0][0].get('activation_dt') or first_entry.replace(
            hour=LOWER_STRATEGY_DECISION_1[0], minute=LOWER_STRATEGY_DECISION_1[1],
            second=0, microsecond=0,
        )
        snapshots = build_lower_tracking_snapshots(activation_dt, day_results, minute_bars_by_symbol)
        if tracking_snapshots_by_date is not None:
            tracking_snapshots_by_date[date_key] = snapshots
        trigger_snapshot = next((snapshot for snapshot in snapshots if snapshot['triggered']), None)
        trigger_dt = trigger_snapshot['dt'] if trigger_snapshot is not None else None
        if trigger_snapshot is not None:
            print(
                f'{date_key} LOWER 比例出場觸發={trigger_dt:%H:%M} '
                f'上漲比例=+{trigger_snapshot["up_percent"]:.2f}% '
                f'> 平倉門檻={LOWER_EXIT_PERCENT_THRESHOLD:g}% '
                '（該分鐘收完後，下一根個股 K 的 open 平倉；當日不再入場）'
            )
        for signal, result in day_results:
            if trigger_dt is None:
                adjusted_results.append((signal, result))
                continue
            if signal['entry_dt'] > trigger_dt:
                continue
            # 觸發當根已停損停利的部位保留；下一根 open 優先於其高低價。
            if result['exit_dt'] > trigger_dt:
                bars = minute_bars_by_symbol[signal['name']].get(date_key, [])
                exit_bar = next(
                    (bar for bar in bars if trigger_dt < bar['dt'] <= result['exit_dt']),
                    None,
                )
                if exit_bar is not None:
                    result = build_outcome_result(
                        'lower_percent', signal['entry_price'], float(exit_bar['open']),
                    )
                    result['exit_dt'] = exit_bar['dt']
                    result['lower_percent_trigger_dt'] = trigger_dt
                    result['lower_percent_trigger_percent'] = trigger_snapshot['up_percent']
                    result['lower_percent_exit_threshold'] = LOWER_EXIT_PERCENT_THRESHOLD
                # 無後續 K 時保留原結算，不虛構可成交價格。
            adjusted_results.append((signal, result))
    return adjusted_results


def main() -> None:
    OUTPUT_BUFFER.clear()
    builtins.print(f'開始時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    builtins.print()
    try:
        args = parse_args()
        if HISTORICAL_CACHE_DATE:
            try:
                if not re.fullmatch(r'\d{8}', HISTORICAL_CACHE_DATE):
                    raise ValueError('歷史日期必須為 YYYYMMDD')
                target_date = datetime.strptime(HISTORICAL_CACHE_DATE, '%Y%m%d').date()
                cache_path = get_api_cache_path(target_date)
                analysis_stock_list, minute_bars_by_symbol, index_minute_bars_by_key = (
                    load_historical_api_cache(cache_path)
                )
            except ValueError as exc:
                print(f'[ERROR] {exc}', file=sys.stderr)
                sys.exit(1)
            lower_decision_stocks[:] = analysis_stock_list
            cached = (minute_bars_by_symbol, index_minute_bars_by_key)
            print(f'歷史 cache 模式：目標日期={target_date}，股票數={len(analysis_stock_list)}')
            print('回測與 LOWER 比例判斷使用 cache 全部股票；忽略 STOCK_DATA_SOURCE、STOCK_LIST 與 --to。')
            print('未使用 stock_data.py；略過產業指數進場過濾、產業排除及產業分類統計。')
        else:
            if STOCK_DATA_SOURCE == "S":
                source_stock_list = selected_stocks
                stock_source_name = 'selected_stocks'
            elif STOCK_DATA_SOURCE == "T":
                source_stock_list = lower_decision_stocks
                stock_source_name = 'total_stocks'
            else:
                raise ValueError('STOCK_DATA_SOURCE 必須設定為 "S" 或 "T"')
            raw_stock_list = STOCK_LIST or source_stock_list
            if STOCK_LIST:
                stock_source_name = 'STOCK_LIST'
            stock_list = filter_stocks_by_excluded_industries(raw_stock_list)
            raw_analysis_stock_list = dedupe_stock_list([raw_stock_list])
            analysis_stock_list = filter_stocks_by_excluded_industries(raw_analysis_stock_list)
            excluded_stock_count = len(raw_analysis_stock_list) - len(analysis_stock_list)
            analysis_stock_list = ensure_backtest_industry_index_metadata(analysis_stock_list)
            eligible_names = {item[0] for item in analysis_stock_list}
            stock_list = [item for item in stock_list if item[0] in eligible_names]
            if EXCLUDED_INDUSTRY_CODES:
                print(
                    f'已排除產業別: {EXCLUDED_INDUSTRY_CODES} '
                    f'排除股票數={excluded_stock_count} '
                    f'剩餘統一回測股票數={len(analysis_stock_list)}'
                )
            print(
                f'{stock_source_name}來源數={len(stock_list)}  '
                f'統一回測股票池={len(analysis_stock_list)}'
            )
            if not analysis_stock_list:
                print('[WARN] 沒有符合回測條件的股票，結束本次回測')
                return

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
            # 進場比例使用 total_stocks，平倉比例使用 selected_stocks；備齊兩者的分 K。
            decision_stock_list = dedupe_stock_list([lower_decision_stocks, selected_stocks])
            minute_data_stock_list = dedupe_stock_list([analysis_stock_list, decision_stock_list])
            cached = load_api_cache(cache_path, analysis_stock_list, decision_stock_list)
        if cached is not None:
            minute_bars_by_symbol, index_minute_bars_by_key = cached
            print(f'已載入API快取: {cache_path.name}')
        else:
            # 初始化 SDK
            _, rest_stock = init_sdk(args.config)

            # 先蒐集分K（raw + parsed）
            minute_raw_by_symbol: dict[str, list] = {}
            minute_bars_by_symbol: dict[str, dict[str, list]] = {}
            index_minute_raw_by_key: dict[str, list] = {}
            index_minute_bars_by_key: dict[str, dict[str, list]] = {}
            total_stocks = len(minute_data_stock_list)
            for idx, stock_item in enumerate(minute_data_stock_list, start=1):
                stock_name = stock_item[0]
                print_api_progress(idx, total_stocks, stock_name)
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
                minute_data_stock_list,
                minute_raw_by_symbol,
                index_minute_raw_by_key,
            )
            print(f'已儲存API快取: {cache_path.name}')

        total_stocks = len(analysis_stock_list)

        def evaluate_one_window() -> dict:
            all_candidates: list = []
            market_start_gate_cache: dict[date, tuple[str, datetime | None]] = {}
            progress_idx = 0
            for stock_item in analysis_stock_list:
                progress_idx += 1
                stock_name = stock_item[0]
                print_progress(progress_idx, total_stocks, f'{stock_name} [LOWER]')
                all_candidates.extend(
                    collect_trade_candidates(
                        stock_item,
                        analysis_stock_list,
                        target_date,
                        minute_bars_by_symbol,
                        index_minute_bars_by_key,
                        market_start_gate_cache,
                    )
                )

            results = evaluate_candidates(
                all_candidates,
                print_results=False,
            )
            tracking_snapshots_by_date: dict[str, list[dict]] = {}
            results = apply_lower_percent_exit(results, minute_bars_by_symbol, tracking_snapshots_by_date)
            return {
                'best_results': results,
                'market_start_gate_cache': market_start_gate_cache,
                'tracking_snapshots_by_date': tracking_snapshots_by_date,
            }

        lower_market_previous_close_reversal_start_hm = (
            LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[0] * 60
            + LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME[1]
        )
        lower_strategy_early_breakout_deadline_hm = (
            LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[0] * 60
            + LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE[1]
        )
        lower_strategy_decision_hm = LOWER_STRATEGY_DECISION_1[0] * 60 + LOWER_STRATEGY_DECISION_1[1]
        try:
            validate_lower_strategy_windows()
        except ValueError as exc:
            print(f'[ERROR] {exc}', file=sys.stderr)
            sys.exit(1)
        if MIN_MINUTE_BARS_BEFORE_0930 <= 0:
            print('[ERROR] MIN_MINUTE_BARS_BEFORE_0930 必須大於 0', file=sys.stderr)
            sys.exit(1)
        if not (0 <= lower_market_previous_close_reversal_start_hm <= 23 * 60 + 59):
            print(
                '[ERROR] LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 設定錯誤，需介於 00:00~23:59',
                file=sys.stderr,
            )
            sys.exit(1)
        if not (0 <= lower_strategy_early_breakout_deadline_hm <= 23 * 60 + 59):
            print(
                '[ERROR] LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE 設定錯誤，需介於 00:00~23:59',
                file=sys.stderr,
            )
            sys.exit(1)
        if lower_strategy_early_breakout_deadline_hm >= lower_strategy_decision_hm:
            print(
                '[ERROR] LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE 必須早於 LOWER_STRATEGY_DECISION_1',
                file=sys.stderr,
            )
            sys.exit(1)
        if lower_market_previous_close_reversal_start_hm > lower_strategy_early_breakout_deadline_hm:
            print(
                '[ERROR] LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME '
                '不可晚於 LOWER_STRATEGY_EARLY_BREAKOUT_DEADLINE',
                file=sys.stderr,
            )
            sys.exit(1)
        if lower_market_previous_close_reversal_start_hm >= lower_strategy_decision_hm:
            print(
                '[ERROR] LOWER_MARKET_PREVIOUS_CLOSE_REVERSAL_START_TIME 必須早於 LOWER_STRATEGY_DECISION_1',
                file=sys.stderr,
            )
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
        if not (
            0 <= LOWER_DECISION_DECLINE_PERCENT_THRESHOLD
            < LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD <= 100
        ):
            print(
                '[ERROR] lower 股票比例門檻須符合 0 <= '
                'LOWER_DECISION_DECLINE_PERCENT_THRESHOLD < '
                'LOWER_DECISION_DECLINE_PERCENT_MAX_THRESHOLD <= 100',
                file=sys.stderr,
            )
            sys.exit(1)
        if not 0 <= LOWER_EXIT_PERCENT_THRESHOLD <= 100:
            print('[ERROR] LOWER_EXIT_PERCENT_THRESHOLD 必須介於 0 與 100', file=sys.stderr)
            sys.exit(1)
        if INDUSTRY_MARKET_FILTER_SHORT_PERCENT < 0:
            print('[ERROR] INDUSTRY_MARKET_FILTER_SHORT_PERCENT 不可小於 0', file=sys.stderr)
            sys.exit(1)
        best_window_result = evaluate_one_window()
        builtins.print()
        best_results = best_window_result['best_results']
        market_start_gate_cache = best_window_result['market_start_gate_cache']

        print_daily_optimization_results(
            best_results,
            index_minute_bars_by_key,
            minute_bars_by_symbol,
            analysis_stock_list,
            market_start_gate_cache,
            best_window_result['tracking_snapshots_by_date'],
        )
    finally:
        builtins.print(f'結束時間: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        flush_each_stock_output_file()


if __name__ == '__main__':
    main()
