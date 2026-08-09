import argparse
import configparser
import json
import math
import sys
import time
from datetime import date, datetime, time as datetime_time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from esun_marketdata import EsunMarketdata
from Z_ORB_ONE.stock_data import selected_stocks


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / 'config.ini'
OUTPUT_FILE = Path(__file__).with_name('analysis_two_day_price_limits_result.txt')
PDF_OUTPUT_DIR = Path(__file__).with_name('pdf_folder')
API_REQUEST_DELAY_SEC = 1
FETCH_BUFFER_DAYS = 14
DEFAULT_SCAN_DAYS = 40
BROKERAGE_FEE_RATE = 0.001425
DAY_TRADE_TAX_RATE = 0.0015
CACHE_VERSION = 2

SHOW_LIMIT_UP_ON_CONSOLE = True
SHOW_LIMIT_DOWN_ON_CONSOLE = False

LONG_LIMIT_UP_DAYS = 2
SHORT_LIMIT_DOWN_DAYS = 2

LONG_STOP_LOSS_PERCENT = 2.0
LONG_TAKE_PROFIT_PERCENT = 10.0

SHORT_STOP_LOSS_PERCENT = 2.0
SHORT_TAKE_PROFIT_PERCENT = 10.0

ENTRY_BAR_TIME = datetime_time(9, 40)
ENTRY_BAR_TIME_END = datetime_time(10, 1)
EXIT_BAR_TIME = datetime_time(13, 0)

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']
plt.rcParams['axes.unicode_minus'] = False

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
        description='找出連續多個交易日收漲停或跌停，並列出次一交易日收盤價。'
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


def is_limit_sequence(candles: list[dict], start_index: int, days: int, direction: str) -> bool:
    for offset in range(days):
        current_index = start_index + offset
        current = candles[current_index]
        previous = candles[current_index - 1]
        limit_up, limit_down = calculate_limit_prices(previous['close'])
        limit_price = limit_up if direction == '漲停' else limit_down
        if not is_same_price(current['close'], limit_price):
            return False
    return True


def find_limit_events(
    candles: list[dict],
    scan_from: date,
    scan_to: date,
) -> list[tuple[dict, dict, dict, str]]:
    events: list[tuple[dict, dict, dict, str]] = []
    direction_days = (
        ('漲停', LONG_LIMIT_UP_DAYS),
        ('跌停', SHORT_LIMIT_DOWN_DAYS),
    )
    for direction, days in direction_days:
        if days < 1:
            raise ValueError(f'{direction}連續天數必須大於等於 1: {days}')
        for index in range(1, len(candles) - days):
            first = candles[index]
            last = candles[index + days - 1]
            next_day = candles[index + days]

            if any(
                not (scan_from <= candles[index + offset]['date'] <= scan_to)
                for offset in range(days)
            ):
                continue

            if is_limit_sequence(candles, index, days, direction):
                events.append((first, last, next_day, direction))
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
            close_price = float(item['close'])
            bar = {
                'dt': bar_dt,
                'open': float(item['open']),
                'high': float(item['high']),
                'low': float(item['low']),
                'close': close_price,
                'volume': int(item.get('volume', 0) or 0),
                'average': float(item.get('average', close_price) or close_price),
            }
        except (KeyError, TypeError, ValueError):
            continue
        candles_by_date.setdefault(bar_dt.date(), {})[bar_dt.time()] = bar
    return candles_by_date


def get_stock_atr_value(stock_item: tuple) -> float | None:
    if len(stock_item) <= 7 or stock_item[7] is None:
        return None
    try:
        return float(stock_item[7])
    except (TypeError, ValueError):
        return None


def get_stock_label(stock_item: tuple) -> str:
    return str(stock_item[0])


def format_tw_price(price: float) -> str:
    tick = get_tick_size(price)
    if tick < 0.1:
        return f'{price:.2f}'
    if tick < 1:
        return f'{price:.1f}'
    return f'{price:.0f}'


def apply_plain_price_y_axis(ax) -> None:
    ax.yaxis.get_offset_text().set_visible(False)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda value, _pos: format_tw_price(value))
    )


def apply_plain_volume_y_axis(ax) -> None:
    ax.yaxis.get_offset_text().set_visible(False)
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda value, _pos: f'{int(round(value)):,}')
    )


def style_inside_right_y_ticks(ax, labelsize: int = 8, pad: int = -10) -> None:
    ax.yaxis.tick_right()
    ax.tick_params(
        axis='y',
        labelsize=labelsize,
        labelright=True,
        labelleft=False,
        right=False,
        left=False,
        pad=pad,
    )
    for label in ax.get_yticklabels():
        label.set_horizontalalignment('right')


def level_in_axis_range(ax, level: float | None) -> bool:
    if level is None:
        return False
    ymin, ymax = ax.get_ylim()
    lower, upper = sorted((ymin, ymax))
    return lower <= level <= upper


def apply_limit_ticks(ax, limit_up: float | None, limit_down: float | None) -> None:
    limit_levels = [value for value in (limit_up, limit_down) if value is not None]
    if not limit_levels:
        return

    ymin, ymax = ax.get_ylim()
    merged_min = min([ymin, *limit_levels])
    merged_max = max([ymax, *limit_levels])
    pad = max((merged_max - merged_min) * 0.02, get_tick_size(merged_max) * 2)
    ax.set_ylim(merged_min - pad, merged_max + pad)

    merged_ticks = sorted(set([float(tick) for tick in ax.get_yticks()] + limit_levels))
    ax.set_yticks(merged_ticks)
    ax.set_yticklabels([format_tw_price(tick) for tick in merged_ticks])

    for label, tick in zip(ax.get_yticklabels(), merged_ticks):
        if limit_up is not None and math.isclose(tick, limit_up, abs_tol=1e-9):
            label.set_color('crimson')
        elif limit_down is not None and math.isclose(tick, limit_down, abs_tol=1e-9):
            label.set_color('darkgreen')


def make_format_coord(x_vals, dt_vals, opens, highs, lows, closes, volumes):
    row_count = len(x_vals)
    if row_count == 0:
        return lambda _x, _y: ''

    def _format_coord(xdata, _ydata):
        if xdata is None:
            return ''
        idx = min(range(row_count), key=lambda item_index: abs(x_vals[item_index] - xdata))
        return (
            f"{dt_vals[idx].strftime('%H:%M')} "
            f"O:{opens[idx]:.2f} H:{highs[idx]:.2f} "
            f"L:{lows[idx]:.2f} C:{closes[idx]:.2f} V:{int(volumes[idx]):,}"
        )

    return _format_coord


def format_bar_time(bar_time: datetime_time) -> str:
    return bar_time.strftime('%H:%M')


def add_intraday_marker(ax, volume_ax, target_date: date, marker_time: datetime_time, label: str, color: str) -> None:
    x_value = mdates.date2num(datetime.combine(target_date, marker_time))
    for target_ax in (ax, volume_ax):
        target_ax.axvline(
            x=x_value,
            color=color,
            linestyle='--',
            linewidth=1.1,
            alpha=0.75,
            zorder=3,
        )
    ymin, ymax = ax.get_ylim()
    ax.text(
        x_value,
        ymax,
        label,
        color=color,
        fontsize=8,
        ha='center',
        va='bottom',
        rotation=90,
    )


def draw_intraday_ohlc_pdf_page(
    stock_item: tuple,
    event: tuple[dict, dict, dict, str],
    result: dict,
    historical_rows: list[dict],
):
    first, second, next_day, direction = event
    target_date = next_day['date']
    fig, (price_ax, volume_ax) = plt.subplots(
        2,
        1,
        figsize=(20, 10),
        gridspec_kw={'height_ratios': [4.5, 1.5], 'hspace': 0.05},
    )

    rows = sorted(historical_rows, key=lambda row: row['dt'])
    dates_dt = [row['dt'] for row in rows]
    opens = [row['open'] for row in rows]
    highs = [row['high'] for row in rows]
    lows = [row['low'] for row in rows]
    closes = [row['close'] for row in rows]
    volumes = [row.get('volume', 0) for row in rows]
    averages = [row.get('average', row['close']) for row in rows]
    x_values = mdates.date2num(dates_dt) if rows else np.array([])

    if len(x_values) >= 2:
        x_diffs = np.diff(x_values)
        positive_diffs = x_diffs[x_diffs > 0]
        base_width = float(np.median(positive_diffs)) if len(positive_diffs) else (1 / 1440)
    else:
        base_width = 1 / 1440
    tick_width = base_width * 0.35

    prev_close = second['close']
    limit_up, limit_down = calculate_limit_prices(prev_close)
    stock_label = get_stock_label(stock_item)
    side = '做多' if direction == '漲停' else '放空'
    atr_value = get_stock_atr_value(stock_item)
    title_parts = [
        f'{stock_label} {target_date:%Y-%m-%d}',
        f'{direction}{side}',
        f'連續期間:{first["date"]:%Y-%m-%d}~{second["date"]:%Y-%m-%d}',
        f'昨收:{format_tw_price(prev_close)}',
        f'漲停:{format_tw_price(limit_up)}',
        f'跌停:{format_tw_price(limit_down)}',
    ]
    if rows:
        title_parts.extend(
            [
                f'開盤:{format_tw_price(opens[0])}',
                f'收盤:{format_tw_price(closes[-1])}',
                f'最高:{format_tw_price(max(highs))}',
                f'最低:{format_tw_price(min(lows))}',
            ]
        )
    if atr_value is not None:
        title_parts.append(f'ATR:{atr_value:.2f}')
    title_parts.extend(
        [
            f'進場:{result["entry_price"]:.2f}',
            f'出場:{result["exit_price"]:.2f}',
            f'淨報酬:{result["net_return"]:.2f}%',
        ]
    )
    if result.get('take_profit_price') is not None:
        title_parts.append(f'停利:{result["take_profit_price"]:.2f}')
    if result.get('stop_loss_price') is not None:
        title_parts.append(f'停損:{result["stop_loss_price"]:.2f}')
    price_ax.set_title(' '.join(title_parts), pad=2)

    price_ax.grid(True)
    volume_ax.grid(True, axis='y', alpha=0.3)
    style_inside_right_y_ticks(price_ax)
    style_inside_right_y_ticks(volume_ax)

    if not rows:
        price_ax.text(0.5, 0.5, 'No intraday data', ha='center', va='center', transform=price_ax.transAxes)
        volume_ax.text(0.5, 0.5, 'No volume data', ha='center', va='center', transform=volume_ax.transAxes)
    else:
        bar_colors = []
        for idx in range(len(x_values)):
            open_price = opens[idx]
            high_price = highs[idx]
            low_price = lows[idx]
            close_price = closes[idx]
            if close_price > open_price:
                color = 'red'
            elif close_price < open_price:
                color = 'green'
            else:
                color = 'black'
            bar_colors.append(color)
            price_ax.vlines(x_values[idx], low_price, high_price, color=color, linewidth=1.2)
            price_ax.hlines(open_price, x_values[idx] - tick_width, x_values[idx], color=color, linewidth=1.2)
            price_ax.hlines(close_price, x_values[idx], x_values[idx] + tick_width, color=color, linewidth=1.2)

        price_ax.plot(x_values, averages, color='#f59e0b', linewidth=1.6, label='AVG', zorder=4)
        volume_ax.bar(
            x_values,
            volumes,
            width=tick_width * 1.6,
            color=bar_colors,
            edgecolor=bar_colors,
            alpha=0.85,
        )
        volume_ax.set_ylim(0, max(max(volumes), 1) * 1.25)
        volume_ax.set_ylabel('Volume', fontsize=9)

        x0 = mdates.date2num(datetime.combine(target_date, datetime_time(8, 59)))
        x1 = mdates.date2num(datetime.combine(target_date, datetime_time(13, 31)))
        price_ax.set_xlim(x0, x1)
        volume_ax.set_xlim(x0, x1)
        locator = mdates.AutoDateLocator(minticks=6, maxticks=12)
        price_ax.xaxis.set_major_locator(locator)
        price_ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        price_ax.tick_params(axis='x', labelbottom=False)
        volume_ax.xaxis.set_major_locator(locator)
        volume_ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        volume_ax.tick_params(axis='x', rotation=0)
        price_ax.format_coord = make_format_coord(
            x_values, dates_dt, opens, highs, lows, closes, volumes
        )

    apply_limit_ticks(price_ax, limit_up, limit_down)
    apply_plain_price_y_axis(price_ax)
    apply_plain_volume_y_axis(volume_ax)

    price_ax.axhline(
        y=prev_close,
        color='blue',
        linestyle='--',
        linewidth=1.0,
        alpha=0.8,
        label='昨日收盤',
    )
    if level_in_axis_range(price_ax, limit_up):
        price_ax.axhline(y=limit_up, color='crimson', linestyle=':', linewidth=1.0, alpha=0.8, label='漲停')
    if level_in_axis_range(price_ax, limit_down):
        price_ax.axhline(y=limit_down, color='darkgreen', linestyle=':', linewidth=1.0, alpha=0.8, label='跌停')
    if result.get('take_profit_price') is not None:
        price_ax.axhline(
            y=result['take_profit_price'],
            color='#0f766e',
            linestyle='-.',
            linewidth=1.0,
            alpha=0.85,
            label='停利',
        )
    if result.get('stop_loss_price') is not None:
        price_ax.axhline(
            y=result['stop_loss_price'],
            color='#dc2626',
            linestyle='-.',
            linewidth=1.0,
            alpha=0.85,
            label='停損',
        )

    add_intraday_marker(
        price_ax,
        volume_ax,
        target_date,
        result.get('entry_time', ENTRY_BAR_TIME),
        '進場',
        '#7c3aed',
    )
    add_intraday_marker(price_ax, volume_ax, target_date, result['exit_time'], '出場', '#111827')

    handles, _labels = price_ax.get_legend_handles_labels()
    if handles:
        price_ax.legend(
            loc='upper left',
            fontsize=8,
            framealpha=0.2,
            facecolor='white',
            edgecolor='gray',
        )
    fig.subplots_adjust(left=0.03, right=0.985, top=0.95, bottom=0.08, hspace=0.05)
    return fig


def build_entered_intraday_pdf(
    scan_from: date,
    scan_to: date,
    pdf_entries: list[dict],
) -> Path | None:
    PDF_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_stem = (
        f'analysis_two_day_price_limits_intraday_'
        f'{scan_from:%Y%m%d}_{scan_to:%Y%m%d}'
    )
    output_path = PDF_OUTPUT_DIR / (
        f'{output_stem}_{datetime.now():%Y%m%d_%H%M%S}.pdf'
    )

    if not pdf_entries:
        return None

    with PdfPages(output_path) as pdf:
        for entry in pdf_entries:
            fig = draw_intraday_ohlc_pdf_page(
                entry['stock_item'],
                entry['event'],
                entry['result'],
                entry['historical_rows'],
            )
            pdf.savefig(fig)
            plt.close(fig)
    return output_path


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


def find_short_entry_from_two_bars(
    bars_by_time: dict[datetime_time, dict],
    previous_close: float,
) -> tuple[dict | None, str | None]:
    candidate_times = sorted(
        bar_time
        for bar_time in bars_by_time
        if ENTRY_BAR_TIME <= bar_time <= ENTRY_BAR_TIME_END
    )
    if len(candidate_times) < 2:
        return None, (
            f'找不到 {format_bar_time(ENTRY_BAR_TIME)}～'
            f'{format_bar_time(ENTRY_BAR_TIME_END)} 的連續2根分K'
        )

    for first_time, second_time in zip(candidate_times, candidate_times[1:]):
        first_bar = bars_by_time[first_time]
        second_bar = bars_by_time[second_time]
        if second_bar['dt'] - first_bar['dt'] != timedelta(minutes=1):
            continue
        if first_bar['high'] <= previous_close and second_bar['high'] > previous_close:
            return second_bar, None

    return None, None


def find_long_entry_from_two_bars(
    bars_by_time: dict[datetime_time, dict],
    previous_close: float,
) -> tuple[dict | None, str | None]:
    candidate_times = sorted(
        bar_time
        for bar_time in bars_by_time
        if ENTRY_BAR_TIME <= bar_time <= ENTRY_BAR_TIME_END
    )
    if len(candidate_times) < 2:
        return None, (
            f'找不到 {format_bar_time(ENTRY_BAR_TIME)}～'
            f'{format_bar_time(ENTRY_BAR_TIME_END)} 的連續2根分K'
        )

    for first_time, second_time in zip(candidate_times, candidate_times[1:]):
        first_bar = bars_by_time[first_time]
        second_bar = bars_by_time[second_time]
        if second_bar['dt'] - first_bar['dt'] != timedelta(minutes=1):
            continue
        if first_bar['low'] >= previous_close and second_bar['low'] < previous_close:
            return second_bar, None

    return None, None


def resolve_short_exit(
    entry_price: float,
    trade_bars: list[dict],
    limit_down_price: float,
) -> tuple[dict, float, str]:
    fallback_bar = trade_bars[-1]
    fallback_price = fallback_bar['close']
    take_profit_price = (
        entry_price * (1 - SHORT_TAKE_PROFIT_PERCENT / 100)
        if SHORT_TAKE_PROFIT_PERCENT > 0
        else None
    )
    stop_loss_price = (
        entry_price * (1 + SHORT_STOP_LOSS_PERCENT / 100)
        if SHORT_STOP_LOSS_PERCENT > 0
        else None
    )

    for bar in trade_bars:
        if stop_loss_price is not None and bar['high'] >= stop_loss_price:
            return bar, stop_loss_price, f'停損{SHORT_STOP_LOSS_PERCENT:.2f}%'
        if bar['low'] <= limit_down_price:
            return bar, limit_down_price, '跌停出場'
        if take_profit_price is not None and bar['low'] <= take_profit_price:
            return bar, take_profit_price, f'停利{SHORT_TAKE_PROFIT_PERCENT:.2f}%'

    return fallback_bar, fallback_price, '收盤'


def resolve_long_exit(
    entry_price: float,
    trade_bars: list[dict],
    limit_up_price: float,
) -> tuple[dict, float, str]:
    fallback_bar = trade_bars[-1]
    fallback_price = fallback_bar['close']
    take_profit_price = (
        entry_price * (1 + LONG_TAKE_PROFIT_PERCENT / 100)
        if LONG_TAKE_PROFIT_PERCENT > 0
        else None
    )
    stop_loss_price = (
        entry_price * (1 - LONG_STOP_LOSS_PERCENT / 100)
        if LONG_STOP_LOSS_PERCENT > 0
        else None
    )

    for bar in trade_bars:
        if stop_loss_price is not None and bar['low'] <= stop_loss_price:
            return bar, stop_loss_price, f'停損{LONG_STOP_LOSS_PERCENT:.2f}%'
        if bar['high'] >= limit_up_price:
            return bar, limit_up_price, '漲停出場'
        if take_profit_price is not None and bar['high'] >= take_profit_price:
            return bar, take_profit_price, f'停利{LONG_TAKE_PROFIT_PERCENT:.2f}%'

    return fallback_bar, fallback_price, '收盤'


def analyze_intraday_event(
    event: tuple[dict, dict, dict, str],
    minute_candles_by_date: dict[date, dict[datetime_time, dict]],
) -> dict:
    _first, second, next_day, direction = event
    bars_by_time = minute_candles_by_date.get(next_day['date'], {})
    if not bars_by_time:
        return {
            'status': 'data_incomplete',
            'direction': direction,
            'reason': f"缺少{next_day['date'].isoformat()} 整日分K",
        }

    entry_price_label = f'{format_bar_time(ENTRY_BAR_TIME)}進場開盤價'
    if direction == '跌停':
        entry_bar, incomplete_reason = find_short_entry_from_two_bars(
            bars_by_time,
            second['close'],
        )
        if incomplete_reason is not None:
            return {
                'status': 'data_incomplete',
                'direction': direction,
                'reason': incomplete_reason,
            }
        if entry_bar is None:
            return {
                'status': 'rejected',
                'direction': direction,
                'reason': (
                    f'{format_bar_time(ENTRY_BAR_TIME)}～'
                    f'{format_bar_time(ENTRY_BAR_TIME_END)} 未符合作空2根分K入場條件'
                ),
            }
        entry_price = entry_bar['high']
        entry_price_label = f'{format_bar_time(entry_bar["dt"].time())}作空觸發價'
    else:
        entry_bar, incomplete_reason = find_long_entry_from_two_bars(
            bars_by_time,
            second['close'],
        )
        if incomplete_reason is not None:
            return {
                'status': 'data_incomplete',
                'direction': direction,
                'reason': incomplete_reason,
            }
        if entry_bar is None:
            return {
                'status': 'rejected',
                'direction': direction,
                'reason': (
                    f'{format_bar_time(ENTRY_BAR_TIME)}～'
                    f'{format_bar_time(ENTRY_BAR_TIME_END)} 未符合作多2根分K入場條件'
                ),
            }
        entry_price = entry_bar['low']
        entry_price_label = f'{format_bar_time(entry_bar["dt"].time())}作多觸發價'

    entry_time = entry_bar['dt'].time()
    eligible_times = sorted(
        bar_time
        for bar_time in bars_by_time
        if entry_time <= bar_time <= EXIT_BAR_TIME
    )
    if not eligible_times:
        return {
            'status': 'data_incomplete',
            'direction': direction,
            'reason': (
                f'找不到 {format_bar_time(entry_time)}～'
                f'{EXIT_BAR_TIME.strftime("%H:%M")} 的交易分K'
            ),
        }

    trade_bars = [bars_by_time[bar_time] for bar_time in eligible_times]
    if entry_price <= 0:
        return {
            'status': 'data_incomplete',
            'direction': direction,
            'reason': f'{entry_price_label}無效: {entry_price:.2f}',
        }

    if direction == '漲停':
        limit_up_price, _limit_down_price = calculate_limit_prices(second['close'])
        exit_bar, exit_price, exit_reason = resolve_long_exit(
            entry_price,
            trade_bars,
            limit_up_price,
        )
        gross_return = (exit_price - entry_price) / entry_price * 100
        mfe = (max(bar['high'] for bar in trade_bars) - entry_price) / entry_price * 100
        mae = (entry_price - min(bar['low'] for bar in trade_bars)) / entry_price * 100
        take_profit_price = (
            entry_price * (1 + LONG_TAKE_PROFIT_PERCENT / 100)
            if LONG_TAKE_PROFIT_PERCENT > 0
            else None
        )
        stop_loss_price = (
            entry_price * (1 - LONG_STOP_LOSS_PERCENT / 100)
            if LONG_STOP_LOSS_PERCENT > 0
            else None
        )
        limit_price = limit_up_price
    else:
        _limit_up_price, limit_down_price = calculate_limit_prices(second['close'])
        exit_bar, exit_price, exit_reason = resolve_short_exit(
            entry_price,
            trade_bars,
            limit_down_price,
        )
        gross_return = (entry_price - exit_price) / entry_price * 100
        mfe = (entry_price - min(bar['low'] for bar in trade_bars)) / entry_price * 100
        mae = (max(bar['high'] for bar in trade_bars) - entry_price) / entry_price * 100
        take_profit_price = (
            entry_price * (1 - SHORT_TAKE_PROFIT_PERCENT / 100)
            if SHORT_TAKE_PROFIT_PERCENT > 0
            else None
        )
        stop_loss_price = (
            entry_price * (1 + SHORT_STOP_LOSS_PERCENT / 100)
            if SHORT_STOP_LOSS_PERCENT > 0
            else None
        )
        limit_price = limit_down_price

    return {
        'status': 'entered',
        'direction': direction,
        'entry_time': entry_time,
        'entry_price': entry_price,
        'take_profit_price': take_profit_price,
        'stop_loss_price': stop_loss_price,
        'limit_price': limit_price,
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
        return f"  分K資料不足：{result['reason']}，未納入當沖統計"
    if result['status'] == 'rejected':
        reason = result.get('reason', '未符合入場條件')
        return f"  {reason}，不進場"
    side = '做多' if result['direction'] == '漲停' else '放空'
    risk_parts = []
    if result.get('take_profit_price') is not None:
        risk_parts.append(f"停利={result['take_profit_price']:.2f}")
    if result.get('stop_loss_price') is not None:
        risk_parts.append(f"停損={result['stop_loss_price']:.2f}")
    risk_text = (' ' + ' '.join(risk_parts)) if risk_parts else ''
    return (
        f"  {side} {format_bar_time(result.get('entry_time', ENTRY_BAR_TIME))}進場={result['entry_price']:.2f} "
        f"{result['exit_time'].strftime('%H:%M')}{result['exit_reason']}={result['exit_price']:.2f} "
        f"毛報酬={result['gross_return']:.2f}% 淨報酬={result['net_return']:.2f}% "
        f"MFE={result['mfe']:.2f}% MAE={result['mae']:.2f}%{risk_text}"
    )


def get_report_priority(result: dict) -> int:
    return 1 if result['status'] == 'entered' else 0


def build_intraday_summary(direction: str, results: list[dict]) -> list[str]:
    side = '做多' if direction == '漲停' else '放空'
    limit_days = LONG_LIMIT_UP_DAYS if direction == '漲停' else SHORT_LIMIT_DOWN_DAYS
    direction_label = (
        f'連漲{limit_days}天'
        if direction == '漲停'
        else f'連跌{limit_days}天'
    )
    entry_text = (
        f'{format_bar_time(ENTRY_BAR_TIME)}～'
        f'{format_bar_time(ENTRY_BAR_TIME_END)} 2根分K觸發進場'
    )
    take_profit_percent = (
        LONG_TAKE_PROFIT_PERCENT if direction == '漲停' else SHORT_TAKE_PROFIT_PERCENT
    )
    stop_loss_percent = (
        LONG_STOP_LOSS_PERCENT if direction == '漲停' else SHORT_STOP_LOSS_PERCENT
    )
    take_profit_text = (
        f'{take_profit_percent:.2f}%'
        if take_profit_percent > 0
        else '關閉'
    )
    stop_loss_text = (
        f'{stop_loss_percent:.2f}%'
        if stop_loss_percent > 0
        else '關閉'
    )
    risk_text = f'、停利{take_profit_text}、停損{stop_loss_text}'
    lines = [
        (
            f'當沖{side}統計（{entry_text}、'
            f'最晚{format_bar_time(EXIT_BAR_TIME)}出場{risk_text}）'
        )
    ]
    entered = [result for result in results if result['status'] == 'entered']
    rejected = sum(result['status'] == 'rejected' for result in results)
    incomplete = sum(result['status'] == 'data_incomplete' for result in results)
    wins = sum(result['net_return'] > 0 for result in entered)
    losses = len(entered) - wins
    lines.append(
        f'{direction_label}{side}: 候選={len(results)} 訊號成立={len(entered)} '
        f'未成立={rejected} 資料不足={incomplete} '
        f'淨利成功={wins} 淨利失敗={losses}'
    )
    if entered:
        average_gross = sum(result['gross_return'] for result in entered) / len(entered)
        average_net = sum(result['net_return'] for result in entered) / len(entered)
        average_mfe = sum(result['mfe'] for result in entered) / len(entered)
        average_mae = sum(result['mae'] for result in entered) / len(entered)
        lines.append(
            f'淨勝率={wins / len(entered) * 100:.2f}% '
            f'平均毛報酬={average_gross:.2f}% 平均淨報酬={average_net:.2f}% '
            f'平均MFE={average_mfe:.2f}% 平均MAE={average_mae:.2f}%'
        )
    return lines


def should_show_on_console(direction: str) -> bool:
    if direction == '漲停':
        return SHOW_LIMIT_UP_ON_CONSOLE
    return SHOW_LIMIT_DOWN_ON_CONSOLE


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

    intraday_results_by_direction: dict[str, list[dict]] = {'漲停': [], '跌停': []}
    report_entries: list[dict] = []
    pdf_entries: list[dict] = []
    report_lines = [
        f'掃描期間: {scan_from.isoformat()} ~ {scan_to.isoformat()}',
        '',
    ]
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
            events = find_limit_events(candles, scan_from, scan_to)
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
                direction = event[3]
                intraday_results_by_direction[direction].append(intraday_result)
                report_entries.append(
                    {
                        'direction': direction,
                        'priority': get_report_priority(intraday_result),
                        'event_line': f'[{direction}] {format_event(stock_item, event)}',
                        'result_line': format_intraday_result(intraday_result),
                    }
                )
                if intraday_result['status'] == 'entered' and should_show_on_console(direction):
                    target_date = event[2]['date']
                    pdf_entries.append(
                        {
                            'stock_item': stock_item,
                            'event': event,
                            'result': intraday_result,
                            'historical_rows': list(
                                minute_candles_by_date.get(target_date, {}).values()
                            ),
                        }
                    )
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
    try:
        pdf_output_path = build_entered_intraday_pdf(scan_from, scan_to, pdf_entries)
    except Exception as exc:
        pdf_output_path = None
        print(f'[ERROR] 產出有入場分K PDF 失敗: {exc}', file=sys.stderr)

    for entry in sorted(report_entries, key=lambda item: item['priority']):
        report_lines.append(entry['event_line'])
        report_lines.append(entry['result_line'])
        if should_show_on_console(entry['direction']):
            print(entry['event_line'])
            print(entry['result_line'])

    report_lines.append('')
    for direction in ('漲停', '跌停'):
        summary_lines = build_intraday_summary(
            direction,
            intraday_results_by_direction[direction],
        )
        report_lines.extend(summary_lines)
        report_lines.append('')
        if should_show_on_console(direction):
            for line in summary_lines:
                print(line)

    report_lines.append(
        f'有入場分KPDF: {pdf_output_path}'
        if pdf_output_path is not None
        else '有入場分KPDF: 無有入場資料或產出失敗'
    )
    OUTPUT_FILE.write_text('\n'.join(report_lines).rstrip() + '\n', encoding='utf-8')
    if pdf_output_path is not None:
        print(f'已產出有入場分KPDF: {pdf_output_path.name}')
    print(f'已產出分析結果: {OUTPUT_FILE.name}')


if __name__ == '__main__':
    main()
