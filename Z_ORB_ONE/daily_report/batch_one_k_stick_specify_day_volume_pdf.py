from __future__ import annotations

import os
import re
import math
import sys
import time as time_module
from datetime import datetime, date, time, timedelta
from configparser import ConfigParser
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.path import Path
from matplotlib.patches import PathPatch
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import pytz

from esun_trade.sdk import SDK
from esun_marketdata import EsunMarketdata

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_ROOT = os.path.dirname(BASE_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Z_ORB_ONE.stock_data import selected_stocks

PDF_DIR = os.path.join(CURRENT_DIR, "pdf_folder")  # 產製結果資料夾
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
SPECIFIED_DATE = ""  # 指定要繪圖的日期，格式 YYYYMMDD；空值時使用今天日期


def normalize_config_paths(config: ConfigParser):
    cert_path = config.get("Cert", "Path", fallback="")
    if cert_path and not os.path.isabs(cert_path):
        config.set("Cert", "Path", os.path.join(BASE_DIR, cert_path))

# ===== 中文字型（Windows 常用：Microsoft JhengHei；若沒有可改 Noto Sans CJK TC / PingFang TC）=====
plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei"]
plt.rcParams["axes.unicode_minus"] = False


def extract_stock_code(label: str) -> str:
    """從 '南亞:1303.TW' / '1303.TW' / '1303' 擷取出 '1303'（抓第一段連續數字）。"""
    m = re.search(r"(\d+)", label)
    if not m:
        raise ValueError(f"Cannot extract stock code from: {label}")
    return m.group(1)


def parse_iso_keep_local_walltime(s: str) -> datetime:
    """把 '...+08:00' 解析後去掉 tzinfo，保留台灣牆上時間，避免時區造成空圖/偏移。"""
    dt = datetime.fromisoformat(s)
    return dt.replace(tzinfo=None)


def parse_specified_date(date_str: str) -> date:
    """將 YYYYMMDD 轉成 date；空值時使用台北時區今天日期。"""
    date_str = date_str.strip()
    if not date_str:
        return now_tpe().date()

    try:
        return datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"SPECIFIED_DATE 格式錯誤: {date_str}，需為 YYYYMMDD") from exc


def to_api_date_str(target_date: date) -> str:
    return target_date.strftime("%Y-%m-%d")


def fetch_recent_candles(rest_stock, symbol_code: str, target_date: date, lookback_days: int = 10) -> dict[date, list[dict]]:
    """一次抓指定日期往前 N 天的 historical 1 分 K，依日期分組。"""
    from_date = target_date - timedelta(days=lookback_days)
    response_data = rest_stock.historical.candles(
        **{
            "symbol": symbol_code,
            "from": to_api_date_str(from_date),
            "to": to_api_date_str(target_date),
            "timeframe": "1",
        }
    )
    time_module.sleep(1)

    grouped_rows: dict[date, list[dict]] = {}
    for row in response_data.get("data", []) or []:
        dt = parse_iso_keep_local_walltime(row["date"])
        row_date = dt.date()
        if not (from_date <= row_date <= target_date):
            continue
        if time(9, 0) <= dt.time() <= time(13, 30):
            grouped_rows.setdefault(row_date, []).append(
                {
                    "dt": dt,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row.get("volume", 0)),
                    "average": float(row.get("average", row["close"])),
                }
            )

    for day_rows in grouped_rows.values():
        day_rows.sort(key=lambda row: row["dt"])
    return grouped_rows


def calculate_previous_day_ohlc(candles_by_day: dict[date, list[dict]], symbol_code: str, target_date: date) -> tuple[date, float, float, float, float]:
    """用已抓到的 historical 資料往前最多找 10 天，回傳前一個有資料交易日的昨開高低收。"""
    for days_back in range(1, 11):
        prev_date = target_date - timedelta(days=days_back)
        prev_rows = candles_by_day.get(prev_date, [])
        if not prev_rows:
            continue

        prev_open = prev_rows[0]["open"]
        prev_high = max(row["high"] for row in prev_rows)
        prev_low = min(row["low"] for row in prev_rows)
        prev_close = prev_rows[-1]["close"]
        return prev_date, prev_open, prev_high, prev_low, prev_close

    raise ValueError(
        f"{symbol_code} 在 {to_api_date_str(target_date)} 往前 10 天內無前一營業日的資料"
    )


def get_tw_tick_size(price: float) -> float:
    """依台股價格區間回傳 tick size。"""
    if price <= 10:
        return 0.01
    if price <= 50:
        return 0.05
    if price <= 100:
        return 0.1
    if price <= 500:
        return 0.5
    if price <= 1000:
        return 1.0
    return 5.0


def format_tw_price(price: float) -> str:
    tick = get_tw_tick_size(price)
    if tick < 0.1:
        return f"{price:.2f}"
    if tick < 1:
        return f"{price:.1f}"
    return f"{price:.0f}"


def round_to_tick(price: float) -> float:
    tick = get_tw_tick_size(price)
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    rounded_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return float(rounded_units * tick_dec)


def floor_to_tick(price: float) -> float:
    tick = get_tw_tick_size(price)
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    floored_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_FLOOR)
    return float(floored_units * tick_dec)


def ceil_to_tick(price: float) -> float:
    tick = get_tw_tick_size(price)
    price_dec = Decimal(str(price))
    tick_dec = Decimal(str(tick))
    ceiled_units = (price_dec / tick_dec).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return float(ceiled_units * tick_dec)


def calculate_limit_prices(prev_close: float) -> tuple[float, float]:
    """以前一日收盤價估算台股今日漲停/跌停價。"""
    return floor_to_tick(prev_close * 1.1), ceil_to_tick(prev_close * 0.9)


def apply_limit_ticks(ax, limit_up: float | None, limit_down: float | None):
    limit_levels = [value for value in (limit_up, limit_down) if value is not None]
    if not limit_levels:
        return

    ymin, ymax = ax.get_ylim()
    merged_min = min([ymin, *limit_levels])
    merged_max = max([ymax, *limit_levels])
    pad = max((merged_max - merged_min) * 0.02, get_tw_tick_size(merged_max) * 2)
    ax.set_ylim(merged_min - pad, merged_max + pad)

    merged_ticks = sorted(set([float(tick) for tick in ax.get_yticks()] + limit_levels))
    ax.set_yticks(merged_ticks)

    labels = []
    for tick in merged_ticks:
        if limit_up is not None and math.isclose(tick, limit_up, abs_tol=1e-9):
            labels.append(f"{format_tw_price(tick)}")
        elif limit_down is not None and math.isclose(tick, limit_down, abs_tol=1e-9):
            labels.append(f"{format_tw_price(tick)}")
        else:
            labels.append(format_tw_price(tick))
    ax.set_yticklabels(labels)

    for label, tick in zip(ax.get_yticklabels(), merged_ticks):
        if limit_up is not None and math.isclose(tick, limit_up, abs_tol=1e-9):
            label.set_color("crimson")
        elif limit_down is not None and math.isclose(tick, limit_down, abs_tol=1e-9):
            label.set_color("darkgreen")


def style_inside_right_y_ticks(ax, labelsize: int = 8, pad: int = -10):
    ax.yaxis.tick_right()
    ax.tick_params(
        axis="y",
        labelsize=labelsize,
        labelright=True,
        labelleft=False,
        right=False,
        left=False,
        pad=pad,
    )
    for label in ax.get_yticklabels():
        label.set_horizontalalignment("right")


def intraday_touches_level(highs, lows, level: float | None) -> bool:
    if level is None:
        return False
    return any(low <= level <= high for low, high in zip(lows, highs))


def level_in_axis_range(ax, level: float | None) -> bool:
    if level is None:
        return False
    ymin, ymax = ax.get_ylim()
    lower, upper = sorted((ymin, ymax))
    return lower <= level <= upper


def add_bracket(ax, x_center, y, x_half_width, y_bump, direction="up", lw=1.6, y_offset=0.0):
    x0 = x_center - x_half_width
    x2 = x_center + x_half_width

    if direction == "up":
        y0 = y + y_offset
        ctrl_y = y0 + y_bump
    else:
        y0 = y - y_offset
        ctrl_y = y0 - y_bump

    verts = [(x0, y0), (x_center, ctrl_y), (x2, y0)]
    codes = [Path.MOVETO, Path.CURVE3, Path.CURVE3]
    patch = PathPatch(Path(verts, codes), fill=False, linewidth=lw)
    ax.add_patch(patch)


def make_format_coord(x_vals, dt_vals, o, h, l, c, v=None, dt_fmt="%Y-%m-%d"):
    """右下角顯示：用滑鼠 x 找最近一根 K，顯示日期/時間 + OHLC(+V)。"""
    n = len(x_vals)
    if n == 0:
        return lambda _x, _y: ""

    def _format_coord(xdata, ydata):
        if xdata is None:
            return ""
        idx = min(range(n), key=lambda i: abs(x_vals[i] - xdata))
        dt_str = dt_vals[idx].strftime(dt_fmt)
        base = (
            f"{dt_str}  "
            f"O:{o[idx]:.2f}  H:{h[idx]:.2f}  L:{l[idx]:.2f}  C:{c[idx]:.2f}"
        )
        if v is not None:
            base += f"  V:{int(v[idx]):,}"
        return base

    return _format_coord


def draw_intraday_ohlc(
    ax,
    volume_ax,
    symbol_code: str,
    target_date: date,
    historical_rows: list[dict],
    fig_title: str,
    atr_value: float | None = None,
    prev_open_loc: float | None = None,
    prev_high_loc: float | None = None,
    prev_low_loc: float | None = None,
    prev_close_loc: float | None = None,
):
    """繪製單一股票的當日分K OHLC 與下方成交量長條圖。"""

    limit_up_loc = None
    limit_down_loc = None
    if prev_close_loc is not None:
        limit_up_loc, limit_down_loc = calculate_limit_prices(prev_close_loc)

    # ---- 指定日期分K ----
    rows_i = [
        (
            row["dt"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row["average"],
        )
        for row in historical_rows
    ]

    dates_dt_i = [r[0] for r in rows_i]
    opens_i = [r[1] for r in rows_i] # K線開盤價
    highs_i = [r[2] for r in rows_i] # K線最高價
    lows_i = [r[3] for r in rows_i] # K線最低價
    closes_i = [r[4] for r in rows_i] # K線收盤價
    volumes_i = [r[5] for r in rows_i] # Ｋ線成交量
    averages_i = [r[6] for r in rows_i] # 均價(AVG)
    x_i = mdates.date2num(dates_dt_i) if rows_i else np.array([])

    # open/close 突出長度（分鐘）
    if len(x_i) >= 2:
        dx_i = np.diff(x_i)
        dx_pos = dx_i[dx_i > 0]
        dx_base_i = float(np.median(dx_pos)) if len(dx_pos) else (1 / 1440)
    else:
        dx_base_i = 1 / 1440
    tick_width_min = dx_base_i * 0.35

    open_text = format_tw_price(opens_i[0]) if opens_i else "N/A"
    atr_text = f"{atr_value:.2f}" if atr_value is not None else "N/A"
    limit_up_text = format_tw_price(limit_up_loc) if limit_up_loc is not None else "N/A"
    limit_down_text = format_tw_price(limit_down_loc) if limit_down_loc is not None else "N/A"
    ax.set_title(
        f"{fig_title} 漲停:{limit_up_text} 跌停:{limit_down_text} "
        f"開盤:{open_text} ATR:{atr_text}",
        pad=2
    )

    ax.grid(True)
    style_inside_right_y_ticks(ax)
    volume_ax.grid(True, axis="y", alpha=0.3)
    style_inside_right_y_ticks(volume_ax)

    if len(x_i) == 0:
        ax.text(0.5, 0.5, "No intraday data", ha="center", va="center", transform=ax.transAxes)
        volume_ax.text(0.5, 0.5, "No volume data", ha="center", va="center", transform=volume_ax.transAxes)
    else:
        W = 1.2 #1.8
        bar_colors = []
        for i in range(len(x_i)):
            o, h, l, c = opens_i[i], highs_i[i], lows_i[i], closes_i[i]
            if c > o:
                color = "red"
            elif c < o:
                color = "green"
            else:
                color = "black"
            bar_colors.append(color)

            ax.vlines(x_i[i], l, h, color=color, linewidth=W)
            ax.hlines(o, x_i[i] - tick_width_min, x_i[i], color=color, linewidth=W)
            ax.hlines(c, x_i[i], x_i[i] + tick_width_min, color=color, linewidth=W)
        ax.plot(x_i, averages_i, color="#f59e0b", linewidth=1.6, label="AVG", zorder=4)
        x0 = mdates.date2num(datetime.combine(target_date, time(8, 59)))
        x1 = mdates.date2num(datetime.combine(target_date, time(13, 31)))
        ax.set_xlim(x0, x1)
        volume_ax.set_xlim(x0, x1)
        volume_ax.bar(x_i, volumes_i, width=tick_width_min * 1.6, color=bar_colors, edgecolor=bar_colors, alpha=0.85)
        volume_ax.set_ylim(0, max(max(volumes_i), 1) * 1.25)
        volume_ax.set_ylabel("Volume", fontsize=9)

        locator_i = mdates.AutoDateLocator(minticks=6, maxticks=12)
        ax.xaxis.set_major_locator(locator_i)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.tick_params(axis="x", labelbottom=False)
        volume_ax.xaxis.set_major_locator(locator_i)
        volume_ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        volume_ax.tick_params(axis="x", rotation=0)

        ax.format_coord = make_format_coord(
            x_i, dates_dt_i, opens_i, highs_i, lows_i, closes_i, volumes_i, dt_fmt="%H:%M"
        )

    apply_limit_ticks(ax, limit_up_loc, limit_down_loc)
    style_inside_right_y_ticks(ax)

    if level_in_axis_range(ax, prev_open_loc):
        ax.axhline(
            y=prev_open_loc,
            color="purple",
            linestyle="--",
            linewidth=1.0, # 昨日開盤線的寬度這裡改
            alpha=0.8,
            label="昨日開盤"
        )

    if level_in_axis_range(ax, prev_close_loc):
        ax.axhline(
            y=prev_close_loc,
            color="blue",
            linestyle="--",
            linewidth=1.0, # 昨日收盤線的寬度這裡改
            alpha=0.8,
            label="昨日收盤"
        )

    if level_in_axis_range(ax, prev_high_loc):
        ax.axhline(
            y=prev_high_loc,
            color="red",
            linestyle="--",
            linewidth=1.0, # 昨日最高線的寬度這裡改
            alpha=0.8,
            label="昨日最高"
        )

    if level_in_axis_range(ax, prev_low_loc):
        ax.axhline(
            y=prev_low_loc,
            color="green",
            linestyle="--",
            linewidth=1.0, # 昨日最低線的寬度這裡改
            alpha=0.8,
            label="昨日最低"
        )

    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(
            loc="upper left",
            fontsize=8,
            framealpha=0.2,
            facecolor="white",
            edgecolor="gray",
        )

def now_tpe() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))

def main():
    # ========= 你提供的陣列 =========
    candidate_symbols = selected_stocks
    target_date = parse_specified_date(SPECIFIED_DATE)

    # ========= 輸出資料夾（以指定日期 YYYYMMDD 命名） =========
    out_dir = target_date.strftime("%Y%m%d")
    os.makedirs(PDF_DIR, exist_ok=True)
    # ========= PDF 檔名（全部圖合併成一份） =========
    out_pdf_path = os.path.join(PDF_DIR, f"{out_dir}_one_k_vwap_bh.pdf")
    # ========= SDK login =========
    config = ConfigParser()
    config.read(CONFIG_PATH)
    normalize_config_paths(config)
    realtime_sdk = EsunMarketdata(config)
    realtime_sdk.login()
    sdk = SDK(config)
    sdk.login()
    rest_stock = realtime_sdk.rest_client.stock

    # ========= 批次產圖：全部寫入同一個 PDF =========
    with PdfPages(out_pdf_path) as pdf:
        for item in candidate_symbols:
            fig, (price_ax, volume_ax) = plt.subplots(
                2,
                1,
                figsize=(20, 10),
                gridspec_kw={"height_ratios": [4.5, 1.5], "hspace": 0.05},
            )

            stock_num = item[0]
            code = extract_stock_code(item[0])
            atr_value = item[7]
            candles_by_day = fetch_recent_candles(rest_stock, code, target_date, lookback_days=10)
            historical_rows = candles_by_day.get(target_date, [])
            if not historical_rows:
                plt.close(fig)
                print(f"[WARN] Skip {code}: {target_date.strftime('%Y-%m-%d')} 無當日分K資料")
                continue
            try:
                prev_trade_date, prev_open_price, prev_high_price, prev_low_price, prev_close_price = (
                    calculate_previous_day_ohlc(candles_by_day, code, target_date)
                )
            except ValueError as exc:
                plt.close(fig)
                print(f"[WARN] Skip {code}: {exc}")
                continue
            title = (
                f"{stock_num} 前日:{prev_trade_date.strftime('%Y-%m-%d')} "
                f"昨開:{format_tw_price(prev_open_price)} "
                f"昨高:{format_tw_price(prev_high_price)} "
                f"昨低:{format_tw_price(prev_low_price)} "
                f"昨收:{format_tw_price(prev_close_price)}"
            )

            print(
                f"[INFO] Add {code} to PDF "
                f"(target={target_date.strftime('%Y-%m-%d')}, prev={prev_trade_date.strftime('%Y-%m-%d')}) ..."
            )
            draw_intraday_ohlc(
                ax=price_ax,
                volume_ax=volume_ax,
                symbol_code=code,
                target_date=target_date,
                historical_rows=historical_rows,
                fig_title=title,
                atr_value=atr_value,
                prev_open_loc=prev_open_price,
                prev_high_loc=prev_high_price,
                prev_low_loc=prev_low_price,
                prev_close_loc=prev_close_price,
            )

            fig.subplots_adjust(left=0.03, right=0.985, top=0.95, bottom=0.08, hspace=0.05)
            pdf.savefig(fig)
            plt.close(fig)

    print(f"[DONE] PDF saved: {out_pdf_path}")


if __name__ == "__main__":
    main()
