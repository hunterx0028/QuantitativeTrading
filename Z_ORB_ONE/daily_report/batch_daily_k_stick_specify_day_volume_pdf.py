from __future__ import annotations

import os
import re
import sys
import time as time_module
from configparser import ConfigParser
from datetime import date, datetime, timedelta

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pytz
from matplotlib.backends.backend_pdf import PdfPages

from esun_marketdata import EsunMarketdata
from esun_trade.sdk import SDK


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_ROOT = os.path.dirname(BASE_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Z_ORB_ONE.stock_data import (
    selected_limit_down_stocks,
    selected_limit_up_stocks,
    selected_stocks,
)


PDF_DIR = os.path.join(CURRENT_DIR, "pdf_folder")
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
SPECIFIED_DATE = ""  # YYYYMMDD；空字串代表台北時區今日
SPECIFIED_INDEX_CODES = ["IX0001", "IX0043"]  # 上市、上櫃指數固定置於個股之前
DAILY_CANDLE_COUNT = 40
LOOKBACK_CALENDAR_DAYS = 120
REQUEST_INTERVAL_SECONDS = 1

plt.rcParams["font.sans-serif"] = ["Microsoft JhengHei"]
plt.rcParams["axes.unicode_minus"] = False


def normalize_config_paths(config: ConfigParser):
    cert_path = config.get("Cert", "Path", fallback="")
    if cert_path and not os.path.isabs(cert_path):
        config.set("Cert", "Path", os.path.join(BASE_DIR, cert_path))


def now_tpe() -> datetime:
    return datetime.now(pytz.timezone("Asia/Taipei"))


def parse_specified_date(date_str: str) -> date:
    date_str = date_str.strip()
    if not date_str:
        return now_tpe().date()
    try:
        return datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"SPECIFIED_DATE 格式錯誤: {date_str}，需為 YYYYMMDD") from exc


def extract_stock_code(label: str) -> str:
    match = re.search(r"(\d+)", label)
    if not match:
        raise ValueError(f"無法從標籤擷取股票代碼: {label}")
    return match.group(1)


def parse_api_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fetch_daily_candles(rest_stock, symbol_code: str, requested_date: date) -> list[dict]:
    """取得 requested_date（含）以前的日 K，並保留最近 40 筆。"""
    from_date = requested_date - timedelta(days=LOOKBACK_CALENDAR_DAYS)
    response_data = rest_stock.historical.candles(
        **{
            "symbol": symbol_code,
            "from": from_date.strftime("%Y-%m-%d"),
            "to": requested_date.strftime("%Y-%m-%d"),
        }
    )
    time_module.sleep(REQUEST_INTERVAL_SECONDS)

    rows_by_date: dict[date, dict] = {}
    for row in response_data.get("data", []) or []:
        candle_date = parse_api_datetime(row["date"]).date()
        if not (from_date <= candle_date <= requested_date):
            continue
        rows_by_date[candle_date] = {
            "date": candle_date,
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row.get("volume", 0) or 0),
        }

    rows = sorted(rows_by_date.values(), key=lambda item: item["date"])
    return rows[-DAILY_CANDLE_COUNT:]


def get_tw_tick_size(price: float) -> float:
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


def format_index_value(value: float) -> str:
    return f"{value:.2f}"


def make_format_coord(rows: list[dict], value_formatter):
    def _format_coord(xdata, _ydata):
        if xdata is None or not rows:
            return ""
        index = max(0, min(len(rows) - 1, int(round(xdata))))
        row = rows[index]
        return (
            f"{row['date'].strftime('%Y-%m-%d')}  "
            f"O:{value_formatter(row['open'])}  H:{value_formatter(row['high'])}  "
            f"L:{value_formatter(row['low'])}  C:{value_formatter(row['close'])}  "
            f"V:{row['volume']:,}"
        )

    return _format_coord


def draw_daily_ohlc(
    price_ax,
    volume_ax,
    rows: list[dict],
    item_label: str,
    atr_value: float | None,
    is_index: bool,
):
    value_formatter = format_index_value if is_index else format_tw_price
    x_values = np.arange(len(rows), dtype=float)
    colors = []

    for x_value, row in zip(x_values, rows):
        if row["close"] > row["open"]:
            color = "red"
        elif row["close"] < row["open"]:
            color = "green"
        else:
            color = "black"
        colors.append(color)
        price_ax.vlines(x_value, row["low"], row["high"], color=color, linewidth=1.4)
        price_ax.hlines(row["open"], x_value - 0.28, x_value, color=color, linewidth=1.4)
        price_ax.hlines(row["close"], x_value, x_value + 0.28, color=color, linewidth=1.4)

    latest = rows[-1]
    previous_close = rows[-2]["close"] if len(rows) >= 2 else None
    change_text = ""
    if previous_close:
        change = latest["close"] - previous_close
        change_percent = change / previous_close * 100
        change_text = f" {change:+.2f} {change_percent:+.2f}%"

    title = (
        f"{item_label} 日K({len(rows)}筆) 截至:{latest['date'].strftime('%Y-%m-%d')} "
        f"開:{value_formatter(latest['open'])} 高:{value_formatter(latest['high'])} "
        f"低:{value_formatter(latest['low'])} 收:{value_formatter(latest['close'])}{change_text}"
    )
    if atr_value is not None:
        title += f" ATR:{atr_value:.2f}"
    price_ax.set_title(title, pad=4)

    if previous_close is not None:
        previous_close_value = float(previous_close)
        price_ax.axhline(
            previous_close_value,
            color="blue",
            linestyle="--",
            linewidth=1.0,
            alpha=0.75,
            label=f"前一日收盤 {value_formatter(previous_close_value)}",
        )
        price_ax.legend(loc="upper left", fontsize=8, framealpha=0.25)

    volumes = [row["volume"] for row in rows]
    volume_ax.bar(x_values, volumes, width=0.65, color=colors, edgecolor=colors, alpha=0.85)
    volume_ax.set_ylim(0, max(max(volumes), 1) * 1.2)
    volume_ax.set_ylabel("Volume", fontsize=9)

    tick_indices = sorted({0, len(rows) - 1, *range(4, len(rows), 5)})
    tick_labels = [rows[index]["date"].strftime("%m-%d") for index in tick_indices]
    for axis in (price_ax, volume_ax):
        axis.set_xlim(-0.8, len(rows) - 0.2)
        axis.set_xticks(tick_indices)
        axis.grid(True, alpha=0.3)
        axis.yaxis.tick_right()
        axis.tick_params(axis="y", labelright=True, labelleft=False, right=False, left=False, pad=-10)
        for label in axis.get_yticklabels():
            label.set_horizontalalignment("right")

    price_ax.tick_params(axis="x", labelbottom=False)
    volume_ax.set_xticklabels(tick_labels)
    price_ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda value, _pos: value_formatter(value)))
    volume_ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda value, _pos: f"{int(round(value)):,}"))
    price_ax.yaxis.get_offset_text().set_visible(False)
    volume_ax.yaxis.get_offset_text().set_visible(False)
    price_ax.format_coord = make_format_coord(rows, value_formatter)


def build_report_items() -> list[dict]:
    report_items = [
        {"code": code, "label": code, "atr_value": None, "is_index": True}
        for code in SPECIFIED_INDEX_CODES
    ]
    for stock_group in (selected_stocks, selected_limit_up_stocks, selected_limit_down_stocks):
        report_items.extend(
            {
                "code": extract_stock_code(item[0]),
                "label": item[0],
                "atr_value": item[7],
                "is_index": False,
            }
            for item in stock_group
        )
    return report_items


def main():
    requested_date = parse_specified_date(SPECIFIED_DATE)
    report_items = build_report_items()

    config = ConfigParser()
    config.read(CONFIG_PATH)
    normalize_config_paths(config)
    realtime_sdk = EsunMarketdata(config)
    realtime_sdk.login()
    sdk = SDK(config)
    sdk.login()
    rest_stock = realtime_sdk.rest_client.stock

    # 以上市指數的最後一筆資料判定整份報表的最近有效交易日。
    calendar_rows = fetch_daily_candles(rest_stock, SPECIFIED_INDEX_CODES[0], requested_date)
    if not calendar_rows:
        raise RuntimeError(
            f"{SPECIFIED_INDEX_CODES[0]} 在 {requested_date.strftime('%Y-%m-%d')} 往前 "
            f"{LOOKBACK_CALENDAR_DAYS} 天內無日K資料"
        )
    effective_date = calendar_rows[-1]["date"]

    os.makedirs(PDF_DIR, exist_ok=True)
    out_pdf_path = os.path.join(
        PDF_DIR,
        f"{effective_date.strftime('%Y%m%d')}_daily_k_batch.pdf",
    )

    with PdfPages(out_pdf_path) as pdf:
        for item in report_items:
            code = item["code"]
            try:
                rows = calendar_rows if code == SPECIFIED_INDEX_CODES[0] else fetch_daily_candles(
                    rest_stock, code, effective_date
                )
                if not rows or rows[-1]["date"] != effective_date:
                    last_date = rows[-1]["date"].strftime("%Y-%m-%d") if rows else "無資料"
                    print(f"[WARN] Skip {code}: 有效日 {effective_date} 無日K（最後資料: {last_date}）")
                    continue

                fig, (price_ax, volume_ax) = plt.subplots(
                    2,
                    1,
                    figsize=(20, 10),
                    gridspec_kw={"height_ratios": [4.5, 1.5], "hspace": 0.05},
                )
                print(
                    f"[INFO] Add {code} to PDF "
                    f"(effective={effective_date.strftime('%Y-%m-%d')}, candles={len(rows)}) ..."
                )
                draw_daily_ohlc(
                    price_ax,
                    volume_ax,
                    rows,
                    item["label"],
                    item["atr_value"],
                    item["is_index"],
                )
                fig.subplots_adjust(left=0.03, right=0.985, top=0.95, bottom=0.08, hspace=0.05)
                pdf.savefig(fig)
                plt.close(fig)
            except Exception as exc:
                plt.close("all")
                print(f"[WARN] Skip {code}: {exc}")

    print(f"[DONE] PDF saved: {out_pdf_path}")


if __name__ == "__main__":
    main()
