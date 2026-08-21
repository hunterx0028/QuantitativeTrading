from __future__ import annotations

import os
import re
import sys
import tkinter as tk
from configparser import ConfigParser
from datetime import date
from tkinter import ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure

from esun_marketdata import EsunMarketdata
from esun_trade.sdk import SDK


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_ROOT = os.path.dirname(BASE_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Z_ORB_ONE.daily_report.batch_daily_k_stick_specify_day_volume_pdf import (
    CONFIG_PATH,
    LOOKBACK_CALENDAR_DAYS,
    draw_daily_ohlc,
    fetch_daily_candles,
    normalize_config_paths,
    parse_specified_date,
)
from Z_ORB_ONE.stock_data import (
    selected_limit_down_stocks,
    selected_limit_up_stocks,
    selected_stocks,
)


SPECIFIED_DATE = ""  # YYYYMMDD；空字串代表台北時區今日
DISPLAY_INDEX_CODES = ["IX0001", "IX0043"]  # 依序以分頁顯示上市、上櫃指數
DISPLAY_STOCK_CODES = [""]  # 要額外顯示的 4 位數股票代碼；空字串會被忽略，等同空陣列


def normalize_stock_code(stock_code: str) -> str:
    normalized_code = str(stock_code).strip()
    if not re.fullmatch(r"\d{4}", normalized_code):
        raise ValueError(
            f"DISPLAY_STOCK_CODES 內的股票代碼必須是 4 位數字: {stock_code!r}"
        )
    return normalized_code


def extract_stock_code(label: str) -> str:
    match = re.search(r"(\d+)", label)
    if not match:
        raise ValueError(f"無法從標籤擷取股票代碼: {label}")
    return match.group(1)


def find_stock_label_and_atr(stock_code: str) -> tuple[str, float | None]:
    code = normalize_stock_code(stock_code)
    for stock_group in (selected_stocks, selected_limit_up_stocks, selected_limit_down_stocks):
        for item in stock_group:
            if extract_stock_code(item[0]) == code:
                return item[0], item[7]
    return f"股票 {code}", None


def create_daily_figure(rows: list[dict], item_label: str, atr_value: float | None, is_index: bool) -> Figure:
    fig = Figure(figsize=(20, 10))
    grid = fig.add_gridspec(2, 1, height_ratios=[4.5, 1.5], hspace=0.05)
    price_ax = fig.add_subplot(grid[0])
    volume_ax = fig.add_subplot(grid[1], sharex=price_ax)
    draw_daily_ohlc(price_ax, volume_ax, rows, item_label, atr_value, is_index)
    fig.subplots_adjust(left=0.03, right=0.985, top=0.95, bottom=0.08, hspace=0.05)
    return fig


def build_index_figure(rest_stock, index_code: str, effective_date: date, calendar_rows: list[dict]) -> tuple[Figure, str]:
    code = index_code.strip().upper()
    rows = calendar_rows if code == DISPLAY_INDEX_CODES[0].strip().upper() else fetch_daily_candles(
        rest_stock,
        code,
        effective_date,
    )
    if not rows or rows[-1]["date"] != effective_date:
        last_date = rows[-1]["date"].strftime("%Y-%m-%d") if rows else "無資料"
        raise ValueError(f"{code} 在有效日 {effective_date.strftime('%Y-%m-%d')} 無日K（最後資料: {last_date}）")

    print(
        f"[INFO] Draw index {code} "
        f"(effective={effective_date.strftime('%Y-%m-%d')}, candles={len(rows)}) ..."
    )
    return create_daily_figure(rows, code, None, True), code


def build_stock_figure(rest_stock, stock_code: str, effective_date: date) -> tuple[Figure, str]:
    code = normalize_stock_code(stock_code)
    label, atr_value = find_stock_label_and_atr(code)
    rows = fetch_daily_candles(rest_stock, code, effective_date)
    if not rows or rows[-1]["date"] != effective_date:
        last_date = rows[-1]["date"].strftime("%Y-%m-%d") if rows else "無資料"
        raise ValueError(f"股票 {code} 在有效日 {effective_date.strftime('%Y-%m-%d')} 無日K（最後資料: {last_date}）")

    print(
        f"[INFO] Draw stock {code} "
        f"(effective={effective_date.strftime('%Y-%m-%d')}, candles={len(rows)}) ..."
    )
    return create_daily_figure(rows, label, atr_value, False), f"股票 {code}"


def add_figure_tab(notebook: ttk.Notebook, fig: Figure, tab_title: str) -> None:
    frame = ttk.Frame(notebook)
    notebook.add(frame, text=tab_title)

    canvas = FigureCanvasTkAgg(fig, master=frame)
    toolbar = NavigationToolbar2Tk(canvas, frame, pack_toolbar=False)
    toolbar.update()
    toolbar.pack(side=tk.TOP, fill=tk.X)

    canvas_widget = canvas.get_tk_widget()
    canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
    canvas.draw()


def main():
    requested_date = parse_specified_date(SPECIFIED_DATE)
    first_index_code = DISPLAY_INDEX_CODES[0].strip().upper()

    config = ConfigParser()
    config.read(CONFIG_PATH)
    normalize_config_paths(config)
    realtime_sdk = EsunMarketdata(config)
    realtime_sdk.login()
    sdk = SDK(config)
    sdk.login()
    rest_stock = realtime_sdk.rest_client.stock

    calendar_rows = fetch_daily_candles(rest_stock, first_index_code, requested_date)
    if not calendar_rows:
        raise RuntimeError(
            f"{first_index_code} 在 {requested_date.strftime('%Y-%m-%d')} 往前 "
            f"{LOOKBACK_CALENDAR_DAYS} 天內無日K資料"
        )
    effective_date = calendar_rows[-1]["date"]

    root = tk.Tk()
    root.title(f"指定日K成交量 - {effective_date.strftime('%Y-%m-%d')}")
    root.geometry("1600x900")

    notebook = ttk.Notebook(root)
    notebook.pack(fill=tk.BOTH, expand=True)

    for index_code in DISPLAY_INDEX_CODES:
        if not str(index_code).strip():
            continue
        fig, tab_title = build_index_figure(rest_stock, index_code, effective_date, calendar_rows)
        add_figure_tab(notebook, fig, tab_title)

    display_stock_codes = [
        stock_code
        for stock_code in DISPLAY_STOCK_CODES
        if str(stock_code).strip()
    ]
    for stock_code in display_stock_codes:
        fig, tab_title = build_stock_figure(rest_stock, stock_code, effective_date)
        add_figure_tab(notebook, fig, tab_title)

    print("[DONE] Charts opened on screen.")
    root.mainloop()


if __name__ == "__main__":
    main()
