import math
import os
import sys
import threading
import time
import tkinter as tk
from configparser import ConfigParser
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, timedelta
from typing import Any, Optional

import matplotlib
matplotlib.use("TkAgg")

import matplotlib.dates as mdates
import pytz
from esun_marketdata import EsunMarketdata
from esun_trade.constant import APCode, Action, PriceFlag, Trade
from esun_trade.order import OrderObject
from esun_trade.sdk import SDK
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter
from tkinter import ttk

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_ROOT = os.path.dirname(BASE_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Z_ORB_ONE.stock_data import selected_stocks

CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
TZ = pytz.timezone("Asia/Taipei")
START_FETCH_TIME = (9, 1, 0)
STOP_MONITOR_TIME = (13, 31, 0)
FETCH_SECOND_IN_MINUTE = 10

def get_stock_name(symbol_str: str) -> str:
    return symbol_str.split(":", 1)[0]

matplotlib.rcParams["font.sans-serif"] = ["Microsoft JhengHei"]
matplotlib.rcParams["axes.unicode_minus"] = False

@dataclass
class CandleView:
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    cumulative_volume: int
    average: float


@dataclass
class StockState:
    symbol_name: str
    symbol_code: str
    symbol_code_with_suf: str
    limit_up_price: float
    limit_down_price: float
    prev_open: float
    prev_high: float
    prev_low: float
    prev_close: float
    industry_code: str
    atr: float
    continue_days: tuple[int, int]
    candles: list[CandleView] = field(default_factory=list)
    last_update: Optional[datetime] = None
    last_error: Optional[str] = None


def now_tpe() -> datetime:
    return datetime.now(TZ)


def format_hms(value: tuple[int, int, int]) -> str:
    return f"{value[0]:02d}:{value[1]:02d}:{value[2]:02d}"


def time_tuple_reached(current_time: datetime, target: tuple[int, int, int]) -> bool:
    return (current_time.hour, current_time.minute, current_time.second) >= target


def next_trigger_time(current_time: datetime, target_second: int) -> datetime:
    candidate = current_time.replace(second=target_second, microsecond=0)
    if candidate <= current_time:
        candidate = candidate.replace(minute=candidate.minute, second=0) + timedelta(minutes=1)
        candidate = candidate.replace(second=target_second)
    return candidate


def parse_iso_keep_local_walltime(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=None)


def log(message: str):
    print(f"[{now_tpe().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def get_pure_symbol(symbol_str: str) -> tuple[str, str]:
    symbol_with_suffix = symbol_str.split(":")[1]
    symbol = symbol_with_suffix.split(".")[0]
    return symbol, symbol_with_suffix


def parse_stock_float(value: Any, field_name: str, symbol_name: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        log(f"{symbol_name} {field_name} 數值格式錯誤，使用 {default:.2f}: {value!r}")
        return default


def normalize_config_paths(config: ConfigParser):
    cert_path = config.get("Cert", "Path", fallback="")
    if cert_path and not os.path.isabs(cert_path):
        config.set("Cert", "Path", os.path.join(BASE_DIR, cert_path))


def place_trade_order(
    order_code: str,
    quantity: int,
    action_type: Action,
    trade_type: Trade,
    price_flag: PriceFlag = PriceFlag.Market,
    price: float = 0.0,
) -> bool:
    if price_flag == PriceFlag.Market:
        price = ""
    elif price_flag in (PriceFlag.LimitUp, PriceFlag.LimitDown):
        price = None

    config = ConfigParser()
    config.read(CONFIG_PATH)
    normalize_config_paths(config)
    mysdk = SDK(config)
    mysdk.login()

    order = OrderObject(
        buy_sell=action_type,
        price_flag=price_flag,
        price=price,
        stock_no=order_code,
        quantity=quantity,
        ap_code=APCode.Common,
        trade=trade_type,
    )

    try:
        mysdk.place_order(order)
        time.sleep(1)
    except Exception as exc:
        print(f"[ERROR] 失敗 {order_code} : {action_type} x {quantity} - {trade_type} - {exc}", flush=True)
        return False

    print(f"[ORDER] 成功 {order_code} : {action_type} x {quantity} - {trade_type}", flush=True)
    return True


def fetch_intraday_ticker(rest_stock, symbol_code_with_suf: str) -> dict[str, Any]:
    symbol_code = symbol_code_with_suf.split(".")[0]
    return rest_stock.intraday.ticker(symbol=symbol_code)


def get_up_down_price(symbol_code_with_suf: str, rest_stock) -> tuple[float, float, bool]:
    stock_intra_ticker = fetch_intraday_ticker(rest_stock, symbol_code_with_suf)
    time.sleep(0.2)  # 避免短時間過量 request
    limit_up_price = round(float(stock_intra_ticker.get("limitUpPrice", 0) or 0), 2)
    limit_down_price = round(float(stock_intra_ticker.get("limitDownPrice", 0) or 0), 2)
    symbol_can_buy_day_trade = bool(stock_intra_ticker.get("canBuyDayTrade", False))
    return limit_up_price, limit_down_price, symbol_can_buy_day_trade


def fetch_intraday_candles(
    rest_stock,
    symbol_code_with_suf: str,
) -> list[CandleView]:
    symbol_code = symbol_code_with_suf.split(".")[0]
    response = rest_stock.intraday.candles(symbol=symbol_code)
    raw_candles = response.get("data", []) or []

    candles: list[CandleView] = []
    for candle in raw_candles:
        dt_value = parse_iso_keep_local_walltime(candle["date"])
        current_time = dt_value.time()
        if dt_time(9, 0) <= current_time <= dt_time(13, 30):
            average = candle.get("average")
            volume = int(candle.get("volume", 0) or 0)
            candles.append(
                CandleView(
                    date=dt_value,
                    open=float(candle["open"]),
                    high=float(candle["high"]),
                    low=float(candle["low"]),
                    close=float(candle["close"]),
                    volume=max(volume, 0),
                    cumulative_volume=0,
                    average=float(average) if average is not None else float(candle["close"]),
                )
            )

    candles.sort(key=lambda row: row.date)
    cumulative_volume = 0
    for candle in candles:
        cumulative_volume += candle.volume
        candle.cumulative_volume = cumulative_volume

    return candles


def draw_intraday_chart(
    state: StockState,
    price_ax: Any,
    value_ax: Any,
    angle_ax: Any,
    volume_ax: Any,
    canvas: FigureCanvasTkAgg,
) -> dict[str, Any]:
    price_ax.clear()
    value_ax.clear()
    angle_ax.clear()
    volume_ax.clear()

    candles = state.candles
    price_ax.set_facecolor("#ffffff")
    value_ax.set_facecolor("#f8fafc")
    angle_ax.set_facecolor("#ffffff")
    volume_ax.set_facecolor("#ffffff")
    price_ax.grid(True, axis="both", color="#d1d5db", linestyle="--", linewidth=0.6, alpha=0.8)
    value_ax.grid(False)
    angle_ax.grid(True, axis="y", color="#d1d5db", linestyle="--", linewidth=0.6, alpha=0.8)
    volume_ax.grid(True, axis="y", color="#d1d5db", linestyle="--", linewidth=0.6, alpha=0.8)

    title = f"{state.symbol_name}  分K走勢"
    if state.last_update:
        title += f"  |  更新 {state.last_update.strftime('%H:%M:%S')}"
    if state.last_error:
        title += f"  |  錯誤 {state.last_error}"
    price_ax.set_title(title, fontsize=13, fontweight="bold")

    if not candles:
        price_ax.text(0.5, 0.5, "尚無今日分K資料", ha="center", va="center", transform=price_ax.transAxes, fontsize=14)
        canvas.draw_idle()
        return {"state": state, "candles": [], "x_vals": []}

    dates_dt = [candle.date for candle in candles]
    opens = [candle.open for candle in candles]
    highs = [candle.high for candle in candles]
    lows = [candle.low for candle in candles]
    closes = [candle.close for candle in candles]
    averages = [candle.average for candle in candles]
    volumes = [candle.volume for candle in candles]

    x_vals = mdates.date2num(dates_dt)
    tick_width = (1 / 1440) * 0.62

    for index, x_value in enumerate(x_vals):
        open_price = opens[index]
        high_price = highs[index]
        low_price = lows[index]
        close_price = closes[index]
        color = "#c62828" if close_price > open_price else "#2e7d32" if close_price < open_price else "#111827"

        price_ax.vlines(x_value, low_price, high_price, color=color, linewidth=1.2, zorder=2)
        body_bottom = min(open_price, close_price)
        body_height = max(abs(close_price - open_price), 0.01)
        price_ax.bar(
            x_value,
            body_height,
            bottom=body_bottom,
            width=tick_width,
            color=color,
            edgecolor=color,
            align="center",
            zorder=3,
        )

    price_ax.plot(x_vals, averages, color="#f59e0b", linewidth=1.6, label="AVG", zorder=4)

    reference_lines = [
        ("昨開", state.prev_open, "#7c3aed", ":"),
        ("昨收", state.prev_close, "#2563eb", ":"),
        ("昨高", state.prev_high, "#ea580c", ":"),
        ("昨低", state.prev_low, "#0891b2", "-."),
    ]
    for label, value, color, linestyle in reference_lines:
        if state.limit_down_price <= value <= state.limit_up_price:
            price_ax.axhline(value, color=color, linestyle=linestyle, linewidth=1.0, label=f"{label} {value:.2f}")

    price_ax.axhline(state.limit_up_price, color="#b91c1c", linestyle="--", linewidth=0.9, label=f"漲停 {state.limit_up_price:.2f}")
    price_ax.axhline(state.limit_down_price, color="#15803d", linestyle="--", linewidth=0.9, label=f"跌停 {state.limit_down_price:.2f}")

    value_ax.set_ylim(0, 1)
    value_ax.set_yticks([])
    value_ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)
    for spine in value_ax.spines.values():
        spine.set_visible(False)
    angle_ax.axhline(0, color="#6b7280", linestyle=":", linewidth=0.9)
    volume_colors = [
        "#c62828" if close_value > open_value else "#2e7d32" if close_value < open_value else "#111827"
        for open_value, close_value in zip(opens, closes)
    ]
    volume_ax.bar(x_vals, volumes, width=tick_width, color=volume_colors, edgecolor=volume_colors, align="center", alpha=0.9)

    trade_day = dates_dt[0].date()
    x_start = mdates.date2num(datetime.combine(trade_day, dt_time(8, 59, 0)))
    x_end = mdates.date2num(datetime.combine(trade_day, dt_time(13, 31, 0)))
    price_ax.set_xlim(x_start, x_end)
    value_ax.set_xlim(x_start, x_end)
    angle_ax.set_xlim(x_start, x_end)
    volume_ax.set_xlim(x_start, x_end)

    data_high = max(max(highs), state.limit_up_price)
    data_low = min(min(lows), state.limit_down_price)
    base_range = max(data_high - data_low, 0.01)
    price_ax.set_ylim(state.limit_down_price - max(base_range * 0.06, 0.3), state.limit_up_price + max(base_range * 0.03, 0.15))

    angle_ax.set_ylim(-1, 1)
    volume_ax.set_ylim(0, max(max(volumes), 1) * 1.25)

    price_ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: f"{value:.2f}"))
    angle_ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: f"{value:.0f}"))
    volume_ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: f"{int(value):,}"))
    volume_ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=12))
    volume_ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    price_ax.tick_params(axis="x", labelbottom=False)
    value_ax.tick_params(axis="x", labelbottom=False)
    angle_ax.tick_params(axis="x", labelbottom=False)
    volume_ax.tick_params(axis="x", rotation=45)
    price_ax.legend(loc="upper right", ncol=2, fontsize=9)
    canvas.draw_idle()
    return {"state": state, "candles": candles, "x_vals": x_vals}


class DesktopIntradayMonitorApp:
    def __init__(self, root: tk.Tk, rest_stock):
        self.root = root
        self.rest_stock = rest_stock
        self.stop_event = threading.Event()
        self.monitor_stopped = False
        self.initial_fetch_done = False
        self.states: list[StockState] = []
        self.selected_symbol: Optional[str] = None
        self.summary_vars: dict[str, tk.StringVar] = {}
        self.chart_context: dict[str, Any] = {"state": None, "candles": [], "x_vals": []}
        self.crosshair_vlines: list[Any] = []
        self.crosshair_hline: Any = None
        self.hover_label: Any = None
        self.stock_code_filter_var = tk.StringVar(value="")
        self.stock_code_filter: Optional[set[str]] = None
        self.trade_window: Optional[tk.Toplevel] = None
        self.trade_side_var = tk.StringVar(value="")
        self.trade_qty_var = tk.StringVar(value="1")
        self.default_left_pane_width = 250
        self.last_window_state = self.root.state()

        self.root.title("券商風即時分K監看")
        self.root.geometry("1680x920")
        self.root.minsize(1400, 760)
        self.root.configure(bg="#e9edf2")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.bind("<Configure>", self.on_root_configure)

        self._build_style()
        self._build_layout()

    def _build_style(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("App.TFrame", background="#e9edf2")
        style.configure("Card.TFrame", background="#f7f9fb", relief="flat")
        style.configure("Header.TLabel", background="#1f2937", foreground="#f9fafb", font=("Microsoft JhengHei", 14, "bold"))
        style.configure("LabelKey.TLabel", background="#f7f9fb", foreground="#4b5563", font=("Microsoft JhengHei", 10))
        style.configure("LabelValue.TLabel", background="#f7f9fb", foreground="#111827", font=("Consolas", 12, "bold"))
        style.configure("Status.TLabel", background="#111827", foreground="#e5e7eb", font=("Microsoft JhengHei", 10))
        style.configure("Treeview", rowheight=24, font=("Consolas", 10), fieldbackground="#ffffff", background="#ffffff")
        style.configure("Treeview.Heading", font=("Microsoft JhengHei", 10, "bold"))

    def _build_layout(self):
        outer = ttk.Frame(self.root, style="App.TFrame")
        outer.pack(fill="both", expand=True, padx=10, pady=10)

        header = ttk.Frame(outer, style="Card.TFrame")
        header.pack(fill="x", pady=(0, 8))
        ttk.Label(header, text="即時分K監看桌面版", style="Header.TLabel", anchor="center").pack(fill="x")

        self.body = ttk.PanedWindow(outer, orient="horizontal")
        self.body.pack(fill="both", expand=True)

        left_card = ttk.Frame(self.body, style="Card.TFrame", padding=8)
        right_card = ttk.Frame(self.body, style="Card.TFrame", padding=(2, 10, 10, 10))
        self.body.add(left_card, weight=1)
        self.body.add(right_card, weight=4)

        ttk.Label(left_card, text="監看清單", font=("Microsoft JhengHei", 10, "bold")).pack(anchor="w", pady=(0, 6))
        filter_frame = ttk.Frame(left_card, style="Card.TFrame")
        filter_frame.pack(fill="x", pady=(0, 8))
        ttk.Label(filter_frame, text="股票號碼(逗號分隔)", style="LabelKey.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Entry(filter_frame, textvariable=self.stock_code_filter_var, width=12, justify="left").grid(
            row=1, column=0, sticky="ew", padx=(0, 6)
        )
        ttk.Button(filter_frame, text="確認", command=self.apply_monitor_stock_code_filter, width=4).grid(row=1, column=1, sticky="e")
        filter_frame.columnconfigure(0, weight=1)
        self.root.bind("<Return>", self.on_price_filter_enter)

        self.stock_tree = ttk.Treeview(
            left_card,
            columns=("symbol", "last", "change"),
            show="headings",
            selectmode="browse",
            height=28,
        )
        for column, text, width, anchor in (
            ("symbol", "股票", 78, "w"),
            ("last", "最新", 78, "e"),
            ("change", "漲跌", 78, "e"),
        ):
            self.stock_tree.heading(column, text=text)
            self.stock_tree.column(column, width=width, anchor=anchor, stretch=column == "symbol")
        self.stock_tree.pack(fill="both", expand=True)
        self.stock_tree.bind("<<TreeviewSelect>>", self.on_select_stock)
        self.stock_tree.tag_configure("rise", foreground="#c62828")
        self.stock_tree.tag_configure("fall", foreground="#2e7d32")
        self.stock_tree.tag_configure("flat", foreground="#374151")
        self.stock_tree.tag_configure("error", foreground="#9ca3af")

        ttk.Label(right_card, text="即時摘要", font=("Microsoft JhengHei", 12, "bold")).pack(anchor="w")
        summary = ttk.Frame(right_card, style="Card.TFrame")
        summary.pack(fill="x", pady=(8, 12))
        summary_labels = {
            "symbol": "股票",
            "prev": "昨日時段",
            "limit": "漲跌停",
            "latest": "最新狀態",
        }
        for index, key in enumerate(("symbol", "prev", "limit", "latest")):
            frame = ttk.Frame(summary, style="Card.TFrame")
            frame.grid(row=index // 3, column=index % 3, sticky="nsew", padx=8, pady=6)
            summary.columnconfigure(index % 3, weight=1)
            ttk.Label(frame, text=summary_labels[key], style="LabelKey.TLabel").pack(anchor="w")
            var = tk.StringVar(value="-")
            ttk.Label(frame, textvariable=var, style="LabelValue.TLabel").pack(anchor="w", pady=(2, 0))
            self.summary_vars[key] = var

        chart_header = ttk.Frame(right_card, style="Card.TFrame")
        chart_header.pack(fill="x")
        ttk.Label(chart_header, text="即時分K圖", font=("Microsoft JhengHei", 12, "bold")).pack(side="left", anchor="w")
        ttk.Button(chart_header, text="交易", command=self.open_trade_window, width=8).pack(side="right", anchor="e")
        chart_container = ttk.Frame(right_card, style="Card.TFrame")
        chart_container.pack(fill="both", expand=True, pady=(8, 0))
        chart_container.rowconfigure(0, weight=1)
        chart_container.columnconfigure(0, weight=1)

        self.figure = Figure(figsize=(12, 7), dpi=100, facecolor="#ffffff")
        self.figure.subplots_adjust(left=0.03, right=0.985, top=0.93, bottom=0.1, hspace=0.04)
        grid_spec = self.figure.add_gridspec(4, 1, height_ratios=[3.2, 0.55, 1, 1.05], hspace=0.03)
        self.price_ax = self.figure.add_subplot(grid_spec[0])
        self.value_ax = self.figure.add_subplot(grid_spec[1], sharex=self.price_ax)
        self.angle_ax = self.figure.add_subplot(grid_spec[2], sharex=self.price_ax)
        self.volume_ax = self.figure.add_subplot(grid_spec[3], sharex=self.price_ax)
        self.canvas = FigureCanvasTkAgg(self.figure, master=chart_container)
        self.canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        self.canvas.mpl_connect("motion_notify_event", self.on_chart_hover)
        self.canvas.mpl_connect("figure_leave_event", self.on_chart_leave)

        self.status_var = tk.StringVar(value="初始化中...")
        ttk.Label(outer, textvariable=self.status_var, style="Status.TLabel", anchor="w").pack(fill="x", pady=(8, 0))
        self.root.after(100, self.apply_default_pane_width)

    def initialize_states(self, stocks: list[tuple]):
        removed_non_day_trade_symbols: list[str] = []
        for stock in stocks:
            symbol_name, _qty, prev_open, prev_high, prev_low, prev_close, industry_code, atr, continue_days = stock
            symbol_code, symbol_code_with_suf = get_pure_symbol(symbol_name)
            prev_open = parse_stock_float(prev_open, "昨開", symbol_name)
            prev_high = parse_stock_float(prev_high, "昨高", symbol_name)
            prev_low = parse_stock_float(prev_low, "昨低", symbol_name)
            prev_close = parse_stock_float(prev_close, "昨收", symbol_name)
            if isinstance(atr, str) and atr.strip() == symbol_code:
                log(f"{symbol_name} ATR 欄位疑似為股票代碼，使用 0.00: {atr!r}")
                atr = 0.0
            else:
                atr = parse_stock_float(atr, "ATR", symbol_name)
            limit_up_price = prev_close
            limit_down_price = prev_close
            try:
                ticker_limit_up, ticker_limit_down, symbol_can_buy_day_trade = get_up_down_price(
                    symbol_code_with_suf,
                    self.rest_stock,
                )
                if not symbol_can_buy_day_trade:
                    removed_non_day_trade_symbols.append(symbol_name)
                    log(f"{symbol_name} 不可當沖，已自監看清單移除。")
                    continue
                limit_up_price = ticker_limit_up or prev_close
                limit_down_price = ticker_limit_down or prev_close
            except Exception as exc:
                log(f"{symbol_name} 取得 ticker 失敗，使用昨收作為上下限: {exc}")
            state = StockState(
                symbol_name=symbol_name,
                symbol_code=symbol_code,
                symbol_code_with_suf=symbol_code_with_suf,
                limit_up_price=limit_up_price,
                limit_down_price=limit_down_price,
                prev_open=prev_open,
                prev_high=prev_high,
                prev_low=prev_low,
                prev_close=prev_close,
                industry_code=industry_code,
                atr=atr,
                continue_days=continue_days,
            )
            self.states.append(state)
            self.stock_tree.insert("", "end", iid=state.symbol_name, values=(state.symbol_name, "-", "-"), tags=("flat",))

        if removed_non_day_trade_symbols:
            log(
                f"已移除不可當沖股票 {len(removed_non_day_trade_symbols)} 檔："
                f"{', '.join(removed_non_day_trade_symbols)}"
            )

        self.refresh_monitor_list_visibility()
        if self.selected_symbol:
            self.render_selected_stock()

    def on_select_stock(self, _event=None):
        selection = self.stock_tree.selection()
        if not selection:
            return
        self.selected_symbol = selection[0]
        self.render_selected_stock()

    def on_price_filter_enter(self, _event=None):
        focused_widget = self.root.focus_get()
        if focused_widget is None:
            return
        widget_path = str(focused_widget)
        if widget_path.startswith(str(self.stock_tree)):
            return
        self.apply_monitor_stock_code_filter()

    def state_matches_stock_code_filter(self, state: StockState) -> bool:
        if self.stock_code_filter is None:
            return True
        return state.symbol_code in self.stock_code_filter

    def has_strategy_for_opening_bias(self, state: StockState) -> bool:
        return True

    def prune_states_without_strategy(self) -> int:
        removed_symbols: list[str] = []
        kept_states: list[StockState] = []
        for state in self.states:
            if self.has_strategy_for_opening_bias(state):
                kept_states.append(state)
                continue
            removed_symbols.append(state.symbol_name)
            if self.stock_tree.exists(state.symbol_name):
                self.stock_tree.delete(state.symbol_name)

        if removed_symbols:
            self.states = kept_states
            if self.selected_symbol in removed_symbols:
                self.selected_symbol = None
        return len(removed_symbols)

    def clear_stock_display(self, message: str = "目前沒有符合條件的股票"):
        self.summary_vars["symbol"].set("-")
        self.summary_vars["prev"].set("-")
        self.summary_vars["latest"].set("-")
        self.summary_vars["limit"].set("-")
        self.chart_context = {"state": None, "candles": [], "x_vals": []}
        for axis in (self.price_ax, self.value_ax, self.angle_ax, self.volume_ax):
            axis.clear()
        self.price_ax.set_title(message, fontsize=13, fontweight="bold")
        self.canvas.draw_idle()

    def refresh_monitor_list_visibility(self):
        visible_symbols: list[str] = []
        attached_symbols = set(self.stock_tree.get_children())
        for state in self.states:
            if self.state_matches_stock_code_filter(state) and self.has_strategy_for_opening_bias(state):
                if state.symbol_name in attached_symbols:
                    self.stock_tree.move(state.symbol_name, "", "end")
                else:
                    self.stock_tree.reattach(state.symbol_name, "", "end")
                visible_symbols.append(state.symbol_name)
            else:
                self.stock_tree.detach(state.symbol_name)

        if not visible_symbols:
            self.selected_symbol = None
            self.stock_tree.selection_remove(self.stock_tree.selection())
            self.clear_stock_display()
            return

        if self.selected_symbol not in visible_symbols:
            self.selected_symbol = visible_symbols[0]

        self.stock_tree.selection_set(self.selected_symbol)
        self.stock_tree.focus(self.selected_symbol)

    def apply_monitor_stock_code_filter(self):
        stock_code_raw = self.stock_code_filter_var.get().strip()
        if stock_code_raw:
            stock_codes_raw = [code.strip() for code in stock_code_raw.split(",")]
            stock_codes = [code for code in stock_codes_raw if code]
            if not stock_codes:
                self.status_var.set("股票號碼輸入錯誤，請輸入4位數字，可用逗號分隔。")
                return
            invalid_codes = [code for code in stock_codes if not code.isdigit() or len(code) != 4]
            if invalid_codes:
                self.status_var.set(f"股票號碼輸入錯誤：{', '.join(invalid_codes)}（需為4位數字，可用逗號分隔）")
                return
            self.stock_code_filter = set(stock_codes)
        else:
            self.stock_code_filter = None

        self.refresh_monitor_list_visibility()

        if self.selected_symbol:
            self.render_selected_stock()
        else:
            self.clear_stock_display()

        visible_count = len(self.stock_tree.get_children())
        if self.stock_code_filter is None:
            self.status_var.set(f"已取消股票號碼篩選，全部顯示 | 目前監看 {visible_count} 檔")
            return

        display_codes = ",".join(sorted(self.stock_code_filter))
        self.status_var.set(f"已套用股票號碼篩選：{display_codes} | 目前監看 {visible_count} 檔")

    def render_selected_stock(self):
        state = self.get_selected_state()
        if state is None:
            return

        self.summary_vars["symbol"].set(f"{state.symbol_name} | ATR {state.atr:.2f}")
        self.summary_vars["limit"].set(f"漲停 {state.limit_up_price:.2f}  跌停 {state.limit_down_price:.2f}")

        update_text = state.last_update.strftime("%H:%M:%S") if state.last_update else "--:--:--"
        if state.candles:
            first = state.candles[0]
            last = state.candles[-1]
            intraday_high = max(candle.high for candle in state.candles)
            intraday_low = min(candle.low for candle in state.candles)
            change_value = last.close - state.prev_close
            change_text = f"{change_value:+.2f}"
            self.summary_vars["prev"].set(
                f"昨開：{state.prev_open:.2f}  昨高：{state.prev_high:.2f}  昨低：{state.prev_low:.2f}  "
                f"昨收：{state.prev_close:.2f}  今開：{first.open:.2f}"
            )
            self.summary_vars["latest"].set(
                f"{last.date.strftime('%H:%M')}  收 {last.close:.2f}  漲跌 {change_text}  "
                f"最高 {intraday_high:.2f}  最低 {intraday_low:.2f}  更新 {update_text}"
            )
        else:
            self.summary_vars["prev"].set(
                f"昨開：{state.prev_open:.2f}  昨高：{state.prev_high:.2f}  昨低：{state.prev_low:.2f}  "
                f"昨收：{state.prev_close:.2f}  今開：-"
            )
            self.summary_vars["latest"].set(f"尚無今日分K  更新 {update_text}")

        if state.last_error:
            self.summary_vars["latest"].set(f"{self.summary_vars['latest'].get()} | 錯誤 {state.last_error}")

        self.chart_context = draw_intraday_chart(
            state,
            self.price_ax,
            self.value_ax,
            self.angle_ax,
            self.volume_ax,
            self.canvas,
        )
        self.setup_crosshair()

    def open_trade_window(self):
        state = self.get_selected_state()
        if state is None:
            self.status_var.set("請先選擇股票後再操作交易。")
            return

        if self.trade_window is not None and self.trade_window.winfo_exists():
            self.trade_window.title(f"交易 - {state.symbol_name}")
            self.trade_window.deiconify()
            self.trade_window.lift()
            self.trade_window.focus_force()
            return

        self.trade_side_var.set("")
        self.trade_qty_var.set("1")

        window = tk.Toplevel(self.root)
        window.title(f"交易 - {state.symbol_name}")
        window.resizable(False, False)
        window.configure(bg="#f7f9fb")
        window.transient(self.root)
        window.protocol("WM_DELETE_WINDOW", self.close_trade_window)
        self.trade_window = window

        outer = ttk.Frame(window, style="Card.TFrame", padding=(12, 10, 12, 8))
        outer.pack(fill="both", expand=True)
        outer.columnconfigure(1, weight=1)

        side_frame = ttk.Frame(outer, style="Card.TFrame")
        side_frame.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
        ttk.Radiobutton(side_frame, text="作空", value="short", variable=self.trade_side_var).pack(side="left", padx=(0, 12))
        ttk.Radiobutton(side_frame, text="作多", value="long", variable=self.trade_side_var).pack(side="left")

        ttk.Label(outer, text="數量", style="LabelKey.TLabel").grid(row=1, column=0, sticky="w", pady=(0, 10))
        ttk.Entry(outer, textvariable=self.trade_qty_var, width=12, justify="right").grid(row=1, column=1, sticky="w", pady=(0, 10))

        action_frame = ttk.Frame(outer, style="Card.TFrame")
        action_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=0)
        action_frame.columnconfigure(0, weight=1)
        action_frame.columnconfigure(1, weight=1)
        ttk.Button(action_frame, text="進場", command=lambda: self.submit_trade_action("entry")).grid(
            row=0, column=0, sticky="ew", padx=(0, 6)
        )
        ttk.Button(action_frame, text="平倉", command=lambda: self.submit_trade_action("exit")).grid(
            row=0, column=1, sticky="ew"
        )
        self.center_window(window, width=320)

    def close_trade_window(self):
        if self.trade_window is not None and self.trade_window.winfo_exists():
            self.trade_window.destroy()
        self.trade_window = None

    def center_window(self, window: tk.Toplevel, width: int, height: Optional[int] = None):
        window.update_idletasks()
        actual_height = height if height is not None else window.winfo_reqheight()
        screen_width = window.winfo_screenwidth()
        screen_height = window.winfo_screenheight()
        x_pos = max((screen_width - width) // 2, 0)
        y_pos = max((screen_height - actual_height) // 2, 0)
        window.geometry(f"{width}x{actual_height}+{x_pos}+{y_pos}")

    def submit_trade_action(self, action: str):
        state = self.get_selected_state()
        if state is None:
            self.status_var.set("目前沒有可交易的股票。")
            return

        side = self.trade_side_var.get()
        if side not in {"short", "long"}:
            self.status_var.set("請先選擇作空或作多。")
            return

        qty_raw = self.trade_qty_var.get().strip()
        try:
            qty = int(qty_raw)
        except ValueError:
            self.status_var.set("數量輸入錯誤，請輸入整數。")
            return

        if qty <= 0:
            self.status_var.set("數量必須大於 0。")
            return

        order_code = state.symbol_code
        side_text = "作空" if side == "short" else "作多"
        action_text = "進場" if action == "entry" else "平倉"
        order_mapping: dict[tuple[str, str], tuple[Action, Trade]] = {
            ("short", "entry"): (Action.Sell, Trade.DayTradingSell),
            ("short", "exit"): (Action.Buy, Trade.Cash),
            ("long", "entry"): (Action.Buy, Trade.Cash),
            ("long", "exit"): (Action.Sell, Trade.DayTradingSell),
        }
        action_type, trade_type = order_mapping[(side, action)]

        print(f"[ORDER] 送出 {order_code} | {side_text} | {action_text} | 數量 {qty}", flush=True)
        success = place_trade_order(order_code, qty, action_type, trade_type, price_flag=PriceFlag.Market, price=0.0)

        if success:
            self.status_var.set(f"{order_code} | {side_text} | 數量 {qty} | {action_text} 成功")
        else:
            self.status_var.set(f"{order_code} | {side_text} | 數量 {qty} | {action_text} 失敗")
        self.close_trade_window()

    def apply_default_pane_width(self):
        try:
            total_width = self.body.winfo_width()
            if total_width <= 1:
                self.root.after(100, self.apply_default_pane_width)
                return
            target_width = min(self.default_left_pane_width, max(180, total_width // 5))
            self.body.sashpos(0, target_width)
        except tk.TclError:
            return

    def on_root_configure(self, _event=None):
        current_state = self.root.state()
        if current_state != self.last_window_state:
            self.last_window_state = current_state
            self.root.after(50, self.apply_default_pane_width)

    def setup_crosshair(self):
        self.crosshair_vlines = [
            self.price_ax.axvline(color="#9ca3af", linewidth=0.8, linestyle="--", visible=False, zorder=5),
            self.value_ax.axvline(color="#9ca3af", linewidth=0.8, linestyle="--", visible=False, zorder=5),
            self.angle_ax.axvline(color="#9ca3af", linewidth=0.8, linestyle="--", visible=False, zorder=5),
            self.volume_ax.axvline(color="#9ca3af", linewidth=0.8, linestyle="--", visible=False, zorder=5),
        ]
        self.crosshair_hline = self.price_ax.axhline(color="#9ca3af", linewidth=0.8, linestyle="--", visible=False, zorder=5)
        self.hover_label = self.price_ax.annotate(
            "",
            xy=(0, 0),
            xytext=(12, 12),
            textcoords="offset points",
            bbox={"boxstyle": "round,pad=0.3", "fc": "#111827", "ec": "#111827", "alpha": 0.92},
            color="#f9fafb",
            fontsize=9,
            visible=False,
        )

    def hide_crosshair(self):
        for line in self.crosshair_vlines:
            line.set_visible(False)
        if self.crosshair_hline is not None:
            self.crosshair_hline.set_visible(False)
        if self.hover_label is not None:
            self.hover_label.set_visible(False)
        self.canvas.draw_idle()

    def on_chart_leave(self, _event=None):
        self.hide_crosshair()

    def on_chart_hover(self, event):
        if event.inaxes != self.price_ax or event.xdata is None or event.ydata is None:
            if event.inaxes != self.price_ax:
                self.hide_crosshair()
            return

        candles: list[CandleView] = self.chart_context.get("candles", [])
        x_vals = self.chart_context.get("x_vals", [])
        if not candles or len(x_vals) == 0:
            return

        nearest_index = min(range(len(x_vals)), key=lambda idx: abs(x_vals[idx] - event.xdata))
        candle = candles[nearest_index]
        x_value = x_vals[nearest_index]

        for line in self.crosshair_vlines:
            line.set_xdata([x_value, x_value])
            line.set_visible(True)

        self.crosshair_hline.set_ydata([event.ydata, event.ydata])
        self.crosshair_hline.set_visible(True)

        self.hover_label.xy = (x_value, candle.close)
        self.hover_label.set_text(
            f"{candle.date.strftime('%H:%M')}\n"
            f"開 {candle.open:.2f}  高 {candle.high:.2f}\n"
            f"低 {candle.low:.2f}  收 {candle.close:.2f}\n"
            f"AVG {candle.average:.2f}\n"
            f"單根量 {candle.volume:,}  累積量 {candle.cumulative_volume:,}"
        )
        self.hover_label.set_visible(True)
        self.canvas.draw_idle()

    def get_selected_state(self) -> Optional[StockState]:
        if not self.selected_symbol:
            return None
        for state in self.states:
            if state.symbol_name == self.selected_symbol:
                return state
        return None

    def update_stock_overview(self, state: StockState):
        if state.candles:
            last = state.candles[-1]
            change_value = last.close - state.prev_close
            change_text = f"{change_value:+.2f}"
            tag = "rise" if change_value > 0 else "fall" if change_value < 0 else "flat"
            last_text = f"{last.close:.2f}"
            if math.isclose(last.close, state.limit_up_price, abs_tol=1e-9) or math.isclose(last.close, state.limit_down_price, abs_tol=1e-9):
                last_text += "*"
            values = (
                state.symbol_name,
                last_text,
                change_text,
            )
        elif state.last_error:
            tag = "error"
            values = (state.symbol_name, "-", "-")
        else:
            tag = "flat"
            values = (state.symbol_name, "-", "-")
        self.stock_tree.item(state.symbol_name, values=values, tags=(tag,))

    def merge_candles(self, state: StockState, incoming: list[CandleView]) -> tuple[int, int]:
        existing_by_key = {candle.date.strftime("%Y-%m-%d %H:%M"): candle for candle in state.candles}
        inserted = 0
        updated = 0

        for candle in incoming:
            key = candle.date.strftime("%Y-%m-%d %H:%M")
            if key in existing_by_key:
                target = existing_by_key[key]
                target.open = candle.open
                target.high = candle.high
                target.low = candle.low
                target.close = candle.close
                target.volume = candle.volume
                target.cumulative_volume = candle.cumulative_volume
                target.average = candle.average
                updated += 1
            else:
                state.candles.append(candle)
                existing_by_key[key] = candle
                inserted += 1

        state.candles.sort(key=lambda row: row.date)
        return inserted, updated

    def apply_updates(self, payloads: list[tuple[StockState, list[CandleView], Optional[str], datetime]]):
        inserted_total = 0
        updated_total = 0
        success_count = 0

        for state, candles, error_message, update_time in payloads:
            if error_message is None:
                inserted, updated = self.merge_candles(state, candles)
                inserted_total += inserted
                updated_total += updated
                state.last_error = None
                state.last_update = update_time
                success_count += 1
            else:
                state.last_error = error_message
            self.update_stock_overview(state)

        removed_count = 0
        self.refresh_monitor_list_visibility()
        if self.selected_symbol:
            self.render_selected_stock()
        else:
            self.clear_stock_display()
        failure_count = len(payloads) - success_count
        status_text = (
            f"最近更新 {now_tpe().strftime('%Y-%m-%d %H:%M:%S')} | 成功 {success_count} | 失敗 {failure_count} | "
            f"新增K {inserted_total} | 覆寫K {updated_total}"
        )
        self.status_var.set(status_text)

    def fetch_once(self, initial: bool):
        update_time = now_tpe()
        payloads: list[tuple[StockState, list[CandleView], Optional[str], datetime]] = []

        for state in self.states:
            try:
                candles = fetch_intraday_candles(
                    self.rest_stock,
                    state.symbol_code_with_suf,
                )
                payloads.append((state, candles, None, update_time))
            except Exception as exc:
                payloads.append((state, state.candles, str(exc), update_time))
            time.sleep(0.1)

        log(f"{'完成初始載入' if initial else '完成本輪更新'}，共 {len(self.states)} 檔。")
        self.root.after(0, lambda: self.apply_updates(payloads))

    def background_loop(self):
        while not self.stop_event.is_set():
            current_time = now_tpe()

            if not time_tuple_reached(current_time, START_FETCH_TIME):
                self.root.after(0, lambda: self.status_var.set(f"等待開始取資料時間 {format_hms(START_FETCH_TIME)}"))
                wait_target = current_time.replace(
                    hour=START_FETCH_TIME[0],
                    minute=START_FETCH_TIME[1],
                    second=START_FETCH_TIME[2],
                    microsecond=0,
                )
                wait_seconds = max(0.2, (wait_target - current_time).total_seconds())
                if self.stop_event.wait(timeout=min(wait_seconds, 1.0)):
                    return
                continue

            if not self.initial_fetch_done:
                log(f"已達開始取資料時間 {format_hms(START_FETCH_TIME)}，開始初始載入。")
                self.fetch_once(initial=True)
                self.initial_fetch_done = True

            if time_tuple_reached(current_time, STOP_MONITOR_TIME) and not self.monitor_stopped:
                self.monitor_stopped = True
                log("已達停止監看時間，停止自動更新。")
                self.root.after(0, lambda: self.status_var.set("已達停止監看時間，停止自動更新。"))

            if self.monitor_stopped:
                if self.stop_event.wait(timeout=1.0):
                    return
                continue

            next_update = next_trigger_time(current_time, FETCH_SECOND_IN_MINUTE)
            wait_seconds = max(0.2, (next_update - current_time).total_seconds())
            if self.stop_event.wait(timeout=wait_seconds):
                return

            log(f"開始更新桌面監看，更新時點 {now_tpe().strftime('%Y-%m-%d %H:%M:%S')}")
            self.fetch_once(initial=False)

    def start(self):
        threading.Thread(target=self.background_loop, daemon=True).start()

    def on_close(self):
        if self.stop_event.is_set():
            try:
                self.close_trade_window()
                self.root.destroy()
            except tk.TclError:
                pass
            return

        self.stop_event.set()
        log("收到關閉事件，結束監看程式。")
        try:
            self.close_trade_window()
            self.root.destroy()
        except tk.TclError:
            pass


def main():
    config = ConfigParser()
    config.read(CONFIG_PATH)
    normalize_config_paths(config)

    realtime_sdk = EsunMarketdata(config)
    realtime_sdk.login()
    rest_stock = realtime_sdk.rest_client.stock

    log("桌面監看程式啟動。")
    log(
        f"開始取資料時間 {format_hms(START_FETCH_TIME)} | "
        f"每分鐘秒數至{FETCH_SECOND_IN_MINUTE:02d}更新 | "
        f"停止監看時間 {format_hms(STOP_MONITOR_TIME)}"
    )
    if not selected_stocks:
        log("沒有可監看的股票，程式結束。")
        return

    log(f"監看股票數量：{len(selected_stocks)}")
    root = tk.Tk()
    app = DesktopIntradayMonitorApp(root, rest_stock)
    app.initialize_states(selected_stocks)
    app.start()
    root.mainloop()


if __name__ == "__main__":
    main()
