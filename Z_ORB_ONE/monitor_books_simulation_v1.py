# -*- coding: utf-8 -*-
"""最佳五檔＋逐筆成交 WebSocket 監聽模擬（只做空，不下單）。

- 監聽同目錄 stock_data.py 的 total_stocks，每檔訂閱 books 與 trades 兩個頻道，共用 1 條行情 WebSocket 連線。
- 不呼叫任何 REST 行情 API；漲跌停價以 total_stocks 的昨收計算。
- 策略（皆只做空，模擬 1 張）：
    A 持續賣壓：OBI ≤ -0.5 持續 30 秒、期間賣一價未上漲、30 秒內盤比 ≥ 55%。
      訊號出場：OBI ≥ +0.2 持續 10 秒，或 30 秒外盤比 ≥ 65%。
    B 賣牆壓頂：賣方某檔 ≥ 其他賣方檔平均 5 倍且 ≥ 50 張、距買一 2 檔內、存在 ≥ 20 秒、
      期間有買方吃牆但牆仍保有最大量 50% 以上。
      訊號出場：牆撤單或被吃光立即回補；成交價或賣一高於牆價（吃穿）停損回補。
    C 買盤抽離：五檔買方總量 10 秒內減少 ≥ 50%，其中 ≥ 70% 為撤單，買一價未變或僅降 1 檔。
      訊號出場：買方總量回復至抽離前 80% 以上並持續 10 秒。
  共用出場：硬停損（賣一 ≥ 進場價 +1%，向上取 tick）、時間停損（持有 10 分鐘後仍未獲利）、12:50 強制回補。
- 同一檔股票同一時間最多 1 張（不分策略，先觸發者進場；同時觸發依 A、B、C 順序）。
  出場後該策略條件須於出場後重新成立（持續時間從出場時重新計算），不另設冷卻時間。
- 放空以買一價成交、回補以賣一價成交；手續費 0.1425%（買賣各一次）、證交稅 0.3%。
- 成交價觸及漲停或跌停的股票當日不再進場；盤中啟動時，啟動前的漲跌停紀錄無法得知。
- 只印在控制台，不輸出任何檔案。

執行：python monitor_books_simulation_v1.py [--debug]
"""
import argparse
import json
import os
import queue
import sys
import threading
import time
from collections import deque
from configparser import ConfigParser
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path

import pytz

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

TAIPEI = pytz.timezone('Asia/Taipei')

# ===== 參數 =====
CONNECT_NOT_BEFORE = (8, 50)        # 早於此時間啟動則等待至此再連線
ENTRY_START = (9, 5)                # 最早進場（含）
ENTRY_END = (12, 0)                 # 最晚進場（含）
FORCE_EXIT = (12, 50)               # 強制回補
STATUS_EVERY_MINUTES = 15           # 狀態列間隔（對齊整點 00/15/30/45）

FEE_RATE = Decimal('0.001425')      # 手續費（買、賣各一次）
TAX_RATE = Decimal('0.003')         # 證交稅（賣出）
SHARES_PER_LOT = 1000

HARD_STOP_PERCENT = Decimal('1')    # 賣一 ≥ 進場價 ×(1+1%) 停損
TIME_STOP_SECONDS = 600             # 持有超過此秒數仍未獲利則出場

TRADE_WINDOW_SECONDS = 30           # 內外盤比統計視窗

A_OBI_ENTRY = -0.5
A_PERSIST_SECONDS = 30
A_INNER_RATIO_MIN = 0.55
A_OBI_EXIT = 0.2
A_EXIT_PERSIST_SECONDS = 10
A_OUTER_RATIO_EXIT = 0.65

B_WALL_MULTIPLE = 5.0
B_WALL_MIN_LOTS = 50
B_WALL_MAX_DISTANCE_TICKS = 2
B_WALL_PERSIST_SECONDS = 20
B_WALL_KEEP_RATIO = 0.5             # 牆須保有最大量的此比例以上
B_CANCEL_SHARE_FOR_PULL = 0.5       # 牆量減少中撤單占比達此值判定為撤單

C_WINDOW_SECONDS = 10
C_DROP_RATIO = 0.5
C_CANCEL_SHARE_MIN = 0.7
C_RECOVER_RATIO = 0.8
C_RECOVER_PERSIST_SECONDS = 10

SIZE_DIVISOR = 1                    # 若 WebSocket 張數單位為「股」，改為 1000
STALL_RECONNECT_SECONDS = 90        # 盤中超過此秒數未收到任何訊息即重連
PING_EVERY_SECONDS = 60
RECONNECT_BACKOFF = (3, 6, 12, 30, 60)
AUTH_TIMEOUT_SECONDS = 15

STRATEGIES = ('A', 'B', 'C')
STRATEGY_NAMES = {'A': '持續賣壓', 'B': '賣牆壓頂', 'C': '買盤抽離'}

DEBUG = False


# ===== 時間與價格工具 =====
def now_dt():
    return datetime.now(TAIPEI)


def now_ts():
    return time.time()


def hhmmss(dt=None):
    return (dt or now_dt()).strftime('%H:%M:%S')


def at_time(dt, hm):
    return dt.replace(hour=hm[0], minute=hm[1], second=0, microsecond=0)


def log(message):
    print(f'[{hhmmss()}] {message}', flush=True)


def debug(message):
    if DEBUG:
        log('除錯 ' + message)


def tick_size(price):
    p = Decimal(str(price))
    if p < 10:
        return Decimal('0.01')
    if p < 50:
        return Decimal('0.05')
    if p < 100:
        return Decimal('0.1')
    if p < 500:
        return Decimal('0.5')
    if p < 1000:
        return Decimal('1')
    return Decimal('5')


def to_tick(price, rounding):
    p = Decimal(str(price))
    t = tick_size(p)
    return float((p / t).to_integral_value(rounding=rounding) * t)


def ticks_between(low, high):
    """low 到 high 之間的檔數（依各價位區間逐檔計算）。"""
    low_d, high_d = Decimal(str(low)), Decimal(str(high))
    if high_d <= low_d:
        return 0
    count, p = 0, low_d
    while p < high_d and count < 1000:
        p += tick_size(p)
        count += 1
    return count


def price_minus_ticks(price, n):
    p = Decimal(str(price))
    for _ in range(n):
        p -= tick_size(p - Decimal('0.0001'))
    return float(p)


def limit_prices(previous_close):
    up = to_tick(Decimal(str(previous_close)) * Decimal('1.1'), ROUND_FLOOR)
    down = to_tick(Decimal(str(previous_close)) * Decimal('0.9'), ROUND_CEILING)
    return up, down


def net_result(entry_price, exit_price):
    """放空 1 股淨損益（元）與報酬率（%）。"""
    e, x = Decimal(str(entry_price)), Decimal(str(exit_price))
    net = e * (1 - FEE_RATE - TAX_RATE) - x * (1 + FEE_RATE)
    return float(net), float(net / e * 100)


def fmt_price(p):
    return f'{p:g}'


# ===== 每檔狀態 =====
class SymbolState:
    def __init__(self, name, code, previous_close):
        self.name = name
        self.code = code
        self.label = f'{name}:{code}'
        self.previous_close = previous_close
        self.limit_up, self.limit_down = limit_prices(previous_close)
        self.bids = []                    # [(price, lots)] 由高到低
        self.asks = []                    # [(price, lots)] 由低到高
        self.first_book_ts = None
        self.trades = deque()             # (ts, price, lots, side)  side: +1 外盤, -1 內盤, 0 不明
        self.cum_inner = 0.0              # 累計內盤張數（用於 C 判斷撤單）
        self.outer_by_price_since_book = {}  # 上次 books 後，各價位外盤成交張數（用於 B）
        self.disabled = False             # 成交觸及漲跌停後當日不再進場
        self.position = None
        self.reset_strategy_states()

    def reset_strategy_states(self):
        self.a_since = None
        self.a_ask_ref = None
        self.b_wall_price = None
        self.b_wall_since = None
        self.b_wall_max = 0.0
        self.b_wall_eaten = 0.0
        self.b_wall_prev_size = 0.0
        self.c_hist = deque()             # (ts, bid_depth, bid1, cum_inner)

    def reset_one(self, strategy):
        if strategy == 'A':
            self.a_since = None
            self.a_ask_ref = None
        elif strategy == 'B':
            self.b_wall_price = None
            self.b_wall_since = None
            self.b_wall_max = 0.0
            self.b_wall_eaten = 0.0
            self.b_wall_prev_size = 0.0
        elif strategy == 'C':
            self.c_hist.clear()

    @property
    def bid1(self):
        return self.bids[0][0] if self.bids else None

    @property
    def ask1(self):
        return self.asks[0][0] if self.asks else None

    def depth(self, side):
        levels = self.bids if side == 'bid' else self.asks
        return sum(size for _, size in levels)

    def obi(self):
        b, a = self.depth('bid'), self.depth('ask')
        return (b - a) / (b + a) if b + a > 0 else None

    def trade_ratio(self, ts, window=TRADE_WINDOW_SECONDS):
        """回傳 (內盤比, 外盤比)；視窗內無成交回傳 (None, None)。"""
        while self.trades and ts - self.trades[0][0] > 60:
            self.trades.popleft()
        inner = outer = 0.0
        for t, _, lots, side in self.trades:
            if ts - t <= window:
                if side > 0:
                    outer += lots
                elif side < 0:
                    inner += lots
        total = inner + outer
        if total <= 0:
            return None, None
        return inner / total, outer / total

    def ask_size_at(self, price):
        for p, s in self.asks:
            if abs(p - price) < 1e-9:
                return s
        return 0.0

    def ask_visible(self, price):
        return bool(self.asks) and price <= self.asks[-1][0] + 1e-9


# ===== 主模擬器 =====
class BooksSimulator:
    def __init__(self, stocks, realtime_sdk):
        self.symbols = {}
        for row in stocks:
            name, rest = row[0].split(':', 1)
            code = rest.split('.')[0]
            self.symbols[code] = SymbolState(name, code, float(row[5]))
        self.realtime_sdk = realtime_sdk
        self.events = queue.Queue()
        self.client = None
        self.generation = 0               # 每次連線遞增；忽略舊連線遺留的事件
        self.connected = False
        self.reconnects = 0
        self.reconnect_at = None
        self.backoff_index = 0
        self.last_message_ts = None
        self.last_ping_ts = 0.0
        self.subscribed = 0
        self.subscribe_reported = False
        self.subscribe_sent_ts = None
        self.error_messages = {}
        self.raw_printed = set()
        self.trades_done = []
        self.skipped = {s: 0 for s in STRATEGIES}
        self.next_status = None
        self.stopping = False

    # ---------- 連線 ----------
    def connect(self):
        self.generation += 1
        gen = self.generation
        client = self.realtime_sdk.websocket_client.stock
        client.on('message', lambda data: self.events.put(('message', gen, data)))
        client.on('disconnect', lambda *args: self.events.put(('disconnect', gen, args)))
        client.on('error', lambda err: self.events.put(('ws_error', gen, err)))
        ws = getattr(client, '_WebSocketClient__ws', None)
        if ws is not None and hasattr(client, 'auth_status'):
            # 不使用 SDK connect() 的忙碌等待；自行以背景執行緒連線並逾時檢查認證。
            threading.Thread(target=ws.run_forever, daemon=True, name='books-ws').start()
            deadline = now_ts() + AUTH_TIMEOUT_SECONDS
            while now_ts() < deadline:
                if client.auth_status == 2:      # AUTHENTICATED
                    break
                if client.auth_status == 3:      # UNAUTHENTICATED
                    raise RuntimeError(f'行情 WebSocket 認證失敗：{client.error}')
                time.sleep(0.05)
            else:
                try:
                    ws.close()
                except Exception:
                    pass
                raise RuntimeError('行情 WebSocket 認證逾時')
        else:
            client.connect()
        self.client = client
        self.connected = True
        self.last_message_ts = now_ts()
        self.subscribed = 0
        self.subscribe_reported = False
        for code in self.symbols:
            for channel in ('books', 'trades'):
                client.subscribe({'channel': channel, 'symbol': code})
                time.sleep(0.005)
        self.subscribe_sent_ts = now_ts()

    def close_client(self):
        client, self.client = self.client, None
        self.connected = False
        if client is None:
            return
        try:
            client.disconnect()
        except Exception:
            try:
                getattr(client, '_WebSocketClient__ws').close()
            except Exception:
                pass

    def schedule_reconnect(self, reason):
        if self.stopping or self.reconnect_at is not None:
            return
        self.close_client()
        delay = RECONNECT_BACKOFF[min(self.backoff_index, len(RECONNECT_BACKOFF) - 1)]
        self.backoff_index += 1
        self.reconnect_at = now_ts() + delay
        log(f'警告 {reason}，{delay} 秒後重連（第{self.backoff_index}次）')

    def try_reconnect(self):
        self.reconnect_at = None
        try:
            self.connect()
        except Exception as exc:
            self.schedule_reconnect(f'重連失敗 {exc!r}')
            return
        self.reconnects += 1
        self.backoff_index = 0
        for state in self.symbols.values():   # 斷線期間資料不連續，持續條件全部重新計算
            state.reset_strategy_states()
            state.trades.clear()
            state.outer_by_price_since_book.clear()
        log(f'重連成功，已重新送出訂閱 {len(self.symbols) * 2}')

    # ---------- 訊息處理 ----------
    def handle_message(self, raw):
        self.last_message_ts = now_ts()
        try:
            message = json.loads(raw)
        except (TypeError, ValueError):
            return
        event = message.get('event')
        if event == 'subscribed':
            data = message.get('data')
            self.subscribed += len(data) if isinstance(data, list) else 1
            return
        if event == 'error':
            text = str((message.get('data') or {}).get('message', message.get('data')))
            count = self.error_messages.get(text, 0) + 1
            self.error_messages[text] = count
            if count <= 3:
                log(f'警告 伺服器錯誤訊息：{text}')
            return
        if event not in ('data', 'snapshot'):
            return
        channel = message.get('channel')
        data = message.get('data') or {}
        if DEBUG and channel in ('books', 'trades') and channel not in self.raw_printed:
            self.raw_printed.add(channel)
            log(f'除錯 第一筆 {channel} 原始訊息：{raw[:600]}')
        state = self.symbols.get(str(data.get('symbol')))
        if state is None or data.get('isTrial'):
            return
        dt = now_dt()
        if dt < at_time(dt, (9, 0)):           # 忽略盤前試撮
            return
        ts = now_ts()
        if channel == 'books':
            self.on_books(state, data, ts)
        elif channel == 'trades':
            self.on_trade(state, data, ts)

    def on_books(self, state, data, ts):
        def levels(key):
            out = []
            for level in data.get(key) or []:
                try:
                    price, size = float(level['price']), float(level['size']) / SIZE_DIVISOR
                except (KeyError, TypeError, ValueError):
                    continue
                if price > 0 and size > 0:
                    out.append((price, size))
            return out
        state.bids = sorted(levels('bids'), key=lambda x: -x[0])[:5]
        state.asks = sorted(levels('asks'), key=lambda x: x[0])[:5]
        if state.first_book_ts is None:
            state.first_book_ts = ts
        self.update_wall(state, ts)
        self.update_c_hist(state, ts)
        state.outer_by_price_since_book.clear()
        self.evaluate(state, ts)

    def on_trade(self, state, data, ts):
        try:
            price = float(data['price'])
            lots = float(data.get('size') or 0) / SIZE_DIVISOR
        except (KeyError, TypeError, ValueError):
            return
        if price <= 0 or lots <= 0:
            return
        bid = data.get('bid', state.bid1)
        ask = data.get('ask', state.ask1)
        side = 0
        if ask is not None and price >= float(ask) - 1e-9:
            side = 1
        elif bid is not None and price <= float(bid) + 1e-9:
            side = -1
        state.trades.append((ts, price, lots, side))
        if side < 0:
            state.cum_inner += lots
        if side > 0:
            state.outer_by_price_since_book[price] = state.outer_by_price_since_book.get(price, 0.0) + lots
            if state.b_wall_price is not None and abs(price - state.b_wall_price) < 1e-9:
                state.b_wall_eaten += lots
            pos = state.position
            if pos and pos['strategy'] == 'B' and abs(price - pos['wall_price']) < 1e-9:
                pos['wall_eaten'] += lots
        if not state.disabled and (price >= state.limit_up - 1e-9 or price <= state.limit_down + 1e-9):
            state.disabled = True
            debug(f'{state.label} 成交 {fmt_price(price)} 觸及漲跌停，當日不再進場')
        pos = state.position
        if pos and pos['strategy'] == 'B' and price > pos['wall_price'] + 1e-9:
            self.exit_position(state, ts, '牆被吃穿')
            return
        self.evaluate(state, ts)

    # ---------- B：賣牆追蹤 ----------
    def find_wall(self, state):
        if len(state.asks) < 2 or state.bid1 is None:
            return None
        best = None
        for i, (price, size) in enumerate(state.asks):
            others = [s for j, (_, s) in enumerate(state.asks) if j != i]
            avg = sum(others) / len(others)
            if size >= B_WALL_MIN_LOTS and size >= B_WALL_MULTIPLE * avg:
                if best is None or size > best[1]:
                    best = (price, size)
        if best and ticks_between(state.bid1, best[0]) <= B_WALL_MAX_DISTANCE_TICKS:
            return best
        return None

    def update_wall(self, state, ts):
        pos = state.position
        if pos and pos['strategy'] == 'B':
            wp = pos['wall_price']
            if state.ask_visible(wp):
                size = state.ask_size_at(wp)
                delta = pos['wall_prev_size'] - size
                if delta > 0:
                    eaten = state.outer_by_price_since_book.get(wp, 0.0)
                    pos['wall_cancel'] += max(0.0, delta - eaten)
                pos['wall_prev_size'] = size
                pos['wall_now'] = size
        wall = self.find_wall(state)
        if wall is None:
            state.reset_one('B')
            return
        price, size = wall
        if state.b_wall_price is None or abs(price - state.b_wall_price) > 1e-9:
            state.b_wall_price, state.b_wall_since = price, ts
            state.b_wall_max, state.b_wall_eaten = size, 0.0
        state.b_wall_max = max(state.b_wall_max, size)
        state.b_wall_prev_size = size

    # ---------- C：買方深度歷史 ----------
    def update_c_hist(self, state, ts):
        if state.bid1 is None:
            return
        state.c_hist.append((ts, state.depth('bid'), state.bid1, state.cum_inner))
        while state.c_hist and ts - state.c_hist[0][0] > C_WINDOW_SECONDS:
            state.c_hist.popleft()

    # ---------- 進出場判斷 ----------
    def in_entry_window(self, dt):
        return at_time(dt, ENTRY_START) <= dt <= at_time(dt, ENTRY_END)

    def check_a(self, state, ts):
        obi = state.obi()
        ask1 = state.ask1
        if obi is None or ask1 is None or obi > A_OBI_ENTRY:
            state.reset_one('A')
            return None
        if state.a_since is None:
            state.a_since, state.a_ask_ref = ts, ask1
            return None
        if ask1 > state.a_ask_ref + 1e-9:
            state.a_since, state.a_ask_ref = ts, ask1
            return None
        if ts - state.a_since < A_PERSIST_SECONDS:
            return None
        inner, _ = state.trade_ratio(ts)
        if inner is None or inner < A_INNER_RATIO_MIN:
            return None
        return (f'OBI={obi:+.2f}({int(ts - state.a_since)}秒) 內盤比={inner:.0%} '
                f'買深={state.depth("bid"):,.0f} 賣深={state.depth("ask"):,.0f}')

    def check_b(self, state, ts):
        if state.b_wall_price is None or ts - state.b_wall_since < B_WALL_PERSIST_SECONDS:
            return None
        if state.b_wall_eaten <= 0 or state.b_wall_prev_size < B_WALL_KEEP_RATIO * state.b_wall_max:
            return None
        return (f'牆={fmt_price(state.b_wall_price)}×{state.b_wall_prev_size:,.0f}張'
                f'(撐{int(ts - state.b_wall_since)}秒，已被吃{state.b_wall_eaten:,.0f}張)')

    def check_c(self, state, ts):
        if (state.first_book_ts is None or ts - state.first_book_ts < C_WINDOW_SECONDS
                or len(state.c_hist) < 2 or state.bid1 is None):
            return None
        ref = max(state.c_hist, key=lambda x: x[1])
        ref_ts, ref_depth, ref_bid1, ref_inner = ref
        depth_now = state.depth('bid')
        if ref_depth <= 0 or depth_now > ref_depth * (1 - C_DROP_RATIO):
            return None
        if state.bid1 < price_minus_ticks(ref_bid1, 1) - 1e-9:
            return None
        decrease = ref_depth - depth_now
        consumed = max(0.0, state.cum_inner - ref_inner)
        cancel_share = max(0.0, decrease - consumed) / decrease
        if cancel_share < C_CANCEL_SHARE_MIN:
            return None
        return (f'買深 {ref_depth:,.0f}→{depth_now:,.0f}張({int(ts - ref_ts)}秒內 '
                f'-{decrease / ref_depth:.0%}，撤單占{cancel_share:.0%})'), ref_depth

    def evaluate(self, state, ts):
        dt = now_dt()
        if state.position:
            self.check_exit(state, ts, dt)
        signals = {}
        a = self.check_a(state, ts)
        if a:
            signals['A'] = (a, None)
        b = self.check_b(state, ts)
        if b:
            signals['B'] = (b, None)
        c = self.check_c(state, ts)
        if c:
            signals['C'] = c
        if not signals:
            return
        can_enter = (not self.stopping and self.connected and not state.disabled
                     and self.in_entry_window(dt) and state.bid1 is not None)
        for strategy in STRATEGIES:
            if strategy not in signals:
                continue
            if state.position is not None:
                if state.position['strategy'] != strategy:
                    self.skipped[strategy] += 1
                    debug(f'{state.label} {strategy} 訊號因持倉({state.position["strategy"]})略過')
                state.reset_one(strategy)
                continue
            if not can_enter:
                continue
            info, extra = signals[strategy]
            self.enter_position(state, strategy, ts, dt, info, extra)

    def enter_position(self, state, strategy, ts, dt, info, extra):
        entry = state.bid1
        stop = to_tick(Decimal(str(entry)) * (1 + HARD_STOP_PERCENT / 100), ROUND_CEILING)
        if stop >= state.limit_up - 1e-9:
            state.reset_one(strategy)
            debug(f'{state.label} {strategy} 停損價達漲停，不進場')
            return
        pos = {'strategy': strategy, 'entry': entry, 'entry_ts': ts, 'entry_dt': dt, 'stop': stop,
               'exit_since': None}
        if strategy == 'B':
            pos.update(wall_price=state.b_wall_price, wall_max=state.b_wall_max,
                       wall_prev_size=state.b_wall_prev_size, wall_now=state.b_wall_prev_size,
                       wall_cancel=0.0, wall_eaten=0.0)
        if strategy == 'C':
            pos['ref_depth'] = extra
        state.position = pos
        state.reset_one(strategy)
        log(f'進場 {strategy} {state.label} 空1張 @{fmt_price(entry)}(買一)｜{info}｜停損={fmt_price(stop)}')

    def check_exit(self, state, ts, dt):
        pos = state.position
        ask1 = state.ask1
        if dt >= at_time(dt, FORCE_EXIT):
            self.exit_position(state, ts, '12:50強制回補')
            return
        if ask1 is None:
            if state.bid1 is not None and state.bid1 >= state.limit_up - 1e-9:
                self.exit_position(state, ts, '漲停鎖住(可能無法成交)', price=state.limit_up)
            return
        if ask1 >= pos['stop'] - 1e-9:
            self.exit_position(state, ts, '停損')
            return
        strategy = pos['strategy']
        if strategy == 'A':
            obi = state.obi()
            if obi is not None and obi >= A_OBI_EXIT:
                pos['exit_since'] = pos['exit_since'] or ts
                if ts - pos['exit_since'] >= A_EXIT_PERSIST_SECONDS:
                    self.exit_position(state, ts, '訊號消失(OBI轉正)')
                    return
            else:
                pos['exit_since'] = None
            _, outer = state.trade_ratio(ts)
            if outer is not None and outer >= A_OUTER_RATIO_EXIT and ts - pos['entry_ts'] >= 1:
                self.exit_position(state, ts, f'訊號消失(外盤比{outer:.0%})')
                return
        elif strategy == 'B':
            wp = pos['wall_price']
            if ask1 > wp + 1e-9:
                self.exit_position(state, ts, '牆被吃穿')
                return
            if state.ask_visible(wp) and pos['wall_now'] < B_WALL_KEEP_RATIO * pos['wall_max']:
                reduction = max(pos['wall_max'] - pos['wall_now'], 1e-9)
                reason = '牆撤單' if pos['wall_cancel'] / reduction >= B_CANCEL_SHARE_FOR_PULL else '牆被吃光'
                self.exit_position(state, ts, reason)
                return
        elif strategy == 'C':
            if state.depth('bid') >= C_RECOVER_RATIO * pos['ref_depth']:
                pos['exit_since'] = pos['exit_since'] or ts
                if ts - pos['exit_since'] >= C_RECOVER_PERSIST_SECONDS:
                    self.exit_position(state, ts, '訊號消失(買盤回補)')
                    return
            else:
                pos['exit_since'] = None
        if ts - pos['entry_ts'] >= TIME_STOP_SECONDS and ask1 >= pos['entry'] - 1e-9:
            self.exit_position(state, ts, '時間停損')

    def exit_position(self, state, ts, reason, price=None):
        pos = state.position
        if pos is None:
            return
        exit_price = price if price is not None else (state.ask1 if state.ask1 is not None else state.limit_up)
        net_share, pct = net_result(pos['entry'], exit_price)
        held = ts - pos['entry_ts']
        self.trades_done.append({'strategy': pos['strategy'], 'label': state.label, 'pct': pct,
                                 'yuan': net_share * SHARES_PER_LOT, 'held': held})
        state.position = None
        state.reset_one(pos['strategy'])   # 出場後條件從此刻重新計算
        held_text = f'{int(held // 60)}分' if held >= 60 else f'{int(held)}秒'
        log(f'出場 {pos["strategy"]} {state.label} @{fmt_price(exit_price)}(賣一) {reason}｜持有{held_text}｜'
            f'淨損益 {pct:+.2f}%（{net_share * SHARES_PER_LOT:+,.0f}元）')

    # ---------- 定時工作 ----------
    def periodic(self):
        ts, dt = now_ts(), now_dt()
        if self.reconnect_at is not None and ts >= self.reconnect_at:
            self.try_reconnect()
        if (self.connected and not self.subscribe_reported and self.subscribe_sent_ts
                and (self.subscribed >= len(self.symbols) * 2 or ts - self.subscribe_sent_ts >= 10)):
            self.subscribe_reported = True
            total = len(self.symbols) * 2
            note = '' if self.subscribed >= total else '（未全部確認，請留意伺服器錯誤訊息）'
            log(f'訂閱確認 {self.subscribed}/{total}{note}')
        if self.connected and ts - self.last_ping_ts >= PING_EVERY_SECONDS:
            self.last_ping_ts = ts
            try:
                self.client.ping('keepalive')
            except Exception:
                pass
        if (self.connected and self.last_message_ts and ts - self.last_message_ts >= STALL_RECONNECT_SECONDS
                and at_time(dt, (9, 0)) <= dt < at_time(dt, FORCE_EXIT)):
            self.schedule_reconnect(f'{STALL_RECONNECT_SECONDS} 秒未收到行情')
        for state in self.symbols.values():
            if state.bids or state.asks:
                self.evaluate(state, ts)
        if self.next_status is None:
            self.next_status = self.next_status_time(dt)
        elif dt >= self.next_status:
            self.print_status()
            self.next_status = self.next_status_time(dt)

    @staticmethod
    def next_status_time(dt):
        base = dt.replace(second=0, microsecond=0)
        minutes = STATUS_EVERY_MINUTES - base.minute % STATUS_EVERY_MINUTES
        return base + timedelta(minutes=minutes)

    def print_status(self):
        holding = sum(1 for s in self.symbols.values() if s.position)
        done = ' '.join(f'{s}:{sum(1 for t in self.trades_done if t["strategy"] == s)}' for s in STRATEGIES)
        total = sum(t['pct'] for t in self.trades_done)
        log(f'狀態｜持倉 {holding}｜已出場 {done}｜累計 {total:+.2f}%｜重連 {self.reconnects}')

    def print_summary(self):
        dt = now_dt()
        print(f'===== {dt:%Y-%m-%d} 模擬總結 =====', flush=True)
        print('策略        交易  獲利  勝率  平均淨損益    合計   合計(元)  平均持有  因持倉限制略過', flush=True)
        for s in STRATEGIES:
            rows = [t for t in self.trades_done if t['strategy'] == s]
            n = len(rows)
            wins = sum(1 for t in rows if t['pct'] > 0)
            if n:
                avg = sum(t['pct'] for t in rows) / n
                total = sum(t['pct'] for t in rows)
                yuan = sum(t['yuan'] for t in rows)
                held = sum(t['held'] for t in rows) / n
                held_text = f'{held / 60:.0f}分' if held >= 60 else f'{held:.0f}秒'
                line = (f'{s} {STRATEGY_NAMES[s]}  {n:4d}  {wins:4d}  {wins / n:4.0%}  {avg:+9.2f}%  '
                        f'{total:+7.2f}%  {yuan:+9,.0f}  {held_text:>6}  {self.skipped[s]:6d}')
            else:
                line = f'{s} {STRATEGY_NAMES[s]}     0     0     -          -        -          -       -  {self.skipped[s]:6d}'
            print(line, flush=True)
        print(f'重連次數 {self.reconnects}', flush=True)

    # ---------- 主迴圈 ----------
    def run(self):
        dt = now_dt()
        if dt >= at_time(dt, FORCE_EXIT):
            log('今日模擬時段已結束（12:50 後），程式結束')
            return
        start_at = at_time(dt, CONNECT_NOT_BEFORE)
        if dt < start_at:
            log(f'等待至 {start_at:%H:%M} 再連線')
            while now_dt() < start_at:
                time.sleep(1)
        log(f'五檔監聽模擬 v1｜{len(self.symbols)} 檔｜訂閱 {len(self.symbols) * 2}（books+trades）｜'
            f'連線 1/2｜不下單')
        try:
            self.connect()
            log(f'行情 WebSocket 認證成功｜策略 A/B/C（只做空）｜手續費{FEE_RATE * 100:g}% 稅{TAX_RATE * 100:g}%')
        except Exception as exc:
            self.schedule_reconnect(f'首次連線失敗 {exc!r}')
        dt = now_dt()
        if dt > at_time(dt, ENTRY_START):
            log('注意：盤中啟動，啟動前是否曾觸及漲跌停無法得知；指標需暖機後才會進場')
        if dt > at_time(dt, ENTRY_END):
            log('已過 12:00 最後進場時間，僅監聽至 12:50 後印出總結')
        last_periodic = 0.0
        try:
            while True:
                try:
                    kind, gen, payload = self.events.get(timeout=0.2)
                except queue.Empty:
                    kind = gen = None
                if kind is not None and gen != self.generation:
                    kind = None
                if kind == 'message':
                    self.handle_message(payload)
                elif kind == 'disconnect' and self.connected and not self.stopping:
                    self.schedule_reconnect('連線中斷')
                elif kind == 'ws_error' and self.connected and not self.stopping:
                    self.schedule_reconnect(f'連線錯誤 {payload!r}')
                ts = now_ts()
                if ts - last_periodic >= 1.0:
                    last_periodic = ts
                    self.periodic()
                    dt = now_dt()
                    if dt >= at_time(dt, FORCE_EXIT):
                        break
        except KeyboardInterrupt:
            log('收到中斷，提前結束')
        self.stopping = True
        ts = now_ts()
        for state in self.symbols.values():
            if state.position:
                self.exit_position(state, ts, '12:50強制回補' if now_dt() >= at_time(now_dt(), FORCE_EXIT)
                                   else '程式結束回補')
        self.close_client()
        self.print_summary()


def load_total_stocks(environment):
    stock_data = environment.prepare_stock_data()
    stocks = list(getattr(stock_data, 'total_stocks', []))
    if not stocks:
        raise ValueError('stock_data.py 的 total_stocks 為空')
    return stocks


def main():
    global DEBUG
    parser = argparse.ArgumentParser(description='最佳五檔＋逐筆成交監聽模擬（不下單）')
    parser.add_argument('--debug', action='store_true', help='印出第一筆原始訊息與候選訊號細節')
    DEBUG = parser.parse_args().debug

    from runtime_environment import RuntimeEnvironment
    from esun_marketdata import EsunMarketdata

    environment = RuntimeEnvironment(str(BASE_DIR), mode='local')
    stocks = load_total_stocks(environment)
    config = ConfigParser()
    config.read(os.path.join(BASE_DIR, 'config.ini'))
    realtime_sdk = EsunMarketdata(config)
    environment.login_sdk(realtime_sdk)   # 與實機程式相同的登入方式
    BooksSimulator(stocks, realtime_sdk).run()


if __name__ == '__main__':
    main()
