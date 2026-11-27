# -*- coding: utf-8 -*-
"""即時交易策略共用參數；修改後需重新啟動主程式。"""
import math
import os

import pytz

# ============ 參數/常數 ============
# 已知對齊原則與刻意差異：
# 1. 回測版以分 K 模擬策略，實機版以 REST 即時 quote 執行；資料粒度差異不視為策略不一致。
# 2. 所有策略共用獲利保護及逐步追蹤停利；啟動逐利的浮盈門檻依策略設定。
TZ = pytz.timezone("Asia/Taipei")
BASE_DIR = os.path.dirname(__file__)
STATE_DIR = os.path.join(BASE_DIR, "stock_state")  # 狀態檔目錄
# 部署前提：每日只啟動一次，允許的最晚啟動時間由 MAIN_LATEST_START_TIME 控制。
# 若該次啟動失敗，當日不會再由排程重新啟動，因此不處理盤中恢復或補單情境。
MAIN_START_TIME = (8, 45)  # 主程序開始執行時間
MAIN_LATEST_START_TIME = (8, 58)  # 最晚允許啟動時刻（08:59:00）；超過即中止。
FORCE_EXIT_TIME = (13, 30)  # 13:30 強制關閉程式

# 單日策略前提：每檔股票在當日只會有一個固定交易方向。
STRATEGY_AVG = 'AVG'
STRATEGY_VOL = 'VOL'
STRATEGY_IND = 'IND'
STRATEGY_TOP = 'TOP'

TRADE_SIDE_SHORT = 'SHORT'
TRADE_SIDE_LONG = 'LONG'

# 一筆入場委託可分批成交；出場意圖鎖定後，晚到成交亦須補平。
# 不主動撤銷入場委託；成交數量與有效回補殘量以券商回報及查詢共同校正。

ENABLE_PROFIT_PROTECTION = False  # 控制獲利保護，不影響固定停損及追蹤停利
PROTECT_PROFIT_PER = 2.5 # 所有策略共用：觸發獲利保護百分比
PROTECT_LOSS_PER = 1.5 # 所有策略共用：獲利保護後的新停損百分比

PROFIT_BACK_PERCENT = 0.5 # 獲利後允許回撤百分比
PROFIT_TARGET_PERCENT = 0.5 # 逐步獲利目標百分比

REALTIME_QUOTE_START_TIME = (9, 3)  # 09:03 後才開始抓個股即時行情，避開開盤初期 quote 欄位不完整

# 共用股票池；候選可並存，同輪完整進場條件成立依順位送單，送單後才固定歸屬。
STOCK_DATA_SOURCE = "T"
STRATEGY_PRIORITY = ["AVG", "IND", "VOL", "TOP"]
# 每日啟動前設定；False 停止該策略的新候選與進場，既有持倉仍繼續風控。
ENABLE_AVG = True
ENABLE_VOL = True
ENABLE_IND = True
ENABLE_TOP = True
PREOPEN_TICKER_ATTEMPTS = 2  # 開盤前資格查詢輪數；仍缺資料者盤中重試，確認前禁止入場
# T: total_stocks；S: selected_stocks；共用候選流程使用副本。
AVG_STAGE_1_START_TIME = (9, 0)
AVG_STAGE_1_MIN_BARS = 8
AVG_STAGE_2_MIN_BARS = 8
AVG_STAGE_3_MIN_BARS = 8
AVG_STAGE_4_MIN_BARS = 3
AVG_STAGE_1_AMPLITUDE_PERCENT = 1.0
AVG_STAGE_2_AMPLITUDE_PERCENT = 1.0
AVG_STAGE_3_AMPLITUDE_PERCENT = 1.0
AVG_SLOPE_CUTOFF_TIME = (9, 20)
AVG_MAX_MISSING_SLOPE_BARS = 5  # 09:00（含）至斜率截止（不含）；無效 average 亦算缺棒，0 表示不可缺棒
AVG_MAX_NORMALIZED_SLOPE = math.tan(math.radians(3.5))  # 約0.061163 %／分鐘；以1分鐘、1百分點為尺度，須嚴格小於此值
AVG_ENTRY_START_TIME = (10, 0)
AVG_ENTRY_DEADLINE = (11, 0)
# 確認完成後保留資格至截止；首次預定入場早於開始時間則當日排除 AVG。
AVG_CANDLE_POLL_SECOND = 15  # 每分鐘於此秒數取一次完整分K，例如 09:33:15
AVG_CANDLE_REQUEST_START_INTERVAL_SECONDS = 0.105  # 最多約 9.5 次請求／秒
AVG_MIN_PRE_ENTRY_RANGE_PERCENT = 5.0
AVG_STOP_LOSS_PERCENT = 2.0
AVG_TAKE_PROFIT_PERCENT = 5.0
AVG_FORCE_CLOSE_TIME = (12, 50)
AVG_ENTRY_ORDER_QUANTITY = 1

# VOL: v35 minute signals; delayed completed-minute confirmation -> 15-second quote entry.
# Ticker eligibility replaces the backtest's 09:00-09:29 bar-count proxy.
VOL_SIGNAL_START_TIME = (9, 20)
VOL_ENTRY_DEADLINE = (12, 0)
VOL_PREVIOUS_TURNOVER_TOP_PERCENT = 30.0  # 共用股票池前日成交金額排名；100為不限排名
VOL_MIN_PREVIOUS_TURNOVER = 300_000_000  # 前日成交金額最低限額（元），另須通過排名
VOL_VOLUME_AVERAGE_DAYS = 5
VOL_MIN_VOLUME_RATIO = 4.0
VOL_REGULAR_SESSION_MINUTES = 270
VOL_MIN_RUNUP_PERCENT = 7.0
VOL_PULLBACK_PERCENT = 3.3
VOL_AVERAGE_CHECK_DELAY_BARS = 3  # 轉弱訊號後第N根有效分K為確認棒；1為次一根
VOL_AVERAGE_CHECK_SKIP_PERCENT = 0.5  # 確認棒close < average×(1−此百分比%)時排除當日VOL
VOL_VOLUME_UNIT_SHARES = 1000
VOL_STOP_LOSS_PERCENT = 2.0
VOL_TAKE_PROFIT_PERCENT = 6.0
VOL_FORCE_CLOSE_TIME = (12, 50)
VOL_ENTRY_ORDER_QUANTITY = 1

# IND：v40 完成分K確認後取得新即時報價，不等待下一根open。
IND_MARKET_KEY = 'TWSE:MARKET'  # symbol/昨收由開盤前已驗證快照取得（IX0001）
IND_MARKET_CHECK_TIME = (9, 15)
IND_STOCK_SCAN_START_TIME = (9, 16)
IND_MARKET_DROP_PERCENT = 1.2
IND_MARKET_SLOPE_START_TIME = (9, 5)  # 大盤close斜率起算分鐘（含），須早於檢查時間
IND_MARKET_MAX_NORMALIZED_SLOPE = math.tan(math.radians(-5))  # 有限數值，可為正、零或負；斜率須嚴格小於此值（%／分鐘）
IND_MARKET_MAX_MISSING_SLOPE_BARS = 3  # 起算至檢查時間（含）缺棒／無效close容許數
IND_STOCK_MAX_DROP_PERCENT = 0.5
IND_SLOPE_CUTOFF_TIME = (9, 5)  # 09:00至此分鐘（含）的已完成close整段回歸
IND_MAX_NORMALIZED_SLOPE = math.tan(math.radians(40))  # 約0.839100 %／分鐘，僅超過才排除
IND_MAX_MISSING_SLOPE_BARS = 2  # 缺棒／close無效容許數；至少2筆有效資料，無法計算不入場
IND_MARKET_RETRY_SECONDS = 5.0
IND_ENTRY_DEADLINE = (12, 0)
IND_STOP_LOSS_PERCENT = 2.0
IND_TAKE_PROFIT_PERCENT = 5.0
IND_FORCE_CLOSE_TIME = (12, 50)
IND_ENTRY_ORDER_QUANTITY = 1
IND_MAX_TRADES_PER_DAY = None  # 每日IND嘗試送單筆數上限（含拒單／結果未知），達到後停止IND新進場；None不限

# TOP：v41 昨日窄幅、自高點回落後均價反壓；完成分K確認後以即時報價入場，不等待下一根open。
# 昨日高低價及前日收盤由 aggregate v3 產生 stock_data.top_history_by_symbol。
TOP_PREV_RANGE_MAX_PERCENT = 3.2  # （昨高−昨低）／前日收盤×100 <= 此值才保留TOP
TOP_PULLBACK_FROM_HIGH_PERCENT = 3.5  # 訊號棒close相對09:00起最高價回落 >= 此值
TOP_SIGNAL_START_TIME = (9, 16)  # 訊號棒最早時間
TOP_ENTRY_DEADLINE = (12, 0)
TOP_STOP_LOSS_PERCENT = 2.0
TOP_TAKE_PROFIT_PERCENT = 5.0
TOP_FORCE_CLOSE_TIME = (12, 50)
TOP_ENTRY_ORDER_QUANTITY = 1
TOP_MAX_TRADES_PER_DAY = 3  # 每日TOP嘗試送單筆數上限（含拒單／結果未知），達到後停止TOP新進場；None不限


ORDER_RESULTS_UPDATE_SECONDS = 10.0  # 成交價量校正查詢間隔，避免每筆下單後立即查詢造成交易 API 連線異常
ORDER_RESULTS_QUERY_START_TIME = (9, 0)  # 開盤前不查委託結果，09:00 後才補成交價量
ORDER_RESULTS_RATE_LIMIT_COOLDOWN_SECONDS = 60.0  # AGR0005 後依券商指示暫停查詢
QUOTE_ROUND_INTERVAL_SECONDS = 15.0
INTRADAY_MARKET_DATA_RATE_LIMIT_PER_MINUTE = 600  # 玉山行情官方日內行情額度
ORDER_UNKNOWN_ALERT_SECONDS = 30.0
ORDER_SUBMIT_WAIT_SECONDS = 5.0  # 主迴圈最多等待送單結果；逾時不代表拒單。
EXIT_RETRY_COOLDOWN_SECONDS = 15.0  # 每檔平倉失敗／SDK 忙碌後的共用冷卻時間。
ENTRY_MODE_NO_TRADE = 0
ENTRY_MODE_AVG = 3
STOCK_REST_REQUEST_INTERVAL_SECONDS = 0.125
MARKETDATA_HTTP_TIMEOUT_SECONDS = 5.0  # 日內行情 HTTP 逾時預算（秒）；分K、即時報價及 ticker 共用
MARKETDATA_PRIORITY_POSITION = 0
MARKETDATA_PRIORITY_INDEX = 5
MARKETDATA_PRIORITY_CANDIDATE = 10
MARKETDATA_PRIORITY_SCAN = 50
