import json
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo  # Python 3.9+
from pathlib import Path
import re

# ================ 對沖策略參數設定 ===================
# 資料區間設定
during_start_data = '2024-06-03' # 2025-10-20
during_end_data = '2025-12-28' # 2025-11-16

# 四個月
train_start_data = '2025-07-28'
train_end_data = '2025-11-30'

# 一個月
test_start_data = '2025-12-01'
test_end_data = '2025-12-28'
# ================ 對沖策略參數設定 ===================

# ================ 當沖策略參數設定 ===================
start_min_number = 3  # 4 代表取5根的資料 (09:00~09:04)，要在 09:03:50 按啟動 / 3 代表取4根的資料 (09:00~09:03)，要在 09:02:50 按啟動

tp_distance_threshold = 0.25  # 停利距離閾值，開盤最高最低價差的倍數，若以原始ORB，應為1。
stop_distance_threshold = 0.3 # 停損距離閾值，開盤最高最低價差的倍數，若以原始ORB，應為1。

invest_qty = 2 # 每次投資股數
buffer_range = 2  # 開盤後須預留漲跌區間緩衝倍數
base_profit = 0.05 #因為會乘上1000，等於起碼要賺50元

ETF_CODE = ["24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "45", "46", "47"]
EXCLUDE_INDUSTRY_CODE = ["15", "17", "10"] # 15航運業 17金融保險業 10鋼鐵工業
# ================ 當沖策略參數設定 ===================

# ANSI 顏色碼
RED = "\033[31m"
GREEN = "\033[32m"
BLUE  = "\033[34m"
RESET = "\033[0m"
YELLOW = "\033[33m"

def find_project_root(start: Path | None = None) -> Path:
    p = (start or Path(__file__)).resolve()
    for parent in [p, *p.parents]:
        if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
            return parent
    # 找不到就退而求其次：使用目前檔案的上層
    return p.parent

# PROJECT_ROOT = find_project_root() # 專案根目錄
# DATA_DIR = PROJECT_ROOT/"data"   # 例：專案根目錄底下 data 資料夾

def get_trade_date():
    tz = ZoneInfo("Asia/Taipei")
    now = datetime.now(tz)
    cutoff = time(13, 30)
    return (now + timedelta(days=1) if now.time() > cutoff else now).strftime('%Y-%m-%d')
