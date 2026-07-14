# 手動新增資料夾
"image",
"papers",
"stocks",
"Z_ORB_ONE/analysis_strategy/analysis_json_cache",
"Z_ORB_ONE/daily_report/pdf_folder",
"Z_ORB_ONE/stock_state"

# 在 Plugins 搜尋並安裝 GitHub Copilot
# 啟動 JetBrains AI 並選擇 Codex

# 換電腦時須安裝的套件
python.exe -m pip install --upgrade pip

# 交易SDK, 版本 2.2.0
pip install esun_trade-2.2.0-cp37-abi3-win_amd64.whl

# 行情SDK, 版本 2.2.0
pip install esun_marketdata-2.2.0-cp37-abi3-win_amd64.whl

pip install yfinance

pip install pandas
pip install pandas-stubs

pip install statsmodels

pip install Pillow
pip install opencv-python
pip install easyocr

pip install schedule

pip install pyarrow
pip install fastparquet

pip install matplotlib


# 換電腦時可能需要在 pycharm 的 Termianl 下切換到 esunTradeAPI 資料夾，執行 esunAPILogin.py
cd esunTradeAPI
python esunAPILogin.py

Enter esun account password:
am1PumpKin
Enter cert password:
262052


行情API連線限制
-日內行情 (Intraday Market Data): 每分鐘 600 次。
-行情快照 (Market Snapshot): 每分鐘 600 次。
-歷史行情 (Historical Market Data): 每分鐘 60 次。
-WebSocket: 訂閱數上限 300，連線數上限 2。

交易API連線限制與異常行為規範
-委託下單: 若每日委託下單筆數（證券/期貨）超過 2,000 筆上限。
-登入/登出: 每分鐘登入/登出超過 10 次，或每日登入/登出超過 300 次。
-連線負載: 造成玉山證券連線伺服器 CPU 使用率超過 85%。
-委託速率: 每秒委託筆數（含取消）超過 20 筆。

#更新電腦時間：以管理員身份執行命令提示字元，輸入以下指令
w32tm /resync /nowait