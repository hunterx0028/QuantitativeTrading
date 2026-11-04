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

# 目前專案程式直接使用的套件；其餘相依套件由 pip 自動安裝
pip install numpy matplotlib pytz requests urllib3 keyring


# 換電腦時可能需要在 pycharm 的 Termianl 下切換到 esunTradeAPI 資料夾，執行 esunAPILogin.py
cd esunTradeAPI
python esunAPILogin.py

Enter esun account password:
am1PumpKin
Enter cert password:
262052


日內行情：600 / min
WebSocket：300 訂閱數；2 連線數
行情快照：600 / min
歷史行情：60 / min

交易API連線限制與異常行為規範
-委託下單: 若每日委託下單筆數（證券/期貨）超過 2,000 筆上限。
-登入/登出: 每分鐘登入/登出超過 10 次，或每日登入/登出超過 300 次。
-連線負載: 造成玉山證券連線伺服器 CPU 使用率超過 85%。
-委託速率: 每秒委託筆數（含取消）超過 20 筆。

#更新電腦時間：以管理員身份執行命令提示字元，輸入以下指令
w32tm /resync /nowait