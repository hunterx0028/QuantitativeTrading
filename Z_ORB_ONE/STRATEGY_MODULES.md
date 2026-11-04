# 策略模組與 Docker 打包

本機與 Docker 現在使用同一份交易主流程。拆分只調整程式組織，策略判斷及共用委託流程維持既有規則。

## 檔案責任

| 檔案 | 責任 |
| --- | --- |
| `execute_strategy_broken_high_falling_singleton_v17.py` | 共用入口 `main()`、市場分流、股票歸屬、行情限流、下單、撤單、成交回報、對帳與共用持倉風控 |
| `strategies/fall.py` | FALL 訊號、候選註冊、當日狀態、篩選排程、報價輪詢與監控流程 |
| `strategies/avg.py` | AVG 股票池副本、設定驗證、分 K 解析、斜率、四階段訊號、候選與持倉報價排程 |
| `strategies/candidate_flow.py` | AVG、FALL 共用背景分 K、當日歸屬、即時報價及整輪耗時紀錄 |
| `runtime_environment.py` | 本機檔案／S3 準備、延後載入股票池、互動／非互動登入及登出清理 |
| `marketdata_transport.py` | 日內行情 HTTP 連線及讀取逾時；需與主程式放在同一層 |
| `docker_space/generate_docker_strategy.py` | 檢查語法、同步共用程式與策略檔案、產生小型 Docker 入口 |

各策略透過明確的設定、當日狀態及共用服務介面取得依賴，不反向匯入主程式。共用交易狀態及鎖仍由主程式協調。

實機只執行 FALL、AVG，`STRATEGY_PRIORITY` 預設為 `["FALL", "AVG"]`。為保留原本調參習慣，`FALL_*`、`AVG_*` 等參數仍在主程式。策略接收目前參數的快照；當日狀態則共用同一份 runtime。

FALL 僅依 `FALL_ENTRY_CONFIRM_BARS` 連續確認根數判斷。只計訊號窗口結束後、low 嚴格低於昨收的完整分K；單根跌幅再大也不能提前確認。

FALL 已取消固定百分比停損及其入場空間篩選。確認成交後，持倉每分鐘繼續查詢分K，從首次成交確認時間所在分鐘的下一根起，僅檢查查詢開始時已完成的棒。若 high > 該根有效 average，記錄均價上方第一個合法 tick 為參考價並走既有市價平倉流程；實際成交以券商回報為準。缺少 average 不補算，行情查詢失敗會在後續輪次重試；獲利保護、追蹤停利、統一平倉及委託防重仍有效。`FALL_AVERAGE_STOP_TRIGGERED` 記錄觸發資料，`FALL_AVERAGE_STOP_ERROR` 記錄查詢或處理失敗。

`stock_data.py` 不會在主程式或策略模組匯入時被讀取。只有呼叫 `main()` 後，完成環境準備才載入股票池。仍維持每日只啟動一次、不支援盤中恢復的原有前提。

## 本機執行

在 `Z_ORB_ONE` 目錄沿用原本的執行方式：

```powershell
python execute_strategy_broken_high_falling_singleton_v17.py
```

預設模式為 `local`，SDK 保留原本互動登入方式。`config.ini` 從主程式所在目錄讀取；本機憑證位置仍依 SDK 的設定處理。

## Docker 打包與部署

先在 `Z_ORB_ONE` 執行：

```powershell
python docker_space/generate_docker_strategy.py
```

產生器會將以下整套程式同步到 `docker_space/`：

```text
execute_strategy_broken_high_falling_docker_v17.py     # 小型入口
execute_strategy_broken_high_falling_singleton_v17.py  # 同一份交易主流程
runtime_environment.py
marketdata_transport.py
strategies/
    __init__.py
    fall.py
    avg.py
    candidate_flow.py
```

**必須將這整套程式放進實際 Docker build 目錄，不能只複製 Docker 入口或 strategies。** 若實際 Dockerfile 在其他目錄，請把上述檔案一起複製過去。`external/`、`wheels/`、`requirements.txt` 沿用原本配置。

既有 Dockerfile 的 `COPY . .` 及下列 CMD 可以維持：

```dockerfile
CMD ["python", "execute_strategy_broken_high_falling_docker_v17.py"]
```

Docker 入口只會呼叫共用的 `main(runtime_mode="docker")`。Docker 模式沿用 `QUANT_S3_BUCKET`、`QUANT_S3_PREFIX`、`QUANT_DOWNLOAD_FROM_S3`、`ESUN_PASSWORD`、`CERT_PASSWORD` 設定。

確認 `.dockerignore` 未排除共用主程式、環境模組或策略目錄後，在 Dockerfile 所在目錄沿用原本的 build、ECR 登入、tag、push 指令。

每次修改主程式或策略，都先重新執行產生器。不要直接修改 `docker_space/` 內的程式副本。

## 離線驗證

在專案根目錄使用專案 Python 執行：

```powershell
python -B -m unittest discover -s Z_ORB_ONE/tests -p "test_*.py"
```

測試使用假 SDK、行情與委託服務，不登入券商、不下單、不存取真實 S3。完整入口測試驗證本機與 Docker 共用啟動及清理流程；這不等同於券商 SDK 或 Linux 容器的實際整合驗證。
