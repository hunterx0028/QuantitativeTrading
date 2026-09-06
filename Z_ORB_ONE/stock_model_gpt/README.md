# stock_model_gpt

以台股日 K 離散狀態訓練的輕量 causal Transformer。每個 timestep 代表一個交易日；模型用歷史狀態預測下一交易日的價格五分類、盤中觸漲停、盤中觸跌停。收盤 `U/N/D` 與成交量只作輸入，不作輸出目標。

## 模組

- `update_data.py`：讀取 `Z_ORB_ONE/stock_data.py` 的 `selected_stocks`、保存每日清單快照、登入玉山 SDK 並增量更新日 K，同時增量同步 FinMind 公司行動資料。程式刻意不呼叫 logout。
- `finmind.py`：匿名或使用可選 `FINMIND_TOKEN` 查詢除權息結果；另保留付費公司行動資料的選用介面。
- `prepare_features.py`：將 OHLCV 轉成議定的每日狀態。
- `model.py`：無股票代號 embedding 的第一版 causal Transformer、三輸出頭。
- `train_initial.py`：由隨機權重訓練初始模型。
- `train_daily.py`：載入前一 checkpoint，以較小學習率繼續訓練。
- `predict.py`：保存下一交易日各分類的完整機率。
- `validate_predictions.py`：用已發生交易日的實際日 K 驗證預測結果，並標出重點訊號。
- `post_training_gate.py`：彙整已驗證的重點訊號交易結果，打印門檻與方向建議。
- `run_daily.py`：串接特徵產生、每日續訓與預測；資料更新由前一步 `update_data.py` 負責。

## 資料表示

每日輸入為：

```text
(price, hit_up, hit_down, close_limit, volume)
```

- `price`: `-2,-1,0,1,2`，以當日收盤價相對當日交易參考價分箱。
- `hit_up`, `hit_down`: 可同時為真，使用實際價格與台股升降單位計算。
- `close_limit`: `U,N,D`。
- `volume`: 相對此前20個有效日成交量中位數的 `-2,-1,0,1,2`；零量或無有效基準為 `X`。

模型預測下一交易日的前三項交易核心目標：

```text
(price, hit_up, hit_down)
```

`close_limit` 與 `volume` 只作為歷史輸入特徵，不作為輸出目標。

預設 loss 權重偏向當沖觸發條件：

```text
price=2.0, hit_up=4.0, hit_down=4.0
```

也就是收盤價格分類仍參與訓練，但盤中觸漲停/觸跌停是較高權重的主任務。

原始 K 棒保存在 `data/candles`，衍生狀態保存在 `data/features`，每日股票清單快照保存在 `data/universe`。這些執行期資料不納入 Git。

## 執行順序

`--as-of` 是強制的資料時間邊界，應填「最後一個已有完整日 K 的交易日」，不是執行程式當天的日期；特徵及訓練目標都只會使用該日以前的資料。`prediction-date` 必須由交易日曆或操作者提供，程式不把曆日的明天誤認為交易日。預測程式也會拒絕載入訓練截止日晚於 `--universe-date` 的 checkpoint，防止本地快取已有未來日 K 時發生資訊洩漏。

從專案根目錄執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.prepare_features --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-03 --prediction-date 2026-09-04
```

`predict.py` 會在保存 JSON 後，同步於控制台列出符合重點訊號門檻的標的。四個門檻預設都是 `0.6`：

```text
LONG: hit_up.T >= long_hit_threshold 或 price.2 >= long_price_threshold
SHORT: hit_down.T >= short_hit_threshold 或 price.-2 >= short_price_threshold
```

`--signal-threshold` 可一次設定四個門檻；也可用 `--long-hit-threshold`、`--long-price-threshold`、`--short-hit-threshold`、`--short-price-threshold` 分別調整。輸出中的 `reason=hit/price/both` 代表該方向是由觸及機率、價格分類機率，或兩者同時觸發。同方向同股票只會打印一筆訊號。

### 驗證已發生的預測

預測日期已經發生後，可驗證預測結果：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-04
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-04
```

驗證前需先讓本地 `data/candles` 含有該預測日的實際日 K；驗證程式會讀取 `predictions/2026-09-04.json`，從本地 `data/candles` 擷取 2026-09-04 實際日 K，另存至 `data/actual_candles/2026-09-04.jsonl`，並在控制台打印前三項各自的命中率與重點訊號實際結果。重點訊號會列出 `actual_hit`、`actual_price` 與實際日 K 的 `O/H/L/C`。驗證程式會直接由實際 K 棒重算該日狀態，不需要先重跑 `prepare_features`。

重點訊號的交易成功定義預設為開盤進場後，最佳順向價差至少 `3%`，且最大逆向價差不超過 `2%`。LONG 使用 `high/open` 與 `low/open` 評估；SHORT 使用 `open/low` 與 `high/open` 評估。門檻可用 `--target-profit-pct` 與 `--max-adverse-pct` 調整。驗證結果會保存至 `data/evaluations/YYYY-MM-DD.json`，作為 post-training gate 的資料來源。

### Post-Training Gate

每個已預測交易日收盤後，先執行驗證並累積 evaluation；累積數個交易日後再看 gate 建議，不建議只根據單日結果調參。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.post_training_gate
```

`post_training_gate.py` 預設讀取最近 20 個已驗證交易日，並額外打印最近 5 日概況。它會依 LONG/SHORT 分開統計重點訊號的成功率、平均最佳順向價差、平均收盤價差與平均逆向價差，給出 `KEEP`、`RAISE_THRESHOLD_OR_PAUSE`、`KEEP_OR_LOWER_SLIGHTLY` 等建議。目前 gate 只打印建議，不會自動修改 `settings.json`、模型 checkpoint 或訊號門檻。

### GPU 確認

訓練與預測會在啟動時印出實際使用的裝置，例如：

```text
device=cuda:0 NVIDIA GeForce RTX 4050 Laptop GPU (...)
```

若顯示 `device=cpu (目前 PyTorch 是 CPU build...)`，代表硬體雖然有 NVIDIA GPU，但目前虛擬環境安裝的是 CPU 版 PyTorch，訓練不會用到 GPU。可先確認：

```powershell
python -c "import torch; print(torch.__version__); print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else None)"
```

若是 `+cpu` 且 `False`，請將目前環境的 PyTorch 換成 CUDA build。依實際 PyTorch 官方頁面選擇與本機 driver 相容的 CUDA wheel；完成後重新跑上面的確認指令，看到 `True` 與 GPU 名稱才代表程式會走 CUDA。CUDA 可用時，訓練會自動使用 pinned memory、non-blocking transfer 與 AMP mixed precision；CPU 環境則維持原本流程。

### 日常更新

每日更新前，請先更新 `Z_ORB_ONE/stock_data.py` 的 `selected_stocks` 清單；`update_data.py` 會依當下清單產生 universe snapshot 並增量更新日 K，已存在的日期不會重複抓取。接著再執行 `run_daily.py` 產生特徵、續訓與預測。`--as-of` 填最後一個完整交易日，`--prediction-date` 填下一個要交易的交易日。

例如目前日期是 2026-09-06，但 2026-09-05 與 2026-09-06 是假日，最後一個完整交易日是 2026-09-04，要預測下一個交易日 2026-09-07，應執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-04
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-04 --prediction-date 2026-09-07
```

日常訓練只讀取 `recent_universe_days` 期間內曾出現在清單快照的股票；更舊股票的本地資料不會刪除，重新入選時可補齊缺口。預測則嚴格限定在 `--universe-date` 的 Active 清單。

玉山若回傳 OHLC 含 `null`、非正價格或最高價低於最低價的歷史列，更新程式會顯示 `[WARN]` 並略過；不會以0補成假行情。成交量單獨為空時則保存為0，特徵化後標記為 `X`。

## 除權息及特殊參考價

玉山 historical candles 提供原始 OHLCV 與 `change`。一般交易日先以 `close - change` 推算參考價；除權息日由 FinMind 公布資料覆蓋：

- `TaiwanStockDividendResult`：除權除息結果與參考價，免費流程預設啟用。

`TaiwanStockCapitalReductionReferencePrice`、`TaiwanStockSplitPrice`、`TaiwanStockParValueChange` 的全市場查詢可能要求 FinMind 付費會員，預設不啟用。若日後具備相應權限，可將 `settings.json` 的 `finmind_extended_corporate_actions` 改為 `true`。未啟用時，這些日期仍使用玉山 `close - change` 推算的交易參考價。

FinMind Token 不是必填；程式預設匿名存取。若日後需要較高流量，在本機設定環境變數 `FINMIND_TOKEN`，不要將 Token 寫入程式、README 或 Git。

FinMind 的 `TaiwanStockPriceAdj` 屬 backer/sponsor 會員資料，因此第一版不依賴它。本模型只保存離散日狀態；有正確的每日交易參考價與實際漲跌停價，即可避免把除權息、減資或分割誤判為行情漲跌。

## 尚待實驗而非寫死的項目

- 玉山個股最早自2010年回溯，每次請求切為365曆日以內。
- 60/120/240 日 context 比較。
- 依日期切割的 walk-forward 驗證與候選模型發布門檻。
- rare-event 類別權重、recency sampling、Active/Recent/Archived replay 比例。
- 第二版是否加入受限制的股票 embedding；第一版準確時不必加入。
