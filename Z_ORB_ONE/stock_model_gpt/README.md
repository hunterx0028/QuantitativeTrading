# stock_model_gpt

以台股日 K 離散狀態訓練的輕量 causal Transformer。每個 timestep 代表一個交易日；模型用歷史狀態預測下一交易日的價格五分類、盤中觸漲停、盤中觸跌停，以及收盤 `U/N/D`。成交量只作輸入，不作輸出目標。

## 模組

- `update_data.py`：讀取 `Z_ORB_ONE/stock_data.py` 的 `selected_stocks`、保存每日清單快照、登入玉山 SDK 並增量更新日 K，同時增量同步 FinMind 公司行動資料。程式刻意不呼叫 logout。
- `finmind.py`：匿名或使用可選 `FINMIND_TOKEN` 查詢除權息結果；另保留付費公司行動資料的選用介面。
- `prepare_features.py`：將 OHLCV 轉成議定的每日狀態。
- `model.py`：無股票代號 embedding 的第一版 causal Transformer、多輸出頭。
- `train_initial.py`：由隨機權重訓練初始模型。
- `train_daily.py`：載入前一 checkpoint，以較小學習率繼續訓練。
- `predict.py`：保存下一交易日各分類的完整機率。
- `run_daily.py`：串接每日更新、特徵、訓練與預測。

## 資料表示

每日輸入為：

```text
(price, hit_up, hit_down, close_limit, volume)
```

- `price`: `-2,-1,0,1,2`，以當日收盤價相對當日交易參考價分箱。
- `hit_up`, `hit_down`: 可同時為真，使用實際價格與台股升降單位計算。
- `close_limit`: `U,N,D`。
- `volume`: 相對此前20個有效日成交量中位數的 `-2,-1,0,1,2`；零量或無有效基準為 `X`。

原始 K 棒保存在 `data/candles`，衍生狀態保存在 `data/features`，每日股票清單快照保存在 `data/universe`。這些執行期資料不納入 Git。

## 執行順序

從專案根目錄執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.prepare_features --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-03 --prediction-date 2026-09-04
```

之後每日可執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-07 --prediction-date 2026-09-08
```

請只在收盤且 API 已提供完整日 K 後執行。`prediction-date` 必須由交易日曆或操作者提供，程式不把曆日的明天誤認為交易日。

`--as-of` 是強制的資料時間邊界：特徵及訓練目標都只會使用該日以前的資料。預測程式也會拒絕載入訓練截止日晚於 `--universe-date` 的 checkpoint，防止本地快取已有未來日 K 時發生資訊洩漏。

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
