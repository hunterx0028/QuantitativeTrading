# stock_model_gpt

以台股日 K 六項離散狀態訓練的輕量 causal Transformer，預測下一交易日價格分類、盤中觸漲停與觸跌停。

操作順序：**首次執行第 4 節；之後每個交易日收盤後依序執行第 5、6 節；每隔約 20 個已驗證交易日執行第 7 節。** 首次訓練前可先依第 3 節確認 GPU。所有指令均從專案根目錄執行，範例日期請替換為自己的完整交易日與下一預測交易日。

| 章節 | 查找內容 |
|---|---|
| 1. 模組說明 | 各程式用途 |
| 2. 資料表示 | 六項輸入、ATR、預測輸出與訊號 |
| 3. GPU確認 | 確認實際訓練裝置 |
| 4. 初始訓練設定 | 從頭重置（選用）、更新資料、自動決定刻度、初始訓練與首次預測 |
| 5. 驗證結果程序 | 取得實際行情、驗證先前預測、recall/precision、checkpoint gate 判定 |
| 6. 每日更新設定 | 新增序列、歷史重播（含稀有事件加權）、續訓與下次預測 |
| 7. Post-Training Gate | 定期查看 signals 統計與建議 |
| 8. 除權息及特殊參考 | 公司行動資料來源與重新同步 |
| 9. 尚待實驗而非固定項目 | 後續實驗方向 |
| 10. Walk-forward 回測（選用工具） | 隔離環境下驗證訓練設定，不影響正式資料 |

## 1. 模組說明

測試程式集中在 `tests/`，不參與初始訓練或每日流程。修改程式後可從專案根目錄執行：

```powershell
python -m pytest Z_ORB_ONE/stock_model_gpt/tests -q
```

- `reset_runtime_data.py`：清空所有可重建的執行期資料（candles/features/checkpoints/predictions/evaluations/universe/atr_analysis/signal_reports），從頭重新開始用；不會動到 `config.ini`、`stock_data.py`、`settings.json` 或原始碼。預設乾跑只列出、不刪除，加 `--yes` 才會真的刪除。
- `update_data.py`：讀取 `Z_ORB_ONE/stock_data.py` 的 `selected_stocks`、保存每日清單快照、登入玉山 SDK 並增量更新日 K，同時增量同步 FinMind 公司行動資料。程式刻意不呼叫 logout。
- `finmind.py`：匿名或使用可選 `FINMIND_TOKEN` 查詢除權息結果；另保留付費公司行動資料的選用介面。
- `resync_corporate_actions.py`：忽略既有同步狀態，重新同步並覆寫 FinMind 公司行動快取。
- `prepare_features.py`：將 OHLCV 轉成議定的每日狀態。
- `atr_calibration.py`：初始訓練擬合五級界線並保存分析報告；每日續訓只繼承 checkpoint 界線，不分析分布或產生新報告。
- `model.py`：無股票代號 embedding 的第一版 causal Transformer、三輸出頭。
- `train_initial.py`：由隨機權重訓練初始模型。
- `train_daily.py`：載入前一 checkpoint，以新增目標序列加部分歷史重播續訓；可選全部歷史模式。
- `predict.py`：保存下一交易日各分類的完整機率。
- `validate_predictions.py`：用實際日 K 驗證三項分類預測與 `signals`，不評估 TXT 的方向參考訊號。
- `post_training_gate.py`：只彙整 `signals` 交易結果，打印門檻建議；忽略舊檔案中的方向訊號欄位。
- `run_daily.py`：串接資料更新、特徵產生、每日續訓與預測；ATR 套用初始模型的固定刻度。
- `checkpoint_gate.py`：每次 `validate_predictions` 完自動判定近期 `signals` 成功率是否明顯退化；退化時 `predict.py` 在沒有明確指定 `--checkpoint` 的情況下會拒絕自動選用最新 checkpoint。
- `walk_forward_backtest.py`：選用工具，粗粒度 walk-forward 回測（每個 fold 只訓練一次、之後凍結預測驗證多個交易日），所有輸出隔離在獨立目錄，不影響正式資料。

## 2. 資料表示

每日輸入為：

```text
(price, hit_up, hit_down, close_limit, volume, ATR)
```

- `price`: `-2,-1,0,1,2`，以當日收盤價相對當日交易參考價分箱。
- `hit_up`, `hit_down`: 可同時為真，使用實際價格與台股升降單位計算。
- `close_limit`: `U,N,D`。
- `volume`: 相對此前20個有效日成交量中位數的 `-2,-1,0,1,2`；零量或無有效基準為 `X`。
- `ATR`: 固定 14 日、依交易參考價調整尺度的 Wilder ATR。特徵檔保存當日價格尺度的 `atr` 與 `atr_ratio = atr / 當日收盤價`；送入模型前依該模型的四個百分比界線分成 `0,1,2,3,4` 五級。等於界線時進入較高級，模型不接收連續 ATR。

ATR 使用當日原始 high/low 與交易參考價，先換算歷史波動的價格尺度，再套用 [Wilder 平滑公式](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/atr)：

```text
factor = 當日交易參考價 / 前日原始收盤價
TR = max(high-low, abs(high-當日交易參考價), abs(low-當日交易參考價))
ATR_t = (13 * ATR_{t-1} * factor + TR) / 14
```

交易參考價優先使用 FinMind 公司行動修正值，其次為玉山 `close - change`；缺少參考價時回退前日收盤價。參考價或前收盤價非有限正數時報錯。
一般日參考價等於前收盤價，factor 為 1；除權息、減資、分割及面額變更日自動換算。例如 1 拆 2 時前日 ATR 乘 0.5，排除機械價格跳空，同時保留當日相對參考價的實際波動。
第一根 K 只提供前收盤價；第 15 根 K 才有 14 筆 TR，取其平均作為首個 ATR。暖機期間遇公司行動，也會先將已累積的 TR 換算為當日尺度。已產生的過去日期 ATR 不會因未來公司行動回頭改值。
本次架構調整後先重建特徵並訓練模型；之後每日新資料滾入時，`prepare_features` 自動按相同規則計算，無須因新除權息事件額外從頭重訓。原本每日續訓可照常進行。
正確調整以參考價資料已同步為前提；若公司行動資料延遲或修訂，可執行 `resync_corporate_actions` 後重跑 `prepare_features`，重算事件日起的遞迴 ATR，並視需要重算 evaluation。
計算只使用當日及以前的 K 棒；不足 ATR 或成交量暖機期的日期不產生狀態。預設 20 日暖機與 120 日 context 維持不變。

六項都使用分類 embedding，ATR 使用 `Embedding(5, d_model)`；相加後送入 causal Transformer。
訓練與預測的輸入 tensor 均為 `[batch, days, 6]` 的 torch.long。`atr_ratio` 只供分析及分級使用，Dataset 與預測共用 `encode_state` 分級，不在共用特徵檔寫死某一模型的刻度，以免不同 checkpoint 的界線互相污染。

模型預測下一交易日的前三項交易核心目標：

```text
(price, hit_up, hit_down)
```

`close_limit`、`volume` 與 `ATR` 只作為歷史輸入特徵，不作為輸出目標。

預設 loss 權重偏向當沖觸發條件：

```text
price=2.0, hit_up=4.0, hit_down=4.0
```

也就是收盤價格分類仍參與訓練，但盤中觸漲停/觸跌停是較高權重的主任務。

`hit_up`/`hit_down` 是稀有事件（多數交易日都不會觸及），只靠上面的任務層級權重不足以處理類別內部的正負樣本不平衡，因此另外疊加兩層機制（`price` 不受影響，仍是一般 `CrossEntropyLoss`）：

- **Class weight**：每次訓練（初始或每日續訓）依當次實際訓練集裡 `hit_up`/`hit_down` 的正負樣本比例，自動算出 inverse-frequency 權重，不是固定值。
- **Focal loss**（`focal_gamma`，預設 `2.0`）：取代 `hit_up`/`hit_down` 原本的 `CrossEntropyLoss`，公式為 `loss = -(1-p_t)^γ * log(p_t)`，讓模型已經很有把握答對的樣本梯度貢獻變小，聚焦在難分的稀有正樣本上。`γ=0` 時等同沒有 class weight 的普通 `CrossEntropyLoss`。

這兩層機制沒有上限保護；如果實際正樣本比例極低（例如 <1%），算出的 class weight 可能到十幾甚至上百倍，訓練時建議留意每個 epoch 印出的 loss 有沒有異常震盪。

原始 K 棒保存在 `data/candles`，衍生狀態保存在 `data/features`，每日股票清單快照保存在 `data/universe`。這些執行期資料不納入 Git。

### 預測輸出與訊號

`predict.py` 會保存 JSON，機率欄位仍是數字，但小機率不使用科學記號，方便目視檢查；保存後會同步於控制台列出符合訊號門檻或方向訊號門檻的標的，並將同一批訊號摘要另存至 `signal_reports/YYYY-MM-DD.txt`。訊號門檻代表較極端的當沖機會，預設門檻都是 `0.6`：

```text
LONG: hit_up.T >= long_hit_threshold 或 price.2 >= long_price_threshold
SHORT: hit_down.T >= short_hit_threshold 或 price.-2 >= short_price_threshold
```

方向訊號僅保留於 `signal_reports` TXT 與對應控制台輸出供參考，不寫入預測 JSON、不參與驗證或 gate。其預設門檻也是 `0.6`：

```text
LONG: price.1 + price.2 >= long_direction_threshold
SHORT: price.-1 + price.-2 >= short_direction_threshold
```

`predict` 的 `--signal-threshold` 可一次設定全部六個門檻；也可用 `--long-hit-threshold`、`--long-price-threshold`、`--short-hit-threshold`、`--short-price-threshold`、`--long-direction-threshold`、`--short-direction-threshold` 分別調整，其中方向門檻僅影響參考 TXT。`validate_predictions` 僅接受前四種訊號門檻，後續評估與調整只針對 `signals`。訊號輸出中的 `reason=hit/price/both` 代表該方向是由觸及機率、價格分類機率，或兩者同時觸發。同方向同股票只會打印一筆訊號。

## 3. GPU確認

訓練與預測會在啟動時印出實際使用的裝置，例如：

```text
device=cuda:0 NVIDIA GeForce RTX 4050 Laptop GPU (...)
```

若顯示 `device=cpu (目前 PyTorch 是 CPU build...)`，代表硬體雖然有 NVIDIA GPU，但目前虛擬環境安裝的是 CPU 版 PyTorch，訓練不會用到 GPU。可先確認：

```powershell
python -c "import torch; print(torch.__version__); print(torch.cuda.is_available()); print(torch.cuda.get_device_name(0) if torch.cuda.is_available() else None)"
```

若是 `+cpu` 且 `False`，請將目前環境的 PyTorch 換成 CUDA build。依實際 PyTorch 官方頁面選擇與本機 driver 相容的 CUDA wheel；完成後重新跑上面的確認指令，看到 `True` 與 GPU 名稱才代表程式會走 CUDA。CUDA 可用時，訓練會自動使用 pinned memory、non-blocking transfer 與 AMP mixed precision；CPU 環境則維持原本流程。

## 4. 初始訓練設定

本節理論上只會執行一次：完成初始訓練後，之後每個交易日都是走第 5、6 節的驗證與續訓流程，不會回頭重跑本節。如果要重新從頭開始（例如想丟棄舊的執行期資料、或架構/資料有重大變更想乾淨重來），先用 `reset_runtime_data.py` 清空舊資料再往下走：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.reset_runtime_data --yes
```

這一步只清 `data/candles`、`data/features`、`checkpoints/` 等執行期資料，不會動到 `config.ini`、`stock_data.py`、`settings.json`。不確定要不要清時，先不加 `--yes` 執行一次看預覽，確認範圍後再加 `--yes` 重跑。

初始流程為「更新資料 → 產生調整後 ATR → 自動分析並決定五級界線 → 分級 → 初始訓練」。無需另外手動執行分析程式。
`train_initial` 預設使用訓練期間 ATR% 的 P20/P40/P60/P80 擬合四個界線；只納入訓練股票範圍內且有足夠 context 與目標日的股票，排除 `--as-of` 之後資料。界線不能用驗證期決定。分位數含零或重複時，明確警告並回退到固定 `1/2/3/5%`，不保證各級筆數均衡。

例如訓練期截止 2026-09-03、2026-09-04 起保留驗證，從專案根目錄依序執行（請替換為自己的訓練截止日）：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.prepare_features --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-03 --prediction-date 2026-09-04
```

ATR 界線一律由初始訓練自動產生，保存至 checkpoint；不提供 CLI 或 settings.json 的手動覆寫。

只有初始訓練產生 `data/atr_analysis/initial_fit_<as-of>_<timestamp>.json`，記錄所用界線、來源、初始擬合日期、整體／逐年／當日占比。每日 checkpoint 繼承原有刻度及初始報告路徑，不新增報告。

### 日期設定

`--as-of` 是強制的資料時間邊界，應填「最後一個已有完整日 K 的交易日」，不是執行程式當天的日期；特徵及訓練目標都只會使用該日以前的資料。`prediction-date` 必須由交易日曆或操作者提供，程式不把曆日的明天誤認為交易日。預測程式也會拒絕載入訓練截止日晚於 `--universe-date` 的 checkpoint，防止本地快取已有未來日 K 時發生資訊洩漏。

## 5. 驗證結果程序

每個交易日收盤後，先更新本次清單並取得完整行情，驗證先前對該日保存的預測。以下範例銜接第 4 節產生的 2026-09-04 預測：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-04
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-04
```

完成後接著執行第 6 節；若尚無該日預測檔，當天不執行此驗證指令。

### 驗證內容與輸出

漲跌訊號與方向訊號皆獨立判定多空門檻：只有一邊達標時輸出 LONG 或 SHORT，兩邊都達標時輸出 `CONFLICT`，兩邊都未達標則無訊號。CONFLICT 表示暫不選交易方向，不表示兩個觸及預測互斥或模型出錯；`hit_up` 與 `hit_down` 仍可同時為真。

衝突記錄保存 `long`、`short` 兩邊的機率及觸發證據。預測 JSON 保存完整 `predictions`、正式訊號 `signals` 與其四個 `signal_thresholds`，不再保存 `direction_signals`。`signal_reports` TXT 的內容與格式維持原樣，包含方向參考及雙向衝突標記。

驗證仍計算所有可驗證股票的三項分類命中率；`signals` 的 CONFLICT 另存至 evaluation 的 `conflicts`，保留實際價格分類、兩個觸及結果及日 K，不選定交易方向、不計算單方向交易成功率。新 evaluation 不含 `direction_signals` 或 `direction_conflicts`。Gate 只處理 `signals` 與 `conflicts`，舊檔案中的方向欄位也會忽略。既有 JSON 不批次改寫，可重跑驗證產生新版 evaluation；此次不需重訓模型，也不修改既有 TXT。

除了命中率（argmax 對答案），evaluation 另外用實際訊號門檻（`long_hit`/`short_hit`）計算 `hit_up`/`hit_down` 的 recall（實際觸及中抓到幾成）與 precision（觸發訊號中真的觸及的比例），存在 `signal_recall_precision` 欄位並同步印在控制台。稀有事件下命中率容易失真（模型永遠猜「不觸及」也能有高命中率），recall/precision 才是判斷訊號品質有沒有改善的依據。

### Checkpoint gate（自動安全煞車）

每次 `validate_predictions` 執行完，會自動彙整最近 `gate_window_days`（預設 20）個已驗證交易日與最近 `gate_short_window_days`（預設 5）日的 `signals` 成功率，寫入 `checkpoints/gate_status.json`：

- 樣本數（`signals` 筆數）不足 `gate_min_signals`（預設 3）時，判定 `INSUFFICIENT_DATA`，不影響任何行為。
- 短窗口成功率比長窗口下降超過 `gate_max_success_rate_drop`（預設 `0.25`，即 25 個百分點）時，判定 `DEGRADED`。
- 其餘情況判定 `OK`。

`predict.py` 在**沒有明確指定 `--checkpoint`** 的自動選檔路徑（也就是 `run_daily` 實際在用的路徑）會檢查這個狀態：`DEGRADED` 時直接報錯拒絕預測，避免不知不覺拿一個表現變差的模型血緣去產生真正的訊號。明確指定 `--checkpoint <路徑>` 永遠不受這個檢查影響。

這不是完整的 A/B 模型比較機制（目前的每日續訓是同一條模型血緣持續更新，沒有辦法在新 checkpoint 上線前先做離線回測），只是偵測「最近訊號表現有沒有明顯變差」的煙霧偵測器，出現 `DEGRADED` 時需要人工檢查訓練或資料是否異常，而不是自動判定該用哪個模型。

驗證前需先讓本地 `data/candles` 含有該預測日的實際日 K；驗證程式會讀取 `predictions/2026-09-04.json`，從本地 `data/candles` 擷取 2026-09-04 實際日 K，另存至 `data/actual_candles/2026-09-04.jsonl`，並在控制台打印前三項各自的命中率及 `signals` 的實際結果。訊號會列出 `actual_hit`、`actual_price` 與實際日 K 的 `O/H/L/C`。驗證程式會直接由實際 K 棒重算該日狀態，不需要先重跑 `prepare_features`。

訊號的交易成功定義預設為開盤進場後，最佳順向價差至少 `3%`，且最大逆向價差不超過 `2%`。LONG 使用 `high/open` 與 `low/open` 評估；SHORT 的最佳順向價差為 `(open-low)/open`，最大逆向價差為 `(high-open)/open`。門檻可用 `--target-profit-pct` 與 `--max-adverse-pct` 調整。驗證結果會保存至 `data/evaluations/YYYY-MM-DD.json`，作為 post-training gate 的資料來源。

特徵產生與實際結果驗證共用 `state_pipeline.load_candle_states`：先依截止日期過濾，再套用公司行動快取後產生狀態，包含上述尺度調整 ATR。實際日 K snapshot 保留原始 OHLC，並包含修正後參考價、漲跌停價及公司行動來源。舊 evaluation 不會自動更新，可重新執行 `validate_predictions` 重算。

### 清單變動與驗證完整性

請在當日收盤、取得完整日 K 後驗證先前保存的預測，再執行第 6 節續訓。`update_data` 目前只抓取當下 `selected_stocks`，若先前預測的股票已退出清單且缺少實際行情，驗證會警告並跳過；請確認 `驗證筆數 / 預測筆數`，缺漏不代表已完成全部驗證。目前沒有自動補抓退出股票或補驗的流程。

驗證預設以 0.6 門檻重新偵測 `signals`，尚未自動沿用預測 JSON 保存的門檻；若當時預測使用不同門檻，驗證時需傳入相同的四項訊號門檻。重新驗證會覆寫同日期 evaluation，不會重新訓練模型或改動 TXT。

### 重產舊日期預測

若只是想用目前的新訊號規則重新檢視既有預測，不需要重跑模型，直接驗證該預測日即可：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-04
```

若要重新產生某個舊日期的預測，必須指定當時尚未看過答案的 checkpoint。例如要重產 2026-09-04 預測，checkpoint 的 `training_as_of` 不可晚於 2026-09-03：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.predict --checkpoint <training_as_of_2026-09-03的checkpoint> --universe-date 2026-09-03 --prediction-date 2026-09-04
```

不要用 `run_daily` 回頭跑舊日期；`run_daily` 會載入最新 checkpoint 續訓，若最新模型已看過該預測日資料，會造成資料洩漏或被程式拒絕。

## 6. 每日更新設定

每個交易日收盤後，先確認 `Z_ORB_ONE/stock_data.py` 的 `selected_stocks` 是本次要使用的清單，完成第 5 節驗證，再執行下列指令。範例沿用第 5 節已取得的 2026-09-04 完整行情，預測下一交易日 2026-09-07：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-04 --prediction-date 2026-09-07
```

`run_daily` 依序呼叫 `update_data → prepare_features → train_daily → predict`。第 5 節已執行過 `update_data` 時，這裡仍會再次登入並檢查更新；正常情況已快取的日期不再抓取。兩個階段應使用同一份本次清單，因為更新程式會重寫該日期的 universe snapshot。

每日會計算新資料的 ATR 並套用 checkpoint 固定刻度，不重新決定界線、不分析 ATR 分布、不產生 ATR 報告。續訓會更新模型權重；`run_daily` 目前不包含結果驗證或 gate，仍須分別依第 5、7 節執行。

### 續訓與歷史重播設定

每日續訓預設 `daily_training_mode="incremental_replay"`：新增序列依「目標日」判定，範圍為前一 checkpoint 的 `training_as_of` 之後至本次 `--as-of`，全部納入；輸入仍保留每筆目標日前完整的 context。例如目標為今天，輸入仍是此前 120 日，不是只輸入今天新增的一列。

歷史重播從前次截止日以前的序列抽樣，預設最多為新增筆數的 1 倍（`daily_replay_ratio=1.0`），總上限 4096（`daily_replay_max_sequences`），每股最多 128（`daily_replay_per_symbol`）。這些是初始工程設定，並非已驗證最佳比例。各股抽樣時，`hit_up`/`hit_down` 為真的日子權重是一般日子的 `daily_replay_hit_oversample`（預設 `4.0`）倍（加權不放回抽樣，抽出後會重新洗牌，避免抽樣權重連帶影響後續輪流選入的順序），再輪流選入，避免長歷史股票佔滿重播預算；候選不足不重複補抽。隨機種子由設定 seed 與本次日期決定。此機制只在每日續訓（`train_daily`/`run_daily`）啟動，`train_initial` 不會用到。

新加入股票也依相同日期規則處理：近期目標全部納入，較早歷史進入受單股上限約束的重播池，不會整批強制訓練。股票池仍使用 recent universe 規則，已退出且不在近期池的股票不會額外加入重播。初始訓練仍使用全部可用序列，ATR 刻度及 optimizer 狀態在每日續訓中繼承既有模型。

Checkpoint 的 `sampling` 記錄模式、前後截止日、seed、新增／重播筆數、逐股重播數及每筆抽樣的股票、目標日、輸入起始日；`seen_symbols` 保留此模型分支曾訓練的股票，`parent_checkpoint` 記錄來源。

同一模型已訓練到本次截止日時，預設跳過；相同來源 checkpoint 路徑與截止日的成功結果也會記錄在 `checkpoints/daily_runs`，重跑會直接回傳先前結果。沒有新增目標時預設跳過，不產生新模型。不會只因資料修訂或新增股票就強制重跑同日期；如需明確重做，可加 `--force-retrain`。此紀錄用於循序重跑，請勿同時啟動多個續訓流程。

保留全部歷史模式供對照：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.train_daily --as-of 2026-09-04 --training-mode full_history
```

也可對同一來源做明確對照重跑：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.train_daily --checkpoint <來源模型路徑> --as-of 2026-09-04 --training-mode full_history --force-retrain
```

`full_history` 使用目前股票池的全部截止日前序列；`--force-retrain` 不會放寬未來日期防護。若增量模式完全沒有新增樣本，其重播預算為零，即使 force 也無樣本可訓練，需使用 full_history 才能明確重訓歷史。`run_daily` 同樣支援 `--training-mode` 與 `--force-retrain`；預設不需另外指定。

此版本依全模型截止日區分新舊資料，遲補且目標日不晚於截止日的資料會歸入歷史池，不保證立即抽中；若要完整重學修訂的歷史資料，可使用全部歷史模式。以上只調整每日訓練取樣；自動補驗、待驗證股票抓取與 gate 自動串接仍未納入 `run_daily`。

日常訓練只讀取 `recent_universe_days` 期間內曾出現在清單快照的股票；更舊股票的本地資料不會刪除，重新入選時可補齊缺口。預測則嚴格限定在 `--universe-date` 的 Active 清單。若 `recent_universe_days` 期間內完全找不到清單快照（例如忘記先執行 `update_data`），訓練會直接報錯，不會靜默改用全部本地股票。

玉山若回傳 OHLC 含 `null`、非正價格或最高價低於最低價的歷史列，更新程式會顯示 `[WARN]` 並略過；不會以0補成假行情。成交量單獨為空時則保存為0，特徵化後標記為 `X`。

### 預測資料檢查

預測逐股要求 `input_last_date == universe-date` 且滿足 context 長度。缺特徵檔、截止日前無資料、資料過期或歷史不足的股票會跳過；控制台與訊號報告會列出原因、預期與實際日期及覆蓋數，預測 JSON 保存 `skipped`、`active_count` 與 `predicted_count`。全部不合格時報錯，不寫入或覆蓋預測檔。此檢查不判定當日日 K 是否已收盤，仍須使用完整交易日。

## 7. Post-Training Gate

完成每日第 5 節驗證並累積 evaluation 後，可每隔約 20 個已驗證交易日執行一次本節。也可提早查看，但不建議只根據單日結果調參。這是手動查看統計的頻率，不是每 20 天自動執行的排程。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.post_training_gate
```

`post_training_gate.py` 預設讀取最近 20 個已驗證交易日，並額外打印最近 5 日概況。只針對 `signals` 依 LONG/SHORT 與 `reason=hit/price/both` 統計交易成功率、平均最佳順向價差、平均收盤價差與平均逆向價差。建議會輸出 `KEEP`、`RAISE_THRESHOLD_OR_PAUSE`、`KEEP_OR_LOWER_SLIGHTLY`。目前 gate 只打印建議，不會自動修改 `settings.json`、模型 checkpoint 或訊號門檻。

Gate 是 signals 的統計與建議工具，不會執行續訓、修改模型權重或自動決定候選模型是否發布。區間若含部分驗證的日期，統計只反映有實際資料的訊號。

## 8. 除權息及特殊參考

玉山 historical candles 提供原始 OHLCV 與 `change`。一般交易日先以 `close - change` 推算參考價；公司行動日由 FinMind 公布資料覆蓋：

- `TaiwanStockDividendResult`：除權除息結果與參考價，免費流程預設啟用，逐股查詢。

以下特殊公司行動預設啟用；實測匿名查詢可用，但初次重建歷史快取時 API 量會增加：

- `TaiwanStockCapitalReductionReferencePrice`：減資恢復買賣參考價，免費流程需逐股查詢；全市場查詢需 backer/sponsor。
- `TaiwanStockSplitPrice`：台股分割後參考價，可全市場查詢並在本機快取。
- `TaiwanStockParValueChange`：變更面額恢復買賣參考價，可全市場查詢並在本機快取。

若日後關閉 `settings.json` 的 `finmind_extended_corporate_actions`，減資、分割、面額變更日期會改用玉山 `close - change` 推算的交易參考價。

若要讓既有歷史資料補上 extended 公司行動修正，可先重建公司行動快取，再重建特徵並重新訓練：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.resync_corporate_actions --as-of 2026-09-04 --include-extended
python -m Z_ORB_ONE.stock_model_gpt.prepare_features --as-of 2026-09-04
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-04
```

`resync_corporate_actions.py` 預設使用 `stock_data.py` 目前的 `selected_stocks`；若只要重建部分股票，可加上 `--symbols 2330 2464`。它只處理 FinMind 公司行動資料，不會登入玉山 SDK，也不會重抓日 K。

FinMind Token 不是必填；程式預設匿名存取。若日後需要較高流量，在本機設定環境變數 `FINMIND_TOKEN`，不要將 Token 寫入程式、README 或 Git。

FinMind 的 `TaiwanStockPriceAdj` 屬 backer/sponsor 會員資料，因此第一版不依賴它。價格分類與漲跌停狀態使用每日交易參考價與實際漲跌停價修正；ATR 使用原始 high/low，並依交易參考價換算歷史波動尺度。

## 9. 尚待實驗而非固定項目

- 玉山個股最早自2010年回溯，每次請求切為365曆日以內。
- 60/120/240 日 context 比較。
- ~~依日期切割的 walk-forward 驗證~~：第 10 節已有粗粒度 MVP（每個 fold 只訓練一次），但不模擬每日續訓，測不到 `daily_replay_hit_oversample`；細粒度版本（逐日模擬續訓）與「點時間股票清單重建」（目前用現在的 `selected_stocks` 回填歷史，有 survivorship bias）仍待做。
- ~~候選模型發布門檻~~：第 5 節已有 `checkpoint_gate.py` 的煞車機制，但只是偵測「近期表現有沒有明顯退化」，不是真正的 A/B 模型比較或自動晉升，仍缺乏離線回測驅動的發布決策。
- ~~rare-event 類別權重~~：已實作 class weight（inverse-frequency，自動依當次訓練集算，無上限）+ focal loss（`focal_gamma`）+ 重播抽樣加權（`daily_replay_hit_oversample`），效果尚未經過完整 walk-forward 驗證。
- recency sampling、Active/Recent/Archived replay 比例。
- 第二版是否加入受限制的股票 embedding；第一版準確時不必加入。

## 10. Walk-forward 回測（選用工具）

不屬於日常操作流程，用來在不影響正式環境的前提下，檢驗訓練設定（例如 class weight、focal loss 這類超參數）跨不同歷史區間的表現，而不用只能一天一天等真實新資料累積。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.walk_forward_backtest --output-dir backtests/run1 --min-training-days 500 --test-window-days 20
```

- `--min-training-days`：第一個 fold 至少要有多少交易日歷史才開始訓練（預設 500，約 2 年），依實際快取的資料量調整。
- fold 數量不用指定，程式會掃過 `data/features/*.jsonl` 算出實際可用的交易日曆，自動切出「擴張窗口訓練 + `--test-window-days`（預設 20）個交易日凍結預測驗證，不重疊」的 fold。
- `--max-as-of` 可選，限制不使用晚於此日期的資料。
- `--settings` 可選，傳給內部呼叫的 `train_initial`/`validate_predictions`（不會呼叫 `update_data`，見下方資料隔離說明）。

**粗粒度限制**：每個 fold 只訓練一次（`train_initial`），之後 `--test-window-days` 天都用同一個 checkpoint 凍結預測，不模擬 `train_daily` 的逐日續訓，因此**測不到只在續訓才啟動的機制**（例如 `daily_replay_hit_oversample`）。

**資料隔離**：透過 `STOCK_MODEL_GPT_WRITE_ROOT` 環境變數（見 `paths.py`），把 `checkpoints/`、`predictions/`、`data/evaluations/`、`data/actual_candles/`、`data/universe/`、`data/atr_analysis/`、`signal_reports/` 全部重新導向到 `--output-dir` 底下，不會覆寫或污染正式環境的同名檔案；`data/candles`、`data/features`、`data/corporate_actions`、`stock_data.py` 維持唯讀共用，不重新抓取或修改。不會呼叫 `update_data.py`（會嘗試登入玉山 SDK），股票清單快照改用 `universe.py` 的函式直接產生。

**已知偏誤（不是 bug，是方法論限制）**：每個模擬的歷史日期，股票清單都是用「現在」的 `selected_stocks` 回頭套用，不是那個時間點真正會選的清單，結果相對於「真的從那個時間點開始上線」會偏樂觀（look-ahead / survivorship bias）。報告的 `caveat` 欄位會註記這件事；結果只適合當作「這次改動前後的相對比較」，不是「這個模型能不能賺錢」的證明。

結果存在 `<output-dir>/backtest_report.json`：每個 fold 的訊號成功率、`hit_up`/`hit_down` 的 pooled recall/precision，以及全部 fold 合併的整體數字。
