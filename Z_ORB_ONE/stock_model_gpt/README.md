# stock_model_gpt 操作筆記

所有指令從專案根目錄執行，範例日期請換成實際交易日。

- **輸入**：股票當日的開、高、低、收、觸漲停、觸跌停、收盤漲跌停狀態、成交量、ATR，共九項，加上**下一交易日的夜盤**，合計十項。
- **輸出**：下一交易日 `high_price` 與 `low_price` 各自的五種機率：`P(-2)`、`P(-1)`、`P(0)`、`P(1)`、`P(2)`；每項機率各自加總為 1。
- **篩選預設**：high 的 `P(1) + P(2) ≥ 60%`；low 的 `P(-2) + P(-1) ≥ 60%`，各自獨立判斷。

### high / low 雙目標第一階段

- low 使用既有特徵中的最低價五級刻度，與 high 使用相同交易參考價及分類界線。十項輸入不變，兩個五分類 head 共用 Transformer。
- 訓練同時學習 high、low；`loss_high_price`、`loss_low_price` 預設皆為 4.0，各自計算類別權重。訓練與驗證會分別列出兩項 loss，以兩項未加權驗證交叉熵的平均選最佳 epoch，兩項都達 patience 才提早停止。
- **舊 high-only checkpoint 不相容，須重新執行 `train_initial`**，完成後才能每日續訓或預測；升級交易日檢查後，請先重新執行 `prepare_features`，重建遇到缺洞時的暖機與連續序列。
- `predict` 同時篩選 high、low，控制台與訊號報表分成兩份清單。`validate_predictions`、checkpoint gate 與 post-training gate 各自評估 high、low；舊 `predicted_class` 欄位仍指 high。
- 預測檔保留既有 high 格式識別並增加 low 欄位，舊 high 預測檔仍可驗證；模型 checkpoint 使用獨立的雙目標格式識別。
- 這是兩組各自的機率分布，並非 high/low 的 25 種聯合機率，也未強制兩項預測的高低順序。

## 0. 先準備交易日曆

日曆須涵蓋**原始行情最早年份至預測年份**。例如使用設定預設的 2010 年起歷史資料：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.trading_calendar --start-year 2010 --end-year 2026
```

日曆保存於 `data/calendar/YYYY.json`。當年及未來年度使用[證交所市場開休市日期](https://www.twse.com.tw/zh/trading/holiday.html)；已結束月份使用[官方加權指數歷史交易日期](https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date=20100101)，包含歷史臨時休市。歷史年度需逐月讀取，初次同步會較久；跨年須先補下一年度，新年度公告尚未提供時會報錯。日常更新使用本機日曆，不會每次重新下載。

當月臨時休市及個股停牌仍須依公告明確登錄；不能因為某支股票沒有行情就自動推定。以下日期、股票與原因僅為格式示例，請按實際公告填寫：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.trading_calendar --date YYYY-MM-DD --closed --reason "公告來源與休市原因"
python -m Z_ORB_ONE.stock_model_gpt.trading_calendar --date YYYY-MM-DD --suspend-symbol 2330 --reason "公告來源與停牌原因"
```

休市例外存於 `overrides.json`，停牌紀錄存於 `suspensions.json`；用 `--open` 可更正指定日期為交易日。年度同步不覆蓋這兩份人工紀錄。

日常流程不必每天重抓日曆，但需要定期維護：

- **月底或月初**：重跑當年度同步，納入剛結束月份的官方實際交易日，包含臨時休市修正。
- **跨年前／新年度第一次使用前**：先補下一年度日曆；若官方尚未公告新年度休市表，程式會提示錯誤，等公告後再重跑。
- **程式提示缺少某年度日曆時**：補該年度或一次補齊從原始行情最早年份到預測年份。

範例：

```powershell
# 每月維護當年度
python -m Z_ORB_ONE.stock_model_gpt.trading_calendar --start-year 2026 --end-year 2026

# 跨年前先補下一年度
python -m Z_ORB_ONE.stock_model_gpt.trading_calendar --start-year 2026 --end-year 2027
```

程式拒絕休市日更新／訓練，並要求 `prediction-date` 正好是 `universe-date` 的下一交易日。股票資料若少了應有的交易日，特徵會重新累積暖機，訓練與預測不跨越缺洞；即使 `previous_date` 被接到較早一筆，也不能繞過檢查。停牌復牌後同樣重新累積暖機及連續輸入長度。

## 1. 初始訓練

先確認歷史夜盤資料已匯入 `data/night_futures.jsonl`，且涵蓋所需日期。股票資料更新不會自動抓夜盤；缺夜盤會造成特徵或訓練序列被略過。

以完整股票資料截止 **2026-09-15** 為例：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-15 --require-complete
python -m Z_ORB_ONE.stock_model_gpt.prepare_features --as-of 2026-09-15
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-15 --training-window-days 150
```

- `--as-of`：完整日 K／訓練資料的截止日期。
- `--training-window-days 150`：訓練目標取最近 150 個可用交易日；每筆目標仍需要此前的歷史輸入序列，不代表只需準備 150 天原始資料。
- `train_initial` 從頭訓練；用於新版五分類預測。

新版 checkpoint 保存實際學習過的「股票＋目標日期＋樣本版本」。舊 checkpoint 若沒有 `trained_sample_versions`，仍可用於相容的雙目標預測，但不能直接續訓，需重新執行一次 `train_initial`。

資料長度以有效交易日計算：目前 `context_days=120`，每筆訓練樣本需要 120 天歷史特徵及下一天的目標。夜盤只有約 100 個交易日時，無法建立樣本；在行情完整、日期連續且暖機資料充足的情況下，至少 121 天有效特徵才能建立第一筆樣本，270 天約可提供最近 150 天的目標。`training-window-days` 不會補足資料或自動縮短輸入。

ATR 五級界線由 `atr_calibration.py` 每 `settings.atr_recalibration_interval_days`（預設 90）天，在**從零訓練的 reseed**（無 `--checkpoint`／`resume_path`）時自動用最近特徵資料的 20/40/60/80 百分位數重新校準一次，而不是每次都重估——避免分桶定義隨每次訓練漂移。校準結果與歷史紀錄存在 `data/atr_analysis/`（`current_calibration.json` 為目前使用值，另有依日期存檔）。任何**續訓**（`train_daily`／`daily=True`，或帶 `--checkpoint` 續跑）一律直接沿用來源 checkpoint 自己的界線，絕不重新校準——因為 `atr_embedding` 的權重是針對那組界線學出來的，換界線等於讓權重在不知情的狀況下錯位。可用歷史樣本不足 100 筆時，退回歷史固定值（`ATR_BOUNDARIES_PCT`）且不寫入校準紀錄，下次 reseed 會再嘗試重新校準。選回最佳 epoch 時，模型、optimizer 與 CUDA scaler 會一起回復至該輪狀態，供每日續訓沿用。

訓練前會檢查 `epochs`、`daily_epochs`、`batch_size` 必須為正整數；學習率與 high／low loss 權重須為有限正數，`focal_gamma` 須為有限非負數。`use_amp` 預設為 `false`，即使用 CUDA 也以 full precision 訓練，避免 class weight 與 focal loss 在 mixed precision 下產生非有限梯度；若明確改成 `true` 才啟用 CUDA AMP。訓練或驗證 loss、梯度出現 NaN／Inf，或 optimizer 未實際完成更新（包含 AMP 跳過更新）時，會中止本次訓練，不發布新模型、不寫入新的樣本學習紀錄；目前模型保持原狀。

新 checkpoint 的 `training_progress` 記錄實際執行輪數及 optimizer 更新次數；選回最佳 epoch 時，`optimizer_steps` 對應該模型，`executed_optimizer_steps` 則保留本次總更新次數。發布前也檢查 loss、optimizer、scaler 的數值有效性。正常的「沒有新樣本，略過續訓」仍會沿用既有模型。

## 2. 初次預測

預測 **2026-09-16**：等歸屬 9/16 的夜盤結束後，開盤前補入夜盤，再執行預測。

以下 `0.87` 只是範例，請替換成實際夜盤漲跌百分比；上漲 0.87% 填 `0.87`，下跌 0.6% 填 `-0.6`。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-16 --change 0.87
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-15 --prediction-date 2026-09-16
```
四個預測篩選參數各自使用以下預設值；只提供其中一個時，其餘仍使用預設值：

```powershell
--high-signal-classes "1,2" --high-signal-threshold-pct 60 --low-signal-classes="-2,-1" --low-signal-threshold-pct 60
```

門檻是所選類別的機率總和，例如 low 的 `P(-2)=25%`、`P(-1)=40%`，合計 65% 即符合 60% 門檻。high、low 同時符合時，該股票會同時出現在兩份清單中。這不代表兩個事件的聯合機率達到 60%。預測指令的舊 `--signal-classes`、`--signal-threshold-pct` 已改用上述 high/low 參數。

### 查看結果

- **控制台／`signal_reports/YYYY-MM-DD.txt`**：同一份摘要，分別列出 high、low 符合條件股票的五種機率、所選刻度合計機率、最高機率類別；兩份清單各自依合計機率由高到低排序，沒有符合股票時也會顯示該清單與條件。
- **`predictions/YYYY-MM-DD.json`**：每支股票一筆，保存 high 與 low 各五種機率及 `signal_matches` 的兩個符合旗標，不符合門檻的股票也會保留。`high_signal_thresholds`、`low_signal_thresholds` 保存兩組設定；`high_signals`、`low_signals` 保存各自的符合清單。為維持現有 high 驗證相容性，`signal_thresholds`、`signals` 暫時保留為 high 的相同內容。

### Meta-labeling 資料累積

meta-labeling 目前先做**自動資料收集**，不影響正式預測清單。當 `predict` 產生當日正式版本時，會把 high／low 入選訊號登錄到 `data/meta_labels/YYYY-MM-DD.json`，狀態為待驗證；同日重跑但未成為正式版本的預測不會寫入。收盤後 `validate_predictions` 驗證正式預測時，會自動把這些樣本標成成功／失敗，並重建 `data/meta_labels/dataset.jsonl` 與 `data/meta_labels/status.json`。

`settings.json` 的 `meta_label_min_days` 預設為 `20`。`status.json` 會顯示已完成標記的交易日數與 `ready` 狀態；未滿 20 個已驗證交易日前只累積資料，不訓練也不套用第二層篩選。未來要啟用 meta-labeling 模型時，應以這份 dataset 作為來源，再先用報表觀察，不直接取代正式訊號。

### 預測版本與正式預測

每次 `predict` 都產生新的 `prediction_id`，保存至 `predictions/versions/YYYY-MM-DD/<prediction_id>.json`；同名 `.txt` 是該版本報表，`.inputs.json` 保存實際使用的特徵與編碼輸入。JSON 記錄建立時間、checkpoint 檔案 SHA-256 及輸入資料指紋。

當日第一份預測自動成為正式版本，另存於原本的 `predictions/YYYY-MM-DD.json` 與 `signal_reports/YYYY-MM-DD.txt`。同日重跑只新增版本，**不覆寫正式預測或正式報表**。如確定要把本次重算結果指定為正式版本，明確加上：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-15 --prediction-date 2026-09-16 --replace-official
```

舊版本會保留，包含升級前只有日期檔名的舊預測。替換後需重新驗證該日，不能將舊版驗證套在新版預測上；若 gate 引用的正式版本已改變，會提示先重做驗證。正式預測的指定由操作人負責，版本紀錄本身不保證它是在開盤前產生。

## 4. 每日更新順序

以 **9/16 收盤後更新、預測 9/17** 為例。

### 步驟一：收盤後更新股票資料並續訓

確認日 K 完整後（例如下午 15:30），先更新 `Z_ORB_ONE/stock_data.py` 的股票清單，並確認歸屬 **9/16** 的夜盤已輸入(其實早上 05:00 後已輸入)，再執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-16 --training-window-days 150
```

若當天是**月底、月初或跨年前後**，先依第 0 節同步交易日曆；一般日常更新只使用本機既有日曆，不需要每天重抓。

啟動時會先檢查 `--as-of` 當天的夜盤資料；缺少時在控制台提示日期，並以錯誤狀態中止，不執行資料更新、特徵產生或續訓。補入該日期夜盤後，再重新執行。

`run_daily` 依序執行：**行情更新與驗收 → 到期預測補驗證 → 重算 gate → 重建特徵 → 續訓**。會重新驗證截至 `--as-of` 的最近 `gate_window_days` 份每日登錄預測，並補驗證更早的漏跑、未完成或版本已變更紀錄；不掃描重跑版本庫。指定 `--settings 路徑` 時，所有步驟沿用同一設定檔。任何步驟拋出錯誤會中止後續工作；gate 為 `STALE`／`DEGRADED` 本身不阻止續訓，只限制正式預測。

`run_daily` 自動使用 `update_data --require-complete`：

- 更新行情時重抓最近 7 個**日曆日**（長期未更新則從更早的快取日期補起），重新合併已存在日期，讓同日重跑可取得修正後的日 K。單獨執行 `update_data` 可用 `--refresh-days N` 調整範圍。
- 公司行動也回補最近 7 個日曆日，與 `update_data --refresh-days N` 共用範圍；同日重跑會重新查詢，以納入晚公布或修正的除權息、減資等資料。成功查詢後，該區間、已查詢資料集的舊紀錄會依最新回應替換（包括撤回紀錄），區間外資料保留；API 失敗不推進同步日期。更早的修正可擴大重抓範圍，或使用 `resync_corporate_actions` 完整重同步。
- 當日驗收須在台北時間 15:30 後，未來日期拒絕執行。當日每支清單股票必須在**本次 API 回應**中取得有效 OHLC，不能只靠舊快取宣稱完成；已明確登錄停牌者會標示 `suspended`。
- 每次完成更新後將驗收結果保存至 `data/data_checks/YYYY-MM-DD.json`，包含股票狀態、快取指紋與驗收時間。任何股票缺少當日新資料或有驗收到的無效 OHLC，就中止後續特徵產生與續訓。
- API 資料在轉換時被拒絕，也會保留原始日期、原因與查詢區間，寫入 `download_rejections`；當日清單股票另有 `rejected_records`。舊快取即使已有該日期，也不能消除本次下載的錯誤。嚴格驗收會阻擋有明確日期的下載拒絕或無效 OHLC；若資料商對上市前／無資料的歷史區間回覆空 data（`date=null`），只保留在報告供追蹤，不會在所需日期已補齊時中止流程。
- 重抓區間會依交易日曆檢查缺洞，未登錄停牌的缺漏列為 `missing_session_dates` 並阻擋嚴格驗收。更早的缺洞會在特徵重建時列出 `[GAP]` 並切斷序列，可擴大重抓範圍修補。15:30 與有效 OHLC 是本機驗收條件，不是資料供應商的最終完成保證。
- 若單獨執行 `update_data` 而未加 `--require-complete`，仍產生報告，但驗收未通過不會以錯誤狀態結束；正式訓練前應使用嚴格模式。

每日增量訓練的歷史重播會提高 high 屬於 `1,2` **或** low 屬於 `-2,-1` 樣本的抽樣權重（`daily_replay_hit_oversample`，預設 4 倍）；兩者同時符合只套用一次權重。這是訓練抽樣設定，不隨預測篩選參數改變。

續訓不再只根據日期跳過：在本次訓練視窗及適用股票範圍內，未學習的樣本、晚到資料，以及特徵／目標／對齊夜盤／ATR 界線改變的樣本，都會被選入本次學習，之後才搭配歷史重播。初始訓練只拿來驗證、未做梯度更新的樣本不會被誤記為已學習。相同來源 checkpoint、資料版本及設定的重跑會沿用已完成結果；使用最新 checkpoint 且沒有資料變更時則跳過。

資料修正後先重建特徵，再續訓；修正的舊樣本若已超出視窗，需要擴大 `--training-window-days`。這是用更正資料再學習，不是消除舊資料對權重的影響；大幅資料修正仍可考慮重新初始訓練。`--force-retrain` 可明確要求資料未變更時再訓練。

### 步驟二：查看驗證，必要時單獨重跑

`run_daily` 已自動補驗證，正常情況不必再執行一次。若要單獨重跑 9/16 的正式預測驗證：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-16
```

驗證預設沿用預測檔保存的 high、low 兩組類別與門檻，不重新套用 60% 預設值。可使用與 `predict` 相同的四個參數逐項覆寫，未提供的欄位仍沿用預測檔。例如只比較不同的 low 門檻：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-16 --low-signal-threshold-pct 70
```

- high、low 各自計算五分類準確率、混淆矩陣、各刻度 precision／recall、平均 log loss，以及所選刻度合併後的 precision／recall。
- 五分類與篩選分開判斷：例如 low 最高機率為 -1、實際為 -2，五分類不正確，但若選擇 `-2,-1` 且合計機率達標，篩選仍成功。同一股票的 high、low 各自計分，不合併為股票總成功率。
- 各組列出入選數、可驗證入選數、成功數與待驗證股票；缺少實際行情或無法產生實際刻度者不計入指標分母。沒有可驗證入選股票時 precision 顯示 `N/A`。
- 有公告紀錄的停牌股票另列於 `excluded_suspensions`，不計成功、不計失敗，也不當作尚待下載資料；未確認的缺資料仍保留在待驗證清單。
- 舊檔沒有 low 預測時，high 正常驗證，low 顯示「無預測資料」。舊檔的 `signal_thresholds` 作為 high 設定；未保存 low 設定時使用 `-2,-1`、60%。
- 原始條件結果保存在 `data/evaluations/YYYY-MM-DD.json`。`targets.high_price`、`targets.low_price` 各自保存完整指標、已驗證訊號及待驗證清單；頂層舊欄位保留 high 內容供舊資料相容使用。
- 驗證會保存 `prediction_id`、預測檔 SHA-256 與來源路徑，並另存至 `data/evaluations/versions/YYYY-MM-DD/<預測檔SHA-256>/<條件識別碼>.json`。重新驗證相同版本與條件會更新該版本的結果，補入晚到的實際行情。
- `actual_data_version` 保存驗證所依據的資料指紋：截至驗證日的相關股票行情、公司行動、夜盤、交易日曆／休市／停牌紀錄，以及 `warmup_days` 和驗證相關程式版本。未來日期新增行情不會使過去驗證失效；歷史來源或驗證設定變更則必須重新驗證。
- 若任一條件與正式預測檔不同，另存至 `data/evaluations/overrides/`，檔名包含日期、條件與預測版本識別，不覆蓋原始條件紀錄，也不更新 gate。
- 預設仍驗證 `predictions/YYYY-MM-DD.json` 的正式版本；指定一般重跑版本時只存版本驗證結果，不改正式日報與 gate。例如：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-16 --predictions "versions/2026-09-16/<prediction_id>.json"
```

- checkpoint gate 分別使用原始條件的 high、low 驗證結果；兩者各自只彙整與該目標最新篩選設定相同的紀錄，不混合不同門檻。任一目標退化即為 `DEGRADED`；兩者皆正常才是 `OK`，其餘為 `INSUFFICIENT_DATA`。待驗證訊號不計入成功率，舊 high-only 紀錄不當作 low 失敗。

### 驗證後查看 post-training gate 建議

先完成上述 `validate_predictions`，再執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.post_training_gate
```

完整參數範例（也是預設值）：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.post_training_gate --days 20 --short-window 5 --min-signals 3 --min-success-rate 0.5 --strong-success-rate 0.7
```

| 參數 | 說明 |
|---|---|
| `--days 20` | 最近最多 20 個有驗證紀錄且篩選設定相同的交易日 |
| `--short-window 5` | 另外顯示上述區間最後最多 5 個交易日 |
| `--min-signals 3` | 每個目標、每個區間至少 3 筆已驗證入選訊號才判斷表現 |
| `--min-success-rate 0.5` | 成功率低於 50% 時，建議提高門檻或暫停該方向 |
| `--strong-success-rate 0.7` | 成功率達 70% 時，顯示可維持或小幅降低門檻的建議 |

程式分開顯示 high、low 的長短區間及建議；成功率是區間內成功筆數除以已驗證入選筆數，不是每日成功率的平均。兩者各自採用最新保存的篩選條件；覆寫條件的比較紀錄不參與統計。樣本不足或沒有 low 驗證資料時會提示先累積資料。

**兩支 gate 的用途不同：**

- `post_training_gate.py`：手動執行，僅輸出上述絕對成功率的建議，不修改篩選門檻、不選用模型，也不寫入自動 gate 狀態。
- `checkpoint_gate.py`：一般由原始條件的 `validate_predictions` 自動呼叫，將狀態保存至 `checkpoints/gate_status.json`。依 `settings.json` 的 `gate_window_days=20`、`gate_short_window_days=5`、`gate_min_signals=3`、`gate_significance_level=0.05`，分別對 high、low 比較「最近 `gate_short_window_days` 天（短期）」與「其之前、不重疊的 `gate_window_days - gate_short_window_days` 天（基準期）」的成功率，用單尾 Fisher's exact test（精確超幾何分布，無需 scipy）檢定短期是否顯著低於基準期，顯著（p 值 ≤ `gate_significance_level`）才判定退化。相較於固定百分點門檻，樣本數少時需要更明顯、更一致的下滑才會觸發，避免單一雜訊訊號誤判；樣本數足夠多時則能偵測到更細微但穩定的退化。

`predict` 自動選模型前會依 `universe-date` 重新計算 gate，不單靠上次的 `gate_status.json`。只要 high 或 low 有**與本次該目標篩選條件相同**的 `DEGRADED` 狀態，就停止正式預測；剛開始沒有足夠樣本時不阻擋，但已退化的目標不會因後續樣本不足而自動解除。

另外檢查最近最多 `gate_window_days` 份已到期的每日登錄預測：缺少驗證、預測版本不符、或仍有待驗證股票時，狀態為 `STALE`，暫停正式預測。已有預測歷史時，最新到期日期還必須等於 `universe-date`，避免把多日前的正常 gate 當作今天仍有效。未來日期的驗證不參與當次計算。第一次尚無任何到期預測可正常啟動。

實際資料指紋改變時同樣視為 `STALE`，不再沿用修正前的成功率。`run_daily` 也會將資料版本已變更的較早紀錄加入補驗證佇列。升級前沒有 `actual_data_version` 的驗證需重跑一次；執行 `run_daily` 可自動補齊，或逐日執行原始條件的 `validate_predictions`。

明確指定 `--checkpoint` 仍可略過自動 gate（包含新鮮度檢查），屬於人工選模；`--settings` 可指定 gate 設定，模型本身仍沿用 checkpoint 內的設定。

已移出清單、但仍有到期正式預測的股票，`update_data` 會補抓缺少的實際行情，且不把它重新加入當日股票清單。

### 步驟三：9/17 夜盤結束後補入資料

一般平日於上午 05:00 夜盤結束、取得收盤資料後輸入；跨週末／連假按第 2 節的日期規則處理。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-17 --change 0.87
```

請將 `0.87` 換成實際夜盤漲跌百分比。

### 步驟四：開盤前預測 9/17

```powershell
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-16 --prediction-date 2026-09-17 --high-signal-classes "1,2" --high-signal-threshold-pct 60 --low-signal-classes="-2,-1" --low-signal-threshold-pct 60
```

## 5. 自動化、模型選擇與中斷恢復

- 所有修改資料的命令共用 `.workflow.lock`。`run_daily` 在同一程序依序執行各步驟，整段流程持有鎖；另一個更新、訓練、預測或重置流程會立即報錯。程序結束或崩潰後由作業系統釋放，不必刪除鎖檔。
- JSON／JSONL 先完整寫入同目錄的唯一暫存檔，再原子替換；轉換或寫入失敗時保留原檔。這是單檔保護，不是整個資料夾的交易式回復；中途失敗請修正原因後重跑。
- checkpoint 先暫存，實際載入並檢查架構、權重與有限數值成功後，才發布新模型並更新 `checkpoints/current_model.json`。該指標保存模型檔名、SHA-256 與訓練截止日；`predict`／`train_daily` 不再依檔名排序猜測最新模型。
- 發布指標前中斷，上一個目前模型不變；多出來的未登錄模型不會自動被選用。模型缺失、內容損壞或不相容時明確報錯，不靜默改挑另一支。

**升級後第一次使用**：先同步日曆並重建特徵。新訓練會自動建立目前模型指標；若已有可用的雙目標 checkpoint，可先確認檔案，再明確登錄（路徑須位於目前的 checkpoints 目錄）：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.checkpoints --checkpoint "Z_ORB_ONE/stock_model_gpt/checkpoints/stock_model_gpt_實際檔名.pt"
```

登錄會驗證模型，但不解除 gate，也不替舊模型補上 `trained_sample_versions`；缺少樣本版本紀錄者仍需 `train_initial` 才能續訓。若歷史資料缺洞影響了舊模型訓練，建議重建特徵後重新初始訓練。

## 6. 重置注意事項

- `reset_runtime_data` 預設只預覽，需 `--yes` 才刪除。若指定 `STOCK_MODEL_GPT_WRITE_ROOT`，只清除隔離目錄內的輸出，不刪除正式行情、公司行動或特徵資料。
- 日曆、人工休市／停牌紀錄與夜盤資料保留；重置也受流程鎖保護。

## 7. 走勢回測（backtest）

`backtest.py` 對歷史區間重跑 `predict` → `validate_predictions`（可選每日續訓），直接重用正式流程的評分邏輯，不是另一套獨立的回測引擎：

```bash
python -m Z_ORB_ONE.stock_model_gpt.backtest --start-date 2026-03-01 --end-date 2026-05-31
```

- `--checkpoint` 可省略：省略時自動選用正式 `checkpoints/` 內、`training_as_of` 不晚於起始日前一交易日的最新一顆（見下方 `list_checkpoints.py`），終端機會印出實際選到哪一顆；要指定特定模型才需要 `--checkpoint 路徑`。
- 輸出完全隔離：內部會把 `STOCK_MODEL_GPT_WRITE_ROOT` 指到 `backtests/<起訖日期>_<時間戳記>/`，不會覆寫正式的 `predictions/`、`checkpoints/`、`evaluations/`、`gate_status.json`；`candles`/`features`/`corporate_actions`/`night_futures` 全程只讀，從不寫回。
- 需要正式環境已累積的歷史股票清單快照（`data/universe/*.json`）；啟動時會自動複製一份到隔離目錄，缺快照的日期會被跳過並列在報告的 `failures` 裡（原因通常是 `找不到當日股票清單快照`）。
- 預設 `--daily-train` 關閉：整段區間只評估同一個起始 checkpoint（凍結模型回測，適合回答「這個模型放著不訓練，接下來表現會不會撐住」）；加上 `--daily-train` 才會在每天驗證後也模擬 `train_daily` 續訓，checkpoint 逐日往下傳遞。
- 結束後在隔離目錄產生 `backtest_report.json`，並在終端機印出各 target 的 pooled 準確率、log loss（含 naive baseline 對照）、訊號 precision/recall，以及區間結束當下的 gate 狀態。
- 任一天 `predict`/`validate` 拋出 `RuntimeError`（例如當天歷史特徵不足、找不到清單快照）只會記錄跳過並繼續下一天，不會中止整段回測；報告裡 `failures` 會列出每一天的原因。

### 手動查詢 checkpoint 的 training_as_of

`.pt` 檔名的時間戳記是訓練**執行當下**的時間，不一定等於訓練用的 `--as-of`（例如補跑、或手動指定較早的 `--as-of`）。`list_checkpoints.py` 直接讀 checkpoint 內容裡的 `training_as_of`，依日期排序列出，不必憑檔名猜：

```bash
python -m Z_ORB_ONE.stock_model_gpt.list_checkpoints --as-of 2026-02-28
```

刻意放在套件原始碼底下（跟 `predict.py`、`train_daily.py` 同一層），不是放進 `checkpoints/` 資料夾——`reset_runtime_data.py --yes` 會整個清空那個資料夾。




## 盤前
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-21 --change -0.05
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-18 --prediction-date 2026-09-21
## 收盤後
先更新 Z_ORB_ONE/stock_data.py
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-21 
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-21 --training-window-days 150


更新日歷
python -m Z_ORB_ONE.stock_model_gpt.trading_calendar --start-year 2026 --end-year 2026
或是
python -m Z_ORB_ONE.stock_model_gpt.trading_calendar --start-year 2026 --end-year 2027



人工報表，可每週五執行
python -m Z_ORB_ONE.stock_model_gpt.post_training_gate
