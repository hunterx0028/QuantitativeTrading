# stock_model_gpt

以台股日 K 七項離散狀態訓練的輕量 causal Transformer，預測下一交易日**盤中 high 是否曾達 price bucket 1 或 2（`intraday_up_1plus`）**這一項。換句話說，目標是「盤中曾經達到 +2% 以上區間，包含 bucket 1、bucket 2，以及自然涵蓋漲停」。`price`（收盤價分類）、`hit_up`（觸漲停）與 `hit_down`（觸跌停）都只作為歷史輸入特徵，不再作為輸出目標。

操作順序：**首次執行第 5 節；之後每個交易日分兩段——收盤後依序執行第 4（當天）、7、8 節的資料更新與續訓（`run_daily --skip-predict`），隔天開盤前再執行第 4 節（`prediction_date` 當天）＋單獨的 `predict`；每隔約 20 個已驗證交易日重新校準一次（第 6 節「定期重新訓練」）、也可執行第 9 節查看訊號統計。** 首次訓練前可先依第 3 節確認 GPU。所有指令均從專案根目錄執行，範例日期請替換為自己的完整交易日與下一預測交易日。**為什麼要分兩段：見第 2 節「第八項輸入」——預測用的夜盤資料是被預測那天自己開盤前的那一次，收盤當下還沒發生，要等到隔天凌晨才有。**

| 章節 | 查找內容 |
|---|---|
| 1. 模組說明 | 各程式用途 |
| 2. 資料表示 | 七項輸入、ATR 固定刻度、預測輸出與訊號 |
| 3. GPU確認 | 確認實際訓練裝置 |
| 4. 夜盤期指資料（每日必做）| 為什麼需要、沒有自動抓取、每日/定期要手動匯入 |
| 5. 初始訓練設定 | 從頭重置（選用）、更新資料、初始訓練與首次預測 |
| 6. 訓練視窗與定期重新訓練 | 滾動視窗、驗證集 early stopping、多久重新校準一次 |
| 7. 驗證結果程序 | 取得實際行情、驗證先前預測、recall/precision、checkpoint gate 判定 |
| 8. 每日更新設定 | 候選股票清單怎麼變動、續訓、下次預測、怎麼看預測結果 |
| 9. Post-Training Gate | 定期查看 signals 統計與建議 |
| 10. 除權息及特殊參考 | 公司行動資料來源與重新同步 |
| 11. Walk-forward 回測（選用工具）| 隔離環境下驗證訓練設定，不影響正式資料 |
| 12. 已知限制 | 目前還沒解決、操作時要留意的事 |

## 1. 模組說明

測試程式集中在 `tests/`，不參與初始訓練或每日流程。修改程式後可從專案根目錄執行：

```powershell
python -m pytest Z_ORB_ONE/stock_model_gpt/tests -q
```

- `reset_runtime_data.py`：清空所有可重建的執行期資料（candles/features/checkpoints/predictions/evaluations/universe/signal_reports）；不會動到 `config.ini`、`stock_data.py`、`settings.json`、原始碼或 `data/night_futures.jsonl`。預設乾跑只列出、不刪除，加 `--yes` 才會真的刪除。
- `update_data.py`：讀取 `Z_ORB_ONE/stock_data.py` 的 `selected_stocks`、保存每日清單快照、登入玉山 SDK 並增量更新日 K，同時增量同步 FinMind 公司行動資料。程式刻意不呼叫 logout。
- `night_futures.py`：台指期近月夜盤資料的分桶邏輯與共用儲存，見第 4 節——**沒有自動每日抓取**。
- `set_night_futures.py`：手動輸入單日夜盤漲跌% 寫入 `night_futures.jsonl`，日常更新用這個，見第 4 節。
- `import_night_futures.py`：批次匯入 TAIFEX CSV 匯出檔，補歷史資料用這個，見第 4 節。
- `finmind.py`：匿名或使用可選 `FINMIND_TOKEN` 查詢除權息結果；另保留付費公司行動資料的選用介面。
- `resync_corporate_actions.py`：忽略既有同步狀態，重新同步並覆寫 FinMind 公司行動快取。
- `prepare_features.py`：將 OHLCV（＋當日夜盤期指桶）轉成七項離散狀態；某天沒有對應的夜盤資料就直接跳過那天，不會硬猜一個值（見第 4 節）。
- `atr_calibration.py`：ATR 五級界線**固定寫死**在程式裡（見第 2 節），初始訓練與每日續訓都直接套用同一組，不再自動分析或產生報告。
- `model.py`：七項離散輸入（含夜盤期指）的 causal Transformer，加上被預測日當天盤前夜盤的獨立輸入（`target_night_futures`，見第 2 節），單一 `intraday_up_1plus` 輸出頭。
- `train_initial.py`：由隨機權重訓練初始模型（或重新校準）；可用 `--training-window-days` 指定只用最近 N 個交易日的滾動視窗，見第 6 節。
- `train_daily.py`：載入前一 checkpoint，以新增目標序列加部分歷史重播續訓；同樣支援 `--training-window-days`；可選全部歷史模式。
- `predict.py`：保存下一交易日 `intraday_up_1plus` 的完整機率，並產生 LONG 訊號。
- `validate_predictions.py`：用實際日 K 驗證 `intraday_up_1plus` 預測與 `signals`。
- `post_training_gate.py`：彙整 `signals` 交易結果，打印門檻建議。
- `run_daily.py`：串接資料更新、特徵產生、每日續訓與預測；**執行前必須已經匯入當天的夜盤期指資料**，見第 4 節。
- `checkpoint_gate.py`：每次 `validate_predictions` 完自動判定近期 `signals` 成功率是否明顯退化；退化時 `predict.py` 在沒有明確指定 `--checkpoint` 的情況下會拒絕自動選用最新 checkpoint。
- `walk_forward_backtest_v2.py`：主要的回測工具，逐日模擬「訓練→預測→驗證」，滾動視窗＋定期重新訓練，見第 11 節。`walk_forward_backtest.py`／`walk_forward_backtest_resume.py` 是較早期的粗粒度版本（fold 只訓練一次、期間凍結預測），保留供對照，不建議再用來評估新設定。

## 2. 資料表示

每日輸入為七項：

```text
(price, hit_up, hit_down, close_limit, volume, ATR, night_futures)
```

- `price`: `-2,-1,0,1,2`，以當日收盤價相對當日交易參考價分箱。**只作輸入，不是預測目標。**
- `hit_up`, `hit_down`: 可同時為真，使用實際價格與台股升降單位計算。兩者都只作為歷史輸入，不是預測目標。
- `close_limit`: `U,N,D`，只作輸入。
- `volume`: 相對此前20個有效日成交量中位數的 `-2,-1,0,1,2`；零量或無有效基準為 `X`，只作輸入。
- `ATR`: 固定 14 日、依交易參考價調整尺度的 Wilder ATR，換算成當日 `atr_ratio = atr / 當日收盤價`，再依**固定**四個百分比界線分成 `0,1,2,3,4` 五級：

  | 桶 | 範圍（`atr_ratio × 100`）|
  |---|---|
  | 0 | < 2.8694729537058703% |
  | 1 | 2.8694729537058703% ~ 3.5912423282596873% |
  | 2 | 3.5912423282596873% ~ 4.20375143031919% |
  | 3 | 4.20375143031919% ~ 5.048733346878557% |
  | 4 | ≥ 5.048733346878557% |

  這組數字寫死在 `atr_calibration.py` 的 `ATR_BOUNDARIES_PCT`，**不再依訓練視窗自動校準**（舊版是每次重新訓練用訓練期 ATR% 的 P20/P40/P60/P80 動態算出來，後來發現這樣界線會隨每次重訓練的資料窗口變動，改成固定值）。要改界線就是直接改這個常數，然後所有 checkpoint 都要重新訓練（界線變了，舊 checkpoint 的刻度就對不上）。
- `night_futures`: 台指期**近月合約**夜盤（盤後交易）收盤，相對於「前一個日盤收盤」的漲跌幅，分成 `-2,-1,0,1,2` 五級（界線 `>1%`／`0.5~1%`／`-0.5~0.5%`／`-1~-0.5%`／`<-1%`，同樣用 boundary-snapping：剛好卡在界線上時歸類到比較不極端的那一桶）。**這項資料沒有自動抓取，需要每日/定期手動匯入，見第 4 節；沒有覆蓋到的日期，那天直接不會產生任何股票的特徵（見 `prepare_features.py` / `night_futures.py` 說明）。**

ATR 使用當日原始 high/low 與交易參考價，先換算歷史波動的價格尺度，再套用 [Wilder 平滑公式](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/atr)：

```text
factor = 當日交易參考價 / 前日原始收盤價
TR = max(high-low, abs(high-當日交易參考價), abs(low-當日交易參考價))
ATR_t = (13 * ATR_{t-1} * factor + TR) / 14
```

交易參考價優先使用 FinMind 公司行動修正值，其次為玉山 `close - change`；缺少參考價時回退前日收盤價。參考價或前收盤價非有限正數時報錯。
一般日參考價等於前收盤價，factor 為 1；除權息、減資、分割及面額變更日自動換算。例如 1 拆 2 時前日 ATR 乘 0.5，排除機械價格跳空，同時保留當日相對參考價的實際波動。
第一根 K 只提供前收盤價；第 15 根 K 才有 14 筆 TR，取其平均作為首個 ATR。暖機期間遇公司行動，也會先將已累積的 TR 換算為當日尺度。已產生的過去日期 ATR 不會因未來公司行動回頭改值。
正確調整以參考價資料已同步為前提；若公司行動資料延遲或修訂，可執行 `resync_corporate_actions` 後重跑 `prepare_features`，重算事件日起的遞迴 ATR，並視需要重算 evaluation。
計算只使用當日及以前的 K 棒；不足 ATR 或成交量暖機期的日期不產生狀態。預設 20 日暖機與 120 日 context 維持不變。

七項都使用分類 embedding；相加後送入 causal Transformer。
訓練與預測的輸入 tensor 均為 `[batch, days, 7]` 的 torch.long。`atr_ratio` 只供分級使用，Dataset 與預測共用 `encode_state` 分級。

### 第八項輸入：被預測日當天的盤前夜盤（`target_night_futures`）

除了上面「過去 N 天」的七項序列輸入，模型還有**另一個獨立輸入**：`prediction_date`（要被預測的那一天）自己開盤前的那一次夜盤，跟 context 裡每一天自己帶的 `night_futures` 是不同的東西——**這不是歷史，是預測當下就能拿到的最新資訊**：那次夜盤在 `prediction_date` 當天開盤前就已經結束，不算資訊外洩，但因為它屬於「被預測的那一天」而不是「過去的某一天」，不能塞進 context 序列裡，是在 causal transformer 算完 context 之後，直接加到最後的隱藏狀態上（`model.py` 的 `target_night_futures_embedding`）。

這一點在**跨週末／連假預測時特別重要**：例如週五（`universe_date`）收盤後要預測下週一（`prediction_date`），週五自己 context 裡帶的 `night_futures` 是「週四晚上到週五那一次」，但週五晚上到週一開盤前**還有一次夜盤**，這次夜盤的資訊如果不透過這個獨立輸入補進去，模型就完全看不到、等於少了最新的盤前資訊。平常日對日的預測（例如週二收盤預測週三）沒有這個落差，因為週二 context 帶的 `night_futures` 剛好就是最新的一次。

操作上：跑 `predict.py` 之前，要先確保 `night_futures.jsonl` 裡有 `prediction_date` 這一天的資料（不是 `universe_date` 那天，是要被預測的那天），用第 4 節的 `set_night_futures.py` 手動輸入。**沒有這筆資料，`predict.py` 會直接報錯拒絕預測**，不會猜一個值頂著跑。訓練時每一筆訓練序列的目標日，本身的 `night_futures` 也是用同樣方式帶入（`dataset.py` 的 `target_night_futures`），確保訓練跟預測看到的是同一種輸入。

模型只預測下一交易日的 `intraday_up_1plus`：

```text
(intraday_up_1plus,)
```

`intraday_up_1plus` 是二分類：

```text
T = 目標日 high 相對交易參考價進入 price bucket 1 或 2（約 +2% 以上，漲停自然包含）
F = 目標日 high 未達 price bucket 1
```

`price`、`hit_up`、`hit_down`、`close_limit`、`volume`、`ATR`、`night_futures` 都只作為歷史輸入特徵，不作為輸出目標。

loss 只有一項：

```text
loss_intraday_up_1plus = 4.0
```

`intraday_up_1plus` 仍可能有正負樣本不平衡，因此另外疊加兩層機制：

- **Class weight**：每次訓練（初始或每日續訓）依當次實際訓練集裡 `intraday_up_1plus` 的正負樣本比例，自動算出 inverse-frequency 權重，不是固定值。
- **Focal loss**（`focal_gamma`，預設 `2.0`）：取代普通 `CrossEntropyLoss`，公式為 `loss = -(1-p_t)^γ * log(p_t)`，讓模型已經很有把握答對的樣本梯度貢獻變小，聚焦在難分樣本上。`γ=0` 時等同沒有 class weight 的普通 `CrossEntropyLoss`。

這兩層機制沒有上限保護；如果實際正樣本比例極低（例如 <1%），算出的 class weight 可能到十幾甚至上百倍。**這個機制是刻意犧牲「機率校準」（log-loss）去換「抓得到稀有事件」（recall/precision）**——回測會看到模型的 log-loss 輸給「什麼都不看、只猜訓練視窗多數類別」的笨方法，這是預期中的設計取捨，不代表模型沒用，只是代表模型輸出的機率不該當成一個校準過的真實機率去解讀，該看的是 recall/precision（見第 7 節）。

原始 K 棒保存在 `data/candles`，衍生狀態保存在 `data/features`，每日股票清單快照保存在 `data/universe`，夜盤期指保存在 `data/night_futures.jsonl`。這些執行期資料不納入 Git。

### 預測輸出與訊號

`predict.py` 會保存 JSON，機率欄位仍是數字，但小機率不使用科學記號，方便目視檢查；保存後會同步於控制台列出符合訊號門檻的標的，並將同一批訊號摘要另存至 `signal_reports/YYYY-MM-DD.txt`。訊號只有一種（原本 LONG/SHORT 雙邊、CONFLICT、方向參考訊號都隨 `hit_down`／`price` 輸出一起移除了）：

```text
LONG: intraday_up_1plus.T >= long_intraday_up_1plus（預設 0.6）
```

`predict`／`validate_predictions` 的 `--signal-threshold` 可設定門檻，也可用 `--long-up-threshold` 單獨覆寫。同一股票只會打印一筆訊號。`--long-hit-threshold` 仍保留為舊參數 alias，等同 `--long-up-threshold`。

### `--signal-threshold` 只是事後的報告門檻，不影響訓練或模型

`--signal-threshold`（及 `--long-up-threshold`）**只出現在 `predict.py` 跟 `validate_predictions.py`**，`training.py`／`train_initial`／`train_daily` 完全不會用到、也不知道這個參數的存在。訓練只針對 `intraday_up_1plus` 的 0/1 標籤最小化 FocalLoss，學的是把機率預測準，不涉及任何門檻；同一個 checkpoint 不管你之後設門檻是 0.5 還是 0.7，模型算出來的 `intraday_up_1plus.T` 機率值都完全一樣。門檻只是「機率算完之後，用哪一條線去判斷要不要列為訊號」，純屬報告/評估層的後處理，不需要重新訓練就能換一個門檻重看。

因此可以用同一個 checkpoint、不同門檻去比較不同的觀點：

- 想看某一天不同門檻下訊號會怎麼變化，直接在 `predict.py` 換參數重跑即可（不用重新訓練）：
  ```powershell
  python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-11 --prediction-date 2026-09-14 --signal-threshold 0.5
  ```
- 想系統性比較不同門檻在歷史上的 recall/precision 取捨（門檻拉低抓得多但精準度下降，拉高則相反），用 `walk_forward_backtest_v2.py` 的 `--long-up-threshold` 掃幾個值比較（見第 11 節），不用真的跑 `predict`：
  ```powershell
  python -m Z_ORB_ONE.stock_model_gpt.walk_forward_backtest_v2 --output-dir C:\tmp\threshold_test --training-window-days 150 --reseed-interval-days 20 --backtest-days 80 --long-up-threshold 0.55
  ```
- `validate_predictions.py` 事後重算某天的 recall/precision 時，也可以用同一個參數換一個門檻重新檢視同一批已保存的預測（見第 7 節）。

0.6 這個預設值目前只是延續原本設計時的預設，沒有特別調校過；真的要決定要不要改，建議照上面第二種方式，拿 `walk_forward_backtest_v2.py` 掃過幾個門檻、比較 recall/precision 之後再決定，而不是憑感覺調。

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

## 4. 夜盤期指資料（每日必做）

模型的第七項輸入是「台指期近月合約夜盤收盤，相對於前一個日盤收盤的漲跌幅」。**這項資料沒有自動化的每日抓取程式**——期交所的下載頁面是 JS 動態產生下載連結，目前沒有確認穩定可用的 API，所以是手動下載 CSV、用程式匯入。

### 為什麼這是「每日必做」而不是「有空再做」

`prepare_features.py` 透過 `state_pipeline.load_candle_states` 產生每日特徵時，**任何一天只要找不到對應的夜盤資料，那一天就完全不會產生任何股票的特徵**（`features.py:encode_candles` 直接 `continue` 跳過，不報錯、不留痕跡）。這代表：如果你忘記匯入某一天的夜盤資料就直接跑 `run_daily`／`prepare_features`，**不會有任何錯誤訊息**，只是那天悄悄沒有被訓練或用來預測——`train_daily` 會因為「沒有新增目標序列」而跳過（印 `[SKIP] 沒有新增目標序列`），`predict` 則會因為所有股票的 `input_last_date` 對不上 `--universe-date` 而報錯「沒有日期與歷史長度合格的股票」。兩種情況都會讓你以為系統壞了，其實只是忘記先匯入當天的夜盤資料。

**建議把「更新當天夜盤資料」當成每天收盤後的第一步，排在 `update_data` 之前。**兩種做法，日常用第一種，補歷史資料用第二種。

### 方法一：手動輸入單日資料（日常用這個）

前往 <https://www.taifex.com.tw/cht/3/futDailyMarketReport>，頁面第一筆就是最新一天的近月合約資料，例如：

```text
TX 202609 46306 46663 46041 46588 ▲401 ▲0.87% 28667 - - 46552 46589 49651 24962
```

把「漲跌%」那個欄位（上例是 `▲0.87%`，代表 +0.87）跟頁面顯示的交易日期，帶入：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-15 --change 0.87
```

`--change` 是純數字，把 `▲`/`▼` 符號跟 `%` 拿掉、只留正負號——上漲不用加 `+`，下跌才加 `-`（例如 `▼0.6%` 就輸入 `-0.6`）。同一天重複執行會覆寫那一天，不會出錯，打錯了重打一次就好。執行完會印出這天算出來的桶值，例如：

```text
已寫入 2026-09-15: change_pct=+0.87% -> bucket=1
```

不用下載檔案、不用管編碼，30 秒可以做完，這是日常建議的做法。

### 方法二：CSV 批次匯入（一次補一段歷史）

同一個查詢頁面下方可以下載 CSV（互動查詢一次最多約一個月；要一次補更久的歷史資料可以用期交所另外提供的年度 zip 檔，2000~去年都有完整年度檔，今年的部分仍要用互動查詢分段下載）。下載下來的 CSV 是 **Big5 編碼**，`night_futures.py` 已經處理好這個編碼，不用自己轉檔。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.import_night_futures 你下載的檔案1.csv 你下載的檔案2.csv ...
```

可以一次丟多個檔案（跨月份分批下載的都可以一起匯），合併是照日期去重，同一天重複匯入不會出錯也不會重複，跟方法一寫入的是同一份檔案、可以混用。匯入完會印出合併後總共涵蓋哪個日期區間，例如：

```text
合併後總計: 411 個交易日（2025-01-02 ~ 2026-09-11）
```

初次建置系統、或中間斷了幾天忘記手動輸入時，用這個方法一次補齊比較快；平常單日更新用方法一就好，不用為了一天特地下載一份 CSV。

兩種方法寫入的都是共用的 `data/night_futures.jsonl`（不分股票，整個系統共用一份），可以用文字編輯器打開直接看某天的值。

### 判斷方式（如果之後要調整或除錯）

近月合約的認定：同一天 CSV 裡「盤後」列可能有好幾個到期月份（例如 202608、202609、202610…），取**最小的到期月份**當近月。漲跌幅直接用期交所自己算好的「漲跌%」欄位，不用自己跨日期去兜——期交所這個欄位對盤後列來說，本來就是相對於**前一個日盤收盤**算的（已經驗證過，不是相對於前一次盤後收盤）。分桶邏輯在 `night_futures.py:night_futures_bucket()`。

## 5. 初始訓練設定

本節理論上只會執行一次：完成初始訓練後，之後每個交易日都是走第 7、8 節的驗證與續訓流程，每隔一段時間再走第 6 節「定期重新訓練」，不會回頭重跑本節。如果要重新從頭開始（例如想丟棄舊的執行期資料、或架構/資料有重大變更想乾淨重來），先用 `reset_runtime_data.py` 清空舊資料再往下走：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.reset_runtime_data --yes
```

這一步只清 `data/candles`、`data/features`、`checkpoints/` 等執行期資料，不會動到 `config.ini`、`stock_data.py`、`settings.json`，也不會動到 `data/night_futures.jsonl`（那是共用的市場資料，不是這個模型的執行期產物）。不確定要不要清時，先不加 `--yes` 執行一次看預覽，確認範圍後再加 `--yes` 重跑。

**開始之前，先確認第 4 節的夜盤期指資料已經涵蓋你要訓練的整段期間**（訓練截止日往前推整個滾動視窗，見第 6 節），沒有涵蓋到的日期不會被拿去訓練。

初始流程為「更新資料 → 產生特徵（自動套用第 2 節的固定 ATR 界線）→ 初始訓練 → 預測」，例如訓練期截止 2026-09-03、2026-09-04 起保留驗證，從專案根目錄依序執行（請替換為自己的訓練截止日）：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.prepare_features --as-of 2026-09-03
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-03 --training-window-days 150
```

**最後的 `predict` 要等 `--prediction-date`（2026-09-04）當天開盤前的夜盤結束才能跑**（見第 2 節「第八項輸入」），跑之前先用第 4 節的工具把那天的資料補上：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-04 --change 0.87
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-03 --prediction-date 2026-09-04
```

`--training-window-days 150` 是回測驗證過、在三個不重疊時期都穩定的設定，見第 6 節；不加這個參數會用全部可用歷史（舊版行為）。

### 日期設定

`--as-of` 是強制的資料時間邊界，應填「最後一個已有完整日 K 的交易日」，不是執行程式當天的日期；特徵及訓練目標都只會使用該日以前的資料。`prediction-date` 必須由交易日曆或操作者提供，程式不把曆日的明天誤認為交易日。預測程式也會拒絕載入訓練截止日晚於 `--universe-date` 的 checkpoint，防止本地快取已有未來日 K 時發生資訊洩漏。

### 查看初始訓練結果

`train_initial` 完成後會在 `checkpoints/` 產生新的 `stock_model_gpt_*.pt`。可先用 PowerShell 看最新模型：

```powershell
Get-ChildItem Z_ORB_ONE\stock_model_gpt\checkpoints | Sort-Object LastWriteTime -Descending | Select-Object -First 5
```

這次切換到 `intraday_up_1plus` 後，2026-09-14 的初訓練結果如下，可作為之後檢查格式是否正常的參考：

```text
checkpoint: stock_model_gpt_20260914_214607_388077.pt
training_as_of: 2026-09-14
target_names: intraday_up_1plus
model head: intraday_up_1plus_head
samples: 16636
intraday_up_1plus=True: 10118（約 60.8%）
intraday_up_1plus=False: 6518（約 39.2%）
majority baseline: true
validation_days: 20
best_epoch: 2
best validation CE loss: 0.659591
train CE loss at best epoch: 0.640325
early_stopped: true（第 5 個 epoch 停止，回滾採用第 2 個 epoch）
```

這代表新目標不是原本 `hit_up` 那種極稀有事件，正樣本反而是多數；判讀時不要只看 accuracy，仍要等隔天驗證後看 `signal_recall_precision.intraday_up_1plus.recall` / `.precision`。`val_loss` 若看到約 `2.638` 是因為乘上 `loss_intraday_up_1plus = 4.0`，若要看一般 cross-entropy，請看 `val_components.intraday_up_1plus`。

## 6. 訓練視窗與定期重新訓練

這節是這一版跟最早期版本最大的行為差異，操作上要記住兩件事：**續訓永遠只用最近一段滾動視窗的資料（不是全部歷史），而且要每隔一段時間整個重新訓練一次（不是永遠續訓下去）。**

### 為什麼

早期版本用「擴張視窗」：每次重新訓練都把從第一天到現在的全部歷史塞進去，資料越訓練越多。回測發現這樣容易過擬合（模型把很久以前、可能已經不適用的市場狀態也背下來），而且固定 epoch 數（例如 20）沒辦法知道自己什麼時候開始背答案而不是學規律。

現在改成：

- **滾動視窗**：只用截止日往前數 N 個交易日的資料（`--training-window-days`），舊資料自然被排除，不會無限累積。回測驗證過 `150` 天在三個不重疊時期（2025-08~12、2025-12~2026-05、2026-05~09）都穩定。
- **驗證集 + early stopping**：只有「重新訓練」（`train_initial`，不是每日續訓 `train_daily`）才會啟用——視窗最後 `validation_days`（預設 20 天，在 `settings.json` 加這個 key 可調）不參與梯度更新，只用來檢查每個 epoch 是否真的在進步；連續 `early_stopping_patience`（預設 3）個 epoch 沒進步就停止，並回滾到驗證集表現最好的那個 epoch，不是用最後一個 epoch。`epochs`（`settings.json` 裡預設 20）現在的角色是「上限」，不是「一定會跑滿」。
- 每日續訓（`train_daily`）**沒有**驗證集/early stopping（只有 1 個 epoch，沒有「訓練過頭」的問題），純粹是每天用新資料微調前一天的權重，滾動視窗一樣適用（`--training-window-days` 一樣要傳）。

### 操作方式

**每日續訓**（第 8 節 `run_daily` 的一部分）要記得加上跟重新訓練同一個 `--training-window-days`：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-04 --prediction-date 2026-09-07 --training-window-days 150
```

如果每天執行 `run_daily` 時忘記加這個參數，那天的續訓會退回「用全部歷史」，跟前後幾天用的資料範圍不一致——沒有驗證過這樣混用的效果，建議固定每次都加。

**定期重新訓練**：大約每 20 個交易日（約一個月），直接重跑一次第 5 節的 `train_initial`（同一個 `--training-window-days`），取代掉目前的模型血緣：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-10-02 --training-window-days 150
```

跑完之後，接下來的 `run_daily` 會自動抓到最新的 checkpoint 續訓（`predict.py` 的自動選檔邏輯是抓 `checkpoints/` 底下時間戳最新的檔案）。20 天這個頻率是回測用的設定，不是理論上的最佳值，你可以依實際運作狀況調整；但**不要完全不做這一步**——只靠每日續訓、永遠不重新訓練，等於又回到「一路續訓不回頭」的舊模式，沒有驗證過長期這樣跑會不會又開始過擬合。

## 7. 驗證結果程序

每個交易日收盤後，先更新本次清單並取得完整行情，驗證先前對該日保存的預測。以下範例銜接第 5 節產生的 2026-09-04 預測：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-04
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-04
```

完成後接著執行第 8 節；若尚無該日預測檔，當天不執行此驗證指令。

### 驗證內容與輸出

驗證計算 `intraday_up_1plus` 的命中率（argmax 對答案，門檻等同 0.5）；另外用實際訊號門檻（`--long-up-threshold`，預設 0.6）計算 recall（實際達標中抓到幾成）與 precision（觸發訊號中真的達標的比例），存在 `signal_recall_precision` 欄位並同步印在控制台。

**這兩組數字門檻不一樣，數字看起來會有落差是正常的**：命中率用比較寬鬆的 0.5 門檻算（模型自己覺得比較可能的答案），recall/precision 用比較嚴格的 0.6 門檻算（真正決定要不要出訊號的那條線）。稀有事件下命中率容易失真（模型永遠猜「不觸及」也能有 87% 以上的命中率），**recall/precision 才是判斷訊號品質有沒有改善的依據**，尤其是 precision 相對於「訓練視窗多數類別」基期的倍數（回測驗證過的三個區間都有 2.5～5 倍的提升）。

`evaluate_signal_trade` 額外算了一個「如果照 3% 停利／2% 停損的規則交易會不會成功」（`success` 欄位），這是保留下來的舊評估邏輯，**現在的判斷不依賴它**——它沒有考慮日內高低點發生的先後順序，會系統性偏樂觀，只當作參考，不要拿它的 `success` 比例當作模型好壞的依據。

### Checkpoint gate（自動安全煞車）

每次 `validate_predictions` 執行完，會自動彙整最近 `gate_window_days`（預設 20）個已驗證交易日與最近 `gate_short_window_days`（預設 5）日的 `signals` 成功率（用的是上面那個 3%/2% 規則，見上一段的保留說明），寫入 `checkpoints/gate_status.json`：

- 樣本數（`signals` 筆數）不足 `gate_min_signals`（預設 3）時，判定 `INSUFFICIENT_DATA`，不影響任何行為。
- 短窗口成功率比長窗口下降超過 `gate_max_success_rate_drop`（預設 `0.25`，即 25 個百分點）時，判定 `DEGRADED`。
- 其餘情況判定 `OK`。

`predict.py` 在**沒有明確指定 `--checkpoint`** 的自動選檔路徑（也就是 `run_daily` 實際在用的路徑）會檢查這個狀態：`DEGRADED` 時直接報錯拒絕預測，避免不知不覺拿一個表現變差的模型血緣去產生真正的訊號。明確指定 `--checkpoint <路徑>` 永遠不受這個檢查影響。

這不是完整的 A/B 模型比較機制，只是偵測「最近訊號表現有沒有明顯變差」的煙霧偵測器，出現 `DEGRADED` 時需要人工檢查訓練或資料是否異常，而不是自動判定該用哪個模型。

驗證前需先讓本地 `data/candles` 含有該預測日的實際日 K；驗證程式會讀取 `predictions/2026-09-04.json`，從本地 `data/candles` 擷取 2026-09-04 實際日 K，另存至 `data/actual_candles/2026-09-04.jsonl`，並在控制台打印命中率及 `signals` 的實際結果。驗證程式會直接由實際 K 棒重算該日狀態，不需要先重跑 `prepare_features`。驗證結果會保存至 `data/evaluations/YYYY-MM-DD.json`，作為 post-training gate 的資料來源。

## 8. 每日更新設定

### 候選股票清單怎麼變動

`Z_ORB_ONE/stock_data.py` 的 `selected_stocks` 是整套系統的股票池來源，每次執行 `update_data.py`（`run_daily` 的第一步）都會**重新讀取這個檔案**，用當下內容產生當天的股票清單快照（`data/universe/YYYY-MM-DD.json`）。要新增或移除候選股票，**直接編輯這個檔案**，改完之後下次跑 `update_data`／`run_daily` 就會生效，不用額外通知程式。

- **新增的股票**：如果這支股票原本就在市場上交易一段時間，`update_data` 第一次抓到它時會直接從 `settings.earliest_date`（預設 2010-01-01）把完整歷史日 K 補齊，不用等 120 天才有預測資格——只要本身歷史夠長（滿足 `context_days=120`）、又剛好落在第 4 節說的夜盤資料涵蓋範圍內，加進去當天或隔天就可能出現在預測名單裡。真正剛掛牌、歷史不滿 120 個交易日的新股，會被 `predict.py` 標記 `insufficient_history` 跳過，要等歷史夠長才會出現。
- **移除的股票**：本地 `data/candles`、`data/features` 資料不會被刪除，之後重新加回清單可以直接補資料缺口，不用整個重抓。但移除之後 `update_data` 就不會再幫它抓新的日 K——如果移除前一天還有掛著的預測沒驗證，隔天驗證會抓不到當天實際行情而跳過並印警告（`[WARN] ... 支缺少 ... 實際日K`）。**移除股票前，先確認前一天的預測已經驗證完（第 7 節），不要留著未驗證的預測就把股票從清單拿掉。**
- 預測嚴格限定在當天 `--universe-date` 的清單內（`active_symbols`），跟訓練用的 `recent_universe_days`（預設 60 個交易日內出現過的清單快照聯集）是兩個不同的範圍——訓練池比較寬鬆，剛被移除沒多久的股票可能還留在訓練池裡，但不會出現在當天的預測名單上。

### 續訓與預測（分兩段）

**第一段，當天收盤後**：先確認今天已經完成第 4 節（匯入*今天*的夜盤資料）跟第 7 節（驗證昨天的預測），加 `--skip-predict` 執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-11 --prediction-date 2026-09-14 --training-window-days 150 --skip-predict
```

這會依序跑 `update_data → prepare_features → train_daily`，先把今天的續訓做完，**先不做預測**——因為預測需要 `--prediction-date` 那天自己開盤前的夜盤資料（第 2 節「第八項輸入」說明），這筆資料現在還不存在。

**第二段，隔天開盤前**：等 `--prediction-date` 當天凌晨的夜盤結束、你能在 <https://www.taifex.com.tw/cht/3/futDailyMarketReport> 查到那筆資料後，先用第 4 節的 `set_night_futures.py` 輸入，再單獨跑預測：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-14 --change 0.87
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-11 --prediction-date 2026-09-14
```

`--universe-date` 維持跟第一段 `run_daily` 用的 `--as-of` 一樣。忘記先做 `set_night_futures` 的話，`predict.py` 會直接報錯拒絕預測，不會猜一個值頂著跑。

（如果不想分兩段、想維持原本一次跑完的習慣，也可以不加 `--skip-predict`，但那樣 `predict` 那一步幾乎一定會因為當天夜盤資料還沒出現而報錯——分兩段是配合這個限制的正確用法。）

每日續訓預設 `daily_training_mode="incremental_replay"`：新增序列依「目標日」判定，範圍為前一 checkpoint 的 `training_as_of` 之後至本次 `--as-of`，全部納入；輸入仍保留每筆目標日前完整的 context。歷史重播從前次截止日以前、且落在 `--training-window-days` 滾動視窗內的序列抽樣，預設最多為新增筆數的 1 倍（`daily_replay_ratio=1.0`），總上限 4096（`daily_replay_max_sequences`），每股最多 128（`daily_replay_per_symbol`）。抽樣時 `intraday_up_1plus` 為真的日子權重是一般日子的 `daily_replay_hit_oversample`（預設 `4.0`）倍。

同一模型已訓練到本次截止日時，預設跳過；沒有新增目標時預設跳過，不產生新模型（最常見原因就是忘記匯入當天夜盤資料，見第 4 節）。如需明確重做，可加 `--force-retrain`。

### 怎麼看預測結果

上面第二段的 `predict` 跑完，會產生三個東西，依「想快速看還是想細看」選：

1. **最快**：直接看 `predict.py` 執行時的控制台輸出，會列出「預測資料覆蓋 X/Y 支」跟每一筆符合門檻的訊號。
2. **人看的摘要**：`signal_reports/<prediction-date>.txt`——只列出符合 `long_intraday_up_1plus >= 0.6`（或你設定的門檻）的股票跟機率，格式是：
   ```text
   [LONG] <股票代號> prediction_date=2026-09-07 intraday_up_1plus.T=0.7231
   ```
   沒有訊號的股票不會出現在這個檔案裡；如果整批都沒有訊號，會印「沒有符合訊號門檻的標的」。
3. **完整資料**：`predictions/<prediction-date>.json`——每一支有納入預測的股票的完整機率（`intraday_up_1plus.T`/`intraday_up_1plus.F`），還有被跳過的股票清單跟原因（`skipped`，例如 `insufficient_history`、`stale_features`）、這次用的 checkpoint 的 `naive_baseline`（訓練視窗多數類別是什麼）跟 `in_sample_loss`（訓練視窗內的 loss，可以跟隔天驗證後的 `evaluations/<date>.json` 裡的 log-loss 對照，檢查有沒有過擬合，見第 11 節的回測報告說明，觀念相通）。

隔天（第 7 節）驗證完，`data/evaluations/<prediction-date>.json` 會補上這批訊號的實際結果（`actual_intraday_up_1plus`）跟 recall/precision，這時候才知道昨天的訊號準不準。

### 預測資料檢查

預測逐股要求 `input_last_date == universe-date` 且滿足 context 長度。缺特徵檔、截止日前無資料、資料過期或歷史不足的股票會跳過；控制台與訊號報告會列出原因、預期與實際日期及覆蓋數，預測 JSON 保存 `skipped`、`active_count` 與 `predicted_count`。全部不合格時報錯，不寫入或覆蓋預測檔（最常見原因一樣是忘記匯入當天夜盤資料）。此檢查不判定當日日 K 是否已收盤，仍須使用完整交易日。

玉山若回傳 OHLC 含 `null`、非正價格或最高價低於最低價的歷史列，更新程式會顯示 `[WARN]` 並略過；不會以0補成假行情。成交量單獨為空時則保存為0，特徵化後標記為 `X`。

## 9. Post-Training Gate

完成每日第 7 節驗證並累積 evaluation 後，可每隔約 20 個已驗證交易日執行一次本節。也可提早查看，但不建議只根據單日結果調參。這是手動查看統計的頻率，不是自動排程。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.post_training_gate
```

`post_training_gate.py` 預設讀取最近 20 個已驗證交易日，並額外打印最近 5 日概況。只針對 `signals` 統計交易成功率（沿用第 7 節提到、沒有考慮日內先後順序的 3%/2% 規則，只當參考）、平均最佳順向價差、平均收盤價差與平均逆向價差。建議會輸出 `KEEP`、`RAISE_THRESHOLD_OR_PAUSE`、`KEEP_OR_LOWER_SLIGHTLY`。目前 gate 只打印建議，不會自動修改 `settings.json`、模型 checkpoint 或訊號門檻。

## 10. 除權息及特殊參考

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
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-04 --training-window-days 150
```

`resync_corporate_actions.py` 預設使用 `stock_data.py` 目前的 `selected_stocks`；若只要重建部分股票，可加上 `--symbols 2330 2464`。它只處理 FinMind 公司行動資料，不會登入玉山 SDK，也不會重抓日 K。

FinMind Token 不是必填；程式預設匿名存取。若日後需要較高流量，在本機設定環境變數 `FINMIND_TOKEN`，不要將 Token 寫入程式、README 或 Git。

## 11. Walk-forward 回測（選用工具）

不屬於日常操作流程，用來在不影響正式環境的前提下，檢驗訓練設定（滾動視窗長度、重新訓練頻率、loss 權重等）跨不同歷史區間的表現，而不用只能一天一天等真實新資料累積。這是目前主要用來驗證過各版設計決策的工具。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.walk_forward_backtest_v2 --output-dir C:\tmp\一個新目錄 --settings Z_ORB_ONE/stock_model_gpt/settings.json --training-window-days 150 --reseed-interval-days 20 --backtest-days 80
```

- `--training-window-days`：每次訓練（重新校準或每日續訓）只用截止日往前數的 N 個交易日，對應第 6 節的滾動視窗。
- `--reseed-interval-days`：每隔幾個交易日做一次完整重新訓練（權重、optimizer 全部重置），對應第 6 節「定期重新訓練」。
- `--backtest-days` / `--start-date` / `--max-as-of`：控制回測要涵蓋哪一段歷史；不指定 `--start-date` 時，用 `--backtest-days` 從最新可用資料往回推。**受第 4 節夜盤資料涵蓋範圍限制**——沒有夜盤資料的日期不會產生特徵，回測涵蓋範圍不能超出你已匯入的夜盤資料區間。
- `--output-dir` 每次都要換新的，不要重複用同一個資料夾跑不同設定（除非是要續跑同一組設定中斷的進度，不加 `--restart` 就會接著跑）。

**每天逐日模擬**（訓練 → 預測 → 驗證，重複整個回測期間的每一天），不是像早期版本那樣「一個 fold 訓練一次、之後凍結預測一段時間」，所以測得到 `daily_replay_hit_oversample` 這類只在每日續訓才啟動的機制。全部 predict/validate/train 都在同一個 process 內直接呼叫函式（不是每次開子行程），因為逐日模擬的呼叫次數比舊版多了一個數量級，開子行程的固定成本會拖垮整體時間。

**資料隔離**：透過 `STOCK_MODEL_GPT_WRITE_ROOT` 環境變數，把 `checkpoints/`、`predictions/`、`data/evaluations/`、`data/actual_candles/`、`data/universe/`、`signal_reports/` 全部重新導向到 `--output-dir` 底下，不會覆寫或污染正式環境的同名檔案；`data/candles`、`data/features`、`data/corporate_actions`、`data/night_futures.jsonl`、`stock_data.py` 維持唯讀共用。

**已知偏誤（不是 bug，是方法論限制）**：每個模擬的歷史日期，股票清單都是用「現在」的 `selected_stocks` 回頭套用，不是那個時間點真正會選的清單，結果相對於「真的從那個時間點開始上線」會偏樂觀（look-ahead / survivorship bias）。滾動視窗已經避免「訓練資料無限往回累積」這個問題，但不會消除這條 universe 偏誤。

結果存在 `<output-dir>/backtest_report_v2.json`：

- `overall.intraday_up_1plus_accuracy` / `overall.naive_baseline.intraday_up_1plus_accuracy`：模型 vs. 「只猜訓練視窗多數類別」的命中率對照。
- `overall.intraday_up_1plus.recall` / `.precision`：實際交易會用到的門檻下，抓得到幾成、喊中幾成。
- `overall.log_loss.intraday_up_1plus` / `.baseline_intraday_up_1plus`：隔天實際結果的機率校準對照（out-of-sample）。
- `overall.in_sample_loss.intraday_up_1plus`：訓練視窗內的機率校準（in-sample）；跟上面的 `log_loss` 一起看可以判斷有沒有過擬合——差距很小代表沒有，差距很大（例如好幾倍）才要擔心。
- `days[]`：每個交易日的細項，包含當天是不是「重新訓練」那天（`reseeded`）、用的是哪個 checkpoint。

## 12. 已知限制

- **夜盤期指沒有自動抓取**：第 4 節說明的手動輸入／匯入流程，是目前唯一的資料來源；忘記做會讓當天的訓練/預測悄悄失效或直接報錯（`prediction_date` 自己那筆缺少時是報錯，見下一點；context 裡某天缺少時是悄悄跳過那天）。如果之後要接自動化，需要先找到期交所穩定可用的下載 API（目前確認互動查詢頁面是 JS 動態產生連結，直接爬 HTML 抓不到）。
- **`predict` 現在得等隔天凌晨、`prediction_date` 自己的夜盤結束才能跑**：見第 2 節「第八項輸入」跟第 8 節「續訓與預測（分兩段）」。這代表原本「收盤後一次跑完 `run_daily`」的習慣不能再用，`run_daily` 收盤後只能先做到續訓（`--skip-predict`），預測要隔天另外執行，且必須先用 `set_night_futures.py` 補上當天的資料。這個時間差目前也沒有自動化，一樣要人工在對的時間點做對的事。
- **股票清單的 look-ahead / survivorship bias**：見第 11 節，回測結果用的是現在的股票清單回頭套用到歷史，不是當時真正的清單。
- **checkpoint gate 只是煞車，不是完整的模型比較機制**：見第 7 節，`DEGRADED` 只代表「最近變差了，人去看一下」，沒有離線回測驅動的自動發布/回退決策。
- **3%/2% 停損停利規則已經不是主要判斷依據**：`evaluate_signal_trade` 跟 `success` 欄位還留著（`post_training_gate.py` 也還在用），但因為沒有日內先後順序資訊會系統性偏樂觀，回測跟驗證都改看 recall/precision/log-loss；這組舊邏輯之後可以考慮整個拿掉。
- **`price`／`hit_up`／`hit_down` 仍是輸入特徵，但模型不會拿它們當答案去學**：目前唯一答案是 `intraday_up_1plus`。如果之後想重新啟用其中一項當輸出目標，回去看這份 README 開頭跟 `training.py`/`model.py` 的 `ensure_checkpoint_compatible()`，裡面有擋掉舊 checkpoint 相容性的判斷邏輯可以參考怎麼加回去。
- **定期重新訓練沒有自動排程**：第 6 節提到大約每 20 個交易日重跑一次 `train_initial`，目前是要人工記得執行，`run_daily.py` 不會自動觸發。


初始訓練
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-14
python -m Z_ORB_ONE.stock_model_gpt.prepare_features --as-of 2026-09-14
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-14 --training-window-days 150
初次預測
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-15 --change-pct <夜盤漲跌百分比>
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-14 --prediction-date 2026-09-15


預設門檻報告為 0.6 , 改變預測門檻的指令。
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-14 --prediction-date 2026-09-15 --signal-threshold 0.5
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-14 --prediction-date 2026-09-15 --signal-threshold 0.7


每日更新順序，以 9/15 這個營業日為例
1. 下午 15:30 先更新stock_data.py, 再更新股票指數續訓
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-15 --prediction-date 2026-09-16 --training-window-days 150 --skip-predict
2. 隔日（9/16）上午 05:00 後更新夜盤指數
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-16 --change-pct <夜盤漲跌百分比>
3. 接著預測次一營業日 9/16
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-15 --prediction-date 2026-09-16
