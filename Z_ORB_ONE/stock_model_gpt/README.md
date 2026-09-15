# stock_model_gpt 操作筆記

所有指令從專案根目錄執行，範例日期請換成實際交易日。

- **輸入**：股票當日的開、高、低、收、觸漲停、觸跌停、收盤漲跌停狀態、成交量、ATR，共九項，加上**下一交易日的夜盤**，合計十項。
- **輸出**：下一交易日 `high_price` 的五種機率：`P(-2)`、`P(-1)`、`P(0)`、`P(1)`、`P(2)`。
- **篩選預設**：`P(1) + P(2) ≥ 60%`。

## 1. 初始訓練

先確認歷史夜盤資料已匯入 `data/night_futures.jsonl`，且涵蓋所需日期。股票資料更新不會自動抓夜盤；缺夜盤會造成特徵或訓練序列被略過。

以完整股票資料截止 **2026-09-15** 為例：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.update_data --as-of 2026-09-15
python -m Z_ORB_ONE.stock_model_gpt.prepare_features --as-of 2026-09-15
python -m Z_ORB_ONE.stock_model_gpt.train_initial --as-of 2026-09-15 --training-window-days 150
```

- `--as-of`：完整日 K／訓練資料的截止日期。
- `--training-window-days 150`：訓練目標取最近 150 個可用交易日；每筆目標仍需要此前的歷史輸入序列，不代表只需準備 150 天原始資料。
- `train_initial` 從頭訓練；用於新版五分類預測。

## 2. 初次預測

預測 **2026-09-16**：等歸屬 9/16 的夜盤結束後，開盤前補入夜盤，再執行預測。

以下 `0.87` 只是範例，請替換成實際夜盤漲跌百分比；上漲 0.87% 填 `0.87`，下跌 0.6% 填 `-0.6`。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-16 --change 0.87
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-15 --prediction-date 2026-09-16
```
predict的其它預設參數 --signal-classes "1,2" --signal-threshold-pct 60

### 查看結果

- **控制台／`signal_reports/YYYY-MM-DD.txt`**：同一份摘要，列出符合條件股票的五種機率、所選刻度合計機率、最高機率類別；依合計機率由高到低排序。
- **`predictions/YYYY-MM-DD.json`**：保存所有完成預測股票的五種機率與篩選設定，不符合門檻的股票也會保留。

## 4. 每日更新順序

以 **9/16 收盤後更新、預測 9/17** 為例。

### 步驟一：收盤後更新股票資料並續訓

確認日 K 完整後（例如下午 15:30），先更新 `Z_ORB_ONE/stock_data.py` 的股票清單，並確認歸屬 **9/16** 的夜盤已輸入(其實早上 05:00 後已輸入)，再執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.run_daily --as-of 2026-09-16 --training-window-days 150
```

啟動時會先檢查 `--as-of` 當天的夜盤資料；缺少時在控制台提示日期，並以錯誤狀態中止，不執行資料更新、特徵產生或續訓。補入該日期夜盤後，再重新執行。

### 步驟二：驗證先前對 9/16 的預測

`run_daily` 不會自動驗證。如果已有 9/16 的預測檔，股票資料更新後執行：

```powershell
python -m Z_ORB_ONE.stock_model_gpt.validate_predictions --prediction-date 2026-09-16
```

預設沿用該預測檔保存的類別與門檻，也可用第 3 節兩個參數覆寫。結果保存至 `data/evaluations/2026-09-16.json`，包括五分類準確率、各刻度 precision／recall，以及所選刻度合併後的 precision／recall。

若有移除股票，請先更新其實際行情並完成前次預測驗證，再從清單移除，避免缺少驗證資料。

### 步驟三：9/17 夜盤結束後補入資料

一般平日於上午 05:00 夜盤結束、取得收盤資料後輸入；跨週末／連假按第 2 節的日期規則處理。

```powershell
python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-17 --change 0.87
```

請將 `0.87` 換成實際夜盤漲跌百分比。

### 步驟四：開盤前預測 9/17

```powershell
python -m Z_ORB_ONE.stock_model_gpt.predict --universe-date 2026-09-16 --prediction-date 2026-09-17 --signal-classes "1,2" --signal-threshold-pct 60
```
