# -*- coding: utf-8 -*-
"""
cointAndZGrid_EngleGranger_OneStep.py

單一步驟：
- 讀取股票清單 → 兩兩配對
- 訓練期做 EG（雙向、trend='c'、autolag='BIC'），雙向皆通過；擇 p 較小方向
- 該方向以 OLS(含常數) 估 α、β；以訓練期 spread 統計標準化 z
- 在同檔內直接跑 z-score 網格（沿用 V2 的 in/out 網格）
- 每個 z-組合層級套用：
    * Sharpe(train/test) ≥ 1.5
    * 交易次數：train ≥ 20、test ≥ 5
    * 最長回撤期間 maxDDD 用單檔一致邏輯
- 於通過組合中挑最佳（test Sharpe 主、train Sharpe 次、再以較小 maxDD、較多交易次數）
"""

import os
import json
import sys
import time
from datetime import datetime, timedelta
from itertools import combinations, repeat
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
from zscoreSingleGraphSharpKelly_EngleGranger import find_max_abs_z, find_longest_z_episode
import globals

# ======== 你的專案依賴 ========
from GetStockData.stockOtherInfo import lookup_name_by_code, get_stock_codes

# ========= 參數 =========
# EG 與訓練/測試長度約束
P_THRESHOLD: float = 0.05
TRAIN_TEST_MIN_RATIO: float = 4  # 訓練:測試 至少 4:1（80/20）

# Sharpe門檻
TRAIN_SHARP_LIMIT = 2.5
TEST_SHARP_LIMIT  = 2.5

# 交易次數門檻
MIN_TRADES_TRAIN  = 8
MIN_TRADES_TEST   = 2

# 允許最大回撤區間
ALLOW_MAX_DDD = 10

# z-score 網格（沿用你的 V2 設定：進場/平倉各自列表）
Z_IN_GRID  = [1.25, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0]
Z_OUT_GRID = [0.5, 0.75, 1.0, 1.25, 1.5]

# 年化參數
ANNAL_RF     = 0.018
YEAR_PERIODS = 252

# 多進程設定
def _suggest_workers() -> int:
    n = os.cpu_count() or 8
    return max(4, min(n - 2, 10))
PROCESS_WORKERS: Optional[int] = None
CHUNKSIZE_PROCESS: int = 150

# ========= 檔案載入 =========
def load_stock(code: str) -> pd.DataFrame:
    p_path = f"../stocks/{code}.parquet"
    df = pd.read_parquet(p_path)

    #print(df) # leo測試用

    return df

'''
def load_candles(code: str) -> pd.DataFrame:
    return None

def convert_candle_to_df(candle_data: dict) -> pd.DataFrame:
    """
    將券商 API 取得的 1 分 K 轉成標準 DataFrame：
    Columns = Date, Open, High, Low, Close, Volume, Adj Close
    日期依時間遞增排序
    """
    rows = []

    for item in candle_data.get("data", []):
        # 取出 datetime 前 16 字，例如 "2025-11-26T13:22"
        date_str = item["date"][:16]

        rows.append({
            "Date": date_str,
            "Open": item["open"],
            "High": item["high"],
            "Low": item["low"],
            "Close": item["close"],
            "Volume": item["volume"],
            "Adj Close": item["close"],   # 同 close
        })

    # 建立 DataFrame
    df = pd.DataFrame(rows)

    # 排序（Date 字串可直接排序；若轉 datetime 也可以）
    df = df.sort_values("Date").reset_index(drop=True)

    return df
'''

# ========= 指標 =========
def annualized_sharpe(r, rf_annual=0.0, periods_per_year=252, ddof=1):
    r = pd.Series(r).dropna()
    if r.empty: return np.nan
    r_excess = r - rf_annual / periods_per_year
    mu_ann   = r_excess.mean() * periods_per_year
    sigma_ann= r_excess.std(ddof=ddof) * np.sqrt(periods_per_year)
    return np.nan if sigma_ann == 0 else float(mu_ann / sigma_ann)

def kelly_fraction(r, rf_annual=0.0, periods_per_year=252, ddof=1):
    r = pd.Series(r).dropna()
    if r.empty: return np.nan
    mu_excess = r.mean() - rf_annual / periods_per_year
    var = r.var(ddof=ddof)
    return np.nan if var == 0 else float(mu_excess / var)


def find_longest_dd_episode_until(cumret: np.ndarray, last_idx: int):
    """
    取到 last_idx（含）為止的最長回撤「期間」：
      1) 先在已恢復段中挑最長（同長取最近）
      2) 若最後一段延伸至尾端且長度 ≥ 既往最長，視為最長（ongoing=True）
    回傳 dict: {t_start, t_end, t_recover, length, ongoing}
    """
    wealth = 1.0 + np.asarray(cumret, float)
    wealth = np.nan_to_num(wealth, nan=1.0)
    n_all = len(wealth)
    if n_all == 0:
        return dict(t_start=None, t_end=None, t_recover=None, length=0, ongoing=False)

    last_idx = int(max(0, min(last_idx, n_all - 1)))
    wealth = wealth[: last_idx + 1]

    hwm = np.maximum.accumulate(wealth)
    dd = (hwm - wealth) / np.where(hwm == 0.0, np.nan, hwm)
    dd = np.nan_to_num(dd, nan=0.0)

    n = len(dd)
    if n == 0:
        return dict(t_start=None, t_end=None, t_recover=None, length=0, ongoing=False)

    completed, ongoing = [], None
    i = 0
    while i < n:
        if dd[i] <= 0:
            i += 1
            continue
        s = i
        while i + 1 < n and dd[i + 1] > 0:
            i += 1
        e = i
        L = e - s + 1
        if e < n - 1 and dd[e + 1] == 0:
            completed.append((s, e, L))
        else:
            ongoing = (s, e, L)
        i += 1

    comp_best = None
    if completed:
        max_len = max(seg[2] for seg in completed)
        comp_best = [seg for seg in completed if seg[2] == max_len][-1]

    if ongoing is not None:
        s, e, L = ongoing
        if comp_best is None or (L >= comp_best[2]):
            return dict(t_start=s, t_end=e, t_recover=None, length=L, ongoing=True)

    if comp_best is not None:
        s, e, L = comp_best
        return dict(t_start=s, t_end=e, t_recover=e + 1, length=L, ongoing=False)

    return dict(t_start=None, t_end=None, t_recover=None, length=0, ongoing=False)

# ========= 工具 =========
def count_round_trips(pos_series: pd.Series) -> int:
    """由 +1/-1/0 持倉序列估 round-trip 次數。"""
    s = pos_series.fillna(0.0)
    entries = ((s.shift(1) == 0) & (s != 0)).sum()
    exits   = ((s.shift(1) != 0) & (s == 0)).sum()
    return int(min(entries, exits))

def effective_slices(idx: pd.DatetimeIndex, worker_train_start, worker_train_end, worker_test_start, worker_test_end) -> Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    ts = pd.to_datetime(worker_train_start); te = pd.to_datetime(worker_train_end)
    vs = pd.to_datetime(worker_test_start);  ve = pd.to_datetime(worker_test_end)
    pair_start, pair_end = idx.min(), idx.max()
    ets = max(ts, pair_start); ete = min(te, pair_end)
    evs = max(vs, pair_start); eve = min(ve, pair_end)
    if evs <= ete:
        later = idx[idx > ete]
        if len(later) == 0:
            return None
        evs = later[0]
    if (ets > ete) or (evs > eve):
        return None
    return ets, ete, evs, eve

# ========= 單一 pair 核心：EG + OLS + 網格 =========
def process_pair_one_step(stock1: str, stock2: str, worker_train_start, worker_train_end, worker_test_start, worker_test_end) -> Optional[Dict[str, Any]]:
    # 讀 & 合併
    df1, df2 = load_stock(stock1).copy(), load_stock(stock2).copy()
    if df1.empty or df2.empty or "Date" not in df1.columns or "Date" not in df2.columns:
        return None
    df = pd.merge(df1, df2, on="Date", suffixes=("_A", "_B"))
    if df.empty: return None
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).copy().set_index("Date").sort_index()

    a_col = "Adj Close_A" if "Adj Close_A" in df.columns else ("Close_A" if "Close_A" in df.columns else None)
    b_col = "Adj Close_B" if "Adj Close_B" in df.columns else ("Close_B" if "Close_B" in df.columns else None)
    if (a_col is None) or (b_col is None): return None

    # 裁切區間（交集且 train/test 不重疊）
    eff = effective_slices(df.index, worker_train_start, worker_train_end, worker_test_start, worker_test_end)
    if eff is None: return None
    ets, ete, evs, eve = eff

    mask_train = (df.index >= ets) & (df.index <= ete)
    mask_test  = (df.index >= evs) & (df.index <= eve)

    train_df = df.loc[mask_train, [a_col, b_col]].dropna()
    test_df  = df.loc[mask_test,  [a_col, b_col]].dropna()
    if train_df.empty or test_df.empty: return None

    if len(train_df) < TRAIN_TEST_MIN_RATIO * len(test_df):
        # print(f'len train_df: {len(train_df)}, len test_df: {len(test_df)}')
        return None

    # EG 雙向（trend='c', autolag='BIC'）
    try:
        _, p_ab, _ = coint(train_df[a_col], train_df[b_col], trend="c", autolag="BIC")
        _, p_ba, _ = coint(train_df[b_col], train_df[a_col], trend="c", autolag="BIC")
    except Exception:
        return None

    if (p_ab > P_THRESHOLD) and (p_ba > P_THRESHOLD): # 如果雙向皆不顯著，退出
        # print(f'stock1: {stock1}, stock2: {stock2}, p_ab: {p_ab}, p_ba: {p_ba} 皆不顯著')
        return None

    # 擇 p 較小方向：y ~ α + β x
    if p_ab <= p_ba:
        y_col, x_col = a_col, b_col
        y_code, x_code = stock1, stock2
    else:
        y_col, x_col = b_col, a_col
        y_code, x_code = stock2, stock1

    # OLS(含常數) 估 α、β（以訓練期）
    X = sm.add_constant(train_df[x_col].values)
    y = train_df[y_col].values
    model = sm.OLS(y, X).fit()
    alpha = float(model.params[0])
    beta = float(model.params[1])
    if (beta > 3 or beta < 0.3) :
        return None

    # 訓練期 spread 統計
    spread_train = (train_df[y_col] - alpha) - beta * train_df[x_col]
    mean = float(spread_train.mean())
    std = float(spread_train.std())
    if std == 0 or np.isnan(std):
        return None

    # 全期 z、報酬
    spread_all = (df[y_col] - alpha) - beta * df[x_col]
    z_all = (spread_all - mean) / std
    dailyret = df[[y_col, x_col]].pct_change()
    dailyret.columns = ["Y", "X"]

    # 網格搜尋（每組檢查 Sharpe 與 交易次數）
    best = None
    idx = df.index
    z = pd.Series(z_all.values, index=idx)
    ret = dailyret.copy()
    for z_in in Z_IN_GRID:
        for z_out in Z_OUT_GRID:
            if z_out >= z_in:  # 平倉門檻須小於進場
                continue

            # 建倉/平倉
            pos_Y_long  = pd.Series(np.nan, index=idx, dtype=float); pos_Y_long.iloc[0]  = 0.0
            pos_X_long  = pd.Series(np.nan, index=idx, dtype=float); pos_X_long.iloc[0]  = 0.0
            pos_Y_short = pd.Series(np.nan, index=idx, dtype=float); pos_Y_short.iloc[0] = 0.0
            pos_X_short = pd.Series(np.nan, index=idx, dtype=float); pos_X_short.iloc[0] = 0.0

            pos_Y_short[z >=  z_in] = -1.0; pos_X_short[z >=  z_in] =  1.0
            pos_Y_long [z <= -z_in] =  1.0; pos_X_long [z <= -z_in] = -1.0

            pos_Y_short[z <=  z_out] = 0.0; pos_X_short[z <=  z_out] = 0.0
            pos_Y_long [z >= -z_out] = 0.0; pos_X_long [z >= -z_out] = 0.0

            pos_Y = pos_Y_long.ffill().add(pos_Y_short.ffill(), fill_value=0.0)
            pos_X = pos_X_long.ffill().add(pos_X_short.ffill(), fill_value=0.0)

            pnl = (pos_Y.shift(1) * ret["Y"] + (-beta) * pos_X.shift(1) * ret["X"]).fillna(0.0)

            tr_idx = np.flatnonzero(mask_train); te_idx = np.flatnonzero(mask_test)
            if tr_idx.size <= 1 or te_idx.size == 0:
                continue
            pnl_train = pnl.iloc[tr_idx].dropna(); pnl_test = pnl.iloc[te_idx].dropna()

            # 交易次數（在 z-組合層級）
            pos_train = pos_Y.iloc[tr_idx]; pos_test = pos_Y.iloc[te_idx]
            n_tr_train = count_round_trips(pos_train); n_tr_test = count_round_trips(pos_test)
            if (n_tr_train < MIN_TRADES_TRAIN) or (n_tr_test < MIN_TRADES_TEST):
                continue

            # Sharpe 門檻
            s_train = annualized_sharpe(pnl_train, rf_annual=ANNAL_RF, periods_per_year=YEAR_PERIODS, ddof=1)
            s_test  = annualized_sharpe(pnl_test,  rf_annual=ANNAL_RF, periods_per_year=YEAR_PERIODS, ddof=1)
            if (np.isnan(s_train) or np.isnan(s_test) or s_train < TRAIN_SHARP_LIMIT or s_test < TEST_SHARP_LIMIT):
                continue
            if s_train > s_test:
                continue

            # ====== 這裡開始：新的「z 版」 Max DD 與 Longest DD ======
            # 1) Max |z|
            zmax_info = find_max_abs_z(z)
            k_zmax, t_zmax, _zv = zmax_info
            maxDD = abs(_zv)
            # 2) Longest z-episode
            #   測試集最後一天 timestamp：給「未回復也可算最長」的規則使用（實作上已用 end==最後一根判斷）
            test_last_ts = df.index[mask_test][-1]
            ldd = find_longest_z_episode(z, z_in, z_out, test_last_ts)
            ldd_len = int(ldd['length'])
            if ldd_len > ALLOW_MAX_DDD:
                continue

            # 回撤（深度 + 最長期間）
            # cumret_all = calculateCumret(pnl.values.astype(float))
            # maxDD, _, _, _ = calculateMaxDD(cumret_all)

            last_test_idx = te_idx[-1]
            # ldd_ep = find_longest_dd_episode_until(cumret_all, last_test_idx)
            # ldd_len = int(ldd_ep["length"])

            # 排序鍵（test Sharpe 主、train Sharpe 次、較小回撤日、較小回撤值、較多交易次數）
            key = (round(s_test, 6), round(s_train, 6), -int(ldd_len), -round(maxDD, 6), int(n_tr_test), int(n_tr_train))
            if (best is None) or (key > best["key"]):
                best = dict(
                    key=key, z_in=z_in, z_out=z_out,
                    metrics=dict(
                        sharpe_train=float(s_train), sharpe_test=float(s_test),
                        kelly_train=float(kelly_fraction(pnl_train, ANNAL_RF, YEAR_PERIODS, 1)),
                        kelly_test=float(kelly_fraction(pnl_test,  ANNAL_RF, YEAR_PERIODS, 1)),
                        maxDD=float(maxDD), maxDDD=int(ldd_len),
                        n_trades_train=int(n_tr_train), n_trades_test=int(n_tr_test),
                    )
                )

    if best is None:
        return None

    # 組輸出（名稱查詢）
    nameOne = lookup_name_by_code(y_code)
    nameTwo = lookup_name_by_code(x_code)

    z_in, z_out = best["z_in"], best["z_out"]
    met = best["metrics"]

    result = {
        "id": f"{y_code}-{x_code}",
        "nameOne": nameOne,
        "nameTwo": nameTwo,
        "nameOneCode": y_code,
        "nameTwoCode": x_code,
        "nameOneQty": 0,
        "nameTwoQty": 0,
        "alpha": round(alpha, 6),
        "beta":  round(beta, 6),
        "spreadMean": round(mean, 4),
        "spreadStd":  round(std, 4),
        "ztop":  round(z_in, 2),
        "zdown": round(z_out, 2),
        "status": "C",
        "onsite": "T/T",
        "comment": (
            f"z-Interval:{z_in:.2f}-{z_out:.2f},"
            f"Sharp:{met['sharpe_train']:.4f}/{met['sharpe_test']:.4f},"
            f"Kelly:{met['kelly_train']:.4f}/{met['kelly_test']:.4f},"
            f"Drawdown:{met['maxDD']:.4f}/{met['maxDDD']},"
            f"TradeCount:{met['n_trades_train']}/{met['n_trades_test']}"
        )
    }

    '''
    print(f"股票組合: {nameOne}:{nameTwo} {y_code}-{x_code}")
    print(f"訓練期 OLS(含常數) α={alpha:.6f}, β={beta:.6f}, std={std:.4f}")
    print(f"z-Interval:{z_in:.2f}-{z_out:.2f}")
    print(f"Sharp:{met['sharpe_train']:.4f}/{met['sharpe_test']:.4f},Kelly:{met['kelly_train']:.4f}/{met['kelly_test']:.4f}")
    print(f"Drawdown:{met['maxDD']:.4f}/{met['maxDDD']},TradeCount:{met['n_trades_train']}/{met['n_trades_test']}")
    print("-" * 50)
    '''

    return result

# ========= 多進程包裝 =========
def _pair_worker(args, worker_train_start, worker_train_end, worker_test_start, worker_test_end) -> Optional[Dict[str, Any]]:
    s1, s2 = args
    try:
        return process_pair_one_step(s1, s2, worker_train_start, worker_train_end, worker_test_start, worker_test_end)
    except Exception:
        return None

def print_many():
    # 四個月
    local_train_start_data = globals.train_start_data
    local_train_end_data = globals.train_end_data

    # 一個月
    local_test_start_data = globals.test_start_data
    local_test_end_data = globals.test_end_data

    # 轉成 datetime
    train_start = datetime.strptime(local_train_start_data, '%Y-%m-%d')
    train_end = datetime.strptime(local_train_end_data, '%Y-%m-%d')
    test_start = datetime.strptime(local_test_start_data, '%Y-%m-%d')
    test_end = datetime.strptime(local_test_end_data, '%Y-%m-%d')

    weeks = 6  # ← 你要跑幾次，自己指定

    for i in range(weeks):
        print(f'Round {i + 1}')
        print('train_start_data:', train_start.strftime('%Y-%m-%d'))
        print('train_end_data  :', train_end.strftime('%Y-%m-%d'))
        print('test_start_data :', test_start.strftime('%Y-%m-%d'))
        print('test_end_data   :', test_end.strftime('%Y-%m-%d'))
        print('-' * 40)

        results = print_once(train_start, train_end, test_start, test_end)

        pairSharpKellyResultsPath = f"../papers/pairs_data_Round{i + 1}.json"
        with open(pairSharpKellyResultsPath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        delta = timedelta(days=7)
        train_start -= delta
        train_end -= delta
        test_start -= delta
        test_end -= delta

def print_once(worker_train_start, worker_train_end, worker_test_start, worker_test_end) -> List[Dict[str, Any]]:
    codes = get_stock_codes()
    pairs = list(combinations(codes, 2))

    results: List[Dict[str, Any]] = []

    workers = PROCESS_WORKERS if PROCESS_WORKERS is not None else _suggest_workers()
    done = 0
    start = time.time()
    tick_every = max(1, len(pairs) // 200)

    with ProcessPoolExecutor(max_workers=workers) as ex:
        for out in ex.map(_pair_worker, pairs, repeat(worker_train_start), repeat(worker_train_end), repeat(worker_test_start), repeat(worker_test_end), chunksize=CHUNKSIZE_PROCESS):
            done += 1
            if out:
                results.append(out)
            if done % tick_every == 0:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                sys.stderr.write(f"\r進度 {done}/{len(pairs)}（{done/len(pairs):.1%}） | {rate:.1f} pairs/s")
                sys.stderr.flush()

    sys.stderr.write("\n")
    sys.stderr.flush()

    # 印出結果
    for res in results:
        print(f"{res["nameOne"]}:{res["nameTwo"]} {res["id"]}")
        print(f"α={res["alpha"]:.6f}, β={res["beta"]:.6f}, std={res["spreadStd"]:.4f}")

        cmt = res["comment"].split(',')
        print(f"{cmt[0]}")
        print(f"{cmt[1]}, {cmt[2]}")
        print(f"{cmt[4]}, {cmt[3]}")

        print("-" * 50)

    return results


def print_once_with_json():
    results = print_once(globals.train_start_data, globals.train_end_data, globals.test_start_data, globals.test_end_data)

    pairSharpKellyResultsPath = "../papers/pairs_data_0.json"
    with open(pairSharpKellyResultsPath, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"已將結果寫入 {pairSharpKellyResultsPath}；總計 {len(results)} 組。")

if __name__ == "__main__":
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}-start")
    print_many()
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}-finish")
