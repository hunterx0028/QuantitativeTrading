# -*- coding: utf-8 -*-
"""
zscoreSingleGraphSharpKelly_EngleGranger.py
單一配對：EG(訓練期) → OLS(含常數) 估 α/β → z-score 產持倉 → 回測 → 三張圖

本版重點：
1) 完整刪除舊的 Max DD / Max DDD / Longest DD（基於 PnL/wealth）計算與繪圖
2) 重新定義：
   - Max DD (z)：全期間 |z| 最大之時點（紅色短實線）
   - Longest DD (z-episode)：當 z 觸發 +Z_IN（或 -Z_IN）後，最久多久回到 Z_OUT（或 -Z_OUT）
     若最後一天仍未回到，且這段長度 ≥ 既往最長，則用「最後一天」作為終點（藍色虛線兩條）
3) 視覺標記仍放在第 3 張（累積 PnL）圖，以利與績效曲線對照
"""

import os
import json
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import coint
import matplotlib.pyplot as plt
import matplotlib
from sympy.abc import alpha

matplotlib.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS']
matplotlib.rcParams['axes.unicode_minus'] = False

# ====== 你的專案設定 ======
from globals import (
    train_start_data, train_end_data,
    test_start_data,  test_end_data,
)
from GetStockData.stockOtherInfo import lookup_name_by_code

PAIRS_JSON_PATH = "../papers/pairs_data.json"

# ===== (C) 讀取 JSON 參數 =====
def load_pairs_params(json_path: str) -> dict[str, dict]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {str(d.get("id")).strip(): d for d in data}

def get_pair_record(pairs_map: dict, pair_id: str) -> dict | None:
    rec = pairs_map.get(pair_id)
    if rec is not None:
        return rec
    if "-" in pair_id:  # 允許反向查一次
        a, b = pair_id.split("-", 1)
        return pairs_map.get(f"{b}-{a}")
    return None

# ====== 讀檔 ======
def load_stock(code: str) -> pd.DataFrame:
    """優先讀 parquet，否則讀 xlsx；輸出含 Date / Close / Adj Close。"""
    p = f"../stocks/{code}.parquet"
    x = f"../stocks/{code}.xlsx"
    if os.path.exists(p):
        df = pd.read_parquet(p)
    else:
        df = pd.read_excel(x)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])

    start = pd.to_datetime(train_start_data)
    return df[df["Date"] >= start].reset_index(drop=True)

# ====== 指標 ======
def annualized_sharpe(r, rf_annual=0.0, periods_per_year=252, ddof=1):
    s = pd.Series(r).dropna()
    if s.empty: return np.nan
    re = s - rf_annual / periods_per_year
    mu = re.mean() * periods_per_year
    sd = re.std(ddof=ddof) * np.sqrt(periods_per_year)
    return np.nan if sd == 0 else float(mu / sd)

def kelly_fraction(r, rf_annual=0.0, periods_per_year=252, ddof=1):
    s = pd.Series(r).dropna()
    if s.empty: return np.nan
    mu_ex = s.mean() - rf_annual / periods_per_year
    var = s.var(ddof=1)
    return np.nan if var == 0 else float(mu_ex / var)

def calculate_cumret(returns: np.ndarray):
    r = np.asarray(returns, float)
    r = np.nan_to_num(r, nan=0.0)
    wealth = np.cumprod(1.0 + r)
    return wealth - 1.0

# ====== 新增：Max |z| 與 Longest z-episode ======
def find_max_abs_z(z: pd.Series):
    """回傳 (idx_pos, timestamp, z_value) 使 |z| 最大；若 z 全 NaN 回 None."""
    if z.isna().all(): return None
    k = int(np.nanargmax(np.abs(z.values)))
    return k, z.index[k], float(z.iloc[k])

def find_longest_z_episode(z: pd.Series, z_in: float, z_out: float, test_last_ts: pd.Timestamp):
    """
    找全期間（訓練+測試）最長的 z 偏離期：
    - 正向：當 z >= z_in → episode start，直到 z <= z_out 才算 recover
    - 負向：當 z <= -z_in → episode start，直到 z >= -z_out 才算 recover
    - 若最後一天仍未 recover，且該段長度 >= 歷史最長，視為最長，end = 最後一天

    回傳 dict:
      { 'start_idx','end_idx','start_ts','end_ts','length','sign','ongoing' }
      若找不到（從未觸發 ±z_in），回傳 None
    """
    idx = z.index
    n = len(z)
    if n == 0: return None

    def _scan(sign=+1):
        episodes = []
        ongoing = None
        i = 0
        while i < n:
            if sign > 0:
                # 觸發 +z_in
                if not (z.iloc[i] >= z_in):
                    i += 1; continue
                s = i
                # 直到回到 z_out（含）為止才恢復
                i += 1
                while i < n and not (z.iloc[i] <= z_out):
                    i += 1
                if i < n:
                    e = i  # 恢復點（含）
                    episodes.append((s, e, e - s + 1))
                else:
                    # 沒恢復 → ongoing 到最後
                    ongoing = (s, n - 1, (n - 1) - s + 1)
                    break
            else:
                # 觸發 -z_in
                if not (z.iloc[i] <= -z_in):
                    i += 1; continue
                s = i
                i += 1
                while i < n and not (z.iloc[i] >= -z_out):
                    i += 1
                if i < n:
                    e = i
                    episodes.append((s, e, e - s + 1))
                else:
                    ongoing = (s, n - 1, (n - 1) - s + 1)
                    break
            i += 1
        return episodes, ongoing

    pos_eps, pos_ongo = _scan(+1)
    neg_eps, neg_ongo = _scan(-1)

    # 已完成段中的最長（同長取最近）
    candidates = []
    if pos_eps:
        Lmax = max(e[2] for e in pos_eps)
        for e in pos_eps:
            if e[2] == Lmax:
                candidates.append(("pos", e))
    if neg_eps:
        Lmax = max(e[2] for e in neg_eps)
        for e in neg_eps:
            if e[2] == Lmax:
                candidates.append(("neg", e))

    best = None
    if candidates:
        # 同長取「最後出現的」
        best = candidates[-1]

    # 比對 ongoing 規則：若最後一段未恢復且長度 ≥ 既往最長 → 視為最長
    def _cmp_update(curr_best, ongoing, sign_label):
        if ongoing is None: return curr_best
        if curr_best is None or (ongoing[2] >= curr_best[1][2]):
            return (sign_label, ongoing)
        return curr_best

    if pos_ongo is not None: best = _cmp_update(best, pos_ongo, "pos")
    if neg_ongo is not None: best = _cmp_update(best, neg_ongo, "neg")

    if best is None:
        return None

    sign_label, (s, e, L) = best
    return dict(
        start_idx=s, end_idx=e,
        start_ts=idx[s], end_ts=idx[e],
        length=int(L),
        sign=sign_label,
        ongoing=(e == n - 1)
    )

# ====== 主流程（單一配對） ======
def run_single(pair_code_y: str, pair_code_x: str, z_in: float, z_out: float, alpha: float, beta: float):
    nameY = lookup_name_by_code(pair_code_y) or pair_code_y
    nameX = lookup_name_by_code(pair_code_x) or pair_code_x

    dfY = load_stock(pair_code_y); dfX = load_stock(pair_code_x)
    df = pd.merge(dfY, dfX, on="Date", suffixes=("_Y", "_X")).dropna(subset=["Date"])
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.set_index("Date").sort_index()

    y_col = "Adj Close_Y" if "Adj Close_Y" in df.columns else ("Close_Y" if "Close_Y" in df.columns else None)
    x_col = "Adj Close_X" if "Adj Close_X" in df.columns else ("Close_X" if "Close_X" in df.columns else None)
    if (y_col is None) or (x_col is None):
        raise RuntimeError("缺少收盤價欄位")

    # 時間裁切（交集且 train/test 不重疊）
    ts = pd.to_datetime(train_start_data); te = pd.to_datetime(train_end_data)
    vs = pd.to_datetime(test_start_data);  ve = pd.to_datetime(test_end_data)

    pair_min, pair_max = df.index.min(), df.index.max()
    ets, ete = max(ts, pair_min), min(te, pair_max)
    evs, eve = max(vs, pair_min), min(ve, pair_max)
    if evs <= ete:
        later = df.index[df.index > ete]
        if len(later) == 0:
            raise RuntimeError("測試期與訓練期重疊且無後續資料")
        evs = later[0]

    mask_train = (df.index >= ets) & (df.index <= ete)
    mask_test  = (df.index >= evs) & (df.index <= eve)

    train_df = df.loc[mask_train, [y_col, x_col]].dropna()
    test_df  = df.loc[mask_test,  [y_col, x_col]].dropna()
    if train_df.empty or test_df.empty:
        raise RuntimeError("訓練/測試期資料不足")

    # EG 雙向（trend='c', autolag='BIC'），擇較小 p 的方向： y ~ a + b x
    stat_yx, p_yx, _ = coint(train_df[y_col], train_df[x_col], trend="c", autolag="BIC")
    stat_xy, p_xy, _ = coint(train_df[x_col], train_df[y_col], trend="c", autolag="BIC")
    if p_yx <= p_xy:
        y_use, x_use = y_col, x_col
    else:
        y_use, x_use = x_col, y_col
        nameY, nameX = nameX, nameY
        pair_code_y, pair_code_x = pair_code_x, pair_code_y
        train_df = train_df[[y_use, x_use]]
        test_df  = test_df[[y_use, x_use]]

    '''
    # OLS(含常數) 估 α/β（訓練期）
    X = sm.add_constant(train_df[x_use].values)
    y = train_df[y_use].values
    model = sm.OLS(y, X).fit()
    alpha = float(model.params[0])
    beta  = float(model.params[1])
    '''

    # 訓練期 spread 統計 → 用來標準化全期間 z
    spread_train = (train_df[y_use] - alpha) - beta * train_df[x_use]
    mu = float(spread_train.mean())
    sd = float(spread_train.std())
    if sd == 0 or np.isnan(sd):
        raise RuntimeError("訓練期 spread 標準差為 0 或 NaN")

    # 全期 z 與報酬
    spread_all = (df[y_use] - alpha) - beta * df[x_use]
    z_all = (spread_all - mu) / sd
    z_all = z_all.reindex(df.index)

    rets = df[[y_use, x_use]].pct_change()
    rets.columns = ["Y", "X"]

    # 進出規則（對稱）
    idx = df.index
    z = z_all
    pos_Y_long  = pd.Series(np.nan, index=idx); pos_Y_long.iloc[0]  = 0.0
    pos_X_long  = pd.Series(np.nan, index=idx); pos_X_long.iloc[0]  = 0.0
    pos_Y_short = pd.Series(np.nan, index=idx); pos_Y_short.iloc[0] = 0.0
    pos_X_short = pd.Series(np.nan, index=idx); pos_X_short.iloc[0] = 0.0

    pos_Y_short[z >=  z_in] = -1.0; pos_X_short[z >=  z_in] =  1.0
    pos_Y_long [z <= -z_in] =  1.0; pos_X_long [z <= -z_in] = -1.0

    pos_Y_short[z <=  z_out] = 0.0; pos_X_short[z <=  z_out] = 0.0
    pos_Y_long [z >= -z_out] = 0.0; pos_X_long [z >= -z_out] = 0.0

    pos_Y = pos_Y_long.ffill().add(pos_Y_short.ffill(), fill_value=0.0)
    pos_X = pos_X_long.ffill().add(pos_X_short.ffill(), fill_value=0.0)

    pnl = (pos_Y.shift(1) * rets["Y"] + (-beta) * pos_X.shift(1) * rets["X"]).fillna(0.0)
    pnl_train = pnl.loc[mask_train]
    pnl_test  = pnl.loc[mask_test]

    annal_rf = 0.018
    year_periods = 252

    sharpe_train = annualized_sharpe(pnl_train, annal_rf, year_periods, 1)
    sharpe_test  = annualized_sharpe(pnl_test,  annal_rf, year_periods, 1)
    kelly_train  = kelly_fraction(pnl_train,    annal_rf, year_periods, 1)
    kelly_test   = kelly_fraction(pnl_test,     annal_rf, year_periods, 1)

    # ====== 這裡開始：新的「z 版」 Max DD 與 Longest DD ======
    # 1) Max |z|
    zmax_info = find_max_abs_z(z)
    # 2) Longest z-episode
    #   測試集最後一天 timestamp：給「未回復也可算最長」的規則使用（實作上已用 end==最後一根判斷）
    test_last_ts = df.index[mask_test][-1]
    ldd = find_longest_z_episode(z, z_in, z_out, test_last_ts)

    # ====== 繪圖 ======
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(13, 10), sharex=True,
                                        gridspec_kw={"height_ratios": [2, 1.6, 2.2]})

    for ax in [ax1, ax2]:
        ax.tick_params(labelbottom=True)

    fig.suptitle(f"[Pair] {nameY}:{pair_code_y} - {nameX}:{pair_code_x}\n"
                 f"[Params] alpha={alpha:.6f}, beta={beta:.6f} | z_in={z_in:.2f}, z_out={z_out:.2f}\n"
                 f"[Sharpe] train={sharpe_train:.4f}, test={sharpe_test:.4f} | "
                 f"[Kelly] train={kelly_train:.4f}, test={kelly_test:.4f}",
                 fontsize=12, y=0.98)

    # --- 圖1：價格 ---
    ax1.plot(df.index, df[y_use], label=f"{nameY}", linewidth=1.0)
    ax1.plot(df.index, df[x_use], label=f"{nameX}", linewidth=1.0, alpha=0.8)
    ax1.axvspan(ets, ete, color="gold", alpha=0.10, label="Train")
    ax1.axvspan(evs, eve, color="cyan", alpha=0.10, label="Test")
    ax1.set_ylabel("Price")
    ax1.legend(loc="upper left")

    # --- 圖2：z-score 與門檻 ---
    ax2.plot(df.index, z, label="z-score", linewidth=1.0)
    ax2.axhline(z_in,  color="red",   linestyle="--", linewidth=1.1, label="+Z_IN")
    ax2.axhline(-z_in, color="red",   linestyle="--", linewidth=1.1, label="-Z_IN")
    ax2.axhline(z_out,  color="gray", linestyle=":",  linewidth=1.1, label="+Z_OUT")
    ax2.axhline(-z_out, color="gray", linestyle=":",  linewidth=1.1, label="-Z_OUT")
    ax2.axvspan(ets, ete, color="gold", alpha=0.10)
    ax2.axvspan(evs, eve, color="cyan", alpha=0.10)
    '''
    if zmax_info is not None:
        _, t_zmax, zmax_val = zmax_info
        ax2.axvline(t_zmax, color="crimson", linestyle="-", linewidth=1.2, alpha=0.9)
        ax2.text(t_zmax, zmax_val, f" Max|z|={abs(zmax_val):.2f}", color="crimson",
                 ha="left", va="bottom", fontsize=9, rotation=0)
    '''
    ax2.set_ylabel("z-score")
    ax2.legend(loc="upper left")

    # --- 圖3：累積 PnL（把 z 指標的 Max/Longest 也標在這裡，方便對照績效） ---
    cumret = calculate_cumret(pnl.values)
    ax3.plot(df.index, cumret, label="Cumulative PnL", linewidth=1.1)

    # 新：Max |z| → 紅色短實線（沿用你之前「短段，不貫穿整張圖」的風格）
    if zmax_info is not None:
        k_zmax, t_zmax, _zv = zmax_info
        y_here = float(cumret[k_zmax]) if 0 <= k_zmax < len(cumret) else 0.0
        y_min, y_max = float(np.nanmin(cumret)), float(np.nanmax(cumret))
        span = max(1e-6, 0.06 * (y_max - y_min))  # 6% 視覺高度
        ax3.vlines(t_zmax, y_here - span/2, y_here + span/2, colors="red", linewidth=2.0, label="Max |z|")

    # 新：Longest z-episode → 藍色虛線兩條（起訖）
    if ldd is not None and ldd["start_ts"] is not None and ldd["end_ts"] is not None:
        ax3.axvline(ldd["start_ts"], color="blue", linestyle="--", linewidth=1.4, label="Longest z-episode start")
        ax3.axvline(ldd["end_ts"],   color="blue", linestyle="--", linewidth=1.4, label="Longest z-episode end")
        tag = "ongoing" if ldd["ongoing"] else "recovered"
        ax3.set_title(f"Longest z-episode: {ldd['length']} bars ({tag}) | z-Max={round(_zv, 4)}",
                      fontsize=11)

    ax3.axvspan(ets, ete, color="gold", alpha=0.10)
    ax3.axvspan(evs, eve, color="cyan", alpha=0.10)
    ax3.set_ylabel("Cumulative Return")
    ax3.legend(loc="upper left")

    # 控制台輸出重點
    print(f"[Pair] {nameY}:{pair_code_y} - {nameX}:{pair_code_x}")
    print(f"[Params] alpha={alpha:.6f}, beta={beta:.6f} | z_in={z_in:.2f}, z_out={z_out:.2f}")
    if zmax_info is not None:
        _, t_zmax, zmax_val = zmax_info
        print(f"[Max |z|] {abs(zmax_val):.4f} at {t_zmax.date()} (z={zmax_val:.4f})")
    if ldd is not None:
        print(f"[Longest z-episode] {ldd['length']} bars | sign={ldd['sign']} | "
              f"{ldd['start_ts'].date()} ~ {ldd['end_ts'].date()} | "
              f"{'ongoing' if ldd['ongoing'] else 'recovered'}")
    print(f"[Sharpe] train={sharpe_train:.4f}, test={sharpe_test:.4f} | "
          f"[Kelly] train={kelly_train:.4f}, test={kelly_test:.4f}")

    plt.tight_layout()
    plt.show()

# ====== 範例呼叫 ======
if __name__ == "__main__":
    # 例：自行替換配對與門檻
    PAIR_ID = "00688L.tw-00947.tw"

    pairs_map = load_pairs_params(PAIRS_JSON_PATH)
    rec = get_pair_record(pairs_map, PAIR_ID)
    pair_y = rec["nameOneCode"]
    pair_x = rec["nameTwoCode"]
    Z_IN  = float(rec["ztop"])
    Z_OUT = float(rec["zdown"])
    alpha = float(rec.get("alpha", 0.0))
    beta  = float(rec.get("beta", 0.0))

    '''
    pair_y = "8027.two"
    pair_x = "9958.tw"
    Z_IN  = 1.25
    Z_OUT = 1.0
    alpha = 13.889139
    beta = 0.377567
    '''

    run_single(pair_y, pair_x, Z_IN, Z_OUT, alpha, beta)
