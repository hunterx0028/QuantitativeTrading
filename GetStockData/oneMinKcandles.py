import pandas as pd
import numpy as np


def robust_orb_width(
    df_1m: pd.DataFrame,  # index = tz-naive or tz-aware datetime；包含多天1分K
    day: str,             # "YYYY-MM-DD"（台灣時間）
    product_type: str = "stock_large",  # "etf" | "stock_large" | "stock_small"
    lookback_days: int = 20,
    use_ema: bool = True,
):
    cfg = {
        "etf":        {"q": (0.05, 0.95), "alpha": 0.6, "atr_span": 7,  "floor": 0.5, "cap": 2.0, "hist_span": 12},
        "stock_large":{"q": (0.10, 0.90), "alpha": 0.5, "atr_span": 5,  "floor": 0.6, "cap": 2.5, "hist_span": 15},
        "stock_small":{"q": (0.10, 0.90), "alpha": 0.4, "atr_span": 5,  "floor": 0.7, "cap": 3.0, "hist_span": 20},
    }[product_type]

    df = df_1m.copy()
    # 轉成 tz-naive 的日期字串，robust_orb_width 內部用這個切日
    if df.index.tz is not None:
        df["date"] = df.index.tz_convert("Asia/Taipei").tz_localize(None).strftime("%Y-%m-%d")
        idx_naive = df.index.tz_convert("Asia/Taipei").tz_localize(None)
        df.index = idx_naive
    else:
        df["date"] = df.index.strftime("%Y-%m-%d")

    start_time = "09:00"
    end_time = "09:14"

    # 取當天09:00–09:14（含09:14）
    df_today = df[df["date"] == day]
    df_orb = df_today.between_time(start_time, end_time)
    if df_orb.empty:
        raise ValueError(f"No 1-min bars for ORB window on {day}.")
    H, L = df_orb["high"].max(), df_orb["low"].min()
    W_today_raw = float(max(H - L, 0.0))

    # 建立歷史 ORB 寬度序列
    days_sorted = sorted(df["date"].unique())
    if day not in days_sorted:
        raise ValueError("day not in dataframe date range")
    idx = days_sorted.index(day)
    hist_days = days_sorted[max(0, idx - lookback_days):idx]

    widths_hist, vols_hist = [], []
    for d in hist_days:
        _orb = df[df["date"] == d].between_time(start_time, end_time)
        if len(_orb):
            widths_hist.append(_orb["high"].max() - _orb["low"].min())
            vols_hist.append(_orb["volume"].sum())
    widths_hist = np.array(widths_hist) if len(widths_hist) else np.array([W_today_raw])
    vols_hist   = np.array(vols_hist)   if len(vols_hist)   else np.array([df_orb["volume"].sum()])

    # Winsorize 今日寬度
    ql, qh = np.quantile(widths_hist, cfg["q"])
    W_today_clip = float(np.clip(W_today_raw, ql, qh))

    # 量能濾波（避免冷門時段寬度失真）
    vol_today = df_orb["volume"].sum()
    vol_q20 = np.quantile(vols_hist, 0.20)
    if vol_today < vol_q20:
        W_today_eff = 0.5 * W_today_clip + 0.5 * float(np.median(widths_hist))
    else:
        W_today_eff = W_today_clip

    # 歷史均衡值（EMA 或 Median）
    if use_ema and len(widths_hist) >= 3:
        span = cfg["hist_span"]
        W_avg = float(pd.Series(widths_hist).ewm(span=span, adjust=False).mean().iloc[-1])
    else:
        W_avg = float(np.median(widths_hist))

    # 自適應權重
    MAD = np.median(np.abs(widths_hist - np.median(widths_hist))) + 1e-9
    z = abs(W_today_eff - W_avg) / MAD
    alpha = float(np.clip(0.7 - 0.1 * z, 0.3, 0.7))
    W_final = alpha * W_today_eff + (1 - alpha) * W_avg

    # 用 ATR 夾住（用日內粗估；你也可換成日K ATR）
    df_daily = df.resample("1D").agg({"high":"max","low":"min","close":"last"}).dropna()
    if df_daily.empty:
        raise ValueError("Not enough data to compute daily TR/ATR.")
    df_daily["TR"] = df_daily["high"] - df_daily["low"]
    atr_span = cfg["atr_span"]
    ATR = float(df_daily["TR"].rolling(atr_span).mean().iloc[-1]) if len(df_daily) >= atr_span else float(df_daily["TR"].mean())

    W_floor = cfg["floor"] * ATR
    W_cap   = cfg["cap"]   * ATR
    W_final = float(np.clip(W_final, W_floor, W_cap))

    return {
        "W_today_raw": W_today_raw,
        "W_today_eff": W_today_eff,
        "W_avg": W_avg,
        "alpha_used": alpha,
        "ATR": ATR,
        "W_final": W_final
    }

# —— 這裡開始：把兩種API的回應整併成 1 分K DataFrame —— #

def _normalize_bars_list(data_list):
    """
    將 list[dict] -> DataFrame，並統一欄位大小寫/型別：
    columns: date(open time), open, high, low, close, volume
    時間會解析為 datetime（保留時區），index 設為該時間。
    """
    if not isinstance(data_list, list) or len(data_list) == 0:
        return pd.DataFrame(columns=["open","high","low","close","volume"])

    df = pd.DataFrame(data_list).copy()

    # 統一欄名（避免大小寫或額外欄位）
    rename_map = {
        "Date": "date", "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Volume": "volume", "AvgPrice": "average", "Average": "average"
    }
    df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)

    # 只保留需要的欄
    keep_cols = [c for c in ["date","open","high","low","close","volume"] if c in df.columns]
    df = df[keep_cols]

    # 轉型
    # 時間：解析含 +08:00 的 ISO 格式，保留時區資訊
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    # 其餘欄位轉數字
    for c in ["open","high","low","close","volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 設定時間索引
    df = df.dropna(subset=["date"]).set_index("date").sort_index()  # 最終升冪
    return df

def build_1m_dataframe(today_json: dict, history_json: dict) -> pd.DataFrame:
    """
    將「當日1分K（asc）」與「歷史1分K（desc）」合併，
    產出一個升冪、時區正確（Asia/Taipei）的 1 分K DataFrame：
    index = Asia/Taipei 時間（tz-aware）
    columns = open, high, low, close, volume（float/float/int）
    同一分鐘若重疊，以 today_json 的資料優先（視為較「即時/新」）。
    """
    # 解析兩個 data list
    today_list   = (today_json or {}).get("data", [])
    history_list = (history_json or {}).get("data", [])

    df_today   = _normalize_bars_list(today_list)
    df_history = _normalize_bars_list(history_list)

    # 歷史回傳通常為 desc，但我們在 _normalize 內已 sort_index 升冪，不用再反轉
    # 合併：先串接，後去重（保留最後一筆，同時間以 today 覆蓋 history）
    df_all = pd.concat([df_history, df_today]).sort_index()
    df_all = df_all[~df_all.index.duplicated(keep="last")]

    # 轉為台北時區（tz-aware）
    df_all = df_all.tz_convert("Asia/Taipei")

    # 過濾交易時段（台股現貨 09:00–13:30；依你需求可調整）
    df_all = df_all.between_time("09:00", "13:30")

    # 型別微調：volume 整數化（非必要）
    if "volume" in df_all.columns:
        # 保留缺失為 NaN，其餘轉整數
        df_all["volume"] = (df_all["volume"].round().astype("Int64"))

    # 最後確保欄齊全
    for c in ["open","high","low","close","volume"]:
        if c not in df_all.columns:
            df_all[c] = np.nan

    return df_all[["open","high","low","close","volume"]]

def orb_tp_distance(result: dict, K: float) -> float:

    W_final = float(result["W_final"])
    ATR_val = float(result["ATR"])  # robust_orb_width 已算好（ETF用7、個股用5，可照你需求調整）

    return max(W_final, K * ATR_val)