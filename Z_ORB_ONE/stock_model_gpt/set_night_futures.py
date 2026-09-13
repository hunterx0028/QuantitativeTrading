"""Manually record one day's TX (台指期貨) near-month night-session change,
read straight off https://www.taifex.com.tw/cht/3/futDailyMarketReport,
without downloading/parsing a CSV.

The page shows one row per contract, already sorted with the near month
first, e.g.:

    TX 202609 46306 46663 46041 46588 ▲401 ▲0.87% 28667 ...

Take the 漲跌% value from that row (the 7th/8th column, here "▲0.87%",
meaning +0.87) and the trading date shown on the page, then run:

    python -m Z_ORB_ONE.stock_model_gpt.set_night_futures --date 2026-09-15 --change 0.87

`--change` is a plain signed number — drop the ▲/▼ arrows and the %% sign,
only keep a leading "-" for a drop (▼). Re-running for a date you already
entered overwrites that date only (safe to correct a typo)."""
from __future__ import annotations

import argparse
from datetime import date

from .night_futures import merge_night_futures, night_futures_bucket


def main() -> None:
    parser = argparse.ArgumentParser(description="手動輸入單日 TX 近月夜盤漲跌幅，寫入 night_futures.jsonl")
    parser.add_argument("--date", required=True, help="夜盤資料標示的交易日期 YYYY-MM-DD（頁面上顯示的那個日期）")
    parser.add_argument("--change", required=True, type=float, help="漲跌%%，純數字，例如 0.87 或 -0.6（下跌才加負號）")
    parser.add_argument("--contract-month", default="manual", help="到期月份，選填，純紀錄用，例如 202609")
    args = parser.parse_args()

    day = date.fromisoformat(args.date).isoformat()
    bucket = night_futures_bucket(args.change)

    merge_night_futures([{
        "date": day,
        "contract_month": args.contract_month,
        "change_pct": args.change,
        "bucket": bucket,
    }])
    print(f"已寫入 {day}: change_pct={args.change:+.2f}% -> bucket={bucket}")


if __name__ == "__main__":
    main()
