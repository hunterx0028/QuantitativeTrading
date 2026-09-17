"""Import one or more manually-downloaded TAIFEX futDailyMarketReport CSV
exports into the shared night_futures store.

There is no automated fetch for this yet (the download page is JS-driven and
its underlying API isn't confirmed) — download CSVs by hand from
https://www.taifex.com.tw/cht/3/futDailyMarketReport (each query covers at
most ~1 month) and run this against them, e.g.:

    python -m Z_ORB_ONE.stock_model_gpt.import_night_futures a.csv b.csv c.csv

Or put CSV files under data/history_night and run without arguments:

    python -m Z_ORB_ONE.stock_model_gpt.import_night_futures

Safe to re-run with overlapping files; merging is keyed by date so later
files don't duplicate earlier ones.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from .night_futures import merge_night_futures, parse_night_futures_csv
from .paths import HISTORY_NIGHT_DIR


def default_csv_paths() -> list[Path]:
    HISTORY_NIGHT_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(HISTORY_NIGHT_DIR.glob("*.csv"))


from .runtime_lock import locked


@locked
def main() -> None:
    parser = argparse.ArgumentParser(description="匯入 TAIFEX 夜盤期指 CSV")
    parser.add_argument(
        "csv_paths",
        nargs="*",
        type=Path,
        help=f"futDailyMarketReport 下載的 CSV 檔案；未指定時讀取 {HISTORY_NIGHT_DIR}/*.csv",
    )
    args = parser.parse_args()
    csv_paths = args.csv_paths or default_csv_paths()
    if not csv_paths:
        raise RuntimeError(f"沒有指定 CSV，且 {HISTORY_NIGHT_DIR} 中沒有 *.csv 檔案")

    incoming: list[dict] = []
    for path in csv_paths:
        if not path.exists():
            raise RuntimeError(f"找不到檔案: {path}")
        records = parse_night_futures_csv(path)
        print(f"{path}: {len(records)} 個交易日")
        incoming.extend(records)
    if not incoming:
        raise RuntimeError("CSV 中沒有可匯入的 TX 近月盤後交易資料")

    merged = merge_night_futures(incoming)
    print(f"合併後總計: {len(merged)} 個交易日（{merged[0]['date']} ~ {merged[-1]['date']}）")


if __name__ == "__main__":
    main()
