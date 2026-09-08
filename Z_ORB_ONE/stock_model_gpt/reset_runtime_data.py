"""Wipe all regenerable runtime data to start completely over.

Clears: data/candles, data/corporate_actions, data/features, data/actual_candles,
data/universe, data/evaluations, data/atr_analysis, checkpoints/ (including
checkpoints/daily_runs and gate_status.json), predictions/, signal_reports/.

Never touches config.ini, stock_data.py, settings.json, or any source code —
those are user-authored configuration, not regenerable execution-period data.

Defaults to a dry run that only lists what would be deleted. Pass --yes to
actually delete. Respects STOCK_MODEL_GPT_WRITE_ROOT like every other script in
this package — if that env var is set, this clears the isolated directory it
points to, not production; the script prints a loud warning when that is the
case so you don't reset the wrong thing by accident.
"""
from __future__ import annotations

import argparse
import os
import shutil

from .paths import (
    ACTUAL_CANDLES_DIR,
    ATR_ANALYSIS_DIR,
    CANDLES_DIR,
    CHECKPOINT_DIR,
    CORPORATE_ACTIONS_DIR,
    EVALUATIONS_DIR,
    FEATURES_DIR,
    PREDICTIONS_DIR,
    SIGNAL_REPORTS_DIR,
    UNIVERSE_DIR,
    ensure_runtime_dirs,
)


TARGET_DIRS = (
    CANDLES_DIR,
    CORPORATE_ACTIONS_DIR,
    FEATURES_DIR,
    ACTUAL_CANDLES_DIR,
    UNIVERSE_DIR,
    EVALUATIONS_DIR,
    ATR_ANALYSIS_DIR,
    CHECKPOINT_DIR,
    PREDICTIONS_DIR,
    SIGNAL_REPORTS_DIR,
)


def _dir_stats(path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    files = [item for item in path.rglob("*") if item.is_file()]
    return len(files), sum(item.stat().st_size for item in files)


def _human_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f}{unit}"
        size /= 1024
    return f"{size:.1f}TB"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="清空所有可重建的執行期資料，重新開始；不會動到 config.ini、stock_data.py、settings.json 或原始碼"
    )
    parser.add_argument("--yes", action="store_true", help="實際刪除；不加此旗標只會預覽，不會真的刪除")
    args = parser.parse_args()

    write_root_override = os.environ.get("STOCK_MODEL_GPT_WRITE_ROOT")
    if write_root_override:
        print(
            f"[WARN] STOCK_MODEL_GPT_WRITE_ROOT 已設定為 {write_root_override}，"
            "這次清空的是這個隔離目錄，不是正式環境；如果你要重置的是正式環境，"
            "請先在這個 shell 取消設定這個環境變數再重跑一次。"
        )

    print("目標資料夾：")
    total_files = 0
    total_bytes = 0
    for path in TARGET_DIRS:
        count, size = _dir_stats(path)
        total_files += count
        total_bytes += size
        print(f"  {path.resolve()}: {count} 個檔案, {_human_size(size)}")

    if total_files == 0:
        print("目前所有目標資料夾都是空的，沒有東西可清。")
        return

    if not args.yes:
        print()
        print(f"[DRY RUN] 以上共 {total_files} 個檔案，{_human_size(total_bytes)}。這是預覽模式，尚未刪除任何東西。")
        print("確認要清空後，加上 --yes 重新執行一次。")
        return

    for path in TARGET_DIRS:
        if path.exists():
            shutil.rmtree(path)
    ensure_runtime_dirs()
    print(f"已清空並重建空資料夾結構，共刪除 {total_files} 個檔案，{_human_size(total_bytes)}。")
    print("config.ini、stock_data.py、settings.json 未受影響。")


if __name__ == "__main__":
    main()
