import os
import shutil
from pathlib import Path

SRC = Path(r"C:\Users\leegu\PycharmProjects\QuantitativeTrading")
DST = Path(r"C:\Users\leegu\OneDrive\QTProject")

EXCLUDE_TOP_DIRS = {".idea", ".venv", "image", "papers", "stocks", "Z_ORB_ONE/analysis_strategy/analysis_json_cache", "Z_ORB_ONE/daily_report/pdf_folder", "Z_ORB_ONE/stock_state"}
EXCLUDE_ROOT_FILES = {"Local2OneDrive.py", "OneDrive2Local.py"}

EXCLUDE_DIR_PARTS = {
    tuple(part.casefold() for part in Path(path).parts)
    for path in EXCLUDE_TOP_DIRS
}


def is_excluded_dir(rel_path: Path) -> bool:
    rel_parts = tuple(part.casefold() for part in rel_path.parts)
    return any(
        rel_parts == excluded_parts
        or rel_parts[:len(excluded_parts)] == excluded_parts
        for excluded_parts in EXCLUDE_DIR_PARTS
    )


def copy_project(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)

    for root, dirs, files in os.walk(src):
        root_path = Path(root)
        rel = root_path.relative_to(src)

        if is_excluded_dir(rel):
            dirs[:] = []
            continue

        # 1) 全域排除 __pycache__ 與 EXCLUDE_TOP_DIRS 內列出的相對路徑
        dirs[:] = [
            d for d in dirs
            if d != "__pycache__" and not is_excluded_dir(rel / d)
        ]

        for name in files:
            src_file = root_path / name

            # 2) 排除 __pycache__ 裡面所有檔案
            if "__pycache__" in src_file.parts:
                continue

            # 3) 只複製 root 的 globals.py
            if name == "globals.py" and src_file.parent != src:
                continue

            # 4) 排除根目錄的 Local2OneDrive.py
            if src_file.parent == src and name in EXCLUDE_ROOT_FILES:
                continue

            # 5) 複製（不保留時間戳，中繼資料）
            rel = src_file.relative_to(src)
            dst_file = dst / rel
            dst_file.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy(src_file, dst_file)
            print(f"Copied: {src_file} -> {dst_file}")


if __name__ == "__main__":
    ans = input("確認要將本機檔案上傳到OneDrive嗎？(Y/N): ").strip().upper()

    if ans == "Y":
        copy_project(SRC, DST)
        print("✅ 執行完成")
    elif ans == "N":
        print("❌ 已取消執行")
    else:
        print("⚠️ 請輸入 Y 或 N")

    # copy_project(SRC, DST)
    # print("\n✅ 完成：已依規則複製專案（不保留時間戳，直接覆蓋）。")
