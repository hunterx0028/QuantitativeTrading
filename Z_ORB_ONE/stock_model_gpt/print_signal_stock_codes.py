"""從指定日期的 signal_reports 報表印出股票代碼清單。"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path


SPECIFIED_DATE = ""  # YYYYMMDD，例如 "20260921"；空字串取檔名日期最新的報表
REPORT_DIR = Path(__file__).resolve().parent / "signal_reports"


def find_report(specified_date: str, report_dir: Path = REPORT_DIR) -> Path:
    specified_date = specified_date.strip()
    if specified_date:
        if not re.fullmatch(r"[0-9]{8}", specified_date):
            raise ValueError("SPECIFIED_DATE 需為 YYYYMMDD，例如 20260921")
        target_date = datetime.strptime(specified_date, "%Y%m%d").date()
        report_path = report_dir / f"{target_date.isoformat()}.txt"
        if not report_path.is_file():
            raise FileNotFoundError(f"找不到報表：{report_path}")
        return report_path

    reports = []
    for path in report_dir.glob("????-??-??.txt"):
        if not path.is_file() or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", path.stem):
            continue
        try:
            report_date = datetime.strptime(path.stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        reports.append((report_date, path))
    if not reports:
        raise FileNotFoundError(f"找不到 YYYY-MM-DD.txt 格式的報表：{report_dir}")
    return max(reports, key=lambda item: item[0])[1]


def extract_stock_codes(report_text: str) -> list[str]:
    """擷取機率表格的股票代碼，保留原始順序並去除重複。"""
    row_pattern = re.compile(
        r"^\s*([0-9]+)(?:\s+[0-9]+(?:\.[0-9]+)?%){6}\s+-?[0-9]+\s*$"
    )
    codes = []
    seen = set()
    for line in report_text.splitlines():
        match = row_pattern.fullmatch(line)
        if match and match.group(1) not in seen:
            code = match.group(1)
            codes.append(code)
            seen.add(code)
    return codes


def main():
    report_path = find_report(SPECIFIED_DATE)
    codes = extract_stock_codes(report_path.read_text(encoding="utf-8-sig"))
    print(",".join(f'"{code}"' for code in codes))


if __name__ == "__main__":
    main()
