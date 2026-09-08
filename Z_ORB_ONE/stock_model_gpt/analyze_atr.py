"""Inspect training-period ATR ratios without changing features or model settings."""
from __future__ import annotations

import argparse
from bisect import bisect_right
from datetime import date, datetime
import json
import math
from pathlib import Path

from .config import Settings
from .paths import ATR_ANALYSIS_DIR, FEATURES_DIR
from .storage import read_jsonl
from .universe import recent_symbols


def validate_boundaries(boundaries: list[float]) -> None:
    if len(boundaries) not in (3, 4):
        raise ValueError("四級需 3 個界線，五級需 4 個界線")
    if any(not math.isfinite(x) or x <= 0 for x in boundaries):
        raise ValueError("界線必須是有限正數（百分比單位）")
    if any(a >= b for a, b in zip(boundaries, boundaries[1:])):
        raise ValueError("界線必須嚴格遞增")


def percentile(values: list[float], probability: float) -> float:
    position = (len(values) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def summarise(values: list[float], boundaries: list[float]) -> dict:
    ordered = sorted(values)
    counts = [0] * (len(boundaries) + 1)
    for value in ordered:
        counts[bisect_right(boundaries, value)] += 1
    quantiles = {f"P{p}": percentile(ordered, p / 100)
                 for p in (0, 20, 25, 40, 50, 60, 75, 80, 100)} if ordered else {}
    return {"count": len(ordered), "counts": counts,
            "shares": [n / len(ordered) if ordered else 0 for n in counts],
            "percentiles_pct": quantiles}


def analyze(paths: list[Path], start: date | None, as_of: date, boundaries: list[float]) -> dict:
    validate_boundaries(boundaries)
    values: list[float] = []
    years: dict[str, list[float]] = {}
    invalid = 0
    symbols: dict[str, int] = {}
    dates: list[str] = []
    for path in paths:
        count = 0
        for row in read_jsonl(path):
            day = date.fromisoformat(row["date"])
            if day > as_of or (start is not None and day < start):
                continue
            raw = row.get("atr_ratio")
            try:
                if isinstance(raw, bool):
                    raise ValueError("boolean ATR")
                ratio = float(raw)
                if not math.isfinite(ratio) or ratio < 0 or not math.isfinite(ratio * 100):
                    raise ValueError("invalid ATR")
            except (TypeError, ValueError, OverflowError):
                invalid += 1
                continue
            value = ratio * 100
            values.append(value)
            years.setdefault(str(day.year), []).append(value)
            dates.append(day.isoformat())
            count += 1
        symbols[path.stem] = count
    if not values:
        raise ValueError("指定期間沒有有效 atr_ratio，請先執行 prepare_features 並確認日期與股票範圍")
    summary = summarise(values, boundaries)
    levels = len(boundaries) + 1
    ordered = sorted(values)
    candidates = [percentile(ordered, i / levels) for i in range(1, levels)]
    return {"as_of": as_of.isoformat(), "from_date": start.isoformat() if start else None,
            "actual_first_date": min(dates), "actual_last_date": max(dates),
            "boundaries_pct": boundaries, "summary": summary,
            "equal_frequency_candidate_pct": candidates,
            "candidate_usable": all(x > 0 for x in candidates)
                                and all(a < b for a, b in zip(candidates, candidates[1:])),
            "invalid_rows": invalid, "symbols": symbols,
            "by_year": {year: summarise(items, boundaries) for year, items in sorted(years.items())}}


def print_distribution(label: str, summary: dict, boundaries: list[float]) -> None:
    print(f"{label}: {summary['count']} 筆股票日")
    for index, (count, share) in enumerate(zip(summary["counts"], summary["shares"])):
        lower = 0 if index == 0 else boundaries[index - 1]
        upper = f"< {boundaries[index]:g}%" if index < len(boundaries) else "無上限"
        print(f"  刻度 {index}: {lower:g}% <= ATR% {upper}: {count} ({share:.2%})")


def main() -> None:
    parser = argparse.ArgumentParser(description="訓練前 ATR 刻度分布分析（不修改設定）")
    parser.add_argument("--as-of", type=date.fromisoformat, required=True, help="訓練截止日，不含後續驗證期")
    parser.add_argument("--from-date", type=date.fromisoformat, default=None)
    parser.add_argument("--boundaries-pct", nargs="+", type=float, default=[1, 2, 3, 5],
                        help="百分比界線，例：1 2 3 5；四級提供三個界線")
    parser.add_argument("--symbols", nargs="+", default=None)
    parser.add_argument("--settings", default=None)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    if args.from_date and args.from_date > args.as_of:
        parser.error("from-date 不可晚於 as-of")
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    eligible = set(args.symbols) if args.symbols else recent_symbols(args.as_of, settings.recent_universe_days)
    paths = sorted(path for path in FEATURES_DIR.glob("*.jsonl")
                   if not eligible or path.stem in eligible)
    try:
        report = analyze(paths, args.from_date, args.as_of, args.boundaries_pct)
    except ValueError as exc:
        parser.error(str(exc))
    report["selection"] = "explicit_symbols" if args.symbols else "recent_universe" if eligible else "all_features_fallback"
    report["missing_symbols"] = sorted(eligible - {path.stem for path in paths})
    report["created_at"] = datetime.now().isoformat(timespec="seconds")
    print(f"股票範圍: {report['selection']}; 有效日期: {report['actual_first_date']} ~ {report['actual_last_date']}")
    print_distribution("整體", report["summary"], args.boundaries_pct)
    for year, summary in report["by_year"].items():
        print_distribution(year, summary, args.boundaries_pct)
    print("分位數（%）:", report["summary"]["percentiles_pct"])
    print("等頻候選界線（%，僅供比較）:", report["equal_frequency_candidate_pct"])
    if not report["candidate_usable"]:
        print("[WARN] 候選界線有零值或重複值，不能直接作為刻度界線")
    print(f"無效或缺 ATR 筆數: {report['invalid_rows']}; 缺特徵股票: {report['missing_symbols']}")
    output = args.output or ATR_ANALYSIS_DIR / f"{args.as_of.isoformat()}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(f"分析已保存: {output}；未修改設定、特徵或 checkpoint")


if __name__ == "__main__":
    main()
