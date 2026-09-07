"""Fit ATR levels initially; subsequent training only inherits saved levels."""
from dataclasses import replace
from datetime import date, datetime
import json
from pathlib import Path

from .analyze_atr import analyze, print_distribution, summarise, validate_boundaries
from .config import Settings
from .paths import DATA_DIR
from .storage import read_jsonl


ATR_ENCODING = "reference_adjusted_atr14_bucket5_v1"


def validate_five_levels(boundaries) -> None:
    if not isinstance(boundaries, (list, tuple)) or len(boundaries) != 4:
        raise ValueError("模型 ATR 五級需要四個百分比界線")
    validate_boundaries(boundaries)


def prepare_atr_levels(settings: Settings, paths: list[Path], as_of: date,
                       checkpoint: dict | None = None) -> tuple[Settings, dict, Path | None]:
    if checkpoint is not None:
        # Daily training must inherit boundaries even if local settings change.
        boundaries = checkpoint["settings"]["atr_boundaries_pct"]
        calibration = checkpoint["atr_calibration"]
        validate_five_levels(boundaries)
        initial_report = checkpoint.get("atr_report")
        return (replace(settings, atr_boundaries_pct=list(boundaries)), calibration,
                Path(initial_report) if initial_report else None)
    else:
        pilot = analyze(paths, None, as_of, [1, 2, 3, 5])
        boundaries = pilot["equal_frequency_candidate_pct"]
        method = "training_quantiles_20_40_60_80"
        if not pilot["candidate_usable"]:
            # Flat or sparse distributions cannot yield five distinct quantile bins.
            boundaries = [1.0, 2.0, 3.0, 5.0]
            method = "fixed_fallback_degenerate_quantiles"
            print("[WARN] ATR 分位數界線重複或含零，回退固定 1/2/3/5%；不保證等頻")
        calibration = {"method": method, "fit_as_of": as_of.isoformat(),
                       "fit_count": pilot["summary"]["count"]}
        mode = "initial_fit"
    validate_five_levels(boundaries)
    report = analyze(paths, None, as_of, list(boundaries))
    if report["invalid_rows"]:
        raise ValueError("訓練資料含缺少或無效 ATR，請先重新執行 prepare_features")
    # Distribution reporting is only performed during initial calibration.
    current_values = [float(row["atr_ratio"]) * 100 for path in paths for row in read_jsonl(path)
                      if row["date"] == as_of.isoformat()]
    report.update(mode=mode, encoding=ATR_ENCODING, calibration=calibration,
                  current_day=summarise(current_values, list(boundaries)))
    report.pop("equal_frequency_candidate_pct", None)
    report.pop("candidate_usable", None)
    print(f"ATR 刻度模式={mode} 界線（%）={boundaries}")
    print_distribution("訓練期間", report["summary"], boundaries)
    print_distribution(f"當日 {as_of}", report["current_day"], boundaries)
    output = DATA_DIR / "atr_analysis" / f"{mode}_{as_of}_{datetime.now():%Y%m%d_%H%M%S_%f}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(f"ATR 分布報告: {output}")
    return replace(settings, atr_boundaries_pct=list(boundaries)), calibration, output
