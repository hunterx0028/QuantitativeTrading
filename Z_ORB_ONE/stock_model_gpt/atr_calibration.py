"""ATR(14) five-level discretization.

The boundaries used to be re-calibrated per equal-frequency quantiles (20/40/
60/80) of each reseed's own training window, but that made the bucket
definitions a moving target across every reseed on top of the model itself
learning, so this was fixed at one set of values (a fit on a 2025-08~2025-12
window — see checkpoint `stock_model_gpt_20260912_221215_399564.pt`'s
`atr_calibration`/`atr_report`) with no further recalibration.

Reintroduced here as a *periodic*, not per-reseed, recalibration: boundaries
only move at most once every `settings.atr_recalibration_interval_days`, and
only when training a brand-new model from scratch (`checkpoint is None`) —
never mid-continuation. A `daily=True` (or any other) continuation of an
existing checkpoint always inherits that checkpoint's exact boundaries
verbatim, because its atr_embedding weights were learned against that
specific bucketing; silently reassigning what bucket 2 means, say, would
corrupt those weights without changing them. Every recalibration is written
to data/atr_analysis/ (current_calibration.json plus a dated snapshot) so a
boundary change is a visible, auditable event rather than a silent drift.
"""
from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path

from .config import Settings
from .paths import ATR_ANALYSIS_DIR
from .provenance import atomic_text
from .storage import read_jsonl


ATR_ENCODING = "reference_adjusted_atr14_bucket5_v1"
# Bootstrap/fallback only: used until the first successful quantile fit (see
# _fit_quantile_boundaries) — either because no recalibration has ever run yet
# or because there isn't enough recent history to fit one responsibly.
ATR_BOUNDARIES_PCT = (2.8694729537058703, 3.5912423282596873, 4.20375143031919, 5.048733346878557)
MIN_CALIBRATION_SAMPLES = 100
CALIBRATION_PATH = ATR_ANALYSIS_DIR / "current_calibration.json"


def validate_five_levels(boundaries) -> None:
    if not isinstance(boundaries, (list, tuple)) or len(boundaries) != 4:
        raise ValueError("模型 ATR 五級需要四個百分比界線")
    if any(not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0 for value in boundaries):
        raise ValueError("界線必須是有限正數（百分比單位）")
    if any(a >= b for a, b in zip(boundaries, boundaries[1:])):
        raise ValueError("界線必須嚴格遞增")


def _percentile(ordered: list[float], pct: float) -> float:
    """Linear-interpolated percentile of an already-sorted sample."""
    rank = (pct / 100) * (len(ordered) - 1)
    lower, upper = math.floor(rank), math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    fraction = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _fit_quantile_boundaries(feature_paths: list[Path], as_of: date) -> tuple[list[float], dict]:
    cutoff = as_of.isoformat()
    ratios: list[float] = []
    for path in feature_paths:
        ratios.extend(row["atr_ratio"] * 100 for row in read_jsonl(path) if row["date"] <= cutoff)
    if len(ratios) < MIN_CALIBRATION_SAMPLES:
        raise RuntimeError(
            f"ATR 重新校準需要至少 {MIN_CALIBRATION_SAMPLES} 筆歷史樣本，目前只有 {len(ratios)} 筆"
        )
    ordered = sorted(ratios)
    percentiles = (20, 40, 60, 80)
    boundaries = [_percentile(ordered, pct) for pct in percentiles]
    stats = {
        "sample_count": len(ordered), "min": ordered[0], "max": ordered[-1],
        "percentiles": {f"p{pct}": value for pct, value in zip(percentiles, boundaries)},
    }
    return boundaries, stats


def _load_calibration_record() -> dict | None:
    if not CALIBRATION_PATH.exists():
        return None
    return json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))


def _save_calibration_record(calibration: dict) -> None:
    text = json.dumps(calibration, ensure_ascii=False, indent=2) + "\n"
    atomic_text(CALIBRATION_PATH, text)
    atomic_text(ATR_ANALYSIS_DIR / f"{calibration['calibrated_as_of']}.json", text)


def _recalibration_due(previous: dict | None, as_of: date, interval_days: int) -> bool:
    if not previous or not previous.get("calibrated_as_of"):
        return True
    elapsed = (as_of - date.fromisoformat(previous["calibrated_as_of"])).days
    return elapsed >= interval_days


def prepare_atr_levels(
    settings: Settings, as_of: date, feature_paths: list[Path], checkpoint: dict | None = None,
) -> tuple[Settings, dict]:
    if checkpoint is not None:
        boundaries = list(checkpoint["settings"]["atr_boundaries_pct"])
        validate_five_levels(boundaries)
        return replace(settings, atr_boundaries_pct=boundaries), checkpoint["atr_calibration"]

    previous = _load_calibration_record()
    if not _recalibration_due(previous, as_of, settings.atr_recalibration_interval_days):
        boundaries = list(previous["boundaries_pct"])
        validate_five_levels(boundaries)
        return replace(settings, atr_boundaries_pct=boundaries), previous

    try:
        boundaries, sample_stats = _fit_quantile_boundaries(feature_paths, as_of)
    except RuntimeError:
        if previous:
            # Due for recalibration but not enough fresh history to trust a
            # new fit — keep the last one rather than fitting on too little.
            boundaries = list(previous["boundaries_pct"])
            validate_five_levels(boundaries)
            return replace(settings, atr_boundaries_pct=boundaries), previous
        # Never calibrated at all yet (e.g. a brand-new deployment) — use the
        # historical fixed fallback until enough data accumulates to fit one;
        # not persisted, so the very next reseed tries a fresh fit again.
        boundaries = list(ATR_BOUNDARIES_PCT)
        validate_five_levels(boundaries)
        return replace(settings, atr_boundaries_pct=boundaries), {"method": "fixed_fallback", "boundaries_pct": boundaries}

    validate_five_levels(boundaries)
    calibration = {
        "method": "quantile_fit_20_40_60_80",
        "boundaries_pct": boundaries,
        "calibrated_as_of": as_of.isoformat(),
        "calibrated_at": datetime.now().isoformat(timespec="seconds"),
        "recalibration_interval_days": settings.atr_recalibration_interval_days,
        "sample_stats": sample_stats,
        "previous_boundaries_pct": previous["boundaries_pct"] if previous else None,
    }
    _save_calibration_record(calibration)
    return replace(settings, atr_boundaries_pct=boundaries), calibration
