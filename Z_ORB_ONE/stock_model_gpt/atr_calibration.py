"""ATR(14) five-level discretization.

The boundaries used to be re-calibrated per equal-frequency quantiles (20/40/
60/80) of each reseed's own training window — but that made the bucket
definitions a moving target across reseeds/periods on top of the model itself
learning, and the quantile-fit boundaries only ever needed to be right once.
Fixed here at the values a training-quantile fit produced on a 2025-08~2025-12
window (see checkpoint `stock_model_gpt_20260912_221215_399564.pt`'s
`atr_calibration`/`atr_report` for that original fit's distribution)."""
from dataclasses import replace

from .config import Settings


ATR_ENCODING = "reference_adjusted_atr14_bucket5_v1"
ATR_BOUNDARIES_PCT = (2.8694729537058703, 3.5912423282596873, 4.20375143031919, 5.048733346878557)


def validate_five_levels(boundaries) -> None:
    if not isinstance(boundaries, (list, tuple)) or len(boundaries) != 4:
        raise ValueError("模型 ATR 五級需要四個百分比界線")
    if any(not isinstance(value, (int, float)) or value <= 0 for value in boundaries):
        raise ValueError("界線必須是有限正數（百分比單位）")
    if any(a >= b for a, b in zip(boundaries, boundaries[1:])):
        raise ValueError("界線必須嚴格遞增")


def prepare_atr_levels(settings: Settings) -> tuple[Settings, dict]:
    """Always the fixed ATR_BOUNDARIES_PCT — no training-window fitting."""
    boundaries = list(ATR_BOUNDARIES_PCT)
    validate_five_levels(boundaries)
    calibration = {"method": "fixed", "boundaries_pct": boundaries}
    return replace(settings, atr_boundaries_pct=boundaries), calibration
