import math
from typing import Any, Optional

EMA_SPAN_MULTIPLIER = 2.0 # 1.5 ~ 3.0 預設 2.0，值越大代表對於較近的股價不敏感


def round_half_away_from_zero(value: float) -> int:
    if value >= 0:
        return math.floor(value + 0.5)
    return math.ceil(value - 0.5)


def _extract_close_value(candle: Any) -> float:
    if isinstance(candle, dict):
        return float(candle.get("close", 0.0) or 0.0)
    return float(getattr(candle, "close"))


def calculate_regression_gradient(candles: list[Any]) -> Optional[int]:
    current_window = len(candles)
    if current_window < 2:
        return None

    closes = [_extract_close_value(candle) for candle in candles]
    base_close = closes[0]
    if math.isclose(base_close, 0.0, abs_tol=1e-12):
        return 0

    normalized_closes = [(close / base_close - 1.0) * 100.0 for close in closes]
    x_values = list(range(current_window))
    ema_span = current_window * EMA_SPAN_MULTIPLIER
    alpha = 2.0 / (ema_span + 1.0)
    decay = 1.0 - alpha
    weights = [decay ** (current_window - 1 - i) for i in range(current_window)]
    weight_sum = sum(weights)
    if math.isclose(weight_sum, 0.0, abs_tol=1e-12):
        return 0

    x_avg = sum(w * x for w, x in zip(weights, x_values)) / weight_sum
    y_avg = sum(w * y for w, y in zip(weights, normalized_closes)) / weight_sum

    numerator = sum(w * (x - x_avg) * (y - y_avg) for w, x, y in zip(weights, x_values, normalized_closes))
    denominator = sum(w * (x - x_avg) ** 2 for w, x in zip(weights, x_values))
    if math.isclose(denominator, 0.0, abs_tol=1e-12):
        return 0

    slope = numerator / denominator
    angle = math.degrees(math.atan(slope))
    return round_half_away_from_zero(angle)
