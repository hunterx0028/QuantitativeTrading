from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Iterable


PRICE_VALUES = (-2, -1, 0, 1, 2)
VOLUME_VALUES = (-2, -1, 0, 1, 2)
CLOSE_VALUES = ("D", "N", "U")


@dataclass(frozen=True)
class DailyState:
    date: str
    price: int
    hit_up: bool
    hit_down: bool
    close_limit: str
    volume: int | str

    def to_dict(self) -> dict:
        return asdict(self)


def tick_size(price: float) -> Decimal:
    value = Decimal(str(price))
    if value < Decimal("10"):
        return Decimal("0.01")
    if value < Decimal("50"):
        return Decimal("0.05")
    if value < Decimal("100"):
        return Decimal("0.1")
    if value < Decimal("500"):
        return Decimal("0.5")
    if value < Decimal("1000"):
        return Decimal("1")
    return Decimal("5")


def _round_to_tick(value: Decimal, tick: Decimal, rounding: str) -> float:
    units = (value / tick).to_integral_value(rounding=rounding)
    return float(units * tick)


def calculate_limit_prices(reference_price: float) -> tuple[float, float]:
    reference = Decimal(str(reference_price))
    raw_up = reference * Decimal("1.10")
    raw_down = reference * Decimal("0.90")
    return (
        _round_to_tick(raw_up, tick_size(float(raw_up)), ROUND_FLOOR),
        _round_to_tick(raw_down, tick_size(float(raw_down)), ROUND_CEILING),
    )


def price_bucket(adjusted_previous_close: float, adjusted_close: float) -> int:
    if adjusted_previous_close <= 0:
        raise ValueError("前一日還原收盤價必須大於0")
    change_pct = (adjusted_close / adjusted_previous_close - 1.0) * 100.0
    # 浮點除法在恰好 -6%、-2% 等邊界可能多出極小誤差。
    if change_pct < -6.0 and not math.isclose(change_pct, -6.0, abs_tol=1e-9):
        return -2
    if change_pct < -2.0 and not math.isclose(change_pct, -2.0, abs_tol=1e-9):
        return -1
    if change_pct < 2.0 and not math.isclose(change_pct, 2.0, abs_tol=1e-9):
        return 0
    if change_pct < 6.0 and not math.isclose(change_pct, 6.0, abs_tol=1e-9):
        return 1
    return 2


def volume_bucket(volume: int, baseline: float) -> int | str:
    if volume <= 0 or baseline <= 0:
        return "X"
    ratio = volume / baseline
    if ratio < 0.5:
        return -2
    if ratio < 0.8:
        return -1
    if ratio < 1.25:
        return 0
    if ratio < 2.0:
        return 1
    return 2


def _is_same_price(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=0.0, abs_tol=float(tick_size(right)) / 10)


def encode_candles(candles: Iterable[dict], warmup_days: int = 20) -> list[DailyState]:
    rows = sorted(candles, key=lambda item: item["date"])
    states: list[DailyState] = []
    for index in range(warmup_days, len(rows)):
        row = rows[index]
        previous = rows[index - 1]
        prior_volumes = [int(item.get("volume", 0) or 0) for item in rows[index - warmup_days:index]]
        positive_prior_volumes = [value for value in prior_volumes if value > 0]
        baseline = statistics.median(positive_prior_volumes) if positive_prior_volumes else 0.0

        reference_price = float(row.get("reference_price") or previous["close"])
        calculated_up, calculated_down = calculate_limit_prices(reference_price)
        limit_up = float(row.get("limit_up") or calculated_up)
        limit_down = float(row.get("limit_down") or calculated_down)

        close = float(row["close"])
        close_limit = "U" if _is_same_price(close, limit_up) else "D" if _is_same_price(close, limit_down) else "N"
        states.append(
            DailyState(
                date=row["date"],
                price=price_bucket(reference_price, close),
                hit_up=float(row["high"]) >= limit_up,
                hit_down=float(row["low"]) <= limit_down,
                close_limit=close_limit,
                volume=volume_bucket(int(row.get("volume", 0) or 0), baseline),
            )
        )
    return states
