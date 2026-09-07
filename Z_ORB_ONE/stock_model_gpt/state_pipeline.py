from pathlib import Path

from .features import DailyState, encode_candles
from .finmind import apply_corporate_actions
from .storage import corporate_action_path, read_jsonl


def load_candle_states(
    path: Path, cutoff: str, warmup_days: int,
) -> tuple[list[dict], list[DailyState]]:
    """Shared as-of company-action processing for training and evaluation."""
    candles = sorted(
        (row for row in read_jsonl(path) if row["date"] <= cutoff),
        key=lambda row: row["date"],
    )
    actions = [row for row in read_jsonl(corporate_action_path(path.stem))
               if row["date"] <= cutoff]
    candles = apply_corporate_actions(candles, actions)
    return candles, encode_candles(candles, warmup_days)
