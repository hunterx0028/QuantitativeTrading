from pathlib import Path

from .features import DailyState, encode_candles
from .finmind import apply_corporate_actions
from .night_futures import load_night_futures
from .storage import corporate_action_path, read_jsonl
from .trading_calendar import shared_calendar


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
    night_futures_by_date = load_night_futures()
    calendar = shared_calendar()
    segments = []
    for row in candles:
        calendar.require_session(row["date"])
        if not segments or calendar.next_session(segments[-1][-1]["date"]) != row["date"]:
            if segments:
                print(f"[GAP] {path.stem}: {segments[-1][-1]['date']} → {row['date']}，重新累積暖機與連續輸入")
            segments.append([])
        segments[-1].append(row)
    states = [state for segment in segments for state in encode_candles(segment, warmup_days, night_futures_by_date)]
    return candles, states
