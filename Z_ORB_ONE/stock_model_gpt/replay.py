"""Select dated next-day targets; keep their complete historical contexts."""
import math
import random
from collections import Counter, defaultdict
from datetime import date

from .config import Settings
from .dataset import StockSequenceDataset


def select_daily_sequences(dataset: StockSequenceDataset, previous_as_of: str,
                           as_of: date, settings: Settings, seen_symbols: set[str]) -> dict:
    mode = settings.daily_training_mode
    if mode not in ("incremental_replay", "full_history"):
        raise ValueError("daily_training_mode 必須為 incremental_replay 或 full_history")
    if not math.isfinite(settings.daily_replay_ratio) or settings.daily_replay_ratio < 0:
        raise ValueError("daily_replay_ratio 必須為有限非負數")
    for value in (settings.daily_replay_max_sequences, settings.daily_replay_per_symbol):
        if type(value) is not int or value < 0:
            raise ValueError("重播筆數限制必須為非負整數")
    seed = settings.seed + as_of.toordinal()
    rng = random.Random(seed)
    new_refs = []
    history = defaultdict(list)
    for ref in dataset.refs:
        target_date = dataset.rows_by_path[ref.feature_path][ref.end]["date"]
        if target_date > as_of.isoformat():
            raise ValueError("訓練序列包含截止日之後的目標")
        if target_date > previous_as_of:
            new_refs.append(ref)
        else:
            history[ref.feature_path.stem].append(ref)
    if mode == "full_history":
        replay_refs = [ref for refs in history.values() for ref in refs]
    else:
        budget = min(math.ceil(len(new_refs) * settings.daily_replay_ratio),
                     settings.daily_replay_max_sequences)
        # Random samples per stock, then round-robin to avoid large histories dominating.
        pools = {symbol: rng.sample(refs, min(len(refs), settings.daily_replay_per_symbol))
                 for symbol, refs in sorted(history.items())}
        symbols = list(pools)
        rng.shuffle(symbols)
        replay_refs = []
        while len(replay_refs) < budget and symbols:
            remaining = []
            for symbol in symbols:
                if pools[symbol] and len(replay_refs) < budget:
                    replay_refs.append(pools[symbol].pop())
                if pools[symbol]:
                    remaining.append(symbol)
            symbols = remaining
    dataset.refs = new_refs + replay_refs
    manifest = [{"symbol": ref.feature_path.stem,
                 "target_date": dataset.rows_by_path[ref.feature_path][ref.end]["date"],
                 "input_start_date": dataset.rows_by_path[ref.feature_path][ref.end - dataset.context_days]["date"],
                 "kind": kind}
                for kind, refs in (("new", new_refs), ("replay", replay_refs)) for ref in refs]
    return {"mode": mode, "previous_as_of": previous_as_of, "as_of": as_of.isoformat(),
            "seed": seed, "new_count": len(new_refs), "replay_count": len(replay_refs),
            "available_history_count": sum(map(len, history.values())),
            "replay_by_symbol": dict(Counter(ref.feature_path.stem for ref in replay_refs)),
            "new_symbols": sorted({ref.feature_path.stem for ref in dataset.refs} - seen_symbols),
            "sequences": manifest}
