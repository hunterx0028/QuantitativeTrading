from __future__ import annotations

import argparse
from datetime import date

from .config import Settings
from .features import encode_candles
from .finmind import apply_corporate_actions
from .paths import CANDLES_DIR
from .storage import corporate_action_path, feature_path, read_jsonl, write_jsonl


def main() -> None:
    parser = argparse.ArgumentParser(description="將日K轉為每日離散狀態")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--as-of", required=True, help="特徵截止日 YYYY-MM-DD")
    args = parser.parse_args()
    as_of = date.fromisoformat(args.as_of)
    cutoff = as_of.isoformat()
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    for path in sorted(list(CANDLES_DIR.glob("*.jsonl"))):
        candles = [row for row in read_jsonl(path) if row["date"] <= cutoff]
        actions = [
            row for row in read_jsonl(corporate_action_path(path.stem))
            if row["date"] <= cutoff
        ]
        candles = apply_corporate_actions(candles, actions)
        states = encode_candles(candles, settings.warmup_days)
        write_jsonl(feature_path(path.stem), (state.to_dict() for state in states))
        print(
            f"{path.stem}: as_of={cutoff} candles={len(candles)} states={len(states)}"
        )


if __name__ == "__main__":
    main()
