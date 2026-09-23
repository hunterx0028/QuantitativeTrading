from __future__ import annotations

import argparse
from datetime import date

from .config import Settings
from .state_pipeline import load_candle_states
from .paths import CANDLES_DIR
from .storage import feature_path, write_jsonl
from .trading_calendar import TradingCalendar
from .market_indices import load_index_features, join_index_features


from .runtime_lock import locked


@locked
def main() -> None:
    parser = argparse.ArgumentParser(description="將日K轉為每日離散狀態")
    parser.add_argument("--settings", default=None)
    parser.add_argument("--as-of", required=True, help="特徵截止日 YYYY-MM-DD")
    args = parser.parse_args()
    as_of = date.fromisoformat(args.as_of)
    TradingCalendar().require_session(as_of)
    cutoff = as_of.isoformat()
    settings = Settings.load(args.settings) if args.settings else Settings.load()
    indices = load_index_features(cutoff)
    if cutoff not in indices:
        raise ValueError(f"{cutoff} 缺少兩組指數當日／前一交易日 OHLC，請先執行 update_data")
    for path in sorted(list(CANDLES_DIR.glob("*.jsonl"))):
        candles, states = load_candle_states(path, cutoff, settings.warmup_days)
        rows = join_index_features(states, indices)
        write_jsonl(feature_path(path.stem), rows)
        print(
            f"{path.stem}: as_of={cutoff} candles={len(candles)} states={len(rows)} "
            f"missing_index_days={len(states) - len(rows)}"
        )


if __name__ == "__main__":
    main()
