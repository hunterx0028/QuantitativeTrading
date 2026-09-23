"""Ordered input contract shared by feature encoding, models and snapshots."""

INDEX_SYMBOLS = ("IX0001", "IX0043")
OHLC_FIELDS = ("open", "high", "low", "close")
INDEX_FIELDS = tuple(f"{symbol.lower()}_{field}" for symbol in INDEX_SYMBOLS for field in OHLC_FIELDS)
INPUT_FIELDS = (
    "open_price", "high_price", "low_price", "close_price",
    "hit_up", "hit_down", "close_limit", "volume", "atr_ratio",
    *INDEX_FIELDS, "night_futures",
)
INPUT_ALIGNMENT = "stock_seventeen_index_ohlc_next_trading_day_night_v3"
FEATURE_WEIGHT_NAMES = ("stock_price_weight", "stock_activity_weight", "market_weight")
