from datetime import date

from .dataset import StockSequenceDataset
from .features import calculate_limit_prices, encode_candles, price_bucket, volume_bucket
from .finmind import DATASET_ACCEPTS_DATA_ID, FREE_DATASETS, apply_corporate_actions
from .market_data import _normalise_candle
from .predict import SignalThresholds, detect_signal
from .validate_predictions import evaluate_signal_trade
from .storage import write_jsonl


def test_price_bucket_boundaries():
    assert price_bucket(100, 94) == -1
    assert price_bucket(100, 98) == 0
    assert price_bucket(100, 102) == 1
    assert price_bucket(100, 106) == 2


def test_volume_bucket_boundaries():
    assert volume_bucket(49, 100) == -2
    assert volume_bucket(50, 100) == -1
    assert volume_bucket(80, 100) == 0
    assert volume_bucket(125, 100) == 1
    assert volume_bucket(200, 100) == 2
    assert volume_bucket(0, 100) == "X"


def test_limit_tick_rounding():
    assert calculate_limit_prices(100) == (110.0, 90.0)
    assert calculate_limit_prices(67.8) == (74.5, 61.1)


def test_encode_uses_prior_20_days_only():
    candles = []
    for day in range(1, 22):
        candles.append(
            {
                "date": f"2026-01-{day:02d}",
                "open": 100,
                "high": 100,
                "low": 100,
                "close": 100,
                "volume": 100 if day <= 20 else 200,
                "adjustment_factor": 1,
            }
        )
    states = encode_candles(candles)
    assert len(states) == 1
    assert states[0].volume == 2


def test_finmind_action_overrides_reference_and_limits():
    candle = {"date": "2026-01-02", "close": 91, "reference_price": 100}
    action = {
        "date": "2026-01-02",
        "source": "TaiwanStockDividendResult",
        "reference_price": 90,
        "limit_up": 99,
        "limit_down": 81,
    }
    result = apply_corporate_actions([candle], [action])[0]
    assert result["reference_price"] == 90
    assert result["limit_up"] == 99
    assert result["limit_down"] == 81


def test_finmind_global_datasets_do_not_send_data_id():
    assert FREE_DATASETS == ("TaiwanStockDividendResult",)
    assert DATASET_ACCEPTS_DATA_ID["TaiwanStockSplitPrice"] is False
    assert DATASET_ACCEPTS_DATA_ID["TaiwanStockCapitalReductionReferencePrice"] is False
    assert DATASET_ACCEPTS_DATA_ID["TaiwanStockParValueChange"] is False
    assert DATASET_ACCEPTS_DATA_ID["TaiwanStockDividendResult"] is True


def test_invalid_candle_is_skipped():
    assert _normalise_candle(
        {
            "date": "2026-01-02",
            "open": None,
            "high": 10,
            "low": 9,
            "close": 9.5,
            "volume": 0,
            "change": None,
        }
    ) is None


def test_valid_candle_with_null_volume_is_kept():
    result = _normalise_candle(
        {
            "date": "2026-01-02",
            "open": 9.5,
            "high": 10,
            "low": 9,
            "close": 9.8,
            "volume": None,
            "change": 0.3,
        }
    )
    assert result is not None
    assert result["volume"] == 0


def test_dataset_hard_filters_future_targets(tmp_path):
    path = tmp_path / "2330.jsonl"
    rows = [
        {
            "date": f"2026-01-0{day}",
            "price": 0,
            "hit_up": False,
            "hit_down": False,
            "close_limit": "N",
            "volume": 0,
        }
        for day in range(1, 5)
    ]
    write_jsonl(path, rows)
    dataset = StockSequenceDataset([path], context_days=2, max_target_date=date(2026, 1, 3))
    assert len(dataset) == 1


def test_dataset_uses_five_inputs_and_three_targets(tmp_path):
    path = tmp_path / "2330.jsonl"
    rows = [
        {
            "date": f"2026-01-0{day}",
            "price": 0,
            "hit_up": day == 3,
            "hit_down": False,
            "close_limit": "N",
            "volume": 0,
        }
        for day in range(1, 4)
    ]
    write_jsonl(path, rows)
    dataset = StockSequenceDataset([path], context_days=2)
    inputs, targets = dataset[0]
    assert inputs.shape == (2, 5)
    assert set(targets) == {"price", "hit_up", "hit_down"}


def test_detect_signal_flags_short_setup():
    prediction = {
        "symbol": "2464",
        "prediction_date": "2026-09-04",
        "price": {"-2": 0.7048, "-1": 0.1647, "0": 0.0624, "1": 0.0318, "2": 0.0362},
        "hit_up": {"F": 0.9595, "T": 0.0405},
        "hit_down": {"F": 0.3842, "T": 0.6158},
    }
    signal = detect_signal(prediction, 0.6)
    assert signal is not None
    assert signal["side"] == "SHORT"
    assert signal["symbol"] == "2464"
    assert signal["reason"] == "both"


def test_detect_signal_flags_long_setup():
    prediction = {
        "symbol": "2330",
        "prediction_date": "2026-09-04",
        "price": {"-2": 0.01, "-1": 0.02, "0": 0.05, "1": 0.12, "2": 0.8},
        "hit_up": {"F": 0.2, "T": 0.8},
        "hit_down": {"F": 0.99, "T": 0.01},
    }
    signal = detect_signal(prediction, 0.6)
    assert signal is not None
    assert signal["side"] == "LONG"
    assert signal["reason"] == "both"


def test_detect_signal_flags_short_setup_when_only_price_passes():
    prediction = {
        "symbol": "2464",
        "prediction_date": "2026-09-04",
        "price": {"-2": 0.7048, "-1": 0.1647, "0": 0.0624, "1": 0.0318, "2": 0.0362},
        "hit_up": {"F": 0.9595, "T": 0.0405},
        "hit_down": {"F": 0.55, "T": 0.45},
    }
    signal = detect_signal(prediction, SignalThresholds(short_hit=0.6, short_price=0.6))
    assert signal is not None
    assert signal["side"] == "SHORT"
    assert signal["reason"] == "price"


def test_detect_signal_flags_long_setup_when_only_hit_passes():
    prediction = {
        "symbol": "2330",
        "prediction_date": "2026-09-04",
        "price": {"-2": 0.01, "-1": 0.02, "0": 0.05, "1": 0.32, "2": 0.55},
        "hit_up": {"F": 0.2, "T": 0.8},
        "hit_down": {"F": 0.99, "T": 0.01},
    }
    signal = detect_signal(prediction, SignalThresholds(long_hit=0.6, long_price=0.6))
    assert signal is not None
    assert signal["side"] == "LONG"
    assert signal["reason"] == "hit"


def test_signal_trade_success_uses_intraday_profit_not_classification_match():
    signal = {"side": "SHORT"}
    candle = {
        "open": 223.5,
        "high": 224.0,
        "low": 203.0,
        "close": 211.0,
        "actual_state": {"hit_down": False},
    }
    trade = evaluate_signal_trade(signal, candle, target_profit_pct=3.0, max_adverse_pct=2.0)
    assert trade["actual_hit"] is False
    assert round(trade["best_profit_pct"], 2) == 9.17
    assert round(trade["adverse_pct"], 2) == 0.22
    assert trade["success"] is True
