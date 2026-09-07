from datetime import date
from types import SimpleNamespace

import pytest

from ..features import DailyState
from ..predict import (SignalThresholds, build_signal_report_lines, detect_signal,
                      detect_direction_signal)
from ..validate_predictions import print_summary, evaluate_signal_trade
from ..post_training_gate import print_window


def prediction():
    return {"symbol": "2330", "prediction_date": "2026-09-07",
            "price": {"-2": 0.3, "-1": 0.2, "0": 0, "1": 0.2, "2": 0.3},
            "hit_up": {"F": 0.2, "T": 0.8}, "hit_down": {"F": 0.3, "T": 0.7}}


@pytest.mark.parametrize("long_hit,long_price,short_hit,short_price,long_reason,short_reason", [
    (0.8, 0.1, 0.7, 0.1, "hit", "hit"),
    (0.8, 0.1, 0.1, 0.7, "hit", "price"),
    (0.8, 0.7, 0.7, 0.1, "both", "hit"),
    (0.1, 0.7, 0.7, 0.1, "price", "hit"),
])
def test_conflict_preserves_both_sides(long_hit, long_price, short_hit, short_price,
                                       long_reason, short_reason):
    row = prediction()
    row["hit_up"]["T"], row["hit_down"]["T"] = long_hit, short_hit
    row["price"]["2"], row["price"]["-2"] = long_price, short_price
    signal = detect_signal(row)
    assert signal["side"] == "CONFLICT"
    assert signal["long"]["reason"] == long_reason
    assert signal["short"]["reason"] == short_reason
    assert signal["long"]["hit_probability"] == long_hit
    assert signal["short"]["price_probability"] == short_price


def test_conflict_report_evaluation_and_gate(capsys):
    row = prediction()
    thresholds = SignalThresholds(long_direction=0.5, short_direction=0.5)
    signal = detect_signal(row, thresholds)
    direction = detect_direction_signal(row, thresholds)
    assert direction["side"] == "CONFLICT"
    report = "\n".join(build_signal_report_lines(date(2026, 9, 7), thresholds, [signal], [direction]))
    assert "[SIGNAL CONFLICT] 2330" in report
    assert "hit_up.T=0.8000" in report and "hit_down.T=0.7000" in report
    assert "[DIRECTION CONFLICT] 2330" in report
    actual = DailyState("2026-09-07", 0, True, True, "N", 0, 2, 0.02)
    candle = {"symbol": "2330", "open": 100, "high": 110, "low": 90, "close": 100,
              "actual_state": actual.to_dict()}
    summary = print_summary([row], {"2330": actual}, [candle], thresholds, 3, 2)
    assert summary["signals"] == []
    assert "direction_signals" not in summary
    assert "direction_conflicts" not in summary
    assert set(summary["signal_thresholds"]) == {"long_hit", "long_price", "short_hit", "short_price"}
    assert summary["evaluated"] == 1
    assert summary["conflicts"][0]["actual_hit_up"] is True
    assert summary["conflicts"][0]["actual_hit_down"] is True
    assert "success" not in summary["conflicts"][0]
    # Legacy direction fields must not influence gate output or recommendations.
    summary["direction_signals"] = [{"side": "LONG"}]
    summary["direction_conflicts"] = [{"side": "CONFLICT"}]
    print_window("test", [summary], SimpleNamespace())
    text = capsys.readouterr().out
    assert "實際雙觸及=1" in text
    assert "漲跌訊號: 0 筆" in text
    assert "DIRECTION" not in text
    assert "方向訊號" not in text
    with pytest.raises(ValueError, match="CONFLICT"):
        evaluate_signal_trade(signal, candle, 3, 2)


def test_no_signal_and_single_sides_are_preserved():
    row = prediction()
    row["hit_up"]["T"] = 0.1
    assert detect_signal(row)["side"] == "SHORT"
    row["hit_down"]["T"] = 0.1
    assert detect_signal(row) is None
    row["hit_up"]["T"] = 0.6
    assert detect_signal(row)["side"] == "LONG"
