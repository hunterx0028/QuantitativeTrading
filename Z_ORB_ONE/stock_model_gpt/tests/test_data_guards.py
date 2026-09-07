import json
import sys
from dataclasses import asdict
from datetime import date, timedelta

import pytest
import torch

from .. import prepare_features, predict, state_pipeline, validate_predictions
from ..config import Settings
from ..storage import read_jsonl
from ..training import build_model
from ..atr_calibration import ATR_ENCODING


def save_rows(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


@pytest.mark.parametrize("source", ["TaiwanStockDividendResult",
                                    "TaiwanStockCapitalReductionReferencePrice",
                                    "TaiwanStockSplitPrice"])
def test_prepare_and_validation_share_company_actions(tmp_path, monkeypatch, source):
    candles_dir = tmp_path / "candles"
    candles_dir.mkdir()
    path = candles_dir / "2330.jsonl"
    rows = [{"date": (date(2026, 1, 1) + timedelta(days=i)).isoformat(),
             "open": 100, "high": 101, "low": 99, "close": 100, "volume": 100}
            for i in range(22)]
    rows[20].update(open=90, high=99, low=90, close=99, reference_price=100)
    save_rows(path, rows)
    action_path = tmp_path / "actions.jsonl"
    save_rows(action_path, [{"date": "2026-01-21", "source": source,
                             "reference_price": 90, "limit_up": 99, "limit_down": 81},
                            {"date": "2026-01-22", "source": source,
                             "reference_price": 1}])
    monkeypatch.setattr(state_pipeline, "corporate_action_path", lambda symbol: action_path)
    monkeypatch.setattr(prepare_features, "CANDLES_DIR", candles_dir)
    feature_path = tmp_path / "features.jsonl"
    monkeypatch.setattr(prepare_features, "feature_path", lambda symbol: feature_path)
    monkeypatch.setattr(prepare_features, "write_jsonl", save_rows)
    monkeypatch.setattr(sys, "argv", ["prepare_features", "--as-of", "2026-01-21"])
    prepare_features.main()
    monkeypatch.setattr(validate_predictions, "CANDLES_DIR", candles_dir)
    states, actuals = validate_predictions.load_actuals(
        [{"symbol": "2330"}], "2026-01-21", Settings())
    assert read_jsonl(feature_path) == [asdict(states["2330"])]
    assert states["2330"].price == 2
    assert states["2330"].hit_up is True
    assert actuals[0]["reference_price_source"] == source
    assert actuals[0]["reference_price"] == 90
    assert actuals[0]["close"] == 99


def test_prediction_filters_stale_missing_short_and_future_data(tmp_path, monkeypatch):
    monkeypatch.setattr(predict, "FEATURES_DIR", tmp_path)
    save_rows(tmp_path / "fresh.jsonl", [{"date": "2026-01-22"},
                                        {"date": "2026-01-21"}, {"date": "2026-01-20"}])
    save_rows(tmp_path / "stale.jsonl", [{"date": "2026-01-19"}, {"date": "2026-01-20"}])
    save_rows(tmp_path / "short.jsonl", [{"date": "2026-01-21"}])
    save_rows(tmp_path / "future.jsonl", [{"date": "2026-01-22"}])
    eligible, skipped = predict.select_prediction_inputs(
        {"fresh", "stale", "short", "future", "missing"}, date(2026, 1, 21), 2)
    assert list(eligible) == ["fresh"]
    assert eligible["fresh"] == [{"date": "2026-01-20"}, {"date": "2026-01-21"}]
    assert {r["symbol"]: r["reason"] for r in skipped} == {
        "stale": "stale_features", "short": "insufficient_history",
        "future": "no_features_as_of", "missing": "missing_features"}


def test_all_ineligible_fails_without_overwriting_prediction(tmp_path, monkeypatch):
    settings = Settings(context_days=2, d_model=16, n_heads=2, n_layers=1,
                        atr_boundaries_pct=[1, 2, 3, 5])
    checkpoint = {"model": build_model(settings).state_dict(), "settings": asdict(settings),
                  "training_as_of": "2026-01-20", "atr_encoding": ATR_ENCODING,
                  "atr_calibration": {"method": "explicit"}}
    monkeypatch.setattr(torch, "load", lambda *a, **kw: checkpoint)
    monkeypatch.setattr(predict, "select_device", lambda: torch.device("cpu"))
    monkeypatch.setattr(predict, "ensure_runtime_dirs", lambda: None)
    monkeypatch.setattr(predict, "UNIVERSE_DIR", tmp_path)
    monkeypatch.setattr(predict, "FEATURES_DIR", tmp_path)
    monkeypatch.setattr(predict, "PREDICTIONS_DIR", tmp_path)
    (tmp_path / "2026-01-21.json").write_text(json.dumps({"stocks": [
        {"label": "台積電:2330.TW", "symbol": "2330", "exchange": "TWSE"}]}), encoding="utf-8")
    output = tmp_path / "2026-01-22.json"
    output.write_text("existing prediction", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["predict", "--checkpoint", "unused.pt",
                                      "--universe-date", "2026-01-21",
                                      "--prediction-date", "2026-01-22"])
    with pytest.raises(RuntimeError, match="未產生預測"):
        predict.main()
    assert output.read_text(encoding="utf-8") == "existing prediction"
