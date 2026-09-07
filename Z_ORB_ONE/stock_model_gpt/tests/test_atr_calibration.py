from dataclasses import replace
from datetime import date
import json
import sys

import pytest
import torch

from .. import atr_calibration, training, predict
from ..config import Settings
from ..dataset import encode_state


def write_features(path, ratios):
    rows = [{"date": f"2026-01-{i + 1:02d}", "price": 0, "hit_up": False,
             "hit_down": False, "close_limit": "N", "volume": 0, "atr_ratio": ratio}
            for i, ratio in enumerate(ratios)]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def test_fits_only_as_of_and_daily_keeps_checkpoint_bins(tmp_path, monkeypatch):
    monkeypatch.setattr(atr_calibration, "DATA_DIR", tmp_path)
    path = tmp_path / "2330.jsonl"
    write_features(path, [0.01, 0.02, 0.03, 0.04, 0.05, 0.9])
    settings, origin, report_path = atr_calibration.prepare_atr_levels(
        Settings(atr_boundaries_pct=[10, 20, 30, 40]), [path], date(2026, 1, 5))
    assert settings.atr_boundaries_pct == pytest.approx([1.8, 2.6, 3.4, 4.2])
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["summary"]["count"] == 5
    checkpoint = {"settings": {"atr_boundaries_pct": settings.atr_boundaries_pct},
                  "atr_calibration": origin, "atr_report": str(report_path)}
    def unexpected_analysis(*args, **kwargs):
        pytest.fail("daily training must not analyze ATR distribution")
    monkeypatch.setattr(atr_calibration, "analyze", unexpected_analysis)
    monkeypatch.setattr(atr_calibration, "read_jsonl", unexpected_analysis)
    reports_before = set(tmp_path.rglob("*.json"))
    daily, daily_origin, path_report = atr_calibration.prepare_atr_levels(
        replace(settings, atr_boundaries_pct=[10, 20, 30, 40]), [path], date(2026, 1, 6), checkpoint)
    assert daily.atr_boundaries_pct == settings.atr_boundaries_pct
    assert daily_origin == origin
    assert path_report == report_path
    assert set(tmp_path.rglob("*.json")) == reports_before


def test_degenerate_fallback_and_exact_bucket_boundaries(tmp_path, monkeypatch):
    monkeypatch.setattr(atr_calibration, "DATA_DIR", tmp_path)
    path = tmp_path / "2330.jsonl"
    write_features(path, [0.02] * 5)
    settings, origin, _ = atr_calibration.prepare_atr_levels(Settings(), [path], date(2026, 1, 5))
    assert settings.atr_boundaries_pct == [1, 2, 3, 5]
    assert origin["method"] == "fixed_fallback_degenerate_quantiles"
    row = json.loads(path.read_text().splitlines()[0])
    assert encode_state(row, settings.atr_boundaries_pct)[5] == 2
    assert encode_state({**row, "atr_ratio": 0.019}, settings.atr_boundaries_pct)[5] == 1


def test_settings_reject_manual_bins_and_save_only_user_settings(tmp_path):
    path = tmp_path / "settings.json"
    Settings(atr_boundaries_pct=[1, 2, 3, 5]).save(path)
    assert "atr_boundaries_pct" not in json.loads(path.read_text(encoding="utf-8"))
    assert Settings.load(path).atr_boundaries_pct is None
    path.write_text('{"atr_boundaries_pct": [1, 2, 3, 5]}', encoding="utf-8")
    with pytest.raises(ValueError, match="自動產生"):
        Settings.load(path)


def test_initial_cli_rejects_manual_boundaries(monkeypatch):
    from .. import train_initial
    monkeypatch.setattr(sys, "argv", ["train_initial", "--as-of", "2026-01-05",
                                      "--atr-boundaries-pct", "1", "2", "3", "5"])
    with pytest.raises(SystemExit) as exc:
        train_initial.main()
    assert exc.value.code == 2


def test_initial_daily_checkpoint_integration(tmp_path, monkeypatch):
    monkeypatch.setattr(training, "ensure_runtime_dirs", lambda: None)
    monkeypatch.setattr(training, "FEATURES_DIR", tmp_path)
    monkeypatch.setattr(training, "CHECKPOINT_DIR", tmp_path)
    monkeypatch.setattr(training, "recent_symbols", lambda *args: {"2330"})
    monkeypatch.setattr(training, "select_device", lambda: torch.device("cpu"))
    monkeypatch.setattr(atr_calibration, "DATA_DIR", tmp_path)
    write_features(tmp_path / "2330.jsonl", [0.01, 0.02, 0.03, 0.04, 0.05, 0.06])
    settings = Settings(context_days=2, d_model=8, n_heads=2, n_layers=1,
                        epochs=1, daily_epochs=1, batch_size=8, dropout=0)
    first = training.train(settings, as_of=date(2026, 1, 5))
    first_checkpoint = torch.load(first, weights_only=False)
    training.ensure_checkpoint_compatible(first_checkpoint)
    assert "atr_embedding.weight" in first_checkpoint["model"]
    assert "atr_projection.weight" not in first_checkpoint["model"]
    second = training.train(replace(settings, atr_boundaries_pct=[10, 20, 30, 40]),
                            resume_path=first, daily=True, as_of=date(2026, 1, 6))
    second_checkpoint = torch.load(second, weights_only=False)
    assert second_checkpoint["settings"]["atr_boundaries_pct"] == first_checkpoint["settings"]["atr_boundaries_pct"]
    assert second_checkpoint["atr_calibration"] == first_checkpoint["atr_calibration"]
    assert second_checkpoint["atr_report"] == first_checkpoint["atr_report"]
    assert len(list((tmp_path / "atr_analysis").glob("*.json"))) == 1
    assert second_checkpoint["training_as_of"] == "2026-01-06"
    assert second_checkpoint["sampling"]["new_count"] == 1
    assert second_checkpoint["sampling"]["replay_count"] == 1
    assert training.train(settings, resume_path=second, daily=True, as_of=date(2026, 1, 6)) == second
    assert training.train(settings, resume_path=first, daily=True, as_of=date(2026, 1, 6)) == second.resolve()
    assert len(list(tmp_path.glob("stock_model_gpt_*.pt"))) == 2
    assert training.train(settings, resume_path=second, daily=True, as_of=date(2026, 1, 7)) == second
    full = training.train(replace(settings, daily_training_mode="full_history"),
                          resume_path=first, daily=True, as_of=date(2026, 1, 6), force_retrain=True)
    assert torch.load(full, weights_only=False)["sampling"]["replay_count"] == 3
    universe_dir = tmp_path / "universe"
    universe_dir.mkdir()
    (universe_dir / "2026-01-06.json").write_text(json.dumps({"stocks": [
        {"label": "台積電:2330.TW", "symbol": "2330", "exchange": "TWSE"}]}), encoding="utf-8")
    monkeypatch.setattr(predict, "ensure_runtime_dirs", lambda: None)
    monkeypatch.setattr(predict, "UNIVERSE_DIR", universe_dir)
    monkeypatch.setattr(predict, "FEATURES_DIR", tmp_path)
    monkeypatch.setattr(predict, "PREDICTIONS_DIR", tmp_path)
    monkeypatch.setattr(predict, "SIGNAL_REPORTS_DIR", tmp_path)
    monkeypatch.setattr(predict, "select_device", lambda: torch.device("cpu"))
    monkeypatch.setattr(sys, "argv", ["predict", "--checkpoint", str(second),
                                      "--universe-date", "2026-01-06",
                                      "--prediction-date", "2026-01-07"])
    predict.main()
    prediction = json.loads((tmp_path / "2026-01-07.json").read_text(encoding="utf-8"))
    assert prediction["atr_boundaries_pct"] == first_checkpoint["settings"]["atr_boundaries_pct"]
    assert prediction["predicted_count"] == 1
    assert "direction_signals" not in prediction
    assert "signals" in prediction
    assert set(prediction["signal_thresholds"]) == {"long_hit", "long_price", "short_hit", "short_price"}
    txt_report = (tmp_path / "2026-01-07.txt").read_text(encoding="utf-8")
    assert "方向訊號" in txt_report
    assert prediction["predictions"][0]["input_last_date"] == "2026-01-06"
    with pytest.raises(RuntimeError, match="未來模型"):
        training.train(settings, resume_path=second, daily=True, as_of=date(2026, 1, 5))
