from dataclasses import replace
from datetime import date
import json

from ..config import Settings
from ..dataset import StockSequenceDataset
from ..replay import select_daily_sequences


def dataset(tmp_path):
    paths = []
    for symbol in ("2330", "9999"):
        path = tmp_path / f"{symbol}.jsonl"
        rows = [{"date": f"2026-01-{i:02d}", "price": 0, "hit_up": False,
                 "hit_down": False, "close_limit": "N", "volume": 0, "atr_ratio": 0.02}
                for i in range(1, 12)]
        path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        paths.append(path)
    return StockSequenceDataset(paths, 2, max_target_date=date(2026, 1, 10))


def test_incremental_replay_dates_caps_context_and_reproducibility(tmp_path):
    settings = Settings(daily_replay_ratio=2, daily_replay_max_sequences=3,
                        daily_replay_per_symbol=2)
    data = dataset(tmp_path)
    report = select_daily_sequences(data, "2026-01-08", date(2026, 1, 10), settings, {"2330"})
    assert report["new_count"] == 4
    assert report["replay_count"] == 3
    assert report["new_symbols"] == ["9999"]
    assert max(report["replay_by_symbol"].values()) <= 2
    assert len(data) == 7
    assert all("2026-01-08" < r["target_date"] <= "2026-01-10"
               for r in report["sequences"] if r["kind"] == "new")
    assert all(r["target_date"] <= "2026-01-08"
               for r in report["sequences"] if r["kind"] == "replay")
    assert data[0][0].shape == (2, 6)
    assert report["sequences"][0]["input_start_date"] == "2026-01-07"
    assert len({(r["symbol"], r["target_date"]) for r in report["sequences"]}) == 7
    repeated = select_daily_sequences(dataset(tmp_path), "2026-01-08", date(2026, 1, 10), settings, {"2330"})
    assert repeated == report


def test_full_history_and_no_new_targets(tmp_path):
    settings = Settings(daily_training_mode="full_history")
    data = dataset(tmp_path)
    report = select_daily_sequences(data, "2026-01-08", date(2026, 1, 10), settings, set())
    assert report["new_count"] == 4
    assert report["replay_count"] == 12
    assert len(data) == 16
    data = dataset(tmp_path)
    report = select_daily_sequences(data, "2026-01-10", date(2026, 1, 10),
                                    replace(settings, daily_training_mode="incremental_replay"), set())
    assert len(data) == 0
    assert report["new_count"] == report["replay_count"] == 0
