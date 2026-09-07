from datetime import date
import json
import sys

import pytest

from .. import analyze_atr


def test_bins_and_quantiles():
    result = analyze_atr.summarise([0, 1, 2, 3, 5], [1, 2, 3, 5])
    assert result["counts"] == [1, 1, 1, 1, 1]
    assert result["shares"] == [0.2] * 5
    assert result["percentiles_pct"]["P20"] == pytest.approx(0.8)
    assert analyze_atr.summarise([1, 2, 4], [1, 2, 4])["counts"] == [0, 1, 1, 1]


@pytest.mark.parametrize("bounds", [[1, 1, 2], [2, 1, 3], [0, 1, 2], [1, 2], [1, 2, float("nan")]])
def test_bad_boundaries(bounds):
    with pytest.raises(ValueError):
        analyze_atr.validate_boundaries(bounds)


def test_cli_cutoff_invalid_rows_and_output(tmp_path, monkeypatch):
    path = tmp_path / "2330.jsonl"
    rows = [{"date": "2019-01-01", "atr_ratio": 0.9},
            {"date": "2020-01-01", "atr_ratio": 0.01},
            {"date": "2021-01-01", "atr_ratio": 0.03},
            {"date": "2021-01-02"},
            {"date": "2021-01-03", "atr_ratio": -1},
            {"date": "2022-01-01", "atr_ratio": 0.9}]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    before = path.read_bytes()
    output = tmp_path / "out.json"
    monkeypatch.setattr(analyze_atr, "FEATURES_DIR", tmp_path)
    monkeypatch.setattr(sys, "argv", ["analyze_atr", "--from-date", "2020-01-01",
                                      "--as-of", "2021-12-31", "--symbols", "2330", "9999",
                                      "--output", str(output)])
    analyze_atr.main()
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["summary"]["count"] == 2
    assert report["summary"]["counts"] == [0, 1, 0, 1, 0]
    assert report["invalid_rows"] == 2
    assert report["missing_symbols"] == ["9999"]
    assert set(report["by_year"]) == {"2020", "2021"}
    assert path.read_bytes() == before


def test_no_data_and_repeated_quantiles(tmp_path):
    with pytest.raises(ValueError, match="沒有有效"):
        analyze_atr.analyze([], None, date(2026, 1, 1), [1, 2, 3, 5])
    path = tmp_path / "2330.jsonl"
    path.write_text('{"date":"2025-01-01","atr_ratio":0.02}\n', encoding="utf-8")
    report = analyze_atr.analyze([path], None, date(2026, 1, 1), [1, 2, 3, 5])
    assert report["candidate_usable"] is False
