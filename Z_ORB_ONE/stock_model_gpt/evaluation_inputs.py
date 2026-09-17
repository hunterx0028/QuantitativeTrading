"""Version the actual data and label settings consumed by validation."""
import json
from types import SimpleNamespace

from . import paths, trading_calendar
from .provenance import fingerprint, file_fingerprint
from .storage import read_jsonl


def snapshot(symbols, day, settings):
    symbols = sorted(set(symbols))
    def through(path):
        return sorted((row for row in read_jsonl(path) if row["date"] <= day),
                      key=lambda row: (row["date"], row.get("source", "")))
    candles = {symbol: through(paths.CANDLES_DIR / f"{symbol}.jsonl") for symbol in symbols}
    years = sorted({row["date"][:4] for rows in candles.values() for row in rows})
    calendars = {}
    for year in years:
        path = trading_calendar.CALENDAR_DIR / f"{year}.json"
        calendars[year] = ({key: value for key, value in json.loads(path.read_text(encoding="utf-8"))["sessions"].items()
                            if key <= day} if path.exists() else None)
    def records(name):
        path = trading_calendar.CALENDAR_DIR / name
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    suspensions = records("suspensions.json")
    inputs = {
        "candles": candles,
        "actions": {symbol: through(paths.CORPORATE_ACTIONS_DIR / f"{symbol}.jsonl") for symbol in symbols},
        "nights": through(paths.NIGHT_FUTURES_PATH),
        "calendar": calendars,
        "closures": {key: value for key, value in records("overrides.json").items() if key <= day},
        "suspensions": {symbol: suspensions.get(f"{symbol}|{day}") for symbol in symbols},
        "warmup_days": settings.warmup_days,
        "label_code": {name: file_fingerprint(paths.PACKAGE_DIR / name)
                       for name in ("features.py", "state_pipeline.py", "validate_predictions.py",
                                    "finmind.py", "trading_calendar.py", "night_futures.py",
                                    "signals.py", "classification_metrics.py")},
    }
    return {"schema": 1, "symbols": symbols, "warmup_days": settings.warmup_days,
            "sha256": fingerprint(inputs)}


def matches(record, settings=None):
    saved = record.get("actual_data_version")
    if not saved or saved.get("schema") != 1:
        return False
    settings = settings or SimpleNamespace(warmup_days=saved["warmup_days"])
    return saved == snapshot(saved["symbols"], record["prediction_date"], settings)
