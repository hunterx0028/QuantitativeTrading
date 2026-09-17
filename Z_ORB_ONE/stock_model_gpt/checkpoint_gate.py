"""Minimal champion/challenger safety gate.

There is no offline backtest in this pipeline, so a freshly trained checkpoint
cannot be evaluated before it is used. Instead this acts as a circuit breaker:
after each day's `validate_predictions` run, compare the recent short-window
signal success rate against the longer baseline window (same two windows
`post_training_gate.py` already reports). If it has dropped sharply, flag the
gate as DEGRADED so `predict.py` refuses to silently keep auto-selecting the
current checkpoint for live signals. Forward observation forecasts allow
continued evaluation and recovery without publishing formal signals.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime

from .config import Settings
from .signals import OUTPUT_SCHEMA
from .paths import CHECKPOINT_DIR, EVALUATIONS_DIR, PREDICTIONS_DIR
from .provenance import file_fingerprint, atomic_text
from .evaluation_inputs import matches as actual_inputs_match


GATE_STATUS_PATH = CHECKPOINT_DIR / "gate_status.json"


def evaluation_matches_official(record, predictions_dir=None):
    digest = record.get("prediction_content_sha256")
    if digest is None:
        return True  # Legacy evaluations have no prediction fingerprint.
    directory = predictions_dir if predictions_dir is not None else PREDICTIONS_DIR
    if record.get("mode") == "observation":
        directory = directory / "observations"
    path = directory / f"{record['prediction_date']}.json"
    return path.exists() and file_fingerprint(path) == digest


def target_evaluations(records: list[dict], target: str) -> list[dict]:
    """Select one target and its latest saved filter without mixing filters."""
    rows = []
    for record in records:
        row = record.get("targets", {}).get(target)
        if row is None and target == "high_price":
            row = record
        if row is not None and row.get("status") != "no_predictions":
            rows.append(row)
    if not rows:
        return []
    selected = rows[-1].get("signal_thresholds")
    return [row for row in rows if row.get("signal_thresholds") == selected]


def evaluation_records(evaluations_dir=None, predictions_dir=None, as_of=None):
    directory = evaluations_dir if evaluations_dir is not None else EVALUATIONS_DIR
    predictions = predictions_dir if predictions_dir is not None else PREDICTIONS_DIR
    records = {}
    # A formal forecast always takes precedence, even when its evaluation is missing.
    for path in sorted((directory / "observations").glob("*.json")):
        if not (predictions / path.name).exists():
            row = json.loads(path.read_text(encoding="utf-8"))
            if row.get("observation_eligible") is True:
                records[path.stem] = row
    for path in sorted(directory.glob("*.json")):
        records[path.stem] = json.loads(path.read_text(encoding="utf-8"))
    cutoff = str(as_of) if as_of is not None else None
    return [row for day, row in sorted(records.items())
            if (cutoff is None or day <= cutoff)
            and row.get("output_schema") == OUTPUT_SCHEMA
            and not row.get("conditions_overridden")
            and (not row.get("actual_data_version") or actual_inputs_match(row))
            and evaluation_matches_official(row, predictions)]


def _load_recent_evaluations(days: int, target: str = "high_price", as_of=None) -> list[dict]:
    records = evaluation_records(as_of=as_of)
    return target_evaluations(records, target)[-days:]


def signal_success_rate(evaluations: list[dict]) -> tuple[int, float | None]:
    signals = [signal for item in evaluations for signal in item.get("signals", [])]
    if not signals:
        return 0, None
    success = sum(1 for signal in signals if signal["success"])
    return len(signals), success / len(signals)


def pooled_recall_precision(evaluations: list[dict], field: str) -> dict:
    """Sum tp/fp/fn across evaluation days for `field` (e.g. 'high_price'),
    then derive recall/precision from the pooled counts (not an average of
    per-day rates, which would overweight low-volume days)."""
    tp = sum(item.get("signal_recall_precision", {}).get(field, {}).get("tp", 0) for item in evaluations)
    fp = sum(item.get("signal_recall_precision", {}).get(field, {}).get("fp", 0) for item in evaluations)
    fn = sum(item.get("signal_recall_precision", {}).get(field, {}).get("fn", 0) for item in evaluations)
    return {
        "tp": tp, "fp": fp, "fn": fn,
        "recall": tp / (tp + fn) if (tp + fn) else None,
        "precision": tp / (tp + fp) if (tp + fp) else None,
    }


def _compute_gate_status(settings: Settings, target: str = "high_price", as_of=None) -> dict:
    long_window = _load_recent_evaluations(settings.gate_window_days, target, as_of)
    short_window = long_window[-settings.gate_short_window_days:]
    long_count, long_rate = signal_success_rate(long_window)
    short_count, short_rate = signal_success_rate(short_window)
    checked_at = datetime.now().isoformat(timespec="seconds")

    if long_count < settings.gate_min_signals or short_count < settings.gate_min_signals:
        return {
            "verdict": "INSUFFICIENT_DATA",
            "checked_at": checked_at,
            "long_window": {"days": len(long_window), "signals": long_count, "success_rate": long_rate},
            "short_window": {"days": len(short_window), "signals": short_count, "success_rate": short_rate},
            "reason": "訊號樣本數不足，暫不判定此目標是否退化",
        }

    drop = long_rate - short_rate
    if drop >= settings.gate_max_success_rate_drop or math.isclose(
            drop, settings.gate_max_success_rate_drop, rel_tol=0, abs_tol=1e-12):
        verdict = "DEGRADED"
        reason = (
            f"最近 {len(short_window)} 個交易日訊號成功率 {short_rate:.2%}，"
            f"較前 {len(long_window)} 日的 {long_rate:.2%} 下降 {drop:.2%}，"
            f"超過容許值 {settings.gate_max_success_rate_drop:.2%}"
        )
    else:
        verdict = "OK"
        reason = "近期訊號成功率在容許範圍內"

    return {
        "verdict": verdict,
        "checked_at": checked_at,
        "long_window": {"days": len(long_window), "signals": long_count, "success_rate": long_rate},
        "short_window": {"days": len(short_window), "signals": short_count, "success_rate": short_rate},
        "reason": reason,
    }


def compute_gate_status(settings: Settings, as_of=None) -> dict:
    if not (0 < settings.gate_short_window_days <= settings.gate_window_days
            and settings.gate_min_signals > 0 and 0 < settings.gate_max_success_rate_drop <= 1):
        raise ValueError("gate 視窗須為正數且短期不大於長期，min_signals > 0，退化門檻須在 (0, 1]")
    targets = {}
    prediction_versions = {}
    previous = json.loads(GATE_STATUS_PATH.read_text(encoding="utf-8")) if GATE_STATUS_PATH.exists() else {}
    if (previous.get("output_schema") != OUTPUT_SCHEMA
            or (as_of is not None and previous.get("as_of", "") > str(as_of))):
        previous = {}
    for target in ("high_price", "low_price"):
        recent = _load_recent_evaluations(settings.gate_window_days, target, as_of)
        prediction_versions.update({row["prediction_date"]: {
                                        "sha256": row["prediction_content_sha256"],
                                        "mode": row.get("mode", "official")}
                                    for row in recent if row.get("prediction_content_sha256")})
        targets[target] = {**_compute_gate_status(settings, target, as_of),
                           "signal_thresholds": recent[-1]["signal_thresholds"] if recent else None}
        old = previous.get("targets", {}).get(target, {})
        if (old.get("verdict") == "DEGRADED" and targets[target]["verdict"] == "INSUFFICIENT_DATA"
                and old.get("signal_thresholds") == targets[target]["signal_thresholds"]):
            targets[target].update(verdict="DEGRADED", reason="曾判定退化，恢復樣本不足，維持阻擋並繼續觀察")
    verdicts = [row["verdict"] for row in targets.values()]
    verdict = ("DEGRADED" if "DEGRADED" in verdicts else
               "OK" if all(value == "OK" for value in verdicts) else "INSUFFICIENT_DATA")
    return {"verdict": verdict, "checked_at": datetime.now().isoformat(timespec="seconds"),
            "as_of": str(as_of) if as_of is not None else max(prediction_versions, default=""),
            "reason": "; ".join(f"{target}: {row['verdict']} — {row['reason']}" for target, row in targets.items()),
            "targets": targets, "output_schema": OUTPUT_SCHEMA,
            "prediction_versions": prediction_versions,
            "signal_thresholds": targets["high_price"]["signal_thresholds"],
            "metric": "selected_class_precision"}


def save_gate_status(status: dict) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    atomic_text(GATE_STATUS_PATH, json.dumps(status, ensure_ascii=False, indent=2) + "\n")


def load_gate_status() -> dict | None:
    if not GATE_STATUS_PATH.exists():
        return None
    status = json.loads(GATE_STATUS_PATH.read_text(encoding="utf-8"))
    for day, digest in status.get("prediction_versions", {}).items():
        mode = digest.get("mode", "official") if isinstance(digest, dict) else "official"
        digest = digest["sha256"] if isinstance(digest, dict) else digest
        if not evaluation_matches_official({"prediction_date": day, "prediction_content_sha256": digest, "mode": mode}):
            raise RuntimeError("正式預測版本已變更，gate 仍引用舊版本；請重新執行該日 validate_predictions")
    return status if status.get("output_schema") == OUTPUT_SCHEMA else None


def due_predictions(as_of):
    """Registered forward forecasts only; never scan immutable rerun archives."""
    cutoff = str(as_of)
    paths = {}
    for directory in (PREDICTIONS_DIR / "observations", PREDICTIONS_DIR):
        for path in directory.glob("*.json"):
            try:
                day = date.fromisoformat(path.stem).isoformat()
            except ValueError:
                continue
            if day <= cutoff:
                paths[day] = path
    return [path for _, path in sorted(paths.items())]


def validation_queue(settings, as_of):
    due = due_predictions(as_of)
    recent = set(due[-settings.gate_window_days:])
    selected = []
    for prediction in due:
        directory = EVALUATIONS_DIR / "observations" if prediction.parent.name == "observations" else EVALUATIONS_DIR
        evaluation = directory / prediction.name
        if prediction in recent or not evaluation.exists():
            selected.append(prediction)
            continue
        row = json.loads(evaluation.read_text(encoding="utf-8"))
        if (row.get("output_schema") != OUTPUT_SCHEMA or row.get("conditions_overridden")
                or row.get("prediction_content_sha256") != file_fingerprint(prediction)
                or not actual_inputs_match(row, settings)
                or any(target.get("pending_symbols") for target in row.get("targets", {"high_price": row}).values())):
            selected.append(prediction)
    return selected


def refresh_gate_for_prediction(settings, as_of, *, require_fresh=True):
    """Rebuild the gate, rejecting missing/stale/partial validation, not low sample counts."""
    status = compute_gate_status(settings, as_of)
    due = due_predictions(as_of)[-settings.gate_window_days:]
    issues = []
    if due and due[-1].stem != str(as_of):
        issues.append(f"尚無 {as_of} 的正式或觀察預測驗證，最新預測為 {due[-1].stem}")
    for prediction in due:
        observation = prediction.parent.name == "observations"
        directory = EVALUATIONS_DIR / "observations" if observation else EVALUATIONS_DIR
        path = directory / prediction.name
        if not path.exists():
            issues.append(f"{prediction.stem}: 尚未驗證")
            continue
        row = json.loads(path.read_text(encoding="utf-8"))
        if (row.get("output_schema") != OUTPUT_SCHEMA or row.get("conditions_overridden")
                or row.get("prediction_content_sha256") != file_fingerprint(prediction)
                or (observation and row.get("observation_eligible") is not True)):
            issues.append(f"{prediction.stem}: 驗證版本、條件或觀察時間不合格")
            continue
        if not actual_inputs_match(row, settings):
            issues.append(f"{prediction.stem}: 實際資料／驗證設定已變更或缺少版本，請重新驗證")
            continue
        for target, values in row.get("targets", {"high_price": row}).items():
            if values.get("pending_symbols"):
                issues.append(f"{prediction.stem} {target}: 待驗證 {len(values['pending_symbols'])} 支")
    status["as_of"] = str(as_of)
    status["freshness"] = {"ready": not issues, "issues": issues, "checked_predictions": len(due)}
    if issues:
        status["performance_verdict"] = status["verdict"]
        status["verdict"] = "STALE"
        status["reason"] = "; ".join(issues)
    save_gate_status(status)
    if issues and require_fresh:
        raise RuntimeError("gate 驗證未更新或不完整，暫停正式預測；請 run_daily 補驗證，"
                           "或使用 --observe 累積後續觀察結果：" + status["reason"])
    return status
