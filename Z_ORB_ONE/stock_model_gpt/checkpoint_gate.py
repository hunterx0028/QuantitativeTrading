"""Minimal champion/challenger safety gate.

There is no offline backtest in this pipeline, so a freshly trained checkpoint
cannot be evaluated before it is used. Instead this acts as a circuit breaker:
after each day's `validate_predictions` run, compare the recent short-window
signal success rate against the longer baseline window (same two windows
`post_training_gate.py` already reports). If it has dropped sharply, flag the
gate as DEGRADED so `predict.py` refuses to silently keep auto-selecting the
newest checkpoint for live signals, until a human looks at it.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from .config import Settings
from .paths import CHECKPOINT_DIR, EVALUATIONS_DIR


GATE_STATUS_PATH = CHECKPOINT_DIR / "gate_status.json"


def _load_recent_evaluations(days: int) -> list[dict]:
    paths = sorted(EVALUATIONS_DIR.glob("*.json"))[-days:]
    return [json.loads(path.read_text(encoding="utf-8")) for path in paths]


def signal_success_rate(evaluations: list[dict]) -> tuple[int, float | None]:
    signals = [signal for item in evaluations for signal in item.get("signals", [])]
    if not signals:
        return 0, None
    success = sum(1 for signal in signals if signal["success"])
    return len(signals), success / len(signals)


def pooled_recall_precision(evaluations: list[dict], field: str) -> dict:
    """Sum tp/fp/fn across evaluation days for `field` ('hit_up' or 'hit_down'),
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


def compute_gate_status(settings: Settings) -> dict:
    long_window = _load_recent_evaluations(settings.gate_window_days)
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
            "reason": "訊號樣本數不足，暫不判定，predict.py 會照常自動選最新 checkpoint",
        }

    drop = long_rate - short_rate
    if drop >= settings.gate_max_success_rate_drop:
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


def save_gate_status(status: dict) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    temporary = GATE_STATUS_PATH.with_suffix(".tmp")
    temporary.write_text(json.dumps(status, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(GATE_STATUS_PATH)


def load_gate_status() -> dict | None:
    if not GATE_STATUS_PATH.exists():
        return None
    return json.loads(GATE_STATUS_PATH.read_text(encoding="utf-8"))
