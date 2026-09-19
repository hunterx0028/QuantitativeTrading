from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

from .config import Settings
from .paths import META_LABELS_DIR, ensure_runtime_dirs
from .provenance import atomic_text, file_fingerprint, fingerprint
from .signals import CLASSES


META_LABEL_SCHEMA = "stock_model_gpt_meta_labels_v1"
DATASET_PATH = META_LABELS_DIR / "dataset.jsonl"
STATUS_PATH = META_LABELS_DIR / "status.json"


def record_prediction(payload: dict, prediction_path: Path, settings: Settings) -> None:
    """Register the official prediction's candidate signals for later labeling.

    This is intentionally passive: it only collects data. A later meta-label
    model can use the dataset after enough validated trading days exist.
    """
    ensure_runtime_dirs()
    day = payload["prediction_date"]
    prediction_hash = file_fingerprint(prediction_path)
    samples = _samples_from_prediction(payload)
    record = {
        "schema": META_LABEL_SCHEMA,
        "prediction_date": day,
        "created_at": datetime.now().astimezone().isoformat(),
        "status": "pending",
        "meta_label_min_days": settings.meta_label_min_days,
        "prediction_id": payload.get("prediction_id"),
        "prediction_content_sha256": prediction_hash,
        "prediction_source": str(prediction_path.resolve()),
        "checkpoint": _checkpoint_name(payload),
        "checkpoint_sha256": payload.get("checkpoint_sha256"),
        "input_data_version": payload.get("input_data_version"),
        "universe_date": payload.get("universe_date"),
        "night_futures_date": payload.get("night_futures_date"),
        "samples": samples,
        "sample_count": len(samples),
    }
    _write_daily_record(record)
    rebuild_dataset(settings)
    print(f"meta-labeling 樣本已登錄: {META_LABELS_DIR / (day + '.json')} ({len(samples)}筆，待驗證)")


def update_from_evaluation(summary: dict, settings: Settings) -> None:
    ensure_runtime_dirs()
    day = summary["prediction_date"]
    path = META_LABELS_DIR / f"{day}.json"
    if not path.exists():
        prediction_source = summary.get("prediction_source")
        if prediction_source:
            prediction_path = Path(prediction_source)
            if prediction_path.exists():
                payload = json.loads(prediction_path.read_text(encoding="utf-8"))
                record_prediction(payload, prediction_path, settings)
        if not path.exists():
            print(f"meta-labeling: 找不到 {day} 的預測樣本，略過標記")
            rebuild_dataset(settings)
            return

    record = json.loads(path.read_text(encoding="utf-8"))
    if record.get("prediction_content_sha256") != summary.get("prediction_content_sha256"):
        print(f"meta-labeling: {day} 預測版本與驗證版本不同，略過標記")
        rebuild_dataset(settings)
        return

    evaluated = _evaluated_signal_map(summary)
    pending = _pending_signal_keys(summary)
    excluded = _excluded_symbol_keys(summary)
    for sample in record.get("samples", []):
        key = _sample_key(sample["target"], sample["symbol"])
        if key in evaluated:
            outcome = evaluated[key]
            sample.update({
                "label": 1 if outcome["success"] else 0,
                "success": bool(outcome["success"]),
                "actual_class": outcome.get("actual_class"),
                "label_status": "labeled",
                "validated_at": datetime.now().astimezone().isoformat(),
            })
        elif key in pending:
            sample.update({"label": None, "success": None, "label_status": "pending_actual"})
        elif key in excluded:
            sample.update({"label": None, "success": None, "label_status": "excluded"})
        else:
            sample.update({"label": None, "success": None, "label_status": "unknown"})

    open_samples = [
        sample for sample in record.get("samples", [])
        if sample.get("label_status") in {"pending", "pending_actual", "unknown"}
    ]
    record.update({
        "status": "validated" if not open_samples else "partial",
        "validated_at": datetime.now().astimezone().isoformat(),
        "evaluation_content_sha256": fingerprint(summary),
        "labeled_count": sum(1 for sample in record.get("samples", []) if sample.get("label_status") == "labeled"),
        "pending_count": len(open_samples),
    })
    _write_daily_record(record)
    status = rebuild_dataset(settings)
    readiness = "ready" if status["ready"] else "collecting"
    print(
        f"meta-labeling: {day} 已更新標記，"
        f"{status['validated_days']}/{settings.meta_label_min_days} 個交易日，"
        f"{status['labeled_samples']} 筆已標記樣本，狀態={readiness}"
    )


def rebuild_dataset(settings: Settings) -> dict:
    ensure_runtime_dirs()
    dataset_rows = []
    validated_days = 0
    for path in _daily_record_paths():
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("schema") != META_LABEL_SCHEMA:
            continue
        if record.get("status") == "validated":
            validated_days += 1
        for sample in record.get("samples", []):
            if sample.get("label_status") == "labeled":
                dataset_rows.append({
                    "schema": META_LABEL_SCHEMA,
                    "prediction_date": record["prediction_date"],
                    "prediction_id": record.get("prediction_id"),
                    "prediction_content_sha256": record.get("prediction_content_sha256"),
                    "checkpoint": record.get("checkpoint"),
                    "checkpoint_sha256": record.get("checkpoint_sha256"),
                    **sample,
                })
    text = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
                   for row in dataset_rows)
    atomic_text(DATASET_PATH, text)
    status = {
        "schema": META_LABEL_SCHEMA,
        "updated_at": datetime.now().astimezone().isoformat(),
        "min_days": settings.meta_label_min_days,
        "validated_days": validated_days,
        "labeled_samples": len(dataset_rows),
        "ready": validated_days >= settings.meta_label_min_days,
        "dataset": str(DATASET_PATH.resolve()),
    }
    atomic_text(STATUS_PATH, json.dumps(status, ensure_ascii=False, indent=2) + "\n")
    return status


def _samples_from_prediction(payload: dict) -> list[dict]:
    by_symbol = {row["symbol"]: row for row in payload.get("predictions", [])}
    samples = []
    for target, signal_key, threshold_key in (
        ("high_price", "high_signals", "high_signal_thresholds"),
        ("low_price", "low_signals", "low_signal_thresholds"),
    ):
        thresholds = payload.get(threshold_key, {})
        for signal in payload.get(signal_key, []):
            prediction = by_symbol.get(signal["symbol"], {})
            values = prediction.get(target, signal.get(target, {}))
            if not values:
                continue
            selected_probability = signal.get("target_probability")
            if selected_probability is None:
                selected_probability = math.fsum(values[str(c)] for c in thresholds.get("classes", []))
            samples.append({
                "sample_id": _sample_id(target, signal["symbol"]),
                "symbol": signal["symbol"],
                "target": target,
                "target_key": signal.get("target_key"),
                "selected_classes": signal.get("selected_classes", thresholds.get("classes", [])),
                "threshold_pct": thresholds.get("threshold_pct"),
                "selected_probability": selected_probability,
                "margin_to_threshold": (
                    selected_probability - thresholds["threshold_pct"] / 100
                    if "threshold_pct" in thresholds else None
                ),
                "predicted_class": signal.get("predicted_class"),
                "probabilities": values,
                "max_probability": max(values.values()),
                "entropy": _entropy(values),
                "label": None,
                "success": None,
                "label_status": "pending",
            })
    return sorted(samples, key=lambda row: (row["target"], -row["selected_probability"], row["symbol"]))


def _evaluated_signal_map(summary: dict) -> dict[tuple[str, str], dict]:
    rows = {}
    for target, target_summary in summary.get("targets", {}).items():
        actual_key = f"actual_{target}"
        for signal in target_summary.get("signals", []):
            rows[_sample_key(target, signal["symbol"])] = {
                "success": signal.get("success"),
                "actual_class": signal.get(actual_key),
            }
    return rows


def _pending_signal_keys(summary: dict) -> set[tuple[str, str]]:
    keys = set()
    for target, target_summary in summary.get("targets", {}).items():
        keys.update(_sample_key(target, signal["symbol"]) for signal in target_summary.get("pending_signals", []))
    return keys


def _excluded_symbol_keys(summary: dict) -> set[tuple[str, str]]:
    keys = set()
    for target, target_summary in summary.get("targets", {}).items():
        keys.update(_sample_key(target, item["symbol"]) for item in target_summary.get("excluded_suspensions", []))
    return keys


def _sample_key(target: str, symbol: str) -> tuple[str, str]:
    return target, symbol


def _sample_id(target: str, symbol: str) -> str:
    return f"{target}:{symbol}"


def _entropy(values: dict[str, float]) -> float:
    return -math.fsum(value * math.log(max(value, 1e-12)) for value in values.values())


def _checkpoint_name(payload: dict) -> str | None:
    checkpoints = {row.get("checkpoint") for row in payload.get("predictions", []) if row.get("checkpoint")}
    return next(iter(checkpoints)) if len(checkpoints) == 1 else None


def _write_daily_record(record: dict) -> None:
    text = json.dumps(record, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    day = record["prediction_date"]
    atomic_text(META_LABELS_DIR / f"{day}.json", text)
    if record.get("prediction_id"):
        atomic_text(META_LABELS_DIR / "versions" / day / f"{record['prediction_id']}.json", text)


def _daily_record_paths() -> list[Path]:
    paths = []
    for path in META_LABELS_DIR.glob("*.json"):
        try:
            datetime.strptime(path.stem, "%Y-%m-%d")
        except ValueError:
            continue
        paths.append(path)
    return sorted(paths)
