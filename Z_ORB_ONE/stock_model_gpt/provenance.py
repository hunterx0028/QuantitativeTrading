"""Stable data fingerprints and atomic runtime records."""
import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4


def fingerprint(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def file_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def sample_key(dataset, ref) -> str:
    return ref.feature_path.stem + "|" + dataset.rows_by_path[ref.feature_path][ref.end]["date"]


def sample_versions(dataset) -> dict[str, str]:
    row_hashes = {path: [fingerprint(row) for row in rows]
                  for path, rows in dataset.rows_by_path.items()}
    versions = {}
    for ref in dataset.refs:
        rows = dataset.rows_by_path[ref.feature_path]
        start = ref.end - dataset.context_days
        versions[sample_key(dataset, ref)] = fingerprint({
            "rows": row_hashes[ref.feature_path][start:ref.end],
            "target": {field: rows[ref.end][field] for field in ("date", "high_price", "low_price")},
            "nights": [(row["date"], dataset.night_by_date[row["date"]])
                       for row in rows[start + 1:ref.end + 1]],
            "atr_boundaries_pct": dataset.atr_boundaries_pct,
        })
    return versions
