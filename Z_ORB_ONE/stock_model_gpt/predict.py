from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import torch

from .checkpoint_gate import load_gate_status
from .config import Settings
from .dataset import INPUT_ALIGNMENT, encode_sequence
from .device import describe_device, select_device
from .night_futures import load_night_futures
from .paths import CHECKPOINT_DIR, FEATURES_DIR, PREDICTIONS_DIR, SIGNAL_REPORTS_DIR, ensure_runtime_dirs
from .storage import read_jsonl
from .training import build_model, ensure_checkpoint_compatible
from .universe import load_universe_snapshot
from .paths import UNIVERSE_DIR
from .provenance import fingerprint, file_fingerprint, atomic_text
from .checkpoints import current_checkpoint
from .trading_calendar import assert_sequence_dates


def save_prediction_version(payload, inputs, report_text, replace_official=False):
    observation = payload.get("mode") == "observation"
    if observation and replace_official:
        raise ValueError("觀察模式不可替換正式預測")
    day = payload["prediction_date"]
    run_id = payload["prediction_id"]
    directory = PREDICTIONS_DIR / "versions" / day
    version = directory / f"{run_id}.json"
    if version.exists():
        raise FileExistsError(f"預測版本已存在，不可覆寫: {version}")
    text = dumps_json_no_scientific(payload) + "\n"
    atomic_text(directory / f"{run_id}.inputs.json", dumps_json_no_scientific(inputs) + "\n")
    atomic_text(directory / f"{run_id}.txt", report_text)
    atomic_text(version, text)
    official = (PREDICTIONS_DIR / "observations" if observation else PREDICTIONS_DIR) / f"{day}.json"
    if not official.exists() or replace_official:
        if official.exists():
            old_text = official.read_text(encoding="utf-8")
            # Also archive legacy daily files which predate versioned predictions.
            old_id = file_fingerprint(official)
            atomic_text(directory / f"previous_{old_id}.json", old_text)
        atomic_text(official, text)
        if not observation:
            atomic_text(SIGNAL_REPORTS_DIR / f"{day}.txt", report_text)
        print(f"{'觀察' if observation else '正式'}預測: {official}，prediction_id={run_id}")
    else:
        print(f"已保留既有{'觀察' if observation else '正式'}預測；本次僅另存版本 {run_id}")
        if not observation:
            official_id = json.loads(official.read_text(encoding="utf-8")).get("prediction_id")
            saved_report = directory / f"{official_id}.txt"
            if saved_report.exists():
                atomic_text(SIGNAL_REPORTS_DIR / f"{day}.txt", saved_report.read_text(encoding="utf-8"))
    return version


from .signals import (CLASSES, OUTPUT_SCHEMA, SignalThresholds,
                      build_signal_thresholds, detect_signal, build_signal_report_lines,
                      predicted_class, sorted_signals)


def add_prediction_signal_arguments(parser):
    for target, classes in (("high", "1,2"), ("low", "-2,-1")):
        parser.add_argument(f"--{target}-signal-classes", default=classes,
                            help=f"{target} 篩選刻度；負數用 --{target}-signal-classes=-2,-1")
        parser.add_argument(f"--{target}-signal-threshold-pct", type=float, default=60.0,
                            help=f"{target} 所選刻度的合計機率門檻（百分比）")


def prediction_signal_thresholds(args, target):
    return build_signal_thresholds(argparse.Namespace(
        signal_classes=getattr(args, f"{target}_signal_classes"),
        signal_threshold_pct=getattr(args, f"{target}_signal_threshold_pct"),
    ))


def latest_checkpoint() -> Path:
    return current_checkpoint(CHECKPOINT_DIR)


def select_checkpoint_for_prediction(thresholds: SignalThresholds = SignalThresholds(),
                                     low_thresholds: SignalThresholds = SignalThresholds((-2, -1), 60)) -> Path:
    """Auto-select path only; an explicit --checkpoint always bypasses this gate."""
    candidate = latest_checkpoint()
    status = load_gate_status()
    if status and status.get("verdict") == "STALE":
        raise RuntimeError(f"gate 驗證未更新或不完整：{status['reason']}")
    targets = (status or {}).get("targets", {"high_price": status} if status else {})
    degraded = [target for target, options in (("high_price", thresholds), ("low_price", low_thresholds))
                if targets.get(target) and targets[target].get("signal_thresholds") == options.signal_values()
                and targets[target]["verdict"] == "DEGRADED"]
    if degraded:
        raise RuntimeError(
            f"checkpoint gate 判定 {'/'.join(degraded)} 近期訊號表現明顯退化（{status['reason']}），"
            f"拒絕自動使用最新 checkpoint {candidate.name}；"
            "請先確認訓練或資料是否異常，或明確指定 --checkpoint 選用你確認過的模型"
        )
    return candidate


def select_prediction_inputs(
    active_symbols: set[str], universe_date: date, context_days: int,
) -> tuple[dict[str, list[dict]], list[dict]]:
    cutoff = universe_date.isoformat()
    eligible: dict[str, list[dict]] = {}
    skipped: list[dict] = []
    for symbol in sorted(active_symbols):
        path = FEATURES_DIR / f"{symbol}.jsonl"
        rows = sorted((row for row in read_jsonl(path) if row["date"] <= cutoff),
                      key=lambda row: row["date"])
        last_date = rows[-1]["date"] if rows else None
        reason = None
        if not path.exists():
            reason = "missing_features"
        elif not rows:
            reason = "no_features_as_of"
        elif last_date != cutoff:
            reason = "stale_features"
        elif len(rows) < context_days:
            reason = "insufficient_history"
        if reason:
            skipped.append({"symbol": symbol, "reason": reason,
                            "expected_date": cutoff, "input_last_date": last_date,
                            "history_days": len(rows)})
        else:
            try:
                assert_sequence_dates([row["date"] for row in rows[-context_days:]])
            except ValueError:
                skipped.append({"symbol": symbol, "reason": "calendar_gap", "expected_date": cutoff,
                                "input_last_date": last_date, "history_days": len(rows)})
                continue
            eligible[symbol] = rows[-context_days:]
    return eligible, skipped


def run_prediction(
    checkpoint_path: Path,
    universe_date: date,
    prediction_date: date,
    thresholds: SignalThresholds,
    low_thresholds: SignalThresholds = SignalThresholds((-2, -1), 60),
    replace_official: bool = False,
    observe: bool = False,
) -> tuple[Path, dict | None, dict | None]:
    """Core prediction step, reusable both by the CLI (`main`) and by in-process
    callers such as a walk-forward backtest that would otherwise pay a fresh
    Python/torch interpreter startup cost for every simulated trading day."""
    if prediction_date <= universe_date:
        raise ValueError("prediction-date 必須晚於 universe-date")
    if observe and datetime.now(timezone.utc) >= datetime.combine(
            prediction_date, time(9), timezone(timedelta(hours=8))):
        raise ValueError("觀察預測必須在被預測日台北時間 09:00 開盤前產生，不能事後補做恢復證據")
    assert_sequence_dates([universe_date.isoformat(), prediction_date.isoformat()])
    ensure_runtime_dirs()
    device = select_device()
    print(f"device={describe_device(device)}")
    checkpoint = torch.load(checkpoint_path, map_location=device, weights_only=False)
    ensure_checkpoint_compatible(checkpoint)
    training_as_of = checkpoint.get("training_as_of")
    if training_as_of and training_as_of > universe_date.isoformat():
        raise RuntimeError(
            f"checkpoint 訓練截止日 {training_as_of} 晚於預測基準日 "
            f"{universe_date.isoformat()}，已拒絕可能洩漏未來資料的預測"
        )
    settings = Settings(**checkpoint["settings"])
    model = build_model(settings).to(device)
    model.load_state_dict(checkpoint["model"])
    model.eval()
    # Latest night data fills column ten of the final stock row.
    prediction_date_str = prediction_date.isoformat()
    night_by_date = load_night_futures()
    prediction_night_bucket = night_by_date.get(prediction_date_str)
    if prediction_night_bucket is None:
        raise RuntimeError(
            f"找不到 {prediction_date_str} 開盤前的夜盤資料；"
            f"請先用 set_night_futures.py（或 import_night_futures.py）匯入 {prediction_date_str} 這筆，"
            "再重新預測——這是被預測日當天的必要輸入，不能省略"
        )
    universe_path = UNIVERSE_DIR / f"{universe_date.isoformat()}.json"
    if not universe_path.exists():
        raise RuntimeError(f"找不到當日股票清單快照: {universe_path}")
    active_symbols = {stock.symbol for stock in load_universe_snapshot(universe_path)}
    eligible, skipped = select_prediction_inputs(active_symbols, universe_date, settings.context_days)
    coverage_lines = [f"預測資料覆蓋: {len(eligible)}/{len(active_symbols)} 支"]
    coverage_lines.extend(
        f"[SKIP] {item['symbol']} reason={item['reason']} "
        f"expected_date={item['expected_date']} input_last_date={item['input_last_date']} "
        f"history_days={item['history_days']}" for item in skipped
    )
    for line in coverage_lines:
        print(line)
    if not eligible:
        raise RuntimeError("沒有日期與歷史長度合格的股票，未產生預測")

    predictions: list[dict] = []
    signals: list[dict] = []
    low_signals: list[dict] = []
    input_snapshot = {}
    with torch.no_grad():
        for symbol, rows in eligible.items():
            states = torch.tensor(
                [encode_sequence(rows, prediction_date_str, night_by_date, settings.atr_boundaries_pct)],
                dtype=torch.long,
                device=device,
            )
            input_snapshot[symbol] = {"feature_rows": rows, "encoded_states": states[0].cpu().tolist()}
            outputs = model(states)
            probabilities = {key: torch.softmax(value, dim=-1)[0].cpu().tolist() for key, value in outputs.items()}
            prediction = {
                "symbol": symbol,
                "prediction_date": prediction_date.isoformat(),
                "input_last_date": rows[-1]["date"],
                "checkpoint": checkpoint_path.name,
                "high_price": _probabilities(probabilities["high_price"]),
                "low_price": _probabilities(probabilities["low_price"]),
                "predicted_class": CLASSES[max(range(5), key=lambda i: probabilities["high_price"][i])],
            }
            predictions.append(prediction)
            signal = detect_signal(prediction, thresholds)
            low_signal = detect_signal(prediction, low_thresholds, "low_price")
            prediction["signal_matches"] = {"high_price": signal is not None, "low_price": low_signal is not None}
            if signal:
                signals.append(signal)
            if low_signal:
                low_signals.append(low_signal)
    signals = sorted_signals(signals)
    low_signals = sorted_signals(low_signals)
    naive_baseline = checkpoint.get("naive_baseline")
    # The checkpoint's own last-epoch, in-sample (training-window) unweighted
    # loss per target — surfaced alongside the prediction so a caller such as a
    # walk-forward backtest can compare it against tomorrow's actual
    # out-of-sample loss to check for overfitting (low in-sample, high
    # out-of-sample is the classic symptom).
    in_sample_loss = checkpoint.get("loss_components")
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S_%f") + "_" + uuid4().hex[:12]
    payload = {"created_at": datetime.now().astimezone().isoformat(), "predictions": predictions,
               "prediction_id": run_id, "prediction_date": prediction_date.isoformat(),
               "mode": "observation" if observe else "official",
               "checkpoint_sha256": file_fingerprint(checkpoint_path),
               "input_data_version": fingerprint(input_snapshot),
               "universe_date": universe_date.isoformat(), "active_count": len(active_symbols),
               "predicted_count": len(predictions), "skipped": skipped,
               "signal_thresholds": thresholds.signal_values(),
               "high_signal_thresholds": thresholds.signal_values(),
               "low_signal_thresholds": low_thresholds.signal_values(),
               "atr_boundaries_pct": settings.atr_boundaries_pct,
               "night_futures_date": prediction_date_str,
               "night_futures_bucket": prediction_night_bucket,
               "input_alignment": INPUT_ALIGNMENT,
               "output_schema": OUTPUT_SCHEMA,
               "naive_baseline": naive_baseline,
               "in_sample_loss": in_sample_loss,
               "signals": signals,
               "high_signals": signals,
               "low_signals": low_signals}
    report_lines = build_signal_report_lines(prediction_date, thresholds, signals)
    report_lines[1:1] = coverage_lines
    report_lines.insert(1 + len(coverage_lines), "[high 符合清單]")
    report_lines.extend(["", "[low 符合清單]",
                         *build_signal_report_lines(prediction_date, low_thresholds, low_signals, "low_price")[1:]])
    if observe:
        report_lines.insert(0, "[觀察模式] 僅供後續驗證，不發布正式訊號")
        print(report_lines[0])
    else:
        for line in report_lines:
            print(line)
    report_text = "\n".join(report_lines) + "\n"
    output = save_prediction_version(payload, input_snapshot, report_text, replace_official)
    print(f"預測版本已儲存: {output} ({len(predictions)}支)")
    return output, naive_baseline, in_sample_loss


from .runtime_lock import locked


@locked
def main() -> None:
    parser = argparse.ArgumentParser(description="預測下一交易日狀態機率")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--settings", default=None, help="gate 設定；模型設定仍取自 checkpoint")
    parser.add_argument("--prediction-date", default=date.today().isoformat())
    parser.add_argument("--universe-date", default=date.today().isoformat())
    parser.add_argument("--replace-official", action="store_true", help="明確將本次新版本指定為當日正式預測；舊版本仍保留")
    parser.add_argument("--observe", action="store_true", help="繞過 gate 產生觀察預測，供恢復驗證，不發布正式訊號")
    add_prediction_signal_arguments(parser)
    args = parser.parse_args()
    if args.observe and args.replace_official:
        parser.error("observe 不可搭配 replace-official")
    thresholds = prediction_signal_thresholds(args, "high")
    low_thresholds = prediction_signal_thresholds(args, "low")
    universe_date = date.fromisoformat(args.universe_date)
    prediction_date = date.fromisoformat(args.prediction_date)
    assert_sequence_dates([universe_date.isoformat(), prediction_date.isoformat()])
    ensure_runtime_dirs()
    if not args.observe and not args.checkpoint:
        from .checkpoint_gate import refresh_gate_for_prediction
        refresh_gate_for_prediction(Settings.load(args.settings) if args.settings else Settings.load(), universe_date)
    checkpoint_path = (Path(args.checkpoint) if args.checkpoint else latest_checkpoint() if args.observe
                       else select_checkpoint_for_prediction(thresholds, low_thresholds))
    run_prediction(checkpoint_path, universe_date, prediction_date, thresholds, low_thresholds,
                   replace_official=args.replace_official, observe=args.observe)


def dumps_json_no_scientific(value, indent: int = 2) -> str:
    return _format_json_value(value, indent, 0)


def _format_json_value(value, indent: int, level: int) -> str:
    space = " " * (indent * level)
    child_space = " " * (indent * (level + 1))
    if isinstance(value, dict):
        if not value:
            return "{}"
        items = [
            f"{child_space}{json.dumps(str(key), ensure_ascii=False)}: "
            f"{_format_json_value(item, indent, level + 1)}"
            for key, item in value.items()
        ]
        return "{\n" + ",\n".join(items) + "\n" + space + "}"
    if isinstance(value, list):
        if not value:
            return "[]"
        items = [f"{child_space}{_format_json_value(item, indent, level + 1)}" for item in value]
        return "[\n" + ",\n".join(items) + "\n" + space + "]"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"JSON 不支援非有限浮點數: {value}")
        return format(Decimal(str(value)), "f")
    return json.dumps(value, ensure_ascii=False)


def _probabilities(values: list[float]) -> dict[str, float]:
    return {str(c): values[i] for i, c in enumerate(CLASSES)}


if __name__ == "__main__":
    main()
