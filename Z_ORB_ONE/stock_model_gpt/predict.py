from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import torch

from .config import Settings
from .dataset import encode_state
from .device import describe_device, select_device
from .paths import CHECKPOINT_DIR, FEATURES_DIR, PREDICTIONS_DIR, ensure_runtime_dirs
from .storage import read_jsonl
from .training import build_model, ensure_checkpoint_compatible
from .universe import load_universe_snapshot
from .paths import UNIVERSE_DIR


PRICE_LABELS = [-2, -1, 0, 1, 2]


@dataclass(frozen=True)
class SignalThresholds:
    long_hit: float = 0.6
    long_price: float = 0.6
    short_hit: float = 0.6
    short_price: float = 0.6


def latest_checkpoint() -> Path:
    paths = sorted(list(CHECKPOINT_DIR.glob("stock_model_gpt_*.pt")))
    if not paths:
        raise RuntimeError("找不到 checkpoint")
    return paths[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description="預測下一交易日狀態機率")
    parser.add_argument("--checkpoint", default=None)
    parser.add_argument("--prediction-date", default=date.today().isoformat())
    parser.add_argument("--universe-date", default=date.today().isoformat())
    parser.add_argument("--signal-threshold", type=float, default=0.6)
    parser.add_argument("--long-hit-threshold", type=float, default=None)
    parser.add_argument("--long-price-threshold", type=float, default=None)
    parser.add_argument("--short-hit-threshold", type=float, default=None)
    parser.add_argument("--short-price-threshold", type=float, default=None)
    args = parser.parse_args()
    thresholds = build_signal_thresholds(args)
    universe_date = date.fromisoformat(args.universe_date)
    prediction_date = date.fromisoformat(args.prediction_date)
    if prediction_date <= universe_date:
        raise ValueError("prediction-date 必須晚於 universe-date")
    ensure_runtime_dirs()
    checkpoint_path = Path(args.checkpoint) if args.checkpoint else latest_checkpoint()
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
    universe_path = UNIVERSE_DIR / f"{args.universe_date}.json"
    if not universe_path.exists():
        raise RuntimeError(f"找不到當日股票清單快照: {universe_path}")
    active_symbols = {stock.symbol for stock in load_universe_snapshot(universe_path)}

    predictions: list[dict] = []
    signals: list[dict] = []
    with torch.no_grad():
        feature_paths = sorted(list(FEATURES_DIR.glob("*.jsonl")))
        for path in feature_paths:
            if path.stem not in active_symbols:
                continue
            rows = [
                row for row in read_jsonl(path)
                if row["date"] <= universe_date.isoformat()
            ]
            if len(rows) < settings.context_days:
                continue
            states = torch.tensor(
                [[encode_state(row) for row in rows[-settings.context_days:]]],
                dtype=torch.long,
                device=device,
            )
            outputs = model(states)
            probabilities = {key: torch.softmax(value, dim=-1)[0].cpu().tolist() for key, value in outputs.items()}
            prediction = {
                "symbol": path.stem,
                "prediction_date": prediction_date.isoformat(),
                "input_last_date": rows[-1]["date"],
                "checkpoint": checkpoint_path.name,
                "price": dict(zip(map(str, PRICE_LABELS), probabilities["price"])),
                "hit_up": _probabilities(probabilities["hit_up"]),
                "hit_down": _probabilities(probabilities["hit_down"]),
            }
            predictions.append(prediction)
            signal = detect_signal(prediction, thresholds)
            if signal:
                signals.append(signal)
    output = PREDICTIONS_DIR / f"{prediction_date.isoformat()}.json"
    payload = {"created_at": datetime.now().isoformat(timespec="seconds"), "predictions": predictions}
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"預測已儲存: {output} ({len(predictions)}支)")
    if signals:
        print(
            "符合訊號門檻: "
            f"long_hit>={thresholds.long_hit:.2f}, "
            f"long_price>={thresholds.long_price:.2f}, "
            f"short_hit>={thresholds.short_hit:.2f}, "
            f"short_price>={thresholds.short_price:.2f}"
        )
        for signal in signals:
            print(
                f"[{signal['side']}] {signal['symbol']} "
                f"reason={signal['reason']} "
                f"prediction_date={signal['prediction_date']} "
                f"{signal['hit_key']}={signal['hit_probability']:.4f} "
                f"{signal['price_key']}={signal['price_probability']:.4f}"
            )
    else:
        print("沒有符合訊號門檻的標的")


def build_signal_thresholds(args) -> SignalThresholds:
    threshold = args.signal_threshold
    values = SignalThresholds(
        long_hit=args.long_hit_threshold if args.long_hit_threshold is not None else threshold,
        long_price=args.long_price_threshold if args.long_price_threshold is not None else threshold,
        short_hit=args.short_hit_threshold if args.short_hit_threshold is not None else threshold,
        short_price=args.short_price_threshold if args.short_price_threshold is not None else threshold,
    )
    for name, value in values.__dict__.items():
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"{name} threshold 必須介於 0 到 1")
    return values


def detect_signal(prediction: dict, thresholds: SignalThresholds | float = SignalThresholds()) -> dict | None:
    if isinstance(thresholds, float):
        thresholds = SignalThresholds(thresholds, thresholds, thresholds, thresholds)
    long_hit = prediction["hit_up"]["T"]
    long_price = prediction["price"]["2"]
    short_hit = prediction["hit_down"]["T"]
    short_price = prediction["price"]["-2"]
    long_hit_pass = long_hit >= thresholds.long_hit
    long_price_pass = long_price >= thresholds.long_price
    short_hit_pass = short_hit >= thresholds.short_hit
    short_price_pass = short_price >= thresholds.short_price
    if long_hit_pass or long_price_pass:
        return {
            "side": "LONG",
            "reason": signal_reason(long_hit_pass, long_price_pass),
            "symbol": prediction["symbol"],
            "prediction_date": prediction["prediction_date"],
            "hit_key": "hit_up.T",
            "hit_probability": long_hit,
            "price_key": "price.2",
            "price_probability": long_price,
        }
    if short_hit_pass or short_price_pass:
        return {
            "side": "SHORT",
            "reason": signal_reason(short_hit_pass, short_price_pass),
            "symbol": prediction["symbol"],
            "prediction_date": prediction["prediction_date"],
            "hit_key": "hit_down.T",
            "hit_probability": short_hit,
            "price_key": "price.-2",
            "price_probability": short_price,
        }
    return None


def signal_reason(hit_pass: bool, price_pass: bool) -> str:
    if hit_pass and price_pass:
        return "both"
    if hit_pass:
        return "hit"
    return "price"


def _probabilities(values: list[float]) -> dict[str, float]:
    return {"F": values[0], "T": values[1]}


if __name__ == "__main__":
    main()
