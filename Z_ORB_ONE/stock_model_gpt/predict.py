from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

import torch

from .config import Settings
from .dataset import encode_state
from .paths import CHECKPOINT_DIR, FEATURES_DIR, PREDICTIONS_DIR, ensure_runtime_dirs
from .storage import read_jsonl
from .training import build_model
from .universe import load_universe_snapshot
from .paths import UNIVERSE_DIR


PRICE_LABELS = [-2, -1, 0, 1, 2]
CLOSE_LABELS = ["D", "N", "U"]


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
    args = parser.parse_args()
    universe_date = date.fromisoformat(args.universe_date)
    prediction_date = date.fromisoformat(args.prediction_date)
    if prediction_date <= universe_date:
        raise ValueError("prediction-date 必須晚於 universe-date")
    ensure_runtime_dirs()
    checkpoint_path = Path(args.checkpoint) if args.checkpoint else latest_checkpoint()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    checkpoint = torch.load(checkpoint_path, map_location=device, weights_only=False)
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
            predictions.append(
                {
                    "symbol": path.stem,
                    "prediction_date": prediction_date.isoformat(),
                    "input_last_date": rows[-1]["date"],
                    "checkpoint": checkpoint_path.name,
                    "price": dict(zip(map(str, PRICE_LABELS), probabilities["price"])),
                    "hit_up": _probabilities(probabilities["hit_up"]),
                    "hit_down": _probabilities(probabilities["hit_down"]),
                    "close_limit": dict(zip(CLOSE_LABELS, probabilities["close_limit"])),
                }
            )
    output = PREDICTIONS_DIR / f"{prediction_date.isoformat()}.json"
    payload = {"created_at": datetime.now().isoformat(timespec="seconds"), "predictions": predictions}
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"預測已儲存: {output} ({len(predictions)}支)")


def _probabilities(values: list[float]) -> dict[str, float]:
    return {"F": values[0], "T": values[1]}


if __name__ == "__main__":
    main()
