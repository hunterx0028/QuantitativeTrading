"""Atomic, load-verified checkpoints and explicit current-model manifest."""
import json
import os
import argparse
import math
from pathlib import Path
from uuid import uuid4

import torch

from .provenance import atomic_text, file_fingerprint


def require_finite(value, name):
    if isinstance(value, torch.Tensor):
        valid = bool(torch.isfinite(value).all())
    elif isinstance(value, float):
        valid = math.isfinite(value)
    elif isinstance(value, dict):
        for key, item in value.items():
            require_finite(item, f"{name}.{key}")
        return
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            require_finite(item, f"{name}[{index}]")
        return
    else:
        return
    if not valid:
        raise ValueError(f"{name} 含非有限數值，停止訓練／發布")


def verify_training_result(checkpoint, *, required=False):
    for field in ("loss", "loss_components", "optimizer", "scaler", "validation"):
        require_finite(checkpoint.get(field), field)
    progress = checkpoint.get("training_progress")
    if progress is None and not required:
        return  # Existing trained checkpoints predate progress metadata.
    if not isinstance(progress, dict) or any(
            type(progress.get(key)) is not int or progress[key] <= 0
            for key in ("epochs_run", "optimizer_steps")):
        raise ValueError("模型缺少有效訓練輪數／權重更新紀錄，拒絕發布")
    losses = checkpoint.get("loss_components")
    if (not isinstance(checkpoint.get("loss"), (int, float)) or not isinstance(losses, dict)
            or any(not isinstance(losses.get(key), (int, float)) for key in ("high_price", "low_price"))):
        raise ValueError("模型缺少有效 high／low 訓練 loss，拒絕發布")


def verify_checkpoint(path):
    from .config import Settings
    from .training import build_model, ensure_checkpoint_compatible
    checkpoint = torch.load(path, map_location="cpu", weights_only=False)
    ensure_checkpoint_compatible(checkpoint)
    verify_training_result(checkpoint)
    if any(not torch.isfinite(tensor).all() for tensor in checkpoint["model"].values()):
        raise ValueError("checkpoint 模型含非有限數值，拒絕發布")
    build_model(Settings(**checkpoint["settings"])).load_state_dict(checkpoint["model"])
    return checkpoint


def publish_checkpoint(path, payload):
    verify_training_result(payload, required=True)
    if path.exists():
        raise FileExistsError(f"checkpoint 不可覆寫: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        with temporary.open("wb") as stream:
            torch.save(payload, stream)
            stream.flush()
            os.fsync(stream.fileno())
        verify_checkpoint(temporary)
        temporary.replace(path)
        atomic_text(path.parent / "current_model.json", json.dumps({
            "checkpoint": path.name, "sha256": file_fingerprint(path),
            "training_as_of": payload["training_as_of"],
        }, indent=2) + "\n")
    finally:
        temporary.unlink(missing_ok=True)


def current_checkpoint(directory):
    manifest = directory / "current_model.json"
    if not manifest.exists():
        raise RuntimeError("缺少 current_model.json；請先 train_initial，或明確指定 --checkpoint 使用已確認模型")
    record = json.loads(manifest.read_text(encoding="utf-8"))
    path = (directory / record["checkpoint"]).resolve()
    if path.parent != directory.resolve() or not path.is_file() or file_fingerprint(path) != record["sha256"]:
        raise RuntimeError("目前模型不存在或指紋不符，拒絕自動選用")
    verify_checkpoint(path)
    return path


def activate_checkpoint(path, directory):
    path = Path(path).resolve()
    if path.parent != directory.resolve():
        raise ValueError("指定模型必須位於目前 checkpoints 目錄")
    checkpoint = verify_checkpoint(path)
    atomic_text(directory / "current_model.json", json.dumps({
        "checkpoint": path.name, "sha256": file_fingerprint(path),
        "training_as_of": checkpoint["training_as_of"],
    }, indent=2) + "\n")


from .runtime_lock import locked


@locked
def main():
    from .paths import CHECKPOINT_DIR
    parser = argparse.ArgumentParser(description="驗證並明確指定目前模型，不依檔名自動猜測")
    parser.add_argument("--checkpoint", required=True, help="位於 checkpoints 內的模型路徑")
    args = parser.parse_args()
    activate_checkpoint(Path(args.checkpoint), CHECKPOINT_DIR)
    print(f"目前模型已指定: {args.checkpoint}")


if __name__ == "__main__":
    main()
