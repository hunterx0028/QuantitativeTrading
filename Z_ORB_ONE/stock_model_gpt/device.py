from __future__ import annotations

import torch


def select_device() -> torch.device:
    if torch.cuda.is_available():
        if hasattr(torch, "set_float32_matmul_precision"):
            torch.set_float32_matmul_precision("high")
        return torch.device("cuda")
    return torch.device("cpu")


def describe_device(device: torch.device) -> str:
    if device.type == "cuda":
        index = device.index if device.index is not None else torch.cuda.current_device()
        name = torch.cuda.get_device_name(index)
        capability = torch.cuda.get_device_capability(index)
        memory_gb = torch.cuda.get_device_properties(index).total_memory / 1024**3
        return (
            f"cuda:{index} {name} "
            f"(compute {capability[0]}.{capability[1]}, "
            f"{memory_gb:.1f} GiB, torch CUDA {torch.version.cuda})"
        )
    if torch.version.cuda is None:
        return "cpu (目前 PyTorch 是 CPU build，需安裝 CUDA build 才會使用 NVIDIA GPU)"
    return "cpu (torch.cuda.is_available() is False)"


def move_targets_to_device(
    targets: dict[str, torch.Tensor],
    device: torch.device,
    non_blocking: bool = False,
) -> dict[str, torch.Tensor]:
    return {
        key: value.to(device, non_blocking=non_blocking)
        for key, value in targets.items()
    }
