from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_SETTINGS_PATH = PACKAGE_DIR / "settings.json"


@dataclass(frozen=True)
class Settings:
    warmup_days: int = 20
    context_days: int = 120
    earliest_date: str = "2010-01-01"
    request_chunk_calendar_days: int = 365
    request_interval_seconds: float = 1.0
    finmind_request_interval_seconds: float = 0.25
    finmind_extended_corporate_actions: bool = True
    recent_universe_days: int = 60
    min_history_days: int = 60
    batch_size: int = 64
    learning_rate: float = 0.0003
    daily_learning_rate: float = 0.00003
    epochs: int = 20
    daily_epochs: int = 1
    daily_training_mode: str = "incremental_replay"
    daily_replay_ratio: float = 1.0
    daily_replay_max_sequences: int = 4096
    daily_replay_per_symbol: int = 128
    daily_replay_hit_oversample: float = 4.0
    d_model: int = 128
    n_heads: int = 4
    n_layers: int = 4
    dropout: float = 0.1
    seed: int = 42
    loss_price: float = 2.0
    loss_hit_up: float = 4.0
    loss_hit_down: float = 4.0
    focal_gamma: float = 2.0
    gate_window_days: int = 20
    gate_short_window_days: int = 5
    gate_min_signals: int = 3
    gate_max_success_rate_drop: float = 0.25
    # Internal checkpoint metadata only; never a user configuration override.
    atr_boundaries_pct: list[float] | None = None

    @classmethod
    def load(cls, path: Path | str = DEFAULT_SETTINGS_PATH) -> "Settings":
        values = json.loads(Path(path).read_text(encoding="utf-8"))
        if "atr_boundaries_pct" in values:
            raise ValueError("ATR 界線由初始訓練自動產生，請移除設定檔的 atr_boundaries_pct")
        return cls(**values)

    def save(self, path: Path | str = DEFAULT_SETTINGS_PATH) -> None:
        values = asdict(self)
        values.pop("atr_boundaries_pct")
        Path(path).write_text(
            json.dumps(values, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
