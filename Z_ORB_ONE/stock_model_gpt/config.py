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
    finmind_extended_corporate_actions: bool = False
    recent_universe_days: int = 60
    min_history_days: int = 60
    batch_size: int = 64
    learning_rate: float = 0.0003
    daily_learning_rate: float = 0.00003
    epochs: int = 20
    daily_epochs: int = 1
    d_model: int = 128
    n_heads: int = 4
    n_layers: int = 4
    dropout: float = 0.1
    seed: int = 42
    loss_price: float = 4.0
    loss_hit_up: float = 2.0
    loss_hit_down: float = 2.0
    loss_close_limit: float = 1.0

    @classmethod
    def load(cls, path: Path | str = DEFAULT_SETTINGS_PATH) -> "Settings":
        values = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(**values)

    def save(self, path: Path | str = DEFAULT_SETTINGS_PATH) -> None:
        Path(path).write_text(
            json.dumps(asdict(self), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
