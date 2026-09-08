import os
from pathlib import Path


PACKAGE_DIR = Path(__file__).resolve().parent
Z_ORB_ONE_DIR = PACKAGE_DIR.parent
PROJECT_ROOT = Z_ORB_ONE_DIR.parent

# Read-only inputs: always the real package data, even when write output is
# redirected below. Candle/feature/corporate-action history and stock_data.py
# are shared, never mutated by a walk-forward backtest.
DATA_DIR = PACKAGE_DIR / "data"
CANDLES_DIR = DATA_DIR / "candles"
CORPORATE_ACTIONS_DIR = DATA_DIR / "corporate_actions"
FEATURES_DIR = DATA_DIR / "features"
CONFIG_PATH = Z_ORB_ONE_DIR / "config.ini"
STOCK_DATA_PATH = Z_ORB_ONE_DIR / "stock_data.py"

# Write targets: redirected under STOCK_MODEL_GPT_WRITE_ROOT when that env var
# is set (e.g. by a walk-forward backtest simulating historical dates), so a
# simulated run never overwrites production checkpoints, predictions,
# evaluations, universe snapshots, or signal reports. Unset (the default),
# this resolves to exactly the same paths as before this env var existed.
_write_root_override = os.environ.get("STOCK_MODEL_GPT_WRITE_ROOT")
_WRITE_ROOT = Path(_write_root_override).resolve() if _write_root_override else PACKAGE_DIR
ACTUAL_CANDLES_DIR = _WRITE_ROOT / "data" / "actual_candles"
UNIVERSE_DIR = _WRITE_ROOT / "data" / "universe"
EVALUATIONS_DIR = _WRITE_ROOT / "data" / "evaluations"
ATR_ANALYSIS_DIR = _WRITE_ROOT / "data" / "atr_analysis"
CHECKPOINT_DIR = _WRITE_ROOT / "checkpoints"
PREDICTIONS_DIR = _WRITE_ROOT / "predictions"
SIGNAL_REPORTS_DIR = _WRITE_ROOT / "signal_reports"


def ensure_runtime_dirs() -> None:
    for path in (
        CANDLES_DIR,
        ACTUAL_CANDLES_DIR,
        CORPORATE_ACTIONS_DIR,
        UNIVERSE_DIR,
        FEATURES_DIR,
        EVALUATIONS_DIR,
        ATR_ANALYSIS_DIR,
        CHECKPOINT_DIR,
        PREDICTIONS_DIR,
        SIGNAL_REPORTS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
