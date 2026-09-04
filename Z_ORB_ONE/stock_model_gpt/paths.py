from pathlib import Path


PACKAGE_DIR = Path(__file__).resolve().parent
Z_ORB_ONE_DIR = PACKAGE_DIR.parent
PROJECT_ROOT = Z_ORB_ONE_DIR.parent
DATA_DIR = PACKAGE_DIR / "data"
CANDLES_DIR = DATA_DIR / "candles"
CORPORATE_ACTIONS_DIR = DATA_DIR / "corporate_actions"
UNIVERSE_DIR = DATA_DIR / "universe"
FEATURES_DIR = DATA_DIR / "features"
CHECKPOINT_DIR = PACKAGE_DIR / "checkpoints"
PREDICTIONS_DIR = PACKAGE_DIR / "predictions"
CONFIG_PATH = Z_ORB_ONE_DIR / "config.ini"
STOCK_DATA_PATH = Z_ORB_ONE_DIR / "stock_data.py"


def ensure_runtime_dirs() -> None:
    for path in (
        CANDLES_DIR,
        CORPORATE_ACTIONS_DIR,
        UNIVERSE_DIR,
        FEATURES_DIR,
        CHECKPOINT_DIR,
        PREDICTIONS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
