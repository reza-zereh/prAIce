from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = Path(__file__).resolve().parent

# config files
CONFIGS_DIR = SRC_DIR / "configs"
TA_CONFIGS_DIR = CONFIGS_DIR / "ta_indicators"
ML_CONFIGS_DIR = CONFIGS_DIR / "ml_models"
INSTRUMENT_CONFIGS_DIR = CONFIGS_DIR / "instruments"

# notebooks and data
NOTEBOOKS_DIR = ROOT_DIR / "notebooks"
DATA_DIR = NOTEBOOKS_DIR / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_TICKERS_DIR = DATA_RAW_DIR / "tickers"
