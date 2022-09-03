from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
SRC_DIR = ROOT_DIR / "prAIce"
DATA_DIR = SRC_DIR / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_TICKERS_DIR = DATA_RAW_DIR / "tickers"
LIBS_DIR = SRC_DIR / "libs"
CONFIGS_DIR = SRC_DIR / "configs"
TA_CONFIGS_DIR = CONFIGS_DIR / "ta_indicators"
