from pathlib import Path

# Projektwurzel (predictive-maintenance-pipeline/)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Datenordner
DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
VALIDATED_DIR = DATA_DIR / "validated"
CLEAN_DIR = DATA_DIR / "clean"
FEATURE_DIR = DATA_DIR / "features"
MODEL_DIR = DATA_DIR / "models"

# Reports
REPORT_DIR = PROJECT_ROOT / "reports"
DATA_QUALITY_REPORT_DIR = REPORT_DIR / "data_quality"
MODEL_REPORT_DIR = REPORT_DIR / "model_reports"

# Configs
CONFIG_DIR = PROJECT_ROOT / "configs"