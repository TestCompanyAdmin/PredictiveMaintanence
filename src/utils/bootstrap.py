from src.utils.paths import (
    RAW_DIR,
    VALIDATED_DIR,
    CLEAN_DIR,
    FEATURE_DIR,
    REPORT_DIR,
    DATA_QUALITY_REPORT_DIR,
)


def ensure_project_directories():
    directories = [
        RAW_DIR,
        VALIDATED_DIR,
        CLEAN_DIR,
        FEATURE_DIR,
        REPORT_DIR,
        DATA_QUALITY_REPORT_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)