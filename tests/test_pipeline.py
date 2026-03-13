import sys
import shutil
import subprocess
import pandas as pd
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_PATH = PROJECT_ROOT / "data/raw/raw.parquet"
VALIDATED_PATH = PROJECT_ROOT / "data/validated/validated.parquet"
CLEAN_PATH = PROJECT_ROOT / "data/clean/clean.parquet"
FEATURE_PATH = PROJECT_ROOT / "data/features/features.parquet"
QUALITY_REPORT_PATH = PROJECT_ROOT / "reports/data_quality/quality_report.json"


def test_pipeline_runs():

    result = subprocess.run(
        [sys.executable, "-m", "src.run_pipeline"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"Pipeline failed:\n{result.stderr}"


def test_raw_dataset_exists():

    assert RAW_PATH.exists(), "raw.parquet wurde nicht erzeugt"


def test_validated_dataset_exists():

    assert VALIDATED_PATH.exists(), "validated.parquet wurde nicht erzeugt"


def test_clean_dataset_exists():

    assert CLEAN_PATH.exists(), "clean.parquet wurde nicht erzeugt"


def test_feature_dataset_exists():

    assert FEATURE_PATH.exists(), "features.parquet wurde nicht erzeugt"


def test_quality_report_exists():

    assert QUALITY_REPORT_PATH.exists(), "quality_report.json wurde nicht erzeugt"


def test_raw_schema():

    df = pd.read_parquet(RAW_PATH)

    expected_columns = {
        "ts",
        "asset_id",
        "sensor_id",
        "value",
        "unit",
    }

    assert expected_columns.issubset(df.columns)


def test_clean_dataset_not_empty():

    df = pd.read_parquet(CLEAN_PATH)

    assert len(df) > 0


def test_feature_dataset_not_empty():

    df = pd.read_parquet(FEATURE_PATH)

    assert len(df) > 0


def test_pipeline_bootstraps_directories():

    assert (PROJECT_ROOT / "src").exists(), "PROJECT_ROOT wirkt ungültig"
    assert (PROJECT_ROOT / "configs").exists(), "configs/ nicht gefunden"

    directories_to_reset = [
        PROJECT_ROOT / "data/raw",
        PROJECT_ROOT / "data/validated",
        PROJECT_ROOT / "data/clean",
        PROJECT_ROOT / "data/features",
        PROJECT_ROOT / "reports/data_quality",
    ]

    for directory in directories_to_reset:
        assert str(directory).startswith(str(PROJECT_ROOT)), f"Unsicherer Pfad: {directory}"

    for directory in directories_to_reset:
        if directory.exists():
            shutil.rmtree(directory)

    result = subprocess.run(
        [sys.executable, "-m", "src.run_pipeline"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"Pipeline failed:\n{result.stderr}"

    for directory in directories_to_reset:
        assert directory.exists(), f"Verzeichnis wurde nicht erzeugt: {directory}"