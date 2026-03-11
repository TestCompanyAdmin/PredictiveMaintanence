from pathlib import Path
import pandas as pd


RAW_PATH = Path("data/raw.parquet")
CLEAN_PATH = Path("data/clean.parquet")
FEATURES_PATH = Path("data/features.parquet")
REPORT_PATH = Path("reports/quality_report.json")


def test_output_files_exist():
    assert RAW_PATH.exists(), "raw.parquet was not created"
    assert CLEAN_PATH.exists(), "clean.parquet was not created"
    assert FEATURES_PATH.exists(), "features.parquet was not created"
    assert REPORT_PATH.exists(), "quality_report.json was not created"


def test_raw_schema():
    df = pd.read_parquet(RAW_PATH)
    assert list(df.columns) == ["ts", "sensor_id", "value"]


def test_clean_schema():
    df = pd.read_parquet(CLEAN_PATH)
    assert list(df.columns) == ["ts", "sensor_id", "value", "quality_flag"]


def test_features_schema():
    df = pd.read_parquet(FEATURES_PATH)
    expected_cols = ["sensor_id", "mean", "std", "min", "max", "p95", "rms"]
    assert list(df.columns) == expected_cols


def test_quality_flag_values():
    df = pd.read_parquet(CLEAN_PATH)
    assert df["quality_flag"].isin([0, 1, 2]).all()


def test_features_not_empty():
    df = pd.read_parquet(FEATURES_PATH)
    assert len(df) > 0
