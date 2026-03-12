import pandas as pd
import yaml
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_string_dtype,
)

from src.utils.paths import RAW_DIR, VALIDATED_DIR, CONFIG_DIR


def load_contract():
    contract_path = CONFIG_DIR / "data_contract.yaml"
    with open(contract_path, "r") as f:
        return yaml.safe_load(f)


def validate_columns(df: pd.DataFrame, contract: dict):
    required_columns = contract["required_columns"]
    missing = [c for c in required_columns if c not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_not_null(df: pd.DataFrame, contract: dict):
    required_not_null = contract.get("required_not_null", [])

    for col in required_not_null:
        if df[col].isna().any():
            print(f"WARNING: null values detected in column '{col}'")


def validate_dtypes(df: pd.DataFrame, contract: dict):
    column_types = contract.get("column_types", {})

    for col, expected_type in column_types.items():
        if col not in df.columns:
            continue

        if expected_type == "datetime":
            if not is_datetime64_any_dtype(df[col]):
                raise TypeError(f"Column '{col}' must be datetime, got {df[col].dtype}")

        elif expected_type == "float":
            if not is_numeric_dtype(df[col]):
                raise TypeError(f"Column '{col}' must be numeric, got {df[col].dtype}")

        elif expected_type == "string":
            if not is_string_dtype(df[col]):
                raise TypeError(f"Column '{col}' must be string-like, got {df[col].dtype}")


def validate_allowed_units(df: pd.DataFrame, contract: dict):
    allowed_units = contract.get("allowed_units", {})

    flattened_units = set()
    for unit_list in allowed_units.values():
        flattened_units.update(unit_list)

    invalid_units = ~df["unit"].isin(flattened_units)

    if invalid_units.any():
        found_units = df.loc[invalid_units, "unit"].dropna().unique().tolist()
        raise ValueError(f"Invalid units detected: {found_units}")

def main():
    raw_file = RAW_DIR / "raw.parquet"

    print(f"loading raw data: {raw_file}")

    df = pd.read_parquet(raw_file)

    contract = load_contract()

    validate_columns(df, contract)
    validate_not_null(df, contract)
    validate_dtypes(df, contract)
    validate_allowed_units(df, contract)

    VALIDATED_DIR.mkdir(parents=True, exist_ok=True)

    output_file = VALIDATED_DIR / "validated.parquet"
    df.to_parquet(output_file, index=False)

    print(f"validated dataset written to: {output_file}")
    print(f"rows: {len(df)}")
    print(f"columns: {list(df.columns)}")


if __name__ == "__main__":
    main()