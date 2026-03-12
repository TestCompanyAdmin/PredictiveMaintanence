import pandas as pd

from src.utils.paths import VALIDATED_DIR, CLEAN_DIR


def remove_unusable_data(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["is_usable"] == True]


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=["ts", "asset_id", "sensor_id"])


def sort_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["ts", "sensor_id"])


def main():

    input_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated dataset: {input_file}")

    df = pd.read_parquet(input_file)

    rows_before = len(df)

    df = remove_unusable_data(df)
    df = remove_duplicates(df)
    df = sort_data(df)

    rows_after = len(df)

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    output_file = CLEAN_DIR / "clean.parquet"

    df.to_parquet(output_file, index=False)

    print(f"clean dataset written to: {output_file}")
    print(f"rows before: {rows_before}")
    print(f"rows after: {rows_after}")


if __name__ == "__main__":
    main()