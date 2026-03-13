import pandas as pd

from src.utils.paths import VALIDATED_DIR, CLEAN_DIR


REQUIRED_COLUMNS = {
    "ts",
    "asset_id",
    "sensor_id",
    "value",
    "unit",
    "quality_flag",
    "quality_reason",
    "quality_reasons",
    "is_usable",
}

EXACT_DUPLICATE_SUBSET = ["ts", "asset_id", "sensor_id", "value", "unit"]
COLLISION_SUBSET = ["ts", "asset_id", "sensor_id"]


def validate_input_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "validated dataset is missing required columns for cleaning: "
            f"{sorted(missing)}"
        )


def remove_unusable_data(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    usable_mask = df["is_usable"].fillna(False).astype(bool)
    cleaned_df = df.loc[usable_mask].copy()
    removed_count = int((~usable_mask).sum())
    return cleaned_df, removed_count


def count_conflicting_collisions(df: pd.DataFrame) -> int:
    grouped = (
        df.groupby(COLLISION_SUBSET)
        .agg(
            value_nunique=("value", "nunique"),
            unit_nunique=("unit", "nunique"),
        )
        .reset_index()
    )

    conflict_groups = grouped[
        (grouped["value_nunique"] > 1) | (grouped["unit_nunique"] > 1)
    ]

    return int(len(conflict_groups))


def remove_exact_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    before = len(df)

    deduplicated_df = (
        df.sort_values(["asset_id", "sensor_id", "ts"])
        .drop_duplicates(subset=EXACT_DUPLICATE_SUBSET, keep="first")
        .copy()
    )

    removed_count = before - len(deduplicated_df)
    return deduplicated_df, removed_count


def sort_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.sort_values(["asset_id", "sensor_id", "ts"])
        .reset_index(drop=True)
        .copy()
    )


def print_cleaning_summary(
    rows_before: int,
    unusable_removed: int,
    exact_duplicates_removed: int,
    conflict_collisions_found: int,
    rows_after: int,
) -> None:
    print("\nCLEANING SUMMARY")
    print(f"rows before cleaning: {rows_before}")
    print(f"rows removed as unusable: {unusable_removed}")
    print(f"rows removed as exact duplicates: {exact_duplicates_removed}")
    print(f"conflicting timestamp/sensor collisions found: {conflict_collisions_found}")
    print(f"rows after cleaning: {rows_after}")


def main():
    input_file = VALIDATED_DIR / "validated.parquet"
    output_file = CLEAN_DIR / "clean.parquet"

    print(f"loading validated data: {input_file}")
    df = pd.read_parquet(input_file)

    validate_input_columns(df)

    rows_before = len(df)

    df, unusable_removed = remove_unusable_data(df)
    conflict_collisions_found = count_conflicting_collisions(df)
    df, exact_duplicates_removed = remove_exact_duplicates(df)
    df = sort_data(df)

    rows_after = len(df)

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_file, index=False)

    print_cleaning_summary(
        rows_before=rows_before,
        unusable_removed=unusable_removed,
        exact_duplicates_removed=exact_duplicates_removed,
        conflict_collisions_found=conflict_collisions_found,
        rows_after=rows_after,
    )

    print(f"\ncleaned data written to: {output_file}")


if __name__ == "__main__":
    main()