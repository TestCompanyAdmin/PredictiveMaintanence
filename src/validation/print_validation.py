import pandas as pd

from src.utils.paths import VALIDATED_DIR
from src.validation.load_rules import load_validation_rules
from src.validation.quality_flags import QUALITY_RULES_IN_PRIORITY_ORDER


REQUIRED_COLUMNS = {
    "sensor_id",
    "quality_flag",
    "quality_reason",
    "quality_reasons",
    "is_usable",
}


def validate_input_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "validated dataset is missing required columns for print_validation: "
            f"{sorted(missing)}. Run quality_flags before print_validation."
        )


def has_reason(series: pd.Series, reason: str) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .apply(lambda x: reason in [part.strip() for part in x.split("|") if part.strip()])
    )


def find_missing_sensors(df: pd.DataFrame, rules: dict) -> list[str]:
    expected = set(rules["allowed_sensors"].keys())
    observed = set(df["sensor_id"].dropna().unique())
    return sorted(expected - observed)


def get_allowed_sensors(rules: dict) -> list[str]:
    return sorted(rules["allowed_sensors"].keys())


def get_unknown_sensor_names(df: pd.DataFrame, rules: dict) -> list[str]:
    allowed = set(rules["allowed_sensors"].keys())
    observed = set(df["sensor_id"].dropna().unique())
    return sorted(observed - allowed)


def count_by_sensor_and_reason(
    df: pd.DataFrame,
    reason: str,
    sensors: list[str],
    use_all_reasons: bool = False,
) -> dict[str, int]:
    if use_all_reasons:
        subset = df[has_reason(df["quality_reasons"], reason)]
    else:
        subset = df[df["quality_reason"] == reason]

    counts = subset["sensor_id"].value_counts().to_dict()
    return {sensor: counts.get(sensor, 0) for sensor in sensors}


def count_all_reasons(df: pd.DataFrame, known_reasons: list[str]) -> dict[str, int]:
    reason_counts = {reason: 0 for reason in known_reasons}

    for value in df["quality_reasons"].fillna("").astype(str):
        parts = [part.strip() for part in value.split("|") if part.strip()]
        for part in parts:
            if part not in reason_counts:
                reason_counts[part] = 0
            reason_counts[part] += 1

    return reason_counts


def count_unusable_by_reason(df: pd.DataFrame, known_reasons: list[str]) -> dict[str, int]:
    unusable_df = df[df["is_usable"].fillna(False) == False]
    counts = {}

    for reason in known_reasons:
        counts[reason] = int(has_reason(unusable_df["quality_reasons"], reason).sum())

    return counts


def print_series_block(title: str, series: pd.Series) -> None:
    print(title)
    print(series.to_string())


def print_reason_count_block(title: str, counts: dict[str, int]) -> None:
    print(title)
    for reason, count in counts.items():
        print(f"    {reason}: {count}")


def print_sensor_count_block(title: str, counts: dict[str, int], sensors: list[str]) -> None:
    print(title)
    for sensor in sensors:
        print(f"    {sensor}: {counts.get(sensor, 0)}")


def main():
    input_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated data: {input_file}")

    df = pd.read_parquet(input_file)
    rules = load_validation_rules()

    validate_input_columns(df)

    allowed_sensors = get_allowed_sensors(rules)
    unknown_sensor_names = get_unknown_sensor_names(df, rules)
    missing_sensors = find_missing_sensors(df, rules)

    known_reasons = [reason for _, reason, _ in QUALITY_RULES_IN_PRIORITY_ORDER]
    all_reason_counts = count_all_reasons(df, known_reasons)
    unusable_reason_counts = count_unusable_by_reason(df, known_reasons)

    print("\nVALIDATION SUMMARY")

    print_series_block(
        "quality_flag counts:",
        df["quality_flag"].value_counts(dropna=False)
    )

    print()
    print_series_block(
        "final quality_reason counts:",
        df["quality_reason"].value_counts(dropna=False)
    )

    print()
    print_reason_count_block(
        "all quality_reasons counts:",
        all_reason_counts
    )

    print()
    print_series_block(
        "is_usable counts:",
        df["is_usable"].value_counts(dropna=False)
    )

    print()
    print_reason_count_block(
        "unusable rows by reason (all reasons):",
        unusable_reason_counts
    )

    print()

    for _, reason, _ in QUALITY_RULES_IN_PRIORITY_ORDER:
        if reason == "unknown_sensor":
            sensors_for_reason = unknown_sensor_names
            title_prefix = "unknown sensors"
        else:
            sensors_for_reason = allowed_sensors
            title_prefix = reason.replace("_", " ")

        all_counts = count_by_sensor_and_reason(
            df=df,
            reason=reason,
            sensors=sensors_for_reason,
            use_all_reasons=True,
        )
        final_counts = count_by_sensor_and_reason(
            df=df,
            reason=reason,
            sensors=sensors_for_reason,
            use_all_reasons=False,
        )

        print_sensor_count_block(
            f"{title_prefix} by sensor (all reasons):",
            all_counts,
            sensors_for_reason,
        )
        print_sensor_count_block(
            f"{title_prefix} by sensor (final reason):",
            final_counts,
            sensors_for_reason,
        )
        print()

    print("missing sensors:")
    if missing_sensors:
        for sensor in missing_sensors:
            print(f"    {sensor}")
    else:
        print("    0")

    print("\nvalue validation finished")


if __name__ == "__main__":
    main()