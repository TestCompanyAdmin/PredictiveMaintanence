import pandas as pd

from src.utils.paths import VALIDATED_DIR
from src.validation.load_rules import load_validation_rules
from src.validation.checks import get_unknown_sensor_mask
from src.validation.checks import get_unknown_sensor_mask, get_wrong_unit_mask
from src.validation.checks import (
    get_unknown_sensor_mask,
    get_wrong_unit_mask,
    get_missing_value_mask,
    get_duplicate_row_mask,
    get_out_of_range_mask,
    get_sampling_mismatch_intervals,
)

QUALITY_SEVERITY_ORDER = {
    "GOOD": 0,
    "WARNING": 1,
    "BAD": 2,
}

QUALITY_RULES_IN_PRIORITY_ORDER = [
    ("BAD", "unknown_sensor", False),
    ("BAD", "missing_value", False),
    ("WARNING", "duplicate_row", True),
    ("BAD", "wrong_unit", False),
    ("WARNING", "out_of_range", True),
    ("WARNING", "sampling_mismatch", True),
]


def get_sampling_mismatch_row_mask(df, rules, tolerance_ratio=0.05):
    mismatch_indices = []

    for sensor in rules["allowed_sensors"].keys():
        mismatch_df = get_sampling_mismatch_intervals(
            df, rules, sensor, tolerance_ratio=tolerance_ratio
        )

        if mismatch_df is None or mismatch_df.empty:
            continue

        sensor_rows = df.reset_index().merge(
            mismatch_df[["ts", "asset_id", "sensor_id"]],
            on=["ts", "asset_id", "sensor_id"],
            how="inner",
        )

        mismatch_indices.extend(sensor_rows["index"].tolist())

    return df.index.isin(mismatch_indices)


# Priority model:
# - BAD overrides WARNING and GOOD
# - WARNING overrides GOOD
# - quality_reason stores the highest-severity reason seen for a row
# - is_usable is the operational decision for downstream processing

def apply_quality_flags(df, rules):
    df["quality_flag"] = "GOOD"
    df["quality_reason"] = "valid"
    df["is_usable"] = True

    allowed_sensors = rules["allowed_sensors"]

    def apply_flag(mask, flag, reason, usable):
        severity_order = QUALITY_SEVERITY_ORDER

        current_severity = df["quality_flag"].map(severity_order)
        new_severity = severity_order[flag]

        to_update = mask & (current_severity < new_severity)

        df.loc[to_update, "quality_flag"] = flag
        df.loc[to_update, "quality_reason"] = reason
        df.loc[to_update, "is_usable"] = usable

    checks = [
        ("BAD", "unknown_sensor", False, get_unknown_sensor_mask(df, rules)),
        ("BAD", "missing_value", False, get_missing_value_mask(df)),
        ("WARNING", "duplicate_row", True, get_duplicate_row_mask(df)),
        ("WARNING", "sampling_mismatch", True, get_sampling_mismatch_row_mask(df, rules))
    ]

    for sensor in allowed_sensors.keys():
        checks.append(("BAD", "wrong_unit", False, get_wrong_unit_mask(df, rules, sensor)))

    for sensor in allowed_sensors.keys():
        checks.append(("WARNING", "out_of_range", True, get_out_of_range_mask(df, rules, sensor)))

    for flag, reason, usable, mask in checks:
        apply_flag(mask, flag, reason, usable)

    return df


def main():
    file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated dataset: {file}")

    df = pd.read_parquet(file)
    rules = load_validation_rules()

    df = apply_quality_flags(df, rules)

    output_file = VALIDATED_DIR / "validated.parquet"
    df.to_parquet(output_file, index=False)

    print("quality flags applied")
    print(df["quality_flag"].value_counts())


if __name__ == "__main__":
    main()