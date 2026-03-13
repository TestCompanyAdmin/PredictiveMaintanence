import pandas as pd

from src.utils.paths import VALIDATED_DIR
from src.validation.load_rules import load_validation_rules
from src.validation.checks import (
    get_unknown_sensor_mask,
    get_wrong_unit_mask,
    get_missing_value_mask,
    get_duplicate_row_mask,
    get_out_of_range_mask,
    get_sampling_mismatch_intervals,
    get_conflicting_collision_mask,
)

QUALITY_SEVERITY_ORDER = {
    "GOOD": 0,
    "WARNING": 1,
    "BAD": 2,
}

# Zentrale fachliche Regelbasis:
# - Reihenfolge = Priorität innerhalb gleicher Severity
# - Höhere Severity überschreibt niedrigere Severity
QUALITY_RULES_IN_PRIORITY_ORDER = [
    ("BAD", "unknown_sensor", False),
    ("BAD", "missing_value", False),
    ("BAD", "conflicting_collision", False),
    ("BAD", "wrong_unit", False),
    ("WARNING", "duplicate_row", True),
    ("WARNING", "out_of_range", True),
    ("WARNING", "sampling_mismatch", True),
]


def get_sampling_mismatch_row_mask(df, rules, tolerance_ratio=0.05):
    mismatch_indices = []
    df_with_index = df.reset_index()

    for sensor in rules["allowed_sensors"].keys():
        mismatch_df = get_sampling_mismatch_intervals(
            df, rules, sensor, tolerance_ratio=tolerance_ratio
        )

        if mismatch_df is None or mismatch_df.empty:
            continue

        sensor_rows = df_with_index.merge(
            mismatch_df[["ts", "asset_id", "sensor_id"]],
            on=["ts", "asset_id", "sensor_id"],
            how="inner",
        )

        mismatch_indices.extend(sensor_rows["index"].tolist())

    return df.index.isin(mismatch_indices)


def build_rule_masks(df, rules):
    allowed_sensors = rules["allowed_sensors"]
    rule_masks = {}

    rule_masks["unknown_sensor"] = get_unknown_sensor_mask(df, rules)
    rule_masks["missing_value"] = get_missing_value_mask(df)
    rule_masks["conflicting_collision"] = get_conflicting_collision_mask(df)
    rule_masks["duplicate_row"] = get_duplicate_row_mask(df)
    rule_masks["sampling_mismatch"] = get_sampling_mismatch_row_mask(df, rules)

    wrong_unit_mask = pd.Series(False, index=df.index)
    for sensor in allowed_sensors.keys():
        wrong_unit_mask = wrong_unit_mask | pd.Series(
            get_wrong_unit_mask(df, rules, sensor),
            index=df.index,
        )
    rule_masks["wrong_unit"] = wrong_unit_mask.to_numpy()

    out_of_range_mask = pd.Series(False, index=df.index)
    for sensor in allowed_sensors.keys():
        out_of_range_mask = out_of_range_mask | pd.Series(
            get_out_of_range_mask(df, rules, sensor),
            index=df.index,
        )
    rule_masks["out_of_range"] = out_of_range_mask.to_numpy()

    return rule_masks


def apply_quality_flags(df, rules):
    df = df.copy()

    df["quality_flag"] = "GOOD"
    df["quality_reason"] = "valid"
    df["quality_reasons"] = ""
    df["is_usable"] = True

    rule_masks = build_rule_masks(df, rules)

    # Für Gleichstände innerhalb derselben Severity gilt:
    # Die früher definierte Regel in QUALITY_RULES_IN_PRIORITY_ORDER bleibt
    # der primäre quality_reason.
    current_priority_rank = pd.Series(float("inf"), index=df.index)

    def append_reason(mask, reason):
        matched_index = df.index[mask]

        for idx in matched_index:
            current = df.at[idx, "quality_reasons"]

            if not current:
                df.at[idx, "quality_reasons"] = reason
            else:
                existing = [part.strip() for part in current.split("|") if part.strip()]
                if reason not in existing:
                    df.at[idx, "quality_reasons"] = current + "|" + reason

    for priority_rank, (flag, reason, usable) in enumerate(QUALITY_RULES_IN_PRIORITY_ORDER):
        if reason not in rule_masks:
            raise ValueError(f"No mask defined for quality rule: {reason}")

        mask = pd.Series(rule_masks[reason], index=df.index).fillna(False).astype(bool)

        if not mask.any():
            continue

        append_reason(mask, reason)

        new_severity = QUALITY_SEVERITY_ORDER[flag]
        current_severity = df["quality_flag"].map(QUALITY_SEVERITY_ORDER)

        higher_severity_mask = mask & (current_severity < new_severity)
        same_severity_but_higher_priority_mask = (
            mask
            & (current_severity == new_severity)
            & (current_priority_rank > priority_rank)
        )

        to_update = higher_severity_mask | same_severity_but_higher_priority_mask

        df.loc[to_update, "quality_flag"] = flag
        df.loc[to_update, "quality_reason"] = reason
        current_priority_rank.loc[to_update] = priority_rank

        if not usable:
            df.loc[mask, "is_usable"] = False

    no_reason_mask = df["quality_reasons"] == ""
    df.loc[no_reason_mask, "quality_reasons"] = "valid"

    return df


def main():
    input_file = VALIDATED_DIR / "validated.parquet"
    output_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated dataset: {input_file}")

    df = pd.read_parquet(input_file)
    rules = load_validation_rules()

    df = apply_quality_flags(df, rules)
    df.to_parquet(output_file, index=False)

    print("quality flags applied")
    print(df["quality_flag"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()