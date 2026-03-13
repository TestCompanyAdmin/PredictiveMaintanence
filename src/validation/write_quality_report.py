import json
import pandas as pd

from src.utils.paths import VALIDATED_DIR, DATA_QUALITY_REPORT_DIR
from src.validation.load_rules import load_validation_rules
from src.validation.checks import count_sampling_mismatches_by_sensor


def count_by_sensor_and_reason(df, reason):
    subset = df[df["quality_reason"] == reason]
    return {str(k): int(v) for k, v in subset["sensor_id"].value_counts().to_dict().items()}


def count_final_reason_by_sensor(df, reason):
    subset = df[df["quality_reason"] == reason]
    return {str(k): int(v) for k, v in subset["sensor_id"].value_counts().to_dict().items()}


def find_missing_sensors(df, rules):
    expected = set(rules["allowed_sensors"].keys())
    observed = set(df["sensor_id"].dropna().unique())
    return sorted(expected - observed)


def main():
    input_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated dataset: {input_file}")

    df = pd.read_parquet(input_file)
    rules = load_validation_rules()
    sampling_mismatch_counts = count_sampling_mismatches_by_sensor(df, rules)
    final_sampling_reason_counts = count_final_reason_by_sensor(df, "sampling_mismatch")

    required_quality_cols = {"quality_flag", "quality_reason", "is_usable"}
    missing_quality_cols = required_quality_cols - set(df.columns)
    if missing_quality_cols:
        raise ValueError(
            "validated dataset is missing quality columns: "
            f"{sorted(missing_quality_cols)}. "
            "Run quality_flags before write_quality_report."
        )

    report = {
        "row_count": int(len(df)),
        "quality_flag_counts": {
            str(k): int(v)
            for k, v in df["quality_flag"].value_counts(dropna=False).to_dict().items()
        },
        "quality_reason_counts": {
            str(k): int(v)
            for k, v in df["quality_reason"].value_counts(dropna=False).to_dict().items()
        },
        "is_usable_counts": {
            str(k): int(v)
            for k, v in df["is_usable"].value_counts(dropna=False).to_dict().items()
        },
        "missing_sensors": find_missing_sensors(df, rules),
        "unknown_sensor": count_by_sensor_and_reason(df, "unknown_sensor"),
        "sampling_mismatches_by_sensor": sampling_mismatch_counts,
        "sampling_mismatch_final_reason_by_sensor": final_sampling_reason_counts,
        "by_sensor": {
            "wrong_unit": count_by_sensor_and_reason(df, "wrong_unit"),
            "duplicate_row": count_by_sensor_and_reason(df, "duplicate_row"),
            "out_of_range": count_by_sensor_and_reason(df, "out_of_range"),
            "missing_value": count_by_sensor_and_reason(df, "missing_value"),
        },
    }

    DATA_QUALITY_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = DATA_QUALITY_REPORT_DIR / "quality_report.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"quality report written to: {output_file}")


if __name__ == "__main__":
    main()