import pandas as pd

from src.utils.paths import VALIDATED_DIR
from src.validation.load_rules import load_validation_rules
from src.validation.checks import count_sampling_mismatches_by_sensor

def find_missing_sensors(df, rules):
    expected = set(rules["allowed_sensors"].keys())
    observed = set(df["sensor_id"].dropna().unique())
    return sorted(expected - observed)


def count_by_sensor_and_reason(df, reason):
    subset = df[df["quality_reason"] == reason]
    counts = subset["sensor_id"].value_counts().to_dict()
    return counts


def print_sensor_counts(title, counts, sensors):
    print(f"{title}:")
    for sensor in sensors:
        print(f"    {sensor}: {counts.get(sensor, 0)}")


def count_final_reason_by_sensor(df, reason):
    subset = df[df["quality_reason"] == reason]
    return subset["sensor_id"].value_counts().to_dict()


def main():
    input_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated data: {input_file}")

    df = pd.read_parquet(input_file)
    rules = load_validation_rules()

    required_quality_cols = {"quality_flag", "quality_reason", "is_usable"}
    missing_quality_cols = required_quality_cols - set(df.columns)
    if missing_quality_cols:
        raise ValueError(
            "validated dataset is missing quality columns: "
            f"{sorted(missing_quality_cols)}. "
            "Run quality_flags before print_validation."
        )

    sensors = sorted(df["sensor_id"].dropna().unique().tolist())

    duplicate_counts = count_by_sensor_and_reason(df, "duplicate_row")
    missing_counts = count_by_sensor_and_reason(df, "missing_value")
    out_of_range_counts = count_by_sensor_and_reason(df, "out_of_range")
    wrong_unit_counts = count_by_sensor_and_reason(df, "wrong_unit")
    unknown_sensor_counts = count_by_sensor_and_reason(df, "unknown_sensor")
    missing_sensors = find_missing_sensors(df, rules)
    sampling_mismatch_counts = count_sampling_mismatches_by_sensor(df, rules)
    final_sampling_reason_counts = count_final_reason_by_sensor(df, "sampling_mismatch")

    print("\nVALIDATION SUMMARY")
    print("quality_flag counts:")
    print(df["quality_flag"].value_counts(dropna=False).to_string())

    print("\nquality_reason counts:")
    print(df["quality_reason"].value_counts(dropna=False).to_string())

    print("\nis_usable counts:")
    print(df["is_usable"].value_counts(dropna=False).to_string())

    print()
    print_sensor_counts("wrong units by sensor", wrong_unit_counts, sensors)
    print_sensor_counts("duplicates by sensor", duplicate_counts, sensors)
    print_sensor_counts("out of range by sensor", out_of_range_counts, sensors)
    print_sensor_counts("missing values by sensor", missing_counts, sensors)
    print("sampling mismatches by sensor:")
    print("sampling mismatches by sensor:")
    for sensor in sorted(rules["allowed_sensors"].keys()):
        detected = sampling_mismatch_counts.get(sensor)
        final_reason = final_sampling_reason_counts.get(sensor, 0)

        if detected is None:
            print(f"    {sensor}: not configured")
        else:
            overridden = detected - final_reason
            print(
                f"    {sensor}: detected={detected}, "
                f"final_reason={final_reason}, overridden={overridden}"
            )
    print("missing sensors:")
    if missing_sensors:
        for sensor in missing_sensors:
            print(f"    {sensor}")
    else:
        print("    0")

    if unknown_sensor_counts:
        print("unknown sensors:")
        for sensor, count in unknown_sensor_counts.items():
            print(f"    {sensor}: {count}")
    else:
        print("unknown sensors:")
        print("    0")
    
    print("\nvalue validation finished")


if __name__ == "__main__":
    main()