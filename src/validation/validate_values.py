import pandas as pd
import yaml

from src.utils.paths import VALIDATED_DIR, CONFIG_DIR


def load_validation_rules():
    path = CONFIG_DIR / "validation_rules.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def count_unknown_sensors(df, rules):
    allowed = set(rules["allowed_sensors"].keys())
    return int((~df["sensor_id"].isin(allowed)).sum())


def count_wrong_units(df, rules):
    total = 0

    for sensor, cfg in rules["allowed_sensors"].items():
        expected_unit = cfg["unit"]
        mask = df["sensor_id"] == sensor
        wrong_unit = mask & (df["unit"] != expected_unit)
        total += int(wrong_unit.sum())

    return total

def count_duplicates(df):
    duplicate_mask = df.duplicated(subset=["ts", "asset_id", "sensor_id"], keep=False)
    return int(duplicate_mask.sum())

def count_out_of_range(df, rules):
    counts = {}

    for sensor, cfg in rules["allowed_sensors"].items():
        min_v = cfg["min"]
        max_v = cfg["max"]

        mask = df["sensor_id"] == sensor
        out_of_range = mask & ((df["value"] < min_v) | (df["value"] > max_v))

        counts[sensor] = int(out_of_range.sum())

    return counts

def count_missing_by_sensor(df, rules):
    counts = {}

    for sensor in rules["allowed_sensors"].keys():
        mask = df["sensor_id"] == sensor
        missing = mask & df["value"].isna()
        counts[sensor] = int(missing.sum())

    return counts

def main():
    file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated data: {file}")

    df = pd.read_parquet(file)
    rules = load_validation_rules()

    unknown_sensor_count = count_unknown_sensors(df, rules)
    wrong_unit_count = count_wrong_units(df, rules)
    duplicate_count = count_duplicates(df)
    out_of_range_counts = count_out_of_range(df, rules)
    missing_counts = count_missing_by_sensor(df, rules)

    print("\nVALIDATION SUMMARY")
    print(f"unknown sensors: {unknown_sensor_count}")
    print(f"wrong units: {wrong_unit_count}")
    print(f"duplicates: {duplicate_count}")
    print("out of range by sensor:")
    for sensor, count in out_of_range_counts.items():
        print(f"  {sensor}: {count}")
    print("missing values by sensor:")
    for sensor, count in missing_counts.items():
        print(f"  {sensor}: {count}")


    print("\nvalue validation finished")


if __name__ == "__main__":
    main()