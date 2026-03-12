import pandas as pd
import yaml

from src.utils.paths import VALIDATED_DIR, CONFIG_DIR


def load_rules():
    path = CONFIG_DIR / "validation_rules.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def apply_quality_flags(df, rules):

    df["quality_flag"] = "GOOD"
    df["quality_reason"] = "valid"
    df["is_usable"] = True

    allowed_sensors = rules["allowed_sensors"]

    # Unknown sensor
    unknown_sensor = ~df["sensor_id"].isin(allowed_sensors.keys())
    df.loc[unknown_sensor, "quality_flag"] = "BAD"
    df.loc[unknown_sensor, "quality_reason"] = "unknown_sensor"
    df.loc[unknown_sensor, "is_usable"] = False

    # Wrong unit per sensor
    for sensor, cfg in allowed_sensors.items():
        expected_unit = cfg["unit"]

        mask = df["sensor_id"] == sensor
        wrong_unit = mask & (df["unit"] != expected_unit)

        df.loc[wrong_unit, "quality_flag"] = "BAD"
        df.loc[wrong_unit, "quality_reason"] = "wrong_unit"
        df.loc[wrong_unit, "is_usable"] = False

    # Missing values
    missing = df["value"].isna()
    df.loc[missing, "quality_flag"] = "BAD"
    df.loc[missing, "quality_reason"] = "missing_value"
    df.loc[missing, "is_usable"] = False

    # Duplicate rows based on timestamp + asset + sensor
    duplicate_mask = df.duplicated(subset=["ts", "asset_id", "sensor_id"], keep=False)
    df.loc[duplicate_mask, "quality_flag"] = "BAD"
    df.loc[duplicate_mask, "quality_reason"] = "duplicate_row"
    df.loc[duplicate_mask, "is_usable"] = False

    # Range checks
    for sensor, cfg in allowed_sensors.items():

        mask = df["sensor_id"] == sensor
        min_v = cfg["min"]
        max_v = cfg["max"]

        out_of_range = mask & ((df["value"] < min_v) | (df["value"] > max_v))

        df.loc[out_of_range, "quality_flag"] = "BAD"
        df.loc[out_of_range, "quality_reason"] = "out_of_range"
        df.loc[out_of_range, "is_usable"] = False

    return df


def main():

    file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated dataset: {file}")

    df = pd.read_parquet(file)

    rules = load_rules()

    df = apply_quality_flags(df, rules)

    output_file = VALIDATED_DIR / "validated.parquet"

    df.to_parquet(output_file, index=False)

    print("quality flags applied")
    print(df["quality_flag"].value_counts())


if __name__ == "__main__":
    main()