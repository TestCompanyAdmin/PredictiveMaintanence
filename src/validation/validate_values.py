from src.utils.sensors import get_known_sensors


def count_unknown_sensors(df, rules):
    allowed = set(get_known_sensors(rules))
    unknown = df.loc[~df["sensor_id"].isin(allowed), "sensor_id"]
    return unknown.value_counts().to_dict()


def find_missing_sensors(df, rules):
    observed = set(df["sensor_id"].dropna().unique())
    expected = set(get_known_sensors(rules))
    return sorted(expected - observed)


def count_wrong_units(df, rules):
    counts = {}

    for sensor, cfg in rules["allowed_sensors"].items():
        expected_unit = cfg["unit"]
        mask = df["sensor_id"] == sensor
        wrong_unit = mask & (df["unit"] != expected_unit)
        counts[sensor] = int(wrong_unit.sum())

    return counts


def count_duplicates(df, rules):
    counts = {sensor: 0 for sensor in get_known_sensors(rules)}

    duplicate_mask = df.duplicated(subset=["ts", "asset_id", "sensor_id"], keep=False)
    dup_df = df.loc[duplicate_mask]
    dup_counts = dup_df["sensor_id"].value_counts()

    for sensor, count in dup_counts.items():
        if sensor in counts:
            counts[sensor] = int(count)

    return counts


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

    for sensor in get_known_sensors(rules):
        mask = df["sensor_id"] == sensor
        missing = mask & df["value"].isna()
        counts[sensor] = int(missing.sum())

    return counts


def build_validation_summary(df, rules):
    return {
        "unknown_sensors": count_unknown_sensors(df, rules),
        "missing_sensors": find_missing_sensors(df, rules),
        "wrong_units_by_sensor": count_wrong_units(df, rules),
        "duplicates_by_sensor": count_duplicates(df, rules),
        "out_of_range_by_sensor": count_out_of_range(df, rules),
        "missing_values_by_sensor": count_missing_by_sensor(df, rules),
    }