def get_unknown_sensor_mask(df, rules):
    allowed_sensors = set(rules["allowed_sensors"].keys())
    return ~df["sensor_id"].isin(allowed_sensors)

def get_wrong_unit_mask(df, rules, sensor):
    expected_unit = rules["allowed_sensors"][sensor]["unit"]
    mask = df["sensor_id"] == sensor
    return mask & (df["unit"] != expected_unit)

def get_missing_value_mask(df):
    return df["value"].isna()

def get_duplicate_row_mask(df):
    return df.duplicated(subset=["ts", "asset_id", "sensor_id"], keep=False)

def get_out_of_range_mask(df, rules, sensor):
    min_v = rules["allowed_sensors"][sensor]["min"]
    max_v = rules["allowed_sensors"][sensor]["max"]
    sensor_mask = df["sensor_id"] == sensor
    return sensor_mask & ((df["value"] < min_v) | (df["value"] > max_v))

def get_expected_sampling_hz(rules, sensor):
    return rules["allowed_sensors"][sensor].get("sampling_hz")

def get_expected_sampling_interval_seconds(rules, sensor):
    sampling_hz = get_expected_sampling_hz(rules, sensor)

    if sampling_hz is None:
        return None

    if sampling_hz <= 0:
        raise ValueError(f"sampling_hz must be > 0 for sensor '{sensor}', got {sampling_hz}")

    return 1.0 / sampling_hz

def get_sampling_intervals_seconds(df, sensor):
    sensor_df = df.loc[df["sensor_id"] == sensor, ["ts", "asset_id", "sensor_id"]].copy()

    if sensor_df.empty:
        return sensor_df

    sensor_df = sensor_df.sort_values(["asset_id", "sensor_id", "ts"]).copy()

    sensor_df["delta_seconds"] = (
        sensor_df.groupby(["asset_id", "sensor_id"])["ts"]
        .diff()
        .dt.total_seconds()
    )

    return sensor_df

def get_sampling_mismatch_intervals(df, rules, sensor, tolerance_ratio=0.05):
    expected_interval = get_expected_sampling_interval_seconds(rules, sensor)

    if expected_interval is None:
        return None

    intervals_df = get_sampling_intervals_seconds(df, sensor).copy()

    if intervals_df.empty:
        return intervals_df

    intervals_df = intervals_df.loc[intervals_df["delta_seconds"].notna()].copy()

    lower_bound = expected_interval * (1 - tolerance_ratio)
    upper_bound = expected_interval * (1 + tolerance_ratio)

    intervals_df["expected_interval_seconds"] = expected_interval
    intervals_df["sampling_ok"] = intervals_df["delta_seconds"].between(lower_bound, upper_bound)
    intervals_df["sampling_deviation_seconds"] = (
        intervals_df["delta_seconds"] - expected_interval
    )

    return intervals_df.loc[~intervals_df["sampling_ok"]].copy()

def count_sampling_mismatches_by_sensor(df, rules, tolerance_ratio=0.05):
    counts = {}

    for sensor in rules["allowed_sensors"].keys():
        mismatch_df = get_sampling_mismatch_intervals(
            df, rules, sensor, tolerance_ratio=tolerance_ratio
        )

        if mismatch_df is None:
            counts[sensor] = None
        else:
            counts[sensor] = int(len(mismatch_df))

    return counts