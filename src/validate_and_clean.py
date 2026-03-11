import pandas as pd
import numpy as np
import json


RAW_PATH = "data/raw.parquet"
CLEAN_PATH = "data/clean.parquet"
REPORT_PATH = "reports/quality_report.json"


EXPECTED_COLUMNS = ["ts", "sensor_id", "value"]


SENSOR_LIMITS = {
    "pump01_temp_motor": (-20, 150),
    "pump01_vib_x": (-50, 50),
    "pump01_vib_y": (-50, 50),
    "pump01_current_l1": (0, 100),
    "pump01_pressure_inlet": (0, 25),
}


def validate_schema(df):
    if list(df.columns) != EXPECTED_COLUMNS:
        raise ValueError("Dataset schema does not match data contract")


def handle_duplicates(df):
    duplicates = df.duplicated(subset=["ts", "sensor_id"]).sum()

    df = (
        df.groupby(["ts", "sensor_id"], as_index=False)
        .agg({"value": "mean"})
    )

    return df, duplicates


def detect_outliers(df):
    outlier_count = 0

    for sensor, (min_v, max_v) in SENSOR_LIMITS.items():
        mask = df["sensor_id"] == sensor
        outliers = mask & ((df["value"] < min_v) | (df["value"] > max_v))

        outlier_count += int(outliers.sum())

        df.loc[outliers, "value"] = np.nan
        df.loc[outliers, "quality_flag"] = 2

    return df, outlier_count


def handle_missing(df):
    missing_before = int(df["value"].isna().sum())

    df = df.sort_values(["sensor_id", "ts"]).copy()

    df["value"] = df.groupby("sensor_id")["value"].transform(
        lambda x: x.interpolate(limit=5)
    )

    missing_after = int(df["value"].isna().sum())

    df.loc[df["value"].isna(), "quality_flag"] = 2

    return df, missing_before, missing_after


def detect_stuck(df):
    stuck_count = 0

    for sensor in df["sensor_id"].unique():
        s = df[df["sensor_id"] == sensor]

        if len(s) < 30:
            continue

        rolling_std = s["value"].rolling(30).std()
        stuck_mask = rolling_std == 0

        stuck_count += int(stuck_mask.sum())
        df.loc[s.index[stuck_mask], "quality_flag"] = 2

    return df, stuck_count


def detect_jumps(df):
    jump_count = 0

    for sensor in df["sensor_id"].unique():
        s = df[df["sensor_id"] == sensor]

        diff = s["value"].diff()
        rolling_std = s["value"].rolling(60).std()

        jumps = abs(diff) > (5 * rolling_std)

        jump_count += int(jumps.sum())
        df.loc[s.index[jumps], "quality_flag"] = 1

    return df, jump_count


def main():
    df = pd.read_parquet(RAW_PATH)

    validate_schema(df)

    df["quality_flag"] = 0

    df, duplicate_count = handle_duplicates(df)
    df, outlier_count = detect_outliers(df)
    df, missing_before, missing_after = handle_missing(df)
    df, stuck_count = detect_stuck(df)
    df, jump_count = detect_jumps(df)

    df["quality_flag"] = df["quality_flag"].fillna(0).astype(int)
    df.to_parquet(CLEAN_PATH, index=False)

    report = {
        "duplicates_removed": int(duplicate_count),
        "outliers_detected": int(outlier_count),
        "missing_before": int(missing_before),
        "missing_after": int(missing_after),
        "stuck_values": int(stuck_count),
        "jumps_detected": int(jump_count),
    }

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

    print("clean dataset written to:", CLEAN_PATH)
    print("quality report written to:", REPORT_PATH)


if __name__ == "__main__":
    main()