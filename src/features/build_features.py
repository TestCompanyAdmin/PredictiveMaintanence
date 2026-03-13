import numpy as np
import pandas as pd
import yaml

from src.utils.paths import CLEAN_DIR, FEATURE_DIR, CONFIG_DIR, VALIDATED_DIR


def load_feature_config():
    path = CONFIG_DIR / "feature_config.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def has_reason(series, reason):
    return (
        series.fillna("")
        .astype(str)
        .apply(lambda x: reason in [part.strip() for part in x.split("|") if part.strip()])
    )


def get_enabled_feature_flags(config):
    features_cfg = config.get("features", {})

    enabled_statistical = set(features_cfg.get("statistical", []))
    enabled_quality = set(features_cfg.get("quality", []))

    return enabled_statistical, enabled_quality


def compute_statistical_window_features(group, asset_id, sensor_id, window_size_seconds, step_size_seconds, enabled_statistical):
    group = group.sort_values("ts").copy()

    start_time = group["ts"].min()
    end_time = group["ts"].max()

    results = []
    current_start = start_time

    while current_start <= end_time:
        current_end = current_start + pd.Timedelta(seconds=window_size_seconds)

        window_df = group[
            (group["ts"] >= current_start) & (group["ts"] < current_end)
        ]

        if not window_df.empty:
            values = window_df["value"]

            if len(window_df) >= 2:
                x = np.arange(len(window_df))
                y = values.to_numpy()
                slope = np.polyfit(x, y, 1)[0]
            else:
                slope = 0.0

            row = {
                "window_start": current_start,
                "window_end": current_end,
                "asset_id": asset_id,
                "sensor_id": sensor_id,
            }

            if "mean" in enabled_statistical:
                row["mean"] = values.mean()
            if "std" in enabled_statistical:
                row["std"] = values.std()
            if "min" in enabled_statistical:
                row["min"] = values.min()
            if "max" in enabled_statistical:
                row["max"] = values.max()
            if "rms" in enabled_statistical:
                row["rms"] = (values.pow(2).mean()) ** 0.5
            if "slope" in enabled_statistical:
                row["slope"] = slope
            if "sample_count" in enabled_statistical:
                row["sample_count"] = len(window_df)

            results.append(row)

        current_start = current_start + pd.Timedelta(seconds=step_size_seconds)

    return pd.DataFrame(results)


def compute_quality_window_features(group, asset_id, sensor_id, window_size_seconds, step_size_seconds, enabled_quality):
    group = group.sort_values("ts").copy()

    start_time = group["ts"].min()
    end_time = group["ts"].max()

    results = []
    current_start = start_time

    while current_start <= end_time:
        current_end = current_start + pd.Timedelta(seconds=window_size_seconds)

        window_df = group[
            (group["ts"] >= current_start) & (group["ts"] < current_end)
        ]

        if not window_df.empty:
            total_rows = len(window_df)

            invalid_rows = (window_df["is_usable"] == False).sum()
            missing_rows = has_reason(window_df["quality_reasons"], "missing_value").sum()
            out_of_range_rows = has_reason(window_df["quality_reasons"], "out_of_range").sum()
            duplicate_rows = has_reason(window_df["quality_reasons"], "duplicate_row").sum()
            sampling_mismatch_rows = has_reason(window_df["quality_reasons"], "sampling_mismatch").sum()

            row = {
                "window_start": current_start,
                "window_end": current_end,
                "asset_id": asset_id,
                "sensor_id": sensor_id,
            }

            if "invalid_ratio" in enabled_quality:
                row["invalid_ratio"] = invalid_rows / total_rows
            if "missing_ratio" in enabled_quality:
                row["missing_ratio"] = missing_rows / total_rows
            if "out_of_range_ratio" in enabled_quality:
                row["out_of_range_ratio"] = out_of_range_rows / total_rows
            if "duplicate_ratio" in enabled_quality:
                row["duplicate_ratio"] = duplicate_rows / total_rows
            if "sampling_mismatch_ratio" in enabled_quality:
                row["sampling_mismatch_ratio"] = sampling_mismatch_rows / total_rows
            if "validated_sample_count" in enabled_quality:
                row["validated_sample_count"] = total_rows

            results.append(row)

        current_start = current_start + pd.Timedelta(seconds=step_size_seconds)

    return pd.DataFrame(results)


def compute_statistical_features(df, window_size_seconds, step_size_seconds, enabled_statistical):
    feature_frames = []

    for (asset_id, sensor_id), group in df.groupby(["asset_id", "sensor_id"]):
        feat = compute_statistical_window_features(
            group=group,
            asset_id=asset_id,
            sensor_id=sensor_id,
            window_size_seconds=window_size_seconds,
            step_size_seconds=step_size_seconds,
            enabled_statistical=enabled_statistical,
        )
        feature_frames.append(feat)

    if not feature_frames:
        return pd.DataFrame()

    return pd.concat(feature_frames, ignore_index=True)


def compute_quality_features(df, window_size_seconds, step_size_seconds, enabled_quality):
    feature_frames = []

    for (asset_id, sensor_id), group in df.groupby(["asset_id", "sensor_id"]):
        feat = compute_quality_window_features(
            group=group,
            asset_id=asset_id,
            sensor_id=sensor_id,
            window_size_seconds=window_size_seconds,
            step_size_seconds=step_size_seconds,
            enabled_quality=enabled_quality,
        )
        feature_frames.append(feat)

    if not feature_frames:
        return pd.DataFrame()

    return pd.concat(feature_frames, ignore_index=True)


def add_zscore_features(feature_df, enabled_statistical):
    if "zscore_mean" not in enabled_statistical:
        return feature_df

    if "mean" not in feature_df.columns:
        raise ValueError("zscore_mean requires 'mean' to be present in feature_df")

    feature_df = feature_df.copy()

    sensor_mean = feature_df.groupby("sensor_id")["mean"].transform("mean")
    sensor_std = feature_df.groupby("sensor_id")["mean"].transform("std")

    feature_df["zscore_mean"] = 0.0

    valid_std_mask = sensor_std.notna() & (sensor_std != 0)
    feature_df.loc[valid_std_mask, "zscore_mean"] = (
        (feature_df.loc[valid_std_mask, "mean"] - sensor_mean.loc[valid_std_mask])
        / sensor_std.loc[valid_std_mask]
    )

    return feature_df

def main():
    clean_input_file = CLEAN_DIR / "clean.parquet"
    validated_input_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading clean data: {clean_input_file}")
    clean_df = pd.read_parquet(clean_input_file)

    print(f"loading validated data: {validated_input_file}")
    validated_df = pd.read_parquet(validated_input_file)

    config = load_feature_config()

    window_size_seconds = config["windowing"]["window_size_seconds"]
    step_size_seconds = config["windowing"]["step_size_seconds"]
    enabled_statistical, enabled_quality = get_enabled_feature_flags(config)

    stat_feature_df = compute_statistical_features(
        clean_df,
        window_size_seconds,
        step_size_seconds,
        enabled_statistical,
    )

    quality_feature_df = compute_quality_features(
        validated_df,
        window_size_seconds,
        step_size_seconds,
        enabled_quality,
    )

    feature_df = stat_feature_df.merge(
        quality_feature_df,
        on=["window_start", "window_end", "asset_id", "sensor_id"],
        how="outer",
    )

    feature_df = feature_df.sort_values(
        ["asset_id", "sensor_id", "window_start"]
    ).reset_index(drop=True)

    feature_df = add_zscore_features(feature_df, enabled_statistical)

    FEATURE_DIR.mkdir(parents=True, exist_ok=True)

    output_file = FEATURE_DIR / "features.parquet"
    feature_df.to_parquet(output_file, index=False)

    print(f"features written to: {output_file}")
    print(f"rows: {len(feature_df)}")
    print(f"columns: {list(feature_df.columns)}")


if __name__ == "__main__":
    main()