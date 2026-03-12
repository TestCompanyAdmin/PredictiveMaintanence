import pandas as pd
import yaml

from src.utils.paths import CLEAN_DIR, FEATURE_DIR, CONFIG_DIR


def load_feature_config():
    path = CONFIG_DIR / "feature_config.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def compute_window_features(group, asset_id, sensor_id, window_size_seconds, step_size_seconds):
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

            results.append({
                "window_start": current_start,
                "window_end": current_end,
                "asset_id": asset_id,
                "sensor_id": sensor_id,
                "mean": values.mean(),
                "std": values.std(),
                "min": values.min(),
                "max": values.max(),
                "rms": (values.pow(2).mean()) ** 0.5,
                "sample_count": len(window_df),
            })

        current_start = current_start + pd.Timedelta(seconds=step_size_seconds)

    return pd.DataFrame(results)


def compute_features(df, window_size_seconds, step_size_seconds):
    feature_frames = []

    for (asset_id, sensor_id), group in df.groupby(["asset_id", "sensor_id"]):
        feat = compute_window_features(
            group=group,
            asset_id=asset_id,
            sensor_id=sensor_id,
            window_size_seconds=window_size_seconds,
            step_size_seconds=step_size_seconds,
        )
        feature_frames.append(feat)

    if not feature_frames:
        return pd.DataFrame()

    return pd.concat(feature_frames, ignore_index=True)


def main():
    input_file = CLEAN_DIR / "clean.parquet"

    print(f"loading clean data: {input_file}")

    df = pd.read_parquet(input_file)

    config = load_feature_config()

    window_size_seconds = config["windowing"]["window_size_seconds"]
    step_size_seconds = config["windowing"]["step_size_seconds"]

    feature_df = compute_features(df, window_size_seconds, step_size_seconds)

    FEATURE_DIR.mkdir(parents=True, exist_ok=True)

    output_file = FEATURE_DIR / "features.parquet"
    feature_df.to_parquet(output_file, index=False)

    print(f"features written to: {output_file}")
    print(f"rows: {len(feature_df)}")
    print(f"columns: {list(feature_df.columns)}")


if __name__ == "__main__":
    main()