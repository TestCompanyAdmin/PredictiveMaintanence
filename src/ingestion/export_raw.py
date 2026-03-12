import numpy as np
import pandas as pd

from src.utils.paths import RAW_DIR


def build_sensor_data() -> pd.DataFrame:
    np.random.seed(42)

    asset_id = "pump_1"

    duration_seconds = 120
    base_start = pd.Timestamp("2025-01-01 10:00:00", tz="UTC")

    sensors = {
        "temp_motor": {
            "unit": "C",
            "base": 60,
            "noise": 1.5,
            "sampling_hz": 1,
        },
        "pressure_inlet": {
            "unit": "bar",
            "base": 10,
            "noise": 0.3,
            "sampling_hz": 2,
        },
        "rpm_motor": {
            "unit": "rpm",
            "base": 1450,
            "noise": 15,
            "sampling_hz": 1,
        },
        "vibration_x": {
            "unit": "mm/s",
            "base": 3.5,
            "noise": 0.4,
            "sampling_hz": 10,
        },
        "vibration_y": {
            "unit": "mm/s",
            "base": 3.8,
            "noise": 0.5,
            "sampling_hz": 10,
        },
    }

    rows = []

    for sensor_id, cfg in sensors.items():
        sampling_hz = cfg["sampling_hz"]
        periods = duration_seconds * sampling_hz
        freq_ms = int(1000 / sampling_hz)

        timestamps = pd.date_range(
            start=base_start,
            periods=periods,
            freq=f"{freq_ms}ms",
            tz="UTC",
        )

        for ts in timestamps:
            value = cfg["base"] + np.random.normal(0, cfg["noise"])
            rows.append([ts, asset_id, sensor_id, value, cfg["unit"]])

    df = pd.DataFrame(
        rows,
        columns=["ts", "asset_id", "sensor_id", "value", "unit"]
    )

    # Missing values
    temp_ts = df.loc[df["sensor_id"] == "temp_motor", "ts"].sort_values().iloc[10:13]
    df.loc[
        (df["sensor_id"] == "temp_motor") & (df["ts"].isin(temp_ts)),
        "value"
    ] = np.nan

    # Outlier
    pressure_ts = df.loc[df["sensor_id"] == "pressure_inlet", "ts"].sort_values().iloc[30]
    df.loc[
        (df["sensor_id"] == "pressure_inlet") & (df["ts"] == pressure_ts),
        "value"
    ] = 100.0

    # Sensor stuck sequence
    vib_x_ts = df.loc[df["sensor_id"] == "vibration_x", "ts"].sort_values().iloc[50:85]
    df.loc[
        (df["sensor_id"] == "vibration_x") & (df["ts"].isin(vib_x_ts)),
        "value"
    ] = 2.2

    # Duplicate rows
    dup_rows = df[
        (df["sensor_id"] == "temp_motor")
    ].sort_values("ts").iloc[20:22]
    df = pd.concat([df, dup_rows], ignore_index=True)

    return df


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    df = build_sensor_data()

    output_path = RAW_DIR / "raw.parquet"
    df.to_parquet(output_path, index=False)

    print(f"raw dataset written to: {output_path}")
    print(f"rows: {len(df)}")
    print(f"columns: {list(df.columns)}")
    print("\nrows by sensor:")
    print(df["sensor_id"].value_counts())


if __name__ == "__main__":
    main()