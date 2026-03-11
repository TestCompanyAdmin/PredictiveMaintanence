import numpy as np
import pandas as pd


def build_sensor_data() -> pd.DataFrame:
    np.random.seed(42)

    timestamps = pd.date_range(
        start="2025-01-01 10:00:00",
        periods=120,
        freq="1s",
        tz="UTC"
    )

    sensor_ids = [
        "pump01_temp_motor",
        "pump01_pressure_inlet",
        "pump01_current_l1",
    ]

    rows = []

    for ts in timestamps:
        temp_value = 60 + np.random.normal(0, 1.5)
        pressure_value = 10 + np.random.normal(0, 0.3)
        current_value = 25 + np.random.normal(0, 1.0)

        rows.append([ts, sensor_ids[0], temp_value])
        rows.append([ts, sensor_ids[1], pressure_value])
        rows.append([ts, sensor_ids[2], current_value])

    df = pd.DataFrame(rows, columns=["ts", "sensor_id", "value"])

    # Missing values
    df.loc[
        (df["sensor_id"] == "pump01_temp_motor") &
        (df["ts"].isin(timestamps[10:13])),
        "value"
    ] = np.nan

    # Outlier
    df.loc[
        (df["sensor_id"] == "pump01_pressure_inlet") &
        (df["ts"] == timestamps[30]),
        "value"
    ] = 100.0

    # Sensor stuck sequence
    stuck_mask = (
        (df["sensor_id"] == "pump01_current_l1") &
        (df["ts"].isin(timestamps[50:85]))
    )
    df.loc[stuck_mask, "value"] = 22.0

    # Duplicate rows
    duplicate_rows = df[
        (df["sensor_id"] == "pump01_temp_motor") &
        (df["ts"].isin([timestamps[20], timestamps[21]]))
    ]
    df = pd.concat([df, duplicate_rows], ignore_index=True)

    return df


def main() -> None:
    df = build_sensor_data()

    output_path = "data/raw.parquet"
    df.to_parquet(output_path, index=False)

    print(f"raw dataset written to: {output_path}")
    print(f"rows: {len(df)}")
    print(f"columns: {list(df.columns)}")


if __name__ == "__main__":
    main()
