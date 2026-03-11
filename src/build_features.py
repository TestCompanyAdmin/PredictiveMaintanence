import pandas as pd
import numpy as np


CLEAN_PATH = "data/clean.parquet"
FEATURES_PATH = "data/features.parquet"


def rms(series: pd.Series) -> float:
    return float(np.sqrt(np.mean(np.square(series))))


def main():
    df = pd.read_parquet(CLEAN_PATH)

    # Nur gültige oder verdächtige Werte verwenden
    df = df[df["quality_flag"] != 2].copy()

    features = (
        df.groupby("sensor_id")["value"]
        .agg(
            mean="mean",
            std="std",
            min="min",
            max="max",
            p95=lambda x: x.quantile(0.95),
        )
        .reset_index()
    )

    features["rms"] = (
        df.groupby("sensor_id")["value"]
        .apply(rms)
        .values
    )

    features.to_parquet(FEATURES_PATH, index=False)

    print("features dataset written to:", FEATURES_PATH)
    print(features)


if __name__ == "__main__":
    main()