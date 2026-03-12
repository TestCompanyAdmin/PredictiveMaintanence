import pandas as pd

from src.utils.paths import FEATURE_DIR


def time_split(df, time_col="window_start", train_ratio=0.7, val_ratio=0.15):
    df = df.sort_values(time_col).reset_index(drop=True)

    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_df = df.iloc[:train_end].copy()
    val_df = df.iloc[train_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()

    return train_df, val_df, test_df


def main():
    input_path = FEATURE_DIR / "feature.parquet"
    df = pd.read_parquet(input_path)

    train_df, val_df, test_df = time_split(df, time_col="window_start")

    print(f"Input: {input_path}")
    print(f"Train rows: {len(train_df)}")
    print(f"Val rows:   {len(val_df)}")
    print(f"Test rows:  {len(test_df)}")

    print("\nTrain range:")
    print(train_df["window_start"].min(), "->", train_df["window_start"].max())

    print("\nVal range:")
    print(val_df["window_start"].min(), "->", val_df["window_start"].max())

    print("\nTest range:")
    print(test_df["window_start"].min(), "->", test_df["window_start"].max())


if __name__ == "__main__":
    main()