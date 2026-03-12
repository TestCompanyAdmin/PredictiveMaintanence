import json
import pandas as pd

from src.utils.paths import VALIDATED_DIR, DATA_QUALITY_REPORT_DIR


def main():
    input_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated dataset: {input_file}")

    df = pd.read_parquet(input_file)

    report = {
        "row_count": int(len(df)),
        "quality_flag_counts": {
            str(k): int(v) for k, v in df["quality_flag"].value_counts(dropna=False).to_dict().items()
        },
        "quality_reason_counts": {
            str(k): int(v) for k, v in df["quality_reason"].value_counts(dropna=False).to_dict().items()
        },
        "usable_count": int(df["is_usable"].sum()),
        "unusable_count": int((~df["is_usable"]).sum()),
                "sensor_counts": {
            str(k): int(v) for k, v in df["sensor_id"].value_counts(dropna=False).to_dict().items()
        },
        "sensor_quality_flag_counts": {
            sensor: {
                str(flag): int(count)
                for flag, count in group["quality_flag"].value_counts(dropna=False).to_dict().items()
            }
            for sensor, group in df.groupby("sensor_id")
        },
        "sensor_quality_reason_counts": {
            sensor: {
                str(reason): int(count)
                for reason, count in group["quality_reason"].value_counts(dropna=False).to_dict().items()
            }
            for sensor, group in df.groupby("sensor_id")
        },
        "missing_by_sensor": {
            sensor: int(((group["quality_reason"] == "missing_value")).sum())
            for sensor, group in df.groupby("sensor_id")
        },
        "out_of_range_by_sensor": {
            sensor: int(((group["quality_reason"] == "out_of_range")).sum())
            for sensor, group in df.groupby("sensor_id")
        },
        "duplicate_by_sensor": {
            sensor: int(((group["quality_reason"] == "duplicate_row")).sum())
            for sensor, group in df.groupby("sensor_id")
        },
        "wrong_unit_by_sensor": {
            sensor: int(((group["quality_reason"] == "wrong_unit")).sum())
            for sensor, group in df.groupby("sensor_id")
        },
    }

    DATA_QUALITY_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    output_file = DATA_QUALITY_REPORT_DIR / "quality_report.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"quality report written to: {output_file}")


if __name__ == "__main__":
    main()