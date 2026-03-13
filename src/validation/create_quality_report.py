def build_quality_report(df, validation_summary):
    return {
        "row_count": int(len(df)),
        "quality_flag_counts": {
            str(k): int(v)
            for k, v in df["quality_flag"].value_counts(dropna=False).to_dict().items()
        },
        "quality_reason_counts": {
            str(k): int(v)
            for k, v in df["quality_reason"].value_counts(dropna=False).to_dict().items()
        },
        "usable_count": int(df["is_usable"].sum()),
        "unusable_count": int((~df["is_usable"]).sum()),
        "sensor_counts": {
            str(k): int(v)
            for k, v in df["sensor_id"].value_counts(dropna=False).to_dict().items()
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
        "validation_summary": validation_summary,
    }