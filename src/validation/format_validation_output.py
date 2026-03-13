from src.utils.sensors import get_known_sensors


def print_sensor_counts(title, counts, rules):
    print(title)

    sensors = get_known_sensors(rules)

    if not counts:
        print("  0")
        return

    for sensor in sensors:
        print(f"  {sensor}: {counts.get(sensor, 0)}")

    extra = set(counts.keys()) - set(sensors)
    for sensor in sorted(extra):
        print(f"  {sensor}: {counts[sensor]}")


def print_missing_sensors(missing_sensors):
    print("missing sensors:")

    if not missing_sensors:
        print("  0")
        return

    for sensor in missing_sensors:
        print(f"  {sensor}")


def print_validation_summary(summary, rules):
    print("\nVALIDATION SUMMARY")

    print_sensor_counts("unknown sensors:", summary["unknown_sensors"], rules)
    print_missing_sensors(summary["missing_sensors"])
    print_sensor_counts("wrong units by sensor:", summary["wrong_units_by_sensor"], rules)
    print_sensor_counts("duplicates by sensor:", summary["duplicates_by_sensor"], rules)
    print_sensor_counts("out of range by sensor:", summary["out_of_range_by_sensor"], rules)
    print_sensor_counts("missing values by sensor:", summary["missing_values_by_sensor"], rules)