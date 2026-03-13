import json
import pandas as pd
import yaml

from src.utils.paths import VALIDATED_DIR, DATA_QUALITY_REPORT_DIR, CONFIG_DIR
from src.validation.validate_values import build_validation_summary
from src.validation.create_quality_report import build_quality_report


def load_validation_rules():
    path = CONFIG_DIR / "validation_rules.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    input_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated dataset: {input_file}")

    df = pd.read_parquet(input_file)
    rules = load_validation_rules()
    validation_summary = build_validation_summary(df, rules)
    report = build_quality_report(df, validation_summary)

    DATA_QUALITY_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    output_file = DATA_QUALITY_REPORT_DIR / "quality_report.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"quality report written to: {output_file}")


if __name__ == "__main__":
    main()