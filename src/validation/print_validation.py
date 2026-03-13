import pandas as pd
import yaml

from src.utils.paths import VALIDATED_DIR, CONFIG_DIR
from src.validation.validate_values import build_validation_summary
from src.validation.format_validation_output import print_validation_summary


def load_validation_rules():
    path = CONFIG_DIR / "validation_rules.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    input_file = VALIDATED_DIR / "validated.parquet"

    print(f"loading validated data: {input_file}")

    df = pd.read_parquet(input_file)
    rules = load_validation_rules()
    summary = build_validation_summary(df, rules)

    print_validation_summary(summary, rules)

    print("\nvalue validation finished")


if __name__ == "__main__":
    main()