import yaml

from src.utils.paths import CONFIG_DIR


def load_validation_rules():
    path = CONFIG_DIR / "validation_rules.yaml"
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)