from pathlib import Path

from entityresolver.utils.config import load_json_from_name


def load_cleaning_rules(name: str, config_dir=Path("config/cleaning")):
    config = load_json_from_name(name, config_dir)

    # Domain validation
    if "columns" not in config:
        raise ValueError("Cleaning config missing 'columns' section")

    return config