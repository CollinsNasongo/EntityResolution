from pathlib import Path
import json


def load_json(path: Path) -> dict:
    """
    Load JSON file safely.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in config: {path}") from exc


def load_json_from_name(name: str, config_dir: Path) -> dict:
    """
    Load JSON config by name from a directory.
    """
    path = config_dir / f"{name}.json"
    return load_json(path)