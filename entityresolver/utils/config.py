"""
entityresolver.utils.config

Utilities for loading JSON configuration files.
"""

import json
from pathlib import Path
from typing import Any, Dict


def load_json(path: Path) -> Dict[str, Any]:
    """
    Load a JSON file safely.

    Parameters
    ----------
    path : Path
        Path to the JSON file.

    Returns
    -------
    Dict[str, Any]
        Parsed JSON content.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file contains invalid JSON.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in config: {path}") from exc


def load_json_from_name(
    name: str,
    config_dir: Path,
) -> Dict[str, Any]:
    """
    Load a JSON config by name from a directory.

    Parameters
    ----------
    name : str
        Configuration name (without extension).
    config_dir : Path
        Directory containing configuration files.

    Returns
    -------
    Dict[str, Any]
        Parsed JSON content.
    """
    path = config_dir / f"{name}.json"
    return load_json(path)