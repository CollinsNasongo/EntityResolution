"""
entityresolver.mapping.loader

Load user-defined mapping configurations from JSON files.
"""

from pathlib import Path
from typing import Dict
import json
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# CORE LOADER
# ---------------------------------------------------------
def load_mapping(path: Path) -> Dict[str, str]:
    """
    Load mapping configuration from JSON file.

    Expected format:
    {
        "mapping": {
            "target_column": "source_column"
        }
    }

    Parameters
    ----------
    path : Path
        Path to mapping JSON file

    Returns
    -------
    dict
        Mapping dictionary {target: source}
    """

    if not path.exists():
        raise FileNotFoundError(f"Mapping file not found: {path}")

    logger.info("Loading mapping config: %s", path)

    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)

    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in mapping file: {path}") from exc

    # -----------------------------------------------------
    # Validate structure
    # -----------------------------------------------------
    if not isinstance(config, dict):
        raise ValueError("Mapping config must be a JSON object")

    if "mapping" not in config:
        raise ValueError("Mapping config missing required key: 'mapping'")

    mapping = config["mapping"]

    if not isinstance(mapping, dict):
        raise ValueError("'mapping' must be a dictionary")

    # Validate keys/values
    for target, source in mapping.items():
        if not isinstance(target, str) or not isinstance(source, str):
            raise ValueError(
                f"Invalid mapping entry: {target} -> {source} (must be strings)"
            )

    logger.info("Loaded mapping with %d fields", len(mapping))

    return mapping


# ---------------------------------------------------------
# OPTIONAL: LOAD BY NAME (CONFIG ROOT SUPPORT)
# ---------------------------------------------------------
def load_mapping_from_name(
    name: str,
    config_dir: Path = Path("config/mappings"),
) -> Dict[str, str]:
    """
    Load mapping by name.

    Example:
        load_mapping_from_name("north_carolina")

    Will load:
        config/mappings/north_carolina.json
    """

    path = config_dir / f"{name}.json"

    return load_mapping(path)