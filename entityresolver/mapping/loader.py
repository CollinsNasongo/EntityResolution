"""
entityresolver.mapping.loader

Load user-defined mapping configurations from JSON files.
"""

from pathlib import Path
from typing import Dict
import logging

from entityresolver.utils.config import load_json, load_json_from_name

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
    """

    logger.info("Loading mapping config: %s", path)

    config = load_json(path)

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
# LOAD BY NAME (CONFIG ROOT SUPPORT)
# ---------------------------------------------------------
def load_mapping_from_name(
    name: str,
    config_dir: Path = Path("config/mappings"),
) -> Dict[str, str]:
    """
    Load mapping by name.

    Example:
        load_mapping_from_name("north_carolina")
    """

    logger.info("Loading mapping config by name: %s", name)

    config = load_json_from_name(name, config_dir)

    # Reuse validation logic
    if "mapping" not in config:
        raise ValueError("Mapping config missing required key: 'mapping'")

    mapping = config["mapping"]

    if not isinstance(mapping, dict):
        raise ValueError("'mapping' must be a dictionary")

    return mapping