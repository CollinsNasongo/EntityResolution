"""
entityresolver.cleaning.config

Utilities for loading and validating cleaning configurations.
"""

from pathlib import Path
from typing import Any, Dict

from entityresolver.utils.config import load_json_from_name


def load_cleaning_rules(
    name: str,
    config_dir: Path = Path("config/cleaning"),
) -> Dict[str, Any]:
    """
    Load and validate a cleaning configuration by name.

    Parameters
    ----------
    name : str
        Name of the configuration file (without extension).
    config_dir : Path, optional
        Directory containing cleaning configs.

    Returns
    -------
    Dict[str, Any]
        Loaded and validated cleaning configuration.

    Raises
    ------
    ValueError
        If required sections are missing from the config.
    """

    config = load_json_from_name(name, config_dir)

    # Validate required structure
    if "columns" not in config:
        raise ValueError(
            "Cleaning config missing required 'columns' section",
        )

    return config