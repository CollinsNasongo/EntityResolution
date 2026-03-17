"""
entityresolver.parsing.json_handler
"""

import pandas as pd
from pathlib import Path
import json


def load_json(path: Path) -> pd.DataFrame:
    """
    Load JSON file into DataFrame.
    Supports:
    - list of records
    - NDJSON
    """

    try:
        # Try standard JSON (list/dict)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return pd.DataFrame(data)

        if isinstance(data, dict):
            return pd.json_normalize(data)

    except Exception:
        # Fallback: NDJSON
        return pd.read_json(path, lines=True)

    raise ValueError("Unsupported JSON structure")