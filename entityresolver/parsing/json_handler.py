"""
entityresolver.parsing.json_handler
"""

import json
import pandas as pd


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # handle common structures
    if isinstance(data, list):
        return pd.DataFrame(data)

    if isinstance(data, dict):
        for key in ["results", "data", "items"]:
            if key in data and isinstance(data[key], list):
                return pd.json_normalize(data[key])

        # fallback
        return pd.json_normalize(data)

    raise ValueError("Unsupported JSON structure")