"""
entityresolver.parsing.csv_handler
"""

import pandas as pd
from pathlib import Path


def load_csv(path: Path) -> pd.DataFrame:
    """
    Load CSV / TXT / TSV file with automatic type inference.
    """

    try:
        return pd.read_csv(path, low_memory=False)
    except Exception:
        # fallback for tab-separated
        return pd.read_csv(path, sep="\t", low_memory=False)