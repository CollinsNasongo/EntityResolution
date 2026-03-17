"""
entityresolver.parsing.loader

Unified interface for loading files into DataFrames.
"""

from pathlib import Path
import pandas as pd

from entityresolver.parsing.detector import detect_file_type
from entityresolver.parsing.csv_handler import load_csv
from entityresolver.parsing.json_handler import load_json
from entityresolver.parsing.zip_handler import extract_zip


def load_dataframe(path: Path, temp_dir: Path = None):
    """
    Load any supported file into a pandas DataFrame.
    """

    file_type = detect_file_type(path)

    # -----------------------------------------------------
    # ZIP
    # -----------------------------------------------------
    if file_type == "zip":
        if not temp_dir:
            raise ValueError("temp_dir required for zip extraction")

        files = extract_zip(path, temp_dir)

        # load first supported file (simple strategy)
        for f in files:
            try:
                return load_dataframe(f, temp_dir)
            except Exception:
                continue

        raise ValueError("No valid files found inside ZIP")

    # -----------------------------------------------------
    # CSV / TXT
    # -----------------------------------------------------
    if file_type == "csv":
        return load_csv(path)

    # -----------------------------------------------------
    # JSON
    # -----------------------------------------------------
    if file_type == "json":
        return load_json(path)

    # -----------------------------------------------------
    # Excel
    # -----------------------------------------------------
    if file_type == "excel":
        return pd.read_excel(path, dtype=str)

    # -----------------------------------------------------
    # Parquet
    # -----------------------------------------------------
    if file_type == "parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported file type: {file_type}")