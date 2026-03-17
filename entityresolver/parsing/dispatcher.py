"""
entityresolver.parsing.dispatcher
Route files to appropriate parser.
"""

from pathlib import Path
import pandas as pd

from entityresolver.parsing.detector import detect_file_type
from entityresolver.parsing.csv_handler import load_csv
from entityresolver.parsing.json_handler import load_json
from entityresolver.parsing.zip_handler import extract_zip


def parse_file(path: Path, working_dir: Path) -> list[pd.DataFrame]:
    """
    Parse a file into one or more DataFrames.

    Returns
    -------
    list[pd.DataFrame]
        Always returns a list of DataFrames:
        - CSV/JSON → [df]
        - ZIP → recursively parsed DataFrames
    """

    file_type = detect_file_type(path)

    if file_type == "csv":
        return [load_csv(path)]

    elif file_type == "json":
        return [load_json(path)]

    elif file_type == "zip":
        extracted_files = extract_zip(path, working_dir)

        dfs: list[pd.DataFrame] = []

        for file in extracted_files:
            dfs.extend(parse_file(file, working_dir))

        return dfs

    else:
        raise ValueError(f"Unsupported file type: {path}")