"""
entityresolver.parsing.parser

Unified interface for loading files into DataFrames.
"""

from pathlib import Path
from typing import Optional

import pandas as pd

from entityresolver.parsing.csv_handler import load_csv
from entityresolver.parsing.detector import detect_file_type
from entityresolver.parsing.json_handler import load_json
from entityresolver.parsing.zip_handler import extract_zip


def load_dataframe(
    path: Path,
    temp_dir: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Load a supported file into a pandas DataFrame.

    Parameters
    ----------
    path : Path
        File path.
    temp_dir : Path, optional
        Temporary directory for ZIP extraction.

    Returns
    -------
    pd.DataFrame
        Loaded DataFrame.

    Raises
    ------
    ValueError
        If file type is unsupported or ZIP handling fails.
    """
    file_type = detect_file_type(path)

    if file_type == "zip":
        if temp_dir is None:
            raise ValueError("temp_dir required for zip extraction")

        files = extract_zip(path, temp_dir)

        valid_files = []

        for file_path in files:
            try:
                detected_type = detect_file_type(file_path)

                if detected_type in {
                    "csv",
                    "json",
                    "excel",
                    "parquet",
                }:
                    valid_files.append(file_path)

            except Exception:
                continue

        if not valid_files:
            raise ValueError("No supported files found inside ZIP")

        priority = {
            "csv": 1,
            "json": 2,
            "excel": 3,
            "parquet": 4,
        }

        valid_files.sort(
            key=lambda file_path: priority.get(
                detect_file_type(file_path),
                99,
            ),
        )

        dataframes = [
            load_dataframe(file_path, temp_dir)
            for file_path in valid_files
        ]

        return pd.concat(dataframes, ignore_index=True)

    if file_type == "csv":
        return load_csv(path)

    if file_type == "json":
        return load_json(path)

    if file_type == "excel":
        return pd.read_excel(path, dtype=str)

    if file_type == "parquet":
        return pd.read_parquet(path)

    raise ValueError(f"Unsupported file type: {file_type}")