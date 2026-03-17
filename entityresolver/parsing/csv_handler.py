"""
entityresolver.parsing.detector

File type detection utilities.
"""

from pathlib import Path


def detect_file_type(path: Path) -> str:
    suffix = path.suffix.lower()

    if suffix == ".zip":
        return "zip"

    if suffix in [".csv", ".txt", ".tsv"]:
        return "csv"

    if suffix == ".json":
        return "json"

    if suffix in [".xlsx", ".xls"]:
        return "excel"

    if suffix == ".parquet":
        return "parquet"

    raise ValueError(f"Unsupported file type: {suffix}")