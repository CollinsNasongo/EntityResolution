"""
entityresolver.schema.extractor

Extract schema information from a DataFrame.
"""

from typing import Dict, Any, Optional
from pathlib import Path
from datetime import datetime
import hashlib
import json
import pandas as pd


# ---------------------------------------------------------
# CORE SCHEMA EXTRACTION
# ---------------------------------------------------------
def extract_schema(
    df: pd.DataFrame,
    filename: Optional[str] = None,
    source: Optional[str] = None,
    file_path: Optional[Path] = None,
    include_hash: bool = False,
) -> Dict[str, Any]:
    """
    Extract schema metadata from a DataFrame.
    """

    schema: Dict[str, Any] = {
        "filename": filename,
        "source": source,
        "columns": list(df.columns),
        "column_count": len(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "row_count": int(len(df)),
        "generated_at": datetime.utcnow().isoformat(),
    }

    # -----------------------------------------------------
    # File metadata
    # -----------------------------------------------------
    if file_path and Path(file_path).exists():
        p = Path(file_path)
        schema["file_size_bytes"] = p.stat().st_size

        if include_hash:
            schema["file_hash"] = _compute_file_hash(p)

    return schema


# ---------------------------------------------------------
# HASHING (optional)
# ---------------------------------------------------------
def _compute_file_hash(path: Path, algorithm: str = "sha256") -> str:
    hasher = hashlib.new(algorithm)

    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


# ---------------------------------------------------------
# SAVE
# ---------------------------------------------------------
def save_schema(schema: Dict[str, Any], path: Path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)


# ---------------------------------------------------------
# LOAD
# ---------------------------------------------------------
def load_schema(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)