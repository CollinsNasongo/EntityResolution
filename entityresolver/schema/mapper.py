"""
entityresolver.schema.mapper

Apply schema mappings to transform DataFrames into a standard structure.
"""

from typing import Dict
import pandas as pd


def apply_mapping(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Apply column mapping.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame
    mapping : dict
        {target_column: source_column}

    Returns
    -------
    pd.DataFrame
        Transformed DataFrame with renamed columns
    """

    # -----------------------------------------------------
    # Validate mapping
    # -----------------------------------------------------
    missing = [src for src in mapping.values() if src not in df.columns]

    if missing:
        raise ValueError(f"Mapping refers to missing columns: {missing}")

    # -----------------------------------------------------
    # Rename columns
    # -----------------------------------------------------
    renamed_df = df.rename(columns={v: k for k, v in mapping.items()})

    # -----------------------------------------------------
    # Select only mapped columns in correct order
    # -----------------------------------------------------
    return renamed_df[list(mapping.keys())]


def infer_identity_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """
    Create identity mapping (no transformation).

    Useful as fallback.
    """
    return {col: col for col in df.columns}


def validate_mapping(mapping: Dict[str, str]):
    """
    Basic validation for mapping structure.
    """
    if not isinstance(mapping, dict):
        raise ValueError("Mapping must be a dictionary")

    if not mapping:
        raise ValueError("Mapping cannot be empty")

    for k, v in mapping.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError("Mapping keys and values must be strings")