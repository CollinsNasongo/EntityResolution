"""
entityresolver.mapping.mapper

Apply schema mappings to transform DataFrames into a standard structure.
"""

from typing import Dict

import pandas as pd


def apply_mapping(
    df: pd.DataFrame,
    mapping: Dict[str, str],
) -> pd.DataFrame:
    """
    Apply column mapping.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    mapping : Dict[str, str]
        Mapping of {target_column: source_column}.

    Returns
    -------
    pd.DataFrame
        Transformed DataFrame with renamed and selected columns.

    Raises
    ------
    ValueError
        If mapping refers to missing columns.
    """
    missing = [
        source
        for source in mapping.values()
        if source not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Mapping refers to missing columns: {missing}",
        )

    renamed_df = df.rename(
        columns={source: target for target, source in mapping.items()},
    )

    return renamed_df[list(mapping.keys())]


def infer_identity_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """
    Create an identity mapping (no transformation).

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    Dict[str, str]
        Mapping where each column maps to itself.
    """
    return {column: column for column in df.columns}


def validate_mapping(mapping: Dict[str, str]) -> None:
    """
    Validate mapping structure.

    Parameters
    ----------
    mapping : Dict[str, str]
        Mapping of {target_column: source_column}.

    Raises
    ------
    ValueError
        If mapping is invalid.
    """
    if not isinstance(mapping, dict):
        raise ValueError("Mapping must be a dictionary")

    if not mapping:
        raise ValueError("Mapping cannot be empty")

    for key, value in mapping.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError(
                "Mapping keys and values must be strings",
            )