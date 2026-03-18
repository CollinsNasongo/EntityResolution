"""
entityresolver.cleaning.identity

Utilities for adding unique identifiers to datasets.
"""

from typing import Optional
import pandas as pd
import uuid


def add_unique_id(
    df: pd.DataFrame,
    column_name: str = "unique_id",
    prefix: Optional[str] = None,
) -> pd.DataFrame:
    """
    Add a unique ID column to a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame
    column_name : str
        Name of the ID column
    prefix : str, optional
        Optional prefix (e.g., dataset name)

    Returns
    -------
    pd.DataFrame
    """

    df = df.copy()

    if column_name in df.columns:
        raise ValueError(f"Column already exists: {column_name}")

    def generate_id():
        uid = uuid.uuid4().hex
        return f"{prefix}_{uid}" if prefix else uid

    df.insert(
        0,
        column_name,
        [generate_id() for _ in range(len(df))]
    )

    return df