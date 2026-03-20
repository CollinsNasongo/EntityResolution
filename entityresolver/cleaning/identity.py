"""
entityresolver.cleaning.transform

Utilities for applying configurable cleaning rules to DataFrames.
"""

from typing import Any, Dict

import pandas as pd


def apply_cleaning(
    df: pd.DataFrame,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """
    Apply configurable cleaning rules to a DataFrame.

    The configuration supports:
    - global rules applied to all columns
    - column-specific rules applied per column

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    config : Dict[str, Any]
        Cleaning configuration dictionary.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame.
    """

    df = df.copy()

    global_cfg = config.get("global", {})
    column_cfg = config.get("columns", {})

    # ---------------------------
    # GLOBAL RULES
    # ---------------------------
    for column in df.columns:
        series = df[column].astype("string")

        if global_cfg.get("strip"):
            series = series.str.strip()

        if global_cfg.get("lowercase"):
            series = series.str.lower()

        if "null_values" in global_cfg:
            series = series.replace(
                global_cfg["null_values"],
                pd.NA,
            )

        df[column] = series

    # ---------------------------
    # COLUMN-SPECIFIC RULES
    # ---------------------------
    for column, rules in column_cfg.items():
        if column not in df.columns:
            continue

        series = df[column]

        if rules.get("remove_titles"):
            series = series.str.replace(
                r"^(mr|mrs|ms|dr)\s+",
                "",
                regex=True,
            )

        if rules.get("alpha_only"):
            series = series.str.replace(
                r"[^a-z\s]",
                "",
                regex=True,
            )

        if rules.get("type") == "numeric":
            series = pd.to_numeric(
                series,
                errors="coerce",
            )

        if "extract_regex" in rules:
            series = series.str.extract(
                rules["extract_regex"],
            )

        df[column] = series

    return df