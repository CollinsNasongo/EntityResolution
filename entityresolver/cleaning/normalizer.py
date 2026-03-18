from typing import Dict, Any
import pandas as pd


def apply_cleaning(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    df = df.copy()

    global_cfg = config.get("global", {})
    col_cfg = config.get("columns", {})

    # ---------------------------
    # GLOBAL RULES
    # ---------------------------
    for col in df.columns:
        series = df[col].astype("string")

        if global_cfg.get("strip"):
            series = series.str.strip()

        if global_cfg.get("lowercase"):
            series = series.str.lower()

        if "null_values" in global_cfg:
            series = series.replace(global_cfg["null_values"], pd.NA)

        df[col] = series

    # ---------------------------
    # COLUMN RULES
    # ---------------------------
    for col, rules in col_cfg.items():
        if col not in df.columns:
            continue

        series = df[col]

        if rules.get("remove_titles"):
            series = series.str.replace(
                r"^(mr|mrs|ms|dr)\s+", "", regex=True
            )

        if rules.get("alpha_only"):
            series = series.str.replace(r"[^a-z\s]", "", regex=True)

        if rules.get("type") == "numeric":
            series = pd.to_numeric(series, errors="coerce")

        if "extract_regex" in rules:
            series = series.str.extract(rules["extract_regex"])

        df[col] = series

    return df