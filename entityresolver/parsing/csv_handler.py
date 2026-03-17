"""
entityresolver.parsing.csv_handler
Utilities for loading messy CSV/TXT files with automatic delimiter
and encoding detection.
"""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_DELIMITERS = ["\t", "|", ","]
_ENCODINGS = ["utf-8", "latin-1"]


def detect_data_start(path: Path, max_lines: int = 50) -> int:
    """
    Detect where actual tabular data starts by scanning for lines
    with a high density of common delimiters.

    Parameters
    ----------
    path : Path
        File to scan.
    max_lines : int
        Maximum number of lines to scan before giving up.

    Returns
    -------
    int
        Line index where tabular data appears to begin.
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for i in range(max_lines):
            line = f.readline()
            if line.count("\t") > 5 or line.count("|") > 5:
                return i
    return 0


def load_csv(path: Path, sample_rows: int = 10000) -> pd.DataFrame:
    """
    Load a messy CSV/TXT file with automatic delimiter detection,
    encoding fallback, header skipping, and row sampling.

    Tries tab, pipe, and comma delimiters against utf-8 and latin-1
    encodings. On a delimiter mismatch the encoding loop is skipped
    immediately. On an encoding error the next encoding is tried
    before moving to the next delimiter.

    Parameters
    ----------
    path : Path
        Path to the CSV or TXT file.
    sample_rows : int
        Maximum number of rows to load.

    Returns
    -------
    pd.DataFrame
        Parsed DataFrame with more than one column.

    Raises
    ------
    ValueError
        If no delimiter/encoding combination produces a valid parse.
    """
    skiprows = detect_data_start(path)
    logger.debug("Detected data start at line %s for %s", skiprows, path.name)

    for sep in _DELIMITERS:
        for encoding in _ENCODINGS:
            try:
                df = pd.read_csv(
                    path,
                    sep=sep,
                    engine="python",
                    on_bad_lines="skip",
                    nrows=sample_rows,
                    skiprows=skiprows,
                    encoding=encoding,
                )

                if len(df.columns) > 1:
                    logger.info(
                        "Parsed %s using sep=%r encoding=%s (%s columns, %s rows)",
                        path.name, sep, encoding, len(df.columns), len(df),
                    )
                    return df

            except UnicodeDecodeError:
                logger.debug(
                    "Encoding %s failed for %s with sep=%r — trying next encoding",
                    encoding, path.name, sep,
                )
                continue

            except Exception as exc:
                logger.debug(
                    "Delimiter %r failed for %s: %s — trying next delimiter",
                    sep, path.name, exc,
                )
                break  # wrong delimiter — no point retrying with a different encoding

    raise ValueError(
        f"Could not parse {path.name} — no delimiter/encoding combination succeeded. "
        f"Tried delimiters={_DELIMITERS}, encodings={_ENCODINGS}."
    )