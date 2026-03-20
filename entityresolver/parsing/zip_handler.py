"""
entityresolver.parsing.zip_handler

Utilities for safely extracting ZIP archives.
"""

from pathlib import Path
from typing import List
import zipfile


def extract_zip(
    zip_path: Path,
    extract_to: Path,
) -> List[Path]:
    """
    Extract a ZIP archive and return extracted file paths.

    Extraction is performed safely:
    - prevents path traversal (ZIP slip)
    - ignores directories
    - skips hidden/system files

    Parameters
    ----------
    zip_path : Path
        Path to the ZIP file.
    extract_to : Path
        Destination directory.

    Returns
    -------
    List[Path]
        List of extracted file paths.
    """
    extract_to.mkdir(parents=True, exist_ok=True)

    extracted_files: List[Path] = []

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.infolist():
            member_path = Path(member.filename)

            if ".." in member_path.parts:
                continue

            extracted_path = zip_ref.extract(member, extract_to)
            path = Path(extracted_path)

            if not path.is_file():
                continue

            if path.name.startswith(".") or path.name.startswith("__"):
                continue

            extracted_files.append(path)

    return extracted_files