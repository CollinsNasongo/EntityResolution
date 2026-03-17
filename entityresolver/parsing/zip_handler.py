"""
entityresolver.parsing.zip_handler
"""

from pathlib import Path
import zipfile


def extract_zip(zip_path: Path, extract_to: Path):
    """
    Extract ZIP and return list of extracted file paths.

    Safe extraction:
    - prevents path traversal
    - ignores directories
    """

    extract_to.mkdir(parents=True, exist_ok=True)

    extracted_files = []

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.infolist():

            # -------------------------------------------------
            # Prevent ZIP SLIP (security)
            # -------------------------------------------------
            member_path = Path(member.filename)

            if ".." in member_path.parts:
                continue  # skip unsafe paths

            # -------------------------------------------------
            # Extract
            # -------------------------------------------------
            extracted_path = zip_ref.extract(member, extract_to)
            p = Path(extracted_path)

            # -------------------------------------------------
            # Keep only files (ignore directories)
            # -------------------------------------------------
            if not p.is_file():
                continue

            # -------------------------------------------------
            # Optional: skip hidden/system files
            # -------------------------------------------------
            if p.name.startswith(".") or p.name.startswith("__"):
                continue

            extracted_files.append(p)

    return extracted_files