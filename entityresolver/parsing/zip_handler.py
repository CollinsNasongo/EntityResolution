"""
entityresolver.parsing.zip_handler
"""

from pathlib import Path
import zipfile


def extract_zip(path: Path, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    extracted_files = []

    with zipfile.ZipFile(path, "r") as zip_ref:
        for member in zip_ref.namelist():
            extracted_path = zip_ref.extract(member, output_dir)

            final_path = output_dir / Path(member).name
            Path(extracted_path).rename(final_path)

            extracted_files.append(final_path)

    return extracted_files