"""
entityresolver.utils.fs

Filesystem utilities for file handling across the pipeline.
"""

from pathlib import Path
from typing import List, Optional
import shutil
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# DIRECTORY HELPERS
# ---------------------------------------------------------
def ensure_dir(path: Path) -> Path:
    """
    Ensure a directory exists.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------
# FILE DISCOVERY
# ---------------------------------------------------------
def list_files(
    directory: Path,
    patterns: Optional[List[str]] = None,
) -> List[Path]:
    """
    List files in a directory matching patterns.
    """

    if not directory.exists():
        return []

    if not patterns:
        return [p for p in directory.iterdir() if p.is_file()]

    files: List[Path] = []
    for pattern in patterns:
        files.extend(directory.glob(pattern))

    return files


def files_exist(
    directory: Path,
    patterns: Optional[List[str]] = None,
    min_files: int = 1,
) -> bool:
    """
    Check if files exist in a directory.
    """
    files = list_files(directory, patterns)
    return len(files) >= min_files


# ---------------------------------------------------------
# FILE OPERATIONS
# ---------------------------------------------------------
def safe_delete(path: Path):
    """
    Delete a file if it exists.
    """
    if path.exists():
        path.unlink(missing_ok=True)
        logger.info("Deleted file: %s", path)


def move_file(src: Path, dest: Path, overwrite: bool = True):
    """
    Move file safely.
    """
    ensure_dir(dest.parent)

    if dest.exists():
        if overwrite:
            safe_delete(dest)
        else:
            raise FileExistsError(f"{dest} already exists")

    shutil.move(str(src), str(dest))
    logger.info("Moved %s → %s", src, dest)


def copy_file(src: Path, dest: Path, overwrite: bool = True):
    """
    Copy file safely.
    """
    ensure_dir(dest.parent)

    if dest.exists():
        if overwrite:
            safe_delete(dest)
        else:
            raise FileExistsError(f"{dest} already exists")

    shutil.copy2(src, dest)
    logger.info("Copied %s → %s", src, dest)


# ---------------------------------------------------------
# METADATA
# ---------------------------------------------------------
def file_size(path: Path) -> int:
    """
    Get file size in bytes.
    """
    return path.stat().st_size


def is_valid_file(path: Path, min_size_bytes: int = 1) -> bool:
    """
    Check if file exists and meets minimum size.
    """
    return path.exists() and file_size(path) >= min_size_bytes