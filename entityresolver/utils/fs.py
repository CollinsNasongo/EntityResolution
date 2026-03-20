"""
entityresolver.utils.fs

Filesystem utilities for file handling across the pipeline.
"""

import logging
import shutil
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def ensure_dir(path: Path) -> Path:
    """
    Ensure a directory exists.

    Parameters
    ----------
    path : Path
        Directory path.

    Returns
    -------
    Path
        The created or existing directory path.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_files(
    directory: Path,
    patterns: Optional[List[str]] = None,
) -> List[Path]:
    """
    List files in a directory matching patterns.

    Parameters
    ----------
    directory : Path
        Directory to search.
    patterns : List[str], optional
        Glob patterns to filter files.

    Returns
    -------
    List[Path]
        List of matching file paths.
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

    Parameters
    ----------
    directory : Path
        Directory to check.
    patterns : List[str], optional
        Glob patterns to filter files.
    min_files : int, optional
        Minimum number of files required.

    Returns
    -------
    bool
        True if at least `min_files` exist, otherwise False.
    """
    files = list_files(directory, patterns)
    return len(files) >= min_files


def safe_delete(path: Path) -> None:
    """
    Delete a file if it exists.

    Parameters
    ----------
    path : Path
        File path to delete.
    """
    if path.exists():
        path.unlink(missing_ok=True)
        logger.info("Deleted file: %s", path)


def move_file(
    src: Path,
    dest: Path,
    overwrite: bool = True,
) -> None:
    """
    Move a file safely.

    Parameters
    ----------
    src : Path
        Source file path.
    dest : Path
        Destination file path.
    overwrite : bool, optional
        Whether to overwrite if destination exists.

    Raises
    ------
    FileExistsError
        If destination exists and overwrite is False.
    """
    ensure_dir(dest.parent)

    if dest.exists():
        if overwrite:
            safe_delete(dest)
        else:
            raise FileExistsError(f"{dest} already exists")

    shutil.move(str(src), str(dest))
    logger.info("Moved %s -> %s", src, dest)


def copy_file(
    src: Path,
    dest: Path,
    overwrite: bool = True,
) -> None:
    """
    Copy a file safely.

    Parameters
    ----------
    src : Path
        Source file path.
    dest : Path
        Destination file path.
    overwrite : bool, optional
        Whether to overwrite if destination exists.

    Raises
    ------
    FileExistsError
        If destination exists and overwrite is False.
    """
    ensure_dir(dest.parent)

    if dest.exists():
        if overwrite:
            safe_delete(dest)
        else:
            raise FileExistsError(f"{dest} already exists")

    shutil.copy2(src, dest)
    logger.info("Copied %s -> %s", src, dest)


def file_size(path: Path) -> int:
    """
    Get file size in bytes.

    Parameters
    ----------
    path : Path
        File path.

    Returns
    -------
    int
        File size in bytes.
    """
    return path.stat().st_size


def is_valid_file(
    path: Path,
    min_size_bytes: int = 1,
) -> bool:
    """
    Check if file exists and meets minimum size.

    Parameters
    ----------
    path : Path
        File path.
    min_size_bytes : int, optional
        Minimum file size in bytes.

    Returns
    -------
    bool
        True if file exists and meets size requirement.
    """
    return path.exists() and file_size(path) >= min_size_bytes