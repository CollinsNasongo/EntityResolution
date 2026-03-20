"""
entityresolver.ingestion.manager

Unified ingestion manager with manifest tracking.
"""

import logging
import time
from pathlib import Path
from typing import Iterable, Optional

from entityresolver.download.downloader import save_stream
from entityresolver.ingestion.manifest import Manifest

logger = logging.getLogger(__name__)


def is_valid_file(
    path: Path,
    min_size: int = 1,
) -> bool:
    """
    Check if a file exists and meets a minimum size.

    Parameters
    ----------
    path : Path
        File path.
    min_size : int, optional
        Minimum file size in bytes.

    Returns
    -------
    bool
        True if file exists and meets size requirement.
    """
    return path.exists() and path.stat().st_size >= min_size


def generate_filename(source: object) -> str:
    """
    Generate a filename for a source.

    Parameters
    ----------
    source : object
        Source object.

    Returns
    -------
    str
        Generated filename.
    """
    if hasattr(source, "name"):
        return source.name()

    return f"{source.__class__.__name__}_{int(time.time())}.dat"


def ingest(
    source: object,
    destination: Optional[Path],
    manifest: Manifest,
    overwrite: bool = False,
    min_size_bytes: int = 1,
) -> Path:
    """
    Ingest data from a source into a destination with manifest tracking.

    Parameters
    ----------
    source : object
        Data source with a `fetch()` method returning an iterable of bytes.
    destination : Path, optional
        Destination file path.
    manifest : Manifest
        Manifest tracker for ingestion state.
    overwrite : bool, optional
        Whether to overwrite existing files.
    min_size_bytes : int, optional
        Minimum valid file size.

    Returns
    -------
    Path
        Path to the saved file.

    Raises
    ------
    Exception
        If ingestion fails.
    """
    if destination is None:
        destination = Path(generate_filename(source))

    filename = destination.name

    if not overwrite and manifest.is_completed(filename):
        if is_valid_file(destination, min_size_bytes):
            logger.info("Skipping already ingested file: %s", filename)
            return destination

    attempt_id = manifest.mark_started(filename, str(source))

    try:

        def stream_factory() -> Iterable[bytes]:
            return source.fetch()

        start = time.time()

        saved_path = save_stream(
            stream_factory=stream_factory,
            destination=destination,
        )

        size = saved_path.stat().st_size

        if size < min_size_bytes:
            raise ValueError(
                f"Downloaded file too small ({size} bytes): {filename}",
            )

        duration = time.time() - start

        logger.info(
            "Downloaded %s (%d bytes in %.2fs)",
            filename,
            size,
            duration,
        )

        manifest.mark_completed(
            attempt_id=attempt_id,
            size_bytes=size,
            checksum=None,
        )

        return saved_path

    except Exception as exc:
        logger.error("Ingestion failed for %s: %s", filename, exc)

        manifest.mark_failed(attempt_id, str(exc))
        raise