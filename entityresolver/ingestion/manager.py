"""
entityresolver.ingestion.manager

Unified ingestion manager with manifest tracking.
"""

from pathlib import Path
from typing import Optional, Iterable
import logging
import time

from entityresolver.download.downloader import save_stream
from entityresolver.ingestion.manifest import Manifest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def is_valid_file(path: Path, min_size: int = 1) -> bool:
    return path.exists() and path.stat().st_size >= min_size


def generate_filename(source) -> str:
    if hasattr(source, "name"):
        return source.name()

    return f"{source.__class__.__name__}_{int(time.time())}.dat"


# ---------------------------------------------------------
# Core ingestion
# ---------------------------------------------------------

def ingest(
    source,
    destination: Optional[Path],
    manifest: Manifest,
    overwrite: bool = False,
    min_size_bytes: int = 1,
) -> Path:

    if destination is None:
        destination = Path(generate_filename(source))

    filename = destination.name

    # -----------------------------------------------------
    # Idempotency check
    # -----------------------------------------------------
    if not overwrite and manifest.is_completed(filename):
        if is_valid_file(destination, min_size_bytes):
            logger.info("Skipping already ingested file: %s", filename)
            return destination

    attempt_id = manifest.mark_started(filename, str(source))

    try:
        # -------------------------------------------------
        # Stream factory (retry-safe)
        # -------------------------------------------------
        def stream_factory() -> Iterable[bytes]:
            return source.fetch()

        # -------------------------------------------------
        # Download
        # -------------------------------------------------
        start = time.time()

        saved_path = save_stream(
            stream_factory=stream_factory,
            destination=destination,
        )

        size = saved_path.stat().st_size

        # -------------------------------------------------
        # Validation
        # -------------------------------------------------
        if size < min_size_bytes:
            raise ValueError(
                f"Downloaded file too small ({size} bytes): {filename}"
            )

        duration = time.time() - start

        logger.info(
            "Downloaded %s (%d bytes in %.2fs)",
            filename,
            size,
            duration,
        )

        # -------------------------------------------------
        # Mark success
        # -------------------------------------------------
        manifest.mark_completed(
            attempt_id=attempt_id,
            size_bytes=size,
            checksum=None,
        )

        return saved_path

    except Exception as e:
        logger.error("Ingestion failed for %s: %s", filename, e)

        manifest.mark_failed(attempt_id, str(e))
        raise