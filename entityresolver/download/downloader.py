"""
entityresolver.download.downloader

Robust utilities for saving downloaded data streams.
"""

from pathlib import Path
from typing import Iterable, Optional, Callable
import hashlib
import logging
import time

from tqdm import tqdm

logger = logging.getLogger(__name__)


def save_stream(
    stream_factory: Callable[[], Iterable[bytes]],
    destination: Path,
    retries: int = 3,
    retry_delay: float = 2.0,
    checksum: Optional[str] = None,
    algorithm: str = "sha256",
    total_bytes: Optional[int] = None,
) -> Path:
    """
    Save a binary stream to a file with retries, temp file safety,
    progress bar, and optional checksum validation.

    Parameters
    ----------
    stream_factory : Callable[[], Iterable[bytes]]
        Factory that returns a fresh stream on each retry.
    destination : Path
        Final file path.
    retries : int
        Number of retry attempts.
    retry_delay : float
        Initial delay between retries (exponential backoff).
    checksum : str, optional
        Expected checksum for validation.
    algorithm : str
        Hash algorithm (default: sha256).
    total_bytes : int, optional
        Expected total size (for progress bar).

    Returns
    -------
    Path
        Path to saved file.
    """

    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".tmp")

    # Validate hash algorithm early
    try:
        hashlib.new(algorithm)
    except ValueError:
        raise ValueError(f"Invalid hash algorithm: {algorithm}")

    for attempt in range(retries + 1):
        try:
            logger.info(
                "Saving file to %s (attempt %s)",
                destination,
                attempt + 1,
            )

            hasher = hashlib.new(algorithm)

            with open(temp_path, "wb") as f:
                with tqdm(
                    total=total_bytes,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=destination.name,
                    disable=total_bytes is None,  # cleaner logs in Docker/Airflow
                ) as bar:

                    stream = stream_factory()  # ✅ fresh stream per retry
                    bytes_written = 0

                    for chunk in stream:
                        if not chunk:
                            continue

                        f.write(chunk)
                        hasher.update(chunk)
                        bar.update(len(chunk))
                        bytes_written += len(chunk)

            # -------------------------------------------------
            # Validate non-empty file
            # -------------------------------------------------
            if bytes_written == 0:
                raise ValueError("Empty stream received")

            # -------------------------------------------------
            # Checksum validation
            # -------------------------------------------------
            if checksum:
                file_hash = hasher.hexdigest().lower()

                if file_hash != checksum.lower():
                    raise ValueError(
                        f"Checksum mismatch: expected {checksum}, got {file_hash}"
                    )

            # -------------------------------------------------
            # Atomic move
            # -------------------------------------------------
            temp_path.replace(destination)

            size = destination.stat().st_size

            logger.info(
                "File saved successfully to %s (%d bytes)",
                destination,
                size,
            )

            return destination

        except Exception as exc:
            logger.warning(
                "Download attempt %s failed: %s",
                attempt + 1,
                exc,
            )

            # Cleanup temp file
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

            if attempt >= retries:
                logger.error("All retries exhausted for %s", destination)
                raise

            delay = retry_delay * (2 ** attempt)

            logger.info("Retrying in %.1fs...", delay)
            time.sleep(delay)