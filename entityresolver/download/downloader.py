"""
entityresolver.download.downloader
Robust utilities for saving downloaded data streams.
"""

from pathlib import Path
from typing import Iterable, Optional
import hashlib
import logging
import time

from tqdm import tqdm

logger = logging.getLogger(__name__)


def save_stream(
    stream: Iterable[bytes],
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
    """

    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".tmp")

    attempt = 0

    try:
        hashlib.new(algorithm)
    except ValueError:
        raise ValueError(f"Invalid hash algorithm: {algorithm}")

    while attempt <= retries:
        try:
            logger.info("Saving file to %s (attempt %s)", destination, attempt + 1)

            hasher = hashlib.new(algorithm)

            with open(temp_path, "wb") as f:
                with tqdm(
                    total=total_bytes,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=destination.name,
                ) as bar:
                    for chunk in stream:
                        if chunk:
                            f.write(chunk)
                            hasher.update(chunk)
                            bar.update(len(chunk))

            if checksum:
                file_hash = hasher.hexdigest().lower()

                if file_hash != checksum.lower():
                    raise ValueError(
                        f"Checksum mismatch: expected {checksum}, got {file_hash}"
                    )

            temp_path.replace(destination)

            logger.info("File saved successfully to %s", destination)

            return destination

        except Exception as exc:
            attempt += 1

            logger.warning("Download attempt %s failed: %s", attempt, exc)

            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

            if attempt > retries:
                logger.error("All retries exhausted for %s", destination)
                raise

            delay = retry_delay * (2 ** (attempt - 1))

            logger.info("Retrying in %.1fs...", delay)

            time.sleep(delay)