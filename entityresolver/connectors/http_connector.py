"""
entityresolver.connectors.http_connector

HTTP connector for streaming remote files.
"""

from typing import Iterable, Optional
import logging
import time
import requests

logger = logging.getLogger(__name__)


def http_stream(
    url: str,
    chunk_size: int = 8192,
    retries: int = 3,
    timeout: int = 30,
    headers: Optional[dict] = None,
) -> Iterable[bytes]:
    """
    Stream content from an HTTP endpoint.

    Parameters
    ----------
    url : str
        Remote file URL.
    chunk_size : int
        Stream chunk size.
    retries : int
        Number of retry attempts.
    timeout : int
        Request timeout in seconds.
    headers : dict, optional
        HTTP headers.

    Returns
    -------
    Iterable[bytes]
        Stream of file chunks.
    """

    attempt = 0

    while attempt <= retries:
        try:
            logger.info("Requesting %s (attempt %s)", url, attempt + 1)

            response = requests.get(
                url,
                stream=True,
                timeout=timeout,
                headers=headers,
            )

            response.raise_for_status()

            return response.iter_content(chunk_size=chunk_size)

        except requests.RequestException as exc:
            attempt += 1
            logger.warning("HTTP request failed: %s", exc)

            if attempt > retries:
                logger.error("All retries exhausted for %s", url)
                raise

            delay = 2 ** attempt
            logger.info("Retrying in %ss", delay)
            time.sleep(delay)