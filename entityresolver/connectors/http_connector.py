"""
entityresolver.connectors.http_connector

HTTP connector for streaming remote files and API requests.
"""

from typing import Iterable, Optional, Dict, Any
import logging
import time
import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# INTERNAL RETRY WRAPPER (SHARED LOGIC)
# ---------------------------------------------------------
def _request_with_retries(
    method: str,
    url: str,
    retries: int,
    timeout: int,
    headers: Optional[Dict[str, str]] = None,
    auth: Optional[Any] = None,
    proxies: Optional[Dict[str, str]] = None,
    **kwargs,
) -> requests.Response:
    attempt = 0

    while attempt <= retries:
        try:
            logger.info("Requesting %s (attempt %s)", url, attempt + 1)

            response = requests.request(
                method=method,
                url=url,
                timeout=timeout,
                headers=headers,
                auth=auth,
                proxies=proxies,
                **kwargs,
            )

            response.raise_for_status()
            return response

        except requests.RequestException as exc:
            attempt += 1
            logger.warning("HTTP request failed: %s", exc)

            if attempt > retries:
                logger.error("All retries exhausted for %s", url)
                raise

            delay = 2 ** attempt
            logger.info("Retrying in %ss", delay)
            time.sleep(delay)


# ---------------------------------------------------------
# STREAMING (FILES)
# ---------------------------------------------------------
def http_stream(
    url: str,
    chunk_size: int = 8192,
    retries: int = 3,
    timeout: int = 30,
    headers: Optional[Dict[str, str]] = None,
    auth: Optional[Any] = None,
    proxies: Optional[Dict[str, str]] = None,
) -> Iterable[bytes]:
    """
    Stream content from an HTTP endpoint.
    """

    response = _request_with_retries(
        method="GET",
        url=url,
        retries=retries,
        timeout=timeout,
        headers=headers,
        auth=auth,
        proxies=proxies,
        stream=True,
    )

    return response.iter_content(chunk_size=chunk_size)


# ---------------------------------------------------------
# JSON / API REQUEST
# ---------------------------------------------------------
def http_request(
    url: str,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    auth: Optional[Any] = None,
    proxies: Optional[Dict[str, str]] = None,
    retries: int = 3,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Make an HTTP request and return JSON response.
    """

    response = _request_with_retries(
        method=method,
        url=url,
        retries=retries,
        timeout=timeout,
        params=params,
        headers=headers,
        auth=auth,
        proxies=proxies,
    )

    try:
        return response.json()
    except ValueError:
        raise ValueError("Response is not valid JSON")