"""
entityresolver.sources.http_file_source

HTTP file-based data source.
"""

from typing import Iterable, Optional, Dict
import logging

from entityresolver.connectors.http_connector import http_stream

logger = logging.getLogger(__name__)


class HttpFileSource:
    """
    Data source for downloading files over HTTP as a stream.
    """

    def __init__(
        self,
        url: str,
        chunk_size: int = 8192,
        retries: int = 3,
        timeout: int = 30,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.url = url
        self.chunk_size = chunk_size
        self.retries = retries
        self.timeout = timeout
        self.headers = headers or {}

    def fetch(self) -> Iterable[bytes]:
        """
        Stream file content from HTTP endpoint.

        Returns
        -------
        Iterable[bytes]
        """
        logger.info("Fetching HTTP file: %s", self.url)

        return http_stream(
            url=self.url,
            chunk_size=self.chunk_size,
            retries=self.retries,
            timeout=self.timeout,
            headers=self.headers,
        )

    def __repr__(self) -> str:
        return f"HttpFileSource(url={self.url})"