"""
entityresolver.sources.http_file_source

HTTP file-based data source.
"""

import logging
from typing import Dict, Iterable, Optional

from entityresolver.connectors.http_connector import http_stream

logger = logging.getLogger(__name__)


class HttpFileSource:
    """
    Data source for streaming files over HTTP.
    """

    def __init__(
        self,
        url: str,
        chunk_size: int = 8192,
        retries: int = 3,
        timeout: int = 30,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Initialize HTTP file source.

        Parameters
        ----------
        url : str
            File URL.
        chunk_size : int, optional
            Size of chunks to stream.
        retries : int, optional
            Number of retry attempts.
        timeout : int, optional
            Request timeout in seconds.
        headers : Dict[str, str], optional
            HTTP headers.
        """
        self.url = url
        self.chunk_size = chunk_size
        self.retries = retries
        self.timeout = timeout
        self.headers = headers or {}

    def fetch(self) -> Iterable[bytes]:
        """
        Stream file content from an HTTP endpoint.

        Returns
        -------
        Iterable[bytes]
            Iterator over file content chunks.
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
        """
        Return string representation of the HTTP file source.
        """
        return f"HttpFileSource(url={self.url})"