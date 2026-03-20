"""
entityresolver.sources.api_source

API source for streaming paginated JSON data.
"""

import json
import logging
from typing import Any, Dict, Iterable, Optional

from entityresolver.connectors.http_connector import http_request

logger = logging.getLogger(__name__)


class ApiSource:
    """
    API data source supporting pagination and record extraction.
    """

    def __init__(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        auth: Optional[Any] = None,
        proxies: Optional[Dict[str, str]] = None,
        retries: int = 3,
        timeout: int = 30,
        pagination_key: Optional[str] = None,
        data_key: Optional[str] = None,
    ) -> None:
        """
        Initialize API source.

        Parameters
        ----------
        url : str
            API endpoint.
        method : str, optional
            HTTP method.
        params : Dict[str, Any], optional
            Query parameters.
        headers : Dict[str, str], optional
            HTTP headers.
        auth : Any, optional
            Authentication object.
        proxies : Dict[str, str], optional
            Proxy configuration.
        retries : int, optional
            Number of retry attempts.
        timeout : int, optional
            Request timeout in seconds.
        pagination_key : str, optional
            Key used for pagination token.
        data_key : str, optional
            Key used to extract records from response.
        """
        self.url = url
        self.method = method.upper()
        self.params = params or {}
        self.headers = headers or {}
        self.auth = auth
        self.proxies = proxies
        self.retries = retries
        self.timeout = timeout
        self.pagination_key = pagination_key
        self.data_key = data_key

    def fetch(self) -> Iterable[bytes]:
        """
        Fetch data from the API as a stream of JSON lines.

        Yields
        ------
        Iterable[bytes]
            JSON-encoded records.
        """
        params = dict(self.params)

        while True:
            response_json = http_request(
                url=self.url,
                method=self.method,
                params=params,
                headers=self.headers,
                auth=self.auth,
                proxies=self.proxies,
                retries=self.retries,
                timeout=self.timeout,
            )

            if self.data_key:
                records = response_json.get(self.data_key, [])
            else:
                if isinstance(response_json, list):
                    records = response_json
                elif isinstance(response_json, dict):
                    records = None

                    for key in ("results", "data", "items"):
                        value = response_json.get(key)

                        if isinstance(value, list):
                            records = value
                            logger.debug("Auto-detected key '%s'", key)
                            break

                    if records is None:
                        records = [response_json]
                else:
                    raise ValueError("Unsupported API response format")

            if not isinstance(records, list):
                raise ValueError("API response records are not a list")

            logger.info("Fetched %d records from API", len(records))

            for record in records:
                yield (json.dumps(record) + "\n").encode("utf-8")

            if not self.pagination_key:
                break

            next_token = response_json.get(self.pagination_key)

            if not next_token:
                break

            params[self.pagination_key] = next_token

    def __repr__(self) -> str:
        """
        Return string representation of the API source.
        """
        return f"ApiSource(url={self.url})"