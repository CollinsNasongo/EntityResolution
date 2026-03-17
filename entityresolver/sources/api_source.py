"""
entityresolver.sources.api_source

API-based data source (JSON → streaming NDJSON).
"""

from typing import Iterable, Optional, Dict, Any
import logging
import time
import json
import requests

logger = logging.getLogger(__name__)


class ApiSource:
    """
    Data source for REST APIs.

    Converts JSON responses into a stream of newline-delimited JSON (NDJSON).
    """

    def __init__(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: int = 3,
        timeout: int = 30,
        pagination_key: Optional[str] = None,
        data_key: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        url : str
            API endpoint.
        method : str
            HTTP method (GET, POST, etc.).
        params : dict
            Query parameters.
        headers : dict
            HTTP headers.
        retries : int
            Retry attempts.
        timeout : int
            Request timeout.
        pagination_key : str, optional
            Key for next page token (if paginated API).
        data_key : str, optional
            Key where actual records live in response.
        """
        self.url = url
        self.method = method.upper()
        self.params = params or {}
        self.headers = headers or {}
        self.retries = retries
        self.timeout = timeout
        self.pagination_key = pagination_key
        self.data_key = data_key

    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Internal request handler with retries."""
        attempt = 0

        while attempt <= self.retries:
            try:
                logger.info("API request to %s (attempt %s)", self.url, attempt + 1)

                response = requests.request(
                    method=self.method,
                    url=self.url,
                    params=params,
                    headers=self.headers,
                    timeout=self.timeout,
                )

                response.raise_for_status()
                return response.json()

            except requests.RequestException as exc:
                attempt += 1
                logger.warning("API request failed: %s", exc)

                if attempt > self.retries:
                    logger.error("All retries exhausted for %s", self.url)
                    raise

                delay = 2 ** attempt
                logger.info("Retrying in %ss...", delay)
                time.sleep(delay)

    def fetch(self) -> Iterable[bytes]:
        """
        Fetch API data and stream as NDJSON.

        Yields
        ------
        bytes
            JSON records encoded as UTF-8 lines.
        """
        params = dict(self.params)

        while True:
            response_json = self._make_request(params)

            # Extract records
            if self.data_key:
                records = response_json.get(self.data_key, [])
            else:
                records = response_json

            if not isinstance(records, list):
                raise ValueError("API response is not a list of records")

            logger.info("Fetched %d records from API", len(records))

            for record in records:
                yield (json.dumps(record) + "\n").encode("utf-8")

            # Handle pagination
            if not self.pagination_key:
                break

            next_token = response_json.get(self.pagination_key)

            if not next_token:
                break

            params[self.pagination_key] = next_token

    def __repr__(self) -> str:
        return f"ApiSource(url={self.url})"