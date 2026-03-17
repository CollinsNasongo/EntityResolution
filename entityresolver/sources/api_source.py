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
        self.url = url
        self.method = method.upper()
        self.params = params or {}
        self.headers = headers or {}
        self.retries = retries
        self.timeout = timeout
        self.pagination_key = pagination_key
        self.data_key = data_key

    # -----------------------------------------------------
    # Internal request logic
    # -----------------------------------------------------
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        attempt = 0

        while attempt <= self.retries:
            try:
                logger.info(
                    "API request to %s (attempt %s)",
                    self.url,
                    attempt + 1,
                )

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

    # -----------------------------------------------------
    # Public fetch
    # -----------------------------------------------------
    def fetch(self) -> Iterable[bytes]:
        """
        Fetch API data and stream as NDJSON.
        """
        params = dict(self.params)

        while True:
            response_json = self._make_request(params)

            # -------------------------------------------------
            # Extract records safely
            # -------------------------------------------------
            if self.data_key:
                records = response_json.get(self.data_key, [])
            else:
                # Auto-detect structure
                if isinstance(response_json, list):
                    records = response_json
                elif isinstance(response_json, dict):
                    # Try common keys
                    for key in ["results", "data", "items"]:
                        if key in response_json and isinstance(response_json[key], list):
                            records = response_json[key]
                            logger.debug(
                                "Auto-detected data key '%s' for API response",
                                key,
                            )
                            break
                    else:
                        # Fallback: wrap entire response as single record
                        records = [response_json]
                else:
                    raise ValueError("Unsupported API response format")

            # -------------------------------------------------
            # Validate records
            # -------------------------------------------------
            if not isinstance(records, list):
                raise ValueError("API response records are not a list")

            logger.info("Fetched %d records from API", len(records))

            # -------------------------------------------------
            # Yield NDJSON
            # -------------------------------------------------
            for record in records:
                yield (json.dumps(record) + "\n").encode("utf-8")

            # -------------------------------------------------
            # Pagination
            # -------------------------------------------------
            if not self.pagination_key:
                break

            next_token = response_json.get(self.pagination_key)

            if not next_token:
                break

            params[self.pagination_key] = next_token

    def __repr__(self) -> str:
        return f"ApiSource(url={self.url})"