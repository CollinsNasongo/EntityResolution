from typing import Iterable, Optional, Dict, Any
import logging
import json

from entityresolver.connectors.http_connector import http_request

logger = logging.getLogger(__name__)


class ApiSource:
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
    ):
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
        params = dict(self.params)

        while True:
            response_json = http_request(
                url=self.url,
                method=self.method,
                params=params,
                headers=self.headers,
                auth=self.auth,              # ✅ added
                proxies=self.proxies,        # ✅ added
                retries=self.retries,
                timeout=self.timeout,
            )

            # -------------------------------------------------
            # Extract records
            # -------------------------------------------------
            if self.data_key:
                records = response_json.get(self.data_key, [])
            else:
                if isinstance(response_json, list):
                    records = response_json
                elif isinstance(response_json, dict):
                    for key in ["results", "data", "items"]:
                        if key in response_json and isinstance(response_json[key], list):
                            records = response_json[key]
                            logger.debug("Auto-detected key '%s'", key)
                            break
                    else:
                        records = [response_json]
                else:
                    raise ValueError("Unsupported API response format")

            if not isinstance(records, list):
                raise ValueError("API response records are not a list")

            logger.info("Fetched %d records from API", len(records))

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