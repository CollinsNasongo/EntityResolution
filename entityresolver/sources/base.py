"""
entityresolver.sources.base

Base interface for all data sources.
"""

from typing import Iterable, Protocol, runtime_checkable


@runtime_checkable
class DataSource(Protocol):
    """
    A generic data source that produces a stream of bytes.

    All sources (HTTP, API, S3, DB, etc.) must implement this interface.
    """

    def fetch(self) -> Iterable[bytes]:
        """
        Fetch data as a stream of bytes.

        Returns
        -------
        Iterable[bytes]
            Stream of binary chunks.
        """
        ...