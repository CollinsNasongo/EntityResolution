"""
entityresolver.ingestion.manifest

JSON-based ingestion manifest with attempt-level tracking.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class Manifest:
    """
    JSON-backed manifest for tracking ingestion attempts and file states.
    """

    def __init__(self, path: Path) -> None:
        """
        Initialize the manifest.

        Parameters
        ----------
        path : Path
            Path to the manifest file.
        """
        self.path = path

        self.data: Dict[str, Any] = {
            "files": {},
            "attempts": {},
        }

        if self.path.exists():
            self._load()

    def _load(self) -> None:
        """
        Load manifest data from disk.
        """
        with open(self.path, "r") as file:
            self.data = json.load(file)

        if "files" not in self.data:
            self.data = {
                "files": self.data,
                "attempts": {},
            }

    def _save(self) -> None:
        """
        Persist manifest data to disk.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.path, "w") as file:
            json.dump(self.data, file, indent=2)

    def get_file(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve file-level metadata.

        Parameters
        ----------
        filename : str
            File name.

        Returns
        -------
        Dict[str, Any], optional
            File record if it exists.
        """
        return self.data["files"].get(filename)

    def is_completed(self, filename: str) -> bool:
        """
        Check if a file has been successfully ingested.

        Parameters
        ----------
        filename : str
            File name.

        Returns
        -------
        bool
            True if file is marked as completed.
        """
        record = self.get_file(filename)
        return bool(record and record.get("status") == "completed")

    def latest_attempt(self, filename: str) -> Optional[str]:
        """
        Get the latest attempt ID for a file.

        Parameters
        ----------
        filename : str
            File name.

        Returns
        -------
        str, optional
            Latest attempt ID.
        """
        record = self.get_file(filename)
        return record.get("latest_attempt") if record else None

    def attempts_for(self, filename: str) -> List[Dict[str, Any]]:
        """
        Retrieve all attempts for a given file.

        Parameters
        ----------
        filename : str
            File name.

        Returns
        -------
        List[Dict[str, Any]]
            List of attempt records.
        """
        return [
            attempt
            for attempt in self.data["attempts"].values()
            if attempt["filename"] == filename
        ]

    def mark_started(self, filename: str, source: str) -> str:
        """
        Mark the start of an ingestion attempt.

        Parameters
        ----------
        filename : str
            File name.
        source : str
            Source identifier.

        Returns
        -------
        str
            Generated attempt ID.
        """
        timestamp = datetime.utcnow().isoformat()
        attempt_id = f"{filename}_{timestamp}_{uuid.uuid4().hex}"

        self.data["attempts"][attempt_id] = {
            "attempt_id": attempt_id,
            "filename": filename,
            "source": source,
            "status": "in_progress",
            "started_at": timestamp,
        }

        self.data["files"][filename] = {
            "status": "in_progress",
            "latest_attempt": attempt_id,
        }

        self._save()
        return attempt_id

    def mark_completed(
        self,
        attempt_id: str,
        size_bytes: int,
        checksum: Optional[str] = None,
    ) -> None:
        """
        Mark an ingestion attempt as completed.

        Parameters
        ----------
        attempt_id : str
            Attempt ID.
        size_bytes : int
            File size in bytes.
        checksum : str, optional
            File checksum.
        """
        attempt = self.data["attempts"][attempt_id]

        attempt.update(
            {
                "status": "completed",
                "completed_at": datetime.utcnow().isoformat(),
                "size_bytes": size_bytes,
                "checksum": checksum,
            }
        )

        filename = attempt["filename"]

        self.data["files"][filename] = {
            "status": "completed",
            "latest_attempt": attempt_id,
            "size_bytes": size_bytes,
            "checksum": checksum,
        }

        self._save()

    def mark_failed(self, attempt_id: str, error: str) -> None:
        """
        Mark an ingestion attempt as failed.

        Parameters
        ----------
        attempt_id : str
            Attempt ID.
        error : str
            Error message.
        """
        attempt = self.data["attempts"][attempt_id]

        attempt.update(
            {
                "status": "failed",
                "error": error,
                "failed_at": datetime.utcnow().isoformat(),
            }
        )

        filename = attempt["filename"]

        self.data["files"][filename]["status"] = "failed"

        self._save()