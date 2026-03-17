"""
entityresolver.ingestion.manifest

JSON-based ingestion manifest with attempt-level tracking.
"""

from pathlib import Path
import json, uuid
from datetime import datetime
from typing import Optional, Dict, Any, List


class Manifest:
    def __init__(self, path: Path):
        self.path = path

        # structure:
        # {
        #   "files": { filename: {...} },
        #   "attempts": { attempt_id: {...} }
        # }
        self.data: Dict[str, Any] = {"files": {}, "attempts": {}}

        if self.path.exists():
            self._load()

    # -----------------------------------------------------
    # Internal
    # -----------------------------------------------------
    def _load(self):
        with open(self.path, "r") as f:
            self.data = json.load(f)

        # backward compatibility (old flat format)
        if "files" not in self.data:
            self.data = {"files": self.data, "attempts": {}}

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2)

    # -----------------------------------------------------
    # Public API
    # -----------------------------------------------------
    def get_file(self, filename: str) -> Optional[dict]:
        return self.data["files"].get(filename)

    def is_completed(self, filename: str) -> bool:
        record = self.get_file(filename)
        return record and record.get("status") == "completed"

    def latest_attempt(self, filename: str) -> Optional[str]:
        record = self.get_file(filename)
        return record.get("latest_attempt") if record else None

    def attempts_for(self, filename: str) -> List[dict]:
        return [
            a for a in self.data["attempts"].values()
            if a["filename"] == filename
        ]

    # -----------------------------------------------------
    # State transitions
    # -----------------------------------------------------
    def mark_started(self, filename: str, source: str) -> str:
        ts = datetime.utcnow().isoformat()
        attempt_id = f"{filename}_{ts}_{uuid.uuid4().hex}"

        # record attempt
        self.data["attempts"][attempt_id] = {
            "attempt_id": attempt_id,
            "filename": filename,
            "source": source,
            "status": "in_progress",
            "started_at": ts,
        }

        # update file-level state
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
    ):
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

    def mark_failed(self, attempt_id: str, error: str):
        attempt = self.data["attempts"][attempt_id]

        attempt.update(
            {
                "status": "failed",
                "error": error,
                "failed_at": datetime.utcnow().isoformat(),
            }
        )

        filename = attempt["filename"]

        # update file-level status (last known state)
        self.data["files"][filename]["status"] = "failed"

        self._save()