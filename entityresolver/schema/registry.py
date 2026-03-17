"""
entityresolver.schema.registry

Schema registry for tracking schema history over time.
"""

from typing import Dict, Any, List
from pathlib import Path
import json


class SchemaRegistry:
    """
    Stores and retrieves schema history for a dataset.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if self.path.exists():
            with open(self.path, "r", encoding="utf-8") as f:
                self._data = json.load(f)
        else:
            self._data = {
                "schemas": []
            }

    # -----------------------------------------------------
    # INTERNAL SAVE
    # -----------------------------------------------------
    def _save(self):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)

    # -----------------------------------------------------
    # ADD NEW SCHEMA
    # -----------------------------------------------------
    def register(self, schema: Dict[str, Any]):
        """
        Store a new schema snapshot.
        """
        self._data["schemas"].append(schema)
        self._save()

    # -----------------------------------------------------
    # GET LATEST SCHEMA
    # -----------------------------------------------------
    def latest(self) -> Dict[str, Any]:
        if not self._data["schemas"]:
            raise ValueError("No schemas registered yet")

        return self._data["schemas"][-1]

    # -----------------------------------------------------
    # GET ALL SCHEMAS
    # -----------------------------------------------------
    def history(self) -> List[Dict[str, Any]]:
        return self._data["schemas"]

    # -----------------------------------------------------
    # CHECK IF EMPTY
    # -----------------------------------------------------
    def is_empty(self) -> bool:
        return len(self._data["schemas"]) == 0