"""
entityresolver.schema.diff

Utilities for comparing schema changes.
"""

from typing import Dict, Any


def diff_schemas(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare two schemas and return differences.
    """

    old_cols = set(old.get("columns", []))
    new_cols = set(new.get("columns", []))

    added = new_cols - old_cols
    removed = old_cols - new_cols

    # dtype changes
    old_dtypes = old.get("dtypes", {})
    new_dtypes = new.get("dtypes", {})

    dtype_changes = {}

    for col in old_cols & new_cols:
        if old_dtypes.get(col) != new_dtypes.get(col):
            dtype_changes[col] = {
                "old": old_dtypes.get(col),
                "new": new_dtypes.get(col),
            }

    return {
        "added_columns": sorted(list(added)),
        "removed_columns": sorted(list(removed)),
        "dtype_changes": dtype_changes,
        "has_changes": bool(added or removed or dtype_changes),
    }