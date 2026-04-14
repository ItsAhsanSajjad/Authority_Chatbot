"""
PERA AI — JSON Flattener

Deterministic JSON flattening for nested API response records.
Produces stable, sorted key-value pairs suitable for hashing.

Phase 2 module.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple


class ApiJsonFlattener:
    """Flattens nested JSON objects into stable dot-path keys."""

    def flatten_record(self, record: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """
        Flatten a nested dict into dot-separated path keys.
        Lists are expanded with numeric indices.
        Returns dict with deterministic key ordering.
        """
        items: List[Tuple[str, Any]] = []
        self._flatten_recursive(record, prefix, items)
        return dict(sorted(items))

    def _flatten_recursive(
        self,
        obj: Any,
        prefix: str,
        items: List[Tuple[str, Any]],
    ) -> None:
        if isinstance(obj, dict):
            for key in sorted(obj.keys()):
                new_key = f"{prefix}.{key}" if prefix else key
                self._flatten_recursive(obj[key], new_key, items)
        elif isinstance(obj, list):
            for i, val in enumerate(obj):
                new_key = f"{prefix}.{i}" if prefix else str(i)
                self._flatten_recursive(val, new_key, items)
        else:
            items.append((prefix, obj))

    def flatten_nested_value(self, value: Any, prefix: str = "") -> Dict[str, Any]:
        """Flatten any value (dict, list, or scalar) into path keys."""
        if isinstance(value, dict):
            return self.flatten_record(value, prefix)
        if isinstance(value, list):
            items: List[Tuple[str, Any]] = []
            for i, v in enumerate(value):
                key = f"{prefix}.{i}" if prefix else str(i)
                self._flatten_recursive(v, key, items)
            return dict(sorted(items))
        return {prefix: value} if prefix else {"": value}
