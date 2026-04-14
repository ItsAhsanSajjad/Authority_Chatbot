"""
PERA AI — API Diff Engine

Compares new normalized records against existing records
to identify added, changed, unchanged, and deleted records.

Phase 2 module.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from api_record_builder import NormalizedApiRecord
from log_config import get_logger

log = get_logger("pera.api.diff")


@dataclass
class ApiDiffResult:
    """Result of a diff operation between new and existing records."""
    source_id: str = ""
    added_records: List[NormalizedApiRecord] = field(default_factory=list)
    changed_records: List[NormalizedApiRecord] = field(default_factory=list)
    unchanged_records: List[NormalizedApiRecord] = field(default_factory=list)
    deleted_record_ids: List[str] = field(default_factory=list)

    @property
    def added_count(self) -> int:
        return len(self.added_records)

    @property
    def changed_count(self) -> int:
        return len(self.changed_records)

    @property
    def unchanged_count(self) -> int:
        return len(self.unchanged_records)

    @property
    def deleted_count(self) -> int:
        return len(self.deleted_record_ids)

    @property
    def has_changes(self) -> bool:
        return bool(self.added_records or self.changed_records or self.deleted_record_ids)

    def summary(self) -> str:
        return (
            f"Added: {self.added_count} | "
            f"Changed: {self.changed_count} | "
            f"Unchanged: {self.unchanged_count} | "
            f"Deleted: {self.deleted_count}"
        )


class ApiDiffEngine:
    """Computes incremental diffs between record sets."""

    def diff_records(
        self,
        source_id: str,
        new_records: List[NormalizedApiRecord],
        existing_records: Dict[str, str],
    ) -> ApiDiffResult:
        """
        Diff new normalized records against existing records.

        Args:
            source_id: The source being diffed.
            new_records: List of newly normalized records.
            existing_records: Dict mapping record_id → record_hash
                              for currently stored records.

        Returns:
            ApiDiffResult with classified records.
        """
        result = ApiDiffResult(source_id=source_id)

        new_ids = set()

        for record in new_records:
            rid = record.record_id
            new_ids.add(rid)

            if rid not in existing_records:
                result.added_records.append(record)
            elif existing_records[rid] != record.record_hash:
                result.changed_records.append(record)
            else:
                result.unchanged_records.append(record)

        # Deleted = in existing but not in new
        for rid in existing_records:
            if rid not in new_ids:
                result.deleted_record_ids.append(rid)

        log.info("Diff for %s: %s", source_id, result.summary())
        return result
