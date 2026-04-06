"""
PERA AI — API Snapshot Store

Persistence layer for normalized API records and sync run tracking.
Uses api_db.py tables: api_records and api_sync_runs.

Phase 2 module.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from api_db import ApiDatabase
from api_record_builder import NormalizedApiRecord
from log_config import get_logger

log = get_logger("pera.api.snapshot")


class ApiSnapshotStore:
    """
    Persists normalized records and sync run metadata.
    Uses the Phase 1 database foundation.
    """

    def __init__(self, db: ApiDatabase):
        self.db = db

    # ── Record Operations ─────────────────────────────────────

    def get_existing_records(self, source_id: str) -> Dict[str, str]:
        """
        Get existing active records for a source.
        Returns dict mapping record_id → content_hash.
        """
        with self.db.connection() as conn:
            rows = conn.execute(
                "SELECT record_id, content_hash FROM api_records "
                "WHERE source_id = ? AND is_active = 1",
                (source_id,),
            ).fetchall()
            return {row["record_id"]: row["content_hash"] for row in rows}

    def upsert_records(
        self, source_id: str, records: List[NormalizedApiRecord]
    ) -> int:
        """
        Insert or update normalized records. Returns count of upserted records.
        """
        now = time.time()
        count = 0
        with self.db.connection() as conn:
            for rec in records:
                conn.execute(
                    """
                    INSERT INTO api_records
                        (source_id, record_id, record_type, content_hash,
                         raw_json, normalized_text, first_seen_at,
                         last_updated_at, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(source_id, record_id) DO UPDATE SET
                        record_type = excluded.record_type,
                        content_hash = excluded.content_hash,
                        raw_json = excluded.raw_json,
                        normalized_text = excluded.normalized_text,
                        last_updated_at = excluded.last_updated_at,
                        is_active = 1
                    """,
                    (
                        source_id,
                        rec.record_id,
                        rec.record_type,
                        rec.record_hash,
                        rec.canonical_json,
                        rec.canonical_text,
                        now,
                        now,
                    ),
                )
                count += 1
        log.info("Upserted %d records for source %s", count, source_id)

        # Analytics write-through (PostgreSQL)
        self._write_analytics_records(source_id, records)

        return count

    def _write_analytics_records(
        self, source_id: str, records: list
    ) -> None:
        """Write records to PostgreSQL analytics store (non-fatal)."""
        try:
            from settings import get_settings
            s = get_settings()
            if not getattr(s, "ANALYTICS_WRITE_ENABLED", False):
                return

            from analytics_store import get_analytics_store
            store = get_analytics_store()
            if not store:
                return

            record_dicts = []
            for rec in records:
                record_dicts.append({
                    "record_id": getattr(rec, "record_id", ""),
                    "record_type": getattr(rec, "record_type", ""),
                    "content_hash": getattr(rec, "record_hash", ""),
                    "raw_json": getattr(rec, "canonical_json", "{}"),
                    "normalized_text": getattr(rec, "canonical_text", ""),
                })

            if record_dicts:
                store.upsert_records(source_id, record_dicts)
        except Exception as e:
            log.warning(
                "Analytics snapshot write failed for %s (non-fatal): %s",
                source_id, e,
            )

    def soft_delete_records(
        self, source_id: str, record_ids: List[str]
    ) -> int:
        """
        Soft-delete records by setting is_active = 0.
        Returns count of deactivated records.
        """
        if not record_ids:
            return 0
        now = time.time()
        count = 0
        with self.db.connection() as conn:
            for rid in record_ids:
                conn.execute(
                    "UPDATE api_records SET is_active = 0, last_updated_at = ? "
                    "WHERE source_id = ? AND record_id = ?",
                    (now, source_id, rid),
                )
                count += 1
        log.info("Soft-deleted %d records for source %s", count, source_id)
        return count

    def delete_all_source_records(self, source_id: str) -> int:
        """
        Hard-delete all records for a source. Use with caution.
        Returns count of deleted records.
        """
        with self.db.connection() as conn:
            cursor = conn.execute(
                "DELETE FROM api_records WHERE source_id = ?",
                (source_id,),
            )
            count = cursor.rowcount
        log.info("Deleted %d records for source %s", count, source_id)
        return count

    def get_record(
        self, source_id: str, record_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get a single record by source_id and record_id."""
        with self.db.connection() as conn:
            row = conn.execute(
                "SELECT * FROM api_records "
                "WHERE source_id = ? AND record_id = ?",
                (source_id, record_id),
            ).fetchone()
            return dict(row) if row else None

    # ── Sync Run Operations ───────────────────────────────────

    def create_sync_run(
        self,
        source_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Create a new sync run record. Returns the run_id.
        """
        now = time.time()
        meta_json = json.dumps(metadata or {})
        with self.db.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO api_sync_runs
                    (source_id, started_at, status, run_metadata)
                VALUES (?, ?, 'running', ?)
                """,
                (source_id, now, meta_json),
            )
            run_id = cursor.lastrowid
        log.info("Created sync run %d for source %s", run_id, source_id)
        return run_id

    def finish_sync_run(
        self,
        run_id: int,
        *,
        status: str = "completed",
        records_fetched: int = 0,
        records_new: int = 0,
        records_updated: int = 0,
        records_removed: int = 0,
        error_message: str = "",
    ) -> None:
        """Update a sync run with completion data."""
        now = time.time()
        with self.db.connection() as conn:
            conn.execute(
                """
                UPDATE api_sync_runs SET
                    completed_at = ?,
                    status = ?,
                    records_fetched = ?,
                    records_new = ?,
                    records_updated = ?,
                    records_removed = ?,
                    error_message = ?
                WHERE run_id = ?
                """,
                (now, status, records_fetched, records_new,
                 records_updated, records_removed, error_message, run_id),
            )
        log.info("Finished sync run %d: %s", run_id, status)

    def get_sync_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        """Get a sync run by ID."""
        with self.db.connection() as conn:
            row = conn.execute(
                "SELECT * FROM api_sync_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_latest_sync_run(
        self, source_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent sync run for a source."""
        with self.db.connection() as conn:
            row = conn.execute(
                "SELECT * FROM api_sync_runs WHERE source_id = ? "
                "ORDER BY started_at DESC LIMIT 1",
                (source_id,),
            ).fetchone()
            return dict(row) if row else None
