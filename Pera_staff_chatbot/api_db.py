"""
PERA AI — API Ingestion Database Foundation

SQLite-based metadata storage for API ingestion sources, sync runs,
and record tracking. Provides migration scaffolding for Phase 1.

Usage:
    from api_db import ApiDatabase
    db = ApiDatabase("data/api_ingestion.db")
    db.migrate()
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple

from log_config import get_logger

log = get_logger("pera.api.db")

# ── Migration Definitions ─────────────────────────────────────
# Each migration is (version, description, sql_statements)
_MIGRATIONS: List[Tuple[int, str, List[str]]] = [
    (1, "Create knowledge_sources table", [
        """
        CREATE TABLE IF NOT EXISTS knowledge_sources (
            source_id       TEXT PRIMARY KEY,
            source_type     TEXT NOT NULL DEFAULT 'api',
            display_name    TEXT NOT NULL DEFAULT '',
            status          TEXT NOT NULL DEFAULT 'active',
            status_message  TEXT DEFAULT '',
            config_hash     TEXT DEFAULT '',
            config_path     TEXT DEFAULT '',
            first_seen_at   REAL NOT NULL,
            last_updated_at REAL NOT NULL,
            last_sync_at    REAL DEFAULT NULL,
            record_count    INTEGER DEFAULT 0,
            metadata_json   TEXT DEFAULT '{}'
        )
        """,
    ]),
    (2, "Create api_source_configs table", [
        """
        CREATE TABLE IF NOT EXISTS api_source_configs (
            source_id       TEXT PRIMARY KEY,
            config_yaml     TEXT NOT NULL,
            config_hash     TEXT NOT NULL,
            parsed_at       REAL NOT NULL,
            url             TEXT DEFAULT '',
            auth_type       TEXT DEFAULT 'none',
            sync_interval   INTEGER DEFAULT 30,
            enabled         INTEGER DEFAULT 1,
            FOREIGN KEY (source_id) REFERENCES knowledge_sources(source_id)
        )
        """,
    ]),
    (3, "Create api_sync_runs table", [
        """
        CREATE TABLE IF NOT EXISTS api_sync_runs (
            run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id       TEXT NOT NULL,
            started_at      REAL NOT NULL,
            completed_at    REAL DEFAULT NULL,
            status          TEXT NOT NULL DEFAULT 'running',
            records_fetched INTEGER DEFAULT 0,
            records_new     INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            records_removed INTEGER DEFAULT 0,
            error_message   TEXT DEFAULT '',
            run_metadata    TEXT DEFAULT '{}',
            FOREIGN KEY (source_id) REFERENCES knowledge_sources(source_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_sync_runs_source ON api_sync_runs(source_id)",
        "CREATE INDEX IF NOT EXISTS idx_sync_runs_started ON api_sync_runs(started_at)",
    ]),
    (4, "Create api_records table", [
        """
        CREATE TABLE IF NOT EXISTS api_records (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id       TEXT NOT NULL,
            record_id       TEXT NOT NULL,
            record_type     TEXT DEFAULT '',
            content_hash    TEXT NOT NULL,
            raw_json        TEXT NOT NULL,
            normalized_text TEXT DEFAULT '',
            first_seen_at   REAL NOT NULL,
            last_updated_at REAL NOT NULL,
            is_active       INTEGER DEFAULT 1,
            FOREIGN KEY (source_id) REFERENCES knowledge_sources(source_id),
            UNIQUE(source_id, record_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_api_records_source ON api_records(source_id)",
        "CREATE INDEX IF NOT EXISTS idx_api_records_active ON api_records(is_active)",
    ]),
    (5, "Create knowledge_chunks table", [
        """
        CREATE TABLE IF NOT EXISTS knowledge_chunks (
            chunk_id        TEXT PRIMARY KEY,
            source_id       TEXT NOT NULL,
            record_id       TEXT DEFAULT '',
            chunk_index     INTEGER DEFAULT 0,
            chunk_text      TEXT NOT NULL,
            content_hash    TEXT NOT NULL,
            created_at      REAL NOT NULL,
            is_active       INTEGER DEFAULT 1,
            metadata_json   TEXT DEFAULT '{}',
            FOREIGN KEY (source_id) REFERENCES knowledge_sources(source_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_chunks_source ON knowledge_chunks(source_id)",
        "CREATE INDEX IF NOT EXISTS idx_chunks_record ON knowledge_chunks(source_id, record_id)",
    ]),
    (6, "Create vector_index_map table", [
        """
        CREATE TABLE IF NOT EXISTS vector_index_map (
            chunk_id        TEXT PRIMARY KEY,
            source_id       TEXT NOT NULL,
            faiss_index_id  INTEGER DEFAULT NULL,
            index_version   TEXT DEFAULT '',
            indexed_at      REAL DEFAULT NULL,
            is_active       INTEGER DEFAULT 1,
            FOREIGN KEY (chunk_id) REFERENCES knowledge_chunks(chunk_id),
            FOREIGN KEY (source_id) REFERENCES knowledge_sources(source_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_vector_map_source ON vector_index_map(source_id)",
    ]),
    (7, "Create schema_migrations tracking table", [
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            description TEXT NOT NULL,
            applied_at  REAL NOT NULL
        )
        """,
    ]),
]


class ApiDatabaseMigrator:
    """Runs schema migrations for the API ingestion database."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _ensure_dir(self) -> None:
        d = os.path.dirname(os.path.abspath(self.db_path))
        if d:
            os.makedirs(d, exist_ok=True)

    def get_applied_versions(self, conn: sqlite3.Connection) -> set:
        """Return set of already-applied migration versions."""
        try:
            cursor = conn.execute("SELECT version FROM schema_migrations")
            return {row[0] for row in cursor.fetchall()}
        except sqlite3.OperationalError:
            # schema_migrations table doesn't exist yet
            return set()

    def migrate(self) -> int:
        """Run all pending migrations. Returns count of newly applied migrations."""
        self._ensure_dir()
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")

        try:
            applied = self.get_applied_versions(conn)
            count = 0

            for version, description, statements in _MIGRATIONS:
                if version in applied:
                    continue

                log.info("Applying migration v%d: %s", version, description)
                for sql in statements:
                    conn.execute(sql)

                # Record the migration (skip for v7 itself on first run)
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO schema_migrations (version, description, applied_at) "
                        "VALUES (?, ?, ?)",
                        (version, description, time.time()),
                    )
                except sqlite3.OperationalError:
                    pass  # schema_migrations table created in this batch

                count += 1

            conn.commit()
            if count > 0:
                log.info("Applied %d migration(s) to %s", count, self.db_path)
            return count
        finally:
            conn.close()


class ApiDatabase:
    """
    Lightweight database access layer for API ingestion metadata.
    Uses SQLite with WAL mode for concurrent reads.
    """

    def __init__(self, db_path: str = "data/api_ingestion.db"):
        self.db_path = db_path
        self._migrator = ApiDatabaseMigrator(db_path)

    def migrate(self) -> int:
        """Run pending migrations."""
        return self._migrator.migrate()

    @contextmanager
    def connection(self):
        """Context manager for a database connection."""
        d = os.path.dirname(os.path.abspath(self.db_path))
        if d:
            os.makedirs(d, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Knowledge Sources CRUD ────────────────────────────────

    def upsert_source(
        self,
        source_id: str,
        *,
        source_type: str = "api",
        display_name: str = "",
        status: str = "active",
        status_message: str = "",
        config_hash: str = "",
        config_path: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Insert or update a knowledge source."""
        now = time.time()
        meta_json = json.dumps(metadata or {})
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO knowledge_sources
                    (source_id, source_type, display_name, status, status_message,
                     config_hash, config_path, first_seen_at, last_updated_at, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    status = excluded.status,
                    status_message = excluded.status_message,
                    config_hash = excluded.config_hash,
                    config_path = excluded.config_path,
                    last_updated_at = excluded.last_updated_at,
                    metadata_json = excluded.metadata_json
                """,
                (source_id, source_type, display_name, status, status_message,
                 config_hash, config_path, now, now, meta_json),
            )

    def get_source(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get a knowledge source by ID."""
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM knowledge_sources WHERE source_id = ?",
                (source_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_sources_by_status(self, *statuses: str) -> List[Dict[str, Any]]:
        """Get all sources matching given statuses."""
        placeholders = ", ".join("?" * len(statuses))
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM knowledge_sources WHERE status IN ({placeholders})",
                statuses,
            ).fetchall()
            return [dict(r) for r in rows]

    def get_all_sources(self) -> List[Dict[str, Any]]:
        """Get all knowledge sources."""
        with self.connection() as conn:
            rows = conn.execute("SELECT * FROM knowledge_sources").fetchall()
            return [dict(r) for r in rows]

    def set_source_status(
        self, source_id: str, status: str, message: str = "",
        record_count: int | None = None,
    ) -> None:
        """Update the status of a knowledge source.
        When status is 'synced', also updates last_sync_at and record_count.
        """
        now = time.time()
        with self.connection() as conn:
            if status == "synced":
                # Count active records for this source
                if record_count is None:
                    row = conn.execute(
                        "SELECT COUNT(*) FROM api_records "
                        "WHERE source_id = ? AND is_active = 1",
                        (source_id,),
                    ).fetchone()
                    record_count = row[0] if row else 0
                conn.execute(
                    "UPDATE knowledge_sources SET status = ?, status_message = ?, "
                    "last_updated_at = ?, last_sync_at = ?, record_count = ? "
                    "WHERE source_id = ?",
                    (status, message, now, now, record_count, source_id),
                )
            else:
                conn.execute(
                    "UPDATE knowledge_sources SET status = ?, status_message = ?, "
                    "last_updated_at = ? WHERE source_id = ?",
                    (status, message, now, source_id),
                )

    # ── API Source Configs CRUD ────────────────────────────────

    def upsert_source_config(
        self,
        source_id: str,
        config_yaml: str,
        config_hash: str,
        url: str = "",
        auth_type: str = "none",
        sync_interval: int = 30,
        enabled: bool = True,
    ) -> None:
        """Store or update a parsed API source configuration."""
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO api_source_configs
                    (source_id, config_yaml, config_hash, parsed_at, url,
                     auth_type, sync_interval, enabled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    config_yaml = excluded.config_yaml,
                    config_hash = excluded.config_hash,
                    parsed_at = excluded.parsed_at,
                    url = excluded.url,
                    auth_type = excluded.auth_type,
                    sync_interval = excluded.sync_interval,
                    enabled = excluded.enabled
                """,
                (source_id, config_yaml, config_hash, time.time(), url,
                 auth_type, sync_interval, 1 if enabled else 0),
            )

    def get_source_config(self, source_id: str) -> Optional[Dict[str, Any]]:
        """Get a source config by ID."""
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM api_source_configs WHERE source_id = ?",
                (source_id,),
            ).fetchone()
            return dict(row) if row else None
