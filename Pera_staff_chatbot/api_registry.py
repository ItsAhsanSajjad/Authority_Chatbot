"""
PERA AI — API Source Registry

Manages the lifecycle metadata of API data sources. Scans config files,
registers/updates sources in the database, and tracks status transitions.

Phase 1: metadata registration only. No fetch/normalize/index logic.

Usage:
    from api_registry import ApiSourceRegistry
    registry = ApiSourceRegistry(db)
    registry.scan_source_configs("assets/apis")
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from api_config_models import (
    ApiConfigError,
    ApiSourceConfig,
    load_api_source_config,
)
from api_db import ApiDatabase
from api_source_utils import (
    scan_api_config_dir,
    stable_file_hash,
)
from log_config import get_logger

log = get_logger("pera.api.registry")


# ── Valid status transitions ──────────────────────────────────
VALID_STATUSES = {
    "active",
    "disabled",
    "error",
    "pending_sync",
    "syncing",
    "synced",
    "pending_removal",
    "removed",
}


@dataclass
class ApiSourceRegistryEntry:
    """In-memory representation of a registered API source."""
    source_id: str
    display_name: str = ""
    status: str = "active"
    status_message: str = ""
    config_hash: str = ""
    config_path: str = ""
    config: Optional[ApiSourceConfig] = None
    first_seen_at: float = 0.0
    last_updated_at: float = 0.0
    last_sync_at: Optional[float] = None
    record_count: int = 0

    @classmethod
    def from_db_row(cls, row: Dict) -> "ApiSourceRegistryEntry":
        return cls(
            source_id=row["source_id"],
            display_name=row.get("display_name", ""),
            status=row.get("status", "active"),
            status_message=row.get("status_message", ""),
            config_hash=row.get("config_hash", ""),
            config_path=row.get("config_path", ""),
            first_seen_at=row.get("first_seen_at", 0),
            last_updated_at=row.get("last_updated_at", 0),
            last_sync_at=row.get("last_sync_at"),
            record_count=row.get("record_count", 0),
        )


class ApiSourceRegistry:
    """
    Registry for API data sources. Tracks source metadata,
    config versions, and status transitions.
    """

    def __init__(self, db: ApiDatabase):
        self.db = db
        self._cache: Dict[str, ApiSourceRegistryEntry] = {}

    def scan_source_configs(self, source_dir: str) -> List[ApiSourceConfig]:
        """
        Scan a directory for API config files, load and validate each.
        Returns list of successfully parsed configs.
        Does NOT register them — use register_or_update_source for that.
        """
        paths = scan_api_config_dir(source_dir)
        configs: List[ApiSourceConfig] = []

        for path in paths:
            try:
                config = load_api_source_config(path)
                configs.append(config)
                log.info("Loaded API config: %s from %s", config.source_id, path)
            except ApiConfigError as e:
                log.warning("Skipping invalid config %s: %s", path, e)
            except Exception as e:
                log.error("Unexpected error loading %s: %s", path, e)

        return configs

    def load_source_config(self, path: str) -> Optional[ApiSourceConfig]:
        """Load a single source config file. Returns None on error."""
        try:
            return load_api_source_config(path)
        except ApiConfigError as e:
            log.warning("Invalid config %s: %s", path, e)
            return None

    def register_or_update_source(self, config: ApiSourceConfig) -> str:
        """
        Register a new source or update an existing one if config changed.
        Returns: "new", "updated", or "unchanged"
        """
        source_id = config.source_id
        config_hash = stable_file_hash(config.config_path) if config.config_path else ""

        existing = self.db.get_source(source_id)

        if existing is None:
            # New source
            log.info("Registering new API source: %s", source_id)
            self.db.upsert_source(
                source_id=source_id,
                display_name=config.display_name,
                status="active" if config.enabled else "disabled",
                config_hash=config_hash,
                config_path=config.config_path,
            )
            self._persist_config(config, config_hash)
            self._cache[source_id] = ApiSourceRegistryEntry(
                source_id=source_id,
                display_name=config.display_name,
                status="active" if config.enabled else "disabled",
                config_hash=config_hash,
                config_path=config.config_path,
                config=config,
                first_seen_at=time.time(),
                last_updated_at=time.time(),
            )
            return "new"

        # Existing source — check if config changed
        if existing.get("config_hash") == config_hash:
            log.debug("Source %s unchanged (hash match)", source_id)
            return "unchanged"

        # Config changed
        log.info("Updating API source %s (config changed)", source_id)
        new_status = "active" if config.enabled else "disabled"
        # Preserve active/pending_sync status, don't downgrade
        if existing.get("status") in ("pending_sync",):
            new_status = existing["status"]

        self.db.upsert_source(
            source_id=source_id,
            display_name=config.display_name,
            status=new_status,
            config_hash=config_hash,
            config_path=config.config_path,
        )
        self._persist_config(config, config_hash)

        # Update cache
        self._cache[source_id] = ApiSourceRegistryEntry(
            source_id=source_id,
            display_name=config.display_name,
            status=new_status,
            config_hash=config_hash,
            config_path=config.config_path,
            config=config,
            last_updated_at=time.time(),
        )
        return "updated"

    def mark_missing_sources(
        self,
        current_source_ids: set,
        enable_grace: bool = True,
        grace_minutes: int = 1440,
    ) -> List[str]:
        """
        Mark sources whose config files are no longer present.
        Returns list of source_ids that were marked.
        """
        all_sources = self.db.get_all_sources()
        marked: List[str] = []

        for row in all_sources:
            sid = row["source_id"]
            if sid in current_source_ids:
                continue
            if row["status"] in ("removed", "pending_removal"):
                continue

            if enable_grace:
                log.info("Marking source %s as pending_removal (grace period)", sid)
                self.db.set_source_status(
                    sid, "pending_removal",
                    f"Config file removed. Grace period: {grace_minutes} minutes.",
                )
            else:
                log.info("Marking source %s as removed (no grace)", sid)
                self.db.set_source_status(sid, "removed", "Config file removed.")

            marked.append(sid)

        return marked

    def get_active_sources(self) -> List[ApiSourceRegistryEntry]:
        """Get all sources with status 'active'."""
        rows = self.db.get_sources_by_status("active")
        return [ApiSourceRegistryEntry.from_db_row(r) for r in rows]

    def get_source(self, source_id: str) -> Optional[ApiSourceRegistryEntry]:
        """Get a single source by ID."""
        # Check cache first
        if source_id in self._cache:
            return self._cache[source_id]
        row = self.db.get_source(source_id)
        if row:
            entry = ApiSourceRegistryEntry.from_db_row(row)
            self._cache[source_id] = entry
            return entry
        return None

    def set_source_status(
        self, source_id: str, status: str, message: str = ""
    ) -> None:
        """Update the status of a source."""
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status '{status}'. Must be one of: {VALID_STATUSES}")
        self.db.set_source_status(source_id, status, message)
        if source_id in self._cache:
            self._cache[source_id].status = status
            self._cache[source_id].status_message = message

    def _persist_config(self, config: ApiSourceConfig, config_hash: str) -> None:
        """Store the config YAML content in the database."""
        yaml_content = ""
        if config.config_path and os.path.isfile(config.config_path):
            with open(config.config_path, "r", encoding="utf-8") as f:
                yaml_content = f.read()

        self.db.upsert_source_config(
            source_id=config.source_id,
            config_yaml=yaml_content,
            config_hash=config_hash,
            url=config.fetch.url,
            auth_type=config.auth.type,
            sync_interval=config.sync.interval_minutes,
            enabled=config.enabled,
        )
