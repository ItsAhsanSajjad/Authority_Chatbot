"""
PERA AI — API Source Discovery

Detects new, changed, and removed API source config files in assets/apis/.
Reconciles filesystem state with the source registry database.

Phase 1: discovery and metadata reconciliation only.
No fetch/normalize/index execution.

Usage:
    from api_discovery import ApiSourceDiscovery
    discovery = ApiSourceDiscovery(registry, settings)
    result = discovery.reconcile_sources()
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set

from api_config_models import ApiConfigError, ApiSourceConfig
from api_registry import ApiSourceRegistry
from api_source_utils import scan_api_config_dir, stable_file_hash
from log_config import get_logger
from settings import get_settings

log = get_logger("pera.api.discovery")


@dataclass
class DiscoveryResult:
    """Summary of a discovery/reconciliation run."""
    new_sources: List[str] = field(default_factory=list)
    updated_sources: List[str] = field(default_factory=list)
    unchanged_sources: List[str] = field(default_factory=list)
    removed_sources: List[str] = field(default_factory=list)
    error_sources: Dict[str, str] = field(default_factory=dict)  # source_id/path -> error

    @property
    def total_scanned(self) -> int:
        return (
            len(self.new_sources)
            + len(self.updated_sources)
            + len(self.unchanged_sources)
            + len(self.error_sources)
        )

    @property
    def has_changes(self) -> bool:
        return bool(self.new_sources or self.updated_sources or self.removed_sources)

    def summary(self) -> str:
        parts = [
            f"Scanned: {self.total_scanned}",
            f"New: {len(self.new_sources)}",
            f"Updated: {len(self.updated_sources)}",
            f"Unchanged: {len(self.unchanged_sources)}",
            f"Removed: {len(self.removed_sources)}",
        ]
        if self.error_sources:
            parts.append(f"Errors: {len(self.error_sources)}")
        return " | ".join(parts)


class ApiSourceDiscovery:
    """
    Discovers API source config files and reconciles with the registry.
    """

    def __init__(self, registry: ApiSourceRegistry, source_dir: str = ""):
        self.registry = registry
        s = get_settings()
        self.source_dir = source_dir or s.API_SOURCE_DIR
        self._enable_grace = s.API_ENABLE_PENDING_REMOVAL_GRACE
        self._grace_minutes = s.API_REMOVAL_GRACE_MINUTES

    def discover_configs(self) -> List[ApiSourceConfig]:
        """
        Scan assets/apis/ for YAML config files and return parsed configs.
        Invalid configs are logged but skipped.
        """
        return self.registry.scan_source_configs(self.source_dir)

    def reconcile_sources(self) -> DiscoveryResult:
        """
        Full reconciliation:
        1. Scan filesystem for config files
        2. Register new / update changed sources
        3. Mark missing sources as pending_removal or removed
        Returns a DiscoveryResult summarizing changes.
        """
        result = DiscoveryResult()

        # 1. Discover all valid configs from filesystem
        configs = self.discover_configs()

        # Track which source_ids we found on disk
        found_source_ids: Set[str] = set()

        # 2. Register or update each config
        for config in configs:
            found_source_ids.add(config.source_id)
            try:
                action = self.registry.register_or_update_source(config)
                if action == "new":
                    result.new_sources.append(config.source_id)
                elif action == "updated":
                    result.updated_sources.append(config.source_id)
                else:
                    result.unchanged_sources.append(config.source_id)
            except Exception as e:
                log.error(
                    "Error registering source %s: %s", config.source_id, e
                )
                result.error_sources[config.source_id] = str(e)

        # Also track configs that failed to parse (from scan errors)
        paths = scan_api_config_dir(self.source_dir)
        for path in paths:
            try:
                from api_config_models import load_api_source_config
                cfg = load_api_source_config(path)
                # Already processed above if it parsed successfully
            except ApiConfigError as e:
                result.error_sources[path] = str(e)

        # 3. Mark sources whose config files are gone
        marked = self.registry.mark_missing_sources(
            current_source_ids=found_source_ids,
            enable_grace=self._enable_grace,
            grace_minutes=self._grace_minutes,
        )
        result.removed_sources = marked

        log.info("Discovery reconciliation: %s", result.summary())
        return result
