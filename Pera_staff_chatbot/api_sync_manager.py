"""
PERA AI — API Sync Manager

End-to-end sync orchestrator for API sources. Takes a source from
config → fetch → normalize → diff → chunk → embed → index.
Ingestion-only — not called from query-time path.

Phase 3 module.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from api_auth import ApiAuthResolver
from api_chunker import ApiChunker
from api_config_models import ApiSourceConfig, load_api_source_config
from api_db import ApiDatabase
from api_diff import ApiDiffEngine
from api_discovery import ApiSourceDiscovery
from api_fetcher import ApiFetcher
from api_normalizer import ApiNormalizer
from api_registry import ApiSourceRegistry
from api_snapshot_store import ApiSnapshotStore
from index_store import (
    upsert_api_chunks,
    delete_chunks_by_record_ids,
    delete_chunks_by_source,
)
from log_config import get_logger
from settings import get_settings

log = get_logger("pera.api.sync_manager")


class _SyncEarlyExit(Exception):
    """Sentinel for early sync exit — error already recorded in result dict."""
    pass


class ApiSyncManager:
    """
    Orchestrates API source sync: fetch → validate → normalize → diff
    → persist records → chunk → embed → index.
    """

    def __init__(
        self,
        db: ApiDatabase,
        index_dir: str = "assets/index",
    ):
        self.db = db
        self.index_dir = index_dir
        self._registry = ApiSourceRegistry(db)
        self._discovery = ApiSourceDiscovery(self._registry)
        self._snapshot = ApiSnapshotStore(db)
        self._fetcher = ApiFetcher(auth_resolver=ApiAuthResolver())
        self._normalizer = ApiNormalizer()
        self._diff = ApiDiffEngine()
        self._chunker = ApiChunker()
        self._settings = get_settings()
        # Retry state: source_id -> consecutive failure count
        self._retry_counts: Dict[str, int] = {}
        self._max_retries: int = 5
        self._base_backoff: float = float(self._settings.API_DEFAULT_RETRY_BACKOFF_SECONDS)

    def sync_source(self, source_id: str) -> Dict[str, Any]:
        """
        Full sync pipeline for a single API source.

        Steps:
        1. Load config
        2. Create sync run
        3. Fetch payload
        4. Normalize/validate
        5. Diff with existing records
        6. Persist normalized records
        7. Chunk added/changed records
        8. Delete chunks/vectors for deleted records
        9. Embed/index added/changed chunks
        10. Finish sync run
        11. Update source status
        """
        result: Dict[str, Any] = {"source_id": source_id, "success": False}
        run_id = None

        try:
            # 1. Load config
            source = self.db.get_source(source_id)
            if not source:
                result["error"] = f"Source '{source_id}' not found in registry"
                raise _SyncEarlyExit()

            config_row = self.db.get_source_config(source_id)
            if not config_row:
                result["error"] = f"No config stored for '{source_id}'"
                raise _SyncEarlyExit()

            config = self._load_config_from_row(config_row)
            if not config:
                result["error"] = f"Failed to parse stored config for '{source_id}'"
                raise _SyncEarlyExit()

            # 2. Create sync run
            run_id = self._snapshot.create_sync_run(source_id)
            self._registry.set_source_status(source_id, "syncing")

            # 3. Fetch
            fetch_result = self._fetcher.fetch_source(config)
            if not fetch_result.success:
                self._finish_error(run_id, source_id, f"Fetch failed: {fetch_result.error_message}")
                result["error"] = fetch_result.error_message
                raise _SyncEarlyExit()

            # 4. Normalize
            norm_result = self._normalizer.normalize_payload(config, fetch_result)
            if not norm_result.success:
                errors = "; ".join(norm_result.validation_errors)
                self._finish_error(run_id, source_id, f"Normalization failed: {errors}")
                result["error"] = errors
                raise _SyncEarlyExit()

            # 5. Diff
            existing = self._snapshot.get_existing_records(source_id)
            diff_result = self._diff.diff_records(
                source_id, norm_result.normalized_records, existing
            )

            # 6. Persist records
            records_to_upsert = diff_result.added_records + diff_result.changed_records
            if records_to_upsert:
                self._snapshot.upsert_records(source_id, records_to_upsert)

            if diff_result.deleted_record_ids and config.sync.delete_missing_records:
                self._snapshot.soft_delete_records(source_id, diff_result.deleted_record_ids)

            # 6.5 Analytics write-through (PostgreSQL)
            # Always write ALL normalized records — fact tables use
            # snapshot_date in their UNIQUE key, so daily snapshots
            # need every record even when the diff engine sees no change.
            self._write_analytics(
                source_id, norm_result.normalized_records, norm_result,
                raw_payload=fetch_result.payload,
            )

            # 7. Chunk added/changed records
            chunks = self._chunker.chunk_records(records_to_upsert, config)

            # 8. Delete chunks for deleted records
            if diff_result.deleted_record_ids:
                delete_chunks_by_record_ids(
                    self.index_dir, source_id, diff_result.deleted_record_ids
                )

            # 9. Embed/index new chunks
            index_result = {}
            if chunks:
                index_result = upsert_api_chunks(
                    self.index_dir, chunks, source_id=source_id
                )

            # 10. Finish sync run
            self._snapshot.finish_sync_run(
                run_id,
                status="completed",
                records_fetched=norm_result.record_count,
                records_new=diff_result.added_count,
                records_updated=diff_result.changed_count,
                records_removed=diff_result.deleted_count,
            )

            # 11. Update source status
            self._registry.set_source_status(source_id, "synced")
            self.db.set_source_status(
                source_id, "synced",
                message=f"Synced: +{diff_result.added_count} ~{diff_result.changed_count} -{diff_result.deleted_count}"
            )

            result["success"] = True
            result["diff"] = diff_result.summary()
            result["chunks_indexed"] = index_result.get("chunks_added", 0)
            result["records_fetched"] = norm_result.record_count

            log.info(
                "Sync completed for %s: %s, %d chunks indexed",
                source_id, diff_result.summary(),
                index_result.get("chunks_added", 0),
            )

        except _SyncEarlyExit:
            pass  # Error already recorded in result
        except Exception as e:
            log.error("Sync failed for %s: %s", source_id, e, exc_info=True)
            result["error"] = str(e)
            if run_id:
                self._finish_error(run_id, source_id, str(e))

        # Track retry state (runs for ALL exit paths)
        if result.get("success"):
            self._retry_counts.pop(source_id, None)
        else:
            self._retry_counts[source_id] = self._retry_counts.get(source_id, 0) + 1

        return result

    def should_retry(self, source_id: str) -> bool:
        """Check if a source should be retried based on failure count."""
        return self._retry_counts.get(source_id, 0) < self._max_retries

    def get_retry_delay(self, source_id: str) -> float:
        """Exponential backoff delay for a source."""
        count = self._retry_counts.get(source_id, 0)
        return min(self._base_backoff * (2 ** count), 300.0)  # max 5 min

    def sync_all_sources(self) -> Dict[str, Any]:
        """Sync all active/synced sources. Also retries errored sources under limit."""
        sources = self.db.get_sources_by_status("active", "synced")
        # Also retry errored sources that haven't exceeded retry limit
        errored = self.db.get_sources_by_status("error")
        for src in errored:
            sid = src["source_id"]
            if self.should_retry(sid):
                delay = self.get_retry_delay(sid)
                log.info(
                    "Retrying errored source %s (attempt %d, delay %.1fs)",
                    sid, self._retry_counts.get(sid, 0) + 1, delay,
                )
                sources.append(src)
            else:
                log.warning(
                    "Source %s exceeded max retries (%d) — skipping",
                    sid, self._max_retries,
                )

        results: Dict[str, Any] = {
            "total": len(sources),
            "synced": 0,
            "failed": 0,
            "source_results": {},
        }

        for source in sources:
            sid = source["source_id"]
            try:
                res = self.sync_source(sid)
                results["source_results"][sid] = res
                if res.get("success"):
                    results["synced"] += 1
                else:
                    results["failed"] += 1
            except Exception as e:
                log.error("Sync failed for %s: %s", sid, e)
                results["source_results"][sid] = {"error": str(e)}
                results["failed"] += 1

        log.info(
            "Sync all complete: %d synced, %d failed out of %d",
            results["synced"], results["failed"], results["total"],
        )
        return results

    def run_once(self) -> Dict[str, Any]:
        """
        One-shot: run discovery to detect config changes, then sync all active sources.
        """
        result: Dict[str, Any] = {}

        # Discovery pass
        try:
            discovery_result = self._discovery.reconcile_sources()
            result["discovery"] = discovery_result.summary()
        except Exception as e:
            log.error("Discovery failed: %s", e)
            result["discovery_error"] = str(e)

        # Sync all
        sync_result = self.sync_all_sources()
        result.update(sync_result)
        return result

    def start_background_sync(self) -> None:
        """
        Placeholder for background sync scheduling.
        In Phase 3 this is a no-op — continuous scheduling deferred.
        """
        log.info(
            "Background API sync not yet active (Phase 3 scaffold). "
            "Use run_once() for manual sync."
        )

    def schedule_loop(self) -> None:
        """
        Scaffold for continuous scheduling. Not active in Phase 3.
        """
        log.info("API sync schedule_loop is a Phase 5 feature (not active).")

    # ── Private helpers ───────────────────────────────────────

    def _load_config_from_row(self, config_row: Dict[str, Any]) -> Optional[ApiSourceConfig]:
        """Parse an ApiSourceConfig from stored DB row."""
        try:
            from api_config_models import parse_api_source_config
            yaml_text = config_row.get("config_yaml", "")
            if not yaml_text:
                return None
            return parse_api_source_config(yaml_text)
        except Exception as e:
            log.error("Failed to parse config for %s: %s", config_row.get("source_id"), e)
            return None

    def _finish_error(
        self, run_id: int, source_id: str, message: str
    ) -> None:
        """Mark a sync run as errored and update source status."""
        try:
            self._snapshot.finish_sync_run(
                run_id, status="error", error_message=message
            )
            self._registry.set_source_status(source_id, "error")
            self.db.set_source_status(source_id, "error", message=message)
        except Exception as e:
            log.error("Failed to finish error state for %s: %s", source_id, e)

    def _write_analytics(
        self, source_id: str, records: list, norm_result: Any,
        raw_payload: Any = None,
    ) -> None:
        """
        Write-through to PostgreSQL analytics store after successful
        record persistence. Fails silently — never affects main pipeline.

        Args:
            source_id: API source identifier
            records: Normalized records to write
            norm_result: Normalization result (for metadata)
            raw_payload: Full raw API response (for summary extraction)
        """
        if not self._settings.ANALYTICS_WRITE_ENABLED:
            return

        try:
            from analytics_store import get_analytics_store
            from analytics_mapping import AnalyticsMapper

            store = get_analytics_store()
            if not store:
                return

            # Write raw records
            record_dicts = []
            raw_records_for_mapping = []
            for rec in records:
                rec_dict = {
                    "record_id": getattr(rec, "record_id", ""),
                    "record_type": getattr(rec, "record_type", ""),
                    "content_hash": getattr(rec, "record_hash", ""),
                    "raw_json": getattr(rec, "canonical_json", "{}"),
                    "normalized_text": getattr(rec, "canonical_text", ""),
                }
                record_dicts.append(rec_dict)
                raw_records_for_mapping.append({
                    "record_id": rec_dict["record_id"],
                    "raw_record": getattr(rec, "raw_record", {}),
                })

            if record_dicts:
                store.upsert_records(source_id, record_dicts)

            # Run curated mapping (pass raw_payload for summary extraction)
            mapper = AnalyticsMapper(store)
            if mapper.has_mapping(source_id):
                mapper.map_and_store(
                    source_id, raw_records_for_mapping,
                    raw_payload=raw_payload,
                )

            log.info(
                "Analytics write-through: %d records for %s",
                len(record_dicts), source_id,
            )

        except Exception as e:
            log.warning(
                "Analytics write-through failed for %s (non-fatal): %s",
                source_id, e,
            )

