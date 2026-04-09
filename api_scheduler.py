"""
PERA AI — API Background Sync Scheduler

Runs API sync in a background thread, respecting settings flags.
Safe: catches all exceptions, never crashes the server.

Phase 5 module.
"""
from __future__ import annotations

import threading
import time
from typing import Optional

from log_config import get_logger
from settings import get_settings

log = get_logger("pera.api.scheduler")


class ApiScheduler:
    """
    Background scheduler for API source synchronisation.

    Runs in a daemon thread. Respects API_SYNC_ENABLED and
    API_SYNC_POLL_SECONDS settings. Fully exception-safe.
    """

    def __init__(self, index_dir: str = "assets/index"):
        self._settings = get_settings()
        self._index_dir = index_dir
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False
        self._cycle_count = 0

    @property
    def running(self) -> bool:
        return self._running

    @property
    def cycle_count(self) -> int:
        return self._cycle_count

    def start(self) -> bool:
        """
        Start the background sync loop.
        Returns True if started, False if disabled/already running.
        """
        if not self._settings.API_INGESTION_ENABLED:
            log.info("API scheduler not started — API_INGESTION_ENABLED=False")
            return False
        if not self._settings.API_SYNC_ENABLED:
            log.info("API scheduler not started — API_SYNC_ENABLED=False")
            return False
        if self._running:
            log.warning("API scheduler already running")
            return False

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="api-sync-scheduler",
            daemon=True,
        )
        self._thread.start()
        self._running = True
        log.info(
            "API scheduler started (poll every %ds)",
            self._settings.API_SYNC_POLL_SECONDS,
        )

        # Update health module
        try:
            from api_health import set_scheduler_running
            set_scheduler_running(True)
        except Exception:
            pass

        return True

    def stop(self) -> None:
        """Stop the scheduler gracefully."""
        if not self._running:
            return
        log.info("Stopping API scheduler...")
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        self._running = False

        try:
            from api_health import set_scheduler_running
            set_scheduler_running(False)
        except Exception:
            pass

        log.info("API scheduler stopped after %d cycles", self._cycle_count)

    def _loop(self) -> None:
        """
        Main loop: discovery → sync_all → sleep.
        Never raises — all exceptions caught and logged.
        """
        poll = max(30, self._settings.API_SYNC_POLL_SECONDS)
        log.info("API sync loop starting (interval=%ds)", poll)

        while not self._stop_event.is_set():
            try:
                self._run_cycle()
            except Exception as e:
                log.error(
                    "API sync cycle %d failed: %s", self._cycle_count, e,
                    exc_info=True,
                )
            finally:
                self._cycle_count += 1

            # Sleep in small increments so stop is responsive
            for _ in range(poll):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

        log.info("API sync loop exiting")

    def _run_cycle(self) -> None:
        """Execute one sync cycle."""
        from api_db import ApiDatabase
        from api_sync_manager import ApiSyncManager
        from api_health import set_last_sync

        db = ApiDatabase(self._settings.API_DB_URL)
        db.migrate()
        mgr = ApiSyncManager(db, index_dir=self._index_dir)

        log.info("API sync cycle %d starting", self._cycle_count)
        result = mgr.run_once()

        synced = result.get("synced", 0)
        failed = result.get("failed", 0)
        status = "ok" if failed == 0 else "partial_failure"
        set_last_sync(time.time(), status)

        log.info(
            "API sync cycle %d complete: %d synced, %d failed",
            self._cycle_count, synced, failed,
        )

        # ── Operational Activity refresh (every 6 hours) ─────
        self._maybe_refresh_operational_activity()

        # ── Inspection Performance summaries (every 5 min) ─────
        self._maybe_refresh_inspection_summaries()

        # ── Inspection Performance details (every 2 hours) ─────
        self._maybe_refresh_inspection_details()

        # Deletion lifecycle — process pending_removal sources
        self._process_pending_removals(db, mgr)

    _last_oa_refresh: float = 0.0
    _OA_REFRESH_INTERVAL = 6 * 3600  # 6 hours

    _last_ip_summary_refresh: float = 0.0
    _IP_SUMMARY_REFRESH_INTERVAL = 5 * 60      # 5 minutes

    _last_ip_detail_refresh: float = 0.0
    _IP_DETAIL_REFRESH_INTERVAL = 2 * 3600     # 2 hours

    def _maybe_refresh_operational_activity(self) -> None:
        """Refresh operational_activity summary tables every 6 hours."""
        now = time.time()
        if now - self._last_oa_refresh < self._OA_REFRESH_INTERVAL:
            return  # Not yet time

        try:
            from operational_activity_ingest import (
                ingest_operational_activity, reconcile_summary_from_details
            )
            log.info("Refreshing operational activity data...")
            # Refresh all summary levels + tehsil details
            results = ingest_operational_activity(
                levels=["divisions", "districts", "tehsils", "tehsil_breakdown"]
            )
            for lvl, cnt in results.items():
                log.info("  OA %s: %d records", lvl, cnt)
            # Reconcile so summary matches detail exactly
            reconcile_summary_from_details()
            log.info("Operational activity summary refresh complete")

            # Also refresh real requisition data from SDEO APIs
            try:
                from requisition_ingest import ingest_requisitions_quick
                log.info("Refreshing requisition detail data (SDEO)...")
                req_stats = ingest_requisitions_quick(days=30)
                log.info("  Requisitions: %d stored, Members: %d stored",
                         req_stats.get("requisitions_stored", 0),
                         req_stats.get("members_stored", 0))
            except Exception as re:
                log.error("Requisition ingestion failed: %s", re, exc_info=True)

            self._last_oa_refresh = now
            log.info("Operational activity + requisition refresh complete")
        except Exception as e:
            log.error("Operational activity refresh failed: %s", e, exc_info=True)

    def _maybe_refresh_inspection_summaries(self) -> None:
        """Refresh inspection performance summaries every 5 minutes."""
        now = time.time()
        if now - self._last_ip_summary_refresh < self._IP_SUMMARY_REFRESH_INTERVAL:
            return
        try:
            from inspection_ingest import ingest_inspection_summaries
            log.info("Refreshing inspection performance summaries...")
            results = ingest_inspection_summaries()
            for lvl, cnt in results.items():
                log.info("  Inspection %s: %d records", lvl, cnt)
            self._last_ip_summary_refresh = now
            log.info("Inspection summary refresh complete")
        except Exception as e:
            log.error("Inspection summary refresh failed: %s", e, exc_info=True)

    def _maybe_refresh_inspection_details(self) -> None:
        """Refresh inspection details + officer summaries every 2 hours."""
        now = time.time()
        if now - self._last_ip_detail_refresh < self._IP_DETAIL_REFRESH_INTERVAL:
            return
        try:
            from inspection_ingest import (
                ingest_inspection_details,
                ingest_inspection_officer_summaries,
                ingest_officer_inspection_details,
                ingest_officer_inspections,
            )
            log.info("Refreshing inspection performance details...")
            total = ingest_inspection_details()
            log.info("  Inspection details: %d records stored", total)

            # SDEO officer-level summaries
            log.info("Refreshing SDEO officer inspection summaries...")
            stats = ingest_inspection_officer_summaries()
            log.info("  Officer summaries: %d tehsils, %d officers stored",
                     stats.get("tehsils_processed", 0),
                     stats.get("officers_stored", 0))

            # PCM officer-inspection-details (with fineAmount)
            log.info("Refreshing PCM officer inspection details...")
            pcm_stats = ingest_officer_inspection_details()
            log.info("  PCM officers: %d tehsils, %d officers stored",
                     pcm_stats.get("tehsils_processed", 0),
                     pcm_stats.get("officers_stored", 0))

            # PCM individual officer inspections (granular records)
            log.info("Refreshing PCM individual officer inspections...")
            insp_stats = ingest_officer_inspections()
            log.info("  PCM inspections: %d officers, %d records stored",
                     insp_stats.get("officers_processed", 0),
                     insp_stats.get("records_stored", 0))

            self._last_ip_detail_refresh = now
            log.info("Inspection detail + officer refresh complete")
        except Exception as e:
            log.error("Inspection detail refresh failed: %s", e, exc_info=True)

    def _process_pending_removals(self, db, mgr) -> None:
        """
        Execute deletion lifecycle for sources in pending_removal status
        that have exceeded their grace period.
        """
        try:
            grace_minutes = self._settings.API_REMOVAL_GRACE_MINUTES
            now = time.time()
            pending = db.get_sources_by_status("pending_removal")

            for src in pending:
                updated = src.get("last_updated_at", now)
                elapsed_min = (now - updated) / 60.0

                if elapsed_min < grace_minutes:
                    log.debug(
                        "Source %s pending removal: %.0f/%.0f min elapsed",
                        src["source_id"], elapsed_min, grace_minutes,
                    )
                    continue

                sid = src["source_id"]
                log.info(
                    "Executing deletion lifecycle for source %s "
                    "(grace period %.0f min exceeded)",
                    sid, grace_minutes,
                )
                self._execute_source_deletion(db, sid)

        except Exception as e:
            log.error("Pending removal processing failed: %s", e, exc_info=True)

    def _execute_source_deletion(self, db, source_id: str) -> None:
        """
        Full deletion lifecycle for a source:
        1. Delete FAISS vectors + chunks.jsonl entries
        2. Delete api_records
        3. Mark source as removed
        """
        try:
            from index_store import delete_chunks_by_source
            from api_snapshot_store import ApiSnapshotStore

            # Step 1: Remove from FAISS + chunks.jsonl
            delete_chunks_by_source(self._index_dir, source_id)
            log.info("Deleted index chunks for source %s", source_id)

            # Step 2: Delete all api_records
            snapshot = ApiSnapshotStore(db)
            count = snapshot.delete_all_source_records(source_id)
            log.info("Deleted %d records for source %s", count, source_id)

            # Step 3: Mark source as removed
            db.set_source_status(
                source_id, "removed",
                f"Fully removed at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}",
            )
            log.info("Source %s marked as removed", source_id)

        except Exception as e:
            log.error(
                "Deletion lifecycle failed for %s: %s", source_id, e,
                exc_info=True,
            )
            db.set_source_status(
                source_id, "error",
                f"Deletion failed: {e}",
            )
