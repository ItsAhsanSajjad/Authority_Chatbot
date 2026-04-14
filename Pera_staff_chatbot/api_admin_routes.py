"""
PERA AI — Admin API Routes for API Ingestion

FastAPI router providing admin control over API data sources.
Protected by existing auth system. Never exposes secrets.

Phase 5 module.
"""
from __future__ import annotations

import threading
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Depends

from log_config import get_logger
from settings import get_settings

log = get_logger("pera.api.admin")

router = APIRouter(prefix="/api/admin", tags=["api-admin"])

# ── Lazy DB accessor (avoids import-time DB creation) ────────

_db_instance = None
_db_lock = threading.Lock()


def _get_db():
    """Get or create the API database instance."""
    global _db_instance
    if _db_instance is None:
        with _db_lock:
            if _db_instance is None:
                from api_db import ApiDatabase
                s = get_settings()
                _db_instance = ApiDatabase(s.API_DB_URL)
    return _db_instance


def _require_api_enabled():
    """Dependency: ensure API ingestion is enabled."""
    s = get_settings()
    if not s.API_INGESTION_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="API ingestion is not enabled on this instance",
        )
    return True


# ── Endpoints ────────────────────────────────────────────────

@router.get("/sources")
def list_sources(enabled: bool = Depends(_require_api_enabled)):
    """List all API sources with status, last_synced, record_count, last_error."""
    db = _get_db()
    sources = db.get_all_sources()

    result = []
    for src in sources:
        entry = {
            "source_id": src["source_id"],
            "display_name": src.get("display_name", ""),
            "status": src.get("status", "unknown"),
            "status_message": src.get("status_message", ""),
            "record_count": src.get("record_count", 0),
            "last_sync_at": src.get("last_sync_at"),
            "last_updated_at": src.get("last_updated_at"),
        }
        # Get last error from most recent failed sync run
        from api_snapshot_store import ApiSnapshotStore
        snap = ApiSnapshotStore(db)
        last_run = snap.get_latest_sync_run(src["source_id"])
        if last_run and last_run.get("status") == "error":
            entry["last_error"] = last_run.get("error_message", "")
        else:
            entry["last_error"] = ""

        result.append(entry)

    return {"sources": result, "total": len(result)}


@router.post("/sources/{source_id}/sync")
def trigger_sync(source_id: str, enabled: bool = Depends(_require_api_enabled)):
    """Manually trigger sync for a single source."""
    db = _get_db()
    source = db.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    if source.get("status") in ("removed", "pending_removal"):
        raise HTTPException(
            status_code=400,
            detail=f"Source '{source_id}' is in '{source['status']}' state — cannot sync",
        )

    try:
        from api_sync_manager import ApiSyncManager
        s = get_settings()
        idx_dir = s.INDEX_DIR if hasattr(s, 'INDEX_DIR') else "assets/index"
        mgr = ApiSyncManager(db, index_dir=idx_dir)
        result = mgr.sync_source(source_id)

        # Update health
        try:
            import time
            from api_health import set_last_sync
            status = "ok" if result.get("success") else "error"
            set_last_sync(time.time(), status)
        except Exception:
            pass

        return {
            "source_id": source_id,
            "success": result.get("success", False),
            "records_fetched": result.get("records_fetched", 0),
            "chunks_indexed": result.get("chunks_indexed", 0),
            "error": result.get("error"),
        }
    except Exception as e:
        log.error("Manual sync failed for %s: %s", source_id, e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Sync failed: {e}")


@router.post("/sources/sync_all")
def trigger_sync_all(enabled: bool = Depends(_require_api_enabled)):
    """Trigger sync for all active sources."""
    db = _get_db()
    try:
        from api_sync_manager import ApiSyncManager
        s = get_settings()
        idx_dir = s.INDEX_DIR if hasattr(s, 'INDEX_DIR') else "assets/index"
        mgr = ApiSyncManager(db, index_dir=idx_dir)
        result = mgr.run_once()

        # Update health
        try:
            import time
            from api_health import set_last_sync
            failed = result.get("failed", 0)
            status = "ok" if failed == 0 else "partial_failure"
            set_last_sync(time.time(), status)
        except Exception:
            pass

        return {
            "total": result.get("total", 0),
            "synced": result.get("synced", 0),
            "failed": result.get("failed", 0),
        }
    except Exception as e:
        log.error("Sync all failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Sync all failed: {e}")


@router.get("/sources/{source_id}/history")
def get_sync_history(
    source_id: str,
    limit: int = 20,
    enabled: bool = Depends(_require_api_enabled),
):
    """Return sync run history for a source."""
    db = _get_db()
    source = db.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    with db.connection() as conn:
        rows = conn.execute(
            "SELECT run_id, source_id, started_at, completed_at, status, "
            "records_fetched, records_new, records_updated, records_removed, "
            "error_message FROM api_sync_runs "
            "WHERE source_id = ? ORDER BY started_at DESC LIMIT ?",
            (source_id, min(limit, 100)),
        ).fetchall()

    runs = [dict(r) for r in rows]
    return {"source_id": source_id, "runs": runs, "total": len(runs)}


@router.delete("/sources/{source_id}")
def delete_source(source_id: str, enabled: bool = Depends(_require_api_enabled)):
    """
    Mark a source for deletion (pending_removal).
    Actual cleanup happens after grace period via scheduler.
    """
    db = _get_db()
    source = db.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    if source.get("status") == "removed":
        return {"source_id": source_id, "status": "already_removed"}

    if source.get("status") == "pending_removal":
        return {"source_id": source_id, "status": "already_pending_removal"}

    s = get_settings()
    grace = s.API_REMOVAL_GRACE_MINUTES

    db.set_source_status(
        source_id,
        "pending_removal",
        f"Admin-requested removal. Grace period: {grace} minutes.",
    )
    log.info("Source %s marked for removal by admin (grace=%d min)", source_id, grace)

    return {
        "source_id": source_id,
        "status": "pending_removal",
        "grace_minutes": grace,
        "message": f"Source will be fully removed after {grace} minutes",
    }


@router.get("/health")
def api_ingestion_health(enabled: bool = Depends(_require_api_enabled)):
    """Return API ingestion health status."""
    from api_health import get_api_health_status
    db = _get_db()
    return get_api_health_status(db=db)
