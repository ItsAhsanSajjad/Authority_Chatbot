"""
PERA AI — API Health & Status

Provides API ingestion health/status visibility for /health and /ready endpoints.
Phase 5 module.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

from log_config import get_logger

log = get_logger("pera.api.health")

# Module-level state (set by scheduler/startup)
_scheduler_running: bool = False
_last_sync_time: Optional[float] = None
_last_sync_status: str = "unknown"


def set_scheduler_running(val: bool) -> None:
    global _scheduler_running
    _scheduler_running = val


def set_last_sync(ts: float, status: str) -> None:
    global _last_sync_time, _last_sync_status
    _last_sync_time = ts
    _last_sync_status = status


def get_api_health_status(db=None) -> Dict[str, Any]:
    """
    Return API ingestion health summary.

    Returns:
        total_sources, active_sources, failed_sources,
        last_sync_time, last_sync_status, scheduler_running
    """
    result: Dict[str, Any] = {
        "api_ingestion_enabled": False,
        "total_sources": 0,
        "active_sources": 0,
        "failed_sources": 0,
        "synced_sources": 0,
        "pending_removal_sources": 0,
        "last_sync_time": None,
        "last_sync_status": _last_sync_status,
        "scheduler_running": _scheduler_running,
    }

    try:
        from settings import get_settings
        s = get_settings()
        result["api_ingestion_enabled"] = s.API_INGESTION_ENABLED

        if not s.API_INGESTION_ENABLED:
            return result

        if db is None:
            from api_db import ApiDatabase
            db = ApiDatabase(s.API_DB_URL)

        sources = db.get_all_sources()
        result["total_sources"] = len(sources)

        for src in sources:
            status = src.get("status", "")
            if status in ("active", "synced", "syncing", "pending_sync"):
                result["active_sources"] += 1
            if status == "synced":
                result["synced_sources"] += 1
            if status == "error":
                result["failed_sources"] += 1
            if status == "pending_removal":
                result["pending_removal_sources"] += 1

        # Last sync time from module state or DB
        if _last_sync_time:
            result["last_sync_time"] = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime(_last_sync_time)
            )
        result["last_sync_status"] = _last_sync_status

    except Exception as e:
        log.warning("Failed to gather API health: %s", e)
        result["health_error"] = str(e)

    return result
