"""
PERA AI — Freshness Scheduler

Background daemon that keeps the analytics PostgreSQL data fresh by
periodically re-running the per-domain ingest scripts. Goal: never
let the chatbot serve stored API data older than ~1 hour.

Three independent ingest tracks, each on its own daemon thread:

  • inspection_summaries  → calls ingest_inspection_summaries()
                           every FRESH_INSPECTION_SUMMARY_INTERVAL sec.
                           Refreshes inspection_performance.
  • inspection_officers   → calls ingest_inspection_officer_summaries()
                           and ingest_officer_inspection_details()
                           every FRESH_INSPECTION_OFFICER_INTERVAL sec.
                           Refreshes inspection_officer_summary +
                           officer_inspection_detail.
  • operational_activity  → calls ingest_operational_activity()
                           every FRESH_OPERATIONAL_ACTIVITY_INTERVAL sec.
                           Refreshes operational_activity*.

This scheduler is COMPLEMENTARY to ChallanScheduler (which is already
running for challan data on a 5s/60s tier). Together they cover every
analytics table the chatbot reads from.

Failure handling: each ingest call is wrapped in try/except — an upstream
404 from one tehsil never kills the loop. Status (last success time,
last error) is exposed via get_freshness_status() so the /api/freshness
endpoint can report it.

Defaults are conservative: every track runs every 30 minutes, well under
the 1-hour staleness budget. Override via env vars in .env:
  FRESHNESS_ENABLED                          (default 1)
  FRESH_INSPECTION_SUMMARY_INTERVAL          (default 1800 = 30 min)
  FRESH_INSPECTION_OFFICER_INTERVAL          (default 3600 = 60 min)
  FRESH_OPERATIONAL_ACTIVITY_INTERVAL        (default 3600 = 60 min)
  FRESH_INITIAL_DELAY                        (default 30 = grace period
                                               before first run, lets
                                               the rest of the app boot)
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, Optional

from log_config import get_logger

log = get_logger("pera.freshness")


# ── Config (env-driven) ──────────────────────────────────────
FRESHNESS_ENABLED = os.getenv("FRESHNESS_ENABLED", "1").strip() != "0"
# Defaults: 2-3 h cadence for the fast tracks, 6 h for the slow per-tehsil
# breakdown. Each cycle gets enough head-room to complete cleanly inside
# its window without thrashing the upstream PERA APIs.
FRESH_INSPECTION_SUMMARY_INTERVAL = int(os.getenv("FRESH_INSPECTION_SUMMARY_INTERVAL", "7200"))   # 2 h
FRESH_INSPECTION_OFFICER_INTERVAL = int(os.getenv("FRESH_INSPECTION_OFFICER_INTERVAL", "10800"))  # 3 h
# Operational activity is split into TWO tiers because the per-tehsil
# breakdown takes ~2 h while the summary endpoints take ~1 min:
#   • SUMMARY tier  — divisions + districts + tehsils aggregates (fast)
#   • BREAKDOWN tier — tehsil-by-tehsil detail records (slow)
# Putting both in one short loop made the cycle perpetually overflow
# its window and never report a successful completion.
FRESH_OPERATIONAL_ACTIVITY_INTERVAL = int(os.getenv("FRESH_OPERATIONAL_ACTIVITY_INTERVAL", "7200"))  # 2 h
FRESH_OPERATIONAL_ACTIVITY_BREAKDOWN_INTERVAL = int(
    os.getenv("FRESH_OPERATIONAL_ACTIVITY_BREAKDOWN_INTERVAL", "21600")  # 6 h
)
FRESH_INITIAL_DELAY = int(os.getenv("FRESH_INITIAL_DELAY", "30"))


# ── Status registry ──────────────────────────────────────────
# Read by /api/freshness so operators can see when each track last
# completed successfully and whether it's currently running.
_status: Dict[str, Dict[str, Any]] = {
    "inspection_summaries": {"interval_s": FRESH_INSPECTION_SUMMARY_INTERVAL,
                              "last_started_at": 0.0, "last_finished_at": 0.0,
                              "last_success_at": 0.0, "last_error": "",
                              "cycle_count": 0, "running_now": False,
                              "last_result": None},
    "inspection_officers":  {"interval_s": FRESH_INSPECTION_OFFICER_INTERVAL,
                              "last_started_at": 0.0, "last_finished_at": 0.0,
                              "last_success_at": 0.0, "last_error": "",
                              "cycle_count": 0, "running_now": False,
                              "last_result": None},
    "operational_activity": {"interval_s": FRESH_OPERATIONAL_ACTIVITY_INTERVAL,
                              "last_started_at": 0.0, "last_finished_at": 0.0,
                              "last_success_at": 0.0, "last_error": "",
                              "cycle_count": 0, "running_now": False,
                              "last_result": None},
    "operational_activity_breakdown": {
                              "interval_s": FRESH_OPERATIONAL_ACTIVITY_BREAKDOWN_INTERVAL,
                              "last_started_at": 0.0, "last_finished_at": 0.0,
                              "last_success_at": 0.0, "last_error": "",
                              "cycle_count": 0, "running_now": False,
                              "last_result": None},
}
_status_lock = threading.Lock()


def _stamp_started(track: str) -> None:
    with _status_lock:
        s = _status[track]
        s["last_started_at"] = time.time()
        s["running_now"] = True


def _stamp_finished(track: str, *, ok: bool, result: Any = None,
                    error: str = "") -> None:
    with _status_lock:
        s = _status[track]
        now = time.time()
        s["last_finished_at"] = now
        s["running_now"] = False
        s["cycle_count"] += 1
        if ok:
            s["last_success_at"] = now
            s["last_error"] = ""
            s["last_result"] = result
        else:
            s["last_error"] = error


def get_freshness_status() -> Dict[str, Any]:
    """Snapshot of all tracks. Safe to serialize as JSON.

    Adds derived fields:
      • seconds_since_success: how stale this track currently is
      • staleness_status:     'fresh' | 'aging' | 'stale' (vs interval)
    """
    out: Dict[str, Any] = {}
    now = time.time()
    with _status_lock:
        for name, s in _status.items():
            entry = dict(s)
            last_ok = s["last_success_at"]
            seconds_since = (now - last_ok) if last_ok else None
            entry["seconds_since_success"] = (
                int(seconds_since) if seconds_since is not None else None
            )
            interval = s["interval_s"] or 1
            if seconds_since is None:
                entry["staleness_status"] = "never_run"
            elif seconds_since < interval * 1.5:
                entry["staleness_status"] = "fresh"
            elif seconds_since < interval * 3:
                entry["staleness_status"] = "aging"
            else:
                entry["staleness_status"] = "stale"
            out[name] = entry
    return {"enabled": FRESHNESS_ENABLED, "tracks": out}


# ── Track runners (wrap one ingest call each) ────────────────

def _run_inspection_summaries() -> Any:
    from inspection_ingest import ingest_inspection_summaries
    return ingest_inspection_summaries()


def _run_inspection_officers() -> Any:
    """Run both officer-detail ingest modes back-to-back.
    Each one is independently wrapped so a failure in one doesn't
    sink the other."""
    from inspection_ingest import (
        ingest_inspection_officer_summaries,
        ingest_officer_inspection_details,
    )
    sub_results: Dict[str, Any] = {}
    try:
        sub_results["sdeo_officer_summaries"] = ingest_inspection_officer_summaries()
    except Exception as e:
        sub_results["sdeo_officer_summaries_error"] = str(e)[:200]
        log.error("Freshness: SDEO officer summaries failed: %s", e)
    try:
        sub_results["pcm_officer_details"] = ingest_officer_inspection_details()
    except Exception as e:
        sub_results["pcm_officer_details_error"] = str(e)[:200]
        log.error("Freshness: PCM officer details failed: %s", e)
    return sub_results


def _run_operational_activity() -> Any:
    """SUMMARY tier — fast aggregate endpoints only.

    Skips tehsil_breakdown (which iterates ~156 tehsils and takes ~2 h);
    that's covered by the slower _run_operational_activity_breakdown
    track on a 6 h cadence.
    """
    from operational_activity_ingest import ingest_operational_activity
    return ingest_operational_activity(levels=["divisions", "districts", "tehsils"])


def _run_operational_activity_breakdown() -> Any:
    """BREAKDOWN tier — per-tehsil detail records.

    Hits the upstream API once per tehsil so a full cycle is slow
    (~1.5–2 hours). Runs every 6 h by default to stay under-loaded
    upstream while still keeping detail records reasonably fresh.
    """
    from operational_activity_ingest import ingest_operational_activity
    return ingest_operational_activity(levels=["tehsil_breakdown"])


# ── Loops ────────────────────────────────────────────────────

class FreshnessScheduler:
    """Holds the three daemon threads and wires them to the runners.

    Mirrors the ChallanScheduler shape so operators see one consistent
    pattern across the codebase."""

    def __init__(self):
        self._stop = threading.Event()
        self._threads: Dict[str, threading.Thread] = {}
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def start(self) -> bool:
        if not FRESHNESS_ENABLED:
            log.info("Freshness scheduler disabled via FRESHNESS_ENABLED=0")
            return False
        if self._running:
            log.warning("Freshness scheduler already running")
            return False

        # Sanity: needs analytics DB. If it's down we still start the
        # threads — each ingest call will fail-fast and be retried on
        # the next interval, which is fine.
        try:
            from analytics_db import get_analytics_db
            db = get_analytics_db()
            if not db or not db.is_available():
                log.warning("Freshness scheduler: analytics DB not available, "
                             "ingest cycles will fail until DB is restored.")
        except Exception as e:
            log.warning("Freshness scheduler DB pre-check failed (non-fatal): %s", e)

        self._stop.clear()
        self._spawn("inspection_summaries",  _run_inspection_summaries,
                    FRESH_INSPECTION_SUMMARY_INTERVAL)
        self._spawn("inspection_officers",   _run_inspection_officers,
                    FRESH_INSPECTION_OFFICER_INTERVAL)
        self._spawn("operational_activity",  _run_operational_activity,
                    FRESH_OPERATIONAL_ACTIVITY_INTERVAL)
        self._spawn("operational_activity_breakdown",
                    _run_operational_activity_breakdown,
                    FRESH_OPERATIONAL_ACTIVITY_BREAKDOWN_INTERVAL)
        self._running = True
        log.info("Freshness scheduler started "
                  "(insp_summary=%ds, insp_officer=%ds, op_activity=%ds, "
                  "op_breakdown=%ds, initial_delay=%ds)",
                  FRESH_INSPECTION_SUMMARY_INTERVAL,
                  FRESH_INSPECTION_OFFICER_INTERVAL,
                  FRESH_OPERATIONAL_ACTIVITY_INTERVAL,
                  FRESH_OPERATIONAL_ACTIVITY_BREAKDOWN_INTERVAL,
                  FRESH_INITIAL_DELAY)
        return True

    def stop(self) -> None:
        self._stop.set()
        self._running = False

    def _spawn(self, track: str, runner, interval_s: int) -> None:
        t = threading.Thread(
            target=self._loop, args=(track, runner, interval_s),
            name=f"freshness-{track}", daemon=True,
        )
        t.start()
        self._threads[track] = t

    def _sleep(self, seconds: int) -> None:
        # Sleep in 1-second slices so stop() is responsive.
        end = time.time() + max(1, seconds)
        while time.time() < end:
            if self._stop.is_set():
                return
            time.sleep(1)

    def _loop(self, track: str, runner, interval_s: int) -> None:
        # Stagger first runs so we don't slam Postgres + upstream APIs
        # all at once during boot. Each track adds a small offset.
        offset = {"inspection_summaries": 0,
                  "inspection_officers": 15,
                  "operational_activity": 30,
                  "operational_activity_breakdown": 60}.get(track, 0)
        self._sleep(FRESH_INITIAL_DELAY + offset)

        while not self._stop.is_set():
            _stamp_started(track)
            t0 = time.time()
            log.info("Freshness[%s]: cycle starting", track)
            try:
                result = runner()
                elapsed = time.time() - t0
                log.info("Freshness[%s]: cycle ok in %.1fs result=%s",
                          track, elapsed, str(result)[:200])
                _stamp_finished(track, ok=True, result=result)
            except Exception as e:
                _stamp_finished(track, ok=False, error=str(e)[:300])
                log.error("Freshness[%s]: cycle failed after %.1fs: %s",
                           track, time.time() - t0, e)
            # Wait the remainder of the interval before the next cycle.
            self._sleep(interval_s)


# Module-level singleton — fastapi_app.py constructs it once at startup.
_scheduler: Optional[FreshnessScheduler] = None


def get_scheduler() -> FreshnessScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = FreshnessScheduler()
    return _scheduler
