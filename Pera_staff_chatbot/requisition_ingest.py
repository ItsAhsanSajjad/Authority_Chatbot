"""
PERA AI - Requisition Ingestion from SDEO Dashboard APIs

Fetches real requisition data from two PERA360 endpoints:
  1. /api/sdeo-dashboard/requisitions-details   -> per-tehsil requisition list
  2. /api/sdeo-dashboard/requisition-members-detail -> member details per requisition

Stores in:
  - requisition_detail   (one row per requisition per snapshot)
  - requisition_member   (one row per member per requisition per snapshot)

Replaces the old dummy tehsil_breakdown data in operational_activity_detail.
"""
from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from log_config import get_logger

log = get_logger("pera.requisition.ingest")

# ── Constants ────────────────────────────────────────────────
BASE_URL = "https://pera360.punjab.gov.pk/backend/api/sdeo-dashboard"

ENDPOINTS = {
    "requisitions_list":   f"{BASE_URL}/requisitions-details",
    "requisition_members": f"{BASE_URL}/requisition-members-detail",
}

HEADERS = {"accept": "application/json"}

# Rate-limit: sleep between API calls to avoid hammering the server
_API_SLEEP = 0.3  # seconds between calls


# ── DB access ────────────────────────────────────────────────
def _get_db():
    from analytics_db import get_analytics_db
    return get_analytics_db()


# ── API fetch helpers ────────────────────────────────────────
def _fetch_json(url: str, params: Dict[str, Any]) -> Any:
    """GET a URL and return parsed JSON (list or dict)."""
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.error("Failed to fetch %s params=%s: %s", url, params, e)
        return None


def _fetch_requisitions_for_tehsil(
    tehsil_id: int,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    """Fetch all requisitions for a tehsil within a date range."""
    data = _fetch_json(ENDPOINTS["requisitions_list"], {
        "tahsilId": tehsil_id,
        "startDate": start_date,
        "endDate": end_date,
    })
    if isinstance(data, list):
        return data
    return []


def _fetch_requisition_members(
    requisition_id: str,
    start_date: str,
    end_date: str,
) -> Optional[Dict[str, Any]]:
    """Fetch member details for a single requisition."""
    data = _fetch_json(ENDPOINTS["requisition_members"], {
        "requistionId": requisition_id,  # Note: API typo 'requistion' not 'requisition'
        "startDate": start_date,
        "endDate": end_date,
    })
    if isinstance(data, dict):
        return data
    return None


# ── Geo lookup ───────────────────────────────────────────────
_geo_cache: Dict[int, Dict[str, Any]] = {}


def _load_geo_cache(db) -> Dict[int, Dict[str, Any]]:
    """Load tehsil -> district -> division mapping from dim tables."""
    global _geo_cache
    if _geo_cache:
        return _geo_cache

    rows = db.fetch_all("""
        SELECT t.tehsil_id, t.tehsil_name,
               d.district_id, d.district_name,
               dv.division_id, dv.division_name
        FROM dim_tehsil t
        LEFT JOIN dim_district d ON t.district_id = d.district_id
        LEFT JOIN dim_division dv ON d.division_id = dv.division_id
        ORDER BY t.tehsil_id
    """)
    for r in rows:
        _geo_cache[r["tehsil_id"]] = {
            "tehsil_name":   r["tehsil_name"],
            "district_id":   r["district_id"],
            "district_name": r["district_name"],
            "division_id":   r["division_id"],
            "division_name": r["division_name"],
        }
    log.info("Geo cache loaded: %d tehsils", len(_geo_cache))
    return _geo_cache


# ── Storage ──────────────────────────────────────────────────
def _store_requisition(db, req: Dict[str, Any], tehsil_id: int, geo: Dict[str, Any],
                       snapshot: date) -> bool:
    """Insert/update a single requisition record."""
    try:
        db.execute("""
            INSERT INTO requisition_detail (
                snapshot_date, requisition_id, tehsil_id, tehsil_name,
                district_id, district_name, division_id, division_name,
                requisition_type_id, requisition_type_name,
                created_at, created_by_name, area_location,
                total_squad_members, arrived_members, arrival_percentage,
                arrival_latitude, arrival_longitude
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (requisition_id, snapshot_date) DO UPDATE SET
                requisition_type_id     = EXCLUDED.requisition_type_id,
                requisition_type_name   = EXCLUDED.requisition_type_name,
                created_at              = EXCLUDED.created_at,
                created_by_name         = EXCLUDED.created_by_name,
                area_location           = EXCLUDED.area_location,
                total_squad_members     = EXCLUDED.total_squad_members,
                arrived_members         = EXCLUDED.arrived_members,
                arrival_percentage      = EXCLUDED.arrival_percentage,
                arrival_latitude        = EXCLUDED.arrival_latitude,
                arrival_longitude       = EXCLUDED.arrival_longitude,
                ingested_at             = NOW()
        """, (
            snapshot.isoformat(),
            req.get("requisitionId", ""),
            tehsil_id,
            geo.get("tehsil_name", ""),
            geo.get("district_id"),
            geo.get("district_name", ""),
            geo.get("division_id"),
            geo.get("division_name", ""),
            req.get("requisitionType"),
            req.get("requisitionTypeName", ""),
            req.get("created"),
            req.get("createdByName", ""),       # populated from members API
            req.get("areaLocation", ""),        # populated from members API
            req.get("totalSquadMembers", 0),
            req.get("arrivedMembers", 0),
            req.get("arrivalPercentage", 0),
            req.get("arrivalLatitude"),
            req.get("arrivalLongitude"),
        ))
        return True
    except Exception as e:
        log.error("Failed to store requisition %s: %s", req.get("requisitionId"), e)
        return False


def _store_members(db, requisition_id: str, members: List[Dict[str, Any]],
                   snapshot: date) -> int:
    """Insert/update members for a requisition. Returns count stored."""
    stored = 0
    for m in members:
        try:
            db.execute("""
                INSERT INTO requisition_member (
                    snapshot_date, requisition_id, member_id, member_name,
                    is_completed, arrival_time, departure_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (requisition_id, member_id, snapshot_date) DO UPDATE SET
                    member_name     = EXCLUDED.member_name,
                    is_completed    = EXCLUDED.is_completed,
                    arrival_time    = EXCLUDED.arrival_time,
                    departure_time  = EXCLUDED.departure_time,
                    ingested_at     = NOW()
            """, (
                snapshot.isoformat(),
                requisition_id,
                m.get("memberId", ""),
                m.get("memberName", ""),
                m.get("isCompleted", False),
                m.get("arrivalTime"),
                m.get("departureTime"),
            ))
            stored += 1
        except Exception as e:
            log.error("Failed to store member %s for req %s: %s",
                      m.get("memberId"), requisition_id, e)
    return stored


# ── Main ingestion ───────────────────────────────────────────
def ingest_requisitions(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    tehsil_ids: Optional[List[int]] = None,
    fetch_members: bool = True,
) -> Dict[str, Any]:
    """
    Ingest requisition data from SDEO Dashboard APIs.

    Args:
        start_date: ISO date string (default: 30 days ago)
        end_date:   ISO date string (default: today)
        tehsil_ids: List of specific tehsil IDs to fetch (default: all from dim_tehsil)
        fetch_members: Whether to also fetch member details per requisition

    Returns:
        Summary dict with counts.
    """
    db = _get_db()
    if not db:
        log.error("Analytics DB not available")
        return {"error": "DB not available"}

    # Run migrations first
    from analytics_migrations import AnalyticsMigrator
    AnalyticsMigrator(db).migrate()

    today = date.today()
    snapshot = today

    if not start_date:
        start_date = (today - timedelta(days=30)).isoformat()
    if not end_date:
        end_date = today.isoformat()

    log.info("=== Requisition Ingestion: %s to %s ===", start_date, end_date)

    # Load geo mapping
    geo_cache = _load_geo_cache(db)

    # Get tehsil IDs to process
    if tehsil_ids is None:
        tehsil_rows = db.fetch_all(
            "SELECT tehsil_id FROM dim_tehsil WHERE is_active = TRUE ORDER BY tehsil_id"
        )
        tehsil_ids = [r["tehsil_id"] for r in tehsil_rows]

    if not tehsil_ids:
        log.warning("No tehsil IDs found in dim_tehsil — skipping requisition ingestion")
        return {"error": "No tehsils found", "tehsils": 0}

    log.info("Processing %d tehsils...", len(tehsil_ids))

    stats = {
        "tehsils_processed": 0,
        "tehsils_with_data": 0,
        "requisitions_stored": 0,
        "members_stored": 0,
        "api_errors": 0,
    }

    for tid in tehsil_ids:
        geo = geo_cache.get(tid, {})
        tehsil_name = geo.get("tehsil_name", f"TehsilID-{tid}")

        # Fetch requisitions list for this tehsil
        reqs = _fetch_requisitions_for_tehsil(tid, start_date, end_date)
        stats["tehsils_processed"] += 1

        if not reqs:
            continue

        stats["tehsils_with_data"] += 1
        log.info("  %s (id=%d): %d requisitions", tehsil_name, tid, len(reqs))

        for req in reqs:
            req_id = req.get("requisitionId", "")
            if not req_id:
                continue

            # Optionally fetch member details (which also gives createdByName + areaLocation)
            if fetch_members:
                time.sleep(_API_SLEEP)
                member_data = _fetch_requisition_members(req_id, start_date, end_date)
                if member_data:
                    # Enrich the requisition record with member-detail fields
                    req["createdByName"] = member_data.get("createdByName", "")
                    req["areaLocation"] = member_data.get("areaLocation", "")

                    # Store members
                    members = member_data.get("members", [])
                    if members:
                        cnt = _store_members(db, req_id, members, snapshot)
                        stats["members_stored"] += cnt
                else:
                    stats["api_errors"] += 1

            # Store requisition record
            if _store_requisition(db, req, tid, geo, snapshot):
                stats["requisitions_stored"] += 1

        # Rate-limit between tehsils
        time.sleep(_API_SLEEP)

    log.info("=== Requisition Ingestion Complete ===")
    log.info("  Tehsils: %d processed, %d with data",
             stats["tehsils_processed"], stats["tehsils_with_data"])
    log.info("  Requisitions stored: %d", stats["requisitions_stored"])
    log.info("  Members stored: %d", stats["members_stored"])
    if stats["api_errors"]:
        log.warning("  API errors: %d", stats["api_errors"])

    return stats


def ingest_requisitions_quick(days: int = 7) -> Dict[str, Any]:
    """Quick ingestion — only last N days, used by scheduler."""
    today = date.today()
    start = (today - timedelta(days=days - 1)).isoformat()
    end = today.isoformat()
    return ingest_requisitions(start_date=start, end_date=end)


# ── Snapshot helpers for queries ─────────────────────────────
def latest_requisition_snapshot() -> str:
    """SQL fragment for latest snapshot filter."""
    return "snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_detail)"


def latest_member_snapshot() -> str:
    """SQL fragment for latest member snapshot filter."""
    return "snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_member)"
