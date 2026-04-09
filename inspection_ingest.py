"""
PERA AI — Inspection Performance Ingestion

Fetches inspection performance data from the DG Dashboard APIs and stores
it in PostgreSQL for the chatbot to query.

APIs (base: https://pera360.punjab.gov.pk/backend):
  1. GET /api/dg-dashboard/inspection-performance/divisions
  2. GET /api/dg-dashboard/inspection-performance/districts
  3. GET /api/dg-dashboard/inspection-performance/tehsils
  4. GET /api/dg-dashboard/inspection-performance/tehsil/{tehsilId}/details

IMPORTANT: The 'challans' count in these APIs EXCLUDES overdue challans.

Schedule:
  - APIs 1-3 (summaries): every 5-10 minutes
  - API 4 (details):      every 1-2 hours

Usage:
  python inspection_ingest.py                   # all summaries
  python inspection_ingest.py --mode details    # tehsil details only
  python inspection_ingest.py --mode all        # summaries + details
"""
from __future__ import annotations

import argparse
import time
from datetime import date
from typing import Any, Dict, List, Optional

import requests

from log_config import get_logger

log = get_logger("pera.inspection_performance.ingest")

# ── API configuration ──────────────────────────────────────────
BASE_URL = "https://pera360.punjab.gov.pk/backend/api/dg-dashboard/inspection-performance"

ENDPOINTS = {
    "divisions": f"{BASE_URL}/divisions",
    "districts": f"{BASE_URL}/districts",
    "tehsils":   f"{BASE_URL}/tehsils",
}

DETAIL_URL_TEMPLATE = f"{BASE_URL}/tehsil/{{tehsil_id}}/details"

# SDEO Dashboard — inspections summary with officer breakdown
SDEO_BASE = "https://pera360.punjab.gov.pk/backend/api/sdeo-dashboard"
SDEO_INSPECTIONS_SUMMARY = f"{SDEO_BASE}/inspections-summary"

# PCM — officer inspection details (with fineAmount)
PCM_OFFICER_DETAILS = "https://pera360.punjab.gov.pk/backend/api/Pcm/officer-inspection-details"

# PCM — individual officer inspection records
PCM_OFFICER_INSPECTIONS = "https://pera360.punjab.gov.pk/backend/api/Pcm/officer-inspections"

HEADERS = {"accept": "application/json"}
_API_TIMEOUT = 30
_API_SLEEP = 0.5  # seconds between calls (rate limiting)


# ── DB helpers ────────────────────────────────────────────────
def _get_db():
    from analytics_db import get_analytics_db
    return get_analytics_db()


def _ensure_schema(db):
    """Run pending migrations so the tables exist."""
    from analytics_migrations import AnalyticsMigrator
    AnalyticsMigrator(db).migrate()


# ── Fetch helpers ─────────────────────────────────────────────
def _fetch_json(url: str, params: dict = None) -> List[Dict]:
    """GET a URL and return parsed JSON (expecting a list)."""
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=_API_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "data" in data:
            return data["data"] if isinstance(data["data"], list) else [data["data"]]
        return [data] if data else []
    except Exception as e:
        log.error("Failed to fetch %s: %s", url, e)
        return []


def fetch_divisions() -> List[Dict]:
    """Fetch division-level inspection performance."""
    log.info("Fetching inspection performance: divisions")
    rows = _fetch_json(ENDPOINTS["divisions"])
    log.info("  -> Got %d division records", len(rows))
    return rows


def fetch_districts(db=None) -> List[Dict]:
    """
    Fetch district-level inspection performance for ALL divisions.
    Requires divisionId param per call, so we iterate over stored divisions.
    Enriches each record with parent division info.
    """
    if db is None:
        db = _get_db()

    # Get division IDs from stored data (or from dim_division)
    div_rows = db.fetch_all(
        "SELECT DISTINCT division_id, division_name FROM inspection_performance "
        "WHERE level = 'division' AND division_id IS NOT NULL "
        "AND snapshot_date = (SELECT MAX(snapshot_date) FROM inspection_performance "
        "                     WHERE level = 'division')"
    )
    if not div_rows:
        # Fallback to dim_division
        try:
            div_rows = db.fetch_all(
                "SELECT division_id, division_name FROM dim_division"
            )
        except Exception:
            div_rows = []

    if not div_rows:
        log.warning("No divisions found — fetch divisions first")
        return []

    all_districts: List[Dict] = []
    log.info("Fetching inspection districts for %d divisions", len(div_rows))

    for div in div_rows:
        div_id = div["division_id"]
        div_name = div["division_name"]
        log.info("  Fetching districts for division: %s (ID=%d)", div_name, div_id)

        rows = _fetch_json(ENDPOINTS["districts"], params={"divisionId": div_id})
        # Enrich each record with parent division info
        for r in rows:
            r["divisionId"] = div_id
            r["divisionName"] = div_name
        all_districts.extend(rows)
        time.sleep(_API_SLEEP)

    log.info("  -> Got %d total district records", len(all_districts))
    return all_districts


def fetch_tehsils(db=None) -> List[Dict]:
    """
    Fetch tehsil-level inspection performance for ALL districts.
    Requires districtId param per call, so we iterate over stored districts.
    Enriches each record with parent district + division info.
    """
    if db is None:
        db = _get_db()

    # Get district IDs from stored data
    dist_rows = db.fetch_all(
        "SELECT DISTINCT district_id, district_name, division_id, division_name "
        "FROM inspection_performance "
        "WHERE level = 'district' AND district_id IS NOT NULL "
        "AND snapshot_date = (SELECT MAX(snapshot_date) FROM inspection_performance "
        "                     WHERE level = 'district')"
    )
    if not dist_rows:
        # Fallback to dim_district
        try:
            dist_rows = db.fetch_all(
                "SELECT district_id, district_name, division_id FROM dim_district"
            )
        except Exception:
            dist_rows = []

    if not dist_rows:
        log.warning("No districts found — fetch districts first")
        return []

    all_tehsils: List[Dict] = []
    log.info("Fetching inspection tehsils for %d districts", len(dist_rows))

    for dist in dist_rows:
        dist_id = dist["district_id"]
        dist_name = dist["district_name"]
        div_id = dist.get("division_id")
        div_name = dist.get("division_name")
        log.info("  Fetching tehsils for district: %s (ID=%d)", dist_name, dist_id)

        rows = _fetch_json(ENDPOINTS["tehsils"], params={"districtId": dist_id})
        # Enrich each record with parent district + division info
        for r in rows:
            r["districtId"] = dist_id
            r["districtName"] = dist_name
            r["divisionId"] = div_id
            r["divisionName"] = div_name
        all_tehsils.extend(rows)
        time.sleep(_API_SLEEP)

    log.info("  -> Got %d total tehsil records", len(all_tehsils))
    return all_tehsils


def fetch_tehsil_details(tehsil_id: int) -> List[Dict]:
    """Fetch detailed inspection records for a specific tehsil."""
    url = DETAIL_URL_TEMPLATE.format(tehsil_id=tehsil_id)
    return _fetch_json(url)


def fetch_inspections_summary(
    tehsil_id: int,
    start_date: str = None,
    end_date: str = None,
    requisition_type_id: int = None,
) -> Optional[Dict]:
    """
    Fetch SDEO inspections summary for a tehsil with officer breakdown.
    GET /api/sdeo-dashboard/inspections-summary?tehsilId=X&startDate=Y&endDate=Z

    Returns dict with tehsil totals + officers array, or None on failure.
    """
    params: Dict[str, Any] = {"tehsilId": tehsil_id}
    if start_date:
        params["startDate"] = start_date
    if end_date:
        params["endDate"] = end_date
    if requisition_type_id:
        params["RequisitionTypeId"] = requisition_type_id

    try:
        resp = requests.get(
            SDEO_INSPECTIONS_SUMMARY,
            headers=HEADERS,
            params=params,
            timeout=_API_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            return data
        return None
    except Exception as e:
        log.error("Failed to fetch inspections-summary for tehsil %d: %s", tehsil_id, e)
        return None


def fetch_officer_inspection_details(
    tehsil_id: int,
    requisition_type_id: int = 0,
    start_date: str = None,
    end_date: str = None,
    **_kwargs,
) -> List[Dict]:
    """
    Fetch PCM officer inspection details for a tehsil.
    GET /api/Pcm/officer-inspection-details

    NOTE: Only tehsilId + requisitionTypeId are passed. The PCM API uses
    different division/district IDs than the DG Dashboard API, so passing
    those causes mismatches and empty responses.

    Returns list of officers with totalInspections, totalChallans, fineAmount, etc.
    """
    params: Dict[str, Any] = {
        "tehsilId": tehsil_id,
        "requisitionTypeId": requisition_type_id,
    }
    if start_date:
        params["startDate"] = start_date
    if end_date:
        params["endDate"] = end_date

    return _fetch_json(PCM_OFFICER_DETAILS, params=params)


def fetch_officer_inspections(
    officer_id: str,
    tehsil_id: int,
    from_date: str = None,
    to_date: str = None,
    requisition_type_id: int = None,
    **_kwargs,
) -> List[Dict]:
    """
    Fetch individual inspection records for one officer in one tehsil.
    GET /api/Pcm/officer-inspections?officerId=X&tehsilId=Y&fromDate=Z&toDate=W

    NOTE: Only tehsilId + officerId are passed. The PCM API uses different
    division/district IDs than the DG Dashboard API.

    Returns list of inspection records (ownerName, cnic, address, case type, fine, lat/lng).
    """
    params: Dict[str, Any] = {
        "officerId": officer_id,
        "tehsilId": tehsil_id,
    }
    if from_date:
        params["fromDate"] = from_date
    if to_date:
        params["toDate"] = to_date
    if requisition_type_id is not None:
        params["requisitionTypeId"] = requisition_type_id

    return _fetch_json(PCM_OFFICER_INSPECTIONS, params=params)


# ── Transform ─────────────────────────────────────────────────
def _transform_summary(record: dict, level: str) -> dict:
    """
    Transform an API record to our DB schema.
    Maps camelCase → snake_case and adds the level discriminator.
    """
    return {
        "level":         level,
        "division_id":   record.get("divisionId"),
        "division_name": record.get("divisionName"),
        "district_id":   record.get("districtId"),
        "district_name": record.get("districtName"),
        "tehsil_id":     record.get("tehsilId"),
        "tehsil_name":   record.get("tehsilName"),
        "total_actions": record.get("totalActions", 0),
        "challans":      record.get("challans", 0),       # excludes overdue!
        "firs":          record.get("fiRs", 0),
        "warnings":      record.get("warnings", 0),
        "no_offenses":   record.get("noOffenses", 0),
        "sealed":        record.get("sealed", 0),
        "snapshot_date": date.today().isoformat(),
    }


# ── Geo enrichment cache ─────────────────────────────────────
_geo_cache: Dict[str, Dict] = {}


def _build_geo_cache(db) -> None:
    """Build division_id -> name and district_id -> (name, division_id, division_name) caches."""
    global _geo_cache
    if _geo_cache:
        return

    try:
        divs = db.fetch_all(
            "SELECT division_id, division_name FROM dim_division"
        )
        _geo_cache["div"] = {r["division_id"]: r["division_name"] for r in divs}
    except Exception:
        _geo_cache["div"] = {}

    try:
        dists = db.fetch_all(
            "SELECT district_id, district_name, division_id FROM dim_district"
        )
        _geo_cache["dist"] = {
            r["district_id"]: {
                "district_name": r["district_name"],
                "division_id": r["division_id"],
            }
            for r in dists
        }
    except Exception:
        _geo_cache["dist"] = {}


def _enrich_district(record: dict) -> dict:
    """If district record is missing division info, try to enrich from cache."""
    if record.get("division_name"):
        return record
    div_id = record.get("division_id")
    if div_id and "div" in _geo_cache and div_id in _geo_cache["div"]:
        record["division_name"] = _geo_cache["div"][div_id]
    return record


def _enrich_tehsil(record: dict) -> dict:
    """If tehsil record is missing district/division info, try to enrich from cache."""
    dist_id = record.get("district_id")
    if dist_id and not record.get("district_name"):
        dist_info = _geo_cache.get("dist", {}).get(dist_id, {})
        record["district_name"] = dist_info.get("district_name")
        if not record.get("division_id"):
            record["division_id"] = dist_info.get("division_id")
    if record.get("division_id") and not record.get("division_name"):
        record["division_name"] = _geo_cache.get("div", {}).get(record["division_id"])
    return record


# ── Store ─────────────────────────────────────────────────────
def _store_summary_batch(records: List[dict], db) -> int:
    """
    UPSERT inspection_performance records.
    Returns number of rows stored.
    """
    if not records:
        return 0

    stored = 0
    sql = """
        INSERT INTO inspection_performance
            (level, division_id, division_name, district_id, district_name,
             tehsil_id, tehsil_name, total_actions, challans, firs,
             warnings, no_offenses, sealed, snapshot_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (level, COALESCE(division_id, 0), COALESCE(district_id, 0),
                     COALESCE(tehsil_id, 0), snapshot_date)
        DO UPDATE SET
            total_actions = EXCLUDED.total_actions,
            challans      = EXCLUDED.challans,
            firs          = EXCLUDED.firs,
            warnings      = EXCLUDED.warnings,
            no_offenses   = EXCLUDED.no_offenses,
            sealed        = EXCLUDED.sealed,
            division_name = EXCLUDED.division_name,
            district_name = EXCLUDED.district_name,
            tehsil_name   = EXCLUDED.tehsil_name,
            ingested_at   = NOW()
    """
    with db.connection() as conn:
        for r in records:
            try:
                conn.execute(sql, (
                    r["level"],
                    r.get("division_id"),  r.get("division_name"),
                    r.get("district_id"),  r.get("district_name"),
                    r.get("tehsil_id"),    r.get("tehsil_name"),
                    r.get("total_actions", 0),
                    r.get("challans", 0),
                    r.get("firs", 0),
                    r.get("warnings", 0),
                    r.get("no_offenses", 0),
                    r.get("sealed", 0),
                    r.get("snapshot_date", date.today().isoformat()),
                ))
                stored += 1
            except Exception as e:
                log.warning("Failed to store %s record %s: %s",
                            r.get("level"), r.get("division_name") or r.get("district_name") or r.get("tehsil_name"), e)

    return stored


# ── Main ingestion orchestrators ──────────────────────────────

def ingest_inspection_summaries() -> Dict[str, int]:
    """
    Fetch and store all 3 summary levels (divisions, districts, tehsils).
    This is the "fast" ingestion meant to run every 5-10 minutes.

    Returns dict of {level: count_stored}.
    """
    db = _get_db()
    if not db:
        log.error("Analytics DB not available")
        return {}

    _ensure_schema(db)
    _build_geo_cache(db)

    results: Dict[str, int] = {}

    # 1. Divisions
    raw = fetch_divisions()
    transformed = [_transform_summary(r, "division") for r in raw]
    results["divisions"] = _store_summary_batch(transformed, db)
    log.info("Stored %d division records", results["divisions"])
    time.sleep(_API_SLEEP)

    # 2. Districts (needs division IDs from step 1)
    raw = fetch_districts(db=db)
    transformed = [_enrich_district(_transform_summary(r, "district")) for r in raw]
    results["districts"] = _store_summary_batch(transformed, db)
    log.info("Stored %d district records", results["districts"])
    time.sleep(_API_SLEEP)

    # 3. Tehsils (needs district IDs from step 2)
    raw = fetch_tehsils(db=db)
    transformed = [_enrich_tehsil(_transform_summary(r, "tehsil")) for r in raw]
    results["tehsils"] = _store_summary_batch(transformed, db)
    log.info("Stored %d tehsil records", results["tehsils"])

    total = sum(results.values())
    log.info("Inspection summary ingestion complete: %d total records (%s)",
             total, ", ".join(f"{k}={v}" for k, v in results.items()))

    return results


def ingest_inspection_details() -> int:
    """
    Fetch and store tehsil-level detail records.
    This is the "slow" ingestion meant to run every 1-2 hours.

    Reads tehsil IDs from the summary table, then fetches /tehsil/{id}/details
    for each one.

    Returns total number of detail records stored.
    """
    db = _get_db()
    if not db:
        log.error("Analytics DB not available")
        return 0

    _ensure_schema(db)

    # Get all tehsils from summary table
    tehsil_rows = db.fetch_all(
        "SELECT DISTINCT tehsil_id, tehsil_name, district_id, district_name, "
        "division_id, division_name "
        "FROM inspection_performance "
        "WHERE level = 'tehsil' AND tehsil_id IS NOT NULL AND tehsil_id > 0 "
        "AND snapshot_date = (SELECT MAX(snapshot_date) FROM inspection_performance "
        "                     WHERE level = 'tehsil')"
    )

    if not tehsil_rows:
        log.warning("No tehsils found in inspection_performance — run summaries first")
        return 0

    log.info("Starting detail ingestion for %d tehsils", len(tehsil_rows))
    total = 0

    for i, t in enumerate(tehsil_rows, 1):
        tid = t["tehsil_id"]
        tname = t["tehsil_name"] or f"tehsil-{tid}"
        try:
            raw = fetch_tehsil_details(tid)
            if not raw:
                continue

            geo = {
                "tehsil_name":   t["tehsil_name"],
                "district_id":   t["district_id"],
                "district_name": t["district_name"],
                "division_id":   t["division_id"],
                "division_name": t["division_name"],
            }
            stored = _store_detail_batch(raw, tid, geo, db)
            total += stored
            if stored > 0:
                log.info("  [%d/%d] %s: %d detail records",
                         i, len(tehsil_rows), tname, stored)
        except Exception as e:
            log.error("  [%d/%d] %s: detail fetch failed: %s",
                      i, len(tehsil_rows), tname, e)

        time.sleep(_API_SLEEP)

    log.info("Inspection detail ingestion complete: %d total records", total)
    return total


def _store_detail_batch(
    records: List[Dict], tehsil_id: int, geo: dict, db,
) -> int:
    """
    Store detail records for a tehsil into inspection_performance_detail.
    Returns number of rows stored.

    NOTE: The exact schema depends on the detail API response format.
    This implementation stores the full record as JSON and extracts
    known fields. Will be refined when the user provides API 4's response.
    """
    if not records:
        return 0

    import json
    stored = 0
    today = date.today().isoformat()

    sql = """
        INSERT INTO inspection_performance_detail
            (snapshot_date, tehsil_id, tehsil_name, district_id, district_name,
             division_id, division_name, record_id, action_type, action_date,
             officer_name, location, details_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tehsil_id, COALESCE(record_id, ''), snapshot_date)
        DO UPDATE SET
            action_type  = EXCLUDED.action_type,
            action_date  = EXCLUDED.action_date,
            officer_name = EXCLUDED.officer_name,
            location     = EXCLUDED.location,
            details_json = EXCLUDED.details_json,
            ingested_at  = NOW()
    """
    with db.connection() as conn:
        for r in records:
            # Extract fields — adjust once exact API response is known
            record_id = str(r.get("id", r.get("inspectionId", r.get("recordId", ""))))
            if not record_id:
                record_id = f"{tehsil_id}_{stored}"

            try:
                conn.execute(sql, (
                    today,
                    tehsil_id,
                    geo.get("tehsil_name"),
                    geo.get("district_id"),
                    geo.get("district_name"),
                    geo.get("division_id"),
                    geo.get("division_name"),
                    record_id,
                    r.get("actionType") or r.get("action_type"),
                    r.get("actionDate") or r.get("action_date"),
                    r.get("officerName") or r.get("officer_name"),
                    r.get("location") or r.get("area"),
                    json.dumps(r, default=str),
                ))
                stored += 1
            except Exception as e:
                log.warning("Failed to store detail record for tehsil %s: %s", tehsil_id, e)

    return stored


# ── SDEO Inspections Summary (officer-level) ─────────────────

def _store_officer_summary(
    data: Dict, tehsil_id: int, geo: dict, db,
    start_date: str = None, end_date: str = None,
) -> int:
    """
    Store officer-level inspection summary records.
    One row per officer per tehsil per date range per snapshot.
    """
    officers = data.get("officers", [])
    if not officers:
        return 0

    today = date.today().isoformat()
    stored = 0

    sql = """
        INSERT INTO inspection_officer_summary
            (snapshot_date, tehsil_id, tehsil_name, district_id, district_name,
             division_id, division_name, start_date, end_date,
             total_actions, total_challans, total_firs, total_warnings,
             total_no_offenses, total_sealed,
             officer_name, officer_challans, officer_firs, officer_warnings,
             officer_no_offenses, officer_inspections, officer_sealed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tehsil_id, COALESCE(officer_name, ''),
                     COALESCE(start_date, '1900-01-01'::date),
                     COALESCE(end_date, '1900-01-01'::date),
                     snapshot_date)
        DO UPDATE SET
            total_actions     = EXCLUDED.total_actions,
            total_challans    = EXCLUDED.total_challans,
            total_firs        = EXCLUDED.total_firs,
            total_warnings    = EXCLUDED.total_warnings,
            total_no_offenses = EXCLUDED.total_no_offenses,
            total_sealed      = EXCLUDED.total_sealed,
            officer_challans    = EXCLUDED.officer_challans,
            officer_firs        = EXCLUDED.officer_firs,
            officer_warnings    = EXCLUDED.officer_warnings,
            officer_no_offenses = EXCLUDED.officer_no_offenses,
            officer_inspections = EXCLUDED.officer_inspections,
            officer_sealed      = EXCLUDED.officer_sealed,
            tehsil_name   = EXCLUDED.tehsil_name,
            district_name = EXCLUDED.district_name,
            division_name = EXCLUDED.division_name,
            ingested_at   = NOW()
    """

    # Tehsil-level totals from the response
    t_actions = data.get("totalActions", 0)
    t_challans = data.get("challans", 0)
    t_firs = data.get("fiRs", 0)
    t_warnings = data.get("warnings", 0)
    t_no_offenses = data.get("noOffenses", 0)
    t_sealed = data.get("sealed", 0)
    t_name = data.get("tehsilNameEnglish") or geo.get("tehsil_name")

    with db.connection() as conn:
        for off in officers:
            oname = off.get("officerName", "").strip()
            if not oname:
                continue
            try:
                conn.execute(sql, (
                    today, tehsil_id, t_name,
                    geo.get("district_id"), geo.get("district_name"),
                    geo.get("division_id"), geo.get("division_name"),
                    start_date, end_date,
                    t_actions, t_challans, t_firs, t_warnings,
                    t_no_offenses, t_sealed,
                    oname,
                    off.get("challan", 0),
                    off.get("fir", 0),
                    off.get("warning", 0),
                    off.get("noOffense", 0),
                    off.get("inspection", 0),
                    off.get("sealed", 0),
                ))
                stored += 1
            except Exception as e:
                log.warning("Failed to store officer record (%s, tehsil %d): %s",
                            oname, tehsil_id, e)

    return stored


def ingest_inspection_officer_summaries(
    start_date: str = None,
    end_date: str = None,
) -> Dict[str, int]:
    """
    Fetch SDEO inspections-summary for all tehsils (with officer breakdown).
    This is the "slow" ingestion meant to run every 1-2 hours.

    Args:
        start_date: date filter (YYYY-MM-DD). Defaults to start of current year.
        end_date: date filter (YYYY-MM-DD). Defaults to today.

    Returns dict with stats: {tehsils_processed, officers_stored, errors}.
    """
    # API requires date filters — default to current year if not specified
    if not start_date:
        start_date = f"{date.today().year}-01-01"
    if not end_date:
        end_date = date.today().isoformat()
    db = _get_db()
    if not db:
        log.error("Analytics DB not available")
        return {}

    _ensure_schema(db)

    # Get all tehsils from the inspection_performance summary table
    tehsil_rows = db.fetch_all(
        "SELECT DISTINCT tehsil_id, tehsil_name, district_id, district_name, "
        "division_id, division_name "
        "FROM inspection_performance "
        "WHERE level = 'tehsil' AND tehsil_id IS NOT NULL AND tehsil_id > 0 "
        "AND snapshot_date = (SELECT MAX(snapshot_date) FROM inspection_performance "
        "                     WHERE level = 'tehsil')"
    )

    if not tehsil_rows:
        log.warning("No tehsils in inspection_performance — run summaries first")
        return {"tehsils_processed": 0, "officers_stored": 0, "errors": 0}

    log.info("Starting SDEO inspections-summary ingestion for %d tehsils "
             "(date range: %s to %s)",
             len(tehsil_rows), start_date or "all", end_date or "all")

    total_officers = 0
    total_processed = 0
    total_errors = 0

    for i, t in enumerate(tehsil_rows, 1):
        tid = t["tehsil_id"]
        tname = t["tehsil_name"] or f"tehsil-{tid}"

        try:
            data = fetch_inspections_summary(tid, start_date, end_date)
            if not data:
                continue

            geo = {
                "tehsil_name":   t["tehsil_name"],
                "district_id":   t["district_id"],
                "district_name": t["district_name"],
                "division_id":   t["division_id"],
                "division_name": t["division_name"],
            }

            stored = _store_officer_summary(data, tid, geo, db, start_date, end_date)
            total_officers += stored
            total_processed += 1

            if stored > 0 and (i % 20 == 0 or i == len(tehsil_rows)):
                log.info("  [%d/%d] Progress: %d tehsils, %d officers stored",
                         i, len(tehsil_rows), total_processed, total_officers)

        except Exception as e:
            log.error("  [%d/%d] %s (ID=%d): failed: %s",
                      i, len(tehsil_rows), tname, tid, e)
            total_errors += 1

        time.sleep(_API_SLEEP)

    stats = {
        "tehsils_processed": total_processed,
        "officers_stored": total_officers,
        "errors": total_errors,
    }
    log.info("SDEO inspections-summary ingestion complete: %s", stats)
    return stats


# ── PCM Officer Inspection Details ─────────────────────────────

def _store_officer_inspection_batch(
    records: List[Dict], tehsil_id: int, geo: dict, db,
    requisition_type_id: int = 0,
    start_date: str = None, end_date: str = None,
) -> int:
    """Store PCM officer inspection detail records."""
    if not records:
        return 0

    today = date.today().isoformat()
    stored = 0

    sql = """
        INSERT INTO officer_inspection_detail
            (snapshot_date, tehsil_id, tehsil_name, district_id, district_name,
             division_id, division_name, requisition_type_id, start_date, end_date,
             user_id, officer_name, total_inspections, total_challans,
             fine_amount, sealed, arrest_case)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tehsil_id, COALESCE(user_id, ''),
                     COALESCE(requisition_type_id, 0),
                     COALESCE(start_date, '1900-01-01'::date),
                     COALESCE(end_date, '1900-01-01'::date),
                     snapshot_date)
        DO UPDATE SET
            officer_name      = EXCLUDED.officer_name,
            total_inspections = EXCLUDED.total_inspections,
            total_challans    = EXCLUDED.total_challans,
            fine_amount       = EXCLUDED.fine_amount,
            sealed            = EXCLUDED.sealed,
            arrest_case       = EXCLUDED.arrest_case,
            tehsil_name       = EXCLUDED.tehsil_name,
            district_name     = EXCLUDED.district_name,
            division_name     = EXCLUDED.division_name,
            ingested_at       = NOW()
    """
    with db.connection() as conn:
        for r in records:
            oname = (r.get("fullName") or "").strip()
            uid = r.get("userId", "")
            if not oname:
                continue
            try:
                conn.execute(sql, (
                    today, tehsil_id,
                    geo.get("tehsil_name"),
                    geo.get("district_id"), geo.get("district_name"),
                    geo.get("division_id"), geo.get("division_name"),
                    requisition_type_id, start_date, end_date,
                    uid, oname,
                    r.get("totalInspections", 0),
                    r.get("totalChallans", 0),
                    r.get("fineAmount", 0),
                    r.get("sealed", 0),
                    r.get("arrestCase", 0),
                ))
                stored += 1
            except Exception as e:
                log.warning("Failed to store PCM officer record (%s, tehsil %d): %s",
                            oname, tehsil_id, e)

    return stored


def ingest_officer_inspection_details(
    requisition_type_id: int = 0,
) -> Dict[str, int]:
    """
    Fetch PCM officer-inspection-details for all tehsils.
    Runs every 1-2 hours alongside other detail ingestions.

    Returns dict with stats.
    """
    db = _get_db()
    if not db:
        log.error("Analytics DB not available")
        return {}

    _ensure_schema(db)

    # Get all tehsils with their geo hierarchy
    tehsil_rows = db.fetch_all(
        "SELECT DISTINCT tehsil_id, tehsil_name, district_id, district_name, "
        "division_id, division_name "
        "FROM inspection_performance "
        "WHERE level = 'tehsil' AND tehsil_id IS NOT NULL AND tehsil_id > 0 "
        "AND snapshot_date = (SELECT MAX(snapshot_date) FROM inspection_performance "
        "                     WHERE level = 'tehsil')"
    )

    if not tehsil_rows:
        log.warning("No tehsils in inspection_performance — run summaries first")
        return {"tehsils_processed": 0, "officers_stored": 0, "errors": 0}

    log.info("Starting PCM officer-inspection-details ingestion for %d tehsils "
             "(reqTypeId=%d)", len(tehsil_rows), requisition_type_id)

    total_officers = 0
    total_processed = 0
    total_errors = 0

    for i, t in enumerate(tehsil_rows, 1):
        tid = t["tehsil_id"]
        tname = t["tehsil_name"] or f"tehsil-{tid}"

        try:
            records = fetch_officer_inspection_details(
                tehsil_id=tid,
                requisition_type_id=requisition_type_id,
            )
            if not records:
                total_processed += 1
                continue

            geo = {
                "tehsil_name":   t["tehsil_name"],
                "district_id":   t["district_id"],
                "district_name": t["district_name"],
                "division_id":   t["division_id"],
                "division_name": t["division_name"],
            }

            stored = _store_officer_inspection_batch(
                records, tid, geo, db,
                requisition_type_id=requisition_type_id,
            )
            total_officers += stored
            total_processed += 1

            if i % 20 == 0 or i == len(tehsil_rows):
                log.info("  [%d/%d] Progress: %d tehsils, %d officers stored",
                         i, len(tehsil_rows), total_processed, total_officers)

        except Exception as e:
            log.error("  [%d/%d] %s (ID=%d): failed: %s",
                      i, len(tehsil_rows), tname, tid, e)
            total_errors += 1

        time.sleep(_API_SLEEP)

    stats = {
        "tehsils_processed": total_processed,
        "officers_stored": total_officers,
        "errors": total_errors,
    }
    log.info("PCM officer-inspection-details ingestion complete: %s", stats)
    return stats


# ── PCM Individual Officer Inspections ─────────────────────────

def _store_officer_inspections_batch(
    records: List[Dict], officer_id: str, officer_name: str,
    tehsil_id: int, geo: dict, db,
    from_date: str = None, to_date: str = None,
) -> int:
    """Store individual inspection records for one officer."""
    if not records:
        return 0

    today = date.today().isoformat()
    stored = 0

    sql = """
        INSERT INTO officer_inspection_record
            (snapshot_date, tehsil_id, tehsil_name, district_id, district_name,
             division_id, division_name, officer_id, officer_name,
             owner_name, cnic, address, is_challan, is_warning,
             is_no_offense, is_arrest, is_confiscated,
             fine_amount, latitude, longitude, from_date, to_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (tehsil_id, officer_id, COALESCE(cnic, ''),
                     COALESCE(owner_name, ''), COALESCE(fine_amount, 0),
                     COALESCE(from_date, '1900-01-01'::date),
                     COALESCE(to_date, '1900-01-01'::date),
                     snapshot_date)
        DO UPDATE SET
            address        = EXCLUDED.address,
            is_challan     = EXCLUDED.is_challan,
            is_warning     = EXCLUDED.is_warning,
            is_no_offense  = EXCLUDED.is_no_offense,
            is_arrest      = EXCLUDED.is_arrest,
            is_confiscated = EXCLUDED.is_confiscated,
            fine_amount    = EXCLUDED.fine_amount,
            latitude       = EXCLUDED.latitude,
            longitude      = EXCLUDED.longitude,
            officer_name   = EXCLUDED.officer_name,
            ingested_at    = NOW()
    """

    def _parse_bool(val):
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.lower() in ("true", "1", "yes")
        return bool(val)

    with db.connection() as conn:
        for r in records:
            try:
                conn.execute(sql, (
                    today, tehsil_id,
                    geo.get("tehsil_name"),
                    geo.get("district_id"), geo.get("district_name"),
                    geo.get("division_id"), geo.get("division_name"),
                    officer_id, officer_name,
                    r.get("ownerName"),
                    r.get("cnic"),
                    r.get("address"),
                    _parse_bool(r.get("challanCase", False)),
                    _parse_bool(r.get("warningCase", False)),
                    _parse_bool(r.get("noOffense", False)),
                    _parse_bool(r.get("arrestCase", False)),
                    _parse_bool(r.get("confiscated", False)),
                    r.get("fineAmount", 0),
                    r.get("latitude"),
                    r.get("longitude"),
                    from_date, to_date,
                ))
                stored += 1
            except Exception as e:
                log.warning("Failed to store inspection record (officer=%s, tehsil=%d): %s",
                            officer_id[:8], tehsil_id, e)

    return stored


def ingest_officer_inspections(
    from_date: str = None,
    to_date: str = None,
    max_officers_per_tehsil: int = 10,
) -> Dict[str, int]:
    """
    Fetch individual inspection records for all officers across all tehsils.
    Reads officer IDs from officer_inspection_detail (populated by API 5),
    then fetches /api/Pcm/officer-inspections for each.

    This is a heavy operation — rate limited and capped per tehsil.
    Meant to run every 1-2 hours.

    Args:
        from_date: Start date (YYYY-MM-DD). Defaults to start of current year.
        to_date:   End date (YYYY-MM-DD). Defaults to today.
        max_officers_per_tehsil: Cap how many officers to fetch per tehsil (top by inspections).

    Returns dict with stats.
    """
    db = _get_db()
    if not db:
        return {}

    _ensure_schema(db)

    if not from_date:
        from_date = f"{date.today().year}-01-01"
    if not to_date:
        to_date = date.today().isoformat()

    # Get officers grouped by tehsil from the PCM details table
    # Only fetch top N officers per tehsil (by inspections) to limit API calls
    officer_rows = db.fetch_all(
        """SELECT tehsil_id, tehsil_name, district_id, district_name,
                  division_id, division_name, user_id, officer_name,
                  total_inspections
           FROM officer_inspection_detail
           WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM officer_inspection_detail)
             AND user_id IS NOT NULL AND user_id != ''
           ORDER BY tehsil_id, total_inspections DESC"""
    )

    if not officer_rows:
        log.warning("No officers in officer_inspection_detail — run PCM officer details first")
        return {"officers_processed": 0, "records_stored": 0, "errors": 0}

    # Group by tehsil and cap
    from collections import defaultdict
    by_tehsil: Dict[int, List[Dict]] = defaultdict(list)
    for r in officer_rows:
        tid = r["tehsil_id"]
        if len(by_tehsil[tid]) < max_officers_per_tehsil:
            by_tehsil[tid].append(dict(r))

    total_officers = sum(len(v) for v in by_tehsil.values())
    log.info("Starting PCM officer-inspections ingestion: %d tehsils, %d officers "
             "(date: %s to %s, max %d/tehsil)",
             len(by_tehsil), total_officers, from_date, to_date,
             max_officers_per_tehsil)

    total_records = 0
    total_processed = 0
    total_errors = 0
    officer_count = 0

    for tid, officers in by_tehsil.items():
        geo = {
            "tehsil_name":   officers[0].get("tehsil_name"),
            "district_id":   officers[0].get("district_id"),
            "district_name": officers[0].get("district_name"),
            "division_id":   officers[0].get("division_id"),
            "division_name": officers[0].get("division_name"),
        }

        for off in officers:
            uid = off["user_id"]
            oname = off.get("officer_name", "")
            officer_count += 1

            try:
                records = fetch_officer_inspections(
                    officer_id=uid,
                    tehsil_id=tid,
                    from_date=from_date,
                    to_date=to_date,
                )
                if records:
                    stored = _store_officer_inspections_batch(
                        records, uid, oname, tid, geo, db,
                        from_date=from_date, to_date=to_date,
                    )
                    total_records += stored
                total_processed += 1
            except Exception as e:
                log.error("Officer %s (tehsil %d) failed: %s", uid[:8], tid, e)
                total_errors += 1

            time.sleep(_API_SLEEP)

        if officer_count % 50 == 0:
            log.info("  Progress: %d/%d officers, %d records stored",
                     officer_count, total_officers, total_records)

    stats = {
        "officers_processed": total_processed,
        "records_stored": total_records,
        "errors": total_errors,
    }
    log.info("PCM officer-inspections ingestion complete: %s", stats)
    return stats


# ── CLI ───────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest inspection performance data")
    parser.add_argument(
        "--mode",
        choices=["summaries", "details", "officers", "pcm_officers",
                 "pcm_inspections", "all"],
        default="summaries",
        help="What to ingest: summaries (fast), details (DG), officers (SDEO), or all",
    )
    parser.add_argument("--start-date", help="Start date filter for officer summaries (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date filter for officer summaries (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.mode in ("summaries", "all"):
        results = ingest_inspection_summaries()
        print(f"Summaries: {results}")

    if args.mode in ("details", "all"):
        total = ingest_inspection_details()
        print(f"Details: {total} records stored")

    if args.mode in ("officers", "all"):
        stats = ingest_inspection_officer_summaries(
            start_date=args.start_date,
            end_date=args.end_date,
        )
        print(f"Officer summaries: {stats}")

    if args.mode in ("pcm_officers", "all"):
        stats = ingest_officer_inspection_details()
        print(f"PCM officer details: {stats}")

    if args.mode in ("pcm_inspections", "all"):
        stats = ingest_officer_inspections(
            from_date=args.start_date,
            to_date=args.end_date,
        )
        print(f"PCM inspections: {stats}")
