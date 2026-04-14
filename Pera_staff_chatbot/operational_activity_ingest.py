#!/usr/bin/env python
"""
PERA AI — Operational Activity Data Ingestion

Fetches operational activity data from PERA360 APIs and stores it
in the `operational_activity` PostgreSQL table.

Endpoints:
    GET /api/operational-activity/divisions
    GET /api/operational-activity/districts
    GET /api/operational-activity/tehsils
    GET /api/operational-activity/tehsil-breakdown

Each endpoint returns requisition-type counts (Price Control,
Anti-Encroachment, Eviction, Anti-Hoarding, Public Nuisance)
aggregated at the corresponding geographic level.

Usage:
    # Fetch all levels
    python operational_activity_ingest.py

    # Fetch only divisions
    python operational_activity_ingest.py --level divisions

    # Fetch with date range
    python operational_activity_ingest.py --start 2026-01-01 --end 2026-03-31
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import requests

from log_config import get_logger

log = get_logger("pera.operational_activity.ingest")

# ── Constants ────────────────────────────────────────────────
BASE_URL = "https://pera360.punjab.gov.pk/backend/api/operational-activity"

ENDPOINTS = {
    "divisions":         f"{BASE_URL}/divisions",
    "districts":         f"{BASE_URL}/districts",
    "tehsils":           f"{BASE_URL}/tehsils",
    "tehsil_breakdown":  f"{BASE_URL}/tehsil-breakdown",
}

HEADERS = {"accept": "application/json"}


# ── Fetch ────────────────────────────────────────────────────
def _fetch_url(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Low-level: GET a URL, return parsed JSON list."""
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        log.warning("Unexpected response type from %s: %s", url, type(data))
        return []
    except requests.RequestException as e:
        log.error("Failed to fetch %s: %s", url, e)
        return []


def fetch_operational_activity(
    level: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    division_id: Optional[int] = None,
    district_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch operational activity data for a given geographic level.

    Args:
        level:       One of 'divisions', 'districts', 'tehsils', 'tehsil_breakdown'
        start:       Optional start date (ISO format)
        end:         Optional end date (ISO format)
        division_id: Required for 'districts' — fetch districts within this division
        district_id: Required for 'tehsils'/'tehsil_breakdown' — fetch tehsils within this district

    Returns:
        List of records from the API.
    """
    url = ENDPOINTS.get(level)
    if not url:
        log.error("Unknown level: %s", level)
        return []

    params: Dict[str, Any] = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    if division_id is not None:
        params["divisionId"] = division_id
    if district_id is not None:
        params["districtId"] = district_id

    log.info("Fetching operational activity: level=%s, params=%s", level, params)
    records = _fetch_url(url, params)
    log.info("  -> Got %d records for %s", len(records), level)
    return records


def fetch_all_districts(
    start: Optional[str] = None,
    end: Optional[str] = None,
    db=None,
) -> List[Dict[str, Any]]:
    """
    Fetch districts for ALL divisions by first reading division IDs
    from the database, then calling the districts API for each.
    """
    if not db:
        from analytics_db import get_analytics_db
        db = get_analytics_db()

    # Get all division IDs from the database
    div_rows = db.fetch_all(
        "SELECT DISTINCT division_id, division_name "
        "FROM operational_activity WHERE level = 'division' AND division_id > 0 "
        "ORDER BY division_name"
    )
    if not div_rows:
        # Fallback: fetch divisions first
        log.info("No divisions in DB yet — fetching divisions first")
        div_data = fetch_operational_activity("divisions", start=start, end=end)
        div_rows = [
            {"division_id": d.get("DivisionId", 0),
             "division_name": d.get("DivisionNameEnglish", "")}
            for d in div_data if d.get("DivisionId")
        ]

    all_districts: List[Dict[str, Any]] = []
    for div in div_rows:
        div_id = div["division_id"]
        div_name = div.get("division_name", "")
        log.info("Fetching districts for division: %s (ID=%d)", div_name, div_id)

        records = fetch_operational_activity(
            "districts", start=start, end=end, division_id=div_id
        )
        # Enrich each record with the parent division info
        for r in records:
            if not r.get("DivisionId") or r["DivisionId"] == 0:
                r["DivisionId"] = div_id
            if not r.get("DivisionNameEnglish"):
                r["DivisionNameEnglish"] = div_name

        all_districts.extend(records)
        time.sleep(0.5)  # Be polite to the API

    log.info("Total districts fetched: %d across %d divisions",
             len(all_districts), len(div_rows))
    return all_districts


def fetch_all_tehsils(
    start: Optional[str] = None,
    end: Optional[str] = None,
    db=None,
) -> List[Dict[str, Any]]:
    """
    Fetch tehsils for ALL districts by reading district IDs from the DB,
    then calling the tehsils API for each.
    """
    if not db:
        from analytics_db import get_analytics_db
        db = get_analytics_db()

    # Get all district rows with their parent division info
    dist_rows = db.fetch_all(
        "SELECT DISTINCT district_id, district_name, division_id, division_name "
        "FROM operational_activity WHERE level = 'district' AND district_id > 0 "
        "ORDER BY division_name, district_name"
    )
    if not dist_rows:
        log.error("No districts in DB — run districts ingestion first")
        return []

    all_tehsils: List[Dict[str, Any]] = []
    for dist in dist_rows:
        dist_id = dist["district_id"]
        dist_name = dist.get("district_name", "")
        div_id = dist.get("division_id", 0)
        div_name = dist.get("division_name", "")
        log.info("Fetching tehsils for district: %s (ID=%d)", dist_name, dist_id)

        records = fetch_operational_activity(
            "tehsils", start=start, end=end, district_id=dist_id
        )
        # Enrich each record with parent division + district info
        for r in records:
            if not r.get("DivisionId") or r["DivisionId"] == 0:
                r["DivisionId"] = div_id
            if not r.get("DivisionNameEnglish"):
                r["DivisionNameEnglish"] = div_name
            if not r.get("DistrictId") or r["DistrictId"] == 0:
                r["DistrictId"] = dist_id
            if not r.get("DistrictNameEnglish"):
                r["DistrictNameEnglish"] = dist_name

        all_tehsils.extend(records)
        time.sleep(0.5)

    log.info("Total tehsils fetched: %d across %d districts",
             len(all_tehsils), len(dist_rows))
    return all_tehsils


def fetch_and_store_tehsil_breakdown(
    start: Optional[str] = None,
    end: Optional[str] = None,
    db=None,
) -> int:
    """
    Fetch tehsil-breakdown (individual action details) for ALL tehsils
    and store directly into operational_activity_detail table.

    Returns total rows inserted.
    """
    if not db:
        from analytics_db import get_analytics_db
        db = get_analytics_db()

    # Get all tehsils with parent info
    tehsil_rows = db.fetch_all(
        "SELECT DISTINCT tehsil_id, tehsil_name, district_id, district_name, "
        "       division_id, division_name "
        "FROM operational_activity WHERE level = 'tehsil' AND tehsil_id > 0 "
        "ORDER BY division_name, district_name, tehsil_name"
    )
    if not tehsil_rows:
        log.error("No tehsils in DB — run tehsils ingestion first")
        return 0

    today = date.today().isoformat()
    total_inserted = 0

    for t in tehsil_rows:
        t_id = t["tehsil_id"]
        t_name = t.get("tehsil_name", "")
        d_id = t.get("district_id", 0)
        d_name = t.get("district_name", "")
        div_id = t.get("division_id", 0)
        div_name = t.get("division_name", "")

        log.info("Fetching breakdown for tehsil: %s (ID=%d)", t_name, t_id)

        params: Dict[str, Any] = {"tehsilId": t_id}
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        url = ENDPOINTS["tehsil_breakdown"]
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            log.error("Failed to fetch breakdown for %s: %s", t_name, e)
            time.sleep(0.5)
            continue

        if not isinstance(data, dict):
            log.warning("Unexpected response for %s: %s", t_name, type(data))
            time.sleep(0.5)
            continue

        details = data.get("details", [])
        if not details:
            log.info("  -> No detail records for %s", t_name)
            time.sleep(0.3)
            continue

        inserted = 0
        for rec in details:
            try:
                db.execute(
                    """
                    INSERT INTO operational_activity_detail
                        (tehsil_id, tehsil_name, district_id, district_name,
                         division_id, division_name, action_date, created_date,
                         requisition_type_id, requisition_name, status,
                         assigned_to, snapshot_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tehsil_id, action_date, created_date,
                                 assigned_to, requisition_type_id, snapshot_date)
                    DO NOTHING
                    """,
                    (
                        t_id,
                        t_name,
                        d_id,
                        d_name,
                        div_id,
                        div_name,
                        rec.get("actionDate"),
                        rec.get("createdDate"),
                        rec.get("requisitionType"),
                        (rec.get("requisitionName") or "").strip() or None,
                        rec.get("status"),
                        (rec.get("assignedTo") or "").strip() or None,
                        today,
                    ),
                )
                inserted += 1
            except Exception as e:
                log.error("Failed to insert detail: %s", e)

        total_inserted += inserted
        log.info("  -> Stored %d detail records for %s", inserted, t_name)
        time.sleep(0.5)

    log.info("Total tehsil-breakdown records: %d across %d tehsils",
             total_inserted, len(tehsil_rows))
    return total_inserted


# ── Transform ────────────────────────────────────────────────
def _determine_level(record: Dict[str, Any], api_level: str) -> str:
    """Determine the geographic level of a record."""
    if api_level == "divisions":
        return "division"
    elif api_level == "districts":
        return "district"
    elif api_level in ("tehsils", "tehsil_breakdown"):
        return "tehsil"
    return "unknown"


def transform_record(record: Dict[str, Any], api_level: str) -> Dict[str, Any]:
    """
    Transform an API record into a row for the operational_activity table.
    """
    return {
        "level":             _determine_level(record, api_level),
        "division_id":       record.get("DivisionId") or 0,
        "division_name":     (record.get("DivisionNameEnglish") or "").strip() or None,
        "district_id":       record.get("DistrictId") or 0,
        "district_name":     (record.get("DistrictNameEnglish") or "").strip() or None,
        "tehsil_id":         record.get("TehsilId") or 0,
        "tehsil_name":       (record.get("TehsilNameEnglish") or "").strip() or None,
        "price_control":     record.get("Price Control", 0) or 0,
        "anti_encroachment": record.get("Anti-Encroachment", 0) or 0,
        "eviction":          record.get("Eviction", 0) or 0,
        "anti_hoarding":     record.get("Anti-Hoarding", 0) or 0,
        "public_nuisance":   record.get("Public Nuisance", 0) or 0,
        "total":             record.get("total", 0) or 0,
    }


# ── Store ────────────────────────────────────────────────────
def store_records(records: List[Dict[str, Any]], db) -> int:
    """
    Insert records into the operational_activity table.
    Uses a daily snapshot approach — each day's fetch is a new snapshot.

    Returns:
        Number of rows inserted.
    """
    if not records:
        return 0

    today = date.today().isoformat()
    inserted = 0

    for rec in records:
        try:
            db.execute(
                """
                INSERT INTO operational_activity
                    (level, division_id, division_name, district_id, district_name,
                     tehsil_id, tehsil_name, price_control, anti_encroachment,
                     eviction, anti_hoarding, public_nuisance, total, snapshot_date)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (level, division_id, district_id, tehsil_id, snapshot_date)
                DO UPDATE SET
                    division_name     = EXCLUDED.division_name,
                    district_name     = EXCLUDED.district_name,
                    tehsil_name       = EXCLUDED.tehsil_name,
                    price_control     = EXCLUDED.price_control,
                    anti_encroachment = EXCLUDED.anti_encroachment,
                    eviction          = EXCLUDED.eviction,
                    anti_hoarding     = EXCLUDED.anti_hoarding,
                    public_nuisance   = EXCLUDED.public_nuisance,
                    total             = EXCLUDED.total
                """,
                (
                    rec["level"],
                    rec["division_id"],
                    rec["division_name"],
                    rec["district_id"],
                    rec["district_name"],
                    rec["tehsil_id"],
                    rec["tehsil_name"],
                    rec["price_control"],
                    rec["anti_encroachment"],
                    rec["eviction"],
                    rec["anti_hoarding"],
                    rec["public_nuisance"],
                    rec["total"],
                    today,
                ),
            )
            inserted += 1
        except Exception as e:
            log.error("Failed to insert record: %s — %s", rec, e)

    log.info("Stored %d / %d records", inserted, len(records))
    return inserted


# ── Main Ingestion ───────────────────────────────────────────
def ingest_operational_activity(
    levels: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict[str, int]:
    """
    Full ingestion pipeline: fetch → transform → store.

    Args:
        levels: Which levels to ingest. Default: all four.
        start:  Optional start date filter.
        end:    Optional end date filter.

    Returns:
        Dict mapping level name to number of rows inserted.
    """
    from analytics_db import get_analytics_db
    from analytics_migrations import AnalyticsMigrator

    db = get_analytics_db()
    if not db:
        log.error("Cannot connect to analytics database")
        return {}

    # Run migrations to ensure table exists
    migrator = AnalyticsMigrator(db)
    migrator.migrate()

    if levels is None:
        levels = list(ENDPOINTS.keys())

    results: Dict[str, int] = {}

    for level in levels:
        log.info("=== Ingesting level: %s ===", level)

        # Districts/tehsils need parent IDs — use the special fetchers
        if level == "districts":
            raw_records = fetch_all_districts(start=start, end=end, db=db)
        elif level == "tehsils":
            raw_records = fetch_all_tehsils(start=start, end=end, db=db)
        elif level == "tehsil_breakdown":
            # Tehsil breakdown goes to a different table — handle directly
            count = fetch_and_store_tehsil_breakdown(
                start=start, end=end, db=db
            )
            results[level] = count
            continue
        else:
            raw_records = fetch_operational_activity(level, start=start, end=end)

        if not raw_records:
            log.warning("No records returned for %s", level)
            results[level] = 0
            continue

        transformed = [transform_record(r, level) for r in raw_records]
        count = store_records(transformed, db)
        results[level] = count

        # Small delay between API calls
        time.sleep(1)

    total = sum(results.values())
    log.info("=== Ingestion complete: %d total records across %s ===",
             total, list(results.keys()))

    # Reconcile summary totals from detail table (single source of truth)
    if "tehsil_breakdown" in (levels or list(ENDPOINTS.keys())):
        reconcile_summary_from_details(db)

    return results


def reconcile_summary_from_details(db=None) -> None:
    """
    Recalculate operational_activity summary totals (tehsil/district/division)
    from the operational_activity_detail table so counts always match exactly.
    """
    if not db:
        from analytics_db import get_analytics_db
        db = get_analytics_db()
    if not db:
        return

    today = date.today().isoformat()

    # ── 1. Update tehsil-level summaries from detail ────────
    log.info("Reconciling tehsil-level summaries from detail table...")
    tehsil_agg = db.fetch_all("""
        SELECT tehsil_id, tehsil_name, district_id, district_name,
               division_id, division_name,
               COUNT(*) FILTER (WHERE requisition_name = 'Price Control') AS price_control,
               COUNT(*) FILTER (WHERE requisition_name = 'Anti-Encroachment') AS anti_encroachment,
               COUNT(*) FILTER (WHERE requisition_name = 'Eviction') AS eviction,
               COUNT(*) FILTER (WHERE requisition_name = 'Anti-Hoarding') AS anti_hoarding,
               COUNT(*) FILTER (WHERE requisition_name = 'Public Nuisance') AS public_nuisance,
               COUNT(*) AS total
        FROM operational_activity_detail
        GROUP BY tehsil_id, tehsil_name, district_id, district_name,
                 division_id, division_name
    """)
    for t in tehsil_agg:
        db.execute("""
            UPDATE operational_activity
            SET price_control = %s, anti_encroachment = %s, eviction = %s,
                anti_hoarding = %s, public_nuisance = %s, total = %s
            WHERE level = 'tehsil' AND tehsil_id = %s AND snapshot_date = %s
        """, (t["price_control"], t["anti_encroachment"], t["eviction"],
              t["anti_hoarding"], t["public_nuisance"], t["total"],
              t["tehsil_id"], today))
    log.info("  Updated %d tehsil summaries", len(tehsil_agg))

    # ── 2. Update district-level summaries ──────────────────
    log.info("Reconciling district-level summaries...")
    district_agg = db.fetch_all("""
        SELECT district_id, district_name, division_id, division_name,
               COUNT(*) FILTER (WHERE requisition_name = 'Price Control') AS price_control,
               COUNT(*) FILTER (WHERE requisition_name = 'Anti-Encroachment') AS anti_encroachment,
               COUNT(*) FILTER (WHERE requisition_name = 'Eviction') AS eviction,
               COUNT(*) FILTER (WHERE requisition_name = 'Anti-Hoarding') AS anti_hoarding,
               COUNT(*) FILTER (WHERE requisition_name = 'Public Nuisance') AS public_nuisance,
               COUNT(*) AS total
        FROM operational_activity_detail
        GROUP BY district_id, district_name, division_id, division_name
    """)
    for d in district_agg:
        db.execute("""
            UPDATE operational_activity
            SET price_control = %s, anti_encroachment = %s, eviction = %s,
                anti_hoarding = %s, public_nuisance = %s, total = %s
            WHERE level = 'district' AND district_id = %s AND snapshot_date = %s
        """, (d["price_control"], d["anti_encroachment"], d["eviction"],
              d["anti_hoarding"], d["public_nuisance"], d["total"],
              d["district_id"], today))
    log.info("  Updated %d district summaries", len(district_agg))

    # ── 3. Update division-level summaries ──────────────────
    log.info("Reconciling division-level summaries...")
    division_agg = db.fetch_all("""
        SELECT division_id, division_name,
               COUNT(*) FILTER (WHERE requisition_name = 'Price Control') AS price_control,
               COUNT(*) FILTER (WHERE requisition_name = 'Anti-Encroachment') AS anti_encroachment,
               COUNT(*) FILTER (WHERE requisition_name = 'Eviction') AS eviction,
               COUNT(*) FILTER (WHERE requisition_name = 'Anti-Hoarding') AS anti_hoarding,
               COUNT(*) FILTER (WHERE requisition_name = 'Public Nuisance') AS public_nuisance,
               COUNT(*) AS total
        FROM operational_activity_detail
        GROUP BY division_id, division_name
    """)
    for div in division_agg:
        db.execute("""
            UPDATE operational_activity
            SET price_control = %s, anti_encroachment = %s, eviction = %s,
                anti_hoarding = %s, public_nuisance = %s, total = %s
            WHERE level = 'division' AND division_id = %s AND snapshot_date = %s
        """, (div["price_control"], div["anti_encroachment"], div["eviction"],
              div["anti_hoarding"], div["public_nuisance"], div["total"],
              div["division_id"], today))
    log.info("  Updated %d division summaries", len(division_agg))

    # ── 4. Zero out tehsils with no detail records ────────
    log.info("Zeroing tehsils with no detail records...")
    db.execute("""
        UPDATE operational_activity oa
        SET price_control = 0, anti_encroachment = 0, eviction = 0,
            anti_hoarding = 0, public_nuisance = 0, total = 0
        WHERE oa.level = 'tehsil' AND oa.snapshot_date = %s
          AND NOT EXISTS (
              SELECT 1 FROM operational_activity_detail d
              WHERE d.tehsil_id = oa.tehsil_id
          )
    """, (today,))

    log.info("Reconciliation complete — summary totals now match detail counts")


# ── CLI ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Ingest PERA360 Operational Activity data into PostgreSQL"
    )
    parser.add_argument(
        "--level",
        choices=list(ENDPOINTS.keys()),
        help="Specific level to ingest (default: all)",
    )
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    levels = [args.level] if args.level else None
    results = ingest_operational_activity(
        levels=levels, start=args.start, end=args.end
    )

    print("\n=== Results ===")
    for level, count in results.items():
        print(f"  {level}: {count} records")
    print(f"  Total: {sum(results.values())} records")


if __name__ == "__main__":
    main()
