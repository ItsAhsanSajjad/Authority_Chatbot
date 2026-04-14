#!/usr/bin/env python
"""
PERA AI — Challan Data Ingestion Script

Reusable script to fetch individual challan records from the PERA360
challan-status/list API and store them in PostgreSQL with daily snapshots.

Table: challan_data
    - UPSERT on (challan_id, snapshot_date) — one row per challan per day
    - Supports historical tracking by running with different --asOfDate values

Usage:
    # Fetch paid challans for tehsil 459 (Nankana Sahib)
    python challan_ingest.py --tehsilId 459 --requisitionTypeId 0 --status paid

    # Fetch all statuses for a tehsil
    python challan_ingest.py --tehsilId 459 --all-statuses

    # Fetch with a specific date snapshot
    python challan_ingest.py --tehsilId 459 --status paid --asOfDate 2026-03-15

    # Fetch all tehsils × all statuses (full ingestion)
    python challan_ingest.py --all-tehsils --all-statuses

    # Fetch only Price Control challans in a tehsil
    python challan_ingest.py --tehsilId 459 --requisitionTypeId 0 --status paid

API Endpoint:
    GET https://pera360.punjab.gov.pk/backend/api/dashboard/challan-status/list
    Params: tehsilId (int), requisitionTypeId (int), status (str), asOfDate (datetime)

Requisition Types:
    0 = Price Control
    1 = Anti Hoarding
    2 = Anti Encroachment
    3 = Land Eviction
    4 = Public Nuisance
    5 = Demarcation
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

# ── Project path setup ───────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from log_config import get_logger
    log = get_logger("pera.challan.ingest")
except Exception:
    import logging
    log = logging.getLogger("challan_ingest")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ── Constants ────────────────────────────────────────────────
BASE_URL = "https://pera360.punjab.gov.pk/backend"
API_PATH = "/api/dashboard/challan-status/list"
TIMEOUT  = 60
ALL_STATUSES = ["paid", "unpaid", "overdue"]
ALL_REQ_TYPE_IDS = [0, 1, 2, 3, 4, 5]

REQUISITION_TYPES = {
    0: "Price Control",
    1: "Anti Hoarding",
    2: "Anti Encroachment",
    3: "Land Eviction",
    4: "Public Nuisance",
    5: "Demarcation",
}


# ══════════════════════════════════════════════════════════════
# SQL — Table Creation
# ══════════════════════════════════════════════════════════════

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS challan_data (
    id                      SERIAL,
    snapshot_date           DATE NOT NULL DEFAULT CURRENT_DATE,
    challan_id              TEXT NOT NULL,
    status                  TEXT NOT NULL,
    action_date             TIMESTAMP,
    paid_date               TIMESTAMP,
    consumer_number         TEXT,
    requisition_type_id     INTEGER,
    requisition_type_name   TEXT,
    officer_name            TEXT,
    fine_amount             NUMERIC(18,2) DEFAULT 0,
    paid_amount             NUMERIC(18,2) DEFAULT 0,
    outstanding_amount      NUMERIC(18,2) DEFAULT 0,
    challan_status          TEXT,
    address                 TEXT,
    tehsil_name             TEXT,
    district_name           TEXT,
    division_name           TEXT,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT challan_data_pkey PRIMARY KEY (challan_id, snapshot_date)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_challan_data_status       ON challan_data (status);
CREATE INDEX IF NOT EXISTS idx_challan_data_tehsil       ON challan_data (tehsil_name);
CREATE INDEX IF NOT EXISTS idx_challan_data_district     ON challan_data (district_name);
CREATE INDEX IF NOT EXISTS idx_challan_data_division     ON challan_data (division_name);
CREATE INDEX IF NOT EXISTS idx_challan_data_snapshot     ON challan_data (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_challan_data_req_type     ON challan_data (requisition_type_id);
CREATE INDEX IF NOT EXISTS idx_challan_data_officer      ON challan_data (officer_name);
"""

UPSERT_SQL = """
INSERT INTO challan_data (
    snapshot_date, challan_id, status, action_date, paid_date,
    consumer_number, requisition_type_id, requisition_type_name,
    officer_name, fine_amount, paid_amount, outstanding_amount,
    challan_status, address, tehsil_name, district_name, division_name,
    tehsil_id, district_id, division_id,
    created_at, updated_at
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    NOW(), NOW()
)
ON CONFLICT (challan_id, snapshot_date) DO UPDATE SET
    status              = EXCLUDED.status,
    action_date         = EXCLUDED.action_date,
    paid_date           = EXCLUDED.paid_date,
    consumer_number     = EXCLUDED.consumer_number,
    requisition_type_id = EXCLUDED.requisition_type_id,
    requisition_type_name = EXCLUDED.requisition_type_name,
    officer_name        = EXCLUDED.officer_name,
    fine_amount         = EXCLUDED.fine_amount,
    paid_amount         = EXCLUDED.paid_amount,
    outstanding_amount  = EXCLUDED.outstanding_amount,
    challan_status      = EXCLUDED.challan_status,
    address             = EXCLUDED.address,
    tehsil_name         = EXCLUDED.tehsil_name,
    district_name       = EXCLUDED.district_name,
    division_name       = EXCLUDED.division_name,
    tehsil_id           = EXCLUDED.tehsil_id,
    district_id         = EXCLUDED.district_id,
    division_id         = EXCLUDED.division_id,
    updated_at          = NOW()
"""


# ══════════════════════════════════════════════════════════════
# DATABASE CONNECTION
# ══════════════════════════════════════════════════════════════

def get_connection():
    """
    Get a psycopg (v3) connection using the project's POSTGRES_URL.
    Falls back to AnalyticsDB if available, otherwise reads .env directly.
    """
    try:
        from analytics_db import get_analytics_db
        db = get_analytics_db()
        if db and db.is_available():
            return db
    except ImportError:
        pass

    # Fallback: connect directly
    import psycopg
    url = os.environ.get(
        "POSTGRES_URL",
        "postgresql://pera_user:%40AhsanFaheemMubeen@localhost:5432/pera_ai",
    )
    return psycopg.connect(url, autocommit=False)


def ensure_table(db) -> None:
    """Create the challan_data table if it doesn't exist."""
    from analytics_db import AnalyticsDB
    if isinstance(db, AnalyticsDB):
        with db.connection() as conn:
            for stmt in CREATE_TABLE_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(stmt)
    else:
        cur = db.cursor()
        for stmt in CREATE_TABLE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        db.commit()
    log.info("Table 'challan_data' ensured (with indexes)")


# ══════════════════════════════════════════════════════════════
# CORE FUNCTIONS: fetch → transform → save
# ══════════════════════════════════════════════════════════════

def fetch_data(
    tehsil_id: int,
    requisition_type_id: Optional[int] = None,
    status: Optional[str] = None,
    as_of_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    retries: int = 3,
) -> List[Dict[str, Any]]:
    """
    Fetch challan records from the PERA360 list API.

    When called with only tehsilId (no status/requisitionTypeId), the API
    returns ALL challans for that tehsil in a single request — this is
    the preferred mode for full ingestion.

    Args:
        tehsil_id:           Tehsil ID (integer)
        requisition_type_id: Optional requisition type (0-5). Omit for all types.
        status:              Optional "paid"/"unpaid"/"overdue". Omit for all statuses.
        as_of_date:          Optional date string (YYYY-MM-DD)
        start_date:          Optional start date filter (YYYY-MM-DD)
        end_date:            Optional end date filter (YYYY-MM-DD)
        retries:             Number of retry attempts on failure

    Returns:
        List of raw JSON record dicts from the API
    """
    params: Dict[str, Any] = {"tehsilId": tehsil_id}
    if requisition_type_id is not None:
        params["requisitionTypeId"] = requisition_type_id
    if status is not None:
        params["status"] = status
    if as_of_date:
        params["asOfDate"] = as_of_date
    if start_date:
        params["startDate"] = start_date
    if end_date:
        params["endDate"] = end_date

    url = f"{BASE_URL}{API_PATH}"

    for attempt in range(1, retries + 1):
        try:
            log.info(
                "Fetching: tehsilId=%d%s%s%s (attempt %d/%d)",
                tehsil_id,
                f", reqType={requisition_type_id}" if requisition_type_id is not None else "",
                f", status={status}" if status else "",
                f", asOfDate={as_of_date}" if as_of_date else "",
                attempt, retries,
            )
            resp = requests.get(
                url,
                params=params,
                headers={"Accept": "application/json"},
                timeout=TIMEOUT,
                verify=False,
            )

            if 400 <= resp.status_code < 500:
                log.warning("HTTP %d (client error) — skipping", resp.status_code)
                return []

            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, list):
                log.warning("Unexpected response type: %s", type(data).__name__)
                return []

            log.info("Fetched %d records", len(data))
            return data

        except requests.RequestException as exc:
            log.warning("Attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(2 * attempt)

    log.error("All %d attempts failed for tehsilId=%d", retries, tehsil_id)
    return []


def transform_data(
    raw_records: List[Dict[str, Any]],
    status: Optional[str] = None,
    snapshot_date: Optional[date] = None,
    tehsil_id: Optional[int] = None,
    geo_lookup: Optional[Dict[str, Any]] = None,
) -> List[Tuple]:
    """
    Transform raw API records into tuples ready for PostgreSQL insertion.

    Args:
        raw_records:   List of dicts from fetch_data()
        status:        The status used in the API call, or None to derive
                       from each record's challanStatus field.
        snapshot_date: Date for daily snapshot (defaults to today)
        tehsil_id:     Optional tehsil ID (passed from ingest_all_tehsils)
        geo_lookup:    Optional geo lookup dict from _build_geo_lookup()

    Returns:
        List of tuples matching UPSERT_SQL column order
    """
    if not raw_records:
        return []

    snap = snapshot_date or date.today()
    rows = []

    # Resolve geographic IDs
    t2d = geo_lookup.get("tehsil_to_district", {}) if geo_lookup else {}
    d2v = geo_lookup.get("district_to_division", {}) if geo_lookup else {}
    n2t = geo_lookup.get("name_to_tehsil", {}) if geo_lookup else {}

    for r in raw_records:
        # Parse dates safely
        action_dt = _parse_datetime(r.get("actionDate"))
        paid_dt   = _parse_datetime(r.get("challanPaidDate"))

        # Derive status from record if not provided by caller
        record_status = status or (r.get("challanStatus") or "").lower() or "unknown"

        # Resolve tehsil_id → district_id → division_id
        t_id = tehsil_id
        if t_id is None and n2t:
            tname = (r.get("tehsilNameEnglish") or "").strip().lower()
            t_id = n2t.get(tname)
        d_id = t2d.get(t_id) if t_id else None
        v_id = d2v.get(d_id) if d_id else None

        row = (
            snap,                                        # snapshot_date
            r.get("challanId", ""),                       # challan_id
            record_status,                               # status
            action_dt,                                   # action_date
            paid_dt,                                     # paid_date
            r.get("consumerNumber", ""),                  # consumer_number
            r.get("requisitionTypeId"),                   # requisition_type_id
            r.get("requisitionTypeName", ""),             # requisition_type_name
            r.get("actionOfficerName", ""),               # officer_name
            r.get("fineAmount", 0) or 0,                 # fine_amount
            r.get("totalPaidAmount", 0) or 0,            # paid_amount
            r.get("outstandingAmount", 0) or 0,          # outstanding_amount
            r.get("challanStatus", ""),                   # challan_status
            r.get("challanAddressText", ""),              # address
            r.get("tehsilNameEnglish", ""),               # tehsil_name
            r.get("districtNameEnglish", ""),             # district_name
            r.get("divisionNameEnglish", ""),             # division_name
            t_id,                                        # tehsil_id
            d_id,                                        # district_id
            v_id,                                        # division_id
        )
        rows.append(row)

    log.info("Transformed %d records (snapshot: %s)", len(rows), snap)
    return rows


def save_to_db(
    db,
    rows: List[Tuple],
    batch_size: int = 500,
) -> int:
    """
    Insert/update records into challan_data using UPSERT.

    Args:
        db:         AnalyticsDB instance or psycopg connection
        rows:       List of tuples from transform_data()
        batch_size: Commit every N rows

    Returns:
        Number of records inserted/updated
    """
    if not rows:
        log.info("No records to save")
        return 0

    from analytics_db import AnalyticsDB

    total = 0

    if isinstance(db, AnalyticsDB):
        # Use AnalyticsDB context manager (batched)
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            with db.connection() as conn:
                for row in batch:
                    conn.execute(UPSERT_SQL, row)
            total += len(batch)
            if total % 1000 == 0 or total == len(rows):
                log.info("  Saved %d / %d records", total, len(rows))
    else:
        # Direct psycopg connection
        cur = db.cursor()
        for i, row in enumerate(rows, 1):
            cur.execute(UPSERT_SQL, row)
            if i % batch_size == 0:
                db.commit()
                log.info("  Saved %d / %d records", i, len(rows))
        db.commit()
        total = len(rows)

    log.info("Saved %d records to challan_data", total)
    return total


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _parse_datetime(val: Any) -> Optional[datetime]:
    """Parse ISO datetime string from API, handling timezone offsets."""
    if not val or not isinstance(val, str):
        return None
    try:
        # Remove timezone offset for simple timestamp storage
        # e.g. "2026-03-19T11:30:20.917+05:00" → datetime
        clean = val.replace("+05:00", "").replace("+00:00", "").replace("Z", "")
        # Handle microseconds
        if "." in clean:
            return datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S.%f")
        else:
            return datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
    except (ValueError, TypeError):
        return None


def get_all_tehsil_ids(db) -> List[Tuple[int, str]]:
    """Get all tehsil IDs and names from the dim_tehsil table."""
    from analytics_db import AnalyticsDB
    if isinstance(db, AnalyticsDB):
        rows = db.fetch_all(
            "SELECT tehsil_id, tehsil_name FROM dim_tehsil ORDER BY tehsil_id"
        )
        return [(r["tehsil_id"], r["tehsil_name"]) for r in rows]
    else:
        cur = db.execute(
            "SELECT tehsil_id, tehsil_name FROM dim_tehsil ORDER BY tehsil_id"
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


def _build_geo_lookup(db) -> Dict[str, Any]:
    """Build reverse lookup maps: tehsil_id → district_id → division_id."""
    from analytics_db import AnalyticsDB
    tehsil_to_district: Dict[int, int] = {}
    district_to_division: Dict[int, int] = {}
    name_to_tehsil: Dict[str, int] = {}

    if isinstance(db, AnalyticsDB):
        for r in db.fetch_all("SELECT tehsil_id, tehsil_name, district_id FROM dim_tehsil"):
            tehsil_to_district[r["tehsil_id"]] = r["district_id"]
            name_to_tehsil[r["tehsil_name"].strip().lower()] = r["tehsil_id"]
        for r in db.fetch_all("SELECT district_id, division_id FROM dim_district"):
            district_to_division[r["district_id"]] = r["division_id"]

    return {
        "tehsil_to_district": tehsil_to_district,
        "district_to_division": district_to_division,
        "name_to_tehsil": name_to_tehsil,
    }


# ══════════════════════════════════════════════════════════════
# ORCHESTRATION
# ══════════════════════════════════════════════════════════════

def ingest(
    tehsil_id: int,
    requisition_type_id: int = 0,
    status: str = "paid",
    as_of_date: Optional[str] = None,
    db=None,
) -> int:
    """
    Full pipeline: fetch → transform → save for one (tehsil, reqType, status) combo.

    Args:
        tehsil_id:           Tehsil ID
        requisition_type_id: Requisition type (0-5)
        status:              "paid", "unpaid", or "overdue"
        as_of_date:          Optional snapshot date (YYYY-MM-DD)
        db:                  Optional AnalyticsDB or connection (auto-created if None)

    Returns:
        Number of records saved
    """
    close_db = False
    if db is None:
        db = get_connection()
        close_db = True

    try:
        # 1. Ensure table exists
        ensure_table(db)

        # 2. Fetch from API
        raw = fetch_data(tehsil_id, requisition_type_id, status, as_of_date)
        if not raw:
            return 0

        # 3. Transform
        snap = None
        if as_of_date:
            try:
                snap = datetime.strptime(as_of_date, "%Y-%m-%d").date()
            except ValueError:
                snap = date.today()
        rows = transform_data(raw, status, snap)

        # 4. Save to database
        count = save_to_db(db, rows)
        return count

    finally:
        if close_db and hasattr(db, "close"):
            try:
                db.close()
            except Exception:
                pass


def ingest_all_statuses(
    tehsil_id: int,
    requisition_type_id: int = 0,
    as_of_date: Optional[str] = None,
    db=None,
) -> Dict[str, int]:
    """Ingest all 3 statuses (paid, unpaid, overdue) for a single tehsil."""
    close_db = False
    if db is None:
        db = get_connection()
        close_db = True

    results = {}
    try:
        ensure_table(db)
        for status in ALL_STATUSES:
            raw = fetch_data(tehsil_id, requisition_type_id, status, as_of_date)
            snap = None
            if as_of_date:
                try:
                    snap = datetime.strptime(as_of_date, "%Y-%m-%d").date()
                except ValueError:
                    snap = date.today()
            rows = transform_data(raw, status, snap)
            count = save_to_db(db, rows)
            results[status] = count
    finally:
        if close_db and hasattr(db, "close"):
            try:
                db.close()
            except Exception:
                pass

    return results


def ingest_all_tehsils(
    req_type_ids: Optional[List[int]] = None,
    statuses: Optional[List[str]] = None,
    as_of_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Full ingestion: fetch ALL challans for every tehsil in one API call each.

    By default, calls the API with only tehsilId (no status/reqType filters),
    which returns ALL challans for that tehsil. This is ~18x faster than
    iterating over every reqType × status combination.

    If statuses or req_type_ids are explicitly provided, falls back to
    the filtered mode for backward compatibility.

    Uses dim_tehsil for the list of tehsil IDs.

    Args:
        req_type_ids: Optional list of requisition type IDs (omit for unfiltered)
        statuses:     Optional list of statuses (omit for unfiltered)
        as_of_date:   Optional snapshot date
        start_date:   Optional start date filter (YYYY-MM-DD)
        end_date:     Optional end date filter (YYYY-MM-DD)
    """
    db = get_connection()
    ensure_table(db)

    tehsils = get_all_tehsil_ids(db)
    if not tehsils:
        log.error("No tehsils found in dim_tehsil. Run hierarchy sync first.")
        return {"error": "no tehsils", "total": 0}

    # Build geographic ID lookup (tehsil → district → division)
    geo_lookup = _build_geo_lookup(db)
    log.info("Geo lookup: %d tehsils, %d districts mapped",
             len(geo_lookup["name_to_tehsil"]), len(geo_lookup["district_to_division"]))

    snap = None
    if as_of_date:
        try:
            snap = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        except ValueError:
            snap = date.today()

    total = 0
    errors = 0
    api_calls = 0
    start_t = time.time()

    # ── Fast mode: one unfiltered API call per tehsil ────────
    # The API returns ALL challans (all statuses, all req types) when
    # called with only tehsilId. Status is derived from challanStatus field.
    use_fast_mode = (statuses is None and req_type_ids is None) or (
        statuses is not None and set(statuses) == set(ALL_STATUSES) and req_type_ids is None
    )

    if use_fast_mode:
        log.info("=" * 60)
        log.info("FAST INGESTION: %d tehsils × 1 unfiltered call each = %d API calls",
                 len(tehsils), len(tehsils))
        log.info("=" * 60)

        for i, (tid, tname) in enumerate(tehsils, 1):
            try:
                raw = fetch_data(tid, as_of_date=as_of_date,
                                 start_date=start_date, end_date=end_date)
                # Status derived from each record's challanStatus field
                rows = transform_data(raw, status=None, snapshot_date=snap,
                                      tehsil_id=tid, geo_lookup=geo_lookup)
                count = save_to_db(db, rows)
                total += count
                api_calls += 1
            except Exception as exc:
                log.error("Failed: tehsil=%s (%d): %s", tname, tid, exc)
                errors += 1

            if i % 10 == 0 or i == len(tehsils):
                elapsed_so_far = time.time() - start_t
                rate = api_calls / elapsed_so_far if elapsed_so_far > 0 else 0
                remaining = (len(tehsils) - api_calls) / rate if rate > 0 else 0
                log.info("Progress: %d/%d tehsils | %s records | %d errors | %.1f calls/sec | ETA: %.0fs",
                         i, len(tehsils), f"{total:,}", errors, rate, remaining)
    else:
        # ── Filtered mode: iterate reqTypes × statuses ───────
        if statuses is None:
            statuses = ALL_STATUSES
        if req_type_ids is None:
            req_type_ids = ALL_REQ_TYPE_IDS

        total_combos = len(tehsils) * len(req_type_ids) * len(statuses)
        log.info("=" * 60)
        log.info("FILTERED INGESTION: %d tehsils x %d req types x %d statuses = %d API calls",
                 len(tehsils), len(req_type_ids), len(statuses), total_combos)
        log.info("=" * 60)

        for i, (tid, tname) in enumerate(tehsils, 1):
            for req_id in req_type_ids:
                for status in statuses:
                    try:
                        raw = fetch_data(tid, req_id, status, as_of_date,
                                         start_date=start_date, end_date=end_date)
                        rows = transform_data(raw, status, snap,
                                              tehsil_id=tid, geo_lookup=geo_lookup)
                        count = save_to_db(db, rows)
                        total += count
                        api_calls += 1
                    except Exception as exc:
                        log.error("Failed: tehsil=%s (%d), reqType=%d, status=%s: %s",
                                  tname, tid, req_id, status, exc)
                        errors += 1

            if i % 5 == 0 or i == len(tehsils):
                elapsed_so_far = time.time() - start_t
                rate = api_calls / elapsed_so_far if elapsed_so_far > 0 else 0
                remaining = (total_combos - api_calls) / rate if rate > 0 else 0
                log.info("Progress: %d/%d tehsils | %s records | %d errors | %.0f calls/sec | ETA: %.0fs",
                         i, len(tehsils), f"{total:,}", errors, rate, remaining)

    elapsed = time.time() - start_t
    log.info("=" * 60)
    log.info("INGESTION COMPLETE: %s records in %.1fs (%d API calls, %d errors)",
             f"{total:,}", elapsed, api_calls, errors)
    log.info("=" * 60)

    return {
        "tehsils_processed": len(tehsils),
        "total_records": total,
        "api_calls": api_calls,
        "errors": errors,
        "elapsed_seconds": round(elapsed, 1),
    }


# ══════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="PERA Challan Data Ingestion — Fetch from API → Store in PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch paid challans for Nankana Sahib (tehsilId=459)
  python challan_ingest.py --tehsilId 459 --requisitionTypeId 0 --status paid

  # Fetch all statuses for a tehsil
  python challan_ingest.py --tehsilId 459 --all-statuses

  # Fetch with a snapshot date
  python challan_ingest.py --tehsilId 459 --status paid --asOfDate 2026-03-15

  # Full ingestion: all tehsils × all statuses
  python challan_ingest.py --all-tehsils --all-statuses

  # Only create the table (no data fetch)
  python challan_ingest.py --create-table-only

Requisition Types:
  0 = Price Control       3 = Land Eviction
  1 = Anti Hoarding       4 = Public Nuisance
  2 = Anti Encroachment   5 = Demarcation
        """,
    )

    parser.add_argument("--tehsilId", type=int, help="Tehsil ID (e.g., 459)")
    parser.add_argument("--requisitionTypeId", type=int, default=None,
                        help="Requisition type ID (0-5). Omit for all types.")
    parser.add_argument("--status", type=str, choices=ALL_STATUSES,
                        help="Challan status: paid, unpaid, or overdue")
    parser.add_argument("--asOfDate", type=str, default=None,
                        help="Snapshot date (YYYY-MM-DD). Default: today")
    parser.add_argument("--startDate", type=str, default=None,
                        help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--endDate", type=str, default=None,
                        help="End date filter (YYYY-MM-DD)")
    parser.add_argument("--all-statuses", action="store_true",
                        help="Fetch all 3 statuses (paid, unpaid, overdue)")
    parser.add_argument("--all-req-types", action="store_true",
                        help="Fetch all 6 requisition types (0-5)")
    parser.add_argument("--all-tehsils", action="store_true",
                        help="Fetch for ALL tehsils (uses dim_tehsil table)")
    parser.add_argument("--create-table-only", action="store_true",
                        help="Only create the challan_data table, no data fetch")

    args = parser.parse_args()

    # ── Create table only ─────────────────────────────────────
    if args.create_table_only:
        db = get_connection()
        ensure_table(db)
        print("[OK] Table 'challan_data' created successfully")
        return

    # ── Full ingestion: all tehsils ───────────────────────────
    if args.all_tehsils:
        statuses = ALL_STATUSES if args.all_statuses else ([args.status] if args.status else ALL_STATUSES)

        # Determine req type IDs
        if args.all_req_types:
            req_ids = ALL_REQ_TYPE_IDS
        elif args.requisitionTypeId is not None:
            req_ids = [args.requisitionTypeId]
        else:
            req_ids = ALL_REQ_TYPE_IDS  # Default: all types

        result = ingest_all_tehsils(
            req_type_ids=req_ids,
            statuses=statuses,
            as_of_date=args.asOfDate,
            start_date=args.startDate,
            end_date=args.endDate,
        )
        print(f"\n{'='*50}")
        print(f"Ingestion complete!")
        print(f"  Tehsils processed: {result['tehsils_processed']}")
        print(f"  Req types:         {[REQUISITION_TYPES.get(r, r) for r in result.get('req_types', [])]}")
        print(f"  Statuses:          {result.get('statuses', [])}")
        print(f"  Total records:     {result['total_records']:,}")
        print(f"  API calls:         {result.get('api_calls', 'N/A')}")
        print(f"  Errors:            {result['errors']}")
        print(f"  Time:              {result['elapsed_seconds']}s")
        print(f"{'='*50}")
        return

    # ── Single tehsil ─────────────────────────────────────────
    if not args.tehsilId:
        parser.error("--tehsilId is required (or use --all-tehsils)")

    # Determine req types for single-tehsil mode
    req_id = args.requisitionTypeId if args.requisitionTypeId is not None else 0

    if args.all_statuses:
        results = ingest_all_statuses(
            tehsil_id=args.tehsilId,
            requisition_type_id=req_id,
            as_of_date=args.asOfDate,
        )
        total = sum(results.values())
        print(f"\n{'='*50}")
        print(f"Ingestion complete for tehsilId={args.tehsilId}")
        for st, cnt in results.items():
            print(f"  {st:>8}: {cnt:,} records")
        print(f"  {'TOTAL':>8}: {total:,} records")
        print(f"{'='*50}")
    else:
        if not args.status:
            parser.error("--status is required (or use --all-statuses)")

        count = ingest(
            tehsil_id=args.tehsilId,
            requisition_type_id=req_id,
            status=args.status,
            as_of_date=args.asOfDate,
        )
        print(f"\n{'='*50}")
        print(f"Ingestion complete!")
        print(f"  tehsilId:           {args.tehsilId}")
        print(f"  requisitionTypeId:  {req_id} ({REQUISITION_TYPES.get(req_id, 'Unknown')})")
        print(f"  status:             {args.status}")
        print(f"  Records saved:      {count:,}")
        print(f"{'='*50}")


if __name__ == "__main__":
    main()
