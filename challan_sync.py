"""
PERA AI — Challan Data Sync (PostgreSQL)

Fetches data from PERA360 challan and reference APIs and stores it
directly in PostgreSQL (the pera_ai analytics database).

Usage (standalone):
    python challan_sync.py                  # Sync hierarchy + APIs 1-6, 8
    python challan_sync.py --include-list   # Also sync API 7 (large)
    python challan_sync.py --api 1,2,3      # Only specific APIs
    python challan_sync.py --hierarchy-only  # Only divisions/districts/tehsils

Continuous sync (integrated):
    The ChallanScheduler class runs in a daemon thread, cycling through
    all APIs continuously with a configurable interval.
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(__file__))

from log_config import get_logger

log = get_logger("pera.challan.sync")

BASE_URL = "https://pera360.punjab.gov.pk/backend"
TIMEOUT = 30
STATUSES = ["paid", "unpaid", "overdue"]


# ── PostgreSQL connection helper ─────────────────────────────

def _get_db():
    """Get the AnalyticsDB singleton. Returns None if unavailable."""
    from analytics_db import get_analytics_db
    return get_analytics_db()


# ── HTTP Helper ──────────────────────────────────────────────

def _api_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 3,
) -> Optional[Any]:
    """GET request to PERA360 with retries and backoff."""
    url = f"{BASE_URL}{path}"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                url,
                params=params,
                headers={"Accept": "application/json"},
                timeout=TIMEOUT,
            )
            # Don't retry client errors (4xx) — they won't succeed on retry
            if 400 <= resp.status_code < 500:
                log.debug("  HTTP %d for %s %s — skipping (client error)",
                          resp.status_code, path, params or {})
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            log.warning(
                "  Attempt %d/%d failed: %s %s — %s",
                attempt, retries, path, params or {}, exc,
            )
            if attempt < retries:
                time.sleep(2 * attempt)
    return None


# ══════════════════════════════════════════════════════════════
# PHASE 1 — HIERARCHY  (uses existing dim_* tables)
# ══════════════════════════════════════════════════════════════

def sync_hierarchy() -> Dict[str, int]:
    """Sync divisions, districts, tehsils into dim_* PostgreSQL tables."""
    db = _get_db()
    if not db or not db.is_available():
        log.error("PostgreSQL not available — cannot sync hierarchy")
        return {"divisions": 0, "districts": 0, "tehsils": 0}

    stats = {"divisions": 0, "districts": 0, "tehsils": 0}

    # ── 1.1  Divisions ───────────────────────────────────────
    log.info("[Hierarchy] Fetching divisions...")
    divisions = _api_get("/api/app-data/divisions")
    if not isinstance(divisions, list) or not divisions:
        log.error("  Could not fetch divisions. Aborting hierarchy sync.")
        return stats

    with db.connection() as conn:
        for div in divisions:
            conn.execute(
                "INSERT INTO dim_division (division_id, division_name, updated_at) "
                "VALUES (%s, %s, NOW()) "
                "ON CONFLICT (division_id) DO UPDATE SET "
                "division_name = EXCLUDED.division_name, updated_at = NOW()",
                (div["id"], div["name"]),
            )
    stats["divisions"] = len(divisions)
    log.info("  %d divisions stored.", len(divisions))

    # ── 1.2  Districts (per division for linkage) ────────────
    log.info("[Hierarchy] Fetching districts per division...")
    total_districts = 0
    with db.connection() as conn:
        for div in divisions:
            dists = _api_get("/api/app-data/districts", {"divisionId": div["id"]})
            if not isinstance(dists, list):
                continue
            for d in dists:
                conn.execute(
                    "INSERT INTO dim_district (district_id, district_name, division_id, updated_at) "
                    "VALUES (%s, %s, %s, NOW()) "
                    "ON CONFLICT (district_id) DO UPDATE SET "
                    "district_name = EXCLUDED.district_name, "
                    "division_id = EXCLUDED.division_id, updated_at = NOW()",
                    (d["id"], d["name"], div["id"]),
                )
            total_districts += len(dists)
    stats["districts"] = total_districts
    log.info("  %d districts stored.", total_districts)

    # ── 1.3  Tehsils ─────────────────────────────────────────
    log.info("[Hierarchy] Fetching tehsils...")
    tehsils = _api_get("/api/app-data/tehsils")
    if not isinstance(tehsils, list):
        log.warning("  Could not fetch tehsils.")
        return stats

    with db.connection() as conn:
        for t in tehsils:
            conn.execute(
                "INSERT INTO dim_tehsil (tehsil_id, tehsil_name, district_id, updated_at) "
                "VALUES (%s, %s, %s, NOW()) "
                "ON CONFLICT (tehsil_id) DO UPDATE SET "
                "tehsil_name = EXCLUDED.tehsil_name, "
                "district_id = EXCLUDED.district_id, updated_at = NOW()",
                (t["id"], t["name"], t.get("districtId")),
            )
    stats["tehsils"] = len(tehsils)
    log.info("  %d tehsils stored.", len(tehsils))
    return stats


# ══════════════════════════════════════════════════════════════
# PHASE 2 — CHALLAN APIs  (one function per endpoint)
# ══════════════════════════════════════════════════════════════

def sync_1_totals() -> int:
    """API 1: /api/dashboard/challan-status/totals"""
    db = _get_db()
    if not db:
        return 0
    log.info("[API 1/8] challan-status/totals")
    data = _api_get("/api/dashboard/challan-status/totals")
    if not isinstance(data, dict):
        log.error("  No data returned.")
        return 0

    with db.connection() as conn:
        conn.execute(
            """
            INSERT INTO challan_totals
                (id, total_challans, total_fine_amount, paid, unpaid, overdue,
                 paid_percent, unpaid_percent, overdue_percent,
                 paid_fine_amount, unpaid_fine_amount, overdue_fine_amount, updated_at)
            VALUES (1, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                total_challans      = EXCLUDED.total_challans,
                total_fine_amount   = EXCLUDED.total_fine_amount,
                paid                = EXCLUDED.paid,
                unpaid              = EXCLUDED.unpaid,
                overdue             = EXCLUDED.overdue,
                paid_percent        = EXCLUDED.paid_percent,
                unpaid_percent      = EXCLUDED.unpaid_percent,
                overdue_percent     = EXCLUDED.overdue_percent,
                paid_fine_amount    = EXCLUDED.paid_fine_amount,
                unpaid_fine_amount  = EXCLUDED.unpaid_fine_amount,
                overdue_fine_amount = EXCLUDED.overdue_fine_amount,
                updated_at          = NOW()
            """,
            (
                data.get("total_Challan", 0),
                data.get("total_FineAmount", 0),
                data.get("paid", 0),
                data.get("unPaid", 0),
                data.get("overdue", 0),
                data.get("paid_Percent", 0),
                data.get("unPaid_Percent", 0),
                data.get("overdue_Percent", 0),
                data.get("paid_FineAmount", 0),
                data.get("unPaid_FineAmount", 0),
                data.get("overdue_FineAmount", 0),
            ),
        )

    log.info("  Total challans: %s, Fine: Rs. %s",
             f"{data.get('total_Challan',0):,}",
             f"{data.get('total_FineAmount',0):,.0f}")
    return 1


def sync_2_division() -> int:
    """API 2: /api/dashboard/challan-status/division"""
    db = _get_db()
    if not db:
        return 0
    log.info("[API 2/8] challan-status/division")
    total = 0
    with db.connection() as conn:
        for status in STATUSES:
            rows = _api_get("/api/dashboard/challan-status/division", {"status": status})
            if not isinstance(rows, list):
                continue
            for r in rows:
                conn.execute(
                    """
                    INSERT INTO challan_by_division
                        (status, division_id, division_name, total_challans, total_amount, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (status, division_id) DO UPDATE SET
                        division_name  = EXCLUDED.division_name,
                        total_challans = EXCLUDED.total_challans,
                        total_amount   = EXCLUDED.total_amount,
                        updated_at     = NOW()
                    """,
                    (status, r["divisionId"], r["divisionName"],
                     r.get("totalChallans", 0), r.get("totalAmount", 0)),
                )
            total += len(rows)
            log.info("  status=%s: %d divisions", status, len(rows))
    log.info("  Total rows: %d", total)
    return total


def sync_3_district() -> int:
    """API 3: /api/dashboard/challan-status/district (requires divisionId)"""
    db = _get_db()
    if not db:
        return 0
    log.info("[API 3/8] challan-status/district")

    # Get divisions from PostgreSQL
    divisions = db.fetch_all(
        "SELECT division_id, division_name FROM dim_division ORDER BY division_id"
    )
    if not divisions:
        log.error("  No divisions in DB. Run hierarchy sync first.")
        return 0

    total = 0
    with db.connection() as conn:
        for status in STATUSES:
            status_count = 0
            for div in divisions:
                rows = _api_get(
                    "/api/dashboard/challan-status/district",
                    {"status": status, "divisionId": div["division_id"]},
                )
                if not isinstance(rows, list):
                    continue
                for r in rows:
                    conn.execute(
                        """
                        INSERT INTO challan_by_district
                            (status, division_id, district_id, district_name,
                             total_challans, total_amount, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (status, district_id) DO UPDATE SET
                            division_id    = EXCLUDED.division_id,
                            district_name  = EXCLUDED.district_name,
                            total_challans = EXCLUDED.total_challans,
                            total_amount   = EXCLUDED.total_amount,
                            updated_at     = NOW()
                        """,
                        (status, div["division_id"], r["districtId"], r["districtName"],
                         r.get("totalChallans", 0), r.get("totalAmount", 0)),
                    )
                status_count += len(rows)
            total += status_count
            log.info("  status=%s: %d districts", status, status_count)
    log.info("  Total rows: %d", total)
    return total


def sync_4_tehsil() -> int:
    """API 4: /api/dashboard/challan-status/tehsil"""
    db = _get_db()
    if not db:
        return 0
    log.info("[API 4/8] challan-status/tehsil")
    total = 0
    with db.connection() as conn:
        for status in STATUSES:
            rows = _api_get("/api/dashboard/challan-status/tehsil", {"status": status})
            if not isinstance(rows, list):
                continue
            for r in rows:
                conn.execute(
                    """
                    INSERT INTO challan_by_tehsil (status, tehsil_name, count, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (status, tehsil_name) DO UPDATE SET
                        count      = EXCLUDED.count,
                        updated_at = NOW()
                    """,
                    (status, r.get("tehsilNameEnglish", ""), r.get("count", 0)),
                )
            total += len(rows)
            log.info("  status=%s: %d tehsils", status, len(rows))
    log.info("  Total rows: %d", total)
    return total


def sync_5_tehsil_drill() -> int:
    """API 5: /api/dashboard/challan-status/tehsil-drill (requires districtId)"""
    db = _get_db()
    if not db:
        return 0
    log.info("[API 5/8] challan-status/tehsil-drill")

    districts = db.fetch_all(
        "SELECT district_id FROM dim_district ORDER BY district_id"
    )
    if not districts:
        log.error("  No districts in DB. Run hierarchy sync first.")
        return 0

    total = 0
    with db.connection() as conn:
        for status in STATUSES:
            status_count = 0
            for dist in districts:
                rows = _api_get(
                    "/api/dashboard/challan-status/tehsil-drill",
                    {"status": status, "districtId": dist["district_id"]},
                )
                if not isinstance(rows, list):
                    continue
                for r in rows:
                    conn.execute(
                        """
                        INSERT INTO challan_tehsil_drill
                            (status, district_id, tehsil_id, tehsil_name,
                             total_challans, total_amount, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (status, district_id, tehsil_id) DO UPDATE SET
                            tehsil_name    = EXCLUDED.tehsil_name,
                            total_challans = EXCLUDED.total_challans,
                            total_amount   = EXCLUDED.total_amount,
                            updated_at     = NOW()
                        """,
                        (status, dist["district_id"],
                         r.get("tehsilId"), r.get("tehsilName"),
                         r.get("totalChallans", 0), r.get("totalAmount", 0)),
                    )
                status_count += len(rows)
            total += status_count
            log.info("  status=%s: %d drill rows", status, status_count)
    log.info("  Total rows: %d", total)
    return total


def sync_6_requisition_type() -> int:
    """API 6: /api/dashboard/challan-status/requisition-type"""
    db = _get_db()
    if not db:
        return 0
    log.info("[API 6/8] challan-status/requisition-type")
    total = 0
    with db.connection() as conn:
        for status in STATUSES:
            rows = _api_get(
                "/api/dashboard/challan-status/requisition-type",
                {"status": status},
            )
            if not isinstance(rows, list):
                continue
            for r in rows:
                conn.execute(
                    """
                    INSERT INTO challan_requisition_type
                        (status, requisition_type_id, requisition_type_name,
                         total_challans, total_amount, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (status, requisition_type_id) DO UPDATE SET
                        requisition_type_name = EXCLUDED.requisition_type_name,
                        total_challans        = EXCLUDED.total_challans,
                        total_amount          = EXCLUDED.total_amount,
                        updated_at            = NOW()
                    """,
                    (status, r["requisitionTypeId"], r["requisitionTypeName"],
                     r.get("totalChallans", 0), r.get("totalAmount", 0)),
                )
            total += len(rows)
            log.info("  status=%s: %d types", status, len(rows))
    log.info("  Total rows: %d", total)
    return total


def sync_7_list() -> int:
    """API 7: /api/dashboard/challan-status/list (requires tehsilId, LARGE)"""
    db = _get_db()
    if not db:
        return 0
    log.info("[API 7/8] challan-status/list (large — may take minutes)")

    tehsils = db.fetch_all("SELECT tehsil_id FROM dim_tehsil ORDER BY tehsil_id")
    if not tehsils:
        log.error("  No tehsils in DB. Run hierarchy sync first.")
        return 0

    # Reverse lookups
    tehsil_district = {}
    for row in db.fetch_all("SELECT tehsil_id, district_id FROM dim_tehsil"):
        tehsil_district[row["tehsil_id"]] = row["district_id"]
    district_division = {}
    for row in db.fetch_all("SELECT district_id, division_id FROM dim_district"):
        district_division[row["district_id"]] = row["division_id"]

    total = 0
    done = 0
    for t in tehsils:
        tid = t["tehsil_id"]
        dist_id = tehsil_district.get(tid)
        div_id = district_division.get(dist_id) if dist_id else None

        with db.connection() as conn:
            for status in STATUSES:
                rows = _api_get(
                    "/api/dashboard/challan-status/list",
                    {"tehsilId": tid, "status": status},
                )
                if not isinstance(rows, list) or not rows:
                    continue
                for r in rows:
                    conn.execute(
                        """
                        INSERT INTO challan_list
                            (challan_id, status, action_date, challan_paid_date,
                             consumer_number, requisition_type_name,
                             action_officer_name, fine_amount,
                             total_paid_amount, outstanding_amount,
                             challan_status, challan_address,
                             tehsil_name, district_name, division_name,
                             tehsil_id, district_id, division_id, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (challan_id) DO UPDATE SET
                            status              = EXCLUDED.status,
                            fine_amount         = EXCLUDED.fine_amount,
                            total_paid_amount   = EXCLUDED.total_paid_amount,
                            outstanding_amount  = EXCLUDED.outstanding_amount,
                            challan_status      = EXCLUDED.challan_status,
                            updated_at          = NOW()
                        """,
                        (
                            r.get("challanId"), status,
                            r.get("actionDate"), r.get("challanPaidDate"),
                            r.get("consumerNumber"),
                            r.get("requisitionTypeName"),
                            r.get("actionOfficerName"),
                            r.get("fineAmount", 0),
                            r.get("totalPaidAmount", 0),
                            r.get("outstandingAmount", 0),
                            r.get("challanStatus"),
                            r.get("challanAddressText"),
                            r.get("tehsilNameEnglish"),
                            r.get("districtNameEnglish"),
                            r.get("divisionNameEnglish"),
                            tid, dist_id, div_id,
                        ),
                    )
                total += len(rows)

        done += 1
        if done % 20 == 0 or done == len(tehsils):
            log.info("  Progress: %d/%d tehsils, %s challans", done, len(tehsils), f"{total:,}")

    log.info("  Total challan records: %s", f"{total:,}")
    return total


def sync_8_tehsil_breakdown() -> int:
    """API 8: /api/challan-status/tehsil-breakdown"""
    db = _get_db()
    if not db:
        return 0
    log.info("[API 8/8] challan-status/tehsil-breakdown")

    # Get unique tehsil names from challan_by_tehsil
    names_rows = db.fetch_all(
        "SELECT DISTINCT tehsil_name FROM challan_by_tehsil "
        "WHERE tehsil_name != '' ORDER BY tehsil_name"
    )
    tehsil_names = [r["tehsil_name"] for r in names_rows]
    if not tehsil_names:
        log.warning("  No tehsil names found. Run API 4 sync first.")
        return 0

    total = 0
    for status in STATUSES:
        status_count = 0
        with db.connection() as conn:
            for tname in tehsil_names:
                data = _api_get(
                    "/api/challan-status/tehsil-breakdown",
                    {"status": status, "tehsilNameEnglish": tname},
                )
                if not isinstance(data, dict) or not data.get("tehsilNameEnglish"):
                    continue
                conn.execute(
                    """
                    INSERT INTO challan_tehsil_breakdown
                        (status, tehsil_name, total_requisitions,
                         hoarding_count, price_control_count, encroachment_count,
                         land_retrieval_count, public_nuisance_count, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (status, tehsil_name) DO UPDATE SET
                        total_requisitions   = EXCLUDED.total_requisitions,
                        hoarding_count       = EXCLUDED.hoarding_count,
                        price_control_count  = EXCLUDED.price_control_count,
                        encroachment_count   = EXCLUDED.encroachment_count,
                        land_retrieval_count = EXCLUDED.land_retrieval_count,
                        public_nuisance_count= EXCLUDED.public_nuisance_count,
                        updated_at           = NOW()
                    """,
                    (
                        status, data["tehsilNameEnglish"],
                        data.get("totalRequisitions", 0),
                        data.get("hoardingCount", 0),
                        data.get("priceControlCount", 0),
                        data.get("encroachmentCount", 0),
                        data.get("landRetrievalCount", 0),
                        data.get("publicNuisanceCount", 0),
                    ),
                )
                status_count += 1
        total += status_count
        log.info("  status=%s: %d breakdowns", status, status_count)
    log.info("  Total rows: %d", total)
    return total


# ══════════════════════════════════════════════════════════════
# ORCHESTRATION
# ══════════════════════════════════════════════════════════════

_SYNC_FUNCTIONS = {
    1: ("challan_totals", sync_1_totals),
    2: ("challan_by_division", sync_2_division),
    3: ("challan_by_district", sync_3_district),
    4: ("challan_by_tehsil", sync_4_tehsil),
    5: ("challan_tehsil_drill", sync_5_tehsil_drill),
    6: ("challan_requisition_type", sync_6_requisition_type),
    7: ("challan_list", sync_7_list),
    8: ("challan_tehsil_breakdown", sync_8_tehsil_breakdown),
}


def sync_all(
    api_numbers: Optional[List[int]] = None,
    include_list: bool = False,
    hierarchy_only: bool = False,
) -> Dict[str, Any]:
    """Run the complete sync pipeline."""
    start = time.time()
    log.info("=" * 60)
    log.info("PERA Challan Data Sync (PostgreSQL)")
    log.info("=" * 60)

    # Run migrations first
    from analytics_migrations import run_migrations_safe
    run_migrations_safe()

    # Phase 1: Hierarchy
    h_stats = sync_hierarchy()
    log.info("Hierarchy: %d divisions, %d districts, %d tehsils",
             h_stats["divisions"], h_stats["districts"], h_stats["tehsils"])

    if hierarchy_only:
        elapsed = time.time() - start
        log.info("Done (hierarchy only) in %.1fs", elapsed)
        return {"hierarchy": h_stats, "elapsed": elapsed}

    # Phase 2: Challan APIs
    if api_numbers is None:
        api_numbers = [1, 2, 3, 4, 5, 6, 8]
        if include_list:
            api_numbers.insert(6, 7)

    results = {}
    for num in sorted(api_numbers):
        if num not in _SYNC_FUNCTIONS:
            log.warning("Unknown API number %d, skipping.", num)
            continue
        name, fn = _SYNC_FUNCTIONS[num]
        try:
            count = fn()
            results[name] = count
        except Exception as exc:
            log.error("Sync failed for API %d (%s): %s", num, name, exc, exc_info=True)
            results[name] = -1

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info("SYNC COMPLETE in %.1fs", elapsed)
    for name, count in results.items():
        status_str = f"{count:,} rows" if count >= 0 else "FAILED"
        log.info("  %s: %s", name, status_str)

    return {"hierarchy": h_stats, "apis": results, "elapsed": elapsed}


# ══════════════════════════════════════════════════════════════
# CONTINUOUS BACKGROUND SCHEDULER
# ══════════════════════════════════════════════════════════════

class ChallanScheduler:
    """
    Background daemon threads that continuously sync challan data
    from PERA360 APIs into PostgreSQL.

    Three-tier design:
      - FAST thread: syncs API 1 (totals) every 5 seconds
      - FULL thread: syncs APIs 2-6, 8 + hierarchy every 60 seconds
      - DAILY thread: full challan_data ingestion (all tehsils × statuses)
        every daily_interval seconds (default 6 hours)
    """

    def __init__(self, fast_interval: int = 5, full_interval: int = 60,
                 daily_interval: int = 21600):
        self._fast_interval = fast_interval
        self._full_interval = full_interval
        self._daily_interval = daily_interval  # 6 hours default
        self._fast_thread: Optional[threading.Thread] = None
        self._full_thread: Optional[threading.Thread] = None
        self._daily_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._running = False
        self._fast_cycle = 0
        self._full_cycle = 0
        self._daily_cycle = 0

    @property
    def running(self) -> bool:
        return self._running

    @property
    def cycle_count(self) -> int:
        return self._fast_cycle

    def start(self) -> bool:
        """Start both sync threads. Returns True if started."""
        if self._running:
            log.warning("Challan scheduler already running")
            return False
        db = _get_db()
        if not db or not db.is_available():
            log.warning("Challan scheduler not started -- PostgreSQL not available")
            return False

        self._stop.clear()

        # Fast thread: totals every 5 seconds
        self._fast_thread = threading.Thread(
            target=self._fast_loop,
            name="challan-fast-sync",
            daemon=True,
        )
        # Full thread: all other APIs every 60 seconds
        self._full_thread = threading.Thread(
            target=self._full_loop,
            name="challan-full-sync",
            daemon=True,
        )
        # Daily thread: full challan_data ingestion every N hours
        self._daily_thread = threading.Thread(
            target=self._daily_loop,
            name="challan-daily-ingest",
            daemon=True,
        )

        self._fast_thread.start()
        self._full_thread.start()
        self._daily_thread.start()
        self._running = True
        log.info("Challan scheduler started (fast=%ds, full=%ds, daily=%ds)",
                 self._fast_interval, self._full_interval, self._daily_interval)
        return True

    def stop(self):
        """Signal both threads to stop."""
        self._stop.set()
        self._running = False

    def _sleep(self, seconds: int):
        """Sleep in 1-second increments for responsive shutdown."""
        for _ in range(seconds):
            if self._stop.is_set():
                return
            time.sleep(1)

    def _fast_loop(self):
        """Fast sync: API 1 (totals) every few seconds."""
        # Wait a moment for full loop to do init
        self._sleep(3)

        while not self._stop.is_set():
            try:
                sync_1_totals()
                self._fast_cycle += 1
            except Exception as e:
                log.error("Fast sync (totals) error: %s", e)
            self._sleep(self._fast_interval)

    def _full_loop(self):
        """Full sync: hierarchy + APIs 2-6, 8 every 60 seconds."""
        # Run migrations + hierarchy on first cycle
        try:
            from analytics_migrations import run_migrations_safe
            run_migrations_safe()
            sync_hierarchy()
        except Exception as e:
            log.error("Challan scheduler init failed: %s", e)

        while not self._stop.is_set():
            try:
                for num in [2, 3, 4, 5, 6, 8]:
                    if self._stop.is_set():
                        break
                    name, fn = _SYNC_FUNCTIONS[num]
                    try:
                        fn()
                    except Exception as e:
                        log.error("Challan API %d (%s) failed: %s", num, name, e)

                # Re-sync hierarchy
                try:
                    sync_hierarchy()
                except Exception:
                    pass

                self._full_cycle += 1
                log.info("Challan full sync cycle #%d complete", self._full_cycle)
            except Exception as e:
                log.error("Full sync cycle error: %s", e, exc_info=True)

            self._sleep(self._full_interval)

    def _daily_loop(self):
        """Daily ingestion: all tehsils × all statuses → challan_data table.

        Runs immediately on startup (to catch up on missed data), then
        repeats every daily_interval seconds (default 6 hours).
        """
        # Wait for hierarchy to be populated first
        self._sleep(30)

        while not self._stop.is_set():
            try:
                log.info("[Daily Ingest] Starting full challan_data ingestion (fast mode)...")
                from challan_ingest import ingest_all_tehsils
                # Fast mode: one unfiltered API call per tehsil (155 calls vs 2790)
                result = ingest_all_tehsils()
                self._daily_cycle += 1
                log.info(
                    "[Daily Ingest] Cycle #%d complete: %s records from %d tehsils (%d errors, %.1fs)",
                    self._daily_cycle,
                    f"{result.get('total_records', 0):,}",
                    result.get("tehsils_processed", 0),
                    result.get("errors", 0),
                    result.get("elapsed_seconds", 0),
                )
            except Exception as e:
                log.error("[Daily Ingest] Failed: %s", e, exc_info=True)

            self._sleep(self._daily_interval)


# ── CLI ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PERA Challan Data Sync (PostgreSQL)")
    parser.add_argument(
        "--api", type=str, default=None,
        help="Comma-separated API numbers to sync (e.g. 1,2,3)",
    )
    parser.add_argument(
        "--include-list", action="store_true",
        help="Include API 7 (challan_list) — large, takes several minutes",
    )
    parser.add_argument(
        "--hierarchy-only", action="store_true",
        help="Only sync the hierarchy (divisions, districts, tehsils)",
    )
    args = parser.parse_args()

    api_nums = None
    if args.api:
        api_nums = [int(x.strip()) for x in args.api.split(",")]

    result = sync_all(
        api_numbers=api_nums,
        include_list=args.include_list,
        hierarchy_only=args.hierarchy_only,
    )
    print(f"\nSync completed in {result['elapsed']:.1f}s")


if __name__ == "__main__":
    main()
