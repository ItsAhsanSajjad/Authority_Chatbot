"""
PERA AI — Analytics Structured Ingestion Store

Writes normalized API records and curated dimension/fact data
into the PostgreSQL analytics database.

All writes are wrapped in try/except — failures are logged but
never crash the existing pipeline.

Usage:
    from analytics_store import get_analytics_store
    store = get_analytics_store()
    if store:
        store.upsert_records(source_id, records)
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from log_config import get_logger

log = get_logger("pera.analytics.store")


class AnalyticsStore:
    """
    Structured data persistence for the analytics PostgreSQL database.
    All public methods catch exceptions and log warnings instead of raising.
    """

    def __init__(self, db):
        """
        Args:
            db: AnalyticsDB instance
        """
        self.db = db

    # ── Source Config ──────────────────────────────────────────

    def upsert_source_config(
        self,
        source_id: str,
        display_name: str = "",
        source_type: str = "api",
        config_hash: str = "",
        url: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Write/update source config metadata. Returns True on success."""
        try:
            self.db.execute(
                """
                INSERT INTO api_source_configs_pg
                    (source_id, display_name, source_type, config_hash, url,
                     metadata_jsonb, last_updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (source_id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    source_type = EXCLUDED.source_type,
                    config_hash = EXCLUDED.config_hash,
                    url = EXCLUDED.url,
                    metadata_jsonb = EXCLUDED.metadata_jsonb,
                    last_updated_at = NOW()
                """,
                (source_id, display_name, source_type, config_hash, url,
                 json.dumps(metadata or {})),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to upsert source config %s: %s", source_id, e)
            return False

    # ── Sync Runs ─────────────────────────────────────────────

    def write_sync_run(
        self,
        source_id: str,
        status: str = "completed",
        records_fetched: int = 0,
        records_new: int = 0,
        records_updated: int = 0,
        records_removed: int = 0,
        error_message: str = "",
    ) -> Optional[int]:
        """Record a sync run. Returns run_id or None on failure."""
        try:
            # Ensure source exists first
            self.upsert_source_config(source_id)
            row = self.db.fetch_one(
                """
                INSERT INTO api_sync_runs_pg
                    (source_id, status, records_fetched, records_new,
                     records_updated, records_removed, error_message,
                     completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                        CASE WHEN %s != 'running' THEN NOW() ELSE NULL END)
                RETURNING run_id
                """,
                (source_id, status, records_fetched, records_new,
                 records_updated, records_removed, error_message, status),
            )
            return row["run_id"] if row else None
        except Exception as e:
            log.warning("Analytics: failed to write sync run for %s: %s", source_id, e)
            return None

    # ── Raw Snapshots ─────────────────────────────────────────

    def write_raw_snapshot(
        self,
        source_id: str,
        snapshot_data: Any,
        record_count: int = 0,
        content_hash: str = "",
        sync_run_id: Optional[int] = None,
    ) -> bool:
        """Store a raw API snapshot. Returns True on success."""
        try:
            snapshot_json = json.dumps(snapshot_data, ensure_ascii=False, default=str)
            self.db.execute(
                """
                INSERT INTO api_raw_snapshots
                    (source_id, sync_run_id, snapshot_json, record_count,
                     content_hash)
                VALUES (%s, %s, %s::jsonb, %s, %s)
                """,
                (source_id, sync_run_id, snapshot_json, record_count, content_hash),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to write raw snapshot for %s: %s", source_id, e)
            return False

    # ── Normalized Records ────────────────────────────────────

    def upsert_records(
        self,
        source_id: str,
        records: List[Dict[str, Any]],
        snapshot_date: Optional[date] = None,
    ) -> int:
        """
        Upsert normalized API records into PostgreSQL.
        Accepts dicts with keys: record_id, record_type, content_hash,
        raw_json/canonical_json, normalized_text/canonical_text.
        Returns count of records written.
        """
        if not records:
            return 0

        snap_date = snapshot_date or date.today()
        count = 0

        try:
            # Ensure source exists
            self.upsert_source_config(source_id)

            with self.db.connection() as conn:
                for rec in records:
                    record_id = rec.get("record_id", "")
                    record_type = rec.get("record_type", "")
                    content_hash = rec.get("content_hash") or rec.get("record_hash", "")
                    raw_json = rec.get("raw_json") or rec.get("canonical_json", "{}")
                    normalized_text = rec.get("normalized_text") or rec.get("canonical_text", "")

                    # Ensure raw_json is a JSON string
                    if isinstance(raw_json, dict):
                        raw_json = json.dumps(raw_json, ensure_ascii=False, default=str)

                    conn.execute(
                        """
                        INSERT INTO api_records_pg
                            (source_id, record_id, record_type, content_hash,
                             raw_json, normalized_text, snapshot_date)
                        VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                        ON CONFLICT (source_id, record_id) DO UPDATE SET
                            record_type = EXCLUDED.record_type,
                            content_hash = EXCLUDED.content_hash,
                            raw_json = EXCLUDED.raw_json,
                            normalized_text = EXCLUDED.normalized_text,
                            snapshot_date = EXCLUDED.snapshot_date,
                            last_updated_at = NOW(),
                            is_active = TRUE
                        """,
                        (source_id, record_id, record_type, content_hash,
                         raw_json, normalized_text, snap_date),
                    )
                    count += 1

            log.info("Analytics: upserted %d records for %s", count, source_id)
        except Exception as e:
            log.warning("Analytics: failed to upsert records for %s: %s", source_id, e)

        return count

    # ── Geography Dimensions ──────────────────────────────────

    def upsert_division(
        self,
        division_id: int,
        division_name: str,
        division_name_ur: str = "",
        code: str = "",
        source_id: str = "",
        raw_json: Optional[Dict] = None,
    ) -> bool:
        """Upsert a division record."""
        try:
            self.db.execute(
                """
                INSERT INTO dim_division
                    (division_id, division_name, division_name_ur, code,
                     source_id, raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (division_id) DO UPDATE SET
                    division_name = EXCLUDED.division_name,
                    division_name_ur = EXCLUDED.division_name_ur,
                    code = EXCLUDED.code,
                    source_id = EXCLUDED.source_id,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                """,
                (division_id, division_name, division_name_ur, code,
                 source_id, json.dumps(raw_json or {}, default=str)),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to upsert division %d: %s", division_id, e)
            return False

    def upsert_district(
        self,
        district_id: int,
        district_name: str,
        division_id: Optional[int] = None,
        district_name_ur: str = "",
        code: str = "",
        source_id: str = "",
        raw_json: Optional[Dict] = None,
    ) -> bool:
        """Upsert a district record."""
        try:
            self.db.execute(
                """
                INSERT INTO dim_district
                    (district_id, district_name, division_id, district_name_ur,
                     code, source_id, raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (district_id) DO UPDATE SET
                    district_name = EXCLUDED.district_name,
                    division_id = EXCLUDED.division_id,
                    district_name_ur = EXCLUDED.district_name_ur,
                    code = EXCLUDED.code,
                    source_id = EXCLUDED.source_id,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                """,
                (district_id, district_name, division_id, district_name_ur,
                 code, source_id, json.dumps(raw_json or {}, default=str)),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to upsert district %d: %s", district_id, e)
            return False

    def upsert_tehsil(
        self,
        tehsil_id: int,
        tehsil_name: str,
        district_id: Optional[int] = None,
        tehsil_name_ur: str = "",
        code: str = "",
        source_id: str = "",
        raw_json: Optional[Dict] = None,
    ) -> bool:
        """Upsert a tehsil record."""
        try:
            self.db.execute(
                """
                INSERT INTO dim_tehsil
                    (tehsil_id, tehsil_name, district_id, tehsil_name_ur,
                     code, source_id, raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (tehsil_id) DO UPDATE SET
                    tehsil_name = EXCLUDED.tehsil_name,
                    district_id = EXCLUDED.district_id,
                    tehsil_name_ur = EXCLUDED.tehsil_name_ur,
                    code = EXCLUDED.code,
                    source_id = EXCLUDED.source_id,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                """,
                (tehsil_id, tehsil_name, district_id, tehsil_name_ur,
                 code, source_id, json.dumps(raw_json or {}, default=str)),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to upsert tehsil %d: %s", tehsil_id, e)
            return False

    # ── Fact Tables ───────────────────────────────────────────

    def upsert_workforce(
        self,
        source_id: str,
        snapshot_date: date,
        designation: str = "",
        cadre: str = "",
        division_id: Optional[int] = None,
        district_id: Optional[int] = None,
        sanctioned_posts: int = 0,
        filled_posts: int = 0,
        vacant_posts: int = 0,
        contract_employees: int = 0,
        deputation_in: int = 0,
        deputation_out: int = 0,
        raw_json: Optional[Dict] = None,
    ) -> bool:
        """Upsert a workforce strength fact record.

        Uses COALESCE with sentinel values (0 for IDs, '' for text)
        so that NULL columns participate correctly in ON CONFLICT.
        PostgreSQL treats NULL != NULL in UNIQUE constraints, which
        would prevent the upsert from matching existing rows.
        """
        # Normalize NULLs to sentinel values for the UNIQUE key columns
        safe_division_id = division_id if division_id is not None else 0
        safe_district_id = district_id if district_id is not None else 0
        safe_designation = designation or ""
        safe_cadre = cadre or ""

        try:
            self.db.execute(
                """
                INSERT INTO fact_workforce_strength
                    (source_id, snapshot_date, division_id, district_id,
                     designation, cadre, sanctioned_posts, filled_posts,
                     vacant_posts, contract_employees, deputation_in,
                     deputation_out, raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, NOW())
                ON CONFLICT (source_id, snapshot_date, division_id,
                             district_id, designation, cadre)
                DO UPDATE SET
                    sanctioned_posts = EXCLUDED.sanctioned_posts,
                    filled_posts = EXCLUDED.filled_posts,
                    vacant_posts = EXCLUDED.vacant_posts,
                    contract_employees = EXCLUDED.contract_employees,
                    deputation_in = EXCLUDED.deputation_in,
                    deputation_out = EXCLUDED.deputation_out,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                """,
                (source_id, snapshot_date, safe_division_id, safe_district_id,
                 safe_designation, safe_cadre, sanctioned_posts, filled_posts,
                 vacant_posts, contract_employees, deputation_in,
                 deputation_out, json.dumps(raw_json or {}, default=str)),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to upsert workforce: %s", e)
            return False

    def upsert_finance(
        self,
        source_id: str,
        snapshot_date: date,
        fiscal_year: str = "",
        division_id: Optional[int] = None,
        district_id: Optional[int] = None,
        budget_head: str = "",
        allocated_amount: float = 0.0,
        released_amount: float = 0.0,
        utilized_amount: float = 0.0,
        balance_amount: float = 0.0,
        utilization_pct: float = 0.0,
        raw_json: Optional[Dict] = None,
    ) -> bool:
        """Upsert a finance overview fact record."""
        try:
            self.db.execute(
                """
                INSERT INTO fact_finance_overview
                    (source_id, snapshot_date, fiscal_year, division_id,
                     district_id, budget_head, allocated_amount,
                     released_amount, utilized_amount, balance_amount,
                     utilization_pct, raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, NOW())
                ON CONFLICT (source_id, snapshot_date, fiscal_year,
                             division_id, district_id, budget_head)
                DO UPDATE SET
                    allocated_amount = EXCLUDED.allocated_amount,
                    released_amount = EXCLUDED.released_amount,
                    utilized_amount = EXCLUDED.utilized_amount,
                    balance_amount = EXCLUDED.balance_amount,
                    utilization_pct = EXCLUDED.utilization_pct,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                """,
                (source_id, snapshot_date, fiscal_year, division_id,
                 district_id, budget_head, allocated_amount, released_amount,
                 utilized_amount, balance_amount, utilization_pct,
                 json.dumps(raw_json or {}, default=str)),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to upsert finance: %s", e)
            return False

    def upsert_finance_monthly(
        self,
        source_id: str,
        snapshot_date: date,
        month_label: str = "",
        expenditure_amount: float = 0.0,
        fiscal_year: str = "",
        raw_json: Optional[Dict] = None,
    ) -> bool:
        """Upsert a monthly expenditure record to fact_finance_overview_monthly."""
        try:
            self.db.execute(
                """
                INSERT INTO fact_finance_overview_monthly
                    (source_id, snapshot_date, fiscal_year, month_label,
                     expenditure_amount, raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (source_id, snapshot_date, month_label)
                DO UPDATE SET
                    fiscal_year = EXCLUDED.fiscal_year,
                    expenditure_amount = EXCLUDED.expenditure_amount,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                """,
                (source_id, snapshot_date, fiscal_year, month_label,
                 expenditure_amount, json.dumps(raw_json or {}, default=str)),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to upsert finance monthly: %s", e)
            return False

    def upsert_finance_summary(
        self,
        source_id: str,
        snapshot_date: date,
        total_released: float = 0.0,
        total_utilized: float = 0.0,
        utilization_rate: float = 0.0,
        fiscal_year: str = "",
        raw_json: Optional[Dict] = None,
    ) -> bool:
        """Upsert a finance overview summary record to fact_finance_overview_summary."""
        try:
            self.db.execute(
                """
                INSERT INTO fact_finance_overview_summary
                    (source_id, snapshot_date, fiscal_year,
                     total_released, total_utilized, utilization_rate,
                     raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
                ON CONFLICT (source_id, snapshot_date)
                DO UPDATE SET
                    fiscal_year = EXCLUDED.fiscal_year,
                    total_released = EXCLUDED.total_released,
                    total_utilized = EXCLUDED.total_utilized,
                    utilization_rate = EXCLUDED.utilization_rate,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
                """,
                (source_id, snapshot_date, fiscal_year,
                 total_released, total_utilized, utilization_rate,
                 json.dumps(raw_json or {}, default=str)),
            )
            return True
        except Exception as e:
            log.warning("Analytics: failed to upsert finance summary: %s", e)
            return False

    # ── Queries (read helpers for future use) ─────────────────

    def get_division_count(self) -> int:
        """Return count of divisions."""
        try:
            row = self.db.fetch_one("SELECT COUNT(*) AS cnt FROM dim_division")
            return row["cnt"] if row else 0
        except Exception:
            return 0

    def get_district_count(self) -> int:
        """Return count of districts."""
        try:
            row = self.db.fetch_one("SELECT COUNT(*) AS cnt FROM dim_district")
            return row["cnt"] if row else 0
        except Exception:
            return 0

    def get_record_count(self, source_id: Optional[str] = None) -> int:
        """Return count of analytics API records."""
        try:
            if source_id:
                row = self.db.fetch_one(
                    "SELECT COUNT(*) AS cnt FROM api_records_pg WHERE source_id = %s",
                    (source_id,),
                )
            else:
                row = self.db.fetch_one("SELECT COUNT(*) AS cnt FROM api_records_pg")
            return row["cnt"] if row else 0
        except Exception:
            return 0


# ── Factory ───────────────────────────────────────────────────


def get_analytics_store():
    """
    Return an AnalyticsStore instance, or None if analytics DB is disabled.
    """
    try:
        from analytics_db import get_analytics_db
        db = get_analytics_db()
        if not db:
            return None
        return AnalyticsStore(db)
    except Exception as e:
        log.warning("Failed to create AnalyticsStore: %s", e)
        return None
