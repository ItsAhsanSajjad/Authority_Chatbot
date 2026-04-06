"""
PERA AI — Analytics Database Migrations

Idempotent SQL migrations for the PostgreSQL analytics schema.
Creates all dimension tables, fact tables, raw ingestion tables,
and supporting indexes.

Usage:
    from analytics_migrations import AnalyticsMigrator
    from analytics_db import get_analytics_db
    db = get_analytics_db()
    if db:
        migrator = AnalyticsMigrator(db)
        migrator.migrate()
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

from log_config import get_logger

log = get_logger("pera.analytics.migrations")

# ── Migration Definitions ─────────────────────────────────────
# Each migration: (version, description, sql_statements_list)

_MIGRATIONS: List[Tuple[int, str, List[str]]] = [
    # ── 1. Migration tracking ─────────────────────────────────
    (1, "Create analytics schema_migrations tracking table", [
        """
        CREATE TABLE IF NOT EXISTS analytics_schema_migrations (
            version         INTEGER PRIMARY KEY,
            description     TEXT NOT NULL,
            applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
    ]),

    # ── 2. Source metadata for analytics ──────────────────────
    (2, "Create api_source_configs_pg table", [
        """
        CREATE TABLE IF NOT EXISTS api_source_configs_pg (
            source_id       TEXT PRIMARY KEY,
            display_name    TEXT NOT NULL DEFAULT '',
            source_type     TEXT NOT NULL DEFAULT 'api',
            config_hash     TEXT DEFAULT '',
            url             TEXT DEFAULT '',
            auth_type       TEXT DEFAULT 'none',
            sync_interval   INTEGER DEFAULT 300,
            enabled         BOOLEAN DEFAULT TRUE,
            first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            metadata_jsonb  JSONB DEFAULT '{}'::jsonb
        )
        """,
    ]),

    # ── 3. Sync run history ───────────────────────────────────
    (3, "Create api_sync_runs_pg table", [
        """
        CREATE TABLE IF NOT EXISTS api_sync_runs_pg (
            run_id          SERIAL PRIMARY KEY,
            source_id       TEXT NOT NULL REFERENCES api_source_configs_pg(source_id),
            started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at    TIMESTAMPTZ,
            status          TEXT NOT NULL DEFAULT 'running',
            records_fetched INTEGER DEFAULT 0,
            records_new     INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            records_removed INTEGER DEFAULT 0,
            error_message   TEXT DEFAULT '',
            run_metadata    JSONB DEFAULT '{}'::jsonb
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_pg_sync_runs_source ON api_sync_runs_pg(source_id)",
        "CREATE INDEX IF NOT EXISTS idx_pg_sync_runs_started ON api_sync_runs_pg(started_at)",
    ]),

    # ── 4. Raw JSON snapshots ─────────────────────────────────
    (4, "Create api_raw_snapshots table", [
        """
        CREATE TABLE IF NOT EXISTS api_raw_snapshots (
            snapshot_id     SERIAL PRIMARY KEY,
            source_id       TEXT NOT NULL REFERENCES api_source_configs_pg(source_id),
            sync_run_id     INTEGER REFERENCES api_sync_runs_pg(run_id),
            snapshot_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
            record_count    INTEGER DEFAULT 0,
            content_hash    TEXT DEFAULT '',
            fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_raw_snapshots_source ON api_raw_snapshots(source_id)",
        "CREATE INDEX IF NOT EXISTS idx_raw_snapshots_fetched ON api_raw_snapshots(fetched_at)",
    ]),

    # ── 5. Normalized API records ─────────────────────────────
    (5, "Create api_records_pg table", [
        """
        CREATE TABLE IF NOT EXISTS api_records_pg (
            id              SERIAL PRIMARY KEY,
            source_id       TEXT NOT NULL REFERENCES api_source_configs_pg(source_id),
            record_id       TEXT NOT NULL,
            record_type     TEXT DEFAULT '',
            content_hash    TEXT NOT NULL,
            raw_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
            normalized_text TEXT DEFAULT '',
            snapshot_date   DATE DEFAULT CURRENT_DATE,
            first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_active       BOOLEAN DEFAULT TRUE,
            UNIQUE(source_id, record_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_pg_records_source ON api_records_pg(source_id)",
        "CREATE INDEX IF NOT EXISTS idx_pg_records_active ON api_records_pg(is_active)",
        "CREATE INDEX IF NOT EXISTS idx_pg_records_snapshot ON api_records_pg(snapshot_date)",
    ]),

    # ── 6. Geography: Divisions ───────────────────────────────
    (6, "Create dim_division table", [
        """
        CREATE TABLE IF NOT EXISTS dim_division (
            division_id     INTEGER PRIMARY KEY,
            division_name   TEXT NOT NULL,
            division_name_ur TEXT DEFAULT '',
            code            TEXT DEFAULT '',
            is_active       BOOLEAN DEFAULT TRUE,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE DEFAULT CURRENT_DATE,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
    ]),

    # ── 7. Geography: Districts ───────────────────────────────
    (7, "Create dim_district table", [
        """
        CREATE TABLE IF NOT EXISTS dim_district (
            district_id     INTEGER PRIMARY KEY,
            district_name   TEXT NOT NULL,
            division_id     INTEGER REFERENCES dim_division(division_id),
            district_name_ur TEXT DEFAULT '',
            code            TEXT DEFAULT '',
            is_active       BOOLEAN DEFAULT TRUE,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE DEFAULT CURRENT_DATE,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_district_division ON dim_district(division_id)",
    ]),

    # ── 8. Geography: Tehsils ─────────────────────────────────
    (8, "Create dim_tehsil table", [
        """
        CREATE TABLE IF NOT EXISTS dim_tehsil (
            tehsil_id       INTEGER PRIMARY KEY,
            tehsil_name     TEXT NOT NULL,
            district_id     INTEGER REFERENCES dim_district(district_id),
            tehsil_name_ur  TEXT DEFAULT '',
            code            TEXT DEFAULT '',
            is_active       BOOLEAN DEFAULT TRUE,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE DEFAULT CURRENT_DATE,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_tehsil_district ON dim_tehsil(district_id)",
    ]),

    # ── 9. Date dimension ─────────────────────────────────────
    (9, "Create dim_date table", [
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key        DATE PRIMARY KEY,
            year            INTEGER NOT NULL,
            month           INTEGER NOT NULL,
            day             INTEGER NOT NULL,
            quarter         INTEGER NOT NULL,
            fiscal_year     TEXT NOT NULL,
            day_of_week     INTEGER NOT NULL,
            is_weekend      BOOLEAN DEFAULT FALSE,
            month_name      TEXT NOT NULL,
            month_name_short TEXT NOT NULL
        )
        """,
        # Pre-populate 2020-01-01 to 2030-12-31
        """
        INSERT INTO dim_date (date_key, year, month, day, quarter,
                              fiscal_year, day_of_week, is_weekend,
                              month_name, month_name_short)
        SELECT
            d::date AS date_key,
            EXTRACT(YEAR FROM d)::int AS year,
            EXTRACT(MONTH FROM d)::int AS month,
            EXTRACT(DAY FROM d)::int AS day,
            EXTRACT(QUARTER FROM d)::int AS quarter,
            CASE
                WHEN EXTRACT(MONTH FROM d) >= 7
                THEN EXTRACT(YEAR FROM d)::text || '-' || (EXTRACT(YEAR FROM d) + 1)::text
                ELSE (EXTRACT(YEAR FROM d) - 1)::text || '-' || EXTRACT(YEAR FROM d)::text
            END AS fiscal_year,
            EXTRACT(ISODOW FROM d)::int AS day_of_week,
            EXTRACT(ISODOW FROM d)::int IN (6, 7) AS is_weekend,
            TO_CHAR(d, 'Month') AS month_name,
            TO_CHAR(d, 'Mon') AS month_name_short
        FROM generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day'::interval) AS d
        ON CONFLICT (date_key) DO NOTHING
        """,
    ]),

    # ── 10. Fact: Workforce Strength ──────────────────────────
    (10, "Create fact_workforce_strength table", [
        """
        CREATE TABLE IF NOT EXISTS fact_workforce_strength (
            id              SERIAL PRIMARY KEY,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE,
            division_id     INTEGER REFERENCES dim_division(division_id),
            district_id     INTEGER REFERENCES dim_district(district_id),
            designation     TEXT DEFAULT '',
            cadre           TEXT DEFAULT '',
            sanctioned_posts INTEGER DEFAULT 0,
            filled_posts    INTEGER DEFAULT 0,
            vacant_posts    INTEGER DEFAULT 0,
            contract_employees INTEGER DEFAULT 0,
            deputation_in   INTEGER DEFAULT 0,
            deputation_out  INTEGER DEFAULT 0,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source_id, snapshot_date, division_id, district_id, designation, cadre)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_workforce_snapshot ON fact_workforce_strength(snapshot_date)",
        "CREATE INDEX IF NOT EXISTS idx_workforce_division ON fact_workforce_strength(division_id)",
        "CREATE INDEX IF NOT EXISTS idx_workforce_district ON fact_workforce_strength(district_id)",
    ]),

    # ── 11. Fact: Finance Overview ────────────────────────────
    (11, "Create fact_finance_overview table", [
        """
        CREATE TABLE IF NOT EXISTS fact_finance_overview (
            id              SERIAL PRIMARY KEY,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE,
            fiscal_year     TEXT DEFAULT '',
            division_id     INTEGER REFERENCES dim_division(division_id),
            district_id     INTEGER REFERENCES dim_district(district_id),
            budget_head     TEXT DEFAULT '',
            allocated_amount NUMERIC(18, 2) DEFAULT 0,
            released_amount  NUMERIC(18, 2) DEFAULT 0,
            utilized_amount  NUMERIC(18, 2) DEFAULT 0,
            balance_amount   NUMERIC(18, 2) DEFAULT 0,
            utilization_pct  NUMERIC(7, 2) DEFAULT 0,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source_id, snapshot_date, fiscal_year, division_id, district_id, budget_head)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_finance_snapshot ON fact_finance_overview(snapshot_date)",
        "CREATE INDEX IF NOT EXISTS idx_finance_fiscal ON fact_finance_overview(fiscal_year)",
        "CREATE INDEX IF NOT EXISTS idx_finance_division ON fact_finance_overview(division_id)",
    ]),

    # ── 12. Fact: Challan Status Summary (scaffold) ───────────
    (12, "Create fact_challan_status_summary table (scaffold)", [
        """
        CREATE TABLE IF NOT EXISTS fact_challan_status_summary (
            id              SERIAL PRIMARY KEY,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE,
            division_id     INTEGER REFERENCES dim_division(division_id),
            district_id     INTEGER REFERENCES dim_district(district_id),
            challan_type    TEXT DEFAULT '',
            total_issued    INTEGER DEFAULT 0,
            total_paid      INTEGER DEFAULT 0,
            total_pending   INTEGER DEFAULT 0,
            total_amount    NUMERIC(18, 2) DEFAULT 0,
            paid_amount     NUMERIC(18, 2) DEFAULT 0,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source_id, snapshot_date, division_id, district_id, challan_type)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_challan_snapshot ON fact_challan_status_summary(snapshot_date)",
    ]),

    # ── 13. Fact: Finance Monthly Expenditure ─────────────────
    (13, "Create fact_finance_monthly table", [
        """
        CREATE TABLE IF NOT EXISTS fact_finance_monthly (
            id              SERIAL PRIMARY KEY,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE,
            fiscal_year     TEXT DEFAULT '',
            month_label     TEXT NOT NULL,
            expenditure_amount NUMERIC(18, 2) DEFAULT 0,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source_id, snapshot_date, month_label)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_finance_monthly_snapshot ON fact_finance_monthly(snapshot_date)",
        "CREATE INDEX IF NOT EXISTS idx_finance_monthly_month ON fact_finance_monthly(month_label)",
    ]),

    # ── 14. Cleanup test/demo rows ────────────────────────────
    (14, "Remove test/demo rows from curated tables", [
        "DELETE FROM dim_division WHERE division_id = 999 AND division_name = 'Test Division'",
        "DELETE FROM fact_workforce_strength WHERE source_id = 'test_live'",
        "DELETE FROM fact_workforce_strength WHERE source_id = 'test_live_source'",
    ]),

    # ── 15. Backfill workforce structured columns from raw_json
    (15, "Backfill workforce structured columns from raw_json", [
        """
        UPDATE fact_workforce_strength
        SET
            division_id     = COALESCE((raw_json->>'divisionId')::int, division_id),
            sanctioned_posts = COALESCE((raw_json->>'total')::int, sanctioned_posts),
            filled_posts    = COALESCE((raw_json->>'onDuty')::int, filled_posts),
            vacant_posts    = COALESCE((raw_json->>'absent')::int, vacant_posts),
            designation     = COALESCE(raw_json->>'divisionName', designation),
            updated_at      = NOW()
        WHERE source_id = 'pera_strength'
          AND raw_json IS NOT NULL
          AND raw_json != '{}'::jsonb
          AND (sanctioned_posts = 0 AND filled_posts = 0 AND vacant_posts = 0)
        """,
    ]),

    # ── 16. Fact: Finance Overview Summary ────────────────────
    (16, "Create fact_finance_overview_summary table", [
        """
        CREATE TABLE IF NOT EXISTS fact_finance_overview_summary (
            id              SERIAL PRIMARY KEY,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE,
            fiscal_year     TEXT DEFAULT '',
            total_released  NUMERIC(18, 2) DEFAULT 0,
            total_utilized  NUMERIC(18, 2) DEFAULT 0,
            utilization_rate NUMERIC(7, 2) DEFAULT 0,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source_id, snapshot_date)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ffo_summary_snapshot ON fact_finance_overview_summary(snapshot_date)",
    ]),

    # ── 17. Rename fact_finance_monthly → fact_finance_overview_monthly
    (17, "Rename fact_finance_monthly to fact_finance_overview_monthly", [
        """
        CREATE TABLE IF NOT EXISTS fact_finance_overview_monthly (
            id              SERIAL PRIMARY KEY,
            source_id       TEXT DEFAULT '',
            snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE,
            fiscal_year     TEXT DEFAULT '',
            month_label     TEXT NOT NULL,
            expenditure_amount NUMERIC(18, 2) DEFAULT 0,
            raw_json        JSONB DEFAULT '{}'::jsonb,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source_id, snapshot_date, month_label)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ffo_monthly_snapshot ON fact_finance_overview_monthly(snapshot_date)",
        "CREATE INDEX IF NOT EXISTS idx_ffo_monthly_month ON fact_finance_overview_monthly(month_label)",
        # Migrate existing data from old table
        """
        INSERT INTO fact_finance_overview_monthly
            (source_id, snapshot_date, fiscal_year, month_label,
             expenditure_amount, raw_json, created_at, updated_at)
        SELECT source_id, snapshot_date, fiscal_year, month_label,
               expenditure_amount, raw_json, created_at, updated_at
        FROM fact_finance_monthly
        ON CONFLICT (source_id, snapshot_date, month_label) DO NOTHING
        """,
    ]),

    # ── 18. Deprecate old fact_finance_overview (truncate bad rows)
    (18, "Truncate misused fact_finance_overview rows", [
        "DELETE FROM fact_finance_overview WHERE source_id = 'finance_overview'",
    ]),

    # ── 25. Challan tables (simple names) ────────────────────────
    (25, "Create challan tables with simple names", [
        # 19a — challan_totals (API 1: overall aggregate, single row)
        """
        CREATE TABLE IF NOT EXISTS challan_totals (
            id                  INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            total_challans      INTEGER DEFAULT 0,
            total_fine_amount   NUMERIC(18,2) DEFAULT 0,
            paid                INTEGER DEFAULT 0,
            unpaid              INTEGER DEFAULT 0,
            overdue             INTEGER DEFAULT 0,
            paid_percent        NUMERIC(7,2) DEFAULT 0,
            unpaid_percent      NUMERIC(7,2) DEFAULT 0,
            overdue_percent     NUMERIC(7,2) DEFAULT 0,
            paid_fine_amount    NUMERIC(18,2) DEFAULT 0,
            unpaid_fine_amount  NUMERIC(18,2) DEFAULT 0,
            overdue_fine_amount NUMERIC(18,2) DEFAULT 0,
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
        """,

        # 19b — challan_by_division (API 2: per status × division)
        """
        CREATE TABLE IF NOT EXISTS challan_by_division (
            id              SERIAL PRIMARY KEY,
            status          TEXT NOT NULL,
            division_id     INTEGER NOT NULL,
            division_name   TEXT DEFAULT '',
            total_challans  INTEGER DEFAULT 0,
            total_amount    NUMERIC(18,2) DEFAULT 0,
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(status, division_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_cbd_status ON challan_by_division(status)",

        # 19c — challan_by_district (API 3: per status × district)
        """
        CREATE TABLE IF NOT EXISTS challan_by_district (
            id              SERIAL PRIMARY KEY,
            status          TEXT NOT NULL,
            division_id     INTEGER,
            district_id     INTEGER NOT NULL,
            district_name   TEXT DEFAULT '',
            total_challans  INTEGER DEFAULT 0,
            total_amount    NUMERIC(18,2) DEFAULT 0,
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(status, district_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_cbdi_status ON challan_by_district(status)",
        "CREATE INDEX IF NOT EXISTS idx_cbdi_division ON challan_by_district(division_id)",

        # 19d — challan_by_tehsil (API 4: per status × tehsil name)
        """
        CREATE TABLE IF NOT EXISTS challan_by_tehsil (
            id              SERIAL PRIMARY KEY,
            status          TEXT NOT NULL,
            tehsil_name     TEXT NOT NULL,
            count           INTEGER DEFAULT 0,
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(status, tehsil_name)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_cbt_status ON challan_by_tehsil(status)",

        # 19e — challan_tehsil_drill (API 5: per status × district × tehsil)
        """
        CREATE TABLE IF NOT EXISTS challan_tehsil_drill (
            id              SERIAL PRIMARY KEY,
            status          TEXT NOT NULL,
            district_id     INTEGER NOT NULL,
            tehsil_id       INTEGER,
            tehsil_name     TEXT DEFAULT '',
            total_challans  INTEGER DEFAULT 0,
            total_amount    NUMERIC(18,2) DEFAULT 0,
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(status, district_id, tehsil_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ctd_status ON challan_tehsil_drill(status)",
        "CREATE INDEX IF NOT EXISTS idx_ctd_district ON challan_tehsil_drill(district_id)",

        # 19f — challan_requisition_type (API 6: per status × req type)
        """
        CREATE TABLE IF NOT EXISTS challan_requisition_type (
            id                      SERIAL PRIMARY KEY,
            status                  TEXT NOT NULL,
            requisition_type_id     INTEGER NOT NULL,
            requisition_type_name   TEXT DEFAULT '',
            total_challans          INTEGER DEFAULT 0,
            total_amount            NUMERIC(18,2) DEFAULT 0,
            updated_at              TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(status, requisition_type_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_crt_status ON challan_requisition_type(status)",

        # 19g — challan_list (API 7: individual challan records — large)
        """
        CREATE TABLE IF NOT EXISTS challan_list (
            challan_id              TEXT PRIMARY KEY,
            status                  TEXT,
            action_date             TEXT,
            challan_paid_date       TEXT,
            consumer_number         TEXT,
            requisition_type_name   TEXT,
            action_officer_name     TEXT,
            fine_amount             NUMERIC(18,2) DEFAULT 0,
            total_paid_amount       NUMERIC(18,2) DEFAULT 0,
            outstanding_amount      NUMERIC(18,2) DEFAULT 0,
            challan_status          TEXT,
            challan_address         TEXT,
            tehsil_name             TEXT,
            district_name           TEXT,
            division_name           TEXT,
            tehsil_id               INTEGER,
            district_id             INTEGER,
            division_id             INTEGER,
            updated_at              TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_cl_status ON challan_list(status)",
        "CREATE INDEX IF NOT EXISTS idx_cl_tehsil ON challan_list(tehsil_id)",
        "CREATE INDEX IF NOT EXISTS idx_cl_district ON challan_list(district_id)",

        # 19h — challan_tehsil_breakdown (API 8: breakdown by req type per tehsil)
        """
        CREATE TABLE IF NOT EXISTS challan_tehsil_breakdown (
            id                      SERIAL PRIMARY KEY,
            status                  TEXT NOT NULL,
            tehsil_name             TEXT NOT NULL,
            total_requisitions      INTEGER DEFAULT 0,
            hoarding_count          INTEGER DEFAULT 0,
            price_control_count     INTEGER DEFAULT 0,
            encroachment_count      INTEGER DEFAULT 0,
            land_retrieval_count    INTEGER DEFAULT 0,
            public_nuisance_count   INTEGER DEFAULT 0,
            updated_at              TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(status, tehsil_name)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ctb_status ON challan_tehsil_breakdown(status)",
    ]),

    # ── 26. challan_data — detailed ingestion table with daily snapshots ──
    (26, "Create challan_data table for detailed ingestion with daily snapshots", [
        """
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
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_challan_data_status   ON challan_data (status)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_tehsil   ON challan_data (tehsil_name)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_district ON challan_data (district_name)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_division ON challan_data (division_name)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_snapshot ON challan_data (snapshot_date)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_req_type ON challan_data (requisition_type_id)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_officer  ON challan_data (officer_name)",
    ]),

    # ── 27. Fix NULL columns in fact_workforce_strength UNIQUE key ──
    (27, "Fix NULL handling in fact_workforce_strength unique constraint", [
        # Step 1: Drop old constraint FIRST (it can't handle NULLs properly)
        "ALTER TABLE fact_workforce_strength DROP CONSTRAINT IF EXISTS fact_workforce_strength_source_id_snapshot_date_division_id_key",
        # Step 2: Drop FK on district_id (allows 0 sentinel value)
        "ALTER TABLE fact_workforce_strength DROP CONSTRAINT IF EXISTS fact_workforce_strength_district_id_fkey",
        # Step 3: Replace NULLs with sentinel values
        """
        UPDATE fact_workforce_strength
        SET division_id = COALESCE(division_id, 0),
            district_id = COALESCE(district_id, 0),
            designation = COALESCE(designation, ''),
            cadre = COALESCE(cadre, '')
        """,
        # Step 4: Set NOT NULL defaults on the key columns
        "ALTER TABLE fact_workforce_strength ALTER COLUMN division_id SET DEFAULT 0",
        "ALTER TABLE fact_workforce_strength ALTER COLUMN district_id SET DEFAULT 0",
        "ALTER TABLE fact_workforce_strength ALTER COLUMN designation SET DEFAULT ''",
        "ALTER TABLE fact_workforce_strength ALTER COLUMN cadre SET DEFAULT ''",
        # Step 5: Remove ALL duplicate rows, keeping only the latest per key
        """
        DELETE FROM fact_workforce_strength
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM fact_workforce_strength
            GROUP BY source_id, snapshot_date, division_id, district_id, designation, cadre
        )
        """,
        # Step 6: Recreate the constraint (now NULLs are gone, upserts work)
        """
        ALTER TABLE fact_workforce_strength
            ADD CONSTRAINT fact_workforce_strength_unique_key
            UNIQUE (source_id, snapshot_date, division_id, district_id, designation, cadre)
        """,
    ]),

    # ── 28. Add division_id, district_id, tehsil_id, updated_at to challan_data ──
    (28, "Add geographic IDs and updated_at to challan_data", [
        "ALTER TABLE challan_data ADD COLUMN IF NOT EXISTS division_id INTEGER",
        "ALTER TABLE challan_data ADD COLUMN IF NOT EXISTS district_id INTEGER",
        "ALTER TABLE challan_data ADD COLUMN IF NOT EXISTS tehsil_id INTEGER",
        "ALTER TABLE challan_data ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_division_id ON challan_data (division_id)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_district_id ON challan_data (district_id)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_tehsil_id ON challan_data (tehsil_id)",
        # Backfill tehsil_id from dim_tehsil by matching name
        """
        UPDATE challan_data cd
        SET tehsil_id = dt.tehsil_id
        FROM dim_tehsil dt
        WHERE LOWER(TRIM(cd.tehsil_name)) = LOWER(TRIM(dt.tehsil_name))
          AND cd.tehsil_id IS NULL
        """,
        # Backfill district_id from dim_tehsil → dim_district linkage
        """
        UPDATE challan_data cd
        SET district_id = dt.district_id
        FROM dim_tehsil dt
        WHERE cd.tehsil_id = dt.tehsil_id
          AND cd.district_id IS NULL
          AND dt.district_id IS NOT NULL
        """,
        # Backfill division_id from dim_district → dim_division linkage
        """
        UPDATE challan_data cd
        SET division_id = dd.division_id
        FROM dim_district dd
        WHERE cd.district_id = dd.district_id
          AND cd.division_id IS NULL
          AND dd.division_id IS NOT NULL
        """,
    ]),

    (29, "Add indexes for date-range challan queries", [
        "CREATE INDEX IF NOT EXISTS idx_challan_data_action_date ON challan_data (action_date)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_tehsil_action ON challan_data (tehsil_name, action_date)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_district_action ON challan_data (district_name, action_date)",
        "CREATE INDEX IF NOT EXISTS idx_challan_data_officer_action ON challan_data (officer_name, action_date)",
    ]),

]


class AnalyticsMigrator:
    """
    Runs idempotent SQL migrations against the analytics PostgreSQL database.
    Tracks applied versions in `analytics_schema_migrations` table.
    """

    def __init__(self, db):
        """
        Args:
            db: AnalyticsDB instance
        """
        self.db = db

    def get_applied_versions(self) -> set:
        """Return set of already-applied migration versions."""
        try:
            rows = self.db.fetch_all(
                "SELECT version FROM analytics_schema_migrations"
            )
            return {r["version"] for r in rows}
        except Exception:
            # Table doesn't exist yet — first run
            return set()

    def migrate(self) -> int:
        """
        Run all pending migrations in order.
        Returns count of newly applied migrations.
        """
        applied = self.get_applied_versions()
        count = 0

        for version, description, statements in _MIGRATIONS:
            if version in applied:
                continue

            log.info("Applying analytics migration v%d: %s", version, description)

            with self.db.connection() as conn:
                for sql in statements:
                    conn.execute(sql)

                # Record the migration
                try:
                    conn.execute(
                        "INSERT INTO analytics_schema_migrations "
                        "(version, description) VALUES (%s, %s) "
                        "ON CONFLICT (version) DO NOTHING",
                        (version, description),
                    )
                except Exception:
                    pass  # Table may not exist yet on v1

            count += 1

        if count > 0:
            log.info("Applied %d analytics migration(s)", count)
        else:
            log.info("Analytics schema is up to date")

        return count

    def get_current_version(self) -> int:
        """Return the highest applied migration version."""
        applied = self.get_applied_versions()
        return max(applied) if applied else 0

    def get_table_list(self) -> List[str]:
        """Return list of analytics tables in the database."""
        rows = self.db.fetch_all(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
            "ORDER BY tablename"
        )
        return [r["tablename"] for r in rows]


def run_migrations_safe() -> int:
    """
    Convenience: run migrations using the global AnalyticsDB.
    Returns migration count, or 0 if DB is unavailable.
    """
    try:
        from analytics_db import get_analytics_db
        db = get_analytics_db()
        if not db or not db.is_available():
            log.info("Analytics DB not available — skipping migrations")
            return 0
        migrator = AnalyticsMigrator(db)
        return migrator.migrate()
    except Exception as e:
        log.error("Analytics migration failed: %s", e)
        return 0
