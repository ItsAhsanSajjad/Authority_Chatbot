"""
Tests for Analytics Migrations (analytics_migrations.py).

Validates the migration framework, table creation, and idempotency.
"""
from __future__ import annotations

import os
import pytest


class TestMigrationDefinitions:
    """Test migration definitions are well-formed."""

    def test_migrations_import(self):
        """Migration module should import cleanly."""
        from analytics_migrations import AnalyticsMigrator, _MIGRATIONS
        assert len(_MIGRATIONS) == 18

    def test_migration_versions_sequential(self):
        """Migration versions should be sequential starting from 1."""
        from analytics_migrations import _MIGRATIONS
        versions = [m[0] for m in _MIGRATIONS]
        assert versions == list(range(1, 19))

    def test_migration_descriptions_present(self):
        """Every migration should have a non-empty description."""
        from analytics_migrations import _MIGRATIONS
        for version, desc, stmts in _MIGRATIONS:
            assert desc, f"Migration v{version} has empty description"
            assert stmts, f"Migration v{version} has no SQL statements"

    def test_run_migrations_safe_disabled(self):
        """run_migrations_safe should return 0 when DB is disabled."""
        os.environ["ANALYTICS_DB_ENABLED"] = "0"
        from settings import reset_settings
        from analytics_db import reset_analytics_db
        reset_settings()
        reset_analytics_db()

        from analytics_migrations import run_migrations_safe
        result = run_migrations_safe()
        assert result == 0

        reset_analytics_db()
        os.environ.pop("ANALYTICS_DB_ENABLED", None)


class TestMigrationsLive:
    """
    Live migration tests. Skipped if database is not available.
    """

    @pytest.fixture(autouse=True)
    def _check_live_db(self):
        """Skip tests if live PG is not available."""
        if os.environ.get("ANALYTICS_DB_ENABLED") != "1":
            pytest.skip("ANALYTICS_DB_ENABLED not set")

        from analytics_db import AnalyticsDB
        url = os.environ.get("POSTGRES_URL", "postgresql://localhost:5432/pera_ai")
        self.db = AnalyticsDB(url)
        if not self.db.is_available():
            pytest.skip("PostgreSQL not reachable")

    def test_migrate_creates_tables(self):
        """Migrations should create all expected tables."""
        from analytics_migrations import AnalyticsMigrator
        migrator = AnalyticsMigrator(self.db)
        count = migrator.migrate()
        # Should be >= 0 (tables may already exist)
        assert count >= 0

        tables = migrator.get_table_list()
        expected = [
            "analytics_schema_migrations",
            "api_source_configs_pg",
            "api_sync_runs_pg",
            "api_raw_snapshots",
            "api_records_pg",
            "dim_division",
            "dim_district",
            "dim_tehsil",
            "dim_date",
            "fact_workforce_strength",
            "fact_finance_overview",
            "fact_challan_status_summary",
            "fact_finance_monthly",
            "fact_finance_overview_summary",
            "fact_finance_overview_monthly",
        ]
        for table in expected:
            assert table in tables, f"Table {table} not found"

    def test_migrations_idempotent(self):
        """Running migrations twice should not fail."""
        from analytics_migrations import AnalyticsMigrator
        migrator = AnalyticsMigrator(self.db)
        migrator.migrate()
        # Second run should apply 0 new migrations
        count = migrator.migrate()
        assert count == 0

    def test_dim_date_populated(self):
        """dim_date should have rows for 2020-2030."""
        row = self.db.fetch_one("SELECT COUNT(*) AS cnt FROM dim_date")
        assert row is not None
        # 11 years × ~365 days each ≈ 4017 rows
        assert row["cnt"] > 4000

    def test_get_current_version(self):
        """get_current_version should return the latest migration version."""
        from analytics_migrations import AnalyticsMigrator
        migrator = AnalyticsMigrator(self.db)
        migrator.migrate()
        version = migrator.get_current_version()
        assert version == 18
