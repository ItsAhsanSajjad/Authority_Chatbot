"""
Tests for Analytics Store (analytics_store.py).

Tests write operations for records, dimensions, and fact tables.
"""
from __future__ import annotations

import os
from datetime import date
from unittest.mock import MagicMock, patch

import pytest


class TestAnalyticsStoreFactory:
    """Test analytics store factory function."""

    def test_store_returns_none_when_disabled(self):
        """get_analytics_store should return None when DB is disabled."""
        os.environ["ANALYTICS_DB_ENABLED"] = "0"
        from settings import reset_settings
        from analytics_db import reset_analytics_db
        reset_settings()
        reset_analytics_db()

        from analytics_store import get_analytics_store
        store = get_analytics_store()
        assert store is None

        reset_analytics_db()
        os.environ.pop("ANALYTICS_DB_ENABLED", None)


class TestAnalyticsStoreWithMock:
    """Test store operations with mocked DB."""

    @pytest.fixture
    def mock_db(self):
        """Create a mock AnalyticsDB."""
        db = MagicMock()
        db.is_available.return_value = True
        db.execute.return_value = None
        db.fetch_one.return_value = {"run_id": 1}
        return db

    @pytest.fixture
    def store(self, mock_db):
        """Create an AnalyticsStore with mock DB."""
        from analytics_store import AnalyticsStore
        return AnalyticsStore(mock_db)

    def test_upsert_source_config(self, store, mock_db):
        """upsert_source_config should call execute."""
        result = store.upsert_source_config("test_source", display_name="Test")
        assert result is True
        assert mock_db.execute.called

    def test_write_sync_run(self, store, mock_db):
        """write_sync_run should return a run_id."""
        run_id = store.write_sync_run("test_source", status="completed")
        assert run_id == 1

    def test_upsert_records(self, store, mock_db):
        """upsert_records should process records list."""
        # Mock the connection context manager
        mock_conn = MagicMock()
        mock_db.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db.connection.return_value.__exit__ = MagicMock(return_value=False)

        records = [
            {"record_id": "r1", "record_type": "employee", "content_hash": "abc123",
             "raw_json": '{"name": "test"}', "normalized_text": "Test record"},
            {"record_id": "r2", "record_type": "employee", "content_hash": "def456",
             "raw_json": '{"name": "test2"}', "normalized_text": "Test record 2"},
        ]
        count = store.upsert_records("test_source", records)
        assert count == 2

    def test_upsert_division(self, store, mock_db):
        """upsert_division should call execute."""
        result = store.upsert_division(1, "Lahore Division")
        assert result is True
        assert mock_db.execute.called

    def test_upsert_district(self, store, mock_db):
        """upsert_district should call execute."""
        result = store.upsert_district(10, "Lahore", division_id=1)
        assert result is True

    def test_upsert_tehsil(self, store, mock_db):
        """upsert_tehsil should call execute."""
        result = store.upsert_tehsil(100, "Model Town", district_id=10)
        assert result is True

    def test_upsert_workforce(self, store, mock_db):
        """upsert_workforce should call execute."""
        result = store.upsert_workforce(
            source_id="pera_strength",
            snapshot_date=date.today(),
            designation="Teacher",
            sanctioned_posts=100,
            filled_posts=80,
            vacant_posts=20,
        )
        assert result is True

    def test_upsert_finance(self, store, mock_db):
        """upsert_finance should call execute."""
        result = store.upsert_finance(
            source_id="finance_overview",
            snapshot_date=date.today(),
            fiscal_year="2025-2026",
            budget_head="Education",
            allocated_amount=1000000.00,
            released_amount=750000.00,
            utilized_amount=500000.00,
        )
        assert result is True

    def test_upsert_records_empty_list(self, store, mock_db):
        """upsert_records with empty list should return 0."""
        count = store.upsert_records("test_source", [])
        assert count == 0


class TestAnalyticsStoreGraceful:
    """Test that store fails gracefully."""

    @pytest.fixture
    def failing_db(self):
        """Create a DB mock that raises on every call."""
        db = MagicMock()
        db.execute.side_effect = Exception("DB down")
        db.fetch_one.side_effect = Exception("DB down")
        db.connection.side_effect = Exception("DB down")
        return db

    @pytest.fixture
    def store(self, failing_db):
        from analytics_store import AnalyticsStore
        return AnalyticsStore(failing_db)

    def test_upsert_source_config_fails_gracefully(self, store):
        """Should return False, not raise."""
        result = store.upsert_source_config("src", display_name="X")
        assert result is False

    def test_write_sync_run_fails_gracefully(self, store):
        """Should return None, not raise."""
        result = store.write_sync_run("src")
        assert result is None

    def test_upsert_division_fails_gracefully(self, store):
        """Should return False, not raise."""
        result = store.upsert_division(1, "Test")
        assert result is False

    def test_upsert_workforce_fails_gracefully(self, store):
        """Should return False, not raise."""
        result = store.upsert_workforce("src", date.today())
        assert result is False


class TestAnalyticsMappingMock:
    """Test the analytics mapping with mocked store."""

    @pytest.fixture
    def mock_store(self):
        store = MagicMock()
        store.upsert_division.return_value = True
        store.upsert_district.return_value = True
        store.upsert_tehsil.return_value = True
        store.upsert_workforce.return_value = True
        store.upsert_finance.return_value = True
        store.upsert_finance_monthly.return_value = True
        store.upsert_finance_summary.return_value = True
        return store

    @pytest.fixture
    def mapper(self, mock_store):
        from analytics_mapping import AnalyticsMapper
        return AnalyticsMapper(mock_store)

    def test_has_mapping(self, mapper):
        """Known sources should have mappings."""
        assert mapper.has_mapping("app_data_divisions") is True
        assert mapper.has_mapping("app_data_districts") is True
        assert mapper.has_mapping("pera_strength") is True
        assert mapper.has_mapping("finance_overview") is True
        assert mapper.has_mapping("unknown_api") is False

    def test_map_divisions(self, mapper):
        """Division mapping should process records."""
        records = [
            {"raw_record": {"id": 1, "name": "Lahore"}},
            {"raw_record": {"id": 2, "name": "Rawalpindi"}},
        ]
        count = mapper.map_and_store("app_data_divisions", records)
        assert count == 2

    def test_map_districts(self, mapper):
        """District mapping should process records."""
        records = [
            {"raw_record": {"id": 10, "name": "Lahore", "division_id": 1}},
        ]
        count = mapper.map_and_store("app_data_districts", records)
        assert count == 1

    def test_map_workforce_real_fields(self, mapper, mock_store):
        """Workforce mapping should use real API field names."""
        records = [
            {"raw_record": {
                "id": 21, "divisionId": 21, "divisionName": "Bahawalpur",
                "total": 890, "onDuty": 890, "absent": 0,
            }},
        ]
        count = mapper.map_and_store("pera_strength", records)
        assert count == 1

        # Verify the store was called with correct mapped values
        call_kwargs = mock_store.upsert_workforce.call_args
        assert call_kwargs is not None
        _, kwargs = call_kwargs
        assert kwargs["division_id"] == 21
        assert kwargs["sanctioned_posts"] == 890
        assert kwargs["filled_posts"] == 890
        assert kwargs["vacant_posts"] == 0
        assert kwargs["designation"] == "Bahawalpur"

    def test_map_finance_two_part_model(self, mapper, mock_store):
        """Finance mapping should write BOTH summary and monthly rows."""
        records = [
            {"raw_record": {"month": "Jul", "expenditure": 12.6}},
            {"raw_record": {"month": "Aug", "expenditure": 282.03}},
        ]
        raw_payload = {
            "totalReleased": 9461.06,
            "totalUtilized": 2339.78,
            "utilizationRate": 24.73,
            "series": [
                {"month": "Jul", "expenditure": 12.6},
                {"month": "Aug", "expenditure": 282.03},
            ],
        }
        count = mapper.map_and_store(
            "finance_overview", records, raw_payload=raw_payload,
        )
        # 1 summary + 2 monthly = 3
        assert count == 3

        # Verify summary was written
        assert mock_store.upsert_finance_summary.call_count == 1
        _, summary_kwargs = mock_store.upsert_finance_summary.call_args
        assert summary_kwargs["total_released"] == 9461.06
        assert summary_kwargs["total_utilized"] == 2339.78
        assert summary_kwargs["utilization_rate"] == 24.73

        # Verify monthly was written
        assert mock_store.upsert_finance_monthly.call_count == 2
        assert not mock_store.upsert_finance.called

    def test_map_finance_monthly_only_without_payload(self, mapper, mock_store):
        """Finance mapping without raw_payload should only write monthly rows."""
        records = [
            {"raw_record": {"month": "Jul", "expenditure": 12.6}},
        ]
        count = mapper.map_and_store("finance_overview", records)
        assert count == 1

        assert mock_store.upsert_finance_monthly.call_count == 1
        assert mock_store.upsert_finance_summary.call_count == 0

    def test_finance_skips_empty_month(self, mapper, mock_store):
        """Finance records with no month should be skipped."""
        records = [
            {"raw_record": {"expenditure": 100.0}},
        ]
        count = mapper.map_and_store("finance_overview", records)
        assert count == 0

    def test_unknown_source_skipped(self, mapper):
        """Unknown source_id should return 0."""
        count = mapper.map_and_store("some_other_api", [{"data": 1}])
        assert count == 0

    def test_map_with_missing_id_skips(self, mapper):
        """Records without an id field should be skipped."""
        records = [
            {"raw_record": {"name": "No ID Division"}},
        ]
        count = mapper.map_and_store("app_data_divisions", records)
        assert count == 0


class TestAnalyticsStoreFinanceMethods:
    """Test finance store methods (summary + monthly)."""

    @pytest.fixture
    def mock_db(self):
        db = MagicMock()
        db.execute.return_value = None
        return db

    @pytest.fixture
    def store(self, mock_db):
        from analytics_store import AnalyticsStore
        return AnalyticsStore(mock_db)

    def test_upsert_finance_monthly(self, store, mock_db):
        """upsert_finance_monthly should call execute."""
        result = store.upsert_finance_monthly(
            source_id="finance_overview",
            snapshot_date=date.today(),
            month_label="Jul",
            expenditure_amount=12.6,
        )
        assert result is True
        assert mock_db.execute.called
        # Verify it writes to the correct table
        sql = mock_db.execute.call_args[0][0]
        assert "fact_finance_overview_monthly" in sql

    def test_upsert_finance_summary(self, store, mock_db):
        """upsert_finance_summary should call execute."""
        result = store.upsert_finance_summary(
            source_id="finance_overview",
            snapshot_date=date.today(),
            total_released=9461.06,
            total_utilized=2339.78,
            utilization_rate=24.73,
        )
        assert result is True
        assert mock_db.execute.called
        sql = mock_db.execute.call_args[0][0]
        assert "fact_finance_overview_summary" in sql

    def test_upsert_finance_monthly_fails_gracefully(self):
        """Should return False on failure."""
        db = MagicMock()
        db.execute.side_effect = Exception("DB down")
        from analytics_store import AnalyticsStore
        store = AnalyticsStore(db)
        result = store.upsert_finance_monthly("src", date.today(), month_label="Jan")
        assert result is False

    def test_upsert_finance_summary_fails_gracefully(self):
        """Should return False on failure."""
        db = MagicMock()
        db.execute.side_effect = Exception("DB down")
        from analytics_store import AnalyticsStore
        store = AnalyticsStore(db)
        result = store.upsert_finance_summary("src", date.today())
        assert result is False


class TestAnalyticsStoreLive:
    """
    Live store tests against real PostgreSQL.
    Skipped if ANALYTICS_DB_ENABLED != 1.
    """

    @pytest.fixture(autouse=True)
    def _check_live_db(self):
        if os.environ.get("ANALYTICS_DB_ENABLED") != "1":
            pytest.skip("ANALYTICS_DB_ENABLED not set")

        from analytics_db import AnalyticsDB
        from analytics_migrations import AnalyticsMigrator
        url = os.environ.get("POSTGRES_URL", "postgresql://localhost:5432/pera_ai")
        self.db = AnalyticsDB(url)
        if not self.db.is_available():
            pytest.skip("PostgreSQL not reachable")

        # Ensure tables exist (including new v16-18 migrations)
        AnalyticsMigrator(self.db).migrate()

        from analytics_store import AnalyticsStore
        self.store = AnalyticsStore(self.db)

    def test_live_upsert_records(self):
        """Should write records to api_records_pg."""
        records = [
            {"record_id": "test_mapping_1", "record_type": "test",
             "content_hash": "hash_mc1", "raw_json": '{"test": true}',
             "normalized_text": "Mapping correction test"},
        ]
        count = self.store.upsert_records("test_mapping_source", records)
        assert count == 1

    def test_live_workforce_structured_columns(self):
        """Workforce rows should have structured columns filled."""
        row = self.db.fetch_one(
            "SELECT * FROM fact_workforce_strength "
            "WHERE source_id = 'pera_strength' LIMIT 1"
        )
        if row is None:
            pytest.skip("No pera_strength workforce rows yet")

        # After backfill migration v15, these should be populated
        assert row["sanctioned_posts"] > 0 or row["filled_posts"] > 0, \
            "Workforce structured columns still zero after backfill"

    def test_live_finance_monthly_write(self):
        """Should write to fact_finance_overview_monthly."""
        result = self.store.upsert_finance_monthly(
            source_id="test_finance_live",
            snapshot_date=date(2026, 3, 1),
            month_label="Mar",
            expenditure_amount=999.99,
        )
        assert result is True

        row = self.db.fetch_one(
            "SELECT * FROM fact_finance_overview_monthly "
            "WHERE source_id = %s AND month_label = %s",
            ("test_finance_live", "Mar"),
        )
        assert row is not None
        assert float(row["expenditure_amount"]) == 999.99

    def test_live_finance_summary_write(self):
        """Should write to fact_finance_overview_summary."""
        result = self.store.upsert_finance_summary(
            source_id="test_finance_summary_live",
            snapshot_date=date(2026, 3, 1),
            total_released=9461.06,
            total_utilized=2339.78,
            utilization_rate=24.73,
        )
        assert result is True

        row = self.db.fetch_one(
            "SELECT * FROM fact_finance_overview_summary "
            "WHERE source_id = %s",
            ("test_finance_summary_live",),
        )
        assert row is not None
        assert float(row["total_released"]) == 9461.06
        assert float(row["total_utilized"]) == 2339.78
        assert float(row["utilization_rate"]) == 24.73

    def test_live_old_finance_overview_cleaned(self):
        """Migration v18 should have cleaned stale fact_finance_overview rows."""
        row = self.db.fetch_one(
            "SELECT * FROM fact_finance_overview WHERE source_id = 'finance_overview'"
        )
        assert row is None, "Old fact_finance_overview rows not cleaned by migration v18"

    def test_live_test_rows_cleaned(self):
        """Migration v14 should have removed test rows."""
        row = self.db.fetch_one(
            "SELECT * FROM dim_division WHERE division_id = 999 AND division_name = 'Test Division'"
        )
        assert row is None, "Test division row was not cleaned up by migration v14"

        row = self.db.fetch_one(
            "SELECT * FROM fact_workforce_strength WHERE source_id = 'test_live'"
        )
        assert row is None, "Test workforce row was not cleaned up by migration v14"


