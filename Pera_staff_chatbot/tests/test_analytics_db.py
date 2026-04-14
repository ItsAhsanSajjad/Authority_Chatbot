"""
Tests for Analytics PostgreSQL Database Layer (analytics_db.py).

Tests run against settings config and verify graceful degradation
when PostgreSQL is unavailable.
"""
from __future__ import annotations

import os
import pytest


class TestAnalyticsDBConfig:
    """Test PostgreSQL configuration loading from settings."""

    def test_analytics_settings_defaults(self):
        """Analytics settings should load from env or have defaults."""
        os.environ["ANALYTICS_DB_ENABLED"] = "0"
        os.environ["ANALYTICS_WRITE_ENABLED"] = "0"
        os.environ.pop("POSTGRES_URL", None)

        from settings import Settings
        s = Settings()
        assert s.ANALYTICS_DB_ENABLED is False
        assert s.ANALYTICS_WRITE_ENABLED is False
        assert s.ANALYTICS_DB_POOL_SIZE == 5
        assert s.ANALYTICS_RETENTION_DAYS == 730
        assert s.ANALYTICS_AUTO_MIGRATE is True
        assert "postgresql" in s.POSTGRES_URL

        os.environ.pop("ANALYTICS_DB_ENABLED", None)
        os.environ.pop("ANALYTICS_WRITE_ENABLED", None)

    def test_analytics_settings_env_override(self):
        """Analytics settings should read from environment variables."""
        os.environ["ANALYTICS_DB_ENABLED"] = "1"
        os.environ["POSTGRES_URL"] = "postgresql://test:pass@db:5432/testdb"
        os.environ["ANALYTICS_DB_POOL_SIZE"] = "10"
        os.environ["ANALYTICS_WRITE_ENABLED"] = "1"

        from settings import Settings
        s = Settings()
        assert s.ANALYTICS_DB_ENABLED is True
        assert s.POSTGRES_URL == "postgresql://test:pass@db:5432/testdb"
        assert s.ANALYTICS_DB_POOL_SIZE == 10
        assert s.ANALYTICS_WRITE_ENABLED is True

        # Cleanup
        os.environ.pop("ANALYTICS_DB_ENABLED", None)
        os.environ.pop("POSTGRES_URL", None)
        os.environ.pop("ANALYTICS_DB_POOL_SIZE", None)
        os.environ.pop("ANALYTICS_WRITE_ENABLED", None)

    def test_analytics_db_returns_none_when_disabled(self):
        """get_analytics_db should return None when disabled."""
        os.environ["ANALYTICS_DB_ENABLED"] = "0"

        from analytics_db import get_analytics_db, reset_analytics_db
        reset_analytics_db()
        db = get_analytics_db()
        assert db is None
        reset_analytics_db()

        os.environ.pop("ANALYTICS_DB_ENABLED", None)

    def test_analytics_db_initializes_when_enabled(self):
        """get_analytics_db should return AnalyticsDB when enabled."""
        os.environ["ANALYTICS_DB_ENABLED"] = "1"
        os.environ["POSTGRES_URL"] = "postgresql://localhost:5432/test"

        from analytics_db import get_analytics_db, reset_analytics_db, AnalyticsDB
        reset_analytics_db()
        db = get_analytics_db()
        assert db is not None
        assert isinstance(db, AnalyticsDB)
        reset_analytics_db()

        os.environ.pop("ANALYTICS_DB_ENABLED", None)
        os.environ.pop("POSTGRES_URL", None)


class TestAnalyticsDBGraceful:
    """Test graceful failure when PostgreSQL is unreachable."""

    def test_is_available_returns_false_unreachable(self):
        """is_available should return False when PG is unreachable."""
        from analytics_db import AnalyticsDB
        db = AnalyticsDB("postgresql://nobody:nopass@localhost:59999/nonexistent?connect_timeout=2")
        assert db.is_available() is False

    def test_connection_raises_on_bad_url(self):
        """connection() should raise when PG is unreachable."""
        from analytics_db import AnalyticsDB
        db = AnalyticsDB("postgresql://nobody:nopass@localhost:59999/nonexistent?connect_timeout=2")
        with pytest.raises(Exception):
            with db.connection() as conn:
                conn.execute("SELECT 1")

    def test_fetch_one_unreachable(self):
        """fetch_one should raise when PG is unreachable."""
        from analytics_db import AnalyticsDB
        db = AnalyticsDB("postgresql://nobody:nopass@localhost:59999/nonexistent?connect_timeout=2")
        with pytest.raises(Exception):
            db.fetch_one("SELECT 1")

    def test_reset_availability(self):
        """reset_availability should clear cached status."""
        from analytics_db import AnalyticsDB
        db = AnalyticsDB("postgresql://nobody:nopass@localhost:59999/nonexistent?connect_timeout=2")
        assert db.is_available() is False
        db.reset_availability()
        assert db._available is None


class TestAnalyticsDBLive:
    """
    Live PostgreSQL tests. Skipped if ANALYTICS_DB_ENABLED != 1
    or database is unreachable.
    """

    @pytest.fixture(autouse=True)
    def _check_live_db(self):
        """Skip tests if live PG is not available."""
        if os.environ.get("ANALYTICS_DB_ENABLED") != "1":
            pytest.skip("ANALYTICS_DB_ENABLED not set — skipping live DB tests")

        from analytics_db import AnalyticsDB
        url = os.environ.get("POSTGRES_URL", "postgresql://localhost:5432/pera_ai")
        db = AnalyticsDB(url)
        if not db.is_available():
            pytest.skip("PostgreSQL not reachable — skipping live DB tests")
        self.db = db

    def test_live_connection(self):
        """Should be able to connect and run a simple query."""
        row = self.db.fetch_one("SELECT 1 AS val")
        assert row is not None
        assert row["val"] == 1

    def test_live_fetch_all(self):
        """fetch_all should return a list of dicts."""
        rows = self.db.fetch_all("SELECT generate_series(1, 3) AS n")
        assert len(rows) == 3
        assert rows[0]["n"] == 1
