"""Tests for API health status module."""
import pytest
from unittest.mock import patch, MagicMock

from api_health import get_api_health_status, set_scheduler_running, set_last_sync


class TestApiHealth:

    def test_health_disabled(self):
        """When API ingestion is disabled, should return minimal status."""
        with patch("settings.get_settings") as mock_s:
            s = MagicMock()
            s.API_INGESTION_ENABLED = False
            mock_s.return_value = s

            result = get_api_health_status()
            assert result["api_ingestion_enabled"] is False
            assert result["total_sources"] == 0

    def test_health_with_sources(self):
        """Should count sources by status correctly."""
        mock_db = MagicMock()
        mock_db.get_all_sources.return_value = [
            {"source_id": "s1", "status": "synced"},
            {"source_id": "s2", "status": "active"},
            {"source_id": "s3", "status": "error"},
            {"source_id": "s4", "status": "pending_removal"},
            {"source_id": "s5", "status": "removed"},
        ]

        with patch("settings.get_settings") as mock_s:
            s = MagicMock()
            s.API_INGESTION_ENABLED = True
            mock_s.return_value = s

            result = get_api_health_status(db=mock_db)
            assert result["total_sources"] == 5
            assert result["active_sources"] == 2  # synced + active
            assert result["synced_sources"] == 1
            assert result["failed_sources"] == 1
            assert result["pending_removal_sources"] == 1

    def test_scheduler_running_state(self):
        """Should reflect scheduler running state."""
        set_scheduler_running(True)
        with patch("settings.get_settings") as mock_s:
            s = MagicMock()
            s.API_INGESTION_ENABLED = False
            mock_s.return_value = s
            result = get_api_health_status()
            assert result["scheduler_running"] is True

        set_scheduler_running(False)
        with patch("settings.get_settings") as mock_s:
            s = MagicMock()
            s.API_INGESTION_ENABLED = False
            mock_s.return_value = s
            result = get_api_health_status()
            assert result["scheduler_running"] is False

    def test_last_sync_time(self):
        """Should record last sync time from set_last_sync."""
        set_last_sync(1742490000.0, "ok")
        mock_db = MagicMock()
        mock_db.get_all_sources.return_value = []

        with patch("settings.get_settings") as mock_s:
            s = MagicMock()
            s.API_INGESTION_ENABLED = True
            mock_s.return_value = s
            result = get_api_health_status(db=mock_db)
            assert result["last_sync_time"] is not None
            assert result["last_sync_status"] == "ok"

    def test_health_structure(self):
        """Return dict should have all expected keys."""
        with patch("settings.get_settings") as mock_s:
            s = MagicMock()
            s.API_INGESTION_ENABLED = False
            mock_s.return_value = s
            result = get_api_health_status()

            expected_keys = {
                "api_ingestion_enabled", "total_sources", "active_sources",
                "failed_sources", "synced_sources", "pending_removal_sources",
                "last_sync_time", "last_sync_status", "scheduler_running",
            }
            assert expected_keys.issubset(result.keys())
