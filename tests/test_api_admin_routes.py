"""Tests for API admin routes."""
import pytest
from unittest.mock import patch, MagicMock


class TestAdminRoutes:
    """Test admin route logic without requiring a real FastAPI server."""

    def test_list_sources_returns_correct_shape(self):
        """list_sources should return source entries with expected fields."""
        from api_admin_routes import list_sources

        mock_db = MagicMock()
        mock_db.get_all_sources.return_value = [
            {
                "source_id": "pera_employees",
                "display_name": "PERA Employees",
                "status": "synced",
                "status_message": "",
                "record_count": 42,
                "last_sync_at": 1742490000.0,
                "last_updated_at": 1742490000.0,
            },
        ]

        mock_snap = MagicMock()
        mock_snap.get_latest_sync_run.return_value = None

        with patch("api_admin_routes._get_db", return_value=mock_db), \
             patch("api_snapshot_store.ApiSnapshotStore", return_value=mock_snap):
            result = list_sources(enabled=True)

        assert result["total"] == 1
        src = result["sources"][0]
        assert src["source_id"] == "pera_employees"
        assert src["status"] == "synced"
        assert src["record_count"] == 42
        assert "last_error" in src

    def test_trigger_sync_unknown_source_raises_404(self):
        """trigger_sync should raise 404 for unknown source."""
        from api_admin_routes import trigger_sync
        from fastapi import HTTPException

        mock_db = MagicMock()
        mock_db.get_source.return_value = None

        with patch("api_admin_routes._get_db", return_value=mock_db):
            with pytest.raises(HTTPException) as exc:
                trigger_sync("nonexistent", enabled=True)
            assert exc.value.status_code == 404

    def test_delete_source_marks_pending_removal(self):
        """delete_source should mark source as pending_removal."""
        from api_admin_routes import delete_source

        mock_db = MagicMock()
        mock_db.get_source.return_value = {
            "source_id": "test_src",
            "status": "synced",
        }

        with patch("api_admin_routes._get_db", return_value=mock_db):
            result = delete_source("test_src", enabled=True)

        assert result["status"] == "pending_removal"
        assert result["grace_minutes"] > 0
        mock_db.set_source_status.assert_called_once()

    def test_delete_already_removed(self):
        """delete_source on removed source should return already_removed."""
        from api_admin_routes import delete_source

        mock_db = MagicMock()
        mock_db.get_source.return_value = {
            "source_id": "test_src",
            "status": "removed",
        }

        with patch("api_admin_routes._get_db", return_value=mock_db):
            result = delete_source("test_src", enabled=True)
        assert result["status"] == "already_removed"

    def test_sync_all_calls_run_once(self):
        """trigger_sync_all should call run_once on sync manager."""
        from api_admin_routes import trigger_sync_all

        mock_db = MagicMock()
        mock_mgr = MagicMock()
        mock_mgr.run_once.return_value = {"total": 2, "synced": 2, "failed": 0}

        with patch("api_admin_routes._get_db", return_value=mock_db), \
             patch("api_sync_manager.ApiSyncManager", return_value=mock_mgr):
            result = trigger_sync_all(enabled=True)

        assert result["synced"] == 2
        assert result["failed"] == 0

    def test_get_sync_history_unknown_source_raises_404(self):
        """get_sync_history should raise 404 for unknown source."""
        from api_admin_routes import get_sync_history
        from fastapi import HTTPException

        mock_db = MagicMock()
        mock_db.get_source.return_value = None

        with patch("api_admin_routes._get_db", return_value=mock_db):
            with pytest.raises(HTTPException) as exc:
                get_sync_history("nonexistent", enabled=True)
            assert exc.value.status_code == 404

    def test_sync_pending_removal_blocked(self):
        """trigger_sync should block sync on pending_removal sources."""
        from api_admin_routes import trigger_sync
        from fastapi import HTTPException

        mock_db = MagicMock()
        mock_db.get_source.return_value = {
            "source_id": "test_src",
            "status": "pending_removal",
        }

        with patch("api_admin_routes._get_db", return_value=mock_db):
            with pytest.raises(HTTPException) as exc:
                trigger_sync("test_src", enabled=True)
            assert exc.value.status_code == 400
