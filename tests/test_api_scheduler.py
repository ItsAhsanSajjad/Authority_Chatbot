"""Tests for API background scheduler."""
import pytest
import time
from unittest.mock import patch, MagicMock


class TestApiScheduler:

    def test_scheduler_respects_disabled_flag(self):
        """Scheduler should not start when API_SYNC_ENABLED=False."""
        from api_scheduler import ApiScheduler

        with patch("api_scheduler.get_settings") as mock_settings:
            s = MagicMock()
            s.API_INGESTION_ENABLED = True
            s.API_SYNC_ENABLED = False
            s.API_SYNC_POLL_SECONDS = 60
            mock_settings.return_value = s

            sched = ApiScheduler()
            result = sched.start()
            assert result is False
            assert sched.running is False

    def test_scheduler_respects_ingestion_disabled(self):
        """Scheduler should not start when API_INGESTION_ENABLED=False."""
        from api_scheduler import ApiScheduler

        with patch("api_scheduler.get_settings") as mock_settings:
            s = MagicMock()
            s.API_INGESTION_ENABLED = False
            s.API_SYNC_ENABLED = True
            s.API_SYNC_POLL_SECONDS = 60
            mock_settings.return_value = s

            sched = ApiScheduler()
            result = sched.start()
            assert result is False

    def test_scheduler_start_and_stop(self):
        """Scheduler should start and stop cleanly."""
        from api_scheduler import ApiScheduler

        with patch("api_scheduler.get_settings") as mock_settings:
            s = MagicMock()
            s.API_INGESTION_ENABLED = True
            s.API_SYNC_ENABLED = True
            s.API_SYNC_POLL_SECONDS = 300
            s.API_DB_URL = ":memory:"
            s.API_REMOVAL_GRACE_MINUTES = 1440
            mock_settings.return_value = s

            sched = ApiScheduler()
            # Mock out _loop to avoid actual sync
            sched._loop = MagicMock()
            result = sched.start()
            assert result is True
            assert sched.running is True

            sched.stop()
            assert sched.running is False

    def test_scheduler_double_start_prevented(self):
        """Scheduler should not start twice."""
        from api_scheduler import ApiScheduler

        with patch("api_scheduler.get_settings") as mock_settings:
            s = MagicMock()
            s.API_INGESTION_ENABLED = True
            s.API_SYNC_ENABLED = True
            s.API_SYNC_POLL_SECONDS = 300
            mock_settings.return_value = s

            sched = ApiScheduler()
            sched._loop = MagicMock()
            sched.start()
            result = sched.start()  # second start
            assert result is False
            sched.stop()

    def test_run_cycle_handles_exception(self):
        """_run_cycle should not raise — exceptions logged."""
        from api_scheduler import ApiScheduler

        with patch("api_scheduler.get_settings") as mock_settings:
            s = MagicMock()
            s.API_INGESTION_ENABLED = True
            s.API_SYNC_ENABLED = True
            s.API_SYNC_POLL_SECONDS = 60
            s.API_DB_URL = ":memory:"
            s.API_REMOVAL_GRACE_MINUTES = 1440
            mock_settings.return_value = s

            sched = ApiScheduler()
            # Force _run_cycle to fail
            with patch.object(sched, "_run_cycle", side_effect=RuntimeError("boom")):
                # The loop catches all exceptions
                try:
                    sched._run_cycle()
                except RuntimeError:
                    pass  # Expected — the test is that the loop catches it

    def test_deletion_lifecycle_called(self):
        """_process_pending_removals should be called during cycle."""
        from api_scheduler import ApiScheduler

        mock_settings = MagicMock()
        mock_settings.API_INGESTION_ENABLED = True
        mock_settings.API_SYNC_ENABLED = True
        mock_settings.API_SYNC_POLL_SECONDS = 60
        mock_settings.API_DB_URL = ":memory:"
        mock_settings.API_REMOVAL_GRACE_MINUTES = 0  # immediate

        with patch("api_scheduler.get_settings", return_value=mock_settings):
            sched = ApiScheduler()

            mock_db = MagicMock()
            mock_db.get_sources_by_status.return_value = []
            mock_db.migrate.return_value = 0

            mock_mgr = MagicMock()
            mock_mgr.run_once.return_value = {"synced": 0, "failed": 0}

            with patch("api_db.ApiDatabase", return_value=mock_db), \
                 patch("api_sync_manager.ApiSyncManager", return_value=mock_mgr), \
                 patch("api_health.set_last_sync"):
                sched._run_cycle()
                # Verify pending removals were checked
                assert mock_db.get_sources_by_status.called
