"""Tests for API source deletion lifecycle."""
import pytest
import time
from unittest.mock import patch, MagicMock


class TestDeletionLifecycle:
    """Verify complete deletion lifecycle: pending → data cleanup → removed."""

    def test_mark_pending_removal(self):
        """Source should transition to pending_removal status."""
        from api_registry import ApiSourceRegistry
        mock_db = MagicMock()
        registry = ApiSourceRegistry(mock_db)
        registry.set_source_status("test_src", "pending_removal", "Admin requested")
        mock_db.set_source_status.assert_called_with(
            "test_src", "pending_removal", "Admin requested"
        )

    def test_grace_period_not_elapsed(self):
        """Source should NOT be deleted before grace period expires."""
        from api_scheduler import ApiScheduler

        mock_settings = MagicMock()
        mock_settings.API_INGESTION_ENABLED = True
        mock_settings.API_SYNC_ENABLED = True
        mock_settings.API_SYNC_POLL_SECONDS = 60
        mock_settings.API_DB_URL = ":memory:"
        mock_settings.API_REMOVAL_GRACE_MINUTES = 1440  # 24h

        with patch("api_scheduler.get_settings", return_value=mock_settings):
            sched = ApiScheduler()

            mock_db = MagicMock()
            # Source was updated 10 minutes ago — well within grace
            mock_db.get_sources_by_status.return_value = [{
                "source_id": "test_src",
                "status": "pending_removal",
                "last_updated_at": time.time() - 600,  # 10 min ago
            }]

            # Should NOT call deletion
            with patch.object(sched, "_execute_source_deletion") as mock_del:
                sched._process_pending_removals(mock_db, MagicMock())
                mock_del.assert_not_called()

    def test_grace_period_elapsed_triggers_deletion(self):
        """Source should be deleted after grace period expires."""
        from api_scheduler import ApiScheduler

        mock_settings = MagicMock()
        mock_settings.API_INGESTION_ENABLED = True
        mock_settings.API_SYNC_ENABLED = True
        mock_settings.API_SYNC_POLL_SECONDS = 60
        mock_settings.API_DB_URL = ":memory:"
        mock_settings.API_REMOVAL_GRACE_MINUTES = 5  # 5 min

        with patch("api_scheduler.get_settings", return_value=mock_settings):
            sched = ApiScheduler()

            mock_db = MagicMock()
            # Source was updated 10 minutes ago — past 5 min grace
            mock_db.get_sources_by_status.return_value = [{
                "source_id": "test_src",
                "status": "pending_removal",
                "last_updated_at": time.time() - 600,  # 10 min ago
            }]

            with patch.object(sched, "_execute_source_deletion") as mock_del:
                sched._process_pending_removals(mock_db, MagicMock())
                mock_del.assert_called_once_with(mock_db, "test_src")

    def test_execute_deletion_cleans_index(self):
        """Deletion should remove FAISS chunks and DB records."""
        from api_scheduler import ApiScheduler

        mock_settings = MagicMock()
        mock_settings.API_INGESTION_ENABLED = True
        mock_settings.API_SYNC_ENABLED = True
        mock_settings.API_SYNC_POLL_SECONDS = 60
        mock_settings.API_DB_URL = ":memory:"
        mock_settings.API_REMOVAL_GRACE_MINUTES = 0

        with patch("api_scheduler.get_settings", return_value=mock_settings):
            sched = ApiScheduler()

            mock_db = MagicMock()
            mock_snapshot = MagicMock()
            mock_snapshot.delete_all_source_records.return_value = 5

            with patch("index_store.delete_chunks_by_source") as mock_idx_del, \
                 patch("api_snapshot_store.ApiSnapshotStore", return_value=mock_snapshot):
                sched._execute_source_deletion(mock_db, "test_src")

                # Index chunks deleted
                mock_idx_del.assert_called_once()
                # DB records deleted
                mock_snapshot.delete_all_source_records.assert_called_once_with("test_src")
                # Source marked as removed
                mock_db.set_source_status.assert_called_once()
                call_args = mock_db.set_source_status.call_args
                assert call_args[0][1] == "removed"

    def test_execute_deletion_failure_marks_error(self):
        """If deletion fails, source should be marked as error."""
        from api_scheduler import ApiScheduler

        mock_settings = MagicMock()
        mock_settings.API_INGESTION_ENABLED = True
        mock_settings.API_SYNC_ENABLED = True
        mock_settings.API_SYNC_POLL_SECONDS = 60
        mock_settings.API_DB_URL = ":memory:"
        mock_settings.API_REMOVAL_GRACE_MINUTES = 0

        with patch("api_scheduler.get_settings", return_value=mock_settings):
            sched = ApiScheduler()
            mock_db = MagicMock()

            with patch("index_store.delete_chunks_by_source",
                       side_effect=RuntimeError("index locked")):
                sched._execute_source_deletion(mock_db, "test_src")
                # Should mark as error, not crash
                mock_db.set_source_status.assert_called()

    def test_snapshot_store_hard_delete(self):
        """ApiSnapshotStore.delete_all_source_records should hard-delete."""
        from api_snapshot_store import ApiSnapshotStore

        mock_db = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_cursor
        mock_db.connection.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_db.connection.return_value.__exit__ = MagicMock(return_value=False)

        store = ApiSnapshotStore(mock_db)
        count = store.delete_all_source_records("test_src")
        assert count == 3

    def test_sync_manager_retry_tracking(self):
        """SyncManager should track retry counts per source."""
        from api_sync_manager import ApiSyncManager

        mock_db = MagicMock()
        mock_db.get_source.return_value = {"source_id": "bad_src", "status": "active"}
        mock_db.get_source_config.return_value = None  # Fail at config stage

        mgr = ApiSyncManager(mock_db)

        # First failure — config not found triggers error path
        result = mgr.sync_source("bad_src")
        assert not result.get("success")
        assert mgr._retry_counts.get("bad_src", 0) == 1
        assert mgr.should_retry("bad_src") is True

        # After max failures
        mgr._retry_counts["bad_src"] = 5
        assert mgr.should_retry("bad_src") is False

    def test_retry_backoff_exponential(self):
        """Backoff should increase exponentially."""
        from api_sync_manager import ApiSyncManager

        mock_db = MagicMock()
        mgr = ApiSyncManager(mock_db)
        mgr._base_backoff = 2.0

        mgr._retry_counts["s1"] = 0
        assert mgr.get_retry_delay("s1") == 2.0

        mgr._retry_counts["s1"] = 1
        assert mgr.get_retry_delay("s1") == 4.0

        mgr._retry_counts["s1"] = 3
        assert mgr.get_retry_delay("s1") == 16.0

        # Max cap at 300s
        mgr._retry_counts["s1"] = 10
        assert mgr.get_retry_delay("s1") == 300.0
