"""Tests for API sync manager — sync_source, sync_all, run_once, error handling."""
import os
import json
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from api_config_models import (
    ApiSourceConfig, ApiFetchConfig, ApiAuthConfig,
    ApiNormalizationConfig, ApiSyncConfig, ApiIndexingConfig,
)
from api_db import ApiDatabase
from api_fetcher import ApiFetchResult
from api_normalizer import ApiNormalizationResult
from api_record_builder import NormalizedApiRecord
from api_diff import ApiDiffResult
from api_sync_manager import ApiSyncManager


@pytest.fixture
def test_env():
    """Set up fresh DB and temp index dir."""
    db_path = os.path.join(tempfile.gettempdir(), "pera_test_sync.db")
    idx_dir = os.path.join(tempfile.gettempdir(), "pera_test_index")
    os.makedirs(idx_dir, exist_ok=True)

    for f in [db_path, db_path + "-wal", db_path + "-shm"]:
        if os.path.exists(f):
            try:
                os.unlink(f)
            except OSError:
                pass

    db = ApiDatabase(db_path)
    db.migrate()

    # Register a source and config
    db.upsert_source(
        source_id="test_api",
        display_name="Test API",
        status="active",
    )

    config_yaml = """
source_id: test_api
source_type: api
display_name: Test API
enabled: true
fetch:
  method: GET
  url: https://example.com/api/data
  timeout_seconds: 5
auth:
  type: none
normalization:
  root_selector: data
  record_id_field: id
  record_type: test
sync:
  interval_minutes: 30
  delete_missing_records: true
indexing:
  authority: 2
  tags:
    - test
"""
    db.upsert_source_config(
        source_id="test_api",
        config_yaml=config_yaml,
        config_hash="hash123",
        url="https://example.com/api/data",
    )

    manager = ApiSyncManager(db, index_dir=idx_dir)
    yield manager, db, db_path, idx_dir

    # Cleanup
    for f in [db_path, db_path + "-wal", db_path + "-shm"]:
        try:
            if os.path.exists(f):
                os.unlink(f)
        except OSError:
            pass

    import shutil
    try:
        shutil.rmtree(idx_dir, ignore_errors=True)
    except Exception:
        pass


class TestSyncSource:
    @patch("api_sync_manager.upsert_api_chunks")
    @patch("api_sync_manager.delete_chunks_by_record_ids")
    def test_sync_success(self, mock_delete_chunks, mock_upsert_chunks, test_env):
        """Full sync should succeed with mocked fetch/normalize."""
        manager, db, _, _ = test_env
        mock_upsert_chunks.return_value = {"chunks_added": 2, "chunks_deactivated": 0, "vectors_added": 2}
        mock_delete_chunks.return_value = {"chunks_deactivated": 0}

        # Mock the fetcher
        manager._fetcher = MagicMock()
        manager._fetcher.fetch_source.return_value = ApiFetchResult(
            source_id="test_api",
            success=True,
            payload={"data": [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]},
            snapshot_hash="abc123",
        )

        result = manager.sync_source("test_api")

        assert result["success"] is True
        assert result["records_fetched"] == 2

        # Source should be synced
        source = db.get_source("test_api")
        assert source["status"] == "synced"

    def test_sync_unknown_source(self, test_env):
        """Sync should fail for unknown source."""
        manager, _, _, _ = test_env
        result = manager.sync_source("nonexistent")
        assert result["success"] is False
        assert "not found" in result["error"]

    @patch("api_sync_manager.upsert_api_chunks")
    @patch("api_sync_manager.delete_chunks_by_record_ids")
    def test_sync_fetch_failure(self, mock_delete, mock_upsert, test_env):
        """Sync should handle fetch failure gracefully."""
        manager, db, _, _ = test_env

        manager._fetcher = MagicMock()
        manager._fetcher.fetch_source.return_value = ApiFetchResult(
            source_id="test_api",
            success=False,
            error_message="Connection refused",
        )

        result = manager.sync_source("test_api")
        assert result["success"] is False
        assert "Connection refused" in result.get("error", "")

        # Source should be in error state
        source = db.get_source("test_api")
        assert source["status"] == "error"


class TestSyncAllSources:
    @patch("api_sync_manager.upsert_api_chunks")
    @patch("api_sync_manager.delete_chunks_by_record_ids")
    def test_sync_all(self, mock_delete, mock_upsert, test_env):
        """sync_all_sources should process active sources."""
        manager, _, _, _ = test_env
        mock_upsert.return_value = {"chunks_added": 1, "chunks_deactivated": 0, "vectors_added": 1}
        mock_delete.return_value = {"chunks_deactivated": 0}

        manager._fetcher = MagicMock()
        manager._fetcher.fetch_source.return_value = ApiFetchResult(
            source_id="test_api",
            success=True,
            payload={"data": [{"id": "1", "name": "Test"}]},
            snapshot_hash="xyz",
        )

        result = manager.sync_all_sources()
        assert result["total"] >= 1
        assert result["synced"] >= 1


class TestRunOnce:
    @patch("api_sync_manager.upsert_api_chunks")
    @patch("api_sync_manager.delete_chunks_by_record_ids")
    def test_run_once(self, mock_delete, mock_upsert, test_env):
        """run_once should do discovery + sync."""
        manager, _, _, _ = test_env
        mock_upsert.return_value = {"chunks_added": 0, "chunks_deactivated": 0, "vectors_added": 0}
        mock_delete.return_value = {"chunks_deactivated": 0}

        manager._fetcher = MagicMock()
        manager._fetcher.fetch_source.return_value = ApiFetchResult(
            source_id="test_api",
            success=True,
            payload={"data": [{"id": "1"}]},
            snapshot_hash="run123",
        )

        result = manager.run_once()
        # Should have discovery and sync results
        assert "total" in result


class TestBackgroundPlaceholders:
    def test_start_background_no_crash(self, test_env):
        """start_background_sync should not crash (placeholder)."""
        manager, _, _, _ = test_env
        manager.start_background_sync()

    def test_schedule_loop_no_crash(self, test_env):
        """schedule_loop should not crash (placeholder)."""
        manager, _, _, _ = test_env
        manager.schedule_loop()
