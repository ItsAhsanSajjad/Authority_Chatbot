"""Tests for API snapshot store — record upsert/load/delete, sync runs."""
import os
import tempfile
import pytest

from api_db import ApiDatabase
from api_record_builder import NormalizedApiRecord
from api_snapshot_store import ApiSnapshotStore


def _make_record(source_id, record_id, record_hash="hash1", text="test text"):
    return NormalizedApiRecord(
        source_id=source_id,
        record_id=record_id,
        record_type="test",
        canonical_json=f'{{"id": "{record_id}"}}',
        canonical_text=text,
        record_hash=record_hash,
    )


@pytest.fixture
def store_setup():
    """Set up fresh DB and snapshot store for testing."""
    db_path = os.path.join(tempfile.gettempdir(), "pera_test_snapshot.db")
    for f in [db_path, db_path + "-wal", db_path + "-shm"]:
        if os.path.exists(f):
            try:
                os.unlink(f)
            except OSError:
                pass

    db = ApiDatabase(db_path)
    db.migrate()

    # Register a source first (FK requirement)
    db.upsert_source(source_id="test_source", display_name="Test")

    store = ApiSnapshotStore(db)
    yield store, db, db_path

    for f in [db_path, db_path + "-wal", db_path + "-shm"]:
        try:
            if os.path.exists(f):
                os.unlink(f)
        except OSError:
            pass


class TestRecordOperations:
    def test_upsert_and_load(self, store_setup):
        store, db, _ = store_setup
        records = [
            _make_record("test_source", "r1", "h1", "text1"),
            _make_record("test_source", "r2", "h2", "text2"),
        ]
        count = store.upsert_records("test_source", records)
        assert count == 2

        existing = store.get_existing_records("test_source")
        assert len(existing) == 2
        assert existing["r1"] == "h1"
        assert existing["r2"] == "h2"

    def test_upsert_update(self, store_setup):
        store, db, _ = store_setup
        store.upsert_records("test_source", [
            _make_record("test_source", "r1", "old_hash"),
        ])
        store.upsert_records("test_source", [
            _make_record("test_source", "r1", "new_hash"),
        ])

        existing = store.get_existing_records("test_source")
        assert existing["r1"] == "new_hash"

    def test_soft_delete(self, store_setup):
        store, db, _ = store_setup
        store.upsert_records("test_source", [
            _make_record("test_source", "r1"),
            _make_record("test_source", "r2"),
        ])

        count = store.soft_delete_records("test_source", ["r1"])
        assert count == 1

        existing = store.get_existing_records("test_source")
        assert "r1" not in existing
        assert "r2" in existing

    def test_soft_delete_empty(self, store_setup):
        store, _, _ = store_setup
        count = store.soft_delete_records("test_source", [])
        assert count == 0

    def test_delete_all_records(self, store_setup):
        store, _, _ = store_setup
        store.upsert_records("test_source", [
            _make_record("test_source", "r1"),
            _make_record("test_source", "r2"),
        ])
        count = store.delete_all_source_records("test_source")
        assert count == 2

        existing = store.get_existing_records("test_source")
        assert len(existing) == 0

    def test_get_record(self, store_setup):
        store, _, _ = store_setup
        store.upsert_records("test_source", [
            _make_record("test_source", "r1", "h1", "hello"),
        ])
        record = store.get_record("test_source", "r1")
        assert record is not None
        assert record["record_id"] == "r1"
        assert record["content_hash"] == "h1"


class TestSyncRuns:
    def test_create_and_finish(self, store_setup):
        store, _, _ = store_setup
        run_id = store.create_sync_run("test_source")
        assert run_id > 0

        run = store.get_sync_run(run_id)
        assert run["status"] == "running"
        assert run["source_id"] == "test_source"

        store.finish_sync_run(
            run_id,
            status="completed",
            records_fetched=10,
            records_new=5,
            records_updated=3,
            records_removed=2,
        )

        run = store.get_sync_run(run_id)
        assert run["status"] == "completed"
        assert run["records_fetched"] == 10
        assert run["records_new"] == 5
        assert run["completed_at"] is not None

    def test_finish_with_error(self, store_setup):
        store, _, _ = store_setup
        run_id = store.create_sync_run("test_source")

        store.finish_sync_run(
            run_id,
            status="error",
            error_message="Connection refused",
        )

        run = store.get_sync_run(run_id)
        assert run["status"] == "error"
        assert "Connection refused" in run["error_message"]

    def test_latest_sync_run(self, store_setup):
        store, _, _ = store_setup
        run1 = store.create_sync_run("test_source")
        store.finish_sync_run(run1, status="completed")

        run2 = store.create_sync_run("test_source")
        store.finish_sync_run(run2, status="completed")

        latest = store.get_latest_sync_run("test_source")
        assert latest["run_id"] == run2

    def test_no_sync_runs(self, store_setup):
        store, _, _ = store_setup
        latest = store.get_latest_sync_run("nonexistent")
        assert latest is None
