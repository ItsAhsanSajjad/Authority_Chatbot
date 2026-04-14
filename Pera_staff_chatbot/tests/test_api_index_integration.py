"""Tests for API index integration — chunk JSONL+FAISS operations."""
import json
import os
import tempfile
import pytest

from index_store import (
    _read_jsonl,
    _rewrite_jsonl,
    _safe_mkdir,
    get_chunks_by_source,
    delete_chunks_by_source,
    delete_chunks_by_record_ids,
)


@pytest.fixture
def temp_index_dir():
    """Create a temp index dir with an initial JSONL file."""
    idx_dir = os.path.join(tempfile.gettempdir(), "pera_test_idx_integration")
    os.makedirs(idx_dir, exist_ok=True)
    chunks_path = os.path.join(idx_dir, "chunks.jsonl").replace("\\", "/")

    # Seed with some API chunk rows
    rows = [
        {
            "id": 1, "active": True, "source_type": "api",
            "api_source_id": "src_a", "record_id": "r1",
            "doc_name": "Test API", "text": "Record 1 text",
        },
        {
            "id": 2, "active": True, "source_type": "api",
            "api_source_id": "src_a", "record_id": "r2",
            "doc_name": "Test API", "text": "Record 2 text",
        },
        {
            "id": 3, "active": True, "source_type": "api",
            "api_source_id": "src_b", "record_id": "r1",
            "doc_name": "Other API", "text": "Other source text",
        },
        {
            "id": 4, "active": True, "source_type": "pdf",
            "doc_name": "Document.pdf", "text": "Document text",
        },
    ]
    _rewrite_jsonl(chunks_path, rows)

    yield idx_dir

    import shutil
    try:
        shutil.rmtree(idx_dir, ignore_errors=True)
    except Exception:
        pass


class TestGetChunksBySource:
    def test_get_api_chunks(self, temp_index_dir):
        chunks = get_chunks_by_source(temp_index_dir, "src_a")
        assert len(chunks) == 2
        assert all(c["api_source_id"] == "src_a" for c in chunks)

    def test_get_different_source(self, temp_index_dir):
        chunks = get_chunks_by_source(temp_index_dir, "src_b")
        assert len(chunks) == 1

    def test_get_nonexistent_source(self, temp_index_dir):
        chunks = get_chunks_by_source(temp_index_dir, "nonexistent")
        assert len(chunks) == 0

    def test_filter_by_source_type(self, temp_index_dir):
        chunks = get_chunks_by_source(temp_index_dir, "src_a", source_type="api")
        assert len(chunks) == 2
        chunks = get_chunks_by_source(temp_index_dir, "src_a", source_type="pdf")
        assert len(chunks) == 0


class TestDeleteChunksBySource:
    def test_delete_source_chunks(self, temp_index_dir):
        result = delete_chunks_by_source(temp_index_dir, "src_a")
        assert result["chunks_deactivated"] == 2

        # Verify they are inactive
        chunks = get_chunks_by_source(temp_index_dir, "src_a", active_only=True)
        assert len(chunks) == 0

        # Other source unaffected
        chunks = get_chunks_by_source(temp_index_dir, "src_b")
        assert len(chunks) == 1

    def test_delete_nonexistent_source(self, temp_index_dir):
        result = delete_chunks_by_source(temp_index_dir, "nonexistent")
        assert result["chunks_deactivated"] == 0

    def test_doc_chunks_unaffected(self, temp_index_dir):
        """Deleting API source should not affect document chunks."""
        delete_chunks_by_source(temp_index_dir, "src_a")

        # Read all rows and find doc chunks
        chunks_path = os.path.join(temp_index_dir, "chunks.jsonl").replace("\\", "/")
        rows = _read_jsonl(chunks_path)
        doc_rows = [r for r in rows if r.get("source_type") == "pdf" and r.get("active", True)]
        assert len(doc_rows) == 1


class TestDeleteChunksByRecordIds:
    def test_delete_specific_records(self, temp_index_dir):
        result = delete_chunks_by_record_ids(temp_index_dir, "src_a", ["r1"])
        assert result["chunks_deactivated"] == 1

        # r2 should still be active
        chunks = get_chunks_by_source(temp_index_dir, "src_a")
        assert len(chunks) == 1
        assert chunks[0]["record_id"] == "r2"

    def test_delete_empty_list(self, temp_index_dir):
        result = delete_chunks_by_record_ids(temp_index_dir, "src_a", [])
        assert result["chunks_deactivated"] == 0

    def test_delete_multiple_records(self, temp_index_dir):
        result = delete_chunks_by_record_ids(temp_index_dir, "src_a", ["r1", "r2"])
        assert result["chunks_deactivated"] == 2


class TestChunkRowIntegrity:
    def test_inactive_chunks_excluded(self, temp_index_dir):
        """Inactive chunks should not appear in active-only queries."""
        delete_chunks_by_source(temp_index_dir, "src_a")
        active = get_chunks_by_source(temp_index_dir, "src_a", active_only=True)
        all_chunks = get_chunks_by_source(temp_index_dir, "src_a", active_only=False)
        assert len(active) == 0
        assert len(all_chunks) == 2  # still in JSONL but inactive
