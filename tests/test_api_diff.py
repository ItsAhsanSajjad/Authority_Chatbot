"""Tests for API diff engine — added, changed, unchanged, deleted classification."""
import pytest

from api_diff import ApiDiffEngine, ApiDiffResult
from api_record_builder import NormalizedApiRecord


def _make_record(record_id, record_hash="hash_default"):
    return NormalizedApiRecord(
        source_id="test_source",
        record_id=record_id,
        record_hash=record_hash,
    )


@pytest.fixture
def engine():
    return ApiDiffEngine()


class TestDiffRecords:
    def test_all_new(self, engine):
        """All records should be classified as added when no existing."""
        new = [_make_record("1"), _make_record("2")]
        result = engine.diff_records("test", new, {})

        assert result.added_count == 2
        assert result.changed_count == 0
        assert result.unchanged_count == 0
        assert result.deleted_count == 0
        assert result.has_changes is True

    def test_all_unchanged(self, engine):
        """Records with matching hashes should be unchanged."""
        new = [
            _make_record("1", "aaa"),
            _make_record("2", "bbb"),
        ]
        existing = {"1": "aaa", "2": "bbb"}
        result = engine.diff_records("test", new, existing)

        assert result.added_count == 0
        assert result.changed_count == 0
        assert result.unchanged_count == 2
        assert result.deleted_count == 0
        assert result.has_changes is False

    def test_changed_records(self, engine):
        """Records with different hashes should be classified as changed."""
        new = [_make_record("1", "new_hash")]
        existing = {"1": "old_hash"}
        result = engine.diff_records("test", new, existing)

        assert result.added_count == 0
        assert result.changed_count == 1
        assert result.has_changes is True

    def test_deleted_records(self, engine):
        """Records in existing but not in new should be deleted."""
        new = [_make_record("1", "aaa")]
        existing = {"1": "aaa", "2": "bbb", "3": "ccc"}
        result = engine.diff_records("test", new, existing)

        assert result.deleted_count == 2
        assert "2" in result.deleted_record_ids
        assert "3" in result.deleted_record_ids

    def test_mixed_diff(self, engine):
        """Should handle a mix of added, changed, unchanged, deleted."""
        new = [
            _make_record("1", "same"),       # unchanged
            _make_record("2", "updated"),     # changed
            _make_record("4", "new_record"),  # added
        ]
        existing = {"1": "same", "2": "old", "3": "will_delete"}
        result = engine.diff_records("test", new, existing)

        assert result.unchanged_count == 1
        assert result.changed_count == 1
        assert result.added_count == 1
        assert result.deleted_count == 1
        assert result.has_changes is True

    def test_empty_both(self, engine):
        """Empty inputs should produce no changes."""
        result = engine.diff_records("test", [], {})
        assert result.has_changes is False

    def test_summary_format(self, engine):
        new = [_make_record("1"), _make_record("2")]
        result = engine.diff_records("test", new, {})
        summary = result.summary()
        assert "Added: 2" in summary
