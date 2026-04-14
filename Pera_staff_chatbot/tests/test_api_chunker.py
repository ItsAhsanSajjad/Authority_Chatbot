"""Tests for API chunker — splitting, metadata, determinism."""
import pytest

from api_chunker import ApiChunker, ApiChunk
from api_config_models import ApiSourceConfig, ApiNormalizationConfig, ApiIndexingConfig
from api_record_builder import NormalizedApiRecord


def _make_config(authority=3, tags=None, display_name="Test API"):
    return ApiSourceConfig(
        source_id="test_source",
        display_name=display_name,
        normalization=ApiNormalizationConfig(
            record_type="employee",
            record_id_field="id",
        ),
        indexing=ApiIndexingConfig(
            authority=authority,
            tags=tags or ["hr", "pera"],
        ),
    )


def _make_record(record_id="r1", text="Test canonical text", source_id="test_source"):
    return NormalizedApiRecord(
        source_id=source_id,
        record_id=record_id,
        record_type="employee",
        canonical_text=text,
        record_hash="hash123",
        display_title=f"Employee {record_id}",
        field_list=["id", "name"],
        last_updated_at=1234567890.0,
    )


@pytest.fixture
def chunker():
    return ApiChunker()


class TestChunkRecord:
    def test_short_text_single_chunk(self, chunker):
        config = _make_config()
        record = _make_record(text="Short text")
        chunks = chunker.chunk_record(record, config)
        assert len(chunks) == 1
        assert chunks[0].chunk_text == "Short text"
        assert chunks[0].source_type == "api"
        assert chunks[0].source_id == "test_source"
        assert chunks[0].record_id == "r1"

    def test_empty_text_no_chunks(self, chunker):
        config = _make_config()
        record = _make_record(text="")
        chunks = chunker.chunk_record(record, config)
        assert len(chunks) == 0

    def test_long_text_multiple_chunks(self, chunker):
        config = _make_config()
        # Create text longer than default max chars
        text = "A" * 10000
        record = _make_record(text=text)
        chunks = chunker.chunk_record(record, config)
        assert len(chunks) > 1
        # All chunks should have correct metadata
        for i, c in enumerate(chunks):
            assert c.chunk_index == i
            assert c.source_id == "test_source"
            assert c.record_id == "r1"

    def test_chunk_overlap(self, chunker):
        config = _make_config()
        text = "B" * 10000
        record = _make_record(text=text)
        chunks = chunker.chunk_record(record, config)
        # With overlap, total text coverage > original length
        total_len = sum(len(c.chunk_text) for c in chunks)
        assert total_len > len(text)


class TestChunkMetadata:
    def test_metadata_fields(self, chunker):
        config = _make_config(authority=3, tags=["hr", "pera"])
        record = _make_record()
        chunks = chunker.chunk_record(record, config)
        c = chunks[0]
        assert c.authority == 3
        assert c.tags == ["hr", "pera"]
        assert c.record_type == "employee"
        assert c.display_title == "Employee r1"
        assert c.field_list == ["id", "name"]
        assert c.display_name == "Test API"
        assert c.synced_at == 1234567890.0

    def test_chunk_id_deterministic(self, chunker):
        config = _make_config()
        record = _make_record()
        chunks1 = chunker.chunk_record(record, config)
        chunks2 = chunker.chunk_record(record, config)
        assert chunks1[0].chunk_id == chunks2[0].chunk_id

    def test_chunk_hash_deterministic(self, chunker):
        config = _make_config()
        record = _make_record(text="Deterministic text")
        c1 = chunker.chunk_record(record, config)
        c2 = chunker.chunk_record(record, config)
        assert c1[0].chunk_hash == c2[0].chunk_hash

    def test_different_records_different_ids(self, chunker):
        config = _make_config()
        r1 = _make_record(record_id="r1")
        r2 = _make_record(record_id="r2")
        c1 = chunker.chunk_record(r1, config)
        c2 = chunker.chunk_record(r2, config)
        assert c1[0].chunk_id != c2[0].chunk_id


class TestChunkRecords:
    def test_multiple_records(self, chunker):
        config = _make_config()
        records = [
            _make_record(record_id="r1", text="Text 1"),
            _make_record(record_id="r2", text="Text 2"),
        ]
        chunks = chunker.chunk_records(records, config)
        assert len(chunks) == 2
        assert chunks[0].record_id == "r1"
        assert chunks[1].record_id == "r2"

    def test_empty_records(self, chunker):
        config = _make_config()
        chunks = chunker.chunk_records([], config)
        assert len(chunks) == 0
