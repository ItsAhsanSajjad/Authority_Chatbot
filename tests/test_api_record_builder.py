"""Tests for API record builder — extraction, canonical text, hashing."""
import pytest

from api_config_models import ApiSourceConfig, ApiNormalizationConfig
from api_record_builder import ApiRecordBuilder


def _make_config(
    root_selector="data",
    record_id_field="id",
    record_type="employee",
    include_fields=None,
    exclude_fields=None,
    text_template="",
    nested_strategy="flatten",
):
    return ApiSourceConfig(
        source_id="test_source",
        normalization=ApiNormalizationConfig(
            root_selector=root_selector,
            record_id_field=record_id_field,
            record_type=record_type,
            include_fields=include_fields or [],
            exclude_fields=exclude_fields or [],
            text_template=text_template,
            nested_strategy=nested_strategy,
        ),
    )


@pytest.fixture
def builder():
    return ApiRecordBuilder()


class TestExtractRecords:
    def test_extract_from_nested(self, builder):
        config = _make_config(root_selector="data")
        payload = {"data": [{"id": 1}, {"id": 2}]}
        records = builder.extract_records(config, payload)
        assert len(records) == 2

    def test_extract_deep_selector(self, builder):
        config = _make_config(root_selector="response.results")
        payload = {"response": {"results": [{"id": 1}]}}
        records = builder.extract_records(config, payload)
        assert len(records) == 1

    def test_extract_no_selector(self, builder):
        config = _make_config(root_selector="")
        payload = [{"id": 1}, {"id": 2}]
        records = builder.extract_records(config, payload)
        assert len(records) == 2

    def test_extract_missing_path(self, builder):
        config = _make_config(root_selector="missing")
        payload = {"data": []}
        records = builder.extract_records(config, payload)
        assert records == []


class TestBuildRecord:
    def test_basic_record(self, builder):
        config = _make_config(text_template="Name: {name}, ID: {id}")
        raw = {"id": "emp001", "name": "Alice", "dept": "HR"}
        record = builder.build_record(config, raw)

        assert record.source_id == "test_source"
        assert record.record_id == "emp001"
        assert record.record_type == "employee"
        assert "Alice" in record.canonical_text
        assert record.record_hash != ""
        assert record.is_active is True

    def test_include_fields(self, builder):
        config = _make_config(include_fields=["id", "name"])
        raw = {"id": "1", "name": "Bob", "secret": "hidden"}
        record = builder.build_record(config, raw)

        assert "secret" not in record.canonical_json
        assert "Bob" in record.canonical_json

    def test_exclude_fields(self, builder):
        config = _make_config(exclude_fields=["internal_notes"])
        raw = {"id": "1", "name": "Carol", "internal_notes": "remove me"}
        record = builder.build_record(config, raw)

        assert "internal_notes" not in record.canonical_json

    def test_template_rendering(self, builder):
        config = _make_config(
            text_template="Employee: {name} (ID: {id})"
        )
        raw = {"id": "42", "name": "Dave"}
        record = builder.build_record(config, raw)
        assert record.canonical_text == "Employee: Dave (ID: 42)"

    def test_fallback_text_no_template(self, builder):
        config = _make_config(text_template="")
        raw = {"id": "1", "name": "Eve"}
        record = builder.build_record(config, raw)
        assert "Eve" in record.canonical_text
        assert "Employee" in record.canonical_text  # record_type


class TestCanonicalDeterminism:
    def test_json_deterministic(self, builder):
        """Same input should produce identical canonical JSON."""
        config = _make_config()
        raw = {"id": "1", "name": "Alice", "dept": "HR"}

        r1 = builder.build_record(config, raw)
        r2 = builder.build_record(config, raw)
        assert r1.canonical_json == r2.canonical_json

    def test_hash_stability(self, builder):
        """Same record should always produce same hash."""
        config = _make_config()
        raw = {"id": "1", "name": "Alice"}

        r1 = builder.build_record(config, raw)
        r2 = builder.build_record(config, raw)
        assert r1.record_hash == r2.record_hash

    def test_different_records_different_hashes(self, builder):
        """Different records should produce different hashes."""
        config = _make_config()

        r1 = builder.build_record(config, {"id": "1", "name": "Alice"})
        r2 = builder.build_record(config, {"id": "2", "name": "Bob"})
        assert r1.record_hash != r2.record_hash


class TestRecordId:
    def test_id_from_field(self, builder):
        config = _make_config(record_id_field="employee_id")
        raw = {"employee_id": "EMP-001", "name": "Test"}
        record = builder.build_record(config, raw)
        assert record.record_id == "EMP-001"

    def test_id_fallback_hash(self, builder):
        """Missing ID field should generate a hash-based ID."""
        config = _make_config(record_id_field="nonexistent")
        raw = {"name": "Test"}
        record = builder.build_record(config, raw)
        assert len(record.record_id) == 16  # hash prefix


class TestDisplayTitle:
    def test_title_from_name(self, builder):
        config = _make_config()
        raw = {"id": "1", "name": "Alice"}
        record = builder.build_record(config, raw)
        assert record.display_title == "Alice"

    def test_title_fallback(self, builder):
        config = _make_config()
        raw = {"id": "1", "code": "X"}
        record = builder.build_record(config, raw)
        assert "Employee" in record.display_title
