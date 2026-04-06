"""Tests for API payload validators — root selectors, fields, record IDs."""
import pytest

from api_config_models import ApiSourceConfig, ApiNormalizationConfig
from api_validators import ApiPayloadValidator


def _make_config(
    root_selector="data",
    record_id_field="id",
    include_fields=None,
):
    return ApiSourceConfig(
        source_id="test",
        normalization=ApiNormalizationConfig(
            root_selector=root_selector,
            record_id_field=record_id_field,
            include_fields=include_fields or [],
        ),
    )


@pytest.fixture
def validator():
    return ApiPayloadValidator()


class TestValidateJsonPayload:
    def test_valid_payload(self, validator):
        config = _make_config()
        payload = {"data": [{"id": 1, "name": "Alice"}]}
        result = validator.validate_json_payload(config, payload)
        assert result.valid is True
        assert result.record_count == 1

    def test_none_payload(self, validator):
        config = _make_config()
        result = validator.validate_json_payload(config, None)
        assert result.valid is False
        assert any("None" in e for e in result.errors)

    def test_non_dict_payload(self, validator):
        config = _make_config()
        result = validator.validate_json_payload(config, "not json")
        assert result.valid is False

    def test_missing_root_selector(self, validator):
        config = _make_config(root_selector="results")
        payload = {"data": []}
        result = validator.validate_json_payload(config, payload)
        assert result.valid is False
        assert any("root_selector" in e for e in result.errors)


class TestValidateRootSelector:
    def test_simple_selector(self, validator):
        config = _make_config(root_selector="items")
        payload = {"items": [{"id": 1}]}
        result = validator.validate_root_selector(config, payload)
        assert result.valid is True

    def test_nested_selector(self, validator):
        config = _make_config(root_selector="response.data")
        payload = {"response": {"data": [{"id": 1}]}}
        result = validator.validate_root_selector(config, payload)
        assert result.valid is True

    def test_missing_nested_key(self, validator):
        config = _make_config(root_selector="response.items")
        payload = {"response": {"data": []}}
        result = validator.validate_root_selector(config, payload)
        assert result.valid is False


class TestValidateRequiredFields:
    def test_all_fields_present(self, validator):
        config = _make_config(include_fields=["id", "name"])
        records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        result = validator.validate_required_fields(config, records)
        assert len(result.warnings) == 0

    def test_missing_fields_warned(self, validator):
        config = _make_config(include_fields=["id", "name", "email"])
        records = [{"id": 1, "name": "Alice"}]
        result = validator.validate_required_fields(config, records)
        assert any("email" in w for w in result.warnings)

    def test_no_include_fields(self, validator):
        """No include_fields means no field validation."""
        config = _make_config(include_fields=[])
        records = [{"random": "stuff"}]
        result = validator.validate_required_fields(config, records)
        assert len(result.warnings) == 0


class TestValidateRecordIds:
    def test_unique_ids(self, validator):
        config = _make_config(record_id_field="id")
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        result = validator.validate_record_ids(config, records)
        assert result.valid is True
        assert len(result.errors) == 0

    def test_duplicate_ids_rejected(self, validator):
        config = _make_config(record_id_field="id")
        records = [{"id": 1}, {"id": 2}, {"id": 1}]
        result = validator.validate_record_ids(config, records)
        assert any("Duplicate" in e for e in result.errors)

    def test_missing_id_warned(self, validator):
        config = _make_config(record_id_field="id")
        records = [{"id": 1}, {"name": "no id"}]
        result = validator.validate_record_ids(config, records)
        assert any("missing" in w for w in result.warnings)

    def test_no_id_field_skips(self, validator):
        """No record_id_field means skip ID validation."""
        config = _make_config(record_id_field="")
        records = [{"a": 1}, {"a": 1}]
        result = validator.validate_record_ids(config, records)
        assert result.valid is True
