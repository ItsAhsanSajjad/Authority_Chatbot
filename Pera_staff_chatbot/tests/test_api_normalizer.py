"""Tests for API normalizer — orchestrated validation + record building."""
import pytest

from api_config_models import ApiSourceConfig, ApiNormalizationConfig
from api_fetcher import ApiFetchResult
from api_normalizer import ApiNormalizer


def _make_config(
    root_selector="data",
    record_id_field="id",
    record_type="test",
    text_template="",
):
    return ApiSourceConfig(
        source_id="test_source",
        normalization=ApiNormalizationConfig(
            root_selector=root_selector,
            record_id_field=record_id_field,
            record_type=record_type,
            text_template=text_template,
        ),
    )


def _make_fetch_result(payload=None, success=True, error=""):
    return ApiFetchResult(
        source_id="test_source",
        success=success,
        payload=payload,
        snapshot_hash="abc123",
        error_message=error,
    )


@pytest.fixture
def normalizer():
    return ApiNormalizer()


class TestNormalize:
    def test_successful_normalization(self, normalizer):
        config = _make_config()
        fetch = _make_fetch_result(payload={"data": [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]})
        result = normalizer.normalize_payload(config, fetch)

        assert result.success is True
        assert result.record_count == 2
        assert len(result.normalized_records) == 2
        assert result.normalized_records[0].record_id == "1"

    def test_failed_fetch(self, normalizer):
        config = _make_config()
        fetch = _make_fetch_result(success=False, error="timeout")
        result = normalizer.normalize_payload(config, fetch)

        assert result.success is False
        assert any("Fetch failed" in e for e in result.validation_errors)

    def test_validation_failure(self, normalizer):
        config = _make_config(root_selector="missing")
        fetch = _make_fetch_result(payload={"data": []})
        result = normalizer.normalize_payload(config, fetch)

        assert result.success is False
        assert len(result.validation_errors) > 0

    def test_empty_records_warns(self, normalizer):
        config = _make_config(root_selector="")
        fetch = _make_fetch_result(payload=[])
        result = normalizer.normalize_payload(config, fetch)

        # Empty payload is blocked by default
        assert result.success is False or len(result.validation_warnings) > 0

    def test_summary_contains_source(self, normalizer):
        config = _make_config()
        fetch = _make_fetch_result(payload={"data": [{"id": "1"}]})
        result = normalizer.normalize_payload(config, fetch)
        summary = result.summary()
        assert "test_source" in summary

    def test_non_dict_records_warned(self, normalizer):
        config = _make_config(root_selector="data")
        fetch = _make_fetch_result(payload={"data": [
            {"id": "1", "name": "OK"},
            "not a dict",
        ]})
        result = normalizer.normalize_payload(config, fetch)
        assert result.success is True
        assert result.record_count == 1  # only the dict
        assert any("non-dict" in w for w in result.validation_warnings)
