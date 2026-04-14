"""Tests for API fetcher — single request, retries, non-JSON rejection."""
import json
import pytest
from unittest.mock import patch, MagicMock

from api_config_models import (
    ApiSourceConfig,
    ApiFetchConfig,
    ApiAuthConfig,
    ApiPaginationConfig,
    ApiNormalizationConfig,
)
from api_fetcher import ApiFetcher, ApiFetchResult


def _make_config(
    url="https://example.com/api/data",
    method="GET",
    timeout=5,
    retry_count=1,
    retry_backoff=0,
    pagination_type="none",
    root_selector="data",
):
    return ApiSourceConfig(
        source_id="test_source",
        fetch=ApiFetchConfig(
            method=method,
            url=url,
            timeout_seconds=timeout,
            retry_count=retry_count,
            retry_backoff_seconds=retry_backoff,
            pagination=ApiPaginationConfig(type=pagination_type, page_size=2),
        ),
        auth=ApiAuthConfig(type="none"),
        normalization=ApiNormalizationConfig(root_selector=root_selector),
    )


def _mock_response(status=200, json_data=None, content_type="application/json", content=None):
    resp = MagicMock()
    resp.status_code = status
    resp.headers = {"content-type": content_type}
    if content is not None:
        resp.content = content
    elif json_data is not None:
        resp.content = json.dumps(json_data).encode()
    else:
        resp.content = b"{}"
    resp.json.return_value = json_data or {}
    return resp


class TestApiFetcherSingleRequest:
    """Tests for single (non-paginated) fetch."""

    @patch("api_fetcher.httpx.Client")
    def test_successful_json_fetch(self, mock_client_cls):
        """Should successfully fetch and parse JSON."""
        payload = {"data": [{"id": 1}]}
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            json_data=payload, content=json.dumps(payload).encode()
        )
        mock_client_cls.return_value = mock_client

        fetcher = ApiFetcher()
        config = _make_config()
        result = fetcher.fetch_source(config)

        assert result.success is True
        assert result.http_status == 200
        assert result.payload == payload
        assert result.snapshot_hash != ""

    @patch("api_fetcher.httpx.Client")
    def test_non_json_rejection(self, mock_client_cls):
        """Should reject non-JSON content types."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            content_type="text/html", content=b"<html>Not JSON</html>"
        )
        mock_client_cls.return_value = mock_client

        fetcher = ApiFetcher()
        config = _make_config()
        result = fetcher.fetch_source(config)

        assert result.success is False
        assert "Non-JSON" in result.error_message

    @patch("api_fetcher.httpx.Client")
    def test_http_error_status(self, mock_client_cls):
        """Should report HTTP error statuses."""
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = _mock_response(
            status=500, content=b'{"error":"server error"}'
        )
        mock_client_cls.return_value = mock_client

        fetcher = ApiFetcher()
        config = _make_config(retry_count=0)
        result = fetcher.fetch_source(config)

        assert result.success is False
        assert result.http_status == 500

    @patch("api_fetcher.httpx.Client")
    def test_timeout_with_retry(self, mock_client_cls):
        """Should retry on timeout and report failure."""
        import httpx
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = httpx.TimeoutException("timed out")
        mock_client_cls.return_value = mock_client

        fetcher = ApiFetcher()
        config = _make_config(retry_count=1, retry_backoff=0)
        result = fetcher.fetch_source(config)

        assert result.success is False
        assert "timed out" in result.error_message.lower()
        assert mock_client.get.call_count == 2  # original + 1 retry

    def test_http_url_rejected(self):
        """HTTP URLs should be rejected when API_ALLOW_HTTP is False."""
        fetcher = ApiFetcher()
        config = _make_config(url="http://insecure.example.com/api")
        result = fetcher.fetch_source(config)
        assert result.success is False
        assert "HTTP" in result.error_message

    def test_auth_error_reported(self):
        """Auth errors should be reported without crashing."""
        fetcher = ApiFetcher()
        config = _make_config()
        config.auth = ApiAuthConfig(type="bearer_env", token_env="MISSING_TOKEN_XYZ")

        result = fetcher.fetch_source(config)
        assert result.success is False
        assert "Auth error" in result.error_message


class TestApiFetcherSnapshot:
    """Tests for snapshot hashing."""

    def test_snapshot_hash_deterministic(self):
        """Same content should produce same hash."""
        fetcher = ApiFetcher()
        content = b'{"key": "value"}'
        hash1 = fetcher._compute_snapshot_hash(content)
        hash2 = fetcher._compute_snapshot_hash(content)
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256

    def test_snapshot_hash_differs(self):
        """Different content should produce different hashes."""
        fetcher = ApiFetcher()
        hash1 = fetcher._compute_snapshot_hash(b'{"a": 1}')
        hash2 = fetcher._compute_snapshot_hash(b'{"a": 2}')
        assert hash1 != hash2
