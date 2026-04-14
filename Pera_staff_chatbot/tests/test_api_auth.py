"""Tests for API auth resolver — each auth type, failure cases, header sanitization."""
import os
import base64
import pytest

from api_auth import ApiAuthResolver, ApiAuthError
from api_config_models import ApiAuthConfig


@pytest.fixture
def resolver():
    return ApiAuthResolver()


def test_auth_none(resolver):
    """Auth type 'none' should return empty headers."""
    auth = ApiAuthConfig(type="none")
    headers = resolver.build_auth_headers(auth)
    assert headers == {}


def test_bearer_env_success(resolver):
    """Bearer auth should build Authorization header from env var."""
    os.environ["TEST_BEARER_TOKEN"] = "my-secret-token-123"
    try:
        auth = ApiAuthConfig(type="bearer_env", token_env="TEST_BEARER_TOKEN")
        headers = resolver.build_auth_headers(auth)
        assert headers == {"Authorization": "Bearer my-secret-token-123"}
    finally:
        del os.environ["TEST_BEARER_TOKEN"]


def test_bearer_env_missing(resolver):
    """Bearer auth should fail if env var is missing."""
    auth = ApiAuthConfig(type="bearer_env", token_env="NONEXISTENT_TOKEN_XYZ")
    with pytest.raises(ApiAuthError, match="not set or empty"):
        resolver.build_auth_headers(auth)


def test_api_key_env_success(resolver):
    """API key auth should set the correct header."""
    os.environ["TEST_API_KEY"] = "key-abc-123"
    try:
        auth = ApiAuthConfig(
            type="api_key_env", key_env="TEST_API_KEY", key_header="X-Custom-Key"
        )
        headers = resolver.build_auth_headers(auth)
        assert headers == {"X-Custom-Key": "key-abc-123"}
    finally:
        del os.environ["TEST_API_KEY"]


def test_api_key_env_default_header(resolver):
    """API key should default to X-API-Key header."""
    os.environ["TEST_API_KEY_2"] = "key-456"
    try:
        auth = ApiAuthConfig(type="api_key_env", key_env="TEST_API_KEY_2")
        headers = resolver.build_auth_headers(auth)
        assert "X-API-Key" in headers
    finally:
        del os.environ["TEST_API_KEY_2"]


def test_api_key_env_missing(resolver):
    """API key auth should fail if env var is missing."""
    auth = ApiAuthConfig(type="api_key_env", key_env="NONEXISTENT_KEY_XYZ")
    with pytest.raises(ApiAuthError, match="not set or empty"):
        resolver.build_auth_headers(auth)


def test_basic_env_success(resolver):
    """Basic auth should build base64 Authorization header."""
    os.environ["TEST_USER"] = "admin"
    os.environ["TEST_PASS"] = "secret123"
    try:
        auth = ApiAuthConfig(
            type="basic_env", username_env="TEST_USER", password_env="TEST_PASS"
        )
        headers = resolver.build_auth_headers(auth)
        expected = base64.b64encode(b"admin:secret123").decode("ascii")
        assert headers == {"Authorization": f"Basic {expected}"}
    finally:
        del os.environ["TEST_USER"]
        del os.environ["TEST_PASS"]


def test_basic_env_missing_username(resolver):
    """Basic auth should fail if username env var is missing."""
    auth = ApiAuthConfig(
        type="basic_env", username_env="NONEXISTENT_USER", password_env="NONEXISTENT_PASS"
    )
    with pytest.raises(ApiAuthError, match="not set or empty"):
        resolver.build_auth_headers(auth)


def test_resolve_secret_empty_name(resolver):
    """resolve_secret should fail on empty env name."""
    with pytest.raises(ApiAuthError, match="Empty"):
        resolver.resolve_secret("")


def test_sanitize_headers(resolver):
    """Sensitive header values should be masked."""
    headers = {
        "Authorization": "Bearer supersecrettoken",
        "X-API-Key": "mykey12345",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    sanitized = resolver.sanitize_headers_for_logging(headers)
    assert sanitized["Accept"] == "application/json"
    assert "****" in sanitized["Authorization"]
    assert "supersecrettoken" not in sanitized["Authorization"]
    assert "****" in sanitized["X-API-Key"]
    assert "mykey12345" not in sanitized["X-API-Key"]


def test_unsupported_auth_type(resolver):
    """Unsupported auth type should raise."""
    auth = ApiAuthConfig(type="oauth2")
    with pytest.raises(ApiAuthError, match="Unsupported"):
        resolver.build_auth_headers(auth)
