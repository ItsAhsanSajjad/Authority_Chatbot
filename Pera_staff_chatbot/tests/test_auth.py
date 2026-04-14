"""Tests for authentication module."""
import os
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app_with_auth():
    """Create a FastAPI test client with auth enabled."""
    os.environ["AUTH_ENABLED"] = "1"
    os.environ["API_KEYS"] = "test-key-1,test-key-2"
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["RATE_LIMIT_ENABLED"] = "0"
    os.environ["SESSION_BACKEND"] = "memory"

    from settings import reset_settings
    reset_settings()

    # Import fresh to pick up new settings
    import importlib
    import fastapi_app
    importlib.reload(fastapi_app)

    return TestClient(fastapi_app.app)


@pytest.fixture
def app_without_auth():
    """Create a FastAPI test client with auth disabled."""
    os.environ["AUTH_ENABLED"] = "0"
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["RATE_LIMIT_ENABLED"] = "0"
    os.environ["SESSION_BACKEND"] = "memory"

    from settings import reset_settings
    reset_settings()

    import importlib
    import fastapi_app
    importlib.reload(fastapi_app)

    return TestClient(fastapi_app.app)


def test_health_endpoint_is_public(app_with_auth):
    """Health endpoint should work without auth."""
    resp = app_with_auth.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("status") == "ok"


def test_ready_endpoint_is_public(app_with_auth):
    """Ready endpoint should work without auth."""
    resp = app_with_auth.get("/ready")
    # May return 200 or 503 depending on index state, but should not be 401
    assert resp.status_code != 401


def test_auth_disabled_allows_all(app_without_auth):
    """When AUTH_ENABLED=0, endpoints should not require auth."""
    resp = app_without_auth.get("/health")
    assert resp.status_code == 200
