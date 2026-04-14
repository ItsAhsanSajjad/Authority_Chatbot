"""
PERA AI Test Suite — Shared Fixtures

Provides environment overrides, test settings, and mock factories
used across all test modules.
"""
from __future__ import annotations

import os
import sys
import pytest

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(autouse=True)
def _reset_settings():
    """Reset cached settings before each test so env overrides take effect."""
    os.environ.setdefault("OPENAI_API_KEY", "sk-test-key-for-unit-tests")
    os.environ.setdefault("AUTH_ENABLED", "0")
    os.environ.setdefault("RATE_LIMIT_ENABLED", "0")
    os.environ.setdefault("SESSION_BACKEND", "memory")
    os.environ.setdefault("AUDIT_ENABLED", "0")
    os.environ.setdefault("BASE_URL", "http://localhost:8000")
    os.environ.setdefault("CORS_ORIGINS", "http://localhost:3000")

    from settings import reset_settings
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def test_settings():
    """Return a fresh Settings instance for inspection."""
    from settings import Settings
    return Settings()
