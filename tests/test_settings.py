"""Tests for centralized settings module."""
import os
import pytest


def test_settings_loads_defaults(test_settings):
    """Settings should load with sensible defaults."""
    s = test_settings
    assert s.ANSWER_MODEL == "gpt-4o-mini"
    assert s.EMBEDDING_MODEL == "text-embedding-3-small"
    assert s.CHUNK_MAX_CHARS == 4500
    assert s.SESSION_TTL_SECONDS == 3600


def test_settings_reads_env_vars():
    """Settings should read from environment variables."""
    os.environ["BASE_URL"] = "https://custom.example.com"
    os.environ["RATE_LIMIT_ASK"] = "100/minute"

    from settings import Settings
    s = Settings()
    assert s.BASE_URL == "https://custom.example.com"
    assert s.RATE_LIMIT_ASK == "100/minute"


def test_settings_api_key_list():
    """API key parsing from comma-separated string."""
    os.environ["API_KEYS"] = "key1, key2, key3"
    from settings import Settings
    s = Settings()
    assert s.api_key_list == ["key1", "key2", "key3"]


def test_settings_api_key_list_empty():
    """Empty API_KEYS should return empty list."""
    os.environ["API_KEYS"] = ""
    from settings import Settings
    s = Settings()
    assert s.api_key_list == []


def test_settings_cors_origin_list():
    """CORS origins parsing from comma-separated string."""
    os.environ["CORS_ORIGINS"] = "http://localhost:3000, https://app.example.com"
    from settings import Settings
    s = Settings()
    assert len(s.cors_origin_list) == 2
    assert "http://localhost:3000" in s.cors_origin_list


def test_settings_validate_missing_openai_key():
    """Should raise if OPENAI_API_KEY is missing."""
    os.environ["OPENAI_API_KEY"] = ""
    from settings import Settings
    s = Settings()
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        s.validate_required_secrets()


def test_settings_validate_auth_enabled_no_keys():
    """Should raise if AUTH_ENABLED=1 but no API keys or JWT secret."""
    os.environ["OPENAI_API_KEY"] = "sk-test"
    os.environ["AUTH_ENABLED"] = "1"
    os.environ["API_KEYS"] = ""
    os.environ["JWT_SECRET"] = ""
    from settings import Settings
    s = Settings()
    with pytest.raises(RuntimeError, match="AUTH_ENABLED"):
        s.validate_required_secrets()


def test_settings_prompt_version():
    """Prompt version should be readable."""
    os.environ["PROMPT_VERSION"] = "2.0.0"
    from settings import Settings
    s = Settings()
    assert s.PROMPT_VERSION == "2.0.0"
