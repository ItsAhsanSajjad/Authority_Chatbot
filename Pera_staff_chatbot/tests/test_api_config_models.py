"""Tests for API config models — parsing, validation, and defaults."""
import os
import pytest
import tempfile


VALID_BASIC_YAML = """
source_id: test_basic
source_type: api
display_name: "Test Basic API"
enabled: true

fetch:
  method: GET
  url: https://example.com/api/data
  timeout_seconds: 15

auth:
  type: bearer_env
  token_env: TEST_TOKEN

sync:
  interval_minutes: 60

normalization:
  root_selector: data
  record_id_field: id
  record_type: test_record
  text_template: "ID: {id}, Name: {name}"

indexing:
  authority: 3
  tags:
    - test
"""

VALID_PAGINATED_YAML = """
source_id: test_paginated
source_type: api
display_name: "Test Paginated API"
enabled: true

fetch:
  method: GET
  url: https://example.com/api/records
  pagination:
    type: offset
    page_size: 50
    page_param: offset
    size_param: limit
"""

INVALID_NO_SOURCE_ID = """
source_type: api
fetch:
  url: https://example.com/api
"""

INVALID_BAD_SOURCE_ID = """
source_id: "Has Spaces!"
source_type: api
fetch:
  url: https://example.com/api
"""

INVALID_NO_FETCH_URL = """
source_id: no_url
source_type: api
fetch:
  method: GET
"""

INVALID_BAD_SOURCE_TYPE = """
source_id: wrong_type
source_type: database
fetch:
  url: https://example.com
"""

INVALID_BAD_PAGINATION_TYPE = """
source_id: bad_pagination
source_type: api
fetch:
  url: https://example.com
  pagination:
    type: magic
"""


def test_parse_valid_basic_config():
    """Should parse a valid basic config without errors."""
    from api_config_models import parse_api_source_config

    config = parse_api_source_config(VALID_BASIC_YAML)
    assert config.source_id == "test_basic"
    assert config.display_name == "Test Basic API"
    assert config.enabled is True
    assert config.fetch.url == "https://example.com/api/data"
    assert config.fetch.method == "GET"
    assert config.fetch.timeout_seconds == 15
    assert config.auth.type == "bearer_env"
    assert config.auth.token_env == "TEST_TOKEN"
    assert config.sync.interval_minutes == 60
    assert config.normalization.root_selector == "data"
    assert config.normalization.record_id_field == "id"
    assert config.indexing.authority == 3
    assert "test" in config.indexing.tags


def test_parse_valid_paginated_config():
    """Should parse a paginated config with offset pagination."""
    from api_config_models import parse_api_source_config

    config = parse_api_source_config(VALID_PAGINATED_YAML)
    assert config.source_id == "test_paginated"
    assert config.fetch.pagination.type == "offset"
    assert config.fetch.pagination.page_size == 50


def test_parse_defaults():
    """Missing optional sections should use defaults."""
    from api_config_models import parse_api_source_config

    yaml_text = """
source_id: minimal
source_type: api
fetch:
  url: https://example.com/api
"""
    config = parse_api_source_config(yaml_text)
    assert config.enabled is True
    assert config.auth.type == "none"
    assert config.sync.interval_minutes == 30
    assert config.sync.full_refresh_every_hours == 24
    assert config.normalization.nested_strategy == "flatten"
    assert config.indexing.authority == 2
    assert config.fetch.pagination.type == "none"


def test_reject_no_source_id():
    """Should reject config without source_id."""
    from api_config_models import parse_api_source_config, ApiConfigError
    with pytest.raises(ApiConfigError, match="source_id"):
        parse_api_source_config(INVALID_NO_SOURCE_ID)


def test_reject_bad_source_id():
    """Should reject source_id with invalid characters."""
    from api_config_models import parse_api_source_config, ApiConfigError
    with pytest.raises(ApiConfigError, match="source_id"):
        parse_api_source_config(INVALID_BAD_SOURCE_ID)


def test_reject_no_fetch_url():
    """Should reject config without fetch.url."""
    from api_config_models import parse_api_source_config, ApiConfigError
    with pytest.raises(ApiConfigError, match="url"):
        parse_api_source_config(INVALID_NO_FETCH_URL)


def test_reject_bad_source_type():
    """Should reject non-api source_type."""
    from api_config_models import parse_api_source_config, ApiConfigError
    with pytest.raises(ApiConfigError, match="source_type"):
        parse_api_source_config(INVALID_BAD_SOURCE_TYPE)


def test_reject_bad_pagination_type():
    """Should reject invalid pagination type."""
    from api_config_models import parse_api_source_config, ApiConfigError
    with pytest.raises(ApiConfigError, match="pagination type"):
        parse_api_source_config(INVALID_BAD_PAGINATION_TYPE)


def test_load_from_file():
    """Should load config from a YAML file."""
    from api_config_models import load_api_source_config

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False, encoding="utf-8"
    ) as f:
        f.write(VALID_BASIC_YAML)
        f.flush()
        path = f.name

    try:
        config = load_api_source_config(path)
        assert config.source_id == "test_basic"
        assert config.config_path == path
    finally:
        os.unlink(path)


def test_load_missing_file():
    """Should raise for missing file."""
    from api_config_models import load_api_source_config, ApiConfigError
    with pytest.raises(ApiConfigError, match="not found"):
        load_api_source_config("/nonexistent/path.yaml")


def test_auth_resolve_token():
    """Auth should resolve token from env var."""
    from api_config_models import ApiAuthConfig
    os.environ["TEST_RESOLVE_TOKEN"] = "secret123"
    auth = ApiAuthConfig(type="bearer_env", token_env="TEST_RESOLVE_TOKEN")
    assert auth.resolve_token() == "secret123"
    del os.environ["TEST_RESOLVE_TOKEN"]


def test_auth_resolve_token_missing():
    """Auth should return None for missing env var."""
    from api_config_models import ApiAuthConfig
    auth = ApiAuthConfig(type="bearer_env", token_env="NONEXISTENT_VAR_12345")
    assert auth.resolve_token() is None


def test_disabled_config():
    """enabled=false should parse correctly."""
    from api_config_models import parse_api_source_config
    yaml_text = """
source_id: disabled_api
source_type: api
enabled: false
fetch:
  url: https://example.com/api
"""
    config = parse_api_source_config(yaml_text)
    assert config.enabled is False
