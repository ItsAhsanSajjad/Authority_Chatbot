"""
PERA AI — API Source Configuration Models

Typed models for parsing and validating API source YAML config files.
Each file in assets/apis/ describes one external API data source.

Usage:
    from api_config_models import load_api_source_config
    config = load_api_source_config("assets/apis/employees_api.yaml")
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml

from log_config import get_logger

log = get_logger("pera.api.config")


# ── Validation Errors ────────────────────────────────────────
class ApiConfigError(Exception):
    """Raised when an API source config is invalid."""
    pass


# ── Sub-Config Models ────────────────────────────────────────

@dataclass
class ApiPaginationConfig:
    """Pagination strategy for API fetch."""
    type: str = "none"  # none | offset | cursor | page
    page_size: int = 100
    page_param: str = "offset"
    size_param: str = "limit"
    total_field: str = ""
    cursor_field: str = ""
    cursor_param: str = ""
    max_pages: int = 1000

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "ApiPaginationConfig":
        if not d:
            return cls()
        valid_types = {"none", "offset", "cursor", "page"}
        ptype = str(d.get("type", "none")).lower()
        if ptype not in valid_types:
            raise ApiConfigError(
                f"Invalid pagination type '{ptype}'. Must be one of: {valid_types}"
            )
        return cls(
            type=ptype,
            page_size=int(d.get("page_size", 100)),
            page_param=str(d.get("page_param", "offset")),
            size_param=str(d.get("size_param", "limit")),
            total_field=str(d.get("total_field", "")),
            cursor_field=str(d.get("cursor_field", "")),
            cursor_param=str(d.get("cursor_param", "")),
            max_pages=int(d.get("max_pages", 1000)),
        )


@dataclass
class ApiFetchConfig:
    """How to fetch data from the API."""
    method: str = "GET"
    url: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = 30
    retry_count: int = 3
    retry_backoff_seconds: int = 2
    pagination: ApiPaginationConfig = field(default_factory=ApiPaginationConfig)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "ApiFetchConfig":
        if not d:
            raise ApiConfigError("'fetch' section is required")
        url = str(d.get("url", "")).strip()
        if not url:
            raise ApiConfigError("'fetch.url' is required and cannot be empty")
        method = str(d.get("method", "GET")).upper()
        if method not in ("GET", "POST"):
            raise ApiConfigError(f"'fetch.method' must be GET or POST, got '{method}'")
        return cls(
            method=method,
            url=url,
            headers=dict(d.get("headers") or {}),
            timeout_seconds=int(d.get("timeout_seconds", 30)),
            retry_count=int(d.get("retry_count", 3)),
            retry_backoff_seconds=int(d.get("retry_backoff_seconds", 2)),
            pagination=ApiPaginationConfig.from_dict(d.get("pagination")),
        )


@dataclass
class ApiAuthConfig:
    """Authentication for the API source."""
    type: str = "none"  # none | bearer_env | api_key_env | basic_env
    token_env: str = ""
    key_env: str = ""
    key_header: str = "X-API-Key"
    username_env: str = ""
    password_env: str = ""

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "ApiAuthConfig":
        if not d:
            return cls()
        auth_type = str(d.get("type", "none")).lower()
        valid_types = {"none", "bearer_env", "api_key_env", "basic_env"}
        if auth_type not in valid_types:
            raise ApiConfigError(
                f"Invalid auth type '{auth_type}'. Must be one of: {valid_types}"
            )
        return cls(
            type=auth_type,
            token_env=str(d.get("token_env", "")),
            key_env=str(d.get("key_env", "")),
            key_header=str(d.get("key_header", "X-API-Key")),
            username_env=str(d.get("username_env", "")),
            password_env=str(d.get("password_env", "")),
        )

    def resolve_token(self) -> Optional[str]:
        """Resolve the auth token from environment variables. Returns None if not set."""
        if self.type == "bearer_env" and self.token_env:
            return os.environ.get(self.token_env)
        if self.type == "api_key_env" and self.key_env:
            return os.environ.get(self.key_env)
        return None


@dataclass
class ApiSyncConfig:
    """Sync schedule and policy."""
    interval_minutes: int = 30
    full_refresh_every_hours: int = 24
    delete_missing_records: bool = True

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "ApiSyncConfig":
        if not d:
            return cls()
        return cls(
            interval_minutes=int(d.get("interval_minutes", 30)),
            full_refresh_every_hours=int(d.get("full_refresh_every_hours", 24)),
            delete_missing_records=bool(d.get("delete_missing_records", True)),
        )


@dataclass
class ApiNormalizationConfig:
    """How to extract and normalize records from the API response."""
    root_selector: str = ""  # JSON path to the records array (e.g. "data")
    record_id_field: str = ""
    record_type: str = ""
    include_fields: List[str] = field(default_factory=list)
    exclude_fields: List[str] = field(default_factory=list)
    nested_strategy: str = "flatten"  # flatten | preserve
    text_template: str = ""

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "ApiNormalizationConfig":
        if not d:
            return cls()
        return cls(
            root_selector=str(d.get("root_selector", "")),
            record_id_field=str(d.get("record_id_field", "")),
            record_type=str(d.get("record_type", "")),
            include_fields=list(d.get("include_fields") or []),
            exclude_fields=list(d.get("exclude_fields") or []),
            nested_strategy=str(d.get("nested_strategy", "flatten")),
            text_template=str(d.get("text_template", "")),
        )


@dataclass
class ApiIndexingConfig:
    """How to index records into the vector store."""
    authority: int = 2
    tags: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> "ApiIndexingConfig":
        if not d:
            return cls()
        return cls(
            authority=int(d.get("authority", 2)),
            tags=list(d.get("tags") or []),
        )


# ── Main Config Model ────────────────────────────────────────

@dataclass
class ApiSourceConfig:
    """Complete configuration for one API data source."""
    source_id: str = ""
    source_type: str = "api"
    display_name: str = ""
    enabled: bool = True
    config_path: str = ""  # filesystem path to the YAML file

    fetch: ApiFetchConfig = field(default_factory=ApiFetchConfig)
    auth: ApiAuthConfig = field(default_factory=ApiAuthConfig)
    sync: ApiSyncConfig = field(default_factory=ApiSyncConfig)
    normalization: ApiNormalizationConfig = field(default_factory=ApiNormalizationConfig)
    indexing: ApiIndexingConfig = field(default_factory=ApiIndexingConfig)

    @classmethod
    def from_dict(cls, d: Dict[str, Any], config_path: str = "") -> "ApiSourceConfig":
        """Parse a config dict (from YAML) into a validated ApiSourceConfig."""
        source_id = str(d.get("source_id", "")).strip()
        if not source_id:
            raise ApiConfigError("'source_id' is required and cannot be empty")
        if not re.match(r"^[a-z0-9_]+$", source_id):
            raise ApiConfigError(
                f"'source_id' must be lowercase alphanumeric with underscores, got '{source_id}'"
            )

        source_type = str(d.get("source_type", "api")).lower()
        if source_type != "api":
            raise ApiConfigError(
                f"'source_type' must be 'api', got '{source_type}'"
            )

        display_name = str(d.get("display_name", source_id))
        enabled = bool(d.get("enabled", True))

        return cls(
            source_id=source_id,
            source_type=source_type,
            display_name=display_name,
            enabled=enabled,
            config_path=config_path,
            fetch=ApiFetchConfig.from_dict(d.get("fetch")),
            auth=ApiAuthConfig.from_dict(d.get("auth")),
            sync=ApiSyncConfig.from_dict(d.get("sync")),
            normalization=ApiNormalizationConfig.from_dict(d.get("normalization")),
            indexing=ApiIndexingConfig.from_dict(d.get("indexing")),
        )


# ── Loader ────────────────────────────────────────────────────

def load_api_source_config(path: str) -> ApiSourceConfig:
    """Load and validate an API source config from a YAML file."""
    if not os.path.isfile(path):
        raise ApiConfigError(f"Config file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ApiConfigError(f"Invalid YAML in {path}: {e}") from e

    if not isinstance(raw, dict):
        raise ApiConfigError(f"Config file must be a YAML mapping, got {type(raw).__name__}")

    return ApiSourceConfig.from_dict(raw, config_path=path)


def parse_api_source_config(yaml_text: str, config_path: str = "<string>") -> ApiSourceConfig:
    """Parse an API source config from a YAML string."""
    try:
        raw = yaml.safe_load(yaml_text)
    except yaml.YAMLError as e:
        raise ApiConfigError(f"Invalid YAML: {e}") from e

    if not isinstance(raw, dict):
        raise ApiConfigError(f"Config must be a YAML mapping, got {type(raw).__name__}")

    return ApiSourceConfig.from_dict(raw, config_path=config_path)
