"""
PERA AI — Outbound API Auth Resolver

Builds authenticated HTTP headers for remote API sources based on
YAML config auth sections. Secrets are resolved from environment
variables only — never stored in config files.

Phase 2 module.
"""
from __future__ import annotations

import base64
import os
import re
from typing import Dict, Optional

from api_config_models import ApiAuthConfig
from log_config import get_logger

log = get_logger("pera.api.auth")


class ApiAuthError(Exception):
    """Raised when auth resolution fails."""
    pass


class ApiAuthResolver:
    """Resolves outbound auth headers for API ingestion requests."""

    def build_auth_headers(self, auth_config: ApiAuthConfig) -> Dict[str, str]:
        """
        Build auth headers from an ApiAuthConfig.
        Returns dict of headers to merge into the request.
        """
        if auth_config.type == "none":
            return {}
        if auth_config.type == "bearer_env":
            return self._resolve_bearer(auth_config)
        if auth_config.type == "api_key_env":
            return self._resolve_api_key(auth_config)
        if auth_config.type == "basic_env":
            return self._resolve_basic(auth_config)
        raise ApiAuthError(f"Unsupported auth type: {auth_config.type}")

    def resolve_secret(self, env_name: str) -> str:
        """Resolve a secret from an environment variable. Raises if missing."""
        if not env_name:
            raise ApiAuthError("Empty environment variable name for secret")
        value = os.environ.get(env_name)
        if not value:
            raise ApiAuthError(
                f"Required env var '{env_name}' is not set or empty"
            )
        return value

    def sanitize_headers_for_logging(self, headers: Dict[str, str]) -> Dict[str, str]:
        """
        Return a copy of headers with secret values masked.
        Safe for logging and audit trails.
        """
        sensitive_patterns = re.compile(
            r"(auth|token|key|secret|password|bearer|credential)",
            re.IGNORECASE,
        )
        sanitized = {}
        for key, value in headers.items():
            if sensitive_patterns.search(key):
                sanitized[key] = value[:4] + "****" if len(value) > 4 else "****"
            else:
                sanitized[key] = value
        return sanitized

    # ── Private resolvers ─────────────────────────────────────

    def _resolve_bearer(self, auth: ApiAuthConfig) -> Dict[str, str]:
        token = self.resolve_secret(auth.token_env)
        return {"Authorization": f"Bearer {token}"}

    def _resolve_api_key(self, auth: ApiAuthConfig) -> Dict[str, str]:
        key = self.resolve_secret(auth.key_env)
        header_name = auth.key_header or "X-API-Key"
        return {header_name: key}

    def _resolve_basic(self, auth: ApiAuthConfig) -> Dict[str, str]:
        username = self.resolve_secret(auth.username_env)
        password = self.resolve_secret(auth.password_env)
        credentials = base64.b64encode(
            f"{username}:{password}".encode("utf-8")
        ).decode("ascii")
        return {"Authorization": f"Basic {credentials}"}
