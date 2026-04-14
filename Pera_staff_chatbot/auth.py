"""
PERA AI — Authentication & Authorization

FastAPI dependency for securing API endpoints.
Supports:
  - API key via X-API-Key header
  - JWT Bearer token (for future extensibility)
  - Disabled auth for local development (AUTH_ENABLED=0)

Usage:
    from auth import require_auth
    @app.post("/api/ask", dependencies=[Depends(require_auth)])
"""
from __future__ import annotations

from typing import Optional

from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

from settings import get_settings

import logging
log = logging.getLogger("pera.auth")

# ── Security schemes ─────────────────────────────────────────
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_bearer_scheme = HTTPBearer(auto_error=False)


def _verify_jwt(token: str) -> Optional[dict]:
    """Verify a JWT token. Returns payload dict or None."""
    s = get_settings()
    secret = s.JWT_SECRET.get_secret_value()
    if not secret:
        return None
    try:
        import jwt
        payload = jwt.decode(token, secret, algorithms=[s.JWT_ALGORITHM])
        return payload
    except Exception:
        return None


async def require_auth(
    request: Request,
    api_key: Optional[str] = Security(_api_key_header),
    bearer: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme),
) -> Optional[str]:
    """
    FastAPI dependency that enforces authentication.

    Returns the authenticated identity string (API key prefix or JWT sub)
    and stores it in request.state.auth_identity.

    If AUTH_ENABLED=0, allows all requests through.
    """
    s = get_settings()

    if not s.AUTH_ENABLED:
        request.state.auth_identity = "auth_disabled"
        return "auth_disabled"

    # Try API key first
    if api_key:
        valid_keys = s.api_key_list
        if api_key in valid_keys:
            identity = f"apikey:{api_key[:8]}..."
            request.state.auth_identity = identity
            return identity

    # Try JWT Bearer
    if bearer and bearer.credentials:
        payload = _verify_jwt(bearer.credentials)
        if payload:
            sub = payload.get("sub", "jwt_user")
            identity = f"jwt:{sub}"
            request.state.auth_identity = identity
            return identity

    # No valid auth
    log.warning("Unauthorized request to %s from %s", request.url.path, request.client.host if request.client else "unknown")
    raise HTTPException(
        status_code=401,
        detail="Authentication required. Provide X-API-Key header or Bearer token.",
    )


def get_auth_identity(request: Request) -> str:
    """Extract auth identity from request state (set by require_auth)."""
    return getattr(request.state, "auth_identity", "unknown")
