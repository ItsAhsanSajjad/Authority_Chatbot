"""
Admin authentication for the PERA AI admin dashboard.

Self-contained module — does not modify the existing auth.py used by the
public /api/ask flow. Provides:

- Hardcoded admin credentials (as specified by the project owner).
- HMAC-SHA256 signed session tokens (JWT-style, three base64url parts:
  header.payload.signature). Uses only the Python standard library so
  no additional dependency is required.
- ``issue_admin_token`` / ``verify_admin_token`` helpers.
- ``require_admin`` FastAPI dependency.
- A tiny per-IP rate limiter for the login endpoint.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

from fastapi import Header, HTTPException, Request

from log_config import get_logger

log = get_logger("pera.admin_auth")


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
# These are intentionally held as module-level constants so a single change
# here is the only place credentials exist. They are never echoed to logs.
ADMIN_EMAIL = "admin@pera.gop.pk"
ADMIN_PASSWORD = "Pera@112233"

# 12 hours of session validity after login.
TOKEN_TTL_SECONDS = 12 * 60 * 60
TOKEN_ISSUER = "pera-admin"


# ---------------------------------------------------------------------------
# Signing key: generated once on first boot, persisted so admin sessions
# survive server restarts. Lives alongside the app binary; keep it out of
# source control via .gitignore.
# ---------------------------------------------------------------------------
_SECRET_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".admin_secret"
)


def _load_or_create_secret() -> bytes:
    try:
        if os.path.exists(_SECRET_FILE):
            with open(_SECRET_FILE, "rb") as f:
                data = f.read().strip()
            if len(data) >= 32:
                return data
    except OSError as exc:  # pragma: no cover — filesystem issue
        log.warning("Could not read admin secret file: %s", exc)
    secret = secrets.token_bytes(32)
    try:
        with open(_SECRET_FILE, "wb") as f:
            f.write(secret)
        try:
            os.chmod(_SECRET_FILE, 0o600)
        except OSError:
            pass
    except OSError as exc:  # pragma: no cover
        log.warning("Could not persist admin secret: %s", exc)
    return secret


_SECRET = _load_or_create_secret()


# ---------------------------------------------------------------------------
# Minimal HS256 token (JWT-compatible three-part base64url payload).
# ---------------------------------------------------------------------------
def _b64u_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64u_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _sign(message: bytes) -> bytes:
    return hmac.new(_SECRET, message, hashlib.sha256).digest()


def issue_admin_token(email: str) -> Tuple[str, int]:
    """Return (token, exp_unix). 12h TTL."""
    now = int(time.time())
    exp = now + TOKEN_TTL_SECONDS
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": email,
        "role": "admin",
        "iat": now,
        "exp": exp,
        "iss": TOKEN_ISSUER,
    }
    header_b64 = _b64u_encode(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = _b64u_encode(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    sig_b64 = _b64u_encode(_sign(signing_input))
    return f"{header_b64}.{payload_b64}.{sig_b64}", exp


def verify_admin_token(token: str) -> Optional[Dict[str, Any]]:
    """Return the claims dict if the token is valid and unexpired, else None."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header_b64, payload_b64, sig_b64 = parts
        signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
        expected = _sign(signing_input)
        actual = _b64u_decode(sig_b64)
        if not hmac.compare_digest(expected, actual):
            return None
        payload_raw = _b64u_decode(payload_b64)
        claims = json.loads(payload_raw.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(claims, dict):
        return None
    if claims.get("iss") != TOKEN_ISSUER:
        return None
    if claims.get("role") != "admin":
        return None
    exp = claims.get("exp")
    if not isinstance(exp, int) or exp < int(time.time()):
        return None
    return claims


# ---------------------------------------------------------------------------
# Credential check (constant-time)
# ---------------------------------------------------------------------------
def verify_credentials(email: str, password: str) -> bool:
    email_ok = hmac.compare_digest((email or "").strip(), ADMIN_EMAIL)
    pw_ok = hmac.compare_digest(password or "", ADMIN_PASSWORD)
    return email_ok and pw_ok


# ---------------------------------------------------------------------------
# Per-IP login rate limiter — 5 attempts / 60s window.
# ---------------------------------------------------------------------------
_RATE_WINDOW_SECONDS = 60
_RATE_MAX_ATTEMPTS = 5
_rate_buckets: Dict[str, Deque[float]] = {}


def check_rate_limit(ip: str) -> bool:
    now = time.time()
    bucket = _rate_buckets.setdefault(ip, deque())
    while bucket and now - bucket[0] > _RATE_WINDOW_SECONDS:
        bucket.popleft()
    if len(bucket) >= _RATE_MAX_ATTEMPTS:
        return False
    bucket.append(now)
    return True


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------
def require_admin(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail={"error": "missing_token"})
    token = authorization.split(" ", 1)[1].strip()
    claims = verify_admin_token(token)
    if not claims:
        raise HTTPException(status_code=401, detail={"error": "invalid_token"})
    return claims
