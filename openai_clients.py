"""
PERA AI — Centralized OpenAI Client & Config

Single source of truth for API keys, base URLs, model names,
and lazy-init clients used across answerer, retriever, index_store, and speech.
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv(override=True)

# ─── Configuration (all from .env) ───────────────────────────────────────────

OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL: str = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip()

# Model names
ANSWER_MODEL: str = os.getenv("ANSWER_MODEL", "gpt-4o-mini").strip()
EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small").strip()
LLM_REWRITE_MODEL: str = os.getenv("RETRIEVER_LLM_QUERY_REWRITE_MODEL", "gpt-4o-mini").strip()
TRANSCRIBE_MODEL: str = os.getenv("TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe").strip()

# Phase 4: dedicated grounding judge model.
# Defaults to ANSWER_MODEL so existing deployments behave identically out
# of the box. Set GROUNDING_JUDGE_MODEL to a stronger model (for example
# "gpt-4o") to escape self-judging bias — the generator no longer judges
# its own output. Optionally set GROUNDING_JUDGE_BASE_URL and
# GROUNDING_JUDGE_API_KEY if the judge lives behind a different gateway
# or account; otherwise the same chat client is reused.
GROUNDING_JUDGE_MODEL: str = os.getenv(
    "GROUNDING_JUDGE_MODEL", ANSWER_MODEL
).strip()
GROUNDING_JUDGE_BASE_URL: str = os.getenv("GROUNDING_JUDGE_BASE_URL", "").strip()
GROUNDING_JUDGE_API_KEY: str = os.getenv("GROUNDING_JUDGE_API_KEY", "").strip()

# ─── Validation ──────────────────────────────────────────────────────────────

def require_api_key() -> str:
    """Raise RuntimeError if OPENAI_API_KEY is missing."""
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is missing. Ensure .env is present and loaded.")
    return OPENAI_API_KEY


def has_api_key() -> bool:
    """Non-throwing check for readiness probes."""
    return bool(OPENAI_API_KEY)


# ─── Lazy-init Clients ──────────────────────────────────────────────────────
# The "chat/rewrite" client uses OPENAI_BASE_URL (may point to a gateway).
# The "transcription" client always uses the default OpenAI API (Whisper).

_chat_client: Optional[OpenAI] = None
_transcription_client: Optional[OpenAI] = None


def get_chat_client() -> OpenAI:
    """
    Shared client for chat completions and embeddings.
    Uses OPENAI_BASE_URL which may point to a gateway.
    """
    global _chat_client
    if _chat_client is None:
        _chat_client = OpenAI(
            api_key=require_api_key(),
            base_url=OPENAI_BASE_URL or "https://api.openai.com/v1",
        )
    return _chat_client


def get_transcription_client() -> OpenAI:
    """
    Client for Whisper transcription.
    Always uses the standard OpenAI API (not the gateway base_url).
    """
    global _transcription_client
    if _transcription_client is None:
        _transcription_client = OpenAI(api_key=require_api_key())
    return _transcription_client


# Phase 4 — Grounding judge client.
# Returns a separate OpenAI client when the operator configured a
# distinct base URL OR API key for the judge model. Otherwise reuses
# the main chat client (zero behavioral change). Never raises: callers
# pass-through to the chat client on any failure path.

_judge_client: Optional[OpenAI] = None


# ─── OpenAI Availability Tracking ────────────────────────────────────────────
#
# When OpenAI returns a 429 "insufficient_quota" (billing exhausted) we flip
# a module-level flag that other parts of the system check. A background
# daemon thread probes OpenAI every 60 s while offline and flips the flag
# back to available as soon as a call succeeds.
#
# Intentionally narrow: only true billing-quota exhaustion marks the system
# offline. Transient rate-limits (429 rate_limit_exceeded) recover on their
# own via existing retry loops and do NOT flip the flag.

_openai_status: Dict[str, Any] = {
    "available": True,
    "reason": "",            # human-readable reason shown to the user
    "since": 0.0,            # unix ts when availability dropped
    "probe_running": False,
    "last_probe_at": 0.0,
}
_status_lock = threading.Lock()

# Marker substrings that identify billing-quota exhaustion, as distinct
# from transient rate-limit throttles. Matched case-insensitively.
_QUOTA_MARKERS = (
    "insufficient_quota",
    "exceeded your current quota",
    "billing details",
)


def _looks_like_quota_exhaustion(exc: BaseException) -> bool:
    """Return True if the exception indicates OpenAI billing-quota
    exhaustion (not a transient rate limit)."""
    try:
        msg = str(exc).lower()
    except Exception:
        return False
    if any(m in msg for m in _QUOTA_MARKERS):
        return True
    # Some SDK errors expose status_code / body — check combined signal
    status = getattr(exc, "status_code", None) or getattr(exc, "http_status", None)
    body = getattr(exc, "body", None) or getattr(exc, "response", None)
    if status == 429 and body is not None:
        try:
            body_str = str(body).lower()
        except Exception:
            body_str = ""
        if any(m in body_str for m in _QUOTA_MARKERS):
            return True
    return False


def mark_openai_unavailable(exc: BaseException) -> None:
    """DISABLED — system never flips into offline mode.

    Per product decision, the chatbot must always respond to the user
    regardless of OpenAI quota state. Genuine OpenAI errors propagate
    as normal exceptions and surface through the generic error path;
    no dedicated "offline" decision, no banner, no composer disable.
    """
    return


def mark_openai_available() -> None:
    """Called whenever an OpenAI call succeeds. Clears any prior offline
    state so the system flips back to active."""
    with _status_lock:
        if _openai_status["available"]:
            return  # already available; cheap no-op
        _openai_status["available"] = True
        _openai_status["reason"] = ""
        _openai_status["since"] = 0.0


def get_openai_status() -> Dict[str, Any]:
    """Return a shallow copy of the current availability status — safe
    to serialize into the /health response."""
    with _status_lock:
        return dict(_openai_status)


def _probe_openai_worker() -> None:
    """Background thread: while offline, issue a cheap models.retrieve()
    every 60 s. First success flips the flag back to available and the
    thread exits. On subsequent failures the thread keeps polling."""
    backoff = 60.0
    try:
        while True:
            with _status_lock:
                if _openai_status["available"]:
                    _openai_status["probe_running"] = False
                    return
                _openai_status["last_probe_at"] = time.time()
            time.sleep(backoff)
            try:
                client = get_chat_client()
                # models.retrieve is a metadata call — doesn't consume
                # any completion/embedding tokens, so probing is free.
                client.models.retrieve(ANSWER_MODEL)
                # Success → service is back.
                mark_openai_available()
                with _status_lock:
                    _openai_status["probe_running"] = False
                return
            except Exception:
                # Still unavailable; loop and try again after backoff.
                continue
    finally:
        with _status_lock:
            _openai_status["probe_running"] = False


def start_openai_probe_worker() -> None:
    """Spawn a single daemon probe thread if one isn't already running.
    Safe to call repeatedly — a `probe_running` guard prevents duplicates.
    Does nothing if the service is currently marked available (the probe
    worker would immediately exit anyway)."""
    with _status_lock:
        if _openai_status["available"]:
            return
        if _openai_status["probe_running"]:
            return
        _openai_status["probe_running"] = True
    threading.Thread(
        target=_probe_openai_worker,
        name="openai-probe",
        daemon=True,
    ).start()


def get_grounding_judge_client() -> OpenAI:
    """Return the client to use for grounding-judge calls.

    Resolution order:
      1. If GROUNDING_JUDGE_API_KEY or GROUNDING_JUDGE_BASE_URL is set,
         build (once) a dedicated client with those values.
      2. Otherwise return the shared chat client. This preserves the
         current behavior when no operator override is supplied.

    Safe fallback: if the dedicated client cannot be constructed
    (missing key, etc.), the function returns the shared chat client.
    """
    global _judge_client
    use_dedicated = bool(GROUNDING_JUDGE_API_KEY or GROUNDING_JUDGE_BASE_URL)
    if not use_dedicated:
        return get_chat_client()
    if _judge_client is None:
        try:
            api_key = GROUNDING_JUDGE_API_KEY or require_api_key()
            base_url = (
                GROUNDING_JUDGE_BASE_URL
                or OPENAI_BASE_URL
                or "https://api.openai.com/v1"
            )
            _judge_client = OpenAI(api_key=api_key, base_url=base_url)
        except Exception:
            return get_chat_client()
    return _judge_client
