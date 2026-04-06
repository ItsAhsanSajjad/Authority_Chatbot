"""
PERA AI — Centralized OpenAI Client & Config

Single source of truth for API keys, base URLs, model names,
and lazy-init clients used across answerer, retriever, index_store, and speech.

Now reads all config from settings.py (Pydantic BaseSettings).
"""
from __future__ import annotations

from typing import Optional

from openai import OpenAI

from settings import get_settings

# ─── Convenience re-exports (preserves existing import patterns) ─────────────
def _s():
    return get_settings()

# These are module-level attributes that other modules import directly.
# We make them properties via a lazy pattern.
def _get_openai_api_key() -> str:
    return _s().OPENAI_API_KEY.get_secret_value()

OPENAI_BASE_URL: str = ""  # set lazily
ANSWER_MODEL: str = ""
EMBEDDING_MODEL: str = ""
LLM_REWRITE_MODEL: str = ""
TRANSCRIBE_MODEL: str = ""


def _init_module_vars() -> None:
    """Initialize module-level variables from settings (called lazily)."""
    global OPENAI_BASE_URL, ANSWER_MODEL, EMBEDDING_MODEL, LLM_REWRITE_MODEL, TRANSCRIBE_MODEL
    s = _s()
    OPENAI_BASE_URL = s.OPENAI_BASE_URL
    ANSWER_MODEL = s.ANSWER_MODEL
    EMBEDDING_MODEL = s.EMBEDDING_MODEL
    LLM_REWRITE_MODEL = s.LLM_REWRITE_MODEL
    TRANSCRIBE_MODEL = s.TRANSCRIBE_MODEL


_init_module_vars()


# ─── Validation ──────────────────────────────────────────────────────────────

def require_api_key() -> str:
    """Raise RuntimeError if OPENAI_API_KEY is missing."""
    key = _get_openai_api_key()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is missing. Ensure .env is present and loaded.")
    return key


def has_api_key() -> bool:
    """Non-throwing check for readiness probes."""
    return bool(_get_openai_api_key())


# ─── Lazy-init Clients ──────────────────────────────────────────────────────
_chat_client: Optional[OpenAI] = None
_transcription_client: Optional[OpenAI] = None


def get_chat_client() -> OpenAI:
    """
    Shared client for chat completions and embeddings.
    Uses OPENAI_BASE_URL which may point to a gateway.
    """
    global _chat_client
    if _chat_client is None:
        s = _s()
        _chat_client = OpenAI(
            api_key=require_api_key(),
            base_url=s.OPENAI_BASE_URL or "https://api.openai.com/v1",
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
