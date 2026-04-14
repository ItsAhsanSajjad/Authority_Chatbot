"""
PERA AI — Centralized Configuration (Pydantic BaseSettings)

Single source of truth for all environment-driven configuration.
Loads from environment variables first, then .env file as fallback.

Usage:
    from settings import get_settings
    s = get_settings()
    print(s.OPENAI_API_KEY.get_secret_value())
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import List, Optional

from pydantic_settings import BaseSettings
from pydantic import SecretStr, field_validator


class Settings(BaseSettings):
    """All PERA AI configuration in one place."""

    # ── OpenAI ────────────────────────────────────────────────
    OPENAI_API_KEY: SecretStr = SecretStr("")
    OPENAI_BASE_URL: str = "https://api.openai.com/v1"
    ANSWER_MODEL: str = "gpt-4o-mini"
    EMBEDDING_MODEL: str = "text-embedding-3-small"
    LLM_REWRITE_MODEL: str = "gpt-4o-mini"
    TRANSCRIBE_MODEL: str = "whisper-1"

    # ── Base URL (for PDF reference links) ────────────────────
    BASE_URL: str = "http://localhost:8000"

    # ── Auth ──────────────────────────────────────────────────
    AUTH_ENABLED: bool = False
    API_KEYS: str = ""  # comma-separated list of valid API keys
    JWT_SECRET: SecretStr = SecretStr("")
    JWT_ALGORITHM: str = "HS256"

    # ── CORS ──────────────────────────────────────────────────
    CORS_ORIGINS: str = "http://localhost:3000,http://127.0.0.1:3000"

    # ── Rate Limiting ─────────────────────────────────────────
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_ASK: str = "30/minute"        # /api/ask, /ask, /ask_json
    RATE_LIMIT_TRANSCRIBE: str = "5/minute"   # /transcribe
    RATE_LIMIT_DEFAULT: str = "60/minute"     # fallback

    # ── Session Backend ───────────────────────────────────────
    SESSION_BACKEND: str = "redis"  # "redis" | "sqlite" | "memory"
    REDIS_URL: str = "redis://localhost:6379/0"
    SQLITE_SESSION_PATH: str = "data/sessions.db"
    SESSION_TTL_SECONDS: int = 3600
    MAX_SESSIONS: int = 2000
    MAX_TURNS_PER_SESSION: int = 10

    # ── Index / Retrieval ─────────────────────────────────────
    INDEX_POINTER_PATH: str = "assets/indexes/ACTIVE.json"
    DATA_DIR: str = "assets/data"
    INDEXES_ROOT: str = "assets/indexes"
    INDEX_POLL_SECONDS: int = 30
    INDEX_KEEP_LAST_N: int = 3
    CHUNK_MAX_CHARS: int = 4500
    CHUNK_OVERLAP_CHARS: int = 500
    EMBED_TEXT_VERSION: int = 4
    SEARCH_TEXT_VERSION: int = 2
    EMBED_MODEL_VERSION: int = 1
    MAX_EMBED_CHARS_PER_TEXT: int = 7000
    MAX_EMBED_CHARS_PER_BATCH: int = 120000
    FORCE_REBUILD_IF_INDEX_MISSING: bool = True
    EMBED_RETRIES: int = 4
    EMBED_RETRY_BASE_SLEEP: float = 0.8
    PURGE_INACTIVE_FROM_FAISS: bool = True
    STORE_EMBED_TEXT_PREVIEW: bool = True
    EMBED_TEXT_PREVIEW_CHARS: int = 1200
    NORMALIZE_EMBEDDINGS: bool = True

    # ── Retriever ─────────────────────────────────────────────
    RETRIEVER_TOP_K: int = 30
    RETRIEVER_SIM_THRESHOLD: float = 0.18
    RETRIEVER_LLM_REWRITE_ENABLED: bool = True

    # ── Grounding ─────────────────────────────────────────────
    GROUNDING_SEMANTIC_ENABLED: bool = True
    GROUNDING_EVIDENCE_MAX_CHARS: int = 4000

    # ── Audit Trail ───────────────────────────────────────────
    AUDIT_ENABLED: bool = True
    AUDIT_DIR: str = "audit_logs"
    AUDIT_STORE_FULL_TEXT: bool = False
    AUDIT_MAX_ANSWER_CHARS: int = 500
    AUDIT_MAX_EVIDENCE_CHARS: int = 2000

    # ── Prompt Versioning ─────────────────────────────────────
    PROMPT_VERSION: str = "1.0.0"

    # ── Feature Flags ─────────────────────────────────────────
    SMALLTALK_BYPASS_ENABLED: bool = True
    ENTITY_ANCHORING_ENABLED: bool = True

    # ── API Ingestion Foundation ──────────────────────────────
    API_INGESTION_ENABLED: bool = False
    API_SOURCE_DIR: str = "assets/apis"
    API_SYNC_ENABLED: bool = False
    API_SYNC_POLL_SECONDS: int = 300
    API_DB_ENABLED: bool = False
    API_DB_URL: str = "sqlite:///data/api_ingestion.db"
    API_SNAPSHOT_DIR: str = "data/api_snapshots"
    API_SYNC_LOG_DIR: str = "data/api_sync"
    API_DEFAULT_TIMEOUT_SECONDS: int = 30
    API_DEFAULT_RETRY_COUNT: int = 3
    API_DEFAULT_RETRY_BACKOFF_SECONDS: int = 2
    API_MAX_RESPONSE_BYTES: int = 52428800  # 50 MB
    API_ALLOW_HTTP: bool = False
    API_ALLOW_NON_JSON: bool = False
    API_ENABLE_PENDING_REMOVAL_GRACE: bool = True
    API_REMOVAL_GRACE_MINUTES: int = 1440  # 24 hours
    API_BLOCK_EMPTY_SNAPSHOT_REPLACEMENT: bool = True
    API_MIN_EXPECTED_RECORDS_DEFAULT: int = 1
    API_INDEX_AUTHORITY_DEFAULT: int = 2
    API_CHUNK_MAX_CHARS: int = 4500
    API_CHUNK_OVERLAP_CHARS: int = 500
    API_AUDIT_ENABLED: bool = True

    # ── Analytics DB (PostgreSQL) ─────────────────────────────
    ANALYTICS_DB_ENABLED: bool = False
    POSTGRES_URL: str = "postgresql://localhost:5432/pera_ai"
    ANALYTICS_DB_POOL_SIZE: int = 5
    ANALYTICS_WRITE_ENABLED: bool = False
    ANALYTICS_RETENTION_DAYS: int = 730  # 2 years
    ANALYTICS_AUTO_MIGRATE: bool = True

    # ── Validators ────────────────────────────────────────────
    @field_validator("API_KEYS", mode="before")
    @classmethod
    def _strip_api_keys(cls, v: str) -> str:
        return (v or "").strip()

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def _strip_cors(cls, v: str) -> str:
        return (v or "").strip()

    # ── Derived helpers ───────────────────────────────────────
    @property
    def api_key_list(self) -> List[str]:
        """Parsed list of valid API keys."""
        if not self.API_KEYS:
            return []
        return [k.strip() for k in self.API_KEYS.split(",") if k.strip()]

    @property
    def cors_origin_list(self) -> List[str]:
        """Parsed list of CORS origins."""
        if not self.CORS_ORIGINS:
            return []
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    @property
    def has_openai_key(self) -> bool:
        return bool(self.OPENAI_API_KEY.get_secret_value())

    def validate_required_secrets(self) -> None:
        """Raise on startup if critical secrets are missing."""
        if not self.has_openai_key:
            raise RuntimeError(
                "OPENAI_API_KEY is required. Set it in environment or .env file."
            )
        if self.AUTH_ENABLED and not self.api_key_list and not self.JWT_SECRET.get_secret_value():
            raise RuntimeError(
                "AUTH_ENABLED=1 but no API_KEYS or JWT_SECRET configured. "
                "Set API_KEYS (comma-separated) and/or JWT_SECRET."
            )

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": True,
        "extra": "ignore",
    }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton settings accessor. Cached after first call."""
    return Settings()


def reset_settings() -> None:
    """Clear cached settings (for testing)."""
    get_settings.cache_clear()
