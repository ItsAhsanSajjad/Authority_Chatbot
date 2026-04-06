"""
PERA AI — Analytics PostgreSQL Database Layer

Lightweight PostgreSQL connection management using psycopg (v3).
Provides connection pooling, health checks, and graceful degradation.
If PostgreSQL is unavailable, all operations return None / fail silently
so the existing RAG pipeline is never affected.

Usage:
    from analytics_db import get_analytics_db
    db = get_analytics_db()
    if db and db.is_available():
        with db.connection() as conn:
            conn.execute("SELECT 1")
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Dict, List, Optional

from log_config import get_logger

log = get_logger("pera.analytics.db")

# ── Lazy imports to avoid hard dependency on psycopg ──────────
_psycopg = None
_pool_module = None


def _ensure_psycopg():
    """Lazy-import psycopg so the module can be imported even without it."""
    global _psycopg, _pool_module
    if _psycopg is None:
        try:
            import psycopg
            from psycopg import sql as _sql
            _psycopg = psycopg
        except ImportError:
            raise ImportError(
                "psycopg is required for Analytics DB. "
                "Install with: pip install psycopg[binary]"
            )
    return _psycopg


class AnalyticsDB:
    """
    PostgreSQL connection manager for the analytics data layer.
    Uses psycopg v3 with simple connection management.

    Thread-safe: uses a lock around pool creation.
    """

    def __init__(self, postgres_url: str, pool_size: int = 5):
        self._url = postgres_url
        self._pool_size = pool_size
        self._lock = threading.Lock()
        self._available: Optional[bool] = None

    @contextmanager
    def connection(self):
        """
        Context manager for a database connection.
        Auto-commits on success, rolls back on exception.
        """
        psycopg = _ensure_psycopg()
        conn = None
        try:
            conn = psycopg.connect(self._url, autocommit=False)
            yield conn
            conn.commit()
        except Exception:
            if conn and not conn.closed:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if conn and not conn.closed:
                try:
                    conn.close()
                except Exception:
                    pass

    def execute(self, sql: str, params: tuple = ()) -> None:
        """Execute a single SQL statement (no return value)."""
        with self.connection() as conn:
            conn.execute(sql, params)

    def execute_many(self, sql: str, params_list: List[tuple]) -> None:
        """Execute a SQL statement for each set of params."""
        with self.connection() as conn:
            cur = conn.cursor()
            for params in params_list:
                cur.execute(sql, params)

    def fetch_one(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Execute and fetch one row as a dict."""
        with self.connection() as conn:
            cur = conn.execute(sql, params)
            row = cur.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))

    def fetch_all(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Execute and fetch all rows as list of dicts."""
        with self.connection() as conn:
            cur = conn.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def is_available(self) -> bool:
        """
        Check if PostgreSQL is reachable. Caches result until reset.
        """
        if self._available is not None:
            return self._available
        try:
            with self.connection() as conn:
                conn.execute("SELECT 1")
            self._available = True
        except Exception as e:
            log.warning("Analytics DB unavailable: %s", e)
            self._available = False
        return self._available

    def reset_availability(self) -> None:
        """Reset cached availability (for retry after recovery)."""
        self._available = None

    def close(self) -> None:
        """Cleanup (no persistent pool to close in simple mode)."""
        self._available = None


# ── Singleton accessor ────────────────────────────────────────


@lru_cache(maxsize=1)
def get_analytics_db() -> Optional[AnalyticsDB]:
    """
    Return the singleton AnalyticsDB instance, or None if disabled.
    Reads from settings; safe to call even when PostgreSQL is not configured.
    """
    try:
        from settings import get_settings
        s = get_settings()

        if not getattr(s, "ANALYTICS_DB_ENABLED", False):
            log.info("Analytics DB disabled (ANALYTICS_DB_ENABLED=0)")
            return None

        url = getattr(s, "POSTGRES_URL", "")
        if not url:
            log.warning("ANALYTICS_DB_ENABLED=1 but POSTGRES_URL is empty")
            return None

        pool_size = getattr(s, "ANALYTICS_DB_POOL_SIZE", 5)
        db = AnalyticsDB(url, pool_size=pool_size)
        log.info("Analytics DB initialized (pool_size=%d)", pool_size)
        return db

    except Exception as e:
        log.error("Failed to initialize Analytics DB: %s", e)
        return None


def reset_analytics_db() -> None:
    """Clear cached AnalyticsDB (for testing)."""
    get_analytics_db.cache_clear()
