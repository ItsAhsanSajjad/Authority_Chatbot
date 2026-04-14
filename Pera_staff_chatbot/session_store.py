"""
PERA AI — Bounded Session Store (v2: Persistent Backends)

Provides server-side session state for follow-up context integrity.
Supports three backends:
  - Redis (production default, survives restarts)
  - SQLite (local dev / fallback if Redis unavailable)
  - Memory (testing only)

Each session stores:
  - bounded recent turn history (question + answer + decision)
  - last subject/entity/role anchors
  - last evidence references
  - last normalized query

Interface is backend-agnostic: all backends implement get/put/get_or_create.
"""
from __future__ import annotations

import json
import os
import time
import threading
import uuid
import sqlite3
from collections import OrderedDict
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List

from log_config import get_logger

log = get_logger("pera.session")


# ── Session Turn ──────────────────────────────────────────────
@dataclass
class SessionTurn:
    """One turn in the conversation."""
    question: str
    normalized_query: str = ""
    answer_preview: str = ""    # first 300 chars (privacy-bounded)
    decision: str = ""          # answer / refuse / error
    evidence_ids: List[str] = field(default_factory=list)
    doc_names: List[str] = field(default_factory=list)
    subject_entity: str = ""    # extracted subject/role/entity
    timestamp: float = field(default_factory=time.time)


# ── Session State ─────────────────────────────────────────────
@dataclass
class SessionState:
    """Server-side state for one session."""
    session_id: str
    turns: List[SessionTurn] = field(default_factory=list)
    last_subject: str = ""      # most recent anchored entity/role
    last_lookup_type: str = ""  # most recent stored-API lookup type (e.g. challan_daterange:...)
    last_doc_names: List[str] = field(default_factory=list)
    last_evidence_ids: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)

    def add_turn(self, turn: SessionTurn, max_turns: int = 10) -> None:
        self.turns.append(turn)
        if len(self.turns) > max_turns:
            self.turns = self.turns[-max_turns:]
        self.last_active = time.time()

        # Update anchors
        if turn.subject_entity:
            self.last_subject = turn.subject_entity
        if turn.doc_names:
            self.last_doc_names = turn.doc_names[:5]
        if turn.evidence_ids:
            self.last_evidence_ids = turn.evidence_ids[:10]

    @property
    def last_question(self) -> str:
        return self.turns[-1].question if self.turns else ""

    @property
    def last_answer(self) -> str:
        return self.turns[-1].answer_preview if self.turns else ""

    @property
    def last_normalized_query(self) -> str:
        return self.turns[-1].normalized_query if self.turns else ""

    def is_expired(self, ttl_seconds: int = 3600) -> bool:
        return (time.time() - self.last_active) > ttl_seconds

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to JSON-safe dict."""
        return {
            "session_id": self.session_id,
            "turns": [asdict(t) for t in self.turns],
            "last_subject": self.last_subject,
            "last_lookup_type": self.last_lookup_type,
            "last_doc_names": self.last_doc_names,
            "last_evidence_ids": self.last_evidence_ids,
            "created_at": self.created_at,
            "last_active": self.last_active,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SessionState":
        """Deserialize from dict."""
        turns = [SessionTurn(**t) for t in data.get("turns", [])]
        return cls(
            session_id=data.get("session_id", ""),
            turns=turns,
            last_subject=data.get("last_subject", ""),
            last_lookup_type=data.get("last_lookup_type", ""),
            last_doc_names=data.get("last_doc_names", []),
            last_evidence_ids=data.get("last_evidence_ids", []),
            created_at=data.get("created_at", time.time()),
            last_active=data.get("last_active", time.time()),
        )


# ── Abstract Base ─────────────────────────────────────────────
class BaseSessionStore:
    """Interface that all session backends implement."""

    def get(self, session_id: str) -> Optional[SessionState]:
        raise NotImplementedError

    def put(self, session_id: str, state: SessionState) -> None:
        raise NotImplementedError

    def get_or_create(self, session_id: Optional[str] = None) -> SessionState:
        raise NotImplementedError


# ═════════════════════════════════════════════════════════════
# Backend 1: In-Memory (testing / emergency fallback)
# ═════════════════════════════════════════════════════════════
class MemorySessionStore(BaseSessionStore):
    """Thread-safe bounded in-memory session store."""

    def __init__(self, max_sessions: int = 2000, ttl_seconds: int = 3600):
        self._lock = threading.Lock()
        self._store: OrderedDict[str, SessionState] = OrderedDict()
        self._max = max_sessions
        self._ttl = ttl_seconds

    def get(self, session_id: str) -> Optional[SessionState]:
        with self._lock:
            state = self._store.get(session_id)
            if state and state.is_expired(self._ttl):
                del self._store[session_id]
                return None
            if state:
                self._store.move_to_end(session_id)
            return state

    def get_or_create(self, session_id: Optional[str] = None) -> SessionState:
        if not session_id:
            session_id = uuid.uuid4().hex[:16]

        with self._lock:
            existing = self._store.get(session_id)
            if existing and not existing.is_expired(self._ttl):
                self._store.move_to_end(session_id)
                return existing

            state = SessionState(session_id=session_id)
            self._store[session_id] = state
            self._store.move_to_end(session_id)
            self._evict()
            return state

    def put(self, session_id: str, state: SessionState) -> None:
        with self._lock:
            self._store[session_id] = state
            self._store.move_to_end(session_id)
            self._evict()

    def _evict(self) -> None:
        while len(self._store) > self._max:
            self._store.popitem(last=False)


# ═════════════════════════════════════════════════════════════
# Backend 2: SQLite (local dev / Redis fallback)
# ═════════════════════════════════════════════════════════════
class SqliteSessionStore(BaseSessionStore):
    """SQLite-backed persistent session store. WAL mode for concurrency."""

    def __init__(self, db_path: str = "data/sessions.db",
                 max_sessions: int = 2000, ttl_seconds: int = 3600):
        self._db_path = db_path
        self._max = max_sessions
        self._ttl = ttl_seconds
        self._lock = threading.Lock()

        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
        self._init_db()
        log.info("SQLite session store initialized: %s", db_path)

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    last_active REAL NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_last_active ON sessions(last_active)")

    def get(self, session_id: str) -> Optional[SessionState]:
        with self._lock:
            try:
                with self._get_conn() as conn:
                    row = conn.execute(
                        "SELECT data FROM sessions WHERE session_id = ?",
                        (session_id,)
                    ).fetchone()
                    if not row:
                        return None
                    state = SessionState.from_dict(json.loads(row[0]))
                    if state.is_expired(self._ttl):
                        conn.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
                        return None
                    return state
            except Exception as e:
                log.error("SQLite session get error: %s", e)
                return None

    def put(self, session_id: str, state: SessionState) -> None:
        with self._lock:
            try:
                data = json.dumps(state.to_dict(), ensure_ascii=False)
                with self._get_conn() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO sessions (session_id, data, last_active, created_at) "
                        "VALUES (?, ?, ?, ?)",
                        (session_id, data, state.last_active, state.created_at)
                    )
                    self._cleanup(conn)
            except Exception as e:
                log.error("SQLite session put error: %s", e)

    def get_or_create(self, session_id: Optional[str] = None) -> SessionState:
        if not session_id:
            session_id = uuid.uuid4().hex[:16]

        existing = self.get(session_id)
        if existing:
            return existing

        state = SessionState(session_id=session_id)
        self.put(session_id, state)
        return state

    def _cleanup(self, conn: sqlite3.Connection) -> None:
        """Expire old sessions and enforce capacity limit."""
        cutoff = time.time() - self._ttl
        conn.execute("DELETE FROM sessions WHERE last_active < ?", (cutoff,))
        count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        if count > self._max:
            excess = count - self._max
            conn.execute(
                "DELETE FROM sessions WHERE session_id IN "
                "(SELECT session_id FROM sessions ORDER BY last_active ASC LIMIT ?)",
                (excess,)
            )


# ═════════════════════════════════════════════════════════════
# Backend 3: Redis (production)
# ═════════════════════════════════════════════════════════════
class RedisSessionStore(BaseSessionStore):
    """Redis-backed persistent session store with TTL."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0",
                 ttl_seconds: int = 3600, max_turns: int = 10):
        self._ttl = ttl_seconds
        self._max_turns = max_turns
        self._prefix = "pera:session:"

        import redis as redis_mod
        self._client = redis_mod.Redis.from_url(redis_url, decode_responses=True)
        # Test connection
        self._client.ping()
        log.info("Redis session store connected: %s", redis_url.split("@")[-1])

    def get(self, session_id: str) -> Optional[SessionState]:
        try:
            data = self._client.get(self._prefix + session_id)
            if not data:
                return None
            state = SessionState.from_dict(json.loads(data))
            return state
        except Exception as e:
            log.error("Redis session get error: %s", e)
            return None

    def put(self, session_id: str, state: SessionState) -> None:
        try:
            data = json.dumps(state.to_dict(), ensure_ascii=False)
            self._client.setex(self._prefix + session_id, self._ttl, data)
        except Exception as e:
            log.error("Redis session put error: %s", e)

    def get_or_create(self, session_id: Optional[str] = None) -> SessionState:
        if not session_id:
            session_id = uuid.uuid4().hex[:16]

        existing = self.get(session_id)
        if existing:
            return existing

        state = SessionState(session_id=session_id)
        self.put(session_id, state)
        return state


# ═════════════════════════════════════════════════════════════
# Factory — creates the appropriate backend
# ═════════════════════════════════════════════════════════════
def create_session_store(
    backend: str = "redis",
    redis_url: str = "redis://localhost:6379/0",
    sqlite_path: str = "data/sessions.db",
    ttl_seconds: int = 3600,
    max_sessions: int = 2000,
    max_turns: int = 10,
) -> BaseSessionStore:
    """
    Create the session store based on backend preference.
    Falls back to SQLite if Redis is unavailable.
    Falls back to Memory if SQLite fails.
    """
    backend = (backend or "redis").strip().lower()

    if backend == "redis":
        try:
            store = RedisSessionStore(redis_url=redis_url, ttl_seconds=ttl_seconds, max_turns=max_turns)
            return store
        except Exception as e:
            log.warning(
                "Redis session store unavailable (%s). Falling back to SQLite.", e
            )
            backend = "sqlite"

    if backend == "sqlite":
        try:
            return SqliteSessionStore(db_path=sqlite_path, max_sessions=max_sessions, ttl_seconds=ttl_seconds)
        except Exception as e:
            log.warning("SQLite session store failed (%s). Falling back to Memory.", e)
            backend = "memory"

    # Memory fallback
    log.warning("Using in-memory session store. Sessions will NOT survive restarts.")
    return MemorySessionStore(max_sessions=max_sessions, ttl_seconds=ttl_seconds)


# ── Singleton ─────────────────────────────────────────────────
_store: Optional[BaseSessionStore] = None
_store_lock = threading.Lock()


def get_session_store() -> BaseSessionStore:
    """Get the singleton session store, creating it on first access."""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                from settings import get_settings
                s = get_settings()
                _store = create_session_store(
                    backend=s.SESSION_BACKEND,
                    redis_url=s.REDIS_URL,
                    sqlite_path=s.SQLITE_SESSION_PATH,
                    ttl_seconds=s.SESSION_TTL_SECONDS,
                    max_sessions=s.MAX_SESSIONS,
                    max_turns=s.MAX_TURNS_PER_SESSION,
                )
    return _store


def reset_session_store() -> None:
    """Reset singleton (for testing)."""
    global _store
    _store = None
