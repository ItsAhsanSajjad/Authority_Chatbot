"""Tests for session store backends."""
import os
import time
import tempfile
import pytest


def test_memory_store_basic():
    """MemorySessionStore should create and retrieve sessions."""
    from session_store import MemorySessionStore, SessionTurn
    store = MemorySessionStore(max_sessions=100, ttl_seconds=3600)

    session = store.get_or_create("test-session-1")
    assert session.session_id == "test-session-1"
    assert len(session.turns) == 0

    # Add a turn
    session.add_turn(SessionTurn(
        question="What is PERA?",
        answer_preview="PERA is the Punjab...",
        decision="answer",
        subject_entity="PERA",
    ))
    store.put("test-session-1", session)

    # Retrieve
    retrieved = store.get("test-session-1")
    assert retrieved is not None
    assert len(retrieved.turns) == 1
    assert retrieved.last_subject == "PERA"


def test_memory_store_ttl_expiry():
    """Sessions should expire after TTL."""
    from session_store import MemorySessionStore, SessionTurn
    store = MemorySessionStore(max_sessions=100, ttl_seconds=1)

    session = store.get_or_create("expire-test")
    store.put("expire-test", session)

    # Force expiry by manipulating last_active
    session.last_active = time.time() - 2
    store.put("expire-test", session)

    result = store.get("expire-test")
    assert result is None, "Expired session should not be returned"


def test_memory_store_capacity_limit():
    """Store should evict oldest sessions when capacity is exceeded."""
    from session_store import MemorySessionStore
    store = MemorySessionStore(max_sessions=3, ttl_seconds=3600)

    for i in range(5):
        store.get_or_create(f"session-{i}")

    # Oldest sessions should be evicted
    assert store.get("session-0") is None
    assert store.get("session-1") is None
    assert store.get("session-4") is not None


def test_memory_store_history_trimming():
    """Turns should be trimmed to max_turns."""
    from session_store import MemorySessionStore, SessionTurn
    store = MemorySessionStore(max_sessions=100, ttl_seconds=3600)
    session = store.get_or_create("trim-test")

    for i in range(15):
        session.add_turn(SessionTurn(
            question=f"Question {i}",
            answer_preview=f"Answer {i}",
        ), max_turns=10)

    assert len(session.turns) == 10
    assert session.turns[0].question == "Question 5"  # oldest kept


def test_sqlite_store_persistence():
    """SqliteSessionStore should persist across instances."""
    from session_store import SqliteSessionStore, SessionTurn

    # Use a fixed temp path instead of TemporaryDirectory (Windows WAL cleanup issue)
    db_path = os.path.join(tempfile.gettempdir(), "pera_test_persist.db")
    try:
        # Clean up from prior run
        for f in [db_path, db_path + "-wal", db_path + "-shm"]:
            if os.path.exists(f):
                os.unlink(f)

        # Write
        store1 = SqliteSessionStore(db_path=db_path, ttl_seconds=3600)
        session = store1.get_or_create("persist-test")
        session.add_turn(SessionTurn(
            question="test question",
            answer_preview="test answer",
            subject_entity="Test Entity",
        ))
        store1.put("persist-test", session)

        # Read from new instance (simulates restart)
        store2 = SqliteSessionStore(db_path=db_path, ttl_seconds=3600)
        retrieved = store2.get("persist-test")
        assert retrieved is not None
        assert len(retrieved.turns) == 1
        assert retrieved.last_subject == "Test Entity"
    finally:
        # Best-effort cleanup
        for f in [db_path, db_path + "-wal", db_path + "-shm"]:
            try:
                if os.path.exists(f):
                    os.unlink(f)
            except OSError:
                pass


def test_sqlite_store_ttl():
    """SQLite sessions should expire after TTL."""
    from session_store import SqliteSessionStore, SessionTurn

    db_path = os.path.join(tempfile.gettempdir(), "pera_test_ttl.db")
    try:
        for f in [db_path, db_path + "-wal", db_path + "-shm"]:
            if os.path.exists(f):
                os.unlink(f)

        store = SqliteSessionStore(db_path=db_path, ttl_seconds=1)
        session = store.get_or_create("ttl-test")
        session.last_active = time.time() - 2
        store.put("ttl-test", session)

        result = store.get("ttl-test")
        assert result is None
    finally:
        for f in [db_path, db_path + "-wal", db_path + "-shm"]:
            try:
                if os.path.exists(f):
                    os.unlink(f)
            except OSError:
                pass


def test_session_state_serialization():
    """SessionState should round-trip through to_dict/from_dict."""
    from session_store import SessionState, SessionTurn

    original = SessionState(session_id="serial-test")
    original.add_turn(SessionTurn(
        question="What is the CTO salary?",
        normalized_query="salary chief technology officer",
        answer_preview="The CTO salary is...",
        decision="answer",
        subject_entity="Chief Technology Officer",
        evidence_ids=["chunk-1", "chunk-2"],
        doc_names=["PERA Act.pdf"],
    ))

    data = original.to_dict()
    restored = SessionState.from_dict(data)

    assert restored.session_id == "serial-test"
    assert len(restored.turns) == 1
    assert restored.last_subject == "Chief Technology Officer"
    assert restored.turns[0].question == "What is the CTO salary?"


def test_factory_creates_memory_backend():
    """Factory should create memory backend when specified."""
    from session_store import create_session_store, MemorySessionStore
    store = create_session_store(backend="memory")
    assert isinstance(store, MemorySessionStore)


def test_factory_falls_back_from_redis():
    """Factory should fall back to SQLite when Redis is unavailable."""
    from session_store import create_session_store, SqliteSessionStore

    db_path = os.path.join(tempfile.gettempdir(), "pera_test_fallback.db")
    try:
        for f in [db_path, db_path + "-wal", db_path + "-shm"]:
            if os.path.exists(f):
                os.unlink(f)

        store = create_session_store(
            backend="redis",
            redis_url="redis://localhost:99999",  # invalid port
            sqlite_path=db_path,
        )
        assert isinstance(store, SqliteSessionStore)
    finally:
        for f in [db_path, db_path + "-wal", db_path + "-shm"]:
            try:
                if os.path.exists(f):
                    os.unlink(f)
            except OSError:
                pass
