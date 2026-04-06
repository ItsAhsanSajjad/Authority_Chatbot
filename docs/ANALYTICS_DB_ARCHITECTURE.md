# Analytics DB Architecture

## Overview

PERA AI's Analytics DB is a PostgreSQL-backed structured data layer that runs alongside the existing SQLite-based API ingestion and FAISS-based document RAG. It provides:

- **Structured storage** for province-scale Punjab government data
- **Historical tracking** with timestamped snapshots for trend analysis
- **Geography dimensions** (divisions → districts → tehsils)
- **Fact tables** for workforce strength, finance (summary + monthly), and challan data
- **Write-through** from existing API ingestion pipeline (passes raw_payload for summary extraction)

## Architecture

```
┌─────────────────────────────┐
│     API Ingestion Sync      │
│   (api_sync_manager.py)     │
└──────┬──────────────┬───────┘
       │              │
       ▼              ▼
┌──────────────┐ ┌────────────────┐
│   SQLite DB  │ │  PostgreSQL DB │ ← NEW
│  (api_db.py) │ │ (analytics_*)  │
│              │ │                │
│ • Sources    │ │ • Sources PG   │
│ • Records    │ │ • Records PG   │
│ • Sync Runs  │ │ • Raw Snapshots│
│ • Chunks     │ │ • Dimensions   │
│ • Vectors    │ │ • Fact Tables  │
└──────────────┘ └────────────────┘
       │
       ▼
┌──────────────┐
│  FAISS Index │ ← UNCHANGED
│  (RAG Path)  │
└──────────────┘
```

## Key Design Decisions

### 1. Dual-Write, Not Migration
PostgreSQL supplements SQLite rather than replacing it. The existing SQLite pipeline
(api_db.py) continues to own chunk/vector management. PostgreSQL handles analytics.

### 2. Feature-Flag Gated
All PostgreSQL operations are gated behind:
- `ANALYTICS_DB_ENABLED` — master toggle for DB initialization
- `ANALYTICS_WRITE_ENABLED` — toggle for write-through during sync
- `ANALYTICS_AUTO_MIGRATE` — auto-run schema migrations on startup

### 3. Graceful Degradation
If PostgreSQL is unavailable:
- `get_analytics_db()` returns `None`
- All store operations catch exceptions and log warnings
- The existing RAG pipeline is completely unaffected

### 4. psycopg v3 (No ORM)
Direct SQL with psycopg v3 for maximum control and minimal overhead.
No SQLAlchemy or other ORM — keeps the footprint small and queries explicit.

### 5. Idempotent Migrations
All migrations use `CREATE TABLE IF NOT EXISTS` and `ON CONFLICT` patterns.
Re-running migrations is always safe.

## Module Map

| Module | Purpose |
|---|---|
| `analytics_db.py` | Connection management, health checks |
| `analytics_migrations.py` | Schema migrations (18 versions) |
| `analytics_models.py` | Python dataclasses for table rows |
| `analytics_store.py` | CRUD operations for all analytics tables |
| `analytics_mapping.py` | API source → curated table mapping |

## Connection Management

```python
from analytics_db import get_analytics_db

db = get_analytics_db()  # Returns None if disabled
if db and db.is_available():
    with db.connection() as conn:
        conn.execute("SELECT * FROM dim_division")
```

## Safety Model

1. **Import safety**: `analytics_db.py` lazy-imports psycopg so the module loads even without the package
2. **Connection safety**: All store methods use try/except → log warning, return False/None
3. **Pipeline safety**: Write-through in `api_sync_manager.py` is wrapped in try/except
4. **Config safety**: Everything disabled by default — must explicitly enable in `.env`
