# API Ingestion Operations Runbook

## Current Status: Phase 1 (Foundation Only)

Phase 1 provides configuration, discovery, and registry infrastructure.
**No live API fetching, normalization, or indexing is active yet.**

---

## Adding a New API Source

### Step 1: Create a Config File
Create a YAML file in `assets/apis/`:

```bash
# Copy the example template
cp assets/apis/example_basic_api.yaml assets/apis/my_new_api.yaml
```

Edit the file with your API details. See [API_SOURCE_CONFIG.md](API_SOURCE_CONFIG.md)
for the full schema reference.

### Step 2: Set Environment Variables
If your API requires authentication, set the referenced env vars:

```bash
# In .env or system environment
MY_API_TOKEN=your-actual-token-here
```

### Step 3: Validate the Config
```python
from api_config_models import load_api_source_config
config = load_api_source_config("assets/apis/my_new_api.yaml")
print(f"Source: {config.source_id}, URL: {config.fetch.url}")
```

### Step 4: Run Discovery (Phase 1)
```python
from api_db import ApiDatabase
from api_registry import ApiSourceRegistry
from api_discovery import ApiSourceDiscovery

db = ApiDatabase("data/api_ingestion.db")
db.migrate()
registry = ApiSourceRegistry(db)
discovery = ApiSourceDiscovery(registry)
result = discovery.reconcile_sources()
print(result.summary())
```

---

## Source Lifecycle States

| Status | Meaning |
|--------|---------|
| `active` | Config is valid, source is enabled |
| `disabled` | Config has `enabled: false` |
| `error` | Config failed validation or processing |
| `pending_sync` | Awaiting next sync cycle (Phase 2+) |
| `pending_removal` | Config file removed, in grace period |
| `removed` | Config file removed, grace period expired |

---

## Removing an API Source

1. Delete the YAML file from `assets/apis/`
2. On next discovery run, the source enters `pending_removal`
3. After the grace period (default 24 hours), it moves to `removed`
4. Set `API_ENABLE_PENDING_REMOVAL_GRACE=0` to skip the grace period

---

## Database Operations

### Run Migrations
```python
from api_db import ApiDatabase
db = ApiDatabase("data/api_ingestion.db")
db.migrate()
```

### Inspect Sources
```python
from api_db import ApiDatabase
db = ApiDatabase("data/api_ingestion.db")
for source in db.get_all_sources():
    print(f"{source['source_id']}: {source['status']}")
```

---

## What Later Phases Will Add

| Phase | Operations |
|-------|-----------|
| **Phase 2** | `api_fetcher.py` — HTTP fetch, snapshot, diff |
| **Phase 3** | `api_normalizer.py` — record → text → chunks → embeddings |
| **Phase 4** | Retriever/answerer integration |
| **Phase 5** | Background sync scheduler, health monitoring |

---

## Troubleshooting

### Config Not Detected
- Verify file is in `assets/apis/`
- Verify file extension is `.yaml` or `.yml`
- Verify filename doesn't start with `.` or `_`
- Check logs for YAML parse errors

### Invalid Config
- Run `load_api_source_config(path)` directly to see the validation error
- Common issues: missing `source_id`, invalid characters, missing `fetch.url`

### Database Issues
- DB file: `data/api_ingestion.db`
- Uses SQLite WAL mode — may have `-wal` and `-shm` companion files
- Run `db.migrate()` to apply any pending migrations
