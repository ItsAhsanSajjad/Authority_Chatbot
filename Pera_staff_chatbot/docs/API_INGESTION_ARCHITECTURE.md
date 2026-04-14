# API Ingestion Architecture

## Overview

PERA AI is extending its knowledge pipeline beyond static PDF/DOCX documents to include
**live API data sources** — structured data from government endpoints that can be
automatically fetched, normalized, chunked, indexed, and made available to the RAG pipeline.

## Current Status: Phase 1 (Foundation)

Phase 1 establishes the configuration, discovery, and registry foundation. **No live API
fetching or query-time integration exists yet.**

### What Phase 1 Provides
- YAML-based API source configuration schema
- Config parsing and validation (`api_config_models.py`)
- Utility helpers for hashing, scanning, normalization (`api_source_utils.py`)
- SQLite database migration foundation with 7 tables (`api_db.py`)
- Source registry for tracking API source lifecycle (`api_registry.py`)
- Source discovery for detecting new/changed/removed configs (`api_discovery.py`)
- 24 centralized settings in `settings.py`
- Example configs in `assets/apis/`

## Planned Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    PERA AI Knowledge Pipeline                 │
├─────────────────────┬────────────────────────────────────────┤
│   Document Layer    │         API Ingestion Layer            │
│   (PDF/DOCX)        │         (Phase 1+)                     │
│                     │                                        │
│   ┌─────────┐       │   ┌──────────────┐   ┌──────────────┐ │
│   │Extractors│       │   │ Discovery    │   │ Config Models│ │
│   │Chunker  │       │   │ (filesystem) │   │ (YAML parse) │ │
│   │Indexer  │       │   └──────┬───────┘   └──────────────┘ │
│   └────┬────┘       │          │                             │
│        │            │   ┌──────▼───────┐                     │
│        │            │   │  Registry    │ ◄── Source Metadata │
│        │            │   └──────┬───────┘                     │
│        │            │          │                              │
│        │            │   ┌──────▼───────┐  Phase 2+           │
│        │            │   │  Fetcher     │  ← Not yet built    │
│        │            │   │  Normalizer  │                     │
│        │            │   │  Differ      │                     │
│        │            │   │  Chunker     │                     │
│        │            │   └──────┬───────┘                     │
│        │            │          │                              │
│   ┌────▼────────────▼──────────▼───────┐                     │
│   │         Unified Vector Index        │                    │
│   │         (FAISS)                     │                    │
│   └────────────────┬───────────────────┘                     │
│                    │                                         │
│   ┌────────────────▼───────────────────┐                     │
│   │    Retriever → Answerer → LLM      │                    │
│   └────────────────────────────────────┘                     │
└──────────────────────────────────────────────────────────────┘
```

## Database Schema (Phase 1)

| Table | Purpose |
|-------|---------|
| `knowledge_sources` | Master registry of all knowledge sources (docs + APIs) |
| `api_source_configs` | Stored API config YAML content and metadata |
| `api_sync_runs` | Sync execution history and results |
| `api_records` | Individual records fetched from APIs |
| `knowledge_chunks` | Text chunks ready for embedding |
| `vector_index_map` | Mapping between chunks and FAISS index entries |
| `schema_migrations` | Migration version tracking |

## Source Lifecycle

```
Config File Added → Discovery → Registry (active)
Config File Changed → Discovery → Registry (updated, re-hash)
Config File Removed → Discovery → Registry (pending_removal → removed)
Config disabled: true → Registry (disabled)
Parse/validation error → Registry (error)
```

## Phase Roadmap

| Phase | Scope |
|-------|-------|
| **Phase 1** ✅ | Config, discovery, registry, DB foundation |
| **Phase 2** | API fetching, snapshot storage, diffing |
| **Phase 3** | Normalization, chunking, vector indexing |
| **Phase 4** | Retriever/answerer integration |
| **Phase 5** | Background sync, monitoring, audit |
