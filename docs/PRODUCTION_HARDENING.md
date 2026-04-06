# PERA AI — Production Hardening Guide

## Overview
This document summarizes all production hardening changes made in v3.0.0.

## Key Changes

### 1. Centralized Configuration (`settings.py`)
All configuration is now managed through Pydantic `BaseSettings`. This provides:
- Type validation at startup
- Environment variable loading with `.env` fallback
- Secret protection (API keys never appear in logs)
- Single source of truth — no more scattered `os.getenv()` calls

### 2. Authentication (`auth.py`)
All API endpoints (except `/health` and `/ready`) are protected. Supports:
- API key via `X-API-Key` header
- JWT Bearer tokens
- Disabled for local development (`AUTH_ENABLED=0`)

### 3. Persistent Sessions
Sessions now survive server restarts:
- **Redis** (production) — fast, distributed
- **SQLite** (fallback) — local, zero-config
- **Memory** (testing) — original behavior

### 4. DOCX Support
Auto-indexer now processes both PDF and DOCX files.

### 5. Repository Structure
```
PERAAIA/
├── settings.py          # Centralized config
├── auth.py              # Authentication
├── pera_vocab.py        # Unified vocabulary
├── session_store.py     # Persistent sessions (3 backends)
├── fastapi_app.py       # API (auth + rate limiting + CORS)
├── tests/               # pytest suite
├── docs/                # Documentation
├── legacy/              # Archived Streamlit app
├── dev/                 # Debug scripts and logs
└── .env.example         # Config template
```

## Deployment Checklist

1. ✅ Copy `.env.example` → `.env`, fill in production values
2. ✅ Set `AUTH_ENABLED=1`
3. ✅ Configure `API_KEYS` with strong keys
4. ✅ Set `CORS_ORIGINS` to your frontend domain
5. ✅ Set `BASE_URL` to your production URL
6. ✅ Install/configure Redis (or accept SQLite fallback)
7. ✅ Run `pip install -r requirements.txt`
8. ✅ Run `pytest tests/ -v` to verify setup
9. ✅ Start: `uvicorn fastapi_app:app --host 0.0.0.0 --port 8000`
