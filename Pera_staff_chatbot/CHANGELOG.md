# PERA AI — Production Hardening Changelog

## v3.0.0 (2026-03-19)

### 🔐 Security
- **API Authentication**: Added `auth.py` with API key (`X-API-Key` header) and JWT Bearer support
  - All endpoints except `/health` and `/ready` now require authentication
  - `AUTH_ENABLED=0` for local dev, fails fast in production if no keys configured
- **CORS Hardening**: Replaced wildcard `allow_origins=["*"]` with environment-driven `CORS_ORIGINS`
- **Rate Limiting**: Added `slowapi` middleware — 30 req/min for ask endpoints, 5 req/min for transcribe
- **Secret Management**: API keys stored as `SecretStr` (never logged), all secrets loaded from env vars

### 📦 Configuration Centralization
- **New `settings.py`**: Pydantic `BaseSettings` — single source of truth for all configuration
  - Replaces 8+ scattered `os.getenv()` calls across modules
  - Startup validation for required secrets
  - Type-safe config with defaults
- **New `.env.example`**: Template with all environment variables documented
- **Unified `BASE_URL`**: Removed hardcoded URLs from `fastapi_app.py` and `answerer.py`

### 💾 Persistent Session Store
- **Redis backend** (production default): Sessions survive server restarts
- **SQLite backend** (automatic fallback): WAL mode, bounded capacity
- **Memory backend** (testing only): Preserved for test compatibility
- **Graceful degradation**: Redis → SQLite → Memory fallback chain with warning logs
- **TTL-based expiry**: Sessions automatically expire (configurable via `SESSION_TTL_SECONDS`)

### 📚 Vocabulary Consolidation
- **New `pera_vocab.py`**: Single source of truth for abbreviations, schedules, role keywords
  - Merged 3 duplicate abbreviation maps from `retriever.py` and `answerer.py`
  - Includes typed accessors: `get_normalized_abbreviation_map()`, `get_lowercase_abbreviation_map()`
- **Removed duplicates**: All modules now import from `pera_vocab`

### 📄 DOCX Auto-Indexing
- **`doc_registry.py`**: `SUPPORTED_EXTS` now includes `.docx`
- **`index_manager.py`**: Removed `.pdf`-only filter that was blocking DOCX files

### 🎯 Answer Quality
- **Improved support-state wording**: Each confidence level now has explicit, professional qualifiers
  - `supported` → clean answer, no disclaimers
  - `partially_supported` → answer + "may not cover all aspects" note
  - `conflicting` → answer + "differing provisions" note
  - `unsupported` → answer + "do not directly address" note (never refuses)

### 📊 Enhanced Audit Trail
- **`auth_identity`**: Records who made each request (API key prefix or JWT subject)
- **`prompt_version`**: Records which prompt version produced the answer
- All audit config now reads from centralized `settings.py`

### 🧹 Repository Cleanup
- **Legacy Streamlit app**: Moved `app.py` → `legacy/app.py` with deprecation notice
- **Debug files**: Moved all `debug_*.py`, `tmp_*.py` → `dev/scripts/`
- **Log files**: Moved all `*_test.txt`, `debug_*.txt` → `dev/logs/`
- **Old modules**: `abbreviations.py`, `retriever_legacy.py` → `legacy/`

### 🧪 Test Suite
- **New `tests/` directory** with 7 test modules covering:
  - `test_settings.py` — config loading, validation, env overrides
  - `test_auth.py` — auth enforcement, public endpoints
  - `test_pera_vocab.py` — abbreviation maps, expansion, consistency
  - `test_session_store.py` — all 3 backends: create/retrieve/TTL/persistence/fallback
  - `test_context_state.py` — entity anchoring, pronoun substitution
  - `test_doc_registry.py` — DOCX support, authority classification
  - `test_answerer.py` — support-state wording
- **`pytest.ini`**: Test configuration
- **`conftest.py`**: Shared fixtures with automatic settings reset

### 📖 Documentation
- **`CHANGELOG.md`**: This file
- **`docs/SECURITY.md`**: Auth, CORS, rate limiting, secret management
- **`docs/PRODUCTION_HARDENING.md`**: Migration notes, deployment guide
- **`.env.example`**: All environment variables documented

### ⚙️ Infrastructure
- **Updated `requirements.txt`**: Added `pydantic-settings`, `slowapi`, `PyJWT`, `redis`, `pytest`, `httpx`
- **Version bump**: PERA AI Backend → v3.0.0

---

## Migration Notes

### For existing deployments
1. Copy `.env.example` to `.env` and fill in values
2. Set `AUTH_ENABLED=1` and configure `API_KEYS` for production
3. Set `CORS_ORIGINS` to your frontend domain(s)
4. Install Redis or ensure SQLite fallback is acceptable
5. Install new dependencies: `pip install -r requirements.txt`
6. Existing `.env` files will continue to work (backward compatible)

### Breaking changes
- API endpoints now require authentication when `AUTH_ENABLED=1`
- CORS no longer allows `*` by default — must configure `CORS_ORIGINS`
- Frontend must send `X-API-Key` header (or Bearer token) for API calls
