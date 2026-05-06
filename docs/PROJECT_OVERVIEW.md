# PERA AI Chatbot — Complete Project Overview

> An end-to-end description of what the system is, what it does, every
> component that makes it work, and how the pieces fit together.

## 1. Purpose

The Punjab Enforcement and Regulatory Authority (PERA) needs a single
conversational interface where higher-authority officials can ask
natural-language questions about:

1. **Policy and regulations** — the PERA Act 2024, service rules,
   schedules, annexures (medical, gratuity, weapons), pay scales.
2. **Operational performance** — inspection counts, challans issued
   and recovered, FIRs, sealing, arrests, warnings, fine amounts.
3. **Hierarchical analytics** — same metrics rolled up by tehsil,
   district, or division, with date filters.
4. **Officer-level performance** — per-officer inspection and
   challan productivity.
5. **Live operational status** — ranking by metric (top tehsils by
   FIRs, by paid amount), today's operations, freshness.

The chatbot must answer correctly, cite its sources, and refuse to
hallucinate when the underlying data does not contain the answer.

## 2. Tech stack

| Layer | Technology |
|---|---|
| Frontend | **Next.js 14.2.5** + React 18 + TypeScript + Tailwind CSS |
| Backend | **FastAPI** + uvicorn (ASGI) on Python 3.14 |
| Vector store | **FAISS** (`IndexIDMap2` over `IndexFlatIP`) for cosine via normalized inner product |
| Analytics DB | **PostgreSQL** (curated dimensional + fact tables) |
| Ingestion DB | **SQLite** (`api_ingestion.db`) for raw API snapshots |
| Session store | **Redis** (configurable; falls back to SQLite or in-memory) |
| LLMs | **OpenAI** — `gpt-4o` (answer), `gpt-4o-mini` (rewrite + grounding judge), `text-embedding-3-small` (1536-d), `whisper-1` (voice) |
| Auth | JWT (PyJWT) on admin routes |
| Background | Threading-based schedulers (no Celery) |
| Live data | **SDEO Dashboard API** at `pera360.punjab.gov.pk/backend/api` |

## 3. Repository layout

```
D:/authority_chatbot/
├── fastapi_app.py            # main API entrypoint, 3 ask endpoints
├── settings.py               # Pydantic BaseSettings, all config
├── openai_clients.py         # singleton OpenAI clients
│
├── retriever.py              # FAISS search, dual-query, page expansion
├── answerer.py               # prompt construction, gpt-4o call, grounding
├── reranker.py               # hybrid rerank (semantic 0.65 + lexical 0.20 + authority 0.15)
├── grounding.py              # post-gen verification (Tier-1 regex + Tier-2 LLM-as-judge)
├── chunker.py                # role-aware chunking, page markers, 4500/500 chars
├── extractors.py             # PDF/DOCX extraction with section detection
│
├── index_store.py            # embedding, FAISS build, chunk storage
├── index_cache.py            # in-memory FAISS cache with blue/green pointer
├── index_manager.py          # SafeAutoIndexer, ACTIVE.json pointer
├── doc_registry.py           # doc scan + manifest comparison
├── citation_formatter.py     # reference rendering for the UI
│
├── query_router.py           # classifies DOCUMENT vs STRUCTURED vs HYBRID
├── query_intent.py           # high-level routing helper
├── pera_vocab.py             # canonical PERA vocabulary
├── context_state.py          # per-session conversational state
├── session_store.py          # redis/sqlite/memory backed sessions
│
├── inspection_lookup.py      # inspection intent + executor (live + stored)
├── inspection_ingest.py      # SDEO inspection ingest into Postgres
├── challan_lookup.py         # challan intent + executor
├── challan_ingest.py         # challan ingest from PERA challan API
├── challan_sync.py           # background sync scheduler
├── operational_activity_lookup.py
├── operational_activity_ingest.py
├── requisition_ingest.py     # requisition ingest module
├── live_api_handler.py       # generic Live-API routing path
│
├── analytics_db.py           # PG connection pool
├── analytics_models.py       # ORM-style row builders
├── analytics_mapping.py      # API record → curated table mapping
├── analytics_migrations.py   # versioned migrations
├── analytics_store.py        # write-through aggregation
│
├── api_admin_routes.py       # /api/admin/* — index ops, document upload
├── admin_routes.py           # dashboard data routes
├── admin_auth.py             # admin token issuance
├── api_auth.py               # API key validation
├── auth.py                   # shared JWT helpers
│
├── api_db.py                 # SQLite ingestion DB
├── api_registry.py           # API source registry
├── api_discovery.py          # auto-discovers config files
├── api_fetcher.py            # http GET wrapper
├── api_normalizer.py         # JSON → flat record
├── api_flattener.py          # nested → tabular
├── api_chunker.py            # API records → embeddable chunks
├── api_diff.py               # snapshot diff
├── api_snapshot_store.py     # historical snapshots
├── api_sync_manager.py       # orchestrates fetch → diff → store
├── api_scheduler.py          # background poll loop
├── api_lookup_registry.py    # registry for stored-API lookup
├── api_record_builder.py     # builds canonical record dicts
├── api_health.py             # /health endpoint helpers
├── api_source_utils.py
├── api_validators.py
├── api_config_models.py      # Pydantic models for YAML configs
│
├── freshness_scheduler.py    # keeps inspection / officer / OA tables fresh
├── freshness_helper.py       # tier classification + footer formatting
├── audit_trail.py            # request-level audit log
├── log_config.py             # central logger
├── check_index.py            # health check for FAISS index
│
├── frontend/                 # Next.js premium UI (admin + chat)
│   ├── package.json
│   ├── next.config.js
│   └── src/app/
│       ├── page.tsx          # chat home
│       ├── login/page.tsx
│       ├── admin/page.tsx
│       ├── components/
│       │   ├── chat/         # ChatWindow, MessageBubble, Composer, ThinkingIndicator, WelcomeScreen
│       │   ├── sidebar/      # session sidebar
│       │   ├── pdf/          # PDF viewer
│       │   └── common/
│       ├── hooks/            # useAutoResizeTextarea, useChatSessions, useHealthCheck, useThemePreference, useVoiceRecorder
│       └── lib/              # api.ts, adminApi.ts, types.ts, markdown.ts, storage.ts
│
├── assets/
│   ├── data/                 # PDFs/DOCX (junctioned to Pera_staff_chatbot/assets/data)
│   ├── apis/                 # YAML API configs
│   ├── index/                # legacy chunks.jsonl + faiss.index
│   ├── indexes/              # versioned builds + ACTIVE.json
│   └── chats/                # session storage (sqlite mode)
│
├── data/                     # api_ingestion.db, sessions.db
├── audit_logs/               # per-request JSONL
├── tests/                    # pytest suites
├── dev/                      # one-off scripts
└── legacy/                   # archived Streamlit app + retriever
```

The `Pera_staff_chatbot/` subdirectory holds the older, simpler version
of the project; the active code lives at the repo root and the data
folder is shared via a Windows junction.

## 4. Backend — module by module

### 4.1 Entrypoint — `fastapi_app.py`

A 1 280-line FastAPI app exposing three ask endpoints (legacy `/ask`,
`/ask_json`, modern `/api/ask`) plus admin and document mounts. It:

- Loads `assets/data` as a static mount.
- Boots the `SafeAutoIndexer` background thread (configurable poll
  interval — default high to avoid retry loops on quota errors).
- Boots the `ApiScheduler` (sync loop for stored-API sources).
- Boots the `ChallanScheduler` (fast 5 s + full 60 s + daily 6 h).
- Boots the `FreshnessScheduler` (inspection summary every 2 h,
  officer breakdown every 3 h, OA summary every 2 h, OA tehsil every
  6 h).
- Mounts `admin_router` and `api_admin_router`.
- Emits `Deprecation` HTTP headers on the legacy `/ask` and `/ask_json`
  routes pointing at `/api/ask`.
- Adds a `RequestIDMiddleware` so every response carries `X-Request-ID`
  and the audit log threads them.

### 4.2 Settings — `settings.py`

Pydantic `BaseSettings`. Loads from environment first, then `.env`.
Fields include: OpenAI key/model, FAISS settings (`INDEX_POLL_SECONDS`,
`INDEXES_ROOT`, `INDEX_POINTER_PATH`), retriever thresholds
(`RETRIEVER_SIM_THRESHOLD`), PostgreSQL URL, Redis URL,
`ANALYTICS_DB_ENABLED`, chunk sizes (`CHUNK_MAX_CHARS=4500`,
`CHUNK_OVERLAP_CHARS=350`), live-API toggles, freshness scheduler
intervals, deprecation flags.

### 4.3 RAG pipeline core

| Module | Role |
|---|---|
| `extractors.py` | PDF/DOCX text extraction with section detection. Produces page-marked text. |
| `chunker.py` | Splits extracted text into 4 500-char chunks with 350-char overlap and per-page anchors. |
| `index_store.py` | Embeds chunks via `text-embedding-3-small`, builds FAISS, writes `chunks.jsonl` + `faiss.index` + `manifest.json`. |
| `index_manager.py` | `SafeAutoIndexer`: blue/green build directories, atomic `ACTIVE.json` pointer swap, validation gate. Background thread polls `assets/data` for new/changed PDFs. |
| `index_cache.py` | In-memory cache of the active FAISS index. Reload on pointer change. |
| `doc_registry.py` | Scans `assets/data`, hashes files, compares against manifest. |
| `retriever.py` | FAISS search. Supports dual-query (verbatim + rewritten), `TOP_K=30`, page expansion radius (configurable, default 2), keyword fallback (capped weight 0.40-0.50), LRU embedding cache (256 entries). |
| `reranker.py` | Hybrid rerank: 0.65 semantic + 0.20 lexical + 0.15 authority. Per-doc cap to prevent one PDF dominating. |
| `answerer.py` | Constructs the gpt-4o prompt. Includes 18 numbered rules covering grounding, salary linkage, position-title boundaries, audit-table preservation, refusal phrasing. Calls LLM, optionally refines. |
| `grounding.py` | Tier-1 regex check + Tier-2 LLM-as-judge using gpt-4o-mini. Classifies as Supported / Partially Supported / Unsupported. |
| `citation_formatter.py` | Builds the reference list and evidence-citation array consumed by the frontend. |

### 4.4 Query routing layer

| Module | Role |
|---|---|
| `query_router.py` | `classify_query(question) → DOCUMENT / STRUCTURED / HYBRID`. Signal-counting over two regex sets. |
| `query_intent.py` | High-level dispatcher between document RAG and structured lookups. |
| `pera_vocab.py` | Canonical PERA vocabulary (synonyms, abbreviations) used by the rewriter. |
| `inspection_lookup.detect_inspection_intent` | Encodes intent strings: `insp_summary`, `insp_division:<n>`, `insp_district:<n>`, `insp_tehsil:<n>`, `insp_officer:<n>`, `insp_cnic:<n>`, `insp_repeat_offenders`, `insp_top:<metric>:<level>:<order>`. |
| `challan_lookup.detect_challan_intent` | Encodes: `challan_totals`, `challan_by_division`, `challan_by_district`, `challan_by_tehsil`, `challan_location:<level>:<n>`, `challan_daterange:<intent>:<start>:<end>`, etc. |
| `operational_activity_lookup.detect_operational_activity_intent` | Encodes: `oa_summary`, `oa_division:<n>`, `oa_district:<n>`, `oa_tehsil:<n>`, etc. |

### 4.5 Stored-API ingestion (the "snapshot stack")

A pluggable pipeline that ingests external HTTP APIs into PostgreSQL on
a schedule, computes diffs, and exposes them as evidence to the LLM.
Each API has a YAML config in `assets/apis/`.

| Module | Role |
|---|---|
| `api_registry.py` | Loads YAML configs into `APISourceConfig` objects. |
| `api_discovery.py` | Auto-detects new YAML files; reconciles with PG `api_source_configs_pg`. |
| `api_fetcher.py` | `requests` GET wrapper with timeout, retry, header injection. |
| `api_normalizer.py` | JSON → flat record. Pulls fields per the YAML "fields" map. |
| `api_flattener.py` | Optional second pass for nested arrays. |
| `api_diff.py` | Compares this snapshot against the previous; tags Added / Changed / Unchanged / Deleted. |
| `api_snapshot_store.py` | Persists each `sync_run` and per-record snapshot to PG. |
| `api_chunker.py` | Converts changed records to embeddable chunks for the FAISS index. |
| `api_sync_manager.py` | Orchestrator. Calls fetcher → normalizer → diff → snapshot → write-through → chunker. |
| `api_scheduler.py` | Background poll loop. Default 300 s. |
| `api_lookup_registry.py` | Registry for direct SQL lookups (bypasses FAISS for known structured queries). |
| `analytics_mapping.py` | Maps an API record into curated tables (e.g. `app_data_divisions` → `dim_division`). |

### 4.6 Domain-specific lookups

#### Inspection (`inspection_lookup.py`, 2 400 LoC)

- Intent detector with location, officer, CNIC, ranking, repeat-offender,
  date-range branches.
- Live SDEO calls: `inspections-summary`, `top-kpis`,
  `challan-status-breakdown`, `Pcm/dashboard-counts`,
  `Pcm/officer-inspections`.
- Aggregator that fans out to every tehsil under a district or
  division when the user supplies a date range, sums the responses,
  collapses same-officer rows, and renders a per-tehsil audit table
  that the answerer must reproduce verbatim.
- Ranking handler `_query_top_locations` that ranks by any of:
  `firs, sealed, challans, warnings, no_offenses, total_actions,
  paid_challans, unpaid_challans, overdue_challans, paid_amount,
  unpaid_amount, overdue_amount, fine_amount`, at any of:
  `tehsil, district, division`, in either order (`asc` / `desc`).
  Pulls from `challan_tehsil_breakdown` for count-based payment
  rankings and from `challan_data` for amount-based rankings.

#### Challan (`challan_lookup.py`, 2 915 LoC)

- Intent detector with division, district, tehsil, officer,
  date-range, requisition-type, status filters.
- Date-range parser handles `from X to Y`, `between X and Y`,
  Roman-Urdu word order (`X se Y tak`), single-date "same day",
  yearless dates, relative phrases (`last week`, `last 7 days`,
  `this month`, `last N months`).
- SQL queries against `challan_totals`, `challan_by_division`,
  `challan_by_district`, `challan_by_tehsil`, `challan_tehsil_drill`,
  `challan_data` for status breakdowns.
- Spelling normaliser for Pakistani city / tehsil name variants.

#### Operational Activity (`operational_activity_lookup.py`, 1 545 LoC)

- Mirrors the inspection structure for OA data.
- Sources: `operational_activity`, `operational_activity_detail`,
  `requisition_detail`, `requisition_member`.

### 4.7 Schedulers

| Scheduler | Cadence | Purpose |
|---|---|---|
| `SafeAutoIndexer` | configurable (default 86 400 s while quota out) | Blue/green FAISS rebuild |
| `ApiScheduler` | 300 s | Refresh stored-API snapshots |
| `ChallanScheduler` | fast 5 s + full 60 s + daily 6 h | Pull challan API into PG |
| `FreshnessScheduler` | insp summary 7 200 s, insp officer 10 800 s, OA summary 7 200 s, OA tehsil 21 600 s | Keep date-stamped tables current |

All run on daemon threads; the FastAPI process is the single orchestrator.

### 4.8 Auth + Admin

- `auth.py` issues short-lived JWT access tokens.
- `admin_auth.py` validates the admin role.
- `api_auth.py` validates API keys for service-to-service calls.
- `admin_routes.py` (mounted unconditionally) exposes:
  `/api/admin/login`, `/api/admin/documents` (CRUD), `/api/admin/index-status`,
  document upload (multipart), document deletion.
- `api_admin_routes.py` exposes index-management routes.
- `audit_trail.py` writes per-request JSONL (`audit_logs/`) with
  request-id, route, status, timing.

## 5. Frontend — Next.js 14 app router

`frontend/src/app/` lays out:

| Route | File | Purpose |
|---|---|---|
| `/` | `page.tsx` | Chat surface |
| `/login` | `login/page.tsx` | Sign-in flow + sign-out confirm |
| `/admin` | `admin/page.tsx` | Dashboard — index status, freshness, document upload, deletion |

Components (`components/chat/`):
- `ChatWindow.tsx` — message list with auto-scroll
- `MessageBubble.tsx` — markdown rendering, evidence panel (DOC/API badge, page range, score, snippet), confidence badge
- `Composer.tsx` — input box, voice record, send
- `ThinkingIndicator.tsx` — animated dots
- `WelcomeScreen.tsx` — landing card

Hooks:
- `useChatSessions` — local-storage persistence, multi-session
- `useVoiceRecorder` — MediaRecorder → POST to `/transcribe`
- `useHealthCheck` — periodic backend ping
- `useThemePreference` — light/dark
- `useAutoResizeTextarea`

Library helpers (`lib/`):
- `api.ts` — chat client (`/api/ask`)
- `adminApi.ts` — admin endpoints
- `types.ts` — shared TS types (Message, EvidenceCitation, etc.)
- `markdown.ts` — markdown-to-HTML
- `storage.ts` — localStorage wrappers

Frontend reads `NEXT_PUBLIC_API_URL` (set in `.env.local`) to point
at the FastAPI host.

## 6. Data sources

### 6.1 PostgreSQL (`pera_ai` database)

37 tables across three groups:

**Dimensions** — `dim_date`, `dim_division`, `dim_district`, `dim_tehsil`, `dim_officer`.

**Snapshot tables** —
- `inspection_performance` (155 tehsils × snapshots) + `inspection_performance_detail`
- `inspection_officer_summary`, `officer_inspection_record`, `officer_inspection_detail`
- `operational_activity`, `operational_activity_detail`
- `requisition_detail`, `requisition_member`
- `challan_data` (row-level), `challan_list`, `challan_totals`,
  `challan_by_division`, `challan_by_district`, `challan_by_tehsil`,
  `challan_tehsil_drill`, `challan_tehsil_breakdown`,
  `challan_requisition_type`
- `fact_challan_status_summary`, `fact_finance_*`,
  `fact_inspection_performance_*`, `fact_inspection_events`,
  `fact_workforce_strength`

**Plumbing** — `analytics_schema_migrations`, `api_records_pg`,
`api_source_configs_pg`, `api_sync_runs_pg`.

### 6.2 SQLite

- `data/api_ingestion.db` — raw API snapshots before they're promoted
  to PG (challans, OA).
- `data/sessions.db` — chat-session backup when Redis isn't reachable.

### 6.3 FAISS

- `assets/indexes/build_<timestamp>/{chunks.jsonl, faiss.index, manifest.json}`
- `assets/indexes/ACTIVE.json` — atomic pointer to the live build
- `assets/index/{chunks.jsonl, faiss.index, manifest.json}` — legacy
  fallback path

### 6.4 Documents

`assets/data/` (junctioned) — 19 PDFs:
- The Punjab Enforcement and Regulation Bill 2024
- Annex G–P (Employees SRs, Contractual SRs, Performance Appraisal,
  Squads & Weapons, Medical, Gratuity, Enforcement Cost Formula)
- Compiled Working Papers
- Flag-C: Operations & Procedures Code

### 6.5 Live SDEO API

Base: `https://pera360.punjab.gov.pk/backend/api`. ~153 endpoints
catalogued in Swagger. Currently consumed:

- `/sdeo-dashboard/inspections-summary` — counts (inspections, challans, FIRs, sealed, warnings, removal-orders, EPO, no-offenses) + per-officer breakdown for a tehsilId + date range.
- `/sdeo-dashboard/top-kpis` — `totalFineImposed`, `totalFineRecovered`, `unpaidFineAmount`.
- `/sdeo-dashboard/challan-status-breakdown` — `paidCount`, `unpaidCount`, paid/unpaid amounts.
- `/Pcm/dashboard-counts` — all-time arrest, PCM, sealed, warnings (date params accepted but ignored).
- `/Pcm/officer-inspections` — per-officer per-record details (no actionDate field).

## 7. End-to-end request flow

```
User types message in browser
        │
        ▼
POST /api/ask  { question, mode: "documents" | "stored_api" | "both" | "live_api", session_id }
        │
        ▼
[ Pre-process ] typo-fix, Roman-Urdu fold, strip controls, NFC normalise
        │
        ▼
[ Smalltalk gate ] hello/thanks/bye → canned reply, exit
        │
        ▼
[ Session lookup ] restore last_intent, last_entity, last_date_range
        │
        ▼
[ Query rewrite (gpt-4o-mini) ] expand abbrevs, expand synonyms
        │
        ▼
[ Query router ] DOCUMENT | STRUCTURED | HYBRID
        │
        ▼
[ Domain detectors run in parallel ]
        │   inspection_lookup.detect_inspection_intent → e.g. insp_top:fine_amount:tehsil:desc
        │   challan_lookup.detect_challan_intent       → e.g. challan_location:district:Lahore
        │   operational_activity_lookup.detect_*       → e.g. oa_summary
        │
        ▼
[ Stored API lookup wins if a high-confidence intent fires ]
        │   else → FAISS retrieval (TOP_K=30, dual-query, page expand)
        │
        ▼
[ Reranker ] hybrid score, per-doc cap, salary-bridge filter
        │
        ▼
[ Evidence assembly ] per-tehsil audit tables, ranking tables,
                       formatted_context with section banners
        │
        ▼
[ Answerer (gpt-4o) ] system prompt = 18 grounding rules + rule 0
                      forcing audit-table preservation
        │
        ▼
[ Grounding verifier ] Tier-1 regex (does each number appear in ctx?)
                       Tier-2 LLM-as-judge (gpt-4o-mini classifier)
                       state ∈ {Supported, Partial, Unsupported}
        │
        ▼
[ Response ] {
   answer,
   support_state,
   confidence (0..1),
   references [{document, page, snippet, source_type, ...}],
   evidence_citations [{ chunk_id, page, score, snippet }],
   request_id
}
        │
        ▼
Frontend renders: markdown answer + verified-response badge + sources panel
```

## 8. Aggregation flow (date-ranged district/division)

```
User: "inspections summary lahore district from 1 jan to 10 jan 2026"
        │
        ▼
detect_inspection_intent → insp_district:Lahore
        │
        ▼
_extract_date_range → (2026-01-01, 2026-01-10)
        │
        ▼
_query_district_live(db, "Lahore", start, end)
        │
        ▼
SQL: SELECT t.tehsil_id FROM dim_tehsil t JOIN dim_district d
     WHERE d.district_name='Lahore' AND t.is_active = TRUE
        │   → 11 tehsil rows
        │
        ▼
ThreadPoolExecutor(8):
   for each tehsil:
     fetch /inspections-summary  (counts + officers)
     fetch /top-kpis             (fine totals)
     fetch /challan-status-breakdown (paid/unpaid)
     fetch /Pcm/dashboard-counts  (all-time arrest + PCM)
        │
        ▼
_aggregate_tehsil_metrics:
   sum totalActions, challans, FIRs, warnings, sealed, …
   merge same officers across tehsils
   track tehsils_covered / tehsils_failed
        │
        ▼
_format_aggregate_context:
   - banner: "Aggregation: sum of 11/11 tehsils"
   - summary: total inspections / challans / fines
   - "Per-Tehsil Breakdown (REQUIRED in answer)" with
     audit-table flag the system-prompt rule 0 enforces
   - officer table (top 25)
        │
        ▼
LLM renders answer with table verbatim
```

## 9. Auth + admin

JWT-based:
- User logs in at `/login` → `POST /api/admin/login {username, password}` →
  returns `access_token`.
- Subsequent admin calls send `Authorization: Bearer <token>`.
- Admin can: list documents, upload PDFs, delete documents,
  trigger an index rebuild, view freshness widgets, see live counts
  of indexed/pending documents.
- All admin actions are logged to `audit_logs/`.

## 10. Deployment

Currently runs locally:

```
backend  : cd D:/authority_chatbot && \
           D:/authority_chatbot/.venv/Scripts/python.exe -m uvicorn fastapi_app:app \
           --host 0.0.0.0 --port 8000

frontend : cd D:/authority_chatbot/frontend && npm run dev
           → http://localhost:3000
           → http://10.x.x.x:3000   (LAN access via NEXT_PUBLIC_API_URL)
```

`.env` lives at the repo root and contains all secrets (OpenAI key,
Postgres URL, Redis URL, admin credentials, deprecation flags). The
`assets/data` directory is a Windows junction back to
`Pera_staff_chatbot/assets/data` so the older corpus is shared with
the new tree.

## 11. Test suite

Two pytest files, **505 deterministic cases + 10 live-gated**:

| File | Cases | Focus |
|---|---|---|
| `tests/test_inspection_aggregation.py` | 250 | inspection routing, aggregation arithmetic, hierarchy disambiguation, Roman-Urdu, date-format extraction, officer detection, live SDEO smoke (gated by `RUN_LIVE_AGG=1`) |
| `tests/test_chatbot_quality.py` | 255 | 17 categories: positive simple/complex, Roman-Urdu, typos, out-of-scope, misleading, very short, empty, stress long, date formats, officer, fallback, hierarchy, cross-domain, query router, casing, partial |

Last full run: **436 passed, 32 xfail (tracked gaps), 10 skipped (live),
27 xpass, 0 unexpected failures, ~9 s**.

Old phase tests (131) and migration / API integration tests (14) all
pass alongside.

## 12. Capabilities the chatbot has today

- Answer policy questions from the PERA Acts and annexures, with page
  citations and a confidence ribbon.
- Compute live inspection performance for any tehsil over any date
  range and match the SDEO dashboard tile-for-tile.
- Aggregate the same up to district and division level via parallel
  per-tehsil fan-out (zero divergence from the source-of-truth
  endpoint values).
- Rank tehsils, districts, or divisions by any of nine count metrics
  (`inspections, challans, FIRs, sealed, warnings, no_offenses,
  paid_challans, unpaid_challans, overdue_challans`) or four amount
  metrics (`paid_amount, unpaid_amount, overdue_amount, fine_amount`),
  ascending or descending.
- Per-officer queries with date filtering via the live PCM endpoint.
- Roman-Urdu phrasing (`sab sy ziada`, `kitne`, `mein`, `ki`, `konsa`,
  `kaunsa`, `jo pay`, `baki`).
- Typo tolerance for the most common spellings (`chaallans`,
  `summery`, `summmery`, `inspecton`, `chalans`, `chellan`, etc.).
- Followup queries that drop the explicit metric (using session
  context for `last_lookup_type`, partial — not yet a full structured
  session field).
- Audit-trail tables forced into every aggregate or ranking answer so
  the consumer can verify totals against the dashboard.
- Document upload + deletion through the admin UI; the index rebuilds
  automatically (when OpenAI quota allows).
- Voice input via Whisper.
- Per-request audit log written to `audit_logs/`.
- Multi-session chat history kept in localStorage with optional
  Redis persistence.

## 13. Known limitations

These all show up as `xfail` in the test suite or as open items in
[CHATBOT_SYSTEM_AUDIT.md](CHATBOT_SYSTEM_AUDIT.md):

1. Date parser misses several formats (`DD-MM-YYYY`, `DD/MM/YYYY`,
   yearless month-day pairs, hyphen-separated ranges, etc.).
2. Intent detector cannot route on `performance` alone; needs
   `inspection performance` together.
3. Officer + outcome without `inspection` keyword goes to officer
   when it should go to inspection.
4. Dotted division names (`D.G. Khan`) not auto-normalised.
5. Officer-name detector still has substring collisions with multi-
   word tehsil names (`Allama Iqbal Town` vs officer `M. Iqbal`).
6. PCM dashboard-counts endpoint does not respect date filters.
   Arrest and PCM are reported as "(all-time)".
7. Hierarchy lookup runs on every request (no in-process cache).
8. Per-tehsil API responses not memoised.
9. No circuit-breaker on aggregate fan-out failures.
10. No numerical post-processor between the LLM and the user, so a
    rare hallucinated number can still slip through.
11. No structured follow-up state — second turn of a conversation
    does not inherit the first turn's metric / qualifier.
12. Daily snapshot columns for paid / unpaid / fine_imposed / arrest
    / PCM not yet materialised on `inspection_performance`, so non-
    date-ranged queries that include them still need a live API hit.
13. OpenAI quota / billing is single-tenant; no failover key.

The roadmap to close all of these is in
[CHATBOT_SYSTEM_AUDIT.md §5](CHATBOT_SYSTEM_AUDIT.md) and
[CHATBOT_QUALITY_REPORT.md §4](CHATBOT_QUALITY_REPORT.md).
