# PERA AI System — Complete Technical Documentation

**Project:** PERA AI Assistant (Staff Chatbot)  
**Organization:** Punjab Enforcement and Regulatory Authority (PERA), Government of Punjab  
**Lead AI Developer:** Muhammad Ahsan Sajjad  
**Document Date:** February 17, 2026  

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture Overview](#2-system-architecture-overview)
3. [Technology Stack](#3-technology-stack)
4. [Data Pipeline (Ingestion → Indexing)](#4-data-pipeline-ingestion--indexing)
5. [Query Pipeline (Question → Answer)](#5-query-pipeline-question--answer)
6. [Backend API Layer](#6-backend-api-layer)
7. [Frontend Application](#7-frontend-application)
8. [File-by-File Inventory](#8-file-by-file-inventory)
9. [Configuration & Environment Variables](#9-configuration--environment-variables)
10. [Data Assets & Document Corpus](#10-data-assets--document-corpus)
11. [Blue/Green Index Management](#11-bluegreen-index-management)
12. [Design Decisions & Rationale](#12-design-decisions--rationale)
13. [Deployment & Running](#13-deployment--running)

---

## 1. Executive Summary

PERA AI is a **Retrieval-Augmented Generation (RAG)** chatbot built for the Punjab Enforcement and Regulatory Authority. It allows PERA staff — from enforcement officers to the Director General — to ask questions in **English, Urdu, or Roman Urdu** about PERA's official documents (Acts, Service Rules, HR Manuals, Regulations, FAQs, etc.) and receive accurate, citation-backed answers.

### What It Does

- **Ingests** 20 official PERA PDF documents (Acts, Service Rules, Annexures, Regulations, FAQs, Working Papers)
- **Extracts** text from PDFs page-by-page with table preservation, header/footer removal, and hyphenation repair
- **Chunks** extracted text into semantically meaningful blocks respecting page boundaries, headings, tables, and role contexts
- **Embeds** each chunk using OpenAI's `text-embedding-3-small` model and stores vectors in a FAISS index
- **Retrieves** the most relevant chunks for any user question using semantic search + keyword fallback + smart page expansion
- **Rewrites** follow-up and Roman Urdu queries using GPT-4o-mini for better retrieval
- **Generates** answers using GPT-4o-mini, strictly grounded in retrieved evidence with no hallucination
- **Serves** answers through a FastAPI REST backend and a modern Next.js frontend with dark mode, voice input, PDF viewer, and chat history

### Key Capabilities

| Capability | Details |
|---|---|
| **Tri-lingual** | English, Urdu (script), Roman Urdu |
| **Voice Input** | Browser microphone → OpenAI Whisper transcription |
| **Inline PDF Viewer** | Click any citation to view the source PDF page |
| **Chat History** | Persistent per-session with localStorage |
| **Auto-Indexing** | Blue/green builds — hot-swap indexes with zero downtime |
| **Smart Context Expansion** | Salary/detail queries fetch ±3 adjacent pages for complete tables |
| **Creator Identity** | Hardcoded intercept for "who made you" questions |
| **Smalltalk Handling** | Deterministic greeting/smalltalk detection (no LLM cost) |

---

## 2. System Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                        USER (Browser)                        │
│                                                              │
│   ┌──────────────────────────────────────────────────────┐   │
│   │           Next.js Frontend (port 3001)                │   │
│   │  • Chat UI (dark/light mode)                          │   │
│   │  • Voice recording (MediaRecorder API)                │   │
│   │  • Inline PDF viewer (iframe)                         │   │
│   │  • Chat history (localStorage)                        │   │
│   └──────────────┬──────────────────┬────────────────────┘   │
│                  │                  │                         │
│        POST /api/ask        POST /transcribe                 │
│        GET /pdf/{name}                                       │
└─────────────────┬──────────────────┬─────────────────────────┘
                  │                  │
                  ▼                  ▼
┌──────────────────────────────────────────────────────────────┐
│              FastAPI Backend (port 8000)                      │
│              fastapi_app.py                                   │
│                                                              │
│  ┌────────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │  /api/ask       │  │ /transcribe  │  │ /pdf/{name}     │  │
│  │  /ask (HTML)    │  │ speech.py    │  │ Static files    │  │
│  │  /ask_json      │  │ (Whisper)    │  │ /assets/data/   │  │
│  └───────┬────────┘  └──────────────┘  └─────────────────┘  │
│          │                                                    │
│          ▼                                                    │
│  ┌─────────────────────────────────────────────────────┐     │
│  │            retriever.py                              │     │
│  │  1. Expand abbreviations (CTO→Chief Tech Officer)   │     │
│  │  2. Embed query (OpenAI text-embedding-3-small)     │     │
│  │  3. FAISS similarity search (top-K)                 │     │
│  │  4. Smart page expansion (±3 for salary/tables)     │     │
│  │  5. Keyword fallback (Urdu + English tokenization)  │     │
│  │  6. Deduplicate and score                           │     │
│  └───────┬─────────────────────────────────────────────┘     │
│          │                                                    │
│          ▼                                                    │
│  ┌─────────────────────────────────────────────────────┐     │
│  │            answerer.py                               │     │
│  │  1. Format evidence with XML tags                   │     │
│  │  2. Build system prompt (PERA persona + rules)      │     │
│  │  3. Call GPT-4o-mini with evidence + history        │     │
│  │  4. Strip inline citations (UI shows them)          │     │
│  │  5. Return answer + references                      │     │
│  └─────────────────────────────────────────────────────┘     │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐     │
│  │         Background: SafeAutoIndexer                  │     │
│  │  (index_manager.py → index_store.py)                │     │
│  │  • Polls assets/data every 30s for new/changed PDFs │     │
│  │  • Builds new index in fresh directory (green)      │     │
│  │  • Validates → switches ACTIVE.json pointer         │     │
│  │  • Cleans up old builds (keep last 3)               │     │
│  └─────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                    FAISS Index (Disk)                         │
│  assets/indexes/build_YYYYMMDD_HHMMSS/                       │
│    ├── faiss.index    (IndexIDMap2 + IndexFlatIP)             │
│    ├── chunks.jsonl   (chunk metadata + text + embeddings)    │
│    └── manifest.json  (file hashes for change detection)      │
│                                                              │
│  assets/indexes/ACTIVE.json → points to current build dir    │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                  Document Corpus                              │
│  assets/data/                                                 │
│    ├── PERA Act 2024                                          │
│    ├── Service Rules (Employees, Contractual)                 │
│    ├── HR Manual, Medical Policy, Gratuity                    │
│    ├── Uniform & Weapons Regulations                          │
│    ├── E&D Regulations, O&Ps Code                             │
│    ├── Working Paper, FAQs, Notifications                     │
│    └── ... (20 PDFs total, ~75MB)                             │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. Technology Stack

### Backend (Python)

| Component | Technology | Purpose |
|---|---|---|
| **Web Framework** | FastAPI + Uvicorn | REST API server (async, ASGI) |
| **Vector Store** | FAISS (faiss-cpu) | Similarity search over chunk embeddings |
| **Embeddings** | OpenAI `text-embedding-3-small` | Convert text chunks to 1536-dim vectors |
| **LLM** | OpenAI `gpt-4o-mini` | Answer generation + query rewriting |
| **Speech** | OpenAI Whisper (`whisper-1`) | Voice-to-text transcription |
| **PDF Parsing** | PyPDF (`pypdf`) | Extract text from PDF documents |
| **Data Validation** | Pydantic | Request/response schemas |
| **Numerical** | NumPy | Vector operations and normalization |
| **Environment** | python-dotenv | Configuration via `.env` file |
| **Legacy UI** | Streamlit | Original chatbot UI (still functional) |

### Frontend (TypeScript/React)

| Component | Technology | Purpose |
|---|---|---|
| **Framework** | Next.js 14.2.5 | React SSR/CSR framework |
| **Language** | TypeScript | Type-safe frontend code |
| **Styling** | Tailwind CSS 3.4 | Utility-first CSS framework |
| **State** | React useState/useEffect | Component-level state management |
| **Storage** | localStorage | Chat history persistence |
| **Voice** | MediaRecorder API | Browser-native audio recording |
| **PDF Display** | iframe | Inline PDF viewing with page anchors |

---

## 4. Data Pipeline (Ingestion → Indexing)

The data pipeline transforms raw PDF documents into searchable vector embeddings. It runs automatically in the background.

### Stage 1: Document Discovery (`doc_registry.py`)

```
assets/data/ → scan_assets_data() → List of PDF metadata
```

- Scans `assets/data/` directory for PDF files
- Extracts metadata: filename, file size, modification time, SHA-256 hash
- Assigns priority rank from filename pattern (e.g., `book1`, `book2` → higher number = higher priority)
- Compares against stored manifest to detect new, changed, or removed files
- Only processes documents that have actually changed (incremental)

### Stage 2: Text Extraction (`extractors.py`)

```
PDF file → extract_pdf_units() → List[ExtractedUnit]
```

Each `ExtractedUnit` represents one page of a PDF and contains:
- `doc_name`: filename of the source document
- `source_type`: "pdf"
- `loc_kind`: "page"
- `loc_start` / `loc_end`: page number
- `text`: cleaned, extracted text
- `path`: public URL path for serving

**Extraction Quality Features:**

| Feature | What It Does |
|---|---|
| **Header/Footer Removal** | Detects repeated first/last lines across pages (e.g., "Page X of Y") and removes them |
| **Table Preservation** | Detects column-aligned text (multiple spaces/tabs) and converts to pipe-delimited format (`col1 \| col2 \| col3`) |
| **Hyphenation Repair** | Joins lines like `"regula-" + "tory"` → `"regulatory"` |
| **Paragraph Merging** | Joins narrative lines into paragraphs while keeping bullets/tables as separate lines |
| **Junk Filtering** | Skips pages that are only whitespace, numbers, or punctuation |

The system also supports DOCX extraction (`extract_docx_units()`) which splits by heading styles, though the current corpus is PDF-only.

### Stage 3: Chunking (`chunker.py`)

```
List[ExtractedUnit] → chunk_units() → List[Chunk]
```

Each `Chunk` is a fixed-budget text segment with full traceability back to its source:
- Maximum `4000` characters per chunk (configurable via `CHUNK_MAX_CHARS`)
- `200` character overlap between adjacent chunks (configurable via `CHUNK_OVERLAP_CHARS`)
- Never mixes different pages (PDF) or different sections (DOCX)

**Intelligent Splitting:**

| Feature | What It Does |
|---|---|
| **Heading Detection** | Recognizes Schedule I, Annexure A, Chapter 2, Section 12, Rule 3, Part II, etc. |
| **Role Heading Detection** | Identifies job title headings (e.g., "Director General", "Investigation Officer") and injects role context into chunks |
| **Structure Preservation** | Never splits mid-table or mid-list; tables and narrative are separated into different blocks |
| **All-Caps Headings** | Detects short uppercase headings like "POWERS", "DUTIES", "ELIGIBILITY" |
| **Overlap Trimming** | Overlap always starts at a clean word/sentence boundary |
| **Short Chunk Protection** | Force-keeps short chunks that answer common questions (salary, SPPP, allowance, powers, etc.) |
| **Global Cap** | Maximum 50,000 chunks per run to prevent memory exhaustion |

### Stage 4: Embedding & Indexing (`index_store.py`)

```
List[Chunk] → embed_texts() → FAISS IndexIDMap2
```

This is the core indexing engine:

**a) Text Preparation**

For each chunk, two text representations are built:

1. **`embed_text`** (for vector embedding): Combines entity identity line + document name + location label + search tags + raw chunk text. This enriched text improves embedding quality by giving the model context about what entity and document the chunk belongs to.

2. **`search_text`** (for keyword fallback): A lowercased, cleaned version with document/location prefixes for hybrid keyword matching.

**Entity Identity Line** (prepended to every embed_text):
```
ENTITY: PERA (Punjab Enforcement and Regulatory Authority, Punjab).
ALIASES: pira, perra, peera, peraa, pehra.
TOPICS: enforcement, regulation, scheduled laws, complaints, hearings, recruitment, HR, discipline, contracts.
```

**Search Tags:** Regex-derived topic tags (e.g., `salary`, `appointment`, `discipline`, `schedule`, `annexure`) are appended to `embed_text` to boost recall for domain-specific queries.

**b) OpenAI Embedding**

- Model: `text-embedding-3-small` (1536 dimensions)
- Batch size: 100 texts per API call (configurable)
- Character cap: 120,000 chars per batch
- Retry logic: 4 retries with exponential backoff
- L2 normalization of output vectors (for cosine similarity via inner product)
- Text truncation: max 8000 characters per text (OpenAI token limit safety)

**c) FAISS Index**

- Index type: `IndexIDMap2` wrapping `IndexFlatIP` (inner product = cosine similarity on normalized vectors)
- Each chunk gets a unique integer ID
- Supports incremental add/remove via `IDMap2`
- Stored as `faiss.index` on disk
- Chunk metadata stored in `chunks.jsonl` (one JSON line per chunk)

**d) Chunk JSONL Record**

Each chunk is stored as a JSON line in `chunks.jsonl` with these fields:

```json
{
  "id": 42,
  "doc_name": "PERA – Frequently Asked Questions (FAQs).pdf",
  "source_type": "pdf",
  "loc_kind": "page",
  "loc_start": 3,
  "loc_end": 3,
  "text": "...",
  "embed_text": "...",
  "search_text": "...",
  "doc_rank": 0,
  "path": "/assets/data/PERA – Frequently Asked Questions (FAQs).pdf",
  "public_path": "/assets/data/PERA%20%E2%80%93%20Frequently%20Asked%20Questions%20%28FAQs%29.pdf",
  "active": true,
  "embed_text_version": 4,
  "search_text_version": 3,
  "embed_model_version": 1,
  "sha256": "abc123...",
  "indexed_at": "2026-02-15T..."
}
```

**e) Low-Signal Filtering**

Chunks are skipped at ingestion if they are:
- Only "Page X of Y" text
- Only numbers and punctuation
- Fewer than 5 letters with no meaningful words

Exception: short chunks containing salary, allowance, SPPP, powers, or similar key terms are force-kept.

---

## 5. Query Pipeline (Question → Answer)

When a user sends a question, the following pipeline executes:

### Step 1: Query Rewriting (`retriever.py → rewrite_contextual_query()`)

If the user asked a follow-up question, the LLM rewrites it to be standalone:

```
User: "What are the DG's powers?"       → (no rewrite needed)
User: "And what about the CTO?"         → "What are the Chief Technology Officer's powers, functions, responsibilities?"
User: "uski salary kitni hai?"           → "What is the salary of the Chief Technology Officer?"
```

- Uses GPT-4o-mini with temperature 0.0 (deterministic)
- Expands abbreviations (CTO, DG, HR, etc.)
- Maps broad terms to PERA-specific phrasing
- Resolves pronouns from conversation history
- Preserves Urdu/Roman Urdu subject/object direction
- Skips for trivial messages ("ok", "thanks", "theek", "sahi")

### Step 2: Abbreviation Expansion (`retriever.py → _expand_abbreviations()`)

Known abbreviations are expanded inline before embedding:

| Abbreviation | Expansion |
|---|---|
| CTO | Chief Technology Officer |
| DG | Director General |
| EO | Enforcement Officer |
| IO | Investigation Officer |
| SSO | System Support Officer |
| HR | Human Resources |
| JD | Job Description |
| SR | Service Rules |
| TOR | Terms of Reference |
| SPPP | Special Pay Package PERA |
| Schedule-I | Organizational Structure |
| Schedule-II | Appointment & Conditions of Service |
| Schedule-III | Special Pay Package PERA (SPPP) |
| Schedule-IV | Rules / Regulations Adopted by the Authority |
| Schedule-V | Transfer and Posting |
| Schedule-VI | Special Allowance and Benefits |

### Step 3: Semantic Search (FAISS)

```
query → embed_texts() → qv (1536-dim vector)
qv → FAISS search(top_k=30) → [(score, chunk_id), ...]
```

- Query vector is L2-normalized to match index normalization
- Returns top 30 nearest neighbors with cosine similarity scores
- Minimum similarity threshold: 0.14 (configurable)
- Filters out stale/inactive chunk IDs

### Step 4: Smart Page Expansion

If the query contains salary/detail keywords (e.g., "salary", "tankhwah", "pay scale", "kitni"), the system fetches ±3 adjacent pages from the top 10 results. This ensures complete salary tables and schedules are included even if only one row matched.

**Trigger keywords:** salary, pay, scale, grade, allowance, benefits, detail, schedule, tankhwah, tankha, kitni, payscale, maaash, maash

### Step 5: Keyword Fallback (Hybrid Search)

A parallel keyword scan runs over all chunks to catch matches that semantic search missed:

- Tokenizes the query into meaningful words (removes stop words)
- Supports both English and Urdu Unicode ranges (`\u0600-\u06FF`)
- **Phrase matching:** full query phrase match in chunk text gives a boost
- **Token matching:** individual word matches scored proportionally
- Minimum 1 keyword match required (configurable)
- Keyword hits get a small semantic boost if they also have a FAISS score

### Step 6: Score Fusion & Deduplication

All hits (semantic + keyword + expanded neighbors) are merged:

- Each hit is mapped to its source document
- Hits within the same document are grouped
- Deduplication by (page, text[:200]) signature
- Documents sorted by maximum score descending
- Smart context chunks (from page expansion) are marked with `_is_smart_context: True`

### Step 7: Evidence Formatting (`answerer.py → format_evidence_for_llm()`)

Retrieved chunks are formatted into XML-tagged evidence blocks:

```xml
<evidence doc="PERA Act 2024" page="15">
The Director General shall exercise the following powers...
</evidence>
```

**Filtering applied:**
- Minimum top-doc score: 0.28 (skip weak documents)
- Minimum hit score: 0.26 (skip weak individual chunks)
- Maximum 6 documents and 15 hits per document
- Maximum 24,000 characters of total evidence
- **Subject-relevance sorting:** hits containing query subject words are prioritized to the top

### Step 8: LLM Answer Generation

The system calls GPT-4o-mini with:

- **System Prompt:** A strict persona prompt defining PERA AI Assistant behavior
- **Conversation History:** Last 4 exchanges for context
- **User Question**
- **Temperature:** 0.3 (mostly deterministic with slight creativity)

**Non-Negotiable Rules enforced in the prompt:**
1. Answer ONLY from provided Context (no external knowledge)
2. Do NOT invent facts (powers, procedures, numbers, dates)
3. Do NOT infer authority from seniority unless explicitly stated
4. Do NOT write references in the answer (UI shows them separately)
5. Treat "powers", "functions", and "duties" as synonyms unless Context distinguishes them
6. If exact answer not found: state what IS available, give 2-5 closest points, ask clarification questions
7. Reply in the same language as the user (English, Urdu, Roman Urdu)

### Step 9: Post-Processing

- **Reference stripping:** Removes any source/citation text the LLM may have included (UI shows references separately via structured data)
- **No-info detection:** If the answer contains phrases like "not found in the provided context", the response is marked as `decision: "refuse"`
- **Creator question intercept:** "Who made you?" → hardcoded response crediting Muhammad Ahsan Sajjad

---

## 6. Backend API Layer

### FastAPI Application (`fastapi_app.py`)

The main backend server providing 6 endpoints:

| Endpoint | Method | Purpose | Used By |
|---|---|---|---|
| `POST /api/ask` | POST | Primary chat endpoint (JSON) | Next.js frontend |
| `POST /ask` | POST | Legacy HTML-formatted endpoint | External integrations |
| `POST /ask_json` | POST | Structured JSON with grouped references | Mobile/API clients |
| `POST /transcribe` | POST | Audio file → text via Whisper | Voice input feature |
| `GET /pdf/{filename}` | GET | Serve PDF files with smart fuzzy matching | PDF viewer |
| `GET /download/{filename}` | GET | Force-download PDFs | Download button |
| `GET /assets/data/*` | GET | Static file serving for PDFs | Direct PDF access |

**Key Middleware:**
- CORS: allows all origins (`*`) for development/internal use
- Content-Security-Policy: `frame-ancestors *` to enable iframe PDF viewing

### `/api/ask` Endpoint (Primary)

```
Request:  { "question": "...", "conversation_history": [...] }
Response: { "answer": "...", "decision": "answer|refuse|error", "references": [...] }
```

Flow:
1. Extract last Q/A from conversation history
2. Rewrite query with LLM if follow-up detected
3. Call `retrieve()` with rewritten query
4. Call `answer_question()` with evidence + history
5. Return structured response

### `/pdf/{filename}` Endpoint

Smart PDF serving with 4-tier filename resolution:
1. **Exact match** (URL-decoded)
2. **Dash normalization** (en-dash → hyphen)
3. **Extension addition** (try adding `.pdf`)
4. **Fuzzy scan** (normalize all special chars and compare against directory listing)

This handles the common issue where PDF filenames contain en-dashes (–) but URLs use hyphens (-).

### Streamlit Application (`app.py`)

The original/legacy UI built with Streamlit:
- Two-column layout: Chat on left, PDF viewer on right
- Inline PDF rendering (one page at a time with prev/next navigation)
- Base64 PDF encoding for browser display
- Sidebar with PERA logo and clear button
- Bilingual greeting: "Assalam-o-Alaikum! PERA Act ke baare mein poochein."

---

## 7. Frontend Application

### Next.js App (`frontend/src/app/page.tsx`)

A single-page application with 710 lines of TypeScript/React providing a modern chat experience:

**UI Components:**

| Component | Description |
|---|---|
| **Chat Area** | Message bubbles with timestamps, copy button, and typing indicator |
| **Thinking Animation** | 4-phase animation: Scanning → Cross-referencing → Synthesizing → Composing |
| **Voice Input** | Hold-to-record button using MediaRecorder API, sends to `/transcribe` |
| **PDF Viewer Panel** | Right-side panel opens PDFs in iframe with page anchors |
| **Chat Sessions Sidebar** | List of previous chats with titles, stored in localStorage |
| **Dark/Light Mode** | System preference detection + toggle button |
| **Stats Badge** | Shows "20+ Documents", "1000+ Sections", "Instant Response" |
| **Capabilities List** | Voice Input, PDF Viewer, Citation Links, Dark Mode, Chat History, Instant Search |

**Message Rendering:**

The `renderBotContent()` function parses markdown-like formatting from LLM responses:
- Bold text (`**text**`)
- Bullet points (`- item` or `• item`)
- Numbered lists (`1. item`)
- Headings (`### heading`)
- Multi-level nested lists

**Chat History (`localStorage`):**
- Each session has: `id`, `title`, `messages[]`, `createdAt`
- Title auto-generated from first user message
- Sessions persist across browser refreshes
- Delete individual sessions

**Reference Cards:**

When the bot returns references, they are rendered as clickable cards showing:
- Document name
- Page number
- Click opens the PDF in the side panel via `/pdf/{filename}#page=N`

### Styling (`frontend/src/app/globals.css`)

16,385 bytes of custom CSS providing:
- Dark mode via media query and class toggle
- Glassmorphism effects on chat bubbles
- Smooth transitions and hover effects
- Responsive layout (sidebar collapses on mobile)
- Custom scrollbar styling
- Typing indicator animation (bouncing dots)
- Voice recording pulse animation

### Configuration

- `frontend/.env.local`: `NEXT_PUBLIC_API_URL=http://localhost:8000`
- `frontend/next.config.js`: Basic Next.js config
- `frontend/tailwind.config.js`: Tailwind CSS configuration
- `frontend/package.json`: Dependencies (Next.js 14.2.5, React 18.3.1, TypeScript 5)

---

## 8. File-by-File Inventory

### Core Production Files

| File | Size | Role |
|---|---|---|
| `fastapi_app.py` | 24 KB | Main FastAPI server — all REST endpoints, request handling, reference grouping, PDF serving |
| `retriever.py` | 16 KB | Semantic + hybrid search engine — FAISS queries, keyword fallback, page expansion, query rewriting |
| `answerer.py` | 15 KB | LLM answer generation — evidence formatting, GPT-4o-mini calls, reference extraction, citation stripping |
| `index_store.py` | 33 KB | Indexing engine — OpenAI embeddings, FAISS management, chunk ingestion, embed/search text builders |
| `extractors.py` | 17 KB | Document extraction — PDF page parsing, table detection, header/footer removal, DOCX support |
| `chunker.py` | 16 KB | Text chunking — heading-aware splitting, role context injection, overlap management |
| `index_manager.py` | 10 KB | Blue/green indexer — background polling, atomic pointer switching, build validation, cleanup |
| `doc_registry.py` | 6 KB | Document scanning — file discovery, SHA-256 manifests, change detection |
| `smalltalk_intent.py` | 8 KB | Greeting/smalltalk detector — tri-lingual pattern matching, greeting+question splitting |
| `speech.py` | 4 KB | Voice transcription — audio format detection, ffmpeg conversion, Whisper API calls |
| `reranker.py` | 1 KB | Lexical re-ranker — keyword overlap scoring, blended semantic+lexical ranking |
| `app.py` | 13 KB | Streamlit legacy UI — two-column chat+PDF viewer, session state management |
| `context_state.py` | 0 KB | Empty (deprecated — context management now handled inline) |

### Configuration Files

| File | Purpose |
|---|---|
| `.env` | Backend config: API keys, model names, retrieval thresholds, chunking params |
| `requirements.txt` | Python dependencies (openai, faiss-cpu, pypdf, fastapi, uvicorn, streamlit, etc.) |
| `.gitignore` | Git exclusion patterns |
| `README.md` | Basic project readme |

### Debug & Test Files (Development Only)

| File | Purpose |
|---|---|
| `check_openai.py` | Quick test of OpenAI API connectivity |
| `debug_creator_query.py` | Test creator question detection |
| `debug_extraction.py` | Test PDF text extraction |
| `debug_hang.py` | Diagnose server hangs |
| `debug_infra_salary.py` | Test salary query retrieval |
| `debug_load.py` | Test index loading |
| `debug_manager_query.py` | Test manager role queries |
| `debug_pdf.py` | Test PDF parsing |
| `force_rebuild_openai.py` | Force rebuild index with fresh embeddings |
| `inspect_index.py` | Inspect FAISS index contents |
| `test_extract.py` | Extraction unit tests |
| `test_retrieval.py` | Retrieval unit tests |
| `test_variants.py` | Query variant tests |
| `verify_brain.py` | End-to-end brain verification |

### Debug Log Files

| File | Purpose |
|---|---|
| `cto_debug.txt` | Debug output for CTO-related queries |
| `debug_output.txt` | General debug output |
| `debug_pdf.log` | PDF parsing debug log |
| `debug_pdf_mgr.log` | PDF manager debug log |
| `extraction_log.txt` | Extraction process log |
| `retrieval_debug.json` | Full retrieval debug dump (245 KB) |
| `retrieval_log.txt` | Retrieval process log |
| `retrieval_log_utf8.txt` | UTF-8 retrieval log |
| `verify_infra.log` | Infrastructure verification log |
| `test_download.bin` | Test binary file |

### Frontend Files

| File | Size | Purpose |
|---|---|---|
| `frontend/src/app/page.tsx` | 33 KB | Main chat application — full UI logic, message rendering, voice, PDF panel |
| `frontend/src/app/globals.css` | 16 KB | Complete styling — dark mode, glassmorphism, animations, responsive layout |
| `frontend/src/app/layout.tsx` | 0.4 KB | Root layout component |
| `frontend/package.json` | 0.6 KB | NPM dependencies and scripts |
| `frontend/.env.local` | 43 B | API URL configuration |
| `frontend/next.config.js` | 151 B | Next.js configuration |
| `frontend/tailwind.config.js` | 560 B | Tailwind CSS configuration |
| `frontend/tsconfig.json` | 759 B | TypeScript configuration |

### Asset Files

| Path | Contents |
|---|---|
| `assets/data/` | 20 PDF documents (~75 MB total) |
| `assets/indexes/` | Blue/green index builds (FAISS + chunks.jsonl) |
| `assets/indexes/ACTIVE.json` | Pointer to currently active index directory |
| `assets/pera_logo.png` | PERA logo (4.5 MB) |
| `assets/pera_banner.png` | PERA banner image (2.1 MB) |
| `frontend/public/pera_logo.png` | Frontend copy of PERA logo |
| `frontend/public/pera_banner.png` | Frontend copy of PERA banner |

---

## 9. Configuration & Environment Variables

All configuration is in `.env` at the project root:

### OpenAI API

| Variable | Value | Purpose |
|---|---|---|
| `OPENAI_API_KEY` | `sk-proj-...` | OpenAI API authentication |
| `ANSWER_MODEL` | `gpt-4o-mini` | LLM used for answer generation |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | Embedding model for vectorization |

### Retriever Settings

| Variable | Default | Purpose |
|---|---|---|
| `RETRIEVER_TOP_K` | 30 | Number of FAISS results to fetch |
| `RETRIEVER_SIM_THRESHOLD` | 0.14 | Minimum cosine similarity for inclusion |
| `RETRIEVER_STRONG_SIM_THRESHOLD` | 0.23 | Threshold for "strong" matches |
| `RETRIEVER_MAX_CHUNKS_PER_DOC` | 8 | Max chunks per document in results |
| `RETRIEVER_MAX_DOCS_RETURNED` | 5 | Max documents in results |
| `RETRIEVER_RELATIVE_DOC_SCORE_KEEP` | 0.80 | Keep docs scoring ≥80% of best doc |
| `RETRIEVER_QUERY_VARIANTS_ENABLED` | 1 | Enable query variant generation |
| `RETRIEVER_MAX_QUERY_VARIANTS` | 3 | Max query variants |
| `RETRIEVER_LEX_FALLBACK_ENABLED` | 1 | Enable keyword fallback search |
| `RETRIEVER_LEX_FALLBACK_MAX` | 80 | Max keyword fallback results |
| `RETRIEVER_LEX_FALLBACK_PER_DOC` | 3 | Max keyword results per document |
| `RETRIEVER_MIN_KEYWORD_MATCHES` | 1 | Min keyword matches for inclusion |
| `RETRIEVER_SPELL_CORRECTION_ENABLED` | 1 | Enable spell correction |
| `RETRIEVER_SPELL_MAX_TOKEN_FIXES` | 2 | Max tokens to fix per query |
| `RETRIEVER_SPELL_EDIT_DISTANCE` | 2 | Max edit distance for corrections |
| `RETRIEVER_LLM_QUERY_REWRITE_ENABLED` | 1 | Enable LLM query rewriting |
| `RETRIEVER_LLM_QUERY_REWRITE_ALWAYS` | 1 | Rewrite even first queries |
| `RETRIEVER_LLM_QUERY_REWRITE_MODEL` | gpt-4o-mini | Model for query rewriting |
| `RETRIEVER_DEBUG` | 1 | Enable retriever debug logging |

### Answerer Settings

| Variable | Default | Purpose |
|---|---|---|
| `MAX_EVIDENCE_CHARS` | 24000 | Max total evidence characters for LLM |
| `ANSWER_MIN_TOP_SCORE` | 0.28 | Min document score to include evidence |
| `HIT_MIN_SCORE` | 0.26 | Min individual hit score |
| `HIT_STRONG_SCORE_BYPASS` | 0.55 | Score above which hits bypass filters |
| `MAX_HITS_PER_DOC_FOR_PROMPT` | 15 | Max hits per document in prompt |
| `MAX_DOCS_FOR_PROMPT` | 6 | Max documents in prompt |

### Chunking Settings

| Variable | Default | Purpose |
|---|---|---|
| `CHUNK_MAX_CHARS` | 4000 | Max characters per chunk |
| `CHUNK_OVERLAP_CHARS` | 200 | Overlap between adjacent chunks |

### Index Versioning

| Variable | Default | Purpose |
|---|---|---|
| `EMBED_TEXT_VERSION` | 4 | Triggers re-embedding when incremented |
| `SEARCH_TEXT_VERSION` | 3 | Triggers search text rebuild when incremented |
| `EMBED_MODEL_VERSION` | 1 | Triggers full re-embed if model changes |

### Other

| Variable | Value | Purpose |
|---|---|---|
| `Base_URL` | `https://ask.pera.gop.pk/` | Base URL for reference links |
| `INDEX_POINTER_PATH` | `assets/indexes/ACTIVE.json` | Path to active index pointer |

---

## 10. Data Assets & Document Corpus

The system indexes 20 official PERA documents stored in `assets/data/`:

| # | Document | Size | Description |
|---|---|---|---|
| 1 | PERA Act 2024 (Bill 2024-B) | 1.0 MB | Founding legislation of PERA |
| 2 | Annex G – Employees Service Rules | 2.7 MB | Permanent employee service rules |
| 3 | Annex H – Contractual Employees SRs | 2.8 MB | Contractual employee service rules |
| 4 | Annex I – Performance Appraisal | 0.5 MB | Performance evaluation procedures |
| 5 | Annex L – Squads and Weapons Regulations | 0.4 MB | Enforcement squad regulations |
| 6 | Annex M – Medical Policy | 0.3 MB | Employee medical benefits policy |
| 7 | Annex N – Gratuity Regulations | 0.3 MB | End-of-service gratuity rules |
| 8 | Annex P – Enforcement Cost Formula | 0.2 MB | Cost calculation for enforcement operations |
| 9 | Compiled Working Paper (2nd Meeting) | 64.4 MB | Comprehensive working paper for PERA's 2nd Authority meeting |
| 10 | Flag-C – O&Ps Code | 1.1 MB | Operations & Procedures Code |
| 11 | Flag-F – E&D Regulations | 0.3 MB | Efficiency & Discipline regulations |
| 12 | Flag-H – Uniform Regulations | 0.4 MB | Staff uniform requirements |
| 13 | Flag-I – HR Manual | 0.6 MB | Human Resources manual |
| 14 | Flag-L – Weapons | 0.2 MB | Weapons handling regulations |
| 15 | Flag-Q – Circulation Procedure | 0.2 MB | Document circulation procedures |
| 16 | Flag-S – MOM Selection Panel | 0.4 MB | Minutes of Selection Panel meeting |
| 17 | PERA Commencement Notification | 0.1 MB | Official commencement notification |
| 18 | PERA Establishment Notification | 0.2 MB | Authority establishment notification |
| 19 | PERA Special Allowance Advice | 0.1 MB | Special allowance advisory |
| 20 | PERA FAQs | 0.2 MB | Frequently asked questions |

**Total corpus size:** ~75 MB across 20 PDF documents

---

## 11. Blue/Green Index Management

The system uses a production-safe blue/green deployment strategy for its search indexes:

### How It Works

```
┌─────────────────────────────────────────────────┐
│          SafeAutoIndexer (Background Thread)     │
│                                                  │
│  1. POLL: Scan assets/data/ every 30 seconds     │
│  2. COMPARE: Hash current files vs manifest      │
│  3. BUILD: If changes detected →                 │
│     a. Create new dir: build_YYYYMMDD_HHMMSS/   │
│     b. Extract → Chunk → Embed → Build FAISS    │
│     c. Write chunks.jsonl + faiss.index          │
│  4. VALIDATE:                                    │
│     - faiss.index exists                         │
│     - chunks.jsonl has ≥1 active row             │
│  5. SWITCH: Atomically update ACTIVE.json        │
│  6. CLEANUP: Delete old builds (keep last 3)     │
└─────────────────────────────────────────────────┘
```

### ACTIVE.json Pointer

```json
{"active_dir": "assets/indexes/build_20260215_225627"}
```

The retriever reads this pointer at query time to know which index directory to load. The pointer update is atomic (write to temp file → `os.replace()`) so readers never see a partial write.

### Benefits

| Benefit | How |
|---|---|
| **Zero-downtime updates** | New index built in separate dir, pointer switched atomically |
| **Rollback safety** | Old indexes kept (last 3), can manually revert ACTIVE.json |
| **Corruption protection** | Validation gate prevents bad indexes from becoming active |
| **Incremental processing** | Only re-processes new/changed documents (manifest-based) |

---

## 12. Design Decisions & Rationale

### Why FAISS IndexIDMap2 + IndexFlatIP?

- `IndexFlatIP` provides exact (brute-force) inner product search — no approximation errors, which matters for a small corpus (20 docs, ~2000 chunks)
- `IndexIDMap2` wraps it to support custom integer IDs and ID-based deletion for incremental updates
- L2 normalization + inner product = cosine similarity (mathematically equivalent)
- CPU-only (no GPU needed) — appropriate for a government office deployment

### Why text-embedding-3-small?

- 1536 dimensions — good balance of quality and speed
- Cost-effective for a government project
- Strong multi-lingual support (English + Urdu)
- Sufficient for the domain (legal/regulatory documents)

### Why GPT-4o-mini for Answers?

- Low latency (~1-3 seconds) for interactive chat
- Cost-effective (much cheaper than GPT-4o)
- Strong instruction following for the strict persona constraints
- Good multi-lingual output (English, Urdu, Roman Urdu)
- Temperature 0.3 prevents hallucination while allowing natural phrasing

### Why Hybrid Search (Semantic + Keyword)?

- Semantic search alone misses exact term matches (especially for Urdu transliterations)
- Keyword fallback catches specific regulation numbers, section names, and Urdu words that embedding models may not encode perfectly
- The blend (75% semantic, 25% keyword overlap) gives the best of both worlds

### Why Strip References from LLM Output?

- The LLM tends to cite "Source: Document.pdf, Page X" in the answer text
- The UI already shows structured reference cards below the answer
- Duplicating references looks unprofessional and clutters the response
- Reference stripping is skipped if the user explicitly asks for sources/pages

### Why Smart Page Expansion for Salary Queries?

- Salary tables in PERA documents span multiple pages
- A single matching row might be on page 15, but the table header and context is on page 13
- Fetching ±3 pages ensures the LLM sees the complete table structure
- Only triggered for salary/pay/benefit keywords to avoid over-fetching for other queries

---

## 13. Deployment & Running

### Prerequisites

- Python 3.10+ with venv
- Node.js 18+ with npm
- OpenAI API key with access to `gpt-4o-mini` and `text-embedding-3-small`
- (Optional) FFmpeg for voice input format conversion

### Starting the Backend

```bash
cd "d:\Working well backup\PERAAIA"
.\venv\Scripts\activate
python -m uvicorn fastapi_app:app --host 0.0.0.0 --port 8000
```

The auto-indexer starts automatically on server boot and begins polling `assets/data/` for documents.

### Starting the Frontend

```bash
cd "d:\Working well backup\PERAAIA\frontend"
npx next dev --hostname 0.0.0.0 --port 3001
```

### Access URLs

| Service | Local | Network |
|---|---|---|
| Frontend (Chat UI) | http://localhost:3001 | http://{YOUR_IP}:3001 |
| Backend API | http://localhost:8000 | http://{YOUR_IP}:8000 |
| API Docs (Swagger) | http://localhost:8000/docs | — |
| Legacy Streamlit UI | `streamlit run app.py` | — |

### Starting the Legacy Streamlit UI (Optional)

```bash
cd "d:\Working well backup\PERAAIA"
.\venv\Scripts\activate
streamlit run app.py
```

---

*This document provides a complete technical overview of the PERA AI System as of February 17, 2026. For questions about specific modules, refer to the inline code comments in each file.*
