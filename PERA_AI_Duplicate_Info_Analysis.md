# PERA AI — Cross-Document Duplicate Information Analysis

**Date:** February 17, 2026  
**Scope:** How PERA AI handles identical/overlapping content across different PDF documents  
**Code Commit:** Current production (as-is in `d:\Working well backup\PERAAIA`)

---

## A) Current Logic — Step by Step

### A.1) Indexing Pipeline (No Cross-Document Dedup)

**Files:** `index_store.py` → `scan_and_ingest_if_needed()` (line 609), `doc_registry.py` → `compare_with_manifest()`

```
PDF → extractors.py → List[ExtractedUnit] → chunker.py → List[Chunk] → index_store.py → FAISS + chunks.jsonl
```

**Critical fact: There is ZERO cross-document text deduplication at index time.**

The indexing pipeline works per-document:

1. `scan_assets_data()` scans `assets/data/` — lists all PDFs with metadata (filename, mtime, SHA-256, rank)
2. `compare_with_manifest()` compares against `manifest.json` — detects **file-level** changes (new/changed/removed) using filename as key
3. For each **new or changed** document:
   - Extract text pages: `extract_units_from_files()`
   - Chunk pages: `chunk_units()`
   - Filter low-signal chunks: `_is_low_signal_chunk()`
   - Build `embed_text` (enriched) and `search_text` (keyword) for each chunk
   - Embed via OpenAI: `embed_texts()`
   - Assign unique sequential chunk IDs: `_next_chunk_id()`
   - Add vectors to FAISS: `idx.add_with_ids()`
   - Append rows to `chunks.jsonl`
4. Save manifest

**What this means:** If identical text appears in `Annex H Contractual Employees SRs.pdf` (page 22) AND `Flag-I HR Manual.pdf` (page 15), **both chunks are indexed independently**. Each gets:
- Its own unique `id` (e.g., 142 and 587)
- Its own `doc_name` (different)
- Its own `embed_text` (slightly different because doc_name is prepended)
- Its own FAISS vector (slightly different embeddings due to different doc_name prefix)

There is no text-hash dedup, no content fingerprinting, no cross-document comparison.

### A.2) Retrieval Pipeline

**File:** `retriever.py` → `retrieve()` (lines 166–410)

#### Step 1: Query Preparation (lines 189–201)
```python
expanded_q = _expand_abbreviations(question)   # "CTO" → "Chief Technology Officer"
qv = embed_texts([expanded_q])[0]               # 1536-dim vector
qv = _normalize_vec(qv)                         # L2-normalize for cosine sim
```

#### Step 2: FAISS Semantic Search (lines 203–223)
```python
D, I = idx.search(qv.reshape(1, -1), TOP_K)    # TOP_K=30, returns (scores[], ids[])
```
Returns up to 30 nearest neighbors. **Both duplicates from PDF#1 and PDF#2 will appear** if they are semantically close to the query. They may have slightly different scores due to different `embed_text` prefixes.

#### Step 3: Smart Page Expansion (lines 225–253)
For salary/detail queries, fetches ±3 adjacent pages from top 10 hits. Uses `(doc_name, page)` as the key — so expansions stay within the same document. This does NOT create cross-document interaction.

#### Step 4: Keyword Fallback (lines 256–308)
Linear scan over ALL active chunks. Any chunk matching ≥50% of query words gets a keyword score (0.55–0.72). **Both copies will score independently.** No cross-document filtering.

#### Step 5: Score Fusion + Within-Doc Dedup (lines 331–402)

This is the **critical section**. Here is the exact logic:

```python
docs_map: Dict[str, Dict[str, Any]] = {}  # Keyed by doc_name

def _ensure_doc(doc_name: str, initial_score: float) -> Dict:
    if doc_name not in docs_map:
        docs_map[doc_name] = {
            "doc_name": doc_name,
            "max_score": float(initial_score),
            "hits": [],
            "_seen": set(),  # ← dedupe set (WITHIN this doc only)
        }
    return docs_map[doc_name]

def _process_hit(chunk_id: int, score_val: float, is_context: bool = False):
    # ...
    doc_name = r.get("doc_name", "Unknown")
    text = r.get("text", "")
    page = r.get("loc_start", "?")

    doc_group = _ensure_doc(doc_name, final_score)

    # Dedupe: (page, text[:200]) — WITHIN this doc_group only
    sig = (str(page), text[:200])
    if sig in doc_group["_seen"]:
        return
    doc_group["_seen"].add(sig)

    doc_group["hits"].append({...})
```

**Key findings:**

| Aspect | Behavior | Impact |
|---|---|---|
| `docs_map` key | `doc_name` (string) | Each PDF gets its own entry — duplicates are in SEPARATE groups |
| `_seen` set | Per `doc_group` (per document) | Only deduplicates within the SAME PDF |
| `sig` tuple | `(str(page), text[:200])` | Does NOT include `doc_name` — but this is irrelevant since `_seen` is per-doc anyway |
| Cross-doc dedup | **NONE** | Identical text from two PDFs survives as two separate entries |

The `_seen` set is created per `doc_group`, so it exists inside each document's dictionary. Two chunks with identical text from different documents will NEVER collide in `_seen` because they are in different groups.

#### Step 6: Sorting & Return (lines 398–410)
```python
evidence.sort(key=lambda x: float(x.get("max_score", 0)), reverse=True)
```
Documents are sorted by their best hit score. The document with the highest single-chunk score wins the top position.

### A.3) Evidence Selection for LLM

**File:** `answerer.py` → `format_evidence_for_llm()` (lines 41–132)

```python
for doc_group in evidence_list:
    if doc_group.get("max_score", 0) < ANSWER_MIN_TOP_SCORE:  # 0.28
        continue
    if docs_used >= MAX_DOCS:  # 6
        break

    # Sort hits by subject relevance, then score
    sorted_hits = sorted(hits, key=_hit_relevance)

    for hit in sorted_hits:
        if not is_context and hit.get("score", 0) < HIT_MIN_SCORE:  # 0.26
            continue
        if hits_used >= MAX_HITS_PER_DOC:  # 15
            break

        part = f'<evidence doc="{doc_name}" page="{page}">\n{text}\n</evidence>'

        if total_chars + len(part) > MAX_EVIDENCE_CHARS:  # 24000
            break

        context_parts.append(part)
```

**Key findings:**

| Parameter | Value | Effect on Duplicates |
|---|---|---|
| `MAX_DOCS` | 6 | Up to 6 different PDFs can contribute evidence |
| `MAX_HITS_PER_DOC` | 15 | Up to 15 chunks per document |
| `HIT_MIN_SCORE` | 0.26 | Both copies pass easily (both score ~0.5+) |
| `ANSWER_MIN_TOP_SCORE` | 0.28 | Both documents pass easily |
| `MAX_EVIDENCE_CHARS` | 24,000 | Budget is consumed by duplicates |
| Cross-doc dedup | **NONE** | GPT-4o-mini sees the same text twice |

---

## B) Duplicate-Info Behavior — Clear Rules

### B.1) "If the SAME information exists in TWO different PDFs, how does PERA AI decide what to return?"

**Answer: It returns BOTH.** There is no mechanism to detect or suppress duplicates across documents.

### B.2) Does dedupe logic remove duplicates only within a document or also across documents?

**Within a single document ONLY.**

The dedup key is `sig = (str(page), text[:200])`, stored in `doc_group["_seen"]`. Since `_seen` is per-document (created fresh for each `doc_name`), two chunks with identical text from different PDFs are **never compared against each other**.

### B.3) Does `doc_rank` / priority affect which PDF wins when content overlaps?

**No.** `doc_rank` is stored in chunk metadata but is **never used in retrieval scoring or evidence selection**. The only ranking factor is:

1. **FAISS cosine similarity score** (semantic)
2. **Keyword match score** (hybrid)
3. **Subject-word relevance** (within evidence selection)

`doc_rank` appears in the chunk JSONL but is only used in `reranker.py` as the 4th tiebreaker in `rerank_hits()` — and `rerank_hits()` is **never called** from the current `retrieve()` function.

```python
# reranker.py line 27-34 — this function exists but is NOT imported or called by retriever.py
hits.sort(key=lambda x: (
    float(x.get("_blend", 0.0)),
    int(x.get("_lex_ov", 0)),
    int(x.get("doc_rank", 0) or 0),    # ← exists but unused
    str(x.get("doc_name", "")),
    int(x.get("id", 0) or 0),
), reverse=True)
```

### B.4) If both PDFs survive scoring, does the LLM see both (duplicate evidence)?

**Yes.** The LLM receives something like:

```xml
<evidence doc="Annex H Contractual Employees SRs" page="22">
The Chief Technology Officer shall be responsible for...
[full text of CTO duties]
</evidence>

...other evidence blocks...

<evidence doc="Flag-I HR Manual" page="15">
The Chief Technology Officer shall be responsible for...
[same/similar text of CTO duties]
</evidence>
```

**The LLM sees the same information twice**, consuming ~2x the token budget for no additional value.

### B.5) What happens if the two PDFs conflict (slightly different wording/numbers)?

The system prompt says:

> *"6) If the Context contains conflicting statements, present both neutrally."*

So GPT-4o-mini is instructed to present both versions. However, there is **no mechanism to indicate which document is authoritative**. The LLM has no way to know that the Act supersedes the FAQ, or that the latest Notification overrides the Working Paper.

---

## C) CTO Example Walkthrough

### Query: "What are the CTO's duties and salary?"

#### Step 1: Query Rewriting
```
Input:  "What are the CTO's duties and salary?"
Output: "What are the Chief Technology Officer's duties, responsibilities, functions, salary, pay scale at PERA?"
```
(LLM rewrites because `RETRIEVER_LLM_QUERY_REWRITE_ALWAYS=1`)

#### Step 2: Abbreviation Expansion
```python
_expand_abbreviations("What are the CTO's duties and salary?")
→ "What are the Chief Technology Officer's duties and salary?"
```
(The rewrite already handled this, but both paths expand CTO)

#### Step 3: Embedding + FAISS Search (TOP_K=30)

The query vector is closest to chunks containing "Chief Technology Officer" text. Let's say FAISS returns:

| Rank | Score | Chunk ID | Document | Page |
|---|---|---|---|---|
| 1 | 0.62 | 247 | `Annex H Contractual Employees SRs.pdf` | 22 |
| 2 | 0.59 | 248 | `Annex H Contractual Employees SRs.pdf` | 23 |
| 3 | 0.57 | 512 | `Flag-I HR Manual.pdf` | 15 |
| 4 | 0.55 | 89 | `Compiled Working Paper.pdf` | 143 |
| 5 | 0.53 | 513 | `Flag-I HR Manual.pdf` | 16 |
| 6 | 0.48 | 90 | `Compiled Working Paper.pdf` | 144 |
| 7 | 0.44 | 301 | `PERA FAQs.pdf` | 3 |
| ... | ... | ... | ... | ... |

Note: Chunks 247 and 512 may contain **nearly identical text** (CTO duties), but get different scores because their `embed_text` prefixes differ:
- Chunk 247: `"ENTITY: PERA... DOC: Annex H Contractual Employees SRs... PAGE: 22... The Chief Technology Officer shall..."`
- Chunk 512: `"ENTITY: PERA... DOC: Flag-I HR Manual... PAGE: 15... The Chief Technology Officer shall..."`

#### Step 4: Smart Page Expansion (TRIGGERED)

Query contains "salary" → expansion enabled. For top 10 hits, fetch ±3 pages:

- From `Annex H` page 22: also fetch pages 19, 20, 21, 23, 24, 25
- From `HR Manual` page 15: also fetch pages 12, 13, 14, 16, 17, 18
- From `Working Paper` page 143: also fetch pages 140, 141, 142, 144, 145, 146

This adds **many more chunks** — potentially 30-60 additional chunks from all three documents.

#### Step 5: Keyword Fallback

Linear scan finds any chunk containing "chief", "technology", "officer", "duties", "salary":
- More hits from Annex H, HR Manual, Working Paper, and FAQs

#### Step 6: Score Fusion + Dedup

```python
docs_map = {
    "Annex H Contractual Employees SRs.pdf": {
        "max_score": 0.62,
        "hits": [
            {"text": "CTO duties...", "score": 0.62, "page_start": 22},
            {"text": "CTO salary table...", "score": 0.59, "page_start": 23},
            # + expanded pages 19-25
        ],
        "_seen": {("22", "The Chief Technology Officer shall..."), ...}
    },
    "Flag-I HR Manual.pdf": {
        "max_score": 0.57,
        "hits": [
            {"text": "CTO duties...", "score": 0.57, "page_start": 15},  # ← DUPLICATE TEXT
            {"text": "CTO reporting...", "score": 0.53, "page_start": 16},
            # + expanded pages 12-18
        ],
        "_seen": {("15", "The Chief Technology Officer shall..."), ...}
    },
    "Compiled Working Paper.pdf": {
        "max_score": 0.55,
        "hits": [
            {"text": "CTO section...", "score": 0.55, "page_start": 143},
            # ...
        ]
    },
    "PERA FAQs.pdf": {
        "max_score": 0.44,
        "hits": [
            {"text": "FAQ about CTO...", "score": 0.44, "page_start": 3},
        ]
    }
}
```

Note that `"CTO duties..."` appears in BOTH `Annex H` (score 0.62) and `HR Manual` (score 0.57) but they are in **separate** `doc_group` entries. Neither `_seen` set blocks the other.

#### Step 7: Evidence Selection for GPT-4o-mini

```python
evidence_list sorted by max_score:
  1. Annex H (0.62)     → passes ANSWER_MIN_TOP_SCORE (0.28) ✓
  2. HR Manual (0.57)    → passes ✓
  3. Working Paper (0.55) → passes ✓
  4. FAQs (0.44)         → passes ✓
```

All 4 documents pass. `MAX_DOCS=6` allows all of them.

**The final evidence pack sent to GPT-4o-mini:**

```xml
<!-- From Annex H (doc #1, max_score=0.62) -->
<evidence doc="Annex H Contractual Employees SRs" page="22">
The Chief Technology Officer (CTO) shall be responsible for:
(a) Overseeing technology infrastructure...
(b) Managing IT operations...
(c) Development of software systems...
Appointment: Through Selection Panel
Salary: BPS-19 equivalent, SPPP scales apply
</evidence>

<evidence doc="Annex H Contractual Employees SRs" page="23">
[salary table continuation, expanded chunks...]
</evidence>

<!-- From HR Manual (doc #2, max_score=0.57) -->
<evidence doc="Flag-I HR Manual" page="15">
The Chief Technology Officer (CTO) shall be responsible for:
(a) Overseeing technology infrastructure...       ← DUPLICATE
(b) Managing IT operations...                      ← DUPLICATE
(c) Development of software systems...             ← DUPLICATE
</evidence>

<!-- From Working Paper (doc #3, max_score=0.55) -->
<evidence doc="Compiled Working Paper" page="143">
[CTO section from working paper — may be a draft version]
</evidence>

<!-- From FAQs (doc #4, max_score=0.44) -->
<evidence doc="PERA FAQs" page="3">
Q: What does the CTO do?
A: The CTO is responsible for technology and IT systems...
</evidence>
```

**Result:** The LLM sees CTO duties **3-4 times** (Annex H, HR Manual, Working Paper, and a simplified version in FAQs). This wastes ~3-4x the context budget for the same information.

---

## D) Issues + Fixes

### Issue 1: Redundant Evidence Wastes Context Budget

**Risk:** With `MAX_EVIDENCE_CHARS=24,000`, duplicate text from multiple documents consumes the budget rapidly. If CTO duties appear in 3 PDFs, you lose ~3x the tokens for the same information, leaving less room for salary tables or other relevant details.

**Severity:** HIGH — directly degrades answer quality for complex queries.

**Fix: Cross-Document Content Dedup**

```python
# In retriever.py → _process_hit()
# Change from per-doc _seen to a GLOBAL _seen_cross_doc set

# Before the per-doc loop:
_seen_global = set()   # NEW: cross-doc dedup

def _process_hit(chunk_id, score_val, is_context=False):
    # ... existing code ...
    text = r.get("text", "")
    
    # Cross-doc content fingerprint
    text_hash = hashlib.md5(text[:300].encode()).hexdigest()[:12]
    cross_sig = text_hash   # Content-based, ignores doc_name
    
    if cross_sig in _seen_global:
        # Keep only if this doc has higher authority
        return  # or: merge into existing group
    _seen_global.add(cross_sig)
    
    # ... rest of existing code ...
```

### Issue 2: No Canonical Source Policy

**Risk:** When the same regulation appears in:
- **The PERA Act** (authoritative legislation)
- **Service Rules** (delegated regulation)
- **Working Paper** (draft/discussion)
- **FAQs** (simplified summary)

...the system treats them equally. The FAQ's simplified version might contradict the Act's precise language, and the LLM has no instruction on which to prefer.

**Severity:** HIGH — can produce misleading answers in a government context.

**Fix: Document Authority Tiers**

```python
# Define authority tiers (in .env or config)
DOC_AUTHORITY = {
    "Act": 1,       # Highest: PERA Act 2024
    "Annex": 2,     # Service Rules, Regulations, Annexures
    "Flag": 3,      # Internal SOPs, Manuals
    "Working": 4,   # Working Papers (draft)
    "FAQ": 5,       # Summaries (lowest)
    "Notification": 2,  # Official Notifications
}

def _get_authority_tier(doc_name: str) -> int:
    for prefix, tier in DOC_AUTHORITY.items():
        if prefix.lower() in doc_name.lower():
            return tier
    return 3  # Default mid-tier

# In evidence selection: when cross-doc duplicate detected,
# keep only the chunk from the highest-authority document (lowest tier number)
```

### Issue 3: `doc_rank` Is Indexed but Never Used

**Risk:** The `doc_rank` field is computed from filename patterns (e.g., `book1`, `book2`) and stored in every chunk row. However:
- `retrieve()` never reads `doc_rank`
- `format_evidence_for_llm()` never reads `doc_rank`
- `reranker.py` uses it but is **never called**

This is dead code that could be leveraged for authority ranking.

**Severity:** MEDIUM — missed opportunity, not a bug.

**Fix:** Either:
- (a) Import and call `rerank_hits()` before evidence selection, or
- (b) Use `doc_rank` in `_process_hit()` as a tiebreaker, or
- (c) Remove `doc_rank` computation to avoid confusion

### Issue 4: Conflict Handling Is Passive

**Risk:** If Annex H says "CTO salary: BPS-19" but a Notification says "CTO salary: BPS-20", the LLM prompt says *"present both neutrally"*. This is fine for a lawyer but confusing for a staff member.

**Severity:** MEDIUM — government employees need clear answers, not legal ambiguity.

**Fix: Active Conflict Detection**

```python
# In answerer.py → after formatting evidence:
# If same entity appears in multiple evidence blocks with different numbers/dates,
# add a note to the system prompt:

"CONFLICT NOTICE: Multiple documents provide different values for <X>. "
"Present the most recent document's value as the current rule, and note the "
"discrepancy. Cite the Notification date to establish recency."
```

### Issue 5: Evidence Budget Starvation

**Risk:** With 4 documents × 15 hits/doc × ~800 chars/chunk, the theoretical maximum is 48,000 characters — but `MAX_EVIDENCE_CHARS=24,000` caps it. If 3 documents contribute duplicate CTO duties (2,400 chars each = 7,200 chars wasted), the actual useful evidence is only ~16,800 chars. This means salary tables or other details may be **cut off**.

**Severity:** HIGH — the user asked about salary AND duties, but salary tables get truncated because duties ate the budget 3x.

**Fix:** Cross-doc dedup (Issue #1) directly solves this. Deduplicating saves ~7,200 chars → salary tables fit.

### Issue 6: Within-Doc Dedup Signature Is Weak

**Risk:** The dedup signature is `(str(page), text[:200])`. If two chunks from the same page have different first 200 characters but are otherwise identical (e.g., due to overlap), both survive.

**Severity:** LOW — overlap is usually 200 chars, and the first 200 chars of overlapping chunks ARE different. This is fine.

---

## E) Exact Code Locations / Functions Involved

### Indexing (No Cross-Doc Dedup)

| File | Function | Lines | Role |
|---|---|---|---|
| [index_store.py](file:///d:/Working%20well%20backup/PERAAIA/index_store.py) | `scan_and_ingest_if_needed()` | 609–880 | Main ingestion — processes docs independently, no text dedup |
| [index_store.py](file:///d:/Working%20well%20backup/PERAAIA/index_store.py) | `_mark_inactive_for_doc()` | 558–569 | Deactivates old chunks by doc_name only |
| [index_store.py](file:///d:/Working%20well%20backup/PERAAIA/index_store.py) | `_is_low_signal_chunk()` | 313–325 | Filters garbage, not duplicates |
| [doc_registry.py](file:///d:/Working%20well%20backup/PERAAIA/doc_registry.py) | `compare_with_manifest()` | 92–168 | File-level change detection (not content dedup) |

### Retrieval (Within-Doc Dedup Only)

| File | Function | Lines | Role |
|---|---|---|---|
| [retriever.py](file:///d:/Working%20well%20backup/PERAAIA/retriever.py) | `retrieve()` | 166–410 | Main retrieval — groups by doc, dedup within doc only |
| [retriever.py](file:///d:/Working%20well%20backup/PERAAIA/retriever.py) | `_process_hit()` | 344–378 | Core dedup: `sig = (str(page), text[:200])` in per-doc `_seen` |
| [retriever.py](file:///d:/Working%20well%20backup/PERAAIA/retriever.py) | `_ensure_doc()` | 334–342 | Creates per-doc group with isolated `_seen` set |

### Evidence Selection (No Cross-Doc Dedup)

| File | Function | Lines | Role |
|---|---|---|---|
| [answerer.py](file:///d:/Working%20well%20backup/PERAAIA/answerer.py) | `format_evidence_for_llm()` | 41–132 | Formats evidence — no cross-doc dedup, iterates docs independently |
| [answerer.py](file:///d:/Working%20well%20backup/PERAAIA/answerer.py) | `extract_references_simple()` | 135–184 | Builds UI references — dedup by `f"{doc_name}_{page}"` (within doc) |
| [answerer.py](file:///d:/Working%20well%20backup/PERAAIA/answerer.py) | `answer_question()` | 280–385 | Calls `format_evidence_for_llm()`, passes result to GPT-4o-mini |

### Unused but Relevant

| File | Function | Lines | Role |
|---|---|---|---|
| [reranker.py](file:///d:/Working%20well%20backup/PERAAIA/reranker.py) | `rerank_hits()` | 18–37 | Uses `doc_rank` as tiebreaker — **NOT CALLED from retrieve()** |

---

## Summary: The Core Problem

```
┌─────────────────────────────────────────────────────────────┐
│                 CURRENT DEDUP BOUNDARY                       │
│                                                              │
│   PDF #1 ──→ chunks ──┐                                     │
│                        ├──→ FAISS ──→ retrieve()             │
│   PDF #2 ──→ chunks ──┘         ↓                           │
│                          ┌──────────────────┐                │
│                          │ docs_map grouped  │                │
│                          │ by doc_name       │                │
│                          │                   │                │
│                          │  "Annex H": {     │                │
│                          │    _seen: {sig1}  │  ← dedup HERE │
│                          │    hits: [...]    │    (per-doc)   │
│                          │  }                │                │
│                          │  "HR Manual": {   │                │
│                          │    _seen: {sig1}  │  ← dedup HERE │
│                          │    hits: [...]    │    (per-doc)   │
│                          │  }                │                │
│                          │                   │                │
│                          │  NO CROSS-DOC     │                │
│                          │  COMPARISON!      │                │
│                          └──────────────────┘                │
│                                   ↓                          │
│                    Both docs → format_evidence_for_llm()     │
│                    Both docs → GPT-4o-mini sees duplicates   │
└─────────────────────────────────────────────────────────────┘
```

### Recommended Priority Order for Fixes

1. **P0 — Cross-doc content dedup in `_process_hit()`** — Hash-based fingerprint, keep the copy from the highest-scoring or highest-authority document
2. **P1 — Document authority tiers** — Act > Annexures > Manuals > Working Papers > FAQs
3. **P2 — Wire up `doc_rank` or `rerank_hits()`** — Use existing dead code
4. **P3 — Active conflict detection** — Annotate evidence when conflicting values detected
5. **P4 — Index-time content fingerprinting** — Optional; dedup at retrieval time is sufficient for this corpus size (~2000 chunks)
