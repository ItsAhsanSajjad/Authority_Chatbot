# PERA AI Chatbot — Quality, Accuracy & Robustness Plan

## 1. Goal

A government-grade enforcement chatbot whose answers can be trusted by
higher-authority officials at the Punjab Enforcement and Regulatory
Authority. Five operating principles:

1. **Accurate** — every figure either comes from a verified source or is
   absent.
2. **Robust** — malformed, vague, multilingual, and typo-laden queries
   resolve to a useful answer or a clear "cannot answer" signal.
3. **Tolerant** — minor spelling errors, ordering differences, and
   informal Roman-Urdu phrasing don't break routing.
4. **Context-aware** — multi-source, hierarchical, and partial-data
   queries are stitched into a single coherent reply.
5. **Honest** — when data is unavailable or partial, the answer says so;
   no hallucinations.

## 2. Current architecture (high-level)

```
User query
  │
  ▼
[ FastAPI /api/ask ]
  │
  ▼
[ Typo + Roman-Urdu preprocessor ]   ← single-pass spell-fix, ASCII fold
  │
  ▼
[ Smalltalk + session lookup ]       ← memory of last entity / intent
  │
  ▼
[ Query rewrite (gpt-4o-mini) ]      ← abbreviation expansion
  │
  ▼
[ Query router (DOCUMENT / STRUCTURED / HYBRID) ]
  │
  ▼
[ Domain detectors run in parallel ]
  │  inspection_lookup.detect_inspection_intent
  │  challan_lookup.detect_challan_intent
  │  operational_activity_lookup.detect_operational_activity_intent
  │
  ▼
[ Stored API lookup OR FAISS retrieval OR live SDEO API ]
  │
  ▼
[ Reranker + evidence formatter ]
  │
  ▼
[ Answerer (gpt-4o) ]                ← system prompt with grounding rules
  │
  ▼
[ Grounding verifier ]               ← Tier1 regex + Tier2 LLM-as-judge
  │
  ▼
Final answer + references + confidence + evidence_citations
```

### Data sources

- **FAISS index** — PERA Acts, regulations, schedules, working papers.
- **PostgreSQL analytics** — `inspection_performance`, `challan_data`,
  `operational_activity`, `dim_division/district/tehsil/officer`,
  `fact_*` tables.
- **SDEO live API** — `/sdeo-dashboard/inspections-summary`,
  `/sdeo-dashboard/top-kpis`,
  `/sdeo-dashboard/challan-status-breakdown`, `/Pcm/dashboard-counts`,
  `/Pcm/officer-inspections`.

## 3. Improvements applied this iteration

| Layer | Improvement | File |
|---|---|---|
| Date-range aggregation | District + division-level live fan-out | `inspection_lookup._query_district_live`, `_query_division_live` |
| Aggregation correctness | Per-tehsil parallel fetch with thread pool | `_fetch_tehsil_metrics` + `ThreadPoolExecutor(max_workers=8)` |
| Officer collapse | Same officer across tehsils sums into a single row | `_aggregate_tehsil_metrics` |
| Audit trail | Per-tehsil markdown table forced into LLM answer | `_format_aggregate_context` + system-prompt rule 0 |
| Date-boundary | `endDate` passed verbatim to mirror SDEO UI's date-picker behaviour | `_fetch_tehsil_metrics`, `_query_tehsil_live` |
| Intent fallback | "division/district/tehsil/station + date range" routes to inspection even without `inspection` keyword | `detect_inspection_intent` |
| Synonym | `station[s]` accepted as alias for `tehsil` | `_detect_location` |
| Token-match | `allama iqbal` matches `Allama Iqbal Town` | `_tehsil_token_match` |
| Order swap | Location detection before officer detection (avoid name-pair LIKE collisions) | `detect_inspection_intent` |
| Stop-words | Month abbreviations `jan, feb, …, dec` filtered from officer-name candidates | `_NAME_STOP_WORDS` |
| Typo regex | Challan keyword tolerates `chaallans, challanns, chalaans, chellan, …` | `_CHALLAN_PATTERNS` |
| Typo regex | Summary tolerates `summery, summmery, summari, summer y, …` | `_INSP_KEYWORDS` |
| Typo regex | `removal order` tolerates plural `removal orders` | `_INSP_KEYWORDS` |
| OpenAI fallback | Working API key swapped in `.env`; backend boot-loop disabled with `INDEX_POLL_SECONDS=86400` | `.env` |

## 4. Recommended future improvements

These move the system from "tolerant" to "industrial-grade reliable".

### 4.1 Intent layer

1. **Levenshtein-based fuzzy match** for the keyword set so we don't
   need a regex for every typo. A short list of canonical tokens
   (`inspection`, `challan`, `tehsil`, `division`, `district`, `summary`,
   `sealed`, `warning`, `removal`) is enough — anything within edit
   distance ≤ 2 maps to the canonical token.
2. **Hybrid intent classifier** — small embedding-based classifier
   (≤256 examples per class, k-NN cosine over `text-embedding-3-small`)
   layered on top of the regex matcher. Use the regex as a fast path,
   fall back to embedding similarity when the regex is ambiguous.
3. **Single normalised entity index** — one in-memory dict keyed by
   lowercased token for divisions, districts, tehsils, officers.
   Resolves the `D.G. Khan` ↔ `DG Khan` ↔ `Dera Ghazi Khan`
   normalisation issue once for all callers.
4. **Coarse-to-fine match for multi-word names** — try full-string
   match first (`Allama Iqbal Town`), then token-subset match
   (`Allama Iqbal`), then per-word Soundex as last resort.
5. **Followup memory** — currently
   `detect_inspection_followup(prev_intent)` covers a few patterns. Move
   this to a structured session object on the FastAPI side that tracks:
   `last_level`, `last_location`, `last_date_range`, `last_domain`. Make
   it part of every `/api/ask` request so the bot can resolve "or how
   many challans?" without re-detecting.

### 4.2 Retrieval / aggregation layer

1. **Cache `dim_tehsil ⨝ dim_district ⨝ dim_division`** in process
   memory with a 5-minute TTL. The hierarchy changes ≤ daily.
2. **Memoise per-tehsil API responses** keyed by
   `(tehsil_id, start_date, end_date)`. Most authority queries hit a
   small set of date windows.
3. **Circuit breaker** — if > 30 % of tehsils in a fan-out call error,
   abort early and return a degraded "partial coverage" answer.
4. **Materialise daily snapshot columns**:
   `paid_count`, `unpaid_count`, `fine_imposed`, `fine_recovered`,
   `arrest_count`, `pcm_count`. Keeps non-date-ranged queries
   answerable from PostgreSQL alone — no live API round-trip.
5. **Evidence freshness footer** — every aggregate already prints a
   per-tehsil breakdown. Extend with the snapshot timestamp the SDEO
   UI uses, so the user can match minute-to-minute.

### 4.3 Answer layer

1. **Forced sections** — system-prompt rule 0 already requires the
   per-tehsil table. Extend with:
   - "If the date range is < 24 h, prefix the answer with a 'live data'
     disclaimer."
   - "If `tehsils_failed > 0`, prefix the answer with the partial-
     coverage warning."
2. **Numerical post-processor** — after the LLM responds, parse all
   numbers and verify each against the structured context. Flag any
   number that doesn't appear verbatim. Stops hallucination.
3. **Confidence ribbon** — a single-line summary of route + grounding +
   freshness shown at the top of every answer (e.g.
   `Confidence 0.92 · Source SDEO API + 11 tehsils · Live · 2 s ago`).

### 4.4 Robustness

1. **Validation tier per query** — before retrieval, run a 50-ms guard:
   - Strip control chars, collapse whitespace, fold to NFC.
   - If query length > 1 000 chars, summarise to 200 chars before
     intent detection (avoids regex catastrophic backtracking).
   - If query has > 5 distinct admin-level keywords, ask for
     disambiguation rather than guessing.
2. **Cost ceiling per request** — fan-outs capped at 25 tehsils per
   query (covers any single division). Reject "all of Punjab last
   year" queries with a polite error and a suggestion to narrow.
3. **Graceful degradation order**: live SDEO → cached SDEO snapshot →
   stored PostgreSQL aggregate → "data unavailable for this window".

## 5. Test coverage

Two test files together exercise 500+ cases:

| File | Cases | Focus |
|---|---|---|
| `tests/test_inspection_aggregation.py` | 250 | Inspection routing + aggregation arithmetic + live smoke (gated) |
| `tests/test_chatbot_quality.py` | 255 | 14 quality categories (positive, negative, edge, stress) |

### `test_chatbot_quality.py` categories

| Section | Cases | Focus |
|---|---|---|
| A. Positive simple | 30 | clear single-intent queries across inspection/challan/OA |
| B. Positive complex | 15 | multi-part analytical queries |
| C. Roman-Urdu / mixed | 25 | Urdu-English code-mixed phrasing |
| D. Typo tolerance | 25 | spelling errors, transposed letters, dropped letters |
| E. Out-of-scope | 15 | non-PERA topics — must return None |
| F. Misleading | 10 | nonsense locations, contradictory queries |
| G. Very short | 15 | one-word queries — keyword match vs no-context |
| H. Empty / nonsense | 5 | empty strings, whitespace, gibberish |
| I. Stress / long | 15 | 100+ word multi-intent queries |
| J. Date format variations | 20 | numeric, named-month, ISO, ordinal, yearless, relative |
| K. Officer queries | 15 | name-based queries with and without date range |
| L. Fallback / ambiguous | 10 | should resolve to summary, not specific routes |
| M. Hierarchy disambiguation | 10 | division vs district vs tehsil with overlapping name (`Lahore`) |
| N. Cross-domain | 15 | inspection vs challan vs OA prefer logic |
| O. Query router | 10 | DOCUMENT / STRUCTURED / HYBRID classification |
| P. Casing & spacing | 10 | uppercase, extra spaces, hyphenated forms |
| Q. Partial / dangling | 10 | half-typed queries that should not over-route |

### Current outcome (deterministic, no live API)

```
246 passed, 8 xfailed, 1 xpassed   (test_chatbot_quality.py)
190 passed, 32 xfailed, 18 xpassed, 10 skipped   (test_inspection_aggregation.py)
─────────────────────────────────────────
Combined: 436 passed, 40 xfail, 19 xpass, 10 skip
```

Every xfail is a tracked gap (see backlog in
`docs/INSPECTION_AGGREGATION_FAILURE_ANALYSIS.md`).

## 6. Recommendations summary

1. **Bring in fuzzy matching.** Will replace ~80 % of the typo-regex
   bandages and unblock the section-D xfails.
2. **Move location/officer/keyword detection to a single normalised
   entity index** so canonical name forms (`D.G. Khan` etc.) are not
   handled as one-off cases.
3. **Cache hierarchy + per-tehsil API responses.** Highest user-
   visible latency improvement at lowest engineering cost.
4. **Persist aggregated date-ranged metrics** so non-live queries
   don't re-fan-out for repeat dashboard prompts.
5. **Add a numerical post-processor** between the LLM answer and the
   user — last-line defence against hallucinated counts.
6. **Always show the per-tehsil audit table** for district/division
   answers. Already enforced; keep it.
7. **Run both test files in CI on every PR.** The combined 500-case
   suite catches regressions in seconds.
