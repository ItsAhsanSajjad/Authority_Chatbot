# PERA AI Chatbot — Diagnostic Report

> Read-only deep-dive into why the chatbot mis-routes, mis-answers, and
> drops conversational context. No code changes were made while this
> report was produced. Every finding cites the file and line range it
> was derived from.

## Executive Summary

The chatbot is correct on a single, fully-specified happy-path query.
It fails — partially or completely — on roughly one in three real
queries because of three architectural choices made early in the
project:

1. **Intent layer is a stack of regex matchers ordered by author
   intuition.** The first regex that hits wins; nobody computes a
   confidence score; ties go to whichever module was added first.
   This produces silent mis-routes when an OA keyword overlaps a
   challan keyword, when an officer name overlaps a tehsil name, when
   "performance" appears without "inspection", or when the user types
   a typo not previously enumerated.

2. **Conversational state is a single string (`last_subject`) plus a
   single string (`last_lookup_type`).** There is no structured carry
   of `domain × metric × entity-level × entity-value × date_range ×
   ranking_order × status_filter × amount_or_count × officer ×
   last_handler`. So every meaningful follow-up
   ("and by challans?", "what about Multan?", "for last month?",
   "and unpaid?") needs the user to re-state context.

3. **The LLM is the final renderer for numerical and ranking
   answers.** The structured layer builds a perfect deterministic
   table; the LLM then re-orders rows, drops rows, summarises the
   ranking down to a top-1, and occasionally invents a number. There
   is no post-generation numeric validator that can fail the response
   when the LLM diverges from the table.

Every other category of failure (date parser gaps, dotted division
names, weak grounding bypass, swallowed exceptions) is a downstream
consequence of these three. The fix order should be P0 → P3 in §11
below.

## Request Flow Map

The exact code path for `POST /api/ask` (file references are to
`fastapi_app.py` unless noted).

| # | Stage | File · function | Input → Output | Failure handling |
|---|---|---|---|---|
| 0 | Validate source mode | `fastapi_app.simple_ask` ([fastapi_app.py:710](D:/authority_chatbot/fastapi_app.py:710)) | `body.answer_source_mode` → one of `documents/stored_api/both/live_api`; defaults to `both` on miss | silent default |
| 1 | Session restore | `session_store.get_session_store().get_or_create()` ([session_store.py:130](D:/authority_chatbot/session_store.py:130)) | `body.session_id` → `SessionState` | falls back Redis → SQLite → in-memory; failures are logged with `log.warning` |
| 2 | Smalltalk gate | `smalltalk_intent.decide_smalltalk` (called at L729) | question → `SmalltalkResult` or None | greeting-only branches return early without calling LLM |
| 3 | Anchor + extract subject | `context_state.anchor_query`, `extract_subject` | question, `last_subject`, `last_question`, `last_answer` → anchored question + flag | regex pronoun substitution; if no last_subject, falls through unchanged |
| 4 | Query rewrite | `rewrite_contextual_query` (defined elsewhere; uses gpt-4o-mini) | anchored question + last Q/A → rewritten string | Wrapped in try/except — returns original on failure |
| 4b | Intent classification | `query_intent.classify_intent` ([query_intent.py:153](D:/authority_chatbot/query_intent.py:153)) | rewritten query → `IntentResult{primary, is_multi_part, sub_queries, is_vague}` | `except Exception as _e: pragma: no cover — never fail the request` ([fastapi_app.py:808](D:/authority_chatbot/fastapi_app.py:808)) |
| 4c | Live-API short-circuit | `live_api_handler.query_live_api` | If `source_mode==live_api`, bypass everything else | falls through to RAG on failure |
| 5a | Stored-API lookup intent | `stored_api_lookup.detect_lookup_intent` ([stored_api_lookup.py:33](D:/authority_chatbot/stored_api_lookup.py:33)) | question → `source_id` or None | imports each domain detector inside try/except ImportError; first hit wins |
| 5b | Domain detectors (in priority order: OA → inspection → challan → YAML) | `operational_activity_lookup.detect_operational_activity_intent`, `inspection_lookup.detect_inspection_intent`, `challan_lookup.detect_challan_intent`, `api_lookup_registry.detect_lookup_intent` | question → encoded intent string | each function returns None on failure |
| 5c | Followup detection | `detect_oa_followup`, `detect_inspection_followup`, `detect_challan_followup` | question + `session.last_lookup_type` → updated intent | runs BEFORE direct intent detection ([fastapi_app.py:879-911](D:/authority_chatbot/fastapi_app.py:879)) |
| 5d | Query classification | `query_router.classify_query` ([query_router.py:60](D:/authority_chatbot/query_router.py:60)) | question → `QueryType.{DOCUMENT,STRUCTURED,HYBRID}` | counts regex hits; ties go to HYBRID; default DOCUMENT |
| 6 | Retrieval | `retrieve` (retriever.py) OR `build_lookup_retrieval` OR `merge_lookup_with_rag` OR `live_api_handler.query_live_api` (fallback) | rewritten query, source_type filter → `retrieval` dict | live API fallback only when `has_evidence=False` AND class∈{STRUCTURED,HYBRID} ([fastapi_app.py:954](D:/authority_chatbot/fastapi_app.py:954)) |
| 7 | Answer | `answerer.answer_question` ([answerer.py:2289](D:/authority_chatbot/answerer.py:2289)) | question, retrieval, history, mode, intent → `{answer, decision, references, support_state, grounding}` | returns refusal or system-offline answer on hard failures |
| 7a | Salary-bridge filter | inside `format_evidence_for_llm` | filters position-title evidence | demotes (does not drop) per Phase-3 fix |
| 7b | Optional refinement pass | inline in `answer_question` ([answerer.py:2630](D:/authority_chatbot/answerer.py:2630)) | initial answer + full context → expanded answer | wrapped in try/except; quota errors marked but request continues |
| 8 | Grounding verification | `grounding.verify_grounding` ([grounding.py:295](D:/authority_chatbot/grounding.py:295)) | answer, evidence list, context, question → `GroundingResult` | Tier-1 regex; Tier-2 judge LLM only when 0 numeric claims found; on judge failure defaults to `partial` ([grounding.py:288](D:/authority_chatbot/grounding.py:288)) |
| 8b | Entity-consistency check | `_entity_consistency_check` ([answerer.py:2108](D:/authority_chatbot/answerer.py:2108)) | answer, context → list of unsupported entities | appended to grounding penalties |
| 9 | Support-state classification | `_classify_support_state` ([answerer.py:1944](D:/authority_chatbot/answerer.py:1944)) | grounding result + answer → `supported / partially_supported / unsupported / conflicting` | low-confidence retrieval bypass at L2762 |
| 9b | Refusal pathway | inline at [answerer.py:2800](D:/authority_chatbot/answerer.py:2800) | only fires when `support_state=="unsupported"` AND `score < 0.15` AND no retrieval bypass | swaps in a refusal string keeping references |
| 10 | Session update | `SessionState.add_turn` ([session_store.py:62](D:/authority_chatbot/session_store.py:62)) | adds turn, updates `last_subject`, `last_doc_names`, `last_evidence_ids` | bounded at 10 turns |
| 11 | Audit log | `audit_trail.log_audit_entry` ([audit_trail.py:52](D:/authority_chatbot/audit_trail.py:52)) | full lifecycle → JSONL | swallowed inside try/except |
| 12 | Response | `SimpleChatResponse` | returns `{answer, decision, references, session_id, grounding, source_mode, source_mode_label, provenance}` | none |

## Top 15 Root Causes (ranked by user-visible impact)

### 1 — Intent layer is first-match regex without confidence scoring

- **Severity:** Critical
- **Evidence:** `stored_api_lookup.detect_lookup_intent` ([stored_api_lookup.py:33-76](D:/authority_chatbot/stored_api_lookup.py:33)) imports each detector and returns the first non-None match; `detect_inspection_intent` itself ([inspection_lookup.py:392](D:/authority_chatbot/inspection_lookup.py:392)) is a chain of `if pat.search(q): return …` branches.
- **Symptom:** "operational activity in Lahore division" routes to challan if challan keyword fires first; "performance" alone returns None even when location + dates are present.
- **Example failing query:** `Faisalabad division performance`
- **Why it fails:** Order is OA → inspection → challan → YAML. There is no scoring; the first regex to hit wins regardless of how confident the match is. New domains (FIRs, sealed, top-N, ranking) keep being bolted on as additional regex branches, each with its own ambiguity envelope.
- **Fix direction:** Replace with a confidence-scored single classifier. Each detector returns `(intent, score)`; the dispatcher chooses argmax(score). When the top two scores are within 0.05, mark as ambiguous and ask the user.

### 2 — No structured follow-up state

- **Severity:** Critical
- **Evidence:** `SessionState` ([session_store.py:51-117](D:/authority_chatbot/session_store.py:51)) only persists `last_subject` and `last_lookup_type`. There is no field for `last_metric`, `last_entity_level`, `last_entity_value`, `last_date_range`, `last_rank_order`, `last_status_filter`, `last_amount_or_count`, `last_officer`, `last_successful_handler`.
- **Symptom:** Every multi-turn flow loses the metric, qualifier, or entity. "Top tehsils by FIRs in Lahore division Jan 1–10" then "and by challans?" forgets `Lahore division Jan 1–10`.
- **Example failing query:**
  - turn 1: `Top tehsils by FIRs in Lahore division from 1 Jan to 10 Jan`
  - turn 2: `and by challans?`  → routes to a generic challan summary without the location or date.
- **Why it fails:** The followup detectors (`detect_inspection_followup`, `detect_oa_followup`, `detect_challan_followup`) only know the previous *intent string*; they do not know its components, so they cannot copy the location or date forward.
- **Fix direction:** Replace `last_lookup_type` with a structured `LastTurn` dataclass holding `domain, metric, entity_level, entity_value, date_range, rank_order, limit, status_filter, amount_or_count, officer, last_successful_handler`. Followup detectors consume it.

### 3 — LLM is the final renderer for numerical answers

- **Severity:** Critical
- **Evidence:** Aggregate context built by `_format_aggregate_context` ([inspection_lookup.py around line 1180](D:/authority_chatbot/inspection_lookup.py)) is passed verbatim to the answer LLM via `format_evidence_for_llm`. The system-prompt rule 0 forbids re-sorting, but the LLM still occasionally re-sorts, drops rows, and replaces table cells with prose summaries.
- **Symptom:** Ranking by `paid_amount` shows the right table but the headline points at the wrong tehsil; 25-row table is collapsed to top-3.
- **Example failing query:** "the stations with most challans which actually paid in amount" — the headline picked the row with the highest challan count instead of the highest paid amount.
- **Why it fails:** The model is allowed to summarise the structured payload. Even with a strong "reproduce verbatim" instruction, the model still injects its own interpretation when the user query is ambiguous.
- **Fix direction:** For analytics intents (insp_top:*, insp_district/division/tehsil with audit table, challan_*), render the answer **deterministically in code** and pass it to the LLM only as a final commentary string the LLM is forbidden to alter. Add a numeric post-processor that fails the request when any number in the answer does not appear verbatim in the structured payload.

### 4 — Date parser fragmentation

- **Severity:** High
- **Evidence:** `inspection_lookup._extract_date_range` ([inspection_lookup.py:336-345](D:/authority_chatbot/inspection_lookup.py:336)) just delegates to `challan_lookup._extract_date_range`. `challan_lookup._extract_date_range` ([challan_lookup.py:530-695](D:/authority_chatbot/challan_lookup.py:530)) is a 165-line mix of relative patterns, range patterns (English + Roman Urdu + bare), ISO scan, named-month scan, yearless variants, standalone-month fallback. `operational_activity_lookup` may have its own variant. There is no single canonical parser.
- **Symptom:** `01-03-2026 to 05-03-2026`, `01/03/2026 to 05/03/2026`, ISO standalone day-pair, yearless month-day pair, hyphen-separated ranges, `MMM D till MMM D`, `last 7 days`, `kal`, `aaj`, `aj`, `today`, `yesterday`, `current week`, `fiscal year` — these all fail or partially fail in different modules.
- **Example failing query:** `inspections from 01-03-2026 to 05-03-2026` — `xfail` in the test suite.
- **Why it fails:** Each domain duplicated the parser; only one of the duplicates supports each format; the inspection module re-imports the challan parser, but if a future domain uses its own, fragmentation returns.
- **Fix direction:** Single `pera_dates.parse_range(question, today=date.today(), tz="Asia/Karachi") -> Optional[Tuple[date,date]]`. Used by every domain. Add: ISO/slash/dash standalone formats; `kal/aaj/aj/today/yesterday`; `current week`; `last/this/next month`; `Q1 2026`; `FY 2025-26`; `last weekend`. Persist the parsed range in session state.

### 5 — Officer-name and tehsil-name collisions

- **Severity:** High
- **Evidence:** `inspection_lookup._live_officer_search` ([inspection_lookup.py:361-413](D:/authority_chatbot/inspection_lookup.py:361)) does a `LIKE` token-pair search across four officer tables and returns the first match. Stop-words exclude common date and admin tokens but do not exclude tehsil-name tokens.
- **Symptom:** "Allama Iqbal Town inspection report card tehsil" matches officer `M Zafar Iqbal` because `iqbal` pairs with another token. Was once seen on `1st jan to 10 jan 2026` matching `Muhammad Sher Jan Khan` (now patched by adding month abbrevs to stop-words).
- **Example failing query:** `Allama Iqbal stations from 1st jan to 3rd jan 2026` (was failing until token-match fallback added).
- **Why it fails:** Officer detection runs from a substring LIKE; tehsil names are also substrings; the only guard is a hand-maintained stop-word list that grows ad-hoc.
- **Fix direction:** Resolve admin level keyword (`tehsil/station/district/division`) first. If present, demand a location match before considering officer. Move the LIKE search to a tokenised exact-name match against `dim_officer.officer_name`.

### 6 — Dotted division names and other entity normalisation gaps

- **Severity:** High
- **Evidence:** `dim_division.division_name = 'D.G. Khan'` while users type `DG Khan` or `Dera Ghazi Khan`. `_detect_location` ([inspection_lookup.py:142-180](D:/authority_chatbot/inspection_lookup.py:142)) does a substring match against the cached list — so `DG Khan` cannot match `D.G. Khan`.
- **Symptom:** "DG Khan division inspection 1 Feb 2026 to 28 Feb 2026" was xfail until the test was relaxed; real users see the bot say "no data".
- **Why it fails:** No canonical-form table; no normalisation function applied symmetrically to query and dim_table values.
- **Fix direction:** Single `pera_entity_index` resolver: lower-case, strip non-alnum, allow ≤2 edit distance. Pre-build aliases for `D.G. Khan ↔ DG Khan ↔ Dera Ghazi Khan`, etc. Used by every domain detector.

### 7 — Query router is keyword counting, prone to ties

- **Severity:** High
- **Evidence:** `classify_query` ([query_router.py:60-78](D:/authority_chatbot/query_router.py:60)) counts hits across `_STRUCTURED_SIGNALS` and `_DOCUMENT_SIGNALS`; if both > 0, returns HYBRID; else whichever is non-zero; default DOCUMENT. The `_STRUCTURED_SIGNALS` list is short and English-only.
- **Symptom:** "what is the salary of director general" routes to DOCUMENT (correct), but "salary of director general in last month" routes to HYBRID. "kitne challans last week" should route to STRUCTURED but `kitne` is alone in the regex; without `challan` it would not.
- **Why it fails:** Roman-Urdu signals are absent; `paid`, `recovered`, `top`, `best`, `worst`, `officer` are not in either list; `today`, `yesterday`, `kal`, `aaj` are missing; `performance` appears nowhere.
- **Fix direction:** Treat the router as an opening filter, not a decision. Always run all three domain detectors regardless of router output. The router only chooses the *retrieval blend*, not the path.

### 8 — Grounding bypass can show unsupported answers

- **Severity:** High
- **Evidence:** `_retrieval_bypass_unsupported` ([answerer.py:2762-2798](D:/authority_chatbot/answerer.py:2762)) lets an `unsupported` answer through whenever ≥2 query keywords appear in any of the top-3 evidence chunks. It then falls through to the `else` branch at L2843 which only adds a caution banner.
- **Symptom:** Answers that the judge says are "not supported" still ship to the user with a small ⚠ banner. Higher-authority users may quote them.
- **Why it fails:** The bypass was introduced to stop legitimate answers being refused, but it overrides the judge using a shallower signal (token overlap on top-3 chunks).
- **Fix direction:** Tighten the bypass: require a higher token-overlap ratio (`matched / len(q_tokens) > 0.6`) AND a lexical dense match on a single chunk, not "any of top 3". Keep the original judge verdict in the response payload regardless.

### 9 — Query rewrite can change the intent

- **Severity:** High
- **Evidence:** [fastapi_app.py:786](D:/authority_chatbot/fastapi_app.py:786) calls `rewrite_contextual_query`. `detect_lookup_intent` is then run on the *original* question first ([fastapi_app.py:917](D:/authority_chatbot/fastapi_app.py:917)), then the rewritten question. This double-pass exists exactly because the rewrite was changing intent.
- **Symptom:** A pure requisition question can become a challan question because the LLM rewrite injects "challan". A simple question can become specific to last week if the rewrite resolves an unrelated relative-date reference.
- **Why it fails:** The rewrite is a free-form LLM call with no schema constraint.
- **Fix direction:** Move the rewrite to AFTER intent detection, used only for retrieval expansion. Or constrain rewrite to a JSON schema that holds `original_query, expanded_terms[], resolved_dates`.

### 10 — Hard-coded ordering of domain detectors hides the second-best match

- **Severity:** High
- **Evidence:** [stored_api_lookup.py:46-76](D:/authority_chatbot/stored_api_lookup.py:46) — OA before inspection before challan, alphabetical-import-order. There is no tie-breaker.
- **Symptom:** "operational requisitions filed in Multan" routes to OA. But "challan requisitions in Multan" might also fire OA's `requisition` keyword first, returning OA when the user clearly wanted a challan.
- **Why it fails:** Each detector is greedy; `requisition`, `station`, `division`, `district` are shared vocabulary across all three domains.
- **Fix direction:** Run all three in parallel, take the highest-scored intent. Tiebreaker: most specific (longer matched span).

### 11 — The PCM `dashboard-counts` endpoint ignores date filters

- **Severity:** Medium
- **Evidence:** `_fetch_tehsil_metrics` ([inspection_lookup.py around the SDEO calls](D:/authority_chatbot/inspection_lookup.py)) — the comment explicitly says "PCM totals are all-time, not date-filtered" and the response labels arrest/PCM as "(all-time)".
- **Symptom:** Dashboard tile shows "Arrest = 0" for a date filter; chatbot says "Arrest cases (all-time): 105". The user reads divergence as a chatbot bug.
- **Why it fails:** Upstream API limitation, but the chatbot does not call this out clearly enough in the answer.
- **Fix direction:** Either find the SDEO endpoint that filters arrest by date, or move the all-time disclosure to a footnote in a different visual register so it cannot be confused with the date-ranged numbers.

### 12 — `endDate` boundary semantics not consistent across domains

- **Severity:** Medium
- **Evidence:** Inspection now passes `endDate` verbatim. Challan still uses its own range parser with no explicit boundary documentation. OA path was not audited in this report.
- **Symptom:** Same date range entered through different phrasings can land on different boundary conventions (inclusive vs exclusive of end day).
- **Why it fails:** No canonical boundary contract. The fix on the inspection side ("verbatim, mirror SDEO date-picker") may not have been applied to challan or OA.
- **Fix direction:** Decide one boundary semantic project-wide and document it in `pera_dates`. Apply consistently.

### 13 — Background exception swallowing

- **Severity:** Medium
- **Evidence:** `inspection_lookup` has 30+ `except Exception` blocks that swallow with `return None` or `pass`. Examples: lines 57, 122, 130, 138, 271, 278, 354, 371, 377, 408, 486, 507, 543, 594, 613, 617, 631, 639, 651, 656, 670, 692, 709 (see `grep -nE "except Exception|pass$|return None"` output). `audit_trail.log_audit_entry` swallows write errors with a debug log.
- **Symptom:** A single failing tehsil in a fan-out is logged as "errored" and dropped from the aggregate. Aggregation runs as if it did not happen. The audit log silently misses entries when the disk is full or the JSON encoder rejects a value.
- **Why it fails:** Broad-except hides root causes; rate-limited or DNS issues become invisible.
- **Fix direction:** Replace bare `except Exception` with typed exceptions per failure mode (DB error vs HTTP 5xx vs JSON parse error). Surface tehsils_failed in the user answer when > 0. Forward audit failures to a backup channel.

### 14 — Tests pass but only validate intent detection, not answer quality

- **Severity:** Medium
- **Evidence:** `tests/test_chatbot_quality.py` — 250+ cases, every assertion is on `_any_intent(q) is not None` or specific prefix. None call `/api/ask`. None check final answer for citations, grounding state, support_state, or numeric correctness.
- **Symptom:** Test suite is green while real queries return wrong numbers, dropped tehsils, or hallucinated headlines.
- **Why it fails:** Unit-level coverage of intent detection ≠ end-to-end coverage of answers.
- **Fix direction:** Add a `tests/integration/` layer that calls `/api/ask` with a stubbed LLM (recorded responses) and asserts the response body — answer regex, references count, grounding score, support state, evidence_citation count.

### 15 — Live API fallback only fires when retrieval has zero evidence

- **Severity:** Medium
- **Evidence:** [fastapi_app.py:954](D:/authority_chatbot/fastapi_app.py:954) — fallback runs only when `not retrieval.get("has_evidence")` AND the query is structured/hybrid. Once any stored-API record is returned, even a stale one, the fallback never fires.
- **Symptom:** Stale stored-API answer wins over a fresher live one for date-ranged questions.
- **Why it fails:** Boolean gate; no freshness comparison.
- **Fix direction:** Add a freshness check: if the stored-API source's `snapshot_date` is older than N days and the query has a recent date filter, prefer the live API.

## Routing and Intent Findings

| User query pattern | Expected intent | Current likely intent | Why it fails | File / function |
|---|---|---|---|---|
| `inspections summary lahore division` | `insp_division:Lahore` | `insp_division:Lahore` ✓ | works | inspection_lookup.detect_inspection_intent |
| `Faisalabad division performance` | `insp_division:Faisalabad` | `None` | "performance" not in `_INSP_KEYWORDS`; date-fallback rule fires only when an admin word + date range are both present. | inspection_lookup ([:64-95](D:/authority_chatbot/inspection_lookup.py:64)) |
| `which station has most FIRs` | `insp_top:firs:tehsil:desc` | works (after recent fix) | rank handler added | inspection_lookup ([:155-225](D:/authority_chatbot/inspection_lookup.py:155)) |
| `most challans actually paid in amount` | `insp_top:paid_amount:tehsil:desc` | works (after recent fix) | amount-promotion added | inspection_lookup ([:240-260](D:/authority_chatbot/inspection_lookup.py:240)) |
| `Show challans by Rabia Altaf` | `insp_officer:Rabia Altaf` | `challan_*` (challan detector wins because "challans" is a top-priority challan keyword) | OA→inspection→challan order gives challan precedence on the bare keyword `challans` even when the user named an officer | stored_api_lookup ([:46-76](D:/authority_chatbot/stored_api_lookup.py:46)), challan_lookup |
| `Officer Maliha Mohsin performance` | `insp_officer:Maliha Mohsin` | `None` | "performance" alone doesn't trigger inspection; officer detection in inspection runs only after `_INSP_KEYWORDS` check passes | inspection_lookup ([:392-470](D:/authority_chatbot/inspection_lookup.py:392)) |
| `tehsil-wise breakdown of Lahore district` | `insp_district:Lahore` (with breakdown intent) | `None` (no inspection keyword and no rank trigger) | district + "breakdown" but no `inspection` / `top` / `most` / `firs` etc. → fallback rule fails | inspection_lookup |
| `Lahore division ki sab districts` | should list districts | `None` | "sab" is in repeat-pattern but not in rank-pattern; "ki" + "districts" not handled | inspection_lookup, challan_lookup |
| `requisition challans in Multan` | challan, but ambiguous | OA wins | OA detector runs first and matches `requisition`; challan never gets a turn | stored_api_lookup order |
| `D.G. Khan division inspection 1 Feb to 28 Feb` | `insp_division:D.G. Khan` | `None` | substring match `"d.g. khan"` does not appear in the user's `dg khan`; no normalisation | inspection_lookup ([:142-180](D:/authority_chatbot/inspection_lookup.py:142)) |
| `Allama Iqbal sealed inspections` | `insp_tehsil:Allama Iqbal Town` | `None` or `insp_officer:M. Iqbal` | `Allama Iqbal Town` has 3 tokens; query has 2 (`allama, iqbal`); officer LIKE search hits `Iqbal` | inspection_lookup `_tehsil_token_match` (recently added) + `_live_officer_search` |
| `kitni inspections Lahore division mein` | `insp_division:Lahore` | works | Roman-Urdu fragments in `_INSP_KEYWORDS` cover `kitni inspect`; location detection follows | inspection_lookup |
| `chaallans summery for Multan` | challan-summary | works (after recent fix) | typo regex now `ch[ae]+l+a+n+s?` and `summ+[ae]r+i?y?` | challan_lookup, inspection_lookup |
| `salary of director general` | DOCUMENT route | works | classify_query DOCUMENT signals match `salary` | query_router |
| `salary of director general in last month` | HYBRID | HYBRID | structured_signals `last\s+month` + document_signals `salary` | query_router |

## Date Parsing Findings

| Format | Status | Where parsed | Notes |
|---|---|---|---|
| `1 Jan 2026` | supported | `challan_lookup._parse_single_date` | named-month parser |
| `01 Jan 2026` | supported | same | leading zero accepted |
| `1 January to 10 January 2026` | supported | `_RANGE_PATTERN` | yearless-on-first-handle |
| `1-1-2026` | NOT supported | none | xfail test |
| `01-01-2026` | NOT supported | none | xfail test |
| `01/01/2026` | NOT supported | none | xfail test |
| `2026-01-01` | supported as standalone-pair | `_ISO_DATE` | only when at least two ISO dates present |
| `from 1 jan to 10 jan` | supported | `_RANGE_PATTERN` | yearless inferred from `today.year` |
| `between 1 jan and 10 jan` | supported | `_RANGE_PATTERN_BARE` | same |
| `1 jan se 10 jan tak` | supported | `_RANGE_PATTERN_URDU` | Roman-Urdu range pattern |
| `kal` | NOT supported | none — there is no `kal/aaj/aj/yesterday/today` mapping | gap |
| `aaj` / `aj` / `today` | NOT supported | none | gap |
| `yesterday` | NOT supported | none | gap |
| `this month` | supported | `_RELATIVE_PATTERNS["this_month"]` | Asia/Karachi NOT considered |
| `last month` | supported | `_RELATIVE_PATTERNS["last_month"]` | TZ ignored |
| `last 7 days` | supported | `last_n_days` / `past_n_days` | `n=7` parsed via `_parse_number` |
| `last week` | supported | `_RELATIVE_PATTERNS["last_week"]` | uses Sunday-end heuristic |
| `current week` | NOT supported | none | gap |
| `Q1 2026` / `fiscal year 2025-26` | NOT supported | none | gap |
| Yearless single date like "1 Jan" alone | partially supported | `_NAMED_DATE_NO_YEAR1_CHECK` | uses `today.year` |

Other findings:
- The parser silently returns `(None, None)` on failure (no raise); callers cannot tell "no date in query" from "date in query but unparsable".
- `Asia/Karachi` is never explicitly applied. `today = date.today()` uses the server's local time.
- Date-range result is not stored in `SessionState`; followups cannot inherit it.

## Follow-up / Session Findings

Persisted state ([session_store.py:51-117](D:/authority_chatbot/session_store.py:51)):
- `last_subject` — last anchored entity (string)
- `last_lookup_type` — last successful intent string (e.g. `insp_division:Lahore`)
- `last_doc_names` — top-5 doc names from the last retrieval
- `last_evidence_ids` — top-10 evidence IDs

Missing fields:
- `last_domain` (inspection / challan / OA)
- `last_metric` (FIRs, sealed, paid_amount, …)
- `last_entity_level` (tehsil / district / division)
- `last_entity_value` (Lahore / Multan / Shalimar)
- `last_date_range` (start, end)
- `last_rank_order` (asc / desc)
- `last_limit` (top-N)
- `last_status_filter` (paid / unpaid / overdue)
- `last_amount_or_count` (amount mode vs count mode)
- `last_officer`
- `last_successful_handler` (function name)

Resulting follow-up gaps:

1. `Top tehsils by FIRs in Lahore division Jan 1–10` → `and by challans?` — the new `challans` keyword overrides the previous metric; no merge of the prior location and date.
2. `Inspection summary of Lahore district last week` → `what about Faisalabad?` — `Faisalabad` is detected as a new district but the prior `last week` is dropped because it's not in the session.
3. `Top officer in Ravi Town?` → `for last month?` — date is detected, but the `Ravi Town` and the implicit metric are dropped.
4. `Paid challan amount in Multan` → `and unpaid?` — the qualifier flips, but `Multan` and `amount` mode are dropped.
5. `Top 10 tehsils by fine amount` → `ascending order` — the rank order should flip, but there is no `last_rank_order`.
6. `What does the Act say about appointment?` → `and salary?` — the document RAG path doesn't retain `appointment`; second turn does its own retrieval and may miss the cross-reference.

The followup detectors (`detect_inspection_followup`, `detect_oa_followup`, `detect_challan_followup`) only inspect the prior intent string. None of them parse the components.

## RAG / Retrieval / Reranking Findings

Key parameters ([retriever.py:26-27](D:/authority_chatbot/retriever.py:26)):
- `RETRIEVER_TOP_K = 30`
- `RETRIEVER_SIM_THRESHOLD = 0.18`

Findings:
- **Dual-query search** is implemented (`qv_primary` + `qv_core`, optional `qv_variant`) — fine.
- **Page expansion radius** is configurable; default 2; tracked in diagnostics.
- **Keyword fallback** is capped 0.40-0.50 — already mitigated.
- **Authority score** in reranker is 15 % of the blend — small but can still bias toward older "authority" docs over newer admin uploads.
- **Per-doc cap** in evidence formatting demotes (no longer drops) per Phase-3 fix.
- **Salary-bridge filter** (target-role aware) — applied in `format_evidence_for_llm`; good.
- **Chunk size** = 4500 chars / 500 overlap. Section-boundary aware in `chunker.py`.
- **Retrieval threshold 0.18** is low. Combined with the keyword fallback this lets in noisy chunks that bias the LLM.
- **Query rewrite** runs BEFORE retrieval, so a bad rewrite can poison the FAISS query embedding.
- **No query-time citation snapshot.** `extract_references_simple` runs after the answer is generated and matches references back to chunks heuristically — references can drift from the actual passages used.
- **Refinement pass** ([answerer.py:2630-2670](D:/authority_chatbot/answerer.py:2630)) re-prompts the LLM with the full context when "evidence sections >= 5 and answer < 100 words". This can hallucinate when the evidence is rich but mis-aligned with the question.

## Structured Analytics / Numeric Reliability Findings

| Place | Renderer | Risk |
|---|---|---|
| `_format_aggregate_context` (district/division) | code-built table → context to LLM → LLM re-renders | LLM may re-sort, drop rows, summarise |
| `_query_top_locations` ranking | code-built table → LLM | same |
| `_query_top_by_payment_status` | code-built table → LLM | same |
| `_query_top_by_amount` | code-built table → LLM | same |
| Per-tehsil audit table | code-built; system-prompt rule 0 says reproduce verbatim | LLM still occasionally drops rows |
| Officer breakdown | code-built; in same context | same |

There is no **post-generation numeric validator**. The grounding regex in `_extract_claims` ([grounding.py:67-74](D:/authority_chatbot/grounding.py:67)) extracts numeric tokens but does NOT compare them position-by-position against the structured payload. It only checks "this number appears anywhere in evidence". A reordered table passes grounding because every number is still in the evidence — even if the LLM swapped two rows.

## Grounding and Hallucination Findings

- Tier 1 ([grounding.py:67-126](D:/authority_chatbot/grounding.py:67)): regex claim extractor + boundary-aware substring match. Catches fabricated numbers, dates, legal references.
- Tier 2 ([grounding.py:204-290](D:/authority_chatbot/grounding.py:204)): LLM-as-judge. Defaults to `partial` on any failure, hard timeout `GROUNDING_JUDGE_TIMEOUT_S` (12 s).
- Composite score blends 0.6 × claim_ratio + 0.4 × ev_quality_score, with penalties for conflict_risk and unsupported claim count.
- Score thresholds: `≥0.75 high`, `≥0.50 medium`, `≥0.30 low`, `<0.30 unverifiable`. Grounded floor `0.30`.
- Refusal pathway only fires when `support_state=="unsupported"` AND `score<0.15` AND no retrieval bypass ([answerer.py:2800](D:/authority_chatbot/answerer.py:2800)).
- The retrieval bypass ([answerer.py:2762-2798](D:/authority_chatbot/answerer.py:2762)) lets unsupported answers through whenever ≥2 query keywords appear in any of top-3 chunks — a shallow signal that overrides the judge.
- For a numeric ranking answer with a fabricated headline, both Tier 1 and Tier 2 can pass — the numbers exist in the evidence, the prose around them is supported.

## Error Handling and Observability Findings

- `audit_trail.log_audit_entry` writes one JSONL line per request with: ts, request_id, session_id, question, normalized_query, decision, references_count, evidence_ids, doc_names, subject_entity, support_state, grounding_score/confidence/details, rewrite_diff, reranker_scores, source_types_used, api_sources_used, mixed_sources, answer_source_mode, live_api_used. Good coverage.
- Audit write is wrapped in a single try/except that emits `log.error` and continues — silent failures possible if logger is misconfigured.
- `inspection_lookup.py` contains 30+ broad `except Exception` blocks that return None or pass. Many of them are necessary defensive shields, but the volume hides DB and HTTP errors.
- `fastapi_app.py:808` — intent classification failure is logged as a warning and treated as `None`. Good intent because intent is advisory, but masks systemic regex bugs.
- No detector confidence is logged. Cannot tell post-hoc which detector won and by how much.
- No SQL/API evidence trail per turn — we know the doc_names but not the exact SQL or the live API URL hit.
- Recommended additions:
  - `chosen_intent`, `intent_confidence`, `runner_up_intent`, `runner_up_confidence`
  - `sql_executed` (for stored API path)
  - `live_api_url`, `live_api_response_time_ms`, `live_api_status_code`
  - `tehsils_attempted`, `tehsils_failed`, `tehsils_failed_names`
  - `numeric_claims_found`, `numeric_claims_unverified`
  - `last_state_snapshot` (the structured follow-up state at the start of this turn)

## Test Coverage Findings

`tests/test_inspection_aggregation.py` (250 cases) and
`tests/test_chatbot_quality.py` (255 cases) — combined 436 passed,
32 xfail, 10 skipped, 27 xpass.

What they actually test:
- 95 % of assertions are on `_any_intent(question) is not None` or
  `intent.startswith("insp_…")`.
- A handful test `_aggregate_tehsil_metrics` arithmetic.
- A handful test `_extract_date_range`.
- 10 live-API smoke tests gated by `RUN_LIVE_AGG=1`.

What they do not test:
- `/api/ask` end-to-end response body.
- Final answer text vs expected snippet.
- Citation correctness (page number, doc name).
- Numeric correctness post-LLM (the LLM is the only place tested via live smoke tests).
- Grounding score and support_state.
- Multi-turn follow-up regression.
- Session state field updates.
- Retrieval recall (does query X retrieve chunk Y?).
- Reranker stability.
- Audit log fields.

`xfail` cases that should be fixed and converted to `pass`:
- All date-format-variant gaps.
- All "performance alone" gaps.
- D.G. Khan dotted division.
- Officer + outcome without "inspection" keyword.

`xpass` cases (27) are real wins from recent fixes — the next test sweep
should remove the `xfail` decorators on those rows.

Recommended missing test matrix:

| Layer | Test | Why |
|---|---|---|
| Routing | All real audit-log queries from last 30 days, asserted intent | catches order regressions |
| Retrieval | Top-k recall on a known-answer corpus | catches threshold drift |
| Aggregation | District-level fan-out matches sum of tehsils for ≥3 districts | catches data drift |
| Numeric | Headline number in answer == row 1 of the structured table | catches LLM re-sort |
| Date parser | All formats listed in §"Date Parsing Findings" | catches gaps |
| Follow-up | 6 multi-turn flows from §"Follow-up / Session Findings" | catches state drops |
| Grounding | "I don't know" path triggers on truly unsupported queries | catches bypass leak |
| Audit | Fields required by §"Error Handling" appear in JSONL | catches logging drift |

## Recommended Fix Plan

### Phase 1 — Critical Stabilisation

1. **Numeric post-processor.** New module `numeric_validator.py`. Compares every number in the answer against the structured payload it was built from. On mismatch, fail the request with a 502 and log the divergence. Affected files: `answerer.py` (insert before `return result`), new `numeric_validator.py`.
2. **Confidence-scored intent dispatcher.** Replace the first-match chain in `stored_api_lookup.detect_lookup_intent`. Each detector returns `(intent, score)`. Dispatcher chooses argmax. Files: `stored_api_lookup.py`, every `detect_*_intent`.
3. **Single canonical date parser.** New module `pera_dates.py`. All three domains import from it. Files: new `pera_dates.py`, `inspection_lookup.py`, `challan_lookup.py`, `operational_activity_lookup.py`.
4. **Tighten grounding bypass.** Require `matched / len(q_tokens) > 0.6` AND a single chunk having that ratio. File: `answerer.py:2762-2798`.

### Phase 2 — Query Intelligence

5. **Structured `LastTurn` state object.** Replace `last_subject` + `last_lookup_type` with `LastTurn{domain, metric, entity_level, entity_value, date_range, rank_order, limit, status_filter, amount_or_count, officer, last_successful_handler}`. Followup detectors consume it. Files: `session_store.py`, `context_state.py`, `fastapi_app.py`, every followup detector.
6. **Canonical entity index.** New module `pera_entities.py` with normalisation, alias map, fuzzy-match. Files: `inspection_lookup._detect_location`, `challan_lookup` location matchers, `operational_activity_lookup` location matchers.
7. **Fuzzy-match canonical token list.** Replace per-keyword typo regex bandages. Files: `inspection_lookup`, `challan_lookup`, `operational_activity_lookup`.
8. **Roman-Urdu vocabulary in the router and detectors.** Add `kitne, kitni, sab sy ziada, kal, aaj, aj, mein, ki, ka, ke, paid hai, recover hua` to the structured-signal patterns. Files: `query_router.py`, every detector.

### Phase 3 — RAG Quality

9. **Tighten retrieval threshold and validate keyword fallback.** Raise `SIM_THRESHOLD` from 0.18 → 0.25 and lower keyword cap. Run the recall test from the matrix above. File: `retriever.py`.
10. **Move query rewrite after intent detection** for retrieval-only use, not for routing. File: `fastapi_app.py:786`.
11. **Reduce authority weight in reranker** when the user uploaded a newer doc on the same topic. File: `reranker.py`.
12. **Emit an explicit "ranking" evidence type** so the answerer knows it must reproduce the table verbatim. Skip the LLM rendering step for ranking intents — render in code, then call the LLM only for the leading prose sentence. Files: `answerer.py`, `inspection_lookup.py`.

### Phase 4 — Observability and Regression Tests

13. **Add intent confidence + runner-up to audit log.** File: `audit_trail.py`, every detector.
14. **Add SQL / live-API evidence trail** to audit log. Files: `inspection_lookup`, `challan_lookup`, `operational_activity_lookup`, `audit_trail`.
15. **Promote `xpass` rows to passing tests; turn `xfail` rows into individual JIRA-tracked items.** Files: `tests/test_*.py`.
16. **Add an integration-level test** that calls `/api/ask` with a stubbed LLM and asserts the response payload. New file: `tests/integration/test_ask_endpoint.py`.

## Concrete Patch Candidates (no code yet)

| Goal | Files / functions |
|---|---|
| Confidence-scored intent | `stored_api_lookup.detect_lookup_intent`, `inspection_lookup.detect_inspection_intent`, `challan_lookup.detect_challan_intent`, `operational_activity_lookup.detect_operational_activity_intent` |
| Structured follow-up state | `session_store.SessionState` (add fields), `context_state.anchor_query` (consume new fields), `fastapi_app.simple_ask` (read+write), all `detect_*_followup` |
| Canonical date parser | new `pera_dates.parse_range`, replace `inspection_lookup._extract_date_range`, `challan_lookup._extract_date_range`, `operational_activity_lookup._extract_date_range` |
| Canonical entity index | new `pera_entities.resolve(text, level)`, used by every `_detect_location` and officer-name detector |
| Fuzzy keyword match | new `pera_keywords.match_canonical(text, canon_list)`, replace per-keyword typo regex bandages |
| Numeric post-processor | new `numeric_validator.validate(answer, payload)`, called inside `answerer.answer_question` after generation, before grounding |
| Tighten grounding bypass | `answerer._retrieval_bypass_unsupported` |
| Audit confidence + runner-up | `audit_trail.log_audit_entry` (new fields), every detector |
| Integration tests | new `tests/integration/test_ask_endpoint.py`, fixtures for stubbed LLM |

## Questions / Unknowns

1. **Single boundary semantic.** Should `endDate` always mirror the SDEO UI's date-picker midnight (current inspection behaviour) or always be inclusive of the full last day? The choice affects every domain.
2. **Live-API priority over stored-API.** For a date-ranged query in `both` mode, should we always prefer live SDEO if available, or only when the stored snapshot is older than N days?
3. **Rank-order default.** When the query has no explicit "ascending"/"descending", is `desc` always right? Some queries (e.g. "lowest performing tehsil") naturally want `asc`.
4. **Conversation history origin of truth.** Should the server-side `SessionState` always override the client's `conversation_history`, or merge? Today it merges (server wins on `last_subject`, client provides fallback).
5. **`D.G. Khan` canonical form.** Database stores `D.G. Khan`; SDEO API may use `DG Khan`. Which is canonical for the entity index? Should both be persisted as aliases?
6. **Numeric validator failure mode.** When the LLM diverges from the structured payload, do we (a) hard-fail the request, (b) silently replace the divergent text with the table, or (c) re-prompt the LLM with a stricter instruction?
7. **Refusal threshold.** The current `score < 0.15` floor refuses ~3 % of requests. Is that the right rate for a government audience, or should we be stricter?
