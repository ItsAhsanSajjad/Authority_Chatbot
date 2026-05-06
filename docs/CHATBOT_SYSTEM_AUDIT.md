# PERA AI Chatbot — Complete System Audit & Issue Inventory

> Comprehensive diagnostic of the chatbot's intent layer, retrieval
> layer, aggregation layer, and answer layer. Lists every known issue,
> the data source involved, and the engineering fix.

## Executive summary

The chatbot is correct on the **happy path** (a single, fully-specified
query — clear admin level, clear date range, clear metric). It still
fails on **multi-axis queries** (metric × qualifier × level × date) and
on **conversational follow-ups** that depend on a prior turn's state.
Every recent failure traces back to one of three root causes:

| # | Root cause | Symptoms |
|---|---|---|
| **R1** | Intent layer is regex-only — no fuzzy match, no embedding fallback. | Typos, mid-word breaks, and unusual phrasings get routed to a generic summary instead of the specific intent. |
| **R2** | Metric → data-source mapping is incomplete. Counts came from one table; amounts from another; date-ranged metrics from a third (the live SDEO API). New metrics (e.g. `paid_amount`) require a new handler. | "Most challans actually paid" returned counts when the user asked for amounts. "Which station files most FIRs" needed a ranking handler that didn't exist. |
| **R3** | Conversation state isn't persisted between turns. Every query is reinterpreted from scratch. | Follow-up "tell me in amount" doesn't carry the previous "ranking by paid challans" context. |

## 1. Architecture map

```
                 ┌──────────────────────────────────────────────┐
                 │                FastAPI /api/ask              │
                 └────────────────────┬─────────────────────────┘
                                      ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ Stage 1 — Pre-processing                                      │
   │   typo_fixer · roman_urdu_fold · NFC · strip controls        │
   └────────────────────┬─────────────────────────────────────────┘
                        ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ Stage 2 — Smalltalk / session lookup                         │
   │   detects "hello/thanks", restores last_intent, last_entity  │
   └────────────────────┬─────────────────────────────────────────┘
                        ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ Stage 3 — Query rewrite (gpt-4o-mini)                        │
   │   abbrev expansion, synonym normalisation                    │
   └────────────────────┬─────────────────────────────────────────┘
                        ▼
   ┌──────────────────────────────────────────────────────────────┐
   │ Stage 4 — Query router (DOCUMENT / STRUCTURED / HYBRID)      │
   └─────────┬────────────────────────────────┬───────────────────┘
             ▼                                ▼
   ┌──────────────────┐             ┌──────────────────────────┐
   │ FAISS retrieval  │             │ Domain detectors         │
   │ — PERA Acts,     │             │ inspection_lookup        │
   │   regulations,   │             │ challan_lookup           │
   │   schedules      │             │ operational_activity     │
   └─────────┬────────┘             └─────────────┬────────────┘
             │                                    ▼
             │             ┌────────────────────────────────────┐
             │             │ Executor branches                  │
             │             │ • insp_summary                     │
             │             │ • insp_division / district / tehsil│
             │             │ • insp_officer / cnic / repeat     │
             │             │ • insp_top:<metric>:<level>:<order>│
             │             │ • challan_*                        │
             │             │ • oa_*                             │
             │             └────────────────────────────────────┘
             │                                    │
             ▼                                    ▼
   ┌─────────────────────────────────────────────────────────────┐
   │ Stage 5 — Evidence assembly                                  │
   │   reranker · per-doc cap · salary-bridge · audit tables     │
   └────────────────────┬────────────────────────────────────────┘
                        ▼
   ┌─────────────────────────────────────────────────────────────┐
   │ Stage 6 — Answerer (gpt-4o)                                 │
   │   system prompt rule-0 forces audit-table preservation      │
   └────────────────────┬────────────────────────────────────────┘
                        ▼
   ┌─────────────────────────────────────────────────────────────┐
   │ Stage 7 — Grounding verifier (Tier-1 regex + Tier-2 LLM-J)  │
   └────────────────────┬────────────────────────────────────────┘
                        ▼
                 (final answer JSON)
```

## 2. Data sources mapped to query types

| Query type | Data source | Date-filtered? | Used by |
|---|---|---|---|
| Document / policy | FAISS + PostgreSQL chunks | n/a | DOCUMENT route |
| Inspection counts (cumulative) | `inspection_performance` (PG) | snapshot-day only | `insp_*` |
| Inspection counts (date-ranged) | SDEO `/inspections-summary` | yes | `_query_tehsil_live` |
| Inspection KPIs (fines, paid/unpaid) | SDEO `/top-kpis` + `/challan-status-breakdown` | yes | `_fetch_tehsil_metrics` |
| Arrest / PCM | SDEO `/Pcm/dashboard-counts` | **no** (returns all-time) | aggregator footnote |
| Challan ranking by **count** | `challan_tehsil_breakdown` | no | `_query_top_by_payment_status` |
| Challan ranking by **amount** | `challan_data` (SUM rows) | no | `_query_top_by_amount` |
| Challan totals / breakdown | `challan_data`, `challan_by_*` | yes | `challan_lookup` |
| Operational activity | `operational_activity*` tables | yes | `operational_activity_lookup` |
| Live officer details | SDEO `/Pcm/officer-inspections` | partial | `_query_insp_officer` |

## 3. Issue inventory (every known gap, by component)

### 3.1 Intent layer

| # | Issue | Example query | Status |
|---|---|---|---|
| I-1 | Date parser misses `DD-MM-YYYY`, `DD/MM/YYYY`, ISO standalone day-pairs, yearless month-day pairs, hyphen-separated ranges, `MMM D till MMM D`. | `01-03-2026 to 05-03-2026` | xfail in tests |
| I-2 | "performance" alone (without "inspection") doesn't trigger the inspection intent. | `Faisalabad division performance` | xfail |
| I-3 | Officer-name detection runs greedy LIKE on token pairs. Month abbreviations (`jan, feb, …`) used to leak in and false-match `Muhammad Sher Jan Khan`. **Patched**: month abbrevs in stop-word set; location detection runs first. | `1st jan to 10 jan 2026` | fixed |
| I-4 | Tehsil names matched only as full substrings. "Allama Iqbal" couldn't hit "Allama Iqbal Town". **Patched**: `_tehsil_token_match` ignores suffix tokens (`Town`, `Cantt`, `City`, `Saddar`, `HQ`). | `allama iqbal stations from 1 jan` | fixed |
| I-5 | Dotted division names not normalised (`D.G. Khan` vs `DG Khan`). | `DG Khan division` | xfail |
| I-6 | Typo regex bandages don't scale (`chaallans, challanns, summery, summmery` each had to be added by hand). Real fix is Levenshtein + canonical token list. | `summmery`, `chaallans` | partially patched |
| I-7 | Officer + outcome without `inspection` keyword routes to officer instead of inspection. | `Show challans by Rabia Altaf` | xfail |
| I-8 | Cross-level rollup phrasings (`which division has most challans`) initially returned generic summary. **Patched**: ranking handler. | `which station has most FIRs` | fixed |
| I-9 | Payment qualifier vs count qualifier ambiguity. "most challans which actually paid" used to rank by total challans, ignoring `paid` filter. **Patched**: payment-status metric mapping in `_RANK_METRIC_MAP`. | `stations with most paid challans` | fixed |
| I-10 | Amount qualifier vs count qualifier. "most paid in amount" returned counts not Rs totals. **Patched**: `_AMOUNT_QUALIFIER_RE` promotes to `*_amount` metric backed by `challan_data` SUM. | `most challans actually paid in amount` | fixed |
| I-11 | Officer-name / tehsil-name collision (`Allama Iqbal Town` vs officer `M. Iqbal`). | rare | xfail |
| I-12 | Smalltalk → no domain intent must return `None`, not a stale prior intent. | `hi`, `help` | passing |
| I-13 | Followup memory not implemented as a structured session field. Multi-turn queries like "and what about Multan?" require user to re-state context. | conversational | open |

### 3.2 Retrieval / aggregation layer

| # | Issue | Status |
|---|---|---|
| R-1 | Earlier code added `+1 day` to `endDate` to include the full day. Conflicts with the SDEO UI's date-picker default of midnight (00:00). **Patched**: `endDate` passed verbatim. | fixed |
| R-2 | District / division + date-range queries used to hit a stale snapshot. **Patched**: live fan-out via `dim_tehsil ⨝ dim_district ⨝ dim_division` and parallel SDEO calls. | fixed |
| R-3 | `Pcm/dashboard-counts` returns all-time arrest/PCM regardless of date params. Chatbot now labels these "(all-time)" but cannot match a date-filtered dashboard tile. | open (requires upstream fix) |
| R-4 | Hierarchy lookups re-run SQL on every request. Should cache for 5 min in-process. | open |
| R-5 | Per-tehsil API responses are not memoised — repeat dashboard prompts re-fan-out. | open |
| R-6 | No circuit-breaker. If many tehsils 5xx, the response is partial and the user is told "sum of N/M" but the LLM tends to ignore this. | open |
| R-7 | Cumulative `paid_count`, `unpaid_count`, `fine_imposed`, `arrest_count`, `pcm_count` not yet stored as daily snapshot columns on `inspection_performance`. Without them, every non-date-ranged query that includes those metrics needs a live API hit. | open |

### 3.3 Answer / output layer

| # | Issue | Status |
|---|---|---|
| A-1 | LLM used to silently drop the per-tehsil audit table. **Patched**: system-prompt rule 0 forces verbatim reproduction of `Per-Tehsil Breakdown (REQUIRED in answer)` and `Ranking — *` sections. | fixed |
| A-2 | LLM occasionally summarises a 25-row ranking down to "top 1". **Patched**: rule-0 wording explicitly forbids this. | fixed |
| A-3 | Every aggregate answer ends with the long PERA caveat ("This answer is derived from..."). Visually heavy and confuses high-confidence numerical answers with low-confidence document inferences. | open |
| A-4 | No numerical post-processor — LLM-generated numbers are not verified against the structured context. Rare, but possible for the LLM to output a number that didn't appear in evidence. | open |
| A-5 | Confidence ribbon (route + grounding + freshness) not shown at the top of every answer. | open |

### 3.4 OpenAI / OpsOps

| # | Issue | Status |
|---|---|---|
| O-1 | OpenAI API key out-of-quota → 429 on every retrieval call. **Patched**: working key swapped in `.env`. | fixed |
| O-2 | Background rebuild loop created an empty build directory every 30s when embedding failed. **Patched**: `INDEX_POLL_SECONDS=86400`. | fixed |
| O-3 | Backend startup mounts `assets/data` as static dir; this raised `Directory does not exist` on the new repo root. **Patched**: junction link from root `assets/data` to `Pera_staff_chatbot/assets/data`. | fixed |
| O-4 | The frontend at root `D:/authority_chatbot/frontend/` had no `node_modules`. **Patched**: `npm install`. | fixed |

## 4. Why the chatbot still occasionally fails

Even after the fixes above, three categories of queries can still
return a generic or inaccurate answer:

1. **Cross-axis novel queries** — combinations not in
   `_RANK_METRIC_MAP`. Each new metric × qualifier needs a handler.
   Example fixed this session: `most paid challans in amount`.
2. **Implicit follow-ups** — the second turn of a conversation drops
   context from the first. Example: user asks "stations with most
   paid challans", then "tell me in amount" — second turn loses the
   `most paid` context.
3. **Live dashboard tile reproduction** — when the dashboard's date
   filter, time-component, or `paid_amount` calc differs from the
   chatbot's call, totals diverge. Often not a bug; user is comparing
   different time-windows.

## 5. Recommended next steps (priority-ordered)

### P0 — close the largest gap classes

1. **Replace regex typo bandages with a fuzzy-matcher.** Build a
   canonical token list (`inspection`, `challan`, `tehsil`, `division`,
   `district`, `summary`, `sealed`, `warning`, `fir`, `paid`, `unpaid`,
   `overdue`, `amount`). Anything within edit-distance ≤ 2 maps to the
   canonical token. Drop ~80 % of the typo regex maintenance.
2. **Persist follow-up state in the FastAPI session.** Track
   `last_level`, `last_location`, `last_metric`, `last_qualifier`,
   `last_date_range`, `last_domain`. Every `/api/ask` should pass and
   update these. Resolves "tell me in amount" follow-ups.
3. **Promote ranking aliases to the canonical map.** `_RANK_METRIC_MAP`
   currently lists nine metrics; once we add fine-recovery,
   challan-density, and challan-recovery-rate the map will keep
   growing. Move it into a config file with metric-name, source-table,
   sort-column, label, alias-list — declarative, easier to extend.

### P1 — robustness

4. **Cache `dim_tehsil ⨝ dim_district ⨝ dim_division`** for 5 min.
5. **Memoise per-tehsil SDEO responses** keyed by
   `(tehsil_id, start, end)` for 60 s.
6. **Circuit-breaker** on fan-out: > 30 % errors → return early with
   a "partial coverage" banner.
7. **Numerical post-processor** between the LLM answer and the user.
   Verify each number in the answer appears in the structured context;
   flag hallucinations.

### P2 — UX polish

8. **Confidence ribbon** at top of every answer: route + grounding +
   freshness in a single line.
9. **Trim the PERA caveat** so it only appears on document-grounded
   answers, not on structured numerical answers.
10. **Per-tehsil audit table promoted to evidence panel** in the
    frontend so the LLM doesn't have to render a markdown table every
    time.

### P3 — data engineering

11. **Materialise daily snapshot columns** `paid_count`, `unpaid_count`,
    `fine_imposed`, `fine_recovered`, `arrest_count`, `pcm_count`,
    `paid_amount`, `unpaid_amount`, `overdue_amount` on
    `inspection_performance`. Removes the live-API dependency for the
    common "show me last N days" query.
12. **Ingest a per-day fact table** so date-range ≠ "snapshot date"
    becomes answerable from PG without ever calling SDEO.

## 6. What "smart and professional" looks like after these changes

A query like

> "show me top 3 tehsils with most paid amount in lahore division for
> Q1 2026 and tell me what % of total fine they recovered"

would resolve as:
- Detector: ranking + qualifier `paid` + qualifier `amount` + admin
  filter `lahore division` + date range `Q1 2026` + metric
  `recovery_rate` → composite intent.
- Aggregator: fan out to Lahore-division tehsils for Q1 2026 via SDEO
  `top-kpis`, sum `paid_amount` per tehsil, divide by `fine_imposed`,
  rank top 3.
- Answer: a 3-row markdown table with rank, tehsil, paid-amount,
  total-fine, recovery %, plus a one-line explanation, plus the
  confidence ribbon. No PERA caveat (this is structured, not
  document-grounded).

That's the bar. Each P0–P3 item closes one gap between today and that
target answer.

## 7. Test coverage

Two test files, 505 deterministic cases + 10 live-gated:

- `tests/test_inspection_aggregation.py` — 250 cases on routing +
  aggregation arithmetic + live SDEO smoke (gated by `RUN_LIVE_AGG=1`).
- `tests/test_chatbot_quality.py` — 255 cases across 17 categories
  (positive simple/complex, Roman-Urdu, typos, out-of-scope,
  misleading, very short, empty, stress, date formats, officer,
  fallback, hierarchy, cross-domain, query router, casing, partial).

Last run: **436 passed, 32 xfail, 10 skipped, 27 xpass, 0 unexpected
failures, 9 s**.
