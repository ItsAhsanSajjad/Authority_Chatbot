# PERA Financial Accuracy Diagnostic Report

> Read-only audit of every amount-bearing code path. Verifies live SDEO
> values for Multan City 2026-04-01 .. 2026-04-20, traces ten user
> queries to their handlers, and lists the remaining accuracy risks.
> No code was modified during this audit.

## Executive Summary

Tehsil-level performance amounts are correct after the recent
inclusive-end-date fix. Dashboard cards (Multan City Apr 1-20) match
the chatbot's `_query_tehsil_live` output to the rupee:

| Field | Dashboard | Chatbot | Source |
|---|---:|---:|---|
| Fine Amount | 2,828,000 | 2,828,000 | top-kpis.totalFineImposed ✓ |
| Paid Amount | 1,713,500 | 1,713,500 | challan-status-breakdown.paidAmount ✓ |
| Outstanding Amount | 1,114,500 | 1,114,500 | challan-status-breakdown.unpaidAmount ✓ |
| Fine Recovered | 1,713,500 | 1,713,500 | top-kpis.totalFineRecovered ✓ |

Where the chatbot **still gets amounts wrong**:

1. **District / division performance** (`_fanout_aggregate`) does NOT
   capture `paidAmount`/`unpaidAmount` from challan-status-breakdown
   per tehsil and renders amounts via the legacy plain-text formatter,
   not `render_dashboard_summary`. Output diverges from per-tehsil
   sum.
2. **Ranking by amount** (`_query_top_by_amount`) reads from
   `challan_data` SUM — **all-time only, never date-filtered**. Footer
   discloses but the amounts can be 10-100× larger than the user's
   implicit date window.
3. **Officer fine amount** is computed by summing per-record
   `fineAmount` from the PCM endpoint, which is **fine imposed per
   record**, NOT fine recovered. Labelled `Total Fine Amount` —
   ambiguous.
4. **Numeric validator** only checks "does this number appear anywhere
   in the payload?" It cannot detect a label-value swap (e.g. answer
   says `Fine Amount: 1,713,500` when 1,713,500 is the Paid Amount, not
   Fine Imposed). Both numbers are in the payload, so validator passes.
5. **All-time amount leak** can still happen for queries that route to
   `Pcm/dashboard-counts`-only paths (e.g. the focused-metric branch
   for arrest doesn't yet exist; future `arrest amount` queries would
   leak the all-time totalFineAmount).
6. **Officer endpoint param mismatch** with rest of code: officer uses
   `fromDate`/`toDate` (with `+1 day` for end), other endpoints use
   `startDate`/`endDate` with `T23:59:59`. Two date conventions live in
   one module.

## Amount Field Inventory

| Label shown | Internal key | Endpoint / table | Raw key | Meaning | Risk |
|---|---|---|---|---|---|
| Fine Amount | `fine_amount` | top-kpis | `totalFineImposed` | imposed per date range | low |
| Fine Recovered | `fine_recovered` | top-kpis | `totalFineRecovered` | recovered per date range | low |
| Outstanding Amount | `outstanding_amount` | challan-status-breakdown | `unpaidAmount` (fallback `unpaidFineAmount`) | unpaid amount per date range | low |
| Paid Amount | `paid_amount` | challan-status-breakdown | `paidAmount` | paid amount per date range | low |
| Partially Paid Amount | `partially_paid_amount` | challan-status-breakdown | `partiallyPaidAmount` | partial-pay amount | column reserved, not surfaced |
| Total Fine Amount (all-time) | n/a | Pcm/dashboard-counts | `totalFineAmount` | cumulative imposed | **HIGH risk if used in date-ranged answer** |
| paidChallanAmount (all-time) | n/a | Pcm/dashboard-counts | `paidChallanAmount` | cumulative paid | **HIGH risk** |
| unPaidChallanAmount (all-time) | n/a | Pcm/dashboard-counts | `unPaidChallanAmount` | cumulative unpaid | **HIGH risk** |
| Officer Total Fine | `total_fine` (local var) | Pcm/officer-inspections | sum of `fineAmount` per record | per-record imposed sum | **medium — labelled ambiguously** |
| Stored fine (DB) | `inspection_performance.fine_imposed/recovered/outstanding` | inspection_performance (PG) | NULL until enrichment ingest runs | snapshot date-only | medium — currently NULL for all rows |
| Ranked amount | metric_value | challan_data SUM | `fine_amount`/`paid_amount`/`outstanding_amount` | row-level cumulative | **medium — never date-filtered** |

## Live Endpoint Comparison (Multan City 2026-04-01 to 2026-04-20)

Live probes confirmed the source-of-truth matrix:

| Endpoint | Field | Raw value | Date-filtered? | Dashboard match |
|---|---|---|---|---|
| top-kpis | totalFineImposed | 2,828,000.0 | yes | ✓ |
| top-kpis | totalFineRecovered | 1,713,500.0 | yes | ✓ |
| top-kpis | unpaidFineAmount | 1,114,500.0 | yes | ✓ |
| top-kpis | totalForceDeployed | 4 | yes | ✓ |
| challan-status-breakdown | paidAmount | 1,713,500.0 | yes | = top-kpis ✓ |
| challan-status-breakdown | unpaidAmount | 1,114,500.0 | yes | = top-kpis ✓ |
| challan-status-breakdown | partiallyPaidAmount | 0.0 | yes | not surfaced |
| Pcm/dashboard-counts | totalFineAmount | **17,057,500** | **no** (all-time) | ✗ if used as date-ranged |
| Pcm/dashboard-counts | paidChallanAmount | **9,571,000** | **no** | ✗ if used as date-ranged |
| Pcm/dashboard-counts | unPaidChallanAmount | **7,486,500** | **no** | ✗ if used as date-ranged |
| Pcm/tehsil-details | fineAmount | 17,057,500 | no | ✗ |
| Pcm/officer-inspections (Mubbashir, Apr 1-20) | sum(fineAmount) | 1,481,000 | yes via `fromDate`/`toDate` | matches if date-filtered |

Important: `Pcm/dashboard-counts` accepts `startDate`/`endDate` params
but **silently ignores them** — same value with or without the params.

## Root Causes Found

### R1 — District/division aggregator drops `paidAmount` / `unpaidAmount`

- **Severity:** High
- **File / function:** [inspection_lookup.py `_fetch_tehsil_metrics`](D:/authority_chatbot/inspection_lookup.py) (line ~1998), [`_aggregate_tehsil_metrics`](D:/authority_chatbot/inspection_lookup.py) (line ~2062)
- **Evidence:** the per-tehsil dict initialises `paid_count`, `unpaid_count`, `fine_imposed`, `fine_recovered`, `unpaid_fine` — but **does NOT store** `paid_amount` or `unpaid_amount` from the challan-status-breakdown response. Aggregator's money_keys = `("fine_imposed", "fine_recovered", "unpaid_fine")` — missing `paid_amount`/`unpaid_amount`/`fine_outstanding`.
- **Example query:** "Lahore district performance from 1 April to 20 April 2026"
- **Wrong behavior:** District total Paid Amount missing or computed from a different field; user sees inconsistency vs sum of tehsil tiles.
- **Correct behavior:** sum of per-tehsil `paidAmount` should equal the sum of all child tehsil dashboard tiles' Paid Amount.
- **Fix direction:** add `paid_amount`/`unpaid_amount` keys to `_fetch_tehsil_metrics` output + extend `_aggregate_tehsil_metrics.money_keys` + render via `render_dashboard_summary`.

### R2 — `_format_aggregate_context` uses legacy plain text, not `render_dashboard_summary`

- **Severity:** High
- **File / function:** `_format_aggregate_context` (line ~2125–2155)
- **Evidence:** outputs `Fine Amount (imposed): Rs. {fine_imposed:,}` etc. as plain text, not the section-headed markdown KPI table. Skips `paid_amount`, `unpaid_amount`, `outstanding_amount`. Skips Officer Breakdown markdown table.
- **Example query:** "Multan division performance from 1 April to 20 April 2026"
- **Wrong behavior:** division performance reads as ASCII text; UI may strip whitespace; no clean Financial Breakdown section; mismatched look-and-feel vs tehsil performance.
- **Fix direction:** route `_format_aggregate_context` through `render_dashboard_summary(officer_rows=…)` after migrating amount keys (see R1).

### R3 — Ranking by amount is silently all-time

- **Severity:** Medium
- **File / function:** `_query_top_by_amount` (line ~786–880)
- **Evidence:** SQL is `SELECT SUM(fine_amount/paid_amount/outstanding_amount) FROM challan_data GROUP BY tehsil_name`. No `WHERE action_date BETWEEN …` — even when the user includes a date range in the question.
- **Example query:** "top tehsils by paid amount in April 2026" — returns cumulative ALL-TIME paid amount per tehsil, ignoring April. Footer disclosure says "Cumulative all-time totals" but easy to miss.
- **Fix direction:** parse date range from question, gate the SQL with `AND action_date BETWEEN %s AND %s`. Or fan out to per-tehsil top-kpis live calls (matches dashboard).

### R4 — Pcm/dashboard-counts amounts are never used in primary tables (good) but available to ranking/officer paths (risk)

- **Severity:** Medium
- **File / function:** caller-side discipline. `_query_tehsil_live` already correctly omits PCM amounts from primary table.
- **Evidence:** `Pcm/dashboard-counts` returns date-ignored cumulative amounts. If a future handler reads `totalFineAmount` from this endpoint and labels it date-ranged, the user sees 17M instead of 2.8M.
- **Fix direction:** wrap the endpoint with a thin module-level constant `IS_ALL_TIME=True` so any caller is forced to think about it; never use `totalFineAmount`/`paidChallanAmount`/`unPaidChallanAmount` for date-ranged answers.

### R5 — Officer Total Fine label ambiguity

- **Severity:** Medium
- **File / function:** `_query_insp_officer` live path (line ~2532, 2544)
- **Evidence:** computes `total_fine = sum(r.get("fineAmount", 0) for r in records)` and renders `Total Fine Amount: Rs. X`. Not labelled imposed vs recovered. The PCM `fineAmount` per record is **imposed at the time of challan**, not recovered.
- **Example query:** "Mubbashir Quyyam fine amount from 1 April to 20 April 2026" → Rs 1,481,000 (imposed). User may interpret as "money he collected" (recovered).
- **Fix direction:** rename label to "Officer Fine Imposed" + add a "Fine Recovered" column if the endpoint exposes a paid flag per record (it does — `paidAmount` per record but currently ignored).

### R6 — Officer endpoint date-param convention diverges

- **Severity:** Medium
- **File / function:** `_query_insp_officer` live path (line ~2499–2506)
- **Evidence:** uses `fromDate`/`toDate` with `+timedelta(days=1)` exclusive, while every other handler now uses `startDate`/`endDate` with inclusive `T23:59:59`. PCM officer endpoint silently ignores `startDate`/`endDate` (returns 8632 all-time rows when those names are sent — confirmed by probe).
- **Fix direction:** keep the `fromDate`/`toDate` names (only working set) but document explicitly that this endpoint is the **only** PCM endpoint with that convention. Add a small constant `_PCM_OFFICER_DATE_PARAMS = ("fromDate","toDate")` and a TODO that the upstream API should normalise.

### R7 — Stored `inspection_performance.fine_*` columns are NULL

- **Severity:** Medium
- **File / function:** [analytics_migrations migration 38](D:/authority_chatbot/analytics_migrations.py)
- **Evidence:** migration added `fine_imposed`, `fine_recovered`, `fine_outstanding`, `paid_count`, `unpaid_count`, `paid_amount`, `unpaid_amount`, `arrest_count`, `pcm_count` columns. Ingestion (`inspection_ingest._store_summary_batch`) only writes `removal_order`, `epo`, `extra_metrics`. The other columns stay NULL. `_query_insp_summary` reads them; output skips NULL rows. Net effect: stored summary path never shows fine numbers.
- **Fix direction:** implement enrichment fetcher that, after summary ingest, calls top-kpis + challan-status-breakdown per tehsil/district/division and writes to the new columns.

### R8 — Numeric validator is label-blind

- **Severity:** Medium
- **File / function:** [numeric_validator.validate_numeric_answer](D:/authority_chatbot/numeric_validator.py)
- **Evidence:** validator only checks `if number in payload_set`. Cannot detect that an answer renders `Fine Amount: 1,713,500` when 1,713,500 is the Paid Amount in the payload (different label). Both numbers in payload → both pass.
- **Fix direction:** parse rows from the payload as `(label, value)` tuples (e.g. `^\| (Fine Amount) \| ([\d,]+) \|$`) and verify each `(label, value)` pair appears in the answer. A label-aware compare would catch swaps and wrong-row mappings.

### R9 — Yearless dates default to today's year — risky for officer "March 1-25" query

- **Severity:** Low
- **File / function:** [pera_dates.parse_date_range](D:/authority_chatbot/pera_dates.py) yearless branch
- **Evidence:** "1st march to 25th march" → no year → uses `today.year` (= 2026). If user is on May 6 2026 and references March 2026, the assumption is fine. If they re-run the same query in 2027 expecting last year's data, they'd get the wrong year.
- **Fix direction:** when yearless and no explicit "last year"/"current year"/"this year" qualifier, prefer `today.year` if the resulting range is in the past; otherwise prompt for clarification.

### R10 — Officer endpoint per-record amount loses payment-status

- **Severity:** Low
- **File / function:** `_query_insp_officer`
- **Evidence:** code sums `fineAmount` only. Per-record fields include `paidAmount` (per probe earlier they came back zero-or-positive). Currently dropped — user can't see Officer Paid Recovery %.
- **Fix direction:** also extract `paidAmount` per record, sum, compute `recovery_pct = paid / imposed * 100`.

## Query Trace Matrix

| # | Query | Handler | Amount source | Deterministic? | Risk |
|---|---|---|---|---|---|
| 1 | Multan City fine amount from 1st April to 20 April 2026 | focused → `render_focused_metric_summary` keys=[`fine_amount`] via `_query_tehsil_live` | top-kpis.totalFineImposed | yes | low |
| 2 | Multan City paid amount from 1st April to 20 April 2026 | focused keys=[`paid_amount`] | challan-status-breakdown.paidAmount | yes | low |
| 3 | Multan City outstanding amount … | focused keys=[`outstanding_amount`] | challan-status-breakdown.unpaidAmount | yes | low |
| 4 | Multan City recovery amount … | focused keys=[`fine_recovered`] | top-kpis.totalFineRecovered | yes | low (note: matches paidAmount in current data) |
| 5 | top tehsils by paid amount | `insp_top:paid_amount:tehsil:desc` → `_query_top_by_amount` | challan_data SUM (paid_amount) | deterministic markdown table | **medium — all-time only** (R3) |
| 6 | top tehsils by fine amount | `insp_top:fine_amount:tehsil:desc` → `_query_top_by_amount` | challan_data SUM (fine_amount) | deterministic | **medium — all-time only** |
| 7 | paid challan amount in Multan | challan-side ranking? falls to `insp_top:paid_amount:tehsil:desc` if challan keyword + amount qualifier | challan_data | deterministic | medium |
| 8 | unpaid challan amount in Multan | similar → `insp_top:unpaid_amount:tehsil:desc` | challan_data | deterministic | medium |
| 9 | fine recovered by officers in Multan City … | likely routes to `insp_tehsil:Multan city` focused [`fine_recovered`] — officer table is currently the dashboard's officer breakdown (no fine column) | top-kpis (tehsil-level) | yes for tehsil total; **per-officer recovered NOT computed** | medium |
| 10 | Mubbashir Quyyam fine amount from 1st March to 25th March | `insp_officer:Mubbashir Quyyam` → `_query_insp_officer` live | sum(fineAmount) per record from PCM officer-inspections | yes (no LLM) | medium — labelled ambiguously (R5), year defaults today's (R9) |

## Officer Amount Findings

`_query_insp_officer` live path (line 2492+):
- Endpoint: `/Pcm/officer-inspections` — uses `fromDate`/`toDate`, `+1` day exclusive convention.
- Per-record fields: `ownerName`, `cnic`, `address`, `challanCase`, `warningCase`, `noOffense`, `arrestCase`, `confiscated`, `fineAmount`, `firCase`, `sealed`, `removalOrder`, `epo`, `latitude`, `longitude` (no per-record date — date filter is server-side).
- `total_fine = sum(r.get("fineAmount", 0))` — this is the **sum of imposed fines** in records the API returns for the date range.
- Probe for Mubbashir Quyyam Apr 1-20 returned 2557 rows / 190 challans / Rs 1,481,000 fine. Matches dashboard officer tile if dashboard shows "Fine Imposed", but ambiguous because the chatbot calls it "Total Fine Amount".
- Officer Paid Recovery % is **not computed** — would require summing `paidAmount` per record (field exists per probe earlier; currently dropped).
- "Total Inspections" for officer = `len(records)` = total records returned. This includes warnings, no-offense, etc. Not just inspections — matches PCM endpoint definition.

If user previously saw 1,341,000 for "1st March to 25 March" but now sees 1,461,000 / 1,481,000, possible causes:
1. Yearless date — parser used `today.year`; if user ran query in different year, year inference may have changed.
2. Data drifted between user observation and chatbot query (live ingestion adds rows).
3. Officer endpoint date convention — if user typed "to 25 March", endpoint sees `toDate=2026-03-26` (+1 day exclusive). But the underlying server-side filter may interpret differently.
4. User compared dashboard tile "Recovered" vs chatbot label "Fine Amount" (= imposed). They're different metrics.

## Labeling / Professional Output Findings

Current labels are inconsistent:

| Current | Recommended canonical | Reason |
|---|---|---|
| Fine Amount | **Fine Imposed** | matches dashboard "Fine Amount" but disambiguates from "Fine Recovered" |
| Total Fine Amount | **Officer Fine Imposed** (officer query) | matches imposed-not-recovered semantic |
| Fine Recovered | **Fine Recovered** | unchanged |
| Paid Amount | **Paid Amount** | matches dashboard tile |
| Unpaid Amount | **Outstanding Amount** | dashboard says Outstanding when discussing money |
| Outstanding Amount | **Outstanding Amount** | unchanged |
| FIRs | **FIRs Registered** | matches dashboard "Officer Activity Summary" |
| FIRs Registered | **FIRs Registered** | unchanged |
| Arrest | **Arrest Cases** | matches dashboard tile |
| Enforcer | **Enforcers (Force Deployed)** | clarify origin |
| No Offense | **No Offense** | unchanged (en-US) |
| Removal Orders | **Removal Orders** | unchanged |
| EPO | **EPO** | unchanged |
| Sealed | **Sealed Premises** | matches dashboard wording |
| PCM | **PCM (Police Confiscation Memo)** | spell out acronym |
| Total Inspections | **Total Inspections / Actions** | dashboard uses both terms |

Recommendation: define a canonical label map module `pera_labels.py` with one mapping used by every renderer.

## Numeric Validator Findings

Current `validate_numeric_answer(answer, payload_text)`:
- extracts numeric tokens from both, normalises commas/Rs, checks set membership.
- catches **fabricated numbers** (number not in payload).
- **does NOT catch**:
  - label-value swap (Fine Amount: 1,713,500 — if 1,713,500 is actually Paid Amount in payload).
  - row-mapping error (Mubbashir's fine vs Asif's fine — both numbers exist somewhere).
  - unit confusion (rupees vs counts).
  - all-time vs date-ranged confusion (cumulative number leaks if it appears anywhere in payload).

Required upgrade:
1. Parse payload as `[(label_canonical, value_str)]` tuples.
2. Parse answer same way.
3. For every answer pair, require an exact label-value match in payload.
4. Skip free integers and dates (already implemented).
5. On mismatch, replace answer with deterministic payload (already done) AND log the diverging pair in audit.

## Test Coverage Gaps

| Area | Have | Missing |
|---|---|---|
| Date boundary | yes (`to_sdeo_start_end`) | per-endpoint test that **all 4 amount endpoints** receive identical inclusive params |
| Source priority | implicit | explicit: "Fine Amount = top-kpis.totalFineImposed, NOT Pcm.totalFineAmount" |
| District/division aggregation amount sum | no | per-tehsil sum equals district total |
| Ranking by amount with date filter | no | when date is in question, ranking SQL gets `WHERE action_date BETWEEN …` |
| All-time amount leak | no | "Multan fine amount" should NOT show 17M cumulative |
| Yearless date | partial | "1 March to 25 March" picks today.year and warns if range > 1 year ago |
| Label-value validation | no | answer "Fine Amount: 1,713,500" fails when payload only has 1,713,500 as Paid Amount |
| Officer amount label | no | label is "Officer Fine Imposed" not ambiguous "Total Fine Amount" |
| Officer recovery % | no | computed paid/imposed %, not 0-or-missing |
| Partially paid amount | no | when partiallyPaidCount > 0, surface row |
| Confidence dispatcher tie-break for amount queries | partial | "paid amount" not stolen by inspection ranking |

## Recommended Repair Plan

### Phase 1 — Financial source-of-truth map

Single module `pera_financial_sources.py` exporting:

```python
AMOUNT_SOURCES = {
    "fine_imposed":   ("top-kpis", "totalFineImposed",     "date-filtered"),
    "fine_recovered": ("top-kpis", "totalFineRecovered",   "date-filtered"),
    "paid_amount":    ("challan-status-breakdown", "paidAmount", "date-filtered"),
    "unpaid_amount":  ("challan-status-breakdown", "unpaidAmount", "date-filtered"),
    "outstanding_amount": "alias_for(unpaid_amount)",
    "all_time_total_fine": ("Pcm/dashboard-counts", "totalFineAmount", "ALL-TIME"),
    ...
}
```

Used by every amount-rendering code path. Forces explicit choice + comment.

### Phase 2 — Amount normalizer

`normalize_sdeo_financial_metrics(summary, kpi, status, pcm) -> dict` returning canonical keys. Centralised `parse_money_safe(raw) -> Optional[Decimal]` for "1,713,500" / "1713500" / "1,713,500.50" / None.

### Phase 3 — Deterministic financial renderers

Three renderers:
- `render_focused_amount_report(metric_key, value, location, date_range)` for single-metric amount queries.
- `render_full_dashboard` (existing) — extend to call `render_dashboard_summary(officer_rows=…)` from `_format_aggregate_context`.
- `render_officer_financial_summary(officer_name, totals, recovery_pct, date_range)` with **Imposed** vs **Recovered** vs **Recovery %**.

### Phase 4 — Label-aware numeric validator

Upgrade `numeric_validator.validate_numeric_answer` to take labelled rows. Two passes:
1. Number-in-payload (existing).
2. `(label, value)` pair appears verbatim in payload. Mismatch → swap with deterministic.

### Phase 5 — Tests + audit logging

- Test matrix per §"Test Coverage Gaps".
- Audit fields: `amount_source_endpoint`, `amount_raw_value`, `amount_label`, `amount_is_all_time`, `amount_is_date_filtered`, `amount_year_inferred`.

## Questions / Unknowns

1. **Officer dashboard tile labels** — does the official SDEO officer card say "Fine" (imposed) or "Fine Recovered" or both?
2. **Paid Recovery %** — confirmed available per officer? Need a definitive endpoint that returns it (or formula `paid / imposed * 100`).
3. **`partiallyPaidAmount`** — non-zero in any production tehsil? If yes, dashboard surfaces it where? Current chatbot omits even when present.
4. **Ranking by amount** — should it default to all-time or to last-N-days? Product call.
5. **PCM vs SDEO arrests** — SDEO dashboard tile shows date-filtered arrest. Find its endpoint or accept that we cannot replicate.
6. **Yearless dates older than 60 days** — silently assume `today.year` or warn "did you mean last year"?
7. **`Pcm/dashboard-counts` partial fields** (`tehsil`, `district`) — always null in our probes; is this a region we never use, or a known missing data?
