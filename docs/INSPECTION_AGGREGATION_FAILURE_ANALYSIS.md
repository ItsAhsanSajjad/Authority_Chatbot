# Inspection Aggregation — Failure Analysis & Remediation

## 1. Failing scenario

Query: `"inspections summery of lahore districts from 1st march to 10 march 2026?"`

Bot reply (before fix):
> The retrieved PERA documents do not contain information about inspections
> in Lahore districts specifically from 1st March to 10th March 2026. The
> available data provides a general summary of inspections in Lahore but
> does not specify dates within March 2026.

Source badge: `insp_district:Lahore` — intent was detected correctly but the
answer was wrong.

## 2. Root cause

`inspection_lookup._query_insp_location` only had a live-API path for
**tehsil + date range**. For **district + date range** (and **division +
date range**) the function fell through to a stale-snapshot SQL query
against `inspection_performance` that *ignores* the user's date filter and
returns the latest cumulative row for the district. The LLM then correctly
reported that no date-filtered figure was available — masking a logic gap
as if it were a data gap.

### Why date-ranged district/division paths existed at all

The upstream SDEO API only accepts `tehsilId` for date-filtered queries:

| Endpoint                                          | Accepts date range? | Granularity      |
|---------------------------------------------------|---------------------|------------------|
| `/sdeo-dashboard/inspections-summary`             | yes                 | tehsil           |
| `/sdeo-dashboard/top-kpis`                        | yes                 | tehsil           |
| `/sdeo-dashboard/challan-status-breakdown`        | yes                 | tehsil           |
| `/Pcm/dashboard-counts`                           | params accepted, **ignored** — returns all-time | tehsil |

There is **no district or division-level date-ranged endpoint** on the
upstream. The dashboard UI is tehsil-only at that granularity; SDEO never
exposed an aggregator.

## 3. Fix — fan-out aggregation

Added in `inspection_lookup.py`:

- `_fetch_tehsil_metrics(tehsil_id, name, start, end)` — single-tehsil
  helper that hits all four SDEO endpoints and returns a flat dict.
- `_aggregate_tehsil_metrics(rows)` — sums numeric fields, collapses
  same-officer rows by name, tracks per-tehsil error counts, and lists
  the tehsils that contributed non-zero activity.
- `_query_district_live(db, district, start, end, source_id)` — looks up
  every active tehsil under `district` via
  `dim_tehsil ⨝ dim_district`, fans out across them in parallel
  (`ThreadPoolExecutor(max_workers=8)`), and aggregates.
- `_query_division_live` — same pattern but joins through
  `dim_division`.
- `_format_aggregate_context` — renders the aggregate with a banner like
  `Aggregation: sum of 11/11 tehsils`, lists fines/paid/unpaid/arrest/PCM,
  shows the unified officer table, and includes the freshness footer.

Routing in `_query_insp_location` was updated so that any
`(level, start_date, end_date)` triple where dates are present and
`level ∈ {tehsil, district, division}` reaches the live path.

### Concurrency cap

Workers are capped at `min(8, len(tehsils))`. SDEO's hosting tier rate-
limits aggressive parallel callers, and 8 is enough to keep a 14-tehsil
division (Multan) under ~2 round-trip latency.

### Failure handling

If a single tehsil's API call errors out, its row is recorded with
`error="…"` and skipped during aggregation; the formatted output reports
`Aggregation: sum of 13/14 tehsils (1 tehsils errored)` so the consumer
knows the answer is partial. We never silently drop rows.

## 4. Validation

After the fix the same query returns:

```
Lahore district, 1 March 2026 → 10 March 2026:
  Total Inspections/Actions: 36,986
  Challans Issued: 1,494
  FIRs Filed: 3
  Warnings Issued: 4,589
  No Offenses Found: 30,856
  Premises Sealed: 12
  Fine Amount Imposed: Rs. 6,094,900
  Fine Amount Recovered (Paid): Rs. 4,134,000
  Fine Amount Outstanding (Unpaid): Rs. 1,960,900
  Paid Challans: 1,187
  Unpaid Challans: 311
  Arrest Cases (all-time): 105
  PCM (all-time): 55
```

Aggregation banner confirms `sum of 11/11 tehsils` for Lahore district.

## 5. Test coverage

`tests/test_inspection_aggregation.py` — 250 cases organised in 15
groups:

| Group | Cases | What it covers |
|---|---|---|
| Division intent | 20 | every Punjab division name routes to `insp_division:*` |
| District intent | 25 | district names + Roman-Urdu phrasing |
| Tehsil intent | 25 | Lahore-district tehsils, Roman-Urdu, hierarchical disambiguation |
| Date-range extraction | ~25 | "from X to Y", "between X and Y", ISO ranges, single month, yearless |
| Date + location combined | 25 | full date-ranged routing for all three levels |
| Roman-Urdu intent | 20 | Urdu/Hindi keywords (`kitne`, `ki`, `mein`, `lagaye`) |
| Hierarchy disambiguation | 10 | "Lahore division" vs "Lahore district" vs "Lahore Cantt" |
| Aggregation arithmetic | 10 | `_aggregate_tehsil_metrics` unit tests — zero rows, single, sum, errors, officer collapse, sort order, fine arithmetic, paid/unpaid sum |
| Challan typo robustness | 15 | `chaallan`, `chalaan`, `challanns`, etc. all route somewhere |
| Smalltalk / out-of-domain | 10 | "hello", "weather today" return None |
| Live aggregation smoke | 10 | gated by `RUN_LIVE_AGG=1`, hits real SDEO API |
| Mixed-level edge prompts | 15 | "compare X and Y", "top 5 tehsils", "rank districts" |
| Officer-name detection | 15 | name + date-range, name + outcome keyword |
| Outcome-keyword routing | 10 | FIR/sealed/arrest/EPO/removal-order |
| Date-format variations | 10 | ISO, slash, dash, named-month, yearless |

Test-suite outcome: **190 passed, 48 xfail (documented gaps), 10 skipped
(live-API), 2 xpass — 0 unexpected failures**.

## 6. Documented gaps (xfail) — follow-up backlog

Each xfail is a real gap that should be closed; they are not accepted
behaviour. Grouped by root cause:

### 6.1 Date parser

| Gap | Example |
|---|---|
| `DD-MM-YYYY` style | `01-03-2026 to 05-03-2026` |
| `DD/MM/YYYY` style | `01/03/2026 to 05/03/2026` |
| ISO standalone day-pair | `Shalimar tehsil 2026-01-01 to 2026-01-31` |
| Yearless month–day pairs | `between 1 April and 30 April 2026` |
| `MMM D till MMM D` | `Shalimar tehsil from Jan 1 till Jan 31 2026` |
| Hyphen-separated date range | `data 1 Apr 2026 - 30 Apr 2026` |

Fix path: extend `_RANGE_PATTERN` and `_parse_single_date` in
`challan_lookup.py` to accept these formats; inspection lookup re-uses
that parser, so the fix lifts both.

### 6.2 Intent detector

| Gap | Example |
|---|---|
| `performance` alone (without "inspection") doesn't trigger | `Faisalabad division performance` |
| Officer + outcome with no `inspection` keyword | `Show challans by Rabia Altaf` |
| Officer suffix codes | `Ahmad Yaar EO-032 challans` |
| Cross-level rollup phrasing | `which division has most challans`, `tehsil-wise breakdown of Lahore district` |
| Roman-Urdu challan→inspection routing | `Faisalabad district kitne challans` (currently only matches challan_lookup) |
| Dotted division name `D.G. Khan` | the substring match collides with sentence punctuation |

Fix path:
- Add `(division|district|tehsil)\s+(performance|stats|figures)\b`
  as an inspection trigger.
- Special-case the dotted division name by normalising
  `D.G. Khan` ↔ `DG Khan` ↔ `Dera Ghazi Khan` in the location cache.
- Decide explicitly whether challan-by-officer queries route to
  `insp_officer:*` or `challan_officer:*` — currently ambiguous.

### 6.3 Officer-name vs tehsil-name collision

`Allama Iqbal Town` is a tehsil; `M. Iqbal` / `M Zafar Iqbal` are real
officers. The officer-name detector currently runs before the tehsil
detector and can win on a substring match. Fix path: prefer location
match when the surrounding token is `tehsil`, `district`, or `division`.

## 7. Reliability recommendations

1. **Cache the per-tehsil-id list per division/district** in process
   memory with a 5-minute TTL. The hierarchy table changes daily at
   most; today every district query re-issues the SQL.
2. **Add a circuit breaker** around `_fetch_tehsil_metrics`. If more
   than 30% of tehsils fail in one fan-out, abort early and return a
   degraded answer rather than spending the budget on doomed retries.
3. **Persist aggregated results** for `(division|district, start, end)`
   tuples for ≤24 h. Higher-authority queries are repetitive ("Lahore
   division this week") and re-fanning out 100+ tehsil calls per query
   is wasteful.
4. **Surface partial-coverage in the LLM prompt**. Today the aggregate
   context says `sum of 13/14 tehsils (1 tehsils errored)`; the answer
   layer should propagate that to the user-facing reply rather than
   silently presenting partial totals as authoritative.
5. **Schema for cumulative fields**. `paid_count`, `unpaid_count`,
   `fine_imposed`, `fine_recovered`, `unpaid_fine` are not yet stored
   in `inspection_performance`. Add them in a daily snapshot column
   group so non-date-ranged queries can answer without any live calls.
6. **Add a typo-tolerance layer** above the regex matcher (Levenshtein
   on canonical keywords: `inspection`, `challan`, `tehsil`,
   `division`, `district`). The current per-keyword regex bandage
   doesn't scale.
