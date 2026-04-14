# Analytics Data Model

## Schema Overview

The analytics schema consists of 15 tables across 4 groups:

1. **Tracking** — migration versioning
2. **Ingestion** — raw + normalized API data in PostgreSQL
3. **Dimensions** — geography and date reference tables
4. **Facts** — curated analytical measures

## Tables

### 1. analytics_schema_migrations
Tracks which migrations have been applied.

| Column | Type | Notes |
|---|---|---|
| version | INTEGER PK | Migration version number |
| description | TEXT | Human-readable description |
| applied_at | TIMESTAMPTZ | When migration was applied |

---

### 2. api_source_configs_pg
API source configuration metadata (PostgreSQL mirror).

| Column | Type | Notes |
|---|---|---|
| source_id | TEXT PK | Unique source identifier |
| display_name | TEXT | Human-readable name |
| source_type | TEXT | Default: 'api' |
| config_hash | TEXT | Hash of config YAML |
| url | TEXT | API endpoint URL |
| auth_type | TEXT | Authentication type |
| sync_interval | INTEGER | Sync interval in seconds |
| enabled | BOOLEAN | Whether sync is active |
| first_seen_at | TIMESTAMPTZ | First registration |
| last_updated_at | TIMESTAMPTZ | Last modification |
| metadata_jsonb | JSONB | Extended metadata |

---

### 3. api_sync_runs_pg
Sync execution history.

| Column | Type | Notes |
|---|---|---|
| run_id | SERIAL PK | Auto-increment ID |
| source_id | TEXT FK | → api_source_configs_pg |
| started_at | TIMESTAMPTZ | Run start time |
| completed_at | TIMESTAMPTZ | Run end time |
| status | TEXT | running/completed/error |
| records_fetched | INTEGER | Total fetched |
| records_new | INTEGER | New records |
| records_updated | INTEGER | Changed records |
| records_removed | INTEGER | Deleted records |
| error_message | TEXT | Error details |
| run_metadata | JSONB | Additional run data |

**Indexes:** source_id, started_at

---

### 4. api_raw_snapshots
Raw JSON snapshots from API fetches.

| Column | Type | Notes |
|---|---|---|
| snapshot_id | SERIAL PK | Auto-increment ID |
| source_id | TEXT FK | → api_source_configs_pg |
| sync_run_id | INTEGER FK | → api_sync_runs_pg |
| snapshot_json | JSONB | Full raw response |
| record_count | INTEGER | Records in snapshot |
| content_hash | TEXT | Response hash |
| fetched_at | TIMESTAMPTZ | Fetch timestamp |
| created_at | TIMESTAMPTZ | Row creation time |

**Indexes:** source_id, fetched_at

---

### 5. api_records_pg
Normalized individual records from API responses.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Auto-increment |
| source_id | TEXT FK | → api_source_configs_pg |
| record_id | TEXT | Record identifier |
| record_type | TEXT | Record classification |
| content_hash | TEXT | Content hash |
| raw_json | JSONB | Record data |
| normalized_text | TEXT | RAG-ready text |
| snapshot_date | DATE | Snapshot date |
| first_seen_at | TIMESTAMPTZ | First seen |
| last_updated_at | TIMESTAMPTZ | Last update |
| is_active | BOOLEAN | Soft-delete flag |

**Unique:** (source_id, record_id)
**Indexes:** source_id, is_active, snapshot_date

---

### 6. dim_division
Punjab administrative divisions.

| Column | Type | Notes |
|---|---|---|
| division_id | INTEGER PK | Division ID |
| division_name | TEXT | English name |
| division_name_ur | TEXT | Urdu name |
| code | TEXT | Division code |
| is_active | BOOLEAN | Active flag |
| source_id | TEXT | Data source |
| snapshot_date | DATE | Snapshot date |
| raw_json | JSONB | Original data |
| created_at | TIMESTAMPTZ | Row created |
| updated_at | TIMESTAMPTZ | Row updated |

---

### 7. dim_district
Punjab districts, linked to divisions.

| Column | Type | Notes |
|---|---|---|
| district_id | INTEGER PK | District ID |
| district_name | TEXT | English name |
| division_id | INTEGER FK | → dim_division |
| district_name_ur | TEXT | Urdu name |
| code | TEXT | District code |
| is_active | BOOLEAN | Active flag |
| source_id | TEXT | Data source |
| snapshot_date | DATE | Snapshot date |
| raw_json | JSONB | Original data |

**Indexes:** division_id

---

### 8. dim_tehsil
Punjab tehsils, linked to districts.

| Column | Type | Notes |
|---|---|---|
| tehsil_id | INTEGER PK | Tehsil ID |
| tehsil_name | TEXT | English name |
| district_id | INTEGER FK | → dim_district |
| tehsil_name_ur | TEXT | Urdu name |
| code | TEXT | Tehsil code |
| is_active | BOOLEAN | Active flag |
| source_id | TEXT | Data source |
| snapshot_date | DATE | Snapshot date |
| raw_json | JSONB | Original data |

**Indexes:** district_id

---

### 9. dim_date
Pre-populated date dimension (2020–2030), supports Pakistani fiscal year.

| Column | Type | Notes |
|---|---|---|
| date_key | DATE PK | Calendar date |
| year | INTEGER | Calendar year |
| month | INTEGER | Month 1-12 |
| day | INTEGER | Day 1-31 |
| quarter | INTEGER | Quarter 1-4 |
| fiscal_year | TEXT | e.g., "2025-2026" |
| day_of_week | INTEGER | ISO day 1-7 |
| is_weekend | BOOLEAN | Saturday/Sunday |
| month_name | TEXT | Full month name |
| month_name_short | TEXT | 3-letter abbreviation |

**Note:** Pakistani fiscal year runs July–June. Pre-populated with ~4,018 rows.

---

### 10. fact_workforce_strength
PERA workforce strength data per division.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Auto-increment |
| source_id | TEXT | Data source (`pera_strength`) |
| snapshot_date | DATE | Snapshot date |
| division_id | INTEGER FK | → dim_division (from `divisionId`) |
| district_id | INTEGER FK | → dim_district (from `districtId`) |
| designation | TEXT | Division name label (from `divisionName`), NULL if absent |
| cadre | TEXT | Staff cadre, NULL if absent |
| sanctioned_posts | INTEGER | Total authorized positions (from `total`) |
| filled_posts | INTEGER | On-duty staff (from `onDuty`) |
| vacant_posts | INTEGER | Absent staff (from `absent`) |
| contract_employees | INTEGER | Contract staff |
| deputation_in | INTEGER | Inbound deputation |
| deputation_out | INTEGER | Outbound deputation |
| raw_json | JSONB | Original data |

**Unique:** (source_id, snapshot_date, division_id, district_id, designation, cadre)
**Indexes:** snapshot_date, division_id, district_id

---

### 11. fact_finance_overview (DEPRECATED)
Legacy table from Phase 1. Rows cleaned by migration v18.
**Not written to. Use fact_finance_overview_summary and fact_finance_overview_monthly instead.**

---

### 12. fact_finance_overview_summary
Finance overview summary from `finance_overview` API top-level fields.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Auto-increment |
| source_id | TEXT | Data source (`finance_overview`) |
| snapshot_date | DATE | Snapshot date |
| fiscal_year | TEXT | Nullable fiscal year |
| total_released | NUMERIC(18,2) | Total budget released (from `totalReleased`) |
| total_utilized | NUMERIC(18,2) | Total budget utilized (from `totalUtilized`) |
| utilization_rate | NUMERIC(7,2) | Utilization % (from `utilizationRate`) |
| raw_json | JSONB | Original data |
| created_at | TIMESTAMPTZ | Row created |
| updated_at | TIMESTAMPTZ | Row updated |

**Unique:** (source_id, snapshot_date)
**Indexes:** snapshot_date

---

### 13. fact_finance_overview_monthly
Monthly expenditure data from `finance_overview` API series.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Auto-increment |
| source_id | TEXT | Data source (`finance_overview`) |
| snapshot_date | DATE | Snapshot date |
| fiscal_year | TEXT | Nullable fiscal year |
| month_label | TEXT | Month name (e.g., "Jul", "Aug") |
| expenditure_amount | NUMERIC(18,2) | Monthly expenditure |
| raw_json | JSONB | Original data |
| created_at | TIMESTAMPTZ | Row created |
| updated_at | TIMESTAMPTZ | Row updated |

**Unique:** (source_id, snapshot_date, month_label)
**Indexes:** snapshot_date, month_label

---

### 14. fact_challan_status_summary (scaffold)
Challan status tracking — table structure only, not yet populated.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | Auto-increment |
| source_id | TEXT | Data source |
| snapshot_date | DATE | Snapshot date |
| division_id | INTEGER FK | → dim_division |
| district_id | INTEGER FK | → dim_district |
| challan_type | TEXT | Challan category |
| total_issued | INTEGER | Challans issued |
| total_paid | INTEGER | Challans paid |
| total_pending | INTEGER | Challans pending |
| total_amount | NUMERIC(18,2) | Total amount |
| paid_amount | NUMERIC(18,2) | Amount paid |

**Unique:** (source_id, snapshot_date, division_id, district_id, challan_type)

## API-to-Table Mapping

| API Source | Target Table | Key Fields |
|---|---|---|
| app_data_divisions | dim_division | id → division_id, name → division_name |
| app_data_districts | dim_district | id → district_id, name → district_name, division_id |
| app_data_tehsils | dim_tehsil | id → tehsil_id, name → tehsil_name, district_id |
| pera_strength | fact_workforce_strength | divisionId → division_id, total → sanctioned_posts, onDuty → filled_posts, absent → vacant_posts |
| finance_overview (summary) | fact_finance_overview_summary | totalReleased → total_released, totalUtilized → total_utilized, utilizationRate → utilization_rate |
| finance_overview (series) | fact_finance_overview_monthly | month → month_label, expenditure → expenditure_amount |
