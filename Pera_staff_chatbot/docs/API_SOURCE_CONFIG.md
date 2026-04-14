# API Source Configuration Guide

## Overview

API data sources are configured using YAML files placed in `assets/apis/`.
Each file describes one external API endpoint, including how to fetch, authenticate,
normalize, and index the data.

## File Location

```
assets/apis/
├── example_basic_api.yaml         # Simple GET, no pagination
├── example_paginated_api.yaml     # Offset-based pagination
└── your_custom_source.yaml        # Your config here
```

**Rules:**
- Files must end in `.yaml` or `.yml`
- Files starting with `.` or `_` are ignored
- Each file must have a unique `source_id`

## Config Schema

### Top-Level Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `source_id` | string | ✅ | — | Unique ID (lowercase, alphanumeric, underscores only) |
| `source_type` | string | ✅ | `api` | Must be `api` |
| `display_name` | string | ❌ | source_id | Human-readable name |
| `enabled` | bool | ❌ | `true` | Whether this source is active |

### `fetch` Section (Required)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `method` | string | ❌ | `GET` | HTTP method (`GET` or `POST`) |
| `url` | string | ✅ | — | API endpoint URL |
| `headers` | map | ❌ | `{}` | Custom HTTP headers |
| `timeout_seconds` | int | ❌ | `30` | Request timeout |
| `retry_count` | int | ❌ | `3` | Number of retries on failure |
| `retry_backoff_seconds` | int | ❌ | `2` | Backoff between retries |
| `pagination` | object | ❌ | `{type: none}` | Pagination config (see below) |

### `fetch.pagination` Sub-Section

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `none` | `none`, `offset`, `cursor`, or `page` |
| `page_size` | int | `100` | Records per page |
| `page_param` | string | `offset` | Query param for offset/page number |
| `size_param` | string | `limit` | Query param for page size |
| `total_field` | string | — | JSON path to total count (offset mode) |
| `cursor_field` | string | — | JSON path to next cursor (cursor mode) |
| `cursor_param` | string | — | Query param for cursor value |
| `max_pages` | int | `1000` | Safety limit on pages |

### `auth` Section (Optional)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `none` | `none`, `bearer_env`, `api_key_env`, `basic_env` |
| `token_env` | string | — | Env var holding Bearer token |
| `key_env` | string | — | Env var holding API key |
| `key_header` | string | `X-API-Key` | Header name for API key |
| `username_env` | string | — | Env var for basic auth username |
| `password_env` | string | — | Env var for basic auth password |

> **Security:** Never put actual secrets in YAML files. Always reference env var names.

### `sync` Section (Optional)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `interval_minutes` | int | `30` | Minutes between syncs |
| `full_refresh_every_hours` | int | `24` | Hours between full refreshes |
| `delete_missing_records` | bool | `true` | Remove records not in latest fetch |

### `normalization` Section (Optional)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `root_selector` | string | — | JSON path to records array (e.g. `data`) |
| `record_id_field` | string | — | Field name for unique record ID |
| `record_type` | string | — | Record type label |
| `include_fields` | list | `[]` | Fields to include (empty = all) |
| `exclude_fields` | list | `[]` | Fields to exclude |
| `nested_strategy` | string | `flatten` | `flatten` or `preserve` |
| `text_template` | string | — | Template for text rendering |

### `indexing` Section (Optional)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `authority` | int | `2` | Authority level for reranker (1-5) |
| `tags` | list | `[]` | Tags for filtering |

## Complete Example

```yaml
source_id: employees_api
source_type: api
display_name: "PERA Employees API"
enabled: true

fetch:
  method: GET
  url: https://example.gov.pk/api/employees
  headers:
    Accept: application/json
  timeout_seconds: 30
  retry_count: 3
  retry_backoff_seconds: 2
  pagination:
    type: none

auth:
  type: bearer_env
  token_env: EMPLOYEES_API_TOKEN

sync:
  interval_minutes: 30
  full_refresh_every_hours: 24
  delete_missing_records: true

normalization:
  root_selector: data
  record_id_field: employee_id
  record_type: employee
  include_fields:
    - employee_id
    - name
    - designation
  text_template: |
    Employee record.
    Name: {name}
    Employee ID: {employee_id}
    Designation: {designation}

indexing:
  authority: 3
  tags:
    - pera
    - hr
```

## Validation Rules

- `source_id` must be lowercase alphanumeric with underscores only
- `source_type` must be `api`
- `fetch.url` is required and cannot be empty
- `fetch.method` must be `GET` or `POST`
- `pagination.type` must be `none`, `offset`, `cursor`, or `page`
- `auth.type` must be `none`, `bearer_env`, `api_key_env`, or `basic_env`
