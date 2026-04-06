"""
PERA AI — API Lookup Registry (Config-Driven)

Auto-discovers all YAML API configs that have a ``lookup:`` section
and provides generic intent detection, data retrieval, and response
formatting at query time.

**Adding a new API lookup requires ONLY a YAML config file** — no
code changes are needed in stored_api_lookup.py, live_api_handler.py,
or anywhere else.

YAML ``lookup:`` section reference
-----------------------------------
lookup:
  enabled: true                       # master switch
  keywords: [division, divisions]     # simple word-in-question matching
  keyword_patterns:                   # regex patterns (re.IGNORECASE)
    - "\\bdivisions?\\b"
  title: "PERA Divisions"            # display title
  subtitle: "({count} divisions)"    # {count} = record count
  sort_by: name                       # sort field
  sort_fallback: ""                   # fallback sort field
  name_field: ""                      # primary name field → {_name}
  name_fallback: name                 # fallback for {_name}
  record_template: "- **{name}**"     # per-record markdown line
  context_title: "List of divisions"  # plain-text heading for LLM
  context_record_template: "- {name}" # per-record line for LLM
  aggregations:                       # optional numeric summaries
    - field: total
      label: Total
  summary_template: "Total {Total}"   # uses aggregation {Label} keys
  live_title: "Live Data: Divisions"  # title for live-API mode
  live_max_records: 50                # cap for live display
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml

from log_config import get_logger

log = get_logger("pera.api.lookup_registry")


# ── Safe template formatting ─────────────────────────────────

class _SafeDict(dict):
    """Dict subclass that returns 'N/A' for missing keys in str.format_map()."""
    def __missing__(self, key: str) -> str:
        return "N/A"


# ── Data Classes ─────────────────────────────────────────────

@dataclass
class AggregationSpec:
    """One aggregation rule (currently only sum is supported)."""
    field: str
    label: str


@dataclass
class LookupSpec:
    """Parsed lookup configuration for one API source."""
    source_id: str
    display_name: str
    enabled: bool

    # Query matching
    keywords: List[str]
    compiled_patterns: List[re.Pattern]

    # Fetch info (from main config sections)
    fetch_url: str
    root_selector: str  # where to find records in API response JSON

    # Display formatting
    title: str
    subtitle: str             # e.g. "({count} divisions)"
    sort_by: str
    sort_fallback: str
    name_field: str           # primary name field for {_name}
    name_fallback: str        # fallback name field for {_name}
    record_template: str      # per-record markdown display line

    # Aggregation (optional)
    aggregations: List[AggregationSpec]
    summary_template: str     # uses {Label} placeholders from aggregations

    # Context formatting (for LLM evidence)
    context_title: str
    context_record_template: str

    # Live display overrides
    live_title: str
    live_max_records: int


# ── Registry Singleton ───────────────────────────────────────

_registry: Optional[Dict[str, LookupSpec]] = None
_DEFAULT_SOURCE_DIR = os.path.join(os.path.dirname(__file__), "assets", "apis")
_DEFAULT_DB_PATH = os.path.join("data", "api_ingestion.db")


def _parse_lookup_spec(yaml_data: dict, config_path: str) -> Optional[LookupSpec]:
    """Parse a LookupSpec from a raw YAML config dict.  Returns None if
    the config has no ``lookup:`` section or it is disabled."""
    lookup = yaml_data.get("lookup")
    if not lookup or not lookup.get("enabled", True):
        return None

    source_id = yaml_data.get("source_id", "")
    if not source_id:
        return None

    # Compile keyword regex patterns
    patterns: List[re.Pattern] = []
    for pat_str in (lookup.get("keyword_patterns") or []):
        try:
            patterns.append(re.compile(pat_str, re.IGNORECASE))
        except re.error as exc:
            log.warning("Invalid regex in %s: %s — %s", config_path, pat_str, exc)

    # Pull fetch / normalization info from the parent config
    fetch = yaml_data.get("fetch", {})
    norm = yaml_data.get("normalization", {})

    # Parse aggregation rules
    aggs: List[AggregationSpec] = []
    for agg in (lookup.get("aggregations") or []):
        if isinstance(agg, dict) and "field" in agg and "label" in agg:
            aggs.append(AggregationSpec(field=agg["field"], label=agg["label"]))

    return LookupSpec(
        source_id=source_id,
        display_name=yaml_data.get("display_name", source_id),
        enabled=yaml_data.get("enabled", True),
        keywords=[str(k).lower().strip() for k in (lookup.get("keywords") or [])],
        compiled_patterns=patterns,
        fetch_url=fetch.get("url", ""),
        root_selector=norm.get("root_selector", ""),
        title=lookup.get("title", yaml_data.get("display_name", source_id)),
        subtitle=lookup.get("subtitle", "({count} records)"),
        sort_by=lookup.get("sort_by", ""),
        sort_fallback=lookup.get("sort_fallback", "name"),
        name_field=lookup.get("name_field", ""),
        name_fallback=lookup.get("name_fallback", "name"),
        record_template=lookup.get("record_template", "- {_name}"),
        aggregations=aggs,
        summary_template=lookup.get("summary_template", ""),
        context_title=lookup.get("context_title", ""),
        context_record_template=lookup.get("context_record_template", "- {_name}"),
        live_title=lookup.get("live_title", ""),
        live_max_records=int(lookup.get("live_max_records", 50)),
    )


def _load_registry(source_dir: str = _DEFAULT_SOURCE_DIR) -> Dict[str, LookupSpec]:
    """Scan all YAML configs in *source_dir* and build the lookup registry."""
    registry: Dict[str, LookupSpec] = OrderedDict()

    if not os.path.isdir(source_dir):
        log.info("API source dir not found: %s", source_dir)
        return registry

    for fname in sorted(os.listdir(source_dir)):
        if fname.startswith((".", "_")):
            continue
        if not fname.lower().endswith((".yaml", ".yml")):
            continue

        path = os.path.join(source_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
            if not isinstance(data, dict):
                continue

            # Skip disabled sources entirely
            if not data.get("enabled", True):
                continue

            spec = _parse_lookup_spec(data, path)
            if spec:
                registry[spec.source_id] = spec
                log.info(
                    "Registered lookup: %s (%d keywords, %d patterns)",
                    spec.source_id, len(spec.keywords), len(spec.compiled_patterns),
                )
        except Exception as exc:
            log.warning("Failed to parse API config %s: %s", path, exc)

    log.info("Lookup registry loaded: %d API source(s)", len(registry))
    return registry


def get_registry() -> Dict[str, LookupSpec]:
    """Return the lookup registry (lazy-loaded singleton)."""
    global _registry
    if _registry is None:
        _registry = _load_registry()
    return _registry


def reload_registry(source_dir: str = _DEFAULT_SOURCE_DIR) -> None:
    """Force-reload the registry (e.g. after adding a new YAML config)."""
    global _registry
    _registry = _load_registry(source_dir)


# ── Intent Detection ─────────────────────────────────────────

def detect_lookup_intent(question: str) -> Optional[str]:
    """
    Detect whether *question* matches any registered API lookup.

    Returns the ``source_id`` of the matching API, or ``None``.

    Matching order:
      1. Compiled regex patterns (most precise)
      2. Simple keyword substring matching (fallback)
    """
    q = (question or "").strip()
    if not q:
        return None

    q_lower = q.lower()
    registry = get_registry()

    # Phase 1 — regex patterns
    for source_id, spec in registry.items():
        for pattern in spec.compiled_patterns:
            if pattern.search(q):
                return source_id

    # Phase 2 — keyword substring
    for source_id, spec in registry.items():
        for kw in spec.keywords:
            if kw in q_lower:
                return source_id

    return None


# ── Stored (SQLite) Lookup ───────────────────────────────────

def execute_stored_lookup(
    source_id: str,
    db_path: str = _DEFAULT_DB_PATH,
) -> Optional[Dict[str, Any]]:
    """
    Generic stored-API lookup: fetch all active records for *source_id*
    from SQLite, then format according to the YAML display spec.

    Returns the same dict shape the old per-API ``lookup_*()`` functions
    produced, or ``None`` if no data is found.
    """
    registry = get_registry()
    spec = registry.get(source_id)
    if not spec:
        log.warning("No lookup spec registered for source_id: %s", source_id)
        return None

    records = _fetch_records_from_db(source_id, db_path)
    if not records:
        return None

    formatted_answer = _format_stored_answer(spec, records)
    formatted_context = build_evidence_context(source_id, records)

    log.info("Generic lookup [%s]: %d records", source_id, len(records))

    return {
        "success": True,
        "source_id": source_id,
        "records": records,
        "formatted_answer": formatted_answer,
        "formatted_context": formatted_context,
        "count": len(records),
    }


def _fetch_records_from_db(source_id: str, db_path: str) -> List[Dict]:
    """Fetch ``raw_json`` records from ``api_records`` by *source_id*."""
    if not os.path.exists(db_path):
        log.warning("API database not found at %s", db_path)
        return []

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT record_id, raw_json, normalized_text "
            "FROM api_records WHERE source_id = ? "
            "AND is_active = 1 ORDER BY record_id",
            (source_id,),
        ).fetchall()
        conn.close()
    except Exception as exc:
        log.error("Failed to query records for %s: %s", source_id, exc)
        return []

    records: List[Dict] = []
    for row in rows:
        try:
            records.append(json.loads(row["raw_json"]))
        except (json.JSONDecodeError, KeyError):
            continue

    return records


# ── Formatting Helpers ───────────────────────────────────────

def _format_value(val: Any) -> str:
    """Format a single value for display (auto comma-separate numbers)."""
    if isinstance(val, int):
        return f"{val:,}"
    if isinstance(val, float):
        return f"{val:,.0f}"
    if val is None:
        return "N/A"
    return str(val)


def _make_format_dict(rec: Dict, spec: LookupSpec) -> _SafeDict:
    """Build a safe format dict from a record with auto-formatted numbers
    and a resolved ``{_name}`` placeholder."""
    fmt: Dict[str, str] = {}
    for k, v in rec.items():
        fmt[k] = _format_value(v)

    # Resolve _name
    if spec.name_field:
        name = rec.get(spec.name_field)
        if name is None and spec.name_fallback:
            name = rec.get(spec.name_fallback)
        fmt["_name"] = str(name) if name is not None else "N/A"
    else:
        fmt["_name"] = str(rec.get("name", "N/A"))

    return _SafeDict(fmt)


def _sort_records(records: List[Dict], spec: LookupSpec) -> List[Dict]:
    """Sort records according to the spec's ``sort_by`` / ``sort_fallback``."""
    if not spec.sort_by:
        return records

    def sort_key(r: Dict) -> str:
        val = r.get(spec.sort_by)
        if val is None and spec.sort_fallback:
            val = r.get(spec.sort_fallback, "")
        return str(val or "").lower()

    return sorted(records, key=sort_key)


def _compute_aggregations(records: List[Dict], spec: LookupSpec) -> Dict[str, str]:
    """Compute sum aggregations and return ``{label: formatted_value}``."""
    result: Dict[str, str] = {}
    for agg in spec.aggregations:
        total = sum(
            r.get(agg.field, 0)
            for r in records
            if isinstance(r.get(agg.field, 0), (int, float))
        )
        result[agg.label] = _format_value(total)
    return result


# ── Stored Answer Formatting ─────────────────────────────────

def _format_stored_answer(spec: LookupSpec, records: List[Dict]) -> str:
    """Format records into a display-ready markdown answer string."""
    lines: List[str] = []

    # Title + subtitle
    title = f"**{spec.title}**"
    if spec.subtitle:
        subtitle = spec.subtitle.replace("{count}", str(len(records)))
        title += f" {subtitle}"
    lines.append(title)
    lines.append("")

    # Summary line (aggregations)
    if spec.aggregations and spec.summary_template:
        agg_values = _compute_aggregations(records, spec)
        try:
            summary = spec.summary_template.format_map(_SafeDict(agg_values))
            lines.append(summary)
            lines.append("")
        except Exception:
            pass

    # Sorted records
    for rec in _sort_records(records, spec):
        fmt = _make_format_dict(rec, spec)
        try:
            lines.append(spec.record_template.format_map(fmt))
        except Exception:
            lines.append(f"- {rec}")

    return "\n".join(lines)


# ── Evidence Context (for LLM) ──────────────────────────────

def build_evidence_context(source_id: str, records: List[Dict]) -> str:
    """
    Build plain-text evidence context for the LLM answerer.

    Public API so ``stored_api_lookup.build_lookup_retrieval()`` can call
    it without reaching into private functions.
    """
    registry = get_registry()
    spec = registry.get(source_id)
    if not spec:
        # Fallback for unknown source_id
        lines = [f"[Source Type: API]", f"[Total Records: {len(records)}]", ""]
        for rec in records:
            lines.append(f"- {json.dumps(rec, default=str)}")
        return "\n".join(lines)

    return _build_context(spec, records)


def _build_context(spec: LookupSpec, records: List[Dict]) -> str:
    """Internal: build plain-text evidence context from a LookupSpec."""
    lines: List[str] = [
        f"[Source Type: API]",
        f"[API Name: {spec.display_name}]",
        f"[Total Records: {len(records)}]",
        "",
    ]

    if spec.context_title:
        lines.append(f"{spec.context_title}:")

    # Aggregation summary (stripped of markdown bold)
    if spec.aggregations and spec.summary_template:
        agg_values = _compute_aggregations(records, spec)
        try:
            summary = spec.summary_template.format_map(_SafeDict(agg_values))
            summary = summary.replace("**", "")
            lines.append(summary)
            lines.append("")
        except Exception:
            pass

    for rec in _sort_records(records, spec):
        fmt = _make_format_dict(rec, spec)
        try:
            lines.append(spec.context_record_template.format_map(fmt))
        except Exception:
            lines.append(f"- {json.dumps(rec, default=str)}")

    return "\n".join(lines)


# ── Live API Formatting ──────────────────────────────────────

def format_live_response(source_id: str, data: Any) -> Optional[str]:
    """
    Format a live API JSON response using the YAML display spec.

    Returns formatted markdown text, or ``None`` if *source_id* is not
    in the registry (caller should fall back to raw formatting).
    """
    registry = get_registry()
    spec = registry.get(source_id)
    if not spec:
        return None

    # Extract records from the response using root_selector
    records = _extract_records_from_response(data, spec.root_selector)
    if not records:
        title = spec.live_title or f"Live Data: {spec.title}"
        return f"**{title}**\n\n{str(data)[:2000]}"

    lines: List[str] = []
    title = spec.live_title or f"Live Data: {spec.title}"
    lines.append(f"**{title}** ({len(records)} records)")
    lines.append("")

    # Summary
    capped = records[: spec.live_max_records]
    if spec.aggregations and spec.summary_template:
        agg_values = _compute_aggregations(capped, spec)
        try:
            summary = spec.summary_template.format_map(_SafeDict(agg_values))
            lines.append(summary)
            lines.append("")
        except Exception:
            pass

    for rec in _sort_records(capped, spec):
        fmt = _make_format_dict(rec, spec)
        try:
            lines.append(spec.record_template.format_map(fmt))
        except Exception:
            lines.append(f"- {rec}")

    if len(records) > spec.live_max_records:
        lines.append(f"\n*...and {len(records) - spec.live_max_records} more records*")

    return "\n".join(lines)


def _extract_records_from_response(data: Any, root_selector: str) -> List[Dict]:
    """Extract a list of record dicts from a JSON API response."""
    if root_selector == "__root__":
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Common wrappers
            for key in ("data", "results", "items"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            return [data]
        return []

    if isinstance(data, dict) and root_selector:
        # Support dotted paths: "data.items"
        current: Any = data
        for key in root_selector.split("."):
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return []
        if isinstance(current, list):
            return current
        if isinstance(current, dict):
            return [current]
        return []

    return data if isinstance(data, list) else []


# ── Live Endpoint Discovery ──────────────────────────────────

def get_live_endpoints() -> Dict[str, Dict[str, Any]]:
    """
    Build a live-endpoints dict from the registry.

    Returns the same shape as the old hardcoded
    ``live_api_handler.LIVE_ENDPOINTS``.
    """
    registry = get_registry()
    endpoints: Dict[str, Dict[str, Any]] = {}

    for source_id, spec in registry.items():
        if spec.fetch_url:
            endpoints[source_id] = {
                "url": spec.fetch_url,
                "display_name": spec.display_name,
                "description": spec.title,
            }

    return endpoints


def match_endpoint(question: str) -> Optional[str]:
    """Match a question to a live API endpoint.  Returns ``source_id``
    or ``None``.  (Alias for ``detect_lookup_intent``.)"""
    return detect_lookup_intent(question)
