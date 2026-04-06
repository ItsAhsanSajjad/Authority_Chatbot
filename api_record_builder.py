"""
PERA AI — API Record Builder

Builds normalized records from raw API response data.
Produces deterministic canonical JSON/text and stable hashes.

Phase 2 module.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api_config_models import ApiSourceConfig
from api_flattener import ApiJsonFlattener
from log_config import get_logger

log = get_logger("pera.api.record_builder")


@dataclass
class NormalizedApiRecord:
    """A single normalized record from an API source."""
    source_id: str = ""
    record_id: str = ""
    record_type: str = ""
    canonical_json: str = ""
    canonical_text: str = ""
    record_hash: str = ""
    display_title: str = ""
    field_list: List[str] = field(default_factory=list)
    raw_record: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True
    first_seen_at: float = 0.0
    last_updated_at: float = 0.0


class ApiRecordBuilder:
    """Builds normalized records from API response payloads."""

    def __init__(self):
        self._flattener = ApiJsonFlattener()

    def extract_records(
        self, config: ApiSourceConfig, payload: Any
    ) -> List[Dict[str, Any]]:
        """Extract the records array from a payload using root_selector.

        Special selectors:
            __root__  — use the payload itself (supports top-level arrays)
            (empty)   — use the payload itself (legacy behavior)
        """
        selector = config.normalization.root_selector
        if not selector:
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                return [payload]
            return []

        # __root__ sentinel: use payload as-is (explicit top-level array)
        if selector == "__root__":
            if isinstance(payload, list):
                return payload
            if isinstance(payload, dict):
                return [payload]
            return []

        current = payload
        for part in selector.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return []

        if isinstance(current, list):
            return current
        if isinstance(current, dict):
            return [current]
        return []

    def build_record(
        self, config: ApiSourceConfig, raw_record: Dict[str, Any]
    ) -> NormalizedApiRecord:
        """Build a NormalizedApiRecord from a raw record dict."""
        now = time.time()

        # Apply include/exclude field filtering
        filtered = self._filter_fields(config, raw_record)

        # Compute record ID
        record_id = self.compute_record_id(config, raw_record)

        # Build canonical representations
        canonical_json = self.build_canonical_json(filtered)
        canonical_text = self.build_canonical_text(config, filtered)
        record_hash = self.compute_record_hash(canonical_json)

        # Display title
        display_title = self._build_display_title(config, filtered, record_id)

        return NormalizedApiRecord(
            source_id=config.source_id,
            record_id=record_id,
            record_type=config.normalization.record_type,
            canonical_json=canonical_json,
            canonical_text=canonical_text,
            record_hash=record_hash,
            display_title=display_title,
            field_list=list(filtered.keys()),
            raw_record=raw_record,
            is_active=True,
            first_seen_at=now,
            last_updated_at=now,
        )

    def build_canonical_json(self, filtered_record: Dict[str, Any]) -> str:
        """Build deterministic canonical JSON from a filtered record."""
        return json.dumps(filtered_record, sort_keys=True, ensure_ascii=False)

    def build_canonical_text(
        self, config: ApiSourceConfig, filtered_record: Dict[str, Any]
    ) -> str:
        """
        Build canonical text from a filtered record.
        Uses text_template if available, otherwise generates fallback.
        """
        template = config.normalization.text_template
        if template:
            return self._apply_template(template, filtered_record)
        return self._build_fallback_text(config, filtered_record)

    def compute_record_id(
        self, config: ApiSourceConfig, raw_record: Dict[str, Any]
    ) -> str:
        """Compute a stable record ID."""
        id_field = config.normalization.record_id_field
        if id_field and id_field in raw_record:
            return str(raw_record[id_field]).strip()

        # Fallback: hash the full record
        canonical = json.dumps(raw_record, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

    def compute_record_hash(self, canonical_json: str) -> str:
        """Compute a stable hash from canonical JSON."""
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    # ── Private helpers ───────────────────────────────────────

    def _filter_fields(
        self, config: ApiSourceConfig, record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply include/exclude field filtering."""
        include = config.normalization.include_fields
        exclude = config.normalization.exclude_fields

        if include:
            filtered = {k: v for k, v in record.items() if k in include}
        else:
            filtered = dict(record)

        if exclude:
            filtered = {k: v for k, v in filtered.items() if k not in exclude}

        # Flatten nested values if strategy is flatten
        if config.normalization.nested_strategy == "flatten":
            result = {}
            for k, v in filtered.items():
                if isinstance(v, (dict, list)):
                    flat = self._flattener.flatten_nested_value(v, k)
                    result.update(flat)
                else:
                    result[k] = v
            return result

        return filtered

    def _apply_template(
        self, template: str, record: Dict[str, Any]
    ) -> str:
        """Apply a text template with field substitution."""
        text = template
        for key, value in record.items():
            placeholder = "{" + key + "}"
            if placeholder in text:
                text = text.replace(placeholder, str(value) if value is not None else "")
        return text.strip()

    def _build_fallback_text(
        self, config: ApiSourceConfig, record: Dict[str, Any]
    ) -> str:
        """Generate text from record fields when no template is provided."""
        parts = []
        rtype = config.normalization.record_type
        if rtype:
            parts.append(f"{rtype.title()} record.")

        for key, value in sorted(record.items()):
            if value is not None and str(value).strip():
                label = key.replace("_", " ").title()
                parts.append(f"{label}: {value}")

        return "\n".join(parts)

    def _build_display_title(
        self, config: ApiSourceConfig, record: Dict[str, Any], record_id: str
    ) -> str:
        """Build a human-readable display title."""
        # Try common title fields
        for field_name in ("name", "title", "display_name", "label"):
            if field_name in record and record[field_name]:
                return str(record[field_name])

        rtype = config.normalization.record_type or "Record"
        return f"{rtype.title()} {record_id}"
