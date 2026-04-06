"""
PERA AI — API Payload Validators

Validates raw JSON payloads from API responses before normalization.
Checks structure, required fields, record IDs, and empty payload guards.

Phase 2 module.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api_config_models import ApiSourceConfig
from log_config import get_logger
from settings import get_settings

log = get_logger("pera.api.validators")


@dataclass
class ValidationResult:
    """Structured result from payload validation."""
    valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    record_count: int = 0

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)
        self.valid = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)


class ApiPayloadValidator:
    """Validates API response payloads before record normalization."""

    def __init__(self):
        self._settings = get_settings()

    def validate_json_payload(
        self, config: ApiSourceConfig, payload: Any
    ) -> ValidationResult:
        """Full validation pipeline for a fetched payload."""
        result = ValidationResult()

        if payload is None:
            result.add_error("Payload is None")
            return result

        if not isinstance(payload, (dict, list)):
            result.add_error(
                f"Payload must be dict or list, got {type(payload).__name__}"
            )
            return result

        # Extract records using root_selector
        records = self._extract_root(config, payload, result)
        if not result.valid:
            return result

        if records is None:
            records = []

        if not isinstance(records, list):
            records = [records]

        result.record_count = len(records)

        # Empty payload check
        min_expected = self._settings.API_MIN_EXPECTED_RECORDS_DEFAULT
        if len(records) == 0:
            if self._settings.API_BLOCK_EMPTY_SNAPSHOT_REPLACEMENT:
                result.add_error("Payload contains 0 records (blocked by safety policy)")
            else:
                result.add_warning("Payload contains 0 records")
            return result

        if len(records) < min_expected:
            result.add_warning(
                f"Payload has {len(records)} records, expected at least {min_expected}"
            )

        # Validate required fields
        field_result = self.validate_required_fields(config, records)
        result.errors.extend(field_result.errors)
        result.warnings.extend(field_result.warnings)
        if field_result.errors:
            result.valid = False

        # Validate record IDs
        id_result = self.validate_record_ids(config, records)
        result.errors.extend(id_result.errors)
        result.warnings.extend(id_result.warnings)
        if id_result.errors:
            result.valid = False

        return result

    def validate_root_selector(
        self, config: ApiSourceConfig, payload: Any
    ) -> ValidationResult:
        """Validate that root_selector can extract data from payload."""
        result = ValidationResult()
        self._extract_root(config, payload, result)
        return result

    def validate_required_fields(
        self, config: ApiSourceConfig, records: List[Dict]
    ) -> ValidationResult:
        """Validate that required include_fields exist in records."""
        result = ValidationResult()
        include = config.normalization.include_fields
        if not include:
            return result  # No required fields specified

        sample_size = min(len(records), 10)
        for i, record in enumerate(records[:sample_size]):
            if not isinstance(record, dict):
                result.add_warning(f"Record {i} is not a dict")
                continue
            missing = [f for f in include if f not in record]
            if missing:
                result.add_warning(
                    f"Record {i} missing fields: {missing}"
                )

        return result

    def validate_record_ids(
        self, config: ApiSourceConfig, records: List[Dict]
    ) -> ValidationResult:
        """Validate record IDs for uniqueness and presence."""
        result = ValidationResult()
        id_field = config.normalization.record_id_field
        if not id_field:
            return result  # No ID field specified, skip

        seen_ids: Dict[str, int] = {}
        missing_count = 0

        for i, record in enumerate(records):
            if not isinstance(record, dict):
                continue
            rid = record.get(id_field)
            if rid is None or str(rid).strip() == "":
                missing_count += 1
                continue
            rid_str = str(rid)
            if rid_str in seen_ids:
                result.add_error(
                    f"Duplicate record ID '{rid_str}' at positions "
                    f"{seen_ids[rid_str]} and {i}"
                )
            else:
                seen_ids[rid_str] = i

        if missing_count > 0:
            result.add_warning(
                f"{missing_count} record(s) missing '{id_field}' field"
            )

        return result

    def _extract_root(
        self,
        config: ApiSourceConfig,
        payload: Any,
        result: ValidationResult,
    ) -> Optional[Any]:
        """Navigate root_selector path in payload.

        Special selectors:
            __root__  — use the payload itself (supports top-level arrays)
            (empty)   — use the payload itself (legacy behavior)
        """
        selector = config.normalization.root_selector
        if not selector:
            return payload if isinstance(payload, list) else payload

        # __root__ sentinel: use payload as-is (explicit top-level array)
        if selector == "__root__":
            return payload

        # Dot-path navigation into dict payloads
        parts = selector.split(".")
        current = payload
        for part in parts:
            if isinstance(current, dict):
                if part not in current:
                    result.add_error(
                        f"root_selector '{selector}': key '{part}' not found in payload"
                    )
                    return None
                current = current[part]
            elif isinstance(current, list):
                result.add_error(
                    f"root_selector '{selector}': payload is a top-level array. "
                    f"Use root_selector: \"__root__\" for top-level list payloads"
                )
                return None
            else:
                result.add_error(
                    f"root_selector '{selector}': cannot navigate into "
                    f"{type(current).__name__} at '{part}'"
                )
                return None
        return current
