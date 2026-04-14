"""
PERA AI — API Normalizer (Orchestrator)

Orchestrates validation + record-building for API fetch results.
Combines payload validation and record normalization into a single result.

Phase 2 module.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from api_config_models import ApiSourceConfig
from api_fetcher import ApiFetchResult
from api_record_builder import ApiRecordBuilder, NormalizedApiRecord
from api_validators import ApiPayloadValidator, ValidationResult
from log_config import get_logger

log = get_logger("pera.api.normalizer")


@dataclass
class ApiNormalizationResult:
    """Result of the normalization pipeline."""
    source_id: str = ""
    success: bool = False
    normalized_records: List[NormalizedApiRecord] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    validation_errors: List[str] = field(default_factory=list)
    record_count: int = 0
    snapshot_hash: str = ""

    def summary(self) -> str:
        status = "OK" if self.success else "FAILED"
        parts = [
            f"[{status}] Source: {self.source_id}",
            f"Records: {self.record_count}",
        ]
        if self.validation_warnings:
            parts.append(f"Warnings: {len(self.validation_warnings)}")
        if self.validation_errors:
            parts.append(f"Errors: {len(self.validation_errors)}")
        return " | ".join(parts)


class ApiNormalizer:
    """
    Orchestrates validation and record-building for API payloads.
    """

    def __init__(
        self,
        validator: Optional[ApiPayloadValidator] = None,
        builder: Optional[ApiRecordBuilder] = None,
    ):
        self._validator = validator or ApiPayloadValidator()
        self._builder = builder or ApiRecordBuilder()

    def normalize_payload(
        self, config: ApiSourceConfig, fetch_result: ApiFetchResult
    ) -> ApiNormalizationResult:
        """
        Full normalization pipeline:
        1. Validate the fetched payload
        2. Extract and build normalized records
        3. Return structured result
        """
        result = ApiNormalizationResult(
            source_id=config.source_id,
            snapshot_hash=fetch_result.snapshot_hash,
        )

        # Check fetch success
        if not fetch_result.success:
            result.validation_errors.append(
                f"Fetch failed: {fetch_result.error_message}"
            )
            log.warning(
                "Normalization skipped for %s: fetch failed", config.source_id
            )
            return result

        # 1. Validate payload
        validation = self._validator.validate_json_payload(
            config, fetch_result.payload
        )
        result.validation_warnings = list(validation.warnings)
        result.validation_errors = list(validation.errors)

        if not validation.valid:
            log.warning(
                "Validation failed for %s: %s",
                config.source_id, "; ".join(validation.errors),
            )
            return result

        # 2. Extract raw records
        raw_records = self._builder.extract_records(config, fetch_result.payload)
        if not raw_records:
            result.validation_warnings.append("No records extracted from payload")
            result.success = True
            return result

        # 3. Build normalized records
        for raw in raw_records:
            if not isinstance(raw, dict):
                result.validation_warnings.append(
                    f"Skipping non-dict record: {type(raw).__name__}"
                )
                continue
            try:
                normalized = self._builder.build_record(config, raw)
                result.normalized_records.append(normalized)
            except Exception as e:
                result.validation_warnings.append(
                    f"Error building record: {e}"
                )
                log.warning("Record build error in %s: %s", config.source_id, e)

        result.record_count = len(result.normalized_records)
        result.success = True

        log.info("Normalized %s: %s", config.source_id, result.summary())
        return result
