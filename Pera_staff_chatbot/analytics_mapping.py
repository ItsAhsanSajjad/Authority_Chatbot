"""
PERA AI — Analytics API-to-Curated Table Mapping

Maps specific API source records into curated PostgreSQL dimension
and fact tables. Only known source_ids are mapped; unknown sources
are silently skipped.

Supported mappings:
    app_data_divisions  → dim_division
    app_data_districts  → dim_district
    app_data_tehsils    → dim_tehsil
    pera_strength       → fact_workforce_strength
    finance_overview    → fact_finance_overview_summary (top-level)
                        + fact_finance_overview_monthly (series rows)

Usage:
    from analytics_mapping import AnalyticsMapper
    mapper = AnalyticsMapper(store)
    mapper.map_and_store(source_id, records, raw_payload=payload)
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, List, Optional

from log_config import get_logger

log = get_logger("pera.analytics.mapping")


# Registry of source_id → mapping function name
_SOURCE_MAP = {
    "app_data_divisions": "_map_divisions",
    "app_data_districts": "_map_districts",
    "app_data_tehsils": "_map_tehsils",
    "pera_strength": "_map_workforce",
    "finance_overview": "_map_finance",
}


class AnalyticsMapper:
    """
    Maps normalized API records into curated dimension and fact tables.
    Unmapped source_ids are silently skipped.
    """

    def __init__(self, store):
        """
        Args:
            store: AnalyticsStore instance
        """
        self.store = store

    def has_mapping(self, source_id: str) -> bool:
        """Check if a source_id has a curated mapping."""
        return source_id in _SOURCE_MAP

    def map_and_store(
        self,
        source_id: str,
        records: List[Dict[str, Any]],
        snapshot_date: Optional[date] = None,
        raw_payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Map records from a known source into curated tables.
        Returns count of records successfully mapped, or 0 if unmapped/failed.

        Args:
            source_id: API source identifier
            records: List of extracted records (each with raw_record dict)
            snapshot_date: Optional snapshot date (defaults to today)
            raw_payload: Optional full API response payload (for summary extraction)
        """
        if source_id not in _SOURCE_MAP:
            return 0

        method_name = _SOURCE_MAP[source_id]
        method = getattr(self, method_name, None)
        if not method:
            log.warning("Mapping method %s not found", method_name)
            return 0

        snap_date = snapshot_date or date.today()
        try:
            count = method(source_id, records, snap_date, raw_payload=raw_payload)
            log.info(
                "Analytics mapping: %d records from %s → curated tables",
                count, source_id,
            )
            return count
        except Exception as e:
            log.warning(
                "Analytics mapping failed for %s: %s", source_id, e
            )
            return 0

    # ── Division Mapping ──────────────────────────────────────

    def _map_divisions(
        self, source_id: str, records: List[Dict[str, Any]], snap_date: date,
        **kwargs,
    ) -> int:
        """Map app_data_divisions → dim_division."""
        count = 0
        for rec in records:
            raw = rec.get("raw_record") or rec
            division_id = _safe_int(raw.get("id") or raw.get("division_id"))
            if division_id is None:
                continue

            name = str(raw.get("name") or raw.get("division_name") or "").strip()
            name_ur = str(raw.get("name_ur") or raw.get("division_name_ur") or "").strip()
            code = str(raw.get("code") or raw.get("division_code") or "").strip()

            success = self.store.upsert_division(
                division_id=division_id,
                division_name=name,
                division_name_ur=name_ur,
                code=code,
                source_id=source_id,
                raw_json=raw,
            )
            if success:
                count += 1
        return count

    # ── District Mapping ──────────────────────────────────────

    def _map_districts(
        self, source_id: str, records: List[Dict[str, Any]], snap_date: date,
        **kwargs,
    ) -> int:
        """Map app_data_districts → dim_district."""
        count = 0
        for rec in records:
            raw = rec.get("raw_record") or rec
            district_id = _safe_int(raw.get("id") or raw.get("district_id"))
            if district_id is None:
                continue

            name = str(raw.get("name") or raw.get("district_name") or "").strip()
            name_ur = str(raw.get("name_ur") or raw.get("district_name_ur") or "").strip()
            division_id = _safe_int(raw.get("division_id") or raw.get("division"))
            code = str(raw.get("code") or raw.get("district_code") or "").strip()

            success = self.store.upsert_district(
                district_id=district_id,
                district_name=name,
                division_id=division_id,
                district_name_ur=name_ur,
                code=code,
                source_id=source_id,
                raw_json=raw,
            )
            if success:
                count += 1
        return count

    # ── Tehsil Mapping ────────────────────────────────────────

    def _map_tehsils(
        self, source_id: str, records: List[Dict[str, Any]], snap_date: date,
        **kwargs,
    ) -> int:
        """Map app_data_tehsils → dim_tehsil."""
        count = 0
        for rec in records:
            raw = rec.get("raw_record") or rec
            tehsil_id = _safe_int(raw.get("id") or raw.get("tehsil_id"))
            if tehsil_id is None:
                continue

            name = str(raw.get("name") or raw.get("tehsil_name") or "").strip()
            name_ur = str(raw.get("name_ur") or raw.get("tehsil_name_ur") or "").strip()
            district_id = _safe_int(raw.get("district_id") or raw.get("district"))
            code = str(raw.get("code") or raw.get("tehsil_code") or "").strip()

            success = self.store.upsert_tehsil(
                tehsil_id=tehsil_id,
                tehsil_name=name,
                district_id=district_id,
                tehsil_name_ur=name_ur,
                code=code,
                source_id=source_id,
                raw_json=raw,
            )
            if success:
                count += 1
        return count

    # ── Workforce Mapping ─────────────────────────────────────

    def _map_workforce(
        self, source_id: str, records: List[Dict[str, Any]], snap_date: date,
        **kwargs,
    ) -> int:
        """Map pera_strength → fact_workforce_strength.

        Real API payload fields:
            id, divisionId, divisionName, total, onDuty, absent
        """
        count = 0
        for rec in records:
            raw = rec.get("raw_record") or rec
            division_id = _safe_int(
                raw.get("divisionId") or raw.get("division_id")
            )
            district_id = _safe_int(
                raw.get("districtId") or raw.get("district_id")
            )
            # Use divisionName as a label in designation (no separate designation field)
            designation = str(
                raw.get("divisionName") or raw.get("designation") or ""
            ).strip() or None
            cadre = str(raw.get("cadre") or "").strip() or None

            success = self.store.upsert_workforce(
                source_id=source_id,
                snapshot_date=snap_date,
                designation=designation,
                cadre=cadre,
                division_id=division_id,
                district_id=district_id,
                sanctioned_posts=_safe_int(
                    raw.get("total") or raw.get("sanctioned") or raw.get("sanctioned_posts")
                ) or 0,
                filled_posts=_safe_int(
                    raw.get("onDuty") or raw.get("filled") or raw.get("filled_posts")
                ) or 0,
                vacant_posts=_safe_int(
                    raw.get("absent") or raw.get("vacant") or raw.get("vacant_posts")
                ) or 0,
                contract_employees=_safe_int(raw.get("contract") or raw.get("contract_employees")) or 0,
                deputation_in=_safe_int(raw.get("deputation_in")) or 0,
                deputation_out=_safe_int(raw.get("deputation_out")) or 0,
                raw_json=raw,
            )
            if success:
                count += 1
        return count

    # ── Finance Mapping ───────────────────────────────────────

    def _map_finance(
        self, source_id: str, records: List[Dict[str, Any]], snap_date: date,
        raw_payload: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> int:
        """Map finance_overview → fact_finance_overview_summary + fact_finance_overview_monthly.

        Real API response shape:
            {
              "totalReleased": 9461.06,
              "totalUtilized": 2339.78,
              "utilizationRate": 24.73,
              "series": [{"month": "Jul", "expenditure": 12.6}, ...]
            }

        - Summary (totalReleased/totalUtilized/utilizationRate) → fact_finance_overview_summary
        - Series records (month/expenditure) → fact_finance_overview_monthly
        """
        count = 0

        # ── Write summary from raw_payload (top-level fields) ──
        if raw_payload and isinstance(raw_payload, dict):
            total_released = _safe_float(raw_payload.get("totalReleased"))
            total_utilized = _safe_float(raw_payload.get("totalUtilized"))
            utilization_rate = _safe_float(raw_payload.get("utilizationRate"))

            if total_released > 0 or total_utilized > 0:
                summary_json = {
                    "totalReleased": raw_payload.get("totalReleased"),
                    "totalUtilized": raw_payload.get("totalUtilized"),
                    "utilizationRate": raw_payload.get("utilizationRate"),
                }
                success = self.store.upsert_finance_summary(
                    source_id=source_id,
                    snapshot_date=snap_date,
                    total_released=total_released,
                    total_utilized=total_utilized,
                    utilization_rate=utilization_rate,
                    raw_json=summary_json,
                )
                if success:
                    count += 1

        # ── Write monthly rows from extracted records ──────────
        for rec in records:
            raw = rec.get("raw_record") or rec
            month_label = str(raw.get("month") or "").strip()
            if not month_label:
                continue

            expenditure = _safe_float(raw.get("expenditure"))

            success = self.store.upsert_finance_monthly(
                source_id=source_id,
                snapshot_date=snap_date,
                month_label=month_label,
                expenditure_amount=expenditure,
                raw_json=raw,
            )
            if success:
                count += 1
        return count


# ── Utility helpers ───────────────────────────────────────────


def _safe_int(val: Any) -> Optional[int]:
    """Safely convert a value to int, or return None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> float:
    """Safely convert a value to float, or return 0.0."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

