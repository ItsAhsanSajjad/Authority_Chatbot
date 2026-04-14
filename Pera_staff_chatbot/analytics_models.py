"""
PERA AI — Analytics Data Models

Python dataclasses representing the structured analytics tables.
Used for type-safe inserts and query results.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Optional


# ── Geography Dimensions ──────────────────────────────────────


@dataclass
class DivisionRecord:
    """Punjab division dimension."""
    division_id: int
    division_name: str
    division_name_ur: str = ""
    code: str = ""
    is_active: bool = True
    source_id: str = ""
    snapshot_date: Optional[date] = None
    raw_json: str = "{}"


@dataclass
class DistrictRecord:
    """Punjab district dimension."""
    district_id: int
    district_name: str
    division_id: Optional[int] = None
    district_name_ur: str = ""
    code: str = ""
    is_active: bool = True
    source_id: str = ""
    snapshot_date: Optional[date] = None
    raw_json: str = "{}"


@dataclass
class TehsilRecord:
    """Punjab tehsil dimension."""
    tehsil_id: int
    tehsil_name: str
    district_id: Optional[int] = None
    tehsil_name_ur: str = ""
    code: str = ""
    is_active: bool = True
    source_id: str = ""
    snapshot_date: Optional[date] = None
    raw_json: str = "{}"


# ── Fact Tables ───────────────────────────────────────────────


@dataclass
class WorkforceRecord:
    """PERA workforce strength fact."""
    source_id: str = ""
    snapshot_date: Optional[date] = None
    division_id: Optional[int] = None
    district_id: Optional[int] = None
    designation: str = ""
    cadre: str = ""
    sanctioned_posts: int = 0
    filled_posts: int = 0
    vacant_posts: int = 0
    contract_employees: int = 0
    deputation_in: int = 0
    deputation_out: int = 0
    raw_json: str = "{}"


@dataclass
class FinanceRecord:
    """PERA finance overview fact."""
    source_id: str = ""
    snapshot_date: Optional[date] = None
    fiscal_year: str = ""
    division_id: Optional[int] = None
    district_id: Optional[int] = None
    budget_head: str = ""
    allocated_amount: float = 0.0
    released_amount: float = 0.0
    utilized_amount: float = 0.0
    balance_amount: float = 0.0
    utilization_pct: float = 0.0
    raw_json: str = "{}"


@dataclass
class FinanceSummaryRecord:
    """PERA finance overview summary fact (fact_finance_overview_summary)."""
    source_id: str = ""
    snapshot_date: Optional[date] = None
    fiscal_year: str = ""
    total_released: float = 0.0
    total_utilized: float = 0.0
    utilization_rate: float = 0.0
    raw_json: str = "{}"


@dataclass
class FinanceMonthlyRecord:
    """PERA finance monthly expenditure fact (fact_finance_overview_monthly)."""
    source_id: str = ""
    snapshot_date: Optional[date] = None
    fiscal_year: str = ""
    month_label: str = ""
    expenditure_amount: float = 0.0
    raw_json: str = "{}"


@dataclass
class ChallanStatusRecord:
    """Challan status summary fact (scaffold)."""
    source_id: str = ""
    snapshot_date: Optional[date] = None
    division_id: Optional[int] = None
    district_id: Optional[int] = None
    challan_type: str = ""
    total_issued: int = 0
    total_paid: int = 0
    total_pending: int = 0
    total_amount: float = 0.0
    paid_amount: float = 0.0
    raw_json: str = "{}"


# ── Raw / Ingestion Models ────────────────────────────────────


@dataclass
class RawSnapshotRecord:
    """Raw API snapshot metadata for PostgreSQL."""
    source_id: str = ""
    sync_run_id: Optional[int] = None
    snapshot_json: str = "{}"
    record_count: int = 0
    content_hash: str = ""
    fetched_at: Optional[datetime] = None


@dataclass
class AnalyticsApiRecord:
    """Normalized API record for PostgreSQL analytics store."""
    source_id: str = ""
    record_id: str = ""
    record_type: str = ""
    content_hash: str = ""
    raw_json: str = "{}"
    normalized_text: str = ""
    snapshot_date: Optional[date] = None
    is_active: bool = True
