"""
PERA AI — Financial Source-of-Truth Map

Single source for the endpoint + raw-key + date-filterability of every
amount-bearing metric used by the chatbot. Consumed by:
  - inspection_lookup._query_tehsil_live (live tehsil performance)
  - inspection_lookup._fetch_tehsil_metrics (district/division fanout)
  - inspection_lookup._format_aggregate_context (rollups)
  - inspection_lookup._query_insp_officer (officer financials)

Rules:
  - Date-ranged answers MUST use date-filtered sources only.
  - Pcm/dashboard-counts amount fields are all-time even when date
    params are passed; they are exposed only under all_time_* keys
    so callers cannot accidentally place them in a date-ranged table.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional


# ── Canonical key → endpoint description ────────────────────
@dataclass(frozen=True)
class AmountSource:
    canonical_key: str
    endpoint: str
    raw_key: str
    is_date_filtered: bool
    description: str = ""


AMOUNT_SOURCES: Dict[str, AmountSource] = {
    # Date-filtered (safe in main tables)
    "fine_imposed": AmountSource(
        "fine_imposed", "/sdeo-dashboard/top-kpis", "totalFineImposed",
        True, "Total fine imposed in the date range",
    ),
    "fine_recovered": AmountSource(
        "fine_recovered", "/sdeo-dashboard/top-kpis", "totalFineRecovered",
        True, "Total fine recovered in the date range",
    ),
    "paid_amount": AmountSource(
        "paid_amount", "/sdeo-dashboard/challan-status-breakdown",
        "paidAmount", True, "Paid amount per date range",
    ),
    "outstanding_amount": AmountSource(
        "outstanding_amount", "/sdeo-dashboard/challan-status-breakdown",
        "unpaidAmount", True,
        "Outstanding/unpaid amount; fallback top-kpis.unpaidFineAmount",
    ),
    "unpaid_amount": AmountSource(
        "unpaid_amount", "/sdeo-dashboard/challan-status-breakdown",
        "unpaidAmount", True, "Alias of outstanding_amount",
    ),
    "partially_paid_amount": AmountSource(
        "partially_paid_amount", "/sdeo-dashboard/challan-status-breakdown",
        "partiallyPaidAmount", True, "Partial-pay amount in date range",
    ),
    "paid_challans": AmountSource(
        "paid_challans", "/sdeo-dashboard/challan-status-breakdown",
        "paidCount", True, "Paid challan count",
    ),
    "unpaid_challans": AmountSource(
        "unpaid_challans", "/sdeo-dashboard/challan-status-breakdown",
        "unpaidCount", True, "Unpaid challan count",
    ),
    "partially_paid_challans": AmountSource(
        "partially_paid_challans", "/sdeo-dashboard/challan-status-breakdown",
        "partiallyPaidCount", True, "Partially paid challan count",
    ),
    # All-time only — must NOT be placed in date-ranged tables.
    "all_time_total_fine": AmountSource(
        "all_time_total_fine", "/Pcm/dashboard-counts", "totalFineAmount",
        False, "Cumulative all-time fine; endpoint ignores date params.",
    ),
    "all_time_paid_amount": AmountSource(
        "all_time_paid_amount", "/Pcm/dashboard-counts", "paidChallanAmount",
        False, "Cumulative all-time paid amount.",
    ),
    "all_time_unpaid_amount": AmountSource(
        "all_time_unpaid_amount", "/Pcm/dashboard-counts", "unPaidChallanAmount",
        False, "Cumulative all-time unpaid amount.",
    ),
}


_DATE_FILTERED_KEYS = {
    k for k, src in AMOUNT_SOURCES.items() if src.is_date_filtered
}
_ALL_TIME_KEYS = {
    k for k, src in AMOUNT_SOURCES.items() if not src.is_date_filtered
}


def is_all_time_source(canonical_key: str) -> bool:
    src = AMOUNT_SOURCES.get(canonical_key)
    return src is not None and not src.is_date_filtered


# ── Money parser ────────────────────────────────────────────
_MONEY_STRIP = re.compile(r"[,\s]")


def parse_money_safe(value: Any) -> Optional[Decimal]:
    """Parse mixed money input ('1,713,500', '1713500.5', 1713500, None)
    into a Decimal. Returns None for unparseable / null input.
    """
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        s = _MONEY_STRIP.sub("", value).replace("Rs.", "").replace("Rs", "")
        if not s:
            return None
        try:
            return Decimal(s)
        except (InvalidOperation, ValueError):
            return None
    return None


def money_to_int(value: Any) -> Optional[int]:
    d = parse_money_safe(value)
    return int(d) if d is not None else None


def format_money(value: Any) -> str:
    """Render a money value as 'Rs. 2,828,000'. Returns 'Rs. 0' for None."""
    d = parse_money_safe(value)
    if d is None:
        return "Rs. 0"
    return f"Rs. {int(d):,}"


# ── Normalizer ──────────────────────────────────────────────
def normalize_sdeo_financial_metrics(
    summary_resp: Optional[Dict[str, Any]] = None,
    top_kpis_resp: Optional[Dict[str, Any]] = None,
    status_resp: Optional[Dict[str, Any]] = None,
    pcm_resp: Optional[Dict[str, Any]] = None,
    *,
    date_ranged: bool = True,
) -> Dict[str, Optional[Decimal]]:
    """Merge raw SDEO endpoint responses into canonical financial keys.

    `date_ranged=True` (default) means the caller's question carries a
    user-supplied date filter; in that case Pcm/dashboard-counts amount
    fields are surfaced ONLY under all_time_* keys so renderers can
    keep them out of the primary table.

    `date_ranged=False` means the caller is intentionally producing an
    all-time answer; PCM amounts are then mirrored into the canonical
    keys for convenience.

    Counts are passed through as-is; only the money fields are routed.
    """
    out: Dict[str, Optional[Decimal]] = {
        "fine_imposed": None,
        "fine_recovered": None,
        "paid_amount": None,
        "outstanding_amount": None,
        "unpaid_amount": None,
        "partially_paid_amount": None,
        "paid_challans": None,
        "unpaid_challans": None,
        "partially_paid_challans": None,
        "all_time_total_fine": None,
        "all_time_paid_amount": None,
        "all_time_unpaid_amount": None,
    }

    if isinstance(top_kpis_resp, dict):
        out["fine_imposed"] = parse_money_safe(top_kpis_resp.get("totalFineImposed"))
        out["fine_recovered"] = parse_money_safe(top_kpis_resp.get("totalFineRecovered"))
        # top-kpis has its own unpaidFineAmount — fallback if status absent.
        if out["outstanding_amount"] is None:
            out["outstanding_amount"] = parse_money_safe(top_kpis_resp.get("unpaidFineAmount"))

    if isinstance(status_resp, dict):
        out["paid_amount"] = parse_money_safe(status_resp.get("paidAmount"))
        outstanding = parse_money_safe(status_resp.get("unpaidAmount"))
        if outstanding is not None:
            out["outstanding_amount"] = outstanding
        out["unpaid_amount"] = out["outstanding_amount"]
        out["partially_paid_amount"] = parse_money_safe(status_resp.get("partiallyPaidAmount"))
        # Counts (kept as integers, not Decimal — but Decimal works too)
        for src_key, dst_key in (
            ("paidCount", "paid_challans"),
            ("unpaidCount", "unpaid_challans"),
            ("partiallyPaidCount", "partially_paid_challans"),
        ):
            v = status_resp.get(src_key)
            if v is not None:
                try:
                    out[dst_key] = int(v)
                except (TypeError, ValueError):
                    pass

    # PCM money: only under all_time_* keys when date_ranged=True.
    if isinstance(pcm_resp, dict):
        out["all_time_total_fine"] = parse_money_safe(pcm_resp.get("totalFineAmount"))
        out["all_time_paid_amount"] = parse_money_safe(pcm_resp.get("paidChallanAmount"))
        out["all_time_unpaid_amount"] = parse_money_safe(pcm_resp.get("unPaidChallanAmount"))
        if not date_ranged:
            # When the caller wants all-time, mirror into canonical keys.
            if out["fine_imposed"] is None:
                out["fine_imposed"] = out["all_time_total_fine"]
            if out["paid_amount"] is None:
                out["paid_amount"] = out["all_time_paid_amount"]
            if out["outstanding_amount"] is None:
                out["outstanding_amount"] = out["all_time_unpaid_amount"]

    return out
