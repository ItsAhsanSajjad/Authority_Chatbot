"""
PERA AI — Structured Last-Turn State

Captures the structured intent of the previous turn so follow-up queries
can inherit metric, entity, date_range, rank_order, status_filter, and
amount_or_count without forcing the user to re-state context.

Encoded intent format examples:
  insp_division:Lahore
  insp_district:Lahore
  insp_tehsil:Shalimar
  insp_officer:Rabia Altaf
  insp_top:firs:tehsil:desc
  insp_top:paid_amount:district:asc
  challan_location:district:Lahore
  challan_daterange:challan_totals:2026-01-01:2026-01-10

Public API:
  - parse_intent_to_last_turn(intent, question="", existing=None)
  - merge_followup(current_question, last_turn) -> LastTurn
  - last_turn_to_intent(last_turn) -> Optional[str]
  - serialize/deserialize via dict
"""
from __future__ import annotations

import re
from dataclasses import dataclass, asdict, field
from datetime import date
from typing import Any, Dict, Optional


# ── Public dataclass ─────────────────────────────────────────
@dataclass
class LastTurn:
    """All structured-routing context from the last successful turn."""
    domain: Optional[str] = None              # inspection | challan | oa | document
    intent: Optional[str] = None              # raw intent string returned by detector
    metric: Optional[str] = None              # firs | sealed | challans | warnings | paid_amount …
    entity_level: Optional[str] = None        # division | district | tehsil | officer
    entity_value: Optional[str] = None        # canonical name
    date_start: Optional[str] = None          # ISO YYYY-MM-DD
    date_end: Optional[str] = None
    rank_order: Optional[str] = None          # asc | desc
    limit: Optional[int] = None
    status_filter: Optional[str] = None       # paid | unpaid | overdue
    amount_or_count: Optional[str] = None     # amount | count
    officer: Optional[str] = None
    last_successful_handler: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "LastTurn":
        if not data:
            return cls()
        # Drop unknown keys so future schema changes don't crash older sessions.
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in valid})


# ── Intent parser ────────────────────────────────────────────
_INSP_TOP_RE = re.compile(
    r"^insp_top:(?P<metric>[a-z_]+):(?P<level>tehsil|district|division)"
    r"(?::(?P<order>asc|desc))?$",
    re.I,
)
_INSP_LOCATION_RE = re.compile(
    r"^insp_(?P<level>division|district|tehsil):(?P<name>.+)$", re.I,
)
_INSP_OFFICER_RE = re.compile(r"^insp_officer:(?P<name>.+)$", re.I)
_CHALLAN_LOCATION_RE = re.compile(
    r"^challan_location:(?P<level>division|district|tehsil):(?P<name>.+)$", re.I,
)
_CHALLAN_DATERANGE_RE = re.compile(
    r"^challan_daterange:(?P<inner>.+?):(?P<start>\d{4}-\d{2}-\d{2}):(?P<end>\d{4}-\d{2}-\d{2})$",
    re.I,
)


def _domain_for(intent: str) -> str:
    if intent.startswith("insp_"):
        return "inspection"
    if intent.startswith("challan"):
        return "challan"
    if intent.startswith("oa_"):
        return "oa"
    return "document"


def parse_intent_to_last_turn(
    intent: str,
    question: str = "",
    existing: Optional[LastTurn] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
) -> LastTurn:
    """Decode a domain-detector intent string into a structured LastTurn.

    `existing` is the prior LastTurn — used to fill fields that the new
    intent doesn't override (typical follow-up case).
    """
    base = existing or LastTurn()
    base.intent = intent
    base.domain = _domain_for(intent or "")

    if date_start:
        base.date_start = date_start.isoformat()
    if date_end:
        base.date_end = date_end.isoformat()

    # insp_top:<metric>:<level>:<order>
    m = _INSP_TOP_RE.match(intent or "")
    if m:
        metric = m.group("metric").lower()
        base.metric = metric
        base.entity_level = m.group("level").lower()
        base.rank_order = (m.group("order") or "desc").lower()
        if metric.endswith("_amount"):
            base.amount_or_count = "amount"
            base.status_filter = (
                metric.replace("_amount", "")
                if metric != "fine_amount" else None
            )
        elif metric.endswith("_challans"):
            base.amount_or_count = "count"
            base.status_filter = metric.replace("_challans", "")
        else:
            base.amount_or_count = "count"
        return base

    m = _INSP_LOCATION_RE.match(intent or "")
    if m:
        base.entity_level = m.group("level").lower()
        base.entity_value = m.group("name").strip()
        return base

    m = _INSP_OFFICER_RE.match(intent or "")
    if m:
        base.entity_level = "officer"
        base.officer = m.group("name").strip()
        base.entity_value = base.officer
        return base

    m = _CHALLAN_LOCATION_RE.match(intent or "")
    if m:
        base.entity_level = m.group("level").lower()
        base.entity_value = m.group("name").strip()
        return base

    m = _CHALLAN_DATERANGE_RE.match(intent or "")
    if m:
        try:
            base.date_start = m.group("start")
            base.date_end = m.group("end")
        except Exception:
            pass

    return base


# ── Follow-up merge ──────────────────────────────────────────
_METRIC_FOLLOWUP_RE = re.compile(
    r"\b(firs?|sealed|warnings?|challans?|inspections?|"
    r"paid|unpaid|overdue|recovered|recovery|fine|amount|"
    r"top|bottom|highest|lowest|maximum|minimum|"
    r"asc(?:ending)?|desc(?:ending)?|"
    r"sab\s*sy\s*z[iy]ada|sab\s*sy\s*kam|kitn[ei]?)\b",
    re.I,
)
_NEW_METRIC_RE = re.compile(
    r"\b(firs?|sealed|warnings?|challans?|inspections?|paid|unpaid|"
    r"overdue|recovered|recovery|fine|amount)\b",
    re.I,
)
_AMOUNT_QUALIFIER_RE = re.compile(
    r"\b(amount|rupees?|rs\.?|pkr|monetary|value|paisa|raqam|by\s+amount)\b",
    re.I,
)
_ASC_RE = re.compile(
    r"\b(asc(?:ending)?|low(?:est)?|least|minimum|min|kam|sab\s*sy\s*kam|bottom|"
    r"reverse(?:\s+order)?)\b",
    re.I,
)
_DESC_RE = re.compile(
    r"\b(desc(?:ending)?|high(?:est)?|most|maximum|max|sab\s*sy\s*z[iy]ada|top)\b",
    re.I,
)


def _detect_status_change(q: str) -> Optional[str]:
    q_low = q.lower()
    if re.search(r"\bunpaid\b", q_low):
        return "unpaid"
    if re.search(r"\boverdue\b", q_low):
        return "overdue"
    if re.search(r"\bpaid\b", q_low):
        return "paid"
    return None


def merge_followup(current_question: str, last_turn: LastTurn) -> LastTurn:
    """Merge a follow-up query into the prior LastTurn.

    Heuristic: the follow-up replaces only the field(s) it explicitly
    mentions; everything else is inherited from the prior turn.

    Examples (with last_turn = ranking by FIRs in Lahore division Jan 1-10):
      "and by challans?"     → metric=challans
      "ascending order"      → rank_order=asc
      "and unpaid?"          → status_filter=unpaid
      "for last month?"      → caller must pass the parsed date_start/end
    """
    out = LastTurn(**asdict(last_turn))  # shallow copy

    if not current_question:
        return out

    # Status filter
    status = _detect_status_change(current_question)
    if status:
        out.status_filter = status
        out.metric = (
            f"{status}_amount" if out.amount_or_count == "amount" else f"{status}_challans"
        )

    # Amount mode toggle
    if _AMOUNT_QUALIFIER_RE.search(current_question):
        out.amount_or_count = "amount"
        if out.status_filter:
            out.metric = f"{out.status_filter}_amount"

    # Order flip
    if _ASC_RE.search(current_question) and not _DESC_RE.search(current_question):
        out.rank_order = "asc"
    elif _DESC_RE.search(current_question) and not _ASC_RE.search(current_question):
        if out.rank_order != "desc":
            out.rank_order = "desc"

    # Bare metric swap ("and by challans?")
    # Skip when the candidate is a status word — those are already
    # handled by _detect_status_change above and overwriting here would
    # discard the carried amount_or_count mode.
    bare_metric_swap = re.match(
        r"^\s*(?:and|or|aur)\s+(?:by\s+)?(\w+)\s*\??\s*$",
        current_question.strip(), re.I,
    )
    if bare_metric_swap:
        candidate = bare_metric_swap.group(1).lower()
        if candidate not in {"paid", "unpaid", "overdue"}:
            m = _NEW_METRIC_RE.search(candidate)
            if m:
                out.metric = candidate

    return out


# ── Re-encode to intent ──────────────────────────────────────
def last_turn_to_intent(lt: LastTurn) -> Optional[str]:
    """Re-encode a LastTurn back into an intent string the existing
    executors understand. Used when the caller wants to skip detector
    re-runs after merging a follow-up.
    """
    if not lt or not lt.domain or lt.domain == "document":
        return None
    if lt.metric and lt.entity_level in ("tehsil", "district", "division"):
        order = lt.rank_order or "desc"
        return f"insp_top:{lt.metric}:{lt.entity_level}:{order}"
    if lt.entity_level == "officer" and (lt.officer or lt.entity_value):
        return f"insp_officer:{lt.officer or lt.entity_value}"
    if lt.entity_level in ("division", "district", "tehsil") and lt.entity_value:
        if lt.domain == "challan":
            return f"challan_location:{lt.entity_level}:{lt.entity_value}"
        return f"insp_{lt.entity_level}:{lt.entity_value}"
    return None
