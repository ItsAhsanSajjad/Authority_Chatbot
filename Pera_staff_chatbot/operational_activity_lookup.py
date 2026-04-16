"""
PERA AI — Operational Activity Lookup

Detects questions about operational activities (requisitions, anti-encroachment,
price control, eviction, anti-hoarding, public nuisance) and queries the
`operational_activity` / `operational_activity_detail` tables.

Returns formatted context for the LLM answerer.
"""
from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from log_config import get_logger

log = get_logger("pera.operational_activity.lookup")

# ── Keyword patterns for intent detection ────────────────────
_OA_KEYWORDS = re.compile(
    r"\b("
    r"req(?:u[ie]?)?sition[s]?|requsition[s]?|"  # requisition + common misspellings
    r"operational\s+activit(?:y|ies)|"
    r"anti[\s-]?encroachment|price[\s-]?control|eviction|"
    r"anti[\s-]?hoarding|public[\s-]?nuisance|"
    # Roman Urdu
    r"tajawuzaat|tajawuz|qeemat\s+control|beydakhal|ihtikaar|"
    r"aam\s+pareshani|"
    # General
    r"operations?\s+(?:count|total|kitni|kitne|report)|"
    r"action[s]?\s+(?:taken|liye|ki|report)|"
    r"how\s+many\s+(?:actions|operations|requisitions)|"
    r"kitni\s+(?:actions?|operations?|karwai|kaarwahi|req(?:u[ie]?)?sitions?|requsitions?)|"
    r"kitne\s+(?:actions?|operations?)|"
    r"assign(?:ed)?|"  # "assign hoi" / "assigned"
    r"karwai|kaarwahi|kaarwaiyan"
    r")\b",
    re.I,
)

# Requisition type mapping
_REQ_TYPE_PATTERNS = {
    "price_control": re.compile(
        r"\b(?:price[\s-]?control|qeemat\s*control|PC)\b", re.I
    ),
    "anti_encroachment": re.compile(
        r"\b(?:anti[\s-]?encroachment|tajawuz(?:aat)?|AE)\b", re.I
    ),
    "eviction": re.compile(
        r"\b(?:eviction[s]?|beydakhal[i]?)\b", re.I
    ),
    "anti_hoarding": re.compile(
        r"\b(?:anti[\s-]?hoarding|ihtikaar|AH)\b", re.I
    ),
    "public_nuisance": re.compile(
        r"\b(?:public[\s-]?nuisance|aam\s*pareshani|PN)\b", re.I
    ),
}

# DB column names for requisition types
_REQ_DB_COL = {
    "price_control": "Price Control",
    "anti_encroachment": "Anti-Encroachment",
    "eviction": "Eviction",
    "anti_hoarding": "Anti-Hoarding",
    "public_nuisance": "Public Nuisance",
}

# Location patterns — division/district/tehsil names
# We'll load these dynamically from the DB
_location_cache: Dict[str, List[Dict]] = {}


def _get_db():
    """Get analytics DB connection."""
    from analytics_db import get_analytics_db
    return get_analytics_db()


def _load_location_cache():
    """Load all known division/district/tehsil names from DB for matching."""
    global _location_cache
    if _location_cache:
        return _location_cache

    db = _get_db()
    if not db:
        return {}

    try:
        divisions = db.fetch_all(
            "SELECT DISTINCT division_id, division_name FROM operational_activity "
            "WHERE level = 'division' AND division_name IS NOT NULL"
        )
        districts = db.fetch_all(
            "SELECT DISTINCT district_id, district_name, division_name FROM operational_activity "
            "WHERE level = 'district' AND district_name IS NOT NULL"
        )
        tehsils = db.fetch_all(
            "SELECT DISTINCT tehsil_id, tehsil_name, district_name, division_name "
            "FROM operational_activity "
            "WHERE level = 'tehsil' AND tehsil_name IS NOT NULL"
        )
        _location_cache = {
            "divisions": divisions,
            "districts": districts,
            "tehsils": tehsils,
        }
    except Exception as e:
        log.error("Failed to load location cache: %s", e)
        _location_cache = {}

    return _location_cache


_EXPLICIT_LEVEL_RE = {
    "division": re.compile(r"\bdivision\b", re.I),
    "district": re.compile(r"\bdistrict\b", re.I),
    "tehsil":   re.compile(r"\btehsil\b", re.I),
}


def _detect_location(question: str) -> Optional[Dict[str, Any]]:
    """
    Detect location (division/district/tehsil) mentioned in question.
    Returns dict with level, name, and IDs, or None.

    If user explicitly says 'division'/'district'/'tehsil', honour that level.
    Otherwise check tehsils → districts → divisions (most specific first).
    """
    q_lower = question.lower()
    cache = _load_location_cache()
    if not cache:
        return None

    # Detect if user explicitly mentions a level
    explicit_level = None
    for lvl, pat in _EXPLICIT_LEVEL_RE.items():
        if pat.search(question):
            explicit_level = lvl
            break

    # Helper to find a name match at a given level
    def _match_division(name_lower):
        for div in cache.get("divisions", []):
            n = (div.get("division_name") or "").lower()
            if n and n in name_lower:
                return {
                    "level": "division",
                    "division_id": div["division_id"],
                    "division_name": div["division_name"],
                }
        return None

    def _match_district(name_lower):
        for d in cache.get("districts", []):
            n = (d.get("district_name") or "").lower()
            if n and n in name_lower:
                return {
                    "level": "district",
                    "district_id": d["district_id"],
                    "district_name": d["district_name"],
                    "division_name": d.get("division_name", ""),
                }
        return None

    def _match_tehsil(name_lower):
        for t in cache.get("tehsils", []):
            n = (t.get("tehsil_name") or "").lower()
            if n and n in name_lower:
                return {
                    "level": "tehsil",
                    "tehsil_id": t["tehsil_id"],
                    "tehsil_name": t["tehsil_name"],
                    "district_name": t.get("district_name", ""),
                    "division_name": t.get("division_name", ""),
                }
        return None

    # If explicit level mentioned, try that level first
    if explicit_level == "division":
        m = _match_division(q_lower)
        if m:
            return m
    elif explicit_level == "district":
        m = _match_district(q_lower)
        if m:
            return m
    elif explicit_level == "tehsil":
        m = _match_tehsil(q_lower)
        if m:
            return m

    # No explicit level or no match → fallback: tehsil → district → division
    m = _match_tehsil(q_lower)
    if m:
        return m
    m = _match_district(q_lower)
    if m:
        return m
    m = _match_division(q_lower)
    if m:
        return m

    return None


def _detect_req_type(question: str) -> Optional[str]:
    """Detect specific requisition type mentioned in question."""
    for req_key, pattern in _REQ_TYPE_PATTERNS.items():
        if pattern.search(question):
            return req_key
    return None


# Cross-domain: user mentions challans in context of OA
_CHALLAN_CROSS_RE = re.compile(
    r"\b(?:challan[s]?|fine[s]?|jurmana|jurmane|penalty|penalties)\b",
    re.I,
)

# ── Officer name cache ─────────────────────────────────────────
_officer_cache: Optional[List[str]] = None


def _load_officer_cache() -> List[str]:
    """Load distinct officer names from requisition_detail for fuzzy matching."""
    global _officer_cache
    if _officer_cache is not None:
        return _officer_cache
    db = _get_db()
    if not db:
        _officer_cache = []
        return _officer_cache
    try:
        rows = db.fetch_all(
            "SELECT DISTINCT created_by_name FROM requisition_detail "
            "WHERE created_by_name IS NOT NULL AND created_by_name != ''"
        )
        _officer_cache = [r["created_by_name"] for r in rows]
    except Exception as e:
        log.warning("Failed to load officer cache: %s", e)
        _officer_cache = []
    return _officer_cache


def _detect_officer_name(question: str) -> Optional[str]:
    """
    Detect if the question mentions a known officer name.
    Uses fuzzy matching: checks if any part of the officer name appears in the
    question (case-insensitive), requiring at least the first+last name to match.
    Returns the full officer name from the DB, or None.
    """
    q_lower = question.lower()
    officers = _load_officer_cache()
    if not officers:
        return None

    best_match = None
    best_score = 0

    for officer in officers:
        # Split officer name into parts (e.g. "Rabia Altaf" or "Amir Hussain EO-0103")
        name_parts = officer.lower().split()
        if not name_parts:
            continue

        # Count how many name parts appear in the question
        matched = sum(1 for p in name_parts if p in q_lower)

        # Require at least 2 parts to match (first + last name),
        # or all parts if the name is only 1 word
        min_required = min(2, len(name_parts))
        if matched >= min_required and matched > best_score:
            best_score = matched
            best_match = officer

    return best_match

# ── Date range extraction (reuse from challan_lookup) ────────
def _extract_date_range_for_oa(question: str) -> Tuple[Optional[date], Optional[date]]:
    """Extract date range from question, reusing challan_lookup's date parsing."""
    try:
        from challan_lookup import _extract_date_range
        result = _extract_date_range(question)
        if result is None:
            return None, None
        return result
    except ImportError:
        log.warning("challan_lookup not available for date parsing")
        return None, None


# ── Intent detection ─────────────────────────────────────────
def detect_operational_activity_intent(question: str) -> Optional[str]:
    """
    Detect if a question is about operational activity data.

    Returns an encoded source_id string like:
      - oa_summary                          (overall totals)
      - oa_division:Lahore                  (division-level)
      - oa_district:Faisalabad              (district-level)
      - oa_tehsil:Model Town                (tehsil-level)
      - oa_req:anti_encroachment            (specific req type, all locations)
      - oa_division_req:Lahore:price_control  (division + req type)
      - oa_officer:Rabia Altaf              (officer-specific)

    Or None if not an operational activity query.
    """
    q = (question or "").strip()
    if not q:
        return None

    # Detect officer name early — "rabia altaf ko requisition" should match
    # even if OA keywords are weak
    officer_name = _detect_officer_name(q)

    has_oa_kw = bool(_OA_KEYWORDS.search(q))

    if not has_oa_kw and not officer_name:
        return None

    # Guard: if query is primarily about INSPECTIONS, defer to inspection handler.
    # e.g. "shalimar station how many inspections ... actions taken" — the "actions taken"
    # matches OA keywords, but the query is about inspections not operational activities.
    _INSP_PRIMARY_RE = re.compile(
        r"\b(inspect(?:ion)?s?|muayina|mu[aā]yin[ae]|firs?\b|sealed|sealing|"
        r"warnings?\b|no\s+offen[cs]e|jaiz[ae]|checking)\b", re.I)
    _OA_SPECIFIC_RE = re.compile(
        r"\b(req(?:u[ie]?)?sitions?|requsitions?|operational\s+activit|"
        r"anti[\s-]?encroachment|price[\s-]?control|eviction|"
        r"anti[\s-]?hoarding|public[\s-]?nuisance|tajawuz|"
        r"qeemat\s+control|beydakhal|ihtikaar)\b", re.I)
    if _INSP_PRIMARY_RE.search(q) and not _OA_SPECIFIC_RE.search(q):
        return None  # defer to inspection handler

    # If officer mentioned + OA keyword (requisition/assign etc), route to officer lookup
    # This check must be BEFORE location detection since officer name shouldn't
    # be confused with a location
    if officer_name and has_oa_kw:
        wants_challan = _CHALLAN_CROSS_RE.search(q)
        if wants_challan:
            return f"oa_cross_challan:oa_officer:{officer_name}"
        return f"oa_officer:{officer_name}"

    # Officer name found but NO OA keyword — not an OA query
    # (could be an inspection query like "how many inspections rabia altaf did")
    if officer_name and not has_oa_kw:
        return None

    location = _detect_location(q)
    req_type = _detect_req_type(q)

    # Check if this is a cross-domain query (OA + Challan)
    wants_challan = _CHALLAN_CROSS_RE.search(q)

    # Build the base OA source_id
    base_oa_id = None
    if location:
        level = location["level"]
        if level == "division":
            name = location["division_name"]
            if req_type:
                base_oa_id = f"oa_division_req:{name}:{req_type}"
            else:
                base_oa_id = f"oa_division:{name}"
        elif level == "district":
            name = location["district_name"]
            if req_type:
                base_oa_id = f"oa_district_req:{name}:{req_type}"
            else:
                base_oa_id = f"oa_district:{name}"
        elif level == "tehsil":
            name = location["tehsil_name"]
            if req_type:
                base_oa_id = f"oa_tehsil_req:{name}:{req_type}"
            else:
                base_oa_id = f"oa_tehsil:{name}"
    elif req_type:
        base_oa_id = f"oa_req:{req_type}"
    else:
        base_oa_id = "oa_summary"

    # If user wants both requisitions AND challans, route to cross-domain
    if wants_challan and base_oa_id:
        return f"oa_cross_challan:{base_oa_id}"

    return base_oa_id


# ── Query execution ──────────────────────────────────────────
def execute_operational_activity_lookup(
    source_id: str,
    question: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Execute an operational activity lookup.
    Returns dict with 'records', 'source_id', 'formatted_context'.
    """
    db = _get_db()
    if not db:
        log.warning("Analytics DB not available")
        return None

    # Parse date range from question
    start_date, end_date = _extract_date_range_for_oa(question)

    # Parse source_id
    parts = source_id.split(":")
    base = parts[0]

    # Cross-domain: oa_cross_challan:<original_oa_source_id>
    if base == "oa_cross_challan":
        oa_sid = ":".join(parts[1:]) if len(parts) > 1 else "oa_summary"
        return _execute_cross_challan_lookup(oa_sid, question)

    result = None

    if base == "oa_summary":
        result = _query_summary(db, start_date, end_date)
    elif base == "oa_division":
        division_name = parts[1] if len(parts) > 1 else ""
        result = _query_by_location(db, "division", division_name, None, start_date, end_date)
    elif base == "oa_district":
        district_name = parts[1] if len(parts) > 1 else ""
        result = _query_by_location(db, "district", district_name, None, start_date, end_date)
    elif base == "oa_tehsil":
        tehsil_name = parts[1] if len(parts) > 1 else ""
        result = _query_by_location(db, "tehsil", tehsil_name, None, start_date, end_date)
    elif base == "oa_division_req":
        division_name = parts[1] if len(parts) > 1 else ""
        req_type = parts[2] if len(parts) > 2 else None
        result = _query_by_location(db, "division", division_name, req_type, start_date, end_date)
    elif base == "oa_district_req":
        district_name = parts[1] if len(parts) > 1 else ""
        req_type = parts[2] if len(parts) > 2 else None
        result = _query_by_location(db, "district", district_name, req_type, start_date, end_date)
    elif base == "oa_tehsil_req":
        tehsil_name = parts[1] if len(parts) > 1 else ""
        req_type = parts[2] if len(parts) > 2 else None
        result = _query_by_location(db, "tehsil", tehsil_name, req_type, start_date, end_date)
    elif base == "oa_req":
        req_type = parts[1] if len(parts) > 1 else None
        result = _query_summary(db, start_date, end_date, req_type)
    elif base == "oa_officer":
        officer_name = ":".join(parts[1:]) if len(parts) > 1 else ""
        result = _query_by_officer(db, officer_name, start_date, end_date)

    return result


def _query_summary(
    db, start_date: Optional[date], end_date: Optional[date],
    req_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Query overall summary — aggregate from detail table if date range, else from summary table."""
    if start_date or end_date:
        return _query_detail_aggregate(
            db, level=None, location_name=None,
            req_type=req_type, start_date=start_date, end_date=end_date,
        )

    # No date range → use summary table (division level for overall)
    if req_type:
        req_col = _REQ_DB_COL.get(req_type, "")
        rows = db.fetch_all(
            "SELECT division_name, " + _safe_col(req_type) + " as count "
            "FROM operational_activity WHERE level = 'division' "
            "AND " + _latest_summary_snapshot() + " "
            "ORDER BY " + _safe_col(req_type) + " DESC"
        )
        total = sum(r["count"] for r in rows)
        context = f"Operational Activity — {req_col} (All Divisions)\n"
        context += f"Total {req_col} actions: {total}\n\n"
        context += "Division-wise breakdown:\n"
        for r in rows:
            context += f"  • {r['division_name']}: {r['count']}\n"
    else:
        rows = db.fetch_all(
            "SELECT division_name, price_control, anti_encroachment, eviction, "
            "anti_hoarding, public_nuisance, total "
            "FROM operational_activity WHERE level = 'division' AND " + _latest_summary_snapshot() + " ORDER BY total DESC"
        )
        grand_total = sum(r["total"] for r in rows)
        context = f"Operational Activity Summary (All Divisions)\n"
        context += f"Grand Total Actions: {grand_total}\n\n"
        context += "Division-wise breakdown:\n"
        for r in rows:
            context += (
                f"  • {r['division_name']}: Total={r['total']} "
                f"(PC={r['price_control']}, AE={r['anti_encroachment']}, "
                f"EV={r['eviction']}, AH={r['anti_hoarding']}, PN={r['public_nuisance']})\n"
            )

    return {
        "source_id": "oa_summary",
        "records": rows,
        "formatted_context": context,
    }


def _safe_col(req_type: str) -> str:
    """Map req_type key to safe SQL column name."""
    mapping = {
        "price_control": "price_control",
        "anti_encroachment": "anti_encroachment",
        "eviction": "eviction",
        "anti_hoarding": "anti_hoarding",
        "public_nuisance": "public_nuisance",
    }
    return mapping.get(req_type, "total")


def _query_by_officer(
    db, officer_name: str,
    start_date: Optional[date], end_date: Optional[date],
) -> Dict[str, Any]:
    """
    Query requisition_detail for a specific officer (created_by_name).
    Returns detailed breakdown of their requisitions by type, location, date.
    """
    use_new = _has_requisition_table(db)
    if not use_new:
        return {
            "source_id": f"oa_officer:{officer_name}",
            "records": [],
            "formatted_context": f"No requisition data available for officer lookup.\n",
        }

    # Use per-officer max snapshot
    snap_sql = "snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_detail WHERE created_by_name = %s)"
    conditions = [snap_sql]
    params: list = [officer_name]

    if start_date:
        conditions.append("created_at >= %s")
        params.append(start_date.isoformat())
    if end_date:
        conditions.append("created_at <= %s")
        params.append(f"{end_date.isoformat()}T23:59:59")

    conditions.append("created_by_name = %s")
    params.append(officer_name)

    where = " AND ".join(conditions)

    rows = db.fetch_all(
        f"SELECT requisition_id, requisition_type_name, created_at::date as dt, "
        f"       tehsil_name, district_name, division_name, area_location, "
        f"       total_squad_members, arrived_members "
        f"FROM requisition_detail WHERE {where} "
        f"ORDER BY created_at",
        tuple(params),
    )

    if not rows:
        date_info = ""
        if start_date and end_date:
            date_info = f" from {start_date} to {end_date}"
        elif start_date:
            date_info = f" from {start_date}"
        return {
            "source_id": f"oa_officer:{officer_name}",
            "records": [],
            "formatted_context": f"No requisitions found for {officer_name}{date_info}.\n",
        }

    # Build context
    date_range_str = ""
    if start_date and end_date:
        date_range_str = f" from {start_date} to {end_date}"
    elif start_date:
        date_range_str = f" from {start_date}"

    context = f"Requisitions by {officer_name}{date_range_str}\n"
    context += f"Total Requisitions: {len(rows)}\n\n"

    # Group by date
    by_date: Dict[str, list] = {}
    by_type: Dict[str, int] = {}
    by_tehsil: Dict[str, int] = {}
    for r in rows:
        dt = str(r["dt"])
        by_date.setdefault(dt, []).append(r)
        rtn = r["requisition_type_name"] or "Unknown"
        by_type[rtn] = by_type.get(rtn, 0) + 1
        tn = r["tehsil_name"] or "Unknown"
        by_tehsil[tn] = by_tehsil.get(tn, 0) + 1

    # Requisition Type breakdown
    context += "Requisition Type Breakdown:\n"
    for rtn, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
        context += f"  • {rtn}: {cnt}\n"

    # Location breakdown (if multiple)
    if len(by_tehsil) > 1:
        context += "\nLocation Breakdown:\n"
        for tn, cnt in sorted(by_tehsil.items(), key=lambda x: -x[1]):
            context += f"  • {tn}: {cnt}\n"

    # Daily detail
    context += "\nDaily Detail:\n"
    for dt in sorted(by_date.keys()):
        day_rows = by_date[dt]
        context += f"  {dt} ({len(day_rows)} requisitions):\n"
        for r in day_rows:
            area = r["area_location"] or "N/A"
            squad = r["total_squad_members"] or 0
            arrived = r["arrived_members"] or 0
            context += (
                f"    - {r['requisition_type_name']} | {r['tehsil_name']} | "
                f"Area: {area} | Squad: {arrived}/{squad}\n"
            )

    records = [dict(r) for r in rows]
    return {
        "source_id": f"oa_officer:{officer_name}",
        "records": records,
        "formatted_context": context,
    }


def _query_by_location(
    db, level: str, location_name: str,
    req_type: Optional[str],
    start_date: Optional[date], end_date: Optional[date],
) -> Dict[str, Any]:
    """Query for a specific location, with optional date range and req type filter."""

    if start_date or end_date:
        return _query_detail_aggregate(
            db, level=level, location_name=location_name,
            req_type=req_type, start_date=start_date, end_date=end_date,
        )

    # No date range → use summary table
    name_col = f"{level}_name"

    if level == "division":
        # Show districts within this division
        children = db.fetch_all(
            "SELECT district_name, price_control, anti_encroachment, eviction, "
            "anti_hoarding, public_nuisance, total "
            "FROM operational_activity WHERE level = 'district' AND " + _latest_summary_snapshot() + " AND division_name = %s "
            "ORDER BY total DESC",
            (location_name,)
        )
        parent = db.fetch_one(
            "SELECT price_control, anti_encroachment, eviction, anti_hoarding, "
            "public_nuisance, total FROM operational_activity "
            "WHERE level = 'division' AND " + _latest_summary_snapshot() + " AND division_name = %s",
            (location_name,)
        )
        context = f"Operational Activity — {location_name} Division\n"
        if parent:
            context += (
                f"Division Total: {parent['total']} actions "
                f"(PC={parent['price_control']}, AE={parent['anti_encroachment']}, "
                f"EV={parent['eviction']}, AH={parent['anti_hoarding']}, "
                f"PN={parent['public_nuisance']})\n\n"
            )
        if req_type:
            req_label = _REQ_DB_COL.get(req_type, req_type)
            context += f"Filtered by: {req_label}\n"

        context += "District-wise breakdown:\n"
        for r in children:
            if req_type:
                col = _safe_col(req_type)
                context += f"  • {r['district_name']}: {r[col]}\n"
            else:
                context += (
                    f"  • {r['district_name']}: Total={r['total']} "
                    f"(PC={r['price_control']}, AE={r['anti_encroachment']}, "
                    f"EV={r['eviction']}, AH={r['anti_hoarding']}, PN={r['public_nuisance']})\n"
                )

        return {
            "source_id": f"oa_division:{location_name}",
            "records": children,
            "formatted_context": context,
        }

    elif level == "district":
        # Show tehsils within this district
        children = db.fetch_all(
            "SELECT tehsil_name, price_control, anti_encroachment, eviction, "
            "anti_hoarding, public_nuisance, total "
            "FROM operational_activity WHERE level = 'tehsil' AND " + _latest_summary_snapshot() + " AND district_name = %s "
            "ORDER BY total DESC",
            (location_name,)
        )
        parent = db.fetch_one(
            "SELECT division_name, price_control, anti_encroachment, eviction, "
            "anti_hoarding, public_nuisance, total FROM operational_activity "
            "WHERE level = 'district' AND " + _latest_summary_snapshot() + " AND district_name = %s",
            (location_name,)
        )
        context = f"Operational Activity — {location_name} District"
        if parent:
            context += f" ({parent['division_name']} Division)\n"
            context += (
                f"District Total: {parent['total']} actions "
                f"(PC={parent['price_control']}, AE={parent['anti_encroachment']}, "
                f"EV={parent['eviction']}, AH={parent['anti_hoarding']}, "
                f"PN={parent['public_nuisance']})\n\n"
            )
        else:
            context += "\n\n"

        context += "Tehsil-wise breakdown:\n"
        for r in children:
            if req_type:
                col = _safe_col(req_type)
                context += f"  • {r['tehsil_name']}: {r[col]}\n"
            else:
                context += (
                    f"  • {r['tehsil_name']}: Total={r['total']} "
                    f"(PC={r['price_control']}, AE={r['anti_encroachment']}, "
                    f"EV={r['eviction']}, AH={r['anti_hoarding']}, PN={r['public_nuisance']})\n"
                )

        return {
            "source_id": f"oa_district:{location_name}",
            "records": children,
            "formatted_context": context,
        }

    elif level == "tehsil":
        # Show tehsil details with officer breakdown
        parent = db.fetch_one(
            "SELECT division_name, district_name, price_control, anti_encroachment, "
            "eviction, anti_hoarding, public_nuisance, total FROM operational_activity "
            "WHERE level = 'tehsil' AND " + _latest_summary_snapshot() + " AND tehsil_name = %s",
            (location_name,)
        )

        # Try new requisition_detail table first, then fall back to old
        use_new = _has_requisition_table(db)
        if use_new:
            officers = db.fetch_all(
                "SELECT created_by_name as assigned_to, requisition_type_name as requisition_name, "
                "COUNT(*) as cnt "
                "FROM requisition_detail WHERE tehsil_name = %s "
                "AND snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_detail WHERE tehsil_name = %s) "
                "GROUP BY created_by_name, requisition_type_name ORDER BY cnt DESC",
                (location_name, location_name)
            )
        else:
            officers = db.fetch_all(
                "SELECT assigned_to, requisition_name, COUNT(*) as cnt "
                "FROM operational_activity_detail WHERE tehsil_name = %s "
                "AND " + _latest_old_snapshot_filter() + " "
                "GROUP BY assigned_to, requisition_name ORDER BY cnt DESC",
                (location_name,)
            )

        context = f"Operational Activity — {location_name} Tehsil"
        if parent:
            context += f" ({parent['district_name']} District, {parent['division_name']} Division)\n"
            context += (
                f"Tehsil Total: {parent['total']} actions "
                f"(PC={parent['price_control']}, AE={parent['anti_encroachment']}, "
                f"EV={parent['eviction']}, AH={parent['anti_hoarding']}, "
                f"PN={parent['public_nuisance']})\n\n"
            )
        else:
            context += "\n\n"

        if officers:
            context += "Officer-wise breakdown:\n"
            officer_totals: Dict[str, Dict[str, int]] = {}
            for o in officers:
                name = o["assigned_to"] or "Unknown"
                if name not in officer_totals:
                    officer_totals[name] = {}
                officer_totals[name][o["requisition_name"]] = o["cnt"]

            for name, reqs in sorted(officer_totals.items(),
                                     key=lambda x: sum(x[1].values()), reverse=True):
                total = sum(reqs.values())
                parts = ", ".join(f"{k}={v}" for k, v in reqs.items())
                context += f"  • {name}: {total} ({parts})\n"

        return {
            "source_id": f"oa_tehsil:{location_name}",
            "records": officers if officers else [],
            "formatted_context": context,
        }

    return {"source_id": "oa_summary", "records": [], "formatted_context": ""}


def _latest_snapshot_filter(level: str = None, location_name: str = None) -> Tuple[str, list]:
    """Return SQL condition + params to restrict to the latest snapshot date for
    the given location.  Different tehsils may be ingested on different days, so
    we pick the max snapshot *per location* when a location filter is provided.
    Returns (sql_fragment, param_list).
    """
    if level and location_name:
        col = f"{level}_name"
        return (
            f"snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_detail WHERE {col} = %s)",
            [location_name],
        )
    return (
        "snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_detail)",
        [],
    )


def _latest_old_snapshot_filter() -> str:
    """Return SQL condition for old operational_activity_detail (legacy)."""
    return "snapshot_date = (SELECT MAX(snapshot_date) FROM operational_activity_detail)"


def _latest_summary_snapshot() -> str:
    """Return SQL condition to restrict to only the latest snapshot (summary table)."""
    return "snapshot_date = (SELECT MAX(snapshot_date) FROM operational_activity)"


def _has_requisition_table(db) -> bool:
    """Check if requisition_detail table exists and has data."""
    try:
        row = db.fetch_one("SELECT COUNT(*) as cnt FROM requisition_detail LIMIT 1")
        return row and row["cnt"] > 0
    except Exception:
        return False


def _query_detail_aggregate(
    db, level: Optional[str], location_name: Optional[str],
    req_type: Optional[str],
    start_date: Optional[date], end_date: Optional[date],
) -> Dict[str, Any]:
    """
    Query requisition_detail (real SDEO data) with date range filters.
    Falls back to old operational_activity_detail if requisition_detail is empty.
    Aggregates counts by location and requisition type.
    """
    # Determine which table to use
    use_new = _has_requisition_table(db)
    if use_new:
        table = "requisition_detail"
        date_col = "created_at"
        req_col = "requisition_type_name"
        officer_col = "created_by_name"
        snap_sql, snap_params = _latest_snapshot_filter(level, location_name)
    else:
        table = "operational_activity_detail"
        date_col = "action_date"
        req_col = "requisition_name"
        officer_col = "assigned_to"
        snap_sql = _latest_old_snapshot_filter()
        snap_params = []

    conditions = [snap_sql]
    params = list(snap_params)

    if start_date:
        conditions.append(f"{date_col} >= %s")
        params.append(start_date.isoformat())
    if end_date:
        conditions.append(f"{date_col} <= %s")
        params.append(f"{end_date.isoformat()}T23:59:59")

    if req_type:
        req_name = _REQ_DB_COL.get(req_type)
        if req_name:
            # New table uses challan-style names ("Anti Encroachment" not "Anti-Encroachment")
            if use_new:
                # Match both naming conventions
                conditions.append(f"({req_col} = %s OR {req_col} = %s)")
                params.append(req_name)
                params.append(_OA_TO_CHALLAN_REQ.get(req_name, req_name))
            else:
                conditions.append(f"{req_col} = %s")
                params.append(req_name)

    # Location filter
    if level == "division" and location_name:
        conditions.append("division_name = %s")
        params.append(location_name)
    elif level == "district" and location_name:
        conditions.append("district_name = %s")
        params.append(location_name)
    elif level == "tehsil" and location_name:
        conditions.append("tehsil_name = %s")
        params.append(location_name)

    where = " AND ".join(conditions) if conditions else "1=1"

    # Date range label
    date_label = ""
    if start_date and end_date:
        date_label = f" from {start_date.isoformat()} to {end_date.isoformat()}"
    elif start_date:
        date_label = f" from {start_date.isoformat()}"
    elif end_date:
        date_label = f" until {end_date.isoformat()}"

    req_label = ""
    if req_type:
        req_label = f" — {_REQ_DB_COL.get(req_type, req_type)}"

    # Total count
    total_row = db.fetch_one(
        f"SELECT COUNT(*) as total FROM {table} WHERE {where}",
        tuple(params)
    )
    grand_total = total_row["total"] if total_row else 0

    # Determine grouping for breakdown
    if level == "division" and location_name:
        group_col = "district_name"
        header = f"Operational Activity — {location_name} Division{req_label}{date_label}"
    elif level == "district" and location_name:
        group_col = "tehsil_name"
        header = f"Operational Activity — {location_name} District{req_label}{date_label}"
    elif level == "tehsil" and location_name:
        group_col = officer_col
        header = f"Operational Activity — {location_name} Tehsil{req_label}{date_label}"
    else:
        group_col = "division_name"
        header = f"Operational Activity Summary{req_label}{date_label}"

    # Grouped breakdown
    rows = db.fetch_all(
        f"SELECT {group_col}, {req_col} as requisition_name, COUNT(*) as cnt "
        f"FROM {table} WHERE {where} "
        f"GROUP BY {group_col}, {req_col} "
        f"ORDER BY {group_col}, cnt DESC",
        tuple(params)
    )

    grouped: Dict[str, Dict[str, int]] = {}
    for r in rows:
        name = r[group_col] or "Unknown"
        if name not in grouped:
            grouped[name] = {}
        grouped[name][r["requisition_name"]] = r["cnt"]

    group_totals = db.fetch_all(
        f"SELECT {group_col}, COUNT(*) as total "
        f"FROM {table} WHERE {where} "
        f"GROUP BY {group_col} ORDER BY total DESC",
        tuple(params)
    )

    # Build context
    context = f"{header}\n"
    context += f"Total Requisitions: {grand_total}\n\n"

    if req_type:
        context += f"Breakdown by {group_col.replace('_', ' ').title().replace(' Name', '')}:\n"
        for gt in group_totals:
            context += f"  • {gt[group_col] or 'Unknown'}: {gt['total']}\n"
    else:
        label = group_col.replace("_name", "").replace("_", " ").title()
        context += f"Breakdown by {label}:\n"
        for gt in group_totals:
            name = gt[group_col] or "Unknown"
            reqs = grouped.get(name, {})
            parts = ", ".join(f"{k}={v}" for k, v in sorted(reqs.items()))
            context += f"  • {name}: Total={gt['total']} ({parts})\n"

    # Per-requisition-type totals
    req_totals = db.fetch_all(
        f"SELECT {req_col} as requisition_name, COUNT(*) as cnt "
        f"FROM {table} WHERE {where} "
        f"GROUP BY {req_col} ORDER BY cnt DESC",
        tuple(params)
    )
    if req_totals and not req_type:
        context += "\nRequisition Type Totals:\n"
        for rt in req_totals:
            context += f"  • {rt['requisition_name']}: {rt['cnt']}\n"

    source_id = "oa_summary"
    if level and location_name:
        source_id = f"oa_{level}:{location_name}"

    return {
        "source_id": source_id,
        "records": group_totals,
        "formatted_context": context,
    }


# ── Follow-up detection ────────────────────────────────────────

# Pronouns / references that indicate a follow-up
_OA_FOLLOWUP_REF_RE = re.compile(
    r"\b(?:ye+h?|wo+h?|y[eé]|is\s*(?:ka|ki|ke|k[eay]\s*ba[a]?d|mein|me|mai)|"
    r"us\s*(?:ka|ki|ke|mein|me|mai)|in\s*(?:ka|ki|ke|mein|me|mai)|"
    r"those|these|that|this|them|it|its|unka|inka|uski|iski|unke|inke)\b",
    re.I,
)

# Department / requisition type follow-up
_DEPT_FOLLOWUP_RE = re.compile(
    r"\b(?:department[s]?|dept[s]?|mehakma|mehakme|type[s]?|qism|category|categories|"
    r"kis\s+(?:qism|type|department)|konse\s+department|kon\s*(?:sa|se|si)\s+department)\b",
    re.I,
)

# Amount follow-up
_OA_AMOUNT_FOLLOWUP_RE = re.compile(
    r"\b(?:amount|amout|raqam|paisa|kitna|kitni|kitne|total|sum|amount\s+of\s+fine)\b",
    re.I,
)

# Detail follow-up
_OA_DETAIL_FOLLOWUP_RE = re.compile(
    r"\b(?:detail[s]?|tafsilat|tafseelat|breakdown|specifics?|elaborate|explain|batao)\b",
    re.I,
)

# Ranking follow-up (who did most/least, next, etc.)
_OA_RANKING_FOLLOWUP_RE = re.compile(
    r"\b(?:is\s*k[eay]?\s*ba[a]?d|usk[eay]?\s*ba[a]?d|next|second|third|"
    r"sab\s*se\s*(?:ziada|zyada|ziyada|kam)|"
    r"kis\s*ne\s*(?:ziada|zyada|ziyada|kam)|"
    r"kon\s*(?:sa|se|si)?.*(?:ziada|zyada|ziyada|kam))\b",
    re.I,
)

# Requisition-type name mapping for challan cross-reference
# OA uses slightly different names than challan system
_OA_TO_CHALLAN_REQ = {
    "Price Control": "Price Control",
    "Anti-Encroachment": "Anti Encroachment",
    "Eviction": "Land Eviction",
    "Anti-Hoarding": "Anti Hoarding",
    "Public Nuisance": "Public Nuisance",
}


def detect_oa_followup(question: str, prev_intent: str) -> Optional[str]:
    """
    Detect if *question* is a follow-up to a previous OA query.

    Handles:
      - Detail / department / breakdown follow-ups → reuse prev OA intent
      - Cross-domain: "challans against these requisitions" → oa_cross_challan:...
      - Ranking / amount follow-ups → reuse prev OA intent

    Returns a source_id string or None.
    """
    if not prev_intent or not prev_intent.startswith("oa_"):
        return None
    q = (question or "").strip()
    if not q:
        return None

    has_ref = _OA_FOLLOWUP_REF_RE.search(q)
    is_short = len(q.split()) <= 12

    if not has_ref and not is_short:
        return None

    # Guard: if user specifies a NEW location that differs from the previous
    # context, this is a fresh query — NOT a follow-up.
    # e.g. prev=oa_tehsil:Shalimar, current="challans in Lahore" → fresh query
    cur_loc = _detect_location(q)
    if cur_loc:
        cur_name = cur_loc.get(f"{cur_loc['level']}_name", "").lower()
        # Extract location name from prev_intent (format: oa_level:Name)
        prev_parts = prev_intent.split(":")
        prev_name = prev_parts[-1].lower() if len(prev_parts) > 1 else ""
        if cur_name and prev_name and cur_name != prev_name:
            return None  # different location → fresh query

    wants_challan = _CHALLAN_CROSS_RE.search(q)
    wants_dept = _DEPT_FOLLOWUP_RE.search(q)
    wants_amount = _OA_AMOUNT_FOLLOWUP_RE.search(q)
    wants_detail = _OA_DETAIL_FOLLOWUP_RE.search(q)
    wants_ranking = _OA_RANKING_FOLLOWUP_RE.search(q)

    # Cross-domain: user wants challan data for the same OA context
    if wants_challan:
        return f"oa_cross_challan:{prev_intent}"

    # OA-only follow-ups: reuse previous intent
    if wants_dept or wants_detail or wants_ranking or wants_amount:
        return prev_intent

    return None


def _execute_cross_challan_lookup(
    oa_source_id: str,
    question: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Cross-domain query: fetch challan counts for the same officers and
    requisition types found in requisition data, across the same date range.

    With new SDEO data (requisition_detail + requisition_member):
      - Squad members (requisition_member) are the officers who issue challans
      - We match member_name → challan officer_name

    Falls back to old operational_activity_detail if new tables are empty.
    """
    db = _get_db()
    if not db:
        return None

    # Parse the OA source_id to extract location or officer
    parts = oa_source_id.split(":")
    base = parts[0]

    level = None
    location_name = None
    officer_name = None
    if base in ("oa_division", "oa_division_req"):
        level = "division"
        location_name = parts[1] if len(parts) > 1 else None
    elif base in ("oa_district", "oa_district_req"):
        level = "district"
        location_name = parts[1] if len(parts) > 1 else None
    elif base in ("oa_tehsil", "oa_tehsil_req"):
        level = "tehsil"
        location_name = parts[1] if len(parts) > 1 else None
    elif base == "oa_officer":
        officer_name = ":".join(parts[1:]) if len(parts) > 1 else None

    start_date, end_date = _extract_date_range_for_oa(question)
    use_new = _has_requisition_table(db)

    # ── Step 1: Get requisition + member data ──────────────────
    if use_new:
        return _cross_challan_new(db, level, location_name, start_date, end_date,
                                  oa_source_id, officer_name=officer_name)
    else:
        return _cross_challan_legacy(db, level, location_name, start_date, end_date, oa_source_id)


def _cross_challan_new(
    db, level, location_name, start_date, end_date, oa_source_id,
    officer_name: str = None,
) -> Dict[str, Any]:
    """
    Cross-reference using new requisition_detail + requisition_member tables.

    Logic:
      1. Get all requisitions for the context (location/officer + date range)
      2. Get ALL challans in the SAME location + date range by requisition type
         (matching how PERA360 dashboard counts — all officers, not filtered)
      3. Also get squad member info for enrichment
      4. Build detailed context
    """
    if officer_name:
        # Officer-based: use per-officer snapshot
        snap_sql = "snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_detail WHERE created_by_name = %s)"
        snap_params = [officer_name]
    else:
        snap_sql, snap_params = _latest_snapshot_filter(level, location_name)
    conditions = [snap_sql]
    params: list = list(snap_params)

    if start_date:
        conditions.append("created_at >= %s")
        params.append(start_date.isoformat())
    if end_date:
        conditions.append("created_at <= %s")
        params.append(f"{end_date.isoformat()}T23:59:59")
    if officer_name:
        conditions.append("created_by_name = %s")
        params.append(officer_name)
    elif level and location_name:
        conditions.append(f"{level}_name = %s")
        params.append(location_name)

    where = " AND ".join(conditions)

    # Step 1: Get requisitions grouped by type + creator
    req_rows = db.fetch_all(
        f"SELECT requisition_id, requisition_type_name, created_at::date as dt, "
        f"       created_by_name, area_location, total_squad_members, arrived_members "
        f"FROM requisition_detail WHERE {where} "
        f"ORDER BY created_at",
        tuple(params),
    )

    if not req_rows:
        return {
            "source_id": f"oa_cross_challan:{oa_source_id}",
            "records": [],
            "formatted_context": "No requisition records found for this context.\n",
        }

    # Build: req_type → {count, creators, areas}
    req_by_type: Dict[str, Dict[str, Any]] = {}
    for r in req_rows:
        rtn = r["requisition_type_name"] or "Unknown"
        if rtn not in req_by_type:
            req_by_type[rtn] = {
                "count": 0, "creators": set(), "areas": set(),
                "total_squad": 0, "total_arrived": 0,
            }
        info = req_by_type[rtn]
        info["count"] += 1
        if r["created_by_name"]:
            info["creators"].add(r["created_by_name"])
        if r["area_location"]:
            info["areas"].add(r["area_location"].strip())
        info["total_squad"] += r["total_squad_members"] or 0
        info["total_arrived"] += r["arrived_members"] or 0

    # Step 2: Get ALL challans in the same location + date range
    # (not filtered by officer — matches PERA360 dashboard behavior)
    cte_conditions = ["action_date IS NOT NULL"]
    cte_params: list = []

    if start_date:
        cte_conditions.append("action_date >= %s")
        cte_params.append(start_date.isoformat())
    if end_date:
        cte_conditions.append("action_date < (%s::date + INTERVAL '1 day')")
        cte_params.append(end_date.isoformat())
    if level and location_name:
        cte_conditions.append(f"{level}_name = %s")
        cte_params.append(location_name)

    challan_cte = (
        "WITH latest_challan AS ( "
        "  SELECT DISTINCT ON (challan_id) * "
        "  FROM challan_data "
        "  WHERE " + " AND ".join(cte_conditions) + " "
        "  ORDER BY challan_id, snapshot_date DESC "
        ") "
    )

    # Get challans grouped by requisition type + status
    challan_rows = db.fetch_all(
        challan_cte +
        "SELECT requisition_type_name, status, "
        "       COUNT(*) as cnt, SUM(fine_amount) as total_fine, "
        "       SUM(paid_amount) as total_paid, "
        "       SUM(outstanding_amount) as total_outstanding "
        "FROM latest_challan "
        "GROUP BY requisition_type_name, status "
        "ORDER BY requisition_type_name, status",
        tuple(cte_params),
    )

    # Also get by officer for enrichment
    challan_by_officer = db.fetch_all(
        challan_cte +
        "SELECT requisition_type_name, officer_name, "
        "       COUNT(*) as cnt, SUM(fine_amount) as total_fine "
        "FROM latest_challan "
        "GROUP BY requisition_type_name, officer_name "
        "ORDER BY requisition_type_name, cnt DESC",
        tuple(cte_params),
    )

    # Pivot challan data: challan_req_name → {total, fine, by_status, by_officer}
    challan_by_type: Dict[str, Dict[str, Any]] = {}
    for cr in challan_rows:
        rtn = cr["requisition_type_name"] or "Unknown"
        if rtn not in challan_by_type:
            challan_by_type[rtn] = {
                "total": 0, "total_fine": 0, "total_paid": 0,
                "total_outstanding": 0, "by_status": {}, "by_officer": {},
            }
        entry = challan_by_type[rtn]
        entry["total"] += cr["cnt"]
        entry["total_fine"] += float(cr["total_fine"] or 0)
        entry["total_paid"] += float(cr["total_paid"] or 0)
        entry["total_outstanding"] += float(cr["total_outstanding"] or 0)
        entry["by_status"][cr["status"]] = entry["by_status"].get(cr["status"], 0) + cr["cnt"]

    for co in challan_by_officer:
        rtn = co["requisition_type_name"] or "Unknown"
        if rtn in challan_by_type:
            off = co["officer_name"] or "Unknown"
            challan_by_type[rtn]["by_officer"][off] = co["cnt"]

    # Step 3: Build context
    date_label = ""
    if start_date and end_date:
        date_label = f" ({start_date.isoformat()} to {end_date.isoformat()})"
    loc_label = f" — {location_name} {(level or '').title()}" if location_name else ""

    total_reqs = sum(info["count"] for info in req_by_type.values())
    grand_challans = sum(v["total"] for v in challan_by_type.values())
    grand_fine = sum(v["total_fine"] for v in challan_by_type.values())

    context = f"Requisitions & Challans Cross-Reference{loc_label}{date_label}\n"
    context += f"Total Requisitions: {total_reqs}\n"
    context += f"Total Challans: {grand_challans}"
    if grand_fine > 0:
        context += f" (Total Fine: Rs. {grand_fine:,.0f})"
    context += "\n\n"

    context += "Department/Requisition Type Breakdown:\n"
    for rtn, rinfo in sorted(req_by_type.items(), key=lambda x: x[1]["count"], reverse=True):
        # Map requisition type name to challan type name for lookup
        challan_rtn = _OA_TO_CHALLAN_REQ.get(rtn, rtn)
        ch = challan_by_type.get(challan_rtn, challan_by_type.get(rtn, {}))

        context += f"\n  {rtn}:\n"
        context += f"    Requisitions: {rinfo['count']}\n"
        if rinfo["creators"]:
            context += f"    Created by: {', '.join(sorted(rinfo['creators']))}\n"
        if rinfo["areas"]:
            areas_list = sorted(rinfo["areas"])
            context += f"    Areas: {', '.join(areas_list[:5])}"
            if len(areas_list) > 5:
                context += f" (+{len(areas_list)-5} more)"
            context += "\n"

        ch_total = ch.get("total", 0)
        context += f"    Challans: {ch_total}"
        if ch.get("total_fine", 0) > 0:
            context += f" (Fine: Rs. {ch['total_fine']:,.0f})"
        context += "\n"

        if ch.get("by_status"):
            status_parts = ", ".join(f"{s}: {c}" for s, c in sorted(ch["by_status"].items()))
            context += f"    Challan Status: {status_parts}\n"

        if ch.get("by_officer"):
            context += "    Challans by officer:\n"
            for off, cnt in sorted(ch["by_officer"].items(), key=lambda x: x[1], reverse=True):
                context += f"      {off}: {cnt}\n"

    return {
        "source_id": f"oa_cross_challan:{oa_source_id}",
        "records": req_rows,
        "formatted_context": context,
    }


def _cross_challan_legacy(
    db, level, location_name, start_date, end_date, oa_source_id,
) -> Dict[str, Any]:
    """Cross-reference using old operational_activity_detail (fallback)."""
    snap = _latest_old_snapshot_filter()
    conditions = [snap]
    params: list = []

    if start_date:
        conditions.append("action_date >= %s")
        params.append(start_date.isoformat())
    if end_date:
        conditions.append("action_date <= %s")
        params.append(f"{end_date.isoformat()}T23:59:59")
    if level and location_name:
        conditions.append(f"{level}_name = %s")
        params.append(location_name)

    oa_where = " AND ".join(conditions)

    oa_by_officer = db.fetch_all(
        f"SELECT assigned_to, requisition_name, COUNT(*) as req_count "
        f"FROM operational_activity_detail WHERE {oa_where} "
        f"GROUP BY assigned_to, requisition_name ORDER BY req_count DESC",
        tuple(params),
    )

    if not oa_by_officer:
        return {
            "source_id": f"oa_cross_challan:{oa_source_id}",
            "records": [],
            "formatted_context": "No requisition records found for this context.\n",
        }

    oa_officers = set(r["assigned_to"] for r in oa_by_officer)

    # Get challans for same officers
    cte_conditions = ["action_date IS NOT NULL"]
    cte_params: list = []
    if start_date:
        cte_conditions.append("action_date >= %s")
        cte_params.append(start_date.isoformat())
    if end_date:
        cte_conditions.append("action_date < (%s::date + INTERVAL '1 day')")
        cte_params.append(end_date.isoformat())
    if level and location_name:
        cte_conditions.append(f"{level}_name = %s")
        cte_params.append(location_name)

    officer_list = list(oa_officers)
    if officer_list:
        placeholders = ", ".join(["%s"] * len(officer_list))
        cte_conditions.append(f"officer_name IN ({placeholders})")
        cte_params.extend(officer_list)

    challan_cte = (
        "WITH latest_challan AS ( "
        "  SELECT DISTINCT ON (challan_id) * FROM challan_data "
        "  WHERE " + " AND ".join(cte_conditions) + " "
        "  ORDER BY challan_id, snapshot_date DESC ) "
    )

    challan_rows = db.fetch_all(
        challan_cte +
        "SELECT officer_name, requisition_type_name, status, "
        "       COUNT(*) as cnt, SUM(fine_amount) as total_fine "
        "FROM latest_challan "
        "GROUP BY officer_name, requisition_type_name, status "
        "ORDER BY officer_name, requisition_type_name",
        tuple(cte_params),
    )

    challan_by_req: Dict[str, Dict[str, Any]] = {}
    for cr in challan_rows:
        rtn = cr["requisition_type_name"] or "Unknown"
        if rtn not in challan_by_req:
            challan_by_req[rtn] = {"total": 0, "total_fine": 0, "by_status": {}}
        challan_by_req[rtn]["total"] += cr["cnt"]
        challan_by_req[rtn]["total_fine"] += float(cr["total_fine"] or 0)
        challan_by_req[rtn]["by_status"][cr["status"]] = \
            challan_by_req[rtn]["by_status"].get(cr["status"], 0) + cr["cnt"]

    date_label = f" ({start_date} to {end_date})" if start_date and end_date else ""
    loc_label = f" — {location_name} {(level or '').title()}" if location_name else ""
    total_reqs = sum(r["req_count"] for r in oa_by_officer)

    context = f"Requisitions & Challans Cross-Reference{loc_label}{date_label}\n"
    context += f"Total Requisitions: {total_reqs}\n\n"

    for oa in oa_by_officer:
        req_name = oa["requisition_name"]
        challan_rtn = _OA_TO_CHALLAN_REQ.get(req_name, req_name)
        ch = challan_by_req.get(challan_rtn, {"total": 0, "total_fine": 0, "by_status": {}})

        context += f"  {req_name}: {oa['req_count']} requisitions → "
        context += f"{ch['total']} challans"
        if ch["total_fine"] > 0:
            context += f" (Rs. {ch['total_fine']:,.0f})"
        context += "\n"

    return {
        "source_id": f"oa_cross_challan:{oa_source_id}",
        "records": oa_by_officer,
        "formatted_context": context,
    }
