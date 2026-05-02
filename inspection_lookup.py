"""
inspection_lookup.py
====================
Detect and execute inspection-performance queries against stored PostgreSQL
data (officer_inspection_record, officer_inspection_detail, inspection_performance,
inspection_officer_summary) and live PCM API calls for date-specific queries.

Intent source_id patterns
-------------------------
  insp_summary                   – overall inspection summary (all divisions)
  insp_division:<name>           – division-level summary
  insp_district:<name>           – district-level summary
  insp_tehsil:<name>             – tehsil-level summary
  insp_officer:<name>            – officer inspection data (stored or live)
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from log_config import get_logger

log = get_logger(__name__)

# ══════════════════════════════════════════════════════════════
# DB + API helpers
# ══════════════════════════════════════════════════════════════
_HEADERS = {"accept": "application/json"}
_API_TIMEOUT = 30

PCM_OFFICER_INSPECTIONS = (
    "https://pera360.punjab.gov.pk/backend/api/Pcm/officer-inspections"
)
SDEO_INSPECTIONS_SUMMARY = (
    "https://pera360.punjab.gov.pk/backend/api/sdeo-dashboard/inspections-summary"
)


def _get_db():
    try:
        from analytics_db import get_analytics_db
        return get_analytics_db()
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
# Keyword / intent detection
# ══════════════════════════════════════════════════════════════
_INSP_KEYWORDS = re.compile(
    r"\b("
    r"inspect(?:ion)?s?|"
    r"insp(?:ection)?s?|"
    r"regulatory\s+inspect(?:ion)?s?|"
    r"field\s+inspect(?:ion)?s?|"
    r"kitni\s+inspect(?:ion)?s?|"
    r"kitny\s+inspect(?:ion)?s?|"
    r"how\s+many\s+inspect(?:ion)?s?|"
    r"total\s+inspect(?:ion)?s?|"
    r"inspection\s+performance|"
    r"inspection\s+report|"
    # Inspection outcome keywords (FIR, sealed, arrest, etc.)
    r"firs?|"
    r"sealed|sealing|"
    r"arrest(?:ed|s)?|arrest\s+case|"
    r"warning(?:s)?|"
    r"no\s+offen[cs]e|"
    r"epo|removal\s+order|"
    r"confiscat(?:ed|ion)|"
    r"total\s+actions|"
    # Urdu / Roman-Urdu
    r"muayina|mu[aā]yin[ae]|"
    r"jaiz[ae]|"
    r"checking|checkings"
    r")\b",
    re.I,
)

# ── Location patterns (shared with OA but we replicate to stay independent) ──
_DIVISION_NAMES: Optional[List[str]] = None
_DISTRICT_NAMES: Optional[List[str]] = None
_TEHSIL_NAMES: Optional[List[str]] = None


def _load_location_cache():
    """Load location names from inspection_performance for location detection."""
    global _DIVISION_NAMES, _DISTRICT_NAMES, _TEHSIL_NAMES
    if _DIVISION_NAMES is not None:
        return
    db = _get_db()
    if not db:
        _DIVISION_NAMES, _DISTRICT_NAMES, _TEHSIL_NAMES = [], [], []
        return
    try:
        rows = db.fetch_all(
            "SELECT DISTINCT division_name FROM inspection_performance "
            "WHERE level='division' AND division_name IS NOT NULL"
        )
        _DIVISION_NAMES = [r["division_name"] for r in rows]
    except Exception:
        _DIVISION_NAMES = []
    try:
        rows = db.fetch_all(
            "SELECT DISTINCT district_name FROM inspection_performance "
            "WHERE level='district' AND district_name IS NOT NULL"
        )
        _DISTRICT_NAMES = [r["district_name"] for r in rows]
    except Exception:
        _DISTRICT_NAMES = []
    try:
        rows = db.fetch_all(
            "SELECT DISTINCT tehsil_name FROM inspection_performance "
            "WHERE level='tehsil' AND tehsil_name IS NOT NULL"
        )
        _TEHSIL_NAMES = [r["tehsil_name"] for r in rows]
    except Exception:
        _TEHSIL_NAMES = []


def _detect_location(question: str) -> Optional[Dict[str, str]]:
    """Return {'level': ..., '<level>_name': ...} or None."""
    _load_location_cache()
    q_lower = question.lower()

    # If user explicitly says "division" / "district" / "tehsil", respect that
    explicit_div = re.search(r"\bdivision\b", q_lower)
    explicit_dist = re.search(r"\bdistrict\b", q_lower)
    explicit_teh = re.search(r"\btehsil\b", q_lower)

    # Explicit level requested — check that level first
    if explicit_teh:
        for t in (_TEHSIL_NAMES or []):
            if t.lower() in q_lower:
                return {"level": "tehsil", "tehsil_name": t}
    if explicit_div:
        for dv in (_DIVISION_NAMES or []):
            if dv.lower() in q_lower:
                return {"level": "division", "division_name": dv}
    if explicit_dist:
        for d in (_DISTRICT_NAMES or []):
            if d.lower() in q_lower:
                return {"level": "district", "district_name": d}

    # No explicit level — default: tehsil > district > division
    for t in (_TEHSIL_NAMES or []):
        if t.lower() in q_lower:
            return {"level": "tehsil", "tehsil_name": t}
    for d in (_DISTRICT_NAMES or []):
        if d.lower() in q_lower:
            return {"level": "district", "district_name": d}
    for dv in (_DIVISION_NAMES or []):
        if dv.lower() in q_lower:
            return {"level": "division", "division_name": dv}
    return None


# ── Officer name cache (from officer_inspection_record + officer_inspection_detail) ──
_insp_officer_cache: Optional[List[str]] = None
_insp_officer_cache_ts: float = 0
_OFFICER_CACHE_TTL_S = 30 * 60  # refresh every 30 minutes

# Stop-words excluded from name-candidate extraction (lowercase)
_NAME_STOP_WORDS = frozenset({
    "what", "about", "the", "same", "dates", "date", "inspections", "inspection",
    "summary", "summery", "from", "tell", "show", "detailed", "detail", "details",
    "total", "how", "many", "give", "officer", "officers", "please", "also",
    "april", "may", "june", "july", "august", "september", "october", "november",
    "december", "january", "february", "march", "with", "and", "for", "this",
    "that", "these", "those", "report", "data", "performance", "field",
    "regulatory", "checking", "checkings", "division", "district", "tehsil",
    "challan", "challans", "batao", "dikhao", "kitni", "kitny", "when",
    "where", "which", "who", "whom", "whose", "more", "less", "most", "least",
    "first", "last", "next", "previous", "insp", "muayina", "jaiza",
    "can", "you", "have", "has", "had", "will", "would", "could", "should",
    "between", "during", "their", "them", "they", "been", "being", "was",
    "were", "are", "not", "but", "only", "just", "like", "all",
})


def _load_insp_officer_cache() -> List[str]:
    global _insp_officer_cache, _insp_officer_cache_ts
    if (
        _insp_officer_cache is not None
        and (time.time() - _insp_officer_cache_ts) < _OFFICER_CACHE_TTL_S
    ):
        return _insp_officer_cache
    db = _get_db()
    if not db:
        _insp_officer_cache = []
        _insp_officer_cache_ts = time.time()
        return _insp_officer_cache
    try:
        rows = db.fetch_all(
            "SELECT DISTINCT officer_name FROM officer_inspection_detail "
            "WHERE officer_name IS NOT NULL AND officer_name != '' "
            "UNION "
            "SELECT DISTINCT officer_name FROM officer_inspection_record "
            "WHERE officer_name IS NOT NULL AND officer_name != '' "
            "UNION "
            "SELECT DISTINCT officer_name FROM inspection_officer_summary "
            "WHERE officer_name IS NOT NULL AND officer_name != '' "
            "UNION "
            "SELECT DISTINCT created_by_name AS officer_name FROM requisition_detail "
            "WHERE created_by_name IS NOT NULL AND created_by_name != ''"
        )
        _insp_officer_cache = [r["officer_name"] for r in rows]
    except Exception as e:
        log.warning("Failed to load inspection officer cache: %s", e)
        _insp_officer_cache = []
    _insp_officer_cache_ts = time.time()
    return _insp_officer_cache


def _live_officer_search(question: str) -> Optional[str]:
    """Fallback: search officer tables with SQL LIKE when cache match fails.

    Extracts potential name tokens from *question* (filtering out common
    English / Urdu / domain stop-words) and tries them pairwise against the
    database.  On match, the officer is hot-added to the in-memory cache so
    subsequent calls are instant.
    """
    db = _get_db()
    if not db:
        return None

    words = re.findall(r"[a-zA-Z]{3,}", question.lower())
    name_candidates = [w for w in words if w not in _NAME_STOP_WORDS]

    if len(name_candidates) < 2:
        return None

    # Try consecutive candidate pairs as potential first-name / last-name
    for i in range(len(name_candidates) - 1):
        w1, w2 = name_candidates[i], name_candidates[i + 1]
        try:
            row = db.fetch_one(
                "SELECT officer_name FROM ("
                "  SELECT DISTINCT officer_name FROM officer_inspection_detail "
                "  WHERE officer_name IS NOT NULL AND officer_name != '' "
                "  UNION "
                "  SELECT DISTINCT officer_name FROM officer_inspection_record "
                "  WHERE officer_name IS NOT NULL AND officer_name != '' "
                "  UNION "
                "  SELECT DISTINCT officer_name FROM inspection_officer_summary "
                "  WHERE officer_name IS NOT NULL AND officer_name != '' "
                "  UNION "
                "  SELECT DISTINCT created_by_name AS officer_name FROM requisition_detail "
                "  WHERE created_by_name IS NOT NULL AND created_by_name != ''"
                ") sub "
                "WHERE LOWER(officer_name) LIKE %s AND LOWER(officer_name) LIKE %s "
                "LIMIT 1",
                (f"%{w1}%", f"%{w2}%"),
            )
            if row and row.get("officer_name"):
                officer = row["officer_name"]
                # Hot-add to in-memory cache for future requests
                if _insp_officer_cache is not None and officer not in _insp_officer_cache:
                    _insp_officer_cache.append(officer)
                log.info("Live officer search found '%s' for tokens [%s, %s]", officer, w1, w2)
                return officer
        except Exception as exc:
            log.debug("Live officer search error for [%s, %s]: %s", w1, w2, exc)

    return None


def _extract_likely_person_name(question: str) -> Optional[str]:
    """Last-resort heuristic: pull consecutive Title-Case words that are not
    domain keywords.  Returns the candidate string (not DB-verified) or None.
    """
    _KW_UPPER = {
        "What", "About", "The", "Same", "Dates", "Inspections", "Inspection",
        "Summary", "Summery", "From", "Tell", "Show", "Detailed", "Detail",
        "Total", "How", "Many", "Give", "Officer", "Please", "Report",
        "April", "May", "June", "July", "August", "September", "October",
        "November", "December", "January", "February", "March",
        "Division", "District", "Tehsil", "Challan", "Challans", "Field",
        "Regulatory", "Performance", "Data", "Between", "During",
    }
    parts: List[str] = []
    for w in question.split():
        clean = re.sub(r"[^a-zA-Z]", "", w)
        if clean and clean[0].isupper() and clean not in _KW_UPPER and len(clean) >= 3:
            parts.append(clean)
        else:
            if len(parts) >= 2:
                return " ".join(parts)
            parts = []
    if len(parts) >= 2:
        return " ".join(parts)
    return None


def _detect_insp_officer_name(question: str) -> Optional[str]:
    """Fuzzy-match officer name from the inspection tables.

    Two-phase strategy (only returns DB-verified officers):
      1. Fast in-memory cache scan (substring match on name parts).
      2. Live DB LIKE fallback — catches officers not yet in cache or whose
         names are stored with different casing/spacing.

    Returns None if the officer is not found in any inspection table,
    which lets the caller fall through to a broader intent (e.g. insp_summary).
    """
    q_lower = question.lower()
    officers = _load_insp_officer_cache()

    # Phase 1: cache-based fuzzy match (fast, zero-cost)
    best_match = None
    best_score = 0
    for officer in officers:
        name_parts = officer.lower().split()
        if not name_parts:
            continue
        matched = sum(1 for p in name_parts if p in q_lower)
        min_required = min(2, len(name_parts))
        if matched >= min_required and matched > best_score:
            best_score = matched
            best_match = officer

    if best_match:
        return best_match

    # Phase 2: live DB search (one SQL query per candidate pair)
    return _live_officer_search(question)


# ══════════════════════════════════════════════════════════════
# Date extraction (reuse challan_lookup's robust implementation)
# ══════════════════════════════════════════════════════════════
def _extract_date_range(question: str) -> Tuple[Optional[date], Optional[date]]:
    """Extract date range from the question, reusing challan_lookup._extract_date_range."""
    try:
        from challan_lookup import _extract_date_range as _challan_dr
        result = _challan_dr(question)
        if result:
            return result
    except ImportError:
        pass
    return (None, None)


# ══════════════════════════════════════════════════════════════
# Public intent detection
# ══════════════════════════════════════════════════════════════
def detect_inspection_intent(question: str) -> Optional[str]:
    """
    Detect if a question is about inspection performance data.

    Returns an encoded source_id string like:
      - insp_summary
      - insp_division:Lahore
      - insp_district:Faisalabad
      - insp_tehsil:Lahore Cantt
      - insp_officer:Rabia Altaf
    Or None if not an inspection query.
    """
    q = (question or "").strip()
    if not q:
        return None

    # CNIC detection FIRST (before keyword check) — "id card 3520148212675"
    # A 13-digit number is almost certainly a CNIC lookup request
    cnic_match = re.search(r"\b(\d{13})\b", q)
    if cnic_match:
        return f"insp_cnic:{cnic_match.group(1)}"

    # Detect officer name first
    officer_name = _detect_insp_officer_name(q)

    # Must have inspection keyword (for non-CNIC queries)
    if not _INSP_KEYWORDS.search(q):
        return None

    # Repeat offender / frequent CNIC detection
    # "kis id card ka name again and again aya", "repeat offender", "baar baar",
    # "sabse zyada baar", "most inspected", "frequent"
    _REPEAT_RE = re.compile(
        r"\b("
        r"again\s+and\s+again|baar\s+baar|bar\s+bar|"
        r"repeat(?:ed)?(?:ly)?|frequent(?:ly)?|"
        r"sab\s*se\s*(?:ziada|zyada|ziyada)|most\s+(?:inspect|challan)|"
        r"kitni\s+(?:dafa|baar|bar|martaba)|"
        r"(?:id\s+card|cnic).*(?:again|baar|bar|repeat|frequent)"
        r")\b",
        re.I,
    )
    if _REPEAT_RE.search(q):
        return "insp_repeat_offenders"

    # Officer + inspection keyword → officer lookup
    if officer_name:
        return f"insp_officer:{officer_name}"

    # Location detection
    location = _detect_location(q)
    if location:
        level = location["level"]
        if level == "division":
            return f"insp_division:{location['division_name']}"
        elif level == "district":
            return f"insp_district:{location['district_name']}"
        elif level == "tehsil":
            return f"insp_tehsil:{location['tehsil_name']}"

    return "insp_summary"


def detect_inspection_followup(question: str, last_lookup_type: str) -> Optional[str]:
    """
    Detect if a question is a follow-up to a previous inspection lookup
    OR a cross-domain follow-up (challan/OA → inspection).

    E.g. after asking about challans in Shalimar, user asks
    "or total inspection kitni hoi hy is durations?" → insp_tehsil:Shalimar
    """
    if not last_lookup_type:
        return None

    q_lower = (question or "").lower()

    # ── Cross-domain: challan/OA → inspection ──
    # User was asking about challans/requisitions for a location, now wants inspections
    if last_lookup_type.startswith(("challan_", "oa_")):
        if _INSP_KEYWORDS.search(q_lower):
            # Extract location from previous lookup type
            loc = _extract_location_from_intent(last_lookup_type)
            if loc:
                level, name = loc
                return f"insp_{level}:{name}"
            # If previous query had an officer
            if ":oa_officer:" in last_lookup_type or last_lookup_type.startswith("oa_officer:"):
                officer = last_lookup_type.split("oa_officer:")[-1]
                if officer:
                    return f"insp_officer:{officer}"
            return "insp_summary"
        return None

    # ── Same-domain: insp → insp follow-up ──
    if not last_lookup_type.startswith("insp_"):
        return None

    # If current question has inspection keywords, check for officer-swap
    # before treating as a totally fresh intent.
    # Scenario: prev = insp_officer:Asif Hussain → "what about the same dates
    # inspections summery of Azka Sahar" → should route to insp_officer:Azka Sahar
    if _INSP_KEYWORDS.search(q_lower):
        if last_lookup_type.startswith("insp_officer:"):
            new_officer = _detect_insp_officer_name(question)
            if new_officer:
                prev_officer = last_lookup_type.split("insp_officer:", 1)[-1]
                if new_officer.lower() != prev_officer.lower():
                    log.info("Officer-swap follow-up: '%s' → '%s'", prev_officer, new_officer)
                    return f"insp_officer:{new_officer}"
        return None  # has insp keywords but not an officer-swap → fresh intent

    # If the question is clearly about challans (not inspections), it's a NEW query
    _CHALLAN_ONLY_RE = re.compile(
        r"\b(challan(?:s|z)?|imposed|jrimana|jarimana|jurmana)\b", re.I
    )
    if _CHALLAN_ONLY_RE.search(q_lower) and not _INSP_KEYWORDS.search(q_lower):
        log.info("follow-up guard: question mentions challans, NOT an inspection follow-up")
        return None

    # If the question mentions a DIFFERENT location, it's NOT a follow-up
    # E.g. previous was insp_tehsil:Shalimar, now asking about "khairpur taimewali challans"
    _load_location_cache()
    new_location = _detect_location(question)
    if new_location:
        prev_loc = _extract_location_from_intent(last_lookup_type)
        if prev_loc:
            prev_level, prev_name = prev_loc
            new_name = new_location.get(f"{new_location['level']}_name", "")
            if new_name.lower() != prev_name.lower():
                return None  # Different location → not a follow-up

    # If question is long enough to be self-contained (>8 words with a location),
    # it's likely a new question, not a follow-up
    if new_location and len(q_lower.split()) > 6:
        return None

    # Follow-up patterns: asking for details/breakdown from previous inspection query
    followup_re = re.compile(
        r"\b(warning|fine|arrest|sealed|"
        r"detail(?:ed|s)?|breakdown|"
        r"batao|bata|dikhao|show|"
        r"kitny|kitni|how\s+many|total|count|"
        r"more|elaborat|explain|summary|"
        r"in\s+detail|in\s+detailed)\b", re.I
    )
    if followup_re.search(q_lower):
        return last_lookup_type

    return None


def _extract_location_from_intent(intent: str) -> Optional[tuple]:
    """Extract (level, name) from a challan/OA intent string.
    E.g. 'challan_location:tehsil:Shalimar' -> ('tehsil', 'Shalimar')
         'challan_daterange:...:challan_location:tehsil:Shalimar' -> ('tehsil', 'Shalimar')
         'oa_tehsil:Shalimar' -> ('tehsil', 'Shalimar')
         'oa_division:Lahore' -> ('division', 'Lahore')
    """
    import re as _re

    # OA intents: oa_tehsil:Name, oa_district:Name, oa_division:Name
    m = _re.search(r"oa_(tehsil|district|division):(.+?)(?:$|:)", intent)
    if m:
        return (m.group(1), m.group(2))

    # Challan intents: challan_location:tehsil:Name or nested in daterange
    m = _re.search(r"challan_location:(tehsil|district|division):(.+?)(?:$|:)", intent)
    if m:
        return (m.group(1), m.group(2))

    return None


# ══════════════════════════════════════════════════════════════
# Execution
# ══════════════════════════════════════════════════════════════
def execute_inspection_lookup(
    source_id: str,
    question: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Execute an inspection lookup.
    Returns dict with 'records', 'source_id', 'formatted_context'.
    """
    db = _get_db()
    if not db:
        log.warning("Analytics DB not available for inspection lookup")
        return None

    start_date, end_date = _extract_date_range(question)

    parts = source_id.split(":")
    base = parts[0]

    if base == "insp_summary":
        return _query_insp_summary(db, start_date, end_date)
    elif base == "insp_division":
        name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_location(db, "division", name, start_date, end_date)
    elif base == "insp_district":
        name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_location(db, "district", name, start_date, end_date)
    elif base == "insp_tehsil":
        name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_location(db, "tehsil", name, start_date, end_date)
    elif base == "insp_officer":
        officer_name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_officer(db, officer_name, start_date, end_date)
    elif base == "insp_cnic":
        cnic = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_cnic(db, cnic)
    elif base == "insp_repeat_offenders":
        return _query_repeat_offenders(db)

    return None


# ── Summary query (all divisions) ────────────────────────────
def _query_insp_summary(
    db, start_date: Optional[date], end_date: Optional[date]
) -> Dict[str, Any]:
    """Query inspection_performance for overall summary.
    Columns: total_actions, challans, firs, warnings, no_offenses, sealed
    """
    rows = db.fetch_all(
        "SELECT division_name, total_actions, challans, "
        "       firs, warnings, no_offenses, sealed "
        "FROM inspection_performance "
        "WHERE level = 'division' AND snapshot_date = ("
        "  SELECT MAX(snapshot_date) FROM inspection_performance WHERE level = 'division'"
        ") ORDER BY division_name"
    )

    if not rows:
        return {
            "source_id": "insp_summary",
            "records": [],
            "formatted_context": "No inspection performance data available.\n",
        }

    # Capture snapshot date for freshness footer
    _summary_snapshot = None
    try:
        _snap = db.fetch_one(
            "SELECT MAX(snapshot_date) AS d FROM inspection_performance "
            "WHERE level = 'division'"
        )
        _summary_snapshot = _snap.get("d") if _snap else None
    except Exception:
        pass

    # Compute totals
    totals = {
        "total_actions": 0, "challans": 0,
        "firs": 0, "warnings": 0, "no_offenses": 0, "sealed": 0,
    }
    for r in rows:
        for k in totals:
            totals[k] += (r.get(k) or 0)

    context = "Inspection Performance Summary (All Divisions)\n"
    context += "=" * 50 + "\n"
    context += f"Total Inspections/Actions: {totals['total_actions']:,}\n"
    context += f"Challans: {totals['challans']:,}\n"
    context += f"FIRs: {totals['firs']:,}\n"
    context += f"Warnings: {totals['warnings']:,}\n"
    context += f"No Offenses: {totals['no_offenses']:,}\n"
    context += f"Sealed: {totals['sealed']:,}\n"
    context += "\nDivision Breakdown:\n"
    for r in rows:
        context += (
            f"  {r['division_name']}: "
            f"{r.get('total_actions', 0):,} inspections, "
            f"{r.get('challans', 0):,} challans, "
            f"{r.get('warnings', 0):,} warnings, "
            f"{r.get('no_offenses', 0):,} no offenses\n"
        )

    # Phase 1 — uniform freshness stamp
    try:
        from freshness_helper import format_freshness_footer
        if _summary_snapshot is not None:
            context += "\n" + format_freshness_footer(
                "inspection_performance",
                snapshot_date=_summary_snapshot,
                sync_interval_s=2 * 60 * 60,
            ) + "\n"
    except Exception:
        pass

    return {
        "source_id": "insp_summary",
        "records": rows,
        "formatted_context": context,
    }


# ── Location query (division / district / tehsil) ───────────
def _query_insp_location(
    db, level: str, name: str,
    start_date: Optional[date], end_date: Optional[date],
) -> Dict[str, Any]:
    """Query inspection_performance for a specific location + its children.
    For tehsil-level queries with date range, calls SDEO API live.
    """
    col = f"{level}_name"
    source_id = f"insp_{level}:{name}"

    # ── Tehsil + date range → LIVE SDEO API CALL ──
    if level == "tehsil" and start_date and end_date:
        return _query_tehsil_live(db, name, start_date, end_date, source_id)

    # Get the location's own summary row
    parent_rows = db.fetch_all(
        f"SELECT * FROM inspection_performance "
        f"WHERE level = %s AND {col} = %s AND snapshot_date = ("
        f"  SELECT MAX(snapshot_date) FROM inspection_performance WHERE level = %s AND {col} = %s"
        f")",
        (level, name, level, name),
    )

    # Get children (e.g. districts under a division)
    child_level = {"division": "district", "district": "tehsil"}.get(level)
    child_rows = []
    if child_level:
        child_rows = db.fetch_all(
            f"SELECT * FROM inspection_performance "
            f"WHERE level = %s AND {col} = %s AND snapshot_date = ("
            f"  SELECT MAX(snapshot_date) FROM inspection_performance WHERE level = %s AND {col} = %s"
            f") ORDER BY {child_level}_name",
            (child_level, name, child_level, name),
        )

    # Also get officer-level data for this location.
    # IMPORTANT: officer_inspection_detail and inspection_performance are
    # populated by DIFFERENT upstream endpoints with different refresh
    # cadences. The PCM officer-detail endpoint frequently returns 404 for
    # specific tehsils, leaving the per-officer table older than the
    # summary table. We capture both snapshot dates so the answer layer
    # can warn the user when they don't match (otherwise sum-of-officers
    # won't equal Total Inspections and looks like a math error).
    officer_rows = []
    officer_snapshot = None
    parent_snapshot = parent_rows[0].get("snapshot_date") if parent_rows else None
    if level == "tehsil":
        officer_rows = db.fetch_all(
            "SELECT officer_name, total_inspections, total_challans, "
            "       fine_amount, sealed, arrest_case, snapshot_date "
            "FROM officer_inspection_detail "
            f"WHERE tehsil_name = %s AND snapshot_date = ("
            f"  SELECT MAX(snapshot_date) FROM officer_inspection_detail WHERE tehsil_name = %s"
            f") ORDER BY total_inspections DESC",
            (name, name),
        )
        if officer_rows:
            officer_snapshot = officer_rows[0].get("snapshot_date")

    if not parent_rows and not child_rows:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"No inspection data found for {level} '{name}'.\n",
        }

    context = f"Inspection Performance — {level.title()}: {name}\n"
    context += "=" * 50 + "\n"

    if parent_rows:
        r = parent_rows[0]
        context += f"Total Inspections/Actions: {r.get('total_actions', 0):,}\n"
        context += f"Challans: {r.get('challans', 0):,}\n"
        context += f"FIRs: {r.get('firs', 0):,}\n"
        context += f"Warnings: {r.get('warnings', 0):,}\n"
        context += f"No Offenses: {r.get('no_offenses', 0):,}\n"
        context += f"Sealed: {r.get('sealed', 0):,}\n"

    if child_rows:
        child_label = child_level.title() if child_level else "Sub-location"
        context += f"\n{child_label} Breakdown:\n"
        for r in child_rows:
            child_name = r.get(f"{child_level}_name", "Unknown")
            context += (
                f"  {child_name}: "
                f"{r.get('total_actions', 0):,} inspections, "
                f"{r.get('challans', 0):,} challans, "
                f"{r.get('warnings', 0):,} warnings\n"
            )

    if officer_rows:
        # Detect snapshot-date mismatch between the summary table and the
        # per-officer table. When they differ, the sum of per-officer
        # inspections will NOT equal Total Inspections — explain why up
        # front so the LLM tells the user instead of looking inconsistent.
        officer_inspect_sum = sum(int(r.get("total_inspections", 0) or 0) for r in officer_rows)
        snapshot_mismatch = (
            parent_snapshot
            and officer_snapshot
            and parent_snapshot != officer_snapshot
        )

        context += f"\nOfficer Breakdown ({len(officer_rows)} officers"
        if officer_snapshot:
            context += f", as of snapshot {officer_snapshot}"
        context += "):\n"

        if snapshot_mismatch:
            # This block is read by the LLM and surfaced to the user. It
            # MUST be in the formatted context so the answerer doesn't
            # silently compose contradictory totals.
            context += (
                f"  [DATA NOTE — SNAPSHOT MISMATCH]\n"
                f"  Summary totals above are from snapshot {parent_snapshot}.\n"
                f"  Per-officer breakdown below is from an older snapshot ({officer_snapshot}) "
                f"because the upstream PCM officer-detail endpoint did not return fresh data "
                f"for this tehsil on {parent_snapshot}.\n"
                f"  Sum of per-officer inspections = {officer_inspect_sum:,} "
                f"(differs from Total Inspections {parent_rows[0].get('total_actions', 0):,} "
                f"by the activity recorded between {officer_snapshot} and {parent_snapshot} "
                f"that has not yet been broken down per officer).\n"
                f"  When listing officers, state this snapshot date and explain the "
                f"discrepancy so the user does not read it as a math error.\n"
            )

        for r in officer_rows:
            context += (
                f"  {r['officer_name']}: "
                f"{r.get('total_inspections', 0):,} inspections, "
                f"{r.get('total_challans', 0):,} challans, "
                f"Rs. {r.get('fine_amount', 0):,} fine\n"
            )

        if snapshot_mismatch:
            context += (
                f"  [End of officer breakdown — sum {officer_inspect_sum:,} "
                f"reflects {officer_snapshot}, not {parent_snapshot}.]\n"
            )

    # Phase 1 — uniform freshness stamp the LLM can surface to the user.
    # Uses the parent (summary-table) snapshot as the headline freshness;
    # the per-officer mismatch (if any) is already explained above.
    try:
        from freshness_helper import format_freshness_footer
        if parent_snapshot is not None:
            context += "\n" + format_freshness_footer(
                "inspection_performance",
                snapshot_date=parent_snapshot,
                # Refreshed by the freshness scheduler every ~2 h
                sync_interval_s=2 * 60 * 60,
            ) + "\n"
    except Exception:
        # Footer is purely informational — never block the answer if it fails
        pass

    return {
        "source_id": source_id,
        "records": parent_rows + child_rows,
        "formatted_context": context,
    }


# ── Live SDEO API call for tehsil + date range ──────────────
def _query_tehsil_live(
    db, tehsil_name: str, start_date: date, end_date: date, source_id: str,
) -> Dict[str, Any]:
    """Call SDEO inspections-summary API live for a tehsil with date filter."""
    # Look up tehsil_id
    rows = db.fetch_all(
        "SELECT tehsil_id FROM dim_tehsil WHERE tehsil_name = %s LIMIT 1",
        (tehsil_name,),
    )
    if not rows:
        # Fallback: try inspection_performance
        rows = db.fetch_all(
            "SELECT tehsil_id FROM inspection_performance "
            "WHERE level = 'tehsil' AND tehsil_name = %s LIMIT 1",
            (tehsil_name,),
        )
    if not rows or not rows[0].get("tehsil_id"):
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"Tehsil '{tehsil_name}' not found in database.\n",
        }

    tehsil_id = rows[0]["tehsil_id"]

    # SDEO API treats endDate as exclusive — to include the full end day,
    # we add 1 day (e.g. "Jan 1 to Jan 30" → API endDate = Jan 31)
    api_end_date = end_date + timedelta(days=1)

    try:
        resp = requests.get(
            SDEO_INSPECTIONS_SUMMARY,
            params={
                "tehsilId": tehsil_id,
                "startDate": start_date.isoformat(),
                "endDate": api_end_date.isoformat(),
            },
            headers=_HEADERS,
            timeout=_API_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning("Live SDEO API call failed for tehsil %s: %s", tehsil_name, e)
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": (
                f"Unable to fetch live inspection data for {tehsil_name} "
                f"({start_date} to {end_date}). API error.\n"
            ),
        }

    if not isinstance(data, dict):
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"No inspection data returned for {tehsil_name} ({start_date} to {end_date}).\n",
        }

    total_actions = data.get("totalActions", 0) or 0
    challans = data.get("challans", 0) or 0
    firs = data.get("fiRs", 0) or 0
    warnings = data.get("warnings", 0) or 0
    no_offenses = data.get("noOffenses", 0) or 0
    sealed = data.get("sealed", 0) or 0
    removal_order = data.get("removalOrder", 0) or 0
    epo = data.get("epo", 0) or 0
    officers = data.get("officers", []) or []

    context = f"Inspection Performance — Tehsil: {tehsil_name}\n"
    context += f"Date Range: {start_date} to {end_date}\n"
    context += "=" * 50 + "\n"
    context += f"Total Inspections/Actions: {total_actions:,}\n"
    context += f"Challans: {challans:,}\n"
    context += f"FIRs: {firs:,}\n"
    context += f"Warnings: {warnings:,}\n"
    context += f"No Offenses: {no_offenses:,}\n"
    context += f"Sealed: {sealed:,}\n"
    if removal_order:
        context += f"Removal Orders: {removal_order:,}\n"
    if epo:
        context += f"EPO (Environmental Protection Orders): {epo:,}\n"

    if officers:
        context += f"\nOfficer Breakdown ({len(officers)} officers):\n"
        for o in sorted(officers, key=lambda x: -(x.get("inspection", 0) or 0)):
            parts = [
                f"{o.get('inspection', 0):,} inspections",
                f"{o.get('challan', 0):,} challans",
                f"{o.get('warning', 0):,} warnings",
            ]
            if o.get("fir", 0):
                parts.append(f"{o['fir']} FIRs")
            if o.get("sealed", 0):
                parts.append(f"{o['sealed']} sealed")
            if o.get("removalOrder", 0):
                parts.append(f"{o['removalOrder']} removal orders")
            if o.get("epo", 0):
                parts.append(f"{o['epo']} EPOs")
            context += f"  {o.get('officerName', 'Unknown')}: {', '.join(parts)}\n"

    # ── Supplementary PCM data: fine amounts, arrest, confiscated ──
    # The SDEO summary API doesn't return fine amounts or arrest counts.
    # Fetch these from stored officer_inspection_detail (latest snapshot)
    # so the chatbot can report them alongside the live SDEO totals.
    fine_amount = 0
    arrest_cases = 0
    confiscated_count = 0
    pcm_supplement_note = ""
    try:
        pcm_rows = db.fetch_all(
            "SELECT officer_name, fine_amount, arrest_case, "
            "       sealed AS pcm_sealed, snapshot_date "
            "FROM officer_inspection_detail "
            "WHERE tehsil_name = %s AND snapshot_date = ("
            "  SELECT MAX(snapshot_date) FROM officer_inspection_detail WHERE tehsil_name = %s"
            ")",
            (tehsil_name, tehsil_name),
        )
        if pcm_rows:
            fine_amount = sum(int(r.get("fine_amount", 0) or 0) for r in pcm_rows)
            arrest_cases = sum(int(r.get("arrest_case", 0) or 0) for r in pcm_rows)
            pcm_snap = pcm_rows[0].get("snapshot_date")
            pcm_supplement_note = f"  (Fine/Arrest data from PCM snapshot {pcm_snap})"
    except Exception as e:
        log.debug("PCM supplement query failed for %s: %s", tehsil_name, e)

    if fine_amount:
        context += f"Fine Amount: Rs. {fine_amount:,}\n"
    if arrest_cases:
        context += f"Arrest Cases: {arrest_cases:,}\n"

    context += f"\n(Data fetched live from SDEO API for the specified date range)\n"
    if pcm_supplement_note:
        context += pcm_supplement_note + "\n"

    # Phase 1 — live API freshness stamp
    try:
        from freshness_helper import format_freshness_footer
        context += format_freshness_footer(
            "SDEO Inspections API",
            snapshot_date=datetime.now(),
            source_label="SDEO Live API",
        ) + "\n"
    except Exception:
        pass

    return {
        "source_id": source_id,
        "records": [data],
        "formatted_context": context,
    }


# ══════════════════════════════════════════════════════════════
# Officer inspection query
# ══════════════════════════════════════════════════════════════
def _query_insp_officer(
    db, officer_name: str,
    start_date: Optional[date], end_date: Optional[date],
) -> Dict[str, Any]:
    """
    Query inspection data for a specific officer.

    Strategy:
      - If date range is specified → call PCM API live for exact date-filtered counts
        (stored records don't have per-record dates)
      - If no date range → use stored officer_inspection_record aggregates
    """
    source_id = f"insp_officer:{officer_name}"

    # First get officer's tehsil_id and officer_id from stored data
    officer_info = db.fetch_all(
        "SELECT DISTINCT officer_id, tehsil_id, tehsil_name, district_name, division_name "
        "FROM officer_inspection_record "
        "WHERE officer_name = %s LIMIT 1",
        (officer_name,),
    )

    if not officer_info:
        # Try officer_inspection_detail
        officer_info = db.fetch_all(
            "SELECT DISTINCT user_id as officer_id, tehsil_id, tehsil_name, "
            "       district_name, division_name "
            "FROM officer_inspection_detail "
            "WHERE officer_name = %s LIMIT 1",
            (officer_name,),
        )

    if not officer_info:
        # Try inspection_officer_summary (SDEO dashboard data)
        ios_info = db.fetch_all(
            "SELECT DISTINCT tehsil_id, tehsil_name, district_name, division_name "
            "FROM inspection_officer_summary "
            "WHERE officer_name = %s LIMIT 1",
            (officer_name,),
        )
        if ios_info:
            # Officer exists only in summary table — use stored summary path
            return _query_officer_from_summary(
                db, officer_name, start_date, end_date, source_id,
            )

    if not officer_info:
        # ── Cross-domain fallback: check OA/requisition data ──
        # Officer might be an OA officer, not an inspection officer
        try:
            from operational_activity_lookup import execute_operational_activity_lookup
            oa_result = execute_operational_activity_lookup(
                f"oa_officer:{officer_name}", question=question or "",
            )
            if oa_result and oa_result.get("records"):
                log.info("Cross-domain: '%s' not in inspection tables, found in OA data", officer_name)
                oa_result["source_id"] = source_id  # keep insp_officer for session context
                # Prepend a note so the LLM knows this is OA data, not inspection data
                note = (
                    f"NOTE: '{officer_name}' was not found in inspection data. "
                    f"However, operational activity (requisition) data is available "
                    f"for this officer and is shown below.\n\n"
                )
                oa_result["formatted_context"] = note + oa_result.get("formatted_context", "")
                return oa_result
        except Exception as e:
            log.debug("OA cross-domain fallback failed for '%s': %s", officer_name, e)

        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"No inspection data found for officer '{officer_name}'.\n",
        }

    info = officer_info[0]
    officer_id = info["officer_id"]
    tehsil_id = info["tehsil_id"]
    tehsil_name = info.get("tehsil_name", "Unknown")
    district_name = info.get("district_name", "Unknown")
    division_name = info.get("division_name", "Unknown")

    # ── Date-specific query → LIVE API CALL ──
    if start_date and end_date:
        return _query_officer_live(
            officer_name, officer_id, tehsil_id,
            tehsil_name, district_name, division_name,
            start_date, end_date, source_id,
        )

    # ── No date filter → use stored data ──
    return _query_officer_stored(
        db, officer_name, officer_id, tehsil_id,
        tehsil_name, district_name, division_name,
        source_id,
    )


def _query_officer_from_summary(
    db, officer_name: str,
    start_date: Optional[date], end_date: Optional[date],
    source_id: str,
) -> Dict[str, Any]:
    """Query officer data from inspection_officer_summary (SDEO dashboard table).

    This table stores pre-aggregated per-officer breakdowns ingested from the
    SDEO inspections-summary API.  Used when the officer is not found in the
    PCM-based tables (officer_inspection_record / officer_inspection_detail).
    """
    from freshness_helper import format_freshness_footer

    # Build the WHERE clause depending on whether dates are supplied
    params: list = [officer_name]
    date_filter = ""
    if start_date and end_date:
        date_filter = " AND start_date >= %s AND end_date <= %s"
        params += [start_date, end_date]

    rows = db.fetch_all(
        "SELECT tehsil_name, district_name, division_name, "
        "       start_date, end_date, "
        "       officer_inspections, officer_challans, officer_firs, "
        "       officer_warnings, officer_no_offenses, officer_sealed, "
        "       snapshot_date "
        "FROM inspection_officer_summary "
        "WHERE officer_name = %s" + date_filter +
        " ORDER BY snapshot_date DESC, start_date DESC",
        tuple(params),
    )

    if not rows:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"No inspection data found for officer '{officer_name}'.\n",
        }

    # Aggregate across all matching rows (could span multiple tehsils/periods)
    total_inspections = sum(r.get("officer_inspections", 0) or 0 for r in rows)
    total_challans = sum(r.get("officer_challans", 0) or 0 for r in rows)
    total_firs = sum(r.get("officer_firs", 0) or 0 for r in rows)
    total_warnings = sum(r.get("officer_warnings", 0) or 0 for r in rows)
    total_no_offenses = sum(r.get("officer_no_offenses", 0) or 0 for r in rows)
    total_sealed = sum(r.get("officer_sealed", 0) or 0 for r in rows)

    # Location info from first row
    first = rows[0]
    tehsil_name = first.get("tehsil_name", "Unknown")
    district_name = first.get("district_name", "Unknown")
    division_name = first.get("division_name", "Unknown")

    # Date range from rows
    all_starts = [r["start_date"] for r in rows if r.get("start_date")]
    all_ends = [r["end_date"] for r in rows if r.get("end_date")]
    period = ""
    if all_starts and all_ends:
        earliest = min(all_starts)
        latest = max(all_ends)
        period = f" ({earliest.strftime('%d %b %Y')} – {latest.strftime('%d %b %Y')})"

    context = f"Inspection Data for {officer_name}{period}\n"
    context += f"Location: {tehsil_name}, {district_name}, {division_name}\n"
    context += "─" * 50 + "\n"
    context += f"  Total Inspections : {total_inspections:,}\n"
    context += f"  Challans Issued   : {total_challans:,}\n"
    context += f"  FIRs Filed        : {total_firs:,}\n"
    context += f"  Warnings          : {total_warnings:,}\n"
    context += f"  No Offense        : {total_no_offenses:,}\n"
    context += f"  Sealed            : {total_sealed:,}\n"

    # If multiple tehsils, show per-tehsil breakdown
    tehsils_seen: Dict[str, Dict] = {}
    for r in rows:
        t = r.get("tehsil_name", "Unknown")
        if t not in tehsils_seen:
            tehsils_seen[t] = {
                "inspections": 0, "challans": 0, "firs": 0,
                "warnings": 0, "no_offenses": 0, "sealed": 0,
            }
        tehsils_seen[t]["inspections"] += r.get("officer_inspections", 0) or 0
        tehsils_seen[t]["challans"] += r.get("officer_challans", 0) or 0
        tehsils_seen[t]["firs"] += r.get("officer_firs", 0) or 0
        tehsils_seen[t]["warnings"] += r.get("officer_warnings", 0) or 0
        tehsils_seen[t]["no_offenses"] += r.get("officer_no_offenses", 0) or 0
        tehsils_seen[t]["sealed"] += r.get("officer_sealed", 0) or 0

    if len(tehsils_seen) > 1:
        context += "\nPer-Tehsil Breakdown:\n"
        for t, d in tehsils_seen.items():
            context += (
                f"  {t}: {d['inspections']} inspections, "
                f"{d['challans']} challans, {d['firs']} FIRs, "
                f"{d['warnings']} warnings, {d['sealed']} sealed\n"
            )

    # Freshness footer
    snap = first.get("snapshot_date")
    try:
        context += "\n" + format_freshness_footer(
            "inspection_officer_summary",
            snapshot_date=snap,
            sync_interval_s=2 * 60 * 60,
            source_label="SDEO Inspection Summary",
        )
    except Exception:
        pass

    return {
        "source_id": source_id,
        "records": rows,
        "formatted_context": context,
    }


def _query_officer_live(
    officer_name: str, officer_id: str, tehsil_id: int,
    tehsil_name: str, district_name: str, division_name: str,
    start_date: date, end_date: date, source_id: str,
) -> Dict[str, Any]:
    """Call PCM officer-inspections API live for a specific date range."""
    try:
        resp = requests.get(
            PCM_OFFICER_INSPECTIONS,
            params={
                "tehsilId": tehsil_id,
                "officerId": officer_id,
                "fromDate": start_date.isoformat(),
                "toDate": (end_date + timedelta(days=1)).isoformat(),
            },
            headers=_HEADERS,
            timeout=_API_TIMEOUT,
        )
        resp.raise_for_status()
        records = resp.json()
        if not isinstance(records, list):
            records = []
    except Exception as e:
        log.warning("Live PCM API call failed for officer %s: %s", officer_name, e)
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": (
                f"Unable to fetch live inspection data for {officer_name} "
                f"({start_date} to {end_date}). API error.\n"
            ),
        }

    # Aggregate the records
    total = len(records)
    challans = sum(1 for r in records if _is_true(r.get("challanCase")))
    warnings = sum(1 for r in records if _is_true(r.get("warningCase")))
    no_offence = sum(1 for r in records if _is_true(r.get("noOffense")))
    arrests = sum(1 for r in records if _is_true(r.get("arrestCase")))
    confiscated = sum(1 for r in records if _is_true(r.get("confiscated")))
    total_fine = sum(r.get("fineAmount", 0) or 0 for r in records)

    context = f"Inspection Data for {officer_name}\n"
    context += f"Date Range: {start_date} to {end_date}\n"
    context += f"Location: {tehsil_name}, {district_name}, {division_name}\n"
    context += "=" * 50 + "\n"
    context += f"Total Inspections: {total:,}\n"
    context += f"Challans Issued: {challans:,}\n"
    context += f"Warnings Issued: {warnings:,}\n"
    context += f"No Offence Found: {no_offence:,}\n"
    context += f"Arrest Cases: {arrests:,}\n"
    context += f"Confiscated: {confiscated:,}\n"
    context += f"Total Fine Amount: Rs. {total_fine:,.0f}\n"

    # Include individual record details (up to 100 for context size)
    if records:
        context += f"\nIndividual Inspection Records ({min(len(records), 100)} of {len(records)}):\n"
        for i, r in enumerate(records[:100]):
            outcome = []
            if _is_true(r.get("challanCase")):
                outcome.append("Challan")
            if _is_true(r.get("warningCase")):
                outcome.append("Warning")
            if _is_true(r.get("noOffense")):
                outcome.append("No Offence")
            if _is_true(r.get("arrestCase")):
                outcome.append("Arrest")
            if _is_true(r.get("confiscated")):
                outcome.append("Confiscated")
            outcome_str = ", ".join(outcome) if outcome else "N/A"
            fine = r.get("fineAmount", 0) or 0
            owner = r.get("ownerName", "N/A") or "N/A"
            address = r.get("address", "N/A") or "N/A"
            cnic = r.get("cnic", "N/A") or "N/A"
            context += (
                f"  {i+1}. Owner: {owner} | CNIC: {cnic} | "
                f"Address: {address} | Outcome: {outcome_str} | "
                f"Fine: Rs. {fine:,.0f}\n"
            )

    context += f"\n(Data fetched live from PCM API for the specified date range)\n"

    # Phase 1 — live API freshness stamp
    try:
        from freshness_helper import format_freshness_footer
        context += format_freshness_footer(
            "PCM Officer Inspections API",
            snapshot_date=datetime.now(),
            source_label="PCM Live API",
        ) + "\n"
    except Exception:
        pass

    return {
        "source_id": source_id,
        "records": records,
        "formatted_context": context,
    }


def _query_officer_stored(
    db, officer_name: str, officer_id: str, tehsil_id: int,
    tehsil_name: str, district_name: str, division_name: str,
    source_id: str,
) -> Dict[str, Any]:
    """Use stored officer_inspection_record data (no date filter)."""
    # Get summary from officer_inspection_detail (has total_inspections etc)
    summary = db.fetch_all(
        "SELECT total_inspections, total_challans, fine_amount, sealed, arrest_case "
        "FROM officer_inspection_detail "
        "WHERE officer_name = %s AND snapshot_date = ("
        "  SELECT MAX(snapshot_date) FROM officer_inspection_detail WHERE officer_name = %s"
        ")",
        (officer_name, officer_name),
    )

    # Get breakdown from officer_inspection_record
    agg = db.fetch_all(
        "SELECT COUNT(*) as total, "
        "  SUM(CASE WHEN is_challan THEN 1 ELSE 0 END) as challans, "
        "  SUM(CASE WHEN is_warning THEN 1 ELSE 0 END) as warnings, "
        "  SUM(CASE WHEN is_no_offense THEN 1 ELSE 0 END) as no_offence, "
        "  SUM(CASE WHEN is_arrest THEN 1 ELSE 0 END) as arrests, "
        "  SUM(CASE WHEN is_confiscated THEN 1 ELSE 0 END) as confiscated, "
        "  SUM(fine_amount) as total_fine, "
        "  MIN(from_date) as data_from, "
        "  MAX(to_date) as data_to "
        "FROM officer_inspection_record "
        "WHERE officer_name = %s AND snapshot_date = ("
        "  SELECT MAX(snapshot_date) FROM officer_inspection_record WHERE officer_name = %s"
        ")",
        (officer_name, officer_name),
    )

    if not summary and (not agg or not agg[0].get("total")):
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"No inspection data found for officer '{officer_name}'.\n",
        }

    # Capture snapshot date for freshness footer
    _officer_snapshot = None
    try:
        _snap = db.fetch_one(
            "SELECT MAX(snapshot_date) AS d FROM officer_inspection_detail "
            "WHERE officer_name = %s",
            (officer_name,),
        )
        _officer_snapshot = (_snap.get("d") if _snap else None)
    except Exception:
        pass

    context = f"Inspection Data for {officer_name}\n"
    context += f"Location: {tehsil_name}, {district_name}, {division_name}\n"
    context += "=" * 50 + "\n"

    if summary:
        s = summary[0]
        context += f"Total Inspections (PCM Summary): {s.get('total_inspections', 0):,}\n"
        context += f"Total Challans (PCM Summary): {s.get('total_challans', 0):,}\n"
        context += f"Fine Amount (PCM Summary): Rs. {s.get('fine_amount', 0):,}\n"
        context += f"Sealed (PCM Summary): {s.get('sealed', 0):,}\n"
        context += f"Arrest Cases (PCM Summary): {s.get('arrest_case', 0):,}\n"

    if agg and agg[0].get("total"):
        a = agg[0]
        data_from = a.get("data_from", "N/A")
        data_to = a.get("data_to", "N/A")
        context += f"\nDetailed Records ({data_from} to {data_to}):\n"
        context += f"  Total Records: {a['total']:,}\n"
        context += f"  Challans: {a.get('challans', 0):,}\n"
        context += f"  Warnings: {a.get('warnings', 0):,}\n"
        context += f"  No Offence: {a.get('no_offence', 0):,}\n"
        context += f"  Arrests: {a.get('arrests', 0):,}\n"
        context += f"  Confiscated: {a.get('confiscated', 0):,}\n"
        context += f"  Total Fine: Rs. {a.get('total_fine', 0):,.0f}\n"

    # Phase 1 — freshness stamp for stored officer data
    try:
        from freshness_helper import format_freshness_footer
        if _officer_snapshot is not None:
            context += "\n" + format_freshness_footer(
                "officer_inspection_detail",
                snapshot_date=_officer_snapshot,
                sync_interval_s=3 * 60 * 60,
            ) + "\n"
    except Exception:
        pass

    return {
        "source_id": source_id,
        "records": summary or agg,
        "formatted_context": context,
    }


def _query_repeat_offenders(db, limit: int = 30) -> Dict[str, Any]:
    """Find CNICs that appear most frequently in inspection records (repeat offenders)."""
    source_id = "insp_repeat_offenders"

    rows = db.fetch_all(
        "SELECT cnic, owner_name, COUNT(*) as times_inspected, "
        "       SUM(CASE WHEN is_challan THEN 1 ELSE 0 END) as challans, "
        "       SUM(CASE WHEN is_warning THEN 1 ELSE 0 END) as warnings, "
        "       SUM(fine_amount) as total_fine, "
        "       COUNT(DISTINCT officer_name) as officers_count, "
        "       STRING_AGG(DISTINCT officer_name, ', ') as officers, "
        "       STRING_AGG(DISTINCT tehsil_name, ', ') as locations "
        "FROM officer_inspection_record "
        "WHERE cnic IS NOT NULL AND cnic != '' "
        "GROUP BY cnic, owner_name "
        "HAVING COUNT(*) > 1 "
        "ORDER BY COUNT(*) DESC "
        f"LIMIT {limit}",
    )

    if not rows:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": "No repeat offenders found in inspection records.\n",
        }

    # Capture snapshot date for freshness footer
    _repeat_snapshot = None
    try:
        _snap = db.fetch_one(
            "SELECT MAX(snapshot_date) AS d FROM officer_inspection_record"
        )
        _repeat_snapshot = _snap.get("d") if _snap else None
    except Exception:
        pass

    context = "Repeat Offenders — CNICs Inspected Multiple Times\n"
    context += "=" * 50 + "\n"
    context += f"Top {len(rows)} most frequently inspected CNICs:\n\n"

    for i, r in enumerate(rows):
        context += (
            f"{i+1}. {r.get('owner_name', 'Unknown')} (CNIC: {r['cnic']})\n"
            f"   Times Inspected: {r['times_inspected']} | "
            f"Challans: {r.get('challans', 0)} | "
            f"Warnings: {r.get('warnings', 0)} | "
            f"Fine: Rs. {r.get('total_fine', 0):,.0f}\n"
            f"   Officers: {r.get('officers', 'N/A')} | "
            f"Locations: {r.get('locations', 'N/A')}\n\n"
        )

    # Phase 1 — freshness stamp
    try:
        from freshness_helper import format_freshness_footer
        if _repeat_snapshot is not None:
            context += "\n" + format_freshness_footer(
                "officer_inspection_record",
                snapshot_date=_repeat_snapshot,
                sync_interval_s=3 * 60 * 60,
            ) + "\n"
    except Exception:
        pass

    return {
        "source_id": source_id,
        "records": rows,
        "formatted_context": context,
    }


def _query_insp_cnic(db, cnic: str) -> Dict[str, Any]:
    """Query officer_inspection_record for a specific CNIC to find all actions taken."""
    source_id = f"insp_cnic:{cnic}"

    rows = db.fetch_all(
        "SELECT officer_name, tehsil_name, district_name, division_name, "
        "       owner_name, cnic, address, is_challan, is_warning, "
        "       is_no_offense, is_arrest, is_confiscated, fine_amount, "
        "       from_date, to_date "
        "FROM officer_inspection_record "
        "WHERE cnic = %s "
        "ORDER BY officer_name, fine_amount DESC",
        (cnic,),
    )

    if not rows:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": (
                f"No inspection records found for CNIC {cnic}.\n"
                f"This person has not been inspected/challaned by any PERA officer.\n"
            ),
        }

    # Capture snapshot date for freshness footer
    _cnic_snapshot = None
    try:
        _snap = db.fetch_one(
            "SELECT MAX(snapshot_date) AS d FROM officer_inspection_record "
            "WHERE cnic = %s",
            (cnic,),
        )
        _cnic_snapshot = _snap.get("d") if _snap else None
    except Exception:
        pass

    # Aggregate
    total = len(rows)
    challans = sum(1 for r in rows if r.get("is_challan"))
    warnings = sum(1 for r in rows if r.get("is_warning"))
    no_offence = sum(1 for r in rows if r.get("is_no_offense"))
    arrests = sum(1 for r in rows if r.get("is_arrest"))
    confiscated = sum(1 for r in rows if r.get("is_confiscated"))
    total_fine = sum(r.get("fine_amount", 0) or 0 for r in rows)

    owner_name = rows[0].get("owner_name", "Unknown")

    context = f"Inspection Records for CNIC: {cnic}\n"
    context += f"Owner Name: {owner_name}\n"
    context += "=" * 50 + "\n"
    context += f"Total Times Inspected: {total}\n"
    context += f"Challans Received: {challans}\n"
    context += f"Warnings Received: {warnings}\n"
    context += f"No Offence Found: {no_offence}\n"
    context += f"Arrest Cases: {arrests}\n"
    context += f"Confiscated: {confiscated}\n"
    context += f"Total Fine Amount: Rs. {total_fine:,.0f}\n"

    # Group by officer
    by_officer: Dict[str, list] = {}
    for r in rows:
        oname = r.get("officer_name", "Unknown")
        by_officer.setdefault(oname, []).append(r)

    context += f"\nActions by Officer ({len(by_officer)} officers):\n"
    for oname, orows in sorted(by_officer.items(), key=lambda x: -len(x[1])):
        o_challans = sum(1 for r in orows if r.get("is_challan"))
        o_warnings = sum(1 for r in orows if r.get("is_warning"))
        o_fine = sum(r.get("fine_amount", 0) or 0 for r in orows)
        location = orows[0].get("tehsil_name", "Unknown")
        context += (
            f"  {oname} ({location}): "
            f"{len(orows)} inspections, "
            f"{o_challans} challans, "
            f"{o_warnings} warnings, "
            f"Rs. {o_fine:,.0f} fine\n"
        )

    # Individual records (up to 50)
    context += f"\nIndividual Records ({min(len(rows), 50)} of {len(rows)}):\n"
    for i, r in enumerate(rows[:50]):
        outcome = []
        if r.get("is_challan"):
            outcome.append("Challan")
        if r.get("is_warning"):
            outcome.append("Warning")
        if r.get("is_no_offense"):
            outcome.append("No Offence")
        if r.get("is_arrest"):
            outcome.append("Arrest")
        if r.get("is_confiscated"):
            outcome.append("Confiscated")
        outcome_str = ", ".join(outcome) if outcome else "N/A"
        fine = r.get("fine_amount", 0) or 0
        officer = r.get("officer_name", "N/A")
        address = r.get("address", "N/A") or "N/A"
        context += (
            f"  {i+1}. Officer: {officer} | Address: {address} | "
            f"Outcome: {outcome_str} | Fine: Rs. {fine:,.0f}\n"
        )

    # Phase 1 — freshness stamp
    try:
        from freshness_helper import format_freshness_footer
        if _cnic_snapshot is not None:
            context += "\n" + format_freshness_footer(
                "officer_inspection_record",
                snapshot_date=_cnic_snapshot,
                sync_interval_s=3 * 60 * 60,
            ) + "\n"
    except Exception:
        pass

    return {
        "source_id": source_id,
        "records": rows,
        "formatted_context": context,
    }


def _is_true(val) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val) if val else False
