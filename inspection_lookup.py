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
SDEO_TOP_KPIS = (
    "https://pera360.punjab.gov.pk/backend/api/sdeo-dashboard/top-kpis"
)
SDEO_CHALLAN_STATUS_BREAKDOWN = (
    "https://pera360.punjab.gov.pk/backend/api/sdeo-dashboard/challan-status-breakdown"
)
PCM_DASHBOARD_COUNTS = (
    "https://pera360.punjab.gov.pk/backend/api/Pcm/dashboard-counts"
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
    r"epo|removal\s+orders?|"
    r"confiscat(?:ed|ion)|"
    r"total\s+actions|"
    # Urdu / Roman-Urdu
    r"muayina|mu[aā]yin[ae]|"
    r"jaiz[ae]|"
    r"checking|checkings|"
    # "summary" + common typos (summery, summmery, summari, etc.) and
    # related general-status words. Without these, a higher-authority
    # query like "tell me the summary of Lahore districts from 1 Jan
    # to 10 Jan 2026" gets dropped on the keyword gate even though the
    # location and date range are clearly present.
    r"summ+[ae]r+i?y?|summer+y|"
    r"performance\s+(?:report|summary|stats|figures)|"
    r"overall\s+(?:summary|performance|figures|report)"
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

    # ── pera_entities alias resolver runs FIRST (production wiring) ──
    # Catches D.G. Khan / DG Khan / Dera Ghazi Khan and any other alias
    # entries before the substring-match path runs. SKIPPED when the
    # user explicitly typed "district" or "tehsil" — the alias map
    # only carries division names, so promoting to division on a
    # district query would be wrong.
    _explicit_non_division = re.search(
        r"\b(district|tehsil[s]?|station[s]?)\b", q_lower,
    )
    if not _explicit_non_division:
        try:
            from pera_entities import canonical_division_name
            canon_div = canonical_division_name(question)
            if canon_div:
                return {"level": "division", "division_name": canon_div}
        except Exception:
            pass

    # If user explicitly says "division" / "district" / "tehsil", respect that.
    # PERA also calls tehsils "stations" colloquially in the dashboard UI.
    explicit_div = re.search(r"\bdivision\b", q_lower)
    explicit_dist = re.search(r"\bdistrict\b", q_lower)
    explicit_teh = re.search(r"\btehsil[s]?\b|\bstation[s]?\b", q_lower)

    def _tehsil_token_match(tname: str) -> bool:
        """True if every meaningful word of `tname` (excluding common
        suffix tokens like 'Town', 'Cantt') appears in the query.
        Lets users say 'allama iqbal' and still hit 'Allama Iqbal Town'.
        """
        suffixes = {"town", "cantt", "city", "saddar", "hq"}
        toks = [t for t in re.findall(r"[a-z]+", tname.lower())
                if t not in suffixes]
        if len(toks) < 2:
            return False
        return all(re.search(rf"\b{re.escape(t)}\b", q_lower) for t in toks)

    # Explicit level requested — check that level first
    if explicit_teh:
        for t in (_TEHSIL_NAMES or []):
            if t.lower() in q_lower:
                return {"level": "tehsil", "tehsil_name": t}
        # Fallback: token match (handles "allama iqbal" → "Allama Iqbal Town")
        for t in (_TEHSIL_NAMES or []):
            if _tehsil_token_match(t):
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
# ── Ranking intent (top-N / which-most queries) ──────────────
# Triggered by superlatives + a metric + (optionally) an admin level.
_RANK_TRIGGER_RE = re.compile(
    r"\b("
    r"top\s*\d*|most|highest|maximum|max|"
    r"largest|biggest|sab\s*s[ey]\s*z[iy]ada|sabse\s*z[iy]ada|"
    r"konsa|kaunsa|which|kis\s+(?:tehsil|station|district|division)|"
    r"rank(?:ing)?|leading|leader|"
    r"least|lowest|minimum|min|kam\s*(?:ziada|zyada)|sab\s*s[ey]\s*kam"
    r")\b",
    re.I,
)

_RANK_LEAST_RE = re.compile(
    r"\b(least|lowest|minimum|min|kam\s*(?:ziada|zyada)|sab\s*s[ey]\s*kam)\b",
    re.I,
)

# Order matters — most specific patterns first. Payment-status qualifiers
# (paid/unpaid/overdue) must be checked BEFORE the bare "challans" pattern,
# otherwise "stations with most challans which actually paid" lands on the
# generic challan-count metric and the answer ignores the payment filter.
_RANK_METRIC_MAP = [
    ("fine_amount", re.compile(
        r"\b(fine\s*amount|total\s*fine|fine\s+imposed|imposed\s+fine)\b", re.I)),
    ("paid_challans", re.compile(
        r"\b(paid|recovered|received|actually\s+paid|jo\s+pay|recovery)\b", re.I)),
    ("unpaid_challans", re.compile(
        r"\b(unpaid|outstanding|pending(?!\s+(?:request|case))|not\s+paid|"
        r"un[\s-]?recovered|baki|jo\s+nahi\s+pay)\b", re.I)),
    ("overdue_challans", re.compile(
        r"\b(overdue|over[\s-]?due|late|expired)\b", re.I)),
    ("firs",          re.compile(r"\bfirs?\b", re.I)),
    ("sealed",        re.compile(r"\bseal(?:ed|ing)?\b", re.I)),
    ("challans",      re.compile(r"\bch[ae]+l+a+n+s?\b", re.I)),
    ("warnings",      re.compile(r"\bwarning(?:s)?\b", re.I)),
    ("no_offenses",   re.compile(r"\bno[\s-]*offen[cs]es?\b", re.I)),
    ("total_actions", re.compile(
        r"\b(inspect(?:ions?)?|muayina|jaiz[ae]|total\s+actions?)\b", re.I)),
]

# Admin-level keyword → column to group by in the ranking query.
_RANK_LEVEL_MAP = [
    ("tehsil",   re.compile(r"\b(tehsils?|stations?)\b", re.I)),
    ("district", re.compile(r"\bdistricts?\b", re.I)),
    ("division", re.compile(r"\bdivisions?\b", re.I)),
]


_AMOUNT_QUALIFIER_RE = re.compile(
    r"\b(amount|amounts|rupees?|rs\.?|pkr|monetary|value|fine\s+value|"
    r"paisa|paise|kitn[ae]\s+paise|raqam|amount[\s-]?wise|by\s+amount)\b",
    re.I,
)


def _detect_rank_intent(q: str) -> Optional[str]:
    """Return e.g. 'insp_top:firs:tehsil:desc' or
    'insp_top:paid_amount:tehsil:desc' or None.

    Encoded format: insp_top:<metric>:<level>:<order>
      metric — count metric (firs, sealed, challans, warnings,
               no_offenses, total_actions, paid_challans,
               unpaid_challans, overdue_challans) OR amount metric
               (paid_amount, unpaid_amount, overdue_amount, fine_amount)
      level  ∈ {tehsil, district, division}    (defaults tehsil)
      order  ∈ {desc, asc}                     (asc for least/lowest)
    """
    if not _RANK_TRIGGER_RE.search(q):
        return None
    metric = None
    for col, pat in _RANK_METRIC_MAP:
        if pat.search(q):
            metric = col
            break
    if not metric:
        return None

    # Amount-mode promotion: when an amount qualifier is present AND the
    # detected metric is payment-related (paid/unpaid/overdue/challans),
    # rank by the corresponding rupee total instead of the row count.
    if _AMOUNT_QUALIFIER_RE.search(q):
        amount_metric = {
            "paid_challans": "paid_amount",
            "unpaid_challans": "unpaid_amount",
            "overdue_challans": "overdue_amount",
            "challans": "fine_amount",
        }.get(metric)
        if amount_metric:
            metric = amount_metric

    level = "tehsil"  # default
    for lvl, pat in _RANK_LEVEL_MAP:
        if pat.search(q):
            level = lvl
            break
    order = "asc" if _RANK_LEAST_RE.search(q) else "desc"
    return f"insp_top:{metric}:{level}:{order}"


_NAME_STOP_WORDS = frozenset({
    "what", "about", "the", "same", "dates", "date", "inspections", "inspection",
    "summary", "summery", "from", "tell", "show", "detailed", "detail", "details",
    "total", "how", "many", "give", "officer", "officers", "please", "also",
    "april", "may", "june", "july", "august", "september", "october", "november",
    "december", "january", "february", "march",
    # Short month abbreviations — without these, "1st jan to 10 jan 2026"
    # leaks "jan" into the candidate pool and pairs it with another token
    # to false-match officers like "Muhammad Sher Jan Khan".
    "jan", "feb", "mar", "apr", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec",
    # Common summary typos and ordinal/range words.
    "summmery", "summmary", "summari", "summaries",
    "till", "until", "between",
    "with", "and", "for", "this",
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
    """Extract date range from the question.

    Strategy (Phase-1 fix): try the canonical `pera_dates.parse_date_range`
    first. Fall back to the legacy challan parser only when the canonical
    parser returns None, so existing callers that depend on legacy formats
    keep working.
    """
    try:
        from pera_dates import parse_date_range
        dr = parse_date_range(question)
        if dr is not None:
            return (dr.start, dr.end)
    except Exception:
        pass

    # Legacy fallback for any pattern the canonical parser doesn't yet cover.
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

    # Officer detection runs LATER — first we want admin-level keywords
    # (division/district/tehsil/station) and explicit location matches to
    # win. Otherwise queries like "allama iqbal stations from..." get
    # mis-routed to a real officer named "M Zafar Iqbal" because token
    # 'iqbal' false-pairs in the officer LIKE search.
    officer_name = None

    # Must have inspection keyword (for non-CNIC queries) — with one
    # fallback: if the user explicitly named an admin level
    # (division/district/tehsil) AND a date range is present, treat the
    # query as an inspection summary even when the inspection vocabulary
    # is missing or misspelled. Higher-authority officials routinely
    # ask "Lahore districts from 1 Jan to 10 Jan 2026" with no other
    # cue — without this fallback such queries fall through to docs.
    # Ranking shortcut — runs BEFORE the keyword gate because superlative
    # + metric + level (e.g. "most challans tehsil") is a high-confidence
    # inspection-ranking query even without the literal "inspection" word.
    rank_intent = _detect_rank_intent(q)
    if rank_intent:
        return rank_intent

    if not _INSP_KEYWORDS.search(q):
        has_admin_word = re.search(r"\b(division|district|tehsil|station)s?\b", q, re.I)
        has_date_range = False
        try:
            dr = _extract_date_range(q)
            if dr and dr[0] and dr[1]:
                has_date_range = True
        except Exception:
            pass
        if not (has_admin_word and has_date_range):
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

    # Location detection runs BEFORE officer detection so admin-level
    # queries don't get hijacked by greedy name-pair LIKE matches on
    # tokens that happen to overlap with officer names.
    location = _detect_location(q)
    if location:
        level = location["level"]
        if level == "division":
            return f"insp_division:{location['division_name']}"
        elif level == "district":
            return f"insp_district:{location['district_name']}"
        elif level == "tehsil":
            return f"insp_tehsil:{location['tehsil_name']}"

    # Officer fallback — only when no location matched.
    officer_name = _detect_insp_officer_name(q)
    if officer_name:
        return f"insp_officer:{officer_name}"

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
    elif base == "insp_top":
        # Format: insp_top:<metric>:<level>:<order>
        metric = parts[1] if len(parts) > 1 else "firs"
        level = parts[2] if len(parts) > 2 else "tehsil"
        order = parts[3] if len(parts) > 3 else "desc"
        return _query_top_locations(db, metric, level, order)

    return None


def _query_top_by_amount(
    db, status_filter: Optional[str], sum_col: str, level: str,
    order: str = "desc", metric_name: str = "fine_amount",
) -> Dict[str, Any]:
    """Rank tehsils/districts/divisions by SUM(<sum_col>) from challan_data,
    optionally filtered to a payment status.
    """
    direction = "ASC" if (order or "").lower() == "asc" else "DESC"
    label_map = {
        "paid_amount": "Paid Amount (Rs)",
        "unpaid_amount": "Unpaid Outstanding (Rs)",
        "overdue_amount": "Overdue Outstanding (Rs)",
        "fine_amount": "Total Fine Imposed (Rs)",
    }
    metric_label = label_map.get(metric_name, "Amount (Rs)")
    level_label = {"tehsil": "Tehsil",
                   "district": "District",
                   "division": "Division"}[level]

    where_clause = ""
    args: tuple = ()
    if status_filter:
        where_clause = "WHERE LOWER(challan_status) = %s"
        args = (status_filter,)

    if level == "tehsil":
        rows = db.fetch_all(
            f"SELECT tehsil_name AS loc_name, "
            f"       COALESCE(SUM({sum_col}), 0) AS metric_value, "
            f"       COUNT(*) AS challan_count "
            f"FROM challan_data {where_clause} "
            f"GROUP BY tehsil_name "
            f"ORDER BY metric_value {direction} NULLS LAST "
            f"LIMIT 25",
            args,
        )
    elif level == "district":
        rows = db.fetch_all(
            f"SELECT district_name AS loc_name, "
            f"       COALESCE(SUM({sum_col}), 0) AS metric_value, "
            f"       COUNT(*) AS challan_count "
            f"FROM challan_data {where_clause} "
            f"GROUP BY district_name "
            f"ORDER BY metric_value {direction} NULLS LAST "
            f"LIMIT 25",
            args,
        )
    else:  # division
        rows = db.fetch_all(
            f"SELECT division_name AS loc_name, "
            f"       COALESCE(SUM({sum_col}), 0) AS metric_value, "
            f"       COUNT(*) AS challan_count "
            f"FROM challan_data {where_clause} "
            f"GROUP BY division_name "
            f"ORDER BY metric_value {direction} NULLS LAST "
            f"LIMIT 25",
            args,
        )

    if not rows:
        return {
            "source_id": f"insp_top:{metric_name}:{level}",
            "records": [],
            "formatted_context": (
                f"No {metric_name} data available at {level} level.\n"
            ),
        }

    sort_word = "Lowest" if direction == "ASC" else "Highest"
    ctx = f"Ranking — {level_label}s by {metric_label} ({sort_word} first)\n"
    ctx += "=" * 50 + "\n"
    ctx += (
        f"{'Rank':<5s}{level_label:<25s} {metric_label:>22s}  "
        f"Challans\n"
    )
    for i, r in enumerate(rows, 1):
        ctx += (
            f"{i:<5d}{(r.get('loc_name') or '—'):<25s} "
            f"Rs. {int(r.get('metric_value') or 0):>18,}  "
            f"{int(r.get('challan_count') or 0):>8,}\n"
        )
    ctx += (
        f"\n(Ranking computed from challan_data row-level SUM. "
        f"Cumulative all-time totals; not date-range filtered.)\n"
    )
    return {
        "source_id": f"insp_top:{metric_name}:{level}:{order}",
        "records": rows,
        "formatted_context": ctx,
        # Production wiring: deterministic answer & evidence_type so
        # answerer.py can bypass LLM rendering for numeric tables.
        "deterministic_answer": ctx,
        "structured_payload": ctx,
        "evidence_type": "ranking",
    }


def _query_top_by_payment_status(
    db, status: str, level: str, order: str = "desc",
) -> Dict[str, Any]:
    """Rank tehsils/districts/divisions by paid/unpaid/overdue challan
    count using challan_tehsil_breakdown. Aggregates upward from tehsil
    rows when the caller asks for district or division level.
    """
    direction = "ASC" if (order or "").lower() == "asc" else "DESC"
    status_label = {
        "paid": "Paid Challans",
        "unpaid": "Unpaid Challans",
        "overdue": "Overdue Challans",
    }[status]
    level_label = {"tehsil": "Tehsil",
                   "district": "District",
                   "division": "Division"}[level]

    if level == "tehsil":
        rows = db.fetch_all(
            "SELECT tehsil_name AS loc_name, "
            "       total_requisitions AS metric_value, "
            "       hoarding_count, price_control_count, "
            "       encroachment_count, land_retrieval_count, "
            "       public_nuisance_count "
            "FROM challan_tehsil_breakdown "
            "WHERE status = %s AND tehsil_name IS NOT NULL "
            f"ORDER BY total_requisitions {direction} NULLS LAST "
            "LIMIT 25",
            (status,),
        )
    elif level == "district":
        rows = db.fetch_all(
            "SELECT d.district_name AS loc_name, "
            "       SUM(b.total_requisitions) AS metric_value, "
            "       SUM(b.hoarding_count) AS hoarding_count, "
            "       SUM(b.price_control_count) AS price_control_count, "
            "       SUM(b.encroachment_count) AS encroachment_count, "
            "       SUM(b.land_retrieval_count) AS land_retrieval_count, "
            "       SUM(b.public_nuisance_count) AS public_nuisance_count "
            "FROM challan_tehsil_breakdown b "
            "JOIN dim_tehsil t ON b.tehsil_name = t.tehsil_name "
            "JOIN dim_district d ON t.district_id = d.district_id "
            "WHERE b.status = %s "
            "GROUP BY d.district_name "
            f"ORDER BY metric_value {direction} NULLS LAST "
            "LIMIT 25",
            (status,),
        )
    else:  # division
        rows = db.fetch_all(
            "SELECT dv.division_name AS loc_name, "
            "       SUM(b.total_requisitions) AS metric_value, "
            "       SUM(b.hoarding_count) AS hoarding_count, "
            "       SUM(b.price_control_count) AS price_control_count, "
            "       SUM(b.encroachment_count) AS encroachment_count, "
            "       SUM(b.land_retrieval_count) AS land_retrieval_count, "
            "       SUM(b.public_nuisance_count) AS public_nuisance_count "
            "FROM challan_tehsil_breakdown b "
            "JOIN dim_tehsil t ON b.tehsil_name = t.tehsil_name "
            "JOIN dim_district d ON t.district_id = d.district_id "
            "JOIN dim_division dv ON d.division_id = dv.division_id "
            "WHERE b.status = %s "
            "GROUP BY dv.division_name "
            f"ORDER BY metric_value {direction} NULLS LAST "
            "LIMIT 25",
            (status,),
        )

    if not rows:
        return {
            "source_id": f"insp_top:{status}_challans:{level}",
            "records": [],
            "formatted_context": (
                f"No {status} challan data available at {level} level.\n"
            ),
        }

    sort_word = "Lowest" if direction == "ASC" else "Highest"
    ctx = f"Ranking — {level_label}s by {status_label} ({sort_word} first)\n"
    ctx += "=" * 50 + "\n"
    ctx += (
        f"{'Rank':<5s}{level_label:<25s} {status_label:>18s}  "
        f"Hoard  Price  Encroach\n"
    )
    for i, r in enumerate(rows, 1):
        ctx += (
            f"{i:<5d}{(r.get('loc_name') or '—'):<25s} "
            f"{int(r.get('metric_value') or 0):>18,}  "
            f"{int(r.get('hoarding_count') or 0):>5,}  "
            f"{int(r.get('price_control_count') or 0):>5,}  "
            f"{int(r.get('encroachment_count') or 0):>8,}\n"
        )
    ctx += (
        f"\n(Ranking computed from challan_tehsil_breakdown table — "
        f"cumulative all-time totals for {status} challans, not date-range "
        f"filtered. Hoard/Price/Encroach columns show requisition-type "
        f"composition.)\n"
    )
    return {
        "source_id": f"insp_top:{status}_challans:{level}:{order}",
        "records": rows,
        "formatted_context": ctx,
        "deterministic_answer": ctx,
        "structured_payload": ctx,
        "evidence_type": "ranking",
    }


def _query_top_locations(
    db, metric: str, level: str, order: str = "desc",
) -> Dict[str, Any]:
    """Rank tehsils/districts/divisions by a metric using the latest
    snapshot in inspection_performance. Used for "which station has the
    most FIRs" / "top 5 tehsils by sealed" / "sab sy ziada konsa stations
    fir kr raha hy" type queries.
    """
    safe_metrics = {"firs", "sealed", "challans", "warnings",
                    "no_offenses", "total_actions",
                    "paid_challans", "unpaid_challans", "overdue_challans",
                    "paid_amount", "unpaid_amount", "overdue_amount",
                    "fine_amount"}
    safe_levels = {"tehsil", "district", "division"}
    if metric not in safe_metrics or level not in safe_levels:
        return {
            "source_id": f"insp_top:{metric}:{level}",
            "records": [],
            "formatted_context": (
                f"Cannot rank by metric={metric!r} at level={level!r}.\n"
            ),
        }
    direction = "ASC" if (order or "").lower() == "asc" else "DESC"

    # Amount-based rankings (Rs totals) come from challan_data row-level
    # aggregation — challan_tehsil_breakdown only stores counts.
    amount_metric_status = {
        "paid_amount": ("paid", "paid_amount"),
        "unpaid_amount": ("unpaid", "outstanding_amount"),
        "overdue_amount": ("overdue", "outstanding_amount"),
        "fine_amount": (None, "fine_amount"),
    }.get(metric)
    if amount_metric_status:
        status_filter, sum_col = amount_metric_status
        return _query_top_by_amount(db, status_filter, sum_col, level,
                                    order, metric)

    # Count-based payment rankings → challan_tehsil_breakdown.
    payment_metric = {
        "paid_challans": "paid",
        "unpaid_challans": "unpaid",
        "overdue_challans": "overdue",
    }.get(metric)
    if payment_metric:
        return _query_top_by_payment_status(db, payment_metric, level, order)

    name_col = f"{level}_name"
    rows = db.fetch_all(
        f"SELECT {name_col} AS loc_name, {metric} AS metric_value, "
        f"       total_actions, challans, firs, warnings, sealed, "
        f"       snapshot_date "
        f"FROM inspection_performance "
        f"WHERE level = %s AND snapshot_date = ("
        f"  SELECT MAX(snapshot_date) FROM inspection_performance WHERE level = %s"
        f") AND {name_col} IS NOT NULL "
        f"ORDER BY {metric} {direction} NULLS LAST "
        f"LIMIT 25",
        (level, level),
    )
    if not rows:
        return {
            "source_id": f"insp_top:{metric}:{level}",
            "records": [],
            "formatted_context": (
                f"No rows in inspection_performance at level={level} to rank.\n"
            ),
        }

    metric_label = {
        "firs": "FIRs",
        "sealed": "Sealed Premises",
        "challans": "Challans",
        "warnings": "Warnings",
        "no_offenses": "No-Offense Cases",
        "total_actions": "Total Inspections/Actions",
    }[metric]
    level_label = {"tehsil": "Tehsil", "district": "District", "division": "Division"}[level]

    snap = rows[0].get("snapshot_date")
    sort_word = "Lowest" if direction == "ASC" else "Highest"
    ctx = f"Ranking — {level_label}s by {metric_label} ({sort_word} first)\n"
    if snap:
        ctx += f"Snapshot date: {snap}\n"
    ctx += "=" * 50 + "\n"
    ctx += f"{'Rank':<5s}{level_label:<25s} {metric_label:>15s}  Insp  Chal  FIR  Seal  Warn\n"
    for i, r in enumerate(rows, 1):
        ctx += (
            f"{i:<5d}{(r.get('loc_name') or '—'):<25s} "
            f"{int(r.get('metric_value') or 0):>15,}  "
            f"{int(r.get('total_actions') or 0):>4,}  "
            f"{int(r.get('challans') or 0):>4,}  "
            f"{int(r.get('firs') or 0):>3,}  "
            f"{int(r.get('sealed') or 0):>4,}  "
            f"{int(r.get('warnings') or 0):>4,}\n"
        )
    ctx += (
        f"\n(Ranking computed from latest stored snapshot of "
        f"inspection_performance. Cumulative all-time totals; not "
        f"date-range filtered.)\n"
    )
    return {
        "source_id": f"insp_top:{metric}:{level}:{order}",
        "records": rows,
        "formatted_context": ctx,
        "deterministic_answer": ctx,
        "structured_payload": ctx,
        "evidence_type": "ranking",
    }


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

    # ── Date-ranged → LIVE SDEO API CALL ──
    # SDEO summary/KPI endpoints accept tehsilId only. For district and
    # division queries with a date filter we fan out to every tehsil in
    # the hierarchy and aggregate the responses, since the SDEO backend
    # has no district/division-level date-range endpoints.
    if start_date and end_date:
        if level == "tehsil":
            return _query_tehsil_live(db, name, start_date, end_date, source_id)
        if level == "district":
            return _query_district_live(db, name, start_date, end_date, source_id)
        if level == "division":
            return _query_division_live(db, name, start_date, end_date, source_id)

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

    # endDate passed verbatim — see _fetch_tehsil_metrics for the
    # rationale (we mirror the SDEO UI's date-picker convention so
    # chatbot totals match what the user reads off the dashboard).
    api_end_date = end_date

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

    # ── Date-ranged supplements from auxiliary SDEO endpoints ──
    # inspections-summary returns counts but no fine/paid/arrest/PCM. The
    # SDEO dashboard fills these from three sibling endpoints, so we mirror
    # that here for parity with the dashboard UI.
    fine_imposed = fine_recovered = unpaid_fine = None
    paid_count = unpaid_count = None
    arrest_total = pcm_total = None
    try:
        kpi = requests.get(
            SDEO_TOP_KPIS,
            params={"tehsilId": tehsil_id,
                    "startDate": start_date.isoformat(),
                    "endDate": api_end_date.isoformat()},
            headers=_HEADERS, timeout=_API_TIMEOUT,
        ).json()
        if isinstance(kpi, dict):
            fine_imposed = kpi.get("totalFineImposed")
            fine_recovered = kpi.get("totalFineRecovered")
            unpaid_fine = kpi.get("unpaidFineAmount")
    except Exception as e:
        log.debug("top-kpis fetch failed for %s: %s", tehsil_name, e)

    try:
        cs = requests.get(
            SDEO_CHALLAN_STATUS_BREAKDOWN,
            params={"tehsilId": tehsil_id,
                    "startDate": start_date.isoformat(),
                    "endDate": api_end_date.isoformat()},
            headers=_HEADERS, timeout=_API_TIMEOUT,
        ).json()
        if isinstance(cs, dict):
            paid_count = cs.get("paidCount")
            unpaid_count = cs.get("unpaidCount")
    except Exception as e:
        log.debug("challan-status-breakdown fetch failed for %s: %s", tehsil_name, e)

    # PCM/dashboard-counts ignores date filter — returns all-time totals.
    # Surface arrest + PCM with an explicit "all-time" disclosure.
    try:
        pcm_dc = requests.get(
            PCM_DASHBOARD_COUNTS,
            params={"tehsilId": tehsil_id},
            headers=_HEADERS, timeout=_API_TIMEOUT,
        ).json()
        if isinstance(pcm_dc, dict):
            arrest_total = pcm_dc.get("totalArrest")
            pcm_total = pcm_dc.get("totalPCM")
    except Exception as e:
        log.debug("Pcm/dashboard-counts fetch failed for %s: %s", tehsil_name, e)

    if fine_imposed is not None:
        context += f"Fine Amount (imposed): Rs. {int(fine_imposed):,}\n"
    if fine_recovered is not None:
        context += f"Fine Recovered (paid): Rs. {int(fine_recovered):,}\n"
    if unpaid_fine is not None:
        context += f"Fine Outstanding (unpaid): Rs. {int(unpaid_fine):,}\n"
    if paid_count is not None:
        context += f"Paid Challans: {int(paid_count):,}\n"
    if unpaid_count is not None:
        context += f"Unpaid Challans: {int(unpaid_count):,}\n"
    if arrest_total is not None:
        context += f"Arrest Cases (all-time): {int(arrest_total):,}\n"
    if pcm_total is not None:
        context += f"PCM (all-time): {int(pcm_total):,}\n"

    context += "\n(Counts and fines are filtered by the requested date range. "
    context += "Arrest and PCM totals are all-time figures from the PCM dashboard endpoint.)\n"

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
# Live tehsil-fan-out aggregation for district / division
# ══════════════════════════════════════════════════════════════
def _fetch_tehsil_metrics(
    tehsil_id: int, tehsil_name: str,
    start_date: date, end_date: date,
) -> Dict[str, Any]:
    """Hit the four SDEO endpoints for one tehsil and return a flat dict.

    SDEO API treats endDate as exclusive — caller passes the user-requested
    end_date and we add a day before sending. Network failures surface as
    `error` so the aggregator can decide whether to keep going or abort.
    """
    # SDEO endDate is exclusive at midnight: passing 2026-01-10 returns
    # data through Jan 9 23:59:59. The dashboard's date-picker matches
    # user-typed dates verbatim (e.g. "To 01/10/2026 12:00 AM"), so we
    # mirror that — no +1-day shift — to keep chatbot totals identical
    # to what the user reads from the SDEO UI.
    api_end_date = end_date
    params = {
        "tehsilId": tehsil_id,
        "startDate": start_date.isoformat(),
        "endDate": api_end_date.isoformat(),
    }
    out: Dict[str, Any] = {
        "tehsil_id": tehsil_id,
        "tehsil_name": tehsil_name,
        "total_actions": 0, "challans": 0, "firs": 0, "warnings": 0,
        "no_offenses": 0, "sealed": 0, "removal_order": 0, "epo": 0,
        "officers": [],
        "fine_imposed": 0.0, "fine_recovered": 0.0, "unpaid_fine": 0.0,
        "paid_count": 0, "unpaid_count": 0,
        "arrest_total": 0, "pcm_total": 0,
        "error": None,
    }
    try:
        d = requests.get(SDEO_INSPECTIONS_SUMMARY, params=params,
                         headers=_HEADERS, timeout=_API_TIMEOUT).json()
        if isinstance(d, dict):
            out["total_actions"] = int(d.get("totalActions") or 0)
            out["challans"] = int(d.get("challans") or 0)
            out["firs"] = int(d.get("fiRs") or 0)
            out["warnings"] = int(d.get("warnings") or 0)
            out["no_offenses"] = int(d.get("noOffenses") or 0)
            out["sealed"] = int(d.get("sealed") or 0)
            out["removal_order"] = int(d.get("removalOrder") or 0)
            out["epo"] = int(d.get("epo") or 0)
            out["officers"] = d.get("officers") or []
    except Exception as e:
        out["error"] = f"summary:{e}"

    try:
        k = requests.get(SDEO_TOP_KPIS, params=params,
                         headers=_HEADERS, timeout=_API_TIMEOUT).json()
        if isinstance(k, dict):
            out["fine_imposed"] = float(k.get("totalFineImposed") or 0)
            out["fine_recovered"] = float(k.get("totalFineRecovered") or 0)
            out["unpaid_fine"] = float(k.get("unpaidFineAmount") or 0)
    except Exception as e:
        out["error"] = (out["error"] or "") + f" kpi:{e}"

    try:
        cs = requests.get(SDEO_CHALLAN_STATUS_BREAKDOWN, params=params,
                          headers=_HEADERS, timeout=_API_TIMEOUT).json()
        if isinstance(cs, dict):
            out["paid_count"] = int(cs.get("paidCount") or 0)
            out["unpaid_count"] = int(cs.get("unpaidCount") or 0)
    except Exception as e:
        out["error"] = (out["error"] or "") + f" cs:{e}"

    # PCM totals are all-time, not date-filtered. Skip params.
    try:
        pdc = requests.get(PCM_DASHBOARD_COUNTS,
                           params={"tehsilId": tehsil_id},
                           headers=_HEADERS, timeout=_API_TIMEOUT).json()
        if isinstance(pdc, dict):
            out["arrest_total"] = int(pdc.get("totalArrest") or 0)
            out["pcm_total"] = int(pdc.get("totalPCM") or 0)
    except Exception as e:
        out["error"] = (out["error"] or "") + f" pcm:{e}"

    return out


def _aggregate_tehsil_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sum tehsil-level metrics into a single roll-up. Officer rows with the
    same officerName collapse into one record with summed counts so the
    output looks like a unified district/division-level breakdown.
    """
    agg = {
        "total_actions": 0, "challans": 0, "firs": 0, "warnings": 0,
        "no_offenses": 0, "sealed": 0, "removal_order": 0, "epo": 0,
        "fine_imposed": 0.0, "fine_recovered": 0.0, "unpaid_fine": 0.0,
        "paid_count": 0, "unpaid_count": 0,
        "arrest_total": 0, "pcm_total": 0,
        "tehsils_covered": 0, "tehsils_failed": 0,
        "tehsils_with_data": [],
    }
    officer_acc: Dict[str, Dict[str, int]] = {}
    for r in rows:
        if r.get("error"):
            agg["tehsils_failed"] += 1
            continue
        agg["tehsils_covered"] += 1
        for k in ("total_actions", "challans", "firs", "warnings",
                  "no_offenses", "sealed", "removal_order", "epo",
                  "paid_count", "unpaid_count",
                  "arrest_total", "pcm_total"):
            agg[k] += int(r.get(k) or 0)
        for k in ("fine_imposed", "fine_recovered", "unpaid_fine"):
            agg[k] += float(r.get(k) or 0)
        if (r.get("total_actions") or 0) > 0:
            agg["tehsils_with_data"].append(r["tehsil_name"])
        for o in (r.get("officers") or []):
            name = (o.get("officerName") or "Unknown").strip()
            slot = officer_acc.setdefault(name, {
                "officerName": name, "inspection": 0, "challan": 0,
                "fir": 0, "warning": 0, "noOffense": 0, "sealed": 0,
                "removalOrder": 0, "epo": 0,
            })
            for k in ("inspection", "challan", "fir", "warning",
                     "noOffense", "sealed", "removalOrder", "epo"):
                slot[k] += int(o.get(k) or 0)
    agg["officers"] = sorted(officer_acc.values(),
                             key=lambda x: -x["inspection"])
    # Stash raw rows so the formatter can render an audit trail without
    # re-fetching. Prefixed with `_` to signal "internal".
    agg["_rows"] = rows
    return agg


def _format_aggregate_context(
    level: str, name: str, agg: Dict[str, Any],
    start_date: date, end_date: date,
    tehsil_count: int,
) -> str:
    ctx = f"Inspection Performance — {level.title()}: {name}\n"
    ctx += f"Date Range: {start_date} to {end_date}\n"
    ctx += f"Aggregation: sum of {agg['tehsils_covered']}/{tehsil_count} tehsils"
    if agg["tehsils_failed"]:
        ctx += f" ({agg['tehsils_failed']} tehsils errored)"
    ctx += "\n" + "=" * 50 + "\n"

    # Audit-trail FIRST so the LLM can't drop it during summarisation.
    # Higher-authority users repeatedly cross-check chatbot totals against
    # dashboard tiles per tehsil — without this table they have no way to
    # spot which tehsil disagrees.
    per_tehsil = sorted(
        list(agg.get("_rows", []) or []),
        key=lambda r: -(r.get("total_actions") or 0),
    )
    if per_tehsil:
        ctx += "Per-Tehsil Breakdown (REQUIRED in answer):\n"
        ctx += f"{'Tehsil':<25s} {'Insp':>6s} {'Chal':>5s} {'War':>4s} {'Sld':>3s} {'Fine(Rs)':>10s}\n"
        for r in per_tehsil:
            tag = " [ERR]" if r.get("error") else ""
            ctx += (
                f"{r['tehsil_name']:<25s} "
                f"{r['total_actions']:>6,} "
                f"{r['challans']:>5,} "
                f"{r['warnings']:>4,} "
                f"{r['sealed']:>3,} "
                f"{int(r['fine_imposed']):>10,}{tag}\n"
            )
        ctx += "-" * 50 + "\n"

    ctx += f"Total Inspections/Actions: {agg['total_actions']:,}\n"
    ctx += f"Challans: {agg['challans']:,}\n"
    ctx += f"FIRs: {agg['firs']:,}\n"
    ctx += f"Warnings: {agg['warnings']:,}\n"
    ctx += f"No Offenses: {agg['no_offenses']:,}\n"
    ctx += f"Sealed: {agg['sealed']:,}\n"
    if agg["removal_order"]:
        ctx += f"Removal Orders: {agg['removal_order']:,}\n"
    if agg["epo"]:
        ctx += f"EPO: {agg['epo']:,}\n"

    ctx += f"Fine Amount (imposed): Rs. {int(agg['fine_imposed']):,}\n"
    ctx += f"Fine Recovered (paid): Rs. {int(agg['fine_recovered']):,}\n"
    ctx += f"Fine Outstanding (unpaid): Rs. {int(agg['unpaid_fine']):,}\n"
    ctx += f"Paid Challans: {agg['paid_count']:,}\n"
    ctx += f"Unpaid Challans: {agg['unpaid_count']:,}\n"
    ctx += f"Arrest Cases (all-time): {agg['arrest_total']:,}\n"
    ctx += f"PCM (all-time): {agg['pcm_total']:,}\n"

    if agg["officers"]:
        ctx += f"\nOfficer Breakdown ({len(agg['officers'])} officers, summed across tehsils):\n"
        for o in agg["officers"][:25]:
            parts = [
                f"{o['inspection']:,} inspections",
                f"{o['challan']:,} challans",
                f"{o['warning']:,} warnings",
            ]
            if o["fir"]:
                parts.append(f"{o['fir']} FIRs")
            if o["sealed"]:
                parts.append(f"{o['sealed']} sealed")
            if o["removalOrder"]:
                parts.append(f"{o['removalOrder']} removal orders")
            if o["epo"]:
                parts.append(f"{o['epo']} EPOs")
            ctx += f"  {o['officerName']}: {', '.join(parts)}\n"


    ctx += (
        "\n(Counts and fines aggregated live from per-tehsil SDEO endpoints "
        "for the requested date range. Arrest and PCM totals are all-time "
        "figures from the PCM dashboard endpoint.)\n"
    )
    return ctx


def _query_district_live(
    db, district_name: str, start_date: date, end_date: date, source_id: str,
) -> Dict[str, Any]:
    """Aggregate inspection metrics for a district by fanning out to tehsils."""
    tehsils = db.fetch_all(
        "SELECT t.tehsil_id, t.tehsil_name FROM dim_tehsil t "
        "JOIN dim_district d ON t.district_id = d.district_id "
        "WHERE d.district_name = %s AND COALESCE(t.is_active, true) = true",
        (district_name,),
    )
    if not tehsils:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": (
                f"District '{district_name}' has no tehsils registered in the "
                "hierarchy table.\n"
            ),
        }
    return _fanout_aggregate("district", district_name, tehsils,
                             start_date, end_date, source_id)


def _query_division_live(
    db, division_name: str, start_date: date, end_date: date, source_id: str,
) -> Dict[str, Any]:
    """Aggregate inspection metrics for a division (all tehsils under it)."""
    tehsils = db.fetch_all(
        "SELECT t.tehsil_id, t.tehsil_name FROM dim_tehsil t "
        "JOIN dim_district d ON t.district_id = d.district_id "
        "JOIN dim_division dv ON d.division_id = dv.division_id "
        "WHERE dv.division_name = %s AND COALESCE(t.is_active, true) = true",
        (division_name,),
    )
    if not tehsils:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": (
                f"Division '{division_name}' has no tehsils registered in the "
                "hierarchy table.\n"
            ),
        }
    return _fanout_aggregate("division", division_name, tehsils,
                             start_date, end_date, source_id)


def _fanout_aggregate(
    level: str, name: str, tehsils: List[Dict[str, Any]],
    start_date: date, end_date: date, source_id: str,
) -> Dict[str, Any]:
    """Parallel-fetch per-tehsil metrics, aggregate, and format context."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    rows: List[Dict[str, Any]] = []
    # Cap concurrency so we don't hammer the upstream API.
    workers = min(8, len(tehsils))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [
            ex.submit(_fetch_tehsil_metrics,
                      t["tehsil_id"], t["tehsil_name"],
                      start_date, end_date)
            for t in tehsils if t.get("tehsil_id")
        ]
        for f in as_completed(futs):
            try:
                rows.append(f.result())
            except Exception as e:
                log.warning("Tehsil metric fetch raised: %s", e)
    agg = _aggregate_tehsil_metrics(rows)
    ctx = _format_aggregate_context(level, name, agg,
                                    start_date, end_date, len(tehsils))
    try:
        from freshness_helper import format_freshness_footer
        ctx += format_freshness_footer(
            "SDEO Inspections API",
            snapshot_date=datetime.now(),
            source_label="SDEO Live API (aggregated)",
        ) + "\n"
    except Exception:
        pass
    return {
        "source_id": source_id,
        "records": rows,
        "formatted_context": ctx,
        "deterministic_answer": ctx,
        "structured_payload": ctx,
        "evidence_type": "aggregate",
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
