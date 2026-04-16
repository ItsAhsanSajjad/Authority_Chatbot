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
from datetime import date, timedelta
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
DG_INSP_DIVISIONS = (
    "https://pera360.punjab.gov.pk/backend/api/dg-dashboard/inspection-performance/divisions"
)
DG_INSP_DISTRICTS = (
    "https://pera360.punjab.gov.pk/backend/api/dg-dashboard/inspection-performance/districts"
)
DG_INSP_TEHSILS = (
    "https://pera360.punjab.gov.pk/backend/api/dg-dashboard/inspection-performance/tehsils"
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
    # FIR / sealed / warning (inspection-only actions)
    r"firs?|"
    r"sealed|sealing|"
    r"warnings?|"
    r"no\s+offen[cs]e|"
    # "station" in PERA context = SDEO inspection station
    r"stations?|"
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


def _normalize_place(name: str) -> str:
    """Normalize a place name for fuzzy matching.
    Handles common Pakistani place-name spelling variants:
    jehlam/jhelum, faisalabad/faislaabad, lahore/lahor, etc.
    """
    s = name.lower().strip()
    # Collapse double letters
    s = re.sub(r"(.)\1+", r"\1", s)
    # Normalize vowel clusters and common swaps
    s = s.replace("ei", "e").replace("ai", "a").replace("ee", "i")
    s = s.replace("oo", "u").replace("ou", "u")
    # Remove silent h in common positions (jh→j, kh→k, gh→g, etc.)
    # But keep 'h' at start of word
    s = re.sub(r"(?<!^)(?<!\s)h", "", s)
    # Normalize 'a'/'e' at end (lahore/lahor, jehlam/jhelum)
    s = re.sub(r"[ae]$", "", s)
    # Remove spaces for multi-word comparison
    s = s.replace(" ", "")
    return s


def _normalize_place_consonants(name: str) -> str:
    """Even more aggressive normalization: strip ALL vowels to match
    consonant skeleton.  jehlam→jlm, jhelum→jlm.
    """
    s = _normalize_place(name)
    return re.sub(r"[aeiou]", "", s)


def _fuzzy_match_location(
    q_lower: str, names: List[str], level: str,
) -> Optional[Dict[str, str]]:
    """Try fuzzy matching of location names against question words."""
    # Extract candidate words from question (2+ chars)
    q_words = re.findall(r"[a-z]{2,}", q_lower)
    # Also try bigrams for multi-word names like "dera ghazi khan"
    q_bigrams = [f"{q_words[i]}{q_words[i+1]}" for i in range(len(q_words)-1)]

    for name in (names or []):
        norm_name = _normalize_place(name)
        cons_name = _normalize_place_consonants(name)
        if len(norm_name) < 3:
            continue
        for w in q_words:
            norm_w = _normalize_place(w)
            if len(norm_w) < 3:
                continue
            # Exact normalized match
            if norm_w == norm_name:
                return {"level": level, f"{level}_name": name}
            # Consonant skeleton match (jehlam→jlm == jhelum→jlm)
            cons_w = _normalize_place_consonants(w)
            if len(cons_w) >= 3 and len(cons_name) >= 3 and cons_w == cons_name:
                log.info("Consonant-skeleton match: '%s' → '%s' (level=%s)", w, name, level)
                return {"level": level, f"{level}_name": name}
            # One is a prefix of other (≥4 chars matched)
            min_len = min(len(norm_w), len(norm_name))
            if min_len >= 4 and (norm_w.startswith(norm_name[:4]) or norm_name.startswith(norm_w[:4])):
                # Require at least 70% overlap
                common = sum(1 for a, b in zip(norm_w, norm_name) if a == b)
                if common / max(len(norm_w), len(norm_name)) >= 0.65:
                    log.info("Fuzzy location match: '%s' → '%s' (level=%s)", w, name, level)
                    return {"level": level, f"{level}_name": name}
        # Multi-word name match via bigrams
        if " " in name:
            for bg in q_bigrams:
                norm_bg = _normalize_place(bg)
                if norm_bg == norm_name or (
                    len(norm_bg) >= 5 and len(norm_name) >= 5
                    and norm_bg[:5] == norm_name[:5]
                ):
                    log.info("Fuzzy bigram match: '%s' → '%s' (level=%s)", bg, name, level)
                    return {"level": level, f"{level}_name": name}
    return None


def _detect_location(question: str) -> Optional[Dict[str, str]]:
    """Return {'level': ..., '<level>_name': ...} or None."""
    _load_location_cache()
    q_lower = question.lower()

    # If user explicitly says "division" / "district" / "tehsil" / "station", respect that
    explicit_div = re.search(r"\bdivisions?\b", q_lower)
    explicit_dist = re.search(r"\bdistricts?\b", q_lower)
    explicit_teh = re.search(r"\btehsils?\b|\bstations?\b|\bcity\b|\bsadar\b|\bcantt?\b", q_lower)

    # ── When user specifies an explicit level (station/tehsil/district/division),
    # search ONLY that level (exact + fuzzy) before trying others ──
    if explicit_teh:
        for t in (_TEHSIL_NAMES or []):
            if t.lower() in q_lower:
                return {"level": "tehsil", "tehsil_name": t}
        # Fuzzy match within tehsil level
        m = _fuzzy_match_location(q_lower, _TEHSIL_NAMES, "tehsil")
        if m:
            return m
    if explicit_div:
        for dv in (_DIVISION_NAMES or []):
            if dv.lower() in q_lower:
                return {"level": "division", "division_name": dv}
        m = _fuzzy_match_location(q_lower, _DIVISION_NAMES, "division")
        if m:
            return m
    if explicit_dist:
        for d in (_DISTRICT_NAMES or []):
            if d.lower() in q_lower:
                return {"level": "district", "district_name": d}
        m = _fuzzy_match_location(q_lower, _DISTRICT_NAMES, "district")
        if m:
            return m

    # If explicit level was given but no match found at that level,
    # DON'T fall through to other levels — return None so disambiguation
    # or summary can handle it
    if explicit_teh or explicit_div or explicit_dist:
        # Last resort: try all levels (user may have said "station" but
        # the name only exists as district)
        pass

    # No explicit level — default: tehsil > district > division (exact first)
    for t in (_TEHSIL_NAMES or []):
        if t.lower() in q_lower:
            return {"level": "tehsil", "tehsil_name": t}
    for d in (_DISTRICT_NAMES or []):
        if d.lower() in q_lower:
            return {"level": "district", "district_name": d}
    for dv in (_DIVISION_NAMES or []):
        if dv.lower() in q_lower:
            return {"level": "division", "division_name": dv}

    # No explicit level — fuzzy: tehsil > district > division
    m = _fuzzy_match_location(q_lower, _TEHSIL_NAMES, "tehsil")
    if m:
        return m
    m = _fuzzy_match_location(q_lower, _DISTRICT_NAMES, "district")
    if m:
        return m
    m = _fuzzy_match_location(q_lower, _DIVISION_NAMES, "division")
    if m:
        return m
    return None


# ── Officer name cache (from officer_inspection_record + officer_inspection_detail) ──
_insp_officer_cache: Optional[List[str]] = None


def _load_insp_officer_cache() -> List[str]:
    global _insp_officer_cache
    # Only return cached result if it was successfully populated (non-empty).
    # If it was empty because the DB was unavailable, retry on next call.
    if _insp_officer_cache is not None and len(_insp_officer_cache) > 0:
        return _insp_officer_cache
    db = _get_db()
    if not db:
        return []  # Don't cache — DB might be available next time
    try:
        rows = db.fetch_all(
            "SELECT DISTINCT officer_name FROM officer_inspection_detail "
            "WHERE officer_name IS NOT NULL AND officer_name != '' "
            "UNION "
            "SELECT DISTINCT officer_name FROM officer_inspection_record "
            "WHERE officer_name IS NOT NULL AND officer_name != ''"
        )
        _insp_officer_cache = [r["officer_name"] for r in rows]
        log.info("Officer cache loaded: %d officers", len(_insp_officer_cache))
    except Exception as e:
        log.warning("Failed to load inspection officer cache: %s", e)
        return []  # Don't cache failures — retry next time
    return _insp_officer_cache


def _detect_insp_officer_name(question: str) -> Optional[str]:
    """Fuzzy-match officer name from the inspection tables."""
    q_lower = question.lower()
    officers = _load_insp_officer_cache()
    if not officers:
        return None

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

    return best_match


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

    # Must have inspection keyword (for non-CNIC queries).
    # Exception: officer ranking queries ("sab sy ziada challan kis officer ne kiye")
    # are about inspection officers even when the metric is "challan" (SDEO officer
    # data includes challan/FIR/warning breakdown per officer).
    _EARLY_OFFICER_RANK_RE = re.compile(
        r"\b(?:sab\s*s[ey]\s*(?:ziada|zyada|ziyada)|most|highest|maximum|lowest|least|kam|more|top|best|worst)\b",
        re.I,
    )
    _EARLY_OFFICER_LEVEL_RE = re.compile(r"\bofficers?\b", re.I)
    _EARLY_WHICH_OFFICER_RE = re.compile(
        r"\b(?:which|kis|kaun|kon|konsa|kon\s+sa|wo)\s+officers?\b", re.I
    )
    _EARLY_METRIC_RE = re.compile(
        r"\b(firs?|challans?|warnings?|sealed|sealing|"
        r"no\s*offen[cs]e|inspect(?:ion)?s?|actions?)\b", re.I
    )
    _is_officer_ranking = bool(
        (_EARLY_OFFICER_RANK_RE.search(q) and _EARLY_OFFICER_LEVEL_RE.search(q))
        or (_EARLY_WHICH_OFFICER_RE.search(q) and _EARLY_METRIC_RE.search(q))
    )
    # Also bypass keyword check when a known officer name is detected with
    # a metric word — "how many challans Muhammad Imran Haider imposed" is
    # about SDEO officer data even though "challans" isn't an inspection keyword.
    _is_named_officer_query = bool(officer_name and _EARLY_METRIC_RE.search(q))
    if not _INSP_KEYWORDS.search(q) and not _is_officer_ranking and not _is_named_officer_query:
        return None

    # Guard: if query is primarily about CHALLANS (not inspections), the "station"
    # keyword alone should NOT trigger inspection routing.
    # e.g. "top 10 station challans summery" → challan lookup, not inspection
    _CHALLAN_PRIMARY_RE = re.compile(
        r"\b(challan(?:s|z)?|jrimana|jarimana|jurmana)\b", re.I
    )
    _INSP_SPECIFIC_RE = re.compile(
        r"\b(inspect(?:ion)?s?|firs?|sealed|sealing|warnings?|no\s+offen[cs]e|"
        r"muayina|mu[aā]yin[ae]|jaiz[ae]|checking)\b", re.I
    )
    # But if the query is a ranking/comparison query about officers, keep it in
    # inspection domain — "sab sy ziada challan kis officer ne kiye" is asking
    # which officer imposed the most challans (inspection officer data, not
    # the challan payment database).
    _OFFICER_RANKING_GUARD = re.compile(
        r"\b(?:sab\s*s[ey]\s*(?:ziada|zyada|ziyada)|most|highest|maximum|lowest|least|kam)\b"
        r".*\bofficers?\b"
        r"|\bofficers?\b.*\b(?:sab\s*s[ey]\s*(?:ziada|zyada|ziyada)|most|highest|maximum|lowest|least|kam)\b",
        re.I,
    )
    if _CHALLAN_PRIMARY_RE.search(q) and not _INSP_SPECIFIC_RE.search(q) and not _OFFICER_RANKING_GUARD.search(q) and not _is_named_officer_query:
        log.info("Challan-primary query with 'station' — deferring to challan lookup: %s", q[:80])
        return None

    # Repeat offender / frequent CNIC detection
    # "kis id card ka name again and again aya", "repeat offender", "baar baar",
    # "sabse zyada baar", "most inspected", "frequent"
    _REPEAT_RE = re.compile(
        r"\b("
        r"again\s+and\s+again|baar\s+baar|bar\s+bar|"
        r"repeat(?:ed)?(?:ly)?|frequent(?:ly)?|"
        r"most\s+(?:inspect|challan)|"
        r"kitni\s+(?:dafa|baar|bar|martaba)|"
        r"(?:id\s+card|cnic).*(?:again|baar|bar|repeat|frequent)"
        r")\b",
        re.I,
    )
    # "sab se ziada" only means repeat offenders when combined with
    # CNIC/id card context, NOT when asking about FIR/warning/sealed counts
    _SABSE_ZIADA_RE = re.compile(
        r"\bsab\s*s[ey]\s*(?:ziada|zyada|ziyada)\b", re.I
    )
    _METRIC_WORDS_RE = re.compile(
        r"\b(firs?|warnings?|sealed|sealing|challans?|no\s*offen[cs]e|"
        r"kis\s+tehsil|kis\s+district|kis\s+division|kis\s+officer|kis\s+station)\b", re.I
    )
    if _REPEAT_RE.search(q):
        return "insp_repeat_offenders"
    if _SABSE_ZIADA_RE.search(q) and not _METRIC_WORDS_RE.search(q):
        return "insp_repeat_offenders"

    # Officer + inspection keyword → officer lookup
    if officer_name:
        return f"insp_officer:{officer_name}"

    # ── Ranking / comparison queries ──
    # "sab sy ziada FIR kis tehsil/station/district/division mein hoi hain?"
    # "which tehsil has the most FIRs?"
    # "which officer issues more FIR in Punjab?"
    _RANKING_RE = re.compile(
        r"\b(?:sab\s*s[ey]\s*(?:ziada|zyada|ziyada)|most|highest|maximum|"
        r"lowest|least|kam|more|top|best|worst)\b",
        re.I,
    )
    _RANK_LEVEL_RE = re.compile(
        r"\b(?:kis\s+)?(stations?|tehsils?|districts?|divisions?|officers?)\b", re.I
    )
    _RANK_METRIC_RE = re.compile(
        r"\b(firs?|challans?|warnings?|sealed|sealing|"
        r"no\s*offen[cs]e|inspect(?:ion)?s?|actions?)\b", re.I
    )

    def _extract_metric(q_text):
        """Extract metric from query text, returns metric key."""
        mm = _RANK_METRIC_RE.search(q_text)
        if not mm:
            return "total_actions"
        mw = mm.group(1).lower()
        if "fir" in mw:
            return "firs"
        elif "challan" in mw:
            return "challans"
        elif "warning" in mw:
            return "warnings"
        elif "seal" in mw:
            return "sealed"
        elif "offen" in mw:
            return "no_offenses"
        return "total_actions"

    def _build_officer_ranking_intent(q_text):
        """Build insp_officer_ranking intent with optional location."""
        metric = _extract_metric(q_text)
        loc = _detect_location(q_text)
        if loc:
            lvl = loc["level"]
            nm = loc.get(f"{lvl}_name", "")
            return f"insp_officer_ranking:{metric}:{lvl}:{nm}"
        return f"insp_officer_ranking:{metric}"

    # "which officer" / "kis officer" / "kon sa officer" + metric keyword
    # catches: "which officer issues more FIR", "kis officer ne FIR ki",
    # "tell me which officer has the most challans"
    _WHICH_OFFICER_RE = re.compile(
        r"\b(?:which|kis|kaun|kon|konsa|kon\s+sa|wo)\s+officers?\b", re.I
    )
    if _WHICH_OFFICER_RE.search(q) and _RANK_METRIC_RE.search(q):
        return _build_officer_ranking_intent(q)

    if _RANKING_RE.search(q):
        level_m = _RANK_LEVEL_RE.search(q)
        metric_m = _RANK_METRIC_RE.search(q)
        if level_m:
            raw_level = level_m.group(1).lower().rstrip("s")
            if raw_level == "station":
                raw_level = "tehsil"
            metric = _extract_metric(q)
            # Officer-level ranking: "kis officer ne sab sy ziada FIR ki?"
            if raw_level == "officer":
                return _build_officer_ranking_intent(q)
            return f"insp_ranking:{raw_level}:{metric}"

    # Location detection
    location = _detect_location(q)
    if location:
        level = location["level"]
        name = location.get(f"{level}_name", "")

        # Check for ambiguity: if user typed a name that matches multiple
        # levels (e.g. "Bahawalpur" = division + district + tehsil City/Sadar)
        # and didn't specify which level → ask for clarification.
        # Also include ALL child tehsils for matching divisions/districts.
        q_lower_for_level = q.lower()
        explicit_div = bool(re.search(r"\bdivisions?\b", q_lower_for_level))
        explicit_dist = bool(re.search(r"\bdistricts?\b", q_lower_for_level))
        explicit_teh = bool(re.search(r"\btehsils?\b|\bstations?\b|\bcity\b|\bsadar\b|\bcantt?\b", q_lower_for_level))
        if not explicit_div and not explicit_dist and not explicit_teh:
            _load_location_cache()
            q_lower_check = q.lower()

            # Count how many LEVELS the name matches at (before child expansion)
            matched_div = None
            matched_dist = None
            matched_teh = None
            level_matches = []  # (level, name) tuples — one per level

            for dv in (_DIVISION_NAMES or []):
                if dv.lower() == name.lower() or dv.lower() in q_lower_check:
                    matched_div = dv
                    level_matches.append(("division", dv))
                    break
            for d in (_DISTRICT_NAMES or []):
                if d.lower() == name.lower() or d.lower() in q_lower_check:
                    matched_dist = d
                    level_matches.append(("district", d))
                    break
            for t in (_TEHSIL_NAMES or []):
                if t.lower() == name.lower() or t.lower() in q_lower_check:
                    matched_teh = t
                    level_matches.append(("tehsil", t))
                    break

            # Only disambiguate when 2+ DIFFERENT levels matched naturally
            # (e.g. "Lahore" = division + district + tehsil,
            #  "Bahawalpur" = division + district)
            # Don't disambiguate when only 1 level matched (e.g. "Jehlum" = district only)
            if len(level_matches) > 1:
                matches = list(level_matches)
                # Add ALL child tehsils for matched division/district
                db = _get_db()
                if db and (matched_div or matched_dist):
                    child_tehsils = []
                    if matched_div:
                        rows = db.fetch_all(
                            "SELECT DISTINCT t.tehsil_name, d.district_name "
                            "FROM dim_tehsil t "
                            "JOIN dim_district d ON t.district_id = d.district_id "
                            "JOIN dim_division dv ON d.division_id = dv.division_id "
                            "WHERE LOWER(dv.division_name) = LOWER(%s) "
                            "ORDER BY d.district_name, t.tehsil_name",
                            (matched_div,),
                        )
                        child_tehsils = rows
                    elif matched_dist:
                        rows = db.fetch_all(
                            "SELECT DISTINCT t.tehsil_name, d.district_name "
                            "FROM dim_tehsil t "
                            "JOIN dim_district d ON t.district_id = d.district_id "
                            "WHERE LOWER(d.district_name) = LOWER(%s) "
                            "ORDER BY t.tehsil_name",
                            (matched_dist,),
                        )
                        child_tehsils = rows
                    # Add child tehsils to matches (avoiding duplicates)
                    existing = {n for _, n in matches}
                    for row in child_tehsils:
                        tn = row["tehsil_name"]
                        if tn not in existing:
                            matches.append(("tehsil", tn))
                            existing.add(tn)

                # Deduplicate and return disambiguation
                unique_matches = list(dict.fromkeys((l, n) for l, n in matches))
                if len(unique_matches) > 1:
                    options_str = "|".join(f"{l}:{n}" for l, n in unique_matches)
                    return f"insp_disambiguate:{options_str}"

        if level == "division":
            return f"insp_division:{name}"
        elif level == "district":
            return f"insp_district:{name}"
        elif level == "tehsil":
            return f"insp_tehsil:{name}"

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
        # Guard: if the query is primarily about CHALLANS, don't hijack to inspection
        # e.g. "last week challan summery of shalimar station" → challan, not inspection
        _CHALLAN_PRI = re.compile(r"\b(challan(?:s|z)?|jrimana|jarimana|jurmana)\b", re.I)
        _INSP_SPECIFIC = re.compile(
            r"\b(inspect(?:ion)?s?|firs?|sealed|sealing|warnings?|no\s+offen[cs]e|"
            r"muayina|mu[aā]yin[ae]|jaiz[ae]|checking)\b", re.I)
        if _CHALLAN_PRI.search(q_lower) and not _INSP_SPECIFIC.search(q_lower):
            return None  # stays in challan domain
        if _INSP_KEYWORDS.search(q_lower):
            # If the current query has its OWN location, treat as a fresh
            # inspection query — let detect_inspection_intent handle it.
            # This avoids follow-up date prepending and keeps the query clean.
            # Cross-domain follow-up only helps for vague queries like
            # "or total inspection kitni hoi?" (no location, needs prev context).
            cur_loc = _detect_location(question)
            if cur_loc:
                # Has own location → fresh query, not follow-up
                return None

            # No location in current query — try previous context
            loc = _extract_location_from_intent(last_lookup_type)
            if loc:
                level, name = loc
                return f"insp_{level}:{name}"
            # If previous query had an officer
            if ":oa_officer:" in last_lookup_type or last_lookup_type.startswith("oa_officer:"):
                officer = last_lookup_type.split("oa_officer:")[-1]
                if officer:
                    return f"insp_officer:{officer}"
            # No location anywhere — treat as fresh intent
            return None
        return None

    # ── Same-domain: insp → insp follow-up ──
    if not last_lookup_type.startswith("insp_"):
        return None

    # If previous query was an officer ranking, follow-ups asking about
    # officers / names should re-execute the same ranking query so the
    # LLM has the full officer list in context.
    # e.g. prev = insp_officer_ranking:firs
    #      followup = "tell me the name of that officer who issues these firs"
    if last_lookup_type.startswith("insp_officer_ranking"):
        _OFFICER_NAME_FOLLOWUP_RE = re.compile(
            r"\b(officer|name|who|kis\s+ne|kaun|kon|list|detail|batao|btao|btaye)\b", re.I
        )
        if _OFFICER_NAME_FOLLOWUP_RE.search(q_lower):
            log.info("Officer ranking follow-up — re-using prev intent: %s", last_lookup_type)
            return last_lookup_type

    # Officer follow-up: "which officer imposed those challans",
    # "kis officer ne challans lagaye", "officer breakdown"
    # Must be checked BEFORE the challan guard (since it mentions "challans")
    _OFFICER_FOLLOWUP_RE = re.compile(
        r"\b((?:which|kis|kaun|kon)\s+officer|"
        r"officer.*(?:challan|impose|lagay|breakdown|detail)|"
        r"(?:challan|impose|lagay).*officer|"
        r"officer\s+(?:wise|breakdown|detail|performance|summary)|"
        r"officers?\s+(?:ne|who|list)|"
        r"(?:how\s+many|kitni|kitne|total).*officers?|"
        r"officers?\s+(?:did|kiye?|kia|perform|conduct|inspect))\b", re.I
    )
    if _OFFICER_FOLLOWUP_RE.search(q_lower):
        # Guard: don't use previous follow-up context if user specifies a
        # DIFFERENT / BROADER scope (e.g. "all over punjab", "overall", a
        # specific new location).  Treat as a fresh query instead.
        _BROAD_SCOPE_RE = re.compile(
            r"\b(all\s+over|whole\s+punjab|overall\s+punjab|province|"
            r"punjab\s*(?:wide|level|bhar)?|sar[ie]?\s+punjab|"
            r"pur[aie]?\s+punjab|tamam)\b", re.I
        )
        cur_loc = _detect_location(question)
        prev_loc = _extract_location_from_intent(last_lookup_type)

        if _BROAD_SCOPE_RE.search(q_lower):
            # Province-wide query → fresh intent, not follow-up
            log.info("Officer query with broad scope — treating as fresh: %s", q_lower[:80])
            return None

        if cur_loc and prev_loc:
            cur_name = cur_loc.get(f"{cur_loc['level']}_name", "").lower()
            prev_name = prev_loc[1].lower()
            if cur_name and prev_name and cur_name != prev_name:
                # User specified a different location → use CURRENT location
                lvl = cur_loc["level"]
                nm = cur_loc.get(f"{lvl}_name", "")
                log.info("Officer query with new location '%s' (prev '%s') — using current",
                         nm, prev_name)
                return f"insp_officers:{lvl}:{nm}"

        # If current query has its own location, always prefer it
        if cur_loc:
            lvl = cur_loc["level"]
            nm = cur_loc.get(f"{lvl}_name", "")
            if nm:
                log.info("Officer query with location '%s' from current question", nm)
                return f"insp_officers:{lvl}:{nm}"

        # No location in current query → use previous context
        if prev_loc:
            level, name = prev_loc
            log.info("Officer follow-up: insp_officers:%s (from %s)", name, last_lookup_type)
            return f"insp_officers:{level}:{name}"
        return "insp_officers:summary"

    # If current question has inspection keywords, treat as fresh intent
    if _INSP_KEYWORDS.search(q_lower):
        return None

    # If the question mentions a known officer name from SDEO data and the
    # previous query was officer-related, treat as officer follow-up.
    # e.g. prev = insp_officers:tehsil:Multan City
    #      q = "how many challans Muhammad Imran Haider imposed till now"
    if last_lookup_type.startswith("insp_officer"):
        named_officer = _detect_insp_officer_name(question)
        if named_officer:
            log.info("Officer name follow-up: '%s' after %s", named_officer, last_lookup_type)
            return f"insp_officer:{named_officer}"

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

    # Amount/fine follow-ups should cross-route to challan lookup
    # (SDEO inspection API has no fine amount data, but challan_data does)
    _amount_re = re.compile(
        r"\b(amount|amout|raqam|paisa|paisay|money|penalty|"
        r"kitni\s+raqam|kitna\s+(?:paisa|amount)|total\s+(?:amount|fine|raqam)|"
        r"in\s+amount)\b", re.I
    )
    if _amount_re.search(q_lower):
        loc = _extract_location_from_intent(last_lookup_type)
        if loc:
            level, name = loc
            log.info("Amount follow-up: cross-routing insp→challan for %s:%s", level, name)
            return f"challan_location:{level}:{name}"
        return None  # Can't cross-route without location

    # Follow-up patterns: asking for details/breakdown from previous inspection query
    followup_re = re.compile(
        r"\b(warning|fine|fines|arrest|sealed|"
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
    """Extract (level, name) from any intent string.
    E.g. 'insp_tehsil:Shalimar' -> ('tehsil', 'Shalimar')
         'challan_location:tehsil:Shalimar' -> ('tehsil', 'Shalimar')
         'challan_daterange:...:challan_location:tehsil:Shalimar' -> ('tehsil', 'Shalimar')
         'oa_tehsil:Shalimar' -> ('tehsil', 'Shalimar')
         'oa_division:Lahore' -> ('division', 'Lahore')
    """
    import re as _re

    # Inspection intents: insp_tehsil:Name, insp_district:Name, insp_division:Name
    m = _re.search(r"insp_(tehsil|district|division):(.+?)(?:$|:)", intent)
    if m:
        return (m.group(1), m.group(2))

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

    if base == "insp_disambiguate":
        # Multiple locations match — ask user to clarify
        # Format: insp_disambiguate:level1:name1|level2:name2|...
        options_str = ":".join(parts[1:])
        options = options_str.split("|")

        # Separate into divisions, districts, and tehsils
        divisions = []
        districts = []
        tehsils = []
        for opt in options:
            opt_parts = opt.split(":", 1)
            if len(opt_parts) == 2:
                lvl, nm = opt_parts
                if lvl == "division":
                    divisions.append(nm)
                elif lvl == "district":
                    districts.append(nm)
                elif lvl == "tehsil":
                    tehsils.append(nm)

        context = "Multiple locations match your query. Please specify which one:\n\n"
        idx = 1
        if divisions:
            for dv in divisions:
                context += f"{idx}. {dv} (Entire Division — all districts & stations)\n"
                idx += 1
        if districts:
            for d in districts:
                context += f"{idx}. {d} (District)\n"
                idx += 1
        if tehsils:
            context += "\nStations/Tehsils:\n"
            # Group tehsils by district if possible
            if db:
                for t in tehsils:
                    context += f"{idx}. {t}\n"
                    idx += 1
            else:
                for t in tehsils:
                    context += f"{idx}. {t}\n"
                    idx += 1

        context += "\nPlease ask again specifying the exact location, e.g.:\n"
        if tehsils:
            context += f'  • "{tehsils[0]} inspections summary"\n'
        if districts:
            context += f'  • "{districts[0]} district inspections"\n'
        if divisions:
            context += f'  • "{divisions[0]} division inspections"\n'

        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": context,
        }

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
    elif base == "insp_ranking":
        # insp_ranking:level:metric  e.g. insp_ranking:tehsil:firs
        rank_level = parts[1] if len(parts) > 1 else "division"
        rank_metric = parts[2] if len(parts) > 2 else "total_actions"
        return _query_insp_ranking(db, rank_level, rank_metric)
    elif base == "insp_officer_ranking":
        # insp_officer_ranking:metric  or  insp_officer_ranking:metric:level:name
        rank_metric = parts[1] if len(parts) > 1 else "firs"
        loc_level = parts[2] if len(parts) > 2 else None
        loc_name = ":".join(parts[3:]) if len(parts) > 3 else None
        return _query_officer_ranking(db, rank_metric, start_date, end_date,
                                       loc_level, loc_name)
    elif base == "insp_officers":
        # insp_officers:level:name  e.g. insp_officers:tehsil:Jhelum
        level = parts[1] if len(parts) > 1 else "tehsil"
        name = ":".join(parts[2:]) if len(parts) > 2 else ""
        return _query_insp_officers(db, level, name, start_date, end_date)

    return None


# ── Summary query (all divisions) ────────────────────────────
def _query_insp_summary(
    db, start_date: Optional[date], end_date: Optional[date]
) -> Dict[str, Any]:
    """Query live DG Dashboard API for overall summary across all divisions."""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    rows = []
    try:
        resp = requests.get(DG_INSP_DIVISIONS, headers=_HEADERS,
                            timeout=_API_TIMEOUT, verify=False)
        resp.raise_for_status()
        api_data = resp.json()
        if isinstance(api_data, list):
            rows = api_data
    except Exception as e:
        log.warning("DG Dashboard divisions API failed: %s", e)

    # Fallback to stored data if API fails
    if not rows:
        rows = db.fetch_all(
            "SELECT division_name AS \"divisionName\", "
            "       total_actions AS \"totalActions\", challans, "
            "       firs AS \"fiRs\", warnings, "
            "       no_offenses AS \"noOffenses\", sealed "
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

    # Compute totals
    totals = {"total_actions": 0, "challans": 0, "firs": 0,
              "warnings": 0, "no_offenses": 0, "sealed": 0}
    for r in rows:
        totals["total_actions"] += (r.get("totalActions") or r.get("total_actions") or 0)
        totals["challans"] += (r.get("challans") or 0)
        totals["firs"] += (r.get("fiRs") or r.get("firs") or 0)
        totals["warnings"] += (r.get("warnings") or 0)
        totals["no_offenses"] += (r.get("noOffenses") or r.get("no_offenses") or 0)
        totals["sealed"] += (r.get("sealed") or 0)

    context = "Inspection Performance Summary (All Divisions)\n"
    context += "=" * 50 + "\n"
    context += f"Total Inspections/Actions: {totals['total_actions']:,}\n"
    context += f"Challans: {totals['challans']:,}\n"
    context += f"FIRs: {totals['firs']:,}\n"
    context += f"Warnings: {totals['warnings']:,}\n"
    context += f"No Offenses: {totals['no_offenses']:,}\n"
    context += f"Sealed: {totals['sealed']:,}\n"
    context += "\nDivision Breakdown:\n"
    for r in sorted(rows, key=lambda x: -(x.get("totalActions") or x.get("total_actions") or 0)):
        dname = r.get("divisionName") or r.get("division_name") or "Unknown"
        ta = r.get("totalActions") or r.get("total_actions") or 0
        ch = r.get("challans") or 0
        fi = r.get("fiRs") or r.get("firs") or 0
        wa = r.get("warnings") or 0
        no = r.get("noOffenses") or r.get("no_offenses") or 0
        se = r.get("sealed") or 0
        context += (
            f"  {dname}: "
            f"{ta:,} inspections, "
            f"{ch:,} challans, "
            f"{fi:,} FIRs, "
            f"{wa:,} warnings, "
            f"{no:,} no offenses, "
            f"{se:,} sealed\n"
        )

    context += "\n(Data fetched live from DG Dashboard API)\n"

    return {
        "source_id": "insp_summary",
        "records": rows,
        "formatted_context": context,
    }


# ── Ranking query (which tehsil/district/division has most X?) ──
def _query_insp_ranking(
    db, level: str, metric: str, limit: int = 30,
) -> Dict[str, Any]:
    """Fetch all locations at the given level from DG Dashboard API and rank by metric."""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    source_id = f"insp_ranking:{level}:{metric}"

    # Metric label for display
    metric_labels = {
        "total_actions": "Total Inspections/Actions",
        "challans": "Challans",
        "firs": "FIRs",
        "warnings": "Warnings",
        "no_offenses": "No Offenses",
        "sealed": "Sealed",
    }
    metric_label = metric_labels.get(metric, metric)

    # API field mapping (DG Dashboard uses camelCase)
    api_field = {
        "total_actions": "totalActions",
        "challans": "challans",
        "firs": "fiRs",
        "warnings": "warnings",
        "no_offenses": "noOffenses",
        "sealed": "sealed",
    }.get(metric, "totalActions")

    rows = []
    try:
        if level == "division":
            resp = requests.get(DG_INSP_DIVISIONS, headers=_HEADERS,
                                timeout=_API_TIMEOUT, verify=False)
            resp.raise_for_status()
            data = resp.json()
            for d in data:
                rows.append({
                    "name": d.get("divisionName", "Unknown"),
                    "value": d.get(api_field, 0) or 0,
                    "total_actions": d.get("totalActions", 0) or 0,
                    "challans": d.get("challans", 0) or 0,
                    "firs": d.get("fiRs", 0) or 0,
                    "warnings": d.get("warnings", 0) or 0,
                    "sealed": d.get("sealed", 0) or 0,
                })

        elif level == "district":
            # Get all divisions first, then districts for each in parallel
            from concurrent.futures import ThreadPoolExecutor, as_completed
            div_resp = requests.get(DG_INSP_DIVISIONS, headers=_HEADERS,
                                    timeout=_API_TIMEOUT, verify=False)
            div_resp.raise_for_status()
            divisions = div_resp.json()

            def _fetch_dist(div):
                div_id = div.get("divisionId")
                if not div_id:
                    return []
                r = requests.get(DG_INSP_DISTRICTS, params={"divisionId": div_id},
                                 headers=_HEADERS, timeout=15, verify=False)
                if r.status_code != 200:
                    return []
                result = []
                for d in r.json():
                    result.append({
                        "name": d.get("districtName", "Unknown"),
                        "division": div.get("divisionName", ""),
                        "value": d.get(api_field, 0) or 0,
                        "total_actions": d.get("totalActions", 0) or 0,
                        "challans": d.get("challans", 0) or 0,
                        "firs": d.get("fiRs", 0) or 0,
                        "warnings": d.get("warnings", 0) or 0,
                        "sealed": d.get("sealed", 0) or 0,
                    })
                return result

            with ThreadPoolExecutor(max_workers=10) as pool:
                futures = {pool.submit(_fetch_dist, dv): dv for dv in divisions}
                for f in as_completed(futures, timeout=30):
                    try:
                        rows.extend(f.result())
                    except Exception:
                        pass

        elif level == "tehsil":
            # Get all divisions → districts in parallel → tehsils in parallel
            from concurrent.futures import ThreadPoolExecutor, as_completed
            div_resp = requests.get(DG_INSP_DIVISIONS, headers=_HEADERS,
                                    timeout=_API_TIMEOUT, verify=False)
            div_resp.raise_for_status()
            divisions = div_resp.json()

            # Phase 1: fetch all districts in parallel
            all_districts = []
            def _fetch_districts(div):
                div_id = div.get("divisionId")
                if not div_id:
                    return []
                r = requests.get(DG_INSP_DISTRICTS, params={"divisionId": div_id},
                                 headers=_HEADERS, timeout=15, verify=False)
                if r.status_code != 200:
                    return []
                return [(d, div.get("divisionName", "")) for d in r.json()]

            with ThreadPoolExecutor(max_workers=10) as pool:
                futures = {pool.submit(_fetch_districts, dv): dv for dv in divisions}
                for f in as_completed(futures, timeout=30):
                    try:
                        all_districts.extend(f.result())
                    except Exception:
                        pass

            # Phase 2: fetch all tehsils in parallel
            def _fetch_tehsils(dist_item):
                dist, div_name = dist_item
                dist_id = dist.get("districtId")
                if not dist_id:
                    return []
                r = requests.get(DG_INSP_TEHSILS, params={"districtId": dist_id},
                                 headers=_HEADERS, timeout=15, verify=False)
                if r.status_code != 200:
                    return []
                result = []
                for t in r.json():
                    result.append({
                        "name": t.get("tehsilName", "Unknown"),
                        "district": dist.get("districtName", ""),
                        "division": div_name,
                        "value": t.get(api_field, 0) or 0,
                        "total_actions": t.get("totalActions", 0) or 0,
                        "challans": t.get("challans", 0) or 0,
                        "firs": t.get("fiRs", 0) or 0,
                        "warnings": t.get("warnings", 0) or 0,
                        "sealed": t.get("sealed", 0) or 0,
                    })
                return result

            with ThreadPoolExecutor(max_workers=20) as pool:
                futures = {pool.submit(_fetch_tehsils, d): d for d in all_districts}
                for f in as_completed(futures, timeout=45):
                    try:
                        rows.extend(f.result())
                    except Exception:
                        pass

    except Exception as e:
        log.warning("DG Dashboard ranking query failed: %s", e)
        return {
            "source_id": source_id, "records": [],
            "formatted_context": f"Unable to fetch ranking data. API error: {e}\n",
        }

    if not rows:
        return {
            "source_id": source_id, "records": [],
            "formatted_context": f"No {level}-level inspection data available.\n",
        }

    # Sort by metric value descending
    rows.sort(key=lambda x: -x["value"])

    # Format context
    context = f"Inspection Ranking by {metric_label} - {level.title()} Level\n"
    context += "=" * 50 + "\n"
    context += f"Top {min(len(rows), limit)} {level}s ranked by {metric_label}:\n\n"

    for i, r in enumerate(rows[:limit]):
        loc_info = r["name"]
        if r.get("district"):
            loc_info += f" ({r['district']})"
        elif r.get("division"):
            loc_info += f" ({r['division']})"
        context += (
            f"  {i+1}. {loc_info}: "
            f"{r['value']:,} {metric_label}, "
            f"{r['total_actions']:,} total inspections, "
            f"{r['challans']:,} challans, "
            f"{r['firs']:,} FIRs, "
            f"{r['warnings']:,} warnings, "
            f"{r['sealed']:,} sealed\n"
        )

    # Grand total
    total_val = sum(r["value"] for r in rows)
    context += f"\nTotal across all {len(rows)} {level}s: {total_val:,} {metric_label}\n"
    context += "\n(Data fetched live from DG Dashboard API)\n"

    return {
        "source_id": source_id,
        "records": rows[:limit],
        "formatted_context": context,
    }


# ── Officer ranking across province / location ──────────────
def _query_officer_ranking(
    db, metric: str,
    start_date: Optional[date], end_date: Optional[date],
    loc_level: Optional[str] = None, loc_name: Optional[str] = None,
    limit: int = 25,
) -> Dict[str, Any]:
    """Fetch officer-level data from SDEO API and rank by metric.

    If loc_level/loc_name given, scope to that division/district/tehsil.
    For province-wide queries (no location), uses a two-phase strategy:
      Phase 1: Use DG Dashboard to find top 5 divisions by the metric.
      Phase 2: Fetch officers from those divisions' tehsils only.
    This avoids calling SDEO API for ALL 150+ tehsils.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    source_id = f"insp_officer_ranking:{metric}"
    if loc_level and loc_name:
        source_id += f":{loc_level}:{loc_name}"

    # Metric labels
    metric_labels = {
        "total_actions": "Total Inspections",
        "challans": "Challans",
        "firs": "FIRs",
        "warnings": "Warnings",
        "no_offenses": "No Offenses",
        "sealed": "Sealed",
    }
    metric_label = metric_labels.get(metric, metric)

    # Map our metric keys to SDEO officer API field names
    sdeo_field = {
        "total_actions": "inspection",
        "challans": "challan",
        "firs": "fir",
        "warnings": "warning",
        "no_offenses": "noOffense",
        "sealed": "sealed",
    }.get(metric, "fir")

    # DG Dashboard API field for ranking divisions
    dg_field = {
        "total_actions": "totalActions",
        "challans": "challans",
        "firs": "fiRs",
        "warnings": "warnings",
        "no_offenses": "noOffenses",
        "sealed": "sealed",
    }.get(metric, "fiRs")

    # Determine which tehsils to query
    tehsil_rows = []
    province_wide = False
    if loc_level == "tehsil" and loc_name:
        tehsil_rows = db.fetch_all(
            "SELECT tehsil_id, tehsil_name FROM dim_tehsil "
            "WHERE LOWER(tehsil_name) = LOWER(%s)", (loc_name,),
        )
    elif loc_level == "district" and loc_name:
        tehsil_rows = db.fetch_all(
            "SELECT t.tehsil_id, t.tehsil_name FROM dim_tehsil t "
            "JOIN dim_district d ON t.district_id = d.district_id "
            "WHERE LOWER(d.district_name) = LOWER(%s)", (loc_name,),
        )
    elif loc_level == "division" and loc_name:
        tehsil_rows = db.fetch_all(
            "SELECT t.tehsil_id, t.tehsil_name FROM dim_tehsil t "
            "JOIN dim_district d ON t.district_id = d.district_id "
            "JOIN dim_division dv ON d.division_id = dv.division_id "
            "WHERE LOWER(dv.division_name) = LOWER(%s)", (loc_name,),
        )
    else:
        # Province-wide: use two-phase approach
        province_wide = True
        # Phase 1: get ALL divisions from DG Dashboard, pick top ones by metric
        try:
            div_resp = requests.get(DG_INSP_DIVISIONS, headers=_HEADERS,
                                    timeout=_API_TIMEOUT, verify=False)
            div_resp.raise_for_status()
            divisions = div_resp.json()
            # Sort by the metric, take ALL divisions (they're only ~9-10)
            divisions.sort(key=lambda d: d.get(dg_field, 0) or 0, reverse=True)
            top_div_names = [d.get("divisionName", "") for d in divisions]
            log.info("Officer ranking province-wide: fetching from all %d divisions: %s",
                     len(top_div_names), ", ".join(top_div_names[:5]))
        except Exception as e:
            log.warning("DG Dashboard division fetch failed: %s", e)
            top_div_names = []

        if top_div_names:
            # Get tehsils for all divisions
            placeholders = ",".join(["%s"] * len(top_div_names))
            tehsil_rows = db.fetch_all(
                f"SELECT t.tehsil_id, t.tehsil_name FROM dim_tehsil t "
                f"JOIN dim_district d ON t.district_id = d.district_id "
                f"JOIN dim_division dv ON d.division_id = dv.division_id "
                f"WHERE LOWER(dv.division_name) IN ({placeholders})",
                tuple(n.lower() for n in top_div_names),
            )

    if not tehsil_rows:
        scope = f"{loc_level} '{loc_name}'" if loc_level else "Punjab"
        return {
            "source_id": source_id, "records": [],
            "formatted_context": f"No tehsils found for {scope}.\n",
        }

    log.info("Officer ranking: querying %d tehsils", len(tehsil_rows))

    # Default date range
    if not start_date:
        start_date = date(2024, 1, 1)
    if not end_date:
        end_date = date.today()

    scope_label = f"{loc_level.title()}: {loc_name}" if loc_level and loc_name else "All Punjab"

    # Fetch officers from SDEO API in parallel (max 20 threads for speed)
    all_officers: Dict[str, Dict[str, Any]] = {}

    def _fetch_tehsil(tid, tname):
        try:
            params = {
                "tehsilId": tid,
                "startDate": f"{start_date.isoformat()}T00:00:00",
                "endDate": f"{end_date.isoformat()}T23:59:59",
            }
            resp = requests.get(
                SDEO_INSPECTIONS_SUMMARY, params=params,
                headers=_HEADERS, timeout=15, verify=False,
            )
            resp.raise_for_status()
            data = resp.json()
            officers = []
            if isinstance(data, dict):
                officers = data.get("officers", [])
            elif isinstance(data, list) and data:
                officers = data[0].get("officers", []) if isinstance(data[0], dict) else []
            return [(o, tname) for o in officers]
        except Exception as e:
            log.debug("Officer ranking: SDEO fetch failed for tehsil %s: %s", tid, e)
            return []

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {
            pool.submit(_fetch_tehsil, r["tehsil_id"], r["tehsil_name"]): r
            for r in tehsil_rows
        }
        for fut in as_completed(futures):
            for off, tname in fut.result():
                oname = (off.get("officerName") or "").strip()
                if not oname:
                    continue
                if oname not in all_officers:
                    all_officers[oname] = {
                        "officerName": oname,
                        "tehsil": tname,
                        "challan": 0, "fir": 0, "warning": 0,
                        "noOffense": 0, "inspection": 0, "sealed": 0,
                    }
                all_officers[oname]["challan"] += off.get("challan", 0) or 0
                all_officers[oname]["fir"] += off.get("fir", 0) or 0
                all_officers[oname]["warning"] += off.get("warning", 0) or 0
                all_officers[oname]["noOffense"] += off.get("noOffense", 0) or 0
                all_officers[oname]["inspection"] += off.get("inspection", 0) or 0
                all_officers[oname]["sealed"] += off.get("sealed", 0) or 0

    if not all_officers:
        return {
            "source_id": source_id, "records": [],
            "formatted_context": f"No officer data available for {scope_label}.\n",
        }

    # Sort by the requested metric
    sorted_officers = sorted(
        all_officers.values(),
        key=lambda o: o.get(sdeo_field, 0),
        reverse=True,
    )

    context = f"Officer Ranking by {metric_label} -- {scope_label}\n"
    context += f"Date Range: {start_date} to {end_date}\n"
    context += "=" * 60 + "\n"
    context += f"Top {min(len(sorted_officers), limit)} officers ranked by {metric_label}:\n\n"

    for i, off in enumerate(sorted_officers[:limit], 1):
        context += f"  {i}. {off['officerName']} (Tehsil: {off['tehsil']})\n"
        context += f"     {metric_label}: {off.get(sdeo_field, 0):,}\n"
        context += f"     Total Inspections: {off['inspection']:,}, "
        context += f"Challans: {off['challan']:,}, FIRs: {off['fir']:,}, "
        context += f"Warnings: {off['warning']:,}, Sealed: {off['sealed']:,}\n\n"

    context += f"Total Officers Found: {len(sorted_officers)}\n"
    context += f"Showing Top {min(len(sorted_officers), limit)}\n"
    context += "(Data fetched live from SDEO Inspections Summary API)\n"

    return {
        "source_id": source_id,
        "records": sorted_officers[:limit],
        "formatted_context": context,
    }


# ── Location query (division / district / tehsil) ───────────
def _query_insp_location(
    db, level: str, name: str,
    start_date: Optional[date], end_date: Optional[date],
) -> Dict[str, Any]:
    """Query inspection data for a specific location.
    With date range → PCM dashboard-counts API (matches official dashboard).
    Without date range: tehsil → SDEO API, division/district → DG Dashboard API.
    """
    col = f"{level}_name"
    source_id = f"insp_{level}:{name}"
    # ── Any level + date range → PCM dashboard-counts API ──
    # The official SDEO Dashboard uses the PCM API under the hood, so PCM
    # numbers are the authoritative reference for all levels (tehsil,
    # district, division).  The SDEO inspections-summary endpoint returns
    # a different (smaller) breakdown and does NOT match the dashboard.
    if start_date and end_date:
        return _query_pcm_dashboard_counts(db, level, name, start_date, end_date, source_id)

    # ── Tehsil without date range → PCM API with wide default dates ──
    if level == "tehsil":
        return _query_pcm_dashboard_counts(
            db, level, name,
            date(2024, 1, 1), date.today(), source_id,
        )

    # ── Division / District → LIVE DG Dashboard API ──
    if level in ("division", "district"):
        return _query_location_live(db, level, name, start_date, end_date, source_id)


def _query_tehsil_stored(db, name: str, source_id: str) -> Dict[str, Any]:
    """Use stored inspection_performance for a tehsil (no date filter)."""
    parent_rows = db.fetch_all(
        "SELECT * FROM inspection_performance "
        "WHERE level = 'tehsil' AND tehsil_name = %s AND snapshot_date = ("
        "  SELECT MAX(snapshot_date) FROM inspection_performance "
        "  WHERE level = 'tehsil' AND tehsil_name = %s"
        ")",
        (name, name),
    )
    officer_rows = db.fetch_all(
        "SELECT officer_name, total_inspections, total_challans, "
        "       fine_amount, sealed, arrest_case "
        "FROM officer_inspection_detail "
        "WHERE tehsil_name = %s AND snapshot_date = ("
        "  SELECT MAX(snapshot_date) FROM officer_inspection_detail WHERE tehsil_name = %s"
        ") ORDER BY total_inspections DESC",
        (name, name),
    )

    if not parent_rows:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"No inspection data found for tehsil '{name}'.\n",
        }

    r = parent_rows[0]
    context = f"Inspection Performance — Tehsil: {name}\n"
    context += "=" * 50 + "\n"
    context += f"Total Inspections/Actions: {r.get('total_actions', 0):,}\n"
    context += f"Challans: {r.get('challans', 0):,}\n"
    context += f"FIRs: {r.get('firs', 0):,}\n"
    context += f"Warnings: {r.get('warnings', 0):,}\n"
    context += f"No Offenses: {r.get('no_offenses', 0):,}\n"
    context += f"Sealed: {r.get('sealed', 0):,}\n"

    if officer_rows:
        context += f"\nOfficer Breakdown ({len(officer_rows)} officers):\n"
        for o in officer_rows:
            context += (
                f"  {o['officer_name']}: "
                f"{o.get('total_inspections', 0):,} inspections, "
                f"{o.get('total_challans', 0):,} challans, "
                f"Rs. {o.get('fine_amount', 0):,} fine\n"
            )

    return {
        "source_id": source_id,
        "records": parent_rows,
        "formatted_context": context,
    }


def _query_location_live(
    db, level: str, name: str,
    start_date: Optional[date], end_date: Optional[date],
    source_id: str,
) -> Dict[str, Any]:
    """Call DG Dashboard API for live division/district inspection data.

    For division level: GET /divisions → find matching row + GET /districts?divisionId=X
    For district level: look up divisionId → GET /districts?divisionId=X → find row
                        + GET /tehsils?districtId=X for children
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # ── If date range given, fall back to SDEO tehsil-by-tehsil approach ──
    if start_date and end_date:
        return _query_location_live_daterange(
            db, level, name, start_date, end_date, source_id,
        )

    # ── No date range → use DG Dashboard API (fast, single call) ──
    try:
        if level == "division":
            # Get division totals
            resp = requests.get(DG_INSP_DIVISIONS, headers=_HEADERS, timeout=_API_TIMEOUT, verify=False)
            resp.raise_for_status()
            all_divs = resp.json()
            parent_data = None
            div_id = None
            for d in all_divs:
                if d.get("divisionName", "").lower() == name.lower():
                    parent_data = d
                    div_id = d.get("divisionId")
                    break
            if not parent_data:
                return {
                    "source_id": source_id, "records": [],
                    "formatted_context": f"Division '{name}' not found in live data.\n",
                }
            # Get child districts
            child_resp = requests.get(
                DG_INSP_DISTRICTS, params={"divisionId": div_id},
                headers=_HEADERS, timeout=_API_TIMEOUT, verify=False,
            )
            child_resp.raise_for_status()
            child_rows = child_resp.json() if child_resp.status_code == 200 else []
            child_level = "district"
            child_name_key = "districtName"

        elif level == "district":
            # Look up divisionId for this district
            dim_row = db.fetch_all(
                "SELECT d.district_id, dv.division_id "
                "FROM dim_district d "
                "JOIN dim_division dv ON d.division_id = dv.division_id "
                "WHERE LOWER(d.district_name) = LOWER(%s) LIMIT 1",
                (name,),
            )
            if not dim_row:
                return {
                    "source_id": source_id, "records": [],
                    "formatted_context": f"District '{name}' not found in database.\n",
                }
            division_id = dim_row[0]["division_id"]
            district_id = dim_row[0]["district_id"]

            # Get all districts under the division, find ours
            resp = requests.get(
                DG_INSP_DISTRICTS, params={"divisionId": division_id},
                headers=_HEADERS, timeout=_API_TIMEOUT, verify=False,
            )
            resp.raise_for_status()
            all_dists = resp.json()
            parent_data = None
            for d in all_dists:
                if d.get("districtName", "").lower() == name.lower():
                    parent_data = d
                    break
            if not parent_data:
                return {
                    "source_id": source_id, "records": [],
                    "formatted_context": f"District '{name}' not found in live data.\n",
                }
            # Get child tehsils
            child_resp = requests.get(
                DG_INSP_TEHSILS, params={"districtId": district_id},
                headers=_HEADERS, timeout=_API_TIMEOUT, verify=False,
            )
            child_resp.raise_for_status()
            child_rows = child_resp.json() if child_resp.status_code == 200 else []
            child_level = "tehsil"
            child_name_key = "tehsilName"
        else:
            return {
                "source_id": source_id, "records": [],
                "formatted_context": f"Unsupported level '{level}'.\n",
            }

    except Exception as e:
        log.warning("DG Dashboard API failed for %s '%s': %s", level, name, e)
        return {
            "source_id": source_id, "records": [],
            "formatted_context": (
                f"Unable to fetch live inspection data for {level} '{name}'. "
                f"API error: {e}\n"
            ),
        }

    # ── Format context ──
    ta = parent_data.get("totalActions", 0) or 0
    ch = parent_data.get("challans", 0) or 0
    fi = parent_data.get("fiRs", 0) or 0
    wa = parent_data.get("warnings", 0) or 0
    no = parent_data.get("noOffenses", 0) or 0
    se = parent_data.get("sealed", 0) or 0

    context = f"Inspection Performance - {level.title()}: {name}\n"
    context += "=" * 50 + "\n"
    context += f"Total Inspections/Actions: {ta:,}\n"
    context += f"Challans: {ch:,}\n"
    context += f"FIRs: {fi:,}\n"
    context += f"Warnings: {wa:,}\n"
    context += f"No Offenses: {no:,}\n"
    context += f"Sealed: {se:,}\n"

    if child_rows and isinstance(child_rows, list):
        context += f"\n{child_level.title()} Breakdown:\n"
        for c in sorted(child_rows, key=lambda x: -(x.get("totalActions", 0) or 0)):
            cname = c.get(child_name_key, "Unknown")
            context += (
                f"  {cname}: "
                f"{c.get('totalActions', 0) or 0:,} inspections, "
                f"{c.get('challans', 0) or 0:,} challans, "
                f"{c.get('warnings', 0) or 0:,} warnings, "
                f"{c.get('fiRs', 0) or 0:,} FIRs, "
                f"{c.get('sealed', 0) or 0:,} sealed\n"
            )

    context += "\n(Data fetched live from DG Dashboard API)\n"

    return {
        "source_id": source_id,
        "records": [parent_data] + (child_rows if isinstance(child_rows, list) else []),
        "formatted_context": context,
    }


def _query_location_live_daterange(
    db, level: str, name: str,
    start_date: date, end_date: date,
    source_id: str,
) -> Dict[str, Any]:
    """For date-filtered division/district queries, call SDEO API per tehsil sequentially."""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Look up all tehsils under this location
    if level == "division":
        tehsil_rows = db.fetch_all(
            "SELECT t.tehsil_id, t.tehsil_name, d.district_name "
            "FROM dim_tehsil t "
            "JOIN dim_district d ON t.district_id = d.district_id "
            "JOIN dim_division dv ON d.division_id = dv.division_id "
            "WHERE LOWER(dv.division_name) = LOWER(%s)",
            (name,),
        )
    else:
        tehsil_rows = db.fetch_all(
            "SELECT t.tehsil_id, t.tehsil_name, d.district_name "
            "FROM dim_tehsil t "
            "JOIN dim_district d ON t.district_id = d.district_id "
            "WHERE LOWER(d.district_name) = LOWER(%s)",
            (name,),
        )

    if not tehsil_rows:
        return {
            "source_id": source_id, "records": [],
            "formatted_context": f"No tehsils found under {level} '{name}'.\n",
        }

    log.info("Live SDEO date-range query: %s '%s' -> %d tehsils",
             level, name, len(tehsil_rows))

    # Match SDEO dashboard 11:59 PM convention: T23:59:59 = inclusive end date
    date_params = {
        "startDate": f"{start_date.isoformat()}T00:00:00",
        "endDate": f"{end_date.isoformat()}T23:59:59",
    }
    _sess = requests.Session()
    _sess.verify = False

    grand = {"total_actions": 0, "challans": 0, "firs": 0,
             "warnings": 0, "no_offenses": 0, "sealed": 0}
    child_level = "district" if level == "division" else "tehsil"
    child_agg: Dict[str, Dict[str, int]] = {}
    failed = 0

    for trow in tehsil_rows:
        tid = trow["tehsil_id"]
        data = None
        for attempt in range(1, 4):
            try:
                resp = _sess.get(
                    SDEO_INSPECTIONS_SUMMARY,
                    params={"tehsilId": tid, **date_params},
                    headers=_HEADERS, timeout=60,
                )
                resp.raise_for_status()
                d = resp.json()
                if isinstance(d, dict):
                    data = d
                    break
            except Exception:
                if attempt < 3:
                    time.sleep(1)
        if data is None:
            failed += 1
            continue

        for k, api_k in [("total_actions", "totalActions"), ("challans", "challans"),
                          ("firs", "fiRs"), ("warnings", "warnings"),
                          ("no_offenses", "noOffenses"), ("sealed", "sealed")]:
            v = data.get(api_k, 0) or 0
            grand[k] += v
            cname = trow.get("district_name" if child_level == "district" else "tehsil_name", "Unknown")
            child_agg.setdefault(cname, {"total_actions": 0, "challans": 0, "firs": 0,
                                          "warnings": 0, "no_offenses": 0, "sealed": 0})
            child_agg[cname][k] += v

    context = f"Inspection Performance - {level.title()}: {name}\n"
    context += f"Date Range: {start_date} to {end_date}\n"
    context += "=" * 50 + "\n"
    context += f"Total Inspections/Actions: {grand['total_actions']:,}\n"
    context += f"Challans: {grand['challans']:,}\n"
    context += f"FIRs: {grand['firs']:,}\n"
    context += f"Warnings: {grand['warnings']:,}\n"
    context += f"No Offenses: {grand['no_offenses']:,}\n"
    context += f"Sealed: {grand['sealed']:,}\n"

    if child_agg:
        context += f"\n{child_level.title()} Breakdown:\n"
        for cname in sorted(child_agg.keys()):
            c = child_agg[cname]
            context += (
                f"  {cname}: {c['total_actions']:,} inspections, "
                f"{c['challans']:,} challans, {c['warnings']:,} warnings\n"
            )

    if failed:
        context += f"\n(Note: {failed}/{len(tehsil_rows)} tehsil API calls failed)\n"
    context += f"\n(Data fetched live from SDEO API for {start_date} to {end_date})\n"

    return {
        "source_id": source_id,
        "records": [grand],
        "formatted_context": context,
    }


# ── PCM dashboard-counts API (matches official PERA dashboard) ──
def _query_pcm_dashboard_counts(
    db, level: str, name: str,
    start_date: date, end_date: date, source_id: str,
) -> Dict[str, Any]:
    """Call PCM dashboard-counts API for any level with date filtering.
    This is the same API the official PERA dashboard uses.
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Look up the ID for this location
    # NOTE: Do NOT send RequisitionTypeId=0 — it filters to a single
    # requisition type, returning only a subset.  Omitting it returns
    # ALL types, matching the official PERA dashboard totals.
    # NOTE: ToDate uses T23:59 (end of day) so "1 Mar to 15 Mar"
    # INCLUDES March 15 data — matching natural language intent.
    # To compare with the SDEO dashboard, set the dashboard's "To" time
    # to 11:59 PM (not the default 12:00 AM which excludes the end date).
    params: Dict[str, Any] = {
        "FromDate": f"{start_date.isoformat()}T00:00",
        "ToDate": f"{end_date.isoformat()}T23:59",
    }

    if level == "tehsil":
        row = db.fetch_all(
            "SELECT tehsil_id FROM dim_tehsil WHERE LOWER(tehsil_name) = LOWER(%s) LIMIT 1",
            (name,),
        )
        if not row:
            return {"source_id": source_id, "records": [],
                    "formatted_context": f"Tehsil '{name}' not found.\n"}
        params["TehsilId"] = row[0]["tehsil_id"]
    elif level == "district":
        row = db.fetch_all(
            "SELECT district_id FROM dim_district WHERE LOWER(district_name) = LOWER(%s) LIMIT 1",
            (name,),
        )
        if not row:
            return {"source_id": source_id, "records": [],
                    "formatted_context": f"District '{name}' not found.\n"}
        params["DistrictId"] = row[0]["district_id"]
    elif level == "division":
        row = db.fetch_all(
            "SELECT division_id FROM dim_division WHERE LOWER(division_name) = LOWER(%s) LIMIT 1",
            (name,),
        )
        if not row:
            return {"source_id": source_id, "records": [],
                    "formatted_context": f"Division '{name}' not found.\n"}
        params["DivisionId"] = row[0]["division_id"]

    try:
        resp = requests.get(
            PCM_DASHBOARD_COUNTS, params=params,
            headers=_HEADERS, timeout=_API_TIMEOUT, verify=False,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning("PCM dashboard-counts failed for %s '%s': %s", level, name, e)
        # Fallback to SDEO approach
        if level == "tehsil":
            return _query_tehsil_live(db, name, start_date, end_date, source_id)
        return {"source_id": source_id, "records": [],
                "formatted_context": f"Unable to fetch data for {level} '{name}'. Error: {e}\n"}

    total_insp = data.get("totalInspections", 0) or 0
    total_ch = data.get("totalChallans", 0) or 0
    total_fine = data.get("totalFineAmount", 0) or 0
    total_sealed = data.get("totalSealed", 0) or 0
    total_arrest = data.get("totalArrest", 0) or 0
    total_pcm = data.get("totalPCM", 0) or 0
    paid_ch = data.get("paidChallans", 0) or 0
    unpaid_ch = data.get("unPaidChallans", 0) or 0
    paid_amt = data.get("paidChallanAmount", 0) or 0
    unpaid_amt = data.get("unPaidChallanAmount", 0) or 0

    context = f"Inspection Performance - {level.title()}: {name}\n"
    context += f"Date Range: {start_date} to {end_date}\n"
    context += "=" * 50 + "\n"
    context += f"Total Inspections: {total_insp:,}\n"
    context += f"Total Challans: {total_ch:,}\n"
    context += f"Total Fine Amount: Rs. {total_fine:,.0f}\n"
    context += f"  Paid Challans: {paid_ch:,} (Rs. {paid_amt:,.0f})\n"
    context += f"  Unpaid Challans: {unpaid_ch:,} (Rs. {unpaid_amt:,.0f})\n"
    context += f"Sealed: {total_sealed:,}\n"
    context += f"Arrests: {total_arrest:,}\n"
    context += f"PCM: {total_pcm:,}\n"
    context += "\n(Data fetched live from PCM Dashboard API)\n"

    return {
        "source_id": source_id,
        "records": [data],
        "formatted_context": context,
    }


# ── Officer breakdown via SDEO API ────────────────────────────
def _query_insp_officers(
    db, level: str, name: str,
    start_date: Optional[date], end_date: Optional[date],
) -> Dict[str, Any]:
    """Fetch officer-level inspection breakdown from the SDEO API.
    The PCM dashboard-counts API only returns aggregates; the SDEO
    inspections-summary API includes per-officer breakdown.
    """
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    source_id = f"insp_officers:{level}:{name}"

    # Only works for tehsil level (SDEO API is tehsil-based)
    # For district/division, we need to find child tehsils
    tehsil_ids = []
    if level == "tehsil":
        row = db.fetch_all(
            "SELECT tehsil_id FROM dim_tehsil WHERE LOWER(tehsil_name) = LOWER(%s) LIMIT 1",
            (name,),
        )
        if row:
            tehsil_ids = [row[0]["tehsil_id"]]
    elif level == "district":
        rows = db.fetch_all(
            "SELECT t.tehsil_id FROM dim_tehsil t "
            "JOIN dim_district d ON t.district_id = d.district_id "
            "WHERE LOWER(d.district_name) = LOWER(%s)",
            (name,),
        )
        tehsil_ids = [r["tehsil_id"] for r in rows]
    elif level == "division":
        rows = db.fetch_all(
            "SELECT t.tehsil_id FROM dim_tehsil t "
            "JOIN dim_district d ON t.district_id = d.district_id "
            "JOIN dim_division dv ON d.division_id = dv.division_id "
            "WHERE LOWER(dv.division_name) = LOWER(%s)",
            (name,),
        )
        tehsil_ids = [r["tehsil_id"] for r in rows]

    if not tehsil_ids:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"No tehsil found for {level} '{name}'.\n",
        }

    # Use default date range if not provided
    if not start_date:
        start_date = date(2024, 1, 1)
    if not end_date:
        end_date = date.today()

    # Fetch officer data from SDEO API for each tehsil
    all_officers: Dict[str, Dict[str, Any]] = {}  # officer_name → aggregated stats
    tehsil_name_display = name

    for tid in tehsil_ids:
        try:
            params = {
                "tehsilId": tid,
                "startDate": f"{start_date.isoformat()}T00:00:00",
                "endDate": f"{end_date.isoformat()}T23:59:59",
            }
            resp = requests.get(
                SDEO_INSPECTIONS_SUMMARY, params=params,
                headers=_HEADERS, timeout=_API_TIMEOUT, verify=False,
            )
            resp.raise_for_status()
            data = resp.json()

            officers = []
            if isinstance(data, dict):
                officers = data.get("officers", [])
            elif isinstance(data, list) and data:
                officers = data[0].get("officers", []) if isinstance(data[0], dict) else []

            for off in officers:
                oname = (off.get("officerName") or "").strip()
                if not oname:
                    continue
                if oname not in all_officers:
                    all_officers[oname] = {
                        "officerName": oname,
                        "challan": 0, "fir": 0, "warning": 0,
                        "noOffense": 0, "inspection": 0, "sealed": 0,
                    }
                all_officers[oname]["challan"] += off.get("challan", 0) or 0
                all_officers[oname]["fir"] += off.get("fir", 0) or 0
                all_officers[oname]["warning"] += off.get("warning", 0) or 0
                all_officers[oname]["noOffense"] += off.get("noOffense", 0) or 0
                all_officers[oname]["inspection"] += off.get("inspection", 0) or 0
                all_officers[oname]["sealed"] += off.get("sealed", 0) or 0
        except Exception as e:
            log.warning("SDEO officer fetch failed for tehsil %s: %s", tid, e)

    if not all_officers:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": f"No officer data available for {level} '{name}'.\n",
        }

    # Sort by total inspections descending
    sorted_officers = sorted(
        all_officers.values(),
        key=lambda o: o.get("inspection", 0),
        reverse=True,
    )

    context = f"Officer-wise Inspection Breakdown — {level.title()}: {name}\n"
    context += f"Date Range: {start_date} to {end_date}\n"
    context += "=" * 60 + "\n\n"

    for i, off in enumerate(sorted_officers, 1):
        context += f"{i}. {off['officerName']}\n"
        context += f"   Total Inspections: {off['inspection']:,}\n"
        context += f"   Challans: {off['challan']:,}\n"
        context += f"   FIRs: {off['fir']:,}\n"
        context += f"   Warnings: {off['warning']:,}\n"
        context += f"   No Offense: {off['noOffense']:,}\n"
        context += f"   Sealed: {off['sealed']:,}\n\n"

    context += f"Total Officers: {len(sorted_officers)}\n"
    context += "(Data fetched live from SDEO Inspections Summary API)\n"

    return {
        "source_id": source_id,
        "records": sorted_officers,
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

    # Match SDEO dashboard 11:59 PM convention: T23:59:59 = inclusive end date.
    # "from 1 March to 10 March" → includes all of March 10.
    # This matches the dashboard when "To" time is set to 11:59 PM.
    from_dt = f"{start_date.isoformat()}T00:00:00"
    to_dt   = f"{end_date.isoformat()}T23:59:59"

    try:
        resp = requests.get(
            SDEO_INSPECTIONS_SUMMARY,
            params={
                "tehsilId": tehsil_id,
                "startDate": from_dt,
                "endDate":   to_dt,
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

    if officers:
        context += f"\nOfficer Breakdown ({len(officers)} officers):\n"
        for o in sorted(officers, key=lambda x: -(x.get("inspection", 0) or 0)):
            context += (
                f"  {o.get('officerName', 'Unknown')}: "
                f"{o.get('inspection', 0):,} inspections, "
                f"{o.get('challan', 0):,} challans, "
                f"{o.get('warning', 0):,} warnings, "
                f"{o.get('fir', 0)} FIRs, "
                f"{o.get('sealed', 0)} sealed\n"
            )

    context += f"\n(Data fetched live from SDEO API for the specified date range)\n"

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
