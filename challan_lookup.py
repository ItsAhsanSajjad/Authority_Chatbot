"""
PERA AI — Challan Chatbot Lookup (PostgreSQL)

Handles challan-related queries by reading from PostgreSQL
and returning structured results compatible with the answer pipeline.

Location-aware + comparison-aware intent detection:
  - "paid challan total in Lahore Saddar?" → queries challan_tehsil_drill
    for Lahore Saddar specifically
  - "which station has more paid challans?" → ranked comparison across stations
  - "price control challans in Nankana Sahib" → breakdown by type for location
  - "challan by division" → queries all divisions
  - "challan totals?" → overall summary

Integrated via hooks in stored_api_lookup.py:
  detect_challan_intent(question) -> Optional[str]
  execute_challan_lookup(source_id, question) -> Optional[Dict]
"""
from __future__ import annotations

import re
import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from log_config import get_logger

log = get_logger("pera.challan.lookup")


# ── PostgreSQL helper ────────────────────────────────────────

def _get_db():
    """Get the AnalyticsDB singleton."""
    from analytics_db import get_analytics_db
    return get_analytics_db()


# ── Location Name Cache ──────────────────────────────────────
# Maps lowercase name -> list of (type, db_name) so the same name
# at multiple hierarchy levels (e.g. "Bahawalpur" is both a division,
# district, AND has tehsils like "Bahawalpur Sadar") are all preserved.

_location_cache: Optional[Dict[str, List[Tuple[str, str]]]] = None
_location_lock = threading.Lock()

# Common spelling normalizations for Pakistani city/tehsil names
_SPELLING_NORMS = {
    "saddar": "sadar",
    "saddr": "sadar",
    "suddar": "sadar",
    # Khairpur / Khaipur variants
    "khairpur": "khaipur",
    "kherpur": "khaipur",
    "khyerpur": "khaipur",
    # Taimewali / Tamewali variants
    "taimewali": "tamewali",
    "tamey wali": "tamewali",
    "taime wali": "tamewali",
    # Muzaffargarh variants
    "muzaffargarh": "muzaffar garh",
    "muzafargarh": "muzaffar garh",
    # Ferozepur variants
    "ferozewala": "ferozwala",
    "firozwala": "ferozwala",
    # Bahawalpur variants
    "bahwalpur": "bahawalpur",
    # Rahim Yar Khan variants
    "rahimyarkhan": "rahim yar khan",
    # Chichawatni variants
    "chicawatni": "chichawatni",
    "chichawatani": "chichawatni",
    # Sheikhupura variants
    "shaikhupura": "sheikhupura",
    "shekhupura": "sheikhupura",
    # Nankana variants
    "nankana sahab": "nankana sahib",
    "nankanasahib": "nankana sahib",
    # DG Khan variants
    "dera ghazi khan": "dera ghazi khan",
    "d g khan": "dera ghazi khan",
    "dg khan": "dera ghazi khan",
    # Pakpattan variants
    "pakpatan": "pakpattan",
    "pakpattan": "pakpattan sharif",
    # Jhelum variants
    "jehlum": "jhelum",
}

# Prefixes the user might add that aren't part of the actual location name
_STRIP_PREFIXES = re.compile(
    r"\b(?:tehsil|district|division|station|area|region|zone|officer|by)\s+", re.I,
)


def _normalize_spelling(text: str) -> str:
    """Normalize common Pakistani location spelling variations."""
    t = text.lower()
    for variant, canonical in _SPELLING_NORMS.items():
        t = t.replace(variant, canonical)
    return t


def _load_locations() -> Dict[str, List[Tuple[str, str]]]:
    """Load all division/district/tehsil names from PostgreSQL."""
    global _location_cache
    if _location_cache is not None:
        return _location_cache

    with _location_lock:
        if _location_cache is not None:
            return _location_cache

        cache: Dict[str, List[Tuple[str, str]]] = {}
        db = _get_db()
        if not db or not db.is_available():
            return cache

        def _clean(name: str) -> str:
            """Strip \r\n and whitespace from DB names."""
            return name.strip().replace("\r\n", "").replace("\r", "").replace("\n", "")

        def _add(key: str, loc_type: str, db_name: str):
            entry = (loc_type, db_name)
            cache.setdefault(key, [])
            if entry not in cache[key]:
                cache[key].append(entry)
            # Also add normalized spelling variant
            norm = _normalize_spelling(key)
            if norm != key:
                cache.setdefault(norm, [])
                if entry not in cache[norm]:
                    cache[norm].append(entry)

        try:
            # Tehsil names (most specific)
            for r in db.fetch_all("SELECT DISTINCT tehsil_name FROM challan_tehsil_drill WHERE tehsil_name != ''"):
                name = _clean(r["tehsil_name"])
                if name:
                    _add(name.lower(), "tehsil", name)

            # District names
            for r in db.fetch_all("SELECT DISTINCT district_name FROM challan_by_district WHERE district_name != ''"):
                name = _clean(r["district_name"])
                if name:
                    _add(name.lower(), "district", name)

            # Division names
            for r in db.fetch_all("SELECT DISTINCT division_name FROM challan_by_division WHERE division_name != ''"):
                name = _clean(r["division_name"])
                if name:
                    _add(name.lower(), "division", name)

            # Officer names (from challan_data detailed records)
            # Officer codes like "EO-091", "SDO-123" are stripped to allow
            # matching when the user types the name without the code suffix.
            _OFFICER_CODE_RE = re.compile(r"\s+[A-Z]{2,5}[-–]\d{2,5}$")
            try:
                for r in db.fetch_all(
                    "SELECT DISTINCT officer_name FROM challan_data "
                    "WHERE officer_name IS NOT NULL AND officer_name != '' "
                    "AND officer_name NOT LIKE 'SDEO%%'"
                ):
                    name = _clean(r["officer_name"])
                    if name and len(name) > 3:  # Skip very short/code names
                        _add(name.lower(), "officer", name)
                        # Also add key without trailing officer code (e.g. "EO-091")
                        name_no_code = _OFFICER_CODE_RE.sub("", name).strip()
                        if name_no_code and name_no_code != name and len(name_no_code) > 3:
                            _add(name_no_code.lower(), "officer", name)
            except Exception:
                pass  # challan_data may not exist yet

            total = sum(len(v) for v in cache.values())
            log.info("Loaded %d location entries (%d keys) for challan lookup",
                     total, len(cache))
        except Exception as e:
            log.error("Failed to load location names: %s", e)

        _location_cache = cache
        return cache


def invalidate_location_cache():
    """Force reload of location cache on next use."""
    global _location_cache
    with _location_lock:
        _location_cache = None


def _pick_best_level(
    entries: List[Tuple[str, str]],
    question: str,
) -> Tuple[str, str]:
    """
    When a name matches multiple hierarchy levels, pick the most specific
    one — UNLESS the user explicitly said 'division' or 'district'.
    Priority: tehsil > district > division (most to least specific).
    """
    q = question.lower()

    # If user explicitly says a level, prefer that
    if any(p.search(q) for p in _DIVISION_PATTERNS):
        for e in entries:
            if e[0] == "division":
                return e
    if any(p.search(q) for p in _DISTRICT_PATTERNS):
        for e in entries:
            if e[0] == "district":
                return e
    if any(p.search(q) for p in _TEHSIL_PATTERNS):
        for e in entries:
            if e[0] == "tehsil":
                return e

    # Officer entries always win when present (they're very specific names)
    for e in entries:
        if e[0] == "officer":
            return e

    # Default: most specific level available
    rank = {"tehsil": 0, "district": 1, "division": 2}
    return min(entries, key=lambda e: rank.get(e[0], 99))


def _find_location_in_question(question: str) -> Optional[Tuple[str, str]]:
    """
    Find a known location name in the question.
    Returns (type, exact_db_name) or None.

    Steps:
      1. Strip prefixes like "tehsil", "district" from the search text
      2. Try exact match (longest first)
      3. Try normalized-spelling match (longest first)
      4. When a name hits multiple levels, use _pick_best_level
    """
    locations = _load_locations()
    if not locations:
        return None

    q_lower = question.lower()
    # Also prepare a version with prefixes stripped for matching
    q_stripped = _STRIP_PREFIXES.sub("", q_lower).strip()
    # And a normalized version
    q_norm = _normalize_spelling(q_lower)
    q_stripped_norm = _normalize_spelling(q_stripped)

    # Collect all candidate search strings
    search_variants = list(dict.fromkeys([q_lower, q_stripped, q_norm, q_stripped_norm]))

    # Sort keys by length descending to match "Lahore Saddar" before "Lahore"
    sorted_keys = sorted(locations.keys(), key=len, reverse=True)

    for key in sorted_keys:
        for variant in search_variants:
            if key in variant:
                entries = locations[key]
                return _pick_best_level(entries, question)

    return None


def _find_all_matches_in_question(question: str) -> List[Tuple[str, str]]:
    """
    Find ALL known names (locations + officers) in the question.
    Returns list of (type, db_name) tuples, sorted by specificity.
    This allows detecting both an officer AND a location in one query.
    """
    locations = _load_locations()
    if not locations:
        return []

    q_lower = question.lower()
    q_stripped = _STRIP_PREFIXES.sub("", q_lower).strip()
    q_norm = _normalize_spelling(q_lower)
    q_stripped_norm = _normalize_spelling(q_stripped)
    search_variants = list(dict.fromkeys([q_lower, q_stripped, q_norm, q_stripped_norm]))

    sorted_keys = sorted(locations.keys(), key=len, reverse=True)

    found: List[Tuple[str, str]] = []
    found_names = set()  # Avoid duplicate names

    for key in sorted_keys:
        for variant in search_variants:
            if key in variant:
                entries = locations[key]
                best = _pick_best_level(entries, question)
                if best[1] not in found_names:
                    found.append(best)
                    found_names.add(best[1])
                break  # Found this key, move to next

    return found


# ── Date Range Detection ────────────────────────────────────

_MONTH_MAP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2,
    "mar": 3, "march": 3, "apr": 4, "april": 4,
    "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "september": 9, "sept": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

# ISO dates: 2026-03-10
_ISO_DATE = re.compile(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b")
# DD/MM/YYYY or DD-MM-YYYY (Pakistani format preferred)
_DMY_DATE = re.compile(r"\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b")
# Ordinal suffix pattern: 1st, 2nd, 3rd, 4th, etc.
_ORD = r"(?:st|nd|rd|th)?"

# "10 March 2026" or "10th March 2026"
_NAMED_DATE1 = re.compile(
    rf"\b(\d{{1,2}}){_ORD}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*[,]?\s+(\d{{4}})\b", re.I
)
# "March 10, 2026" or "March 10th, 2026"
_NAMED_DATE2 = re.compile(
    rf"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\s+(\d{{1,2}}){_ORD}[,]?\s+(\d{{4}})\b", re.I
)
# Range connectors
# English word order: "from X to Y", "between X and Y"
_RANGE_PATTERN = re.compile(
    r"(?:between|from)\s+(.+?)\s+(?:to|and|till|until)\s+(.+?)"
    r"(?:\s*\?|\s*$|\.(?!\d)|,(?!\s*\d))",
    re.I,
)
# Bare "X to Y" without from/between — date-like text on both sides of "to"
_RANGE_PATTERN_BARE = re.compile(
    r"\b(\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*"
    r"(?:\s+\d{4})?)"
    r"\s+(?:to|till|until)\s+"
    r"(\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*"
    r"(?:\s+\d{4})?)\b",
    re.I,
)
# Urdu/Roman-Urdu word order: "X se/sy Y tak/tk"  (date comes before the connector)
_RANGE_PATTERN_URDU = re.compile(
    r"(.+?)\s+(?:se|sy|say)\s+(.+?)\s+(?:tak|tk|tuk)"
    r"(?:\s|[?.!,]|$)",
    re.I,
)
# Word-to-number mapping (English + Roman Urdu)
_WORD_TO_NUM = {
    # English
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "twenty": 20, "thirty": 30,
    # Roman Urdu
    "ek": 1, "ik": 1, "aik": 1, "do": 2, "teen": 3, "char": 4, "panch": 5,
    "chay": 6, "che": 6, "saat": 7, "sat": 7, "aath": 8, "aat": 8,
    "nau": 9, "das": 10, "gyarah": 11, "barah": 12, "terah": 13,
    "choda": 14, "pandrah": 15, "bees": 20, "tees": 30,
}

def _parse_number(text: str) -> Optional[int]:
    """Parse a number from text — supports digits and word numbers."""
    text = text.strip().lower()
    if text.isdigit():
        return int(text)
    return _WORD_TO_NUM.get(text)

# Number pattern for relative dates (digits or word numbers)
_NUM_WORDS = "|".join(_WORD_TO_NUM.keys())
_NUM_PATTERN = rf"(?:\d+|{_NUM_WORDS})"

# Relative dates
_RELATIVE_PATTERNS = {
    "last_week": re.compile(
        r"\b(?:last|previous|guzashta|pichle?|pichy|pichhle?)\s+(?:week|hafta|hafte)\b", re.I
    ),
    "this_week": re.compile(r"\b(?:this|is|current)\s+(?:week|hafta|hafte)\b", re.I),
    "last_month": re.compile(
        r"\b(?:last|previous|guzashta|pichle?|pichy|pichhle?)\s+(?:month|mahina|mahine)\b", re.I
    ),
    "this_month": re.compile(r"\b(?:this|is|current)\s+(?:month|mahina|mahine)\b", re.I),
    "last_n_days": re.compile(
        rf"\b(?:last|past|previous|pichle?|pichy|pichhle?)\s+({_NUM_PATTERN})\s+(?:days?|din)\b", re.I
    ),
    "last_n_weeks": re.compile(
        rf"\b(?:last|past|previous|pichle?|pichy|pichhle?)\s+({_NUM_PATTERN})\s+(?:weeks?|hafta|hafte|hafton)\b", re.I
    ),
    "last_n_months": re.compile(
        rf"\b(?:last|past|previous|pichle?|pichy|pichhle?)\s+({_NUM_PATTERN})\s+(?:months?|mahina|mahine|mahinon)\b", re.I
    ),
    "past_n_days": re.compile(
        rf"\bpast\s+({_NUM_PATTERN})\s+(?:days?|din)\b", re.I
    ),
}


_NAMED_DATE_NO_YEAR1 = re.compile(
    rf"\b(\d{{1,2}}){_ORD}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\b", re.I
)
_NAMED_DATE_NO_YEAR2 = re.compile(
    rf"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\s+(\d{{1,2}}){_ORD}\b", re.I
)
# Month + year only (no day): "February 2025", "jan 2026"
_MONTH_YEAR_ONLY = re.compile(
    r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\s+(\d{4})\b", re.I
)


def _parse_single_date(text: str, default_year: Optional[int] = None,
                       as_end: bool = False) -> Optional[date]:
    """Try to parse a single date from a text fragment.

    If *default_year* is provided, yearless dates like "1 jan" or "feb 10"
    will use that year as a fallback.
    If *as_end* is True and the text is month+year only (e.g. "December 2026"),
    return the last day of that month instead of the first.
    """
    text = text.strip().strip(",").strip()

    # ISO: 2026-03-10
    m = _ISO_DATE.search(text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # DD/MM/YYYY
    m = _DMY_DATE.search(text)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        # If day > 12, it's definitely DD/MM; otherwise assume DD/MM (Pakistani)
        try:
            return date(y, mo, d)
        except ValueError:
            try:
                return date(y, d, mo)
            except ValueError:
                pass

    # "10 March 2026"
    m = _NAMED_DATE1.search(text)
    if m:
        d, mon_str, y = int(m.group(1)), m.group(2).lower()[:3], int(m.group(3))
        mo = _MONTH_MAP.get(mon_str)
        if mo:
            try:
                return date(y, mo, d)
            except ValueError:
                pass

    # "March 10, 2026"
    m = _NAMED_DATE2.search(text)
    if m:
        mon_str, d, y = m.group(1).lower()[:3], int(m.group(2)), int(m.group(3))
        mo = _MONTH_MAP.get(mon_str)
        if mo:
            try:
                return date(y, mo, d)
            except ValueError:
                pass

    # ── Month + Year only: "February 2025", "jan 2026" ──
    m = _MONTH_YEAR_ONLY.search(text)
    if m:
        mon_str = m.group(1).lower()[:3]
        yr = int(m.group(2))
        mo = _MONTH_MAP.get(mon_str)
        if mo:
            import calendar
            if as_end:
                last_day = calendar.monthrange(yr, mo)[1]
                return date(yr, mo, last_day)
            else:
                return date(yr, mo, 1)

    # ── Yearless dates (need default_year) ──
    if default_year:
        # "10 March" or "10 jan"
        m = _NAMED_DATE_NO_YEAR1.search(text)
        if m:
            d, mon_str = int(m.group(1)), m.group(2).lower()[:3]
            mo = _MONTH_MAP.get(mon_str)
            if mo:
                try:
                    return date(default_year, mo, d)
                except ValueError:
                    pass

        # "March 10" or "jan 10"
        m = _NAMED_DATE_NO_YEAR2.search(text)
        if m:
            mon_str, d = m.group(1).lower()[:3], int(m.group(2))
            mo = _MONTH_MAP.get(mon_str)
            if mo:
                try:
                    return date(default_year, mo, d)
                except ValueError:
                    pass

    return None


def _extract_date_range(question: str) -> Optional[Tuple[date, date]]:
    """
    Extract a date range from the question.
    Returns (start_date, end_date) or None.
    """
    today = date.today()

    # 1. Check for relative date patterns first
    for key, pattern in _RELATIVE_PATTERNS.items():
        m = pattern.search(question)
        if m:
            if key == "last_week":
                end = today - timedelta(days=today.weekday() + 1)  # Last Sunday
                start = end - timedelta(days=6)
                return (start, end)
            elif key == "this_week":
                start = today - timedelta(days=today.weekday())  # This Monday
                return (start, today)
            elif key == "last_month":
                first_this = today.replace(day=1)
                last_month_end = first_this - timedelta(days=1)
                last_month_start = last_month_end.replace(day=1)
                return (last_month_start, last_month_end)
            elif key == "this_month":
                return (today.replace(day=1), today)
            elif key in ("last_n_days", "past_n_days"):
                n = _parse_number(m.group(1))
                if n:
                    # "past 7 days" = 7 calendar days including today
                    return (today - timedelta(days=n - 1), today)
            elif key == "last_n_weeks":
                n = _parse_number(m.group(1))
                if n:
                    # "last 2 weeks" = 14 calendar days including today
                    return (today - timedelta(days=n * 7 - 1), today)
            elif key == "last_n_months":
                n = _parse_number(m.group(1))
                if n:
                    # Approximate: 30 days per month, including today
                    return (today - timedelta(days=n * 30 - 1), today)

    # 2. Check for explicit range: "between X to Y", "from X to Y", "X se Y tak"
    #    Must be BEFORE standalone month check so "between 1 jan to 15 jan 2026"
    #    isn't swallowed by the month pattern.
    #    Try both English and Urdu word-order patterns.
    range_match = (_RANGE_PATTERN.search(question)
                   or _RANGE_PATTERN_BARE.search(question)
                   or _RANGE_PATTERN_URDU.search(question))
    if range_match:
        raw_start, raw_end = range_match.group(1).strip(), range_match.group(2).strip()
        start = _parse_single_date(raw_start)
        end = _parse_single_date(raw_end, as_end=True)

        # Handle yearless dates: if one side has no year, borrow from the other
        if start and not end:
            end = _parse_single_date(raw_end, default_year=start.year, as_end=True)
        elif end and not start:
            start = _parse_single_date(raw_start, default_year=end.year)

        if start and end:
            if start > end:
                start, end = end, start
            return (start, end)

    # 3. Check for two standalone dates in the question
    iso_dates = _ISO_DATE.findall(question)
    if len(iso_dates) >= 2:
        dates = []
        for y, mo, d in iso_dates:
            try:
                dates.append(date(int(y), int(mo), int(d)))
            except ValueError:
                pass
        if len(dates) >= 2:
            dates.sort()
            return (dates[0], dates[-1])

    # 3b. Check for a SINGLE specific date: "20 March 2026", "March 20, 2026"
    #     When only one date is found, use it as both start and end (same-day query).
    single_date_match = _NAMED_DATE1.search(question) or _NAMED_DATE2.search(question)
    if single_date_match:
        single = _parse_single_date(single_date_match.group(0))
        if single:
            return (single, single)
    # Also try yearless single dates with current year: "20 march", "march 20"
    if not single_date_match:
        _NAMED_DATE_NO_YEAR1_CHECK = re.compile(
            rf"\b(\d{{1,2}}){_ORD}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\b", re.I
        )
        _NAMED_DATE_NO_YEAR2_CHECK = re.compile(
            rf"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\s+(\d{{1,2}}){_ORD}\b", re.I
        )
        m = _NAMED_DATE_NO_YEAR1_CHECK.search(question)
        if m:
            single = _parse_single_date(m.group(0), default_year=today.year)
            if single:
                return (single, single)
        m = _NAMED_DATE_NO_YEAR2_CHECK.search(question)
        if m:
            single = _parse_single_date(m.group(0), default_year=today.year)
            if single:
                return (single, single)

    # 4. Check for standalone month reference: "january 2026", "march 2026 mein"
    #    This comes AFTER explicit ranges so "between 1 jan to 15 jan 2026"
    #    is handled by the range pattern first.
    _STANDALONE_MONTH = re.compile(
        r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*"
        r"\s+(\d{4})\b", re.I
    )
    _STANDALONE_MONTH2 = re.compile(
        r"\b(\d{4})\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\b",
        re.I,
    )
    m = _STANDALONE_MONTH.search(question)
    if m:
        mon_str = m.group(1).lower()[:3]
        yr = int(m.group(2))
        mo = _MONTH_MAP.get(mon_str)
        if mo:
            import calendar
            last_day = calendar.monthrange(yr, mo)[1]
            return (date(yr, mo, 1), date(yr, mo, last_day))
    m = _STANDALONE_MONTH2.search(question)
    if m:
        yr = int(m.group(1))
        mon_str = m.group(2).lower()[:3]
        mo = _MONTH_MAP.get(mon_str)
        if mo:
            import calendar
            last_day = calendar.monthrange(yr, mo)[1]
            return (date(yr, mo, 1), date(yr, mo, last_day))

    return None


def _strip_dates_from_question(q: str) -> str:
    """Remove date tokens from question so they don't confuse location matching."""
    # Remove ISO dates
    q = _ISO_DATE.sub("", q)
    # Remove DD/MM/YYYY dates
    q = _DMY_DATE.sub("", q)
    # Remove named dates (with year first, then yearless)
    q = _NAMED_DATE1.sub("", q)
    q = _NAMED_DATE2.sub("", q)
    q = _NAMED_DATE_NO_YEAR1.sub("", q)
    q = _NAMED_DATE_NO_YEAR2.sub("", q)
    # Remove standalone month+year references like "january 2026"
    q = re.sub(
        r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\s+\d{4}\b",
        " ", q, flags=re.I,
    )
    q = re.sub(r"\b\d{4}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\w*\b",
               " ", q, flags=re.I)
    # Remove range keywords left behind
    q = re.sub(r"\b(?:between|from|to|and|till|tak|tk|until|se|sy|say)\b", " ", q, flags=re.I)
    # Remove Urdu date context words left behind
    q = re.sub(r"\b(?:waly|wale|wala|mein|ke\s+mahine|month)\b", " ", q, flags=re.I)
    # Remove relative date phrases
    for pattern in _RELATIVE_PATTERNS.values():
        q = pattern.sub("", q)
    # Clean up whitespace
    q = re.sub(r"\s+", " ", q).strip()
    return q


# ── Intent Detection ─────────────────────────────────────────

_CHALLAN_PATTERNS = [
    re.compile(r"\b(?:challan|challans|chalaan|challaan)\b", re.I),
    re.compile(r"\b(?:fine|fines|penalty|penalties)\b", re.I),
    re.compile(r"\b(?:violation|violations|offence|offenses)\b", re.I),
    re.compile(r"\b(?:enforcement\s+action|enforcement\s+data)\b", re.I),
    re.compile(r"\b(?:station|stations)\b", re.I),  # PERA calls locations stations
    re.compile(r"\b(?:officer|officers|imposed\s+by|issued\s+by)\b", re.I),
    # Requisition type names also imply challan context
    re.compile(r"\b(?:price\s*control|encroachment|hoarding|public\s*nuisance|land\s*(?:retrieval|eviction\w*)|eviction\w*)\b", re.I),
    re.compile(r"\b(?:requisition|requisitions)\b", re.I),
    # Roman Urdu / Urdu keywords
    re.compile(r"\b(?:kitne|kitna|kiye|kiya|lagaye|lagaya|kis\s+ne|konsa|ziada|zyada|sabse|sab\s+sy)\b", re.I),
]

_DIVISION_PATTERNS = [
    re.compile(r"\b(?:division|divisions|division[\s-]?wise)\b", re.I),
]

_DISTRICT_PATTERNS = [
    re.compile(r"\b(?:district|districts|district[\s-]?wise)\b", re.I),
]

_TEHSIL_PATTERNS = [
    re.compile(r"\b(?:tehsil|tehsils|tehsil[\s-]?wise)\b", re.I),
]

_TYPE_PATTERNS = [
    re.compile(r"\b(?:type|types|category|categories|requisition)\b", re.I),
    re.compile(r"\b(?:hoarding|price\s+control|encroachment|land\s+(?:eviction|retrieval))\b", re.I),
    re.compile(r"\b(?:public\s+nuisance)\b", re.I),
]

_BREAKDOWN_PATTERNS = [
    re.compile(r"\b(?:breakdown|break[\s-]?down|detail|detailed)\b", re.I),
]

_COMPARISON_PATTERNS = [
    re.compile(r"\bwhich\b.*\bmore\b", re.I),
    re.compile(r"\bwhich\b.*\bmost\b", re.I),
    re.compile(r"\bwhich\b.*\bhighest\b", re.I),
    re.compile(r"\bwhich\b.*\blowest\b", re.I),
    re.compile(r"\btop\b.*\bstation", re.I),
    re.compile(r"\brank\b|\branking\b", re.I),
    re.compile(r"\bcompare\b|\bcomparison\b", re.I),
    # Roman Urdu comparison patterns
    re.compile(r"\bsab\s*se\s*(?:ziada|zyada|ziyada)\b", re.I),   # sab se ziada (most)
    re.compile(r"\bsab\s*se\s*(?:kam|kamm)\b", re.I),              # sab se kam (least)
    re.compile(r"\bkon\s*(?:sa|se|si)?\b.*\b(?:ziada|zyada)\b", re.I),  # konsa ziada
    re.compile(r"\bkis\s*(?:ne)?\b.*\b(?:ziada|zyada|ziyada)\b", re.I), # kisne ziada
    re.compile(r"\b(?:ziada|zyada|ziyada)\b.*\bkis\b", re.I),     # ziada kis
]

# Sub-patterns to detect which status the user asks about in comparisons
_STATUS_PAID = re.compile(r"\bpaid\b", re.I)
_STATUS_UNPAID = re.compile(r"\bunpaid\b", re.I)
_STATUS_OVERDUE = re.compile(r"\boverdue\b", re.I)

# Detect specific requisition type mentioned alongside a location
_REQ_TYPE_MAP = {
    "price_control": re.compile(r"\bprice\s*control\b", re.I),
    "encroachment": re.compile(r"\banti[\s-]*encroachment|encroachment\b", re.I),
    "hoarding": re.compile(r"\banti[\s-]*hoarding|hoarding\b", re.I),
    "land_retrieval": re.compile(r"\bland\s*(?:retrieval|eviction\w*)|eviction\w*\b", re.I),
    "public_nuisance": re.compile(r"\bpublic\s*nuisance\b", re.I),
}


def _detect_single_challan_intent(q: str) -> Optional[str]:
    """Detect a single challan intent from a (sub-)question string."""
    if not q.strip():
        return None
    is_challan = any(p.search(q) for p in _CHALLAN_PATTERNS)
    if not is_challan:
        return None
    return _route_challan_question(q)


def detect_challan_intent(question: str) -> Optional[str]:
    """
    Detect if a question is about challan data.
    Returns a source_id string (which may encode location info) or None.
    Supports compound questions ("X? and Y?") via challan_multi: prefix.

    Priority:
      1. Specific location + type → challan_location_type:type:loc_type:name
      2. Specific location mentioned → challan_location:loc_type:name
      3. Comparison/ranking query → challan_comparison:status
      4. Keyword patterns → specific handler
      5. Default → challan_totals
    """
    q = (question or "").strip()
    if not q:
        return None

    is_challan = any(p.search(q) for p in _CHALLAN_PATTERNS)
    if not is_challan:
        return None

    # Check for compound questions (split by "?" followed by "and"/"&", or by "and which"/"and what")
    _COMPOUND_SPLIT = re.compile(
        r'\?\s*(?:and\s+|&\s*)|(?<=\w)\s+and\s+(?=which\b|what\b|how\b|who\b)',
        re.I,
    )
    parts = _COMPOUND_SPLIT.split(q)
    if len(parts) > 1:
        intents = []
        for part in parts:
            part = part.strip().rstrip("?").strip()
            if not part:
                continue
            intent = _detect_single_challan_intent(part)
            if intent and intent not in intents:
                intents.append(intent)
        if len(intents) > 1:
            return "challan_multi:" + "|".join(intents)
        elif len(intents) == 1:
            return intents[0]

    # Single-question routing (or compound question where splitting failed)
    return _route_challan_question(q)


# ── Challan follow-up patterns (Roman Urdu + English) ──────
_CHALLAN_FOLLOWUP_RE = re.compile(
    r"\b(wo|woh|ye|yeh|us|usk[aeiou]|unk[aeiou]|tha|thi|the|"
    r"that|those|it|them)\b",
    re.I,
)
_OFFICER_FOLLOWUP_RE = re.compile(
    r"\b(?:kis\s+officer|which\s+officer|officer\s+(?:ne|ka|ki|ke)|"
    r"kon(?:sa|se|si)\s+officer|officer\s+name|officer\s+kon)\b",
    re.I,
)
_LOCATION_FOLLOWUP_RE = re.compile(
    r"\b(?:kaha|kidhar|where|location|kahan|kis\s+(?:jagah|jaga|area|station))\b",
    re.I,
)
_REQTYPE_FOLLOWUP_RE = re.compile(
    r"\b(?:kis\s+(?:type|qisam|qism)|which\s+(?:type|category)|"
    r"kya\s+(?:type|qism)|requisition\s+type)\b",
    re.I,
)
_AMOUNT_FOLLOWUP_RE = re.compile(
    r"\b(?:amount|amout|raqam|paisa|paisay|money|fine|fines|"
    r"kitni\s+raqam|kitna\s+(?:paisa|amount)|total\s+(?:amount|fine|raqam)|"
    r"paid\s+amount|outstanding|unpaid|pending\s+amount|"
    r"tell\s+me\s+(?:amount|amout|fine|raqam)|"
    r"amount\s+(?:batao|bata|kya|kitna|kitni))\b",
    re.I,
)
_DETAIL_FOLLOWUP_RE = re.compile(
    r"\b(?:detail[s]?|tafsilat|tafseel|breakdown|summary|"
    r"more\s+(?:info|detail|data)|explain|batao|bata\s+do|"
    r"tell\s+me\s+(?:more|detail|about))\b",
    re.I,
)
_RANKING_FOLLOWUP_RE = re.compile(
    r"\b(?:"
    r"is\s*k[eay]?\s*ba[a]?d|usk[eay]?\s*ba[a]?d|usk[eay]?\s*baad|"   # is k bad, uske baad
    r"phir\s*(?:kis|kon)|aur\s*(?:kis|kon)|"                            # phir kis, aur kon
    r"next|second|third|2nd|3rd|dosr[aeiou]|tesr[aeiou]|"              # next, second, doosra
    r"after\s*(?:him|her|this|that|them)|"                              # after him/this
    r"(?:kis|kon)\s*ne\s*(?:ziada|zyada|ziyada)|"                      # kisne ziada
    r"(?:ziada|zyada|ziyada)\s*(?:kis|kon)|"                           # ziada kisne
    r"number\s*(?:2|3|two|three|do|teen)|"                              # number 2
    r"runner\s*up|(?:sab\s*se\s*)?(?:kam|lowest|least)"                # runner up, sabse kam
    r")\b",
    re.I,
)


def detect_challan_followup(question: str, prev_intent: str) -> Optional[str]:
    """
    Detect if *question* is a follow-up to a previous challan query
    (stored in *prev_intent*).  If so, return a new intent that keeps
    the location/date context but changes the sub-intent.

    Returns None when the question is NOT a challan follow-up.
    """
    if not prev_intent or not prev_intent.startswith("challan_"):
        return None
    q = (question or "").strip()
    if not q:
        return None

    # Must contain a pronoun/reference to the previous answer
    has_ref = _CHALLAN_FOLLOWUP_RE.search(q)
    # Or just a very short follow-up with officer/location keyword
    is_short = len(q.split()) <= 8

    if not has_ref and not is_short:
        return None

    # Determine what the follow-up is asking about
    wants_officer = _OFFICER_FOLLOWUP_RE.search(q) or re.search(
        r"\b(?:officer|officers)\b", q, re.I
    )
    wants_location = _LOCATION_FOLLOWUP_RE.search(q)
    wants_amount = _AMOUNT_FOLLOWUP_RE.search(q)
    wants_detail = _DETAIL_FOLLOWUP_RE.search(q)
    wants_ranking = _RANKING_FOLLOWUP_RE.search(q)

    if not wants_officer and not wants_location and not wants_amount \
       and not wants_detail and not wants_ranking:
        return None  # Can't determine follow-up type

    # Parse the previous intent to extract date-range & location context
    # Formats:
    #   challan_daterange:START:END:SUB_INTENT
    #   challan_location:loc_type:loc_name
    #   challan_location_type:req_key:loc_type:loc_name
    #   challan_officer_at_location:loc_type:loc_name[:req_key]
    #   challan_totals, challan_comparison:...

    date_prefix = ""
    sub_intent = prev_intent

    # Extract date range wrapper if present
    m_dr = re.match(r"^challan_daterange:(\d{4}-\d{2}-\d{2}):(\d{4}-\d{2}-\d{2}):(.+)$", prev_intent)
    if m_dr:
        date_prefix = f"challan_daterange:{m_dr.group(1)}:{m_dr.group(2)}:"
        sub_intent = m_dr.group(3)

    # Extract location from sub_intent
    loc_type = loc_name = req_suffix = ""
    m_loc = re.match(r"challan_(?:location|location_type|officer_at_location):(?:(\w+):)?(\w+):(.+?)(?::(\w+))?$", sub_intent)
    if not m_loc:
        # Try simpler pattern
        m_loc2 = re.match(r"challan_location:(\w+):(.+)", sub_intent)
        if m_loc2:
            loc_type, loc_name = m_loc2.group(1), m_loc2.group(2)

    if m_loc:
        parts = sub_intent.split(":")
        if sub_intent.startswith("challan_location_type:"):
            # challan_location_type:req_key:loc_type:loc_name
            if len(parts) >= 4:
                req_suffix = f":{parts[1]}"
                loc_type, loc_name = parts[2], parts[3]
        elif sub_intent.startswith("challan_officer_at_location:"):
            if len(parts) >= 3:
                loc_type, loc_name = parts[1], parts[2]
                if len(parts) >= 4:
                    req_suffix = f":{parts[3]}"
        elif sub_intent.startswith("challan_location:"):
            if len(parts) >= 3:
                loc_type, loc_name = parts[1], parts[2]

    # For amount/detail/ranking follow-ups, reuse the previous intent
    # so the LLM gets the same context and can answer from it
    if wants_amount or wants_detail or wants_ranking:
        return prev_intent

    if not loc_type or not loc_name:
        return None  # No location context to carry forward

    # Build the new intent based on follow-up type
    if wants_officer:
        new_sub = f"challan_officer_at_location:{loc_type}:{loc_name}{req_suffix}"
    elif wants_location:
        new_sub = f"challan_location:{loc_type}:{loc_name}{req_suffix}"
    else:
        return None

    return f"{date_prefix}{new_sub}" if date_prefix else new_sub


def _route_challan_question(q: str) -> str:
    """Core routing logic for a single challan question.
    Checks for date range first, then delegates to core routing."""
    # Check for date range in the question
    date_range = _extract_date_range(q)
    if date_range:
        start_str = date_range[0].isoformat()
        end_str = date_range[1].isoformat()
        # Strip dates so they don't confuse location detection
        q_clean = _strip_dates_from_question(q)
        sub_intent = _route_challan_question_core(q_clean)
        return f"challan_daterange:{start_str}:{end_str}:{sub_intent}"

    return _route_challan_question_core(q)


def _route_challan_question_core(q: str) -> str:
    """Core routing logic without date-range handling."""
    # Check for specific location/officer names in the question
    all_matches = _find_all_matches_in_question(q)

    officer_match = None
    location_match = None
    for m_type, m_name in all_matches:
        if m_type == "officer" and officer_match is None:
            officer_match = (m_type, m_name)
        elif m_type != "officer" and location_match is None:
            location_match = (m_type, m_name)

    # If officer found → always route to officer handler
    if officer_match:
        return f"challan_officer:{officer_match[1]}"

    # If location found → check if user also asks about officers at that location
    _officer_keyword_re = re.compile(
        r"\b(?:officer|officers|imposed\s+by|issued\s+by|kis\s+officer|konsa\s+officer)\b", re.I
    )
    if location_match:
        loc_type, loc_name = location_match

        # If question also asks about officers → officer ranking at location
        if _officer_keyword_re.search(q):
            # Detect optional requisition type filter
            req_key_found = None
            for req_key, req_pattern in _REQ_TYPE_MAP.items():
                if req_pattern.search(q):
                    req_key_found = req_key
                    break
            req_suffix = f":{req_key_found}" if req_key_found else ""
            return f"challan_officer_at_location:{loc_type}:{loc_name}{req_suffix}"

        # Check if a specific requisition type is also mentioned
        for req_key, req_pattern in _REQ_TYPE_MAP.items():
            if req_pattern.search(q):
                return f"challan_location_type:{req_key}:{loc_type}:{loc_name}"

        return f"challan_location:{loc_type}:{loc_name}"

    # Check for comparison/ranking queries
    is_comparison = any(p.search(q) for p in _COMPARISON_PATTERNS)
    if is_comparison:
        # Determine which status to compare
        if _STATUS_UNPAID.search(q):
            status_key = "unpaid"
        elif _STATUS_OVERDUE.search(q):
            status_key = "overdue"
        else:
            status_key = "paid"

        # Detect ALL matching requisition types
        detected_req_keys = []
        for req_key, req_pattern in _REQ_TYPE_MAP.items():
            if req_pattern.search(q):
                detected_req_keys.append(req_key)

        # Determine which hierarchy level to compare
        _officer_re = re.compile(
            r"\b(?:officer[s]?|imposed\s+by|issued\s+by|"
            r"kis\s+officer|officer\s+ne|"
            r"kon\s*(?:sa|se|si)?\s+officer)\b", re.I
        )
        if _officer_re.search(q):
            level = "officer"
        elif any(p.search(q) for p in _DIVISION_PATTERNS):
            level = "division"
        elif any(p.search(q) for p in _DISTRICT_PATTERNS):
            level = "district"
        else:
            level = "tehsil"

        # If multiple req types detected, return multi-intent
        if len(detected_req_keys) > 1:
            intents = [f"challan_comparison:{status_key}:{level}:{rk}"
                       for rk in detected_req_keys]
            return "challan_multi:" + "|".join(intents)

        req_suffix = f":{detected_req_keys[0]}" if detected_req_keys else ""
        return f"challan_comparison:{status_key}:{level}{req_suffix}"

    # Keyword-based routing
    if any(p.search(q) for p in _TYPE_PATTERNS):
        return "challan_requisition_type"
    if any(p.search(q) for p in _BREAKDOWN_PATTERNS):
        return "challan_tehsil_breakdown"
    if any(p.search(q) for p in _TEHSIL_PATTERNS):
        return "challan_by_tehsil"
    if any(p.search(q) for p in _DISTRICT_PATTERNS):
        return "challan_by_district"
    if any(p.search(q) for p in _DIVISION_PATTERNS):
        return "challan_by_division"

    return "challan_totals"


# ── Lookup Dispatcher ────────────────────────────────────────

_HANDLERS = {}


def _register(source_id: str):
    """Decorator to register a handler function."""
    def decorator(fn):
        _HANDLERS[source_id] = fn
        return fn
    return decorator


def execute_challan_lookup(source_id: str, question: str = "") -> Optional[Dict[str, Any]]:
    """
    Execute a challan lookup from PostgreSQL and return a result dict
    compatible with the answer pipeline.

    source_id can be:
      - Simple: "challan_totals", "challan_by_division", etc.
      - Encoded location: "challan_location:tehsil:Lahore Saddar"
      - Encoded location+type: "challan_location_type:price_control:tehsil:Nankana Sahib"
      - Encoded comparison: "challan_comparison:paid"
    """
    db = _get_db()
    if not db or not db.is_available():
        log.warning("PostgreSQL not available for challan lookup")
        return None

    try:
        # Handle compound multi-intent queries
        if source_id.startswith("challan_multi:"):
            sub_ids = source_id.split(":", 1)[1].split("|")
            merged_records = []
            merged_answer_parts = []
            merged_context_parts = []
            for sub_id in sub_ids:
                sub_result = execute_challan_lookup(sub_id, question)
                if sub_result:
                    merged_records.extend(sub_result.get("records", []))
                    merged_answer_parts.append(
                        sub_result.get("formatted_answer", ""))
                    merged_context_parts.append(
                        sub_result.get("formatted_context", ""))
            if not merged_answer_parts:
                return None
            return _build_result(
                "challan_multi",
                "PERA Challan — Combined Results",
                merged_records,
                "\n\n---\n\n".join(merged_answer_parts),
                "\n\n".join(merged_context_parts),
            )

        # Handle date-range queries
        if source_id.startswith("challan_daterange:"):
            # challan_daterange:START:END:sub_intent
            parts = source_id.split(":", 3)
            start_date = parts[1] if len(parts) > 1 else ""
            end_date = parts[2] if len(parts) > 2 else ""
            sub_intent = parts[3] if len(parts) > 3 else "challan_totals"
            return _lookup_daterange(start_date, end_date, sub_intent, question)

        # Handle encoded source_ids
        if source_id.startswith("challan_officer_at_location:"):
            # challan_officer_at_location:loc_type:loc_name[:req_key]
            parts = source_id.split(":", 3)
            loc_type = parts[1] if len(parts) > 1 else "tehsil"
            loc_name = parts[2] if len(parts) > 2 else ""
            req_key = parts[3] if len(parts) > 3 else None
            return _lookup_officer_at_location(loc_type, loc_name, req_key)

        if source_id.startswith("challan_officer:"):
            # challan_officer:OfficerName
            officer_name = source_id.split(":", 1)[1] if ":" in source_id else ""
            return _lookup_officer(officer_name, question)

        if source_id.startswith("challan_location_type:"):
            # challan_location_type:req_key:loc_type:loc_name
            parts = source_id.split(":", 3)
            if len(parts) == 4:
                _, req_key, loc_type, loc_name = parts
                return _lookup_location_type(req_key, loc_type, loc_name)

        if source_id.startswith("challan_location:"):
            # challan_location:loc_type:loc_name
            parts = source_id.split(":", 2)
            if len(parts) == 3:
                _, loc_type, loc_name = parts
                return _lookup_location(loc_type, loc_name)

        if source_id.startswith("challan_comparison:"):
            # challan_comparison:status:level[:req_key]
            # e.g. challan_comparison:paid:district
            # e.g. challan_comparison:paid:officer:price_control
            parts = source_id.split(":")
            status = parts[1] if len(parts) > 1 else "paid"
            level = parts[2] if len(parts) > 2 else "tehsil"
            req_key = parts[3] if len(parts) > 3 else None
            if level == "officer":
                return _lookup_officer_ranking(status, req_key)
            return _lookup_comparison(status, level, req_key)

        # Simple handlers
        handler = _HANDLERS.get(source_id)
        if not handler:
            log.warning("Unknown challan source_id: %s", source_id)
            return None
        return handler()

    except Exception as exc:
        log.error("Challan lookup failed [%s]: %s", source_id, exc, exc_info=True)
        return None


# ── Helpers ──────────────────────────────────────────────────

def _fnum(val: Any) -> str:
    """Format a number with commas."""
    if isinstance(val, int):
        return f"{val:,}"
    if isinstance(val, float):
        return f"{val:,.0f}"
    try:
        from decimal import Decimal
        if isinstance(val, Decimal):
            if val == int(val):
                return f"{int(val):,}"
            return f"{float(val):,.0f}"
    except Exception:
        pass
    return str(val or 0)


def _pct(part: Any, total: Any) -> str:
    """Calculate percentage string."""
    try:
        p = float(part or 0)
        t = float(total or 0)
        if t > 0:
            return f"{(p / t) * 100:.1f}%"
    except Exception:
        pass
    return "0.0%"


def _build_result(
    source_id: str,
    display_name: str,
    records: List[Dict],
    formatted_answer: str,
    formatted_context: str,
) -> Dict[str, Any]:
    """Build a standard lookup result dict."""
    return {
        "success": True,
        "source_id": source_id,
        "records": records,
        "formatted_answer": formatted_answer,
        "formatted_context": formatted_context,
        "count": len(records),
    }


# ── Handler: Location-Specific ───────────────────────────────

def _lookup_location(loc_type: str, loc_name: str) -> Optional[Dict]:
    """Query challan data for a specific location (tehsil/district/division)."""
    db = _get_db()
    records = []

    if loc_type == "tehsil":
        # Query tehsil drill data
        rows = db.fetch_all(
            "SELECT status, tehsil_name, total_challans, total_amount, district_id "
            "FROM challan_tehsil_drill WHERE TRIM(tehsil_name) = %s "
            "ORDER BY status",
            (loc_name,),
        )
        if rows:
            records = rows
        else:
            # Fallback: check challan_by_tehsil
            rows = db.fetch_all(
                "SELECT status, tehsil_name, count as total_challans "
                "FROM challan_by_tehsil WHERE TRIM(tehsil_name) = %s "
                "ORDER BY status",
                (loc_name,),
            )
            records = rows if rows else []

    elif loc_type == "district":
        rows = db.fetch_all(
            "SELECT cbd.status, cbd.district_name, cbd.total_challans, "
            "       cbd.total_amount, dd.division_name "
            "FROM challan_by_district cbd "
            "LEFT JOIN dim_division dd ON dd.division_id = cbd.division_id "
            "WHERE cbd.district_name = %s ORDER BY cbd.status",
            (loc_name,),
        )
        records = rows if rows else []

    elif loc_type == "division":
        rows = db.fetch_all(
            "SELECT status, division_name, total_challans, total_amount "
            "FROM challan_by_division WHERE division_name = %s "
            "ORDER BY status",
            (loc_name,),
        )
        records = rows if rows else []

    if not records:
        return None

    # Format the response
    lines = [f"**PERA Challan Status — {loc_name}** ({loc_type.title()})\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[API Name: PERA Challan Data for {loc_name}]",
        f"[Location Type: {loc_type}]",
        f"[Location Name: {loc_name}]",
        f"[Total Records: {len(records)}]",
        "",
        f"PERA Challan Status for {loc_name} ({loc_type}):",
        "",
    ]

    grand_challans = 0
    grand_amount = 0

    for r in records:
        st = r.get("status", "unknown")
        challans = r.get("total_challans", r.get("count", 0)) or 0
        amount = r.get("total_amount", 0) or 0
        grand_challans += challans
        grand_amount += float(amount) if amount else 0

    # Now format with percentages
    for r in records:
        st = r.get("status", "unknown")
        challans = r.get("total_challans", r.get("count", 0)) or 0
        amount = r.get("total_amount", 0) or 0
        pct = _pct(challans, grand_challans)

        if amount:
            line = f"- **{st.title()}**: {_fnum(challans)} challans ({pct}) — Rs. {_fnum(amount)}"
        else:
            line = f"- **{st.title()}**: {_fnum(challans)} challans ({pct})"
        lines.append(line)
        ctx_lines.append(line.replace("**", ""))

    # Add total line
    lines.append(f"\n- **Grand Total**: {_fnum(grand_challans)} challans — Rs. {_fnum(grand_amount)}")
    ctx_lines.append(f"")
    ctx_lines.append(f"Grand Total: {_fnum(grand_challans)} challans — Rs. {_fnum(grand_amount)}")

    # Add parent context if available
    if loc_type == "tehsil" and records and records[0].get("district_id"):
        dist_id = records[0]["district_id"]
        dist = db.fetch_one(
            "SELECT district_name FROM dim_district WHERE district_id = %s",
            (dist_id,),
        )
        if dist:
            lines.append(f"\n*District: {dist['district_name']}*")
            ctx_lines.append(f"District: {dist['district_name']}")

    if loc_type == "district" and records and records[0].get("division_name"):
        lines.append(f"\n*Division: {records[0]['division_name']}*")
        ctx_lines.append(f"Division: {records[0]['division_name']}")

    return _build_result(
        "challan_location", f"PERA Challan — {loc_name}",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Location + Requisition Type ─────────────────────

def _lookup_location_type(req_key: str, loc_type: str, loc_name: str) -> Optional[Dict]:
    """Query challan breakdown data for a specific location AND requisition type."""
    db = _get_db()

    # Map req_key to column name in challan_tehsil_breakdown
    col_map = {
        "price_control": "price_control_count",
        "encroachment": "encroachment_count",
        "hoarding": "hoarding_count",
        "land_retrieval": "land_retrieval_count",
        "public_nuisance": "public_nuisance_count",
    }
    count_col = col_map.get(req_key, "total_requisitions")
    display_type = req_key.replace("_", " ").title()

    records = []

    if loc_type == "tehsil":
        # Query tehsil breakdown for this specific tehsil and type
        rows = db.fetch_all(
            "SELECT status, tehsil_name, total_requisitions, "
            "       hoarding_count, price_control_count, "
            "       encroachment_count, land_retrieval_count, "
            "       public_nuisance_count "
            "FROM challan_tehsil_breakdown WHERE TRIM(tehsil_name) = %s "
            "ORDER BY status",
            (loc_name,),
        )
        records = rows if rows else []

        # If no breakdown data, fall back to tehsil drill with a note
        if not records:
            drill_rows = db.fetch_all(
                "SELECT status, tehsil_name, total_challans, total_amount "
                "FROM challan_tehsil_drill WHERE TRIM(tehsil_name) = %s "
                "ORDER BY status",
                (loc_name,),
            )
            if drill_rows:
                records = drill_rows
                # Return general data with a note that type-level breakdown isn't available
                lines = [f"**PERA Challans — {loc_name}** ({display_type})\n"]
                lines.append(f"_Note: Detailed {display_type} breakdown not available for {loc_name}. Showing overall challan data._\n")
                ctx_lines = [
                    "[Source Type: API]",
                    f"[API Name: PERA Challan Data for {loc_name}]",
                    f"[Location: {loc_name}]",
                    f"[Requisition Type Requested: {display_type}]",
                    f"[Note: Type-level breakdown not available, showing overall data]",
                    "",
                ]
                grand = 0
                for r in drill_rows:
                    st = r.get("status", "unknown")
                    c = r.get("total_challans", 0) or 0
                    a = r.get("total_amount", 0) or 0
                    grand += c
                    line = f"- {st.title()}: {_fnum(c)} challans (Rs. {_fnum(a)})"
                    lines.append(line)
                    ctx_lines.append(line)
                lines.append(f"\nTotal: {_fnum(grand)} challans")
                ctx_lines.append(f"Total: {_fnum(grand)} challans")
                return _build_result(
                    "challan_location", f"PERA Challan — {loc_name} ({display_type})",
                    records, "\n".join(lines), "\n".join(ctx_lines),
                )

    elif loc_type in ("district", "division"):
        # For district/division, aggregate breakdown data from all tehsils
        if loc_type == "district":
            # Get tehsil IDs for this district
            rows = db.fetch_all(
                "SELECT ctb.status, ctb.tehsil_name, ctb.total_requisitions, "
                f"       ctb.{count_col} as type_count, "
                "       ctb.hoarding_count, ctb.price_control_count, "
                "       ctb.encroachment_count, ctb.land_retrieval_count, "
                "       ctb.public_nuisance_count "
                "FROM challan_tehsil_breakdown ctb "
                "JOIN dim_tehsil dt ON TRIM(dt.tehsil_name) = TRIM(ctb.tehsil_name) "
                "JOIN dim_district dd ON dd.district_id = dt.district_id "
                "WHERE dd.district_name = %s "
                "ORDER BY ctb.tehsil_name, ctb.status",
                (loc_name,),
            )
        else:
            rows = db.fetch_all(
                "SELECT ctb.status, ctb.tehsil_name, ctb.total_requisitions, "
                f"       ctb.{count_col} as type_count, "
                "       ctb.hoarding_count, ctb.price_control_count, "
                "       ctb.encroachment_count, ctb.land_retrieval_count, "
                "       ctb.public_nuisance_count "
                "FROM challan_tehsil_breakdown ctb "
                "JOIN dim_tehsil dt ON TRIM(dt.tehsil_name) = TRIM(ctb.tehsil_name) "
                "JOIN dim_district dd ON dd.district_id = dt.district_id "
                "JOIN dim_division dv ON dv.division_id = dd.division_id "
                "WHERE dv.division_name = %s "
                "ORDER BY ctb.tehsil_name, ctb.status",
                (loc_name,),
            )
        records = rows if rows else []

    if not records:
        # Fall back to regular location lookup
        return _lookup_location(loc_type, loc_name)

    # Format breakdown response
    lines = [f"**PERA Challans — {loc_name}** ({display_type} Requisitions)\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[API Name: PERA Challan Breakdown for {loc_name}]",
        f"[Location: {loc_name} ({loc_type})]",
        f"[Requisition Type: {display_type}]",
        f"[Total Records: {len(records)}]",
        "",
        f"PERA Challan {display_type} Breakdown for {loc_name}:",
        "",
    ]

    grand_type = 0
    grand_total = 0
    for r in records:
        st = r.get("status", "unknown")
        type_count = r.get("type_count", r.get(count_col, 0)) or 0
        total_req = r.get("total_requisitions", 0) or 0
        tehsil = r.get("tehsil_name", loc_name)
        grand_type += type_count
        grand_total += total_req

        line = f"- **{tehsil}** ({st.title()}): {_fnum(type_count)} {display_type} out of {_fnum(total_req)} total requisitions"
        lines.append(line)
        ctx_lines.append(line.replace("**", ""))

    lines.append(f"\n**Total {display_type}**: {_fnum(grand_type)} out of {_fnum(grand_total)} total requisitions")
    ctx_lines.append(f"")
    ctx_lines.append(f"Total {display_type}: {_fnum(grand_type)} out of {_fnum(grand_total)} total requisitions")

    return _build_result(
        "challan_location", f"PERA Challan — {loc_name} ({display_type})",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Date-Range Queries ─────────────────────────────

def _lookup_daterange(start_str: str, end_str: str,
                      sub_intent: str, question: str = "") -> Optional[Dict]:
    """
    Query challan_data filtered by action_date range.
    Uses DISTINCT ON (challan_id) to avoid double-counting across snapshots.

    sub_intent encodes what kind of query within the date range:
      - challan_totals → overall stats
      - challan_location:tehsil:Name → stats for a specific location
      - challan_officer:Name → stats for a specific officer
      - challan_comparison:status:level → ranked comparison
      - challan_officer_at_location:... → officer ranking at a location
      etc.
    """
    db = _get_db()
    if not db:
        return None

    try:
        start_date = date.fromisoformat(start_str)
        end_date = date.fromisoformat(end_str)
    except (ValueError, TypeError):
        return None

    date_label = f"{start_date.strftime('%d %b %Y')} — {end_date.strftime('%d %b %Y')}"

    # CTE to deduplicate: keep latest snapshot per challan within date range
    # Use < end_date + 1 day to include the full end day (timestamps have time component)
    dedup_cte = (
        "WITH latest_challan AS ( "
        "  SELECT DISTINCT ON (challan_id) * "
        "  FROM challan_data "
        "  WHERE action_date >= %s "
        "    AND action_date < (%s::date + INTERVAL '1 day') "
        "    AND action_date IS NOT NULL "
        "  ORDER BY challan_id, snapshot_date DESC "
        ") "
    )
    base_params: list = [start_str, end_str]

    # ── Route based on sub_intent ───────────────────────
    # 1. Overall totals for date range
    if sub_intent == "challan_totals" or sub_intent == "":
        rows = db.fetch_all(
            dedup_cte +
            "SELECT status, COUNT(*) AS total_challans, "
            "       SUM(fine_amount) AS total_fine, "
            "       SUM(paid_amount) AS total_paid, "
            "       SUM(outstanding_amount) AS total_outstanding "
            "FROM latest_challan "
            "GROUP BY status ORDER BY status",
            tuple(base_params),
        )
        return _format_daterange_summary(
            rows, date_label, f"Overall Challan Summary ({date_label})",
            note="[IMPORTANT: Each challan counted ONCE via deduplication of snapshot rows.]")

    # 2. Location-specific for date range
    if sub_intent.startswith("challan_location:"):
        parts = sub_intent.split(":", 2)
        loc_type = parts[1] if len(parts) > 1 else "tehsil"
        loc_name = parts[2] if len(parts) > 2 else ""
        loc_col = {"tehsil": "tehsil_name", "district": "district_name",
                    "division": "division_name"}.get(loc_type, "tehsil_name")
        rows = db.fetch_all(
            dedup_cte +
            f"SELECT status, COUNT(*) AS total_challans, "
            "       SUM(fine_amount) AS total_fine, "
            "       SUM(paid_amount) AS total_paid, "
            "       SUM(outstanding_amount) AS total_outstanding "
            f"FROM latest_challan WHERE {loc_col} ILIKE %s "
            "GROUP BY status ORDER BY status",
            tuple(base_params + [f"%{loc_name}%"]),
        )
        # Also fetch per-officer breakdown for richer context
        officer_rows = db.fetch_all(
            dedup_cte +
            f"SELECT officer_name, status, COUNT(*) AS cnt, "
            "       SUM(fine_amount) AS fine "
            f"FROM latest_challan WHERE {loc_col} ILIKE %s "
            "AND officer_name IS NOT NULL AND officer_name != '' "
            "GROUP BY officer_name, status ORDER BY COUNT(*) DESC",
            tuple(base_params + [f"%{loc_name}%"]),
        )
        return _format_daterange_location_detail(
            rows, officer_rows, date_label,
            f"Challan Summary for {loc_name} ({date_label})", loc_name)

    # 3. Location + req type for date range
    if sub_intent.startswith("challan_location_type:"):
        parts = sub_intent.split(":", 3)
        req_key = parts[1] if len(parts) > 1 else ""
        loc_type = parts[2] if len(parts) > 2 else "tehsil"
        loc_name = parts[3] if len(parts) > 3 else ""
        loc_col = {"tehsil": "tehsil_name", "district": "district_name",
                    "division": "division_name"}.get(loc_type, "tehsil_name")
        req_db = _REQ_KEY_TO_DB_NAME.get(req_key, "")
        extra_params = [f"%{loc_name}%"]
        req_filter = ""
        if req_db:
            req_filter = "AND requisition_type_name = %s "
            extra_params.append(req_db)
        rows = db.fetch_all(
            dedup_cte +
            f"SELECT status, COUNT(*) AS total_challans, "
            "       SUM(fine_amount) AS total_fine, "
            "       SUM(paid_amount) AS total_paid "
            f"FROM latest_challan WHERE {loc_col} ILIKE %s "
            f"{req_filter}"
            "GROUP BY status ORDER BY status",
            tuple(base_params + extra_params),
        )
        req_label = f" — {req_db}" if req_db else ""
        return _format_daterange_summary(
            rows, date_label,
            f"Challan Summary for {loc_name}{req_label} ({date_label})")

    # 4. Officer-specific for date range
    if sub_intent.startswith("challan_officer:"):
        officer_name = sub_intent.split(":", 1)[1] if ":" in sub_intent else ""
        name_param = f"%{officer_name}%"
        # Summary rows (grouped by status)
        rows = db.fetch_all(
            dedup_cte +
            "SELECT officer_name, status, COUNT(*) AS total_challans, "
            "       SUM(fine_amount) AS total_fine, "
            "       SUM(paid_amount) AS total_paid, "
            "       SUM(outstanding_amount) AS total_outstanding "
            "FROM latest_challan WHERE officer_name ILIKE %s "
            "GROUP BY officer_name, status ORDER BY status",
            tuple(base_params + [name_param]),
        )
        # Also fetch individual challan details for accuracy
        detail_rows = db.fetch_all(
            dedup_cte +
            "SELECT challan_id, officer_name, status, fine_amount, "
            "       paid_amount, outstanding_amount, action_date, "
            "       tehsil_name, district_name, requisition_type_name "
            "FROM latest_challan WHERE officer_name ILIKE %s "
            "ORDER BY action_date",
            tuple(base_params + [name_param]),
        )
        return _format_daterange_officer_detail(
            rows, detail_rows, date_label,
            f"Officer {officer_name} — Challans ({date_label})")

    # 5. Officer ranking at location for date range
    if sub_intent.startswith("challan_officer_at_location:"):
        parts = sub_intent.split(":", 3)
        loc_type = parts[1] if len(parts) > 1 else "tehsil"
        loc_name = parts[2] if len(parts) > 2 else ""
        req_key = parts[3] if len(parts) > 3 else None
        loc_col = {"tehsil": "tehsil_name", "district": "district_name",
                    "division": "division_name"}.get(loc_type, "tehsil_name")
        extra_params = [f"%{loc_name}%"]
        req_filter = ""
        req_label = ""
        if req_key and req_key in _REQ_KEY_TO_DB_NAME:
            req_filter = "AND requisition_type_name = %s "
            extra_params.append(_REQ_KEY_TO_DB_NAME[req_key])
            req_label = f" — {_REQ_KEY_TO_DB_NAME[req_key]}"
        rows = db.fetch_all(
            dedup_cte +
            "SELECT officer_name, COUNT(*) AS total_challans, "
            "       SUM(CASE WHEN LOWER(status)='paid' THEN 1 ELSE 0 END) AS paid_count, "
            "       SUM(CASE WHEN LOWER(status)='unpaid' THEN 1 ELSE 0 END) AS unpaid_count, "
            "       SUM(CASE WHEN LOWER(status)='overdue' THEN 1 ELSE 0 END) AS overdue_count, "
            "       SUM(fine_amount) AS total_fine "
            f"FROM latest_challan WHERE {loc_col} ILIKE %s {req_filter}"
            "AND officer_name IS NOT NULL AND officer_name != '' "
            "GROUP BY officer_name ORDER BY total_challans DESC",
            tuple(base_params + extra_params),
        )
        return _format_daterange_officers(
            rows, date_label,
            f"Officers in {loc_name}{req_label} ({date_label})")

    # 6. Comparison/ranking for date range
    if sub_intent.startswith("challan_comparison:"):
        parts = sub_intent.split(":")
        status_key = parts[1] if len(parts) > 1 else "paid"
        level = parts[2] if len(parts) > 2 else "tehsil"
        req_key = parts[3] if len(parts) > 3 else None

        if level == "officer":
            req_filter = ""
            extra_params = []
            req_label = ""
            if req_key and req_key in _REQ_KEY_TO_DB_NAME:
                req_filter = "AND requisition_type_name = %s "
                extra_params.append(_REQ_KEY_TO_DB_NAME[req_key])
                req_label = f" — {_REQ_KEY_TO_DB_NAME[req_key]}"
            rows = db.fetch_all(
                dedup_cte +
                "SELECT officer_name, COUNT(*) AS total_challans, "
                "       SUM(CASE WHEN LOWER(status)='paid' THEN 1 ELSE 0 END) AS paid_count, "
                "       SUM(CASE WHEN LOWER(status)='unpaid' THEN 1 ELSE 0 END) AS unpaid_count, "
                "       SUM(CASE WHEN LOWER(status)='overdue' THEN 1 ELSE 0 END) AS overdue_count, "
                "       SUM(fine_amount) AS total_fine "
                f"FROM latest_challan WHERE officer_name IS NOT NULL "
                f"AND officer_name != '' {req_filter}"
                "GROUP BY officer_name ORDER BY total_challans DESC",
                tuple(base_params + extra_params),
            )
            return _format_daterange_officers(
                rows, date_label,
                f"Officer Ranking{req_label} ({date_label})")
        else:
            name_col = {"division": "division_name", "district": "district_name"
                        }.get(level, "tehsil_name")
            req_filter = ""
            extra_params = []
            req_label = ""
            if req_key and req_key in _REQ_KEY_TO_DB_NAME:
                req_filter = "AND requisition_type_name = %s "
                extra_params.append(_REQ_KEY_TO_DB_NAME[req_key])
                req_label = f" — {_REQ_KEY_TO_DB_NAME[req_key]}"
            rows = db.fetch_all(
                dedup_cte +
                f"SELECT {name_col} AS name, COUNT(*) AS total_challans, "
                "       SUM(fine_amount) AS total_fine, "
                "       SUM(CASE WHEN LOWER(status)='paid' THEN 1 ELSE 0 END) AS paid_count "
                f"FROM latest_challan WHERE {name_col} IS NOT NULL "
                f"AND {name_col} != '' {req_filter}"
                f"GROUP BY {name_col} ORDER BY total_challans DESC",
                tuple(base_params + extra_params),
            )
            level_label = {"division": "Division", "district": "District"
                           }.get(level, "Tehsil")
            return _format_daterange_comparison(
                rows, date_label, level_label, req_label)

    # Fallback: overall totals
    rows = db.fetch_all(
        dedup_cte +
        "SELECT status, COUNT(*) AS total_challans, "
        "       SUM(fine_amount) AS total_fine, "
        "       SUM(paid_amount) AS total_paid "
        "FROM latest_challan "
        "GROUP BY status ORDER BY status",
        tuple(base_params),
    )
    return _format_daterange_summary(
        rows, date_label, f"Challan Summary ({date_label})")


def _format_daterange_summary(rows, date_label: str, title: str,
                              note: str = "") -> Optional[Dict]:
    """Format a date-range summary (status breakdown).
    Combines 'unpaid' and 'overdue' into a single 'Unpaid' category to match PERA dashboard.
    """
    if not rows:
        no_msg = f"No challan records found for the period {date_label}."
        ctx = (f"[Source Type: API]\n[Date Range: {date_label}]\n"
               f"[Result: NO DATA FOUND]\n\n{no_msg}")
        return _build_result("challan_daterange", title, [], no_msg, ctx)

    # Aggregate into two categories: Paid vs Unpaid (= unpaid + overdue)
    paid_cnt = 0; paid_fine = 0.0; paid_paid = 0.0
    unpaid_cnt = 0; unpaid_fine = 0.0; unpaid_outstanding = 0.0
    for r in rows:
        st = (r.get("status") or "unknown").lower()
        cnt = r.get("total_challans", 0) or 0
        fine = float(r.get("total_fine", 0) or 0)
        paid_amt = float(r.get("total_paid", 0) or 0)
        outstanding = float(r.get("total_outstanding", 0) or 0)
        if st == "paid":
            paid_cnt += cnt; paid_fine += fine; paid_paid += paid_amt
        else:  # unpaid + overdue → Unpaid
            unpaid_cnt += cnt; unpaid_fine += fine; unpaid_outstanding += outstanding

    grand_total = paid_cnt + unpaid_cnt
    grand_fine = paid_fine + unpaid_fine

    lines = [f"**{title}**\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[API Name: PERA Challan Date-Range Query]",
        f"[Date Range: {date_label}]",
    ]
    if note:
        ctx_lines.append(note)
    ctx_lines.extend(["", f"{title}:", ""])

    # Paid
    lines.append(f"- **Paid**: {_fnum(paid_cnt)} challans | Amount Paid: Rs. {_fnum(paid_paid)}")
    ctx_lines.append(f"Paid: {_fnum(paid_cnt)} challans | Amount Paid: Rs. {_fnum(paid_paid)}")
    # Unpaid (combined)
    lines.append(f"- **Unpaid**: {_fnum(unpaid_cnt)} challans | Outstanding Amount: Rs. {_fnum(unpaid_outstanding)}")
    ctx_lines.append(f"Unpaid: {_fnum(unpaid_cnt)} challans | Outstanding Amount: Rs. {_fnum(unpaid_outstanding)}")

    lines.append(f"\n**Total**: {_fnum(grand_total)} challans | Total Fine: Rs. {_fnum(grand_fine)}"
                 f" | Paid: Rs. {_fnum(paid_paid)} | Unpaid/Outstanding: Rs. {_fnum(unpaid_outstanding)}")
    ctx_lines.append(f"\nTotal: {_fnum(grand_total)} challans | Total Fine: Rs. {_fnum(grand_fine)}"
                     f" | Paid: Rs. {_fnum(paid_paid)} | Unpaid/Outstanding: Rs. {_fnum(unpaid_outstanding)}")

    return _build_result(
        "challan_daterange", title,
        rows, "\n".join(lines), "\n".join(ctx_lines),
    )


def _format_daterange_officers(rows, date_label: str, title: str) -> Optional[Dict]:
    """Format date-range officer ranking."""
    if not rows:
        no_msg = f"No officer challan records found for the period {date_label}."
        ctx = (f"[Source Type: API]\n[Date Range: {date_label}]\n"
               f"[Result: NO DATA FOUND]\n\n{no_msg}")
        return _build_result("challan_daterange", title, [], no_msg, ctx)

    lines = [f"**{title}**\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[API Name: PERA Officer Date-Range Query]",
        f"[Date Range: {date_label}]",
        f"[Total Officers: {len(rows)}]",
        "", f"{title}:", "",
    ]
    for i, r in enumerate(rows[:20], 1):
        name = (r.get("officer_name") or "Unknown").strip()
        total = r.get("total_challans", 0) or 0
        paid = r.get("paid_count", 0) or 0
        unpaid = r.get("unpaid_count", 0) or 0
        overdue = r.get("overdue_count", 0) or 0
        fine = r.get("total_fine", 0) or 0
        ratio = (paid / total * 100) if total > 0 else 0
        line = (f"{i}. **{name}**: {_fnum(total)} challans "
                f"(Paid: {_fnum(paid)} | Unpaid: {_fnum(unpaid)} | Overdue: {_fnum(overdue)}) "
                f"— Paid Ratio: {ratio:.1f}% — Fine: Rs. {_fnum(fine)}")
        lines.append(line)
        ctx_lines.append(f"{i}. {name}: {_fnum(total)} challans "
                         f"(Paid: {_fnum(paid)} | Unpaid: {_fnum(unpaid)} | Overdue: {_fnum(overdue)}) "
                         f"Paid Ratio: {ratio:.1f}% Fine: Rs. {_fnum(fine)}")

    grand = sum(r.get("total_challans", 0) or 0 for r in rows)
    lines.append(f"\n**Grand Total**: {_fnum(grand)} challans by {len(rows)} officers")
    ctx_lines.append(f"\nGrand Total: {_fnum(grand)} challans by {len(rows)} officers")

    return _build_result("challan_daterange", title,
                         rows, "\n".join(lines), "\n".join(ctx_lines))


def _format_daterange_comparison(rows, date_label: str,
                                  level_label: str, req_label: str) -> Optional[Dict]:
    """Format date-range location comparison ranking."""
    title = f"{level_label} Ranking{req_label} ({date_label})"
    if not rows:
        no_msg = f"No challan records found for {level_label.lower()}s{req_label} in {date_label}."
        ctx = (f"[Source Type: API]\n[Date Range: {date_label}]\n"
               f"[Result: NO DATA FOUND]\n\n{no_msg}")
        return _build_result("challan_daterange", title, [], no_msg, ctx)

    lines = [f"**PERA {level_label}s Ranked by Challans{req_label} ({date_label})**\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[Date Range: {date_label}]",
        f"[Level: {level_label}]",
        f"[Total: {len(rows)}]", "",
    ]
    for i, r in enumerate(rows[:20], 1):
        name = (r.get("name") or "Unknown").strip()
        total = r.get("total_challans", 0) or 0
        fine = r.get("total_fine", 0) or 0
        paid = r.get("paid_count", 0) or 0
        ratio = (paid / total * 100) if total > 0 else 0
        line = f"{i}. **{name}**: {_fnum(total)} challans (Paid Ratio: {ratio:.1f}%) (Rs. {_fnum(fine)})"
        lines.append(line)
        ctx_lines.append(f"{i}. {name}: {_fnum(total)} challans (Paid Ratio: {ratio:.1f}%) (Rs. {_fnum(fine)})")

    grand = sum(r.get("total_challans", 0) or 0 for r in rows)
    lines.append(f"\n**Grand Total**: {_fnum(grand)} challans across {len(rows)} {level_label.lower()}s")
    ctx_lines.append(f"\nGrand Total: {_fnum(grand)} challans across {len(rows)} {level_label.lower()}s")

    return _build_result("challan_daterange", title,
                         rows, "\n".join(lines), "\n".join(ctx_lines))


def _format_daterange_officer_detail(
    summary_rows, detail_rows, date_label: str, title: str,
) -> Optional[Dict]:
    """Format an officer date-range query with per-challan details for accuracy."""
    if not summary_rows and not detail_rows:
        no_msg = f"No challan records found for the period {date_label}."
        ctx = (f"[Source Type: API]\n[Date Range: {date_label}]\n"
               f"[Result: NO DATA FOUND]\n\n{no_msg}")
        return _build_result("challan_daterange", title, [], no_msg, ctx)

    # Build summary
    grand_total = 0
    grand_fine = 0.0
    grand_paid = 0.0
    grand_outstanding = 0.0
    officer_full = ""
    lines = [f"**{title}**\n"]
    ctx_lines = [
        "[Source Type: API]",
        "[API Name: PERA Challan Date-Range Query]",
        f"[Date Range: {date_label}]",
        "[IMPORTANT: These numbers are from database records with duplicate snapshots"
        " removed. Each challan is counted ONCE using the latest snapshot.]",
        "", f"{title}:", "",
    ]
    for r in summary_rows:
        st = r.get("status", "unknown")
        cnt = r.get("total_challans", 0) or 0
        fine = r.get("total_fine", 0) or 0
        paid = r.get("total_paid", 0) or 0
        outstanding = r.get("total_outstanding", 0) or 0
        if not officer_full:
            officer_full = (r.get("officer_name") or "").strip()
        grand_total += cnt
        grand_fine += float(fine)
        grand_paid += float(paid)
        grand_outstanding += float(outstanding)
        line = f"- **{st.title()}**: {_fnum(cnt)} challans | Fine: Rs. {_fnum(fine)}"
        if paid:
            line += f" | Paid: Rs. {_fnum(paid)}"
        if outstanding:
            line += f" | Outstanding: Rs. {_fnum(outstanding)}"
        lines.append(line)
        ctx_lines.append(
            f"{st.title()}: {_fnum(cnt)} challans | Fine: Rs. {_fnum(fine)}"
            + (f" | Paid: Rs. {_fnum(paid)}" if paid else "")
            + (f" | Outstanding: Rs. {_fnum(outstanding)}" if outstanding else "")
        )

    if officer_full:
        ctx_lines.insert(4, f"[Officer Full Name: {officer_full}]")

    lines.append(f"\n**Total**: {_fnum(grand_total)} challans | "
                 f"Fine: Rs. {_fnum(grand_fine)} | Paid: Rs. {_fnum(grand_paid)}"
                 f" | Outstanding: Rs. {_fnum(grand_outstanding)}")
    ctx_lines.append(f"\nTotal: {_fnum(grand_total)} challans | "
                     f"Fine: Rs. {_fnum(grand_fine)} | Paid: Rs. {_fnum(grand_paid)}"
                     f" | Outstanding: Rs. {_fnum(grand_outstanding)}")

    # Add individual challan details (for ≤50 challans)
    if detail_rows and len(detail_rows) <= 50:
        ctx_lines.append(f"\n--- Individual Challan Details ({len(detail_rows)} challans) ---")
        for i, d in enumerate(detail_rows, 1):
            cid = (d.get("challan_id") or "")[:12]
            st = d.get("status", "?")
            fine = d.get("fine_amount", 0) or 0
            paid = d.get("paid_amount", 0) or 0
            out = d.get("outstanding_amount", 0) or 0
            act = str(d.get("action_date", ""))[:10]
            loc = d.get("tehsil_name") or d.get("district_name") or ""
            req = d.get("requisition_type_name") or ""
            ctx_lines.append(
                f"  {i}. [{act}] Status: {st} | Fine: Rs.{_fnum(fine)} | "
                f"Paid: Rs.{_fnum(paid)} | Outstanding: Rs.{_fnum(out)}"
                + (f" | Location: {loc}" if loc else "")
                + (f" | Type: {req}" if req else "")
            )

    all_rows = summary_rows + (detail_rows or [])
    return _build_result(
        "challan_daterange", title,
        all_rows, "\n".join(lines), "\n".join(ctx_lines),
    )


def _format_daterange_location_detail(
    summary_rows, officer_rows, date_label: str, title: str, loc_name: str,
) -> Optional[Dict]:
    """Format a location date-range query with per-officer breakdown for accuracy.
    Combines 'unpaid' and 'overdue' into a single 'Unpaid' category to match PERA dashboard.
    """
    if not summary_rows:
        no_msg = f"No challan records found for {loc_name} in the period {date_label}."
        ctx = (f"[Source Type: API]\n[Date Range: {date_label}]\n"
               f"[Result: NO DATA FOUND]\n\n{no_msg}")
        return _build_result("challan_daterange", title, [], no_msg, ctx)

    # Aggregate into two categories: Paid vs Unpaid (= unpaid + overdue)
    paid_cnt = 0; paid_fine = 0.0; paid_paid = 0.0
    unpaid_cnt = 0; unpaid_fine = 0.0; unpaid_outstanding = 0.0
    for r in summary_rows:
        st = (r.get("status") or "unknown").lower()
        cnt = r.get("total_challans", 0) or 0
        fine = float(r.get("total_fine", 0) or 0)
        paid_amt = float(r.get("total_paid", 0) or 0)
        outstanding = float(r.get("total_outstanding", 0) or 0)
        if st == "paid":
            paid_cnt += cnt; paid_fine += fine; paid_paid += paid_amt
        else:  # unpaid + overdue → Unpaid
            unpaid_cnt += cnt; unpaid_fine += fine; unpaid_outstanding += outstanding

    grand_total = paid_cnt + unpaid_cnt
    grand_fine = paid_fine + unpaid_fine

    lines = [f"**{title}**\n"]
    ctx_lines = [
        "[Source Type: API]",
        "[API Name: PERA Challan Date-Range Query]",
        f"[Date Range: {date_label}]",
        f"[Location: {loc_name}]",
        "[IMPORTANT: These numbers are from database records with duplicate snapshots"
        " removed. Each challan is counted ONCE using the latest snapshot.]",
        "", f"{title}:", "",
    ]

    # Paid
    lines.append(f"- **Paid**: {_fnum(paid_cnt)} challans | Amount Paid: Rs. {_fnum(paid_paid)}")
    ctx_lines.append(f"Paid: {_fnum(paid_cnt)} challans | Amount Paid: Rs. {_fnum(paid_paid)}")
    # Unpaid (combined unpaid + overdue)
    lines.append(f"- **Unpaid**: {_fnum(unpaid_cnt)} challans | Outstanding Amount: Rs. {_fnum(unpaid_outstanding)}")
    ctx_lines.append(f"Unpaid: {_fnum(unpaid_cnt)} challans | Outstanding Amount: Rs. {_fnum(unpaid_outstanding)}")

    lines.append(f"\n**Total**: {_fnum(grand_total)} challans | "
                 f"Total Fine: Rs. {_fnum(grand_fine)} | Paid: Rs. {_fnum(paid_paid)}"
                 f" | Unpaid/Outstanding: Rs. {_fnum(unpaid_outstanding)}")
    ctx_lines.append(f"\nTotal: {_fnum(grand_total)} challans | "
                     f"Total Fine: Rs. {_fnum(grand_fine)} | Paid: Rs. {_fnum(paid_paid)}"
                     f" | Unpaid/Outstanding: Rs. {_fnum(unpaid_outstanding)}")

    # Add per-officer breakdown
    if officer_rows:
        # Aggregate officer rows (they may have multiple status rows)
        from collections import defaultdict
        officer_agg = defaultdict(lambda: {"total": 0, "fine": 0.0})
        for r in officer_rows:
            name = (r.get("officer_name") or "Unknown").strip()
            officer_agg[name]["total"] += r.get("cnt", 0) or 0
            officer_agg[name]["fine"] += float(r.get("fine", 0) or 0)

        sorted_officers = sorted(officer_agg.items(), key=lambda x: x[1]["total"], reverse=True)
        ctx_lines.append(f"\n--- Officer Breakdown ({len(sorted_officers)} officers) ---")
        for i, (name, agg) in enumerate(sorted_officers[:30], 1):
            ctx_lines.append(f"  {i}. {name}: {_fnum(agg['total'])} challans | Fine: Rs. {_fnum(agg['fine'])}")

    return _build_result(
        "challan_daterange", title,
        summary_rows, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Officers at a Specific Location ───────────────

def _lookup_officer_at_location(loc_type: str, loc_name: str,
                                 req_key: str = None) -> Optional[Dict]:
    """
    Rank officers at a specific location, optionally filtered by req type.
    e.g. "which officer imposed more price control challans in Bahawalpur City?"
    """
    db = _get_db()
    if not db:
        return None

    # Map loc_type to DB column
    loc_col = {
        "tehsil": "tehsil_name",
        "district": "district_name",
        "division": "division_name",
    }.get(loc_type, "tehsil_name")

    # Build query
    conditions = [f"{loc_col} ILIKE %s"]
    params: list = [f"%{loc_name}%"]

    req_label = ""
    if req_key and req_key in _REQ_KEY_TO_DB_NAME:
        req_db_name = _REQ_KEY_TO_DB_NAME[req_key]
        conditions.append("requisition_type_name = %s")
        params.append(req_db_name)
        req_label = f" — {req_db_name}"

    where = " AND ".join(conditions)

    rows = db.fetch_all(
        "SELECT officer_name, "
        "       COUNT(*) AS total_challans, "
        "       SUM(CASE WHEN LOWER(status) = 'paid' THEN 1 ELSE 0 END) AS paid_count, "
        "       SUM(CASE WHEN LOWER(status) = 'unpaid' THEN 1 ELSE 0 END) AS unpaid_count, "
        "       SUM(CASE WHEN LOWER(status) = 'overdue' THEN 1 ELSE 0 END) AS overdue_count, "
        "       SUM(fine_amount) AS total_fine, "
        "       SUM(paid_amount) AS total_paid_amount "
        "FROM challan_data "
        f"WHERE officer_name IS NOT NULL AND officer_name != '' AND {where} "
        "GROUP BY officer_name "
        "ORDER BY total_challans DESC",
        tuple(params),
    )

    if not rows:
        # Return an explicit "no data" result so the LLM can say so clearly
        no_data_msg = f"No challan records found for officers in {loc_name}{req_label}."
        no_data_ctx = (
            f"[Source Type: API]\n"
            f"[API Name: PERA Officer Ranking at {loc_name}{req_label}]\n"
            f"[Location: {loc_name} ({loc_type})]\n"
            f"[Result: NO DATA FOUND — 0 records match this filter]\n\n"
            f"There are no {req_label.strip(' —') or 'challan'} records "
            f"for any officer in {loc_name}."
        )
        return _build_result(
            "challan_officer_at_location",
            f"Officers in {loc_name}{req_label}",
            [], no_data_msg, no_data_ctx,
        )

    records = rows
    loc_display = f"{loc_name} ({loc_type})"
    title = f"Officers in {loc_name}{req_label}"

    lines = [f"**PERA Officers Ranked by Challans in {loc_name}{req_label}**\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[API Name: PERA Officer Ranking at {loc_display}{req_label}]",
        f"[Location: {loc_display}]",
        f"[Requisition Type: {req_label.strip(' —') or 'All'}]",
        f"[Total Officers: {len(records)}]",
        "",
        f"Officers ranked by challans in {loc_name}{req_label} (highest to lowest):",
        "",
    ]

    for i, r in enumerate(records[:20], 1):
        name = (r.get("officer_name") or "Unknown").strip()
        total = r.get("total_challans", 0) or 0
        paid = r.get("paid_count", 0) or 0
        unpaid = r.get("unpaid_count", 0) or 0
        overdue = r.get("overdue_count", 0) or 0
        fine = r.get("total_fine", 0) or 0
        paid_ratio = (paid / total * 100) if total > 0 else 0

        line = (
            f"{i}. **{name}**: {_fnum(total)} challans "
            f"(Paid: {_fnum(paid)} | Unpaid: {_fnum(unpaid)} | Overdue: {_fnum(overdue)}) "
            f"— Paid Ratio: {paid_ratio:.1f}% — Fine: Rs. {_fnum(fine)}"
        )
        lines.append(line)
        ctx_lines.append(
            f"{i}. {name}: {_fnum(total)} challans "
            f"(Paid: {_fnum(paid)} | Unpaid: {_fnum(unpaid)} | Overdue: {_fnum(overdue)}) "
            f"Paid Ratio: {paid_ratio:.1f}% Fine: Rs. {_fnum(fine)}"
        )

    if len(records) > 20:
        lines.append(f"\n_...and {len(records) - 20} more officers_")
        ctx_lines.append(f"...and {len(records) - 20} more officers")

    grand_total = sum(r.get("total_challans", 0) or 0 for r in records)
    grand_fine = sum(float(r.get("total_fine", 0) or 0) for r in records)
    lines.append(
        f"\n**Grand Total**: {_fnum(grand_total)} challans by {len(records)} officers "
        f"in {loc_name}{req_label} | Total Fine: Rs. {_fnum(grand_fine)}"
    )
    ctx_lines.append("")
    ctx_lines.append(
        f"Grand Total: {_fnum(grand_total)} challans by {len(records)} officers "
        f"in {loc_name}{req_label} | Total Fine: Rs. {_fnum(grand_fine)}"
    )

    return _build_result(
        "challan_officer_at_location", title,
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Officer Ranking ────────────────────────────────

_REQ_KEY_TO_DB_NAME = {
    "price_control": "Price Control",
    "encroachment": "Anti Encroachment",
    "hoarding": "Anti Hoarding",
    "land_retrieval": "Eviction",
    "public_nuisance": "Public Nuisance",
}


def _lookup_officer_ranking(status: str = "paid", req_key: str = None) -> Optional[Dict]:
    """
    Rank officers by challan count, including their primary tehsil and paid ratio.
    Optionally filter by requisition type (e.g. "price_control").
    """
    db = _get_db()
    if not db:
        return None

    status_title = status.title()

    # Build optional requisition type filter
    req_filter = ""
    params: list = []
    req_label = ""
    if req_key and req_key in _REQ_KEY_TO_DB_NAME:
        req_db_name = _REQ_KEY_TO_DB_NAME[req_key]
        req_filter = "AND requisition_type_name = %s "
        params.append(req_db_name)
        req_label = f" ({req_db_name})"

    # Get top officers with total challans, paid count, and primary location
    rows = db.fetch_all(
        "SELECT officer_name, "
        "       COUNT(*) AS total_challans, "
        "       SUM(CASE WHEN LOWER(status) = 'paid' THEN 1 ELSE 0 END) AS paid_count, "
        "       SUM(CASE WHEN LOWER(status) = 'unpaid' THEN 1 ELSE 0 END) AS unpaid_count, "
        "       SUM(CASE WHEN LOWER(status) = 'overdue' THEN 1 ELSE 0 END) AS overdue_count, "
        "       SUM(fine_amount) AS total_fine, "
        "       SUM(paid_amount) AS total_paid_amount, "
        "       MODE() WITHIN GROUP (ORDER BY tehsil_name) AS primary_tehsil, "
        "       MODE() WITHIN GROUP (ORDER BY district_name) AS primary_district "
        "FROM challan_data "
        "WHERE officer_name IS NOT NULL AND officer_name != '' "
        f"{req_filter}"
        "GROUP BY officer_name "
        "ORDER BY total_challans DESC",
        tuple(params),
    )

    if not rows:
        return None

    records = rows
    title_suffix = f" — {_REQ_KEY_TO_DB_NAME[req_key]}" if req_key and req_key in _REQ_KEY_TO_DB_NAME else ""

    lines = [f"**PERA Officers Ranked by Total Challans Imposed{title_suffix}**\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[API Name: PERA Challan Officer Ranking{req_label}]",
        f"[Requisition Type Filter: {req_label.strip(' ()') or 'All'}]",
        f"[Total Officers: {len(records)}]",
        "",
        f"Officers ranked by total challans imposed{req_label} (highest to lowest):",
        "",
    ]

    # Show top 20
    for i, r in enumerate(records[:20], 1):
        name = (r.get("officer_name") or "Unknown").strip()
        total = r.get("total_challans", 0) or 0
        paid = r.get("paid_count", 0) or 0
        unpaid = r.get("unpaid_count", 0) or 0
        overdue = r.get("overdue_count", 0) or 0
        fine = r.get("total_fine", 0) or 0
        tehsil = (r.get("primary_tehsil") or "").strip()
        district = (r.get("primary_district") or "").strip()
        paid_ratio = (paid / total * 100) if total > 0 else 0
        location = tehsil or district or "Unknown"
        if district and tehsil and district != tehsil:
            location = f"{tehsil}, {district}"

        line = (
            f"{i}. **{name}**: {_fnum(total)} challans "
            f"(Paid: {_fnum(paid)} | Unpaid: {_fnum(unpaid)} | Overdue: {_fnum(overdue)}) "
            f"— Paid Ratio: {paid_ratio:.1f}% — Location: {location} "
            f"— Fine: Rs. {_fnum(fine)}"
        )
        lines.append(line)
        ctx_lines.append(
            f"{i}. {name}: {_fnum(total)} challans "
            f"(Paid: {_fnum(paid)} | Unpaid: {_fnum(unpaid)} | Overdue: {_fnum(overdue)}) "
            f"Paid Ratio: {paid_ratio:.1f}% Location: {location} "
            f"Fine: Rs. {_fnum(fine)}"
        )

    if len(records) > 20:
        lines.append(f"\n_...and {len(records) - 20} more officers_")
        ctx_lines.append(f"...and {len(records) - 20} more officers")

    # Grand totals
    grand_total = sum(r.get("total_challans", 0) or 0 for r in records)
    grand_paid = sum(r.get("paid_count", 0) or 0 for r in records)
    grand_ratio = (grand_paid / grand_total * 100) if grand_total > 0 else 0
    grand_fine = sum(float(r.get("total_fine", 0) or 0) for r in records)
    lines.append(
        f"\n**Grand Total**: {_fnum(grand_total)} challans across {len(records)} officers "
        f"| Overall Paid Ratio: {grand_ratio:.1f}% | Total Fine: Rs. {_fnum(grand_fine)}"
    )
    ctx_lines.append("")
    ctx_lines.append(
        f"Grand Total: {_fnum(grand_total)} challans across {len(records)} officers "
        f"| Overall Paid Ratio: {grand_ratio:.1f}% | Total Fine: Rs. {_fnum(grand_fine)}"
    )

    return _build_result(
        "challan_comparison", f"PERA Officer Ranking by Challans",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Comparison/Ranking ──────────────────────────────

def _lookup_comparison(status: str = "paid", level: str = "tehsil",
                       req_key: str = None) -> Optional[Dict]:
    """
    Return locations ranked by a specific challan status.

    Args:
        status: "paid", "unpaid", or "overdue"
        level:  "tehsil" (station), "district", or "division"
        req_key: optional requisition type filter (e.g. "price_control")
    """
    db = _get_db()
    status_title = status.title()

    req_label = ""
    if req_key and req_key in _REQ_KEY_TO_DB_NAME:
        req_db_name = _REQ_KEY_TO_DB_NAME[req_key]
        req_label = f" — {req_db_name}"

    # When a requisition type filter is applied, query challan_data directly
    # (pre-aggregated tables don't have req type breakdown)
    if req_key and req_key in _REQ_KEY_TO_DB_NAME:
        name_col = {
            "division": "division_name",
            "district": "district_name",
        }.get(level, "tehsil_name")
        level_label = {
            "division": "Division",
            "district": "District",
        }.get(level, "Station/Tehsil")

        rows = db.fetch_all(
            f"SELECT {name_col} AS name, "
            "       COUNT(*) AS total_challans, "
            "       SUM(fine_amount) AS total_amount, "
            "       SUM(CASE WHEN LOWER(status) = 'paid' THEN 1 ELSE 0 END) AS paid_count, "
            "       SUM(CASE WHEN LOWER(status) = 'unpaid' THEN 1 ELSE 0 END) AS unpaid_count, "
            "       SUM(CASE WHEN LOWER(status) = 'overdue' THEN 1 ELSE 0 END) AS overdue_count "
            "FROM challan_data "
            f"WHERE {name_col} IS NOT NULL AND {name_col} != '' "
            "AND requisition_type_name = %s "
            f"GROUP BY {name_col} "
            "ORDER BY total_challans DESC",
            (req_db_name,),
        )
    else:
        # No req filter — use pre-aggregated tables (faster)
        if level == "division":
            rows = db.fetch_all(
                "SELECT division_name AS name, total_challans, total_amount "
                "FROM challan_by_division WHERE LOWER(status) = %s "
                "AND total_challans > 0 "
                "ORDER BY total_challans DESC",
                (status.lower(),),
            )
            level_label = "Division"
        elif level == "district":
            rows = db.fetch_all(
                "SELECT cbd.district_name AS name, cbd.total_challans, cbd.total_amount "
                "FROM challan_by_district cbd "
                "WHERE LOWER(cbd.status) = %s "
                "AND cbd.total_challans > 0 "
                "ORDER BY cbd.total_challans DESC",
                (status.lower(),),
            )
            level_label = "District"
        else:
            rows = db.fetch_all(
                "SELECT tehsil_name AS name, total_challans, total_amount "
                "FROM challan_tehsil_drill WHERE LOWER(status) = %s "
                "AND total_challans > 0 "
                "ORDER BY total_challans DESC",
                (status.lower(),),
            )
            level_label = "Station/Tehsil"

    if not rows:
        return None

    records = rows

    lines = [f"**PERA {level_label}s Ranked by {status_title} Challans{req_label}**\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[API Name: PERA Challan {level_label} Comparison by {status_title}{req_label}]",
        f"[Comparison Level: {level_label}]",
        f"[Status Filter: {status_title}]",
        f"[Requisition Type: {req_label.strip(' —') or 'All'}]",
        f"[Total {level_label}s: {len(records)}]",
        "",
        f"All {level_label}s ranked by {status_title} challans{req_label} (highest to lowest):",
        "",
    ]

    # Show top 20 with ranks
    for i, r in enumerate(records[:20], 1):
        name = (r.get("name") or "Unknown").strip()
        challans = r.get("total_challans", 0) or 0
        amount = r.get("total_amount", 0) or 0

        # Include paid ratio if available (from challan_data query)
        extra = ""
        if r.get("paid_count") is not None:
            paid = r.get("paid_count", 0) or 0
            unpaid = r.get("unpaid_count", 0) or 0
            overdue = r.get("overdue_count", 0) or 0
            ratio = (paid / challans * 100) if challans > 0 else 0
            extra = f" (Paid: {_fnum(paid)} | Unpaid: {_fnum(unpaid)} | Overdue: {_fnum(overdue)} | Paid Ratio: {ratio:.1f}%)"

        line = f"{i}. **{name}**: {_fnum(challans)} challans{extra} (Rs. {_fnum(amount)})"
        lines.append(line)
        ctx_lines.append(f"{i}. {name}: {_fnum(challans)} challans{extra} (Rs. {_fnum(amount)})")

    if len(records) > 20:
        lines.append(f"\n_...and {len(records) - 20} more {level_label.lower()}s_")
        ctx_lines.append(f"...and {len(records) - 20} more {level_label.lower()}s")

    # Grand total
    grand = sum(r.get("total_challans", 0) or 0 for r in records)
    grand_amt = sum(float(r.get("total_amount", 0) or 0) for r in records)
    lines.append(f"\n**Grand Total ({status_title}{req_label})**: {_fnum(grand)} challans (Rs. {_fnum(grand_amt)})")
    ctx_lines.append(f"")
    ctx_lines.append(f"Grand Total ({status_title}{req_label}): {_fnum(grand)} challans (Rs. {_fnum(grand_amt)})")

    return _build_result(
        "challan_comparison", f"PERA Challan {level_label} Comparison — {status_title}",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Officer-Specific ────────────────────────────────

def _lookup_officer(officer_name: str, question: str = "") -> Optional[Dict]:
    """
    Query challan_data for a specific officer's performance.
    Returns detailed breakdown: total challans, by status, by req type,
    by tehsil, fine amounts, etc.
    """
    db = _get_db()
    if not db:
        return None

    # Use ILIKE for partial matching — DB stores names with code suffixes
    # (e.g. "Muhammad Azhar Saeed EO-091") but users ask without the code.
    # First try exact match, then fall back to ILIKE contains.
    name_param = officer_name
    where_clause = "officer_name = %s"
    test_row = db.fetch_one(
        "SELECT COUNT(*) as cnt FROM challan_data WHERE officer_name = %s",
        (officer_name,),
    )
    if not test_row or (test_row.get("cnt") or 0) == 0:
        # Exact match failed — use partial ILIKE match
        name_param = f"%{officer_name}%"
        where_clause = "officer_name ILIKE %s"

    # 1. Overall stats for this officer
    summary_rows = db.fetch_all(
        "SELECT status, COUNT(*) as total_challans, "
        "       SUM(fine_amount) as total_fine, "
        "       SUM(paid_amount) as total_paid, "
        "       SUM(outstanding_amount) as total_outstanding "
        f"FROM challan_data WHERE {where_clause} "
        "GROUP BY status ORDER BY status",
        (name_param,),
    )
    if not summary_rows:
        return None

    # Resolve actual officer name from DB for display
    actual_name_row = db.fetch_one(
        f"SELECT DISTINCT officer_name FROM challan_data WHERE {where_clause} LIMIT 1",
        (name_param,),
    )
    display_name = (actual_name_row or {}).get("officer_name", officer_name)

    # 2. Breakdown by requisition type
    type_rows = db.fetch_all(
        "SELECT requisition_type_name, status, COUNT(*) as cnt, "
        "       SUM(fine_amount) as total_fine "
        f"FROM challan_data WHERE {where_clause} "
        "GROUP BY requisition_type_name, status "
        "ORDER BY requisition_type_name, status",
        (name_param,),
    )

    # 3. Breakdown by tehsil/location
    location_rows = db.fetch_all(
        "SELECT tehsil_name, district_name, division_name, "
        "       COUNT(*) as cnt, SUM(fine_amount) as total_fine "
        f"FROM challan_data WHERE {where_clause} "
        "GROUP BY tehsil_name, district_name, division_name "
        "ORDER BY cnt DESC",
        (name_param,),
    )

    # Build formatted response
    grand_challans = 0
    grand_fine = 0
    grand_paid = 0
    grand_outstanding = 0

    lines = [f"**PERA Challan Officer Report — {display_name}**\n"]
    ctx_lines = [
        "[Source Type: API]",
        f"[API Name: PERA Challan Officer Report]",
        f"[Officer Name: {display_name}]",
        f"[Total Status Categories: {len(summary_rows)}]",
        "",
        f"PERA Challan Officer Report for {display_name}:",
        "",
        "--- Status Breakdown ---",
    ]

    lines.append("**Status Breakdown:**")
    for r in summary_rows:
        st = r.get("status", "unknown")
        cnt = r.get("total_challans", 0) or 0
        fine = r.get("total_fine", 0) or 0
        paid = r.get("total_paid", 0) or 0
        outstanding = r.get("total_outstanding", 0) or 0
        grand_challans += cnt
        grand_fine += float(fine)
        grand_paid += float(paid)
        grand_outstanding += float(outstanding)

        line = f"- **{st.title()}**: {_fnum(cnt)} challans | Fine: Rs. {_fnum(fine)} | Paid: Rs. {_fnum(paid)} | Outstanding: Rs. {_fnum(outstanding)}"
        lines.append(line)
        ctx_lines.append(f"{st.title()}: {_fnum(cnt)} challans | Fine: Rs. {_fnum(fine)} | Paid: Rs. {_fnum(paid)} | Outstanding: Rs. {_fnum(outstanding)}")

    lines.append(f"\n**Grand Total**: {_fnum(grand_challans)} challans | Fine: Rs. {_fnum(grand_fine)} | Paid: Rs. {_fnum(grand_paid)} | Outstanding: Rs. {_fnum(grand_outstanding)}")
    ctx_lines.append("")
    ctx_lines.append(f"Grand Total: {_fnum(grand_challans)} challans | Fine: Rs. {_fnum(grand_fine)} | Paid: Rs. {_fnum(grand_paid)} | Outstanding: Rs. {_fnum(grand_outstanding)}")

    # Requisition type breakdown
    if type_rows:
        lines.append("\n**By Requisition Type:**")
        ctx_lines.append("")
        ctx_lines.append("--- By Requisition Type ---")
        for r in type_rows:
            tname = r.get("requisition_type_name", "Unknown")
            st = r.get("status", "unknown")
            cnt = r.get("cnt", 0) or 0
            fine = r.get("total_fine", 0) or 0
            line = f"- {tname} ({st.title()}): {_fnum(cnt)} challans (Rs. {_fnum(fine)})"
            lines.append(line)
            ctx_lines.append(f"{tname} ({st.title()}): {_fnum(cnt)} challans (Rs. {_fnum(fine)})")

    # Location breakdown
    if location_rows:
        lines.append("\n**Locations Served:**")
        ctx_lines.append("")
        ctx_lines.append("--- Locations Served ---")
        for r in location_rows:
            tehsil = r.get("tehsil_name", "")
            district = r.get("district_name", "")
            division = r.get("division_name", "")
            cnt = r.get("cnt", 0) or 0
            fine = r.get("total_fine", 0) or 0
            loc_str = tehsil or district or division or "Unknown"
            if district and tehsil and district != tehsil:
                loc_str = f"{tehsil}, {district}"
            line = f"- {loc_str}: {_fnum(cnt)} challans (Rs. {_fnum(fine)})"
            lines.append(line)
            ctx_lines.append(f"{loc_str}: {_fnum(cnt)} challans (Rs. {_fnum(fine)})")

    records = summary_rows + type_rows + location_rows

    return _build_result(
        "challan_officer", f"PERA Challan Officer — {display_name}",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Totals ──────────────────────────────────────────

@_register("challan_totals")
def _lookup_totals() -> Optional[Dict]:
    db = _get_db()
    row = db.fetch_one("SELECT * FROM challan_totals WHERE id = 1")
    if not row:
        return None

    records = [row]

    answer = (
        f"**PERA Challan Status — Overall Summary**\n\n"
        f"- **Total Challans**: {_fnum(row['total_challans'])}\n"
        f"- **Total Fine Amount**: Rs. {_fnum(row['total_fine_amount'])}\n"
        f"- **Paid**: {_fnum(row['paid'])} ({row.get('paid_percent', 0):.1f}%)\n"
        f"- **Unpaid**: {_fnum(row['unpaid'])} ({row.get('unpaid_percent', 0):.1f}%)\n"
        f"- **Overdue**: {_fnum(row['overdue'])} ({row.get('overdue_percent', 0):.1f}%)\n"
        f"- **Paid Fine Amount**: Rs. {_fnum(row['paid_fine_amount'])}\n"
        f"- **Unpaid Fine Amount**: Rs. {_fnum(row['unpaid_fine_amount'])}\n"
        f"- **Overdue Fine Amount**: Rs. {_fnum(row['overdue_fine_amount'])}"
    )

    context = (
        f"[Source Type: API]\n"
        f"[API Name: PERA Challan Overall Totals]\n"
        f"[Total Records: 1]\n\n"
        f"PERA Challan Status Overall Summary:\n"
        f"Total Challans: {_fnum(row['total_challans'])}\n"
        f"Total Fine Amount: Rs. {_fnum(row['total_fine_amount'])}\n"
        f"Paid: {_fnum(row['paid'])} ({row.get('paid_percent', 0):.1f}%)\n"
        f"Unpaid: {_fnum(row['unpaid'])} ({row.get('unpaid_percent', 0):.1f}%)\n"
        f"Overdue: {_fnum(row['overdue'])} ({row.get('overdue_percent', 0):.1f}%)\n"
        f"Paid Fine Amount: Rs. {_fnum(row['paid_fine_amount'])}\n"
        f"Unpaid Fine Amount: Rs. {_fnum(row['unpaid_fine_amount'])}\n"
        f"Overdue Fine Amount: Rs. {_fnum(row['overdue_fine_amount'])}"
    )

    return _build_result("challan_totals", "PERA Challan Totals", records, answer, context)


# ── Handler: By Division ─────────────────────────────────────

@_register("challan_by_division")
def _lookup_by_division() -> Optional[Dict]:
    db = _get_db()
    rows = db.fetch_all(
        "SELECT status, division_name, total_challans, total_amount "
        "FROM challan_by_division ORDER BY division_name, status"
    )
    if not rows:
        return None

    records = rows

    divs: Dict[str, Dict[str, Any]] = {}
    for r in records:
        name = r["division_name"]
        if name not in divs:
            divs[name] = {}
        divs[name][r["status"]] = {
            "challans": r["total_challans"],
            "amount": r["total_amount"],
        }

    lines = ["**PERA Challans by Division**\n"]
    ctx_lines = [
        "[Source Type: API]",
        "[API Name: PERA Challan by Division]",
        f"[Total Records: {len(records)}]",
        "",
        "PERA Challans by Division:",
    ]

    for name in sorted(divs):
        parts = []
        for st in ["paid", "unpaid", "overdue"]:
            d = divs[name].get(st, {})
            if d:
                parts.append(f"{st.title()}: {_fnum(d['challans'])} (Rs. {_fnum(d['amount'])})")
        line = f"- **{name}**: " + " | ".join(parts)
        lines.append(line)
        ctx_lines.append(f"- {name}: " + " | ".join(parts))

    return _build_result(
        "challan_by_division", "PERA Challan by Division",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: By District ─────────────────────────────────────

@_register("challan_by_district")
def _lookup_by_district() -> Optional[Dict]:
    db = _get_db()
    rows = db.fetch_all(
        "SELECT cbd.status, cbd.district_name, cbd.total_challans, "
        "       cbd.total_amount, dd.division_name "
        "FROM challan_by_district cbd "
        "LEFT JOIN dim_division dd ON dd.division_id = cbd.division_id "
        "ORDER BY dd.division_name, cbd.district_name, cbd.status"
    )
    if not rows:
        return None

    records = rows

    lines = ["**PERA Challans by District**\n"]
    ctx_lines = [
        "[Source Type: API]",
        "[API Name: PERA Challan by District]",
        f"[Total Records: {len(records)}]",
        "",
        "PERA Challans by District:",
    ]

    dists: Dict[str, Dict] = {}
    for r in records:
        key = r["district_name"] or "Unknown"
        if key not in dists:
            dists[key] = {"division": r.get("division_name", ""), "data": {}}
        dists[key]["data"][r["status"]] = {
            "challans": r["total_challans"],
            "amount": r["total_amount"],
        }

    for name in sorted(dists):
        div = dists[name]["division"]
        parts = []
        for st in ["paid", "unpaid", "overdue"]:
            d = dists[name]["data"].get(st, {})
            if d:
                parts.append(f"{st.title()}: {_fnum(d['challans'])}")
        detail = " | ".join(parts)
        lines.append(f"- **{name}** ({div}): {detail}")
        ctx_lines.append(f"- {name} ({div}): {detail}")

    return _build_result(
        "challan_by_district", "PERA Challan by District",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: By Tehsil ───────────────────────────────────────

@_register("challan_by_tehsil")
def _lookup_by_tehsil() -> Optional[Dict]:
    db = _get_db()
    rows = db.fetch_all(
        "SELECT status, tehsil_name, count "
        "FROM challan_by_tehsil ORDER BY tehsil_name, status"
    )
    if not rows:
        return None

    records = rows

    totals: Dict[str, int] = {}
    for r in records:
        name = r["tehsil_name"]
        totals[name] = totals.get(name, 0) + r["count"]

    top_30 = sorted(totals.items(), key=lambda x: -x[1])[:30]

    lines = [f"**PERA Challans by Tehsil** (top 30 of {len(totals)} tehsils)\n"]
    ctx_lines = [
        "[Source Type: API]",
        "[API Name: PERA Challan by Tehsil]",
        f"[Total Records: {len(records)}]",
        "",
        f"PERA Challans by Tehsil (top 30 of {len(totals)}):",
    ]

    for name, count in top_30:
        lines.append(f"- **{name}**: {_fnum(count)} total challans")
        ctx_lines.append(f"- {name}: {_fnum(count)} total challans")

    return _build_result(
        "challan_by_tehsil", "PERA Challan by Tehsil",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Requisition Type ────────────────────────────────

@_register("challan_requisition_type")
def _lookup_requisition_type() -> Optional[Dict]:
    db = _get_db()
    rows = db.fetch_all(
        "SELECT status, requisition_type_name, total_challans, total_amount "
        "FROM challan_requisition_type ORDER BY requisition_type_name, status"
    )
    if not rows:
        return None

    records = rows

    types: Dict[str, Dict] = {}
    for r in records:
        tname = r["requisition_type_name"]
        if tname not in types:
            types[tname] = {}
        types[tname][r["status"]] = {
            "challans": r["total_challans"],
            "amount": r["total_amount"],
        }

    lines = ["**PERA Challans by Requisition Type**\n"]
    ctx_lines = [
        "[Source Type: API]",
        "[API Name: PERA Challan Requisition Types]",
        f"[Total Records: {len(records)}]",
        "",
        "PERA Challans by Requisition Type:",
    ]

    for tname in sorted(types):
        parts = []
        for st in ["paid", "unpaid", "overdue"]:
            d = types[tname].get(st, {})
            if d and d["challans"]:
                parts.append(f"{st.title()}: {_fnum(d['challans'])}")
        detail = " | ".join(parts) if parts else "No data"
        lines.append(f"- **{tname}**: {detail}")
        ctx_lines.append(f"- {tname}: {detail}")

    return _build_result(
        "challan_requisition_type", "PERA Challan Requisition Types",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )


# ── Handler: Tehsil Breakdown ────────────────────────────────

@_register("challan_tehsil_breakdown")
def _lookup_tehsil_breakdown() -> Optional[Dict]:
    db = _get_db()
    rows = db.fetch_all(
        "SELECT status, tehsil_name, total_requisitions, "
        "       hoarding_count, price_control_count, "
        "       encroachment_count, land_retrieval_count, "
        "       public_nuisance_count "
        "FROM challan_tehsil_breakdown "
        "WHERE total_requisitions > 0 "
        "ORDER BY total_requisitions DESC LIMIT 30"
    )
    if not rows:
        return None

    records = rows

    lines = [f"**PERA Challan Tehsil Breakdown** (top {len(records)} entries)\n"]
    ctx_lines = [
        "[Source Type: API]",
        "[API Name: PERA Challan Tehsil Breakdown]",
        f"[Total Records: {len(records)}]",
        "",
        "PERA Challan Tehsil Breakdown:",
    ]

    for r in records:
        name = r["tehsil_name"]
        status = r["status"]
        parts = []
        if r["price_control_count"]:
            parts.append(f"PriceCtrl: {r['price_control_count']}")
        if r["encroachment_count"]:
            parts.append(f"Encroach: {r['encroachment_count']}")
        if r["hoarding_count"]:
            parts.append(f"Hoard: {r['hoarding_count']}")
        if r["land_retrieval_count"]:
            parts.append(f"Land: {r['land_retrieval_count']}")
        if r["public_nuisance_count"]:
            parts.append(f"Nuisance: {r['public_nuisance_count']}")
        detail = ", ".join(parts) if parts else "No data"
        line = f"- **{name}** ({status}): {_fnum(r['total_requisitions'])} total -- {detail}"
        lines.append(line)
        ctx_lines.append(f"- {name} ({status}): {_fnum(r['total_requisitions'])} total -- {detail}")

    return _build_result(
        "challan_tehsil_breakdown", "PERA Challan Tehsil Breakdown",
        records, "\n".join(lines), "\n".join(ctx_lines),
    )
