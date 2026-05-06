"""
PERA AI — Canonical Date Range Parser

Single source of truth for date parsing across all domains
(inspection, challan, operational activity).

Boundary semantics: INCLUSIVE start AND end. Callers that need an
exclusive endDate (e.g. SDEO API midnight convention) must adjust at
the call site, not here.

Time zone: defaults to Asia/Karachi when computing `today`.

Public API:
  - parse_date_range(text, today=None, tz="Asia/Karachi") -> Optional[DateRange]
  - has_recent_range(date_range, today=None) -> bool
  - format_date_range(date_range) -> str
  - detect_unparsed_date_expression(text) -> bool

If a date-looking expression exists but cannot be parsed, callers can
detect the situation via detect_unparsed_date_expression() so they don't
silently treat unparsable input as "no date".
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional


# ── Public dataclass ─────────────────────────────────────────
@dataclass(frozen=True)
class DateRange:
    """Inclusive [start, end] calendar date range."""
    start: date
    end: date
    source: str           # which pattern produced the range
    matched_text: str     # span from the user query that triggered the match
    confidence: float = 1.0


# ── Time zone helpers ────────────────────────────────────────
def _today_in_tz(tz: str = "Asia/Karachi") -> date:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(tz=ZoneInfo(tz)).date()
    except Exception:
        return date.today()


# ── Building blocks ──────────────────────────────────────────
_MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}
_MONTH_RE = "(?:" + "|".join(sorted(_MONTHS.keys(), key=len, reverse=True)) + ")"

_ORD = r"(?:st|nd|rd|th)?"

# ISO 4-2-2 (handles both - and /)
_ISO_DATE = re.compile(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b")

# Numeric DD-MM-YYYY or DD/MM/YYYY (PERA / Pakistan default ordering)
_DMY_DATE = re.compile(r"\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b")

# "1 Jan 2026"
_DMY_NAMED = re.compile(
    rf"\b(\d{{1,2}}){_ORD}\s+({_MONTH_RE})\w*[,]?\s+(\d{{4}})\b", re.I
)
# "Jan 1 2026", "January 1, 2026"
_MDY_NAMED = re.compile(
    rf"\b({_MONTH_RE})\w*\s+(\d{{1,2}}){_ORD}[,]?\s+(\d{{4}})\b", re.I
)

# Yearless: "1 Jan", "Jan 1"
_DM_YEARLESS = re.compile(rf"\b(\d{{1,2}}){_ORD}\s+({_MONTH_RE})\w*\b", re.I)
_MD_YEARLESS = re.compile(rf"\b({_MONTH_RE})\w*\s+(\d{{1,2}}){_ORD}\b", re.I)

# Range patterns
_RANGE_FROM_TO = re.compile(
    r"\b(?:from\s+)?(.+?)\s+(?:to|till|until|through|thru|-)\s+(.+?)(?=\s+(?:in|for|of|please|kindly|tell|show|give|with|and)|[?.!,]|$)",
    re.I,
)
_RANGE_BETWEEN = re.compile(
    r"\bbetween\s+(.+?)\s+(?:and|&)\s+(.+?)(?=\s+(?:in|for|of|please)|[?.!,]|$)",
    re.I,
)
# Roman-Urdu "X se Y tak"
_RANGE_URDU = re.compile(r"\b(.+?)\s+s[ey]\s+(.+?)\s+tak\b", re.I)

# Standalone "March 2026" / "2026 March"
_MONTH_YEAR = re.compile(rf"\b({_MONTH_RE})\w*\s+(\d{{4}})\b", re.I)
_YEAR_MONTH = re.compile(rf"\b(\d{{4}})\s+({_MONTH_RE})\w*\b", re.I)

# Quarter patterns
_QUARTER = re.compile(r"\bQ([1-4])\s*[-,]?\s*(\d{4})\b", re.I)
_QUARTER_OF = re.compile(r"\b(?:quarter|q)\s*([1-4])\s+(?:of\s+)?(\d{4})\b", re.I)

# Fiscal year
_FY = re.compile(r"\b(?:FY|fiscal\s+year)\s*(\d{4})\s*[-/]\s*(\d{2,4})\b", re.I)

# Relative
_REL_TODAY = re.compile(r"\b(today|aaj|aj)\b", re.I)
_REL_YESTERDAY = re.compile(r"\b(yesterday|kal|gestar)\b", re.I)
_REL_THIS_WEEK = re.compile(r"\b(this|current|chal\s*rahy)\s+week\b", re.I)
_REL_LAST_WEEK = re.compile(r"\b(last|past|pichl[ei])\s+week\b", re.I)
_REL_THIS_MONTH = re.compile(r"\b(this|current|chal\s*rahy)\s+month\b", re.I)
_REL_LAST_MONTH = re.compile(r"\b(last|past|pichl[ei])\s+month\b", re.I)
_REL_LAST_N_DAYS = re.compile(r"\b(?:last|past|pichl[ei])\s+(\d+|seven|ten|fourteen|thirty)\s+days?\b", re.I)
_REL_LAST_N_WEEKS = re.compile(r"\b(?:last|past|pichl[ei])\s+(\d+|two|three|four)\s+weeks?\b", re.I)

_NUMBER_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "fourteen": 14,
    "fifteen": 15, "twenty": 20, "thirty": 30,
}


# ── Single-date parser ───────────────────────────────────────
def _parse_single(token: str, default_year: Optional[int] = None,
                  as_end: bool = False) -> Optional[date]:
    """Parse one date token. Returns None if unparseable."""
    if not token:
        return None
    s = token.strip().rstrip(".,!?")

    # ISO
    m = _ISO_DATE.search(s)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return date(y, mo, d)
        except ValueError:
            pass

    # DD-MM-YYYY / DD/MM/YYYY (Pakistan order)
    m = _DMY_DATE.search(s)
    if m:
        try:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return date(y, mo, d)
        except ValueError:
            pass

    # "1 Jan 2026"
    m = _DMY_NAMED.search(s)
    if m:
        try:
            return date(int(m.group(3)), _MONTHS[m.group(2).lower()],
                        int(m.group(1)))
        except (ValueError, KeyError):
            pass

    # "Jan 1 2026"
    m = _MDY_NAMED.search(s)
    if m:
        try:
            return date(int(m.group(3)), _MONTHS[m.group(1).lower()],
                        int(m.group(2)))
        except (ValueError, KeyError):
            pass

    # Yearless variants — need default year
    if default_year is not None:
        m = _DM_YEARLESS.search(s)
        if m:
            try:
                return date(default_year, _MONTHS[m.group(2).lower()],
                            int(m.group(1)))
            except (ValueError, KeyError):
                pass
        m = _MD_YEARLESS.search(s)
        if m:
            try:
                return date(default_year, _MONTHS[m.group(1).lower()],
                            int(m.group(2)))
            except (ValueError, KeyError):
                pass

    return None


def _last_day_of_month(y: int, m: int) -> int:
    if m == 12:
        return 31
    next_first = date(y, m + 1, 1)
    return (next_first - timedelta(days=1)).day


def _word_to_int(s: str) -> Optional[int]:
    s = s.strip().lower()
    if s.isdigit():
        return int(s)
    return _NUMBER_WORDS.get(s)


# ── Public parser ────────────────────────────────────────────
def parse_date_range(
    text: str,
    today: Optional[date] = None,
    tz: str = "Asia/Karachi",
) -> Optional[DateRange]:
    """Parse a date range from `text`. Returns None when no date expression
    is found. Returns a DateRange with inclusive start and end otherwise.

    See module docstring for full format coverage.
    """
    if not text:
        return None
    if today is None:
        today = _today_in_tz(tz)

    # ── Relative single-day ──
    if _REL_TODAY.search(text):
        return DateRange(today, today, "rel:today",
                         _REL_TODAY.search(text).group(0))
    if _REL_YESTERDAY.search(text):
        y = today - timedelta(days=1)
        return DateRange(y, y, "rel:yesterday",
                         _REL_YESTERDAY.search(text).group(0))

    # ── Relative weeks/months ──
    if _REL_THIS_WEEK.search(text):
        # Mon-anchored ISO week
        start = today - timedelta(days=today.weekday())
        return DateRange(start, today, "rel:this_week",
                         _REL_THIS_WEEK.search(text).group(0))
    if _REL_LAST_WEEK.search(text):
        end = today - timedelta(days=today.weekday() + 1)  # last Sunday
        start = end - timedelta(days=6)
        return DateRange(start, end, "rel:last_week",
                         _REL_LAST_WEEK.search(text).group(0))
    if _REL_THIS_MONTH.search(text):
        start = today.replace(day=1)
        return DateRange(start, today, "rel:this_month",
                         _REL_THIS_MONTH.search(text).group(0))
    if _REL_LAST_MONTH.search(text):
        first_this = today.replace(day=1)
        last_end = first_this - timedelta(days=1)
        last_start = last_end.replace(day=1)
        return DateRange(last_start, last_end, "rel:last_month",
                         _REL_LAST_MONTH.search(text).group(0))
    m = _REL_LAST_N_DAYS.search(text)
    if m:
        n = _word_to_int(m.group(1)) or 7
        start = today - timedelta(days=n - 1)
        return DateRange(start, today, "rel:last_n_days", m.group(0))
    m = _REL_LAST_N_WEEKS.search(text)
    if m:
        n = _word_to_int(m.group(1)) or 1
        start = today - timedelta(days=n * 7 - 1)
        return DateRange(start, today, "rel:last_n_weeks", m.group(0))

    # ── Quarter / FY ──
    m = _QUARTER.search(text) or _QUARTER_OF.search(text)
    if m:
        q = int(m.group(1))
        y = int(m.group(2))
        start_month = (q - 1) * 3 + 1
        end_month = start_month + 2
        start = date(y, start_month, 1)
        end = date(y, end_month, _last_day_of_month(y, end_month))
        return DateRange(start, end, "quarter", m.group(0))

    m = _FY.search(text)
    if m:
        y1 = int(m.group(1))
        y2_raw = int(m.group(2))
        y2 = 2000 + y2_raw if y2_raw < 100 else y2_raw
        # Pakistan FY: July y1 to June y2
        start = date(y1, 7, 1)
        end = date(y2, 6, 30)
        return DateRange(start, end, "fiscal_year", m.group(0))

    # ── Explicit ranges ──
    rng_m = _RANGE_BETWEEN.search(text) or _RANGE_FROM_TO.search(text) or _RANGE_URDU.search(text)
    if rng_m:
        raw_a, raw_b = rng_m.group(1).strip(), rng_m.group(2).strip()
        # Drop trailing junk after the date token
        raw_a = re.split(r"[?!.,;]", raw_a)[0].strip()
        raw_b = re.split(r"[?!.,;]", raw_b)[0].strip()

        a = _parse_single(raw_a)
        b = _parse_single(raw_b, as_end=True)
        if a and not b:
            b = _parse_single(raw_b, default_year=a.year, as_end=True)
        elif b and not a:
            a = _parse_single(raw_a, default_year=b.year)
        elif not a and not b:
            a = _parse_single(raw_a, default_year=today.year)
            b = _parse_single(raw_b, default_year=today.year, as_end=True)

        if a and b:
            if a > b:
                a, b = b, a
            src = ("between" if rng_m.re is _RANGE_BETWEEN
                   else "urdu_se_tak" if rng_m.re is _RANGE_URDU
                   else "from_to")
            return DateRange(a, b, src, rng_m.group(0))

    # ── Two ISO dates standalone ──
    iso_dates = _ISO_DATE.findall(text)
    if len(iso_dates) >= 2:
        try:
            ds = sorted(date(int(y), int(mo), int(d)) for y, mo, d in iso_dates)
            return DateRange(ds[0], ds[-1], "iso_pair",
                             f"{ds[0].isoformat()} to {ds[-1].isoformat()}")
        except ValueError:
            pass

    # ── Two DMY dates standalone ──
    dmy_matches = _DMY_DATE.findall(text)
    if len(dmy_matches) >= 2:
        try:
            ds = sorted(date(int(y), int(mo), int(d)) for d, mo, y in dmy_matches)
            return DateRange(ds[0], ds[-1], "dmy_pair",
                             f"{ds[0].isoformat()} to {ds[-1].isoformat()}")
        except ValueError:
            pass

    # ── Single ISO date ──
    m = _ISO_DATE.search(text)
    if m:
        try:
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return DateRange(d, d, "iso_single", m.group(0))
        except ValueError:
            pass

    # ── Single DMY ──
    m = _DMY_DATE.search(text)
    if m:
        try:
            d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            return DateRange(d, d, "dmy_single", m.group(0))
        except ValueError:
            pass

    # ── Single named date ──
    m = _DMY_NAMED.search(text)
    if m:
        try:
            d = date(int(m.group(3)), _MONTHS[m.group(2).lower()], int(m.group(1)))
            return DateRange(d, d, "named_single", m.group(0))
        except (ValueError, KeyError):
            pass
    m = _MDY_NAMED.search(text)
    if m:
        try:
            d = date(int(m.group(3)), _MONTHS[m.group(1).lower()], int(m.group(2)))
            return DateRange(d, d, "named_single", m.group(0))
        except (ValueError, KeyError):
            pass

    # ── Standalone "Month Year" / "Year Month" ──
    m = _MONTH_YEAR.search(text) or _YEAR_MONTH.search(text)
    if m:
        try:
            if m.re is _MONTH_YEAR:
                month = _MONTHS[m.group(1).lower()]
                y = int(m.group(2))
            else:
                y = int(m.group(1))
                month = _MONTHS[m.group(2).lower()]
            start = date(y, month, 1)
            end = date(y, month, _last_day_of_month(y, month))
            return DateRange(start, end, "month_year", m.group(0))
        except (ValueError, KeyError):
            pass

    # ── Yearless single date (default year = today.year) ──
    m = _DM_YEARLESS.search(text)
    if m:
        try:
            d = date(today.year, _MONTHS[m.group(2).lower()], int(m.group(1)))
            return DateRange(d, d, "yearless_single", m.group(0))
        except (ValueError, KeyError):
            pass
    m = _MD_YEARLESS.search(text)
    if m:
        try:
            d = date(today.year, _MONTHS[m.group(1).lower()], int(m.group(2)))
            return DateRange(d, d, "yearless_single", m.group(0))
        except (ValueError, KeyError):
            pass

    return None


# ── Convenience helpers ──────────────────────────────────────
def has_recent_range(dr: DateRange, today: Optional[date] = None,
                     tz: str = "Asia/Karachi") -> bool:
    """True if the range includes today, yesterday, current week/month,
    last 7 days, OR any date within the last 14 days. Used to decide
    whether to prefer the live API over stored snapshots.
    """
    if dr is None:
        return False
    if today is None:
        today = _today_in_tz(tz)
    cutoff = today - timedelta(days=14)
    return dr.end >= cutoff


def format_date_range(dr: DateRange) -> str:
    return f"{dr.start.isoformat()} to {dr.end.isoformat()}"


def detect_unparsed_date_expression(text: str) -> bool:
    """True if `text` contains a date-looking expression we did NOT manage
    to parse. Lets callers detect "user typed a date but parser failed"
    instead of treating unparseable input as no-date.
    """
    if not text:
        return False
    if parse_date_range(text) is not None:
        return False
    # crude signal: digits or month name present + a range word
    has_digit = re.search(r"\d{1,4}", text) is not None
    has_month = re.search(_MONTH_RE, text, re.I) is not None
    has_range = re.search(r"\b(from|between|to|till|until|se|tak|last\s+\w+|past\s+\w+)\b",
                          text, re.I) is not None
    return (has_digit or has_month) and has_range
