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
# Per-call timeout for live SDEO endpoints. Lowered from 30s so a slow
# auxiliary endpoint cannot stack into a multi-minute total — the
# tehsil-live path issues up to 4 sequential calls.
_API_TIMEOUT = 10

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
# Phase-43: officer-level financial details endpoint. Returns one row
# per officer with totalChallans / fineAmount / paidChallanAmount /
# unPaidChallanAmount / totalPaidChallans / totalUnPaidChallans.
# Probe results: accepts `startDate`/`endDate` date-only and matches the
# dashboard "Top Officers by Challans" tile.
PCM_OFFICER_INSPECTION_DETAILS = (
    "https://pera360.punjab.gov.pk/backend/api/Pcm/officer-inspection-details"
)


def _get_db():
    try:
        from analytics_db import get_analytics_db
        return get_analytics_db()
    except Exception:
        return None


# ── Canonical dashboard-tile source ─────────────────────────
# PCM /dashboard-counts with FromDate/ToDate/TehsilId (CAPITALISED
# params, ISO `YYYY-MM-DDTHH:MM`) returns the EXACT figures the
# PERA360 DC dashboard tiles show — challans, fine, paid/unpaid
# counts + amounts, sealed, warnings, arrests, inspections — in a
# single call per tehsil. This is the authoritative source; the
# SDEO sdeo-dashboard/* endpoints lag by the newly-issued challans
# that have not yet propagated, so they under-report.
def _fetch_pcm_dashboard_counts(
    tid: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """One PCM dashboard-counts call. Returns a normalised dict.
    Date-filtered when start/end supplied; all-time otherwise.
    `paid_n`/`unpaid_n` are the dashboard's own counts (already
    correct — no derivation needed)."""
    out = {
        "tehsil_id": tid,
        "inspections": 0, "challans": 0, "fine": 0.0,
        "paid_n": 0, "unpaid_n": 0,
        "paid_amt": 0.0, "unpaid_amt": 0.0,
        "sealed": 0, "warnings": 0, "arrests": 0, "pcm": 0,
        "err": None,
    }
    params: Dict[str, Any] = {"TehsilId": tid}
    if start_date and end_date:
        params["FromDate"] = f"{start_date.isoformat()}T00:00"
        params["ToDate"] = f"{end_date.isoformat()}T23:59"
    try:
        r = requests.get(
            PCM_DASHBOARD_COUNTS, params=params,
            headers=_HEADERS, timeout=_API_TIMEOUT,
        )
        r.raise_for_status()
        j = r.json() if isinstance(r.json(), dict) else {}
        out["inspections"] = int(j.get("totalInspections") or 0)
        out["challans"]    = int(j.get("totalChallans") or 0)
        out["fine"]        = float(j.get("totalFineAmount") or 0)
        out["paid_n"]      = int(j.get("paidChallans") or 0)
        out["unpaid_n"]    = int(j.get("unPaidChallans") or 0)
        out["paid_amt"]    = float(j.get("paidChallanAmount") or 0)
        out["unpaid_amt"]  = float(j.get("unPaidChallanAmount") or 0)
        out["sealed"]      = int(j.get("totalSealed") or 0)
        out["warnings"]    = int(j.get("totalWarnings") or 0)
        out["arrests"]     = int(j.get("totalArrest") or 0)
        out["pcm"]         = int(j.get("totalPCM") or 0)
    except Exception as e:
        out["err"] = str(e)
    return out


# ── Freshness footer — user-facing format ───────────────────
# Renders the inspection-data snapshot timestamp in the same wording
# the frontend already accepted: "Data last updated: 14 May 2026,
# 12:44 PM." Avoids depending on regex post-fixing of an internal
# `[FRESHNESS — …]` token.
_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _format_user_freshness(snapshot_date) -> str:
    try:
        from datetime import date as _date, datetime as _dt
        if isinstance(snapshot_date, str):
            try:
                snap = _dt.strptime(snapshot_date, "%Y-%m-%d").date()
            except ValueError:
                return f"Data last updated: {snapshot_date}."
        elif isinstance(snapshot_date, _dt):
            snap = snapshot_date.date()
        elif isinstance(snapshot_date, _date):
            snap = snapshot_date
        else:
            return f"Data last updated: {snapshot_date}."
        now = _dt.now()
        h = now.hour
        ampm = "PM" if h >= 12 else "AM"
        h12 = ((h + 11) % 12) + 1
        clock = f"{h12}:{now.minute:02d} {ampm}"
        return (
            f"Data last updated: "
            f"{snap.day} {_MONTH_NAMES[snap.month - 1]} {snap.year}, {clock}."
        )
    except Exception:
        return f"Data last updated: {snapshot_date}."


# ══════════════════════════════════════════════════════════════
# Keyword / intent detection
# ══════════════════════════════════════════════════════════════
# Arrest / FIR-as-arrest detection. In the PERA dashboard, "totalArrest"
# (PCM dashboard-counts endpoint) is the user-visible arrest tally and
# equals the FIR count surfaced in the tooltip. A bare "how many arrests
# in Lahore" must route to that focused number, not the full inspection
# summary table whose totals are tracked by a separate snapshot.
_ARREST_KEYWORDS = re.compile(
    r"\b("
    r"arrest(?:ed|s)?|arrest\s+case|"
    r"giraftari|giraftariyan|giraftaar(?:i|y)?|"
    r"hiraasat|hirasat|"
    r"detain(?:ed|ment)?|"
    r"kitne?\s+(?:logon|log|banday)\s+(?:ko\s+)?giraftaar"
    r")\b",
    re.I,
)


def _detect_arrest_intent(question: str) -> bool:
    """True when the user is asking about arrests / FIR-as-arrest counts."""
    return bool(_ARREST_KEYWORDS.search(question or ""))


# Enforcer tile detection. In the PERA dashboard, "Enforcer" surfaces
# the totalForceDeployed value from SDEO top-kpis (officers/personnel
# deployed for enforcement). It is its own metric — NOT arrest, NOT
# inspection officer count — and is live, date-ranged only.
_ENFORCER_KEYWORDS = re.compile(
    r"\b("
    r"enforcers?|enforcement\s+(?:officers?|staff|force)|"
    r"force\s+deployed|"
    r"deployed\s+(?:force|officers?|staff)|"
    r"kitn[ae]\s+enforcer"
    r")\b",
    re.I,
)


def _detect_enforcer_intent(question: str) -> bool:
    """True when the user is asking about enforcer / force-deployed count."""
    return bool(_ENFORCER_KEYWORDS.search(question or ""))


# Lightweight challan-keyword sniff used by the date-ranged
# short-circuit in `_query_insp_location`. Mirrors the regex from
# `_RANK_METRIC_MAP["challans"]` so the two detectors agree on what
# counts as a challan-focused question.
_FOCUSED_CHALLAN_KEYWORDS = re.compile(
    r"\bch[ae]+l+a+n+s?\b",
    re.I,
)


def _detect_focused_challan_keyword(question: str) -> bool:
    """True when the question explicitly names 'challan(s)'."""
    return bool(_FOCUSED_CHALLAN_KEYWORDS.search(question or ""))


# Single-metric detection for the PCM dashboard-counts focused path.
# Maps a keyword to the PCM-supported metric. Only metrics that the
# PCM /dashboard-counts endpoint actually returns are listed —
# sealed / warnings / challans / arrests / inspections. FIR,
# no-offenses, EPO, removal-order are NOT in that endpoint so they
# fall through to the existing SDEO path.
_PCM_FOCUS_METRICS = [
    ("sealed",      re.compile(r"\bseal(?:ed|ing)?\b|\bsealed\s+(?:cases?|premises?|shops?)\b", re.I)),
    ("warnings",    re.compile(r"\bwarning(?:s)?\b", re.I)),
    ("arrests",     re.compile(r"\barrest(?:ed|s)?\b|\barrest\s+cases?\b|\bgiraftari", re.I)),
    ("challans",    re.compile(r"\bch[ae]+l+a+n+s?\b", re.I)),
    ("inspections", re.compile(r"\binspect(?:ion)?s?\b|\bmuayina\b", re.I)),
]
_PCM_FOCUS_BROAD = re.compile(
    r"\b(summary|breakdown|overview|all|full|detailed?|complete|"
    r"performance|report|dashboard|card|kpis?|"
    r"recovery|fine|paid|unpaid)\b",
    re.I,
)


def _detect_pcm_focus_metric(question: str) -> Optional[str]:
    """Return the single PCM-supported metric the user asked about,
    or None when zero / multiple metrics match, or when the question
    is a broad multi-metric request (summary / breakdown / etc.).

    'challans' is intentionally excluded here — the dedicated
    focused-challan branch handles it with the wider recovery table.
    """
    if not question:
        return None
    if _PCM_FOCUS_BROAD.search(question):
        return None
    hits = [m for m, rx in _PCM_FOCUS_METRICS if rx.search(question)]
    if len(hits) != 1:
        return None
    metric = hits[0]
    # Let the richer challan path own challan-only queries.
    if metric == "challans":
        return None
    return metric


def _query_focused_metric_location(
    db, level: str, name: str, metric: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    question: str = "",
) -> Dict[str, Any]:
    """Focused single-metric per-tehsil table for a district/division.

    Uses PCM /dashboard-counts per child tehsil so values match the
    PERA360 dashboard tiles. `metric` is one of
    sealed | warnings | arrests | inspections.
    """
    from datetime import date as _date, datetime as _dt
    from concurrent.futures import ThreadPoolExecutor, as_completed

    source_id = f"insp_{level}:{name}"
    _LABELS = {
        "sealed":      ("Sealed Premises", "premises sealed"),
        "warnings":    ("Warnings", "warnings issued"),
        "arrests":     ("Arrests", "arrests made"),
        "inspections": ("Inspections", "inspections conducted"),
    }
    col_label, phrase = _LABELS.get(metric, (metric.title(), metric))

    # Resolve target tehsils. Tehsil-level → single row; district /
    # division → all child tehsils.
    try:
        if level == "tehsil":
            tehsils = db.fetch_all(
                "SELECT tehsil_id, tehsil_name FROM dim_tehsil "
                "WHERE tehsil_name = %s",
                (name,),
            ) or []
        elif level == "district":
            tehsils = db.fetch_all(
                "SELECT t.tehsil_id, t.tehsil_name FROM dim_tehsil t "
                "JOIN dim_district d ON d.district_id = t.district_id "
                "WHERE d.district_name = %s ORDER BY t.tehsil_name",
                (name,),
            ) or []
        else:  # division
            tehsils = db.fetch_all(
                "SELECT t.tehsil_id, t.tehsil_name FROM dim_tehsil t "
                "JOIN dim_district d ON d.district_id = t.district_id "
                "JOIN dim_division v ON v.division_id = d.division_id "
                "WHERE v.division_name = %s ORDER BY t.tehsil_name",
                (name,),
            ) or []
    except Exception as e:
        log.warning("focused-metric tehsil resolve failed: %s", e)
        tehsils = []

    if not tehsils:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": (
                f"No tehsils found under {level} '{name}'.\n"
            ),
        }

    def _fetch(tid: int, tname: str) -> Dict[str, Any]:
        c = _fetch_pcm_dashboard_counts(tid, start_date, end_date)
        return {"tehsil": tname, "value": int(c.get(metric) or 0)}

    rows_out: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(8, max(2, len(tehsils)))) as ex:
        futs = [ex.submit(_fetch, t["tehsil_id"], t["tehsil_name"])
                for t in tehsils]
        for f in as_completed(futs):
            rows_out.append(f.result())

    rows_out.sort(key=lambda x: x["value"], reverse=True)
    total = sum(r["value"] for r in rows_out)

    has_dates = bool(start_date and end_date)
    scope = (f" ({start_date.strftime('%d %b %Y')} – "
             f"{end_date.strftime('%d %b %Y')})") if has_dates else " (all-time)"

    if level == "tehsil":
        intro = (
            f"In {name} Tehsil{scope}, PERA recorded "
            f"{total:,} {phrase}."
        )
        # Single-row breakdown is redundant for a tehsil — skip table.
        body = ""
    else:
        intro = (
            f"In the {name} {level.title()}{scope}, PERA recorded "
            f"{total:,} {phrase} across {len(rows_out)} tehsil(s)."
        )
        body = f"\n\nTehsil-wise {col_label}:\n\n"
        body += f"| Tehsil | {col_label} |\n|---|---:|\n"
        for r in rows_out:
            body += f"| {r['tehsil']} | {r['value']:,} |\n"
        body += f"| **Total** | **{total:,}** |\n"
    now = _dt.now()
    h = now.hour
    ampm = "PM" if h >= 12 else "AM"
    h12 = ((h + 11) % 12) + 1
    body += (
        f"\nData last updated: {now.day} {_MONTH_NAMES[now.month - 1]} "
        f"{now.year}, {h12}:{now.minute:02d} {ampm}.\n"
    )

    return {
        "source_id": source_id,
        "records": rows_out,
        "formatted_context": (
            f"{col_label} — {level.title()}: {name}{scope}\n"
            + "=" * 50 + "\n"
            + f"Total {col_label}: {total:,}\n"
        ),
        "direct_answer": intro + body,
    }


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
    r"enforcers?|enforcement\s+(?:officers?|staff|force)|"
    r"force\s+deployed|deployed\s+force|"
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
    r"overall\s+(?:summary|performance|figures|report)|"
    # Phase-39: dashboard / report-card / KPI queries with a location
    # + date range should land on the inspection performance handler
    # so the user gets a full SDEO-dashboard-style answer rather than
    # a generic challan-only or doc-RAG response.
    r"\bperformance\b|"
    r"\breport\s*card\b|"
    r"\bdashboard\b|"
    r"\bkpis?\b|"
    r"\boverview\b|"
    r"\bcard\s+summary\b|"
    # Phase: explicit "breakdown" / "division-wise" / "district-wise"
    # variants. These read as authority-style requests for a full
    # tabular breakdown and should always land on the inspection
    # performance handler.
    r"\bbreakdown\b|"
    r"\b(?:division|district|tehsil|station)[-\s]?wise\b|"
    # Phase-41: financial metric keywords. A query like
    # "Multan City paid amount from 1 April to 20 April 2026" needs
    # to reach the inspection performance handler so the focused
    # extractor can render a clean Paid Amount card.
    r"\bfine\s*(?:amount|imposed|recovered)?\b|"
    r"\bpaid\s+amount\b|"
    r"\bunpaid\s+amount\b|"
    r"\boutstanding\s+amount\b|"
    r"\brecover(?:ed|y)\s+amount\b|"
    r"\brecovery\b|"
    r"\bpaid\s+ch[ae]+l+a+ns?\b|"
    r"\bunpaid\s+ch[ae]+l+a+ns?\b"
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
    # Suffix-alias normalisation. Users routinely abbreviate tehsil
    # suffixes: "lahore cant" → "Lahore Cantt", "model twn" → "Model
    # Town". Without this pre-pass, substring matching falls through
    # to the parent district and "lahore cant" mis-routes to the
    # whole Lahore district.
    _SUFFIX_ALIASES = [
        (r"\bcant\b",   "cantt"),   # cant → cantt
        (r"\bcantnt\b", "cantt"),
        (r"\btwn\b",    "town"),
        (r"\bcty\b",    "city"),
        (r"\bsdr\b",    "saddar"),
        (r"\bsadar\b",  "saddar"),  # alt spelling
    ]
    for pat, repl in _SUFFIX_ALIASES:
        q_lower = re.sub(pat, repl, q_lower)

    # ── pera_entities alias resolver runs FIRST (production wiring) ──
    # Catches D.G. Khan / DG Khan / Dera Ghazi Khan and any other alias
    # entries before the substring-match path runs. SKIPPED when the
    # user explicitly typed "district" or "tehsil" — the alias map
    # only carries division names, so promoting to division on a
    # district query would be wrong.
    _explicit_non_division = re.search(
        r"\b(districts?|tehsil[s]?|station[s]?)\b", q_lower,
    )
    if not _explicit_non_division:
        try:
            from pera_entities import canonical_division_name
            canon_div = canonical_division_name(question)
            if canon_div:
                return {"level": "division", "division_name": canon_div}
        except Exception:
            pass

    # If user explicitly says "division(s)" / "district(s)" / "tehsil(s)" /
    # "station(s)", respect that. The trailing `s?` matters — "Gujrat
    # districts" (plural) must be detected as district level, otherwise
    # the loop falls through to the tehsil-first default which would
    # mis-route to `insp_tehsil:Gujrat` since Gujrat is BOTH a district
    # and a tehsil name.
    explicit_div = re.search(r"\bdivisions?\b", q_lower)
    explicit_dist = re.search(r"\bdistricts?\b", q_lower)
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


# ── Comparison detection (multi-location queries) ───────────
_COMPARISON_KEYWORDS = re.compile(
    r"\b("
    r"vs|versus|"
    r"compare|comparison|compar(?:ing|e)|"
    r"between|"
    r"side[\s-]?by[\s-]?side|"
    r"muqabla|moqabla|muqabila|"
    r"vs\.|"
    r"or\s+(?:between|in)|"
    r"and\s+(?:between|in)"
    r")\b",
    re.I,
)


def _detect_comparison_intent(question: str) -> bool:
    """True when user wants to compare two+ locations (or two periods)."""
    if not question:
        return False
    if _COMPARISON_KEYWORDS.search(question):
        return True
    # Even without an explicit keyword, two location names separated
    # by " and " counts as a comparison request. We don't trigger here
    # — too aggressive — but the multi-location parser will still
    # return both, so downstream code can detect the case.
    return False


def _detect_multiple_locations(question: str) -> List[Dict[str, str]]:
    """Return a list of {level, <level>_name} for every location
    mentioned in the question. De-duplicates and preserves order.

    Recognises tehsils first (since users often pair tehsil names
    against district names), then districts, then divisions. Reuses
    the same suffix-alias normalisation as `_detect_location`.
    """
    _load_location_cache()
    if not question:
        return []
    q_lower = question.lower()
    _SUFFIX_ALIASES = [
        (r"\bcant\b",   "cantt"),
        (r"\bcantnt\b", "cantt"),
        (r"\btwn\b",    "town"),
        (r"\bcty\b",    "city"),
        (r"\bsdr\b",    "saddar"),
        (r"\bsadar\b",  "saddar"),
    ]
    for pat, repl in _SUFFIX_ALIASES:
        q_lower = re.sub(pat, repl, q_lower)

    matched: List[Dict[str, str]] = []
    seen: set = set()

    # Build (start_position, level, name) tuples so the final list
    # preserves the order locations were mentioned.
    candidates: List[Tuple[int, str, str]] = []
    for t in (_TEHSIL_NAMES or []):
        pos = q_lower.find(t.lower())
        if pos >= 0:
            candidates.append((pos, "tehsil", t))
    for d in (_DISTRICT_NAMES or []):
        pos = q_lower.find(d.lower())
        if pos >= 0:
            candidates.append((pos, "district", d))
    for dv in (_DIVISION_NAMES or []):
        pos = q_lower.find(dv.lower())
        if pos >= 0:
            candidates.append((pos, "division", dv))

    # Sort by position, prefer the longest match per character span
    # so "Lahore City" is preferred over "Lahore" when both match.
    candidates.sort(key=lambda x: (x[0], -len(x[2])))

    # Walk left-to-right, dropping matches whose span overlaps a name
    # already accepted at the same start position. Keeps "Lahore
    # Cantt" but drops the bare "Lahore" inside it.
    consumed_spans: List[Tuple[int, int]] = []
    for pos, level, name in candidates:
        span = (pos, pos + len(name))
        overlap = any(
            not (span[1] <= s[0] or span[0] >= s[1])
            for s in consumed_spans
        )
        if overlap:
            continue
        consumed_spans.append(span)
        key = (level, name)
        if key in seen:
            continue
        seen.add(key)
        matched.append({"level": level, f"{level}_name": name})

    # Qualifier-drop. When the user says "lahore wahga vs lahore city"
    # they mean "Wahga (in Lahore) vs Lahore City (in Lahore)" — the
    # standalone "Lahore" district mention is a qualifier, not a
    # comparison target. Drop a district from the result list when
    # one of its child tehsils is also in the list. Same for a
    # division whose child district/tehsil is in the list.
    if len(matched) >= 2:
        try:
            from analytics_db import get_analytics_db
            _db = get_analytics_db()
            if _db is not None:
                tehsil_names = [m["tehsil_name"] for m in matched
                                if m["level"] == "tehsil"]
                district_names = [m["district_name"] for m in matched
                                  if m["level"] == "district"]
                # Build parent district set for the matched tehsils.
                parent_districts_of_tehsils: set = set()
                if tehsil_names:
                    rows = _db.fetch_all(
                        "SELECT d.district_name "
                        "FROM dim_tehsil t "
                        "JOIN dim_district d ON d.district_id = t.district_id "
                        "WHERE t.tehsil_name = ANY(%s)",
                        (tehsil_names,),
                    ) or []
                    parent_districts_of_tehsils = {
                        r["district_name"] for r in rows
                    }
                # Drop districts whose tehsils are already in the list.
                pruned: List[Dict[str, str]] = []
                for m in matched:
                    if (m["level"] == "district"
                            and m["district_name"] in parent_districts_of_tehsils):
                        continue
                    pruned.append(m)
                if len(pruned) >= 2:
                    matched = pruned
        except Exception:
            pass

    return matched


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
    ("arrest_count",  re.compile(
        r"\barrest(?:ed|s)?\b|\barrest\s+cases?\b|\bgiraftari(?:yan|ya[an]?)?\b",
        re.I)),
    ("epo",           re.compile(
        r"\bepo\b|\bemergency\s+protection\s+orders?\b", re.I)),
    ("removal_order", re.compile(
        r"\bremoval\s+orders?\b|\bremoval[\s-]?orders?\b", re.I)),
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


_OFFICER_RANK_RE = re.compile(r"\btop\s+\d*\s*officers?\b|\bofficers?\s+by\b", re.I)


def _detect_rank_intent(q: str) -> Optional[str]:
    """Return ranking intent or None.

    Phase-44: when the user asks "top officers by X" without a
    location, return None — there is no province-wide officer
    ranking endpoint, and ranking by tehsil would mis-answer the
    user. The fastapi operational-refusal gate then asks for a
    location/scope.
    """
    if _OFFICER_RANK_RE.search(q or ""):
        # Block officer-ranking when the query has no location anchor.
        # No public province-wide officer-ranking endpoint exists, and
        # falling through to tehsil-level ranking would mis-answer.
        # The fastapi operational-refusal gate then asks for scope.
        has_location_kw = bool(re.search(
            r"\b(tehsil|station|district|division)s?\b", q or "", re.I
        ))
        if not has_location_kw:
            return None
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

    # If the question names a parent location strictly broader than the
    # ranking level (e.g. "stations in Lahore district"), scope the
    # ranking to that parent location instead of returning a province-
    # wide ranking.
    parent_suffix = ""
    try:
        parent_loc = _detect_location(q)
    except Exception:
        parent_loc = None
    if parent_loc:
        plevel = parent_loc.get("level")
        pname = parent_loc.get(f"{plevel}_name") if plevel else None
        level_rank = {"tehsil": 1, "district": 2, "division": 3}
        if (plevel and pname
                and level_rank.get(plevel, 0) > level_rank.get(level, 0)):
            parent_suffix = f":{plevel}:{pname}"

    return f"insp_top:{metric}:{level}:{order}{parent_suffix}"


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
    keep working. As a final fallback, recognise bare month names
    ("april", "april month", "in april") and map them to the most
    recently completed occurrence of that month — users frequently
    drop the year when asking for a recent period.
    """
    # Normalise common month-name typos / spellings BEFORE parsing so
    # "fab" → "feb", "marhc" → "march" etc. don't silently fail and
    # collapse the range to a single day.
    if question:
        _MONTH_TYPOS = [
            (r"\bfab(?:ruary)?\b",   "february"),
            (r"\bfeb(?:rurary|rary|uary)?\b", "february"),
            (r"\bjanuray\b",         "january"),
            (r"\bjnauary\b",         "january"),
            (r"\bmarhc\b",           "march"),
            (r"\bmarch?h\b",         "march"),
            (r"\bapril?l\b",         "april"),
            (r"\baprl\b",            "april"),
            (r"\bjune?e\b",          "june"),
            (r"\bjuly?y\b",          "july"),
            (r"\bagust\b",           "august"),
            (r"\baugst\b",           "august"),
            (r"\bseptmber\b",        "september"),
            (r"\bseptember?r\b",     "september"),
            (r"\boctuber\b",         "october"),
            (r"\bnovmber\b",         "november"),
            (r"\bdecmber\b",         "december"),
            (r"\bdecember?r\b",      "december"),
        ]
        _qn = question
        for pat, repl in _MONTH_TYPOS:
            _qn = re.sub(pat, repl, _qn, flags=re.I)
        question = _qn

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

    # Bare-month fallback. "april" / "april month" / "in april" / "of
    # april" map to the most recent occurrence — current year when the
    # month has already started/ended, otherwise the previous year.
    if question:
        _MONTHS = {
            "january": 1, "jan": 1, "february": 2, "feb": 2,
            "march": 3, "mar": 3, "april": 4, "apr": 4,
            "may": 5, "june": 6, "jun": 6,
            "july": 7, "jul": 7, "august": 8, "aug": 8,
            "september": 9, "sep": 9, "sept": 9,
            "october": 10, "oct": 10, "november": 11, "nov": 11,
            "december": 12, "dec": 12,
        }
        from datetime import date as _date
        from calendar import monthrange
        today = _date.today()
        m = re.search(
            r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|"
            r"may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t|tember)?|"
            r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b",
            question.lower(),
        )
        if m:
            mn = _MONTHS.get(m.group(1))
            if mn:
                # Most recent year where that month has BEGUN. If the
                # month is the current month or earlier in this year,
                # use this year. Otherwise step back a year.
                year = today.year if mn <= today.month else today.year - 1
                last_day = monthrange(year, mn)[1]
                # Cap the end date at today when the month is the
                # current (in-progress) month so we don't claim future
                # data we don't have.
                end_day = (today.day
                           if (year == today.year and mn == today.month)
                           else last_day)
                return (_date(year, mn, 1), _date(year, mn, end_day))

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
    # Comparison shortcut — runs BEFORE ranking. "X vs Y" with two
    # named locations is a comparison, NOT a province-wide ranking.
    # Without this, "lahore districts vs multan districts paid
    # challans" matches the ranking shortcut ("paid challans" + level)
    # and dumps a 20-row tehsil ranking instead of a 2-column compare.
    if _detect_comparison_intent(q):
        _cmp_locs = _detect_multiple_locations(q)
        if len(_cmp_locs) >= 2:
            parts = [f"{loc['level']}|{loc[loc['level']+'_name']}"
                     for loc in _cmp_locs]
            return "insp_compare:" + "::".join(parts)

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

    # Comparison short-circuit BEFORE single-location detection.
    # When the user explicitly asks for a comparison ("X vs Y",
    # "compare X and Y", "muqabla", "between A and B") and two or
    # more locations are mentioned, route to the dedicated compare
    # handler so the response is a side-by-side table rather than the
    # first-match-only summary that single-location detection would
    # build.
    if _detect_comparison_intent(q):
        locs = _detect_multiple_locations(q)
        if len(locs) >= 2:
            # Encode the location list into the source_id payload.
            # Format: insp_compare:level1|name1::level2|name2::...
            # The dispatcher splits on "::" then on "|" to recover.
            parts = [f"{loc['level']}|{loc[loc['level']+'_name']}"
                     for loc in locs]
            return "insp_compare:" + "::".join(parts)

    # Location detection runs BEFORE officer detection so admin-level
    # queries don't get hijacked by greedy name-pair LIKE matches on
    # tokens that happen to overlap with officer names.
    location = _detect_location(q)
    if location:
        level = location["level"]
        # Arrest queries with an explicit location route to the focused
        # arrest handler so the answer surfaces PCM totalArrest, not the
        # full inspection-summary table (whose totals come from a
        # different upstream API and confuse the user when they don't
        # match the dashboard tooltip).
        if _detect_arrest_intent(q):
            if level == "division":
                return f"insp_arrest_division:{location['division_name']}"
            elif level == "district":
                return f"insp_arrest_district:{location['district_name']}"
            elif level == "tehsil":
                return f"insp_arrest_tehsil:{location['tehsil_name']}"
        # Enforcer queries route to the focused enforcer handler
        # (totalForceDeployed from SDEO top-kpis, summed across child
        # tehsils with a wide default date range).
        if _detect_enforcer_intent(q):
            if level == "division":
                return f"insp_enforcer_division:{location['division_name']}"
            elif level == "district":
                return f"insp_enforcer_district:{location['district_name']}"
            elif level == "tehsil":
                return f"insp_enforcer_tehsil:{location['tehsil_name']}"
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
        return _query_insp_summary(db, start_date, end_date, question=question)
    elif base == "insp_division":
        name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_location(db, "division", name, start_date, end_date,
                                    question=question)
    elif base == "insp_district":
        name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_location(db, "district", name, start_date, end_date,
                                    question=question)
    elif base == "insp_tehsil":
        name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_location(db, "tehsil", name, start_date, end_date,
                                    question=question)
    elif base in ("insp_arrest_division", "insp_arrest_district", "insp_arrest_tehsil"):
        level = base.split("_", 2)[2]  # division | district | tehsil
        name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_arrest_location(db, level, name)
    elif base in ("insp_enforcer_division", "insp_enforcer_district", "insp_enforcer_tehsil"):
        level = base.split("_", 2)[2]
        name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_enforcer_location(db, level, name, start_date, end_date,
                                        question=question)
    elif base == "insp_compare":
        # Payload format: insp_compare:level1|name1::level2|name2::...
        payload = ":".join(parts[1:]) if len(parts) > 1 else ""
        locs: List[Dict[str, str]] = []
        for chunk in payload.split("::"):
            if "|" not in chunk:
                continue
            lvl, name = chunk.split("|", 1)
            lvl = lvl.strip()
            name = name.strip()
            if lvl in ("tehsil", "district", "division") and name:
                locs.append({"level": lvl, "name": name})
        if len(locs) < 2:
            return None
        return _query_compare_locations(db, locs, start_date, end_date,
                                         question=question)
    elif base == "insp_officer":
        officer_name = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_officer(db, officer_name, start_date, end_date,
                                   question=question)
    elif base == "insp_cnic":
        cnic = ":".join(parts[1:]) if len(parts) > 1 else ""
        return _query_insp_cnic(db, cnic)
    elif base == "insp_repeat_offenders":
        return _query_repeat_offenders(db)
    elif base == "insp_top":
        # Format: insp_top:<metric>:<level>:<order>[:<parent_type>:<parent_name>]
        metric = parts[1] if len(parts) > 1 else "firs"
        level = parts[2] if len(parts) > 2 else "tehsil"
        order = parts[3] if len(parts) > 3 else "desc"
        parent_type = parts[4] if len(parts) > 4 else None
        parent_name = ":".join(parts[5:]) if len(parts) > 5 else None
        return _query_top_locations(db, metric, level, order,
                                    question=question,
                                    parent_type=parent_type,
                                    parent_name=parent_name)

    return None


def _query_top_by_amount(
    db, status_filter: Optional[str], sum_col: str, level: str,
    order: str = "desc", metric_name: str = "fine_amount",
    question: str = "",
) -> Dict[str, Any]:
    """Rank tehsils/districts/divisions by SUM(<sum_col>) from challan_data,
    optionally filtered to a payment status.

    Phase-41: when the user's question carries a date range, apply
    `WHERE action_date BETWEEN …` so the ranking is date-filtered
    rather than silently all-time.
    """
    direction = "ASC" if (order or "").lower() == "asc" else "DESC"
    label_map = {
        "paid_amount": "Paid Amount",
        "unpaid_amount": "Outstanding Amount",
        "overdue_amount": "Outstanding Amount",
        "fine_amount": "Fine Imposed",
    }
    metric_label = label_map.get(metric_name, "Amount (Rs)")
    level_label = {"tehsil": "Tehsil",
                   "district": "District",
                   "division": "Division"}[level]

    # Detect optional date range from the question.
    user_start = user_end = None
    try:
        from pera_dates import parse_date_range
        dr = parse_date_range(question or "")
        if dr is not None:
            user_start, user_end = dr.start, dr.end
    except Exception:
        user_start = user_end = None

    where_parts: List[str] = []
    args: List[Any] = []
    if status_filter:
        where_parts.append("LOWER(challan_status) = %s")
        args.append(status_filter)
    if user_start and user_end:
        where_parts.append("action_date BETWEEN %s AND %s")
        args.extend([user_start, user_end])
    where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    args_tuple = tuple(args)
    scope_note = (
        f"date-filtered ({user_start} to {user_end})" if user_start and user_end
        else "all-time stored challan records"
    )

    if level == "tehsil":
        rows = db.fetch_all(
            f"SELECT tehsil_name AS loc_name, "
            f"       COALESCE(SUM({sum_col}), 0) AS metric_value, "
            f"       COUNT(*) AS challan_count "
            f"FROM challan_data {where_clause} "
            f"GROUP BY tehsil_name "
            f"ORDER BY metric_value {direction} NULLS LAST "
            f"LIMIT 25",
            args_tuple,
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
            args_tuple,
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
            args_tuple,
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
    ctx = f"### Ranking — {level_label}s by {metric_label} ({sort_word} first)\n\n"
    ctx += f"**Scope:** {scope_note}  \n"
    ctx += "**Source:** Stored challan_data table\n\n"
    ctx += f"| Rank | {level_label} | {metric_label} | Challans |\n"
    ctx += "| --- | --- | ---: | ---: |\n"
    for i, r in enumerate(rows, 1):
        ctx += (
            f"| {i} | {r.get('loc_name') or '—'} "
            f"| Rs. {int(r.get('metric_value') or 0):,} "
            f"| {int(r.get('challan_count') or 0):,} |\n"
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
    question: str = "",
    parent_type: Optional[str] = None,
    parent_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Rank tehsils/districts/divisions by a metric using the latest
    snapshot in inspection_performance. Used for "which station has the
    most FIRs" / "top 5 tehsils by sealed" / "sab sy ziada konsa stations
    fir kr raha hy" type queries.

    `question` is forwarded so amount rankings can detect a date range
    and apply WHERE action_date BETWEEN … instead of returning all-time
    silently.

    `parent_type`/`parent_name` scope the ranking to a parent location
    (e.g. tehsils within a specific district). Both must be supplied
    together; ignored otherwise.
    """
    # Normalize parent filter — only honor when parent is strictly
    # broader than the ranking level.
    _level_rank = {"tehsil": 1, "district": 2, "division": 3}
    if parent_type and parent_name and (
        _level_rank.get(parent_type, 0) <= _level_rank.get(level, 0)
    ):
        parent_type = parent_name = None
    safe_metrics = {"firs", "sealed", "challans", "warnings",
                    "no_offenses", "total_actions",
                    "arrest_count", "epo", "removal_order",
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

    # ── Date-range aware ranking for count metrics ──
    # If question has explicit date range AND metric is a count metric that
    # maps to a per-row flag in inspection_performance_detail.details_json,
    # aggregate over the JSON instead of returning all-time cumulative.
    _detail_flag_map = {
        "firs":         "firCase",
        "sealed":       "isSealed",
        "challans":     "challanCase",
        "warnings":     "warningCase",
        "no_offenses":  "noOffense",
    }
    # Detect a date range up-front — used both for the daterange branch
    # below and (for non-supported metrics) to flag the cumulative output.
    try:
        from pera_dates import parse_date_range
        _user_dr = parse_date_range(question or "")
    except Exception:
        _user_dr = None

    if metric in _detail_flag_map and _user_dr is not None:
        res = _query_top_locations_daterange(
            db, metric, level, direction,
            _user_dr.start, _user_dr.end, _detail_flag_map[metric],
            parent_type=parent_type, parent_name=parent_name,
        )
        if res is not None:
            return res
        # Fall through to cumulative if no detail data found

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
                                    order, metric, question=question)

    # Count-based payment rankings → challan_tehsil_breakdown.
    payment_metric = {
        "paid_challans": "paid",
        "unpaid_challans": "unpaid",
        "overdue_challans": "overdue",
    }.get(metric)
    if payment_metric:
        return _query_top_by_payment_status(db, payment_metric, level, order)

    # ── arrest / epo / removal_order ──
    # inspection_performance has these as zero/NULL — actual values live
    # in officer_inspection_detail (per-officer breakdown). Aggregate by
    # tehsil/district/division within the latest snapshot.
    _oid_col_map = {
        "arrest_count":  "arrest_case",
        "epo":           "epo",
        "removal_order": "removal_order",
    }
    if metric in _oid_col_map:
        sum_col = _oid_col_map[metric]
        name_col = f"{level}_name"
        parent_filter_sql = ""
        parent_params: tuple = ()
        if parent_type and parent_name:
            parent_col = f"{parent_type}_name"
            parent_filter_sql = f" AND {parent_col} ILIKE %s "
            parent_params = (f"%{parent_name}%",)
        rows = db.fetch_all(
            f"""
            WITH latest_snap AS (
              SELECT MAX(snapshot_date) AS md FROM officer_inspection_detail
            )
            SELECT {name_col} AS loc_name,
                   SUM(COALESCE({sum_col}, 0))         AS metric_value,
                   SUM(COALESCE(arrest_case, 0))       AS arrest_count,
                   SUM(COALESCE(epo, 0))               AS epo,
                   SUM(COALESCE(removal_order, 0))     AS removal_order,
                   SUM(COALESCE(sealed, 0))            AS sealed,
                   SUM(COALESCE(total_challans, 0))    AS challans,
                   SUM(COALESCE(total_inspections, 0)) AS total_actions,
                   SUM(COALESCE(warning_count, 0))     AS warnings,
                   SUM(COALESCE(no_offense_count, 0))  AS no_offenses,
                   SUM(COALESCE(fine_amount, 0))       AS fine_amount,
                   MAX(snapshot_date)                  AS snapshot_date
            FROM officer_inspection_detail
            WHERE snapshot_date = (SELECT md FROM latest_snap)
              AND {name_col} IS NOT NULL
              {parent_filter_sql}
            GROUP BY {name_col}
            HAVING SUM(COALESCE({sum_col}, 0)) > 0
            ORDER BY metric_value {direction} NULLS LAST
            LIMIT 25
            """,
            parent_params,
        )
        metric_label_oid = {
            "arrest_count": "Arrests",
            "epo": "EPOs",
            "removal_order": "Removal Orders",
        }[metric]
        level_label_oid = {
            "tehsil": "Tehsil", "district": "District", "division": "Division"
        }[level]
        sort_word = "Lowest" if direction == "ASC" else "Highest"
        scope = (f" within {parent_name} ({parent_type})"
                 if parent_type and parent_name else "")
        snap_d = rows[0].get("snapshot_date") if rows else None
        ctx = (
            f"Ranking — {level_label_oid}s by {metric_label_oid}{scope} "
            f"({sort_word} first)\n"
        )
        if snap_d:
            ctx += f"Snapshot date: {snap_d}\n"
        if _user_dr is not None:
            date_label = (f"{_user_dr.start.strftime('%d %b %Y')} — "
                          f"{_user_dr.end.strftime('%d %b %Y')}")
            ctx += (
                f"NOTE: '{metric_label_oid}' is stored as a cumulative "
                f"all-time snapshot in officer_inspection_detail. The "
                f"requested window ({date_label}) cannot be applied to "
                f"this metric. Below ranking reflects the latest "
                f"cumulative snapshot.\n"
            )
        if not rows:
            ctx += (
                f"\nNo {metric_label_oid.lower()} recorded for any "
                f"{level_label_oid.lower()}"
                f"{(' within ' + parent_name) if parent_name else ''}.\n"
            )
            return {
                "source_id": f"insp_top:{metric}:{level}:{order}",
                "records": [], "formatted_context": ctx,
                "deterministic_answer": ctx,
                "structured_payload": ctx,
                "evidence_type": "ranking",
            }
        ctx += "=" * 50 + "\n"
        ctx += (
            f"{'Rank':<5s}{level_label_oid:<25s} "
            f"{metric_label_oid:>14s}  Insp  Chal  Seal  Warn  Arr  EPO  RmO\n"
        )
        for i, r in enumerate(rows, 1):
            ctx += (
                f"{i:<5d}{(r.get('loc_name') or '—'):<25s} "
                f"{int(r.get('metric_value') or 0):>14,}  "
                f"{int(r.get('total_actions') or 0):>4,}  "
                f"{int(r.get('challans') or 0):>4,}  "
                f"{int(r.get('sealed') or 0):>4,}  "
                f"{int(r.get('warnings') or 0):>4,}  "
                f"{int(r.get('arrest_count') or 0):>3,}  "
                f"{int(r.get('epo') or 0):>3,}  "
                f"{int(r.get('removal_order') or 0):>3,}\n"
            )
        return {
            "source_id": f"insp_top:{metric}:{level}:{order}",
            "records": rows,
            "formatted_context": ctx,
            "deterministic_answer": ctx,
            "structured_payload": ctx,
            "evidence_type": "ranking",
        }

    name_col = f"{level}_name"
    parent_filter_sql = ""
    parent_params: tuple = ()
    if parent_type and parent_name:
        parent_col = f"{parent_type}_name"
        parent_filter_sql = f" AND {parent_col} ILIKE %s "
        parent_params = (f"%{parent_name}%",)
    rows = db.fetch_all(
        f"SELECT {name_col} AS loc_name, {metric} AS metric_value, "
        f"       total_actions, challans, firs, warnings, sealed, "
        f"       arrest_count, epo, removal_order, "
        f"       snapshot_date "
        f"FROM inspection_performance "
        f"WHERE level = %s AND snapshot_date = ("
        f"  SELECT MAX(snapshot_date) FROM inspection_performance WHERE level = %s"
        f") AND {name_col} IS NOT NULL "
        f"{parent_filter_sql}"
        f"ORDER BY {metric} {direction} NULLS LAST "
        f"LIMIT 25",
        (level, level) + parent_params,
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
        "arrest_count": "Arrests",
        "epo": "EPOs",
        "removal_order": "Removal Orders",
    }[metric]
    level_label = {"tehsil": "Tehsil", "district": "District", "division": "Division"}[level]

    snap = rows[0].get("snapshot_date")
    sort_word = "Lowest" if direction == "ASC" else "Highest"
    scope = (f" within {parent_name} ({parent_type})"
             if parent_type and parent_name else "")
    # Markdown table so the frontend renders a proper grid instead of
    # a paragraph of space-padded text. The headline opener also gets
    # promoted to a one-line summary sentence so the bot reads like
    # the other deterministic-answer paths (arrest / enforcer /
    # focused-metric handlers).
    top_row = rows[0]
    top_name = top_row.get("loc_name") or "—"
    top_val = int(top_row.get("metric_value") or 0)
    intro = (
        f"Ranked by {metric_label} ({sort_word.lower()} first), "
        f"**{top_name}** leads the {level_label.lower()}s with "
        f"{top_val:,} {metric_label.lower()}{scope}."
    )
    ctx = intro + "\n\n"
    ctx += f"### Ranking — {level_label}s by {metric_label}{scope} ({sort_word} first)\n\n"
    if _user_dr is not None and metric not in _detail_flag_map:
        date_label = (f"{_user_dr.start.strftime('%d %b %Y')} — "
                      f"{_user_dr.end.strftime('%d %b %Y')}")
        ctx += (
            f"_Note: `{metric_label}` is only stored as a cumulative "
            f"all-time total. The requested window ({date_label}) cannot "
            f"be applied to this metric; ranking below is the latest "
            f"cumulative snapshot._\n\n"
        )
    # Companion columns shown alongside the ranked metric. Drop any
    # column that is the same as the rank metric so the ranked
    # number doesn't appear twice in the row.
    _extra_cols = [
        ("Inspections", "total_actions"),
        ("Challans",    "challans"),
        ("FIRs",        "firs"),
        ("Sealed",      "sealed"),
        ("Warnings",    "warnings"),
        ("No Offenses", "no_offenses"),
        ("Arrests",     "arrest_count"),
        ("EPO",         "epo"),
        ("Removal",     "removal_order"),
    ]
    _extra_cols = [(lbl, key) for (lbl, key) in _extra_cols if key != metric]
    # Drop columns where every row is 0 — typically No Offenses / EPO /
    # Removal Order / Arrests at division-level aggregate, since those
    # metrics are only populated at the tehsil-detail layer. Avoids a
    # table full of blank zero columns that looks like missing data.
    _extra_cols = [
        (lbl, key) for (lbl, key) in _extra_cols
        if any(int(r.get(key) or 0) != 0 for r in rows)
    ]
    header = f"| Rank | {level_label} | {metric_label} |"
    header += "".join(f" {lbl} |" for (lbl, _k) in _extra_cols) + "\n"
    sep = "| --- | --- | ---: |" + "".join(" ---: |" for _ in _extra_cols) + "\n"
    ctx += header + sep
    for i, r in enumerate(rows, 1):
        row_str = (
            f"| {i} | {r.get('loc_name') or '—'} "
            f"| {int(r.get('metric_value') or 0):,} |"
        )
        for (_lbl, key) in _extra_cols:
            row_str += f" {int(r.get(key) or 0):,} |"
        ctx += row_str + "\n"
    if snap:
        try:
            ctx += "\n" + _format_user_freshness(snap) + "\n"
        except Exception:
            ctx += f"\nSnapshot date: {snap}\n"
    if _user_dr is not None and metric in _detail_flag_map:
        ctx += (
            f"\n_No per-row SDEO detail rows matched the requested "
            f"window; falling back to the latest cumulative snapshot._\n"
        )
    else:
        ctx += (
            f"\n_Ranking computed from the latest stored snapshot of "
            f"`inspection_performance`. Cumulative all-time totals; "
            f"not date-range filtered._\n"
        )
    return {
        "source_id": f"insp_top:{metric}:{level}:{order}",
        "records": rows,
        "formatted_context": ctx,
        "deterministic_answer": ctx,
        "structured_payload": ctx,
        "evidence_type": "ranking",
    }


# ── Date-range aware top-N ranking using detail JSON ─────────
def _query_top_locations_daterange(
    db, metric: str, level: str, direction: str,
    start_dt, end_dt, flag_key: str,
    parent_type: Optional[str] = None,
    parent_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Rank tehsils/districts/divisions by a per-row count metric
    filtered to an explicit [start_dt, end_dt] window.

    Source: inspection_performance_detail.details_json (the SDEO
    detail blob containing one JSON element per inspection action
    with fields actionDate, firCase, challanCase, warningCase,
    isSealed, noOffense).

    When `parent_type`/`parent_name` are provided, the ranking is
    further scoped to that parent location.
    """
    name_col = f"{level}_name"
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()
    parent_filter_sql = ""
    parent_params: tuple = ()
    if parent_type and parent_name:
        parent_col = f"{parent_type}_name"
        parent_filter_sql = f" AND {parent_col} ILIKE %s "
        parent_params = (f"%{parent_name}%",)

    sql = f"""
    WITH latest_detail AS (
      SELECT DISTINCT ON (tehsil_id)
             tehsil_id, tehsil_name, district_name, division_name,
             details_json, snapshot_date
      FROM inspection_performance_detail
      WHERE details_json IS NOT NULL
      ORDER BY tehsil_id, snapshot_date DESC
    ),
    expanded AS (
      SELECT ld.tehsil_name, ld.district_name, ld.division_name,
             (d->>'actionDate')::timestamptz AS action_dt,
             COALESCE((d->>'firCase')::boolean, false)     AS is_fir,
             COALESCE((d->>'challanCase')::boolean, false) AS is_challan,
             COALESCE((d->>'warningCase')::boolean, false) AS is_warning,
             COALESCE((d->>'isSealed')::boolean, false)    AS is_sealed,
             COALESCE((d->>'noOffense')::boolean, false)   AS is_no_offense
      FROM latest_detail ld,
           LATERAL jsonb_array_elements(
               COALESCE(ld.details_json->'details', '[]'::jsonb)
           ) d
      WHERE (d->>'actionDate') IS NOT NULL
    )
    SELECT {name_col} AS loc_name,
           SUM(CASE WHEN is_fir       THEN 1 ELSE 0 END) AS firs,
           SUM(CASE WHEN is_challan   THEN 1 ELSE 0 END) AS challans,
           SUM(CASE WHEN is_warning   THEN 1 ELSE 0 END) AS warnings,
           SUM(CASE WHEN is_sealed    THEN 1 ELSE 0 END) AS sealed,
           SUM(CASE WHEN is_no_offense THEN 1 ELSE 0 END) AS no_offenses,
           COUNT(*) AS total_actions
    FROM expanded
    WHERE action_dt >= %s::timestamptz
      AND action_dt <  (%s::date + INTERVAL '1 day')
      AND {name_col} IS NOT NULL
      {parent_filter_sql}
    GROUP BY {name_col}
    HAVING SUM(CASE WHEN is_fir       THEN 1 ELSE 0 END) +
           SUM(CASE WHEN is_challan   THEN 1 ELSE 0 END) +
           SUM(CASE WHEN is_warning   THEN 1 ELSE 0 END) +
           SUM(CASE WHEN is_sealed    THEN 1 ELSE 0 END) +
           SUM(CASE WHEN is_no_offense THEN 1 ELSE 0 END) > 0
    ORDER BY {metric} {direction} NULLS LAST
    LIMIT 25
    """

    try:
        rows = db.fetch_all(sql, (start_iso, end_iso) + parent_params)
    except Exception as e:
        log.warning("Date-range top-locations query failed: %s", e)
        return None

    if not rows:
        return None

    metric_label = {
        "firs": "FIRs",
        "sealed": "Sealed Premises",
        "challans": "Challans",
        "warnings": "Warnings",
        "no_offenses": "No-Offense Cases",
    }.get(metric, metric)
    level_label = {"tehsil": "Tehsil",
                   "district": "District",
                   "division": "Division"}[level]
    sort_word = "Lowest" if direction == "ASC" else "Highest"
    date_label = f"{start_dt.strftime('%d %b %Y')} — {end_dt.strftime('%d %b %Y')}"

    # Build column list — skip duplicate of the ranked metric.
    extra_cols = [
        ("Inspections", "total_actions"),
        ("Challans",    "challans"),
        ("FIRs",        "firs"),
        ("Sealed",      "sealed"),
        ("Warnings",    "warnings"),
    ]
    extra_cols = [(lbl, key) for (lbl, key) in extra_cols if key != metric]

    scope = (f" within {parent_name} ({parent_type})"
             if parent_type and parent_name else "")
    ctx = f"### Ranking — {level_label}s by {metric_label}{scope} ({sort_word} first)\n\n"
    ctx += f"**Scope:** date-filtered ({date_label}){scope}  \n"
    ctx += "**Source:** inspection_performance_detail.details_json (per-row SDEO actions)\n\n"
    header = f"| Rank | {level_label} | {metric_label} |" + "".join(
        f" {lbl} |" for (lbl, _k) in extra_cols
    ) + "\n"
    sep = "| --- | --- | ---: |" + "".join(" ---: |" for _ in extra_cols) + "\n"
    ctx += header + sep
    for i, r in enumerate(rows, 1):
        row_str = (f"| {i} | {r.get('loc_name') or '—'} "
                   f"| {int(r.get(metric) or 0):,} |")
        for (_lbl, key) in extra_cols:
            row_str += f" {int(r.get(key) or 0):,} |"
        ctx += row_str + "\n"
    ctx += (
        f"\n(Ranking computed from per-row SDEO actions in the requested window. "
        f"Each detail row is one inspection action; counts above are restricted "
        f"to the {date_label} period.)\n"
    )
    return {
        "source_id": f"insp_top:{metric}:{level}:daterange",
        "records": rows,
        "formatted_context": ctx,
        "deterministic_answer": ctx,
        "structured_payload": ctx,
        "evidence_type": "ranking",
    }


# ── Summary query (all divisions) ────────────────────────────
def _query_insp_summary(
    db, start_date: Optional[date], end_date: Optional[date],
    question: str = "",
) -> Dict[str, Any]:
    """Query inspection_performance for overall summary.
    Columns: total_actions, challans, firs, warnings, no_offenses, sealed
    """
    rows = db.fetch_all(
        "SELECT division_name, total_actions, challans, "
        "       firs, warnings, no_offenses, sealed, "
        "       removal_order, epo, "
        "       arrest_count, pcm_count, "
        "       fine_imposed, fine_recovered, fine_outstanding, "
        "       paid_count, unpaid_count "
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

    # Compute totals — additive over all divisions
    int_keys = (
        "total_actions", "challans", "firs", "warnings", "no_offenses",
        "sealed", "removal_order", "epo", "arrest_count", "pcm_count",
        "paid_count", "unpaid_count",
    )
    money_keys = ("fine_imposed", "fine_recovered", "fine_outstanding")
    totals = {k: 0 for k in int_keys}
    money_totals = {k: 0.0 for k in money_keys}
    for r in rows:
        for k in int_keys:
            v = r.get(k)
            try:
                totals[k] += int(v or 0)
            except (TypeError, ValueError):
                # Non-numeric junk in DB → treat as 0 instead of crashing.
                pass
        for k in money_keys:
            v = r.get(k)
            if v is not None:
                try:
                    money_totals[k] += float(v)
                except (TypeError, ValueError):
                    pass

    # Helper: coerce any None/str/float to int safely so a missing
    # column never blows up the `:,` formatter.
    def _i(v):
        try:
            return int(v or 0)
        except (TypeError, ValueError):
            return 0

    context = "Inspection Performance Summary — All Divisions of Punjab\n"
    context += "=" * 50 + "\n"
    # Instruct the LLM to preserve the table verbatim — otherwise it
    # tends to paraphrase the rows into bullets and drop columns.
    context += (
        "IMPORTANT FORMATTING RULE: Your answer MUST include the "
        "Division-wise Inspection Breakdown markdown table below in "
        "full, with every column (Division, Inspections, Challans, "
        "FIRs, Warnings, No Offenses, Sealed) and every row preserved. "
        "Do not collapse it into bullets or drop columns. The province "
        "totals are already implied by the opener sentence; do not "
        "repeat them as bullets above the table.\n\n"
    )
    # Preferred opening sentence — the LLM tends to echo this verbatim.
    context += (
        f"PERA has recorded {_i(totals['total_actions']):,} regulatory "
        f"inspection actions across Punjab. The summary below includes "
        f"inspections, challans, FIRs, warnings, sealed premises, and "
        f"no-offence outcomes.\n"
    )
    # Phase-38 additions — only emit lines for non-zero columns so older
    # snapshots without these fields stay clean.
    if _i(totals.get("removal_order")):
        context += f"Removal Orders: {_i(totals['removal_order']):,}\n"
    if _i(totals.get("epo")):
        context += f"EPO: {_i(totals['epo']):,}\n"
    if _i(totals.get("arrest_count")):
        context += f"Arrest Cases: {_i(totals['arrest_count']):,}\n"
    if _i(totals.get("pcm_count")):
        context += f"PCM: {_i(totals['pcm_count']):,}\n"
    if money_totals.get("fine_imposed"):
        context += f"Fine Imposed: Rs. {_i(money_totals['fine_imposed']):,}\n"
    if money_totals.get("fine_recovered"):
        context += f"Fine Recovered: Rs. {_i(money_totals['fine_recovered']):,}\n"
    if money_totals.get("fine_outstanding"):
        context += f"Fine Outstanding: Rs. {_i(money_totals['fine_outstanding']):,}\n"
    if _i(totals.get("paid_count")):
        context += f"Paid Challans: {_i(totals['paid_count']):,}\n"
    if _i(totals.get("unpaid_count")):
        context += f"Unpaid Challans: {_i(totals['unpaid_count']):,}\n"

    # Division-wise table — emit as a proper markdown table so the LLM
    # preserves the column structure end-to-end. Numeric columns are
    # right-aligned via `---:` so the frontend renderer keeps tabular
    # numerals. Every metric uses `_i()` so missing fields render as 0.
    context += "\nDivision-wise Inspection Breakdown:\n\n"
    context += (
        "| Division | Inspections | Challans | FIRs | Warnings | "
        "No Offenses | Sealed |\n"
        "|---|---:|---:|---:|---:|---:|---:|\n"
    )
    for r in rows:
        context += (
            f"| {r.get('division_name') or 'Unknown'} "
            f"| {_i(r.get('total_actions')):,} "
            f"| {_i(r.get('challans')):,} "
            f"| {_i(r.get('firs')):,} "
            f"| {_i(r.get('warnings')):,} "
            f"| {_i(r.get('no_offenses')):,} "
            f"| {_i(r.get('sealed')):,} |\n"
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

    # ── Focused single-metric short-circuit ─────────────────
    # Mirror of the same logic in _query_insp_location. If the user
    # asked specifically for ONE metric across all of Punjab (e.g.
    # "give me the overall challan summary", "how many FIRs in
    # Punjab"), emit a 2-column table for that metric instead of the
    # full 6-column breakdown.
    _METRIC_LABELS_SUMMARY = {
        "challans":     ("Challans", "challans issued"),
        "firs":         ("FIRs",     "FIRs registered"),
        "warnings":     ("Warnings", "warnings issued"),
        "no_offenses":  ("No-Offence Outcomes", "no-offence outcomes recorded"),
        "sealed":       ("Sealed Premises",     "premises sealed"),
        "total_actions":("Inspection Actions",  "inspection actions recorded"),
    }
    _BROAD_RE_S = re.compile(
        r"\b(summary|breakdown|overview|all|full|detailed?|complete|"
        r"performance|report|dashboard|card|kpis?)\b",
        re.I,
    )

    def _detect_single_metric_summary(q: str) -> Optional[str]:
        if not q:
            return None
        focusable = set(_METRIC_LABELS_SUMMARY.keys())
        hits: List[str] = []
        for col, rx in _RANK_METRIC_MAP:
            if col not in focusable:
                continue
            if rx.search(q):
                hits.append(col)
        # "challan summary" must STILL collapse to focused mode even
        # though it contains the broad token "summary", because the
        # user already named the single metric. The veto only fires
        # when zero focusable metrics matched and the user clearly
        # wants the multi-metric overview.
        if len(hits) == 1:
            return hits[0]
        if len(hits) == 0 and _BROAD_RE_S.search(q):
            return None
        return None

    focused_metric = _detect_single_metric_summary(question or "")
    direct_answer: Optional[str] = None

    # Special case: focused-challan query gets the FULL financial
    # breakdown per division (fine imposed, paid amount, unpaid
    # amount, overdue amount) sourced from `challan_by_division`,
    # which carries per-status counts and amounts. Plain count is too
    # thin for an authority-facing challan summary.
    if focused_metric == "challans":
        # Pivot challan_by_division by (division_name, status) → one
        # row per division with count + amount per status.
        cby: Dict[str, Dict[str, Any]] = {}
        try:
            cby_rows = db.fetch_all(
                "SELECT division_name, status, total_challans, total_amount "
                "FROM challan_by_division "
                "WHERE division_name IS NOT NULL AND division_name != ''"
            )
            for r in cby_rows:
                dn = r.get("division_name") or ""
                st = (r.get("status") or "").lower()
                slot = cby.setdefault(dn, {
                    "paid_n": 0, "paid_amt": 0.0,
                    "unpaid_n": 0, "unpaid_amt": 0.0,
                    "overdue_n": 0, "overdue_amt": 0.0,
                })
                try:
                    n = int(r.get("total_challans") or 0)
                except (TypeError, ValueError):
                    n = 0
                try:
                    a = float(r.get("total_amount") or 0)
                except (TypeError, ValueError):
                    a = 0.0
                if st == "paid":
                    slot["paid_n"] += n
                    slot["paid_amt"] += a
                elif st == "unpaid":
                    slot["unpaid_n"] += n
                    slot["unpaid_amt"] += a
                elif st == "overdue":
                    slot["overdue_n"] += n
                    slot["overdue_amt"] += a
        except Exception as e:
            log.warning("challan_by_division pivot failed: %s", e)

        # Build per-division consolidated rows: total challans, fine
        # imposed, paid amount, unpaid amount, overdue amount.
        per_div: List[Dict[str, Any]] = []
        for r in rows:
            dn = r.get("division_name") or "Unknown"
            slot = cby.get(dn, {})
            paid_n     = int(slot.get("paid_n") or 0)
            unpaid_n   = int(slot.get("unpaid_n") or 0)
            overdue_n  = int(slot.get("overdue_n") or 0)
            paid_amt   = float(slot.get("paid_amt") or 0)
            unpaid_amt = float(slot.get("unpaid_amt") or 0)
            overdue_amt= float(slot.get("overdue_amt") or 0)
            # Total challans is paid + unpaid + overdue from
            # challan_by_division (which INCLUDES overdue, unlike
            # inspection_performance.challans). Fall back to the
            # inspection_performance value when challan_by_division
            # had no rows for this division.
            # Total challans = paid + unpaid only. Overdue is a subset
            # of unpaid in PERA's status model, so adding it would
            # double-count.
            total_n = paid_n + unpaid_n
            if total_n == 0:
                total_n = _i(r.get("challans"))
            # PERA dashboard treats overdue as a SUBSET of unpaid —
            # NOT a separate bucket. So total challans = paid + unpaid
            # only, and fine imposed = paid_amt + unpaid_amt only.
            # Overdue counts/amounts are shown as informational
            # ("of the unpaid, X are past due") and never added to the
            # total.
            try:
                ip_imposed = float(r.get("fine_imposed") or 0)
            except (TypeError, ValueError):
                ip_imposed = 0.0
            cb_imposed = paid_amt + unpaid_amt
            fine_imposed = cb_imposed if cb_imposed > 0 else ip_imposed
            per_div.append({
                "division": dn,
                "total_challans": total_n,
                "fine_imposed":   fine_imposed,
                "paid_n":         paid_n,
                "paid_amt":       paid_amt,
                "unpaid_n":       unpaid_n,
                "unpaid_amt":     unpaid_amt,
                "overdue_n":      overdue_n,
                "overdue_amt":    overdue_amt,
            })

        per_div.sort(key=lambda x: x["total_challans"], reverse=True)
        T = {
            "total_challans": sum(x["total_challans"] for x in per_div),
            "fine_imposed":   sum(x["fine_imposed"]   for x in per_div),
            "paid_n":         sum(x["paid_n"]         for x in per_div),
            "paid_amt":       sum(x["paid_amt"]       for x in per_div),
            "unpaid_n":       sum(x["unpaid_n"]       for x in per_div),
            "unpaid_amt":     sum(x["unpaid_amt"]     for x in per_div),
            "overdue_n":      sum(x["overdue_n"]      for x in per_div),
            "overdue_amt":    sum(x["overdue_amt"]    for x in per_div),
        }
        recovery_pct = (
            (T["paid_amt"] / T["fine_imposed"] * 100) if T["fine_imposed"] else 0
        )

        intro = (
            f"Across Punjab, PERA has issued {T['total_challans']:,} "
            f"challans with a cumulative fine of Rs. "
            f"{int(T['fine_imposed']):,}. Recovery so far is "
            f"Rs. {int(T['paid_amt']):,} ({recovery_pct:.1f}% by amount). "
            f"Outstanding (unpaid) is Rs. {int(T['unpaid_amt']):,} across "
            f"{T['unpaid_n']:,} challans, of which {T['overdue_n']:,} "
            f"({int(T['overdue_amt']):,} Rs) are past due."
        )

        body = "\n\nDivision-wise Challan & Recovery Breakdown:\n\n"
        body += (
            "| Division | Challans | Fine Imposed (Rs) "
            "| Paid (n) | Paid (Rs) "
            "| Unpaid (n) | Unpaid (Rs) "
            "| Overdue (n) | Overdue (Rs) |\n"
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|\n"
        )
        for r in per_div:
            body += (
                f"| {r['division']} "
                f"| {r['total_challans']:,} "
                f"| {int(r['fine_imposed']):,} "
                f"| {r['paid_n']:,} "
                f"| {int(r['paid_amt']):,} "
                f"| {r['unpaid_n']:,} "
                f"| {int(r['unpaid_amt']):,} "
                f"| {r['overdue_n']:,} "
                f"| {int(r['overdue_amt']):,} |\n"
            )
        body += (
            f"| **Total** "
            f"| **{T['total_challans']:,}** "
            f"| **{int(T['fine_imposed']):,}** "
            f"| **{T['paid_n']:,}** "
            f"| **{int(T['paid_amt']):,}** "
            f"| **{T['unpaid_n']:,}** "
            f"| **{int(T['unpaid_amt']):,}** "
            f"| **{T['overdue_n']:,}** "
            f"| **{int(T['overdue_amt']):,}** |\n"
        )

        direct_answer = intro + body
        try:
            if _summary_snapshot is not None:
                direct_answer += "\n" + _format_user_freshness(_summary_snapshot) + "\n"
        except Exception:
            pass

    elif focused_metric:
        label_plural, label_phrase = _METRIC_LABELS_SUMMARY[focused_metric]
        total_v = _i(totals.get(focused_metric))
        intro = (
            f"Across Punjab, PERA has recorded {total_v:,} {label_phrase} "
            f"in the latest snapshot. The division-wise breakdown is below."
        )
        body = "\n\n" + f"Division-wise {label_plural}:\n\n"
        body += f"| Division | {label_plural} |\n|---|---:|\n"
        sorted_rows = sorted(
            rows, key=lambda x: _i(x.get(focused_metric)), reverse=True,
        )
        running = 0
        for r in sorted_rows:
            v = _i(r.get(focused_metric))
            running += v
            body += f"| {r.get('division_name') or 'Unknown'} | {v:,} |\n"
        body += f"| **Total** | **{running:,}** |\n"
        direct_answer = intro + body
        try:
            if _summary_snapshot is not None:
                direct_answer += "\n" + _format_user_freshness(_summary_snapshot) + "\n"
        except Exception:
            pass

    # Deterministic direct answer — bypasses the LLM. We strip the
    # "IMPORTANT FORMATTING RULE" prelude (LLM-only) and the H1 banner,
    # leaving just the opener + breakdown table + freshness footer.
    # The frontend renders this verbatim, so the 7-column table always
    # appears intact regardless of LLM behaviour.
    if direct_answer is None:
        direct_answer = (
            f"PERA has recorded {_i(totals['total_actions']):,} regulatory "
            f"inspection actions across Punjab. The summary below includes "
            f"inspections, challans, FIRs, warnings, sealed premises, and "
            f"no-offence outcomes.\n\n"
            "Division-wise Inspection Breakdown:\n\n"
            "| Division | Inspections | Challans | FIRs | Warnings | "
            "No Offenses | Sealed |\n"
            "|---|---:|---:|---:|---:|---:|---:|\n"
        )
        for r in rows:
            direct_answer += (
                f"| {r.get('division_name') or 'Unknown'} "
                f"| {_i(r.get('total_actions')):,} "
                f"| {_i(r.get('challans')):,} "
                f"| {_i(r.get('firs')):,} "
                f"| {_i(r.get('warnings')):,} "
                f"| {_i(r.get('no_offenses')):,} "
                f"| {_i(r.get('sealed')):,} |\n"
            )
    # Freshness footer in the same human-readable format the frontend
    # sanitizer would otherwise rewrite to. Skipped for the focused-
    # metric path because that branch appended its own freshness line
    # already; appending here would duplicate it.
    if not focused_metric:
        try:
            if _summary_snapshot is not None:
                direct_answer += "\n" + _format_user_freshness(_summary_snapshot) + "\n"
        except Exception:
            pass

    return {
        "source_id": "insp_summary",
        "records": rows,
        "formatted_context": context,
        "direct_answer": direct_answer,
    }


# ── Location query (division / district / tehsil) ───────────
def _query_arrest_location(
    db, level: str, name: str,
) -> Dict[str, Any]:
    """Focused arrest count for a division/district/tehsil.

    Calls PCM /Pcm/dashboard-counts (all-time totalArrest) for every
    tehsil under the location and sums. Arrest is its own metric in
    the PERA dashboard — NOT the same as FIR. FIRs are tracked under
    inspection-summary.fiRs and represent a different enforcement
    action; the dashboard tile labelled "Arrest" maps to PCM
    totalArrest, the tile labelled "FIR" maps to inspection-summary
    fiRs. This handler is the arrest tile only.

    Returns a direct_answer so the LLM is bypassed entirely — the
    number is deterministic and the wording is fixed.
    """
    source_id = f"insp_arrest_{level}:{name}"

    # Resolve the set of tehsil_ids under this location. The
    # dim_tehsil table carries (tehsil_id, tehsil_name, district_name,
    # division_name) so a single filter is enough for any level.
    # dim_tehsil only stores tehsil_id + district_id, so district / division
    # filters must join through dim_district (and dim_division) by id.
    tehsils: List[Dict[str, Any]] = []
    try:
        if level == "tehsil":
            tehsils = db.fetch_all(
                "SELECT tehsil_id, tehsil_name FROM dim_tehsil "
                "WHERE tehsil_name = %s",
                (name,),
            )
        elif level == "district":
            tehsils = db.fetch_all(
                "SELECT t.tehsil_id, t.tehsil_name "
                "FROM dim_tehsil t "
                "JOIN dim_district d ON d.district_id = t.district_id "
                "WHERE d.district_name = %s "
                "ORDER BY t.tehsil_name",
                (name,),
            )
        elif level == "division":
            tehsils = db.fetch_all(
                "SELECT t.tehsil_id, t.tehsil_name "
                "FROM dim_tehsil t "
                "JOIN dim_district d ON d.district_id = t.district_id "
                "JOIN dim_division v ON v.division_id = d.division_id "
                "WHERE v.division_name = %s "
                "ORDER BY t.tehsil_name",
                (name,),
            )
    except Exception as e:
        log.warning("dim_tehsil lookup failed for %s '%s': %s", level, name, e)

    if not tehsils:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": (
                f"No tehsils found under {level} '{name}'. Cannot compute "
                f"arrest totals.\n"
            ),
        }

    # Fan-out PCM dashboard-counts per tehsil in parallel — total wait
    # capped at one _API_TIMEOUT window, not len(tehsils) * timeout.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    per_tehsil: List[Dict[str, Any]] = []

    def _fetch(tid: int, tname: str) -> Dict[str, Any]:
        try:
            r = requests.get(
                PCM_DASHBOARD_COUNTS,
                params={"tehsilId": tid},
                headers=_HEADERS,
                timeout=_API_TIMEOUT,
            )
            r.raise_for_status()
            j = r.json() if isinstance(r.json(), dict) else {}
            return {
                "tehsil_id": tid,
                "tehsil_name": tname,
                "arrest": int(j.get("totalArrest") or 0),
                "pcm": int(j.get("totalPCM") or 0),
                "error": None,
            }
        except Exception as e:
            return {
                "tehsil_id": tid, "tehsil_name": tname,
                "arrest": 0, "pcm": 0, "error": str(e),
            }

    workers = min(8, max(2, len(tehsils)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_fetch, t["tehsil_id"], t["tehsil_name"]) for t in tehsils]
        for f in as_completed(futs):
            per_tehsil.append(f.result())

    per_tehsil.sort(key=lambda r: r["tehsil_name"])
    total_arrest = sum(r["arrest"] for r in per_tehsil)
    total_pcm = sum(r["pcm"] for r in per_tehsil)
    err_count = sum(1 for r in per_tehsil if r["error"])

    # Direct answer — no LLM. PCM dashboard-counts is all-time, so the
    # opener must not claim a date window.
    intro = (
        f"In the {name} {level.title()}, PERA has registered a cumulative "
        f"total of {total_arrest:,} arrests across "
        f"{len(per_tehsil)} tehsil(s). (Arrest is reported separately "
        f"from FIRs in the PERA dashboard — this answer covers arrests "
        f"only.)"
    )
    body = "\n\nTehsil-wise Arrest Breakdown:\n\n"
    body += "| Tehsil | Arrests | PCM Cases |\n|---|---:|---:|\n"
    for r in per_tehsil:
        body += f"| {r['tehsil_name']} | {r['arrest']:,} | {r['pcm']:,} |\n"
    body += f"| **Total** | **{total_arrest:,}** | **{total_pcm:,}** |\n"

    if err_count:
        body += (
            f"\n_Note: {err_count} tehsil(s) had no data for this "
            f"period; shown as 0._\n"
        )

    # Freshness footer — PCM dashboard-counts is live, so the snapshot
    # is "right now".
    from datetime import datetime as _dt
    now = _dt.now()
    h = now.hour
    ampm = "PM" if h >= 12 else "AM"
    h12 = ((h + 11) % 12) + 1
    body += (
        f"\nData last updated: {now.day} {_MONTH_NAMES[now.month - 1]} "
        f"{now.year}, {h12}:{now.minute:02d} {ampm}.\n"
    )

    formatted_context = (
        f"Arrest summary — {level.title()}: {name}\n"
        + "=" * 50 + "\n"
        + f"Total arrests: {total_arrest:,}\n"
        + f"Total PCM cases: {total_pcm:,}\n"
        + f"Tehsils queried: {len(per_tehsil)} (errors: {err_count})\n"
        + "Note: Arrest is a distinct metric in the PERA dashboard, "
          "tracked separately from FIRs. Do not conflate the two.\n"
    )

    return {
        "source_id": source_id,
        "records": per_tehsil,
        "formatted_context": formatted_context,
        "direct_answer": intro + body,
    }


def _query_compare_locations(
    db,
    locations: List[Dict[str, str]],
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    question: str = "",
) -> Dict[str, Any]:
    """Side-by-side comparison of two or more locations.

    Each location may be at any level (tehsil / district / division).
    For each one we fan out the same SDEO endpoint trio used by the
    focused-challan handler — inspections-summary, top-kpis,
    challan-status-breakdown — plus PCM dashboard-counts for arrests.
    Output is a single markdown table with one column per location
    and one row per KPI so authorities can scan differences quickly.

    Default date window: 2024-01-01 → today when the user did not
    supply explicit dates. SDEO endpoints reject open-ended ranges.
    """
    from datetime import date as _date, datetime as _dt
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not end_date:
        end_date = _date.today()
    if not start_date:
        start_date = _date(2024, 1, 1)
    sdate = start_date.isoformat()
    edate = end_date.isoformat()
    SDEO_BASE = "https://pera360.punjab.gov.pk/backend/api/sdeo-dashboard"

    # Resolve the tehsil_id set for each location so we can fan out
    # the SDEO endpoints. Tehsil-level locations carry their own id;
    # district/division locations expand to all child tehsils.
    def _resolve_tehsils(loc: Dict[str, str]) -> List[Dict[str, Any]]:
        try:
            if loc["level"] == "tehsil":
                return db.fetch_all(
                    "SELECT tehsil_id, tehsil_name FROM dim_tehsil "
                    "WHERE tehsil_name = %s",
                    (loc["name"],),
                ) or []
            if loc["level"] == "district":
                return db.fetch_all(
                    "SELECT t.tehsil_id, t.tehsil_name "
                    "FROM dim_tehsil t "
                    "JOIN dim_district d ON d.district_id = t.district_id "
                    "WHERE d.district_name = %s",
                    (loc["name"],),
                ) or []
            if loc["level"] == "division":
                return db.fetch_all(
                    "SELECT t.tehsil_id, t.tehsil_name "
                    "FROM dim_tehsil t "
                    "JOIN dim_district d ON d.district_id = t.district_id "
                    "JOIN dim_division v ON v.division_id = d.division_id "
                    "WHERE v.division_name = %s",
                    (loc["name"],),
                ) or []
        except Exception as e:
            log.warning("compare: tehsil resolve failed for %s '%s': %s",
                        loc["level"], loc["name"], e)
        return []

    def _fetch_tehsil(tid: int) -> Dict[str, Any]:
        # Single authoritative call — matches dashboard tiles exactly.
        c = _fetch_pcm_dashboard_counts(tid, start_date, end_date)
        return {
            "inspections": c["inspections"],
            "challans":    c["challans"],
            "fine":        c["fine"],
            "paid_n":      c["paid_n"],
            "unpaid_n":    c["unpaid_n"],
            "paid_amt":    c["paid_amt"],
            "unpaid_amt":  c["unpaid_amt"],
            "sealed":      c["sealed"],
            "warnings":    c["warnings"],
            "firs":        0,   # PCM dashboard-counts has no FIR field
            "no_offenses": 0,   # nor no-offenses; left at 0 in compare
            "arrests":     c["arrests"],
        }

    # Build aggregated rollup per location.
    per_loc: List[Dict[str, Any]] = []
    for loc in locations:
        tehsils = _resolve_tehsils(loc)
        if not tehsils:
            per_loc.append({
                "label": f"{loc['name']} ({loc['level']})",
                "missing": True,
                "inspections": 0, "challans": 0, "fine": 0.0,
                "paid_n": 0, "unpaid_n": 0, "paid_amt": 0.0,
                "unpaid_amt": 0.0, "sealed": 0, "warnings": 0,
                "firs": 0, "no_offenses": 0, "arrests": 0,
                "tehsil_count": 0,
            })
            continue
        results: List[Dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=min(8, max(2, len(tehsils)))) as ex:
            futs = [ex.submit(_fetch_tehsil, t["tehsil_id"]) for t in tehsils]
            for f in as_completed(futs):
                results.append(f.result())
        rolled = {
            "inspections": sum(r["inspections"] for r in results),
            "challans":    sum(r["challans"]    for r in results),
            "fine":        sum(r["fine"]        for r in results),
            "paid_n":      sum(r["paid_n"]      for r in results),
            "unpaid_n":    sum(r["unpaid_n"]    for r in results),
            "paid_amt":    sum(r["paid_amt"]    for r in results),
            "unpaid_amt":  sum(r["unpaid_amt"]  for r in results),
            "sealed":      sum(r["sealed"]      for r in results),
            "warnings":    sum(r["warnings"]    for r in results),
            "firs":        sum(r["firs"]        for r in results),
            "no_offenses": sum(r["no_offenses"] for r in results),
            "arrests":     sum(r["arrests"]     for r in results),
        }
        per_loc.append({
            "label": f"{loc['name']} ({loc['level']})",
            "missing": False,
            "tehsil_count": len(tehsils),
            **rolled,
        })

    labels = [r["label"] for r in per_loc]
    source_id = "insp_compare:" + "::".join(labels)
    date_label = f"{sdate} – {edate}"

    intro = (
        f"Side-by-side comparison ({date_label}) of "
        f"{', '.join(labels)}."
    )

    # Build comparison table: one column per location, one row per KPI.
    header = "| KPI | " + " | ".join(labels) + " |\n"
    sep = "|---" + (" | ---:" * len(labels)) + " |\n"

    def _row(label: str, key: str, fmt: str = "{:,}") -> str:
        vals = []
        for r in per_loc:
            v = r.get(key)
            if isinstance(v, float):
                vals.append(fmt.format(int(v)))
            else:
                vals.append(fmt.format(v or 0))
        return f"| {label} | " + " | ".join(vals) + " |\n"

    body = "\n\n" + header + sep
    body += _row("Total Inspections", "inspections")
    body += _row("Total Challans",    "challans")
    body += _row("Fine Imposed (Rs)", "fine")
    body += _row("Paid Challans",     "paid_n")
    body += _row("Paid Amount (Rs)",  "paid_amt")
    body += _row("Unpaid Challans",   "unpaid_n")
    body += _row("Unpaid Amount (Rs)","unpaid_amt")
    body += _row("Sealed",            "sealed")
    body += _row("Warnings",          "warnings")
    body += _row("Arrests",           "arrests")

    # Leader call-outs: which location wins on inspections / fine /
    # recovery rate. Skips when only one location has data.
    if len(per_loc) >= 2 and any(not r["missing"] for r in per_loc):
        top_insp = max(per_loc, key=lambda r: r["inspections"])
        top_fine = max(per_loc, key=lambda r: r["fine"])
        recoveries = [
            (r["label"], (r["paid_amt"] / r["fine"] * 100) if r["fine"] else 0)
            for r in per_loc
        ]
        top_rec = max(recoveries, key=lambda x: x[1])
        body += (
            f"\n_Leaders — Inspections: **{top_insp['label']}** "
            f"({top_insp['inspections']:,}) · "
            f"Fine: **{top_fine['label']}** "
            f"(Rs {int(top_fine['fine']):,}) · "
            f"Recovery: **{top_rec[0]}** ({top_rec[1]:.1f}%)_\n"
        )

    now = _dt.now()
    h = now.hour
    ampm = "PM" if h >= 12 else "AM"
    h12 = ((h + 11) % 12) + 1
    body += (
        f"\nData last updated: {now.day} {_MONTH_NAMES[now.month - 1]} "
        f"{now.year}, {h12}:{now.minute:02d} {ampm}.\n"
    )

    formatted_context = (
        f"Comparison — {', '.join(labels)} ({date_label})\n"
        + "=" * 50 + "\n"
        + "\n".join(
            f"{r['label']}: inspections={r['inspections']:,} "
            f"challans={r['challans']:,} "
            f"fine=Rs.{int(r['fine']):,} "
            f"paid={r['paid_n']:,} unpaid={r['unpaid_n']:,}"
            for r in per_loc
        ) + "\n"
    )

    return {
        "source_id": source_id,
        "records": per_loc,
        "formatted_context": formatted_context,
        "direct_answer": intro + body,
    }


def _query_enforcer_location(
    db, level: str, name: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    question: str = "",
) -> Dict[str, Any]:
    """Focused enforcer (force-deployed) count for a location.

    Calls SDEO /sdeo-dashboard/top-kpis per tehsil under the location
    and sums totalForceDeployed. The endpoint is date-ranged; when the
    user gives no explicit dates ("ab tk" / cumulative), default to
    2024-01-01 → today which spans the entire PERA enforcement
    history.

    Enforcer is its own dashboard tile, distinct from arrest and
    distinct from inspection-officer count.
    """
    from datetime import datetime as _dt, date as _date, timedelta as _td
    source_id = f"insp_enforcer_{level}:{name}"

    if not end_date:
        end_date = _date.today()
    if not start_date:
        # Default = full PERA history (2024-01-01 → today) to match
        # the dashboard's "all-time" view when the user gives no
        # explicit range. The dashboard's date picker defaults vary
        # by page (today on DC dashboard, broader elsewhere); we use
        # the widest window so a user asking "kitne enforcer hain
        # total" sees the cumulative roster.
        start_date = _date(2024, 1, 1)

    # Resolve child tehsils via the same dim_district / dim_division
    # joins used by _query_arrest_location.
    tehsils: List[Dict[str, Any]] = []
    try:
        if level == "tehsil":
            tehsils = db.fetch_all(
                "SELECT tehsil_id, tehsil_name FROM dim_tehsil "
                "WHERE tehsil_name = %s",
                (name,),
            )
        elif level == "district":
            tehsils = db.fetch_all(
                "SELECT t.tehsil_id, t.tehsil_name "
                "FROM dim_tehsil t "
                "JOIN dim_district d ON d.district_id = t.district_id "
                "WHERE d.district_name = %s "
                "ORDER BY t.tehsil_name",
                (name,),
            )
        elif level == "division":
            tehsils = db.fetch_all(
                "SELECT t.tehsil_id, t.tehsil_name "
                "FROM dim_tehsil t "
                "JOIN dim_district d ON d.district_id = t.district_id "
                "JOIN dim_division v ON v.division_id = d.division_id "
                "WHERE v.division_name = %s "
                "ORDER BY t.tehsil_name",
                (name,),
            )
    except Exception as e:
        log.warning("dim_tehsil lookup failed for %s '%s': %s", level, name, e)

    if not tehsils:
        return {
            "source_id": source_id,
            "records": [],
            "formatted_context": (
                f"No tehsils found under {level} '{name}'. Cannot compute "
                f"enforcer totals.\n"
            ),
        }

    from concurrent.futures import ThreadPoolExecutor, as_completed
    per_tehsil: List[Dict[str, Any]] = []
    sdate = start_date.isoformat()
    edate = end_date.isoformat()

    def _fetch(tid: int, tname: str) -> Dict[str, Any]:
        # Two calls in sequence per tehsil:
        #   1. PCM /officer-inspection-details → ENFORCER count
        #      (distinct user rows = enforcers, matches dashboard tile)
        #   2. SDEO /top-kpis → vehicles + operations (top-kpis is the
        #      only source for these two fields)
        enforcers = 0
        vehicles = 0
        operations = 0
        err = None
        try:
            r = requests.get(
                PCM_OFFICER_INSPECTION_DETAILS,
                params={"tehsilId": tid,
                        "startDate": sdate, "endDate": edate},
                headers=_HEADERS, timeout=_API_TIMEOUT,
            )
            r.raise_for_status()
            rows = r.json() if isinstance(r.json(), list) else []
            # Distinct userId — same userId appearing twice (e.g. role
            # rename) collapses to one enforcer.
            enforcers = len({row.get("userId") for row in rows
                             if row.get("userId")})
            if not enforcers and rows:
                enforcers = len(rows)
        except Exception as e:
            err = f"officer-details:{e}"
        try:
            k = requests.get(
                "https://pera360.punjab.gov.pk/backend/api/sdeo-dashboard/top-kpis",
                params={"tehsilId": tid,
                        "startDate": sdate, "endDate": edate},
                headers=_HEADERS, timeout=_API_TIMEOUT,
            )
            k.raise_for_status()
            j = k.json() if isinstance(k.json(), dict) else {}
            vehicles   = int(j.get("vehiclesUsed") or 0)
            operations = int(j.get("operationsExecuted") or 0)
        except Exception as e:
            err = (err or "") + f" top-kpis:{e}"
        return {
            "tehsil_id": tid, "tehsil_name": tname,
            "enforcers": enforcers, "vehicles": vehicles,
            "operations": operations, "error": err,
        }

    workers = min(8, max(2, len(tehsils)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_fetch, t["tehsil_id"], t["tehsil_name"]) for t in tehsils]
        for f in as_completed(futs):
            per_tehsil.append(f.result())

    per_tehsil.sort(key=lambda r: r["enforcers"], reverse=True)
    total_enforcers = sum(r["enforcers"] for r in per_tehsil)
    total_vehicles = sum(r["vehicles"] for r in per_tehsil)
    total_ops = sum(r["operations"] for r in per_tehsil)
    err_count = sum(1 for r in per_tehsil if r["error"])

    intro = (
        f"Across the {name} {level.title()} (period {sdate} to {edate}), "
        f"PERA has {total_enforcers:,} enforcers on roster across "
        f"{len(per_tehsil)} tehsil(s)."
    )
    body = "\n\nTehsil-wise Enforcer Breakdown:\n\n"
    body += "| Tehsil | Enforcers | Vehicles | Operations |\n|---|---:|---:|---:|\n"
    for r in per_tehsil:
        body += (
            f"| {r['tehsil_name']} | {r['enforcers']:,} "
            f"| {r['vehicles']:,} | {r['operations']:,} |\n"
        )
    body += (
        f"| **Total** | **{total_enforcers:,}** | **{total_vehicles:,}** "
        f"| **{total_ops:,}** |\n"
    )
    if err_count:
        body += (
            f"\n_Note: {err_count} tehsil(s) had no data for this "
            f"period; shown as 0._\n"
        )

    now = _dt.now()
    h = now.hour
    ampm = "PM" if h >= 12 else "AM"
    h12 = ((h + 11) % 12) + 1
    body += (
        f"\nData last updated: {now.day} {_MONTH_NAMES[now.month - 1]} "
        f"{now.year}, {h12}:{now.minute:02d} {ampm}.\n"
    )

    formatted_context = (
        f"Enforcer summary — {level.title()}: {name} "
        f"({sdate} → {edate})\n"
        + "=" * 50 + "\n"
        + f"Total enforcers (force-deployed): {total_enforcers:,}\n"
        + f"Total vehicles used: {total_vehicles:,}\n"
        + f"Total operations executed: {total_ops:,}\n"
        + f"Tehsils queried: {len(per_tehsil)} (errors: {err_count})\n"
    )

    return {
        "source_id": source_id,
        "records": per_tehsil,
        "formatted_context": formatted_context,
        "direct_answer": intro + body,
    }


def _query_insp_location(
    db, level: str, name: str,
    start_date: Optional[date], end_date: Optional[date],
    question: str = "",
) -> Dict[str, Any]:
    """Query inspection_performance for a specific location + its children.
    For tehsil-level queries with date range, calls SDEO API live.

    `question` is forwarded to the live tehsil handler so it can pick
    between the full dashboard and a focused-metric report.
    """
    col = f"{level}_name"
    source_id = f"insp_{level}:{name}"

    # ── Focused single-metric short-circuit (PCM dashboard-counts) ──
    # When the user asks about ONE specific metric for a
    # district/division (sealed / warnings / challans / arrests /
    # inspections), build a focused per-tehsil table from the PCM
    # dashboard-counts endpoint — which matches the dashboard tiles
    # and carries every per-tehsil value — instead of the generic
    # _query_district_live performance dump (which returns the full
    # KPI card set and, on short windows, mostly zeros for tehsils
    # whose SDEO rows haven't propagated). Works with OR without a
    # date range.
    _pcm_focus_metric = _detect_pcm_focus_metric(question)
    if (_pcm_focus_metric
            and level in ("district", "division", "tehsil")):
        return _query_focused_metric_location(
            db, level, name, _pcm_focus_metric,
            start_date, end_date, question=question,
        )

    _try_focused_challan = bool(
        start_date and end_date
        and level in ("district", "division")
        and _detect_focused_challan_keyword(question)
    )

    # ── Date-ranged → LIVE SDEO API CALL ──
    if start_date and end_date and not _try_focused_challan:
        if level == "tehsil":
            return _query_tehsil_live(db, name, start_date, end_date,
                                      source_id, question=question)
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

    # Safe int coercion for this block too.
    def _i2(v):
        try:
            return int(v or 0)
        except (TypeError, ValueError):
            return 0

    if parent_rows:
        r = parent_rows[0]
        context += f"Total Regulatory Inspection Actions: {_i2(r.get('total_actions')):,}\n"
        context += f"Challans: {_i2(r.get('challans')):,}\n"
        context += f"FIRs: {_i2(r.get('firs')):,}\n"
        context += f"Warnings: {_i2(r.get('warnings')):,}\n"
        context += f"No Offenses: {_i2(r.get('no_offenses')):,}\n"
        context += f"Sealed: {_i2(r.get('sealed')):,}\n"

    if child_rows:
        child_label = child_level.title() if child_level else "Sub-location"
        # District-wise (or sub-location) table — full 7-column breakdown
        # emitted as a proper markdown table for end-to-end column fidelity.
        context += f"\n{child_label}-wise Inspection Breakdown:\n\n"
        context += (
            f"| {child_label} | Inspections | Challans | FIRs | Warnings | "
            "No Offenses | Sealed |\n"
            "|---|---:|---:|---:|---:|---:|---:|\n"
        )
        for r in child_rows:
            child_name = r.get(f"{child_level}_name") or "Unknown"
            context += (
                f"| {child_name} "
                f"| {_i2(r.get('total_actions')):,} "
                f"| {_i2(r.get('challans')):,} "
                f"| {_i2(r.get('firs')):,} "
                f"| {_i2(r.get('warnings')):,} "
                f"| {_i2(r.get('no_offenses')):,} "
                f"| {_i2(r.get('sealed')):,} |\n"
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

    # Deterministic direct answer for location queries — bypasses the
    # LLM. Only emitted when we have parent_rows (so we can introduce
    # the location confidently) AND there is a children breakdown.
    # Officer breakdown stays LLM-only because it can include
    # snapshot-mismatch caveats that need natural-language phrasing.
    direct_answer: Optional[str] = None

    # ── Focused single-metric short-circuit ────────────────────
    # "lahore mein kitne sealed" / "how many FIRs in Faisalabad" —
    # user asked about ONE specific metric. Return only that column
    # (district headline + child breakdown) so the answer matches the
    # question instead of dumping the full 6-column summary table.
    _METRIC_LABELS = {
        "sealed":       ("Sealed Premises", "premises sealed"),
        "firs":         ("FIRs", "FIRs registered"),
        "warnings":     ("Warnings", "warnings issued"),
        "no_offenses":  ("No-Offence Outcomes", "no-offence outcomes recorded"),
        "challans":     ("Challans", "challans issued"),
        "total_actions":("Inspection Actions", "inspection actions recorded"),
    }
    _BROAD_RE = re.compile(
        r"\b(summary|breakdown|overview|all|full|detailed?|complete|"
        r"performance|report|dashboard|card|kpis?)\b",
        re.I,
    )

    def _detect_single_metric(q: str) -> Optional[str]:
        if not q:
            return None
        hits: List[str] = []
        # Skip metrics not surfaced in inspection_performance children
        # to avoid claiming data we don't have at the row level.
        focusable = {"sealed", "firs", "warnings", "no_offenses",
                     "challans", "total_actions"}
        for col, rx in _RANK_METRIC_MAP:
            if col not in focusable:
                continue
            if rx.search(q):
                hits.append(col)
        # Exactly one focused metric mentioned → focused mode wins,
        # even when the user also says "summary" / "breakdown" / etc.
        # "challan summary of Bahawalpur" must collapse to the
        # focused-challan path, not the full 6-column dump.
        if len(hits) == 1:
            return hits[0]
        # Zero metric hits + broad token = explicit overview request.
        # Multiple hits = full table (user named several metrics).
        return None

    focused_metric = _detect_single_metric(question or "")

    # Focused-challan at division/district level → fan out the LIVE
    # SDEO dashboard endpoints per child tehsil so the chatbot table
    # matches the PERA360 dashboard counts exactly. We call THREE
    # SDEO endpoints per tehsil in parallel:
    #   inspections-summary    → challan count (matches dashboard tile)
    #   top-kpis               → totalFineImposed (matches Fine Amount)
    #   challan-status-breakdown → paidCount, unpaidCount, paidAmount,
    #                              unpaidAmount (matches Paid/Unpaid tiles)
    # All three endpoints REQUIRE a (tehsilId, startDate, endDate)
    # tuple. The dashboard defaults to a rolling 12-month window, so
    # we mirror that when the user gives no explicit date range.
    # SDEO has NO 'overdue' concept; we therefore drop that column
    # entirely instead of estimating it.
    if (focused_metric == "challans"
            and parent_rows
            and child_rows
            and not officer_rows
            and level in ("district", "division")):
        # Resolve child tehsils with their tehsil_ids so we can call
        # PCM dashboard-counts. The cached child_rows from
        # inspection_performance carry tehsil_name but not tehsil_id
        # at division-level (which would be districts, not tehsils).
        # So we always resolve tehsils via dim_tehsil JOIN.
        tehsil_rows: List[Dict[str, Any]] = []
        try:
            if level == "district":
                tehsil_rows = db.fetch_all(
                    "SELECT t.tehsil_id, t.tehsil_name "
                    "FROM dim_tehsil t "
                    "JOIN dim_district d ON d.district_id = t.district_id "
                    "WHERE d.district_name = %s "
                    "ORDER BY t.tehsil_name",
                    (name,),
                )
            elif level == "division":
                tehsil_rows = db.fetch_all(
                    "SELECT t.tehsil_id, t.tehsil_name "
                    "FROM dim_tehsil t "
                    "JOIN dim_district d ON d.district_id = t.district_id "
                    "JOIN dim_division v ON v.division_id = d.division_id "
                    "WHERE v.division_name = %s "
                    "ORDER BY t.tehsil_name",
                    (name,),
                )
        except Exception as e:
            log.warning("focused-challan tehsil resolve failed: %s", e)

        # Decide source by date-range presence.
        #
        # No explicit date in the question → mirror the PERA360
        # dashboard tile which uses PCM /Pcm/dashboard-counts. This
        # endpoint is all-time and returns totalChallans /
        # totalFineAmount / paidChallans / unPaidChallans /
        # paidChallanAmount / unPaidChallanAmount per tehsil. Probing
        # shows it agrees with the dashboard tile to the last rupee.
        #
        # Explicit date range supplied → fan out the SDEO date-ranged
        # endpoints (inspections-summary + top-kpis +
        # challan-status-breakdown) so the answer respects the user's
        # window.
        from datetime import datetime as _dt, date as _date, timedelta as _td
        has_explicit_date = bool(start_date and end_date)

        if has_explicit_date:
            date_label = (f"{start_date.strftime('%d %b %Y')} – "
                          f"{end_date.strftime('%d %b %Y')}")
        else:
            date_label = "all-time"
        source_note = (
            f"live PCM /dashboard-counts (FromDate/ToDate) per tehsil — "
            f"matches the PERA360 dashboard tiles exactly"
        )

        if tehsil_rows:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def _fetch(tid: int, tname: str) -> Dict[str, Any]:
                c = _fetch_pcm_dashboard_counts(tid, start_date, end_date)
                return {
                    "tehsil_name": tname,
                    "challans":     c["challans"],
                    "fine_imposed": c["fine"],
                    "paid_n":       c["paid_n"],
                    "paid_amt":     c["paid_amt"],
                    "unpaid_n":     c["unpaid_n"],
                    "unpaid_amt":   c["unpaid_amt"],
                }

            workers = min(8, max(2, len(tehsil_rows)))
            by_name: Dict[str, Dict[str, Any]] = {}
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = [
                    ex.submit(_fetch, t["tehsil_id"], t["tehsil_name"])
                    for t in tehsil_rows
                ]
                for f in as_completed(futs):
                    r = f.result()
                    by_name[r["tehsil_name"]] = r
        else:
            by_name = {}

        per_t: List[Dict[str, Any]] = []
        for t in tehsil_rows:
            tn = t["tehsil_name"]
            r = by_name.get(tn, {})
            per_t.append({
                "tehsil":       tn,
                "challans":     int(r.get("challans") or 0),
                "fine_imposed": float(r.get("fine_imposed") or 0),
                "paid_n":       int(r.get("paid_n") or 0),
                "paid_amt":     float(r.get("paid_amt") or 0),
                "unpaid_n":     int(r.get("unpaid_n") or 0),
                "unpaid_amt":   float(r.get("unpaid_amt") or 0),
            })

        per_t.sort(key=lambda x: x["challans"], reverse=True)
        T = {
            "challans":     sum(x["challans"]     for x in per_t),
            "fine_imposed": sum(x["fine_imposed"] for x in per_t),
            "paid_n":       sum(x["paid_n"]       for x in per_t),
            "paid_amt":     sum(x["paid_amt"]     for x in per_t),
            "unpaid_n":     sum(x["unpaid_n"]     for x in per_t),
            "unpaid_amt":   sum(x["unpaid_amt"]   for x in per_t),
        }
        recovery_pct = (
            (T["paid_amt"] / T["fine_imposed"] * 100) if T["fine_imposed"] else 0
        )
        scope_label = (
            f" ({date_label})" if has_explicit_date else " (all-time)"
        )
        intro = (
            f"In the {name} {level.title()}{scope_label}, PERA has "
            f"issued {T['challans']:,} challans with a cumulative fine "
            f"of Rs. {int(T['fine_imposed']):,}. Recovery so far is "
            f"Rs. {int(T['paid_amt']):,} ({recovery_pct:.1f}% by amount). "
            f"Outstanding (unpaid) is Rs. {int(T['unpaid_amt']):,} "
            f"across {T['unpaid_n']:,} challans."
        )
        body = "\n\nTehsil-wise Challan & Recovery Breakdown:\n\n"
        body += (
            "| Tehsil | Challans | Fine Imposed (Rs) "
            "| Paid (n) | Paid (Rs) | Unpaid (n) | Unpaid (Rs) |\n"
            "|---|---:|---:|---:|---:|---:|---:|\n"
        )
        for r in per_t:
            body += (
                f"| {r['tehsil']} "
                f"| {r['challans']:,} "
                f"| {int(r['fine_imposed']):,} "
                f"| {r['paid_n']:,} "
                f"| {int(r['paid_amt']):,} "
                f"| {r['unpaid_n']:,} "
                f"| {int(r['unpaid_amt']):,} |\n"
            )
        body += (
            f"| **Total** "
            f"| **{T['challans']:,}** "
            f"| **{int(T['fine_imposed']):,}** "
            f"| **{T['paid_n']:,}** "
            f"| **{int(T['paid_amt']):,}** "
            f"| **{T['unpaid_n']:,}** "
            f"| **{int(T['unpaid_amt']):,}** |\n"
        )
        direct_answer = intro + body
        from datetime import datetime as _dt2
        now = _dt2.now()
        h = now.hour
        ampm = "PM" if h >= 12 else "AM"
        h12 = ((h + 11) % 12) + 1
        direct_answer += (
            f"\nData last updated: {now.day} {_MONTH_NAMES[now.month - 1]} "
            f"{now.year}, {h12}:{now.minute:02d} {ampm}.\n"
        )

    elif (focused_metric
            and parent_rows
            and child_rows
            and not officer_rows):
        pr = parent_rows[0]
        cl_label = (child_level or "sub-location").title()
        label_plural, label_phrase = _METRIC_LABELS[focused_metric]
        parent_value = _i2(pr.get(focused_metric))
        intro = (
            f"As of the latest snapshot, the {name} {level.title()} has "
            f"{parent_value:,} {label_phrase} across "
            f"{len(child_rows)} {cl_label.lower()}(s)."
        )
        body = (
            "\n\n"
            f"{cl_label}-wise {label_plural}:\n\n"
            f"| {cl_label} | {label_plural} |\n"
            "|---|---:|\n"
        )
        running = 0
        for r in sorted(child_rows,
                        key=lambda x: _i2(x.get(focused_metric)),
                        reverse=True):
            cname = r.get(f"{child_level}_name") or "Unknown"
            v = _i2(r.get(focused_metric))
            running += v
            body += f"| {cname} | {v:,} |\n"
        body += f"| **Total** | **{running:,}** |\n"
        direct_answer = intro + body
        try:
            if parent_snapshot is not None:
                direct_answer += "\n" + _format_user_freshness(parent_snapshot) + "\n"
        except Exception:
            pass

    if direct_answer is None and parent_rows and child_rows and not officer_rows:
        pr = parent_rows[0]
        cl_label = (child_level or "sub-location").title()
        # The numbers below are sourced from the DG-Dashboard
        # inspection-performance API, which EXCLUDES overdue challans
        # and refreshes on a ~2-hour cadence. The live PERA360
        # dashboard tooltip pulls from a different upstream and can
        # therefore show a slightly higher action total. We surface
        # this in the opener so the user does not read a 5-10% gap as
        # a math error.
        intro = (
            f"As of the latest snapshot, the {name} {level.title()} has "
            f"{_i2(pr.get('total_actions')):,} recorded regulatory "
            f"inspection actions. The breakdown below covers inspections, "
            f"challans, FIRs, warnings, sealed premises, and no-offence "
            f"outcomes (overdue challans are tracked separately and may "
            f"not be included)."
        )
        body = (
            "\n\n"
            f"{cl_label}-wise Inspection Breakdown:\n\n"
            f"| {cl_label} | Inspections | Challans | FIRs | Warnings | "
            "No Offenses | Sealed |\n"
            "|---|---:|---:|---:|---:|---:|---:|\n"
        )
        for r in child_rows:
            child_name = r.get(f"{child_level}_name") or "Unknown"
            body += (
                f"| {child_name} "
                f"| {_i2(r.get('total_actions')):,} "
                f"| {_i2(r.get('challans')):,} "
                f"| {_i2(r.get('firs')):,} "
                f"| {_i2(r.get('warnings')):,} "
                f"| {_i2(r.get('no_offenses')):,} "
                f"| {_i2(r.get('sealed')):,} |\n"
            )
        direct_answer = intro + body
        try:
            if parent_snapshot is not None:
                direct_answer += "\n" + _format_user_freshness(parent_snapshot) + "\n"
        except Exception:
            pass

    out: Dict[str, Any] = {
        "source_id": source_id,
        "records": parent_rows + child_rows,
        "formatted_context": context,
    }
    if direct_answer is not None:
        out["direct_answer"] = direct_answer
    return out


# ── Live SDEO API call for tehsil + date range ──────────────
def _query_tehsil_live(
    db, tehsil_name: str, start_date: date, end_date: date, source_id: str,
    question: str = "",
) -> Dict[str, Any]:
    """Call SDEO inspections-summary API live for a tehsil with date filter.

    `question` is forwarded so the focused-metric extractor can decide
    whether to render the full dashboard or a single-metric report.
    """
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

    # SDEO dashboard date-picker default = 12:00 AM start, 11:59 PM end.
    # Sending date-only would be midnight (= exclusive of the final
    # day), so we use the canonical formatter to get inclusive
    # T00:00:00 / T23:59:59 boundaries that match dashboard tiles.
    from pera_dates import to_sdeo_start_end
    api_start_str, api_end_str = to_sdeo_start_end(start_date, end_date)

    try:
        resp = requests.get(
            SDEO_INSPECTIONS_SUMMARY,
            params={
                "tehsilId": tehsil_id,
                "startDate": api_start_str,
                "endDate": api_end_str,
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

    # ── Auxiliary endpoint merges — fired in parallel so the worst
    #    case is one timeout window, not the sum of three. Without
    #    this the tehsil-live path could stack 3× _API_TIMEOUT and
    #    hang the UI well past a minute.
    fine_imposed = fine_recovered = unpaid_fine = None
    paid_count = unpaid_count = None
    paid_amount = unpaid_amount = None
    arrest_total = pcm_total = None
    force_deployed = None

    def _fetch(url, params):
        try:
            r = requests.get(url, params=params,
                             headers=_HEADERS, timeout=_API_TIMEOUT)
            return r.json() if r.status_code == 200 else None
        except Exception as e:
            log.debug("aux fetch failed (%s): %s", url, e)
            return None

    from concurrent.futures import ThreadPoolExecutor, as_completed
    _common_params = {
        "tehsilId": tehsil_id,
        "startDate": api_start_str,
        "endDate": api_end_str,
    }
    with ThreadPoolExecutor(max_workers=3) as _pool:
        _fut_kpi = _pool.submit(_fetch, SDEO_TOP_KPIS, _common_params)
        _fut_cs = _pool.submit(_fetch, SDEO_CHALLAN_STATUS_BREAKDOWN, _common_params)
        _fut_pcm = _pool.submit(_fetch, PCM_DASHBOARD_COUNTS, {"tehsilId": tehsil_id})
        kpi = _fut_kpi.result()
        cs = _fut_cs.result()
        pcm_dc = _fut_pcm.result()

    if isinstance(kpi, dict):
        fine_imposed = kpi.get("totalFineImposed")
        fine_recovered = kpi.get("totalFineRecovered")
        unpaid_fine = kpi.get("unpaidFineAmount")
        force_deployed = kpi.get("totalForceDeployed")
    if isinstance(cs, dict):
        paid_count = cs.get("paidCount")
        unpaid_count = cs.get("unpaidCount")
        paid_amount = cs.get("paidAmount")
        unpaid_amount = cs.get("unpaidAmount")
    if isinstance(pcm_dc, dict):
        arrest_total = pcm_dc.get("totalArrest")
        pcm_total = pcm_dc.get("totalPCM")

    metrics = {
        "total_inspections": total_actions,
        "total_challans": challans,
        "fine_amount": fine_imposed,
        "sealed": sealed,
        # Arrest is omitted from the date-ranged primary table because
        # the Pcm/dashboard-counts endpoint ignores date filters and
        # would mis-label all-time arrests as date-filtered. Surfaced
        # in the all-time footer below instead.
        "arrest": None,
        "enforcer": force_deployed if force_deployed is not None else (
            len(officers) if officers else None
        ),
        "warnings": warnings,
        "paid_challans": paid_count,
        "unpaid_challans": unpaid_count,
        "paid_amount": paid_amount,
        "outstanding_amount": unpaid_amount if unpaid_amount is not None else unpaid_fine,
        "fine_recovered": fine_recovered,
        "firs": firs,
        "no_offenses": no_offenses,
        "removal_orders": removal_order,
        "epo": epo,
    }

    # Phase-40: focused-metric branch. If the user explicitly mentioned
    # one or more metrics in the question, render the focused report
    # instead of the full dashboard summary.
    requested_metrics = extract_requested_metrics(question or "")
    if requested_metrics:
        # Phase-43: when user asked about a financial metric we swap
        # the officer source from inspections-summary.officers[]
        # (no fine values) to /Pcm/officer-inspection-details (full
        # fine + recovery breakdown).
        money_metrics = {"fine_amount", "paid_amount", "outstanding_amount",
                         "fine_recovered"}
        is_money_focus = any(m in money_metrics for m in requested_metrics)
        officer_source_rows = officers
        officer_source_name = None
        if is_money_focus:
            fin_rows = fetch_officer_financial_breakdown(
                tehsil_id, start_date, end_date, location_name=tehsil_name,
            )
            if fin_rows:
                # Re-shape into the keys our get_officer_metric_value
                # already understands.
                officer_source_rows = [
                    {
                        "officerName": r["officer_name"],
                        "challan": r["challans"],
                        "fineAmount": r["fine_imposed"],
                        "paidAmount": r["fine_recovered"],
                        "outstandingAmount": r["outstanding_amount"],
                        "paid_recovery_pct": r["paid_recovery_pct"],
                    }
                    for r in fin_rows
                ]
                officer_source_name = "Pcm/officer-inspection-details"
        context = render_focused_metric_summary(
            metrics,
            requested_metrics=requested_metrics,
            location_name=tehsil_name,
            date_range=(start_date, end_date),
            source="SDEO Dashboard (Live)",
            officer_rows=officer_source_rows,
            officer_source=officer_source_name,
        )
    else:
        context = render_dashboard_summary(
            metrics,
            location_name=tehsil_name,
            date_range=(start_date, end_date),
            source="SDEO Dashboard (Live)",
            officer_rows=officers,
        )

    # Optional all-time footer for arrest + PCM. Suppressed for
    # money-only focused queries (Phase-42) since the user is asking
    # about a specific financial metric and dashboard parity demands
    # we don't dilute the answer with unrelated cumulative figures.
    money_only_focus = bool(requested_metrics) and all(
        k in {"fine_amount", "paid_amount", "outstanding_amount",
              "fine_recovered"}
        for k in requested_metrics
    )
    show_all_time_footer = (
        not money_only_focus
        and (arrest_total is not None or pcm_total is not None)
    )
    if show_all_time_footer:
        context += "\n#### All-time indicators (cumulative, not limited to the selected dates)\n\n"
        if arrest_total is not None:
            context += f"- Arrest cases: {int(arrest_total):,}\n"
        if pcm_total is not None:
            context += f"- PCM: {int(pcm_total):,}\n"

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
        "deterministic_answer": context,
        "structured_payload": context,
        "evidence_type": "structured_analytics",
    }


# ══════════════════════════════════════════════════════════════
# Phase-40: focused-metric detection + renderers
# ══════════════════════════════════════════════════════════════
# Map of canonical metric key → (display label, regex of aliases).
# Order matters: longer / more specific aliases (e.g. "fine recovered")
# must be checked before shorter overlapping ones (e.g. "fine amount").
_METRIC_LABEL: Dict[str, str] = {
    "firs": "FIRs Registered",
    "sealed": "Sealed Premises",
    "warnings": "Warnings",
    "total_challans": "Total Challans",
    "paid_challans": "Paid Challans",
    "unpaid_challans": "Unpaid Challans",
    "fine_amount": "Fine Imposed",
    "paid_amount": "Paid Amount",
    "outstanding_amount": "Outstanding Amount",
    "fine_recovered": "Fine Recovered",
    "no_offense": "No Offense",
    "epo": "EPO",
    "removal_order": "Removal Orders",
    "arrest": "Arrest Cases",
    "enforcer": "Enforcers",
    "total_inspections": "Total Inspections",
}

_METRIC_PATTERNS: List[Tuple[str, "re.Pattern"]] = [
    # Order: most specific first to avoid "fine recovered" being eaten
    # by the generic "fine" / "fine amount" rule.
    ("fine_recovered",     re.compile(r"\b(fine\s*recover(?:ed|y)?|recovered\s+amount|amount\s+recovered|recovery\s+amount)\b", re.I)),
    ("paid_amount",        re.compile(r"\b(paid\s+amount|recovered\s+(?:fine\s+)?amount|amount\s+paid)\b", re.I)),
    ("outstanding_amount", re.compile(r"\b(outstanding\s+amount|unpaid\s+amount|pending\s+amount|outstanding\s+fine)\b", re.I)),
    ("fine_amount",        re.compile(r"\b(fine\s+amount|fine\s+imposed|total\s+fine|imposed\s+fine)\b", re.I)),
    ("paid_challans",      re.compile(r"\b(paid\s+chall?[ae]+ns?|paid\s+chall?[ae]+n\s+count)\b", re.I)),
    ("unpaid_challans",    re.compile(r"\b(unpaid\s+chall?[ae]+ns?|unpaid\s+chall?[ae]+n\s+count)\b", re.I)),
    ("total_challans",     re.compile(r"\b(total\s+chall?[ae]+ns?|chall?[ae]+n\s+count)\b", re.I)),
    ("firs",               re.compile(r"\bfirs?\b|\bfirs?\s+(?:registered|registr(?:ation|ed))\b", re.I)),
    ("sealed",             re.compile(r"\b(sealed(?:\s+premises)?|sealing|seal\s+orders?)\b", re.I)),
    ("warnings",           re.compile(r"\bwarnings?\b", re.I)),
    ("no_offense",         re.compile(r"\bno\s+(?:offen[cs]es?|violations?)\b", re.I)),
    ("epo",                re.compile(r"\bepos?\b", re.I)),
    ("removal_order",      re.compile(r"\bremoval\s+orders?\b", re.I)),
    ("arrest",             re.compile(r"\barrest(?:s|\s+cases?)?\b", re.I)),
    ("enforcer",           re.compile(r"\b(enforcers?|force\s+deployed|total\s+force)\b", re.I)),
    ("total_inspections",  re.compile(r"\b(total\s+inspect(?:ion)?s?|total\s+actions?)\b", re.I)),
]

# Words that suggest the user is asking about a specific metric, not a
# generic dashboard summary. Without one of these, even keyword hits
# don't trigger focused mode (so "Multan City warnings" alone still
# routes to full summary; "Multan City performance on warnings" triggers
# focused mode).
_FOCUS_TRIGGER_RE = re.compile(
    r"\b(on|about|regarding|of|for|by|specifically|only|just|"
    r"details?|breakdown|stats?|count|counts|from)\b",
    re.I,
)

# Metrics that are themselves explicit two-word phrases. When one of
# these matches, focused mode fires unconditionally — the user clearly
# asked about a specific KPI even if no other trigger word is present.
_SELF_TRIGGER_METRICS = {
    "paid_amount", "outstanding_amount", "fine_amount", "fine_recovered",
}


def extract_requested_metrics(question: str) -> List[str]:
    """Return canonical metric keys the user asked about, or [] for a
    generic dashboard summary.

    Heuristic: the query must contain a focus trigger word (`on`,
    `about`, `for`, `regarding`, etc.) AND match at least one metric
    pattern. This avoids hijacking generic queries like
    "Multan City performance" (no metric — full dashboard) while
    catching "Multan City performance on FIRs" (focused).
    """
    q = (question or "").strip()
    if not q:
        return []
    has_trigger = bool(_FOCUS_TRIGGER_RE.search(q))
    seen: List[str] = []
    for key, pat in _METRIC_PATTERNS:
        if pat.search(q) and key not in seen:
            seen.append(key)
    if not seen:
        return []
    # If at least one matched metric is self-trigger (explicit
    # 2-word money phrase), focus regardless of trigger word.
    if any(k in _SELF_TRIGGER_METRICS for k in seen):
        return seen
    if not has_trigger:
        return []
    return seen


# ── Phase-43: officer financial breakdown fetcher ────────────
def fetch_officer_financial_breakdown(
    tehsil_id: int,
    start_date: date,
    end_date: date,
    *,
    location_name: str = "",
) -> List[Dict[str, Any]]:
    """Fetch per-officer financial rows for a tehsil/date range.

    Source: /Pcm/officer-inspection-details — same endpoint that powers
    the dashboard's "Top Officers by Challans" tile. Date semantics:
    `startDate` / `endDate` (date-only) match the dashboard exactly.

    Returns rows shaped:
      {
        "officer_name", "challans", "fine_imposed",
        "fine_recovered", "outstanding_amount",
        "paid_recovery_pct",
        "total_paid_challans", "total_unpaid_challans",
        "source",
      }

    On HTTP failure returns []. Caller renders a "Not available" note.
    """
    if not tehsil_id or not start_date or not end_date:
        return []
    try:
        resp = requests.get(
            PCM_OFFICER_INSPECTION_DETAILS,
            params={
                "tehsilId": tehsil_id,
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
            },
            headers=_HEADERS, timeout=_API_TIMEOUT,
        )
        resp.raise_for_status()
        rows = resp.json()
        if not isinstance(rows, list):
            return []
    except Exception as e:
        log.warning(
            "Officer financial breakdown fetch failed (tehsil=%s, %s..%s): %s",
            location_name or tehsil_id, start_date, end_date, e,
        )
        return []

    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        total_challans = int(r.get("totalChallans") or 0)
        paid_challans = int(r.get("totalPaidChallans") or 0)
        # Dashboard "Paid Recovery %" = paid challan COUNT / total
        # challan COUNT, NOT amount-ratio. Verified per probe.
        recovery_pct = (
            round(paid_challans / total_challans * 100, 2)
            if total_challans > 0 else None
        )
        out.append({
            "officer_name":          r.get("fullName") or "Unknown",
            "challans":              total_challans,
            "fine_imposed":          float(r.get("fineAmount") or 0),
            "fine_recovered":        float(r.get("paidChallanAmount") or 0),
            "outstanding_amount":    float(r.get("unPaidChallanAmount") or 0),
            "paid_recovery_pct":     recovery_pct,
            "total_paid_challans":   paid_challans,
            "total_unpaid_challans": int(r.get("totalUnPaidChallans") or 0),
            "source":                "Pcm/officer-inspection-details",
        })
    out.sort(key=lambda x: -x["challans"])
    return out


# ── Officer-row metric mapper ────────────────────────────────
def get_officer_metric_value(
    officer_row: Dict[str, Any], metric_key: str,
) -> Optional[float]:
    """Pull a per-officer numeric value for `metric_key`. Falls back
    across the multiple raw key spellings the SDEO API uses.
    """
    if not officer_row:
        return None
    candidates: Dict[str, List[str]] = {
        "firs":              ["fir", "firs", "firCase"],
        "sealed":            ["sealed"],
        "warnings":          ["warning", "warnings", "warningCase"],
        "total_challans":    ["challan", "challans", "challanCase"],
        "paid_challans":     ["paidChallan", "paid_challans", "paidCount"],
        "unpaid_challans":   ["unpaidChallan", "unpaid_challans", "unpaidCount"],
        "fine_amount":       ["fineAmount", "fine_imposed", "imposedFine"],
        "paid_amount":       ["paidAmount", "paid_amount"],
        "outstanding_amount":["unpaidAmount", "outstandingAmount"],
        "no_offense":        ["noOffense", "noOffenses", "no_offense"],
        "epo":               ["epo"],
        "removal_order":     ["removalOrder", "removal_orders"],
        "arrest":            ["arrestCase", "arrest", "arrests"],
        "total_inspections": ["inspection", "totalInspections", "totalActions"],
    }
    for k in candidates.get(metric_key, []):
        if k in officer_row and officer_row[k] is not None:
            try:
                return float(officer_row[k])
            except (TypeError, ValueError):
                pass
    return None


# ── Focused-metric renderer ──────────────────────────────────
def render_focused_metric_summary(
    metrics: Dict[str, Any],
    requested_metrics: List[str],
    location_name: str,
    date_range: Optional[Tuple[Optional[date], Optional[date]]] = None,
    source: str = "SDEO Dashboard",
    freshness: Optional[str] = None,
    officer_rows: Optional[List[Dict[str, Any]]] = None,
    officer_source: Optional[str] = None,
) -> str:
    """Render a focused-metric report. Used when the user asked about
    a specific metric (e.g. "performance on FIRs") instead of a full
    dashboard summary.

    Lays out:
      - Title naming the metric(s) and location
      - Date range + source
      - Compact metric table
      - Officer breakdown limited to the requested metric column(s)
      - Short interpretive note for zero/empty results
    """
    if not requested_metrics:
        return render_dashboard_summary(
            metrics, location_name, date_range, source, freshness,
            officer_rows=officer_rows,
        )

    # Heading reflects requested metrics. Money-only focused queries
    # use a tighter title ("Fine Imposed — Multan City") because
    # "Performance" implies activity totals.
    label_for = lambda k: _METRIC_LABEL.get(k, k.replace("_", " ").title())
    money_only = all(
        k in {"fine_amount", "paid_amount", "outstanding_amount",
              "fine_recovered"}
        for k in requested_metrics
    )
    if len(requested_metrics) == 1:
        title = label_for(requested_metrics[0])
        if not money_only:
            title += " Performance"
    else:
        labels = " and ".join(label_for(k) for k in requested_metrics)
        title = labels if money_only else f"{labels} Performance"

    head = f"### {title} — {location_name}\n\n"
    if date_range and date_range[0] and date_range[1]:
        head += f"**Date range:** {date_range[0]} to {date_range[1]}  \n"
    head += f"**Source:** {source}\n"
    if freshness:
        head += f"**Freshness:** {freshness}\n"

    def _fmt(val: Any, kind: str) -> str:
        if val is None:
            return "—"
        try:
            n = int(round(float(val)))
        except (TypeError, ValueError):
            return str(val)
        return f"Rs. {n:,}" if kind == "money" else f"{n:,}"

    # ── Metric table ──
    head += "\n| Metric | Value |\n| --- | ---: |\n"
    money_keys = {"fine_amount", "paid_amount", "outstanding_amount", "fine_recovered"}
    metric_lines: List[str] = []
    for key in requested_metrics:
        v = metrics.get(key)
        if v is None:
            v = 0
        kind = "money" if key in money_keys else "int"
        metric_lines.append(f"| {label_for(key)} | {_fmt(v, kind)} |")
    head += "\n".join(metric_lines) + "\n"

    # ── Phase-43: financial officer breakdown via dedicated source ──
    # When officer_source == 'Pcm/officer-inspection-details', the rows
    # carry challans + fine + recovery — render a 4-col table.
    if (officer_rows
            and officer_source == "Pcm/officer-inspection-details"
            and money_only):
        head += (
            "\n#### Officer Financial Breakdown\n\n"
            "| Officer | Challans | Fine Imposed | Paid Recovery % |\n"
            "| --- | ---: | ---: | ---: |\n"
        )
        # Top 10 by challans (matches dashboard "Top Officers" tile).
        for o in sorted(officer_rows, key=lambda x: -(x.get("challan") or 0))[:10]:
            name = o.get("officerName") or "Unknown"
            ch = int(o.get("challan") or 0)
            fine = int(round(float(o.get("fineAmount") or 0)))
            rec = o.get("paid_recovery_pct")
            rec_str = f"{rec}%" if rec is not None else "—"
            head += f"| {name} | {ch:,} | Rs. {fine:,} | {rec_str} |\n"

        # Reconciliation note when officer fine sum diverges from KPI total
        kpi_fine = metrics.get("fine_amount")
        if kpi_fine is not None:
            officer_sum = sum(
                int(round(float(o.get("fineAmount") or 0)))
                for o in officer_rows
            )
            try:
                kpi_int = int(round(float(kpi_fine)))
            except (TypeError, ValueError):
                kpi_int = None
            if kpi_int is not None and officer_sum != kpi_int:
                head += (
                    "\n_**Reconciliation note:** Officer-table fine sum "
                    f"(Rs. {officer_sum:,}) differs from the KPI total "
                    f"(Rs. {kpi_int:,}). The two endpoints can disagree "
                    "due to date-boundary or cache timing._\n"
                )
        return head

    # ── Officer breakdown — policy-aware (Phase-42) ──
    # For each requested metric, decide whether ANY officer row carries
    # a real (non-None) value. If yes, render. If no, skip the table
    # entirely so we never display fake "Rs. 0" rows.
    if officer_rows:
        primary_metric = requested_metrics[0]

        # Per-metric availability across officers.
        per_metric_has_data: Dict[str, bool] = {
            k: any(
                get_officer_metric_value(o, k) is not None
                for o in officer_rows
            )
            for k in requested_metrics
        }
        renderable_metrics = [k for k in requested_metrics if per_metric_has_data[k]]
        unavailable_metrics = [k for k in requested_metrics if not per_metric_has_data[k]]

        if renderable_metrics:
            col_labels = ["Officer"] + [label_for(k) for k in renderable_metrics]
            sep = ["---"] + ["---:" for _ in renderable_metrics]

            def _sort_key(o: Dict[str, Any]) -> float:
                v = get_officer_metric_value(o, renderable_metrics[0])
                return -(v if v is not None else 0)

            # Top-10 default — full list available via "all officers"/"top 25".
            sorted_officers = sorted(officer_rows, key=_sort_key)[:10]
            body_rows: List[str] = []
            any_value = False
            for o in sorted_officers:
                name = o.get("officerName") or o.get("officer_name") or "Unknown"
                vals: List[str] = []
                for k in renderable_metrics:
                    v = get_officer_metric_value(o, k)
                    if v is not None and v != 0:
                        any_value = True
                    kind = "money" if k in money_keys else "int"
                    # Real None -> "—"; real 0 -> "0"; real value -> formatted
                    if v is None:
                        vals.append("—")
                    else:
                        vals.append(_fmt(v, kind))
                body_rows.append(f"| {name} | " + " | ".join(vals) + " |")

            section_label = (
                "Officer Financial Breakdown" if money_only
                else "Officer breakdown"
            )
            head += (
                f"\n#### {section_label}\n\n"
                f"| {' | '.join(col_labels)} |\n"
                f"| {' | '.join(sep)} |\n"
                + "\n".join(body_rows) + "\n"
            )

            if not any_value:
                head += (
                    f"\n_No {label_for(primary_metric).lower()} were "
                    f"recorded in {location_name} during this period._\n"
                )

        if unavailable_metrics:
            unavail_labels = ", ".join(label_for(k) for k in unavailable_metrics)
            head += (
                f"\n**Officer-wise breakdown for {unavail_labels}:** "
                f"Not available from the SDEO dashboard summary endpoint.\n"
            )

    return head


# ══════════════════════════════════════════════════════════════
# Phase-39: professional dashboard summary renderer
# ══════════════════════════════════════════════════════════════
def render_dashboard_summary(
    metrics: Dict[str, Any],
    location_name: str,
    date_range: Optional[Tuple[Optional[date], Optional[date]]] = None,
    source: str = "SDEO Dashboard",
    freshness: Optional[str] = None,
    officer_rows: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Render a clean SDEO-dashboard-style KPI markdown report with
    section headings (Key KPI Cards / Financial / Enforcement /
    Officer Breakdown).

    `metrics` keys recognised (any may be None — None rows are skipped,
    0 rows are kept for important KPIs like Arrest):
      total_inspections, total_challans, fine_amount, sealed, arrest,
      enforcer, warnings, paid_challans, unpaid_challans,
      paid_amount, outstanding_amount, fine_recovered,
      firs, no_offenses, removal_orders, epo, pcm,
      partially_paid_count, partially_paid_amount, overdue_challans,
      overdue_amount.

    `officer_rows` is the raw `officers` list from the inspections
    summary endpoint. When provided, an Officer Breakdown markdown
    table is appended.
    """
    def _fmt(val: Any, kind: str) -> str:
        if val is None:
            return ""
        try:
            n = int(round(float(val)))
        except (TypeError, ValueError):
            return str(val)
        return f"Rs. {n:,}" if kind == "money" else f"{n:,}"

    # ── Header ──
    head = f"### Performance Summary — {location_name}\n\n"
    if date_range and date_range[0] and date_range[1]:
        head += f"**Date range:** {date_range[0]} to {date_range[1]}  \n"
    head += f"**Source:** {source}\n"
    if freshness:
        head += f"**Freshness:** {freshness}\n"

    # ── Section: Key KPI Cards (date-filtered, primary tiles) ──
    primary = [
        ("Total Inspections", "total_inspections", "int"),
        ("Total Challans",    "total_challans",    "int"),
        ("Fine Imposed",      "fine_amount",       "money"),
        ("Sealed Premises",   "sealed",            "int"),
        ("Arrest Cases",      "arrest",            "int"),
        ("Enforcers",         "enforcer",          "int"),
        ("Warnings",          "warnings",          "int"),
        ("Paid Challans",     "paid_challans",     "int"),
        ("Unpaid Challans",   "unpaid_challans",   "int"),
    ]
    primary_rows = [
        f"| {label} | {_fmt(metrics.get(key), kind)} |"
        for label, key, kind in primary
        if metrics.get(key) is not None
    ]
    section_kpi = ""
    if primary_rows:
        section_kpi = (
            "\n#### Key KPI Cards\n\n"
            "| KPI | Value |\n| --- | ---: |\n"
            + "\n".join(primary_rows) + "\n"
        )

    # ── Section: Financial Breakdown ──
    financial = [
        ("Paid Amount",        "paid_amount",        "money"),
        ("Outstanding Amount", "outstanding_amount", "money"),
        ("Fine Recovered",     "fine_recovered",     "money"),
    ]
    fin_rows = [
        f"| {label} | {_fmt(metrics.get(key), kind)} |"
        for label, key, kind in financial
        if metrics.get(key) is not None
        and (int(round(float(metrics.get(key) or 0))) != 0)
    ]
    section_fin = ""
    if fin_rows:
        section_fin = (
            "\n#### Financial Breakdown\n\n"
            "| Metric | Value |\n| --- | ---: |\n"
            + "\n".join(fin_rows) + "\n"
        )

    # ── Section: Enforcement Breakdown ──
    enforcement = [
        ("FIRs Registered", "firs",           "int"),
        ("EPO",             "epo",            "int"),
        ("Removal Orders",  "removal_orders", "int"),
        ("No Offense",      "no_offenses",    "int"),
    ]
    enf_rows = [
        f"| {label} | {_fmt(metrics.get(key), kind)} |"
        for label, key, kind in enforcement
        if metrics.get(key) is not None
    ]
    section_enf = ""
    if enf_rows:
        section_enf = (
            "\n#### Enforcement Breakdown\n\n"
            "| Metric | Value |\n| --- | ---: |\n"
            + "\n".join(enf_rows) + "\n"
        )

    # ── Section: Officer Breakdown (markdown table, top-10) ──
    section_officers = ""
    if officer_rows:
        # Cap at 10 rows by default to keep dashboards readable.
        officer_rows = list(officer_rows)[:10]
        cols = [
            ("Inspections", "total_inspections", "int"),
            ("Challans",    "total_challans",    "int"),
            ("Warnings",    "warnings",          "int"),
            ("Sealed",      "sealed",            "int"),
        ]
        col_labels = ["Officer"] + [c[0] for c in cols]
        sep_cells = ["---"] + ["---:" for _ in cols]

        sorted_officers = sorted(
            officer_rows,
            key=lambda o: -(get_officer_metric_value(o, "total_inspections") or 0),
        )

        body_rows: List[str] = []
        for o in sorted_officers:
            name = o.get("officerName") or o.get("officer_name") or "Unknown"
            vals = [
                _fmt(get_officer_metric_value(o, key), kind)
                for _, key, kind in cols
            ]
            body_rows.append(f"| {name} | " + " | ".join(vals) + " |")
        if body_rows:
            section_officers = (
                "\n#### Officer Breakdown\n\n"
                f"| {' | '.join(col_labels)} |\n"
                f"| {' | '.join(sep_cells)} |\n"
                + "\n".join(body_rows) + "\n"
            )

    return head + section_kpi + section_fin + section_enf + section_officers


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
    # SDEO endDate is exclusive at midnight when sent date-only.
    # Use the canonical formatter so end-of-day inclusive boundary
    # matches the dashboard's "To … 11:59 PM" tile.
    from pera_dates import to_sdeo_start_end
    api_start_str, api_end_str = to_sdeo_start_end(start_date, end_date)
    params = {
        "tehsilId": tehsil_id,
        "startDate": api_start_str,
        "endDate": api_end_str,
    }
    out: Dict[str, Any] = {
        "tehsil_id": tehsil_id,
        "tehsil_name": tehsil_name,
        "total_actions": 0, "challans": 0, "firs": 0, "warnings": 0,
        "no_offenses": 0, "sealed": 0, "removal_order": 0, "epo": 0,
        "officers": [],
        # Phase-41 canonical money keys (Decimal-friendly floats)
        "fine_imposed": 0.0, "fine_recovered": 0.0,
        "outstanding_amount": 0.0, "paid_amount": 0.0,
        "partially_paid_amount": 0.0,
        # Counts
        "paid_challans": 0, "unpaid_challans": 0,
        "partially_paid_challans": 0,
        # All-time PCM (kept separate so they never leak into date-ranged tables)
        "arrest_total": 0, "pcm_total": 0,
        "all_time_total_fine": 0.0,
        "all_time_paid_amount": 0.0,
        "all_time_unpaid_amount": 0.0,
        "error": None,
    }
    summary_resp = top_kpis_resp = status_resp = pcm_resp = None
    try:
        summary_resp = requests.get(SDEO_INSPECTIONS_SUMMARY, params=params,
                         headers=_HEADERS, timeout=_API_TIMEOUT).json()
        if isinstance(summary_resp, dict):
            out["total_actions"] = int(summary_resp.get("totalActions") or 0)
            out["challans"] = int(summary_resp.get("challans") or 0)
            out["firs"] = int(summary_resp.get("fiRs") or 0)
            out["warnings"] = int(summary_resp.get("warnings") or 0)
            out["no_offenses"] = int(summary_resp.get("noOffenses") or 0)
            out["sealed"] = int(summary_resp.get("sealed") or 0)
            out["removal_order"] = int(summary_resp.get("removalOrder") or 0)
            out["epo"] = int(summary_resp.get("epo") or 0)
            out["officers"] = summary_resp.get("officers") or []
    except Exception as e:
        out["error"] = f"summary:{e}"

    try:
        top_kpis_resp = requests.get(SDEO_TOP_KPIS, params=params,
                         headers=_HEADERS, timeout=_API_TIMEOUT).json()
    except Exception as e:
        out["error"] = (out["error"] or "") + f" kpi:{e}"

    try:
        status_resp = requests.get(SDEO_CHALLAN_STATUS_BREAKDOWN, params=params,
                          headers=_HEADERS, timeout=_API_TIMEOUT).json()
    except Exception as e:
        out["error"] = (out["error"] or "") + f" cs:{e}"

    # PCM totals are all-time, not date-filtered. Skip params.
    try:
        pcm_resp = requests.get(PCM_DASHBOARD_COUNTS,
                           params={"tehsilId": tehsil_id},
                           headers=_HEADERS, timeout=_API_TIMEOUT).json()
        if isinstance(pcm_resp, dict):
            out["arrest_total"] = int(pcm_resp.get("totalArrest") or 0)
            out["pcm_total"] = int(pcm_resp.get("totalPCM") or 0)
    except Exception as e:
        out["error"] = (out["error"] or "") + f" pcm:{e}"

    # ── Canonical financial merge (Phase-41) ──
    try:
        from pera_financial_sources import normalize_sdeo_financial_metrics
        fin = normalize_sdeo_financial_metrics(
            summary_resp=summary_resp, top_kpis_resp=top_kpis_resp,
            status_resp=status_resp, pcm_resp=pcm_resp,
            date_ranged=True,
        )
        for k in ("fine_imposed", "fine_recovered", "paid_amount",
                  "outstanding_amount", "partially_paid_amount",
                  "all_time_total_fine", "all_time_paid_amount",
                  "all_time_unpaid_amount"):
            v = fin.get(k)
            if v is not None:
                out[k] = float(v)
        for k in ("paid_challans", "unpaid_challans", "partially_paid_challans"):
            v = fin.get(k)
            if v is not None:
                out[k] = int(v)
    except Exception as e:
        log.debug("financial normalizer skipped: %s", e)

    return out


def _aggregate_tehsil_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Sum tehsil-level metrics into a single roll-up. Officer rows with the
    same officerName collapse into one record with summed counts so the
    output looks like a unified district/division-level breakdown.
    """
    agg = {
        "total_actions": 0, "challans": 0, "firs": 0, "warnings": 0,
        "no_offenses": 0, "sealed": 0, "removal_order": 0, "epo": 0,
        "fine_imposed": 0.0, "fine_recovered": 0.0,
        "outstanding_amount": 0.0, "paid_amount": 0.0,
        "partially_paid_amount": 0.0,
        # Legacy keys kept so older callers don't break:
        "unpaid_fine": 0.0,
        "paid_count": 0, "unpaid_count": 0,
        "paid_challans": 0, "unpaid_challans": 0,
        "partially_paid_challans": 0,
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
                  "paid_challans", "unpaid_challans",
                  "partially_paid_challans",
                  "arrest_total", "pcm_total"):
            agg[k] += int(r.get(k) or 0)
        for k in ("fine_imposed", "fine_recovered",
                  "outstanding_amount", "paid_amount",
                  "partially_paid_amount", "unpaid_fine"):
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
    """Render an aggregated district/division performance summary using
    the same `render_dashboard_summary` machinery as tehsil-level so
    the markdown output is consistent across hierarchy levels.
    """
    # Map aggregator keys → render_dashboard_summary keys.
    metrics = {
        "total_inspections":   agg.get("total_actions", 0),
        "total_challans":      agg.get("challans", 0),
        "fine_amount":         agg.get("fine_imposed") or None,
        "sealed":              agg.get("sealed", 0),
        # Arrest is all-time per PCM endpoint — keep out of primary table.
        "arrest":              None,
        "enforcer":            None,
        "warnings":            agg.get("warnings", 0),
        "paid_challans":       agg.get("paid_challans") or agg.get("paid_count") or 0,
        "unpaid_challans":     agg.get("unpaid_challans") or agg.get("unpaid_count") or 0,
        "paid_amount":         agg.get("paid_amount") or None,
        "outstanding_amount":  (
            agg.get("outstanding_amount") or agg.get("unpaid_fine") or None
        ),
        "fine_recovered":      agg.get("fine_recovered") or None,
        "firs":                agg.get("firs", 0),
        "no_offenses":         agg.get("no_offenses", 0),
        "removal_orders":      agg.get("removal_order", 0),
        "epo":                 agg.get("epo", 0),
    }

    head = render_dashboard_summary(
        metrics,
        location_name=f"{level.title()}: {name}",
        date_range=(start_date, end_date),
        source="SDEO Dashboard (Live, aggregated across tehsils)",
        officer_rows=agg.get("officers"),
    )

    head += (
        f"\n_Aggregation: sum of {agg.get('tehsils_covered', 0)}"
        f"/{tehsil_count} tehsils"
    )
    if agg.get("tehsils_failed"):
        head += f" — {agg['tehsils_failed']} tehsils errored"
    head += "._\n"

    # All-time PCM footer
    if agg.get("arrest_total") or agg.get("pcm_total"):
        head += "\n#### All-time indicators (cumulative, not limited to the selected dates)\n\n"
        if agg.get("arrest_total"):
            head += f"- Arrest cases: {int(agg['arrest_total']):,}\n"
        if agg.get("pcm_total"):
            head += f"- PCM: {int(agg['pcm_total']):,}\n"

    # Per-tehsil audit table (kept for dashboard parity verification)
    per_tehsil = sorted(
        list(agg.get("_rows", []) or []),
        key=lambda r: -(r.get("total_actions") or 0),
    )
    if per_tehsil:
        head += "\n#### Per-Tehsil Breakdown\n\n"
        head += "| Tehsil | Inspections | Challans | Warnings | Sealed | Fine Imposed |\n"
        head += "| --- | ---: | ---: | ---: | ---: | ---: |\n"
        for r in per_tehsil:
            tag = " [ERR]" if r.get("error") else ""
            head += (
                f"| {r['tehsil_name']}{tag} "
                f"| {r['total_actions']:,} "
                f"| {r['challans']:,} "
                f"| {r['warnings']:,} "
                f"| {r['sealed']:,} "
                f"| Rs. {int(r.get('fine_imposed') or 0):,} |\n"
            )

    return head


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
    question: str = "",
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
            question=question,
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
    question: str = "",
) -> Dict[str, Any]:
    """Call PCM officer-inspections API live for a specific date range.

    Phase-44: individual inspection records are only emitted when the
    user explicitly asks ("records", "details", "list", "cases").
    Default summary queries get just Activity + Financials tables.
    """
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
    firs = sum(1 for r in records if _is_true(r.get("firCase")))
    sealed = sum(1 for r in records if _is_true(r.get("sealed")))
    epo_count = sum(1 for r in records if _is_true(r.get("epo")))
    rem_orders = sum(1 for r in records if _is_true(r.get("removalOrder")))
    fine_imposed = sum(float(r.get("fineAmount", 0) or 0) for r in records)
    # paidAmount per record may or may not be present in PCM response.
    fine_recovered_raw = [
        r.get("paidAmount") for r in records
        if r.get("paidAmount") not in (None, "")
    ]
    fine_recovered = sum(float(v or 0) for v in fine_recovered_raw) if fine_recovered_raw else None
    outstanding = (
        fine_imposed - fine_recovered
        if fine_recovered is not None and fine_imposed >= fine_recovered
        else None
    )
    recovery_pct = (
        round(fine_recovered / fine_imposed * 100, 2)
        if fine_recovered is not None and fine_imposed > 0
        else None
    )

    # Markdown report
    context = f"### Officer Performance Summary — {officer_name}\n\n"
    context += f"**Date range:** {start_date} to {end_date}  \n"
    context += f"**Location:** {tehsil_name}, {district_name}, {division_name}  \n"
    context += "**Source:** PCM Officer Inspections (Live)\n\n"
    context += "#### Activity\n\n"
    context += "| Metric | Value |\n| --- | ---: |\n"
    context += f"| Total Inspections / Records | {total:,} |\n"
    context += f"| Challans Issued | {challans:,} |\n"
    context += f"| Warnings Issued | {warnings:,} |\n"
    context += f"| No Offense | {no_offence:,} |\n"
    context += f"| FIRs Registered | {firs:,} |\n"
    context += f"| Sealed Premises | {sealed:,} |\n"
    if epo_count:
        context += f"| EPO | {epo_count:,} |\n"
    if rem_orders:
        context += f"| Removal Orders | {rem_orders:,} |\n"
    if arrests:
        context += f"| Arrest Cases | {arrests:,} |\n"
    if confiscated:
        context += f"| Confiscated | {confiscated:,} |\n"

    context += "\n#### Financials\n\n"
    context += "| Metric | Value |\n| --- | ---: |\n"
    context += f"| Officer Fine Imposed | Rs. {int(fine_imposed):,} |\n"
    if fine_recovered is not None:
        context += f"| Officer Fine Recovered | Rs. {int(fine_recovered):,} |\n"
        if outstanding is not None:
            context += f"| Outstanding Amount | Rs. {int(outstanding):,} |\n"
        if recovery_pct is not None:
            context += f"| Paid Recovery % | {recovery_pct}% |\n"
    else:
        context += (
            "\n_Note: Officer Fine Recovered is not available from this "
            "endpoint; only Fine Imposed is reported._\n"
        )

    # Phase-44: emit individual records ONLY when the user asked for
    # them. Default summary stays clean (Activity + Financials tables).
    show_records = False
    record_limit = 10
    if question:
        try:
            from pera_source_policy import user_wants_records, extract_limit
            show_records = user_wants_records(question)
            if show_records:
                record_limit = extract_limit(question, default=10, max_limit=25)
        except Exception:
            pass
    if records and show_records:
        context += (
            f"\n#### Sample Inspection Records — showing "
            f"{min(len(records), record_limit)} of {len(records):,}\n\n"
        )
        for i, r in enumerate(records[:record_limit]):
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
        "deterministic_answer": context,
        "structured_payload": context,
        "evidence_type": "officer_table",
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
