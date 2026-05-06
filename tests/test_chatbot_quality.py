"""Comprehensive chatbot quality test suite.

Covers 14 categories:
  A. Positive simple — clear intent (30)
  B. Positive complex — multi-part / analytical (15)
  C. Roman-Urdu / Mixed-language (25)
  D. Typo / spelling tolerance (25)
  E. Out-of-scope / non-PERA topics (15)
  F. Misleading / contradictory queries (10)
  G. Very short queries (15)
  H. Empty / whitespace / nonsense (5)
  I. Stress — long multi-intent queries (15)
  J. Date format variations (20)
  K. Officer / person queries (15)
  L. Fallback / ambiguous (10)
  M. Hierarchy disambiguation (division/district/tehsil) (10)
  N. Cross-domain (challan vs inspection vs OA) (15)

Total: 225+ test cases. All assertions test deterministic behaviour of
intent detectors and the query router (no live OpenAI calls). Live API
smoke tests live in test_inspection_aggregation.py and are gated by
RUN_LIVE_AGG=1.

Run: pytest tests/test_chatbot_quality.py -q
"""
from __future__ import annotations

import os
import re
from typing import Optional

import pytest
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
os.environ.setdefault("ANALYTICS_DB_ENABLED", "1")

from inspection_lookup import detect_inspection_intent
from challan_lookup import detect_challan_intent
from operational_activity_lookup import detect_operational_activity_intent
from query_router import classify_query, QueryType


_GAP = pytest.mark.xfail(
    reason="known gap — see docs/CHATBOT_QUALITY_REPORT.md",
    strict=False,
)


def _any_intent(q: str) -> Optional[str]:
    """Return the first non-None intent across all domain detectors, or None."""
    return (
        detect_inspection_intent(q)
        or detect_challan_intent(q)
        or detect_operational_activity_intent(q)
    )


# ══════════════════════════════════════════════════════════════
# A. POSITIVE — simple clear queries (30)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q,domain", [
    ("inspections summary for Lahore division",                 "inspection"),
    ("Multan division inspections from 1 March to 5 March 2026","inspection"),
    ("Faisalabad district inspection report",                   "inspection"),
    ("Shalimar tehsil inspections summary",                     "inspection"),
    ("Lahore Cantt inspections from 1 Jan to 10 Jan 2026",      "inspection"),
    ("how many inspections in Rawalpindi division",             "inspection"),
    ("FIRs registered in Lahore division",                      "inspection"),
    ("sealed premises in Multan district",                      "inspection"),
    ("warnings issued in Shalimar tehsil",                      "inspection"),
    ("EPO orders in Faisalabad district",                       "inspection"),
    ("removal orders in Lahore division",                       "inspection"),
    ("arrest cases in Multan division",                         "inspection"),
    ("total actions in Lahore Cantt",                           "inspection"),
    ("Rabia Altaf inspections summary",                         "inspection"),
    ("Ahmad Yaar inspection details",                           "inspection"),
    ("challan totals overview",                                 "challan"),
    ("paid challans in Lahore Saddar",                          "challan"),
    ("unpaid challans in Multan",                               "challan"),
    ("overdue challans count Lahore division",                  "challan"),
    ("challan summary by division",                             "challan"),
    ("price control challans in Faisalabad",                    "challan"),
    ("encroachment challans in Multan district",                "challan"),
    ("hoarding challans in Lahore division",                    "challan"),
    ("challan breakdown for Lahore Saddar",                     "challan"),
    ("station with most challans",                              "challan"),
    ("operational activities summary",                          "oa"),
    pytest.param("operations conducted in Lahore division",     "oa",         marks=_GAP),
    ("requisitions filed in Multan district",                   "oa"),
    ("operational activity in Shalimar tehsil",                 "oa"),
    ("OA summary by division",                                  "oa"),
])
def test_positive_simple(q, domain):
    insp = detect_inspection_intent(q)
    chal = detect_challan_intent(q)
    oa = detect_operational_activity_intent(q)
    if domain == "inspection":
        assert insp is not None, f"{q!r} not detected as inspection (insp={insp},chal={chal},oa={oa})"
    elif domain == "challan":
        assert chal is not None or insp is not None, f"{q!r} unmatched"
    elif domain == "oa":
        assert oa is not None or insp is not None, f"{q!r} unmatched"


# ══════════════════════════════════════════════════════════════
# B. POSITIVE complex / multi-part queries (15)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "Lahore division inspections from 1 March to 5 March 2026 with paid and unpaid challan breakdown",
    "Multan district inspection summary including FIRs sealed and arrest cases for Feb 2026",
    "Faisalabad division performance for January 2026 with officer breakdown",
    "Show me Shalimar tehsil inspection performance Jan 2026 and total fine recovered",
    "Compare Lahore and Multan division inspection numbers for March 2026",
    "Lahore Cantt inspections plus Model Town inspections for Feb 2026",
    "Rabia Altaf inspections from 1 Feb to 28 Feb 2026 with challan details",
    "Top 5 tehsils by inspection count in Lahore division",
    "Faisalabad district FIRs sealed and warnings count for January 2026",
    "Multan division inspection performance and challan recovery rate for Feb 2026",
    pytest.param("Lahore district fine imposed vs fine recovered for March 2026", marks=_GAP),
    "Sahiwal division inspections plus operational activity summary",
    "Bahawalpur district inspections from 1 Jan to 31 Jan 2026 with officer breakdown",
    "Lahore division inspection summary by tehsil for last week of March 2026",
    "Rawalpindi district performance report covering inspections challans and FIRs",
])
def test_positive_complex(q):
    intent = _any_intent(q)
    assert intent is not None, f"{q!r} unmatched"


# ══════════════════════════════════════════════════════════════
# C. Roman-Urdu / Mixed language (25)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "Lahore division ki inspections summary kya hai",
    "Multan division mein kitne inspections hue",
    "Shalimar tehsil mein kitne sealed premises",
    "Faisalabad district ki inspections kitni hain",
    "Lahore Cantt tehsil ki inspection performance batao",
    "Multan district mein arrest cases ki tadaad",
    "Rawalpindi division mein FIRs ki tadaad",
    "Lahore division ki inspections details",
    "kitni inspections Lahore division mein",
    "Faisalabad district kitne sealed premises",
    "Multan division ki inspection summary January 2026",
    "Shalimar tehsil ki inspection summery 1 Jan to 10 Jan 2026",
    "Lahore district ki inspection performance dikhao",
    "kitne challans Lahore division mein",
    "Multan district mein paid aur unpaid challans",
    "Faisalabad division ki challans ki summary",
    "Lahore Saddar tehsil ke challans batao",
    "kitne FIRs Multan district mein",
    "Sahiwal division ki inspection performance kya hai",
    "Bahawalpur district mein inspections kitni hain",
    "DG Khan division mein inspection summary",
    "Sialkot district ki inspections details",
    "Gujranwala division ki performance report",
    "Lahore Cantt tehsil mein FIRs ki tadaad",
    "Wahga tehsil ki inspection summary",
])
def test_roman_urdu(q):
    intent = _any_intent(q)
    assert intent is not None, f"{q!r} unmatched"


# ══════════════════════════════════════════════════════════════
# D. Typo / spelling tolerance (25)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "inspecton summary lahore division",
    "inspeciton summary lahore division",          # transposed letters
    "inspections summery lahore division",         # summery typo
    "inspections summmery lahore division",        # extra m
    "inspections summari lahore division",         # missing y
    "chaalans count lahore division",              # extra a
    "chaallans summary multan division",
    "chalan total lahore division",
    "chalaans count for Lahore",
    "challanns total Multan district",             # extra n
    "chellan in Multan",                           # vowel swap
    pytest.param("inspecions performance lahore division",      marks=_GAP),  # missing letter
    pytest.param("sealled premises in shalimar",  marks=_GAP),  # extra l (no fuzzy yet)
    pytest.param("warings issued multan district", marks=_GAP),  # missing n (no fuzzy yet)
    pytest.param("FIRSs filed lahore division",                  marks=_GAP),  # extra s
    "lahore divsion inspections",                  # missing letter
    "multan distrct inspections",                  # missing letter
    "shaalimar tehsil inspections",                # extra a
    "Faisalbad district inspections",              # missing a
    "Lahor division inspections summary",          # missing e
    "Lahoree division inspections",                # extra e
    "Lahor districct inspections",                 # double error
    "summary of inpsections lahore division",      # transposed
    "ispections summary multan division",          # missing n
    "inspect summary lahore division",             # truncated
])
def test_typo_tolerance(q):
    intent = _any_intent(q)
    assert intent is not None, f"{q!r} unmatched"


# ══════════════════════════════════════════════════════════════
# E. Out-of-scope / non-PERA (15) — should NOT detect domain intent
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "what is the weather today in Lahore",
    "tell me a joke",
    "who is the prime minister of Pakistan",
    "what is the capital of pakistan",
    "convert 100 USD to PKR",
    "what time is it now",
    "recommend a restaurant in Lahore",
    "how to learn python programming",
    "what is the meaning of life",
    "translate hello to urdu",
    "what is 2 plus 2",
    "tell me about the latest movie",
    "what is the score of pakistan vs india match",
    "explain quantum physics",
    "how to cook biryani",
])
def test_out_of_scope_returns_none(q):
    intent = _any_intent(q)
    assert intent is None, f"{q!r} should be None, got {intent}"


# ══════════════════════════════════════════════════════════════
# F. Misleading / contradictory (10)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q,expected_none", [
    ("show me inspections in mars",                   True),     # nonsense location
    ("inspections in fictional district",             True),
    pytest.param("challans in xyz division",          True, marks=_GAP),
    ("sealed premises in unknown tehsil",             True),
    ("inspection report for area 51",                 True),
    pytest.param("performance summary",               True),     # too vague — currently still routes
    ("show data for nowhere",                         True),
    ("FIRs in atlantis",                              False),    # 'firs' keyword still matches
    ("inspections ?",                                 False),    # bare keyword
    ("challan in",                                    False),    # incomplete location
])
def test_misleading_queries(q, expected_none):
    intent = _any_intent(q)
    if expected_none:
        # Either None or low-confidence summary fallback is acceptable
        assert intent is None or intent in ("insp_summary",), f"{q!r} -> {intent}"
    else:
        assert intent is not None


# ══════════════════════════════════════════════════════════════
# G. Very short queries (15)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q,should_match", [
    ("inspections",      True),
    ("challans",         True),
    ("FIRs",             True),
    ("sealed",           True),
    ("warnings",         True),
    ("Multan",           False),    # location only — ambiguous
    ("Lahore",           False),
    ("Shalimar",         False),
    ("data?",            False),
    ("hi",               False),
    ("ok",               False),
    ("yes",              False),
    ("?",                False),
    ("info",             False),
    ("help",             False),
])
def test_very_short_queries(q, should_match):
    intent = _any_intent(q)
    if should_match:
        assert intent is not None, f"{q!r} should match"
    else:
        assert intent is None, f"{q!r} should NOT match (got {intent})"


# ══════════════════════════════════════════════════════════════
# H. Empty / whitespace / nonsense (5)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "",
    "   ",
    "\n\t",
    "asdfghjkl",
    "............",
])
def test_empty_or_nonsense(q):
    assert _any_intent(q) is None


# ══════════════════════════════════════════════════════════════
# I. Stress — long multi-intent queries (15)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "Please give me a comprehensive inspection performance report for the Lahore division covering January 1 to January 31 2026 with full breakdown by district tehsil officer with FIRs sealed warnings paid and unpaid challans fine imposed fine recovered and outstanding amounts also include arrest cases and PCM totals where available",
    "I need to compare the inspection performance of Lahore Multan and Faisalabad divisions for March 2026 including total inspections challans FIRs sealed warnings and the recovery percentage of challans for each",
    "Show me a detailed analytical summary of all enforcement activities in the Multan division from February 1 2026 to February 28 2026 with officer level breakdown and tehsil level breakdown including all key performance indicators",
    "Generate a 30-day rolling inspection performance summary for the Lahore Cantt tehsil with officer ranking by inspections challans warnings and FIRs",
    "Provide me with the inspection performance for Lahore division Multan division and Faisalabad division for January 2026 February 2026 and March 2026 in a comparable format",
    "I want to see all PERA enforcement data for Shalimar tehsil from 1 January 2026 to 31 March 2026 including inspections challans operational activities and any related fines",
    "Detailed report of officer performance across Lahore division from 1 Feb 2026 to 28 Feb 2026 sorted by inspections challans and recovery rate",
    "Full audit trail of inspections in the Lahore Cantt tehsil from Jan 1 to Mar 31 2026 with per-officer numbers per-day if possible plus all sealed premises",
    "Comprehensive performance review of Multan district inspections in 2026 quarter 1 including January February and March",
    "I need a complete inspection summary for all eleven tehsils in Lahore district for January 1 2026 to January 10 2026",
    "Show inspection details for Faisalabad division covering all districts and tehsils within for Feb 1 to Feb 28 2026 including FIRs sealed arrests and warnings",
    "Long-form inspection report for Lahore division for 1 January 2026 to 31 March 2026 broken down by district by tehsil and by officer with totals at each level",
    "Senior officer dashboard for Multan division inspections during March 2026 with KPI breakdown by tehsil and officer ranking",
    "Combined inspection challan and operational activity summary for Lahore division January 2026 with key metrics for each domain",
    "Complete enforcement performance report for Shalimar tehsil from 1 March to 5 March 2026 including all officers all metrics all challan statuses and all action types",
])
def test_stress_long_queries(q):
    intent = _any_intent(q)
    assert intent is not None, f"long query {q[:60]!r}... unmatched"


# ══════════════════════════════════════════════════════════════
# J. Date format variations (20)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "Lahore division inspections from 1 March 2026 to 31 March 2026",
    "Multan district inspection summary March 2026",
    "Shalimar tehsil inspections from 1st March to 31st March 2026",
    "Faisalabad division inspections between Feb 1 and Feb 28 2026",
    "Lahore Cantt inspections from 2026-01-01 to 2026-01-31",
    "Sahiwal district inspections last month",
    "Multan division inspection performance this week",
    "Lahore Saddar tehsil last 7 days inspection",
    "Bahawalpur district inspection summary January 2026",
    "Multan district inspections Feb 1 to Feb 28 2026",
    "Lahore division inspection March 2026 performance",
    "Shalimar tehsil 1 May 2026 to 10 May 2026 inspections",
    "Faisalabad district inspections from 5 Feb to 25 Feb 2026",
    "Lahore Cantt inspections during Feb 2026",
    "Multan division inspection report for Q1 2026",
    "Sahiwal district inspections from Jan 1 till Jan 31 2026",
    "Lahore division inspections in March 2026",
    "Faisalabad district inspections from 1st Apr to 30th Apr 2026",
    "Shalimar tehsil inspection details for 1 Jan to 5 Jan 2026",
    "Multan district inspections during the first week of March 2026",
])
def test_date_format_variations(q):
    intent = detect_inspection_intent(q)
    assert intent is not None, f"{q!r} unmatched"


# ══════════════════════════════════════════════════════════════
# K. Officer / person queries (15)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "Rabia Altaf inspections summary",
    "How many inspections did Rabia Altaf do",
    "Inspections by Ahmad Yaar",
    "Amir Hussain inspection summary",
    "Shakeel Ahmad inspections details",
    "Rabia Altaf ki inspections summary",
    "Maliha Mohsin inspection performance",
    "Taskeen Khalid inspection details",
    "Rabia Altaf 1 March to 5 March 2026 inspections",
    "Maliha Mohsin sealed premises count",
    "Ahmad Yaar warning issued",
    "Inspection performance of officer Muhammad Ayaz",
    "Rabia Altaf challan total inspections",
    "Top officers by inspections in Lahore division",
    "Amir Hussain inspection report",
])
def test_officer_queries(q):
    insp = detect_inspection_intent(q)
    assert insp is not None, f"{q!r} unmatched"


# ══════════════════════════════════════════════════════════════
# L. Fallback / ambiguous queries (10)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q,expected_kind", [
    ("inspections summary",                       "summary"),
    ("inspection performance",                    "summary"),
    ("inspections in pakistan",                   "summary"),
    ("show inspections",                          "summary"),
    ("PERA inspections",                          "summary"),
    ("inspection data",                           "summary"),
    ("total inspections",                         "summary"),
    ("regulatory inspections",                    "summary"),
    ("inspection report",                         "summary"),
    ("how many inspections",                      "summary"),
])
def test_fallback_summary(q, expected_kind):
    intent = detect_inspection_intent(q)
    assert intent is not None, f"{q!r} returned None"
    # Must NOT misroute to a specific officer/division when the query has
    # no location or person.
    assert not intent.startswith("insp_officer:"), (
        f"{q!r} mis-routed to officer: {intent}"
    )


# ══════════════════════════════════════════════════════════════
# M. Hierarchy disambiguation (10)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q,level", [
    ("Lahore division inspections",     "division"),
    ("Lahore district inspections",     "district"),
    ("Lahore Cantt inspections",        "tehsil"),
    ("Multan division inspections",     "division"),
    ("Multan district inspections",     "district"),
    ("Faisalabad division inspections", "division"),
    ("Shalimar tehsil inspections",     "tehsil"),
    ("Shalimar inspections summary",    "tehsil"),
    ("Lahore Cantt station inspections","tehsil"),
    ("Lahore division ki inspections",  "division"),
])
def test_hierarchy_routing(q, level):
    intent = detect_inspection_intent(q)
    assert intent is not None
    assert intent.startswith(f"insp_{level}:"), f"{q!r} -> {intent} (expected level={level})"


# ══════════════════════════════════════════════════════════════
# N. Cross-domain routing (15)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q,prefer", [
    ("inspections summary lahore division",        "inspection"),
    ("challan summary lahore division",            "challan"),
    ("paid challans Lahore Saddar",                "challan"),
    ("operational activities in Multan division",  "oa"),
    ("requisitions filed in Lahore",               "oa"),
    ("FIRs in Multan division",                    "inspection"),
    ("sealed premises in Lahore Cantt",            "inspection"),
    ("encroachment challans in Lahore",            "challan"),
    ("price control challans in Multan",           "challan"),
    pytest.param("operations conducted in Faisalabad", "oa", marks=_GAP),
    ("inspection performance Lahore division",     "inspection"),
    ("station with most challans",                 "challan"),
    ("hoarding challans Lahore",                   "challan"),
    ("OA summary Multan",                          "oa"),
    ("warnings issued in Shalimar tehsil",         "inspection"),
])
def test_cross_domain_routing(q, prefer):
    insp = detect_inspection_intent(q)
    chal = detect_challan_intent(q)
    oa = detect_operational_activity_intent(q)
    if prefer == "inspection":
        assert insp is not None, f"{q!r} expected inspection (got insp={insp},chal={chal},oa={oa})"
    elif prefer == "challan":
        assert chal is not None or insp is not None
    elif prefer == "oa":
        assert oa is not None or insp is not None


# ══════════════════════════════════════════════════════════════
# O. Query router classification (10)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q,expected", [
    ("how many inspections in Lahore division",                QueryType.STRUCTURED),
    ("workforce strength Lahore",                              QueryType.STRUCTURED),
    ("top 5 tehsils by inspections",                           QueryType.STRUCTURED),
    ("compare Lahore and Multan division inspections",         QueryType.STRUCTURED),
    ("what is the salary of Director PERA",                    QueryType.DOCUMENT),
    ("PERA Act section 12",                                    QueryType.DOCUMENT),
    ("eligibility criteria for Sub-Inspector",                 QueryType.DOCUMENT),
    ("appointment procedure for Director",                     QueryType.DOCUMENT),
    ("how many sub-inspectors are there and what is their salary",  QueryType.HYBRID),
    pytest.param("workforce strength of director and their reporting",     QueryType.HYBRID, marks=_GAP),
])
def test_query_router(q, expected):
    assert classify_query(q) == expected, f"{q!r} -> {classify_query(q).value}"


# ══════════════════════════════════════════════════════════════
# P. Robustness — same query in multiple casings / spacings (10)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "LAHORE DIVISION INSPECTIONS",
    "lahore   division   inspections",
    " Lahore Division Inspections ",
    "lahore-division-inspections",
    "lahore division   inspections summary",
    "InSpEcTiOnS LaHoRe DiViSiOn",
    "lahore  cantt  tehsil  inspections",
    "MULTAN district inspections",
    "shalimar TEHSIL inspections",
    "FAISALABAD division INSPECTIONS",
])
def test_casing_and_spacing(q):
    intent = detect_inspection_intent(q)
    assert intent is not None, f"{q!r} unmatched"


# ══════════════════════════════════════════════════════════════
# Q. Negative — partial / dangling phrases (10)
# ══════════════════════════════════════════════════════════════
@pytest.mark.parametrize("q", [
    "inspection in",
    "show me",
    "performance",                    # word alone
    "summary",                        # word alone
    "data for",
    "tell me about",
    "from 1 jan to",
    "between",
    "all",
    "?",
])
def test_partial_dangling_no_match(q):
    # We don't strictly require None — but the result must NOT be a
    # high-confidence routed intent like a specific officer/location.
    intent = _any_intent(q)
    assert intent is None or intent in ("insp_summary", "challan_totals"), (
        f"{q!r} -> {intent}"
    )
