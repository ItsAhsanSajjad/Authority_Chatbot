"""Tests for the SDEO-dashboard-style summary renderer + the
'performance' keyword routing fix."""
from __future__ import annotations

import os
from datetime import date

import pytest
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
os.environ.setdefault("ANALYTICS_DB_ENABLED", "1")

from inspection_lookup import (
    detect_inspection_intent,
    render_dashboard_summary,
)


# ── Routing — performance keyword + location + date range ─────
@pytest.mark.parametrize("q,expected_prefix", [
    ("TELL ME THE PERFORMANCE OF MULTAN CITY FROM 1ST APRIL TO 20 APRIL 2026",
     "insp_tehsil:Multan city"),
    ("Multan City performance from 1 April to 20 April 2026",
     "insp_tehsil:Multan city"),
    ("Lahore Cantt report card 1 April to 20 April 2026",
     "insp_tehsil:Lahore Cantt"),
    ("Lahore Cantt dashboard 1 April to 20 April 2026",
     "insp_tehsil:Lahore Cantt"),
    ("Lahore Cantt KPI summary April 1 to April 20 2026",
     "insp_tehsil:Lahore Cantt"),
    ("Lahore district performance last week",
     "insp_district:Lahore"),
    ("Faisalabad division performance Q1 2026",
     "insp_division:Faisalabad"),
    ("DG Khan division performance",
     "insp_division:D.G. Khan"),
])
def test_performance_routing(q, expected_prefix):
    intent = detect_inspection_intent(q)
    assert intent is not None, f"{q!r} returned None"
    assert intent.startswith(expected_prefix), (
        f"{q!r} -> {intent} (expected prefix {expected_prefix})"
    )


# ── Renderer output for the acceptance case ──────────────────
@pytest.fixture
def acceptance_metrics():
    return {
        "total_inspections": 7400,
        "total_challans": 415,
        "fine_amount": 2828000,
        "sealed": 19,
        "arrest": 0,
        "enforcer": 4,
        "warnings": 625,
        "paid_challans": 264,
        "unpaid_challans": 151,
        "paid_amount": 1713500,
        "outstanding_amount": 1114500,
    }


def test_renderer_contains_all_dashboard_kpis(acceptance_metrics):
    out = render_dashboard_summary(
        acceptance_metrics,
        location_name="Multan City",
        date_range=(date(2026, 4, 1), date(2026, 4, 20)),
    )
    expected_pairs = [
        ("Total Inspections", "7,400"),
        ("Total Challans",    "415"),
        ("Fine Imposed",      "Rs. 2,828,000"),
        ("Sealed Premises",   "19"),
        ("Arrest Cases",      "0"),
        ("Enforcers",         "4"),
        ("Warnings",          "625"),
        ("Paid Challans",     "264"),
        ("Unpaid Challans",   "151"),
        ("Paid Amount",       "Rs. 1,713,500"),
        ("Outstanding Amount","Rs. 1,114,500"),
    ]
    for label, value in expected_pairs:
        assert f"| {label} |" in out, f"missing label {label}"
        assert value in out, f"missing value {value}"
    assert "Performance Summary — Multan City" in out
    assert "2026-04-01 to 2026-04-20" in out


def test_renderer_skips_none_optional_rows():
    metrics = {
        "total_inspections": 100, "total_challans": 10, "fine_amount": 50000,
        "sealed": 1, "arrest": 0, "enforcer": 2, "warnings": 5,
        "paid_challans": 3, "unpaid_challans": 7,
        # optional rows not provided
    }
    out = render_dashboard_summary(metrics, "Test")
    # FIRs, EPO, PCM not included → no row
    assert "| FIRs |" not in out
    assert "| EPO |" not in out
    assert "| PCM |" not in out


def test_renderer_keeps_zero_for_primary_kpis():
    """0 must be visible for important KPIs."""
    metrics = {
        "total_inspections": 100, "total_challans": 10, "fine_amount": 0,
        "sealed": 0, "arrest": 0, "enforcer": 0, "warnings": 0,
        "paid_challans": 0, "unpaid_challans": 0,
    }
    out = render_dashboard_summary(metrics, "Test")
    # All primary KPIs render, even with value 0
    assert "| Arrest Cases | 0 |" in out
    assert "| Sealed Premises | 0 |" in out
    assert "| Total Inspections | 100 |" in out


def test_renderer_pera_dates_helper_format():
    """to_sdeo_start_end must return inclusive 00:00:00 / 23:59:59
    boundaries so live SDEO calls match dashboard tiles."""
    from pera_dates import to_sdeo_start_end
    s, e = to_sdeo_start_end(date(2026, 4, 1), date(2026, 4, 20))
    assert s == "2026-04-01T00:00:00"
    assert e == "2026-04-20T23:59:59"


# ── Phase-40: focused-metric extractor + renderer ────────────
@pytest.mark.parametrize("q,expected", [
    ("Multan City performance on FIRs from 1 April to 20 April 2026", ["firs"]),
    ("Multan City performance about FIRs from 1 April to 20 April 2026", ["firs"]),
    ("Lahore Cantt FIR details", ["firs"]),
    ("Multan City performance on FIRs and sealed", ["firs", "sealed"]),
    ("Lahore performance for warnings", ["warnings"]),
    ("Multan City paid challan details", ["paid_challans"]),
    ("Multan City fine recovered for last week", ["fine_recovered"]),
    ("Lahore performance",                            []),  # generic
    ("Multan City performance from 1 Apr to 20 Apr",  []),  # generic
    ("Multan City warnings",                          []),  # no trigger word
])
def test_extract_requested_metrics(q, expected):
    from inspection_lookup import extract_requested_metrics
    assert extract_requested_metrics(q) == expected


def test_focused_renderer_fir_only():
    from inspection_lookup import render_focused_metric_summary
    metrics = {
        "total_inspections": 7400, "total_challans": 415,
        "fine_amount": 2828000, "sealed": 19, "warnings": 625,
        "paid_challans": 264, "unpaid_challans": 151,
        "firs": 0, "epo": 0, "removal_orders": 0, "no_offenses": 6338,
    }
    officers = [
        {"officerName": "Mubbashir Quyyam", "fir": 0, "inspection": 2557},
        {"officerName": "Asif Hussain", "fir": 0, "inspection": 1703},
    ]
    out = render_focused_metric_summary(
        metrics, ["firs"], "Multan City",
        date_range=(date(2026, 4, 1), date(2026, 4, 20)),
        source="SDEO Dashboard (Live)",
        officer_rows=officers,
    )
    assert "### FIRs Registered Performance — Multan City" in out
    assert "| FIRs Registered | 0 |" in out
    # Officer column-table
    assert "| Officer | FIRs Registered |" in out
    assert "| Mubbashir Quyyam | 0 |" in out
    # Must NOT include unrelated full-dashboard KPIs
    assert "Total Inspections" not in out
    assert "Fine Amount" not in out
    assert "Paid Challans" not in out
    # Note about zero FIRs
    assert "no firs registered" in out.lower()


def test_focused_renderer_multiple_metrics():
    from inspection_lookup import render_focused_metric_summary
    metrics = {"firs": 0, "sealed": 19}
    out = render_focused_metric_summary(
        metrics, ["firs", "sealed"], "Multan City",
        date_range=(date(2026, 4, 1), date(2026, 4, 20)),
    )
    assert "FIRs Registered and Sealed Premises Performance" in out
    assert "| FIRs Registered | 0 |" in out
    assert "| Sealed Premises | 19 |" in out


def test_full_dashboard_section_headings():
    from inspection_lookup import render_dashboard_summary
    metrics = {
        "total_inspections": 7400, "total_challans": 415,
        "fine_amount": 2828000, "sealed": 19,
        "enforcer": 4, "warnings": 625,
        "paid_challans": 264, "unpaid_challans": 151,
        "paid_amount": 1713500, "outstanding_amount": 1114500,
        "fine_recovered": 1713500, "firs": 0, "epo": 0,
        "removal_orders": 0, "no_offenses": 6338,
    }
    officers = [
        {"officerName": "Mubbashir Quyyam", "inspection": 2557,
         "challan": 190, "warning": 347, "sealed": 7, "fir": 0},
    ]
    out = render_dashboard_summary(
        metrics, "Multan City",
        date_range=(date(2026, 4, 1), date(2026, 4, 20)),
        officer_rows=officers,
    )
    assert "#### Key KPI Cards" in out
    assert "#### Financial Breakdown" in out
    assert "#### Enforcement Breakdown" in out
    assert "#### Officer Breakdown" in out
    assert "| Officer | Inspections | Challans | Warnings | Sealed |" in out
    assert "| Mubbashir Quyyam | 2,557 | 190 | 347 | 7 |" in out


def test_get_officer_metric_value():
    from inspection_lookup import get_officer_metric_value
    o = {"officerName": "X", "fir": 3, "challan": 50, "fineAmount": 12000}
    assert get_officer_metric_value(o, "firs") == 3
    assert get_officer_metric_value(o, "total_challans") == 50
    assert get_officer_metric_value(o, "fine_amount") == 12000
    assert get_officer_metric_value(o, "sealed") is None


def test_get_officer_metric_value_distinguishes_zero_from_missing():
    """Real zero must return 0, missing key must return None."""
    from inspection_lookup import get_officer_metric_value
    # Real zeros — value present
    o_zero = {"officerName": "X", "firCase": 0, "fineAmount": 0}
    assert get_officer_metric_value(o_zero, "firs") == 0
    assert get_officer_metric_value(o_zero, "fine_amount") == 0
    # Missing fields → None (NOT 0)
    o_missing = {"officerName": "Y", "challan": 5}
    assert get_officer_metric_value(o_missing, "fine_amount") is None
    assert get_officer_metric_value(o_missing, "firs") is None


# ── Officer breakdown policy: financial metrics ──────────────
def test_focused_financial_no_officer_table_when_unavailable():
    """When officer_rows lack any financial value, the financial
    breakdown table must be omitted and an unavailable note added."""
    from inspection_lookup import render_focused_metric_summary
    metrics = {"fine_amount": 2828000}
    officers = [
        {"officerName": "A", "challan": 190, "inspection": 2557},
        {"officerName": "B", "challan": 138, "inspection": 2072},
    ]
    out = render_focused_metric_summary(
        metrics, ["fine_amount"], "Multan City",
        date_range=(date(2026, 4, 1), date(2026, 4, 20)),
        officer_rows=officers,
    )
    # No fake Rs. 0 rows
    assert "| A | Rs. 0 |" not in out
    assert "| B | Rs. 0 |" not in out
    # Unavailable note present
    assert "Not available from the SDEO dashboard" in out
    # Title uses tighter form (no "Performance" suffix)
    assert "### Fine Imposed — Multan City" in out


def test_focused_financial_renders_when_available():
    from inspection_lookup import render_focused_metric_summary
    metrics = {"fine_amount": 12000}
    officers = [
        {"officerName": "A", "fineAmount": 7000},
        {"officerName": "B", "fineAmount": 5000},
    ]
    out = render_focused_metric_summary(
        metrics, ["fine_amount"], "Multan City",
        officer_rows=officers,
    )
    assert "Officer Financial Breakdown" in out
    assert "| A | Rs. 7,000 |" in out
    assert "| B | Rs. 5,000 |" in out
    assert "Not available" not in out


def test_focused_activity_renders_zeroes_when_metric_present():
    """For activity metrics, zero values are real and should render."""
    from inspection_lookup import render_focused_metric_summary
    metrics = {"firs": 0}
    officers = [
        {"officerName": "A", "fir": 0},
        {"officerName": "B", "fir": 0},
    ]
    out = render_focused_metric_summary(
        metrics, ["firs"], "Multan City",
        officer_rows=officers,
    )
    # FIR column has values (real zeros), so table renders
    assert "| A | 0 |" in out
    assert "| B | 0 |" in out
    # No "Not available" note for activity metrics that are present
    assert "Not available" not in out


def test_focused_money_title_no_performance_suffix():
    from inspection_lookup import render_focused_metric_summary
    out = render_focused_metric_summary(
        {"paid_amount": 100}, ["paid_amount"], "Multan City",
    )
    assert "### Paid Amount — Multan City" in out
    assert "Performance" not in out


# ── Phase-43: officer financial breakdown via dedicated endpoint ──
def test_focused_financial_with_pcm_officer_source():
    """When officer_source='Pcm/officer-inspection-details', render
    the 4-col Officer Financial Breakdown with Challans + Fine Imposed
    + Paid Recovery %."""
    from inspection_lookup import render_focused_metric_summary
    metrics = {"fine_amount": 2828000}
    officers = [
        {"officerName": "Mubbashir Quyyam", "challan": 185,
         "fineAmount": 1453000, "paid_recovery_pct": 70.81},
        {"officerName": "Asif Hussain (EO-248)", "challan": 47,
         "fineAmount": 424500, "paid_recovery_pct": 63.83},
    ]
    out = render_focused_metric_summary(
        metrics, ["fine_amount"], "Multan City",
        date_range=(date(2026, 4, 1), date(2026, 4, 20)),
        officer_rows=officers,
        officer_source="Pcm/officer-inspection-details",
    )
    assert "Officer Financial Breakdown" in out
    assert "officer-inspection-details" in out.lower()
    assert "| Officer | Challans | Fine Imposed | Paid Recovery % |" in out
    assert "| Mubbashir Quyyam | 185 | Rs. 1,453,000 | 70.81% |" in out
    assert "| Asif Hussain (EO-248) | 47 | Rs. 424,500 | 63.83% |" in out
    # Reconciliation note (officer sum 1,877,500 ≠ KPI 2,828,000)
    assert "Reconciliation note" in out


def test_focused_financial_pcm_no_reconciliation_when_match():
    """When officer sum equals KPI, no reconciliation note appears."""
    from inspection_lookup import render_focused_metric_summary
    metrics = {"fine_amount": 1500}
    officers = [
        {"officerName": "A", "challan": 5, "fineAmount": 1000,
         "paid_recovery_pct": 50.0},
        {"officerName": "B", "challan": 2, "fineAmount": 500,
         "paid_recovery_pct": 0.0},
    ]
    out = render_focused_metric_summary(
        metrics, ["fine_amount"], "X",
        officer_rows=officers,
        officer_source="Pcm/officer-inspection-details",
    )
    assert "Reconciliation note" not in out


def test_pcm_officer_source_unavailable_recovery_renders_dash():
    from inspection_lookup import render_focused_metric_summary
    metrics = {"fine_amount": 1000}
    officers = [
        {"officerName": "A", "challan": 5, "fineAmount": 1000,
         "paid_recovery_pct": None},
    ]
    out = render_focused_metric_summary(
        metrics, ["fine_amount"], "X",
        officer_rows=officers,
        officer_source="Pcm/officer-inspection-details",
    )
    # Recovery % renders as "—" when None
    assert "| A | 5 | Rs. 1,000 | — |" in out


def test_officer_financial_fetcher_helper(monkeypatch):
    """Direct test of fetch_officer_financial_breakdown shape."""
    from inspection_lookup import fetch_officer_financial_breakdown
    import inspection_lookup as il
    fake_resp_data = [
        {"fullName": "Mubbashir Quyyam", "totalChallans": 185,
         "totalPaidChallans": 131, "totalUnPaidChallans": 54,
         "fineAmount": 1453000.0, "paidChallanAmount": 969000.0,
         "unPaidChallanAmount": 484000.0},
    ]

    class _Resp:
        def raise_for_status(self): pass
        def json(self): return fake_resp_data

    def fake_get(url, params=None, headers=None, timeout=None):
        return _Resp()

    monkeypatch.setattr(il.requests, "get", fake_get)
    rows = fetch_officer_financial_breakdown(457, date(2026, 4, 1),
                                             date(2026, 4, 20))
    assert len(rows) == 1
    r = rows[0]
    assert r["officer_name"] == "Mubbashir Quyyam"
    assert r["challans"] == 185
    assert r["fine_imposed"] == 1453000.0
    assert r["fine_recovered"] == 969000.0
    assert r["outstanding_amount"] == 484000.0
    # Recovery = paid challan count / total challan count
    assert abs(r["paid_recovery_pct"] - 70.81) < 0.01
    assert r["source"] == "Pcm/officer-inspection-details"


def test_renderer_money_format():
    metrics = {"total_inspections": 1, "total_challans": 0, "fine_amount": 1234567,
               "sealed": 0, "arrest": 0, "enforcer": 0, "warnings": 0,
               "paid_challans": 0, "unpaid_challans": 0}
    out = render_dashboard_summary(metrics, "X")
    assert "Rs. 1,234,567" in out


def test_renderer_no_broken_table():
    metrics = {"total_inspections": 1}
    out = render_dashboard_summary(metrics, "X")
    # Header row + spaced separator must precede any data row
    assert "| KPI | Value |" in out
    assert "| --- | ---: |" in out


def test_renderer_blank_line_before_table():
    metrics = {"total_inspections": 1, "total_challans": 0}
    out = render_dashboard_summary(metrics, "X",
                                   source="SDEO Dashboard (Live)")
    # The two newlines before the table header are required for the
    # frontend markdown parser to recognise the table.
    assert "\n\n| KPI | Value |\n| --- | ---: |" in out


# ── Challan-specific phrasing routes elsewhere ────────────────
def test_challan_specific_phrasing_routes_to_challan_path():
    """'paid challan amount in Multan' should route to a structured
    intent (either inspection ranking, inspection focused, or challan).
    Phase-41 widened inspection's domain keywords to include "paid
    amount" so the canonical financial dashboard handles it. Both
    outcomes are acceptable — the important property is that the
    detector does NOT return None."""
    intent = detect_inspection_intent("paid challan amount in Multan")
    assert intent is not None
