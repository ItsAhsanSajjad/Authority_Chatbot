"""Tests for pera_financial_sources + label-aware numeric validator."""
from __future__ import annotations

from decimal import Decimal

import pytest

from pera_financial_sources import (
    AMOUNT_SOURCES, parse_money_safe, format_money,
    is_all_time_source, normalize_sdeo_financial_metrics,
)
from numeric_validator import (
    validate_numeric_answer, validate_label_value_pairs,
    extract_markdown_label_value_pairs,
)


# ── parse_money_safe ─────────────────────────────────────────
@pytest.mark.parametrize("inp,expected", [
    (None, None),
    ("",   None),
    ("1,713,500",   Decimal("1713500")),
    ("1713500",     Decimal("1713500")),
    ("Rs. 2,828,000", Decimal("2828000")),
    (1713500,        Decimal("1713500")),
    (1713500.0,      Decimal("1713500.0")),
    ("garbage",      None),
])
def test_parse_money_safe(inp, expected):
    assert parse_money_safe(inp) == expected


def test_format_money():
    assert format_money(2828000) == "Rs. 2,828,000"
    assert format_money(None) == "Rs. 0"
    assert format_money("1,713,500") == "Rs. 1,713,500"


# ── source map / all-time guard ─────────────────────────────
def test_is_all_time_source():
    assert is_all_time_source("all_time_total_fine") is True
    assert is_all_time_source("fine_imposed") is False
    assert is_all_time_source("paid_amount") is False


def test_amount_sources_keys():
    # Required canonical keys exist
    for k in ("fine_imposed", "fine_recovered", "paid_amount",
              "outstanding_amount", "all_time_total_fine"):
        assert k in AMOUNT_SOURCES


# ── normalize_sdeo_financial_metrics ─────────────────────────
def test_normalize_routes_topkpis_and_status():
    summary = {"totalActions": 7400, "challans": 415}
    top = {
        "totalFineImposed": 2828000.0,
        "totalFineRecovered": 1713500.0,
        "unpaidFineAmount": 1114500.0,
    }
    status = {
        "paidCount": 264, "unpaidCount": 151,
        "paidAmount": 1713500.0, "unpaidAmount": 1114500.0,
        "partiallyPaidAmount": 0.0,
    }
    out = normalize_sdeo_financial_metrics(
        summary_resp=summary, top_kpis_resp=top, status_resp=status,
        date_ranged=True,
    )
    assert out["fine_imposed"] == Decimal("2828000.0")
    assert out["fine_recovered"] == Decimal("1713500.0")
    assert out["paid_amount"] == Decimal("1713500.0")
    assert out["outstanding_amount"] == Decimal("1114500.0")
    assert out["paid_challans"] == 264
    assert out["unpaid_challans"] == 151


def test_normalize_excludes_pcm_when_date_ranged():
    pcm = {
        "totalFineAmount": 17057500.0,
        "paidChallanAmount": 9571000.0,
        "unPaidChallanAmount": 7486500.0,
    }
    out = normalize_sdeo_financial_metrics(pcm_resp=pcm, date_ranged=True)
    assert out["fine_imposed"] is None
    assert out["paid_amount"] is None
    assert out["outstanding_amount"] is None
    # All-time only under all_time_* keys
    assert out["all_time_total_fine"] == Decimal("17057500.0")
    assert out["all_time_paid_amount"] == Decimal("9571000.0")
    assert out["all_time_unpaid_amount"] == Decimal("7486500.0")


def test_normalize_includes_pcm_when_not_date_ranged():
    pcm = {
        "totalFineAmount": 17057500.0,
        "paidChallanAmount": 9571000.0,
        "unPaidChallanAmount": 7486500.0,
    }
    out = normalize_sdeo_financial_metrics(pcm_resp=pcm, date_ranged=False)
    assert out["fine_imposed"] == Decimal("17057500.0")
    assert out["paid_amount"] == Decimal("9571000.0")


# ── label-aware validator ───────────────────────────────────
def test_extract_label_value_pairs():
    md = (
        "| KPI | Value |\n"
        "| --- | ---: |\n"
        "| Fine Imposed | Rs. 2,828,000 |\n"
        "| Paid Amount | Rs. 1,713,500 |\n"
    )
    pairs = extract_markdown_label_value_pairs(md)
    labels = [p[0] for p in pairs]
    assert "fine imposed" in labels
    assert "paid amount" in labels


def test_label_swap_detected():
    payload = (
        "| KPI | Value |\n| --- | ---: |\n"
        "| Fine Imposed | Rs. 2,828,000 |\n"
        "| Paid Amount | Rs. 1,713,500 |\n"
    )
    answer = (
        "| KPI | Value |\n| --- | ---: |\n"
        "| Fine Imposed | Rs. 1,713,500 |\n"   # swapped value
    )
    mismatches = validate_label_value_pairs(answer, payload)
    assert len(mismatches) == 1
    assert "fine imposed" in mismatches[0]


def test_label_pairs_match_passes():
    payload = (
        "| KPI | Value |\n| --- | ---: |\n"
        "| Fine Imposed | Rs. 2,828,000 |\n"
    )
    answer = (
        "| KPI | Value |\n| --- | ---: |\n"
        "| Fine Imposed | Rs. 2,828,000 |\n"
    )
    assert validate_label_value_pairs(answer, payload) == []


def test_label_alias_normalised():
    # Old "Total Fine Amount" label aliased to canonical "fine imposed"
    payload = (
        "| KPI | Value |\n| --- | ---: |\n"
        "| Fine Imposed | Rs. 2,828,000 |\n"
    )
    answer = (
        "| KPI | Value |\n| --- | ---: |\n"
        "| Total Fine Amount | Rs. 2,828,000 |\n"
    )
    # Should match via alias map
    assert validate_label_value_pairs(answer, payload) == []


def test_validate_numeric_answer_fails_on_swap():
    payload = (
        "| Fine Imposed | Rs. 2,828,000 |\n"
        "| Paid Amount | Rs. 1,713,500 |\n"
    )
    bad = "| Fine Imposed | Rs. 1,713,500 |"
    res = validate_numeric_answer(bad, payload, evidence_type="ranking")
    assert not res.ok
    assert res.label_mismatches
