"""Tests for structured_state — LastTurn parsing and follow-up merge."""
from __future__ import annotations

from datetime import date

import pytest

from structured_state import (
    LastTurn,
    parse_intent_to_last_turn,
    merge_followup,
    last_turn_to_intent,
)


# ── parse_intent_to_last_turn ────────────────────────────────
def test_parse_top_intent():
    lt = parse_intent_to_last_turn("insp_top:firs:tehsil:desc")
    assert lt.domain == "inspection"
    assert lt.metric == "firs"
    assert lt.entity_level == "tehsil"
    assert lt.rank_order == "desc"
    assert lt.amount_or_count == "count"


def test_parse_top_amount_intent():
    lt = parse_intent_to_last_turn("insp_top:paid_amount:district:desc")
    assert lt.metric == "paid_amount"
    assert lt.amount_or_count == "amount"
    assert lt.status_filter == "paid"


def test_parse_division_intent():
    lt = parse_intent_to_last_turn("insp_division:Lahore")
    assert lt.entity_level == "division"
    assert lt.entity_value == "Lahore"


def test_parse_officer_intent():
    lt = parse_intent_to_last_turn("insp_officer:Rabia Altaf")
    assert lt.entity_level == "officer"
    assert lt.officer == "Rabia Altaf"


def test_parse_with_dates():
    lt = parse_intent_to_last_turn(
        "insp_district:Lahore",
        date_start=date(2026, 1, 1),
        date_end=date(2026, 1, 10),
    )
    assert lt.date_start == "2026-01-01"
    assert lt.date_end == "2026-01-10"


# ── merge_followup — six required scenarios ─────────────────
def test_followup_metric_swap():
    lt = parse_intent_to_last_turn(
        "insp_top:firs:tehsil:desc",
        date_start=date(2026, 1, 1), date_end=date(2026, 1, 10),
    )
    lt.entity_value = "Lahore"
    lt.entity_level = "division"  # ranking inside division
    out = merge_followup("and by challans?", lt)
    assert out.metric == "challans"
    assert out.date_start == "2026-01-01"
    assert out.date_end == "2026-01-10"


def test_followup_status_paid_to_unpaid():
    lt = parse_intent_to_last_turn("insp_top:paid_amount:tehsil:desc")
    lt.entity_value = "Multan"
    out = merge_followup("and unpaid?", lt)
    assert out.status_filter == "unpaid"
    assert out.metric == "unpaid_amount"
    assert out.entity_value == "Multan"


def test_followup_order_flip_to_asc():
    lt = parse_intent_to_last_turn("insp_top:fine_amount:tehsil:desc")
    out = merge_followup("ascending order", lt)
    assert out.rank_order == "asc"


def test_followup_inherits_metric_when_not_mentioned():
    lt = parse_intent_to_last_turn("insp_top:challans:tehsil:desc")
    lt.entity_value = "Ravi Town"
    out = merge_followup("for last month", lt)
    assert out.metric == "challans"  # untouched
    assert out.entity_value == "Ravi Town"


def test_followup_amount_qualifier():
    lt = parse_intent_to_last_turn("insp_top:paid_challans:tehsil:desc")
    out = merge_followup("by amount", lt)
    assert out.amount_or_count == "amount"
    assert out.metric == "paid_amount"


def test_followup_document_domain_untouched():
    lt = LastTurn(domain="document", intent="doc:appointment")
    out = merge_followup("and salary?", lt)
    # Document follow-ups should not gain structured fields
    assert out.domain == "document"


# ── last_turn_to_intent ──────────────────────────────────────
def test_re_encode_top():
    lt = LastTurn(
        domain="inspection", metric="firs",
        entity_level="tehsil", rank_order="desc",
    )
    assert last_turn_to_intent(lt) == "insp_top:firs:tehsil:desc"


def test_re_encode_district():
    lt = LastTurn(
        domain="inspection",
        entity_level="district", entity_value="Lahore",
    )
    assert last_turn_to_intent(lt) == "insp_district:Lahore"


def test_re_encode_challan_location():
    lt = LastTurn(
        domain="challan",
        entity_level="tehsil", entity_value="Shalimar",
    )
    assert last_turn_to_intent(lt) == "challan_location:tehsil:Shalimar"


def test_re_encode_document_returns_none():
    lt = LastTurn(domain="document")
    assert last_turn_to_intent(lt) is None


# ── Round-trip serialisation ─────────────────────────────────
def test_serialise_round_trip():
    lt = LastTurn(
        domain="inspection", intent="insp_top:firs:tehsil:desc",
        metric="firs", entity_level="tehsil", rank_order="desc",
        date_start="2026-01-01", date_end="2026-01-10",
    )
    d = lt.to_dict()
    assert isinstance(d, dict)
    lt2 = LastTurn.from_dict(d)
    assert lt2.metric == "firs"
    assert lt2.date_start == "2026-01-01"


def test_from_dict_with_unknown_fields():
    """Older payloads with extra fields shouldn't crash."""
    lt = LastTurn.from_dict({"metric": "challans", "extra_unknown": "x"})
    assert lt.metric == "challans"
