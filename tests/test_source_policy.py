"""Tests for pera_source_policy + top-N limit + records gate."""
from __future__ import annotations

import pytest

from pera_source_policy import (
    is_operational_query,
    should_prefer_live_sdeo,
    extract_limit,
    user_wants_records,
)


# ── is_operational_query ─────────────────────────────────────
@pytest.mark.parametrize("q,expected", [
    ("Multan City performance from 1 April to 20 April", True),
    ("Multan City fine amount", True),
    ("top officers by challans", True),
    ("FIR count last week", True),
    ("paid amount in Multan", True),
    ("inspection summary", True),
    ("what is PERA Act section 12", False),
    ("salary of director general", False),
    ("hello",                         False),
])
def test_is_operational_query(q, expected):
    assert is_operational_query(q) == expected


# ── should_prefer_live_sdeo ──────────────────────────────────
def test_should_prefer_live_sdeo_modes():
    assert should_prefer_live_sdeo("anything", "live_api") is True
    assert should_prefer_live_sdeo("Multan City fine amount", "documents") is False
    assert should_prefer_live_sdeo("Multan City fine amount", "stored_api") is False
    # both + operational → True
    assert should_prefer_live_sdeo(
        "Multan City fine amount from 1 April to 20 April", "both"
    ) is True
    # both + non-operational → False
    assert should_prefer_live_sdeo("salary of director general", "both") is False


# ── extract_limit ────────────────────────────────────────────
@pytest.mark.parametrize("q,expected", [
    ("top 5 tehsils",        5),
    ("top 20 officers",      20),
    ("top 100 stations",     25),     # capped
    ("top officers",         10),     # default
    ("show all officers",    25),     # all → max
    ("tell me everyone",     25),
    ("top fifteen tehsils",  15),
    ("top twenty officers",  20),
    ("",                     10),
])
def test_extract_limit(q, expected):
    assert extract_limit(q) == expected


# ── user_wants_records ───────────────────────────────────────
@pytest.mark.parametrize("q,expected", [
    ("show me the records of Mubbashir",         True),
    ("list inspections by officer X",            True),
    ("Mubbashir details from 1 March to 25 March", True),
    ("show inspection cases",                    True),
    ("Mubbashir performance summary",            False),
    ("Mubbashir fine amount",                    False),
    ("",                                         False),
])
def test_user_wants_records(q, expected):
    assert user_wants_records(q) == expected
