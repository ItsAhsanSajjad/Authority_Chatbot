"""Tests for pera_entities canonical resolver."""
from __future__ import annotations

import pytest

from pera_entities import (
    normalize_entity_text,
    resolve_location,
    resolve_officer,
    has_admin_word,
    canonical_division_name,
)


# ── Normalisation ────────────────────────────────────────────
@pytest.mark.parametrize("inp,expected", [
    ("D.G. Khan",        "d g khan"),
    ("DG  Khan",         "dg khan"),
    ("Dera Ghazi Khan",  "dera ghazi khan"),
    ("  Lahore  ",       "lahore"),
])
def test_normalize(inp, expected):
    assert normalize_entity_text(inp) == expected


# ── D.G. Khan aliases ────────────────────────────────────────
@pytest.mark.parametrize("text", [
    "DG Khan",
    "D G Khan",
    "D.G Khan",
    "D.G. Khan",
    "Dera Ghazi Khan",
    "dera ghazi khan",
    "dg khan division performance",
    "DG Khan division inspection summary",
])
def test_dg_khan_aliases(text):
    canon = canonical_division_name(text)
    assert canon == "D.G. Khan", f"{text!r} -> {canon}"


def test_alias_resolver_returns_division_level():
    e = resolve_location("DG Khan division", level=None)
    assert e is not None
    assert e.canonical_name == "D.G. Khan"
    assert e.level == "division"


# ── Token-subset match for tehsils ───────────────────────────
def test_tehsil_token_subset():
    cands = ["Allama Iqbal Town", "Lahore Cantt", "Model Town"]
    e = resolve_location("allama iqbal sealed inspections",
                         level="tehsil", candidates=cands)
    assert e is not None
    assert e.canonical_name == "Allama Iqbal Town"


def test_tehsil_full_substring_wins():
    cands = ["Allama Iqbal Town", "Lahore Cantt"]
    e = resolve_location("allama iqbal town inspection summary",
                         level="tehsil", candidates=cands)
    assert e is not None
    assert e.canonical_name == "Allama Iqbal Town"
    assert e.score >= 0.90


# ── Officer guard ────────────────────────────────────────────
def test_admin_word_blocks_officer():
    cands = ["Rabia Altaf", "M. Iqbal Khan"]
    e = resolve_officer("Allama Iqbal Town tehsil inspection",
                        candidates=cands)
    assert e is None  # admin word present → officer blocked


def test_officer_resolves_when_no_admin_word():
    cands = ["Rabia Altaf"]
    e = resolve_officer("rabia altaf inspection summary",
                        candidates=cands)
    assert e is not None
    assert e.canonical_name == "Rabia Altaf"


def test_admin_word_detection():
    assert has_admin_word("Lahore tehsil inspection")
    assert has_admin_word("Multan division performance")
    assert has_admin_word("Faisalabad district stats")
    assert has_admin_word("Shalimar station report")
    assert not has_admin_word("Rabia Altaf inspection")


def test_officer_with_force_override():
    cands = ["Rabia Altaf"]
    e = resolve_officer("rabia altaf in tehsil",
                        candidates=cands,
                        allow_when_admin_word=True)
    assert e is not None


# ── No match cases ───────────────────────────────────────────
def test_unknown_location_returns_none():
    assert resolve_location("xyz fictionalland",
                            level="tehsil", candidates=["Lahore Cantt"]) is None


def test_empty_input():
    assert resolve_location("", level=None) is None
    assert resolve_officer("", candidates=["A B"]) is None
