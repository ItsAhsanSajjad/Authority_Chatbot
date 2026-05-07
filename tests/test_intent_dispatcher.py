"""Tests for the confidence-scored intent dispatcher diagnostics."""
from __future__ import annotations

import os
import pytest
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
os.environ.setdefault("ANALYTICS_DB_ENABLED", "1")

from stored_api_lookup import (
    detect_lookup_intent,
    get_last_intent_diagnostics,
    detect_lookup_candidates,
    choose_best_candidate,
)


def test_diagnostics_populated_after_detect():
    intent = detect_lookup_intent("inspections summary lahore division")
    assert intent is not None
    diag = get_last_intent_diagnostics()
    assert diag.get("chosen_intent") == intent
    assert isinstance(diag.get("intent_confidence"), float)
    assert diag.get("intent_confidence") > 0
    assert "matched_signals" in diag


def test_diagnostics_empty_when_no_match():
    intent = detect_lookup_intent("hello world tell me a joke")
    assert intent is None
    diag = get_last_intent_diagnostics()
    assert diag == {} or diag.get("chosen_intent") in (None, "")


def test_candidates_sorted_desc():
    cands = detect_lookup_candidates("inspections summary lahore division")
    if len(cands) >= 2:
        assert cands[0].score >= cands[1].score


def test_choose_best_handles_empty():
    assert choose_best_candidate([]) is None


def test_diagnostics_contain_expected_signals():
    detect_lookup_intent("top tehsils by FIRs Lahore division Jan 2026")
    diag = get_last_intent_diagnostics()
    sigs = diag.get("matched_signals") or []
    # Should have rank + level + metric signals at minimum
    assert "rank_keyword" in sigs or "metric_keyword" in sigs
