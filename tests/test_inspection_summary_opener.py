"""
Tests for the All-Divisions inspection summary opener (Phase: UI polish).

Verifies that the LLM-context formatter produces:
  • The "PERA has recorded ... regulatory inspection actions" sentence
    so the model echoes a professional opener verbatim
  • Safe int coercion: None / "" / non-numeric values render as 0 and
    never raise (avoids producing broken numbers like "914,84,0" or
    crashing the summary endpoint)
  • Total Regulatory Inspection Actions label (no longer the awkward
    "Total Inspections/Actions" slash form)

These tests stub the DB layer; no real Postgres connection required.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, List

import inspection_lookup


class _StubDB:
    """Minimal db.fetch_all stub returning canned rows."""

    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = rows

    def fetch_all(self, *_args, **_kwargs):
        return self._rows


def _make_rows(values):
    """Build inspection_performance rows for each division."""
    rows = []
    for div, v in values.items():
        rows.append(
            {
                "division_name": div,
                "level": "division",
                "total_actions": v.get("total_actions"),
                "challans": v.get("challans"),
                "firs": v.get("firs"),
                "warnings": v.get("warnings"),
                "no_offenses": v.get("no_offenses"),
                "sealed": v.get("sealed"),
                "removal_order": v.get("removal_order"),
                "epo": v.get("epo"),
                "arrest_count": v.get("arrest_count"),
                "pcm_count": v.get("pcm_count"),
                "fine_imposed": v.get("fine_imposed"),
                "fine_recovered": v.get("fine_recovered"),
                "fine_outstanding": v.get("fine_outstanding"),
                "paid_count": v.get("paid_count"),
                "unpaid_count": v.get("unpaid_count"),
                "snapshot_date": "2026-05-14",
            }
        )
    return rows


def test_summary_opener_present_with_clean_total():
    rows = _make_rows(
        {
            "Lahore": {"total_actions": 265272, "challans": 15138, "warnings": 28881, "no_offenses": 200000, "sealed": 300, "firs": 60},
            "Multan": {"total_actions": 50000, "challans": 4000, "warnings": 8000, "no_offenses": 30000, "sealed": 40, "firs": 5},
            "Rawalpindi": {"total_actions": 40000, "challans": 3000, "warnings": 7000, "no_offenses": 25000, "sealed": 20, "firs": 3},
        }
    )
    db = _StubDB(rows)
    result = inspection_lookup._query_insp_summary(db, None, None)
    ctx = result["formatted_context"]

    # Total = 265272 + 50000 + 40000 = 355272
    assert "355,272" in ctx
    assert "PERA has recorded 355,272 regulatory inspection actions across Punjab" in ctx
    # New table-led format — totals are stated in the opener line, then
    # the full markdown breakdown table follows directly.
    assert "Division-wise Inspection Breakdown:" in ctx
    # Old awkward label gone
    assert "Total Inspections/Actions" not in ctx


def test_summary_handles_missing_and_none_values():
    """Rows with None / missing fields must not blow up the formatter."""
    rows = _make_rows(
        {
            "Lahore": {"total_actions": None, "challans": None, "warnings": None},
            "Multan": {},  # all fields missing
            "Rawalpindi": {"total_actions": "not-a-number", "challans": 100},
        }
    )
    db = _StubDB(rows)
    # Must not raise.
    result = inspection_lookup._query_insp_summary(db, None, None)
    ctx = result["formatted_context"]
    # Total = 0 + 0 + 0 = 0 (string coerces to 0)
    assert "PERA has recorded 0 regulatory inspection actions" in ctx
    # New table format: every cell renders as 0
    assert "| Lahore | 0 | 0 | 0 | 0 | 0 | 0 |" in ctx
    assert "| Multan | 0 | 0 | 0 | 0 | 0 | 0 |" in ctx
    # No raw "None" leakage
    assert "None" not in ctx
    # No malformed comma groups like 914,84,0
    import re
    # Allowed: 0, 100, 1,234 — but never 1,23 or 12,3 or 1,2,3
    for token in re.findall(r"\b\d{1,3}(?:,\d+)+\b", ctx):
        groups = token.split(",")
        # All non-first groups must be exactly 3 digits.
        assert all(len(g) == 3 for g in groups[1:]), (
            f"Malformed grouped number leaked: {token!r}"
        )


def test_summary_no_division_unknown_label_leak():
    """A row with missing division_name should render as 'Unknown', not crash."""
    rows = _make_rows({"Lahore": {"total_actions": 100}})
    rows.append({"division_name": None, "level": "division", "total_actions": 50, "snapshot_date": "2026-05-14"})
    db = _StubDB(rows)
    result = inspection_lookup._query_insp_summary(db, None, None)
    ctx = result["formatted_context"]
    assert "Unknown" in ctx


def test_division_wise_table_has_all_seven_columns():
    """Division-wise breakdown must emit a 7-column markdown table:
    Division | Inspections | Challans | FIRs | Warnings | No Offenses | Sealed.
    """
    rows = _make_rows(
        {
            "Lahore": {
                "total_actions": 265272, "challans": 15138, "firs": 68,
                "warnings": 28881, "no_offenses": 200000, "sealed": 326,
            },
            "Multan": {
                "total_actions": 50000, "challans": 4000, "firs": 5,
                "warnings": 8000, "no_offenses": 30000, "sealed": 40,
            },
        }
    )
    db = _StubDB(rows)
    ctx = inspection_lookup._query_insp_summary(db, None, None)["formatted_context"]

    # Heading uses the official wording.
    assert "Division-wise Inspection Breakdown:" in ctx

    # Markdown table header has all 7 columns.
    expected_header = "| Division | Inspections | Challans | FIRs | Warnings | No Offenses | Sealed |"
    assert expected_header in ctx

    # Markdown separator row uses right-alignment for all numeric columns.
    assert "|---|---:|---:|---:|---:|---:|---:|" in ctx

    # Each division row must contain its FIRs, No Offenses, and Sealed values.
    assert "| Lahore | 265,272 | 15,138 | 68 | 28,881 | 200,000 | 326 |" in ctx
    assert "| Multan | 50,000 | 4,000 | 5 | 8,000 | 30,000 | 40 |" in ctx


def test_district_wise_table_has_all_seven_columns(monkeypatch):
    """District-wise (child rows under a division) must emit the same 7-column
    table with `District` as the row header."""
    parent_row = {
        "level": "division",
        "division_name": "Lahore",
        "total_actions": 311500, "challans": 17960, "firs": 68,
        "warnings": 30620, "no_offenses": 262000, "sealed": 368,
        "snapshot_date": "2026-05-14",
    }
    district_rows = [
        {
            "level": "district", "district_name": "Kasur",
            "total_actions": 36127, "challans": 1414, "firs": 0,
            "warnings": 937, "no_offenses": 33775, "sealed": 1,
            "snapshot_date": "2026-05-14",
        },
        {
            "level": "district", "district_name": "Lahore",
            "total_actions": 265272, "challans": 15138, "firs": 68,
            "warnings": 28881, "no_offenses": 220858, "sealed": 326,
            "snapshot_date": "2026-05-14",
        },
    ]

    class _LocStubDB:
        def fetch_all(self, sql, params):
            level = (params[0] if params else "").lower()
            if level == "division":
                return [parent_row]
            if level == "district":
                return district_rows
            return []

        def fetch_one(self, *_a, **_kw):
            return None

    ctx = inspection_lookup._query_insp_location(
        _LocStubDB(), "division", "Lahore", None, None, ""
    )["formatted_context"]

    assert "District-wise Inspection Breakdown:" in ctx
    assert "| District | Inspections | Challans | FIRs | Warnings | No Offenses | Sealed |" in ctx
    assert "|---|---:|---:|---:|---:|---:|---:|" in ctx
    assert "| Kasur | 36,127 | 1,414 | 0 | 937 | 33,775 | 1 |" in ctx
    assert "| Lahore | 265,272 | 15,138 | 68 | 28,881 | 220,858 | 326 |" in ctx


def test_breakdown_tables_tolerate_missing_fields():
    """Missing FIRs / No Offenses / Sealed values must render as 0, never crash."""
    rows = _make_rows(
        {
            # Only total_actions present; everything else missing.
            "Lahore": {"total_actions": 100000},
            "Multan": {},
        }
    )
    db = _StubDB(rows)
    ctx = inspection_lookup._query_insp_summary(db, None, None)["formatted_context"]
    # Every numeric column must render as 0 when its value is missing.
    assert "| Lahore | 100,000 | 0 | 0 | 0 | 0 | 0 |" in ctx
    assert "| Multan | 0 | 0 | 0 | 0 | 0 | 0 |" in ctx
    # No `None` strings should leak.
    assert "None" not in ctx


def test_division_wise_breakdown_query_routes_to_insp_summary():
    """The exact prompt 'Division-wise inspection breakdown' must route to
    the All-Divisions summary handler."""
    intent = inspection_lookup.detect_inspection_intent(
        "Division-wise inspection breakdown"
    )
    assert intent == "insp_summary"

    # The wider variants higher-authority officers might type.
    for variant in (
        "division-wise inspection breakdown",
        "Division wise inspection breakdown",
        "Inspection breakdown division-wise",
        "Tell me the division wise breakdown",
        "PERA division wise breakdown",
    ):
        intent = inspection_lookup.detect_inspection_intent(variant)
        assert intent == "insp_summary", (
            f"Expected insp_summary for {variant!r}, got {intent!r}"
        )


def test_no_malformed_grouped_numbers_in_formatted_summary():
    """No grouped number in the formatted summary may have an off-by-grouping
    sequence like 914,84,0 or 31,46 — every non-first group must be exactly
    three digits."""
    import re

    rows = _make_rows(
        {
            "Lahore": {"total_actions": 265272, "challans": 15138, "warnings": 28881, "no_offenses": 200000, "sealed": 300, "firs": 60},
            "Multan": {"total_actions": 91484, "challans": 4000, "warnings": 8000, "no_offenses": 30000, "sealed": 40, "firs": 5},
            "Rawalpindi": {"total_actions": 3146, "challans": 3000, "warnings": 7000, "no_offenses": 25000, "sealed": 20, "firs": 3},
            "Bahawalpur": {"total_actions": 1075917, "challans": 1, "warnings": 1, "no_offenses": 1, "sealed": 1, "firs": 1},
        }
    )
    db = _StubDB(rows)
    result = inspection_lookup._query_insp_summary(db, None, None)
    ctx = result["formatted_context"]

    # Find every comma-grouped number, assert it is well-formed.
    for token in re.findall(r"\b\d{1,3}(?:,\d+)+\b", ctx):
        groups = token.split(",")
        # First group: 1-3 digits. Every subsequent group: exactly 3.
        assert 1 <= len(groups[0]) <= 3, f"first group too long in {token!r}"
        assert all(len(g) == 3 for g in groups[1:]), (
            f"Malformed grouped number leaked into context: {token!r}"
        )

    # Spot-check some specific values render the way we expect.
    assert "91,484" in ctx          # would have looked like 91,48,4 if buggy
    assert "3,146" in ctx           # would have looked like 31,46 if buggy
    assert "1,075,917" in ctx       # would have looked like 10,75,917 (Indian) if buggy
