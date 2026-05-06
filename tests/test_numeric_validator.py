"""Tests for numeric_validator."""
from __future__ import annotations

import pytest

from numeric_validator import (
    extract_numbers,
    extract_numbers_from_payload,
    validate_numeric_answer,
    NumericValidationResult,
)


def test_extract_numbers_simple():
    nums = extract_numbers("Total inspections: 1,889 and Rs. 4,042,000 fine.")
    norm = sorted(extract_numbers_from_payload(
        "Total inspections: 1,889 and Rs. 4,042,000 fine."))
    assert "1889" in norm
    assert "4042000" in norm


def test_validate_passes_when_all_numbers_present():
    payload = (
        "Tehsil A: 1,889 inspections, Rs. 4,042,000 fine.\n"
        "Tehsil B: 100 inspections, Rs. 50,000 fine."
    )
    answer = "Tehsil A leads with 1,889 inspections and Rs. 4,042,000."
    res = validate_numeric_answer(answer, payload, evidence_type="ranking")
    assert res.ok, res.reason


def test_validate_fails_on_hallucinated_number():
    payload = "Tehsil A: 1,889 inspections."
    answer = "Tehsil A leads with 9,999 inspections."  # 9999 not in payload
    res = validate_numeric_answer(answer, payload, evidence_type="ranking")
    assert not res.ok
    assert any("9,999" in n or "9999" in n.replace(",", "")
               for n in res.unsupported_numbers)


def test_validate_ignores_small_integers():
    payload = "Tehsil A had 1,889 inspections."
    # rank "1" and "5" are free integers; 1889 is the only meaningful
    answer = "Rank 1 of 5: Tehsil A with 1,889 inspections."
    res = validate_numeric_answer(answer, payload, evidence_type="ranking")
    assert res.ok


def test_validate_ignores_dates():
    payload = "Tehsil A: 1,889 inspections."
    answer = "Tehsil A on 2026-01-01 had 1,889 inspections."
    res = validate_numeric_answer(answer, payload, evidence_type="ranking")
    assert res.ok


def test_validate_permissive_skips_strict():
    payload = "Some narrative text without specific numbers."
    answer = "PERA Section 12 covers something with a fine of Rs. 50,000."
    res = validate_numeric_answer(answer, payload, permissive=True)
    assert res.ok


def test_validate_empty_answer():
    res = validate_numeric_answer("", "anything")
    assert res.ok


def test_extract_numbers_with_currency():
    nums = extract_numbers("Total fine Rs. 6,094,900 was imposed.")
    assert any("6,094,900" in n for n in nums)


def test_extract_numbers_with_thousands_separators():
    payload_set = extract_numbers_from_payload(
        "Inspections: 1 889 and 4,042,000 fine."
    )
    assert "1889" in payload_set
    assert "4042000" in payload_set
