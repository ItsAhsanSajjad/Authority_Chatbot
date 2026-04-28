"""Tests for answerer support-state classification and wording."""
import pytest


def test_support_state_wording_supported():
    """Supported state should return clean answer without disclaimers."""
    from answerer import _apply_support_state_wording
    result = _apply_support_state_wording("The salary is 50,000 PKR.", "supported")
    assert "50,000" in result
    assert "Note:" not in result


def test_support_state_wording_partially_supported():
    """Partially supported should add qualification note."""
    from answerer import _apply_support_state_wording
    result = _apply_support_state_wording("The salary is 50,000 PKR.", "partially_supported")
    assert "50,000" in result
    assert "Note:" in result
    # Updated wording (Phase B port): the new engine phrases the partial
    # qualification as "not stated as a single standalone clause".
    assert "not stated as a single standalone clause" in result


def test_support_state_wording_conflicting():
    """Conflicting state should mention differing provisions."""
    from answerer import _apply_support_state_wording
    result = _apply_support_state_wording("The salary is 50,000 PKR.", "conflicting")
    assert "50,000" in result
    # Updated wording (Phase B port): "differing details" → "may differ on this matter".
    assert "may differ on this matter" in result


def test_support_state_wording_unsupported():
    """Unsupported state is no longer decorated by _apply_support_state_wording —
    the caller now substitutes the entire answer text. This function returns
    the input unchanged for that state. The test only enforces that no
    refusal phrasing leaks into the helper's output."""
    from answerer import _apply_support_state_wording
    result = _apply_support_state_wording("Some related info...", "unsupported")
    assert "Some related info" in result
    # Must NOT refuse
    assert "I don't know" not in result
    assert "I cannot" not in result
