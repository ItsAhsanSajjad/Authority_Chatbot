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
    assert "may not cover all aspects" in result


def test_support_state_wording_conflicting():
    """Conflicting state should mention differing provisions."""
    from answerer import _apply_support_state_wording
    result = _apply_support_state_wording("The salary is 50,000 PKR.", "conflicting")
    assert "50,000" in result
    assert "differing details" in result


def test_support_state_wording_unsupported():
    """Unsupported state should note limited evidence but not refuse."""
    from answerer import _apply_support_state_wording
    result = _apply_support_state_wording("Some related info...", "unsupported")
    assert "Some related info" in result
    assert "do not directly address" in result
    # Must NOT refuse
    assert "I don't know" not in result
    assert "I cannot" not in result
