"""Tests for unified domain vocabulary (pera_vocab.py)."""
import pytest


def test_abbreviation_map_has_core_entries():
    """Core abbreviations must be present."""
    from pera_vocab import ABBREVIATION_MAP
    assert "cto" in ABBREVIATION_MAP
    assert "dg" in ABBREVIATION_MAP
    assert "sso" in ABBREVIATION_MAP
    assert "hr" in ABBREVIATION_MAP


def test_abbreviation_map_values_are_strings():
    """All values should be non-empty strings."""
    from pera_vocab import ABBREVIATION_MAP
    for key, val in ABBREVIATION_MAP.items():
        assert isinstance(val, str), f"Key '{key}' has non-string value"
        assert len(val) > 0, f"Key '{key}' has empty value"


def test_schedule_map_completeness():
    """All 6 schedule descriptions must be present."""
    from pera_vocab import SCHEDULE_MAP
    for i in ["I", "II", "III", "IV", "V", "VI"]:
        key = f"Schedule-{i}"
        assert key in SCHEDULE_MAP, f"Missing {key}"


def test_full_abbreviation_map_includes_schedules():
    """Merged map should contain both abbreviations and schedules."""
    from pera_vocab import get_full_abbreviation_map
    full = get_full_abbreviation_map()
    assert "cto" in full
    assert "Schedule-III" in full


def test_normalized_map_normalizes_keys():
    """Normalized map should strip hyphens and lowercase."""
    from pera_vocab import get_normalized_abbreviation_map
    normalized = get_normalized_abbreviation_map()
    # "Schedule-I" -> "schedulei"
    assert "schedulei" in normalized
    assert "cto" in normalized


def test_expand_abbreviations():
    """expand_abbreviations should replace known abbreviations."""
    from pera_vocab import expand_abbreviations
    result = expand_abbreviations("What is the CTO salary?")
    assert "Chief Technology Officer" in result
    assert "salary" in result


def test_expand_abbreviations_preserves_unknown():
    """Unknown words should pass through unchanged."""
    from pera_vocab import expand_abbreviations
    result = expand_abbreviations("Hello world xyz")
    assert result == "Hello world xyz"


def test_standalone_role_keywords():
    """Standalone keywords should include all abbreviation keys."""
    from pera_vocab import STANDALONE_ROLE_KEYWORDS, ABBREVIATION_MAP
    for key in ABBREVIATION_MAP:
        assert key in STANDALONE_ROLE_KEYWORDS, f"Abbreviation '{key}' missing from standalone keywords"


def test_expansion_keywords_exist():
    """Expansion keywords set should have salary-related terms."""
    from pera_vocab import EXPANSION_KEYWORDS
    assert "salary" in EXPANSION_KEYWORDS
    assert "pay" in EXPANSION_KEYWORDS
    assert "allowance" in EXPANSION_KEYWORDS


def test_lowercase_abbreviation_map():
    """Lowercase map should have all values lowercased."""
    from pera_vocab import get_lowercase_abbreviation_map
    lc = get_lowercase_abbreviation_map()
    for key, val in lc.items():
        assert val == val.lower(), f"Value for '{key}' is not lowercase: {val}"
