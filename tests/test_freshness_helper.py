"""Tests for freshness_helper — Phase 1 patch.

Covers:
  * compute_tier threshold behavior + edge cases
  * format_freshness_footer output shape with various snapshot inputs
  * format_int_indian + format_money_pkr edge cases
  * Helper never raises on garbage input
"""
from datetime import date, datetime, timedelta
import pytest

from freshness_helper import (
    compute_tier,
    format_freshness_footer,
    format_int_indian,
    format_money_pkr,
)


# ── compute_tier ─────────────────────────────────────────────────────────

class TestComputeTier:
    def test_live_under_one_minute(self):
        assert compute_tier(0) == "live"
        assert compute_tier(30) == "live"
        assert compute_tier(60) == "live"

    def test_near_live_under_one_hour(self):
        assert compute_tier(61) == "near-live"
        assert compute_tier(1800) == "near-live"
        assert compute_tier(3600) == "near-live"

    def test_hourly_under_four_hours(self):
        assert compute_tier(3601) == "hourly"
        assert compute_tier(7200) == "hourly"
        assert compute_tier(4 * 3600) == "hourly"

    def test_daily_under_two_days(self):
        assert compute_tier(4 * 3600 + 1) == "daily"
        assert compute_tier(86400) == "daily"
        assert compute_tier(2 * 86400) == "daily"

    def test_stale_over_two_days(self):
        assert compute_tier(2 * 86400 + 1) == "stale"
        assert compute_tier(10 * 86400) == "stale"

    def test_unknown_for_none(self):
        assert compute_tier(None) == "unknown"

    def test_unknown_for_negative(self):
        # Future-dated snapshot — suspicious, treat as unknown
        assert compute_tier(-100) == "unknown"

    def test_unknown_for_unparseable(self):
        assert compute_tier("not-a-number") == "unknown"

    def test_sync_interval_sharpens_stale_detection(self):
        # 2 h old when scheduler runs every 30 min = 4× expected = stale
        # (2 h old > 3 × 30 min = 90 min, AND 2 h > _TIER_HOURLY_MAX_S? No,
        #  2h = 7200 == _TIER_HOURLY_MAX_S so it's still hourly without
        #  the sharpening — but with sharpening should become stale.)
        # Pick a value clearly past hourly: 5 h old, sync 30 min → stale
        assert compute_tier(5 * 3600, sync_interval_s=30 * 60) == "stale"

    def test_sync_interval_does_not_demote_fresh(self):
        # 30 s old + sync interval of 5 s — still well within hourly,
        # but more than 3× sync interval. Code only escalates if absolute
        # age is also past hourly_max — so this should remain "live".
        assert compute_tier(30, sync_interval_s=5) == "live"


# ── format_freshness_footer ──────────────────────────────────────────────

class TestFreshnessFooter:
    def test_starts_with_marker_and_table(self):
        footer = format_freshness_footer(
            "challan_totals", snapshot_date=datetime.now(), sync_interval_s=5
        )
        assert footer.startswith("[FRESHNESS — challan_totals:")
        assert footer.endswith("]")

    def test_live_includes_as_of_with_hh_mm(self):
        footer = format_freshness_footer(
            "challan_totals", snapshot_date=datetime.now(), sync_interval_s=5
        )
        assert "live" in footer
        assert "as of" in footer
        # Should contain a colon for HH:MM
        assert ":" in footer.split("as of")[1]

    def test_stale_includes_snapshot_from(self):
        old_date = date(2026, 4, 20)
        footer = format_freshness_footer(
            "inspection_performance", snapshot_date=old_date
        )
        assert "stale" in footer
        assert "snapshot from" in footer
        assert "2026-04-20" in footer

    def test_none_snapshot_returns_unknown(self):
        footer = format_freshness_footer("requisition_detail", snapshot_date=None)
        assert "freshness unknown" in footer
        assert "requisition_detail" in footer

    def test_empty_string_snapshot_returns_unknown(self):
        footer = format_freshness_footer("foo", snapshot_date="")
        assert "freshness unknown" in footer

    def test_garbage_snapshot_does_not_raise(self):
        footer = format_freshness_footer("foo", snapshot_date="not-a-date")
        assert "freshness unknown" in footer
        assert "foo" in footer

    def test_iso_string_snapshot_parsed(self):
        footer = format_freshness_footer(
            "x", snapshot_date="2026-04-20", sync_interval_s=2 * 3600
        )
        assert "2026-04-20" in footer

    def test_unix_timestamp_accepted(self):
        ts = (datetime.now() - timedelta(seconds=30)).timestamp()
        footer = format_freshness_footer("x", snapshot_date=ts, sync_interval_s=5)
        # 30 s old → live
        assert "live" in footer

    def test_source_label_overrides_table_name(self):
        footer = format_freshness_footer(
            "internal_table",
            snapshot_date=datetime.now(),
            source_label="PERA Challan Totals",
        )
        assert "PERA Challan Totals" in footer
        assert "internal_table" not in footer


# ── format_int_indian ─────────────────────────────────────────────────────

class TestFormatIntIndian:
    @pytest.mark.parametrize("value,expected", [
        (0, "0"),
        (1, "1"),
        (999, "999"),
        (1000, "1,000"),
        (12345, "12,345"),
        (123456, "1,23,456"),
        (1234567, "12,34,567"),
        (12345678, "1,23,45,678"),
        (-1234567, "-12,34,567"),
    ])
    def test_grouping(self, value, expected):
        assert format_int_indian(value) == expected

    def test_none_returns_dash(self):
        assert format_int_indian(None) == "—"

    def test_blank_string_returns_dash(self):
        assert format_int_indian("") == "—"
        assert format_int_indian("   ") == "—"

    def test_unparseable_returns_dash(self):
        assert format_int_indian("abc") == "—"

    def test_string_with_commas_accepted(self):
        assert format_int_indian("1,234,567") == "12,34,567"

    def test_float_truncated_to_int(self):
        assert format_int_indian(1234.7) == "1,234"


# ── format_money_pkr ─────────────────────────────────────────────────────

class TestFormatMoneyPkr:
    def test_basic_grouping(self):
        assert format_money_pkr(1234567) == "Rs. 12,34,567"

    def test_zero(self):
        assert format_money_pkr(0) == "Rs. 0"

    def test_none_shows_dash(self):
        assert format_money_pkr(None) == "Rs. —"
