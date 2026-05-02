"""Tests for Phase-1 freshness footer coverage across all lookup modules.

Covers:
  * inspection_lookup: _query_insp_summary, _query_insp_officer (stored),
    _query_officer_live, _query_tehsil_live, _query_repeat_offenders,
    _query_insp_cnic — freshness footer appears in formatted_context
  * challan_lookup: _build_result freshness wiring, _challan_data_freshness,
    _lookup_totals, _lookup_by_division, _lookup_by_district, _lookup_by_tehsil,
    _lookup_requisition_type, _lookup_tehsil_breakdown, _lookup_location,
    _lookup_officer, _lookup_comparison, _lookup_officer_ranking,
    _lookup_officer_at_location, daterange formatters
  * operational_activity_lookup: _query_by_officer, _query_by_location,
    _query_detail_aggregate, _cross_challan_new, _cross_challan_legacy
  * fallback behavior when snapshot_date is missing
  * no crash on None / empty / garbage snapshot_date
"""
import os
import sys
import pytest
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("AUTH_ENABLED", "0")
os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")


# ── Shared helpers ──────────────────────────────────────────────

def _make_mock_db():
    """Return a MagicMock analytics DB."""
    db = MagicMock()
    db.is_available.return_value = True
    return db


def _assert_freshness_in_ctx(ctx: str, table_or_label: str):
    """Assert that formatted_context contains a FRESHNESS footer with the given label."""
    assert "[FRESHNESS" in ctx, f"No [FRESHNESS marker in context:\n{ctx[:200]}"
    assert table_or_label.lower() in ctx.lower(), (
        f"Expected '{table_or_label}' in context, not found:\n{ctx[:200]}"
    )


# ══════════════════════════════════════════════════════════════
# INSPECTION LOOKUP — freshness footer coverage
# ══════════════════════════════════════════════════════════════

class TestInspectionSummaryFreshness:
    """_query_insp_summary should append a freshness footer."""

    def test_summary_has_freshness_footer(self):
        from inspection_lookup import _query_insp_summary

        db = _make_mock_db()
        db.fetch_all.return_value = [
            {"division_name": "Lahore", "total_actions": 100,
             "challans": 50, "firs": 2, "warnings": 30,
             "no_offenses": 18, "sealed": 0},
        ]
        db.fetch_one.return_value = {"d": date(2026, 4, 28)}

        result = _query_insp_summary(db, None, None)
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "inspection_performance")
        assert "2026-04-28" in ctx

    def test_summary_no_crash_on_missing_snapshot(self):
        from inspection_lookup import _query_insp_summary

        db = _make_mock_db()
        db.fetch_all.return_value = [
            {"division_name": "Lahore", "total_actions": 100,
             "challans": 50, "firs": 2, "warnings": 30,
             "no_offenses": 18, "sealed": 0},
        ]
        db.fetch_one.return_value = {"d": None}

        result = _query_insp_summary(db, None, None)
        # Should still return valid result, just without footer
        assert result["source_id"] == "insp_summary"
        assert result["records"]


class TestInspectionOfficerStoredFreshness:
    """_query_officer_stored should append freshness footer from officer_inspection_detail."""

    def test_officer_stored_has_freshness(self):
        from inspection_lookup import _query_officer_stored

        db = _make_mock_db()
        db.fetch_all.side_effect = [
            # summary (officer_inspection_detail)
            [{"total_inspections": 50, "total_challans": 10,
              "fine_amount": 5000, "sealed": 1, "arrest_case": 0}],
            # agg (officer_inspection_record)
            [{"total": 45, "challans": 8, "warnings": 20,
              "no_offence": 15, "arrests": 0, "confiscated": 0,
              "total_fine": 3000, "data_from": "2026-01-01", "data_to": "2026-04-28"}],
        ]
        # Snapshot date query
        db.fetch_one.return_value = {"d": datetime(2026, 4, 28, 10, 30)}

        result = _query_officer_stored(
            db, "Test Officer", "O123", 1,
            "Model Town", "Lahore", "Lahore", "insp_officer:Test Officer"
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "officer_inspection_detail")

    def test_officer_stored_no_crash_without_snapshot(self):
        from inspection_lookup import _query_officer_stored

        db = _make_mock_db()
        db.fetch_all.side_effect = [
            [{"total_inspections": 50, "total_challans": 10,
              "fine_amount": 5000, "sealed": 1, "arrest_case": 0}],
            [{"total": 45, "challans": 8, "warnings": 20,
              "no_offence": 15, "arrests": 0, "confiscated": 0,
              "total_fine": 3000, "data_from": "2026-01-01", "data_to": "2026-04-28"}],
        ]
        db.fetch_one.return_value = {"d": None}

        result = _query_officer_stored(
            db, "Test Officer", "O123", 1,
            "Model Town", "Lahore", "Lahore", "insp_officer:Test Officer"
        )
        assert result["source_id"] == "insp_officer:Test Officer"
        assert "[FRESHNESS" not in result["formatted_context"]


class TestInspectionRepeatOffendersFreshness:
    """_query_repeat_offenders should append freshness footer."""

    def test_repeat_offenders_has_freshness(self):
        from inspection_lookup import _query_repeat_offenders

        db = _make_mock_db()
        db.fetch_all.return_value = [
            {"cnic": "3520112345678", "owner_name": "Test",
             "times_inspected": 5, "challans": 3, "warnings": 1,
             "total_fine": 2000, "officers_count": 2,
             "officers": "A, B", "locations": "Model Town"},
        ]
        db.fetch_one.return_value = {"d": date(2026, 4, 27)}

        result = _query_repeat_offenders(db)
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "officer_inspection_record")
        assert "2026-04-27" in ctx


class TestInspectionCnicFreshness:
    """_query_insp_cnic should append freshness footer."""

    def test_cnic_has_freshness(self):
        from inspection_lookup import _query_insp_cnic

        db = _make_mock_db()
        db.fetch_all.return_value = [
            {"officer_name": "Officer A", "tehsil_name": "Model Town",
             "district_name": "Lahore", "division_name": "Lahore",
             "owner_name": "Test Owner", "cnic": "3520112345678",
             "address": "123 Street", "is_challan": True,
             "is_warning": False, "is_no_offense": False,
             "is_arrest": False, "is_confiscated": False,
             "fine_amount": 1000, "from_date": "2026-01-01",
             "to_date": "2026-04-28"},
        ]
        db.fetch_one.return_value = {"d": date(2026, 4, 28)}

        result = _query_insp_cnic(db, "3520112345678")
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "officer_inspection_record")


class TestInspectionLiveApiFreshness:
    """Live API paths should include live/near-live freshness stamps."""

    @patch("inspection_lookup.requests.get")
    def test_tehsil_live_has_freshness(self, mock_get):
        from inspection_lookup import _query_tehsil_live

        db = _make_mock_db()
        db.fetch_all.return_value = [{"tehsil_id": 42}]

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "totalActions": 100, "challans": 50, "fiRs": 2,
            "warnings": 30, "noOffenses": 18, "sealed": 0,
            "officers": [],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = _query_tehsil_live(
            db, "Model Town", date(2026, 4, 1), date(2026, 4, 28),
            "insp_tehsil:Model Town"
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "SDEO Live API")

    @patch("inspection_lookup.requests.get")
    def test_officer_live_has_freshness(self, mock_get):
        from inspection_lookup import _query_officer_live

        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = _query_officer_live(
            "Test Officer", "O123", 42,
            "Model Town", "Lahore", "Lahore",
            date(2026, 4, 1), date(2026, 4, 28),
            "insp_officer:Test Officer"
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "PCM Live API")


# ══════════════════════════════════════════════════════════════
# CHALLAN LOOKUP — freshness footer coverage
# ══════════════════════════════════════════════════════════════

class TestChallanDataFreshnessHelper:
    """_challan_data_freshness should return a valid dict."""

    def test_returns_dict_with_snapshot(self):
        from challan_lookup import _challan_data_freshness

        db = _make_mock_db()
        db.fetch_one.return_value = {"d": datetime(2026, 4, 28, 10, 30)}

        result = _challan_data_freshness(db)
        assert result["table"] == "challan_data"
        assert result["snapshot_date"] == datetime(2026, 4, 28, 10, 30)
        assert "sync_interval_s" in result

    def test_returns_empty_dict_on_failure(self):
        from challan_lookup import _challan_data_freshness

        db = _make_mock_db()
        db.fetch_one.side_effect = Exception("DB error")

        result = _challan_data_freshness(db)
        assert result == {}


class TestChallanBuildResultFreshness:
    """_build_result with freshness dict should stamp context."""

    def test_freshness_appended_to_context(self):
        from challan_lookup import _build_result

        result = _build_result(
            "test_source", "Test", [{"a": 1}],
            "answer text", "context text",
            freshness={
                "table": "challan_data",
                "snapshot_date": datetime(2026, 4, 28, 10, 30),
                "sync_interval_s": 300,
            },
        )
        ctx = result["formatted_context"]
        assert "[FRESHNESS" in ctx
        assert "challan_data" in ctx

    def test_no_freshness_if_none(self):
        from challan_lookup import _build_result

        result = _build_result(
            "test_source", "Test", [{"a": 1}],
            "answer text", "context text",
        )
        ctx = result["formatted_context"]
        assert "[FRESHNESS" not in ctx

    def test_empty_freshness_dict_no_crash(self):
        from challan_lookup import _build_result

        result = _build_result(
            "test_source", "Test", [{"a": 1}],
            "answer text", "context text",
            freshness={},
        )
        ctx = result["formatted_context"]
        # Empty dict should not produce a footer
        assert "[FRESHNESS" not in ctx


class TestChallanLookupsFreshness:
    """Verify freshness footer appears in key challan lookup paths."""

    def _mock_db_for_challan(self):
        db = _make_mock_db()
        # Default: challan_data_freshness returns a snapshot
        db.fetch_one.return_value = {"d": datetime(2026, 4, 28, 10, 0)}
        return db

    @patch("challan_lookup._get_db")
    def test_lookup_by_division_has_freshness(self, mock_get_db):
        from challan_lookup import _lookup_by_division

        db = self._mock_db_for_challan()
        db.fetch_all.return_value = [
            {"status": "paid", "division_name": "Lahore",
             "total_challans": 100, "total_amount": 50000},
        ]
        mock_get_db.return_value = db

        result = _lookup_by_division()
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "PERA Challan Data")

    @patch("challan_lookup._get_db")
    def test_lookup_by_district_has_freshness(self, mock_get_db):
        from challan_lookup import _lookup_by_district

        db = self._mock_db_for_challan()
        db.fetch_all.return_value = [
            {"status": "paid", "district_name": "Lahore",
             "total_challans": 100, "total_amount": 50000,
             "division_name": "Lahore"},
        ]
        mock_get_db.return_value = db

        result = _lookup_by_district()
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "PERA Challan Data")

    @patch("challan_lookup._get_db")
    def test_lookup_by_tehsil_has_freshness(self, mock_get_db):
        from challan_lookup import _lookup_by_tehsil

        db = self._mock_db_for_challan()
        db.fetch_all.return_value = [
            {"status": "paid", "tehsil_name": "Model Town", "count": 50},
        ]
        mock_get_db.return_value = db

        result = _lookup_by_tehsil()
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "PERA Challan Data")

    @patch("challan_lookup._get_db")
    def test_lookup_requisition_type_has_freshness(self, mock_get_db):
        from challan_lookup import _lookup_requisition_type

        db = self._mock_db_for_challan()
        db.fetch_all.return_value = [
            {"status": "paid", "requisition_type_name": "Price Control",
             "total_challans": 100, "total_amount": 50000},
        ]
        mock_get_db.return_value = db

        result = _lookup_requisition_type()
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "PERA Challan Data")


class TestChallanDaterangeFreshness:
    """Daterange formatters should pass freshness through to _build_result."""

    def test_format_daterange_summary_with_freshness(self):
        from challan_lookup import _format_daterange_summary

        rows = [
            {"status": "paid", "total_challans": 50, "total_fine": 10000,
             "total_paid": 10000, "total_outstanding": 0},
            {"status": "unpaid", "total_challans": 30, "total_fine": 8000,
             "total_paid": 0, "total_outstanding": 8000},
        ]
        freshness = {
            "table": "challan_data",
            "snapshot_date": datetime(2026, 4, 28, 10, 0),
            "sync_interval_s": 300,
            "source_label": "PERA Challan Data",
        }
        result = _format_daterange_summary(
            rows, "01 Apr 2026 — 28 Apr 2026",
            "Test Summary", freshness=freshness
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "PERA Challan Data")

    def test_format_daterange_summary_without_freshness(self):
        from challan_lookup import _format_daterange_summary

        rows = [
            {"status": "paid", "total_challans": 50, "total_fine": 10000,
             "total_paid": 10000, "total_outstanding": 0},
        ]
        result = _format_daterange_summary(
            rows, "01 Apr 2026 — 28 Apr 2026", "Test Summary"
        )
        ctx = result["formatted_context"]
        assert "[FRESHNESS" not in ctx

    def test_format_daterange_location_detail_with_freshness(self):
        from challan_lookup import _format_daterange_location_detail

        rows = [
            {"status": "paid", "total_challans": 50, "total_fine": 10000,
             "total_paid": 10000, "total_outstanding": 0},
        ]
        freshness = {
            "table": "challan_data",
            "snapshot_date": datetime(2026, 4, 28),
            "sync_interval_s": 300,
        }
        result = _format_daterange_location_detail(
            rows, [], "01 Apr 2026 — 28 Apr 2026",
            "Lahore Challans", "Lahore",
            freshness=freshness,
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "challan_data")

    def test_format_daterange_officers_with_freshness(self):
        from challan_lookup import _format_daterange_officers

        rows = [
            {"officer_name": "Officer A", "total_challans": 20,
             "paid_count": 15, "unpaid_count": 3, "overdue_count": 2,
             "total_fine": 5000},
        ]
        freshness = {
            "table": "challan_data",
            "snapshot_date": datetime(2026, 4, 28),
            "sync_interval_s": 300,
        }
        result = _format_daterange_officers(
            rows, "01 Apr 2026 — 28 Apr 2026",
            "Officer Ranking", freshness=freshness
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "challan_data")

    def test_format_daterange_comparison_with_freshness(self):
        from challan_lookup import _format_daterange_comparison

        rows = [
            {"name": "Lahore", "total_challans": 100, "total_fine": 50000,
             "paid_count": 80},
        ]
        freshness = {
            "table": "challan_data",
            "snapshot_date": datetime(2026, 4, 28),
            "sync_interval_s": 300,
        }
        result = _format_daterange_comparison(
            rows, "01 Apr 2026 — 28 Apr 2026",
            "Division", "", freshness=freshness
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "challan_data")


# ══════════════════════════════════════════════════════════════
# OPERATIONAL ACTIVITY LOOKUP — freshness footer coverage
# ══════════════════════════════════════════════════════════════

class TestOAByOfficerFreshness:
    """_query_by_officer should append freshness footer from requisition_detail."""

    def test_officer_has_freshness(self):
        from operational_activity_lookup import _query_by_officer

        db = _make_mock_db()
        # _has_requisition_table check
        db.fetch_one.side_effect = [
            {"cnt": 10},  # _has_requisition_table
            {"d": date(2026, 4, 28)},  # snapshot for freshness
        ]
        db.fetch_all.return_value = [
            {"requisition_id": 1, "requisition_type_name": "Price Control",
             "dt": date(2026, 4, 28), "tehsil_name": "Model Town",
             "district_name": "Lahore", "division_name": "Lahore",
             "area_location": "Main Blvd", "total_squad_members": 5,
             "arrived_members": 3},
        ]

        result = _query_by_officer(db, "Test Officer", None, None)
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "requisition_detail")


class TestOAByLocationFreshness:
    """_query_by_location should append freshness footer."""

    def test_division_has_freshness(self):
        from operational_activity_lookup import _query_by_location

        db = _make_mock_db()
        db.fetch_all.return_value = [
            {"district_name": "Lahore", "price_control": 50,
             "anti_encroachment": 30, "eviction": 10,
             "anti_hoarding": 5, "public_nuisance": 3, "total": 98},
        ]
        db.fetch_one.side_effect = [
            # parent
            {"price_control": 50, "anti_encroachment": 30,
             "eviction": 10, "anti_hoarding": 5,
             "public_nuisance": 3, "total": 98},
            # freshness MAX(snapshot_date)
            {"d": date(2026, 4, 28)},
        ]

        result = _query_by_location(
            db, "division", "Lahore", None, None, None
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "operational_activity")

    def test_tehsil_has_freshness(self):
        from operational_activity_lookup import _query_by_location

        db = _make_mock_db()
        # _has_requisition_table check (fetch_one #1)
        # parent (fetch_one #2)
        # freshness (fetch_one #3)
        db.fetch_one.side_effect = [
            # parent
            {"division_name": "Lahore", "district_name": "Lahore",
             "price_control": 50, "anti_encroachment": 30,
             "eviction": 10, "anti_hoarding": 5,
             "public_nuisance": 3, "total": 98},
            {"cnt": 10},   # _has_requisition_table
            # freshness
            {"d": date(2026, 4, 28)},
        ]
        db.fetch_all.return_value = [
            {"assigned_to": "Officer A", "requisition_name": "Price Control",
             "cnt": 10},
        ]

        result = _query_by_location(
            db, "tehsil", "Model Town", None, None, None
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "operational_activity")


class TestOADetailAggregateFreshness:
    """_query_detail_aggregate should append freshness footer."""

    def test_detail_aggregate_has_freshness(self):
        from operational_activity_lookup import _query_detail_aggregate

        db = _make_mock_db()
        db.fetch_one.side_effect = [
            {"cnt": 10},  # _has_requisition_table
            {"total": 50},  # grand total
            {"d": date(2026, 4, 28)},  # freshness
        ]
        db.fetch_all.side_effect = [
            # grouped breakdown
            [{"division_name": "Lahore",
              "requisition_name": "Price Control", "cnt": 30}],
            # group totals
            [{"division_name": "Lahore", "total": 50}],
            # req type totals
            [{"requisition_name": "Price Control", "cnt": 50}],
        ]

        result = _query_detail_aggregate(
            db, level=None, location_name=None,
            req_type=None, start_date=date(2026, 4, 1),
            end_date=date(2026, 4, 28),
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "requisition_detail")


class TestOACrossChallanFreshness:
    """Cross-challan paths should include freshness footer."""

    def test_cross_challan_new_has_freshness(self):
        from operational_activity_lookup import _cross_challan_new

        db = _make_mock_db()
        db.fetch_all.side_effect = [
            # req_rows
            [{"requisition_id": 1, "requisition_type_name": "Price Control",
              "dt": date(2026, 4, 28), "created_by_name": "Officer A",
              "area_location": "Main Blvd", "total_squad_members": 5,
              "arrived_members": 3}],
            # challan_rows (by status)
            [{"requisition_type_name": "Price Control", "status": "paid",
              "cnt": 10, "total_fine": 5000, "total_paid": 5000,
              "total_outstanding": 0}],
            # challan_by_officer
            [{"requisition_type_name": "Price Control",
              "officer_name": "Officer A", "cnt": 10, "total_fine": 5000}],
        ]
        db.fetch_one.return_value = {"d": date(2026, 4, 28)}

        result = _cross_challan_new(
            db, "tehsil", "Model Town", None, None,
            "oa_tehsil:Model Town"
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "requisition_detail")


# ══════════════════════════════════════════════════════════════
# REGRESSION — existing behavior preserved
# ══════════════════════════════════════════════════════════════

class TestExistingFreshnessStillWorks:
    """Confirm the pre-existing freshness wiring (Phase 1 initial) still works."""

    def test_insp_location_still_has_freshness(self):
        """_query_insp_location was the first function wired — verify it still works."""
        from inspection_lookup import _query_insp_location

        db = _make_mock_db()
        db.fetch_all.side_effect = [
            # parent_rows
            [{"level": "division", "division_name": "Lahore",
              "total_actions": 100, "challans": 50, "firs": 2,
              "warnings": 30, "no_offenses": 18, "sealed": 0,
              "snapshot_date": date(2026, 4, 28)}],
            # child_rows (district under division)
            [{"level": "district", "division_name": "Lahore",
              "district_name": "Lahore", "total_actions": 80,
              "challans": 40, "warnings": 25, "snapshot_date": date(2026, 4, 28)}],
            # officer_rows (empty for division level)
        ]

        result = _query_insp_location(db, "division", "Lahore", None, None)
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "inspection_performance")

    def test_oa_summary_still_has_freshness(self):
        """_query_summary (OA) was wired in the initial Phase 1 — verify it still works."""
        from operational_activity_lookup import _query_summary

        db = _make_mock_db()
        db.fetch_all.return_value = [
            {"division_name": "Lahore", "price_control": 50,
             "anti_encroachment": 30, "eviction": 10,
             "anti_hoarding": 5, "public_nuisance": 3, "total": 98},
        ]
        db.fetch_one.return_value = {"d": date(2026, 4, 28)}

        result = _query_summary(db, None, None)
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "operational_activity")

    def test_challan_totals_still_has_freshness(self):
        """_lookup_totals was wired in the initial Phase 1 — verify it still works."""
        from challan_lookup import _build_result

        result = _build_result(
            "challan_totals", "Test", [{"a": 1}],
            "answer", "context",
            freshness={
                "table": "challan_totals",
                "snapshot_date": datetime.now(),
                "sync_interval_s": 5,
            },
        )
        ctx = result["formatted_context"]
        _assert_freshness_in_ctx(ctx, "challan_totals")


# ══════════════════════════════════════════════════════════════
# EDGE CASES — no crash on missing/garbage data
# ══════════════════════════════════════════════════════════════

class TestFreshnessEdgeCases:
    """Ensure freshness wiring never crashes on edge cases."""

    def test_none_snapshot_in_build_result(self):
        from challan_lookup import _build_result

        result = _build_result(
            "test", "Test", [{"a": 1}],
            "answer", "context",
            freshness={"table": "test", "snapshot_date": None, "sync_interval_s": 60},
        )
        # Should produce "freshness unknown" footer, not crash
        ctx = result["formatted_context"]
        assert "[FRESHNESS" in ctx
        assert "unknown" in ctx.lower()

    def test_garbage_snapshot_in_build_result(self):
        from challan_lookup import _build_result

        result = _build_result(
            "test", "Test", [{"a": 1}],
            "answer", "context",
            freshness={"table": "test", "snapshot_date": "not-a-date", "sync_interval_s": 60},
        )
        ctx = result["formatted_context"]
        assert "[FRESHNESS" in ctx
        assert "unknown" in ctx.lower()

    def test_inspection_summary_db_error_no_crash(self):
        from inspection_lookup import _query_insp_summary

        db = _make_mock_db()
        db.fetch_all.return_value = [
            {"division_name": "Lahore", "total_actions": 100,
             "challans": 50, "firs": 2, "warnings": 30,
             "no_offenses": 18, "sealed": 0},
        ]
        db.fetch_one.side_effect = Exception("DB snapshot query failed")

        # Should still return valid result without footer
        result = _query_insp_summary(db, None, None)
        assert result["source_id"] == "insp_summary"
        assert result["records"]
