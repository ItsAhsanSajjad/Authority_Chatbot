"""Tests for migration 38 — SDEO dashboard field expansion."""
from __future__ import annotations

import os
import pytest
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
os.environ.setdefault("ANALYTICS_DB_ENABLED", "1")


def _get_columns(table: str) -> set[str]:
    import psycopg
    conn = psycopg.connect(os.getenv("POSTGRES_URL"))
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name=%s",
                (table,),
            )
            return {r[0] for r in cur.fetchall()}
    finally:
        conn.close()


@pytest.mark.parametrize("table,required_cols", [
    ("inspection_performance", {
        "removal_order", "epo", "arrest_count", "pcm_count",
        "fine_imposed", "fine_recovered", "fine_outstanding",
        "paid_count", "unpaid_count", "partially_paid_count",
        "paid_amount", "unpaid_amount", "partially_paid_amount",
        "extra_metrics", "source_updated_at",
    }),
    ("inspection_officer_summary", {
        "officer_removal_orders", "officer_epo",
        "officer_fine_imposed", "officer_fine_recovered",
        "extra_metrics",
    }),
    ("officer_inspection_detail", {
        "removal_order", "epo", "warning_count", "no_offense_count",
        "paid_amount", "unpaid_amount", "extra_metrics",
    }),
    ("operational_activity", {"extra_metrics"}),
])
def test_migration_38_columns_present(table, required_cols):
    cols = _get_columns(table)
    missing = required_cols - cols
    assert not missing, f"{table}: missing {missing}"


def test_migration_38_idempotent():
    """Running the migration twice must not error."""
    from analytics_migrations import run_migrations_safe
    n = run_migrations_safe()
    # Already applied → 0 new applications
    assert n == 0


def test_transform_summary_captures_new_fields():
    """_transform_summary should pull removalOrder/epo and route
    unknown keys into extra_metrics."""
    from inspection_ingest import _transform_summary
    payload = {
        "divisionName": "Lahore",
        "totalActions": 100,
        "challans": 30,
        "fiRs": 1,
        "warnings": 10,
        "noOffenses": 50,
        "sealed": 2,
        "removalOrder": 4,
        "epo": 1,
        "futureUnknownField": 999,
    }
    out = _transform_summary(payload, "division")
    assert out["removal_order"] == 4
    assert out["epo"] == 1
    assert out["extra_metrics"] == {"futureUnknownField": 999}


def test_transform_summary_missing_fields_safe():
    from inspection_ingest import _transform_summary
    out = _transform_summary({"divisionName": "X"}, "division")
    assert out["removal_order"] == 0
    assert out["epo"] == 0
    # extra_metrics is None when no unknown keys
    assert out["extra_metrics"] is None
