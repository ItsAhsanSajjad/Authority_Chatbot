"""Tests for Phase-1 response-quality patch.

Covers:
  * No-evidence fallback uses the new template (closest-available + next-step)
  * No-evidence fallback accepts an optional "closest_available" splice
  * System prompt includes table-formatting and freshness rules
  * inspection_lookup formatted_context emits a freshness footer
"""
import os
import sys
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("AUTH_ENABLED", "0")
os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")


# ── Refusal / no-evidence template ───────────────────────────────────────

class TestNoEvidenceFallback:
    def test_documents_template_includes_next_step(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message("documents")
        assert "Next step" in msg or "next step" in msg.lower()

    def test_documents_template_explicitly_refuses_to_fabricate(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message("documents").lower()
        # New template must contain the assurance against fabrication
        assert "fabricate" in msg or "invent" in msg or "estimate" in msg

    def test_stored_api_template_mentions_next_step(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message("stored_api")
        assert "Next step" in msg
        assert "stored" in msg.lower() or "operational" in msg.lower()

    def test_both_template_includes_next_step(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message("both")
        assert "Next step" in msg

    def test_unknown_mode_falls_back_to_both(self):
        from answerer import _get_no_evidence_message
        msg_unknown = _get_no_evidence_message("totally_invalid")
        msg_both = _get_no_evidence_message("both")
        assert msg_unknown == msg_both

    def test_closest_available_is_spliced_in(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message(
            "stored_api",
            closest_available="Total challans across all divisions: 9,75,580",
        )
        assert "Closest available" in msg
        assert "9,75,580" in msg
        # Splice must come BEFORE Next step
        assert msg.index("Closest available") < msg.index("Next step")

    def test_closest_available_optional(self):
        from answerer import _get_no_evidence_message
        # Calling without it must still work
        msg = _get_no_evidence_message("stored_api")
        assert "Closest available" not in msg
        assert "Next step" in msg


# ── System prompt content ────────────────────────────────────────────────

class TestSystemPromptRules:
    """We can't easily call answer_question without a working DB, so we
    test the prompt construction by reading the source file directly.
    This is a coarse but effective check that the new rules made it in."""

    @pytest.fixture(scope="class")
    def answerer_source(self):
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "answerer.py",
        )
        with open(path, encoding="utf-8") as f:
            return f.read()

    def test_prompt_mentions_markdown_table_for_ranked(self, answerer_source):
        # Phase-1 rule 18
        assert "Markdown table" in answerer_source
        assert "ranking" in answerer_source.lower() or "ranked" in answerer_source.lower()

    def test_prompt_mentions_top_list_compare(self, answerer_source):
        # Rule 18 keyword coverage
        for kw in ("top-N", "list", "compare", "breakdown"):
            assert kw.lower() in answerer_source.lower(), f"{kw} missing from prompt"

    def test_prompt_mentions_freshness_marker(self, answerer_source):
        # Rule 20 — answerer must surface [FRESHNESS — ...] from context
        assert "[FRESHNESS" in answerer_source

    def test_prompt_forbids_estimating_unsupported_metrics(self, answerer_source):
        # Rule 21
        assert "Do NOT estimate" in answerer_source or "do not estimate" in answerer_source.lower()
        assert "ratio" in answerer_source.lower()

    def test_prompt_handles_snapshot_mismatch(self, answerer_source):
        # Rule 22
        assert "different freshness stamps" in answerer_source.lower() or "snapshot mismatch" in answerer_source.lower()


# ── Inspection lookup emits the freshness footer ─────────────────────────

class TestInspectionLookupFooter:
    def test_query_insp_location_appends_footer_when_snapshot_present(self):
        """Mock the DB so we can drive _query_insp_location into the path
        where it has parent_rows with a snapshot_date — the freshness
        footer should then appear in formatted_context."""
        from datetime import date
        from inspection_lookup import _query_insp_location

        mock_db = MagicMock()
        # First fetch_all: parent_rows
        mock_db.fetch_all.side_effect = [
            # parent_rows for tehsil
            [{
                "level": "tehsil",
                "tehsil_name": "Multan City",
                "total_actions": 26297,
                "challans": 2909,
                "warnings": 6048,
                "no_offenses": 17290,
                "sealed": 35,
                "firs": 1,
                "snapshot_date": date(2026, 4, 28),
            }],
            # child_rows (none for tehsil)
            [],
            # officer_rows
            [],
        ]

        result = _query_insp_location(
            mock_db, level="tehsil", name="Multan City",
            start_date=None, end_date=None,
        )
        ctx = result.get("formatted_context", "")
        assert "[FRESHNESS — inspection_performance:" in ctx
        assert "2026-04-28" in ctx
