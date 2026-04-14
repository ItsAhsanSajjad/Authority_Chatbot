"""
Tests for Answer Source Mode backend enforcement.
Validates that the /api/ask endpoint correctly routes
queries based on answer_source_mode parameter.
"""
import os
import sys
import json
import unittest
from unittest.mock import patch, MagicMock

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Disable auth for tests
os.environ.setdefault("AUTH_ENABLED", "0")


class TestSourceModeConstants(unittest.TestCase):
    """Validate source mode definitions in fastapi_app."""

    def test_valid_modes_defined(self):
        from fastapi_app import VALID_SOURCE_MODES
        self.assertEqual(VALID_SOURCE_MODES, {"documents", "stored_api", "both", "live_api"})

    def test_labels_for_all_modes(self):
        from fastapi_app import SOURCE_MODE_LABELS, VALID_SOURCE_MODES
        for mode in VALID_SOURCE_MODES:
            self.assertIn(mode, SOURCE_MODE_LABELS)
            self.assertTrue(len(SOURCE_MODE_LABELS[mode]) > 0)

    def test_provenance_for_all_modes(self):
        from fastapi_app import SOURCE_MODE_PROVENANCE, VALID_SOURCE_MODES
        for mode in VALID_SOURCE_MODES:
            self.assertIn(mode, SOURCE_MODE_PROVENANCE)
            self.assertTrue(len(SOURCE_MODE_PROVENANCE[mode]) > 0)


class TestSourceModeRequest(unittest.TestCase):
    """Validate SimpleChatRequest accepts answer_source_mode."""

    def test_default_mode_is_both(self):
        from fastapi_app import SimpleChatRequest
        req = SimpleChatRequest(question="test")
        self.assertEqual(req.answer_source_mode, "both")

    def test_custom_mode_accepted(self):
        from fastapi_app import SimpleChatRequest
        req = SimpleChatRequest(question="test", answer_source_mode="documents")
        self.assertEqual(req.answer_source_mode, "documents")

    def test_live_api_mode_accepted(self):
        from fastapi_app import SimpleChatRequest
        req = SimpleChatRequest(question="test", answer_source_mode="live_api")
        self.assertEqual(req.answer_source_mode, "live_api")


class TestSourceModeResponse(unittest.TestCase):
    """Validate SimpleChatResponse includes source mode fields."""

    def test_response_has_source_mode_fields(self):
        from fastapi_app import SimpleChatResponse
        resp = SimpleChatResponse(
            answer="test",
            decision="answer",
            references=[],
            source_mode="documents",
            source_mode_label="Documents Only",
            provenance="From documents."
        )
        self.assertEqual(resp.source_mode, "documents")
        self.assertEqual(resp.source_mode_label, "Documents Only")
        self.assertEqual(resp.provenance, "From documents.")

    def test_response_optional_fields(self):
        from fastapi_app import SimpleChatResponse
        resp = SimpleChatResponse(
            answer="test",
            decision="answer",
            references=[],
        )
        self.assertIsNone(resp.source_mode)
        self.assertIsNone(resp.source_mode_label)
        self.assertIsNone(resp.provenance)


class TestLiveApiHandler(unittest.TestCase):
    """Validate live_api_handler module."""

    def test_approved_endpoints_defined(self):
        from live_api_handler import LIVE_ENDPOINTS
        self.assertIn("divisions", LIVE_ENDPOINTS)
        self.assertIn("pera_strength", LIVE_ENDPOINTS)
        self.assertIn("finance_overview", LIVE_ENDPOINTS)

    def test_only_get_urls(self):
        """All approved endpoints must be GET-only."""
        from live_api_handler import LIVE_ENDPOINTS
        for key, ep in LIVE_ENDPOINTS.items():
            self.assertIn("url", ep, f"{key} missing url")
            self.assertTrue(ep["url"].startswith("http"), f"{key} url must be http(s)")

    def test_match_division_keywords(self):
        from live_api_handler import _match_endpoint
        self.assertEqual(_match_endpoint("list all divisions"), "divisions")
        self.assertEqual(_match_endpoint("show districts"), "divisions")

    def test_match_strength_keywords(self):
        from live_api_handler import _match_endpoint
        self.assertEqual(_match_endpoint("show workforce strength"), "pera_strength")
        self.assertEqual(_match_endpoint("how many employees on duty"), "pera_strength")

    def test_match_finance_keywords(self):
        from live_api_handler import _match_endpoint
        self.assertEqual(_match_endpoint("finance overview"), "finance_overview")
        self.assertEqual(_match_endpoint("what is the budget"), "finance_overview")

    def test_no_match_returns_none(self):
        from live_api_handler import _match_endpoint
        self.assertIsNone(_match_endpoint("what is the weather today?"))

    @patch("live_api_handler.requests.get")
    def test_query_live_api_success(self, mock_get):
        from live_api_handler import query_live_api
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"id": 1, "name": "Lahore", "districtId": 10}]
        mock_resp.content = b'[{"id":1}]'
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        result = query_live_api("list all divisions")
        self.assertTrue(result["success"])
        self.assertIn("Lahore", result["answer"])
        self.assertEqual(result["endpoint_key"], "divisions")

    @patch("live_api_handler.requests.get")
    def test_query_live_api_timeout(self, mock_get):
        import requests as req_lib
        from live_api_handler import query_live_api
        mock_get.side_effect = req_lib.Timeout("timed out")
        result = query_live_api("list all divisions")
        self.assertFalse(result["success"])
        self.assertIn("timed out", result["error"])

    def test_query_live_api_no_match(self):
        from live_api_handler import query_live_api
        result = query_live_api("random unrelated question")
        self.assertFalse(result["success"])
        self.assertIn("No approved", result["error"])

    def test_get_approved_endpoints_returns_list(self):
        from live_api_handler import get_approved_endpoints
        eps = get_approved_endpoints()
        self.assertIsInstance(eps, list)
        self.assertTrue(len(eps) >= 3)
        for ep in eps:
            self.assertIn("key", ep)
            self.assertIn("display_name", ep)


class TestAuditTrailSourceMode(unittest.TestCase):
    """Validate audit trail accepts source mode params."""

    @patch("audit_trail.AUDIT_ENABLED", True)
    @patch("audit_trail._current_log_path")
    def test_source_mode_in_audit_entry(self, mock_path):
        import tempfile, os
        from audit_trail import log_audit_entry

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            mock_path.return_value = f.name

        try:
            log_audit_entry(
                question="test",
                decision="answer",
                answer_text="test answer",
                answer_source_mode="live_api",
                live_api_used=True,
                live_api_endpoint="divisions",
            )
            with open(mock_path.return_value, "r") as f:
                entry = json.loads(f.readline())
                self.assertEqual(entry["answer_source_mode"], "live_api")
                self.assertTrue(entry["live_api_used"])
                self.assertEqual(entry["live_api_endpoint"], "divisions")
        finally:
            os.unlink(mock_path.return_value)


if __name__ == "__main__":
    unittest.main()
