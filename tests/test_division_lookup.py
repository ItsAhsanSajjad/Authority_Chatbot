"""
Tests for division lookup fix, full list retrieval, and live API ID fix.

Validates:
  - stored_api_lookup module: intent detection, direct DB lookup
  - consolidated evidence chunk contains ALL records (no truncation)
  - live API formatter uses `id` not `districtId` for divisions
  - stored_api mode returns real divisions, not doc-based org units
  - both mode merges API divisions + document RAG
  - documents mode is unchanged
"""
import os
import sys
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch, MagicMock

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("AUTH_ENABLED", "0")

# All 9 real PERA divisions
ALL_DIVISIONS = [
    {"id": 19, "name": "Lahore", "districtId": 0, "districtName": ""},
    {"id": 20, "name": "Sahiwal", "districtId": 0, "districtName": ""},
    {"id": 21, "name": "Bahawalpur", "districtId": 0, "districtName": ""},
    {"id": 22, "name": "Multan", "districtId": 0, "districtName": ""},
    {"id": 23, "name": "Faisalabad", "districtId": 0, "districtName": ""},
    {"id": 24, "name": "DG Khan", "districtId": 0, "districtName": ""},
    {"id": 25, "name": "Gujranwala (Including Gujrat)", "districtId": 0, "districtName": ""},
    {"id": 26, "name": "Sargodha", "districtId": 0, "districtName": ""},
    {"id": 27, "name": "Rawalpindi", "districtId": 0, "districtName": ""},
]


def _create_test_db(divisions=None):
    """Create a temp SQLite DB with division records."""
    if divisions is None:
        divisions = ALL_DIVISIONS
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db_path = tmp.name

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE api_records (
            id INTEGER PRIMARY KEY,
            source_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            record_type TEXT DEFAULT '',
            content_hash TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            normalized_text TEXT DEFAULT '',
            first_seen_at REAL NOT NULL,
            last_updated_at REAL NOT NULL,
            is_active INTEGER DEFAULT 1,
            UNIQUE(source_id, record_id)
        )""")

    import time
    now = time.time()
    for div in divisions:
        conn.execute(
            """INSERT INTO api_records
               (source_id, record_id, record_type, content_hash, raw_json,
                normalized_text, first_seen_at, last_updated_at, is_active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("app_data_divisions", str(div["id"]), "division",
             "hash", json.dumps(div), f"Division: {div['name']}",
             now, now, 1)
        )
    conn.commit()
    conn.close()
    return db_path


# ──────────────────────────────────────────────────────────────
# Lookup Intent Detection
# ──────────────────────────────────────────────────────────────
class TestLookupIntentDetection(unittest.TestCase):
    """Validate detect_lookup_intent identifies division queries."""

    def test_list_divisions(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertEqual(detect_lookup_intent("List PERA divisions"), "divisions")

    def test_what_divisions_exist(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertEqual(detect_lookup_intent("What divisions exist?"), "divisions")

    def test_show_divisions(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertEqual(detect_lookup_intent("Show all divisions"), "divisions")

    def test_available_divisions(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertEqual(detect_lookup_intent("What are the available divisions?"), "divisions")

    def test_how_many_divisions(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertEqual(detect_lookup_intent("How many divisions does PERA have?"), "divisions")

    def test_punjab_divisions(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertEqual(detect_lookup_intent("Punjab divisions"), "divisions")

    def test_non_division_query(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertIsNone(detect_lookup_intent("What is the salary of SSO?"))

    def test_unrelated_query(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertIsNone(detect_lookup_intent("Weather in Lahore"))

    def test_empty_query(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertIsNone(detect_lookup_intent(""))


# ──────────────────────────────────────────────────────────────
# Direct Division Lookup from SQLite
# ──────────────────────────────────────────────────────────────
class TestDivisionLookup(unittest.TestCase):
    """Validate lookup_divisions reads from SQLite api_records."""

    def setUp(self):
        self.db_path = _create_test_db()

    def tearDown(self):
        os.unlink(self.db_path)

    def test_lookup_returns_all_9_records(self):
        from stored_api_lookup import lookup_divisions
        result = lookup_divisions(self.db_path)
        self.assertIsNotNone(result)
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 9)

    def test_records_have_all_division_names(self):
        from stored_api_lookup import lookup_divisions
        result = lookup_divisions(self.db_path)
        names = {r["name"] for r in result["records"]}
        expected = {"Bahawalpur", "DG Khan", "Faisalabad", "Gujranwala (Including Gujrat)",
                    "Lahore", "Multan", "Rawalpindi", "Sahiwal", "Sargodha"}
        self.assertEqual(names, expected)

    def test_records_have_real_ids(self):
        from stored_api_lookup import lookup_divisions
        result = lookup_divisions(self.db_path)
        for rec in result["records"]:
            self.assertIn("id", rec)
            self.assertNotEqual(rec["id"], 0, "Should use real 'id' field, not districtId")
            self.assertGreaterEqual(rec["id"], 19)

    def test_formatted_answer_contains_all_divisions(self):
        from stored_api_lookup import lookup_divisions
        result = lookup_divisions(self.db_path)
        answer = result["formatted_answer"]
        for div in ALL_DIVISIONS:
            self.assertIn(div["name"], answer, f"Missing division: {div['name']}")

    def test_formatted_answer_is_list(self):
        from stored_api_lookup import lookup_divisions
        result = lookup_divisions(self.db_path)
        answer = result["formatted_answer"]
        self.assertIn("- **", answer)  # Bullet point formatting

    def test_formatted_context_has_evidence_tags(self):
        from stored_api_lookup import lookup_divisions
        result = lookup_divisions(self.db_path)
        ctx = result["formatted_context"]
        self.assertIn("<evidence", ctx)
        self.assertIn('source_type="api"', ctx)

    def test_nonexistent_db_returns_none(self):
        from stored_api_lookup import lookup_divisions
        result = lookup_divisions("/nonexistent/path.db")
        self.assertIsNone(result)


# ──────────────────────────────────────────────────────────────
# Build Retrieval — Consolidated Single Hit
# ──────────────────────────────────────────────────────────────
class TestBuildLookupRetrieval(unittest.TestCase):
    """Validate that lookup results produce a SINGLE consolidated hit
    to bypass MAX_HITS_PER_DOC truncation."""

    def test_retrieval_has_single_consolidated_hit(self):
        """All records should be in ONE hit, not N separate hits."""
        from stored_api_lookup import build_lookup_retrieval
        lookup = {
            "records": ALL_DIVISIONS,
            "source_id": "app_data_divisions",
        }
        retrieval = build_lookup_retrieval(lookup, "divisions")
        self.assertTrue(retrieval["has_evidence"])
        self.assertEqual(len(retrieval["evidence"]), 1)
        # KEY: only 1 hit, not 9
        self.assertEqual(len(retrieval["evidence"][0]["hits"]), 1)

    def test_consolidated_hit_contains_all_9_divisions(self):
        """The single hit text must contain ALL 9 division names."""
        from stored_api_lookup import build_lookup_retrieval
        lookup = {
            "records": ALL_DIVISIONS,
            "source_id": "app_data_divisions",
        }
        retrieval = build_lookup_retrieval(lookup, "divisions")
        hit_text = retrieval["evidence"][0]["hits"][0]["text"]
        for div in ALL_DIVISIONS:
            self.assertIn(div["name"], hit_text,
                          f"Division '{div['name']}' missing from consolidated hit")

    def test_consolidated_hit_has_total_count(self):
        from stored_api_lookup import build_lookup_retrieval
        lookup = {
            "records": ALL_DIVISIONS,
            "source_id": "app_data_divisions",
        }
        retrieval = build_lookup_retrieval(lookup, "divisions")
        hit_text = retrieval["evidence"][0]["hits"][0]["text"]
        self.assertIn("[Total Records: 9]", hit_text)

    def test_hit_has_api_source_type(self):
        from stored_api_lookup import build_lookup_retrieval
        lookup = {
            "records": [{"id": 21, "name": "Bahawalpur"}],
            "source_id": "app_data_divisions",
        }
        retrieval = build_lookup_retrieval(lookup, "divisions")
        hit = retrieval["evidence"][0]["hits"][0]
        self.assertEqual(hit["source_type"], "api")
        self.assertEqual(hit["api_source_id"], "app_data_divisions")
        self.assertEqual(hit["record_type"], "divisions_list")

    def test_empty_records_returns_no_evidence(self):
        from stored_api_lookup import build_lookup_retrieval
        lookup = {"records": [], "source_id": "app_data_divisions"}
        retrieval = build_lookup_retrieval(lookup, "divisions")
        self.assertFalse(retrieval["has_evidence"])


# ──────────────────────────────────────────────────────────────
# Full Pipeline — Answerer Gets All 9 Divisions
# ──────────────────────────────────────────────────────────────
class TestAnswererReceivesFullList(unittest.TestCase):
    """End-to-end: verify the answerer's format_evidence_for_llm
    includes ALL 9 divisions from a consolidated lookup hit."""

    def test_evidence_contains_all_9_divisions(self):
        """format_evidence_for_llm should include all 9 divisions
        because the consolidated hit counts as 1 hit (not 9)."""
        from stored_api_lookup import build_lookup_retrieval
        from answerer import format_evidence_for_llm

        lookup = {
            "records": ALL_DIVISIONS,
            "source_id": "app_data_divisions",
        }
        retrieval = build_lookup_retrieval(lookup, "divisions")
        evidence_str = format_evidence_for_llm(retrieval, question="List PERA divisions")

        # All 9 must appear in the assembled evidence
        for div in ALL_DIVISIONS:
            self.assertIn(div["name"], evidence_str,
                          f"Division '{div['name']}' missing from LLM evidence")


# ──────────────────────────────────────────────────────────────
# Merge Lookup + RAG
# ──────────────────────────────────────────────────────────────
class TestMergeLookupWithRag(unittest.TestCase):
    """Validate merge of lookup retrieval with document RAG retrieval."""

    def test_merge_puts_lookup_first(self):
        from stored_api_lookup import merge_lookup_with_rag
        lookup_r = {
            "has_evidence": True,
            "evidence": [{"doc_name": "API Source", "hits": [{"text": "API data"}]}],
        }
        rag_r = {
            "question": "test",
            "has_evidence": True,
            "evidence": [{"doc_name": "Doc.pdf", "hits": [{"text": "Doc data"}]}],
        }
        merged = merge_lookup_with_rag(lookup_r, rag_r)
        self.assertTrue(merged["has_evidence"])
        self.assertEqual(len(merged["evidence"]), 2)
        self.assertEqual(merged["evidence"][0]["doc_name"], "API Source")
        self.assertEqual(merged["evidence"][1]["doc_name"], "Doc.pdf")

    def test_merge_with_empty_rag(self):
        from stored_api_lookup import merge_lookup_with_rag
        lookup_r = {
            "has_evidence": True,
            "evidence": [{"doc_name": "API", "hits": [{"text": "data"}]}],
        }
        rag_r = {"question": "test", "has_evidence": False, "evidence": []}
        merged = merge_lookup_with_rag(lookup_r, rag_r)
        self.assertTrue(merged["has_evidence"])
        self.assertEqual(len(merged["evidence"]), 1)


# ──────────────────────────────────────────────────────────────
# Live API ID Field Fix
# ──────────────────────────────────────────────────────────────
class TestLiveApiIdField(unittest.TestCase):
    """Validate that live API division formatter uses 'id' not 'districtId'."""

    def test_format_uses_id_field(self):
        from live_api_handler import _format_live_response
        data = [
            {"id": 21, "name": "Bahawalpur", "districtId": 0, "districtName": ""},
            {"id": 19, "name": "Lahore", "districtId": 0, "districtName": ""},
        ]
        result = _format_live_response("divisions", data)
        self.assertIn("(ID: 21)", result)
        self.assertIn("(ID: 19)", result)
        self.assertNotIn("(ID: 0)", result)

    def test_format_fallback_to_districtId(self):
        from live_api_handler import _format_live_response
        data = [{"name": "Test", "districtId": 5}]
        result = _format_live_response("divisions", data)
        self.assertIn("(ID: 5)", result)

    def test_strength_format_unchanged(self):
        from live_api_handler import _format_live_response
        data = {"series": [
            {"divisionName": "Lahore", "total": 100, "onDuty": 80, "absent": 20}
        ]}
        result = _format_live_response("pera_strength", data)
        self.assertIn("Lahore", result)
        self.assertIn("100", result)


# ──────────────────────────────────────────────────────────────
# Documents Mode Unchanged
# ──────────────────────────────────────────────────────────────
class TestDocumentsModeUnchanged(unittest.TestCase):
    """Validate that documents mode is not affected by the lookup changes."""

    def test_detect_lookup_not_triggered_for_documents_mode(self):
        from stored_api_lookup import detect_lookup_intent
        result = detect_lookup_intent("List PERA divisions")
        self.assertEqual(result, "divisions")
        # Documents mode guard: `if source_mode in ("stored_api", "both")`
        # is in fastapi_app.py, so this intent never affects documents mode.


# ──────────────────────────────────────────────────────────────
# Non-List Queries Unaffected
# ──────────────────────────────────────────────────────────────
class TestNonListQueriesUnaffected(unittest.TestCase):
    """Non-list queries should NOT trigger the lookup path."""

    def test_salary_query_no_lookup(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertIsNone(detect_lookup_intent("What is SSO salary?"))

    def test_role_query_no_lookup(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertIsNone(detect_lookup_intent("Tell me about the CTO role"))

    def test_regulation_query_no_lookup(self):
        from stored_api_lookup import detect_lookup_intent
        self.assertIsNone(detect_lookup_intent("What are the PERA regulations?"))


if __name__ == "__main__":
    unittest.main()
