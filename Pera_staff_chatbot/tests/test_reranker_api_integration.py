"""Tests for reranker compatibility with API evidence chunks."""
import pytest

from reranker import rerank_hits, lexical_overlap


class TestRerankerApiCompatibility:
    """Verify reranker scores API hits without errors or broken ordering."""

    def test_api_hit_scoring(self):
        """API hit should be scoreable without errors."""
        hits = [{
            "text": "Employee Ahmed Khan, department HR, position Senior Officer",
            "search_text": "DOC: API\nTYPE: api\nEmployee Ahmed Khan",
            "score": 0.78,
            "doc_authority": 2,
            "source_type": "api",
            "api_source_id": "pera_employees",
            "record_id": "EMP-001",
        }]
        result = rerank_hits("Ahmed Khan", hits)
        assert len(result) == 1
        assert "_blend" in result[0]
        assert result[0]["_blend"] > 0

    def test_mixed_doc_api_hits(self):
        """Mixed doc+API hits should be ranked sensibly."""
        hits = [
            {
                "text": "The Director General shall appoint all officers",
                "search_text": "PERA Act appointment powers",
                "score": 0.85,
                "doc_authority": 3,
                "source_type": "pdf",
            },
            {
                "text": "Employee: Ahmed Khan, Department: Enforcement, Appointment: 2025-01-15",
                "search_text": "DOC: API\nEmployee Ahmed Khan appointment",
                "score": 0.72,
                "doc_authority": 2,
                "source_type": "api",
                "api_source_id": "pera_employees",
                "record_id": "EMP-001",
            },
        ]
        result = rerank_hits("appointment", hits)
        assert len(result) == 2
        # Both should have blend scores
        assert all("_blend" in h for h in result)

    def test_authority_handling_for_api(self):
        """API hits with authority=2 should not be penalized vs authority=1."""
        hits = [
            {"text": "Working paper draft", "score": 0.80, "doc_authority": 1, "source_type": "pdf"},
            {"text": "API record data", "score": 0.75, "doc_authority": 2, "source_type": "api"},
        ]
        result = rerank_hits("data", hits)
        # authority=1 should be penalized when authority>=2 exists
        low_auth = [h for h in result if h["doc_authority"] == 1][0]
        api_auth = [h for h in result if h["source_type"] == "api"][0]
        # API with authority=2 should score better than doc with authority=1
        assert api_auth["_blend"] > low_auth["_blend"]

    def test_api_not_boosted_above_official_docs(self):
        """API data (authority=2) should not outrank official docs (authority=3) at same similarity."""
        hits = [
            {"text": "Official PERA Act provision", "score": 0.90, "doc_authority": 3, "source_type": "pdf"},
            {"text": "API record about same topic", "score": 0.90, "doc_authority": 2, "source_type": "api"},
        ]
        result = rerank_hits("pera provision", hits)
        # Official doc should rank first or equal
        assert result[0]["doc_authority"] >= result[1]["doc_authority"]

    def test_lexical_overlap_works_for_api_text(self):
        """Lexical overlap should work with API chunk text."""
        ov = lexical_overlap("Ahmed Khan officer", "Employee Ahmed Khan position officer")
        assert ov >= 2  # "ahmed", "khan", "officer"

    def test_empty_api_fields_no_crash(self):
        """API hit with empty optional fields should not crash reranker."""
        hits = [{
            "text": "Some API data",
            "score": 0.5,
            "doc_authority": 2,
            "source_type": "api",
            "search_text": "",
        }]
        result = rerank_hits("data", hits)
        assert len(result) == 1
        assert "_blend" in result[0]
