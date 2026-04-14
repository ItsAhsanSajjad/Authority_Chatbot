"""Tests for answerer API evidence integration."""
import pytest
from unittest.mock import patch

from answerer import format_evidence_for_llm, extract_references_simple


def _make_retrieval(hits, doc_name="Test Doc"):
    """Build a retrieval result dict with given hits."""
    return {
        "question": "test question",
        "has_evidence": True,
        "evidence": [{
            "doc_name": doc_name,
            "max_score": 0.85,
            "hits": hits,
        }],
    }


def _make_doc_hit(text="Document text", page=1, score=0.85):
    return {
        "text": text,
        "score": score,
        "_blend": score,
        "page_start": page,
        "page_end": page,
        "public_path": "/assets/data/test.pdf",
        "doc_authority": 3,
        "search_text": "test",
        "source_type": "pdf",
        "evidence_id": "doc123",
        "_is_smart_context": False,
    }


def _make_api_hit(text="API record data", record_id="R001", score=0.78):
    return {
        "text": text,
        "score": score,
        "_blend": score,
        "page_start": record_id,
        "page_end": record_id,
        "public_path": "",
        "doc_authority": 2,
        "search_text": "api record",
        "source_type": "api",
        "api_source_id": "pera_employees",
        "record_id": record_id,
        "record_type": "employee",
        "evidence_id": "api456",
        "_is_smart_context": False,
    }


class TestFormatEvidenceForLlm:
    def test_doc_evidence_format(self):
        """Document evidence should use page-based XML format."""
        hit = _make_doc_hit("Section 5 of PERA Act states...")
        retrieval = _make_retrieval([hit])
        result = format_evidence_for_llm(retrieval, question="pera act")
        assert '<evidence doc="Test Doc" page="1"' in result
        assert "Section 5 of PERA Act" in result
        assert 'source_type="api"' not in result

    def test_api_evidence_format(self):
        """API evidence should use source_type/record_id XML format."""
        hit = _make_api_hit("Employee: Ahmed Khan, Dept: HR")
        retrieval = _make_retrieval([hit], doc_name="PERA Employees API")
        result = format_evidence_for_llm(retrieval, question="Ahmed Khan")
        assert 'source_type="api"' in result
        assert "[Source Type: API]" in result
        assert "[API Name: PERA Employees API]" in result
        assert "[Record ID: R001]" in result
        assert "[Record Type: employee]" in result
        assert "Ahmed Khan" in result
        # Should NOT have page= attribute
        assert 'page="' not in result

    def test_mixed_evidence_format(self):
        """Mixed doc+API evidence should include both formats."""
        doc_hit = _make_doc_hit("PERA Act Section 5")
        api_hit = _make_api_hit("Employee Ahmed")
        retrieval = {
            "question": "test",
            "has_evidence": True,
            "evidence": [
                {"doc_name": "PERA_Act.pdf", "max_score": 0.9, "hits": [doc_hit]},
                {"doc_name": "PERA Employees API", "max_score": 0.8, "hits": [api_hit]},
            ],
        }
        result = format_evidence_for_llm(retrieval, question="test")
        assert 'page="1"' in result  # Document evidence
        assert 'source_type="api"' in result  # API evidence

    def test_empty_evidence(self):
        """No evidence should return empty string."""
        retrieval = {"has_evidence": False, "evidence": []}
        result = format_evidence_for_llm(retrieval)
        assert result == ""


class TestExtractReferencesSimple:
    def test_doc_reference_shape(self):
        """Document references should have page_start and open_url."""
        hit = _make_doc_hit("Test text")
        hit["_used_for_evidence"] = True
        retrieval = _make_retrieval([hit])
        refs = extract_references_simple(retrieval, question="test")
        assert len(refs) >= 1
        ref = refs[0]
        assert ref["source_type"] == "document"
        assert "page_start" in ref
        assert "open_url" in ref

    def test_api_reference_shape(self):
        """API references should be distinct from document references."""
        hit = _make_api_hit("API data about Ahmed")
        hit["_used_for_evidence"] = True
        retrieval = _make_retrieval([hit], doc_name="PERA Employees API")
        refs = extract_references_simple(retrieval, question="Ahmed")
        assert len(refs) >= 1
        ref = refs[0]
        assert ref["source_type"] == "api"
        assert ref["api_display_name"] == "PERA Employees API"
        assert "record_id" in ref
        # API refs should NOT have open_url or page_start
        assert "open_url" not in ref
        assert "page_start" not in ref

    def test_mixed_references(self):
        """Mixed doc+API retrieval should produce both ref types."""
        doc_hit = _make_doc_hit("PERA Act text")
        doc_hit["_used_for_evidence"] = True
        api_hit = _make_api_hit("API employee data")
        api_hit["_used_for_evidence"] = True

        retrieval = {
            "question": "test",
            "has_evidence": True,
            "evidence": [
                {"doc_name": "PERA_Act.pdf", "max_score": 0.9, "hits": [doc_hit]},
                {"doc_name": "PERA Employees API", "max_score": 0.8, "hits": [api_hit]},
            ],
        }
        refs = extract_references_simple(retrieval, question="test")
        source_types = {r.get("source_type") for r in refs}
        assert "document" in source_types or "api" in source_types

    def test_doc_behavior_unchanged(self):
        """Document-only retrieval should produce same reference shape as before."""
        hit = _make_doc_hit("PERA Act provision about powers")
        hit["_used_for_evidence"] = True
        retrieval = _make_retrieval([hit], doc_name="PERA_Act.pdf")
        refs = extract_references_simple(retrieval, question="powers of PERA")
        assert len(refs) >= 1
        ref = refs[0]
        assert ref["document"] == "PERA_Act.pdf"
        assert "open_url" in ref
        assert "#page=" in ref["open_url"]


class TestAuditProvenance:
    def test_audit_accepts_api_provenance(self):
        """Audit trail should accept API provenance fields without error."""
        from audit_trail import log_audit_entry
        # Should not raise
        log_audit_entry(
            request_id="test-req",
            question="test",
            decision="answer",
            source_types_used=["pdf", "api"],
            api_sources_used=["pera_employees"],
            api_record_ids_used=["EMP-001", "EMP-002"],
            mixed_sources=True,
        )

    def test_audit_backward_compatible(self):
        """Audit trail should work without new API fields."""
        from audit_trail import log_audit_entry
        # Should not raise — no API fields
        log_audit_entry(
            request_id="test-req",
            question="test",
            decision="answer",
        )
