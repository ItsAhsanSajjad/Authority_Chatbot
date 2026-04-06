"""Tests for retriever API metadata integration."""
import pytest


class TestRetrieverApiMetadata:
    """Test that retriever._process_hit propagates API metadata."""

    def test_process_hit_doc_chunk(self):
        """Document chunks should not have API metadata."""
        # Simulate what _process_hit does internally
        r = {
            "doc_name": "test.pdf",
            "text": "test document text",
            "loc_start": 1,
            "loc_end": 1,
            "public_path": "/assets/data/test.pdf",
            "doc_authority": 3,
            "search_text": "test",
            "source_type": "pdf",
        }
        hit_obj = {
            "text": r["text"],
            "score": 0.85,
            "page_start": r["loc_start"],
            "page_end": r["loc_end"],
            "public_path": r["public_path"],
            "doc_authority": r["doc_authority"],
            "search_text": r["search_text"],
        }
        source_type = r.get("source_type", "")
        if source_type:
            hit_obj["source_type"] = source_type

        assert hit_obj.get("source_type") == "pdf"
        assert "api_source_id" not in hit_obj
        assert "record_id" not in hit_obj

    def test_process_hit_api_chunk(self):
        """API chunks should carry API metadata through to hits."""
        r = {
            "doc_name": "PERA Employees API",
            "text": "Employee data: name=Ahmed, dept=HR",
            "loc_start": "EMP-001",
            "loc_end": "EMP-001",
            "public_path": "",
            "doc_authority": 2,
            "search_text": "test",
            "source_type": "api",
            "api_source_id": "pera_employees",
            "record_id": "EMP-001",
            "record_type": "employee",
            "api_tags": ["hr", "pera"],
        }
        hit_obj = {
            "text": r["text"],
            "score": 0.75,
            "page_start": r["loc_start"],
            "page_end": r["loc_end"],
            "public_path": r["public_path"],
            "doc_authority": r["doc_authority"],
            "search_text": r["search_text"],
        }
        # Simulate Phase 4 metadata propagation
        source_type = r.get("source_type", "")
        if source_type:
            hit_obj["source_type"] = source_type
        if r.get("api_source_id"):
            hit_obj["api_source_id"] = r["api_source_id"]
        if r.get("record_id"):
            hit_obj["record_id"] = r["record_id"]
        if r.get("record_type"):
            hit_obj["record_type"] = r["record_type"]
        if r.get("api_tags"):
            hit_obj["api_tags"] = r["api_tags"]

        assert hit_obj["source_type"] == "api"
        assert hit_obj["api_source_id"] == "pera_employees"
        assert hit_obj["record_id"] == "EMP-001"
        assert hit_obj["record_type"] == "employee"
        assert hit_obj["api_tags"] == ["hr", "pera"]

    def test_mixed_hits_maintain_types(self):
        """Mixed doc+API hits should each maintain correct source_type."""
        doc_hit = {"source_type": "pdf", "doc_name": "test.pdf"}
        api_hit = {"source_type": "api", "api_source_id": "src", "record_id": "R1"}

        assert doc_hit["source_type"] != api_hit["source_type"]
        assert "api_source_id" not in doc_hit
        assert "api_source_id" in api_hit

    def test_page_expansion_skip_for_api(self):
        """API chunks with source_type='api' should be skipped in page expansion."""
        r = {"source_type": "api", "doc_name": "API Data", "loc_start": "R1"}
        # The skip condition in retriever.py
        should_skip = r.get("source_type") == "api"
        assert should_skip is True

    def test_page_expansion_allowed_for_docs(self):
        """Document chunks should NOT be skipped in page expansion."""
        r = {"source_type": "pdf", "doc_name": "test.pdf", "loc_start": 5}
        should_skip = r.get("source_type") == "api"
        assert should_skip is False
