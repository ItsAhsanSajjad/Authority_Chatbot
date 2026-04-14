"""Tests for CitationFormatter — document and API reference formatting."""
import pytest

from citation_formatter import CitationFormatter


@pytest.fixture
def formatter():
    return CitationFormatter()


class TestDocumentReference:
    def test_basic_doc_reference(self, formatter):
        ref = formatter.format_document_reference(
            doc_name="PERA_Act.pdf",
            page_start=5,
            public_path="/assets/data/PERA_Act.pdf",
            snippet="The Authority shall...",
        )
        assert ref["document"] == "PERA_Act.pdf"
        assert ref["page_start"] == 5
        assert ref["source_type"] == "document"
        assert "#page=5" in ref["open_url"]
        assert ref["snippet"] == "The Authority shall..."

    def test_doc_reference_default_page(self, formatter):
        ref = formatter.format_document_reference(doc_name="test.pdf")
        assert ref["page_start"] == 1
        assert "#page=1" in ref["open_url"]

    def test_doc_reference_no_public_path(self, formatter):
        ref = formatter.format_document_reference(doc_name="test.pdf")
        assert "/assets/data/test.pdf" in ref["open_url"]


class TestApiReference:
    def test_basic_api_reference(self, formatter):
        ref = formatter.format_api_reference(
            display_name="PERA Employees API",
            record_id="EMP-001",
            record_type="employee",
            source_id="pera_employees",
        )
        assert ref["source_type"] == "api"
        assert ref["api_display_name"] == "PERA Employees API"
        assert ref["record_id"] == "EMP-001"
        assert ref["record_type"] == "employee"
        assert "PERA Employees API" in ref["document"]
        assert "EMP-001" in ref["document"]

    def test_api_reference_no_page(self, formatter):
        ref = formatter.format_api_reference(
            display_name="Test API",
            record_id="R001",
        )
        # API references must NOT have page_start or open_url
        assert "page_start" not in ref
        assert "open_url" not in ref

    def test_api_reference_with_sync_time(self, formatter):
        ref = formatter.format_api_reference(
            display_name="Test API",
            record_id="R001",
            synced_at=1742490000.0,  # 2025-03-20T...
        )
        assert "last_synced" in ref
        assert len(ref["last_synced"]) > 0

    def test_api_reference_minimal(self, formatter):
        ref = formatter.format_api_reference(display_name="Minimal API")
        assert ref["source_type"] == "api"
        assert ref["api_display_name"] == "Minimal API"
        assert "record_id" not in ref

    def test_api_vs_doc_distinct(self, formatter):
        """API and doc references must have different shapes."""
        doc = formatter.format_document_reference(doc_name="test.pdf", page_start=1)
        api = formatter.format_api_reference(display_name="Test API", record_id="R1")
        assert doc["source_type"] == "document"
        assert api["source_type"] == "api"
        assert "open_url" in doc
        assert "open_url" not in api


class TestReferenceGroup:
    def test_mixed_group(self, formatter):
        doc_refs = [
            formatter.format_document_reference("doc.pdf", 1),
            formatter.format_document_reference("doc.pdf", 5),
        ]
        api_refs = [
            formatter.format_api_reference("Test API", record_id="R1"),
        ]
        merged = formatter.format_reference_group(doc_refs, api_refs)
        assert len(merged) == 3
        # Docs come first
        assert merged[0]["source_type"] == "document"
        assert merged[2]["source_type"] == "api"

    def test_deduplicate(self, formatter):
        doc_refs = [
            formatter.format_document_reference("doc.pdf", 1),
            formatter.format_document_reference("doc.pdf", 1),  # duplicate
        ]
        merged = formatter.format_reference_group(doc_refs)
        assert len(merged) == 1

    def test_empty_groups(self, formatter):
        merged = formatter.format_reference_group()
        assert len(merged) == 0


class TestUtility:
    def test_is_api_source(self, formatter):
        assert formatter.is_api_source({"source_type": "api"})
        assert formatter.is_api_source({"api_source_id": "test"})
        assert not formatter.is_api_source({"source_type": "pdf"})
        assert not formatter.is_api_source({})

    def test_get_display_name(self, formatter):
        assert formatter.get_display_name({"source_type": "api", "doc_name": "My API"}) == "My API"
        assert formatter.get_display_name({"doc_name": "test.pdf"}) == "test.pdf"
        assert formatter.get_display_name({}) == "Unknown Document"
