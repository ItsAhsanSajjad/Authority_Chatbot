"""Tests for doc_registry DOCX support and authority classification."""
import pytest


def test_supported_exts_includes_docx():
    """SUPPORTED_EXTS should include both .pdf and .docx."""
    from doc_registry import SUPPORTED_EXTS
    assert ".pdf" in SUPPORTED_EXTS
    assert ".docx" in SUPPORTED_EXTS


def test_classify_authority_high():
    """Official acts should get authority=3."""
    from doc_registry import classify_doc_authority
    assert classify_doc_authority("PERA Act (10 of 2024).pdf") == 3
    assert classify_doc_authority("Annex G - Notification.pdf") == 3


def test_classify_authority_low():
    """Working papers should get authority=1."""
    from doc_registry import classify_doc_authority
    assert classify_doc_authority("Compiled Working Paper.pdf") == 1
    assert classify_doc_authority("Meeting Minutes March.pdf") == 1


def test_classify_authority_medium():
    """Regular documents should get authority=2."""
    from doc_registry import classify_doc_authority
    assert classify_doc_authority("HR Manual.pdf") == 2
    assert classify_doc_authority("Ops Code.pdf") == 2
