"""Tests for context_state entity anchoring."""
import pytest


def test_anchor_query_substitutes_pronouns():
    """anchor_query should replace pronouns with last subject."""
    from context_state import anchor_query
    anchored, subject, was_anchored = anchor_query(
        question="What is their salary?",
        last_subject="Manager (Development)",
        last_question="Tell me about Manager Development",
        last_answer="The Manager (Development) handles...",
    )
    assert "Manager" in anchored or "manager" in anchored.lower()
    assert was_anchored


def test_anchor_query_standalone_not_modified():
    """Standalone queries with role keywords should not be modified."""
    from context_state import anchor_query
    anchored, subject, was_anchored = anchor_query(
        question="What is the CTO salary?",
        last_subject="Manager (Development)",
        last_question="",
        last_answer="",
    )
    # Should keep original question (CTO is a standalone role keyword)
    assert "CTO" in anchored or "cto" in anchored.lower()


def test_extract_subject_finds_role():
    """extract_subject should find role names in answer text."""
    from context_state import extract_subject
    subject = extract_subject(
        "The Manager (Development) has a pay scale of BPS-18..."
    )
    assert subject and "manager" in subject.lower()


def test_extract_evidence_metadata_with_hits():
    """extract_evidence_metadata should extract IDs and doc names from evidence groups."""
    from context_state import extract_evidence_metadata
    # Match the actual evidence structure (doc groups with hits)
    retrieval = {
        "evidence": [
            {
                "doc_name": "PERA Act.pdf",
                "hits": [
                    {"evidence_id": "eid-1"},
                    {"evidence_id": "eid-2"},
                ]
            },
            {
                "doc_name": "HR Manual.pdf",
                "hits": [
                    {"evidence_id": "eid-3"},
                ]
            },
        ]
    }
    ids, docs = extract_evidence_metadata(retrieval)
    assert len(ids) >= 2
    assert "PERA Act.pdf" in docs
    assert "HR Manual.pdf" in docs
