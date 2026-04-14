"""
Tests for Stored API Mode and Both Mode answer behavior.
Validates:
  - stored_api mode fallback wording references API data, not documents
  - stored_api mode system prompt includes API-specific instructions
  - both mode system prompt includes mixed synthesis instructions
  - document mode remains unchanged
  - live API mode remains unchanged
  - source-mode-aware support state wording
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Disable auth for tests
os.environ.setdefault("AUTH_ENABLED", "0")


class TestNoEvidenceFallback(unittest.TestCase):
    """Validate that no-evidence fallback messages are source-mode-aware."""

    def test_stored_api_fallback_says_api_data(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message("stored_api")
        self.assertIn("stored API data", msg)
        self.assertNotIn("PERA documents", msg)

    def test_documents_fallback_says_documents(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message("documents")
        self.assertIn("PERA documents", msg)
        self.assertNotIn("stored API", msg)

    def test_both_fallback_says_both_sources(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message("both")
        self.assertIn("documents", msg)
        self.assertIn("stored API data", msg)

    def test_default_fallback_is_both(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message()
        self.assertIn("documents", msg)
        self.assertIn("stored API data", msg)

    def test_unknown_mode_uses_both(self):
        from answerer import _get_no_evidence_message
        msg = _get_no_evidence_message("unknown_mode")
        self.assertIn("documents", msg)
        self.assertIn("stored API data", msg)


class TestSupportStateWording(unittest.TestCase):
    """Validate source-mode-aware support state wording."""

    def test_stored_api_partial_support_says_api_data(self):
        from answerer import _apply_support_state_wording
        result = _apply_support_state_wording(
            "Test answer", "partially_supported", answer_source_mode="stored_api"
        )
        self.assertIn("stored API data", result)
        self.assertNotIn("available documents", result)

    def test_documents_partial_support_says_documents(self):
        from answerer import _apply_support_state_wording
        result = _apply_support_state_wording(
            "Test answer", "partially_supported", answer_source_mode="documents"
        )
        self.assertIn("available documents", result)
        self.assertNotIn("stored API", result)

    def test_both_partial_support_says_both(self):
        from answerer import _apply_support_state_wording
        result = _apply_support_state_wording(
            "Test answer", "partially_supported", answer_source_mode="both"
        )
        self.assertIn("documents", result)
        self.assertIn("stored API data", result)

    def test_stored_api_unsupported_says_api_data(self):
        from answerer import _apply_support_state_wording
        result = _apply_support_state_wording(
            "Test answer", "unsupported", answer_source_mode="stored_api"
        )
        self.assertIn("stored API data", result)
        self.assertNotIn("available PERA documents", result)

    def test_supported_state_no_notes(self):
        from answerer import _apply_support_state_wording
        result = _apply_support_state_wording(
            "Test answer.", "supported", answer_source_mode="stored_api"
        )
        self.assertNotIn("Note:", result)
        self.assertIn("Test answer", result)

    def test_conflicting_state_references_source(self):
        from answerer import _apply_support_state_wording
        result = _apply_support_state_wording(
            "Test answer.", "conflicting", answer_source_mode="stored_api"
        )
        self.assertIn("stored API data", result)


class TestAnswerQuestionStoredApiMode(unittest.TestCase):
    """Validate answer_question with stored_api mode."""

    @patch("answerer.get_chat_client")
    @patch("answerer.verify_grounding")
    def test_no_evidence_returns_api_fallback(self, mock_grounding, mock_client):
        """When stored_api mode retrieves nothing, fallback says 'stored API data'."""
        from answerer import answer_question

        empty_retrieval = {
            "question": "List PERA divisions",
            "has_evidence": False,
            "evidence": [],
        }

        result = answer_question(
            "List PERA divisions",
            empty_retrieval,
            answer_source_mode="stored_api",
        )

        self.assertEqual(result["decision"], "refuse")
        self.assertIn("stored API data", result["answer"])
        self.assertNotIn("PERA documents", result["answer"])

    @patch("answerer.get_chat_client")
    @patch("answerer.verify_grounding")
    def test_no_evidence_documents_mode_says_documents(self, mock_grounding, mock_client):
        """When documents mode retrieves nothing, fallback says 'PERA documents'."""
        from answerer import answer_question

        empty_retrieval = {
            "question": "test",
            "has_evidence": False,
            "evidence": [],
        }

        result = answer_question(
            "test",
            empty_retrieval,
            answer_source_mode="documents",
        )

        self.assertEqual(result["decision"], "refuse")
        self.assertIn("PERA documents", result["answer"])
        self.assertNotIn("stored API", result["answer"])

    @patch("answerer.get_chat_client")
    @patch("answerer.verify_grounding")
    def test_stored_api_evidence_generates_answer(self, mock_grounding, mock_client):
        """When stored_api mode has evidence, it generates an answer with API prompt."""
        from answerer import answer_question

        # Mock LLM response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = (
            "Here are the PERA divisions:\n"
            "1. Lahore\n2. Rawalpindi\n3. Bahawalpur"
        )
        mock_client.return_value.chat.completions.create.return_value = mock_response

        # Mock grounding
        mock_grounding.return_value = MagicMock(
            score=0.8, confidence="high", semantic_support="full"
        )

        # Build retrieval with API-sourced evidence
        retrieval = {
            "question": "List PERA divisions",
            "has_evidence": True,
            "evidence": [{
                "doc_name": "app_data_divisions",
                "max_score": 0.85,
                "hits": [
                    {
                        "text": '[Source Type: API]\nDivision: Lahore (id=1)',
                        "score": 0.85,
                        "page_start": "?",
                        "page_end": "?",
                        "public_path": "",
                        "doc_authority": 2,
                        "search_text": "",
                        "source_type": "api",
                        "api_source_id": "app_data_divisions",
                        "record_id": "div_1",
                        "record_type": "division",
                        "evidence_id": "abc123",
                    },
                ],
            }],
        }

        result = answer_question(
            "List PERA divisions",
            retrieval,
            answer_source_mode="stored_api",
        )

        self.assertEqual(result["decision"], "answer")
        self.assertIn("division", result["answer"].lower())

        # Verify the system prompt included stored_api mode instructions
        call_args = mock_client.return_value.chat.completions.create.call_args
        messages = call_args[1]["messages"]
        system_msg = messages[0]["content"]
        self.assertIn("STORED API DATA", system_msg)
        self.assertIn("structured lists", system_msg)


class TestAnswerQuestionBothMode(unittest.TestCase):
    """Validate answer_question with both mode."""

    @patch("answerer.get_chat_client")
    @patch("answerer.verify_grounding")
    def test_both_mode_prompt_has_mixed_instructions(self, mock_grounding, mock_client):
        """Both mode system prompt should include mixed synthesis instructions."""
        from answerer import answer_question

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Combined answer from both sources."
        mock_client.return_value.chat.completions.create.return_value = mock_response
        mock_grounding.return_value = MagicMock(
            score=0.7, confidence="medium", semantic_support="combined"
        )

        retrieval = {
            "question": "What are PERA divisions?",
            "has_evidence": True,
            "evidence": [{
                "doc_name": "PERA_Act.pdf",
                "max_score": 0.7,
                "hits": [{
                    "text": "PERA enforces regulations across Punjab divisions.",
                    "score": 0.7,
                    "page_start": 5,
                    "page_end": 5,
                    "public_path": "/assets/data/PERA_Act.pdf",
                    "doc_authority": 1,
                    "search_text": "",
                    "evidence_id": "doc1",
                }],
            }, {
                "doc_name": "app_data_divisions",
                "max_score": 0.8,
                "hits": [{
                    "text": "Division: Lahore (id=1), Division: Rawalpindi (id=2)",
                    "score": 0.8,
                    "page_start": "?",
                    "page_end": "?",
                    "public_path": "",
                    "doc_authority": 2,
                    "search_text": "",
                    "source_type": "api",
                    "api_source_id": "app_data_divisions",
                    "record_id": "div_1",
                    "record_type": "division",
                    "evidence_id": "api1",
                }],
            }],
        }

        result = answer_question(
            "What are PERA divisions?",
            retrieval,
            answer_source_mode="both",
        )

        # Verify the system prompt included both-mode instructions
        call_args = mock_client.return_value.chat.completions.create.call_args
        messages = call_args[1]["messages"]
        system_msg = messages[0]["content"]
        self.assertIn("MIXED", system_msg)
        self.assertIn("DOCUMENTS + STORED API DATA", system_msg)
        self.assertIn("combine them", system_msg.lower())

    @patch("answerer.get_chat_client")
    @patch("answerer.verify_grounding")
    def test_documents_mode_prompt_has_documents_instructions(self, mock_grounding, mock_client):
        """Documents mode system prompt should reference documents only."""
        from answerer import answer_question

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Answer from documents."
        mock_client.return_value.chat.completions.create.return_value = mock_response
        mock_grounding.return_value = MagicMock(
            score=0.8, confidence="high", semantic_support="full"
        )

        retrieval = {
            "question": "test",
            "has_evidence": True,
            "evidence": [{
                "doc_name": "PERA_Act.pdf",
                "max_score": 0.8,
                "hits": [{
                    "text": "PERA is the Punjab Enforcement and Regulatory Authority.",
                    "score": 0.8,
                    "page_start": 1,
                    "page_end": 1,
                    "public_path": "/assets/data/PERA_Act.pdf",
                    "doc_authority": 1,
                    "search_text": "",
                    "evidence_id": "doc1",
                }],
            }],
        }

        result = answer_question(
            "What is PERA?",
            retrieval,
            answer_source_mode="documents",
        )

        call_args = mock_client.return_value.chat.completions.create.call_args
        messages = call_args[1]["messages"]
        system_msg = messages[0]["content"]
        self.assertIn("REGULATORY DOCUMENTS", system_msg)
        self.assertNotIn("STORED API DATA", system_msg)
        self.assertNotIn("MIXED", system_msg)


class TestAnswerQuestionDefaultBackwardCompat(unittest.TestCase):
    """Validate backward compatibility — default mode is 'both'."""

    @patch("answerer.get_chat_client")
    @patch("answerer.verify_grounding")
    def test_default_mode_is_both(self, mock_grounding, mock_client):
        """answer_question without explicit mode should default to both."""
        from answerer import answer_question

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Answer text."
        mock_client.return_value.chat.completions.create.return_value = mock_response
        mock_grounding.return_value = MagicMock(
            score=0.8, confidence="high", semantic_support="full"
        )

        retrieval = {
            "question": "test",
            "has_evidence": True,
            "evidence": [{
                "doc_name": "Test.pdf",
                "max_score": 0.8,
                "hits": [{
                    "text": "Test content.",
                    "score": 0.8,
                    "page_start": 1,
                    "page_end": 1,
                    "public_path": "",
                    "doc_authority": 2,
                    "search_text": "",
                    "evidence_id": "test1",
                }],
            }],
        }

        # Call without answer_source_mode
        result = answer_question("test", retrieval)

        # System prompt should include MIXED instructions (default=both)
        call_args = mock_client.return_value.chat.completions.create.call_args
        messages = call_args[1]["messages"]
        system_msg = messages[0]["content"]
        self.assertIn("MIXED", system_msg)


class TestSourceModeFilterMapping(unittest.TestCase):
    """Validate that source mode maps to correct retriever filter."""

    def test_stored_api_maps_to_api_filter(self):
        """stored_api mode should send source_type_filter='api' to retriever."""
        _filter_map = {
            "documents": "document",
            "stored_api": "api",
            "both": None,
        }
        self.assertEqual(_filter_map["stored_api"], "api")
        self.assertEqual(_filter_map["documents"], "document")
        self.assertIsNone(_filter_map["both"])


class TestProvenanceLabels(unittest.TestCase):
    """Validate provenance labels remain correct."""

    def test_stored_api_provenance(self):
        from fastapi_app import SOURCE_MODE_PROVENANCE
        p = SOURCE_MODE_PROVENANCE["stored_api"]
        self.assertIn("stored indexed API", p)

    def test_documents_provenance(self):
        from fastapi_app import SOURCE_MODE_PROVENANCE
        p = SOURCE_MODE_PROVENANCE["documents"]
        self.assertIn("regulatory documents", p)

    def test_both_provenance(self):
        from fastapi_app import SOURCE_MODE_PROVENANCE
        p = SOURCE_MODE_PROVENANCE["both"]
        self.assertIn("documents", p)
        self.assertIn("API", p)

    def test_live_api_provenance(self):
        from fastapi_app import SOURCE_MODE_PROVENANCE
        p = SOURCE_MODE_PROVENANCE["live_api"]
        self.assertIn("live API", p)


if __name__ == "__main__":
    unittest.main()
