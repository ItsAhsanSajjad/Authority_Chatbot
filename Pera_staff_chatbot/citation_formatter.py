"""
PERA AI — Citation Formatter

Centralised reference/citation formatting for document and API sources.
Produces distinct, non-confusing references for each source type.

Phase 4 module.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from log_config import get_logger
from settings import get_settings

log = get_logger("pera.citation_formatter")


class CitationFormatter:
    """Formats references for document and API sources."""

    def __init__(self):
        self._settings = get_settings()
        self._base_url = self._settings.BASE_URL.rstrip("/")

    # ── Document References ──────────────────────────────────────

    def format_document_reference(
        self,
        doc_name: str,
        page_start: Any = None,
        public_path: str = "",
        snippet: str = "",
    ) -> Dict[str, Any]:
        """
        Format a standard document reference (PDF/DOCX page-based).
        Preserves the existing reference shape used by the UI.
        """
        page = page_start if page_start is not None else 1
        path = public_path or f"/assets/data/{doc_name}"
        url = f"{self._base_url}{path}#page={page}"

        return {
            "document": doc_name,
            "page_start": page,
            "open_url": url,
            "snippet": (snippet or "")[:200],
            "source_type": "document",
        }

    # ── API References ───────────────────────────────────────────

    def format_api_reference(
        self,
        display_name: str,
        record_id: str = "",
        record_type: str = "",
        source_id: str = "",
        synced_at: Optional[float] = None,
        snippet: str = "",
        data_query: str = "",
        data_table: str = "",
        api_endpoint: str = "",
    ) -> Dict[str, Any]:
        """
        Format an API source reference.
        Never pretends to be a page reference.
        Optionally includes data_query / data_table for verification.
        """
        # Build human-readable label
        label_parts = [display_name]
        if record_type:
            label_parts.append(f"{record_type.title()}")
        if record_id:
            label_parts.append(f"Record {record_id}")

        label = " — ".join(label_parts)

        # Format synced timestamp if available
        synced_str = ""
        if synced_at:
            try:
                synced_str = time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(synced_at)
                )
            except Exception:
                synced_str = str(synced_at)

        ref: Dict[str, Any] = {
            "document": label,
            "source_type": "api",
            "api_display_name": display_name,
            "snippet": (snippet or "")[:200],
        }

        if record_id:
            ref["record_id"] = record_id
        if record_type:
            ref["record_type"] = record_type
        if source_id:
            ref["api_source_id"] = source_id
        if synced_str:
            ref["last_synced"] = synced_str
        if data_query:
            ref["data_query"] = data_query
        if data_table:
            ref["data_table"] = data_table
        if api_endpoint:
            ref["api_endpoint"] = api_endpoint

        return ref

    # ── Mixed Reference Groups ───────────────────────────────────

    def format_reference_group(
        self,
        doc_refs: Optional[List[Dict[str, Any]]] = None,
        api_refs: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Merge and deduplicate document + API references into a single list.
        Documents come first, then API references.
        """
        merged: List[Dict[str, Any]] = []
        seen_keys: set = set()

        # Documents first
        for ref in (doc_refs or []):
            key = f"doc:{ref.get('document', '')}:{ref.get('page_start', '')}"
            if key not in seen_keys:
                seen_keys.add(key)
                merged.append(ref)

        # API references after
        for ref in (api_refs or []):
            key = f"api:{ref.get('api_display_name', '')}:{ref.get('record_id', '')}"
            if key not in seen_keys:
                seen_keys.add(key)
                merged.append(ref)

        return merged

    # ── Utility ──────────────────────────────────────────────────

    def is_api_source(self, hit: Dict[str, Any]) -> bool:
        """Check if a hit/chunk row is from an API source."""
        return (hit.get("source_type", "") == "api"
                or bool(hit.get("api_source_id")))

    def get_display_name(self, hit: Dict[str, Any]) -> str:
        """Get appropriate display name for a hit."""
        if self.is_api_source(hit):
            return (hit.get("doc_name", "")
                    or hit.get("api_display_name", "")
                    or f"API: {hit.get('api_source_id', 'Unknown')}")
        return hit.get("doc_name", "Unknown Document")
