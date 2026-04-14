"""
PERA AI — API Record Chunker

Splits normalized API records into retrieval-friendly text chunks
with full source provenance metadata. Uses the same char-budget
approach as the document chunker but operates on record text
rather than pages.

Phase 3 module.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api_config_models import ApiSourceConfig
from api_record_builder import NormalizedApiRecord
from log_config import get_logger
from settings import get_settings

log = get_logger("pera.api.chunker")


@dataclass
class ApiChunk:
    """A single retrieval-ready chunk from an API record."""
    chunk_id: str = ""
    source_id: str = ""
    source_type: str = "api"
    record_id: str = ""
    record_type: str = ""
    display_title: str = ""
    chunk_index: int = 0
    chunk_text: str = ""
    chunk_hash: str = ""
    authority: int = 2
    field_list: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    display_name: str = ""
    synced_at: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class ApiChunker:
    """Chunks normalized API records into retrieval-ready pieces."""

    def __init__(self):
        settings = get_settings()
        self._max_chars = settings.API_CHUNK_MAX_CHARS
        self._overlap_chars = settings.API_CHUNK_OVERLAP_CHARS

    def chunk_record(
        self, record: NormalizedApiRecord, config: ApiSourceConfig
    ) -> List[ApiChunk]:
        """Split one normalized record into one or more ApiChunks."""
        text = (record.canonical_text or "").strip()
        if not text:
            return []

        texts = self._split_text(text)

        chunks: List[ApiChunk] = []
        for i, chunk_text in enumerate(texts):
            chunk_text = chunk_text.strip()
            if not chunk_text:
                continue

            chunk_id = self._compute_chunk_id(
                record.source_id, record.record_id, i
            )
            chunk_hash = hashlib.sha256(
                chunk_text.encode("utf-8")
            ).hexdigest()

            chunks.append(ApiChunk(
                chunk_id=chunk_id,
                source_id=record.source_id,
                source_type="api",
                record_id=record.record_id,
                record_type=record.record_type,
                display_title=record.display_title,
                chunk_index=i,
                chunk_text=chunk_text,
                chunk_hash=chunk_hash,
                authority=config.indexing.authority,
                field_list=list(record.field_list),
                tags=list(config.indexing.tags),
                display_name=config.display_name,
                synced_at=record.last_updated_at,
                metadata={
                    "source_id": record.source_id,
                    "record_id": record.record_id,
                    "record_type": record.record_type,
                    "display_name": config.display_name,
                },
            ))

        return chunks

    def chunk_records(
        self, records: List[NormalizedApiRecord], config: ApiSourceConfig
    ) -> List[ApiChunk]:
        """Chunk a list of normalized records."""
        all_chunks: List[ApiChunk] = []
        for record in records:
            all_chunks.extend(self.chunk_record(record, config))
        log.info(
            "Chunked %d records -> %d chunks for %s",
            len(records), len(all_chunks), config.source_id,
        )
        return all_chunks

    def _split_text(self, text: str) -> List[str]:
        """Split text into chunks respecting max_chars and overlap."""
        if len(text) <= self._max_chars:
            return [text]

        chunks: List[str] = []
        step = max(200, self._max_chars - self._overlap_chars)
        start = 0

        while start < len(text):
            end = min(len(text), start + self._max_chars)
            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)
            if end >= len(text):
                break
            start += step

        return chunks

    def _compute_chunk_id(
        self, source_id: str, record_id: str, chunk_index: int
    ) -> str:
        """Deterministic chunk ID from source + record + index."""
        raw = f"{source_id}:{record_id}:{chunk_index}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
