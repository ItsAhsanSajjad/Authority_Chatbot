"""
PERA AI — Stored API Direct Lookup (Config-Driven)

Provides direct data retrieval from the SQLite api_records table
for known reference/lookup queries (e.g., "list divisions",
"workforce strength", "finance overview").

**Now powered by api_lookup_registry** — all per-API keyword patterns,
display formatting, and context building are read from the YAML
configs in ``assets/apis/``.  Adding a new stored-API lookup requires
ONLY a YAML file with a ``lookup:`` section; no code changes here.

This module is used by the answer pipeline when:
- source_mode is 'stored_api' or 'both'
- the query matches a known lookup pattern
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional

from log_config import get_logger

log = get_logger("pera.stored_api_lookup")

# Default path to API ingestion database
_DEFAULT_DB_PATH = os.path.join("data", "api_ingestion.db")


# ── Public API (unchanged interface for fastapi_app.py) ──────


def detect_lookup_intent(question: str) -> Optional[str]:
    """
    Detect if a question is a known reference/lookup query.

    Returns a ``source_id`` string, or ``None``.

    Checks (in order):
      1. Operational activity queries → ``oa_*`` source IDs
         (checked first so "requisitions" / "operational activity" keywords
          aren't swallowed by the broader challan patterns)
      2. Challan queries  → ``challan_*`` source IDs
      3. YAML-registered APIs → ``app_data_divisions`` etc.
    """
    # 1. Operational activity queries (PostgreSQL)
    #    Checked BEFORE challans so OA-specific keywords get priority
    try:
        from operational_activity_lookup import detect_operational_activity_intent
        oa_id = detect_operational_activity_intent(question)
        if oa_id:
            return oa_id
    except ImportError:
        pass

    # 2. Inspection performance queries (PostgreSQL)
    try:
        from inspection_lookup import detect_inspection_intent
        insp_id = detect_inspection_intent(question)
        if insp_id:
            return insp_id
    except ImportError:
        pass

    # 3. Challan queries (dedicated relational DB)
    try:
        from challan_lookup import detect_challan_intent
        challan_id = detect_challan_intent(question)
        if challan_id:
            return challan_id
    except ImportError:
        pass

    # 3. YAML-registered generic lookups
    from api_lookup_registry import detect_lookup_intent as _detect
    return _detect(question)


def execute_lookup(
    source_id: str,
    db_path: str = _DEFAULT_DB_PATH,
    question: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Execute a stored-API lookup by *source_id*.

    Routes challan lookups to the dedicated challan_lookup module;
    all other lookups to the generic YAML-driven registry.
    """
    # Challan lookups — handles both simple IDs (challan_totals)
    # and encoded IDs (challan_location:tehsil:Lahore Saddar)
    if source_id and source_id.startswith("challan_"):
        try:
            from challan_lookup import execute_challan_lookup
            return execute_challan_lookup(source_id, question)
        except ImportError:
            log.warning("challan_lookup module not available")
            return None

    # Inspection performance lookups
    if source_id and source_id.startswith("insp_"):
        try:
            from inspection_lookup import execute_inspection_lookup
            return execute_inspection_lookup(source_id, question)
        except ImportError:
            log.warning("inspection_lookup module not available")
            return None

    # Operational activity lookups
    if source_id and source_id.startswith("oa_"):
        try:
            from operational_activity_lookup import execute_operational_activity_lookup
            return execute_operational_activity_lookup(source_id, question)
        except ImportError:
            log.warning("operational_activity_lookup module not available")
            return None

    # Generic YAML-driven lookups
    from api_lookup_registry import execute_stored_lookup
    return execute_stored_lookup(source_id, db_path)


def build_lookup_retrieval(
    lookup_result: Dict[str, Any],
    source_id: str,
) -> Dict[str, Any]:
    """
    Build a retrieval dict (same shape as ``retriever.retrieve()``
    output) from a lookup result so it can be passed to
    ``answer_question()``.

    IMPORTANT: All records are combined into a **single** evidence
    chunk to avoid the answerer's ``MAX_HITS_PER_DOC`` truncation
    (default: 6).  For list/catalog queries we need ALL records in the
    LLM context.
    """
    from api_lookup_registry import build_evidence_context, get_registry

    records = lookup_result.get("records", [])
    sid = lookup_result.get("source_id", source_id)

    # Resolve display name from registry (or from challan result)
    # For encoded challan source_ids, use the base name for registry lookup
    registry = get_registry()
    base_sid = sid.split(":")[0] if ":" in sid else sid
    spec = registry.get(base_sid) or registry.get(sid)
    display_name = spec.display_name if spec else lookup_result.get("source_id", sid)

    # Prefer pre-formatted context (e.g. from challan_lookup);
    # fall back to the registry's generic evidence builder.
    consolidated_text = lookup_result.get("formatted_context") or ""

    if not records and not consolidated_text:
        return {
            "question": "",
            "has_evidence": False,
            "evidence": [],
        }
    if not consolidated_text:
        consolidated_text = build_evidence_context(sid, records)

    record_ids = ",".join(
        str(r.get("id", r.get("record_id", ""))) for r in records
    )

    single_hit = {
        "text": consolidated_text,
        "score": 0.95,
        "_blend": 0.95,
        "page_start": "?",
        "page_end": "?",
        "public_path": "",
        "doc_authority": 2,
        "search_text": "",
        "source_type": "api",
        "api_source_id": sid,
        "record_id": record_ids,
        "record_type": f"{source_id}_list",
        "evidence_id": f"{source_id}_all_{len(records)}",
        "_is_primary_lookup": True,
    }

    return {
        "question": "",
        "has_evidence": True,
        "_has_primary_lookup": True,
        "evidence": [{
            "doc_name": display_name,
            "max_score": 0.95,
            "_is_primary_lookup": True,
            "hits": [single_hit],
        }],
    }


def merge_lookup_with_rag(
    lookup_retrieval: Dict[str, Any],
    rag_retrieval: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Merge a lookup-based retrieval with a FAISS-based RAG retrieval.
    Lookup evidence comes first (higher priority), followed by document
    hits.  Used for ``'both'`` mode.

    When a primary lookup exists (direct API answer), the document RAG
    hits are tagged as supplementary so the reference extractor can
    deprioritize them.
    """
    merged_evidence = []
    has_primary = lookup_retrieval.get("_has_primary_lookup", False)

    # Lookup evidence first (API data)
    if lookup_retrieval.get("has_evidence"):
        merged_evidence.extend(lookup_retrieval.get("evidence", []))

    # Then document RAG evidence
    if rag_retrieval.get("has_evidence"):
        for doc_group in rag_retrieval.get("evidence", []):
            if has_primary:
                doc_group["_is_supplementary"] = True
                for hit in doc_group.get("hits", []):
                    hit["_is_supplementary"] = True
            merged_evidence.append(doc_group)

    return {
        "question": rag_retrieval.get("question", ""),
        "has_evidence": bool(merged_evidence),
        "_has_primary_lookup": has_primary,
        "evidence": merged_evidence,
    }
