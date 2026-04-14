"""
PERA AI — Query Router

Classifies incoming queries to determine optimal retrieval strategy:
- DOCUMENT: Policy, rules, procedures → FAISS semantic search
- STRUCTURED: Metrics, counts, rankings, operational data → stored API / SQL
- HYBRID: Needs both document context and operational data

This sits between the FastAPI endpoint and the retrieval layer.
"""
from __future__ import annotations

import re
from enum import Enum
from typing import Optional

from log_config import get_logger

log = get_logger("pera.query_router")


class QueryType(Enum):
    DOCUMENT = "document"
    STRUCTURED = "structured"
    HYBRID = "hybrid"


# Signals that a query is about structured/operational data
_STRUCTURED_SIGNALS = [
    # Counting / aggregation
    re.compile(r"\b(?:how\s+many|count|total\s+(?:number|staff|employees)|kitne|kitni)\b", re.IGNORECASE),
    # Ranking / comparison
    re.compile(r"\b(?:top\s+\d+|bottom\s+\d+|highest|lowest|ranking|best|worst)\b", re.IGNORECASE),
    re.compile(r"\b(?:compare|comparison|versus|vs\.?|difference\s+between)\b", re.IGNORECASE),
    # Trends / temporal
    re.compile(r"\b(?:trend|growth|increase|decrease|month[\s-]over[\s-]month|year[\s-]over[\s-]year)\b", re.IGNORECASE),
    # Live / real-time signals
    re.compile(r"\b(?:live\s+data|real[\s-]?time|current\s+strength|today|right\s+now|abhi)\b", re.IGNORECASE),
    # Operational data
    re.compile(r"\b(?:workforce|on\s*duty|absent|manpower|attendance|headcount|personnel)\b", re.IGNORECASE),
    re.compile(r"\b(?:expenditure|budget|spending|monthly\s+expense|finance\s+(?:data|overview))\b", re.IGNORECASE),
    re.compile(r"\b(?:challan|fine|penalty|violation)\s*(?:count|data|statistics|stats)\b", re.IGNORECASE),
    # Division/station listing
    re.compile(r"\b(?:list\s+(?:all\s+)?(?:divisions?|stations?|districts?))\b", re.IGNORECASE),
]

# Signals that a query is about policy/rules/documents
_DOCUMENT_SIGNALS = [
    re.compile(r"\b(?:salary|pay\s*scale|sppp|bps|allowance|compensation|benefits?)\b", re.IGNORECASE),
    re.compile(r"\b(?:qualification|experience|eligibility|requirement|education)\b", re.IGNORECASE),
    re.compile(r"\b(?:responsibilities|duties|powers|functions|role)\b", re.IGNORECASE),
    re.compile(r"\b(?:appointment|transfer|promotion|posting|deputation)\b", re.IGNORECASE),
    re.compile(r"\b(?:reporting\s+(?:to|structure)|reports?\s+to|supervisor|hierarchy)\b", re.IGNORECASE),
    re.compile(r"\b(?:schedule|section|rule|clause|act|regulation|annex)\b", re.IGNORECASE),
    re.compile(r"\b(?:policy|procedure|guideline|manual|code\s+of\s+conduct)\b", re.IGNORECASE),
    re.compile(r"\b(?:pera\s+act|service\s+rules|contractual\s+employees)\b", re.IGNORECASE),
]


def classify_query(question: str) -> QueryType:
    """
    Classify a query as DOCUMENT, STRUCTURED, or HYBRID.

    Uses signal-counting: whichever category has more matching signals wins.
    If both have signals, it's HYBRID.
    """
    q = (question or "").strip()
    if not q:
        return QueryType.DOCUMENT

    structured_hits = sum(1 for pat in _STRUCTURED_SIGNALS if pat.search(q))
    document_hits = sum(1 for pat in _DOCUMENT_SIGNALS if pat.search(q))

    if structured_hits > 0 and document_hits > 0:
        return QueryType.HYBRID
    if structured_hits > 0:
        return QueryType.STRUCTURED
    return QueryType.DOCUMENT
