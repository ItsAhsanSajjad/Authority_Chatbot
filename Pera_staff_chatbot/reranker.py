"""
PERA AI — Hybrid Reranker (v3: authority-aware + deduplication)

Blends semantic similarity scores with lexical overlap and document
authority to produce more accurate relevance ranking.

v3 changes: adds near-duplicate deduplication to prevent the same
content (e.g., salary tables appearing on multiple pages of the
Working Paper) from flooding the evidence window.
"""
from __future__ import annotations

import re
from typing import Dict, Any, List

from log_config import get_logger

log = get_logger("pera.reranker")


def _norm(s: str) -> str:
    """Normalize text for token matching (supports English + Urdu)."""
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\u0600-\u06FF\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def lexical_overlap(q: str, t: str) -> int:
    """Count word-level overlap between query and text."""
    qn = _norm(q)
    tn = _norm(t)
    qset = set(qn.split())
    tset = set(tn.split())
    return len(qset.intersection(tset))


def rerank_hits(
    question: str,
    hits: List[Dict[str, Any]],
    sem_weight: float = 0.65,
    lex_weight: float = 0.20,
    authority_weight: float = 0.15,
    lex_cap: int = 12,
) -> List[Dict[str, Any]]:
    """
    Rerank a flat list of hit dicts by blended score.

    Each hit dict should have:
      - 'score': float (semantic similarity)
      - 'text': str
      - 'search_text': str (optional)
      - 'doc_authority': int (1-3, optional)

    Authority handling:
      - authority=3 (official) -> full authority score
      - authority=2 (policy) -> 0.67 authority score
      - authority=1 (working paper) -> 0.33 authority score
      - When higher-authority evidence exists (any hit with authority>=2),
        authority=1 hits get a 0.6x penalty multiplier on their blend score

    Adds '_blend' and '_lex_ov' to each hit.
    Returns hits sorted by blended score (descending).
    """
    if not hits:
        return hits

    # Check if any high-authority evidence exists in this batch
    max_authority = max((int(h.get("doc_authority", 2) or 2) for h in hits), default=2)
    has_authoritative = max_authority >= 2

    for h in hits:
        text = (h.get("text") or "") + "\n" + (h.get("search_text") or "")
        ov = lexical_overlap(question, text)
        h["_lex_ov"] = ov

        sem = float(h.get("score", 0.0) or 0.0)
        lex_score = min(lex_cap, ov) / lex_cap

        # Authority normalization (1->0.33, 2->0.67, 3->1.0)
        authority = int(h.get("doc_authority", 2) or 2)
        authority_score = authority / 3.0

        blend = (sem_weight * sem) + (lex_weight * lex_score) + (authority_weight * authority_score)

        # Strong demotion: when authoritative sources exist, penalize low-authority
        if has_authoritative and authority <= 1:
            blend *= 0.60

        h["_blend"] = blend

    # Sort by blend, then lexical overlap, then authority (all descending)
    hits.sort(
        key=lambda x: (
            float(x.get("_blend", 0.0)),
            int(x.get("_lex_ov", 0)),
            int(x.get("doc_authority", 2) or 2),
        ),
        reverse=True,
    )

    # Deduplicate near-identical chunks (same content from different pages)
    before_dedup = len(hits)
    hits = _deduplicate_hits(hits)
    if len(hits) < before_dedup:
        log.info("Deduplication removed %d near-duplicate hits", before_dedup - len(hits))

    if len(hits) >= 2:
        top3 = hits[:3]
        log.info(
            "Reranked %d hits. Top-3 blend: [%.3f, %.3f, %.3f], authorities: [%d, %d, %d]",
            len(hits),
            top3[0]["_blend"],
            top3[1]["_blend"] if len(top3) > 1 else 0,
            top3[2]["_blend"] if len(top3) > 2 else 0,
            int(top3[0].get("doc_authority", 2)),
            int(top3[1].get("doc_authority", 2)) if len(top3) > 1 else 0,
            int(top3[2].get("doc_authority", 2)) if len(top3) > 2 else 0,
        )

    return hits


def _deduplicate_hits(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Remove near-duplicate hits based on normalized text prefix.
    Keeps the first (highest-scored) occurrence of each content fingerprint.
    """
    seen = set()
    deduped = []
    for h in hits:
        # Use first 200 chars of normalized text as fingerprint
        text_key = _norm((h.get("text") or "")[:200])
        if text_key in seen:
            continue
        seen.add(text_key)
        deduped.append(h)
    return deduped
