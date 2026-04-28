"""
PERA AI — Hybrid Reranker (v3: phrase/role aware, optional LLM rerank)

Blends semantic similarity, lexical overlap, document authority, AND
a small set of precision signals (bigram phrase hits, role-token
match, very-short-chunk demotion) to produce more accurate ordering.

Optionally, an LLM-as-pairwise-reranker can rescore the top-N hits
when GROUNDING_JUDGE_MODEL or LLM_RERANK_MODEL is configured. The LLM
path is gated by RERANK_LLM_ENABLED and falls back to the blended
score on any error so production stability is unaffected.
"""
from __future__ import annotations

import os
import re
from typing import Dict, Any, List, Optional

from log_config import get_logger

log = get_logger("pera.reranker")


# Phase 4 config
RERANK_LLM_ENABLED = os.getenv("RERANK_LLM_ENABLED", "0").strip() != "0"
RERANK_LLM_TOP_N = int(os.getenv("RERANK_LLM_TOP_N", "8"))
RERANK_LLM_TIMEOUT_S = float(os.getenv("RERANK_LLM_TIMEOUT_S", "10"))
RERANK_LLM_MAX_CHUNK_CHARS = int(os.getenv("RERANK_LLM_MAX_CHUNK_CHARS", "900"))


_STOPWORDS = {
    "the", "a", "an", "of", "in", "for", "and", "or", "to", "is", "are",
    "was", "were", "be", "been", "being", "this", "that", "these", "those",
    "what", "which", "who", "whom", "whose", "how", "where", "when", "why",
    "do", "does", "did", "with", "from", "by", "on", "at", "as", "it", "its",
    # Urdu / Roman Urdu fillers
    "kya", "hai", "hain", "ki", "ka", "ke", "se", "ko", "ne", "ye", "yeh",
    "kia", "mein", "par", "say", "batao", "bataein",
}

_ROLE_HINT_RE = re.compile(
    r"\b(manager|officer|director|secretary|chairman|chairperson|"
    r"administrator|developer|engineer|operator|inspector|analyst|"
    r"superintendent|coordinator|assistant|deputy|chief|head|registrar)\b",
    re.IGNORECASE,
)


def _norm(s: str) -> str:
    """Normalize text for token matching (supports English + Urdu)."""
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\u0600-\u06FF\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _content_tokens(s: str) -> List[str]:
    """Lower-cased tokens with stopwords stripped, length>1."""
    return [t for t in _norm(s).split() if len(t) > 1 and t not in _STOPWORDS]


def lexical_overlap(q: str, t: str) -> int:
    """Count word-level overlap between query and text."""
    qn = _norm(q)
    tn = _norm(t)
    qset = set(qn.split())
    tset = set(tn.split())
    return len(qset.intersection(tset))


def _bigram_overlap(q_tokens: List[str], t_tokens: List[str]) -> int:
    """Count overlapping consecutive-bigrams between query and text."""
    if len(q_tokens) < 2 or len(t_tokens) < 2:
        return 0
    qb = {(q_tokens[i], q_tokens[i + 1]) for i in range(len(q_tokens) - 1)}
    tb = {(t_tokens[i], t_tokens[i + 1]) for i in range(len(t_tokens) - 1)}
    return len(qb & tb)


def _role_match_count(q_tokens: List[str], t_norm: str) -> int:
    """Count query tokens that are role-class hints AND appear in text."""
    n = 0
    for tok in q_tokens:
        if _ROLE_HINT_RE.fullmatch(tok) and re.search(rf"\b{re.escape(tok)}\b", t_norm):
            n += 1
    return n


def rerank_hits(
    question: str,
    hits: List[Dict[str, Any]],
    sem_weight: float = 0.55,
    lex_weight: float = 0.18,
    authority_weight: float = 0.12,
    phrase_weight: float = 0.10,
    role_weight: float = 0.05,
    lex_cap: int = 12,
) -> List[Dict[str, Any]]:
    """
    Rerank a flat list of hit dicts by blended score (Phase 4 v3).

    Signals (in addition to v2):
      - phrase: bigram overlap between query and chunk (capped at 4)
      - role:   number of role-hint query tokens that appear in chunk
      - length: very-short chunks (<200 chars) get a small precision
                penalty unless they exactly satisfy a phrase or role
                signal (prevents heading-only chunks from dominating)

    Authority handling (unchanged in spirit):
      - authority=3 (official)        -> 1.00
      - authority=2 (policy)          -> 0.67
      - authority=1 (working paper)   -> 0.33
      - When any hit has authority>=2, authority=1 hits get 0.6x demotion.

    Optional LLM-as-pairwise-reranker:
      - Gated by RERANK_LLM_ENABLED. Re-scores the top-N (default 8)
        on top of the blend. Falls back to the blend on any error.

    Adds: _blend, _lex_ov, _phrase_ov, _role_ov to each hit.
    Returns hits sorted by blended score (descending).
    """
    if not hits:
        return hits

    q_tokens = _content_tokens(question)

    # Check if any high-authority evidence exists in this batch
    max_authority = max((int(h.get("doc_authority", 2) or 2) for h in hits), default=2)
    has_authoritative = max_authority >= 2

    for h in hits:
        text = (h.get("text") or "") + "\n" + (h.get("search_text") or "")
        text_norm = _norm(text)
        t_tokens = _content_tokens(text)

        ov = lexical_overlap(question, text)
        phrase_ov = _bigram_overlap(q_tokens, t_tokens)
        role_ov = _role_match_count(q_tokens, text_norm)
        h["_lex_ov"] = ov
        h["_phrase_ov"] = phrase_ov
        h["_role_ov"] = role_ov

        sem = float(h.get("score", 0.0) or 0.0)
        lex_score = min(lex_cap, ov) / lex_cap
        phrase_score = min(4, phrase_ov) / 4.0
        # Role contribution capped at 3 query-side role tokens.
        role_score = min(3, role_ov) / 3.0

        authority = int(h.get("doc_authority", 2) or 2)
        authority_score = authority / 3.0

        blend = (
            (sem_weight * sem)
            + (lex_weight * lex_score)
            + (authority_weight * authority_score)
            + (phrase_weight * phrase_score)
            + (role_weight * role_score)
        )

        # Strong demotion: when authoritative sources exist, penalize low-authority
        if has_authoritative and authority <= 1:
            blend *= 0.60

        # Length precision guard: tiny chunks usually carry headings only
        # and lack enough content for a grounded answer. Demote unless
        # they hit a phrase or role signal that justifies their presence.
        text_len = len((h.get("text") or "").strip())
        if text_len < 200 and phrase_ov == 0 and role_ov == 0:
            blend *= 0.85

        h["_blend"] = blend

    # Sort by blend, then phrase, then lexical overlap, then authority
    hits.sort(
        key=lambda x: (
            float(x.get("_blend", 0.0)),
            int(x.get("_phrase_ov", 0)),
            int(x.get("_lex_ov", 0)),
            int(x.get("doc_authority", 2) or 2),
        ),
        reverse=True,
    )

    # Optional LLM rerank for the top-N (Phase 4).
    # Safe fallback: any failure leaves the blended order untouched.
    if RERANK_LLM_ENABLED and len(hits) >= 2:
        try:
            llm_top = _llm_rerank_top_n(question, hits[:RERANK_LLM_TOP_N])
            if llm_top is not None and len(llm_top) == min(len(hits), RERANK_LLM_TOP_N):
                hits = llm_top + hits[RERANK_LLM_TOP_N:]
        except Exception as e:
            log.warning("LLM rerank failed (%s) — keeping blended order", e)

    if len(hits) >= 2:
        top3 = hits[:3]
        log.info(
            "Reranked %d hits. Top-3 blend: [%.3f, %.3f, %.3f], "
            "phrases: [%d, %d, %d], roles: [%d, %d, %d], authorities: [%d, %d, %d]",
            len(hits),
            top3[0]["_blend"],
            top3[1]["_blend"] if len(top3) > 1 else 0,
            top3[2]["_blend"] if len(top3) > 2 else 0,
            int(top3[0].get("_phrase_ov", 0)),
            int(top3[1].get("_phrase_ov", 0)) if len(top3) > 1 else 0,
            int(top3[2].get("_phrase_ov", 0)) if len(top3) > 2 else 0,
            int(top3[0].get("_role_ov", 0)),
            int(top3[1].get("_role_ov", 0)) if len(top3) > 1 else 0,
            int(top3[2].get("_role_ov", 0)) if len(top3) > 2 else 0,
            int(top3[0].get("doc_authority", 2)),
            int(top3[1].get("doc_authority", 2)) if len(top3) > 1 else 0,
            int(top3[2].get("doc_authority", 2)) if len(top3) > 2 else 0,
        )

    return hits


# -----------------------------------------------------------------------------
# Optional LLM rerank (top-N).
# Disabled by default. When RERANK_LLM_ENABLED=1, asks the judge model
# to rate each candidate's relevance to the query 0..10. Re-orders by
# that rating, breaking ties on the existing blended score. Falls back
# to blended order on any error.
# -----------------------------------------------------------------------------

_LLM_RERANK_PROMPT = """You score how well each PASSAGE answers the QUERY for a government regulatory chatbot (PERA - Punjab Enforcement and Regulatory Authority).

For each passage, output an integer 0..10 where:
- 10 = directly and completely answers the query (or contains the exact value asked for)
- 7-9 = clearly relevant; supports a substantial part of the answer
- 4-6 = related topic but not a direct answer
- 1-3 = same domain but off-topic for this query
- 0 = unrelated

Output EXACTLY one line per passage, in order, formatted as:
ID=<id> SCORE=<int 0-10>

No prose. No headers. No trailing notes."""


def _llm_rerank_top_n(
    question: str,
    top_hits: List[Dict[str, Any]],
) -> Optional[List[Dict[str, Any]]]:
    """Re-score top-N hits with the judge model. Returns the re-ordered
    list on success, or None on any failure (caller falls back).
    """
    if not top_hits:
        return None

    # Lazy import to avoid pulling openai_clients on systems where it
    # would fail at import time (e.g. missing OPENAI_API_KEY in tests).
    try:
        from openai_clients import (
            get_grounding_judge_client,
            get_chat_client,
            GROUNDING_JUDGE_MODEL,
            ANSWER_MODEL,
        )
    except Exception as e:
        log.warning("LLM rerank: openai_clients unavailable (%s)", e)
        return None

    # Build a compact catalog of candidates.
    cat_lines: List[str] = []
    for i, h in enumerate(top_hits):
        snippet = (h.get("text") or "").strip().replace("\n", " ")
        if len(snippet) > RERANK_LLM_MAX_CHUNK_CHARS:
            snippet = snippet[:RERANK_LLM_MAX_CHUNK_CHARS] + "..."
        cat_lines.append(f"ID={i} | {snippet}")

    user_msg = f"QUERY: {question}\n\nPASSAGES:\n" + "\n".join(cat_lines)

    try:
        try:
            client = get_grounding_judge_client()
            model = GROUNDING_JUDGE_MODEL or ANSWER_MODEL
        except Exception:
            client = get_chat_client()
            model = ANSWER_MODEL

        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _LLM_RERANK_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
            max_tokens=200,
            timeout=RERANK_LLM_TIMEOUT_S,
        )
        raw = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        log.warning("LLM rerank: judge call failed (%s)", e)
        return None

    # Parse scores
    score_by_id: Dict[int, int] = {}
    for line in raw.split("\n"):
        m = re.match(r"\s*ID\s*=\s*(\d+)\s*SCORE\s*=\s*(\d+)", line, re.IGNORECASE)
        if not m:
            continue
        try:
            cid = int(m.group(1))
            sc = max(0, min(10, int(m.group(2))))
        except Exception:
            continue
        if 0 <= cid < len(top_hits):
            score_by_id[cid] = sc

    if not score_by_id:
        log.warning("LLM rerank: no valid scores parsed; raw=%r", raw[:160])
        return None

    # Annotate hits and reorder; preserve blend as the tiebreaker.
    annotated = []
    for i, h in enumerate(top_hits):
        h["_llm_rerank"] = float(score_by_id.get(i, 5))  # neutral midpoint default
        annotated.append(h)

    annotated.sort(
        key=lambda x: (float(x.get("_llm_rerank", 5)), float(x.get("_blend", 0.0))),
        reverse=True,
    )

    log.info(
        "LLM rerank applied to top-%d (model=%s). New top-3 LLM scores: [%s]",
        len(annotated), GROUNDING_JUDGE_MODEL or ANSWER_MODEL,
        ", ".join(f"{int(h.get('_llm_rerank', 0))}" for h in annotated[:3]),
    )
    return annotated
