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


import re as _re_for_dispatch
from dataclasses import dataclass as _dc, field as _dc_field
from typing import List as _List


@_dc
class IntentCandidate:
    """Candidate intent emitted by a domain detector with a confidence
    score and the signals that contributed to the score. Used by the
    dispatcher to break ties without changing existing detector APIs.
    """
    domain: str                          # inspection | challan | oa | api
    intent: str                          # raw intent string
    score: float = 0.0
    matched_signals: _List[str] = _dc_field(default_factory=list)
    source: str = ""                     # detector function name


# ── Signal regexes used for scoring ─────────────────────────
_DOMAIN_KW = {
    "inspection": _re_for_dispatch.compile(
        r"\b(inspect(?:ion)?s?|fir(?:s)?|sealed|warning(?:s)?|"
        r"arrest(?:ed|s)?|arrest\s+cases?|giraftari(?:yan|ya[an]?)?|"
        r"removal\s+orders?|epo|no\s+offen[cs]es?|muayina|jaiz[ae]|"
        r"summery|summary|summmery|performance|dashboard|kpis?|"
        r"report\s*card|"
        # Phase-41: financial dashboard metrics — match here so a
        # tehsil-level financial query routes to the inspection
        # dashboard handler (which has the live SDEO merge) rather
        # than to challan_data ranking.
        r"fine\s+amount|fine\s+imposed|fine\s+recovered|"
        r"paid\s+amount|outstanding\s+amount|unpaid\s+amount|"
        r"recover(?:ed|y)\s+amount)\b",
        _re_for_dispatch.I),
    "challan": _re_for_dispatch.compile(
        r"\bch[ae]+l+a+n+s?\b|\boverdue\b|"
        r"\brecovery(?!\s+amount)\b", _re_for_dispatch.I),
    "oa": _re_for_dispatch.compile(
        r"\boperational?\s+activit(?:y|ies)\b|\boperations?\b|"
        r"\brequisition(?:s)?\b", _re_for_dispatch.I),
}
_METRIC_KW = _re_for_dispatch.compile(
    r"\b(fir(?:s)?|sealed|challans?|warnings?|inspections?|"
    r"arrest(?:ed|s)?|epo|removal\s+orders?|no\s+offen[cs]es?|"
    r"paid|unpaid|overdue|recovered|recovery|fine|amount|"
    r"top|highest|lowest|maximum|minimum|most|least)\b",
    _re_for_dispatch.I,
)
_LEVEL_KW = _re_for_dispatch.compile(
    r"\b(tehsils?|stations?|districts?|divisions?)\b", _re_for_dispatch.I,
)
_RANK_KW = _re_for_dispatch.compile(
    r"\b(top\s*\d*|most|highest|maximum|max|largest|biggest|"
    r"sab\s*s[ey]\s*z[iy]ada|sabse\s*z[iy]ada|konsa|kaunsa|which|"
    r"rank(?:ing)?|leading|leader|"
    r"least|lowest|minimum|min|kam\s*(?:ziada|zyada)|sab\s*s[ey]\s*kam)\b",
    _re_for_dispatch.I,
)
_OFFICER_KW = _re_for_dispatch.compile(
    r"\b(officer|officers|inspector|sub[-\s]?inspector|"
    r"by\s+(?:rabia|ahmad|amir|shakeel|maliha|muhammad|m\.|m\s+))",
    _re_for_dispatch.I,
)


def _score_candidate(intent: str, domain: str, question: str,
                     last_turn_domain: str = "") -> IntentCandidate:
    """Apply heuristic scoring rules to a single (intent, domain) pair."""
    signals: _List[str] = []
    score = 0.30  # base score so any matched intent beats no-match
    pat = _DOMAIN_KW.get(domain)
    if pat and pat.search(question or ""):
        score += 0.30
        signals.append("domain_keyword")
    if _METRIC_KW.search(question or ""):
        score += 0.25
        signals.append("metric_keyword")
    if _LEVEL_KW.search(question or ""):
        score += 0.15
        signals.append("level_keyword")
    if _RANK_KW.search(question or ""):
        score += 0.15
        signals.append("rank_keyword")
    if _OFFICER_KW.search(question or ""):
        score += 0.10
        signals.append("officer_keyword")
    # Date range detection (cheap)
    try:
        from pera_dates import parse_date_range
        if parse_date_range(question or ""):
            score += 0.10
            signals.append("date_range")
    except Exception:
        pass
    # Same-domain follow-up bonus
    if last_turn_domain and last_turn_domain == domain:
        score += 0.10
        signals.append("followup_same_domain")
    # Cap on shared-vocabulary intents
    if domain == "oa" and not _DOMAIN_KW["oa"].search(question or ""):
        score = min(score, 0.55)
    if domain in ("inspection", "challan") and "domain_keyword" not in signals:
        score = min(score, 0.60)
    return IntentCandidate(
        domain=domain, intent=intent, score=round(score, 3),
        matched_signals=signals,
    )


def detect_lookup_candidates(
    question: str,
    last_turn_domain: str = "",
) -> _List[IntentCandidate]:
    """Run every domain detector and wrap each non-None result in a
    scored candidate. Returns the candidates sorted by score (desc).
    """
    out: _List[IntentCandidate] = []

    try:
        from operational_activity_lookup import detect_operational_activity_intent
        oa_id = detect_operational_activity_intent(question)
        if oa_id:
            c = _score_candidate(oa_id, "oa", question, last_turn_domain)
            c.source = "detect_operational_activity_intent"
            out.append(c)
    except ImportError:
        pass

    try:
        from inspection_lookup import detect_inspection_intent
        insp_id = detect_inspection_intent(question)
        if insp_id:
            c = _score_candidate(insp_id, "inspection", question, last_turn_domain)
            c.source = "detect_inspection_intent"
            out.append(c)
    except ImportError:
        pass

    try:
        from challan_lookup import detect_challan_intent
        challan_id = detect_challan_intent(question)
        if challan_id:
            c = _score_candidate(challan_id, "challan", question, last_turn_domain)
            c.source = "detect_challan_intent"
            out.append(c)
    except ImportError:
        pass

    try:
        from api_lookup_registry import detect_lookup_intent as _detect
        api_id = _detect(question)
        if api_id:
            c = _score_candidate(api_id, "api", question, last_turn_domain)
            c.source = "api_lookup_registry"
            out.append(c)
    except ImportError:
        pass

    out.sort(key=lambda x: x.score, reverse=True)
    return out


def choose_best_candidate(
    candidates: _List[IntentCandidate],
    last_turn_domain: str = "",
) -> Optional[IntentCandidate]:
    """Pick the highest-scoring candidate. On near-ties (≤0.05), prefer
    the candidate whose domain matches the previous turn.
    """
    if not candidates:
        return None
    best = candidates[0]
    if len(candidates) >= 2:
        runner = candidates[1]
        if (best.score - runner.score) <= 0.05 and last_turn_domain:
            if (runner.domain == last_turn_domain
                    and best.domain != last_turn_domain):
                return runner
    return best


import threading as _threading

# Thread-local diagnostics from the most recent detect_lookup_intent
# call. Lets fastapi_app surface chosen_intent / runner_up / signals to
# the audit log without changing the public detector API.
_LAST_DIAG = _threading.local()


def get_last_intent_diagnostics() -> Dict[str, Any]:
    """Return the diagnostics dict from the most recent
    `detect_lookup_intent` call on this thread, or {} if none.

    Shape:
      {
        "chosen_intent": str,
        "intent_confidence": float,
        "runner_up_intent": str,
        "runner_up_confidence": float,
        "matched_signals": list[str],
        "candidates_count": int,
      }
    """
    return dict(getattr(_LAST_DIAG, "diag", {}) or {})


def _set_last_intent_diagnostics(d: Dict[str, Any]) -> None:
    _LAST_DIAG.diag = d


def detect_lookup_intent(question: str,
                         last_turn_domain: str = "") -> Optional[str]:
    """
    Detect if a question is a known reference/lookup query.

    Returns a ``source_id`` string, or ``None``.

    Production wiring (Task 3): runs every domain detector, scores each,
    picks the highest. Public API kept identical so existing callers
    keep working.

    Pass `last_turn_domain` (string from `LastTurn.domain`) to bias
    near-tie decisions toward the previous turn's domain.

    Diagnostics from the dispatcher (chosen intent, confidence,
    runner-up, matched signals) are stashed thread-locally and can be
    fetched via `get_last_intent_diagnostics()`.
    """
    cands = detect_lookup_candidates(question, last_turn_domain)
    best = choose_best_candidate(cands, last_turn_domain)
    if best is None:
        _set_last_intent_diagnostics({})
        return None
    runner_up = cands[1] if len(cands) > 1 and cands[1] is not best else None
    _set_last_intent_diagnostics({
        "chosen_intent": best.intent,
        "intent_confidence": best.score,
        "runner_up_intent": (runner_up.intent if runner_up else ""),
        "runner_up_confidence": (runner_up.score if runner_up else None),
        "matched_signals": list(best.matched_signals),
        "candidates_count": len(cands),
    })
    return best.intent


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
        # Production wiring: forward deterministic answer + evidence type
        # so the answerer can bypass LLM rendering for ranking/aggregate
        # tables and the numeric validator can compare answer vs payload.
        "deterministic_answer": lookup_result.get("deterministic_answer"),
        "structured_payload": (
            lookup_result.get("structured_payload")
            or consolidated_text
        ),
        "evidence_type": lookup_result.get("evidence_type"),
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
        # Forward deterministic answer if the lookup side produced one.
        "deterministic_answer": lookup_retrieval.get("deterministic_answer"),
        "structured_payload": lookup_retrieval.get("structured_payload"),
        "evidence_type": lookup_retrieval.get("evidence_type"),
        "evidence": merged_evidence,
    }
