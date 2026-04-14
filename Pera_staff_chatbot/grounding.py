"""
PERA AI — Post-Generation Grounding Verification (v2: semantic support)

Two-tier verification:
  Tier 1: Fast regex claim extraction + substring matching (for numeric claims)
  Tier 2: LLM-as-judge semantic support check (for narrative claims when
           Tier 1 has nothing to check — which is 89.9% of the time)

Gated by GROUNDING_SEMANTIC_ENABLED env var (default "1").
Budget-controlled: max 1 LLM call per answer, evidence bounded to 4000 chars.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set

from log_config import get_logger

log = get_logger("pera.grounding")

# ── Config ────────────────────────────────────────────────────────────────────
GROUNDING_SEMANTIC_ENABLED = os.getenv("GROUNDING_SEMANTIC_ENABLED", "1").strip() != "0"
GROUNDING_EVIDENCE_MAX_CHARS = int(os.getenv("GROUNDING_EVIDENCE_MAX_CHARS", "6000"))


# ── Result ────────────────────────────────────────────────────────────────────

@dataclass
class GroundingResult:
    """Output of the grounding verifier."""
    grounded: bool                     # overall pass/fail
    confidence: str                    # "high" | "medium" | "low" | "unverifiable"
    score: float                       # 0.0–1.0
    total_claims: int = 0
    supported_claims: int = 0
    unsupported_claims: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    semantic_support: Optional[str] = None  # "full" | "combined" | "partial" | "none" | None


# ── Claim Extraction (Tier 1 — fast regex) ────────────────────────────────────

_NUMBER_RE = re.compile(
    r"(?:Rs\.?\s*[\d,]+(?:\.\d+)?|"
    r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|"
    r"\d+(?:\.\d+)?%|"
    r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|"
    r"(?:section|clause|rule|article|annex|schedule|chapter)\s*[\dIVXLivxl]+(?:\s*[\(\)a-zA-Z])?)",
    re.IGNORECASE
)

_AUTHORITY_RE = re.compile(
    r"(?:Director\s+General|Additional\s+Director\s+General|"
    r"Chief\s+Technology\s+Officer|Enforcement\s+Officer|"
    r"Investigation\s+Officer|Competent\s+Authority|"
    r"Chairman|Authority|Board|Government|Governor|"
    r"System\s+Support\s+Officer|Manager|Secretary)",
    re.IGNORECASE
)


def _extract_claims(answer_text: str) -> List[str]:
    """Extract specific, verifiable claims from the answer."""
    claims: List[str] = []
    for m in _NUMBER_RE.finditer(answer_text):
        claim = m.group(0).strip()
        if len(claim) >= 2:
            claims.append(claim)
    return list(set(claims))


def _normalize_for_match(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[,\s]+", "", s)
    return s


def _claim_in_evidence(claim: str, evidence_text: str) -> bool:
    claim_norm = _normalize_for_match(claim)
    evidence_norm = _normalize_for_match(evidence_text)
    if claim_norm in evidence_norm:
        return True
    legal_match = re.match(
        r"(section|clause|rule|article|annex|schedule|chapter)\s*([\divxl]+)",
        claim.lower()
    )
    if legal_match:
        ref_type, ref_num = legal_match.groups()
        pattern = re.compile(
            rf"{re.escape(ref_type)}\s*[-(\s]*{re.escape(ref_num)}",
            re.IGNORECASE
        )
        if pattern.search(evidence_text):
            return True
    return False


# ── Evidence Quality Assessment ───────────────────────────────────────────────

def _assess_evidence_quality(evidence_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not evidence_list:
        return {"has_evidence": False, "n_docs": 0, "top_score": 0.0,
                "avg_score": 0.0, "conflict_risk": False}

    all_scores = []
    doc_names: Set[str] = set()

    for doc_group in evidence_list:
        doc_names.add(doc_group.get("doc_name", ""))
        for hit in doc_group.get("hits", []):
            s = hit.get("score", 0.0)
            if not hit.get("_is_smart_context", False):
                all_scores.append(float(s))

    top_score = max(all_scores) if all_scores else 0.0
    avg_score = sum(all_scores) / len(all_scores) if all_scores else 0.0

    doc_top_scores = {}
    for doc_group in evidence_list:
        dn = doc_group.get("doc_name", "")
        doc_top_scores[dn] = float(doc_group.get("max_score", 0))

    conflict_risk = False
    top_docs = sorted(doc_top_scores.values(), reverse=True)
    if len(top_docs) >= 2:
        if abs(top_docs[0] - top_docs[1]) < 0.05 and top_docs[1] > 0.3:
            conflict_risk = True

    return {
        "has_evidence": True,
        "n_docs": len(doc_names),
        "n_hits": len(all_scores),
        "top_score": top_score,
        "avg_score": avg_score,
        "conflict_risk": conflict_risk,
    }


# ── Tier 2: LLM-as-judge semantic support check ──────────────────────────────

_SEMANTIC_SUPPORT_PROMPT = """You are a fact-checking judge for a government regulatory system (PERA - Punjab Enforcement and Regulatory Authority).

Given the EVIDENCE (retrieved documents) and the ANSWER (generated response), determine how well the evidence supports the answer.

Respond in EXACTLY this format (no other text):
SUPPORT: full|combined|partial|none
UNSUPPORTED: <list of unsupported claims, one per line, or "none">

Support levels:
- "full" = every factual claim in the answer is directly stated in a single evidence passage
- "combined" = the answer is correctly derived by combining information from multiple evidence passages, clauses, or provisions. This is VALID support — regulatory answers often require reading multiple related provisions together.
- "partial" = some claims are supported but other specific claims are not found in the evidence
- "none" = the answer makes specific factual claims not found in any evidence passage

CRITICAL rules:
- If the answer synthesizes information from multiple evidence passages to form a correct conclusion, that is "combined" support, NOT "partial" or "none"
- Only mark as "partial" or "none" if the answer contains specific facts (names, numbers, procedures, roles) that genuinely do NOT appear in any evidence
- Generic/obvious statements ("PERA is an authority") do not count as unsupported
- Do NOT penalize answers for not quoting evidence verbatim — paraphrasing supported content is acceptable"""


def _semantic_support_check(answer_text: str, context_str: str, question: str = "") -> Dict[str, Any]:
    """
    Tier 2: Ask LLM to verify if the answer is supported by the evidence.
    Returns {"support": "full"|"partial"|"none", "unsupported_claims": [...]}

    Budget-controlled: evidence bounded to GROUNDING_EVIDENCE_MAX_CHARS.
    """
    try:
        from openai_clients import get_chat_client, ANSWER_MODEL

        bounded_evidence = context_str[:GROUNDING_EVIDENCE_MAX_CHARS]
        bounded_answer = answer_text[:2000]

        user_msg = (
            f"QUESTION: {question}\n\n"
            f"EVIDENCE:\n{bounded_evidence}\n\n"
            f"ANSWER:\n{bounded_answer}"
        )

        client = get_chat_client()
        resp = client.chat.completions.create(
            model=ANSWER_MODEL,
            messages=[
                {"role": "system", "content": _SEMANTIC_SUPPORT_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
            max_tokens=300,
        )

        raw = (resp.choices[0].message.content or "").strip()
        log.debug("Semantic support check raw response: %s", raw[:200])

        # Parse response
        support = "partial"  # default to cautious
        unsupported: List[str] = []

        for line in raw.split("\n"):
            line = line.strip()
            if line.upper().startswith("SUPPORT:"):
                val = line.split(":", 1)[1].strip().lower()
                if val in ("full", "combined", "partial", "none"):
                    support = val
            elif line.upper().startswith("UNSUPPORTED:"):
                val = line.split(":", 1)[1].strip()
                if val.lower() != "none" and val:
                    unsupported.append(val)
            elif unsupported is not None and line and not line.startswith("SUPPORT"):
                # continuation line of unsupported claims
                if line.lower() != "none" and len(line) > 5:
                    unsupported.append(line)

        return {"support": support, "unsupported_claims": unsupported[:5]}

    except Exception as e:
        log.warning("Semantic support check failed: %s", e)
        return {"support": "partial", "unsupported_claims": []}


# ── Main Verifier ─────────────────────────────────────────────────────────────

def verify_grounding(
    answer_text: str,
    evidence_list: List[Dict[str, Any]],
    context_str: str,
    question: str = "",
) -> GroundingResult:
    """
    Verify that the generated answer is grounded in evidence.

    Two-tier approach:
      - Tier 1: Regex claim extraction (fast, for numbers/dates/legal refs)
      - Tier 2: LLM semantic check (only when Tier 1 finds 0 claims — the 89.9% case)
    """
    if not answer_text or not answer_text.strip():
        return GroundingResult(grounded=False, confidence="unverifiable", score=0.0,
                               notes=["Empty answer"])

    # 1. Assess evidence quality
    eq = _assess_evidence_quality(evidence_list)
    notes: List[str] = []

    if not eq["has_evidence"]:
        return GroundingResult(grounded=False, confidence="unverifiable", score=0.0,
                               notes=["No evidence available"])

    # 2. Tier 1: Extract verifiable claims (numbers, dates, etc.)
    claims = _extract_claims(answer_text)
    supported = 0
    unsupported: List[str] = []

    for claim in claims:
        if _claim_in_evidence(claim, context_str):
            supported += 1
        else:
            unsupported.append(claim)

    total = len(claims)
    semantic_support = None

    # 3. Tier 2: If Tier 1 found zero claims, use LLM semantic check
    if total == 0 and GROUNDING_SEMANTIC_ENABLED:
        log.info("Tier 1 found 0 extractable claims. Running Tier 2 semantic check.")
        sem_result = _semantic_support_check(answer_text, context_str, question)
        semantic_support = sem_result.get("support", "partial")
        sem_unsupported = sem_result.get("unsupported_claims", [])

        if semantic_support == "full":
            claim_ratio = 1.0
            notes.append("Semantic check: fully supported")
        elif semantic_support == "combined":
            claim_ratio = 0.85
            notes.append("Semantic check: supported by combining multiple provisions")
        elif semantic_support == "partial":
            claim_ratio = 0.5
            notes.append("Semantic check: partially supported")
            unsupported.extend(sem_unsupported)
        else:  # "none"
            claim_ratio = 0.1
            notes.append("Semantic check: NOT supported by evidence")
            unsupported.extend(sem_unsupported)
    elif total == 0:
        # Semantic disabled, no claims — cautious medium
        claim_ratio = 0.6
        notes.append("No extractable claims; semantic check disabled")
    else:
        claim_ratio = supported / total

    # 4. Composite score
    ev_quality_score = min(1.0, eq["top_score"] / 0.6)
    score = (0.6 * claim_ratio) + (0.4 * ev_quality_score)

    # Penalties
    if eq["conflict_risk"]:
        score *= 0.85
        notes.append("Conflict risk: multiple docs with similar relevance")

    if eq["avg_score"] < 0.25:
        score *= 0.9
        notes.append("Low average evidence score")

    if len(unsupported) >= 3:
        score *= 0.8
        notes.append(f"{len(unsupported)} unsupported claims")

    # 5. Classify confidence
    if score >= 0.75:
        confidence = "high"
    elif score >= 0.50:
        confidence = "medium"
    elif score >= 0.30:
        confidence = "low"
    else:
        confidence = "unverifiable"

    grounded = score >= 0.30

    result = GroundingResult(
        grounded=grounded,
        confidence=confidence,
        score=round(score, 3),
        total_claims=total,
        supported_claims=supported,
        unsupported_claims=unsupported[:5],
        notes=notes,
        semantic_support=semantic_support,
    )

    log.info("Grounding check: score=%.3f confidence=%s claims=%d/%d semantic=%s grounded=%s",
             result.score, result.confidence, supported, total,
             semantic_support or "n/a", result.grounded)
    if unsupported:
        log.debug("Unsupported claims: %s", unsupported[:5])

    return result
