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
GROUNDING_EVIDENCE_MAX_CHARS = int(os.getenv("GROUNDING_EVIDENCE_MAX_CHARS", "4000"))
# Phase 4: per-judge-call timeout (seconds). Falls back to "partial" on timeout
# so a slow judge cannot hang an answer.
GROUNDING_JUDGE_TIMEOUT_S = float(os.getenv("GROUNDING_JUDGE_TIMEOUT_S", "12"))


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
    """Boundary-aware check that a claim appears in evidence.

    Phase 2 P0 fix: the prior implementation used plain substring match
    after stripping whitespace/commas, which caused false positives such as
    "Rs. 50,000" matching inside "Rs. 500,000" or "Section 12" matching
    inside "Section 120". Grounding therefore silently reported fabricated
    numbers as supported.

    This version enforces digit-flank boundaries on the normalized form
    and word-flank boundaries on legal references, eliminating those
    false positives while still tolerating comma/space variations.
    """
    claim_stripped = (claim or "").strip()
    if not claim_stripped:
        return False

    claim_norm = _normalize_for_match(claim_stripped)
    evidence_norm = _normalize_for_match(evidence_text)

    if claim_norm:
        # Digit-flank boundaries: "rs50000" must NOT match inside "rs500000".
        # Letter flanks are allowed so "rs50000" can still match "rs 50,000".
        pattern = re.compile(
            r"(?<!\d)" + re.escape(claim_norm) + r"(?!\d)"
        )
        if pattern.search(evidence_norm):
            return True

    # Legal references: operate on the ORIGINAL text so "Section 12" does
    # not silently match "Section 120".
    legal_match = re.match(
        r"(section|clause|rule|article|annex|schedule|chapter)\s*([\divxl]+)",
        claim_stripped.lower()
    )
    if legal_match:
        ref_type, ref_num = legal_match.groups()
        pattern = re.compile(
            rf"\b{re.escape(ref_type)}\s*[-(\s]*{re.escape(ref_num)}(?![\w])",
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

_SEMANTIC_SUPPORT_PROMPT = """You are a STRICT fact-checking judge for a government regulatory system (PERA - Punjab Enforcement and Regulatory Authority). The chatbot serves government users; an unsupported claim that slips through can be cited as authoritative. Your default stance is skeptical.

Given the EVIDENCE (retrieved document passages) and the ANSWER (generated response), determine how well the evidence supports each factual claim in the answer.

Respond in EXACTLY this format (no other text, no Markdown):
SUPPORT: full|combined|partial|none
UNSUPPORTED: <list of specific unsupported claims, one per line, or "none">

Support levels (apply strictly):
- "full" = every factual claim in the answer is directly stated in a single evidence passage. The answer would survive a line-by-line audit against that one passage.
- "combined" = every factual claim is supported by direct logical combination of two or more evidence passages where each fact is itself stated explicitly. Use ONLY when the conclusion follows by direct combination, NOT by inference, plausibility, or domain knowledge.
- "partial" = at least one specific factual claim (a name, number, role, authority, date, pay code, section number, procedure step, eligibility criterion) is NOT explicitly stated in any evidence passage. Default to "partial" if you have any doubt between "combined" and "partial".
- "none" = the answer's central factual content is not found in the evidence, or the evidence is off-topic relative to the question.

WEAK INFERENCE IS NOT SUPPORT:
- "Likely", "probably", "should be", "would suggest" → not support.
- Inferring a value from a related but different value → not support.
- Inferring a role's authority from its seniority → not support.
- Combining a role NAME from one passage with a pay scale that belongs to a DIFFERENT row in the same table → NOT support; this is wrong-entity linkage. Mark as "partial" and list the linkage in UNSUPPORTED.
- Restating the question or adding generic boilerplate is not support.

WHAT TO LIST IN UNSUPPORTED:
- Each specific entity, number, role title, schedule/section reference, monetary value, date, or procedural step that appears in the ANSWER but is NOT explicitly stated in the EVIDENCE.
- Be specific. Quote the offending phrase from the answer.
- If the answer is fully supported, write exactly: UNSUPPORTED: none

DO NOT PENALIZE:
- Pure paraphrase of supported content.
- Honest hedging in the answer ("the documents do not specify X").
- Generic non-claim sentences ("PERA is a regulatory authority")."""


def _semantic_support_check(answer_text: str, context_str: str, question: str = "") -> Dict[str, Any]:
    """
    Tier 2: Ask the JUDGE model to verify if the answer is supported by
    the evidence. Returns
        {"support": "full"|"combined"|"partial"|"none",
         "unsupported_claims": [...]}

    Phase 4 changes:
      • Uses get_grounding_judge_client() and GROUNDING_JUDGE_MODEL so
        the judge can be a different model than the generator. Falls back
        to the chat client + ANSWER_MODEL when no override is set, so
        existing deployments behave identically out of the box.
      • Honors GROUNDING_JUDGE_TIMEOUT_S so a slow judge cannot hang an
        answer; on timeout we return "partial" (cautious) and let the
        composite scorer + classifier decide refusal.
      • On any failure, defaults to "partial" — never to "full" — so
        missing-judge scenarios cannot manufacture confidence.
    """
    try:
        from openai_clients import (
            get_grounding_judge_client,
            get_chat_client,
            GROUNDING_JUDGE_MODEL,
            ANSWER_MODEL,
        )

        bounded_evidence = context_str[:GROUNDING_EVIDENCE_MAX_CHARS]
        bounded_answer = answer_text[:2000]

        user_msg = (
            f"QUESTION: {question}\n\n"
            f"EVIDENCE:\n{bounded_evidence}\n\n"
            f"ANSWER:\n{bounded_answer}"
        )

        # Resolve client + model with safe fallback.
        try:
            client = get_grounding_judge_client()
            model = GROUNDING_JUDGE_MODEL or ANSWER_MODEL
        except Exception as ce:
            log.warning("Judge client init failed (%s) — falling back to chat client", ce)
            client = get_chat_client()
            model = ANSWER_MODEL

        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": _SEMANTIC_SUPPORT_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
            max_tokens=400,
            timeout=GROUNDING_JUDGE_TIMEOUT_S,
        )

        raw = (resp.choices[0].message.content or "").strip()
        log.debug("Judge raw response (model=%s): %s", model, raw[:200])

        # Parse response
        support = "partial"  # default to cautious if parsing yields nothing
        unsupported: List[str] = []

        in_unsupported_block = False
        for line in raw.split("\n"):
            line_stripped = line.strip()
            if not line_stripped:
                continue
            if line_stripped.upper().startswith("SUPPORT:"):
                in_unsupported_block = False
                val = line_stripped.split(":", 1)[1].strip().lower()
                if val in ("full", "combined", "partial", "none"):
                    support = val
            elif line_stripped.upper().startswith("UNSUPPORTED:"):
                val = line_stripped.split(":", 1)[1].strip()
                in_unsupported_block = True
                if val and val.lower() != "none":
                    unsupported.append(val)
            elif in_unsupported_block:
                # continuation line of unsupported claims
                if len(line_stripped) > 3 and line_stripped.lower() != "none":
                    unsupported.append(line_stripped.lstrip("-•*").strip())

        return {"support": support, "unsupported_claims": unsupported[:8]}

    except Exception as e:
        log.warning("Semantic support check failed: %s — defaulting to 'partial'", e)
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

        # Phase 4: tighter mapping. "combined" must clear a higher bar to
        # avoid serving as a soft pass for weak inference. "partial" now
        # contributes less, and "none" near-zero, so unsupported answers
        # cannot ride evidence-quality alone past the grounded threshold.
        if semantic_support == "full":
            claim_ratio = 1.0
            notes.append("Judge: fully supported")
        elif semantic_support == "combined":
            claim_ratio = 0.75  # was 0.85
            notes.append("Judge: supported by direct combination of provisions")
        elif semantic_support == "partial":
            claim_ratio = 0.40  # was 0.50
            notes.append("Judge: partially supported")
            unsupported.extend(sem_unsupported)
        else:  # "none"
            claim_ratio = 0.05  # was 0.10
            notes.append("Judge: NOT supported by evidence")
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

    # Phase 4: smoother per-claim penalty in place of the old "≥3 cliff".
    # 0 unsupported → no penalty
    # 1 unsupported → 10% off
    # 2 unsupported → 20% off
    # 3+ unsupported → capped at 35% off (still leaves a refusal pathway)
    n_unsupported = len(unsupported)
    if n_unsupported > 0:
        penalty = min(0.35, 0.10 * n_unsupported)
        score *= (1.0 - penalty)
        notes.append(f"{n_unsupported} unsupported claim(s); penalty=-{int(penalty*100)}%")

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
