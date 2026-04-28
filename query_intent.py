"""
PERA AI — Phase 5: Query Intent Routing, Vague-Query Expansion,
Multi-Part Decomposition

Lightweight, deterministic-first layer that runs BEFORE retrieval.

Design principles:
  - Deterministic regex/heuristic path is the default (fast, auditable,
    zero latency added in the common case).
  - Optional LLM-assisted expansion path for vague/short queries, gated
    by INTENT_LLM_EXPAND_ENABLED. Bounded, with a hard timeout, and a
    safe fallback to the deterministic expansion if anything fails.
  - Output is advisory: it informs retrieval and answerer prompting,
    it does NOT override Phase 2 grounding/refusal logic.

Public API:
  - classify_intent(question, *, has_followup_context=False) -> IntentResult
  - expand_vague_query(question, intent) -> List[str]
  - split_multi_part(question) -> List[str]
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import List, Optional

from log_config import get_logger

log = get_logger("pera.intent")


# ── Config ────────────────────────────────────────────────────────────────────

INTENT_LLM_EXPAND_ENABLED = os.getenv("INTENT_LLM_EXPAND_ENABLED", "0").strip() != "0"
INTENT_LLM_TIMEOUT_S = float(os.getenv("INTENT_LLM_TIMEOUT_S", "8"))
INTENT_LLM_MAX_EXPANSIONS = int(os.getenv("INTENT_LLM_MAX_EXPANSIONS", "3"))
INTENT_VAGUE_MAX_TOKENS = int(os.getenv("INTENT_VAGUE_MAX_TOKENS", "3"))


# ── Intent vocabulary ────────────────────────────────────────────────────────

INTENT_FACTUAL    = "factual"
INTENT_ROLE_INFO  = "role_info"
INTENT_PROCEDURAL = "procedural"
INTENT_COMPARISON = "comparison"
INTENT_LIST       = "list"
INTENT_VAGUE      = "vague"
INTENT_FOLLOWUP   = "followup"
INTENT_OUT_OF_SCOPE = "out_of_scope"
INTENT_SMALLTALK  = "smalltalk"  # smalltalk_intent.py handles upstream; kept for completeness


@dataclass
class IntentResult:
    """Output of classify_intent.

    Fields:
      primary: one of the INTENT_* constants
      is_multi_part: True when the query asks about multiple aspects
      sub_queries: rule-based decomposition (empty if not multi-part)
      is_vague: True when the query is short/broad/underspecified
      expansions: candidate retrieval queries derived from the original
                  (always includes the original query as element 0)
      reasons: short human-readable trace for audit/log
    """
    primary: str = INTENT_FACTUAL
    is_multi_part: bool = False
    sub_queries: List[str] = field(default_factory=list)
    is_vague: bool = False
    expansions: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)


# ── Deterministic detection patterns ─────────────────────────────────────────

# Comparison signals
_COMPARISON_RE = re.compile(
    r"\b(compare|comparison|vs\.?|versus|difference\s+between|differences?\s+between|"
    r"differ\s+from|differ\s+between|how\s+(?:does|do)\s+\w+\s+(?:differ|compare))\b",
    re.IGNORECASE,
)

# Procedural / process signals
_PROCEDURAL_RE = re.compile(
    r"\b(procedure|process|steps?|how\s+to|how\s+do\s+i|how\s+does\s+\w+|"
    r"workflow|protocol|method|stage[s]?|file\s+a\s+complaint|apply\s+for)\b",
    re.IGNORECASE,
)

# List / exhaustive signals
_LIST_RE = re.compile(
    r"\b(list|all\s+(?:of\s+)?the|enumerate|every|complete\s+list|"
    r"give\s+me\s+all|provide\s+a\s+list|what\s+are\s+the|"
    r"which\s+(?:are\s+)?the)\b",
    re.IGNORECASE,
)

# Role detection (mirrors retriever / answerer role hints, intentionally
# kept simple here — full detection lives in answerer._detect_target_role)
_ROLE_HINT_RE = re.compile(
    r"\b(manager|officer|director|secretary|chairman|chairperson|administrator|"
    r"developer|engineer|operator|inspector|analyst|superintendent|coordinator|"
    r"assistant|deputy|chief|head|registrar|cto|dg|adg|ddg|sso|deo|eo|io|sdeo)\b",
    re.IGNORECASE,
)

# Multi-part conjunction signals (English + Roman Urdu)
_MULTI_PART_CONJ_RE = re.compile(
    r"(?:\s+and\s+|\s*&\s*|\s*,\s*also\s+|\s+plus\s+|\s+as\s+well\s+as\s+|"
    r"\s+aur\s+)",  # 'aur' = Roman Urdu 'and'
    re.IGNORECASE,
)

# Multi-aspect tokens that frequently couple in PERA HR queries
_MULTI_ASPECT_TOKENS = (
    "salary", "pay", "scale", "qualification", "qualifications", "experience",
    "duties", "powers", "functions", "responsibilities", "appointment",
    "eligibility", "benefits", "allowance", "kpi", "kpis", "report",
    "reporting", "tenure", "term",
)

# Out-of-scope topical markers
_OUT_OF_SCOPE_RE = re.compile(
    r"\b(weather|football|cricket|movie|movies?|recipe|cook(?:ing)?|"
    r"stock\s+market|bitcoin|crypto|joke|song|lyric[s]?|prime\s+minister\s+of|"
    r"president\s+of|capital\s+of|population\s+of)\b",
    re.IGNORECASE,
)

# Vague / broad-topic markers — single-noun queries that map to
# whole sections of the corpus
_BROAD_TOPIC_HINTS = (
    "procurement", "leave", "complaint", "complaints", "discipline",
    "training", "promotion", "transfer", "posting", "appointment",
    "powers", "duties", "structure", "organisation", "organization",
    "act", "schedule", "annex", "annexure", "rule", "rules",
    "regulation", "regulations", "policy", "policies", "ethics",
    "conduct", "compliance", "audit", "budget", "finance",
)


def _word_count(s: str) -> int:
    return len([t for t in re.split(r"\s+", (s or "").strip()) if t])


def _strip_qmark(s: str) -> str:
    return re.sub(r"[?!.\s]+$", "", s or "").strip()


# ── classify_intent ──────────────────────────────────────────────────────────

def classify_intent(
    question: str,
    *,
    has_followup_context: bool = False,
) -> IntentResult:
    """Classify the user's query into an IntentResult.

    Deterministic and fast. No LLM call. Safe to invoke on every request.

    `has_followup_context=True` should be passed when the upstream pipeline
    detected a pronoun-anchored follow-up; we use that signal but do not
    over-rule it.
    """
    q = (question or "").strip()
    if not q:
        return IntentResult(primary=INTENT_FACTUAL, reasons=["empty"])

    q_lower = q.lower()
    q_naked = _strip_qmark(q_lower)
    n_words = _word_count(q_naked)
    reasons: List[str] = []

    # 1. Out-of-scope topical markers (cheap early exit).
    if _OUT_OF_SCOPE_RE.search(q_lower):
        reasons.append("out_of_scope_marker")
        return IntentResult(primary=INTENT_OUT_OF_SCOPE, reasons=reasons)

    # 2. Comparison
    if _COMPARISON_RE.search(q_lower):
        reasons.append("comparison_keyword")
        sub_queries = split_multi_part(q)
        return IntentResult(
            primary=INTENT_COMPARISON,
            is_multi_part=len(sub_queries) > 1,
            sub_queries=sub_queries,
            reasons=reasons,
        )

    # 3. Procedural
    is_procedural = bool(_PROCEDURAL_RE.search(q_lower))
    # 4. List
    is_list = bool(_LIST_RE.search(q_lower))
    # 5. Role-info
    has_role_hint = bool(_ROLE_HINT_RE.search(q_lower))

    # 6. Multi-part detection (regardless of primary)
    sub_queries = split_multi_part(q)
    is_multi_part = len(sub_queries) > 1

    # 7. Vague: short query OR broad-topic single noun
    is_vague = False
    if n_words <= INTENT_VAGUE_MAX_TOKENS:
        is_vague = True
        reasons.append(f"short_query({n_words}w)")
    else:
        # Even longer queries can be vague if they are only broad-topic
        # nouns + filler ("tell me about procurement").
        content_tokens = [
            t for t in re.split(r"\W+", q_naked)
            if len(t) > 2
            and t not in {"tell", "the", "and", "what", "is", "are", "about",
                          "give", "show", "explain", "describe", "kya", "hai",
                          "btao", "batao"}
        ]
        if content_tokens and len(content_tokens) <= 2 and any(
            t in _BROAD_TOPIC_HINTS for t in content_tokens
        ):
            is_vague = True
            reasons.append("broad_topic_only")

    # 8. Choose primary
    if has_followup_context and n_words <= 6:
        primary = INTENT_FOLLOWUP
        reasons.append("followup_context")
    elif is_procedural:
        primary = INTENT_PROCEDURAL
        reasons.append("procedural_keyword")
    elif is_list:
        primary = INTENT_LIST
        reasons.append("list_keyword")
    elif has_role_hint and not is_vague:
        primary = INTENT_ROLE_INFO
        reasons.append("role_hint")
    elif is_vague:
        primary = INTENT_VAGUE
    else:
        primary = INTENT_FACTUAL

    result = IntentResult(
        primary=primary,
        is_multi_part=is_multi_part,
        sub_queries=sub_queries,
        is_vague=is_vague,
        reasons=reasons,
    )

    # Always populate expansions (deterministic). Caller may upgrade with LLM.
    result.expansions = expand_vague_query(question, result)

    log.info(
        "Intent: primary=%s vague=%s multi_part=%s subs=%d reasons=%s",
        primary, is_vague, is_multi_part, len(sub_queries), reasons,
    )
    return result


# ── Multi-part splitter ──────────────────────────────────────────────────────

def split_multi_part(question: str) -> List[str]:
    """Rule-based decomposition of a multi-aspect query into sub-queries.

    Examples:
      "salary and qualifications of CTO"
        -> ["salary of CTO", "qualifications of CTO"]
      "compare DG vs CTO"
        -> ["DG", "CTO"]
      "powers and duties of Director General"
        -> ["powers of Director General", "duties of Director General"]

    Returns [question] when no decomposition applies.

    Deterministic; never raises.
    """
    q = (question or "").strip()
    if not q:
        return []
    q_clean = _strip_qmark(q)

    # vs / versus / compare X with Y splits
    m = re.match(
        r"^\s*(?:compare\s+|comparison\s+(?:of\s+|between\s+))?(.+?)"
        r"\s+(?:vs\.?|versus|with|and)\s+(.+?)\s*$",
        q_clean, re.IGNORECASE,
    )
    if m and _COMPARISON_RE.search(q_clean):
        a, b = m.group(1).strip(), m.group(2).strip()
        if a and b and a.lower() != b.lower():
            return [a, b]

    # "X and Y of Z"  or  "X and Y for Z"  patterns
    # First check for the multi-aspect coupling at the START of the query.
    m2 = re.match(
        r"^\s*(?:what\s+(?:is|are)\s+the\s+|tell\s+me\s+the\s+|give\s+me\s+the\s+)?"
        r"(.+?)\s+(?:and|&|aur)\s+(.+?)\s+(of|for|about)\s+(.+?)\s*$",
        q_clean, re.IGNORECASE,
    )
    if m2:
        aspect_a = m2.group(1).strip()
        aspect_b = m2.group(2).strip()
        connector = m2.group(3).strip()
        target = m2.group(4).strip()
        # Sanity: each aspect must contain a known multi-aspect token.
        a_low = aspect_a.lower()
        b_low = aspect_b.lower()
        a_has = any(tok in a_low for tok in _MULTI_ASPECT_TOKENS)
        b_has = any(tok in b_low for tok in _MULTI_ASPECT_TOKENS)
        if a_has and b_has and target:
            return [
                f"{aspect_a} {connector} {target}",
                f"{aspect_b} {connector} {target}",
            ]

    # Generic " and / aur / & " split, when both halves are short and look
    # like complete asks ( contains a multi-aspect token OR a role hint).
    parts = _MULTI_PART_CONJ_RE.split(q_clean)
    if len(parts) == 2:
        a, b = parts[0].strip(), parts[1].strip()
        if a and b:
            a_low, b_low = a.lower(), b.lower()
            a_keep = (
                any(tok in a_low for tok in _MULTI_ASPECT_TOKENS)
                or _ROLE_HINT_RE.search(a_low)
            )
            b_keep = (
                any(tok in b_low for tok in _MULTI_ASPECT_TOKENS)
                or _ROLE_HINT_RE.search(b_low)
            )
            if a_keep and b_keep and len(a.split()) >= 1 and len(b.split()) >= 1:
                return [a, b]

    return [q]


# ── Vague-query expansion ────────────────────────────────────────────────────

# Deterministic fan-outs for very common broad topics in PERA's corpus.
# Each entry is a list of phrasings the documents are likely to use.
_BROAD_TOPIC_EXPANSIONS = {
    "procurement": [
        "procurement procedure PERA",
        "purchase rules and tendering",
        "procurement under PERA Act",
    ],
    "leave": [
        "leave policy at PERA",
        "leave entitlement and types of leave",
        "leave rules and approval authority",
    ],
    "complaint": [
        "complaint handling procedure",
        "filing a complaint at PERA",
        "complaint redressal mechanism",
    ],
    "complaints": [
        "complaint handling procedure",
        "filing a complaint at PERA",
        "complaint redressal mechanism",
    ],
    "discipline": [
        "disciplinary action and proceedings",
        "misconduct and penalties under PERA rules",
    ],
    "training": [
        "training and capacity building",
        "training programmes and learning policy",
    ],
    "promotion": [
        "promotion criteria and eligibility",
        "promotion rules under Schedule-II",
    ],
    "transfer": [
        "transfer and posting policy",
        "Schedule-V Transfer and Posting",
    ],
    "appointment": [
        "appointment and conditions of service",
        "Schedule-II appointment rules",
    ],
    "powers": [
        "powers and functions of the Authority",
        "powers under PERA Act",
    ],
    "duties": [
        "duties and responsibilities",
        "key responsibilities and duties",
    ],
    "structure": [
        "organisational structure",
        "Schedule-I Organisational Structure",
    ],
    "schedule": [
        "Schedule-I Organisational Structure",
        "Schedule-II Appointment and Conditions of Service",
        "Schedule-III Special Pay Package PERA (SPPP)",
    ],
    "ethics": [
        "code of conduct and ethics",
        "ethical standards for officers",
    ],
    "conduct": [
        "code of conduct",
        "professional conduct and discipline",
    ],
}


def expand_vague_query(question: str, intent: IntentResult) -> List[str]:
    """Return a list of retrieval queries derived from `question`.

    Element 0 is always the original query (so callers can always rely
    on it being present). Additional expansions are appended only when
    the intent indicates vagueness or a broad-topic single noun.

    Deterministic. Never raises.
    """
    q = (question or "").strip()
    if not q:
        return []

    out: List[str] = [q]
    seen = {q.lower()}

    if not intent.is_vague:
        return out

    q_lower = q.lower()
    q_naked = _strip_qmark(q_lower)

    # Broad-topic noun fan-out
    for tok, expansions in _BROAD_TOPIC_EXPANSIONS.items():
        if re.search(rf"\b{re.escape(tok)}\b", q_naked):
            for e in expansions:
                if e.lower() not in seen:
                    out.append(e)
                    seen.add(e.lower())
            break  # only the first broad-topic match fires; no exponential blow-up

    # Generic "tell me about X" → "X overview", "definition of X"
    if len(out) == 1:
        m = re.match(
            r"^(?:tell\s+me\s+about|what\s+is|what\s+are|explain|describe|"
            r"give\s+me\s+(?:an\s+)?overview\s+of|btao|batao|kya\s+hai)"
            r"\s+(?:the\s+)?(.+)$",
            q_naked, re.IGNORECASE,
        )
        if m:
            topic = m.group(1).strip()
            for tmpl in (f"{topic} at PERA", f"definition of {topic}", f"overview of {topic}"):
                if tmpl.lower() not in seen:
                    out.append(tmpl)
                    seen.add(tmpl.lower())

    return out[:5]  # hard cap so retrieval cost stays bounded


# ── Optional LLM-assisted expansion (gated) ──────────────────────────────────

_LLM_EXPAND_PROMPT = """You generate alternative search queries for a government regulatory chatbot (PERA - Punjab Enforcement and Regulatory Authority).

Given the USER QUERY below, output up to N short rephrasings (one per line) that a regulatory document is likely to contain. Each rephrasing should:
- be a noun phrase or a short question
- use the formal terminology a government document would use
- NOT ask the question, NOT include explanatory text

Do not invent specific role names, schedule numbers, sections, or values
that are not in the user's query. Only rephrase what the user asked.

Output rules:
- One rephrasing per line, no numbering, no bullets
- Maximum N lines
- No prose before or after"""


def llm_expand_vague_query(
    question: str,
    n: int = INTENT_LLM_MAX_EXPANSIONS,
) -> Optional[List[str]]:
    """Optional LLM expansion of a vague query.

    Disabled by default. Gated by INTENT_LLM_EXPAND_ENABLED.
    Returns None on any failure so caller falls back to the
    deterministic expansion.
    """
    if not INTENT_LLM_EXPAND_ENABLED:
        return None
    q = (question or "").strip()
    if not q:
        return None
    try:
        from openai_clients import (
            get_grounding_judge_client,
            get_chat_client,
            GROUNDING_JUDGE_MODEL,
            ANSWER_MODEL,
        )
    except Exception as e:
        log.warning("LLM expand: openai_clients unavailable (%s)", e)
        return None

    try:
        try:
            client = get_grounding_judge_client()
            model = GROUNDING_JUDGE_MODEL or ANSWER_MODEL
        except Exception:
            client = get_chat_client()
            model = ANSWER_MODEL
        prompt = _LLM_EXPAND_PROMPT.replace("N", str(int(n)))
        resp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"USER QUERY: {q}"},
            ],
            temperature=0.0,
            max_tokens=120,
            timeout=INTENT_LLM_TIMEOUT_S,
        )
        raw = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        log.warning("LLM expand: call failed (%s) — using deterministic fallback", e)
        return None

    out: List[str] = []
    seen = set()
    for line in raw.split("\n"):
        line = line.strip().lstrip("-•*0123456789. )")
        if not line or len(line) > 120:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(line)
        if len(out) >= n:
            break
    if not out:
        return None
    log.info("LLM expand produced %d rephrasing(s) for: %s", len(out), q[:80])
    return out
