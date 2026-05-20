"""
PERA AI — Source-of-Truth Policy

Central rules deciding which backend (live SDEO API, stored snapshot,
document RAG) is allowed to answer a given question. Used by
fastapi_app.simple_ask, the lookup dispatchers, and the renderers to
keep operational answers honest.

Policy (highest → lowest trust for OPERATIONAL questions):
  1. SDEO Dashboard Live APIs — date-ranged inspection / challan /
     officer counts and amounts.
  2. PCM officer endpoints — officer accountability detail (with
     endpoint-specific date convention notes).
  3. PostgreSQL stored snapshots — fallback only when source_mode is
     stored_api OR live API is unreachable.
  4. challan_data SQL — all-time stored ranking, or date-filtered if
     action_date is reliable.
  5. Document RAG — NEVER source of truth for numeric operational
     counts; only used for policy/regulation questions.
"""
from __future__ import annotations

import re
from typing import Optional


_OPERATIONAL_KEYWORDS = re.compile(
    r"\b("
    r"performance|dashboard|kpis?|summary|summery|"
    r"chall?[ae]+ns?|fine|paid|unpaid|outstanding|overdue|recover(?:ed|y)|"
    r"inspect(?:ion)?s?|warnings?|firs?|sealed|sealing|"
    r"removal\s+orders?|epo|no\s+offen[cs]es?|arrest(?:s|\s+cases?)?|"
    r"officer|enforcer|tehsils?|districts?|divisions?|"
    r"ranking|highest|lowest|"
    r"count|counts|total\s+actions?|"
    r"today|yesterday|last\s+week|last\s+month|"
    r"q[1-4]\s+\d{4}|fy\s*\d"
    r")\b",
    re.IGNORECASE,
)

# "top N <noun>" / "rank(ed) <noun>" / "top officers / top tehsils" only
# count as operational when the trailing noun is a countable
# operational entity. Bare "top achievements / top issues / top risks /
# top priorities" is narrative and must NOT trip the refusal gate.
_OPERATIONAL_TOP_RE = re.compile(
    r"\btop\s+\d*\s*("
    r"officer|enforcer|tehsil|district|division|station|"
    r"performer|inspect(?:ion)?|chall?[ae]+n|fine|recover|"
    r"sealed|fir|arrest|warning|defaulter|violator"
    r")s?\b|"
    r"\brank(?:ing|ed)?\s+(?:by|of)\s+("
    r"officer|tehsil|district|division|inspect|chall?[ae]+n|fine"
    r")",
    re.IGNORECASE,
)

# Narrative veto — if the question is clearly asking for a list of
# concepts / risks / decisions / achievements / bottlenecks, force
# is_operational_query() to False so the answer routes through doc
# RAG instead of the refusal gate.
_NARRATIVE_VETO_RE = re.compile(
    r"\b("
    r"achievement|achievements|"
    r"issue|issues|red\s+flags?|bottleneck|bottlenecks|"
    r"risk|risks|decision|decisions|approval|approvals|"
    r"recommendation|recommendations|priorit(?:y|ies)|"
    r"challenge|challenges|concern|concerns|"
    r"strategy|strategies|reason|reasons|"
    r"why|how\s+will|how\s+does|what\s+if|"
    r"role|roles|responsibilit(?:y|ies)|"
    r"policy|policies|regulation|regulations|"
    r"mandate|mandates|"
    r"timeline|deadline|roadmap|plan|plans|"
    r"discuss|describe|explain|outline|"
    r"propos(?:al|als|ed)|"
    r"composition|structure|hierarchy|"
    r"benefit|benefits|advantage|advantages|"
    r"objective|objectives|purpose|"
    r"vision|mission|"
    r"meeting|agenda|minutes"
    r")\b",
    re.IGNORECASE,
)


def is_operational_query(question: str) -> bool:
    """True if `question` looks like a request for operational
    counts / amounts / rankings rather than document/policy text.

    Two-pass logic:
      1. Veto first: if the question reads narrative (achievements,
         risks, decisions, why/how, policy/mandate, etc.) it is never
         operational regardless of any accidental keyword match.
      2. Otherwise: any operational keyword OR a properly-qualified
         "top N <operational noun>" pattern flips it to True.
    """
    if not question:
        return False
    if _NARRATIVE_VETO_RE.search(question):
        return False
    if _OPERATIONAL_KEYWORDS.search(question):
        return True
    if _OPERATIONAL_TOP_RE.search(question):
        return True
    return False


def should_prefer_live_sdeo(
    question: str,
    source_mode: str = "both",
    has_date_range: bool = False,
) -> bool:
    """Return True if the live SDEO dashboard handler should be the
    primary path for this query.

    Rules:
      - source_mode="live_api" → always True
      - source_mode="documents" → False (user explicitly chose docs)
      - source_mode="stored_api" → False (user explicitly chose stored)
      - source_mode="both":
          - operational query AND date range → True
          - operational query without date range → True (live still
            wins; stored is fallback)
          - non-operational → False (let docs/stored/RAG handle)
    """
    sm = (source_mode or "both").strip().lower()
    if sm == "live_api":
        return True
    if sm == "documents":
        return False
    if sm == "stored_api":
        return False
    # both
    if not is_operational_query(question):
        return False
    return True


# ── Top-N limit extraction ──────────────────────────────────
_TOP_N_RE = re.compile(
    r"\btop\s*(\d+)\b|"
    r"\btop\s+(?:five|five|ten|fifteen|twenty|twentyfive|fifty)\b",
    re.IGNORECASE,
)
_NUMBER_WORDS = {"five": 5, "ten": 10, "fifteen": 15, "twenty": 20,
                 "twentyfive": 25, "fifty": 25}  # cap fifty → 25
_ALL_TOKEN_RE = re.compile(r"\b(all|everyone|every\s+officer|complete\s+list)\b",
                           re.IGNORECASE)


def extract_limit(question: str, default: int = 10, max_limit: int = 25) -> int:
    """Extract a row-count limit from the user's question.

    - "top 5" → 5
    - "top 20" → 20 (capped at max_limit)
    - "all" → max_limit (with caller responsible for displaying note)
    - missing → default (10)
    """
    if not question:
        return default
    m = _TOP_N_RE.search(question)
    if m:
        if m.group(1):
            try:
                n = int(m.group(1))
                return min(max(1, n), max_limit)
            except ValueError:
                pass
        # number word
        for word, val in _NUMBER_WORDS.items():
            if re.search(rf"\b{word}\b", question, re.IGNORECASE):
                return min(val, max_limit)
    if _ALL_TOKEN_RE.search(question):
        return max_limit
    return default


_DETAIL_REQUEST_RE = re.compile(
    r"\b(records?|details?|list|cases?|individual|each\s+inspection|"
    r"show\s+(?:inspect|record|case))\b",
    re.IGNORECASE,
)


def user_wants_records(question: str) -> bool:
    """True when the user explicitly asks for individual records / a
    detailed list. Used to decide whether the officer query should
    include per-record dump or just summary tables.
    """
    if not question:
        return False
    return bool(_DETAIL_REQUEST_RE.search(question))
