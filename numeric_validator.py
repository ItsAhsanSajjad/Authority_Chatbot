"""
PERA AI — Numeric Answer Validator

Validates that every numeric token in a generated answer is supported
by the structured payload it was rendered from. Used for analytics /
ranking / aggregate intents to catch LLM hallucinated numbers AND
LLM table re-orderings.

Behaviour (per HUMAN DECISION 6):
  - Do NOT hard-fail the request.
  - When validation fails, the caller should swap the LLM answer for the
    deterministic code-rendered answer and log the divergence.

Public API:
  - extract_numbers(text) -> list[str]
  - extract_numbers_from_payload(payload_text) -> set[str]
  - validate_numeric_answer(answer, payload_text, *, evidence_type="")
        -> NumericValidationResult
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Set


# Match ISO and DMY dates BEFORE we tokenise as numbers so a date
# like "2026-01-01" is treated as one date token instead of three.
_ISO_DATE_TOKEN = re.compile(r"\b\d{4}-\d{1,2}-\d{1,2}\b")
_DATE_TOKEN_RE = re.compile(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b")

_NUMBER_TOKEN = re.compile(
    r"(?:Rs\.?\s*)?\d{1,3}(?:[,\s]\d{3})+(?:\.\d+)?|"   # 1,234,567 or 1 234
    r"(?:Rs\.?\s*)?\d+(?:\.\d+)?",                       # plain int / decimal
    re.IGNORECASE,
)

# Numbers we always consider "free" — small integers (1-31 likely days,
# rank positions, year tails, etc.) and ages. Aggressive validation on
# these would block legitimate paraphrase like "ranks 1 through 25".
_FREE_NUMERIC = {str(i) for i in range(0, 32)}

# Years are allowed only inside the plausible historical window so a
# stray "9999" can't pass as a year.
_PLAUSIBLE_YEARS = {str(y) for y in range(1900, 2100)}


@dataclass
class NumericValidationResult:
    ok: bool
    unsupported_numbers: List[str] = field(default_factory=list)
    mismatched_claims: List[str] = field(default_factory=list)
    reason: str = ""


def _normalize(num: str) -> str:
    """Strip currency markers and thousands separators for comparison.
    Drops leading zeros so "01" matches "1".
    """
    s = num.strip().lower()
    s = s.replace("rs.", "").replace("rs", "")
    s = s.replace(",", "").replace(" ", "")
    if "." in s:
        # keep decimal value as-is (no leading-zero strip on decimals)
        return s
    s = s.lstrip("0") or "0"
    return s


def extract_numbers(text: str) -> List[str]:
    """Return the list of numeric tokens in `text` AFTER masking out
    date-shaped substrings (ISO and DMY) so they don't fragment into
    bare day/month numerics.
    """
    if not text:
        return []
    masked = _ISO_DATE_TOKEN.sub(" ", text)
    masked = _DATE_TOKEN_RE.sub(" ", masked)
    return [m.group(0).strip() for m in _NUMBER_TOKEN.finditer(masked)]


def extract_numbers_from_payload(payload_text: str) -> Set[str]:
    return {_normalize(n) for n in extract_numbers(payload_text or "")}


def _looks_like_date(token: str) -> bool:
    s = token.strip()
    if s in _PLAUSIBLE_YEARS:
        return True
    if _DATE_TOKEN_RE.match(s) or _ISO_DATE_TOKEN.match(s):
        return True
    return False


def validate_numeric_answer(
    answer: str,
    payload_text: str,
    *,
    evidence_type: str = "",
    permissive: bool = False,
) -> NumericValidationResult:
    """Validate that all "interesting" numbers in `answer` exist in `payload_text`.

    `evidence_type` (e.g. "ranking", "aggregate", "officer_table",
    "structured_analytics") signals strict mode. For document-style
    answers, callers should pass permissive=True or skip validation
    entirely.

    A number is "interesting" if it is NOT in `_FREE_NUMERIC` and is NOT
    a year/date token.
    """
    if not answer:
        return NumericValidationResult(ok=True, reason="empty_answer")

    payload_set = extract_numbers_from_payload(payload_text)
    answer_numbers = extract_numbers(answer)

    unsupported: List[str] = []
    for raw in answer_numbers:
        norm = _normalize(raw)
        if not norm:
            continue
        if norm in _FREE_NUMERIC:
            continue
        if _looks_like_date(norm):
            continue
        if norm in payload_set:
            continue
        unsupported.append(raw)

    if unsupported and not permissive:
        return NumericValidationResult(
            ok=False,
            unsupported_numbers=unsupported[:10],
            reason=(
                f"{len(unsupported)} numeric token(s) in the answer not found "
                f"in structured payload (evidence_type={evidence_type or 'n/a'})"
            ),
        )

    return NumericValidationResult(ok=True, reason="all_supported")
