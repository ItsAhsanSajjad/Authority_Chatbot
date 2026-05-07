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
    label_mismatches: List[str] = field(default_factory=list)


# ── Label-aware validation (Phase-41) ────────────────────────
_TABLE_ROW_RE = re.compile(
    r"^\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*$",
    re.MULTILINE,
)


def _normalize_label(label: str) -> str:
    s = (label or "").strip().lower()
    s = re.sub(r"[^a-z0-9% ]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Canonical aliases — keep narrow, only the cases that diverge.
    aliases = {
        "fine amount": "fine imposed",
        "fine amount imposed": "fine imposed",
        "total fine amount": "fine imposed",
        "total fine": "fine imposed",
        "imposed fine": "fine imposed",
        "amount paid": "paid amount",
        "outstanding fine": "outstanding amount",
        "unpaid amount": "outstanding amount",
        "fine outstanding": "outstanding amount",
        "fine recovered paid": "fine recovered",
    }
    return aliases.get(s, s)


def extract_markdown_label_value_pairs(text: str) -> List[tuple]:
    """Parse `| Label | Value |` rows. Skips header (`| KPI | Value |`)
    and separator (`| --- | ---: |`) rows.
    """
    out: List[tuple] = []
    for m in _TABLE_ROW_RE.finditer(text or ""):
        label = m.group(1).strip()
        value = m.group(2).strip()
        if not label or not value:
            continue
        if set(label.replace("-", "").strip()) <= {":", " "}:
            continue
        if label.lower() in {"kpi", "metric", "value"}:
            continue
        if value.lower() in {"value"}:
            continue
        # skip pure separator rows
        if re.fullmatch(r":?-+:?", label) or re.fullmatch(r":?-+:?", value):
            continue
        out.append((_normalize_label(label), value))
    return out


def validate_label_value_pairs(answer: str, payload_text: str) -> List[str]:
    """Return a list of mismatch descriptions: any (label, value) pair
    in the answer whose value differs from the corresponding payload
    pair, or whose label is missing from the payload entirely.

    Empty list = all pairs match.
    """
    answer_pairs = extract_markdown_label_value_pairs(answer)
    payload_pairs = extract_markdown_label_value_pairs(payload_text)
    if not answer_pairs or not payload_pairs:
        return []

    payload_map = {lbl: val for lbl, val in payload_pairs}
    mismatches: List[str] = []
    for lbl, ans_val in answer_pairs:
        # date / date-range cells — skip
        if re.search(r"\d{4}-\d{2}-\d{2}", ans_val):
            continue
        if lbl not in payload_map:
            continue   # label not in payload → not a swap, just unrelated text
        payload_val = payload_map[lbl]
        # Compare via _normalize so "Rs. 1,234" matches "1234"
        if _normalize(ans_val) != _normalize(payload_val):
            mismatches.append(
                f"label={lbl!r} answer={ans_val!r} payload={payload_val!r}"
            )
    return mismatches


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

    # Label-aware second pass: catches Fine Imposed/Paid Amount swaps.
    label_mismatches = validate_label_value_pairs(answer, payload_text)

    if (unsupported or label_mismatches) and not permissive:
        return NumericValidationResult(
            ok=False,
            unsupported_numbers=unsupported[:10],
            label_mismatches=label_mismatches[:10],
            reason=(
                f"{len(unsupported)} unsupported number(s), "
                f"{len(label_mismatches)} label/value mismatch(es) "
                f"(evidence_type={evidence_type or 'n/a'})"
            ),
        )

    return NumericValidationResult(ok=True, reason="all_supported")
