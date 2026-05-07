"""
PERA AI — Canonical Entity Resolver

Single source of truth for resolving location and officer names from
free-text user queries. Handles aliases (D.G. Khan ↔ DG Khan ↔ Dera
Ghazi Khan), normalisation (case, dots, whitespace), token-subset
matching for multi-word tehsil names, and an officer-vs-location guard.

Public API:
  - normalize_entity_text(text) -> str
  - resolve_location(text, level=None, candidates=None) -> Optional[ResolvedEntity]
  - resolve_officer(text, candidates=None, allow_when_admin_word=False) -> Optional[ResolvedEntity]
  - has_admin_word(text) -> bool
  - canonical_division_name(text) -> Optional[str]    # quick alias resolver
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Optional


# ── Public dataclass ─────────────────────────────────────────
@dataclass(frozen=True)
class ResolvedEntity:
    canonical_name: str
    level: Optional[str]            # "division" | "district" | "tehsil" | "officer" | None
    matched_text: str
    score: float                    # 0.0 .. 1.0
    alias_used: Optional[str] = None


# ── Alias maps ───────────────────────────────────────────────
# Division aliases — map any normalised alias → canonical DB form.
# Add entries as we encounter more variants.
_DIVISION_ALIASES = {
    # D.G. Khan family
    "dgkhan": "D.G. Khan",
    "dg khan": "D.G. Khan",
    "d g khan": "D.G. Khan",
    "d.g khan": "D.G. Khan",
    "d.g. khan": "D.G. Khan",
    "dera ghazi khan": "D.G. Khan",
    "dera gazi khan": "D.G. Khan",
    "deraghazi khan": "D.G. Khan",
    # Gujranwala including variants
    "gujranwala": "Gujranwala (Including Gujrat)",
    "gujranwala division": "Gujranwala (Including Gujrat)",
    "gujranwala including gujrat": "Gujranwala (Including Gujrat)",
}

# District-level aliases. Map normalised alias → canonical DB form.
# Kept short — only the variants we have seen in the wild. Districts
# whose canonical form is unambiguous (e.g. plain "Lahore", "Multan")
# don't need an entry; substring matching catches them.
_DISTRICT_ALIASES = {
    "dera ghazi khan": "D.G. Khan",   # district share name with division
    "dg khan": "D.G. Khan",
    "d g khan": "D.G. Khan",
    "d.g khan": "D.G. Khan",
    "d.g. khan": "D.G. Khan",
}

# Tehsil-level aliases. Map alias → canonical DB form. Conservative
# coverage — we only encode aliases for tehsils whose canonical name
# carries a redundant suffix the user routinely omits (Town/Cantt/City).
_TEHSIL_ALIASES = {
    "allama iqbal": "Allama Iqbal Town",
    "iqbal town": "Allama Iqbal Town",
    "lahore city": "Lahore City",
    "lahore cantonment": "Lahore Cantt",
    "lahore cantt": "Lahore Cantt",
}

# Officer-name suffix tokens that should NOT count as canonical name
_OFFICER_NAME_SUFFIXES = re.compile(
    r"\s+(EO[-_]?\d+|EO\s*\d+|\d{2,4})$", re.I
)


# ── Admin-word detection ─────────────────────────────────────
_ADMIN_WORD_RE = re.compile(
    r"\b(division|district|tehsil|station|sub[-\s]?division|"
    r"thana|stations?)\b",
    re.I,
)


def has_admin_word(text: str) -> bool:
    """True if the text mentions an admin level keyword."""
    return bool(_ADMIN_WORD_RE.search(text or ""))


# ── Normalisation ───────────────────────────────────────────
def normalize_entity_text(text: str) -> str:
    """Lowercase, strip dots, collapse spaces. Reversible only for
    matching, NOT for display (always render via canonical_name).
    """
    if not text:
        return ""
    s = text.lower().strip()
    # Replace dots with space so "D.G. Khan" → "d g khan"
    s = s.replace(".", " ")
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ── Tehsil-name suffix awareness ────────────────────────────
# These suffix tokens are part of canonical tehsil names but are often
# omitted by users. Stripping them lets "allama iqbal" match
# "Allama Iqbal Town".
_TEHSIL_SUFFIX_TOKENS = {
    "town", "cantt", "city", "saddar", "sadar",
    "hq", "pera hq", "frms",
}


def _tehsil_root_tokens(name: str) -> List[str]:
    norm = normalize_entity_text(name)
    toks = [t for t in re.split(r"\s+", norm) if t]
    return [t for t in toks if t not in _TEHSIL_SUFFIX_TOKENS]


# ── Resolver ────────────────────────────────────────────────
def _alias_match(query_norm: str) -> Optional[ResolvedEntity]:
    """Try aliases first. Returns a ResolvedEntity at division level."""
    for alias, canonical in _DIVISION_ALIASES.items():
        # Whole-token match of the alias inside query_norm
        if re.search(rf"\b{re.escape(alias)}\b", query_norm):
            return ResolvedEntity(
                canonical_name=canonical,
                level="division",
                matched_text=alias,
                score=1.0,
                alias_used=alias,
            )
    return None


def _full_name_match(query_norm: str, candidates: Iterable[str],
                     level: str) -> Optional[ResolvedEntity]:
    """Substring match on full canonical name."""
    best: Optional[ResolvedEntity] = None
    best_len = -1
    for cand in candidates or []:
        cand_norm = normalize_entity_text(cand)
        if not cand_norm:
            continue
        if re.search(rf"\b{re.escape(cand_norm)}\b", query_norm):
            if len(cand_norm) > best_len:
                best = ResolvedEntity(
                    canonical_name=cand,
                    level=level,
                    matched_text=cand_norm,
                    score=0.95,
                )
                best_len = len(cand_norm)
    return best


def _token_subset_match(query_norm: str, candidates: Iterable[str],
                        level: str) -> Optional[ResolvedEntity]:
    """Match when every NON-SUFFIX token of the candidate is in the query.
    Used for `allama iqbal` → `Allama Iqbal Town`.
    """
    q_tokens = set(re.split(r"\s+", query_norm))
    best: Optional[ResolvedEntity] = None
    best_score = 0.0
    for cand in candidates or []:
        roots = _tehsil_root_tokens(cand)
        if len(roots) < 2:
            continue
        if all(r in q_tokens for r in roots):
            score = 0.85 if len(roots) >= 2 else 0.75
            if score > best_score:
                best = ResolvedEntity(
                    canonical_name=cand,
                    level=level,
                    matched_text=" ".join(roots),
                    score=score,
                )
                best_score = score
    return best


def resolve_location(
    text: str,
    level: Optional[str] = None,
    candidates: Optional[Iterable[str]] = None,
) -> Optional[ResolvedEntity]:
    """Resolve a location to a canonical entity.

    Args:
      text: user query text.
      level: optional explicit level. When supplied, only that level is
             tried. When None, alias-table is tried first; caller must
             then call again with each level if alias fails.
      candidates: iterable of canonical names from the DB for the level.

    Returns:
      ResolvedEntity with score ≥ 0.75 on a confident match, or None.
    """
    if not text:
        return None
    q_norm = normalize_entity_text(text)
    if not q_norm:
        return None

    # 1. Alias hit (division-level shortcut)
    if level in (None, "division"):
        hit = _alias_match(q_norm)
        if hit:
            return hit

    if not candidates:
        return None

    # 2. Full-name substring (matches longer strings first)
    full = _full_name_match(q_norm, candidates, level or "tehsil")
    if full:
        return full

    # 3. Token-subset match (multi-word tehsils with suffix omitted)
    if level == "tehsil":
        sub = _token_subset_match(q_norm, candidates, level)
        if sub:
            return sub

    return None


def canonical_division_name(text: str) -> Optional[str]:
    """Quick alias-only resolver for divisions. Returns canonical name or None.
    Useful when caller doesn't want to pass candidates (no DB hit needed).
    """
    if not text:
        return None
    q_norm = normalize_entity_text(text)
    hit = _alias_match(q_norm)
    return hit.canonical_name if hit else None


def canonical_district_name(text: str,
                            candidates: Optional[Iterable[str]] = None
                            ) -> Optional[str]:
    """Resolve a district alias to its canonical DB form. Returns None
    when no alias hit. If `candidates` is supplied, the canonical name
    is only returned when it is also in the candidate list (guards
    against returning a name the DB does not actually have).
    """
    if not text:
        return None
    q_norm = normalize_entity_text(text)
    for alias, canonical in _DISTRICT_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", q_norm):
            if candidates is None or canonical in candidates:
                return canonical
    return None


def canonical_tehsil_name(text: str,
                          candidates: Optional[Iterable[str]] = None
                          ) -> Optional[str]:
    """Resolve a tehsil alias to its canonical DB form. Returns None
    when no alias hit. Guarded by `candidates` like the district variant.
    """
    if not text:
        return None
    q_norm = normalize_entity_text(text)
    # Sort by alias length descending so "lahore cantt" beats "lahore"
    for alias in sorted(_TEHSIL_ALIASES.keys(), key=len, reverse=True):
        if re.search(rf"\b{re.escape(alias)}\b", q_norm):
            canonical = _TEHSIL_ALIASES[alias]
            if candidates is None or canonical in candidates:
                return canonical
    return None


# ── Officer guard ───────────────────────────────────────────
def resolve_officer(
    text: str,
    candidates: Optional[Iterable[str]] = None,
    allow_when_admin_word: bool = False,
) -> Optional[ResolvedEntity]:
    """Resolve an officer name from `text`.

    If `text` contains an admin-level keyword (tehsil/station/district/
    division) AND `allow_when_admin_word` is False, this returns None.
    The caller should resolve location first.

    A successful match requires a multi-token candidate to appear as
    consecutive tokens in the query.
    """
    if not text:
        return None
    if has_admin_word(text) and not allow_when_admin_word:
        return None

    q_norm = normalize_entity_text(text)

    best: Optional[ResolvedEntity] = None
    best_len = -1
    for cand in candidates or []:
        cand_clean = _OFFICER_NAME_SUFFIXES.sub("", cand)
        cand_norm = normalize_entity_text(cand_clean)
        if not cand_norm or " " not in cand_norm:
            # require at least two name tokens
            continue
        if re.search(rf"\b{re.escape(cand_norm)}\b", q_norm):
            if len(cand_norm) > best_len:
                best = ResolvedEntity(
                    canonical_name=cand,
                    level="officer",
                    matched_text=cand_norm,
                    score=0.92,
                )
                best_len = len(cand_norm)
    return best
