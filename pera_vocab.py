"""
PERA AI — Unified Domain Vocabulary

Single source of truth for:
  - Abbreviation → full expansion maps
  - Standalone role keywords (for follow-up detection)
  - Schedule descriptions
  - Smart context expansion keywords

All modules (retriever, answerer, context_state) must import from here.
Do NOT duplicate abbreviation or role data elsewhere.
"""
from __future__ import annotations

import re
from typing import Dict, Set, List


# ── Abbreviation Map ────────────────────────────────────────────
# Keys are lowercase. Values are canonical full forms.
ABBREVIATION_MAP: Dict[str, str] = {
    # Executive / Leadership
    "cto": "Chief Technology Officer",
    "dg": "Director General",
    "ddg": "Deputy Director General",
    "adg": "Additional Director General",

    # Director-level
    "dd": "Deputy Director",

    # Manager-level
    "mgr": "Manager",

    # Officer-level
    "eo": "Enforcement Officer",
    "io": "Investigation Officer",
    "sso": "System Support Officer",
    "sdeo": "Sub-Divisional Enforcement Officer",

    # Operator/Other
    "deo": "Data Entry Operator",
    "dba": "Database Administrator",
    "se": "Software Engineer",

    # Departments
    "hr": "Human Resources",
    "it": "Information Technology",
    "admin": "Administration",
    "m&i": "Monitoring and Implementation",
    "itc": "IT and Communication",

    # Terms
    "sppp": "Special Pay Package PERA",
    "bps": "Basic Pay Scale",
    "eotbr": "Enforcement Officer Transfer to Board of Revenue",
    "tor": "Terms of Reference",
    "jd": "Job Description",
    "sr": "Service Rules",
    "faqs": "Frequently Asked Questions",
    "gli": "Group Life Insurance",
}


# ── Schedule Descriptions ───────────────────────────────────────
SCHEDULE_MAP: Dict[str, str] = {
    "Schedule-I": "Organizational Structure",
    "Schedule-II": "Appointment & Conditions of Service",
    "Schedule-III": "Special Pay Package PERA (SPPP)",
    "Schedule-IV": "Rules / Regulations Adopted by the Authority",
    "Schedule-V": "Transfer and Posting",
    "Schedule-VI": "Special Allowance and Benefits",
}


# ── Merged map with schedules (for retriever embedding) ─────────
def get_full_abbreviation_map() -> Dict[str, str]:
    """Returns ABBREVIATION_MAP + SCHEDULE_MAP merged."""
    merged = dict(ABBREVIATION_MAP)
    merged.update(SCHEDULE_MAP)
    return merged


# ── Normalized key builder ──────────────────────────────────────
def _norm_key(s: str) -> str:
    """Normalize to lowercase alphanumerics only: 'Schedule-I' → 'schedulei'"""
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def get_normalized_abbreviation_map() -> Dict[str, str]:
    """Returns abbreviation map with normalized keys for fuzzy matching."""
    full = get_full_abbreviation_map()
    return {_norm_key(k): v for k, v in full.items()}


# ── Lowercase abbreviation map (for evidence filtering) ─────────
def get_lowercase_abbreviation_map() -> Dict[str, str]:
    """Returns abbreviation map with lowercase values (for pattern matching)."""
    return {k: v.lower() for k, v in ABBREVIATION_MAP.items()}


# ── Standalone Role Keywords ────────────────────────────────────
# Words that indicate a standalone query (not a follow-up)
STANDALONE_ROLE_KEYWORDS: Set[str] = {
    # Roles
    "manager", "officer", "director", "deputy", "assistant", "chief", "head",
    "chairman", "secretary", "registrar", "superintendent", "coordinator",
    "sergeant", "operator", "developer", "analyst", "administrator",
    # Legal references
    "schedule", "section", "rule", "clause", "pera", "sppp", "bps",
    # All abbreviation keys
    *ABBREVIATION_MAP.keys(),
    # Common query terms
    "salary", "pay",
}


# ── Smart Context Expansion Keywords ────────────────────────────
# If query contains these, retriever fetches adjacent pages (±2)
EXPANSION_KEYWORDS: Set[str] = {
    "salary", "pay", "allowance", "benefit", "scale", "sppp", "grade",
    "compensation", "detail", "full", "sab kuch", "batao", "explain",
    "structure", "manager", "officer", "director", "appointment",
    "development", "tafsilat", "tankha", "talab", "maliyat",
}


# ── Convenience: expand abbreviations in text ───────────────────
def expand_abbreviations(text: str) -> str:
    """Expand abbreviations in text. Case-insensitive word matching."""
    words = text.split()
    result = []
    for w in words:
        key = w.strip(".,?!:;()").lower()
        if key in ABBREVIATION_MAP:
            # Preserve surrounding punctuation
            prefix = ""
            suffix = ""
            stripped = w
            while stripped and not stripped[0].isalnum():
                prefix += stripped[0]
                stripped = stripped[1:]
            while stripped and not stripped[-1].isalnum():
                suffix = stripped[-1] + suffix
                stripped = stripped[:-1]
            result.append(f"{prefix}{ABBREVIATION_MAP[key]}{suffix}")
        else:
            result.append(w)
    return " ".join(result)
