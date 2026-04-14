"""
PERA AI — Shared Abbreviation Maps
Single source of truth for abbreviation expansion used across:
- retriever.py (search query expansion)
- answerer.py (evidence filtering + reference extraction)
- context_state.py (entity detection)
"""
from __future__ import annotations

# Abbreviation → Full PERA role/term (lowercase canonical form)
ABBREVIATION_MAP = {
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
    "sdeo": "Sub Divisional Enforcement Officer",

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
}

# Role keywords that indicate a standalone query (not a follow-up)
STANDALONE_ROLE_KEYWORDS = {
    "manager", "officer", "director", "deputy", "assistant", "chief", "head",
    "schedule", "section", "rule", "clause", "pera", "sppp", "bps",
    "chairman", "secretary", "registrar", "superintendent", "coordinator",
    "sergeant", "operator", "developer", "analyst", "administrator",
    # Abbreviations — so "SSO salary?" is never treated as a follow-up
    "cto", "dg", "ddg", "adg", "dd", "eo", "io", "sso", "sdeo", "deo",
    "dba", "se", "mgr", "hr", "it",
}


def expand_abbreviations(text: str) -> str:
    """Expand abbreviations in text. Case-insensitive word matching."""
    words = text.split()
    result = []
    for w in words:
        key = w.strip(".,?!:;()").lower()
        if key in ABBREVIATION_MAP:
            # Preserve punctuation
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
