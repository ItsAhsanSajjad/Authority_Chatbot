"""
PERA AI — Follow-Up Subject/Entity Anchoring

Extracts subject/entity/role from questions and answers,
anchors them for follow-up resolution, and detects when
pronouns or vague references need anchoring.

Works with the session store to carry forward context.
"""
from __future__ import annotations

import re
from typing import Optional, List, Dict, Any, Tuple

from log_config import get_logger

log = get_logger("pera.context_state")


# ── Known PERA entity patterns ────────────────────────────────
# Order matters: longer patterns first for greedy matching
_ROLE_PATTERNS = [
    r"Additional\s+Director\s+General",
    r"Chief\s+Technology\s+Officer",
    r"Director\s+General",
    r"Deputy\s+Director(?:\s+\([^)]+\))?",
    r"Assistant\s+Director(?:\s+\([^)]+\))?",
    r"Enforcement\s+Officer",
    r"Investigation\s+Officer",
    r"System\s+Support\s+Officer",
    r"Competent\s+Authority",
    r"Chairman",
    r"Secretary",
    # Capture full role+specialization: "Manager (Development)", "Manager HR", etc.
    r"(?:Senior\s+)?Manager\s*(?:\([^)]+\)|\w+(?:\s+\w+)?)?",
    r"(?:Senior\s+)?Deputy\s+Manager\s*(?:\([^)]+\)|\w+(?:\s+\w+)?)?",
    r"(?:Senior\s+)?Assistant\s+Manager\s*(?:\([^)]+\)|\w+(?:\s+\w+)?)?",
    r"Head\s+(?:Monitoring|Procurement|Licensing|Training)(?:\s*\([^)]+\))?",
]

_ENTITY_PATTERNS = [
    r"PERA",
    r"Punjab\s+Enforcement\s+and\s+Regulatory\s+Authority",
    r"the\s+Authority",
    r"the\s+Board",
    r"Government\s+of\s+Punjab",
]

_TOPIC_PATTERNS = [
    r"Schedule\s*[-–]?\s*[IVXivx1-6]+",
    r"Section\s+\d+",
    r"Rule\s+\d+",
    r"Clause\s+\d+",
    r"Special\s+Pay\s+Package",
    r"SPPP",
    r"Service\s+Rules",
    r"Terms\s+of\s+Reference",
]

_ROLE_RE = re.compile("|".join(f"({p})" for p in _ROLE_PATTERNS), re.IGNORECASE)
_ENTITY_RE = re.compile("|".join(f"({p})" for p in _ENTITY_PATTERNS), re.IGNORECASE)
_TOPIC_RE = re.compile("|".join(f"({p})" for p in _TOPIC_PATTERNS), re.IGNORECASE)

# ── Pronoun / vague reference detection ───────────────────────
_PRONOUN_PATTERNS = re.compile(
    r"\b(their|them|they|his|her|its|he|she|it|"
    r"this|that|these|those|"
    r"wo|woh|us|ye|yeh|"
    r"unki|uski|unka|inka|inki|inke|iska|iski|iske|"
    r"usk[aeiou]|unk[aeiou]|"
    r"of\s+this|of\s+that|of\s+it|"
    r"that\s+person|this\s+person|this\s+role|that\s+role|this\s+position|that\s+position|"
    r"the\s+same|same\s+person|same\s+one|same\s+position)(?:\s|[?!.,]|$)",
    re.IGNORECASE,
)

_FOLLOWUP_MARKERS = re.compile(
    r"\b(what\s+about|and\s+what|also\s+tell|bhi\s+batao|"
    r"iske\s+ilawa|iska|iski|aur\s+kya|aur\s+batao|"
    r"salary\s+of\s+this|salary\s+of\s+that|"
    r"reporting\s+to|report\s+to|reports\s+to|"
    r"does\s+that|do\s+they|can\s+they|does\s+it|"
    r"how\s+much|kitni|kitna|kya\s+hai)(?:\s|[?!.,]|$)",
    re.IGNORECASE,
)

# Words that indicate standalone queries (not follow-ups)
_STANDALONE_INDICATORS = re.compile(
    r"\b(manager|officer|director|deputy|assistant|chief|head|"
    r"schedule|section|rule|clause|pera|sppp|bps|"
    r"chairman|secretary|registrar|superintendent|coordinator|"
    r"sergeant|operator|developer|analyst|administrator|"
    # Common PERA abbreviations — a query containing these is standalone
    r"cto|dg|ddg|adg|dd|eo|io|sso|sdeo|deo|dba|se|mgr|"
    r"hr|it|admin|salary|pay)\b",
    re.IGNORECASE,
)


def needs_anchoring(question: str) -> bool:
    """
    Returns True if the question contains pronouns or vague references
    that would benefit from entity anchoring from prior context.
    """
    q = (question or "").strip()
    if not q:
        return False

    # If question already has an explicit entity, no anchoring needed
    if _ROLE_RE.search(q) or _ENTITY_RE.search(q) or _TOPIC_RE.search(q):
        return False

    # Check for pronouns or follow-up markers
    if _PRONOUN_PATTERNS.search(q) or _FOLLOWUP_MARKERS.search(q):
        return True

    # Short-query heuristic: queries under 8 words without any
    # standalone indicators are likely follow-ups
    word_count = len(q.split())
    if word_count <= 6 and not _STANDALONE_INDICATORS.search(q):
        return True

    return False


def extract_subject(text: str) -> str:
    """
    Extract the primary subject/entity/role from text.
    Returns the first strong match, or empty string.
    Handles possessive forms: "manager's development" -> "Manager Development"
    """
    if not text:
        return ""

    # Pre-process: normalize possessive forms + strip stop words at end
    clean = re.sub(r"['']s\b", "", text)  # manager's → manager

    _SUBJECT_STOP = {
        "salary", "pay", "benefit", "benefits", "allowance",
        "appointment", "scale", "package", "detail", "details",
        "head", "reporting", "report",
        "is", "are", "at", "in", "for", "the", "a", "of", "to",
        "was", "has", "have", "will", "shall", "may", "can",
    }

    # Priority: Role > Topic > Entity
    m = _ROLE_RE.search(clean)
    if m:
        result = m.group(0).strip()
        # Strip trailing stop words
        words = result.split()
        while words and words[-1].lower() in _SUBJECT_STOP:
            words.pop()
        return " ".join(words) if words else result

    m = _TOPIC_RE.search(clean)
    if m:
        return m.group(0).strip()

    m = _ENTITY_RE.search(clean)
    if m:
        return m.group(0).strip()

    return ""


def anchor_query(
    question: str,
    last_subject: str,
    last_question: str = "",
    last_answer: str = "",
) -> Tuple[str, str, bool]:
    """
    Anchor a follow-up question with the most recent subject if needed.
    
    Instead of just prepending a context hint, this function directly
    substitutes pronouns with the subject to produce a concrete query
    that doesn't need LLM rewriting.
    
    Returns:
        (anchored_query, subject_used, was_anchored)
    """
    q = (question or "").strip()
    
    if not needs_anchoring(q):
        # Extract subject from this question for future use
        subject = extract_subject(q)
        return q, subject, False
    
    if not last_subject:
        log.debug("Follow-up detected but no prior subject to anchor: '%s'", q[:80])
        return q, "", False
    
    # Direct pronoun substitution for concrete query
    anchored = _substitute_pronouns(q, last_subject)
    log.info("Entity anchoring: '%s' -> '%s' (subject='%s')", q[:60], anchored[:60], last_subject)
    
    return anchored, last_subject, True


def _substitute_pronouns(question: str, subject: str) -> str:
    """Replace pronouns/references in the question with the actual subject."""
    q = question.strip()
    
    # Pattern: "X of this/that/it?" → "X of {subject}?"
    q_sub = re.sub(
        r"\b(of|for|about)\s+(this|that|it|these|those)\b",
        lambda m: f"{m.group(1)} {subject}",
        q,
        flags=re.IGNORECASE,
    )
    if q_sub != q:
        return q_sub
    
    # Pattern: "his/her/its/their X" → "{subject} X"
    q_sub = re.sub(
        r"\b(his|her|its|their|this|that)\s+",
        f"{subject} ",
        q,
        count=1,
        flags=re.IGNORECASE,
    )
    if q_sub != q:
        return q_sub
    
    # Pattern: "he/she/it/they verb..." → "{subject} verb..."
    q_sub = re.sub(
        r"^(he|she|it|they)\s+",
        f"{subject} ",
        q,
        count=1,
        flags=re.IGNORECASE,
    )
    if q_sub != q:
        return q_sub
    
    # Pattern: standalone pronoun as the whole query: "this?", "it?"
    if re.match(r"^(this|that|it)\?*$", q.strip(), re.IGNORECASE):
        return subject
    
    # Fallback: prepend subject to make the query self-contained
    return f"{subject} {q}"


def extract_evidence_metadata(retrieval: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    Extract evidence_ids and doc_names from retrieval result
    for session state tracking.
    """
    evidence_ids: List[str] = []
    doc_names: List[str] = []
    seen_docs = set()
    
    for doc_group in retrieval.get("evidence", []):
        dn = doc_group.get("doc_name", "")
        if dn and dn not in seen_docs:
            doc_names.append(dn)
            seen_docs.add(dn)
        for hit in doc_group.get("hits", []):
            eid = hit.get("evidence_id", "")
            if eid:
                evidence_ids.append(eid)
    
    return evidence_ids[:10], doc_names[:5]
