"""
PERA AI Answerer (v4: prompt-versioned, consolidated vocab)

4 answer states: supported, partially_supported, unsupported, conflicting
No more contradictory "not explicitly defined" + correct answer.
"""
from __future__ import annotations

import os
import re
from typing import List, Dict, Any, Optional

from openai_clients import get_chat_client, ANSWER_MODEL
from grounding import verify_grounding, GroundingResult
from log_config import get_logger
from settings import get_settings
from pera_vocab import ABBREVIATION_MAP as _PERA_ABBREV, get_lowercase_abbreviation_map

log = get_logger("pera.answerer")

# Evidence quality thresholds
ANSWER_MIN_TOP_SCORE = float(os.getenv("ANSWER_MIN_TOP_SCORE", "0.15"))
HIT_MIN_SCORE = float(os.getenv("HIT_MIN_SCORE", "0.12"))
MAX_HITS_PER_DOC = int(os.getenv("MAX_HITS_PER_DOC_FOR_PROMPT", "6"))
MAX_DOCS = int(os.getenv("MAX_DOCS_FOR_PROMPT", "5"))
MAX_EVIDENCE_CHARS = int(os.getenv("MAX_EVIDENCE_CHARS", "18000"))


# =============================================================================
# Position Title Detection for Evidence Filtering
# =============================================================================
_POSITION_TITLE_RE = re.compile(
    r"Position\s+Title\s*:\s*-?\s*(.+?)(?:\s*Report\s+To\s*:|\n)",
    re.IGNORECASE
)

# Extended pattern for REFERENCE extraction only (also matches [Role:] format)
_REF_POSITION_TITLE_RE = re.compile(
    r"(?:Position\s+Title\s*:\s*-?\s*(.+?)(?:\s*Report\s+To\s*:|\n)"
    r"|\[Role:\s*(.+?)\])",
    re.IGNORECASE
)

# Stop words that should NOT be part of a role name
_ROLE_STOP_WORDS = {
    # English stop words
    "salary", "pay", "benefit", "benefits", "allowance", "appointment",
    "scale", "package", "sppp", "compensation", "grade", "detail",
    "details", "responsibilities", "duties", "powers", "functions",
    "qualification", "experience", "of", "the", "in", "at", "for",
    "what", "is", "are", "about", "tell", "me", "explain",
    "how", "much", "does", "do", "earn", "get", "paid", "make",
    "structure", "reporting", "report", "reports", "who",
    # Urdu / Roman Urdu stop words
    "ki", "ka", "ke", "ko", "kya", "hai", "hain", "hy", "hen",
    "aur", "ya", "mein", "se", "par", "pe", "ye", "yeh", "woh",
    "nahi", "nhi", "na", "ho", "tha", "thi", "the",
    "batao", "btao", "bataen", "bataiye", "batayein", "btaen",
    "kitni", "kitna", "kitne", "kahan", "kaun", "konsi", "kaunsi",
    "wala", "wali", "wale", "hota", "hoti", "hote",
    "kaise", "kaisa", "kesi", "kaisi",
    "abhi", "kab", "jab", "tab",
}

# Urdu particles that should be stripped before role detection
_URDU_PARTICLES = re.compile(
    r"\b(?:ki|ka|ke|ko|kya|hai|hain|hy|hen|aur|ya|mein|se|par|pe|ye|yeh|woh|"
    r"nahi|nhi|na|ho|tha|thi|batao|btao|bataen|bataiye|batayein|btaen|"
    r"kitni|kitna|kitne|kahan|kaun|konsi|kaunsi|wala|wali|wale|"
    r"hota|hoti|hote|kaise|kaisa|kesi|kaisi|abhi|kab|jab|tab)\b",
    re.IGNORECASE
)

def _normalize_query_for_role(question: str) -> str:
    """Strip Urdu/Roman Urdu particles from query before role detection.
    'SSO ki salary kya hai' → 'SSO salary'
    'manager development ka pay scale kya hai' → 'manager development pay scale'
    """
    q = _URDU_PARTICLES.sub(' ', question or '')
    return re.sub(r'\s+', ' ', q).strip()

def _extract_position_title(text: str) -> str:
    m = _POSITION_TITLE_RE.search(text or "")
    return m.group(1).strip().lower() if m else ""

def _extract_ref_position_title(text: str) -> str:
    """Like _extract_position_title but also matches [Role:] format.
    Used ONLY for reference filtering, not evidence assembly."""
    m = _REF_POSITION_TITLE_RE.search(text or "")
    if not m:
        return ""
    title = (m.group(1) or m.group(2) or "").strip()
    return title.lower()

# Pattern 1: PREFIX SPEC — "manager development", "director monitoring", "assistant manager HR"
_ROLE_PREFIX_RE = re.compile(
    r"\b((?:senior\s+|assistant\s+|deputy\s+)?(?:manager|director|head|coordinator|superintendent))"
    r"\s+([\w&]+(?:\s+[\w&]+)?)",
    re.IGNORECASE
)

# Pattern 1b: BARE PREFIX — "deputy director", "director general", "assistant director"
# These are complete role names without a specialization word
_BARE_ROLE_RE = re.compile(
    r"\b((?:deputy|assistant|additional|joint|regional)\s+(?:director|manager|head)(?:\s+general)?)"
    r"(?:\s+|$)",
    re.IGNORECASE
)

# Pattern 2: SPEC ROLE — "system support officer", "enforcement officer", "data entry operator"
_ROLE_SUFFIX_RE = re.compile(
    r"\b([\w]+(?:\s+[\w]+)?)\s+(officer|operator|sergeant|analyst|developer|writer|administrator)\b",
    re.IGNORECASE
)

# Full title patterns from PERA docs: "Manager (Development)", "Assistant Director (Admin & HR)"
_FULL_TITLE_RE = re.compile(
    r"\b((?:senior\s+|assistant\s+|deputy\s+)?(?:manager|director|head))\s*\(\s*([^)]+)\s*\)",
    re.IGNORECASE
)

def _detect_target_role(question: str) -> str:
    """
    Detect the target role/position from a user's question.
    Handles multiple naming patterns used in PERA:
    - ABBREVIATION: "SSO role" → "system support officer"
    - PREFIX SPEC: "manager development salary" → "manager development"
    - SPEC ROLE: "system support officer salary" → "system support officer"
    - FULL TITLE: "Manager (Development)" → "manager development"
    """
    # First strip Urdu particles, then normalize English
    q = _normalize_query_for_role(question or "")
    q = re.sub(r"[''\u2019]s\b", "", q)

    # Expand PERA abbreviations BEFORE role detection.
    # "SSO role" → "System Support Officer role", "CTO" → "Chief Technology Officer"
    q_expanded = q
    for abbr, full in _PERA_ABBREV.items():
        # Case-insensitive word-boundary replacement
        pattern = re.compile(r'\b' + re.escape(abbr) + r'\b', re.IGNORECASE)
        q_expanded = pattern.sub(full, q_expanded)
    if q_expanded != q:
        log.debug("Role detection: expanded '%s' -> '%s'", q[:40], q_expanded[:60])
        q = q_expanded

    q_lower = q.lower()
    
    # Try full parenthesized title first: "Manager (Development)"
    m = _FULL_TITLE_RE.search(q)
    if m:
        prefix = m.group(1).strip().lower()
        spec = m.group(2).strip().lower()
        return f"{prefix} {spec}"
    
    # Try PREFIX SPEC pattern: "manager development salary"
    m = _ROLE_PREFIX_RE.search(q)
    if m:
        prefix = m.group(1).strip().lower()
        suffix_raw = m.group(2).strip().lower()
        # Strip stop words from the end
        suffix_words = suffix_raw.split()
        cleaned = []
        for w in suffix_words:
            if w in _ROLE_STOP_WORDS:
                break
            cleaned.append(w)
        if cleaned:
            return (prefix + " " + " ".join(cleaned)).strip()
        # PREFIX SPEC failed (spec was all stop words like 'salary')
        # Fall through to BARE PREFIX check below
    
    # Try BARE PREFIX pattern: "deputy director salary" → "deputy director"
    m = _BARE_ROLE_RE.search(q)
    if m:
        return m.group(1).strip().lower()
    
    # Try SPEC ROLE pattern: "system support officer salary", "enforcement officer"
    m = _ROLE_SUFFIX_RE.search(q)
    if m:
        spec_raw = m.group(1).strip().lower()
        role_word = m.group(2).strip().lower()
        # Strip leading stop words from spec
        spec_words = spec_raw.split()
        cleaned = [w for w in spec_words if w not in _ROLE_STOP_WORDS]
        if cleaned:
            return " ".join(cleaned) + " " + role_word
        return role_word
    
    return ""


# =============================================================================
# Context Formatting (v2: position-title-aware)
# =============================================================================
def format_evidence_for_llm(retrieval: Dict[str, Any], question: str = "") -> str:
    """
    Format retrieved chunks into a clean context block.
    Uses position-title-aware scoring to prevent wrong-entity evidence.
    """
    if not retrieval.get("has_evidence"):
        return ""

    evidence_list = retrieval.get("evidence", [])
    context_parts: List[str] = []
    total_chars = 0

    q_lower = question.lower() if question else ""
    _ABBREV = get_lowercase_abbreviation_map()

    expanded_q = q_lower
    for abbr, full in _ABBREV.items():
        if abbr in q_lower.split():
            expanded_q = expanded_q.replace(abbr, full)

    _stop = {
        "what", "which", "where", "when", "does", "that", "this", "with",
        "from", "about", "have", "been", "will", "shall", "their", "these",
        "salary", "scale", "detail", "full", "explain", "the", "for", "and", "how"
    }
    _subject_words = [w for w in expanded_q.split() if len(w) > 2 and w not in _stop]

    # Detect target role/position for entity-aware filtering
    # Use expanded_q so abbreviations like SSO are properly resolved
    target_role = _detect_target_role(expanded_q) or _detect_target_role(question)
    if target_role:
        log.info("Position-aware filtering: target='%s'", target_role)

    # When a direct API lookup answered the query, cap supplementary doc evidence
    # to prevent irrelevant document chunks from drowning the API data.
    has_primary_lookup = retrieval.get("_has_primary_lookup", False)
    supplementary_chars_budget = 4000 if has_primary_lookup else MAX_EVIDENCE_CHARS
    supplementary_chars_used = 0

    # Flatten all hits across doc groups for unified position-aware scoring
    all_hits = []
    log.debug("Evidence groups: %d, has_primary=%s", len(evidence_list), has_primary_lookup)
    for doc_group in evidence_list:
        doc_name = (doc_group.get("doc_name", "Unknown Document") or "Unknown Document").strip()
        log.info("  Doc group: '%s', hits=%d, primary=%s, supplementary=%s",
                 doc_name[:60], len(doc_group.get("hits", [])),
                 doc_group.get("_is_primary_lookup"), doc_group.get("_is_supplementary"))
        for hit in doc_group.get("hits", []):
            hit["_doc_name"] = doc_name
            # Propagate supplementary flag from doc group
            if doc_group.get("_is_supplementary"):
                hit["_is_supplementary"] = True
            all_hits.append(hit)

    # Score each hit with position-title awareness
    scored_hits = []
    for hit in all_hits:
        text = (hit.get("text") or "")
        text_lower = text.lower()
        base_score = float(hit.get("_blend", hit.get("score", 0)))
        is_context = hit.get("_is_smart_context", False)

        if not is_context and base_score < HIT_MIN_SCORE:
            continue

        subject_match = sum(1 for w in _subject_words if w in text_lower)

        # Position-title matching: boost exact, DROP wrong positions entirely
        title_bonus = 0.0
        if target_role:
            chunk_title = _extract_position_title(text)
            if chunk_title:
                target_norm = re.sub(r"[^a-z0-9\s]", "", target_role)
                chunk_norm = re.sub(r"[^a-z0-9\s]", "", chunk_title)
                if target_norm in chunk_norm or chunk_norm in target_norm:
                    title_bonus = 10.0  # Exact position match
                else:
                    # Multi-word matching: check if the SPECIFIC words from the
                    # target role appear in the chunk title.
                    # Generic role words are excluded (they appear in many positions).
                    _generic_role_words = {
                        "manager", "officer", "director", "assistant", "deputy",
                        "chief", "head", "senior", "coordinator", "superintendent",
                        "operator", "sergeant", "analyst", "developer", "writer",
                    }
                    target_words = target_norm.split()
                    # Specific words = non-generic words with len > 2
                    specific_words = [w for w in target_words
                                      if w not in _generic_role_words and len(w) > 2]
                    if specific_words:
                        # How many specific words match?
                        matches = sum(1 for w in specific_words if w in chunk_norm)
                        if matches == len(specific_words):
                            title_bonus = 5.0   # All specific words match
                        elif matches > 0:
                            title_bonus = 2.0   # Partial match
                        else:
                            # HARD DROP — wrong position chunk
                            log.debug("Dropping wrong-position chunk: '%s' (wanted '%s')",
                                     chunk_title[:40], target_role)
                            continue
                    else:
                        # Only generic words (e.g. just "officer") — can't filter
                        # Don't drop, just don't boost
                        title_bonus = 0.0

        # Section-heading boost: DISABLED (caused scoring order changes)
        # heading_bonus commented out to preserve proven ranking
        heading_bonus = 0.0

        combined = subject_match + title_bonus + heading_bonus + base_score
        scored_hits.append((combined, hit))

    log.debug("Scored hits: %d from %d all_hits (target_role='%s')",
             len(scored_hits), len(all_hits), target_role or "")
    if scored_hits:
        top = scored_hits[0]
        log.debug("  Top hit: score=%.3f, doc='%s', primary=%s, text_len=%d",
                 top[0], top[1].get("_doc_name", "?")[:50],
                 top[1].get("_is_primary_lookup"), len(top[1].get("text", "")))

    # FALLBACK: If position filter dropped ALL position-titled chunks,
    # re-scan and keep top hits by subject_match + base_score only.
    # This prevents zero-evidence scenarios when the target role exists
    # in the docs but the position filter was too aggressive.
    if target_role and not scored_hits:
        log.warning("Position filter dropped all hits for '%s' — using fallback", target_role)
        for hit in all_hits:
            text = (hit.get("text") or "")
            text_lower = text.lower()
            base_score = float(hit.get("_blend", hit.get("score", 0)))
            is_context = hit.get("_is_smart_context", False)
            if not is_context and base_score < HIT_MIN_SCORE:
                continue
            subject_match = sum(1 for w in _subject_words if w in text_lower)
            combined = subject_match + base_score
            scored_hits.append((combined, hit))

    scored_hits.sort(key=lambda x: x[0], reverse=True)

    # Content dedup is available but disabled to preserve proven scoring order.
    # Enable by uncommenting when evidence limits are expanded in future.
    # (dedup code preserved below for future use)

    # Assemble evidence respecting per-doc limits
    doc_hit_counts: Dict[str, int] = {}
    docs_used_set: set = set()

    for _score, hit in scored_hits:
        doc_name = hit["_doc_name"]

        if len(docs_used_set) >= MAX_DOCS and doc_name not in docs_used_set:
            continue
        doc_count = doc_hit_counts.get(doc_name, 0)
        if doc_count >= MAX_HITS_PER_DOC:
            continue

        # ── Supplementary budget gate ──
        # When primary API lookup exists, cap how much supplementary doc
        # evidence we feed to the LLM to prevent irrelevant context.
        if hit.get("_is_supplementary") and has_primary_lookup:
            if supplementary_chars_used >= supplementary_chars_budget:
                continue

        # ── Build evidence tag (source-type-aware) ──
        text = (hit.get("text") or "").strip()
        page = hit.get("page_start", "?")
        safe_doc = doc_name.replace("<", "").replace(">", "").replace('"', "").replace("'", "")
        eid = hit.get("evidence_id", "")
        is_api = hit.get("source_type") == "api"
        if is_api:
            record_id = hit.get("record_id", "")
            record_type = hit.get("record_type", "")
            api_source_id = hit.get("api_source_id", "")
            # Truncate record_id in XML attributes to avoid blowing
            # MAX_EVIDENCE_CHARS with thousands of comma-separated IDs
            short_rid = record_id[:200] + "..." if len(record_id) > 200 else record_id
            part = (
                f'<evidence doc="{safe_doc}" source_type="api" '
                f'record_type="{record_type}" '
                f'api_source="{api_source_id}" eid="{eid}">\n'
                f'[Source Type: API]\n'
                f'[API Name: {safe_doc}]\n'
                f'[Record Type: {record_type}]\n'
                f"{text}\n"
                f"</evidence>"
            )
        else:
            part = (
                f'<evidence doc="{safe_doc}" page="{page}" eid="{eid}">\n'
                f"{text}\n"
                f"</evidence>"
            )

        if total_chars + len(part) > MAX_EVIDENCE_CHARS:
            break

        context_parts.append(part)
        total_chars += len(part)
        doc_hit_counts[doc_name] = doc_count + 1
        docs_used_set.add(doc_name)
        # Track hit for reference extraction
        hit["_used_for_evidence"] = True
        # Track supplementary budget usage
        if hit.get("_is_supplementary"):
            supplementary_chars_used += len(part)

        if total_chars >= MAX_EVIDENCE_CHARS:
            break

    evidence_ids = []
    for part in context_parts:
        m = re.search(r'eid="([^"]+)"', part)
        if m:
            evidence_ids.append(m.group(1))
    if evidence_ids:
        log.info("Evidence assembled: %d chunks, %d chars, eids=%s",
                 len(context_parts), total_chars, evidence_ids[:10])

    # --- Salary-Bridge: inject salary-table chunks when position chunks lack salary data ---
    # This fixes the case where position description chunks (e.g., "Head Monitoring")
    # are retrieved but the salary value is on a different page (Schedule-III / SPPP table).
    _SALARY_QUERY_WORDS = {"salary", "pay", "scale", "bps", "sppp", "compensation", "allowance",
                           "kitni", "kya", "btao", "tankhwah", "benefits"}
    is_salary_query = any(w in q_lower.split() for w in _SALARY_QUERY_WORDS)

    if is_salary_query and target_role and context_parts:
        # Check if existing evidence already contains salary values
        assembled_text = " ".join(context_parts).lower()
        has_salary_data = bool(re.search(
            r"(?:sppp[-\s]*\d|bps[-\s]*\d|bs[-\s]*\d|pay\s+(?:scale|package)|"
            r"salary\s+and\s+benefits.*(?:sppp|bps|bs[-\s])|"
            r"minimum\s+pay\s+per\s+month|maximum\s+pay)",
            assembled_text
        ))

        if not has_salary_data:
            log.info("Salary-bridge: position chunks found for '%s' but NO salary data — "
                     "injecting supplementary salary evidence", target_role)

            # Scan ALL scored_hits (including ones not yet assembled) for salary-relevant chunks
            salary_supplements = []
            for _score, hit in scored_hits:
                if hit.get("_used_for_evidence"):
                    continue  # Already in evidence
                text = (hit.get("text") or "").lower()
                # Look for chunks with SPPP/BPS salary data or Schedule-III tables
                if re.search(r"(?:sppp[-\s]*\d|bps[-\s]*\d|salary\s+and\s+benefits|"
                             r"schedule[-\s]*iii|pay\s+(?:scale|package)|"
                             r"minimum\s+pay|maximum\s+pay)", text):
                    salary_supplements.append(hit)
                    if len(salary_supplements) >= 3:
                        break

            # Also scan all_hits for Schedule-III/salary chapter chunks
            # that might not be in scored_hits due to position filtering
            if len(salary_supplements) < 2:
                for hit in all_hits:
                    if hit.get("_used_for_evidence"):
                        continue
                    text = (hit.get("text") or "").lower()
                    if re.search(r"(?:schedule[-\s]*iii|chapter\s+vi.*salary|"
                                 r"salary\s+structure|pay\s+scales)", text):
                        if hit not in salary_supplements:
                            salary_supplements.append(hit)
                            if len(salary_supplements) >= 3:
                                break

            for hit in salary_supplements:
                text = (hit.get("text") or "").strip()
                page = hit.get("page_start", "?")
                doc_name = hit.get("_doc_name", "Unknown")
                safe_doc = doc_name.replace("<", "").replace(">", "").replace('"', "").replace("'", "")
                eid = hit.get("evidence_id", "")
                part = (
                    f'<evidence doc="{safe_doc}" page="{page}" eid="{eid}" '
                    f'role="salary-supplement">\n'
                    f"{text}\n"
                    f"</evidence>"
                )
                if total_chars + len(part) > MAX_EVIDENCE_CHARS + 4000:
                    break  # Allow up to 4K extra for salary supplements
                context_parts.append(part)
                total_chars += len(part)
                hit["_used_for_evidence"] = True
                log.info("Salary-bridge injected: %s p.%s (%d chars)", safe_doc[:30], page, len(part))

    return "\n\n".join(context_parts)


def extract_references_simple(
    retrieval: Dict[str, Any],
    question: str = "",
    answer_text: str = "",
) -> List[Dict[str, Any]]:
    """Extract reference links ONLY from chunks that were actually used for the LLM answer,
    filtered for relevance to the query topic.

    When a primary API lookup answered the query (e.g., division list, strength data),
    supplementary document references are excluded unless they are highly relevant.
    This prevents "Annex L PERA Squads" from appearing next to "PERA App Data Divisions".

    When answer_text is provided, an additional cross-check ensures reference snippets
    share meaningful content overlap with the actual generated answer.
    """
    refs: List[Dict[str, Any]] = []
    seen = set()
    base_url = get_settings().BASE_URL.rstrip("/")

    evidence_list = retrieval.get("evidence", [])
    has_primary_lookup = retrieval.get("_has_primary_lookup", False)

    # Detect target role for relevance filtering
    target_role = _detect_target_role(question)

    # Build subject keywords for relevance scoring
    _stop = {
        "what", "which", "where", "when", "does", "that", "this", "with",
        "from", "about", "have", "been", "will", "shall", "their", "these",
        "the", "for", "and", "how", "tell", "me", "explain", "describe",
        "give", "show", "detail", "details", "full", "salary", "pay",
        "scale", "benefit", "appointment", "who", "pera",
    }
    q_words = set(w.lower() for w in (question or "").split()
                  if len(w) > 2 and w.lower() not in _stop)

    # Collect ONLY hits that were marked as used by format_evidence_for_llm
    used_hits = []
    for doc_group in evidence_list:
        doc_name = doc_group.get("doc_name", "Document")
        for hit in doc_group.get("hits", []):
            if hit.get("_used_for_evidence"):
                score = float(hit.get("_blend", hit.get("score", 0)))
                used_hits.append((score, doc_name, hit))

    # Fallback: if no marks, use high-score hits
    if not used_hits:
        for doc_group in evidence_list:
            doc_name = doc_group.get("doc_name", "Document")
            for hit in doc_group.get("hits", []):
                score = float(hit.get("_blend", hit.get("score", 0)))
                if score >= HIT_MIN_SCORE:
                    used_hits.append((score, doc_name, hit))

    # ── PRIMARY LOOKUP GATE ──
    # When a direct API lookup answered the query, only keep supplementary
    # document hits if they have strong keyword overlap with the question.
    # This prevents irrelevant FAISS doc matches from polluting references.
    if has_primary_lookup:
        filtered = []
        for score, doc_name, hit in used_hits:
            # Always keep primary lookup hits (API data)
            if hit.get("_is_primary_lookup"):
                filtered.append((score, doc_name, hit))
                continue
            # For supplementary doc hits: require strong relevance
            if hit.get("_is_supplementary"):
                text_lower = (hit.get("text") or "").lower()
                overlap = sum(1 for w in q_words if w in text_lower)
                if overlap >= 3:
                    # Strongly relevant doc — keep it but with reduced score
                    filtered.append((score * 0.8, doc_name, hit))
                else:
                    log.debug("Suppressing weak supplementary ref: %s (overlap=%d)",
                              doc_name[:30], overlap)
            else:
                filtered.append((score, doc_name, hit))
        used_hits = filtered
        log.info("Primary lookup gate: %d refs after filtering supplementary docs",
                 len(used_hits))

    # Score hits for REFERENCE relevance (different from evidence scoring)
    scored_refs = []
    for base_score, doc_name, hit in used_hits:
        text = (hit.get("text") or "").lower()
        ref_score = base_score

        # Boost/Drop: snippet position title vs target role
        if target_role:
            target_norm = re.sub(r"[^a-z0-9\s]", "", target_role)
            # Check if target role appears ANYWHERE in the chunk body
            # (multi-role chunks may have a different [Role:] tag but contain our role's data)
            body_has_target = target_norm in text
            chunk_title = _extract_ref_position_title(hit.get("text") or "")
            if chunk_title:
                chunk_norm = re.sub(r"[^a-z0-9\s]", "", chunk_title)
                if target_norm in chunk_norm or chunk_norm in target_norm:
                    ref_score += 5.0  # Exact position match in title
                elif body_has_target:
                    # Title is for a different role BUT the body contains our target
                    # (multi-role chunk). Keep it with a moderate boost.
                    ref_score += 2.0
                else:
                    # Check significant words
                    _generic = {"manager", "officer", "director", "assistant", "deputy",
                               "chief", "head", "senior"}
                    sig = [w for w in target_norm.split() if w not in _generic and len(w) > 2]
                    if sig and all(w in chunk_norm for w in sig):
                        ref_score += 3.0  # Significant words match
                    elif sig and not any(w in chunk_norm for w in sig):
                        # HARD DROP: chunk title is for a DIFFERENT role
                        # AND the body doesn't contain our target role
                        continue
            else:
                # No Position Title in chunk — check if target role words in body
                if body_has_target:
                    ref_score += 3.0
                # No penalty for chunks without Position Title (may be salary tables)

        # Boost: snippet mentions query subject words
        word_hits = sum(1 for w in q_words if w in text)
        ref_score += word_hits * 0.5

        scored_refs.append((ref_score, doc_name, hit))

    # ── ANSWER CROSS-CHECK ──
    # When we have the LLM's actual answer, verify each reference chunk
    # shares meaningful content with what the LLM actually said.
    # This catches cases where a wrong-role chunk was in the evidence
    # but the LLM used a different chunk to compose its answer.
    if answer_text and scored_refs:
        answer_lower = answer_text.lower()
        # Extract distinctive words from the answer (4+ chars, non-generic)
        _answer_stop = _stop | {"position", "title", "report", "wing", "purpose",
                                "enforcement", "station", "authority", "punjab",
                                "pera", "role", "officer", "manager", "director"}
        answer_words = set(
            w for w in re.sub(r"[^a-z0-9\s]", " ", answer_lower).split()
            if len(w) >= 4 and w not in _answer_stop
        )
        if answer_words:
            cross_checked = []
            for ref_score, doc_name, hit in scored_refs:
                chunk_text = (hit.get("text") or "").lower()
                # How many distinctive answer words appear in this chunk?
                overlap = sum(1 for w in answer_words if w in chunk_text)
                overlap_ratio = overlap / max(len(answer_words), 1)
                if overlap_ratio < 0.05 and not hit.get("_is_primary_lookup"):
                    # Less than 5% word overlap with actual answer — drop
                    log.debug("Answer cross-check DROP: %s p.%s (overlap=%.1f%%)",
                              doc_name[:30], hit.get("page_start", "?"),
                              overlap_ratio * 100)
                    continue
                # Boost refs that strongly match the answer content
                if overlap_ratio > 0.15:
                    ref_score += 2.0
                cross_checked.append((ref_score, doc_name, hit))
            scored_refs = cross_checked

    scored_refs.sort(key=lambda x: x[0], reverse=True)

    # ── FALLBACK: if position-title filter dropped ALL refs, recover using
    # answer cross-check alone (without position title gating).
    # This handles renamed positions (e.g., "Manager Development" → "Manager R&I")
    if not scored_refs and used_hits and answer_text:
        log.info("Position filter dropped all refs — recovering via answer cross-check")
        answer_lower = answer_text.lower()
        _fb_words = set(
            w for w in re.sub(r"[^a-z0-9\s]", " ", answer_lower).split()
            if len(w) >= 4
        )
        for base_score, doc_name, hit in used_hits:
            chunk_text = (hit.get("text") or "").lower()
            overlap = sum(1 for w in _fb_words if w in chunk_text)
            ratio = overlap / max(len(_fb_words), 1)
            if ratio >= 0.05:
                scored_refs.append((base_score + ratio * 5, doc_name, hit))
        scored_refs.sort(key=lambda x: x[0], reverse=True)

    # Take top refs (max 4), with per-doc limits
    docs_used = set()
    doc_ref_counts: Dict[str, int] = {}
    max_refs = 4
    max_refs_per_doc = 2

    # ── Build references: document + API ──
    from citation_formatter import CitationFormatter
    _cf = CitationFormatter()

    for _score, doc_name, hit in scored_refs:
        if len(refs) >= max_refs:
            break
        if len(docs_used) >= MAX_DOCS and doc_name not in docs_used:
            continue
        if doc_ref_counts.get(doc_name, 0) >= max_refs_per_doc:
            continue

        is_api = hit.get("source_type") == "api"

        if is_api:
            record_id = hit.get("record_id", "")
            # Use truncated record_id for dedup key and reference display
            short_rid = record_id[:200] if len(record_id) > 200 else record_id
            key = f"api:{doc_name}:{short_rid}"
            if key in seen:
                continue
            seen.add(key)

            ref = _cf.format_api_reference(
                display_name=doc_name,
                record_id=short_rid,
                record_type=hit.get("record_type", ""),
                source_id=hit.get("api_source_id", ""),
                snippet=(hit.get("text") or "")[:200],
                data_query=hit.get("data_query", ""),
                data_table=hit.get("data_table", ""),
            )
        else:
            page = hit.get("page_start", 1)
            path = hit.get("public_path", "")
            text = (hit.get("text") or "")[:200]

            key = f"doc:{doc_name}:{page}"
            if key in seen:
                continue
            seen.add(key)

            ref = _cf.format_document_reference(
                doc_name=doc_name,
                page_start=page,
                public_path=path,
                snippet=text,
            )

        refs.append(ref)
        doc_ref_counts[doc_name] = doc_ref_counts.get(doc_name, 0) + 1
        docs_used.add(doc_name)

    return refs


# =============================================================================
# Creator Question Detection
# =============================================================================
_CREATOR_RESPONSE = "I was developed by **Muhammad Ahsan Sajjad**, Lead AI under the supervision of the CTO of PERA."


def _is_creator_question(question: str) -> bool:
    q = question.lower()
    maker_phrases = [
        "kisne banaya", "kis ne banaya", "kisnyu bnaya", "kisny bnaya",
        "who made", "who created", "who developed", "who built",
        "tumhe banaya", "tumhe bnaya", "aapko banaya", "aapko bnaya",
        "ye banaya", "yeh banaya", "is ko banaya",
        "developed by whom", "created by whom", "made by whom",
    ]
    has_maker = any(phrase in q for phrase in maker_phrases)
    if not has_maker:
        return False
    if "pera" in q and not any(w in q for w in ["pera ai", "pera bot", "pera chatbot", "pera assistant"]):
        return False
    return True


# =============================================================================
# Detect if user explicitly asked for sources/pages/links
# =============================================================================
def _user_wants_references(question: str) -> bool:
    q = (question or "").lower()
    triggers = [
        "source", "sources", "reference", "references", "citation", "citations",
        "document", "pdf", "file", "link", "open url",
        "page", "page number", "kis page", "konse page", "konsi file", "konsa document",
        "hawala", "ref", "proof",
    ]
    return any(t in q for t in triggers)


# =============================================================================
# Strip references inside model answer (UI shows references separately)
# =============================================================================
_INLINE_CITATION_BLOCK_RE = re.compile(
    r"(\(|\[)\s*(sources?|references?|citations?)\s*:\s*.*?(\)|\])",
    re.IGNORECASE | re.DOTALL
)

_CITATION_LINE_RE = re.compile(
    r"^\s*([-*•]\s*)?(sources?|references?|citations?)\s*:\s*.*$",
    re.IGNORECASE | re.MULTILINE
)

_TRAILING_REF_SECTION_RE = re.compile(
    r"(?is)\n\s*(#{1,6}\s*)?(\*\*)?\s*(sources?|references?|citations?)\s*(\*\*)?\s*:?\s*\n.*$"
)

_LINKISH_LINE_RE = re.compile(
    r"^\s*([-*•]\s*)?.*(https?://|/assets/|#page=|open_url)\S.*$",
    re.IGNORECASE | re.MULTILINE
)


def _strip_answer_references(answer_text: str) -> str:
    if not answer_text:
        return answer_text

    txt = answer_text
    txt = re.sub(_TRAILING_REF_SECTION_RE, "", txt)
    txt = re.sub(_INLINE_CITATION_BLOCK_RE, "", txt)
    txt = re.sub(_CITATION_LINE_RE, "", txt)
    txt = re.sub(_LINKISH_LINE_RE, "", txt)

    txt = re.sub(r"[ \t]+\n", "\n", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    txt = re.sub(r"[ \t]{2,}", " ", txt)

    return txt.strip()


# =============================================================================
# Support-State Classification
# =============================================================================
def _classify_support_state(
    grounding: GroundingResult,
    answer_text: str,
) -> str:
    """
    Classify the answer into one of 4 support states:
      - "supported": answer is directly supported by evidence
      - "partially_supported": answer is compositionally supported (multiple clauses/pages)
      - "unsupported": evidence does not support the answer
      - "conflicting": evidence contains conflicting information

    This drives wording, decision, and audit metadata.
    """
    # Check for ACTUAL content conflict — not just score similarity.
    # "Conflict risk" from grounding only means multiple docs scored similarly,
    # which is normal and NOT an indicator of conflicting content.
    # Only classify as "conflicting" if semantic check explicitly flagged conflicts
    # in the answer text itself (e.g. LLM noted conflicting provisions).
    answer_lower = answer_text.lower()
    has_conflict_language = any(phrase in answer_lower for phrase in [
        "conflicting", "contradict", "differ on this",
        "inconsistent", "two different", "opposing provisions",
    ])
    if has_conflict_language:
        return "conflicting"

    # Semantic support provides the most reliable signal
    sem = grounding.semantic_support

    if sem == "full":
        return "supported"
    elif sem == "combined":
        return "partially_supported"
    elif sem == "partial":
        # Partial = some support exists. Only refuse if score is very low.
        if grounding.score >= 0.35:
            return "partially_supported"
        else:
            return "unsupported"
    elif sem == "none":
        # Even 'none' from semantic judge should check evidence quality.
        # If evidence quality is decent, the semantic judge may be too strict.
        if grounding.score >= 0.35:
            log.info("Semantic=none but evidence score=%.3f — downgrading to partially_supported", grounding.score)
            return "partially_supported"
        return "unsupported"

    # When semantic check was not run, use score-based classification
    if grounding.score >= 0.65:
        return "supported"
    elif grounding.score >= 0.35:
        return "partially_supported"
    elif grounding.score < 0.25:
        return "unsupported"
    else:
        return "partially_supported"


# =============================================================================
# Self-Refusal Stripping (catches ALL LLM-generated refusal language)
# =============================================================================
_CONTRADICTORY_DISCLAIMER_PATTERNS = [
    # "The provided PERA documents do not explicitly define/mention/state X"
    re.compile(r"^.*?(?:the\s+)?provided\s+(?:PERA\s+)?(?:official\s+)?(?:documents?|context)\s+do(?:es)?\s+not\s+(?:explicitly\s+)?(?:define|mention|state|specify|address|contain|detail|cover).*?[.\n]", re.IGNORECASE | re.MULTILINE),
    # "This/it is not explicitly mentioned/defined/detailed"
    re.compile(r"^.*?(?:this|it|the\s+position)\s+(?:is|are)\s+not\s+(?:explicitly\s+)?(?:mentioned|defined|stated|specified|detailed|covered|available|found).*?[.\n]", re.IGNORECASE | re.MULTILINE),
    # "not explicitly/specifically/directly defined in the provided/available/PERA..."
    re.compile(r"^.*?not\s+(?:explicitly|specifically|directly)\s+(?:defined|mentioned|stated|addressed|covered|detailed|found|available)\s+in\s+(?:the\s+)?(?:provided|available|PERA|given).*?[.\n]", re.IGNORECASE | re.MULTILINE),
    # "no specific/explicit mention/definition/provision"
    re.compile(r"^.*?(?:no\s+specific|no\s+explicit|no\s+direct)\s+(?:mention|definition|provision|clause|detail|information).*?[.\n]", re.IGNORECASE | re.MULTILINE),
    # "do not contain information" / "does not contain"
    re.compile(r"^.*?(?:do(?:es)?\s+not\s+contain|cannot\s+find|could\s+not\s+find|unable\s+to\s+find)\s+(?:information|details?|data).*?[.\n]", re.IGNORECASE | re.MULTILINE),
    # "insufficient information" / "not available"
    re.compile(r"^.*?(?:insufficient|inadequate)\s+(?:information|details?|evidence|data).*?[.\n]", re.IGNORECASE | re.MULTILINE),
    # "If you need specific information about X, please clarify"
    re.compile(r"^.*?(?:if you need|please\s+(?:clarify|provide|specify)).*?(?:specific|additional|more)\s+(?:information|context|details?).*?[.\n]", re.IGNORECASE | re.MULTILINE),
    # "However, if you are referring to..." type hedging
    re.compile(r"^However,\s+(?:if\s+you\s+are\s+referring|this\s+is\s+not).*?[.\n]", re.IGNORECASE | re.MULTILINE),
]


def _strip_contradictory_disclaimers(answer_text: str) -> str:
    """
    Remove refusal-style disclaimers from answers that are actually supported.
    Only called for 'supported' and 'partially_supported' states.

    Safety rule: if stripping would leave an empty answer, return the original
    so the user always sees meaningful content rather than just the appended Note.
    """
    txt = answer_text
    for pat in _CONTRADICTORY_DISCLAIMER_PATTERNS:
        txt = pat.sub("", txt)

    # Clean up leading whitespace/newlines after stripping
    txt = re.sub(r"^\s*\n+", "", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    stripped = txt.strip()

    # Never return an empty string — always show something meaningful
    if not stripped:
        log.warning("_strip_contradictory_disclaimers wiped the entire answer — keeping original")
        return answer_text.strip()

    return stripped


# =============================================================================
# Source-Mode-Aware Wording
# =============================================================================

# Fallback messages when no evidence is found, per source mode
_NO_EVIDENCE_FALLBACK = {
    "documents": "I'm sorry, I couldn't find any information about that in the PERA documents.",
    "stored_api": "I couldn't find that in the stored API data. Try switching to Documents or Live API mode.",
    "both": "I'm sorry, I couldn't find any information about that in the available documents or stored API data.",
    "live_api": "I'm sorry, I couldn't find any information about that via the live API.",
}

def _get_no_evidence_message(answer_source_mode: str = "both") -> str:
    return _NO_EVIDENCE_FALLBACK.get(answer_source_mode, _NO_EVIDENCE_FALLBACK["both"])


def _apply_support_state_wording(answer_text: str, support_state: str,
                                  answer_source_mode: str = "both") -> str:
    """
    Apply clean, professional wording based on support state.
    Source-mode-aware: uses 'stored API data' when in stored_api mode
    instead of always referencing 'documents'.
    """
    # Determine wording based on source mode
    is_api_mode = answer_source_mode == "stored_api"
    is_both_mode = answer_source_mode == "both"
    source_label = (
        "stored API data" if is_api_mode else
        "available documents and stored API data" if is_both_mode else
        "available documents"
    )

    if support_state == "supported":
        return _strip_contradictory_disclaimers(answer_text)

    elif support_state == "partially_supported":
        cleaned = _strip_contradictory_disclaimers(answer_text)
        # If the answer was stripped to nothing, just return it as-is (no note needed)
        if not cleaned:
            return answer_text.strip()
        if is_api_mode:
            return (
                cleaned + "\n\n"
                "**Note:** This answer is based on the stored API data snapshots, "
                "which may not reflect the very latest state. For real-time data, "
                "try Live API mode."
            )
        return (
            cleaned + "\n\n"
            f"**Note:** This answer is based on {source_label}, which may not "
            "cover all aspects of this topic. Please consult the original regulatory "
            "documents for complete details."
        )

    elif support_state == "conflicting":
        cleaned = _strip_contradictory_disclaimers(answer_text)
        return (
            cleaned + "\n\n"
            f"**Note:** Multiple sources in the {source_label} address this "
            "topic with potentially differing details. The information above reflects "
            "the most relevant data found."
        )

    elif support_state == "unsupported":
        cleaned = _strip_contradictory_disclaimers(answer_text)
        return (
            cleaned + "\n\n"
            f"**Note:** The {source_label} do not directly address this "
            "specific query. The information above is based on the closest related "
            "data found. For definitive guidance, please consult PERA "
            "administration directly."
        )

    # unknown state — return as-is
    return answer_text


# =============================================================================
# Main Answer Function
# =============================================================================
def answer_question(
    current_question: str,
    retrieval: Dict[str, Any],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    answer_source_mode: str = "both",
) -> Dict[str, Any]:
    client = get_chat_client()

    # 0. Creator question intercept
    if _is_creator_question(current_question):
        return {"answer": _CREATOR_RESPONSE, "references": [], "decision": "answer",
                "support_state": "supported"}

    # 0b. Short-query expansion: "pera?" → "What is PERA?", "SSO?" → "Tell me about System Support Officer"
    # Ultra-short queries confuse the LLM into refusing them as "too vague"
    q_stripped = re.sub(r'[?!.\s]+$', '', current_question).strip().lower()
    if len(q_stripped.split()) <= 2:
        _SHORT_EXPANSIONS = {
            "pera": "What is PERA (Punjab Enforcement and Regulatory Authority)?",
        }
        # Check direct mapping first
        if q_stripped in _SHORT_EXPANSIONS:
            current_question = _SHORT_EXPANSIONS[q_stripped]
            log.info("Short-query expanded: '%s' -> '%s'", q_stripped, current_question)
        else:
            # For abbreviations like "SSO?", "CTO?", expand to "Tell me about {expanded}"
            if q_stripped in _PERA_ABBREV:
                current_question = f"Tell me about the {_PERA_ABBREV[q_stripped]} position at PERA"
                log.info("Short-query expanded (abbrev): '%s' -> '%s'", q_stripped, current_question)

    # 1. Build Context
    context_str = format_evidence_for_llm(retrieval, question=current_question)
    if not context_str:
        return {
            "answer": _get_no_evidence_message(answer_source_mode),
            "references": [],
            "decision": "refuse",
            "support_state": "unsupported"
        }

    # 1b. Pre-generation evidence quality gate — REMOVED.
    # If FAISS found evidence and it passed into context, the LLM should always try.
    # The only refuse point is empty context (no evidence at all).
    evidence_list = retrieval.get("evidence", [])

    # 2. System Prompt (v6: source-mode-aware)
    # Base prompt for all modes
    _base_prompt = (
        "You are the PERA AI Assistant for Punjab Enforcement and Regulatory Authority (PERA). "
        "You specialize in answering questions about PERA's regulations, structure, roles, operations, and policies.\n\n"

        "CORE RULES\n"
        "1) Answer using ONLY the provided Context. Do not use external knowledge.\n"
        "2) Do not invent facts (powers, procedures, numbers, dates, thresholds, authorities).\n"
        "3) Do NOT infer authority from seniority or job title unless explicitly stated in Context.\n\n"

        "ANSWER APPROACH\n"
        "4) The Context below contains pre-selected relevant evidence. Your PRIMARY JOB is to extract "
        "and present information from it. ALWAYS give a substantive answer when evidence is present.\n"
        "5) If the answer requires combining information from multiple evidence sections, "
        "do so. This is normal.\n"
        "6) SALARY/PAY RULES: If the Context contains salary, pay scale, SPPP, BPS, or appointment "
        "references for the asked position, STATE THE EXACT VALUES directly (e.g., 'SPPP-3', "
        "'BPS-17', 'BS-20'). Do NOT say 'specific details not provided' when the values ARE in Context.\n"
        "7) If the Context contains conflicting statements, present both positions neutrally.\n"
        "8) If the question is completely unrelated to PERA (weather, sports, cooking), "
        "say the question is outside PERA's scope.\n\n"

        "CRITICAL: AVOID UNNECESSARY REFUSALS\n"
        "9) NEVER say 'not explicitly defined', 'not explicitly mentioned', "
        "'not found in the documents', 'insufficient information', or "
        "'cannot provide specific details' if the Context has ANY relevant content.\n"
        "10) If the Context only partially covers the topic, answer what IS available "
        "and clearly state what specific aspect is not covered.\n"
        "11) When salary/pay data exists in the Context, ALWAYS include it in your answer.\n\n"

        "INTERPRETATION\n"
        "12) Treat 'powers', 'functions', and 'duties' as synonyms unless Context distinguishes them.\n"
        "13) For roles: if Context describes a position's purpose, responsibilities, qualifications, "
        "salary, or reporting structure, present ALL of it as the answer.\n\n"

        "REFERENCES — DO NOT INCLUDE IN ANSWER\n"
        "14) The UI shows references separately. Do NOT output Source/References/page numbers.\n"
        "15) Only mention document/page if the USER explicitly asks for it.\n\n"

        "STYLE\n"
        "16) Professional, composed, concise. Use Markdown formatting.\n"
        "17) ALWAYS answer in English, regardless of the language the user asked in.\n"
        "18) When evidence says 'Salary and Benefits: Pay & Benefits equivalent to BPS-XX', "
        "interpret this as: the salary IS the BPS/SPPP scale mentioned. State it directly.\n\n"

        "COMPLETENESS (CRITICAL)\n"
        "19) EXHAUSTIVE EXTRACTION: If the Context contains a LIST of items — include ALL of them. "
        "Do NOT truncate or summarize lists. Present every item.\n"
        "20) MULTI-SECTION SYNTHESIS: Scan ALL evidence sections and combine information.\n"
        "21) SECTION COVERAGE: Extract and present ALL content under each relevant heading.\n"
        "22) MULTI-PART QUESTIONS: Address EVERY aspect separately. Do not skip any part.\n"
        "23) ENTITY-SALARY LINKAGE: Link position descriptions with salary table data.\n\n"
    )

    # Source-mode-specific prompt additions
    _mode_prompts = {
        "stored_api": (
            "SOURCE TYPE: STORED API DATA\n"
            "The Context below comes from stored API data snapshots (not documents).\n"
            "24) When the context contains structured API records (e.g., lists of divisions, "
            "districts, workforce data, finance data), present them as clean, structured lists.\n"
            "25) For list/lookup questions (e.g., 'list divisions', 'show districts', 'what divisions'), "
            "present a direct, well-formatted list from the data. Use bullet points or numbered lists.\n"
            "26) Do NOT say 'not found in PERA documents' — the data source is stored API data, "
            "not documents. If data is missing, say 'not found in the stored API data'.\n"
            "27) Extract and present ALL records from the API data. Do not summarize or truncate.\n"
            "28) CRITICAL: API data about inspections, challans, requisitions, officers, and performance "
            "statistics IS PERA data. NEVER say 'outside PERA scope' when API evidence is present.\n\n"
        ),
        "both": (
            "SOURCE TYPE: MIXED (DOCUMENTS + STORED API DATA)\n"
            "The Context below may contain BOTH document-based evidence AND stored API data records.\n"
            "24) Evidence tagged with source_type='api' comes from stored API snapshots.\n"
            "25) Evidence without source_type='api' comes from regulatory documents.\n"
            "26) When BOTH types are relevant, EXPLICITLY combine them: first present factual API data "
            "(e.g., lists of divisions, counts, names), then add document-based context and explanation.\n"
            "27) For list/lookup questions, lead with the structured API data, then supplement with "
            "document explanation if available.\n"
            "28) Clearly present ALL API records — do not summarize a list into a paragraph.\n"
            "29) CRITICAL: API data about inspections, challans, requisitions, officers, and performance "
            "statistics IS PERA data. NEVER say 'outside PERA scope' when API evidence contains "
            "inspection counts, challan data, officer performance, or any operational data. "
            "This data comes from PERA's own operational systems.\n"
            "30) When the Context contains numeric data (counts, amounts, totals) from API sources, "
            "ALWAYS present those numbers in your answer — do not ignore them.\n\n"
        ),
        "documents": (
            "SOURCE TYPE: REGULATORY DOCUMENTS\n"
            "The Context below comes from PERA regulatory documents, policies, and rules.\n\n"
        ),
    }

    mode_addition = _mode_prompts.get(answer_source_mode, _mode_prompts["documents"])

    # Detect if evidence contains API-sourced data and add freshness note
    _has_api_evidence = any(
        h.get("source_type") == "api"
        for doc_group in evidence_list
        for h in doc_group.get("hits", [])
    )
    freshness_note = ""
    if _has_api_evidence:
        freshness_note = (
            "DATA FRESHNESS: Some evidence below comes from API data snapshots "
            "that are periodically synchronized. When presenting API-sourced data, "
            "note that it reflects the last sync and may not be real-time.\n\n"
        )

    system_prompt = (
        _base_prompt
        + mode_addition
        + freshness_note
        + "CONTEXT (do not quote or reproduce tags):\n"
        + context_str
    )

    # 3. Construct Messages
    messages = [{"role": "system", "content": system_prompt}]

    if conversation_history:
        valid_history = [m for m in conversation_history if m.get("role") in ("user", "assistant")]
        messages.extend(valid_history[-4:])

    messages.append({"role": "user", "content": current_question})

    # 4. Call LLM — with one automatic retry on transient failure
    def _call_llm(msgs):
        for attempt in range(2):
            try:
                return client.chat.completions.create(
                    model=ANSWER_MODEL,
                    messages=msgs,
                    temperature=0.0,
                )
            except Exception as exc:
                if attempt == 0:
                    log.warning("LLM call attempt 1 failed (%s) — retrying…", exc)
                    import time; time.sleep(1.5)
                else:
                    raise
    try:
        response = _call_llm(messages)
        answer_text = response.choices[0].message.content or ""

        # --- Conditional Multi-Pass Refinement ---
        # Triggers when evidence is rich but answer is short, indicating missed information.
        # Now includes salary queries since salary-bridge ensures correct context.
        evidence_section_count = context_str.count("<evidence ")
        answer_word_count = len(answer_text.split())
        if evidence_section_count >= 5 and answer_word_count < 100:
            log.info("Refinement triggered: %d evidence sections but only %d words in answer",
                     evidence_section_count, answer_word_count)
            refinement_prompt = (
                "You previously answered the question below. However, the context contains "
                f"{evidence_section_count} evidence sections and your answer may be incomplete.\n\n"
                "TASK: Review your answer against ALL the context sections below. "
                "If ANY relevant information from the context is missing from your answer, "
                "produce an EXPANDED answer that includes ALL missing details. "
                "Do NOT just repeat your previous answer — ADD the missing information.\n"
                "If your answer is already complete, return it as-is.\n\n"
                f"ORIGINAL QUESTION: {current_question}\n\n"
                f"YOUR PREVIOUS ANSWER:\n{answer_text}\n\n"
                "CONTEXT (same as before):\n"
                f"{context_str}"
            )
            try:
                refine_response = client.chat.completions.create(
                    model=ANSWER_MODEL,
                    messages=[{"role": "system", "content": refinement_prompt}],
                    temperature=0.0,
                )
                refined = refine_response.choices[0].message.content or ""
                # Only use refinement if it's meaningfully longer
                if len(refined.split()) > answer_word_count + 20:
                    log.info("Refinement expanded answer: %d → %d words",
                             answer_word_count, len(refined.split()))
                    answer_text = refined
                else:
                    log.info("Refinement did not expand answer — keeping original")
            except Exception as e:
                log.warning("Refinement pass failed: %s — using initial answer", e)

        # Strip references ONLY if user did NOT explicitly ask for them
        if not _user_wants_references(current_question):
            answer_text = _strip_answer_references(answer_text)

        # 5. Post-generation grounding verification (AUDIT ONLY — never refuses)
        grounding = verify_grounding(
            answer_text=answer_text,
            evidence_list=evidence_list,
            context_str=context_str,
            question=current_question,
        )

        # 6. Classify support state (for audit and wording — NOT for refusal)
        support_state = _classify_support_state(grounding, answer_text)
        log.info("Support state: %s (grounding score=%.3f, semantic=%s)",
                 support_state, grounding.score, grounding.semantic_support or "n/a")

        refs = extract_references_simple(retrieval, question=current_question, answer_text=answer_text)

        # 7. CRITICAL DESIGN RULE:
        # If evidence was found and the LLM generated an answer, ALWAYS show it.
        # Grounding is audit metadata, NOT a kill switch.
        # The only refusal point is the pre-generation gate above (no evidence at all).
        #
        # For a government chatbot, refusing when evidence exists is worse than
        # showing a potentially imperfect answer with references the user can verify.

        # Apply wording based on support state (never replaces answer with refusal)
        if support_state == "unsupported":
            # Even for "unsupported" grounding, show the LLM's answer with a note
            support_state = "partially_supported"
            log.info("Grounding flagged unsupported but evidence exists — showing answer anyway")

        final_answer = _apply_support_state_wording(answer_text, support_state,
                                                      answer_source_mode=answer_source_mode)

        decision = "answer"

        result = {
            "answer": final_answer,
            "references": refs,
            "decision": decision,
            "support_state": support_state,
            "grounding": {
                "confidence": grounding.confidence,
                "score": grounding.score,
                "semantic_support": grounding.semantic_support,
                "support_state": support_state,
            },
        }

        if grounding.confidence == "low":
            log.info("Low grounding confidence (%.3f) — answer shown with note", grounding.score)

        return result

    except Exception as e:
        log.error("LLM call failed: %s", e, exc_info=True)
        return {
            "answer": "I encountered an error while processing your request. Please try again.",
            "references": [],
            "decision": "error",
            "support_state": "error"
        }
