"""
PERA AI Retriever (Brain 2.0)
Simplified, robust semantic search without manual heuristic filtering.
(Updated: FAISS IDMap-safe + query normalization + safer keyword + dedupe)
"""
from __future__ import annotations

import os
import json
import hashlib
from typing import List, Dict, Any, Optional, Tuple

from openai_clients import get_chat_client, LLM_REWRITE_MODEL
from index_store import embed_texts
from index_cache import get_cached_index
from reranker import rerank_hits
from log_config import get_logger

import numpy as np
import re as _re
from collections import defaultdict

log = get_logger("pera.retriever")

# Configuration (tightened in Part 2)
TOP_K = int(os.getenv("RETRIEVER_TOP_K", "30"))
SIM_THRESHOLD = float(os.getenv("RETRIEVER_SIM_THRESHOLD", "0.18"))
MAX_HITS_PER_DOC_RETRIEVAL = int(os.getenv("MAX_HITS_PER_DOC_RETRIEVAL", "12"))

# Pre-compiled regex for stripping Urdu/Roman Urdu particles from queries before embedding
_URDU_PARTICLES_RETRIEVER = _re.compile(
    r"\b(?:ki|ka|ke|ko|kya|hai|hain|hy|hen|aur|ya|mein|se|par|pe|ye|yeh|woh|"
    r"nahi|nhi|na|ho|tha|thi|batao|btao|bataen|bataiye|batayein|btaen|"
    r"kitni|kitna|kitne|kahan|kaun|konsi|kaunsi|wala|wali|wale|"
    r"hota|hoti|hote|kaise|kaisa|kesi|kaisi|abhi|kab|jab|tab)\b",
    _re.IGNORECASE
)

# Abbreviation -> full expansion (for embedding search quality)
_ABBREV_MAP_RAW = {
    "cto": "Chief Technology Officer",
    "dg": "Director General",
    "adg": "Additional Director General",
    "ddg": "Deputy Director General",
    "dd": "Deputy Director",
    "mgr": "Manager",
    "hr": "Human Resources",
    "it": "Information Technology",
    "eo": "Enforcement Officer",
    "io": "Investigation Officer",
    "sso": "System Support Officer",
    "sdeo": "Sub-Divisional Enforcement Officer",
    "deo": "Data Entry Operator",
    "dba": "Database Administrator",
    "se": "Software Engineer",
    "admin": "Administration",
    "tor": "Terms of Reference",
    "jd": "Job Description",
    "sr": "Service Rules",
    "Schedule-I": "Organizational Structure",
    "Schedule-II": "Appointment & Conditions of Service",
    "Schedule-III": "Special Pay Package PERA (SPPP)",
    "Schedule-IV": "Rules / Regulations Adopted by the Authority",
    "Schedule-V": "Transfer and Posting",
    "Schedule-VI": "Special Allowance and Benefits",
    "sppp": "Special Pay Package PERA",
    "faqs": "Frequently Asked Questions",
    "gli": "Group Life Insurance",
}

def _norm_key(s: str) -> str:
    # Keep alphanumerics; makes "Schedule-I" -> "schedulei"
    return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())

# Normalize abbrev keys once so Schedule-I etc. work
_ABBREV_MAP = {_norm_key(k): v for k, v in _ABBREV_MAP_RAW.items()}

# Smart Context Expansion Keywords
# If query contains these, we fetch adjacent pages (±RADIUS) to capture tables/schedules
_EXPANSION_KEYWORDS = {
    "salary", "pay", "allowance", "benefit", "scale", "sppp", "grade", "compensation",
    "detail", "full", "sab kuch", "batao", "explain", "structure",
    # Role/position keywords — salary data often follows job description on next page
    "manager", "officer", "director", "appointment", "development",
    # Roman Urdu / misspellings
    "salay", "tankhwah", "tankha", "kitni", "payscale", "pay scale",
    "maaash", "maash",
}
_EXPANSION_RADIUS = 2  # Adjacent pages needed for salary/SPPP data that follows job descriptions


def _normalize_vec(v: np.ndarray) -> np.ndarray:
    v = np.asarray(v, dtype=np.float32)
    n = float(np.linalg.norm(v) + 1e-12)
    return v / n

# Numbered entity normalization: "Schedule 3" / "Section three" / "Rule-5" → canonical form
_ARABIC_TO_ROMAN = {"1": "I", "2": "II", "3": "III", "4": "IV", "5": "V", "6": "VI",
                    "7": "VII", "8": "VIII", "9": "IX", "10": "X",
                    "11": "XI", "12": "XII", "13": "XIII", "14": "XIV", "15": "XV"}
_WORD_TO_ROMAN = {
    "one": "I", "two": "II", "three": "III", "four": "IV", "five": "V", "six": "VI",
    "seven": "VII", "eight": "VIII", "nine": "IX", "ten": "X",
    "first": "I", "second": "II", "third": "III", "fourth": "IV", "fifth": "V", "sixth": "VI",
    # Urdu / Roman Urdu
    "ek": "I", "do": "II", "teen": "III", "char": "IV", "panch": "V", "chhe": "VI",
    "aik": "I", "doo": "II",
}
_VALID_ROMAN = {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
                "XI", "XII", "XIII", "XIV", "XV"}

# All document entity types that use numbered references in PERA docs
_NUMBERED_ENTITY_RE = _re.compile(
    r"\b(schedule|section|rule|chapter|annex|part|clause|article|appendix)"
    r"[\s\-_]+"
    r"(\w+)\b",
    _re.IGNORECASE
)


def _normalize_numbered_references(query: str) -> str:
    """Convert numbered entity references to canonical 'Entity-ROMAN' format.
    
    Handles: Schedule/Section/Rule/Chapter/Annex/Part/Clause/Article/Appendix
    Formats: arabic (3), word (three/third), Urdu (teen), roman (III/iii)
    
    Examples:
        'schedule 3'        → 'Schedule-III'
        'section five'      → 'Section-V'
        'rule 10'           → 'Rule-X'
        'chapter two'       → 'Chapter-II'
        'annex iii'         → 'Annex-III'
    """
    def _replace(m):
        entity = m.group(1).capitalize()
        num_part = m.group(2).lower()
        # Try arabic number
        if num_part in _ARABIC_TO_ROMAN:
            return f"{entity}-{_ARABIC_TO_ROMAN[num_part]}"
        # Try word number
        if num_part in _WORD_TO_ROMAN:
            return f"{entity}-{_WORD_TO_ROMAN[num_part]}"
        # Already roman?
        if num_part.upper() in _VALID_ROMAN:
            return f"{entity}-{num_part.upper()}"
        # Not a number — keep original (e.g., "Schedule of", "Section on")
        return m.group(0)

    return _NUMBERED_ENTITY_RE.sub(_replace, query)


# Keep backward-compatible alias
_normalize_schedule_references = _normalize_numbered_references


# PERA-domain vocabulary used for lightweight typo correction. Any query
# word that is a near-miss (Levenshtein ratio >= 0.85) of one of these
# terms gets auto-corrected before abbreviation expansion and role
# detection run. This saves users from losing retrieval quality because
# they typed "develppment" instead of "development".
_PERA_VOCAB = frozenset([
    # Role prefixes
    "manager", "officer", "director", "assistant", "deputy", "chief",
    "head", "coordinator", "superintendent", "inspector", "registrar",
    "analyst", "hearing", "enforcement",
    # Specializations / departments
    "development", "operations", "enforcement", "investigation",
    "planning", "finance", "admin", "administration", "compliance",
    "communication", "legal", "audit", "procurement", "training",
    "monitoring", "evaluation", "research", "coordination", "security",
    "transport", "estate", "quality", "technology", "infrastructure",
    "data", "systems", "support", "services",
    # Common PERA nouns
    "authority", "regulation", "regulations", "regulatory", "punjab",
    "notification", "schedule", "salary", "responsibilities", "powers",
    "reporting", "committee", "chairperson", "chairman", "appointment",
    "hierarchy",
])
_TYPO_MIN_LEN = 5  # don't correct short tokens (too many false positives)
_TYPO_RATIO_MIN = 0.85


def _fix_query_typos(query: str) -> str:
    """Cheap per-word typo correction against PERA vocabulary.

    Users regularly mistype domain terms (e.g. 'develppment'). Without
    correction, the explicit-role guard in the LLM rewriter skips
    rewrite, the embedding search degrades, and `_detect_target_role`
    in answerer.py fails its substring match. difflib is stdlib-only
    and fast for short tokens.
    """
    import difflib as _dl
    out: list[str] = []
    for tok in (query or "").split():
        # Preserve trailing punctuation like ? , .
        m = _re.match(r"^(\w+)(\W*)$", tok, _re.UNICODE)
        if not m:
            out.append(tok)
            continue
        word, tail = m.group(1), m.group(2)
        low = word.lower()
        if len(low) < _TYPO_MIN_LEN or low in _PERA_VOCAB or not low.isalpha():
            out.append(tok)
            continue
        match = _dl.get_close_matches(low, _PERA_VOCAB, n=1, cutoff=_TYPO_RATIO_MIN)
        if match and match[0] != low:
            out.append(match[0] + tail)
        else:
            out.append(tok)
    return " ".join(out)


def _expand_abbreviations(query: str) -> str:
    """Expand known abbreviations in-place for better embedding matches."""
    # First normalize schedule references
    query = _normalize_schedule_references(query)

    # Correct common PERA-domain typos before role detection and
    # abbreviation expansion downstream stages rely on clean spelling.
    query = _fix_query_typos(query)

    # Normalize possessive forms: "manager's development" → "manager development"
    query = _re.sub(r"[''\u2019]s\b", "", query)

    # Role title normalization: "manager development" → also add "Manager (Development)"
    # In PERA PDFs, role titles use parentheses like "Manager (Development)"
    _role_prefix_re = _re.compile(
        r"\b(manager|officer|director|assistant|deputy|chief|head|coordinator|superintendent)\s+"
        r"(development|operations|enforcement|investigation|planning|hr|it|finance|admin|"
        r"administration|compliance|communication|legal|audit|procurement|training|monitoring|"
        r"evaluation|research|coordination|security|transport|estate|quality|technology|"
        r"infrastructure|data|systems?|support|services?)\b",
        _re.IGNORECASE
    )
    role_match = _role_prefix_re.search(query)
    if role_match:
        role = role_match.group(1).title()
        spec = role_match.group(2).title()
        # REPLACE the matched text with parenthesized form (was: appending)
        query = query[:role_match.start()] + f"{role} ({spec})" + query[role_match.end():]

    words = (query or "").split()
    expanded = []
    for w in words:
        key = _norm_key(w)
        if key in _ABBREV_MAP:
            expanded.append(_ABBREV_MAP[key])
        else:
            expanded.append(w)
    return " ".join(expanded)


def _get_page_map_by_id(id_map: Dict[int, Dict[str, Any]]) -> Dict[Tuple[str, int], List[int]]:
    """
    Build map of (doc_name, page) -> list of chunk IDs (NOT list indices).
    """
    m: Dict[Tuple[str, int], List[int]] = defaultdict(list)
    for cid, r in id_map.items():
        doc = r.get("doc_name", "Unknown")
        page = r.get("loc_start")
        if isinstance(page, int):
            m[(doc, page)].append(cid)
    return m


def _get_section_map_by_id(
    id_map: Dict[int, Dict[str, Any]]
) -> Dict[Tuple[str, str], List[int]]:
    """
    Phase 3: build map of (doc_name, section_id) -> list of chunk IDs.
    Used for parent-section companion fetching.

    Backward compatibility: chunks indexed before Phase 3 will have no
    `section_id` field. Those chunks are intentionally EXCLUDED from this
    map, so older indexes simply don't benefit from section-level fetch —
    they keep working via page-adjacency expansion. No migration is
    required to load; a rebuild populates the map.
    """
    m: Dict[Tuple[str, str], List[int]] = defaultdict(list)
    for cid, r in id_map.items():
        sid = r.get("section_id")
        if not sid:
            continue
        doc = r.get("doc_name", "Unknown")
        m[(doc, sid)].append(cid)
    return m



# =============================================================================
# Main Retrieval Logic
# =============================================================================
def _evidence_id(text: str) -> str:
    """Short hash for evidence traceability."""
    return hashlib.sha256((text or "")[:500].encode()).hexdigest()[:10]


# Patterns for the lexical role-anchor in retrieve(): capture phrases
# like "manager (development)", "manager development", "chief technology
# officer", "assistant manager monitoring", "senior manager academics".
# The anchor injects chunks that literally start with "Position Title: -
# <role>" so role-subject chunks can't lose to role-mentioning chunks
# on embeddings alone.
_ROLE_PHRASE_RES = [
    # parenthesised: "Manager (Development)"
    _re.compile(
        r"\b((?:senior|assistant|deputy|additional|joint|regional)\s+)?"
        r"(manager|director|officer|coordinator|superintendent|head|chief)"
        r"\s*\(\s*([A-Za-z][A-Za-z\s&/]{1,40}?)\s*\)",
        _re.IGNORECASE,
    ),
    # "<prefix> <spec>" where spec is a single noun
    _re.compile(
        r"\b((?:senior|assistant|deputy|additional|joint|regional)\s+)?"
        r"(manager|director|officer|coordinator|superintendent|head|chief)"
        r"\s+([A-Za-z][A-Za-z&]{2,30})\b",
        _re.IGNORECASE,
    ),
]


def _extract_role_phrase_from_query(question: str) -> str:
    """Pull a normalised role-phrase from the user's question.

    Returns phrasing suitable for substring matching against chunk
    text, e.g. 'manager (development)'. If nothing matches, returns ''.
    """
    q = (question or "").strip()
    if not q:
        return ""
    for pat in _ROLE_PHRASE_RES:
        m = pat.search(q)
        if not m:
            continue
        prefix = (m.group(1) or "").strip()
        prefix_full = (m.group(2) or "").strip()
        spec = (m.group(3) or "").strip()
        if not prefix_full or not spec:
            continue
        # Avoid tokens the answerer treats as non-specific.
        if spec.lower() in {
            "the", "and", "for", "has", "who", "what", "why", "where",
            "when", "does", "shall", "will", "may", "can",
            "salary", "role", "duties",
        }:
            continue
        if prefix:
            core = f"{prefix} {prefix_full}".lower()
        else:
            core = prefix_full.lower()
        # Prefer parenthesised form since it matches PERA's JD layout.
        if "(" in q and ")" in q:
            return f"{core} ({spec.lower()})"
        return f"{core} {spec.lower()}"
    return ""


# Normalization for cross-doc content fingerprinting.
# Strips role-context prefix ([Role: …]\n) and whitespace so two chunks
# with the same regulatory text but different surrounding formatting
# still collide and get deduplicated.
_ROLE_PREFIX_STRIP_RE = _re.compile(r"^\s*\[Role:[^\]]+\]\s*\n", _re.IGNORECASE)
_NON_ALNUM_RE = _re.compile(r"[^a-z0-9]+")


def _content_fingerprint(text: str, prefix_chars: int = 500) -> str:
    """Produce a stable fingerprint for cross-document dedup.

    Normalizes by:
      • removing the "[Role: …]" prefix the chunker injects,
      • lower-casing,
      • collapsing all non-alphanumerics to single spaces,
      • taking the first `prefix_chars` characters.

    Empty string return means "do not dedup this" (fallback for empty text).
    """
    if not text:
        return ""
    s = _ROLE_PREFIX_STRIP_RE.sub("", text)
    s = s.lower()
    s = _NON_ALNUM_RE.sub(" ", s).strip()
    if not s:
        return ""
    s = s[:prefix_chars]
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def retrieve(
    question: str,
    index_dir: Optional[str] = None,
    source_type_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Semantic search + reranking + smart expansion + keyword fallback.
    Uses in-memory cached index (automatic blue/green invalidation).

    Parameters
    ----------
    source_type_filter
        When set to ``"document"`` or ``"api"``, restricts results to rows
        whose ``source_type`` matches. ``None`` means no filter (default).
        Used by the source-mode toggle (documents / stored_api / both).
    """
    idx, rows, id_map, token_index, resolved_dir = get_cached_index()

    empty_result = {
        "question": question,
        "has_evidence": False,
        "evidence": []
    }

    if idx is None or not id_map:
        log.warning("No index available (cache dir: %s)", resolved_dir)
        return empty_result

    # 1) Expand abbreviations + embed query
    expanded_q = _expand_abbreviations(question)
    if expanded_q != question:
        log.debug("Expanded: '%s' -> '%s'", question, expanded_q)

    # Strip Urdu/Roman Urdu particles from primary query for better embedding match
    clean_q = _URDU_PARTICLES_RETRIEVER.sub(' ', expanded_q)
    clean_q = _re.sub(r'\s+', ' ', clean_q).strip()
    if clean_q != expanded_q:
        log.debug("Urdu-stripped: '%s' -> '%s'", expanded_q[:50], clean_q[:50])

    # Reorder "Role (Spec) attr" or "role spec attr" → "attr of Role (Spec)"
    # FAISS embeddings for "salary of X" consistently retrieve salary tables better
    # After _expand_abbreviations, "manager development salary" becomes
    # "Manager (Development) salary" — this regex handles both forms.
    _REORDER_RE = _re.compile(
        r'^((?:senior\s+|assistant\s+|deputy\s+)?(?:manager|director|head))'
        r'(?:\s+\((\w+(?:\s+\w+)?)\)|\s+(\w+(?:\s+\w+)?))'  # (spec) or spec
        r'\s+(salary|pay\s*(?:scale)?|benefit|allowance|appointment)\s*\??$',
        _re.IGNORECASE
    )
    m_reorder = _REORDER_RE.match(clean_q.strip())
    if m_reorder:
        role = m_reorder.group(1).strip()
        spec = (m_reorder.group(2) or m_reorder.group(3)).strip()
        attr = m_reorder.group(4).strip()
        clean_q = f"{attr} of {role} ({spec})"
        log.info("Query reordered for retrieval: '%s' -> '%s'", expanded_q[:50], clean_q[:50])

    # Generate parenthesized title variant for PERA documents:
    # "manager development salary" → "Manager (Development) salary"
    # Also generate reordered: "salary of Manager (Development)"
    # This matches how PERA docs actually name positions.
    parenthesized_variant = ""
    _PREFIX_SPEC_FIX = _re.compile(
        r'\b((?:senior\s+|assistant\s+|deputy\s+)?(?:manager|director|head))\s+'
        r'(\w+(?:\s+\w+)?)\s+(salary|pay|benefit|scale|appointment)',
        _re.IGNORECASE
    )
    m = _PREFIX_SPEC_FIX.search(clean_q)
    if m:
        prefix = m.group(1).strip()
        spec = m.group(2).strip()
        suffix = m.group(3).strip()
        # Reordered query: "salary of Manager (Development)" — this phrasing
        # consistently produces better FAISS embeddings for salary retrieval
        parenthesized_variant = f"{suffix} of {prefix} ({spec})"
        log.debug("Parenthesized variant: '%s'", parenthesized_variant)
    # Use cleaned query for embedding, keep expanded_q for display/logging
    embed_q = clean_q or expanded_q

    # Build simplified core-terms query for dual retrieval
    _stop_core = {
        "what", "who", "is", "the", "a", "an", "of", "in", "for", "and", "how", "where",
        "when", "which", "does", "was", "are", "do", "can", "will", "shall", "should",
        "about", "tell", "me", "explain", "describe", "give", "show", "detail", "details",
        "kya", "hai", "kon", "kaun", "ki", "ka", "ke", "se", "ko", "ne", "ye", "yeh",
        "kia", "hain", "mein", "par", "say", "batao", "bataein",
    }
    core_words = [w for w in (embed_q or "").lower().split() if w not in _stop_core and len(w) > 1]
    core_query = " ".join(core_words[:8]) if core_words else embed_q

    try:
        log.debug("Dual-query embedding: primary='%s', core='%s'", embed_q[:80], core_query[:80])
        # Build list of queries: primary, core, and optional parenthesized variant
        queries_to_embed = [embed_q, core_query]
        if parenthesized_variant:
            queries_to_embed.append(parenthesized_variant)
            log.debug("Triple-query search: adding variant '%s'", parenthesized_variant[:60])
        vectors = embed_texts(queries_to_embed)
        qv_primary = _normalize_vec(vectors[0])
        qv_core = _normalize_vec(vectors[1])
        qv_variant = _normalize_vec(vectors[2]) if parenthesized_variant else None
        log.debug("Embeddings done (n=%d).", len(queries_to_embed))
    except Exception as e:
        log.error("Embedding failed: %s", e, exc_info=True)
        return empty_result

    # 2) Search FAISS with ALL queries, fuse with Reciprocal Rank Fusion (RRF).
    #
    # Phase 3 change: the prior implementation merged variants by taking the
    # MAX cosine per chunk, which under-weighted chunks that ranked
    # consistently well across every variant (mid-rank everywhere) compared
    # to chunks that happened to spike high on a single variant. For
    # multi-page / continued-topic queries, the "consistent mid-rank" chunks
    # are exactly the ones we want to surface.
    #
    # RRF assigns each chunk a score of sum(1 / (RRF_K + rank_i)) across
    # variants. It is the standard fusion method in production RAG.
    #
    # We still keep the max cosine similarity seen as the chunk's "score"
    # for downstream compatibility (SIM_THRESHOLD filtering, logging, and
    # the reranker's semantic term all expect a cosine value).
    RRF_K = int(os.getenv("RETRIEVER_RRF_K", "60"))
    try:
        log.debug("FAISS search with TOP_K=%d per query", TOP_K)
        D1, I1 = idx.search(qv_primary.reshape(1, -1), TOP_K)
        D2, I2 = idx.search(qv_core.reshape(1, -1), TOP_K)
        search_results = [(D1, I1, "primary"), (D2, I2, "core")]

        if qv_variant is not None:
            D3, I3 = idx.search(qv_variant.reshape(1, -1), TOP_K)
            search_results.append((D3, I3, "variant"))

        rrf_scores: Dict[int, float] = {}
        best_sim: Dict[int, float] = {}
        per_variant_counts: Dict[str, int] = {}

        for D_v, I_v, variant_name in search_results:
            n_valid = 0
            for rank, (score, cid) in enumerate(zip(D_v[0], I_v[0])):
                try:
                    ci = int(cid)
                except Exception:
                    continue
                if ci < 0 or ci not in id_map:
                    continue
                rrf_scores[ci] = rrf_scores.get(ci, 0.0) + 1.0 / (RRF_K + rank + 1)
                s = float(score)
                if s > best_sim.get(ci, -1.0):
                    best_sim[ci] = s
                n_valid += 1
            per_variant_counts[variant_name] = n_valid

        # Order chunks by RRF score descending, cap at TOP_K for downstream.
        sorted_rrf = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)[:TOP_K]

        # ───── Lexical role-anchor injection ─────
        # Embedding retrieval is surprisingly weak at distinguishing
        # chunks where a role NAME is the subject (Position Title: X)
        # from chunks where the same role is merely referenced in a
        # Report-To field. For questions like "Manager (Development)
        # reporting to?" the subject chunk usually loses to a handful
        # of subordinate-role chunks that semantically cluster around
        # the same wording.
        #
        # Fix: whenever we can extract a role-shaped phrase from the
        # question (e.g. "manager (development)" or "manager
        # development"), force-inject ANY chunk whose text starts with
        # "Position Title: - <role>" into the top of the fused list.
        # Keep existing chunks after it; trim to TOP_K to preserve the
        # downstream contract.
        try:
            role_phrase = _extract_role_phrase_from_query(question or "")
            if role_phrase:
                anchor_cids: List[int] = []
                anchor_seen = set()
                _pat_pt = _re.compile(
                    r"position\s*title\s*:?\s*-?\s*"
                    + _re.escape(role_phrase.lower()),
                    _re.IGNORECASE,
                )
                _pat_role = _re.compile(
                    r"\[\s*role\s*:?\s*" + _re.escape(role_phrase.lower()),
                    _re.IGNORECASE,
                )
                existing = {ci for ci, _ in sorted_rrf}
                for ci, row in id_map.items():
                    if ci in anchor_seen:
                        continue
                    txt = (row.get("text") or "").lower()
                    if _pat_pt.search(txt) or _pat_role.search(txt):
                        anchor_cids.append(ci)
                        anchor_seen.add(ci)
                if anchor_cids:
                    # Give anchors a synthetic top RRF score so they sort first.
                    top_rrf = sorted_rrf[0][1] if sorted_rrf else 1.0
                    anchor_entries = [(ci, top_rrf + 1.0 - i * 0.01)
                                       for i, ci in enumerate(anchor_cids)]
                    # Prepend anchors, drop them from the tail if already present.
                    rest = [(ci, s) for ci, s in sorted_rrf if ci not in anchor_seen]
                    sorted_rrf = (anchor_entries + rest)[:TOP_K]
                    # Seed best_sim for anchors that FAISS never surfaced.
                    for ci in anchor_cids:
                        best_sim.setdefault(ci, 0.5)
                    log.info(
                        "Role anchor injected %d chunk(s) for role=%r",
                        len(anchor_cids), role_phrase,
                    )
        except Exception as _anchor_err:
            log.debug("Role anchor skipped: %s", _anchor_err)

        # Reconstruct D and I. D carries the max cosine seen (downstream
        # thresholds stay valid); I carries the RRF-ordered IDs.
        D = np.array([[best_sim[cid] for cid, _ in sorted_rrf]], dtype=np.float32)
        I = np.array([[cid for cid, _ in sorted_rrf]])

        log.info(
            "RRF fusion: %d unique chunks across variants (%s) — top RRF score=%.4f",
            len(sorted_rrf),
            ", ".join(f"{k}={v}" for k, v in per_variant_counts.items()),
            sorted_rrf[0][1] if sorted_rrf else 0.0,
        )
    except Exception as e:
        log.error("FAISS search failed: %s", e, exc_info=True)
        return empty_result

    base_ids: List[int] = []
    base_id_set = set()
    for x in I[0]:
        try:
            xi = int(x)
        except Exception:
            continue
        if xi < 0:
            continue
        if xi in id_map:  # ignore stale IDs
            base_ids.append(xi)
            base_id_set.add(xi)

    # --- Multi-Page Expansion (Phase 3: broadened) ---
    #
    # Previously: expansion only fired for queries containing specific
    # keywords (salary, detail, schedule, ...). A topic that spans
    # pages 1-5 for a general query (e.g. "procurement", "leave policy")
    # would NEVER trigger expansion — mid-topic pages were lost.
    #
    # New behavior:
    #   A) ALWAYS expand ±1 page for the top 5 RRF hits (cheap, universal).
    #   B) ±EXPANSION_RADIUS (default 2) for the top 10 hits when EITHER:
    #        • the query contains an expansion keyword, OR
    #        • the hit's text contains a forward reference
    #          ("see Schedule-III", "continued on", "as detailed below",
    #          "following page/section", "next clause", ...).
    #
    # Budget is bounded — radius-2 still applies only to top-10 and
    # dedup via `expanded_ids` set prevents repeats.
    expanded_ids: set = set()

    _FORWARD_REF_RE = _re.compile(
        r"\b(?:see|refer(?:red)?\s+to|as\s+(?:detailed|explained|mentioned|specified|set\s+out))"
        r"\s+(?:also\s+)?(?:below|in|on|hereafter|"
        r"schedule|section|rule|annex(?:ure)?|chapter|part|article|clause|appendix)"
        r"|continued\s+(?:on|from)\s+(?:next|previous|page)"
        r"|(?:on|in)\s+the\s+(?:following|next|subsequent)\s+(?:page|section|clause|sub-?section)"
        r"|(?:please\s+)?(?:see|refer)\s+(?:to\s+)?(?:page|para)\s*\d+"
        r"|\(continued\)"
        r"|\.\.\.\s*$",
        _re.IGNORECASE | _re.MULTILINE
    )

    if base_ids:
        page_map = _get_page_map_by_id(id_map)
        section_map = _get_section_map_by_id(id_map)
        section_cap_per_hit = int(os.getenv("RETRIEVER_SECTION_COMPANIONS_CAP", "4"))

        keyword_expand = any(k in (question or "").lower() for k in _EXPANSION_KEYWORDS)

        for rank, (score, cid) in enumerate(zip(D[0], base_ids)):
            if rank >= 10:
                break
            r = id_map.get(cid)
            if not r:
                continue
            doc = r.get("doc_name")
            page = r.get("loc_start")
            if not (isinstance(page, int) and doc):
                continue

            # Radius-1 for ALL top-5 hits (narrative continuity guarantee).
            # Radius-RADIUS for top-10 when keyword OR forward-ref present.
            if rank < 5:
                radius = 1
            else:
                radius = 0

            hit_text = r.get("text", "") or ""
            has_forward_ref = bool(_FORWARD_REF_RE.search(hit_text))

            if keyword_expand or has_forward_ref:
                radius = max(radius, _EXPANSION_RADIUS)

            if radius > 0:
                for offset in range(-radius, radius + 1):
                    if offset == 0:
                        continue
                    p = page + offset
                    for neighbor_id in page_map.get((doc, p), []):
                        if neighbor_id not in base_id_set:
                            expanded_ids.add(neighbor_id)

            if has_forward_ref:
                log.debug(
                    "Forward-ref expansion on rank=%d doc=%s p=%s (radius=%d)",
                    rank, (doc or "?")[:30], page, radius
                )

            # --- Section companion fetch (Phase 3) ---
            # If this chunk carries a section_id (i.e. was indexed under
            # Phase 3 chunker), pull a bounded set of sibling chunks in
            # the same section. This directly addresses multi-page topics
            # that span the full section (e.g. a Schedule or a Chapter
            # clause) without needing keyword triggers.
            #
            # Gracefully no-ops on legacy indexes that lack section_id.
            if rank < 5:
                sid = r.get("section_id")
                if sid:
                    companions = section_map.get((doc, sid), [])
                    added = 0
                    for sib_id in companions:
                        if sib_id == cid or sib_id in base_id_set or sib_id in expanded_ids:
                            continue
                        expanded_ids.add(sib_id)
                        added += 1
                        if added >= section_cap_per_hit:
                            break
                    if added:
                        log.debug(
                            "Section-companion fetch on rank=%d doc=%s section=%s: +%d chunks",
                            rank, (doc or "?")[:30], sid[:8], added
                        )

    log.info("Multi-page expansion: added %d context chunks.", len(expanded_ids))

    # --- Hybrid Search: Keyword fallback (using precomputed token index) ---
    keyword_hits: Dict[int, float] = {}

    try:
        q_clean = _re.sub(r"[^\w\s]", " ", (expanded_q or "").lower())
        _stop = {
            "kya", "hai", "kon", "kaun", "ki", "ka", "ke", "se", "ko", "ne", "ye", "yeh",
            "what", "who", "is", "the", "a", "an", "of", "in", "for", "and", "how", "where",
            "when", "which", "does", "was", "are", "kia", "hain", "mein", "par", "say",
        }
        q_words = [w for w in q_clean.split() if w not in _stop and len(w) > 1]

        if q_words and token_index:
            # Find candidate chunks via inverted token index (O(k) instead of O(n))
            candidate_ids = set()
            for w in q_words:
                if w in token_index:
                    candidate_ids.update(token_index[w])

            # Only score candidates (much smaller than full chunk set)
            full_phrase = " ".join(q_words[:12]).strip()
            phrase_enabled = len(q_words) > 1 and len(full_phrase) >= 6

            for cid in candidate_ids:
                r = id_map.get(cid)
                if not r:
                    continue
                txt = (r.get("text") or "").lower()

                match_count = sum(1 for w in q_words if w in txt)
                is_phrase_match = phrase_enabled and (full_phrase in txt)

                score = 0.0
                if is_phrase_match:
                    score = 0.72
                else:
                    ratio = match_count / max(1, len(q_words))
                    if ratio == 1.0:
                        score = 0.64
                    elif ratio >= 0.75 and len(q_words) >= 3:
                        score = 0.60
                    elif ratio >= 0.5 and len(q_words) >= 2:
                        score = 0.55

                if score >= 0.55:
                    prev = keyword_hits.get(cid, 0.0)
                    if score > prev:
                        keyword_hits[cid] = score

            log.debug("Keyword fallback: %d candidates from token index, %d hits",
                     len(candidate_ids), len(keyword_hits))

    except Exception as e:
        log.warning("Keyword fallback error: %s", e, exc_info=True)

    # Map FAISS ID -> semantic score
    faiss_scores: Dict[int, float] = {}
    for score, cid in zip(D[0], I[0]):
        try:
            cii = int(cid)
            if cii in id_map:
                faiss_scores[cii] = float(score)
        except Exception:
            continue

    # Final keyword list with a small semantic boost
    final_keyword_list: List[Tuple[int, float]] = []
    for cid, ks in keyword_hits.items():
        base_score = float(ks)
        if cid in faiss_scores:
            base_score += float(faiss_scores[cid]) * 0.10
        final_keyword_list.append((cid, base_score))

    final_keyword_list.sort(key=lambda x: x[1], reverse=True)
    final_keyword_list = final_keyword_list[:10]  # prevent flooding

    # 3) Format results (Grouped by Document) with score filtering + dedupe
    docs_map: Dict[str, Dict[str, Any]] = {}

    def _ensure_doc(doc_name: str, initial_score: float) -> Dict[str, Any]:
        if doc_name not in docs_map:
            docs_map[doc_name] = {
                "doc_name": doc_name,
                "max_score": float(initial_score),
                "hits": [],
                "_seen": set(),  # internal dedupe
            }
        return docs_map[doc_name]

    def _process_hit(chunk_id: int, score_val: float, is_context: bool = False) -> None:
        r = id_map.get(int(chunk_id))
        if not r:
            return

        # Source-mode filter (preserved from current project): when the
        # caller asks for only document or only API rows, drop everything
        # else here so the rest of the pipeline never sees it.
        if source_type_filter is not None:
            row_st = r.get("source_type") or "document"
            if row_st != source_type_filter:
                return

        final_score = float(score_val)

        # Skip below threshold ONLY if not smart context
        if not is_context and final_score < SIM_THRESHOLD:
            return

        doc_name = r.get("doc_name", "Unknown")
        text = r.get("text", "") or ""
        page = r.get("loc_start", "?")
        public_path = r.get("public_path", f"/assets/data/{doc_name}")

        doc_group = _ensure_doc(doc_name, final_score)

        # Update max score only from non-context hits
        if (not is_context) and final_score > float(doc_group["max_score"]):
            doc_group["max_score"] = final_score

        # Dedupe same (page + text hash prefix)
        sig = (str(page), text[:200])
        if sig in doc_group["_seen"]:
            return
        doc_group["_seen"].add(sig)

        hit_obj = {
            "text": text,
            "score": final_score,
            "page_start": page,
            "page_end": r.get("loc_end", page),
            "public_path": public_path,
            "doc_authority": int(r.get("doc_authority", 2) or 2),
            "search_text": r.get("search_text", ""),
            "_is_smart_context": is_context
        }

        # ── API metadata propagation (preserved from current project) ──
        # Pass through API ingestion metadata so the answerer can format
        # API-sourced evidence with the correct citation shape.
        source_type = r.get("source_type", "")
        if source_type:
            hit_obj["source_type"] = source_type
        if r.get("api_source_id"):
            hit_obj["api_source_id"] = r["api_source_id"]
        if r.get("record_id"):
            hit_obj["record_id"] = r["record_id"]
        if r.get("record_type"):
            hit_obj["record_type"] = r["record_type"]
        if r.get("api_tags"):
            hit_obj["api_tags"] = r["api_tags"]

        doc_group["hits"].append(hit_obj)

    # A) FAISS hits (IDs)
    for score, cid in zip(D[0], I[0]):
        try:
            cii = int(cid)
        except Exception:
            continue
        if cii < 0:
            continue
        _process_hit(cii, float(score), is_context=False)

    # B) Expanded neighbor IDs (context)
    for cid in expanded_ids:
        _process_hit(int(cid), SIM_THRESHOLD, is_context=True)

    # C) Keyword hits (IDs)
    for cid, sc in final_keyword_list:
        _process_hit(int(cid), float(sc), is_context=False)

    evidence = list(docs_map.values())
    # Remove internal dedupe tracker
    for d in evidence:
        if "_seen" in d:
            del d["_seen"]

    # ─── RERANK all hits within each doc group ───────────────────
    all_flat_hits = []
    for d in evidence:
        for h in d.get("hits", []):
            h["doc_name"] = d["doc_name"]
            h["doc_rank"] = int(d.get("doc_rank", 0) or 0)
            all_flat_hits.append(h)

    if all_flat_hits:
        log.info("Pre-rerank: %d total hits across %d docs", len(all_flat_hits), len(evidence))
        reranked = rerank_hits(question, all_flat_hits)

        # Phase 3: cross-document content deduplication.
        #
        # The previous pipeline only deduplicated inside a single document
        # (_seen set per doc_group). Identical or near-identical content
        # copied across multiple PDFs (e.g., CTO duties in both Annex-H
        # and the HR Manual) therefore consumed multiple slots in the
        # evidence budget, caused the LLM to blend or pick arbitrarily,
        # and degraded answer quality.
        #
        # We now drop cross-doc duplicates at rebuild time. Keys are
        # content fingerprints over the normalized first 500 characters
        # (SHA-256, truncated). Since `reranked` is already sorted
        # by the authority-aware blend score, the first occurrence of
        # any fingerprint is the highest-authority / highest-scoring
        # version — exactly what we want to keep.
        seen_fingerprints: Dict[str, Dict[str, Any]] = {}
        deduped: List[Dict[str, Any]] = []
        dedup_dropped = 0
        for h in reranked:
            fp = _content_fingerprint(h.get("text") or "")
            if not fp:
                deduped.append(h)
                continue
            if fp in seen_fingerprints:
                kept = seen_fingerprints[fp]
                log.debug(
                    "Cross-doc dedup: dropping duplicate from doc='%s' p.%s "
                    "(kept: doc='%s' p.%s, blend=%.3f)",
                    h.get("doc_name", "?"), h.get("page_start", "?"),
                    kept.get("doc_name", "?"), kept.get("page_start", "?"),
                    float(kept.get("_blend", kept.get("score", 0.0))),
                )
                dedup_dropped += 1
                continue
            seen_fingerprints[fp] = h
            deduped.append(h)

        if dedup_dropped:
            log.info(
                "Cross-doc dedup: %d duplicates removed, %d unique hits retained",
                dedup_dropped, len(deduped)
            )

        # Rebuild doc groups from deduped, reranked order, respecting per-doc caps
        new_docs_map: Dict[str, Dict[str, Any]] = {}
        per_doc_count: Dict[str, int] = {}
        for h in deduped:
            dn = h.get("doc_name", "Unknown")
            if dn not in new_docs_map:
                new_docs_map[dn] = {
                    "doc_name": dn,
                    "max_score": 0.0,
                    "doc_rank": h.get("doc_rank", 0),
                    "hits": [],
                }
                per_doc_count[dn] = 0

            if per_doc_count[dn] >= MAX_HITS_PER_DOC_RETRIEVAL:
                # Smart context (expansion) chunks bypass the cap
                # They contain critical cross-page data like salary/appointment
                if not h.get("_is_smart_context", False):
                    continue

            # Add evidence_id for traceability
            h["evidence_id"] = _evidence_id(h.get("text", ""))

            new_docs_map[dn]["hits"].append(h)
            per_doc_count[dn] += 1

            hit_score = float(h.get("_blend", h.get("score", 0.0)))
            if hit_score > float(new_docs_map[dn]["max_score"]):
                new_docs_map[dn]["max_score"] = hit_score

        evidence = list(new_docs_map.values())
        total_after = sum(len(d["hits"]) for d in evidence)
        log.info("Post-rerank + dedup: %d hits across %d docs (per-doc cap=%d)",
                 total_after, len(evidence), MAX_HITS_PER_DOC_RETRIEVAL)

    evidence.sort(key=lambda x: float(x.get("max_score", 0)), reverse=True)

    return {
        "question": question,
        "has_evidence": len(evidence) > 0,
        "evidence": evidence
    }


# =============================================================================
# Phase 5 — Multi-query retrieval wrapper
# =============================================================================
def retrieve_multi(
    queries: List[str],
    index_dir: Optional[str] = None,
    rrf_k: Optional[int] = None,
) -> Dict[str, Any]:
    """Run `retrieve()` for each query in `queries`, then fuse the
    per-query evidence with Reciprocal Rank Fusion (RRF) at the
    chunk level. Returns the same shape as `retrieve()`.

    This is the entry point used by the request flow when the intent
    layer produces multiple sub-queries (multi-part question or
    vague-query expansion). Single-query callers should keep using
    `retrieve()` for zero-cost backward compatibility.

    Behavior guarantees:
      • If `queries` has only one entry, this is a thin pass-through to
        `retrieve()` so latency and behavior are unchanged.
      • Phase 3 protections (per-doc cap, cross-doc dedup, page sort)
        run independently inside each `retrieve()` call. The fusion
        stage applies a SECOND content-fingerprint dedup across the
        merged set, so duplicates introduced by overlapping sub-queries
        are removed.
      • Page-expansion / section-companion hits (Phase 3) are preserved
        — they ride through the per-query call.
      • Output contract is identical: question, has_evidence, evidence.
        The `question` field is set to the first non-empty input query
        (the original) so downstream logging stays meaningful.
    """
    cleaned = [q for q in (queries or []) if q and q.strip()]
    if not cleaned:
        return {"question": "", "has_evidence": False, "evidence": []}

    if len(cleaned) == 1:
        return retrieve(cleaned[0], index_dir=index_dir)

    k = int(rrf_k if rrf_k is not None else int(os.getenv("RETRIEVER_RRF_K", "60")))

    # Run sub-queries; collect all hits, key them by evidence_id,
    # accumulate RRF, remember best blend score.
    hit_by_eid: Dict[str, Dict[str, Any]] = {}
    rrf_by_eid: Dict[str, float] = {}
    primary_question = cleaned[0]

    for qi, q in enumerate(cleaned):
        try:
            sub = retrieve(q, index_dir=index_dir)
        except Exception as e:
            log.warning("retrieve_multi: sub-query #%d failed (%s)", qi, e)
            continue
        if not sub.get("has_evidence"):
            continue

        # Flatten doc_groups → hits, with intra-result rank position.
        ranked: List[Dict[str, Any]] = []
        for dg in sub.get("evidence", []):
            for h in dg.get("hits", []):
                ranked.append(h)
        # Stable sort by blend desc to assign a rank for RRF
        ranked.sort(
            key=lambda h: float(h.get("_blend", h.get("score", 0.0))),
            reverse=True,
        )

        for rank, h in enumerate(ranked):
            eid = h.get("evidence_id") or _evidence_id(h.get("text", ""))
            if not eid:
                continue
            rrf_by_eid[eid] = rrf_by_eid.get(eid, 0.0) + 1.0 / (k + rank + 1)
            # Keep the highest-blend instance for downstream consumption.
            cur = hit_by_eid.get(eid)
            if (cur is None) or (
                float(h.get("_blend", h.get("score", 0.0)))
                > float(cur.get("_blend", cur.get("score", 0.0)))
            ):
                # Stamp evidence_id on the kept hit so downstream code
                # can rely on it.
                h["evidence_id"] = eid
                hit_by_eid[eid] = h

    if not hit_by_eid:
        log.info("retrieve_multi: %d sub-queries returned no evidence", len(cleaned))
        return {"question": primary_question, "has_evidence": False, "evidence": []}

    # Cross-sub-query content-fingerprint dedup (defense in depth — the
    # per-call dedup already runs, but two sub-queries can surface the
    # same content from different paths).
    seen_fp: Dict[str, str] = {}  # fingerprint -> kept eid
    survivors: List[str] = []
    for eid in sorted(rrf_by_eid.keys(), key=lambda e: rrf_by_eid[e], reverse=True):
        h = hit_by_eid.get(eid)
        if not h:
            continue
        fp = _content_fingerprint(h.get("text") or "")
        if fp and fp in seen_fp:
            log.debug(
                "retrieve_multi cross-query dedup: drop eid=%s (kept=%s)",
                eid, seen_fp[fp],
            )
            continue
        if fp:
            seen_fp[fp] = eid
        survivors.append(eid)

    # Order by RRF score and re-group by document, preserving doc-order
    # (first-doc-seen-first) and page-ascending within each doc — same
    # convention as the answerer expects.
    docs_map: Dict[str, Dict[str, Any]] = {}
    for eid in survivors:
        h = hit_by_eid[eid]
        dn = h.get("doc_name", "Unknown")
        if dn not in docs_map:
            docs_map[dn] = {
                "doc_name": dn,
                "max_score": 0.0,
                "doc_rank": int(h.get("doc_rank", 0) or 0),
                "hits": [],
            }
        docs_map[dn]["hits"].append(h)
        s = float(h.get("_blend", h.get("score", 0.0)))
        if s > float(docs_map[dn]["max_score"]):
            docs_map[dn]["max_score"] = s

    evidence = list(docs_map.values())
    evidence.sort(key=lambda d: float(d.get("max_score", 0.0)), reverse=True)

    total_hits = sum(len(d["hits"]) for d in evidence)
    log.info(
        "retrieve_multi: fused %d sub-queries -> %d unique chunks across %d docs",
        len(cleaned), total_hits, len(evidence),
    )

    return {
        "question": primary_question,
        "has_evidence": total_hits > 0,
        "evidence": evidence,
    }


# =============================================================================
# Query Contextualizer (Memory)
# =============================================================================
def rewrite_contextual_query(current_query: str, last_question: str, last_answer: str) -> str:
    """
    Rewrite follow-up questions to be standalone using LLM.
    IMPORTANT: deterministic abbreviation expansions are PROTECTED
    from LLM override. The rewriter is told which expansions are confirmed.
    IMPORTANT: Queries that already contain explicit role/position names
    are NOT rewritten — they are self-contained and rewriting them with
    session context can contaminate the search query.

    Fix (Bug-diagnosis): the rewriter now ALSO runs for standalone (no
    last_question) queries when they look like Urdu / Roman-Urdu. An
    English-only corpus cannot be searched effectively with Roman-Urdu
    tokens ('agr', 'kr raha', 'hoto', 'kya', 'ly sakti'); those queries
    produce near-zero embedding similarity and trip Phase-2 refusal.
    Translating the query to English before retrieval dramatically
    improves recall on mixed-language traffic.
    """
    should_rewrite = os.getenv("RETRIEVER_LLM_QUERY_REWRITE_ALWAYS", "0") != "0"

    # Detect Roman-Urdu / Urdu-script queries so we translate them even
    # when there is no prior conversation context. Keyword-based — cheap,
    # deterministic, no LLM call needed just to decide.
    _ROMAN_URDU_MARKERS = {
        "agr", "agar", "kr", "kar", "kia", "kya", "hoto", "hota",
        "raha", "rahi", "rahay", "rahe", "ly", "lain", "sakti", "sakta",
        "btao", "batao", "bataen", "bataein", "bataye",
        "ka", "ki", "ke", "ko", "kya", "kaun", "kon", "kaunsa", "konsi",
        "mein", "main", "par", "pe", "se", "ye", "yeh", "woh", "us", "is",
        "nahin", "nahi", "nhi", "na", "haan", "han", "jee",
        "kitni", "kitna", "kitne", "kaise", "kaisi",
        "against", "banaya",
    }
    q_words = {w.lower().strip("?.,!") for w in (current_query or "").split()}
    has_urdu_script = any("\u0600" <= ch <= "\u06ff" for ch in (current_query or ""))
    has_roman_urdu = len(q_words & _ROMAN_URDU_MARKERS) >= 2
    needs_translation = has_urdu_script or has_roman_urdu

    if not last_question and not should_rewrite and not needs_translation:
        return current_query

    if len(current_query) < 4 and current_query.lower() in ["ok", "thanks", "theek", "sahi"]:
        return current_query

    # GUARD: If the query already contains an explicit role/position,
    # don't rewrite — the user knows what they're asking about.
    # Only use LLM rewrite for pronoun-based follow-ups or translation.
    _explicit_role_re = _re.compile(
        r"\b(manager|officer|director|assistant|deputy|chief|head|coordinator|superintendent"
        r"|inspector|registrar|analyst)\s+\w+",
        _re.IGNORECASE
    )
    if _explicit_role_re.search(current_query) and not needs_translation:
        # Query has explicit role AND is in English — just expand
        # abbreviations, skip LLM rewrite.
        expanded = _expand_abbreviations(current_query)
        if expanded != current_query:
            log.info("Skip LLM rewrite (explicit role in query), abbreviation only: '%s' -> '%s'",
                     current_query[:60], expanded[:60])
        return expanded

    # Pre-expand abbreviations BEFORE rewrite
    pre_expanded = _expand_abbreviations(current_query)
    expansions_done = []
    for w in current_query.split():
        key = _norm_key(w)
        if key in _ABBREV_MAP:
            expansions_done.append(f"{w} = {_ABBREV_MAP[key]}")

    # Build protection instruction
    protection = ""
    if expansions_done:
        protection = (
            "\nCRITICAL: The following abbreviations have been definitively resolved "
            "from PERA domain knowledge. Do NOT change, override, or reinterpret them:\n"
            + "\n".join(f"  - {e}" for e in expansions_done)
            + "\nUse these exact expansions in your rewritten query."
        )

    system_prompt = (
        "You are a query rewriter for a RAG system about PERA (Punjab Enforcement and Regulatory Authority).\n"
        "The underlying document corpus is in ENGLISH. The user's query may be in Urdu, Roman-Urdu, or English.\n"
        "Your task: produce a standalone, semantically-rich ENGLISH search query that will retrieve the most relevant PERA document content.\n"
        "Rules:\n"
        "1. TRANSLATE Urdu and Roman-Urdu queries to natural English first. Preserve the user's intent and direction of action.\n"
        "   Examples:\n"
        "     'agr koi illegal petrol sell kr raha hoto pera kya action ly sakti us k against?'\n"
        "        -> 'What enforcement actions can PERA take against someone illegally selling petrol?'\n"
        "     'manager development ki salary kitni hai?'\n"
        "        -> 'What is the salary of Manager (Development)?'\n"
        "     'complaint kaise file karein?'\n"
        "        -> 'How to file a complaint at PERA'\n"
        "2. Map broad informal terms to the domain terminology PERA documents actually use. For enforcement/illegal-activity questions specifically: PERA uses the vocabulary of 'Enforcement Officer', 'inspection', 'notice', 'Removal Order', 'confiscation', 'sealing of premises', 'enforcement costs', 'public nuisance', 'encroachment', 'FIR' — prefer those exact terms.\n"
        "3. Do NOT invent or guess abbreviation expansions. Only expand abbreviations you are certain about.\n"
        "4. If an abbreviation is ambiguous, keep the original abbreviation.\n"
        "5. Map broad English terms to document phrasing (e.g. 'powers' -> 'powers, functions, responsibilities').\n"
        "6. Resolve pronouns using History if available.\n"
        "7. OUTPUT ONLY THE REWRITTEN ENGLISH QUERY. No preamble. No explanation."
        + protection
    )

    user_prompt = (
        f"History: {last_question or 'None'}\n"
        f"Answer Context: {(last_answer or '')[:200]}...\n"
        f"Current Follow-up: {pre_expanded}\n"
        "Rewritten Query:"
    )

    try:
        client = get_chat_client()
        response = client.chat.completions.create(
            model=LLM_REWRITE_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.0
        )
        rewritten = response.choices[0].message.content.strip()

        # Post-rewrite protection: verify known expansions weren't corrupted
        rewritten_lower = rewritten.lower()
        for w in current_query.split():
            key = _norm_key(w)
            if key in _ABBREV_MAP:
                expected = _ABBREV_MAP[key].lower()
                if expected not in rewritten_lower:
                    log.warning(
                        "Rewriter corrupted abbreviation %s (expected '%s', not found in '%s'). Reverting.",
                        w, _ABBREV_MAP[key], rewritten[:100]
                    )
                    # Replace LLM's wrong expansion with correct one
                    rewritten = re.sub(
                        rf"\b{re.escape(w)}\b",
                        _ABBREV_MAP[key],
                        rewritten,
                        flags=re.IGNORECASE,
                    )

        # Log rewrite diff for auditability
        if rewritten != current_query:
            log.info("Query rewrite: '%s' -> '%s'", current_query, rewritten)

        return rewritten
    except Exception as e:
        log.warning("Query rewrite failed (falling back to original): %s", e)
        return current_query
