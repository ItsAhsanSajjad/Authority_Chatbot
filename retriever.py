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

# Abbreviation -> full expansion (from unified pera_vocab)
from pera_vocab import (
    get_full_abbreviation_map as _get_full_abbrev_map,
    get_normalized_abbreviation_map as _get_norm_abbrev_map,
    EXPANSION_KEYWORDS as _EXPANSION_KW_SET,
)

def _norm_key(s: str) -> str:
    # Keep alphanumerics; makes "Schedule-I" -> "schedulei"
    return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())

# Normalized abbrev map for embedding search quality
_ABBREV_MAP = _get_norm_abbrev_map()

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


def _expand_abbreviations(query: str) -> str:
    """Expand known abbreviations in-place for better embedding matches."""
    # First normalize schedule references
    query = _normalize_schedule_references(query)

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



# =============================================================================
# Main Retrieval Logic
# =============================================================================
def _evidence_id(text: str) -> str:
    """Short hash for evidence traceability."""
    return hashlib.sha256((text or "")[:500].encode()).hexdigest()[:10]


def retrieve(question: str, index_dir: Optional[str] = None, source_type_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Semantic search + reranking + smart expansion + keyword fallback.
    Uses in-memory cached index (automatic blue/green invalidation).

    Args:
        question: User query.
        index_dir: Override index directory (optional).
        source_type_filter: Optional filter — "api" or "document"/"pdf"/"docx".
            When set, only hits with matching source_type are returned.
            Default None returns all hits (mixed).
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

    # 2) Search FAISS with ALL queries, merge results (keep highest score per chunk)
    try:
        log.debug("FAISS search with TOP_K=%d each", TOP_K)
        D1, I1 = idx.search(qv_primary.reshape(1, -1), TOP_K)
        D2, I2 = idx.search(qv_core.reshape(1, -1), TOP_K)

        # Merge: keep highest similarity score per chunk ID
        merged_scores: Dict[int, float] = {}
        for score, cid in zip(D1[0], I1[0]):
            try:
                ci = int(cid)
            except Exception:
                continue
            if ci >= 0 and ci in id_map:
                merged_scores[ci] = max(merged_scores.get(ci, 0.0), float(score))

        for score, cid in zip(D2[0], I2[0]):
            try:
                ci = int(cid)
            except Exception:
                continue
            if ci >= 0 and ci in id_map:
                merged_scores[ci] = max(merged_scores.get(ci, 0.0), float(score))

        # Third search: parenthesized variant (if exists)
        if qv_variant is not None:
            D3, I3 = idx.search(qv_variant.reshape(1, -1), TOP_K)
            for score, cid in zip(D3[0], I3[0]):
                try:
                    ci = int(cid)
                except Exception:
                    continue
                if ci >= 0 and ci in id_map:
                    merged_scores[ci] = max(merged_scores.get(ci, 0.0), float(score))

        # Sort by score descending, take top TOP_K
        sorted_merged = sorted(merged_scores.items(), key=lambda x: x[1], reverse=True)[:TOP_K]

        # Reconstruct D and I arrays for compatibility with downstream code
        D_merged = np.array([[s for _, s in sorted_merged]], dtype=np.float32)
        I_merged = np.array([[cid for cid, _ in sorted_merged]])

        # Replace originals
        D = D_merged
        I = I_merged

        log.info("Dual-query merged: %d unique chunks (primary=%d, core=%d)",
                 len(sorted_merged), len([x for x in I1[0] if int(x) >= 0]),
                 len([x for x in I2[0] if int(x) >= 0]))
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

    # --- Smart Page Expansion Logic (ID-based) ---
    should_expand = any(k in (question or "").lower() for k in _EXPANSION_KEYWORDS)
    expanded_ids = set()

    if should_expand and base_ids:
        log.debug("Smart Expansion Triggered (Salary/Detail context)")
        page_map = _get_page_map_by_id(id_map)

        # For top 10 FAISS hits, fetch neighbor pages ±RADIUS
        for rank, (score, cid) in enumerate(zip(D[0], base_ids)):
            if rank >= 10:
                break

            r = id_map.get(cid)
            if not r:
                continue

            doc = r.get("doc_name")
            page = r.get("loc_start")

            # Skip page expansion for API chunks (no page concept)
            if r.get("source_type") == "api":
                continue

            if isinstance(page, int) and doc:
                for offset in range(-_EXPANSION_RADIUS, _EXPANSION_RADIUS + 1):
                    if offset == 0:
                        continue
                    p = page + offset
                    for neighbor_id in page_map.get((doc, p), []):
                        if neighbor_id not in base_id_set:
                            expanded_ids.add(neighbor_id)

    log.debug("Added %d context chunks.", len(expanded_ids))

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
            "_is_smart_context": is_context,
        }

        # ── API metadata propagation (Phase 4) ──
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

    # B) Expanded neighbor IDs (context) — with relevance check
    # Only include expanded chunks that share at least 2 query keywords,
    # preventing irrelevant adjacent-page content from flooding evidence.
    _exp_q_words = set(w.lower() for w in _re.sub(r"[^\w\s]", " ", question).split()
                       if len(w) > 2)
    _exp_accepted = 0
    for cid in expanded_ids:
        r = id_map.get(int(cid))
        if r:
            chunk_text = (r.get("text") or "").lower()
            overlap = sum(1 for w in _exp_q_words if w in chunk_text)
            if overlap >= 2:
                _process_hit(int(cid), SIM_THRESHOLD, is_context=True)
                _exp_accepted += 1
    if _exp_accepted < len(expanded_ids):
        log.debug("Expansion filter: accepted %d/%d context chunks",
                  _exp_accepted, len(expanded_ids))

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

        # Rebuild doc groups from reranked order, respecting per-doc caps
        new_docs_map: Dict[str, Dict[str, Any]] = {}
        per_doc_count: Dict[str, int] = {}
        for h in reranked:
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
        log.info("Post-rerank: %d hits across %d docs (per-doc cap=%d)",
                 total_after, len(evidence), MAX_HITS_PER_DOC_RETRIEVAL)

    evidence.sort(key=lambda x: float(x.get("max_score", 0)), reverse=True)

    # ── Optional source_type filter (Phase 5) ──────────────────
    if source_type_filter:
        _filter_val = source_type_filter.lower()
        # Normalise: "document" matches pdf/docx/etc (anything non-api)
        if _filter_val in ("document", "doc", "pdf", "docx"):
            _is_match = lambda h: h.get("source_type", "pdf") != "api"
        elif _filter_val == "api":
            _is_match = lambda h: h.get("source_type") == "api"
        else:
            _is_match = lambda h: True

        for doc in evidence:
            doc["hits"] = [h for h in doc.get("hits", []) if _is_match(h)]
        evidence = [d for d in evidence if d.get("hits")]
        log.debug("Source-type filter '%s': %d doc groups remain", _filter_val, len(evidence))

    return {
        "question": question,
        "has_evidence": len(evidence) > 0,
        "evidence": evidence
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
    """
    should_rewrite = os.getenv("RETRIEVER_LLM_QUERY_REWRITE_ALWAYS", "0") != "0"

    if not last_question and not should_rewrite:
        return current_query

    if len(current_query) < 4 and current_query.lower() in ["ok", "thanks", "theek", "sahi"]:
        return current_query

    # GUARD: If the query already contains an explicit role/position,
    # don't rewrite — the user knows what they're asking about.
    # Only use LLM rewrite for pronoun-based follow-ups.
    _explicit_role_re = _re.compile(
        r"\b(manager|officer|director|assistant|deputy|chief|head|coordinator|superintendent"
        r"|inspector|registrar|analyst)\s+\w+",
        _re.IGNORECASE
    )
    # Don't skip rewrite if the query also has follow-up pronouns (Roman Urdu/English)
    _followup_pronoun_re = _re.compile(
        r"\b(wo|woh|ye|yeh|us|tha|thi|the|usk[aeiou]|unk[aeiou]|"
        r"that|those|it|them|kis\b)",
        _re.IGNORECASE
    )
    if _explicit_role_re.search(current_query) and not _followup_pronoun_re.search(current_query):
        # Query has explicit role and no follow-up pronouns — just expand abbreviations, skip LLM rewrite
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
        "Your task: Rewrite the user query to be a standalone, semantically rich search query.\n"
        "Rules:\n"
        "1. Do NOT invent or guess abbreviation expansions. Only expand abbreviations you are certain about.\n"
        "2. If an abbreviation is ambiguous, keep the original abbreviation.\n"
        "3. Map broad terms to specific document phrasing (e.g. 'powers' -> 'powers, functions, responsibilities').\n"
        "4. Urdu/Hindi: Preserve direction of action and correct subject/object.\n"
        "5. Resolve pronouns using History if available.\n"
        "6. Keep final query in English for best match with document corpus.\n"
        "7. OUTPUT ONLY THE REWRITTEN QUERY."
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
