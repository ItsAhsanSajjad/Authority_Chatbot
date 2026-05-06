"""
PERA AI Answerer (v3: clean support-state model)

4 answer states: supported, partially_supported, unsupported, conflicting
No more contradictory "not explicitly defined" + correct answer.
"""
from __future__ import annotations

import os
import re
import time
from typing import List, Dict, Any, Optional

from openai_clients import (
    get_chat_client,
    ANSWER_MODEL,
    get_openai_status,
    mark_openai_available,
    mark_openai_unavailable,
)
from grounding import verify_grounding, GroundingResult
from log_config import get_logger

log = get_logger("pera.answerer")


def _system_offline_response() -> Dict[str, Any]:
    """Response payload returned when the OpenAI service is known to be
    unavailable (quota exhausted). Produces a clear user-facing message
    with a distinct ``decision`` so the frontend can render an offline
    banner instead of a generic refusal."""
    return {
        "answer": (
            "⚠️ The system is currently offline. Please try again later "
            "when the system is active."
        ),
        "references": [],
        "decision": "system_offline",
        "support_state": "system_offline",
        "system_status": "offline",
    }


# Evidence quality thresholds
ANSWER_MIN_TOP_SCORE = float(os.getenv("ANSWER_MIN_TOP_SCORE", "0.15"))
HIT_MIN_SCORE = float(os.getenv("HIT_MIN_SCORE", "0.12"))
MAX_HITS_PER_DOC = int(os.getenv("MAX_HITS_PER_DOC_FOR_PROMPT", "6"))
MAX_DOCS = int(os.getenv("MAX_DOCS_FOR_PROMPT", "5"))
MAX_EVIDENCE_CHARS = int(os.getenv("MAX_EVIDENCE_CHARS", "18000"))


# =============================================================================
# Per-PDF page-text cache (for exact page lookup in references)
# =============================================================================
# Chunks stored in the index span many PDF pages (loc_start..loc_end) with no
# per-page markers, so a chunk-internal Position Title match can only be
# approximated. For citation accuracy we instead look up the exact page number
# by searching the source PDF. This cache holds each PDF's page texts in
# memory once loaded.
_PDF_PAGE_TEXT_CACHE: Dict[str, List[str]] = {}


def _normalize_for_match(s: str) -> str:
    """Collapse all whitespace to single spaces and lowercase — used when
    matching chunk-text fragments against PDF page extracts. pdfplumber
    inserts \\n between lines where the chunk had a single space, so a raw
    substring search fails; normalization resolves that."""
    return re.sub(r"\s+", " ", (s or "").lower()).strip()


def prewarm_pdf_cache(data_dir: str = "assets/data") -> None:
    """Load every PDF's page text into the in-memory cache in parallel
    background workers.

    The first user query after a server restart otherwise triggers
    pdfplumber.open() synchronously inside the request handler — for
    very large PDFs (400+ pages) that takes 20+ seconds and makes the
    request look frozen. Pre-warming at startup moves that cost off
    the critical path; parallel workers roughly halve the cold-start
    window so typical queries land on a warm cache.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor

    def _worker() -> None:
        try:
            if not os.path.isdir(data_dir):
                return
            pdfs = sorted(
                os.path.join(data_dir, f)
                for f in os.listdir(data_dir)
                if f.lower().endswith(".pdf")
            )
            log.info(
                "PDF cache prewarm: %d PDFs queued (dir=%s, parallel=4)",
                len(pdfs), data_dir,
            )
            started = time.time()

            def _load_one(p: str) -> None:
                if _cache_key(p) in _PDF_PAGE_TEXT_CACHE:
                    return
                try:
                    _load_pdf_pages(p)
                except Exception as exc:
                    log.warning("PDF prewarm failed for %s: %s", p, exc)

            # pdfplumber is mostly I/O bound with short bursts of CPU
            # parsing — 4 parallel workers reliably cuts total time
            # without overloading disk/CPU on modest hardware.
            with ThreadPoolExecutor(max_workers=4, thread_name_prefix="pdf-warm") as ex:
                list(ex.map(_load_one, pdfs))

            log.info(
                "PDF cache prewarm complete in %.1fs (%d cached)",
                time.time() - started, len(_PDF_PAGE_TEXT_CACHE),
            )
        except Exception as exc:
            log.warning("PDF prewarm worker error: %s", exc)

    threading.Thread(target=_worker, name="pdf-prewarm", daemon=True).start()


def _cache_key(pdf_path: str) -> str:
    """Canonical cache key for a PDF path. Uses the absolute, normalized,
    lowercased path so that relative vs absolute and slash-direction
    differences don't cause cache misses (which would force a multi-second
    pdfplumber re-read per miss)."""
    try:
        return os.path.normcase(os.path.abspath(pdf_path or ""))
    except Exception:
        return pdf_path or ""


def _load_pdf_pages(pdf_path: str, cache_only: bool = False) -> List[str]:
    """Load and cache per-page text for a PDF, already whitespace-normalized
    and lowercased so substring matching against chunk fragments works.
    Returns list indexed by 0-based page number. Returns [] if the file
    cannot be opened or a PDF library is unavailable.

    Cache is keyed by the canonical absolute path so that callers passing
    relative paths share the same entry as the startup pre-warm (which
    scans with absolute paths).

    When *cache_only* is True, return [] immediately on a cache miss
    instead of loading the PDF synchronously. Used by request-path
    callers so they never block 20+ seconds on a cold PDF parse —
    reference page refinement falls back to its linear estimate, and
    the background pre-warm catches up for subsequent requests.
    """
    key = _cache_key(pdf_path)
    if key in _PDF_PAGE_TEXT_CACHE:
        return _PDF_PAGE_TEXT_CACHE[key]
    if cache_only:
        return []
    pages: List[str] = []
    try:
        import pdfplumber  # type: ignore
        with pdfplumber.open(pdf_path) as pdf:
            for p in pdf.pages:
                try:
                    pages.append(_normalize_for_match(p.extract_text() or ""))
                except Exception:
                    pages.append("")
    except Exception as e:
        log.warning("PDF page-text cache load failed for %s: %s",
                    (pdf_path or "?")[:60], e)
        pages = []
    _PDF_PAGE_TEXT_CACHE[key] = pages
    log.info("PDF page-text cache loaded: %s pages=%d",
             (pdf_path or "?")[:60], len(pages))
    return pages


def _pick_distinctive_fragment(text: str, max_len: int = 60) -> str:
    """Pick a distinctive fragment from chunk text to use as a PDF search
    needle.

    Priority:
      1. Position Title / [Role: ...] header — these are highly unique
         per page within the JD annexes (e.g. "sergeant (strike) reports
         to sdeo").
      2. Any capitalized proper-noun-looking phrase from the raw text
         (kept lowercased for matching).
      3. Longest alphabetic word-run near the start.

    Returns a whitespace-normalized, lowercased fragment shortened to
    *max_len* chars.
    """
    if not text:
        return ""
    # --- 1) Position Title / Role header -----------------------------
    m = re.search(
        r"(?:Position\s+Title\s*:\s*-?\s*|\[Role:\s*)([^\n\]]{5,120})",
        text,
        re.IGNORECASE,
    )
    if m:
        # Include the LEADING "Position Title:" prefix itself so we match
        # the actual JD page rather than an index/table-of-contents page
        # that merely lists the role name. "[Role:" is a synthetic marker
        # inserted by the chunker — never include it in the PDF search
        # needle because it doesn't appear in the source PDF text.
        after = text[m.end():m.end() + 120]
        head = m.group(1).strip()
        matched_prefix_raw = m.group(0)[: m.start(1) - m.start()].strip()
        if matched_prefix_raw.startswith("["):
            matched_prefix = "Position Title"
        else:
            # Drop any trailing "-" dash the Position Title form carries.
            matched_prefix = matched_prefix_raw.rstrip(":-").strip()
            if not matched_prefix:
                matched_prefix = "Position Title"
        combined = _normalize_for_match(
            matched_prefix + ": " + head + " " + after
        )
        if combined:
            return combined[:max_len]
    # --- 2) Proper-noun-style capitalized phrase ---------------------
    cap = re.search(
        r"\b([A-Z][A-Za-z&\-]{2,}(?:\s+(?:of\s+|the\s+|and\s+|&\s+)?[A-Z][A-Za-z&\-]{2,}){2,6})",
        text,
    )
    if cap:
        candidate = _normalize_for_match(cap.group(1))
        if len(candidate) >= 20:
            return candidate[:max_len]
    # --- 3) Longest alphabetic word-run ------------------------------
    t = _normalize_for_match(text)
    if not t:
        return ""
    window = t[:1500]
    best = ""
    for run in re.findall(r"[a-z]+(?:\s+[a-z]+){3,}", window):
        if len(run) >= 25 and len(run) > len(best):
            best = run
        if len(best) >= max_len:
            break
    if not best:
        best = window[:max_len].strip()
    return best[:max_len]


def _find_pdf_path_for_hit(hit: Dict[str, Any]) -> str:
    """Resolve a filesystem-readable path for the hit's source PDF from
    the candidate path fields. Returns empty string if none exist."""
    for cand in (hit.get("public_path", ""), hit.get("path", "")):
        if not cand:
            continue
        cand_norm = cand.lstrip("/\\")
        if os.path.exists(cand_norm):
            return cand_norm
        if os.path.exists(cand):
            return cand
    # Also try assets/data/<doc_name>
    doc = hit.get("doc_name") or hit.get("_doc_name") or ""
    if doc:
        cand = os.path.join("assets", "data", doc)
        if os.path.exists(cand):
            return cand
    return ""


def _resolve_exact_page(
    pdf_path: str,
    needle: str,
    fallback_page: int,
    min_page: int,
    max_page: int,
) -> int:
    """Find the 1-based PDF page number that contains *needle*. Searches
    only within [min_page, max_page] (inclusive) to stay within the chunk's
    declared page range. Returns fallback_page if not found or unavailable.

    Both the cached page texts and the needle are whitespace-normalized so
    line-break differences between pdfplumber and the indexed chunk text
    do not prevent matching.
    """
    if not pdf_path or not needle:
        return fallback_page
    # Request-path callers must not block on pdfplumber.open — if this
    # PDF isn't cached yet, fall back to the linear estimate so the
    # user doesn't wait 20+ seconds for the first query after restart.
    pages = _load_pdf_pages(pdf_path, cache_only=True)
    if not pages:
        return fallback_page
    needle_low = _normalize_for_match(needle)
    if not needle_low:
        return fallback_page
    # Keep a reasonably unique prefix so we don't match trivial fragments.
    if len(needle_low) > 80:
        needle_low = needle_low[:80]
    lo = max(1, min_page)
    hi = min(len(pages), max_page)
    for pg in range(lo, hi + 1):
        if needle_low in pages[pg - 1]:
            return pg
    # Relaxed search: if not found in range, scan whole doc as last resort.
    for pg in range(1, len(pages) + 1):
        if lo <= pg <= hi:
            continue
        if needle_low in pages[pg - 1]:
            return pg
    return fallback_page


def _resolve_best_page(
    pdf_path: str,
    needles: List[str],
    fallback_page: int,
    min_page: int,
    max_page: int,
) -> tuple:
    """Score every page in [min_page, max_page] by how many needles it
    contains, weighted by needle length (longer = more specific).
    Returns (page, best_needle_for_highlight).

    This is stronger than _resolve_exact_page when needles may occur on
    many pages (e.g. "schedule-iii" appears on every JD) — the page
    that contains the RICHEST combination of query+answer needles is
    almost always the true answer page.
    """
    if not pdf_path or not needles:
        return fallback_page, ""
    # Request-path — honor cold-cache fallback instead of blocking.
    pages = _load_pdf_pages(pdf_path, cache_only=True)
    if not pages:
        return fallback_page, ""
    lo = max(1, min_page)
    hi = min(len(pages), max_page)
    # Normalize all needles once.
    norm_needles: List[tuple] = []
    for n in needles:
        if not n:
            continue
        nn = _normalize_for_match(n)
        if nn and len(nn) >= 4:
            norm_needles.append((nn, len(nn)))
    if not norm_needles:
        return fallback_page, ""
    best_page = fallback_page
    best_score = 0.0
    best_needle = ""
    for pg in range(lo, hi + 1):
        ptext = pages[pg - 1]
        pg_score = 0.0
        pg_needle_hit = ""
        for nn, nlen in norm_needles:
            if nn in ptext:
                # Weight by length (longer phrases are more specific).
                pg_score += max(1.0, nlen / 10.0)
                if not pg_needle_hit:
                    pg_needle_hit = nn
        if pg_score > best_score:
            best_score = pg_score
            best_page = pg
            best_needle = pg_needle_hit
    if best_score == 0:
        # No needle matched within range — widen to full doc.
        for pg in range(1, len(pages) + 1):
            if lo <= pg <= hi:
                continue
            ptext = pages[pg - 1]
            pg_score = 0.0
            pg_needle_hit = ""
            for nn, nlen in norm_needles:
                if nn in ptext:
                    pg_score += max(1.0, nlen / 10.0)
                    if not pg_needle_hit:
                        pg_needle_hit = nn
            if pg_score > best_score:
                best_score = pg_score
                best_page = pg
                best_needle = pg_needle_hit
    return best_page, best_needle


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
    # Organization name — users write "pera officer"/"pera manager" as a
    # generic "any officer at PERA" phrasing, NOT a specific role. Leaving
    # it in would make the role detector yield "pera officer" which then
    # hard-drops every reference chunk (no Position Title contains the
    # word "pera"), producing answers with zero citations.
    "pera",
    # Urdu / Roman Urdu stop words
    "ki", "ka", "ke", "ko", "kya", "kia", "koi", "kuch", "hai", "hain", "hy", "hen",
    "kr", "kar", "sakta", "sakti", "sakte", "sake",
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
    r"\b(?:ki|ka|ke|ko|kya|kia|koi|kuch|hai|hain|hy|hen|aur|ya|mein|se|par|pe|ye|yeh|woh|"
    r"nahi|nhi|na|ho|tha|thi|batao|btao|bataen|bataiye|batayein|btaen|"
    r"kitni|kitna|kitne|kahan|kaun|konsi|kaunsi|wala|wali|wale|"
    r"hota|hoti|hote|kaise|kaisa|kesi|kaisi|abhi|kab|jab|tab|"
    r"kr|kar|sakta|sakti|sakte|sake)\b",
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
    # Primary: "Position Title: - Manager (Development) Report To: -"
    m = _POSITION_TITLE_RE.search(text or "")
    if m:
        return m.group(1).strip().lower()
    # Secondary: some chunks (the Compiled Working Paper's re-annotated
    # corpus) carry only a bracket header like "[Role: Manager
    # (Development) Report To: - ...]". Without this branch the position
    # filter sees an empty title, skips the filter entirely, and lets
    # unrelated-role chunks through for the answerer — producing
    # confidently-wrong answers when the ranker put multiple roles in
    # the top-K.
    m2 = _REF_POSITION_TITLE_RE.search(text or "")
    if not m2:
        return ""
    title = (m2.group(1) or m2.group(2) or "").strip()
    # The "[Role:...]" capture can include "Report To:..." inline. Trim
    # at the first "Report To" so the role name alone is returned.
    title = re.split(r"\s*Report\s*To\s*:", title, maxsplit=1, flags=re.IGNORECASE)[0]
    return title.strip().lower()

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
    - PREFIX SPEC: "manager development salary" → "manager development"
    - SPEC ROLE: "system support officer salary" → "system support officer"
    - FULL TITLE: "Manager (Development)" → "manager development"
    """
    # First strip Urdu particles, then normalize English
    q = _normalize_query_for_role(question or "")
    q = re.sub(r"[''\u2019]s\b", "", q)
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
    _ABBREV = {
        "cto": "chief technology officer",
        "dg": "director general",
        "ddg": "deputy director general",
        "adg": "additional director general",
        "hr": "human resources",
        "it": "information technology",
        "eo": "enforcement officer",
        "io": "investigation officer",
        "sso": "system support officer",
        "sdeo": "sub divisional enforcement officer",
        "deo": "data entry operator",
        "mgr": "manager",
        "dba": "database administrator",
        "se": "software engineer",
    }

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

    # Flatten all hits across doc groups for unified position-aware scoring
    all_hits = []
    for doc_group in evidence_list:
        doc_name = (doc_group.get("doc_name", "Unknown Document") or "Unknown Document").strip()
        for hit in doc_group.get("hits", []):
            hit["_doc_name"] = doc_name
            all_hits.append(hit)

    # ── Phase-16-follow-up: headcount-table role->SPPP scanner ──
    # Some PERA documents contain staffing/headcount rows that map roles
    # to SPPP codes directly, e.g. "Head Monitoring (MIS & GIS) / SPPP-2 (1)".
    # These rows are AUTHORITATIVE: they are what the corpus explicitly
    # states the SPPP is for that role, and they don't rely on chunk-
    # boundary guessing (unlike a "Salary and Benefits: SPPP-N" line that
    # may belong to an adjacent Position Title block).
    #
    # If the current query is role-scoped AND the retrieved hits contain
    # such a headcount row naming the queried role, synthesize a small
    # AUTHORITATIVE_ROLE_SALARY evidence block so the LLM can't miss it
    # and can't attribute the wrong SPPP from a neighbouring Position
    # Title. The synthesized block is tagged so downstream reports can
    # identify it, and it cites the real source chunk's page/eid so
    # references remain accurate.
    authoritative_salary_block = ""
    if target_role:
        target_norm_hc = re.sub(r"[^a-z0-9\s]", " ", target_role.lower())
        target_tokens_hc = [w for w in target_norm_hc.split() if len(w) > 2]
        _generic_hc = {
            "manager", "officer", "director", "assistant", "deputy",
            "chief", "head", "senior", "coordinator", "superintendent",
        }
        specific_tokens_hc = [t for t in target_tokens_hc if t not in _generic_hc]
        # Pattern captures "<role name containing specific tokens> / SPPP-N"
        # or "<role> - SPPP-N" with optional "(count)" trailing.
        _HC_PATTERN = re.compile(
            r"([A-Za-z][A-Za-z&()\s/,.\-]{2,80})\s*[/\-\u2013\u2014]\s*"
            r"(SPPP[-\s]?\d+|BPS[-\s]?\d+|BS[-\s]?\d+)"
            r"(?:\s*\((\d+)\))?",
            re.IGNORECASE,
        )
        # Stronger match: the headcount role phrase must contain ALL the
        # queried-role tokens that matter. We split the role into its
        # full token sequence (kept-in-order, lowercased, alphanumerics
        # only) and require the headcount phrase to contain every token,
        # AND require the FIRST kept token of the headcount phrase to
        # match the FIRST kept token of the queried role. The "first
        # token" check is what stops "Manager Monitoring" from latching
        # onto "Head Monitoring / SPPP-2" or "Assistant Manager
        # Monitoring / SPPP-5".
        target_token_seq = [
            w for w in re.split(r"[^a-z0-9]+", target_role.lower()) if w
        ]
        target_first_kept = (
            specific_tokens_hc[0] if specific_tokens_hc else
            (target_token_seq[0] if target_token_seq else "")
        )
        # The "head word" of the queried role for first-token check —
        # the FIRST role-prefix token (head/manager/assistant/etc.) that
        # appears in the queried role string. Used to disambiguate
        # head/manager/assistant variants of the same domain.
        _ROLE_PREFIX_HC = {
            "head", "manager", "assistant", "deputy", "additional",
            "joint", "regional", "director", "chief", "senior",
            "coordinator", "superintendent", "officer",
        }
        target_role_prefix = next(
            (w for w in target_token_seq if w in _ROLE_PREFIX_HC), ""
        )

        best_match = None  # (role_phrase_matched, code, source_hit)
        for hit in all_hits:
            txt = hit.get("text") or ""
            for m in _HC_PATTERN.finditer(txt):
                role_phrase_raw = m.group(1).strip()
                role_phrase = role_phrase_raw.lower()
                code = m.group(2).strip().upper().replace(" ", "-")

                # Tokenize the headcount phrase the same way.
                hc_tokens = [
                    w for w in re.split(r"[^a-z0-9]+", role_phrase) if w
                ]
                # Require ALL non-generic specific tokens of the queried
                # role to appear in the headcount phrase.
                if specific_tokens_hc:
                    if not all(tok in hc_tokens for tok in specific_tokens_hc):
                        continue
                else:
                    if target_role.lower() not in role_phrase:
                        continue
                # Require the role-prefix word to match: if queried role
                # starts with "manager", the headcount row's first
                # role-prefix word must also be "manager" (not "head" /
                # "assistant" / "deputy" etc.). This stops cross-tier
                # contamination on the same domain word.
                if target_role_prefix:
                    hc_first_prefix = next(
                        (w for w in hc_tokens if w in _ROLE_PREFIX_HC), ""
                    )
                    if hc_first_prefix and hc_first_prefix != target_role_prefix:
                        continue
                best_match = (role_phrase_raw, code, hit)
                break
            if best_match:
                break
        if best_match:
            role_phrase, code, src_hit = best_match
            src_doc = src_hit.get("_doc_name", "Unknown")
            src_page = src_hit.get("page_start", "?")
            src_eid = src_hit.get("evidence_id", "")
            safe_doc_hc = src_doc.replace("<", "").replace(">", "").replace('"', "").replace("'", "")
            authoritative_salary_block = (
                f'<evidence doc="{safe_doc_hc}" page="{src_page}" eid="{src_eid}" '
                f'role="authoritative-role-salary">\n'
                f"AUTHORITATIVE ROLE-SALARY MAPPING (from a headcount/staffing table):\n"
                f"  Role: {role_phrase}\n"
                f"  Pay code: {code}\n"
                f"(When the user asks the salary for this role, use THIS pay code; "
                f"ignore any 'Salary and Benefits: <code>' line that belongs to a "
                f"different Position Title block.)\n"
                f"</evidence>"
            )
            log.info(
                "Authoritative role-salary headcount match: role=%r -> %s "
                "(src doc=%s p.%s)",
                role_phrase, code, safe_doc_hc[:30], src_page,
            )
            # Also mark the headcount source hit as used-for-evidence so
            # references cite it even if it never makes it into
            # context_parts via the scoring path.
            src_hit["_used_for_evidence"] = True

            # Find the queried role's OWN Position Title chunk (if any
            # of the retrieved hits carries it) and mark it used too.
            # This ensures the JD page is cited alongside the salary
            # source so the user can read the substantive content.
            target_norm_pt = re.sub(r"[^a-z0-9\s]", " ", target_role.lower())
            target_words_pt = [w for w in target_norm_pt.split() if len(w) > 2]
            target_specific_pt = [
                w for w in target_words_pt if w not in _generic_hc
            ]
            for hit in all_hits:
                txt = hit.get("text") or ""
                # Look for Position Title blocks naming the queried role.
                for pt_match in re.finditer(
                    r"Position\s+Title\s*:\s*-?\s*([^\n]{3,200})", txt, re.IGNORECASE
                ):
                    title = pt_match.group(1).lower()
                    title_tokens = [w for w in re.split(r"[^a-z0-9]+", title) if w]
                    # Require all specific tokens AND role-prefix match.
                    if target_specific_pt and not all(
                        tok in title_tokens for tok in target_specific_pt
                    ):
                        continue
                    if target_role_prefix:
                        title_first_prefix = next(
                            (w for w in title_tokens if w in _ROLE_PREFIX_HC), ""
                        )
                        if title_first_prefix and title_first_prefix != target_role_prefix:
                            continue
                    if not target_specific_pt and target_role.lower() not in title:
                        continue
                    # Match — mark this hit so its page cites in
                    # references alongside the headcount-source page.
                    hit["_used_for_evidence"] = True
                    log.info(
                        "Marked role-JD chunk for citation: doc=%s p.%s "
                        "(matched Position Title %r)",
                        (hit.get("_doc_name") or "?")[:40],
                        hit.get("page_start"),
                        title.strip()[:60],
                    )
                    break

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

        # Phrase-co-occurrence bonus: when 2+ non-stop query words appear
        # within a short window of each other, the chunk is much more
        # likely to discuss the topic (vs. a chunk that merely mentions
        # each word in isolation elsewhere in a long definitions page).
        # This fixes the case where e.g. "structure" + "board" ranked a
        # definitions chunk above the actual governance document because
        # both words appeared scattered across the page.
        phrase_bonus = 0.0
        if len(_subject_words) >= 2:
            positions: Dict[str, int] = {}
            for w in _subject_words:
                idx = text_lower.find(w)
                if idx >= 0:
                    positions[w] = idx
            close_pairs = 0
            words_in_chunk = list(positions.items())
            for i in range(len(words_in_chunk)):
                for j in range(i + 1, len(words_in_chunk)):
                    if abs(words_in_chunk[i][1] - words_in_chunk[j][1]) <= 120:
                        close_pairs += 1
            if close_pairs >= 1:
                phrase_bonus = 1.5 * close_pairs

        # Position-title matching: boost exact, DROP wrong positions entirely
        title_bonus = 0.0
        if target_role:
            # First pass: check if the target role appears ANYWHERE in the
            # chunk as a "Position Title: - <role>" or "[Role: <role>]"
            # header. PERA PDFs often pack two consecutive role JDs into
            # one chunk (e.g. CTO then Manager (Development)); the first
            # pass ensures the second role is also boosted, not ignored
            # because _extract_position_title only returns the first.
            # Collapse all non-alphanumeric to a single space so that
            # "Manager (Development)" (which normalises to "manager  development ")
            # matches the target "manager development" regardless of the
            # original punctuation or spacing.
            text_norm_any = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
            target_norm_any = re.sub(r"[^a-z0-9]+", " ", target_role.lower()).strip()
            target_tokens_any = [w for w in target_norm_any.split() if w]
            role_pattern = r"\s+".join(re.escape(w) for w in target_tokens_any)
            pt_any_re = re.compile(
                r"position\s+title(?:\s+\w+)?\s+" + role_pattern,
                re.IGNORECASE,
            )
            role_any_re = re.compile(
                r"\brole(?:\s+\w+)?\s+" + role_pattern,
                re.IGNORECASE,
            )
            if pt_any_re.search(text_norm_any) or role_any_re.search(text_norm_any):
                # Subject-role mention present mid-chunk. Multi-role PERA
                # JD pages often get merged into one chunk where the
                # first Position Title is a different role; the normal
                # _extract_position_title path only sees that first title
                # and would down-weight this chunk. Bypass the default
                # title logic with a strong bonus.
                title_bonus = 10.0
                scored_hits.append(
                    (title_bonus + phrase_bonus + subject_match + base_score, hit)
                )
                continue

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

        combined = subject_match + phrase_bonus + title_bonus + heading_bonus + base_score
        scored_hits.append((combined, hit))

    # FALLBACK: If position filter dropped ALL position-titled chunks,
    # re-scan but ONLY admit chunks that do NOT carry an explicit Position
    # Title (i.e., chunks that were never evaluated for role match, such
    # as salary tables, schedule rows, or narrative sections).
    #
    # Phase 2 P0 fix: the previous fallback re-admitted ANY hit above the
    # similarity threshold, which silently reintroduced the exact
    # wrong-role chunks the position filter had just dropped — producing
    # confident but wrong-entity answers. Chunks whose Position Title
    # explicitly identifies a DIFFERENT role must stay dropped.
    if target_role and not scored_hits:
        log.warning(
            "Position filter dropped all hits for '%s' — re-admitting ONLY chunks "
            "without an explicit Position Title (role-mismatched chunks stay dropped)",
            target_role
        )
        for hit in all_hits:
            text = (hit.get("text") or "")
            text_lower = text.lower()
            base_score = float(hit.get("_blend", hit.get("score", 0)))
            is_context = hit.get("_is_smart_context", False)
            if not is_context and base_score < HIT_MIN_SCORE:
                continue
            # Skip any chunk whose Position Title disagrees with the target
            # role. Those were deliberately dropped upstream — re-admitting
            # them is a wrong-role hallucination vector.
            chunk_title = _extract_position_title(text)
            if chunk_title:
                continue
            subject_match = sum(1 for w in _subject_words if w in text_lower)
            combined = subject_match + base_score
            scored_hits.append((combined, hit))

        if not scored_hits:
            log.warning(
                "Fallback found no role-neutral chunks for '%s'. Returning empty "
                "evidence — caller will issue an honest 'role not found' refusal.",
                target_role
            )

    scored_hits.sort(key=lambda x: x[0], reverse=True)

    # Diversity injection: for non-role queries, ensure that each of the
    # top-3 retrieved documents gets at least one representative chunk.
    # Without this, a single document with many mid-score chunks can
    # crowd out higher-precision chunks from other documents — producing
    # evidence that is topically unbalanced and leads to hallucinated
    # answers drawing on information that is not actually in the
    # context. Only applies when target_role is empty (role queries
    # already use strict position-title filtering).
    if not target_role and scored_hits:
        doc_best: Dict[str, tuple] = {}
        for combined_s, h in scored_hits:
            d_name = h.get("_doc_name", "")
            if not d_name:
                continue
            # Keep the best-scoring chunk per doc that was observed.
            if d_name not in doc_best or combined_s > doc_best[d_name][0]:
                doc_best[d_name] = (combined_s, h)
        # Docs in blend-score order of their single best chunk.
        ranked_docs = sorted(
            doc_best.items(), key=lambda kv: kv[1][0], reverse=True
        )
        top_docs_for_diversity = [d for d, _ in ranked_docs[:3]]
        docs_in_top4 = set()
        for combined_s, h in scored_hits[:4]:
            docs_in_top4.add(h.get("_doc_name", ""))
        # If any of the top-3 docs are not represented in the top 4 chunks,
        # insert their best chunk into the head of scored_hits so evidence
        # assembly picks it up.
        for d in top_docs_for_diversity:
            if d in docs_in_top4:
                continue
            best = doc_best[d]
            # Insert just after position 1 so the top-1 chunk stays first
            # (usually correct) but diversity doesn't get squeezed out.
            scored_hits.insert(1, best)
            docs_in_top4.add(d)
            log.info(
                "Diversity inject: doc=%s combined_score=%.3f (ensuring "
                "top-3 retrieved docs each contribute a chunk)",
                d[:60], best[0],
            )

    # Content dedup is available but disabled to preserve proven scoring order.
    # Enable by uncommenting when evidence limits are expanded in future.
    # (dedup code preserved below for future use)

    # Assemble evidence respecting per-doc limits.
    #
    # Phase 3 change: within a single document, chunks are now emitted in
    # ASCENDING PAGE ORDER rather than by rerank score. Cross-document
    # ordering is unchanged (first doc to appear in rerank order comes
    # first). This preserves narrative flow for topics that span multiple
    # pages — the LLM now sees page 3 before page 5, so "continued on next
    # page" content reads in natural order. Ranking still controls WHICH
    # chunks are included; page order controls WHERE within a doc they
    # appear.
    doc_hit_counts: Dict[str, int] = {}
    docs_used_set: set = set()
    doc_first_seen: Dict[str, int] = {}
    next_doc_idx = 0

    # Collect selected (doc_rank_idx, page_int, part_text, hit) tuples
    selected_parts: List[tuple] = []

    def _page_to_int(p) -> int:
        if isinstance(p, int):
            return p
        if isinstance(p, str) and p.isdigit():
            return int(p)
        try:
            return int(p)
        except Exception:
            return 10_000  # sort unknowns last

    for _score, hit in scored_hits:
        doc_name = hit["_doc_name"]

        if len(docs_used_set) >= MAX_DOCS and doc_name not in docs_used_set:
            continue
        doc_count = doc_hit_counts.get(doc_name, 0)
        if doc_count >= MAX_HITS_PER_DOC:
            continue

        text = (hit.get("text") or "").strip()
        page = hit.get("page_start", "?")
        safe_doc = doc_name.replace("<", "").replace(">", "").replace('"', "").replace("'", "")
        eid = hit.get("evidence_id", "")

        # API-sourced evidence (Phase 4 of API ingestion) gets a distinct
        # XML envelope so the LLM can format citations differently and the
        # citation_formatter downstream can pick the API reference shape.
        is_api = hit.get("source_type") == "api"
        if is_api:
            record_id = hit.get("record_id", "")
            record_type = hit.get("record_type", "")
            api_source_id = hit.get("api_source_id", "")
            part = (
                f'<evidence doc="{safe_doc}" source_type="api" '
                f'record_id="{record_id}" record_type="{record_type}" '
                f'api_source="{api_source_id}" eid="{eid}">\n'
                f'[Source Type: API]\n'
                f'[API Name: {safe_doc}]\n'
                f'[Record ID: {record_id}]\n'
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

        if doc_name not in doc_first_seen:
            doc_first_seen[doc_name] = next_doc_idx
            next_doc_idx += 1

        selected_parts.append((doc_first_seen[doc_name], _page_to_int(page), part, hit))
        total_chars += len(part)
        doc_hit_counts[doc_name] = doc_count + 1
        docs_used_set.add(doc_name)
        # Track hit for reference extraction
        hit["_used_for_evidence"] = True

        if total_chars >= MAX_EVIDENCE_CHARS:
            break

    # Sort: primary by doc rerank order; secondary by page ascending.
    selected_parts.sort(key=lambda t: (t[0], t[1]))
    context_parts = [t[2] for t in selected_parts]

    evidence_ids = []
    for part in context_parts:
        m = re.search(r'eid="([^"]+)"', part)
        if m:
            evidence_ids.append(m.group(1))
    if evidence_ids:
        log.info("Evidence assembled (page-sorted within doc): %d chunks, %d chars, eids=%s",
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
            # Phase 2 P0 fix: REQUIRE role co-occurrence before injecting a
            # salary chunk. The previous implementation injected ANY chunk
            # containing SPPP/BPS/Schedule-III markers, which caused
            # cross-role contamination (e.g., a query about "Manager
            # Development salary" could import the Manager (Finance) row
            # because both were in the same Schedule-III table).
            # Now: only chunks that co-mention the target role's specific
            # terms (non-generic words) are eligible. If none exist, the
            # LLM will honestly say salary is not specified for this role.

            _generic_role_words = {
                "manager", "officer", "director", "assistant", "deputy",
                "chief", "head", "senior", "coordinator", "superintendent",
                "operator", "sergeant", "analyst", "developer", "writer",
            }
            target_norm = re.sub(r"[^a-z0-9\s]", " ", target_role.lower())
            target_tokens = [w for w in target_norm.split()
                             if len(w) > 2 and w not in _generic_role_words]

            log.info("Salary-bridge: checking role-matched salary chunks for '%s' "
                     "(specific tokens=%s)", target_role, target_tokens or "[generic]")

            _salary_marker_re = re.compile(
                r"(?:sppp[-\s]*\d|bps[-\s]*\d|bs[-\s]*\d|salary\s+and\s+benefits|"
                r"schedule[-\s]*iii|pay\s+(?:scale|package)|"
                r"minimum\s+pay|maximum\s+pay)",
                re.IGNORECASE
            )

            def _role_matches_salary_chunk(chunk_text_lower: str) -> bool:
                """True only when the candidate salary chunk clearly relates
                to the queried role. If the target role has specific
                (non-generic) tokens, at least one must appear. Otherwise
                the role phrase itself must appear."""
                if target_tokens:
                    return any(t in chunk_text_lower for t in target_tokens)
                # No specific tokens (e.g. bare 'manager') — require the
                # whole role phrase to co-occur to avoid generic contamination.
                return target_role.lower() in chunk_text_lower

            salary_supplements = []
            # Only consider chunks already ranked by the position-aware
            # scorer. Do NOT do a second unconditional scan over all_hits —
            # that scan was the main cross-role contamination source.
            for _score, hit in scored_hits:
                if hit.get("_used_for_evidence"):
                    continue
                text_lower = (hit.get("text") or "").lower()
                if not _salary_marker_re.search(text_lower):
                    continue
                if not _role_matches_salary_chunk(text_lower):
                    log.debug(
                        "Salary-bridge skipping chunk (salary markers present but "
                        "no target-role co-occurrence): %s...",
                        text_lower[:80]
                    )
                    continue
                salary_supplements.append(hit)
                if len(salary_supplements) >= 2:
                    break

            if not salary_supplements:
                log.info(
                    "Salary-bridge: no role-matched salary chunk found for '%s' — "
                    "NOT injecting (LLM will honestly report salary as unspecified)",
                    target_role
                )

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
                log.info("Salary-bridge injected (role-matched): %s p.%s (%d chars)",
                         safe_doc[:30], page, len(part))

    # Phase-16-follow-up: prepend the AUTHORITATIVE_ROLE_SALARY block (if
    # detected from a headcount/staffing table) so it's the FIRST thing
    # the LLM reads. This block is small, exempt from the evidence
    # budget, and crucially overrides any conflicting "Salary and
    # Benefits: <code>" line that belongs to a neighbouring Position
    # Title block in the same chunk.
    if authoritative_salary_block:
        context_parts.insert(0, authoritative_salary_block)

    return "\n\n".join(context_parts)


def extract_references_simple(
    retrieval: Dict[str, Any],
    question: str = "",
    answer_text: str = "",
) -> List[Dict[str, Any]]:
    """Extract reference links ONLY from chunks that were actually used for the LLM answer,
    filtered for relevance to the query topic.

    When *answer_text* is provided, chunks are additionally scored by
    how well they support the specific claims in the final answer —
    this ensures the top-ranked references point to the PDF pages that
    actually contain the content the user will see, not merely chunks
    that shared a keyword with the original question.
    """
    refs: List[Dict[str, Any]] = []
    seen = set()
    base_url = os.getenv("BASE_URL", "https://ask.pera.gop.pk").rstrip("/")

    evidence_list = retrieval.get("evidence", [])

    # Detect target role for relevance filtering. Mirror the abbreviation
    # expansion used in format_evidence_for_llm so that queries like
    # "sso ki salary?" resolve to "system support officer" — otherwise
    # downstream Position-Title page refinement cannot locate the
    # queried role inside a multi-page chunk.
    _ABBREV_REFS = {
        "cto": "chief technology officer",
        "dg": "director general",
        "ddg": "deputy director general",
        "adg": "additional director general",
        "hr": "human resources",
        "it": "information technology",
        "eo": "enforcement officer",
        "io": "investigation officer",
        "sso": "system support officer",
        "sdeo": "sub divisional enforcement officer",
        "deo": "data entry operator",
        "mgr": "manager",
        "dba": "database administrator",
        "se": "software engineer",
    }
    q_lower_refs = (question or "").lower()
    expanded_q_refs = q_lower_refs
    for _abbr, _full in _ABBREV_REFS.items():
        if _abbr in q_lower_refs.split():
            expanded_q_refs = expanded_q_refs.replace(_abbr, _full)
    target_role = _detect_target_role(expanded_q_refs) or _detect_target_role(question)

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

    # Answer-grounded scoring: extract distinctive content phrases from
    # the final answer so references are ranked by how well each chunk
    # supports the answer the user will actually see. This fixes the
    # common failure mode where a chunk containing literal query words
    # outranks the chunk that provided the answer's substantive claims.
    #
    # We extract both GENERAL named entities (for ref scoring) AND
    # HIGHLY DISTINCTIVE phrases (for page resolution) — the latter
    # must uniquely identify a specific page, not appear on every JD.
    answer_key_phrases: List[str] = []
    # Phrases strong enough to serve as a page-lookup needle (tried
    # first; must not be too generic to avoid wrong-page hits).
    answer_distinctive_phrases: List[str] = []
    if answer_text:
        # (a) Capitalized multi-word phrases (named entities like
        # "Member Finance", "Punjab Enforcement", "Schedule-III").
        for cap_match in re.finditer(
            r"\b([A-Z][A-Za-z&\-]{2,}(?:[\s\-]+[A-Z][A-Za-z&\-]{2,}){1,4})",
            answer_text,
        ):
            phrase = _normalize_for_match(cap_match.group(1))
            if len(phrase) >= 8 and phrase not in answer_key_phrases:
                answer_key_phrases.append(phrase)
        # (b) Pay codes / section refs / regulatory IDs.
        for tok in re.findall(
            r"\b(?:SPPP[-\s]?\d+|BPS[-\s]?\d+|BS[-\s]?\d+|Schedule[-\s]?[IVX]+|"
            r"Section\s+\d+|Article\s+\d+)\b",
            answer_text,
            re.IGNORECASE,
        ):
            answer_key_phrases.append(_normalize_for_match(tok))
        # (c) Quoted / technical phrases from the answer (3+ word
        # lowercase sequences that look like direct extracts).
        # These are the strongest page-lookup signals because they
        # tend to be rare.
        for tech_match in re.finditer(
            r'"([^"]{15,80})"',  # quoted phrases
            answer_text,
        ):
            ph = _normalize_for_match(tech_match.group(1))
            if ph and len(ph) >= 12:
                answer_distinctive_phrases.append(ph)
        # (d) Specific technical multi-word phrases that rarely appear
        # across unrelated pages (pay-related table headers, etc.).
        for tech_re in [
            r"\bminimum\s+pay\s+per\s+month\b",
            r"\bmaximum\s+pay\s+per\s+month\b",
            r"\bfuel\s+limit(?:\s+in\s+liters)?(?:\s+per\s+month)?\b",
            r"\bspecial\s+pay\s+package\s+of\s+pera\b",
            r"\bappointment\s+and\s+conditions\s+of\s+service\b",
            r"\bterms?\s+of\s+reference(?:\s+for\s+service)?\b",
        ]:
            for m in re.finditer(tech_re, answer_text, re.IGNORECASE):
                ph = _normalize_for_match(m.group(0))
                if ph:
                    answer_distinctive_phrases.append(ph)
    # Dedup, preserve order, cap.
    answer_key_phrases = list(dict.fromkeys(answer_key_phrases))[:20]
    answer_distinctive_phrases = list(
        dict.fromkeys(answer_distinctive_phrases)
    )[:10]

    # Query-based needles — stable page-lookup signals independent of
    # answer quality. When the LLM gives a generic fallback answer,
    # answer-phrase lookup yields nothing useful; these keep the cited
    # page anchored to what the user actually asked about.
    query_needles: List[str] = []
    if question:
        q_norm_nd = question
        # (1) Normalize common schedule/section references so we match
        # the PDF's spelling: "schedule 3" / "schedule iii" / "schedule-3"
        # all map to "schedule-iii".
        def _roman(n: int) -> str:
            rn = ""
            for v, r in [
                (10, "x"), (9, "ix"), (5, "v"),
                (4, "iv"), (1, "i"),
            ]:
                while n >= v:
                    rn += r
                    n -= v
            return rn
        for m_sch in re.finditer(
            r"\bschedule[-\s]?(\d+|[ivx]+)\b", q_norm_nd, re.IGNORECASE,
        ):
            token = m_sch.group(1).lower()
            if token.isdigit():
                roman = _roman(int(token))
                if roman:
                    query_needles.append(f"schedule-{roman}")
                    query_needles.append(f"schedule - {roman}")
                    query_needles.append(f"schedule {roman}")
            else:
                query_needles.append(f"schedule-{token}")
                query_needles.append(f"schedule {token}")
        # (2) Literal multi-word lowercased form of the full question
        # (only as a last-resort when short enough).
        q_full = _normalize_for_match(q_norm_nd)
        if 6 <= len(q_full) <= 60:
            query_needles.append(q_full)
        # (3) Any remaining multi-word non-stop phrase in the query.
        q_words_nd = [w for w in re.split(r"\s+", q_full) if len(w) > 2]
        if len(q_words_nd) >= 2:
            query_needles.append(" ".join(q_words_nd[:4]))
    query_needles = list(dict.fromkeys(query_needles))[:6]

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

    # Score hits for REFERENCE relevance (different from evidence scoring)
    # Bug-fix: a chunk often contains MULTIPLE Position Titles (e.g. a
    # working-paper page that lists several roles in sequence). The
    # previous logic only inspected the FIRST Position Title in the
    # chunk, which caused a HARD DROP whenever the queried role was
    # the 2nd/3rd/Nth title in the chunk — losing the JD page from
    # references. We now scan ALL Position Title blocks in the chunk
    # text and use the BEST match.
    # Capture each Position Title / [Role:] occurrence separately. The
    # capture is bounded: stop at the next " Report To:" / " Wing:" /
    # " Purpose" / next "Position Title:" / next "[Role:" / newline.
    # Without this the greedy [^\n]+ variant swallowed the entire rest
    # of the chunk on the first match, so a chunk carrying multiple JDs
    # (e.g. "CTO ... Manager (Development) ...") only ever matched the
    # first title and the real subject Position Title was invisible to
    # the reference picker.
    _ALL_POS_TITLE_RE = re.compile(
        r"(?:Position\s+Title\s*:\s*-?\s*"
        r"(.+?)(?=\s+Report\s+To\s*:|\s+Wing\s*:|\s+Purpose\s+of|\s+Position\s+Title\s*:|\s*\[Role:|\n|$)"
        r"|\[Role:\s*"
        r"(.+?)(?=\s+Report\s+To\s*:|\s+Wing\s*:|\s+Purpose\s+of|\]|\n|$)\]?)",
        re.IGNORECASE,
    )
    _generic = {"manager", "officer", "director", "assistant", "deputy",
               "chief", "head", "senior"}
    scored_refs = []
    for base_score, doc_name, hit in used_hits:
        text = (hit.get("text") or "").lower()
        ref_score = base_score

        # Boost/Drop: best Position Title match in the chunk vs target role.
        # Also track the chunk-role match explicitly so downstream
        # bonuses (answer-grounded, query-topic) can be gated — for a
        # role-specific query we only want citations from chunks about
        # the subject role, not chunks that merely mention the same
        # supervisor/subordinate roles as the answer.
        _role_match_kind = None  # "exact" | "sig_all" | "sig_partial" | "none" | None
        _target_role_effective = target_role
        if target_role:
            target_norm = re.sub(r"[^a-z0-9\s]", "", target_role)
            target_sig = [w for w in target_norm.split()
                          if w not in _generic and len(w) > 2]
            # Defensive: if the role collapses to only generic words
            # (e.g. "officer" alone from "kia pera officer ..."), treat
            # the query as non-role to avoid hard-dropping every chunk
            # that doesn't carry that exact generic word in its
            # Position Title. Policy/regulatory chunks (body-camera
            # use, digital evidence, recording protocols) rarely have
            # Position Title blocks and would otherwise vanish from
            # references entirely.
            if not target_sig and len(target_norm.split()) <= 1:
                _target_role_effective = ""
        if _target_role_effective:
            # Collect ALL position-title candidates in the chunk.
            chunk_titles = []
            for tm in _ALL_POS_TITLE_RE.finditer(hit.get("text") or ""):
                title = (tm.group(1) or tm.group(2) or "").strip().lower()
                if title:
                    chunk_titles.append(title)

            if chunk_titles:
                # Find the best title match among all candidates.
                best_kind = None  # "exact" / "sig_all" / "sig_partial" / "none"
                for chunk_title in chunk_titles:
                    chunk_norm = re.sub(r"[^a-z0-9\s]", "", chunk_title)
                    if target_norm in chunk_norm or chunk_norm in target_norm:
                        best_kind = "exact"
                        break
                    elif target_sig and all(w in chunk_norm for w in target_sig):
                        if best_kind is None or best_kind == "none":
                            best_kind = "sig_all"
                    elif target_sig and any(w in chunk_norm for w in target_sig):
                        if best_kind is None or best_kind == "none":
                            best_kind = "sig_partial"
                    else:
                        if best_kind is None:
                            best_kind = "none"
                _role_match_kind = best_kind
                if best_kind == "exact":
                    ref_score += 5.0  # Exact position match in some title
                elif best_kind == "sig_all":
                    ref_score += 3.0  # All specific words match in some title
                elif best_kind == "sig_partial":
                    # Partial overlap — keep, no boost. (Was previously
                    # surviving as no-boost too.)
                    pass
                else:
                    # All chunk titles are for unrelated roles AND none
                    # contain even a partial overlap → HARD DROP
                    # (preserves the original protection against citing
                    # wrong-role chunks).
                    continue
            else:
                # No Position Title in chunk — check if target role words in body
                if target_norm in text:
                    ref_score += 3.0
                    _role_match_kind = "body"
                else:
                    # No Position Title AND no body mention of the
                    # target role. For a role-specific query, these
                    # chunks are irrelevant as citations (TOC pages, HR
                    # manual boilerplate, unrelated meeting minutes).
                    # HARD DROP — otherwise they ride in as fillers
                    # when higher-signal chunks all belong to the same
                    # doc and hit the per-doc cap.
                    continue

        # Boost: snippet mentions query subject words
        word_hits = sum(1 for w in q_words if w in text)
        ref_score += word_hits * 0.5

        # Phrase-co-occurrence bonus for references too: chunks where
        # several query terms appear within close proximity are much
        # more likely to be the actual source of the answer. Without
        # this, a definitions chunk mentioning "board" and "structure"
        # 30 lines apart outranks a governance chunk that discusses
        # them together. Mirrors the bonus used in evidence scoring.
        if len(q_words) >= 2:
            positions_r: Dict[str, int] = {}
            for w in q_words:
                idx = text.find(w)
                if idx >= 0:
                    positions_r[w] = idx
            close_pairs_r = 0
            items_r = list(positions_r.items())
            for i in range(len(items_r)):
                for j in range(i + 1, len(items_r)):
                    if abs(items_r[i][1] - items_r[j][1]) <= 120:
                        close_pairs_r += 1
            if close_pairs_r >= 1:
                ref_score += 2.0 * close_pairs_r

        # Query-topic bonus: chunks that contain the user's query
        # topic (e.g. "schedule-iii" for query "schedule 3") are
        # always relevant, regardless of answer quality.
        if query_needles:
            text_normalized_q = _normalize_for_match(hit.get("text") or "")
            q_hits = 0
            for ph in query_needles:
                if ph and len(ph) >= 6 and ph in text_normalized_q:
                    q_hits += 1
            if q_hits > 0:
                ref_score += 5.0 * q_hits

        # Answer-grounded bonus: large reward per distinctive answer
        # phrase present in this chunk. This is the strongest signal
        # because a chunk that contains an entity mentioned in the
        # answer is almost certainly where the model got it from.
        #
        # GATE (role queries only): for questions that target a specific
        # role, chunks that don't carry the subject role's Position
        # Title are almost always cross-referenced mentions (the
        # supervisor's own JD, a definitions page, a TOC, etc.) — they
        # contain answer phrases but are NOT the source of the
        # statement. Skip their answer-grounded boost so they can't
        # climb above the genuine subject chunk.
        _answer_boost_allowed = True
        if _target_role_effective and _role_match_kind in ("sig_partial", "none", None):
            _answer_boost_allowed = False
        if answer_key_phrases and _answer_boost_allowed:
            text_normalized_ans = _normalize_for_match(
                hit.get("text") or ""
            )
            answer_hits = 0
            for ph in answer_key_phrases:
                if ph and ph in text_normalized_ans:
                    answer_hits += 1
            if answer_hits > 0:
                ref_score += 4.0 * answer_hits
                log.info(
                    "Answer-grounded ref boost: doc=%s p=%s hits=%d "
                    "(score %.2f)",
                    doc_name[:35],
                    hit.get("page_start"),
                    answer_hits,
                    ref_score,
                )

        scored_refs.append((ref_score, doc_name, hit, _role_match_kind))

    scored_refs.sort(key=lambda x: x[0], reverse=True)

    # Take top refs (max 4), with per-doc limits
    docs_used = set()
    doc_ref_counts: Dict[str, int] = {}
    max_refs = 4
    max_refs_per_doc = 2

    # Role-specific single-source mode: when the query targets a specific
    # role AND the top-scoring chunk has a strong Position Title match
    # ("exact" or "sig_all"), the user wants ONE authoritative page — the
    # JD page. Filler refs from supervisor/subordinate JDs, organogram
    # pages, or TOC entries are actively harmful here (user reported
    # seeing p.21 + p.2 instead of a single p.22 for the Manager
    # (Infrastructure) JD). Lock to a single ref in that case.
    _top_role_confident = bool(
        target_role
        and scored_refs
        and scored_refs[0][3] in ("exact", "sig_all")
    )
    if _top_role_confident:
        max_refs = 1
        max_refs_per_doc = 1

    for _score, doc_name, hit, _rmk in scored_refs:
        if len(refs) >= max_refs:
            break
        if len(docs_used) >= MAX_DOCS and doc_name not in docs_used:
            continue
        if doc_ref_counts.get(doc_name, 0) >= max_refs_per_doc:
            continue

        # ── API-sourced reference (Phase 4 of API ingestion) ──
        # API hits don't have a PDF page, so the role-aware page-finding
        # logic below would fail. Build an API-shaped reference and
        # short-circuit. CitationFormatter handles the actual rendering.
        if hit.get("source_type") == "api":
            from citation_formatter import CitationFormatter as _CF
            record_id = hit.get("record_id", "")
            api_key = f"api:{doc_name}:{record_id}"
            if api_key in seen:
                continue
            seen.add(api_key)
            try:
                api_ref = _CF().format_api_reference(
                    display_name=doc_name,
                    record_id=record_id,
                    record_type=hit.get("record_type", ""),
                    source_id=hit.get("api_source_id", ""),
                    snippet=(hit.get("text") or "")[:200],
                )
            except Exception:
                api_ref = {
                    "document": doc_name,
                    "source_type": "api",
                    "api_display_name": doc_name,
                    "record_id": record_id,
                    "snippet": (hit.get("text") or "")[:200],
                }
            refs.append(api_ref)
            doc_ref_counts[doc_name] = doc_ref_counts.get(doc_name, 0) + 1
            docs_used.add(doc_name)
            continue

        # Bug-fix: chunks built by the chunker can span MANY pages
        # (loc_start..loc_end with loc_end-loc_start > 5 is common for
        # Annex-G/Annex-H/Working-Paper "section" chunks). The chunk's
        # loc_start is just where the chunk begins in the PDF and is
        # NOT necessarily the page where the queried role's Position
        # Title actually appears. Cite the most accurate page we can:
        #
        #   1. Find the queried role's Position Title within the chunk
        #      text. If found, estimate which page of the chunk's
        #      loc_start..loc_end span that match falls on (linear by
        #      character position).
        #   2. If no match, fall back to loc_start.
        #   3. Anchor the snippet to the matched Position Title block
        #      so the user sees the relevant content, not the chunk
        #      head (which often belongs to a different role).
        chunk_text = hit.get("text") or ""
        loc_start = hit.get("page_start", 1)
        loc_end = hit.get("page_end", loc_start) or loc_start
        try:
            loc_start_i = int(loc_start)
            loc_end_i = int(loc_end)
        except (ValueError, TypeError):
            loc_start_i = loc_end_i = loc_start

        page = loc_start_i
        snippet_text = chunk_text[:200]
        _page_locked_by_role = False

        # Try to refine the page when the queried role appears at a
        # specific Position Title block inside this multi-page chunk.
        if target_role and chunk_text and loc_end_i > loc_start_i:
            target_norm_pg = re.sub(r"[^a-z0-9\s]", " ", target_role.lower())
            target_words_pg = [w for w in target_norm_pg.split() if len(w) > 2]
            target_specific_pg = [
                w for w in target_words_pg if w not in _generic
            ]
            best_match_pos = -1
            best_match_title = ""
            for tm in _ALL_POS_TITLE_RE.finditer(chunk_text):
                title_raw = (tm.group(1) or tm.group(2) or "").strip()
                title_lower = title_raw.lower()
                title_tokens = [
                    w for w in re.split(r"[^a-z0-9]+", title_lower) if w
                ]
                # Require all queried-role specific tokens to appear in
                # this Position Title for the page to be refined.
                if target_specific_pg and not all(
                    tok in title_tokens for tok in target_specific_pg
                ):
                    continue
                if not target_specific_pg and target_role.lower() not in title_lower:
                    continue
                best_match_pos = tm.start()
                best_match_title = title_raw
                break
            if best_match_pos >= 0:
                # Compute a linear-ratio fallback first (used only if
                # the PDF-based exact lookup fails).
                ratio = best_match_pos / max(1, len(chunk_text))
                page_span = loc_end_i - loc_start_i
                est_page = loc_start_i + int(round(ratio * page_span))
                est_page = max(loc_start_i, min(loc_end_i, est_page))

                # Try to resolve the EXACT 1-based PDF page that contains
                # the matched Position Title (falls back to est_page).
                pdf_path_role = _find_pdf_path_for_hit(
                    {**hit, "doc_name": doc_name}
                )
                _needle = re.sub(r"\s+", " ", best_match_title).strip()[:40]
                exact_page = _resolve_exact_page(
                    pdf_path_role,
                    _needle,
                    fallback_page=est_page,
                    min_page=loc_start_i,
                    max_page=loc_end_i,
                )
                page = exact_page
                # Mark the page as role-locked so the downstream
                # generic density resolver doesn't override it with
                # a page that merely has more generic-needle hits.
                _page_locked_by_role = True
                # Snippet anchored on the matched Position Title block,
                # so the user can see WHY this is cited.
                snippet_end = min(len(chunk_text), best_match_pos + 240)
                snippet_text = chunk_text[best_match_pos:snippet_end]
                log.info(
                    "Reference page refined (role): doc=%s loc_start=%s loc_end=%s "
                    "matched %r at char %d est=%s exact=%s",
                    (doc_name or "?")[:40], loc_start_i, loc_end_i,
                    best_match_title[:50], best_match_pos, est_page, page,
                )

        # Generalized page resolver — runs for chunks where role-based
        # resolution did not succeed. When the queried role's Position
        # Title was found (page_locked_by_role=True), that's our best
        # signal and we trust it: running the density resolver here
        # would override it with a page that merely has more generic
        # needle hits (e.g. "schedule-iii" appears on every JD).
        if _page_locked_by_role:
            chunk_text = ""  # Skip the generic block below.
        #
        # PRIORITY: if the answer text has distinctive phrases, try
        # each of them as the needle FIRST — this ensures the cited
        # page is the page that actually contains the content the
        # user sees in the answer (not just the first Position Title
        # in a multi-page chunk). Falls back to the generic
        # fragment picker if no answer-phrase match is found.
        if chunk_text:
            generic_pdf_path = _find_pdf_path_for_hit(
                {**hit, "doc_name": doc_name}
            )
            if generic_pdf_path:
                lo_page = loc_start_i
                hi_page = loc_end_i if loc_end_i >= loc_start_i else loc_start_i
                resolved = page
                needle_used = ""
                # Density-based page lookup: score every page in the
                # chunk's range by how many needles it contains, pick
                # the richest page. This is much more reliable than
                # first-match when the needle (e.g. "schedule-iii")
                # appears on many pages — the page with the RICHEST
                # combination of query+answer needles is almost
                # always the true source.
                text_norm_for_ans = _normalize_for_match(chunk_text)
                all_needles = (
                    query_needles
                    + answer_distinctive_phrases
                    + answer_key_phrases
                )
                # Only consider needles that actually appear in this
                # chunk (prevents cross-chunk bleed).
                in_chunk_needles = [
                    n for n in all_needles
                    if n and len(n) >= 4 and _normalize_for_match(n) in text_norm_for_ans
                ]
                if in_chunk_needles:
                    cand_page, cand_needle = _resolve_best_page(
                        generic_pdf_path,
                        in_chunk_needles,
                        fallback_page=page,
                        min_page=lo_page,
                        max_page=hi_page,
                    )
                    resolved = cand_page
                    needle_used = cand_needle or (in_chunk_needles[0] if in_chunk_needles else "")
                # Fallback: generic fragment (e.g., Position Title) when
                # no answer/query needle is present in this chunk.
                if not needle_used:
                    generic_needle = _pick_distinctive_fragment(
                        snippet_text if snippet_text else chunk_text,
                        max_len=60,
                    )
                    if generic_needle:
                        cand_page = _resolve_exact_page(
                            generic_pdf_path,
                            generic_needle,
                            fallback_page=page,
                            min_page=lo_page,
                            max_page=hi_page,
                        )
                        resolved = cand_page
                        needle_used = generic_needle
                if resolved != page:
                    log.info(
                        "Reference page refined (generic): doc=%s "
                        "range=%s-%s needle=%r %s->%s",
                        (doc_name or "?")[:40], lo_page, hi_page,
                        needle_used[:40], page, resolved,
                    )
                page = resolved
                # Also refresh the snippet/highlight to the answer
                # phrase when that was what we matched — so the
                # reference pill and PDF highlight both reflect the
                # cited content.
                if needle_used and (
                    needle_used in answer_key_phrases
                    or needle_used in answer_distinctive_phrases
                    or needle_used in query_needles
                ):
                    idx_in_chunk = text_norm_for_ans.find(needle_used)
                    if idx_in_chunk >= 0:
                        # Map back to char position in original text.
                        # Because _normalize_for_match only collapses
                        # whitespace/lowercases, char offsets may
                        # differ slightly — use a regex search on the
                        # raw text for a robust snippet.
                        ph_pattern = re.compile(
                            r"\s+".join(
                                re.escape(tok)
                                for tok in needle_used.split()
                                if tok
                            ),
                            re.IGNORECASE,
                        )
                        m = ph_pattern.search(chunk_text)
                        if m:
                            snip_end = min(len(chunk_text), m.start() + 240)
                            snippet_text = chunk_text[m.start():snip_end]

        path = hit.get("public_path", "")
        key = f"{doc_name}_{page}"
        if key in seen:
            continue
        seen.add(key)

        # Build a highlight phrase for Chrome/Edge PDF viewers: they honor
        # `#page=N&search="phrase"` by auto-finding and highlighting the
        # phrase after navigating to the page. Picking the same
        # distinctive fragment used for page lookup maximizes match odds.
        hl_fragment = _pick_distinctive_fragment(snippet_text, max_len=60)
        if not hl_fragment:
            hl_fragment = _pick_distinctive_fragment(chunk_text, max_len=60)
        from urllib.parse import quote as _urlquote
        hl_param = f'&search="{_urlquote(hl_fragment)}"' if hl_fragment else ""

        base_prefix = f"{base_url}{path}" if path else f"{base_url}/assets/data/{doc_name}"
        url = f"{base_prefix}#page={page}{hl_param}"

        ref = {
            "document": doc_name,
            "page_start": page,
            "open_url": url,
            "snippet": snippet_text[:200],
            "highlight": hl_fragment,
            "source_type": "document",
        }
        # When the chunk spans multiple pages, also surface the chunk's
        # full page range so the UI can display it (the actual page is
        # an estimate within this range).
        if loc_end_i > loc_start_i:
            ref["page_end"] = loc_end_i
            ref["page_range"] = f"{loc_start_i}-{loc_end_i}"
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

    # Phase 4 calibration: the judge's verdict is the primary signal.
    # The composite score is treated as a secondary check that can ONLY
    # downgrade a verdict, never upgrade it. In particular:
    #   • "supported" requires sem == "full" AND a healthy composite
    #     score. A judge-flagged "combined" no longer maps to "supported"
    #     because combined verdicts contain inference risk.
    #   • "combined" maps to "partially_supported" by default so the
    #     answer carries a visible inference-note for the user.
    #   • "partial" always maps to either "partially_supported" or
    #     "unsupported"; the previous lenient threshold of 0.35 is
    #     raised to 0.45 so weak partial-support cannot be served as
    #     normal partial.
    #   • "none" stays "unsupported" — confirmed Phase 2 protection.
    sem = grounding.semantic_support
    score = float(grounding.score)

    if sem == "full":
        # High-confidence path. Require composite score to also be healthy.
        if score >= 0.65:
            return "supported"
        # The judge said "full" but composite score is low. This usually
        # means evidence quality was weak even though the claim was
        # technically present. Downgrade to partial rather than refuse.
        return "partially_supported"
    elif sem == "combined":
        # Inference-risk path. Always partial; never "supported".
        if score < 0.30:
            return "unsupported"
        return "partially_supported"
    elif sem == "partial":
        if score >= 0.45:
            return "partially_supported"
        return "unsupported"
    elif sem == "none":
        return "unsupported"

    # Semantic judge was not run (Tier-1 had concrete claims to check).
    # Use stricter score bands than before.
    if score >= 0.70:
        return "supported"
    if score >= 0.45:
        return "partially_supported"
    if score < 0.25:
        return "unsupported"
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
    """Phase 2 P0 fix: DISABLED.

    The prior implementation removed sentences such as "not explicitly
    mentioned", "insufficient information", and "does not contain" from
    answers classified as supported / partially_supported. That was a
    safety inversion: the model's own honest uncertainty — the single
    clearest hallucination-reduction signal the LLM produces — was being
    silently deleted before the user saw it. Hedged answers were being
    converted into falsely confident ones.

    This function is now a pass-through. All call sites are preserved
    so no caller breaks. Honest uncertainty MUST reach the user.
    """
    return answer_text or ""


# =============================================================================
# Wording Notes per Support State (appended at end, not bold)
# =============================================================================
_PARTIAL_SUPPORT_NOTE = (
    "\n\nNote: This answer is derived from the relevant PERA provisions and is not stated "
    "as a single standalone clause in the documents."
)


# =============================================================================
# Phase 4 — Lightweight entity-consistency check
# =============================================================================
# Detects high-risk answer entities (money amounts, BPS/SPPP codes, legal
# references) that do not appear in the retrieved evidence. Findings are
# fed back into the grounding result so the existing composite-score
# penalty + support-state classifier can react. Intentionally narrow:
# this is an additive guard, not a replacement for the LLM judge.

_ENTITY_MONEY_RE = re.compile(
    r"(?:Rs\.?\s*[\d,]+(?:\.\d+)?|"
    r"\bPKR\s*[\d,]+(?:\.\d+)?|"
    r"\b\d{1,3}(?:,\d{3})+(?:\.\d+)?)",
    re.IGNORECASE,
)
_ENTITY_PAYCODE_RE = re.compile(
    r"\b(?:SPPP|BPS|BS)\s*[-\s]?\s*\d{1,2}[A-Z]?\b",
    re.IGNORECASE,
)
_ENTITY_LEGAL_RE = re.compile(
    r"\b(?:Section|Clause|Rule|Article|Annex(?:ure)?|Schedule|Chapter|Part|Appendix)"
    r"\s*[-\s]?\s*[\dIVXLC]+(?:\([\dIVXLCa-z]+\))?\b",
    re.IGNORECASE,
)


def _norm_entity_for_match(s: str) -> str:
    """Normalize money/code/legal-ref strings for boundary-aware matching."""
    return re.sub(r"[\s,\-]+", "", (s or "").lower())


def _entity_present_in_evidence(entity: str, evidence_norm: str) -> bool:
    """Boundary-aware presence check on already-normalized evidence text.
    Numbers must not be a sub-sequence of a longer number (so "rs50000"
    must NOT match inside "rs500000")."""
    e = _norm_entity_for_match(entity)
    if not e:
        return False
    pattern = re.compile(r"(?<!\d)" + re.escape(e) + r"(?!\d)")
    return bool(pattern.search(evidence_norm))


def _entity_consistency_check(answer_text: str, context_str: str) -> List[str]:
    """Return a list of high-risk entities present in the answer but
    absent (after boundary-aware normalization) from the evidence.

    Only runs over money amounts, pay codes, and legal references — the
    classes most damaging when fabricated. Generic terms are not checked.
    """
    if not answer_text or not context_str:
        return []

    answer = answer_text
    evidence_norm = _norm_entity_for_match(context_str)

    found: List[str] = []
    seen: set = set()

    for rx in (_ENTITY_MONEY_RE, _ENTITY_PAYCODE_RE, _ENTITY_LEGAL_RE):
        for m in rx.finditer(answer):
            ent = m.group(0).strip()
            if not ent:
                continue
            key = _norm_entity_for_match(ent)
            if key in seen:
                continue
            seen.add(key)
            if not _entity_present_in_evidence(ent, evidence_norm):
                found.append(ent)
                if len(found) >= 8:
                    return found
    return found

_CONFLICTING_NOTE = (
    "\n\nNote: The PERA documents contain provisions that may differ on this matter. "
    "The relevant positions are presented above."
)


# Source-mode-aware fallback messages.
# Used by the orchestrator when retrieval returns zero evidence — picks
# the wording matching whichever data source the user opted into.
#
# Each message follows the Phase-1 refusal template:
#   1) what's unavailable (clear, honest)
#   2) explicit "won't fabricate" assurance
#   3) optional "closest available" slot (filled by caller when possible)
#   4) one concrete next step the user can take
#
# Replaces the prior dead-end one-liners that just said "couldn't find".
_NO_EVIDENCE_FALLBACK = {
    "documents": (
        "I could not find this in the indexed PERA documents.\n\n"
        "I will not fabricate or estimate facts that are not in the source "
        "material.\n\n"
        "**Next step:** try rephrasing with a more specific term (e.g. an "
        "Act section number, a role title, or the exact regulation name), "
        "or switch the **Knowledge Source** to **Both** to also search the "
        "live operational data."
    ),
    "stored_api": (
        "I could not find this in the stored API data (challans, "
        "inspections, requisitions, divisions, finance).\n\n"
        "I will not invent numbers that are not in the synced datasets.\n\n"
        "**Next step:** switch the **Knowledge Source** to **Documents** "
        "for policy/governance questions, **Live API** for the very latest "
        "value, or **Both** to search across everything."
    ),
    "both": (
        "I could not find this in either the indexed PERA documents or the "
        "stored API data.\n\n"
        "I will not fabricate facts or estimate figures that are not in "
        "the source material.\n\n"
        "**Next step:** try rephrasing with a specific entity name (a "
        "tehsil, division, officer, role, Act section, or KPI), check "
        "spelling, or contact the data team if you believe this should "
        "be available."
    ),
    "live_api": (
        "The live API did not return data for this query.\n\n"
        "I will not synthesise a value when the upstream system returns "
        "no result.\n\n"
        "**Next step:** confirm the entity name (division, district, "
        "tehsil) is spelled correctly, or switch the **Knowledge Source** "
        "to **Stored API** to search the locally-cached snapshot."
    ),
}


def _get_no_evidence_message(
    answer_source_mode: str = "both",
    closest_available: Optional[str] = None,
) -> str:
    """Return the source-mode-appropriate no-evidence fallback.

    When ``closest_available`` is provided (a one-line description of the
    nearest related metric the system DOES have), it's spliced into the
    middle of the template so the user still gets useful direction
    instead of a dead-end refusal.
    """
    base = _NO_EVIDENCE_FALLBACK.get(
        answer_source_mode, _NO_EVIDENCE_FALLBACK["both"]
    )
    if closest_available:
        # Splice the closest-available block in just before "Next step:"
        marker = "**Next step:**"
        if marker in base:
            return base.replace(
                marker,
                f"**Closest available information:** {closest_available}\n\n{marker}",
            )
        # Defensive fallback if the template ever changes shape
        return f"{base}\n\n**Closest available information:** {closest_available}"
    return base


def _apply_support_state_wording(
    answer_text: str,
    support_state: str,
    answer_source_mode: str = "both",
) -> str:
    """
    Apply clean, professional wording based on support state.
    Source-mode-aware (preserved from current project): uses
    'stored API data' / 'available documents' / both depending on
    `answer_source_mode`.

    - supported: answer as-is, no notes
    - partially_supported: answer first, mode-aware note at end
    - conflicting: answer first, mode-aware conflict note at end
    - unsupported: caller substitutes; this function returns answer_text
      unchanged so legacy callers see no decoration leak.
    """
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
        # Includes both the new (Phase B) base wording and the source
        # label so test_answerer.py and test_stored_api_mode both pass.
        return (
            cleaned
            + "\n\nNote: This answer is derived from the relevant PERA provisions "
              f"based on {source_label}, and is not stated as a single standalone "
              "clause in the documents."
        )

    elif support_state == "conflicting":
        cleaned = _strip_contradictory_disclaimers(answer_text)
        return (
            cleaned
            + f"\n\nNote: Multiple sources in the {source_label} address this matter "
              "and may differ on this matter. The relevant positions are presented above."
        )

    elif support_state == "unsupported":
        # The orchestrator usually substitutes the entire answer for an
        # unsupported state. When this helper IS reached for unsupported
        # (legacy callers + tests), append a source-aware caution rather
        # than returning bare text — the test suite checks the source
        # label appears even on this branch.
        cleaned = _strip_contradictory_disclaimers(answer_text)
        return (
            cleaned
            + f"\n\nNote: The {source_label} retrieved for this question do not "
              "directly address every aspect. The response above is inferred from "
              "related content — please verify against the original sources."
        )

    return answer_text


# =============================================================================
# Main Answer Function
# =============================================================================
def answer_question(
    current_question: str,
    retrieval: Dict[str, Any],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    intent: Optional[Dict[str, Any]] = None,
    answer_source_mode: Optional[str] = None,
    **_extra: Any,
) -> Dict[str, Any]:
    """Generate a grounded answer.

    Phase 5: optional `intent` dict. Shape (all keys optional):
      {
        "primary": "factual" | "role_info" | "procedural" |
                   "comparison" | "list" | "vague" | "followup" |
                   "out_of_scope" | "smalltalk",
        "is_multi_part": bool,
        "sub_queries": List[str],
        "is_vague": bool,
      }

    The intent is advisory — it shapes prompt guidance and can fast-path
    out-of-scope refusal. It NEVER overrides Phase 2 grounding/refusal
    logic. When `intent` is None, behavior is identical to pre-Phase-5.
    """
    client = get_chat_client()
    intent = intent or {}

    # ── 0z. DETERMINISTIC-ANSWER BYPASS (Phase production wiring) ──
    # When the retrieval contains a structured payload tagged as a
    # ranking / aggregate / officer_table, return the code-rendered
    # answer directly without sending the table through the LLM. This
    # eliminates the "LLM re-orders rows" and "LLM hallucinates totals"
    # failure modes that motivated this patch.
    _det_answer = retrieval.get("deterministic_answer") if retrieval else None
    _ev_type = retrieval.get("evidence_type") if retrieval else None
    if _det_answer and _ev_type in ("ranking", "aggregate",
                                    "officer_table",
                                    "structured_analytics"):
        try:
            from numeric_validator import validate_numeric_answer
            payload = retrieval.get("structured_payload") or _det_answer
            # Sanity check the deterministic answer against itself to
            # catch any future bug where formatter and payload diverge.
            res = validate_numeric_answer(_det_answer, payload,
                                          evidence_type=_ev_type)
            num_validation = {
                "ok": res.ok, "reason": res.reason,
                "unsupported_count": len(res.unsupported_numbers),
            }
        except Exception:
            num_validation = {"ok": True, "reason": "validator_skipped"}
        log.info("Deterministic answer used (evidence_type=%s)", _ev_type)
        # Build references like the rest of the pipeline so the
        # frontend's source panel is populated.
        try:
            refs = extract_references_simple(
                retrieval, question=current_question, answer_text=_det_answer
            )
        except Exception:
            refs = []
        return {
            "answer": _det_answer,
            "references": refs,
            "decision": "answer",
            "support_state": "supported",
            "deterministic": True,
            "evidence_type": _ev_type,
            "numeric_validation": num_validation,
            "grounding": {
                "score": 0.95,
                "confidence": "high",
                "support_state": "supported",
                "semantic_support": "deterministic",
            },
        }

    # 0y. Typo correction.
    # Apply the same PERA-vocabulary typo correction the retriever uses
    # so `_detect_target_role` and downstream substring-matching logic
    # see the canonical spelling (e.g. "develppment" -> "development").
    # Without this, the retriever rewrite cleans up the query it searches
    # with, but the answerer's position-aware filter still operates on
    # the user's typo'd original text and fails to match.
    try:
        from retriever import _fix_query_typos as _fix_typos
        _corrected = _fix_typos(current_question or "")
        if _corrected and _corrected != current_question:
            log.info("Typo-corrected question: %r -> %r",
                     (current_question or "")[:80], _corrected[:80])
            current_question = _corrected
    except Exception:
        pass

    # 0a. Out-of-scope fast-path (Phase 5).
    # The intent classifier marks weather/sports/recipes/etc. so we can
    # decline cheaply without retrieval cost. We only fast-path when
    # retrieval also returned no evidence — if evidence somehow exists,
    # we let the normal grounded path decide.
    if intent.get("primary") == "out_of_scope" and not retrieval.get("has_evidence"):
        return {
            "answer": (
                "This question is outside PERA's scope. I can help with "
                "questions about PERA's regulations, structure, roles, "
                "and operations."
            ),
            "references": [],
            "decision": "refuse",
            "support_state": "out_of_scope",
        }

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
            _ABBREV_SHORT = {
                "cto": "Chief Technology Officer", "dg": "Director General",
                "ddg": "Deputy Director General", "adg": "Additional Director General",
                "dd": "Deputy Director", "eo": "Enforcement Officer",
                "io": "Investigation Officer", "sso": "System Support Officer",
                "sdeo": "Sub Divisional Enforcement Officer", "deo": "Data Entry Operator",
                "dba": "Database Administrator", "se": "Software Engineer",
            }
            if q_stripped in _ABBREV_SHORT:
                current_question = f"Tell me about the {_ABBREV_SHORT[q_stripped]} position at PERA"
                log.info("Short-query expanded (abbrev): '%s' -> '%s'", q_stripped, current_question)

    # 1. Build Context
    context_str = format_evidence_for_llm(retrieval, question=current_question)
    if not context_str:
        # Retrieval returned nothing. FAISS genuinely has no relevant
        # evidence for this query — return the source-mode-aware
        # "no information found" refusal so the wording matches whichever
        # data source the user opted into.
        return {
            "answer": _get_no_evidence_message(answer_source_mode or "both"),
            "references": [],
            "decision": "refuse",
            "support_state": "unsupported"
        }

    # 1b. Pre-generation evidence quality gate — REMOVED.
    # If FAISS found evidence and it passed into context, the LLM should always try.
    # The only refuse point is empty context (no evidence at all).
    evidence_list = retrieval.get("evidence", [])

    # 2. System Prompt (v6: strict grounded answers — Phase 2 P0)
    # Design change: remove instructions that pressured the model to answer
    # even without direct evidence. Require grounded responses with honest
    # uncertainty when the retrieved Context does not cover the question.
    system_prompt = (
        "You are the PERA AI Assistant for the Punjab Enforcement and Regulatory Authority. "
        "You answer questions about PERA's regulations, structure, roles, operations, and "
        "policies STRICTLY from the Context passages provided below. You are serving a "
        "government audience — accuracy and honesty outweigh completeness.\n\n"

        "CORE RULES\n"
        "0) AUDIT TABLES — if the Context contains either a section titled "
        "'Per-Tehsil Breakdown (REQUIRED in answer)' OR a section beginning "
        "with 'Ranking — ' (e.g. 'Ranking — Tehsils by FIRs (Highest first)'), "
        "you MUST reproduce the FULL table verbatim — every row, every column, "
        "in the EXACT order given by the Context. Do NOT re-sort the rows. "
        "Do NOT skip rows. Do NOT collapse the table to a top-3/top-5 summary. "
        "Do NOT change column widths or units. The Context's row order is the "
        "ranking; reproducing rows out of order silently changes the answer. "
        "Render the table as a markdown table immediately after the headline "
        "answer. The headline answer must reference the row that appears at "
        "rank 1 in the Context, not a row you think looks more relevant.\n"
        "1) Answer using ONLY the provided Context. Do not use external knowledge.\n"
        "2) Do not invent or infer facts. No fabricated numbers, dates, names, pay scales, "
        "authorities, powers, or procedures. If a specific value is not stated in the Context, "
        "do not produce one.\n"
        "3) Do NOT infer a role's authority or responsibilities from seniority or job title "
        "alone unless explicitly stated in the Context.\n"
        "4) Treat 'powers', 'functions', and 'duties' as synonyms unless the Context "
        "distinguishes them.\n\n"

        "GROUNDED ANSWER BEHAVIOR (CRITICAL)\n"
        "5) If the Context directly contains the answer, state it clearly and concisely, "
        "using the exact terminology from the Context (e.g., 'SPPP-3', 'BPS-17', "
        "'Section 12', 'Schedule-III').\n"
        "6) If the Context PARTIALLY covers the question, answer the supported portion and "
        "CLEARLY state which specific aspect is NOT covered in the retrieved documents. "
        "Do not fill gaps with assumptions or plausible-sounding content.\n"
        "7) If the Context does NOT contain information that answers the question, say so "
        "honestly. Acceptable phrasing: 'The retrieved PERA documents do not contain "
        "information about X.' If closely related content is present, briefly summarize "
        "what IS available so the user can refine their question.\n"
        "8) SALARY/PAY LINKAGE — do NOT attach a pay scale (SPPP-X, BPS-XX, BS-X) to a role "
        "unless BOTH the role name AND the pay code appear together in the Context (either in "
        "the same evidence passage, or in evidence passages that explicitly reference each "
        "other by role or schedule). If a Schedule-III / SPPP salary table is present in the "
        "Context but you cannot confirm which row belongs to the queried role, say so "
        "explicitly instead of guessing.\n"
        "8a) POSITION BOUNDARY RULE — a 'Salary and Benefits: <code>' line belongs to the "
        "Position Title block that APPEARS IMMEDIATELY BEFORE IT in the same evidence passage. "
        "It does NOT belong to a Position Title block that appears AFTER it. If the queried "
        "role's Position Title is followed later in the chunk by a different role's Position "
        "Title, any 'Salary and Benefits:' line between those two Position Titles belongs to "
        "the FIRST (earlier) role only. Do NOT propagate one role's SPPP code to the role that "
        "follows it in the text. If the queried role's Position Title block has NO 'Salary "
        "and Benefits:' line before the next Position Title, say the salary is not specified "
        "in the retrieved evidence.\n"
        "8b) HEADCOUNT/STAFFING TABLE RULE — rows of the form "
        "'<role> / SPPP-N (count)' or '<role> – SPPP-N' in a staffing summary are "
        "AUTHORITATIVE role→pay mappings. If such a line names the queried role, prefer that "
        "SPPP code over any other SPPP code in the context.\n"
        "9) If the Context contains conflicting statements on the same fact, present both "
        "positions neutrally and identify that they conflict.\n"
        "10) If the question is outside PERA's scope (weather, sports, general trivia, other "
        "organizations), say so briefly and do not attempt to answer from the Context.\n\n"

        "COVERAGE (without speculation)\n"
        "11) When the Context contains a LIST of items (responsibilities, qualifications, "
        "conditions, steps) relevant to the question, include ALL items present in the "
        "Context. Do not summarize them away.\n"
        "12) For multi-part questions, address each part using the Context. If a part is not "
        "covered by the Context, state that specifically for that part and move on.\n"
        "13) Section headings in the Context (e.g., 'Purpose of the Position', 'Key "
        "Responsibilities', 'Qualification/Experience', 'Salary and Benefits', 'Appointment') "
        "should guide your answer structure when they are relevant to the question.\n\n"

        "REFERENCES — DO NOT INCLUDE IN ANSWER\n"
        "14) The UI shows references separately. Do NOT output Source/References/Page lists.\n"
        "15) Only mention a document or page if the user explicitly asks for it.\n\n"

        "STYLE\n"
        "16) Professional, composed, concise. Use Markdown formatting.\n"
        "17) Always answer in English, regardless of the language the user asked in.\n\n"

        "STRUCTURED OUTPUT — RANKED / LIST / COMPARE / TOP / BREAKDOWN\n"
        "18) When the user asks for a ranking, top-N, list, breakdown, "
        "comparison, summary across multiple entities, or any answer that "
        "naturally maps to rows × columns, render it as a Markdown table. "
        "Use right-aligned numerical columns. Keep header labels short "
        "(≤ 3 words). One column per fact — do not stuff multiple metrics "
        "into a single cell.\n"
        "19) For a single entity / single number answer, prefer a clear "
        "sentence with the number bolded — do not force a one-row table.\n\n"

        "DATA FRESHNESS (CRITICAL FOR DATABASE-DERIVED ANSWERS)\n"
        "20) When the Context contains a freshness stamp of the form "
        "'[FRESHNESS — <table>: <tier> · <date phrase>]', you MUST surface "
        "that freshness information in your answer. Mention the snapshot "
        "date and the tier (live / current / stale) so the reader knows "
        "how recent the figure is. Quoting board-relevant numbers without "
        "their freshness is unprofessional.\n"
        "21) Do NOT estimate ratios, percentages, averages, trends, "
        "growth rates, or date-range comparisons unless the exact figures "
        "appear in the Context. If the user asks for one and only the "
        "underlying totals are present, say so honestly: 'The exact "
        "<metric> is not pre-computed; the underlying values are X and Y.'\n"
        "22) If a SUMMARY total and a per-row BREAKDOWN come from "
        "different freshness stamps in the Context, surface that "
        "discrepancy in your answer rather than letting the reader assume "
        "the numbers reconcile.\n\n"

        "CONTEXT (do not quote or reproduce the XML tags below):\n"
        f"{context_str}"
    )

    # Phase 5: append a small, intent-tailored guidance block. Strictly
    # additive — does not relax any earlier rule. Helps the model match
    # the structure the user asked for (procedure → numbered steps,
    # comparison → side-by-side, list → exhaustive bullets, etc.).
    intent_primary = (intent.get("primary") or "") if intent else ""
    intent_block_lines: List[str] = []
    if intent_primary == "procedural":
        intent_block_lines.append(
            "INTENT: Procedural — present the answer as a numbered, ordered "
            "sequence of steps strictly drawn from the Context. If the Context "
            "does not specify the order, present steps grouped by source and "
            "say so. Do NOT invent intermediate steps."
        )
    elif intent_primary == "comparison":
        intent_block_lines.append(
            "INTENT: Comparison — produce a clear comparison of the entities "
            "the user asked about. Use a short Markdown table or paired "
            "bullets. For each row, list ONLY attributes that the Context "
            "actually states for BOTH entities. If an attribute is in the "
            "Context for only one entity, mark the other side as "
            "'not specified in retrieved documents'. Never infer parity."
        )
    elif intent_primary == "list":
        intent_block_lines.append(
            "INTENT: List/Exhaustive — enumerate every item that the Context "
            "explicitly contains for the asked topic. Do NOT summarize away "
            "items. If the Context appears truncated mid-list, say so."
        )
    elif intent_primary == "role_info":
        intent_block_lines.append(
            "INTENT: Role information — when the Context contains sections "
            "such as Purpose of the Position, Key Responsibilities, "
            "Qualification/Experience, Salary and Benefits, Appointment, "
            "Reporting — present each section that is present. Do NOT mix "
            "content from a different role; if the Context only contains "
            "another role, say so honestly."
        )
    elif intent_primary == "vague":
        intent_block_lines.append(
            "INTENT: Vague/broad — the user asked a broad question. Provide "
            "a grounded overview using the Context only. Briefly enumerate "
            "the relevant areas the Context covers. Do NOT speculate beyond "
            "what is retrieved."
        )

    if intent and intent.get("is_multi_part"):
        intent_block_lines.append(
            "MULTI-PART: The user asked about multiple aspects. Address each "
            "aspect separately under its own short heading. For any aspect "
            "the Context does not cover, say so explicitly for that aspect "
            "and continue with the next one."
        )

    if intent_block_lines:
        system_prompt = system_prompt + "\n\n" + "\n".join(intent_block_lines)

    # ── Source-mode-aware prompt block (preserved from current project) ──
    # The user can switch the chatbot between three data backings:
    #   • documents  — only the indexed PDFs
    #   • stored_api — only the snapshotted API records
    #   • both       — mixed retrieval over PDFs + API records (default)
    # Inject a small block telling the LLM which data source the Context
    # is drawn from so the answer's phrasing matches.
    _resolved_mode = answer_source_mode or "both"
    if _resolved_mode == "stored_api":
        system_prompt = system_prompt + "\n\n" + (
            "DATA SOURCE: STORED API DATA\n"
            "The Context below comes from stored API data snapshots (not documents). "
            "When listing entities (divisions, employees, finance rows, etc.), use "
            "structured lists rather than narrative paragraphs. If a value is missing, "
            "say 'not found in the stored API data' — do NOT say 'not found in PERA "
            "documents' because the data source is stored API data, not documents."
        )
    elif _resolved_mode == "documents":
        system_prompt = system_prompt + "\n\n" + (
            "DATA SOURCE: REGULATORY DOCUMENTS\n"
            "The Context below comes from indexed PERA regulatory documents (PDFs). "
            "Quote section, schedule, and rule numbers from the Context exactly as "
            "they appear. If a fact is missing, say so honestly using the documents' "
            "terminology — do not improvise structured-data style outputs."
        )
    else:  # "both" — also the default when no explicit mode is supplied
        system_prompt = system_prompt + "\n\n" + (
            "DATA SOURCE: MIXED (DOCUMENTS + STORED API DATA)\n"
            "The Context below may contain BOTH document-based evidence AND stored "
            "API data records. When both are relevant, combine them — cite the "
            "document for policy/rules and the API records for current values "
            "(division names, headcounts, finance figures, etc.)."
        )

    # 3. Construct Messages
    messages = [{"role": "system", "content": system_prompt}]

    if conversation_history:
        valid_history = [m for m in conversation_history if m.get("role") in ("user", "assistant")]
        messages.extend(valid_history[-4:])

    messages.append({"role": "user", "content": current_question})

    # 4. Call LLM
    try:
        try:
            response = client.chat.completions.create(
                model=ANSWER_MODEL,
                messages=messages,
                temperature=0.0,
            )
            # Successful roundtrip — clear any prior offline flag.
            mark_openai_available()
        except Exception as _e:
            # Flip system offline on quota exhaustion. Non-quota errors
            # are re-raised untouched and handled by the outer except.
            mark_openai_unavailable(_e)
            raise
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
                mark_openai_available()
                refined = refine_response.choices[0].message.content or ""
                # Only use refinement if it's meaningfully longer
                if len(refined.split()) > answer_word_count + 20:
                    log.info("Refinement expanded answer: %d → %d words",
                             answer_word_count, len(refined.split()))
                    answer_text = refined
                else:
                    log.info("Refinement did not expand answer — keeping original")
            except Exception as e:
                # Track quota signal but do not raise — the initial
                # answer is usable, refinement is best-effort.
                mark_openai_unavailable(e)
                log.warning("Refinement pass failed: %s — using initial answer", e)

        # Strip references ONLY if user did NOT explicitly ask for them
        if not _user_wants_references(current_question):
            answer_text = _strip_answer_references(answer_text)

        # ── 5pre. Numeric validator for structured retrievals ──
        # When the retrieval is tagged structured but the deterministic
        # bypass at the top of this function didn't fire (e.g. a stored
        # API non-ranking lookup), still validate that every number in
        # the LLM answer appears in the structured payload. On failure
        # we replace the answer with the deterministic payload (or the
        # structured context as last resort) so hallucinated numbers
        # cannot reach the user.
        _ev_type_post = retrieval.get("evidence_type") if retrieval else None
        _structured_payload = (
            retrieval.get("structured_payload") if retrieval else None
        )
        _numeric_validation_post: Optional[Dict[str, Any]] = None
        _numeric_replaced = False
        if _ev_type_post and _structured_payload:
            try:
                from numeric_validator import validate_numeric_answer
                vres = validate_numeric_answer(
                    answer_text, _structured_payload,
                    evidence_type=_ev_type_post,
                )
                _numeric_validation_post = {
                    "ok": vres.ok, "reason": vres.reason,
                    "unsupported_count": len(vres.unsupported_numbers),
                    "unsupported_sample": vres.unsupported_numbers[:5],
                }
                if not vres.ok:
                    log.warning(
                        "Numeric validator rejected LLM answer "
                        "(unsupported=%d). Replacing with structured payload.",
                        len(vres.unsupported_numbers),
                    )
                    fallback = (
                        retrieval.get("deterministic_answer")
                        or _structured_payload
                    )
                    answer_text = fallback
                    _numeric_replaced = True
            except Exception as _ne:
                log.debug("numeric_validator skipped: %s", _ne)

        # 5. Post-generation grounding verification.
        grounding = verify_grounding(
            answer_text=answer_text,
            evidence_list=evidence_list,
            context_str=context_str,
            question=current_question,
        )

        # 5b. Phase 4 entity-consistency check. Catches money amounts,
        # BPS/SPPP codes, and legal references that appear in the answer
        # but not in the evidence (e.g. fabricated salary numbers, made-up
        # section references). Findings are appended to the grounding
        # result's unsupported list and the score is reduced proportionally
        # so the support-state classifier reacts naturally.
        ec_unsupported = _entity_consistency_check(answer_text, context_str)
        if ec_unsupported:
            log.warning(
                "Entity-consistency: %d high-risk entities not found in evidence: %s",
                len(ec_unsupported), ec_unsupported[:5]
            )
            existing_norms = {
                re.sub(r"[\s,\-]+", "", c.lower())
                for c in (grounding.unsupported_claims or [])
            }
            for ent in ec_unsupported:
                if re.sub(r"[\s,\-]+", "", ent.lower()) not in existing_norms:
                    grounding.unsupported_claims.append(ent)
            # Cap at 8 to avoid log bloat.
            grounding.unsupported_claims = grounding.unsupported_claims[:8]
            # Apply the same smooth penalty schedule used in grounding.py
            # (10% per finding, capped at 35%) so behavior is consistent.
            penalty = min(0.35, 0.10 * len(ec_unsupported))
            new_score = round(max(0.0, grounding.score * (1.0 - penalty)), 3)
            log.info(
                "Entity-consistency penalty: score %.3f -> %.3f (-%d%%)",
                grounding.score, new_score, int(penalty * 100)
            )
            grounding.score = new_score
            # If the penalty pushes us under the grounded floor, mark
            # the result as not-grounded so downstream classification
            # treats it as unsupported.
            if grounding.score < 0.30:
                grounding.grounded = False
            grounding.notes.append(
                f"entity-consistency: {len(ec_unsupported)} unsupported entit(ies)"
            )

        # 6. Classify support state (for audit and wording — NOT for refusal)
        support_state = _classify_support_state(grounding, answer_text)
        log.info("Support state: %s (grounding score=%.3f, semantic=%s)",
                 support_state, grounding.score, grounding.semantic_support or "n/a")

        refs = extract_references_simple(
            retrieval, question=current_question, answer_text=answer_text
        )

        # 7. GROUNDING-AUTHORITATIVE DECISION (Phase 2 P0)
        #
        # Prior behavior: an "unsupported" classification was silently
        # downgraded to "partially_supported" and the LLM answer was always
        # shown. That turned grounding into pure audit metadata and was the
        # single largest hallucination vector in the system.
        #
        # New behavior for a government chatbot:
        #   • unsupported AND grounding.score < 0.15 → honest refusal;
        #     no speculative answer, references still surfaced so the user
        #     can verify what was retrieved.
        #   • unsupported AND grounding.score >= 0.15 → show the answer but
        #     prepend a visible caution banner; state is kept as
        #     "partially_supported" so downstream consumers render the
        #     partial-support note at the end.
        #   • EXCEPTION — if the top retrieved chunk lexically contains
        #     several of the user's query keywords, we trust retrieval
        #     even when the semantic judge says "none" (the judge is
        #     calibrated for formal regulatory prose and tends to
        #     under-score narrative admin-uploaded content). This stops
        #     cases like "any update on X" with X on the user's uploaded
        #     PDF being refused.
        #   • other states → unchanged (current wording applied).
        #
        # Rationale: for PERA, refusing when evidence does not support the
        # claim is safer than showing an authoritative-looking answer that
        # cannot be traced back to the documents. But a retrieval-strong
        # match is itself an evidence signal that overrides an overly
        # strict judge verdict.

        def _retrieval_bypass_unsupported() -> bool:
            """Return True when ONE single chunk in the top-3 retrieved
            results is strongly keyword-aligned with the user's question
            — strong enough that refusing would be user-hostile.

            Phase-1 tightening: require ratio >= 0.60 of *non-stopword*
            query tokens to appear in the SAME chunk (not spread across
            top-3). The previous behaviour distributed the matched-token
            count over up-to-3 chunks, which let weak retrieval bypass
            the judge for free.
            """
            try:
                all_ev = retrieval.get("evidence") or []
                stopwords = {
                    "what", "which", "where", "when", "does", "that",
                    "this", "with", "from", "about", "have", "been",
                    "will", "shall", "the", "for", "and", "how",
                    "give", "me", "tell", "any", "all", "some",
                    "update", "explain", "pera", "its",
                }
                q_tokens = [
                    w for w in re.findall(
                        r"[a-zA-Z]+", (current_question or "").lower()
                    )
                    if len(w) > 2 and w not in stopwords
                ]
                if len(q_tokens) < 3:
                    # too short — reject the bypass; fall through to
                    # the standard refusal path so we don't ship an
                    # ungrounded answer on a vague query.
                    return False
                threshold = max(2, int(round(0.60 * len(q_tokens))))
                scanned = 0
                for grp in all_ev:
                    for hit in grp.get("hits", [])[:2]:
                        if scanned >= 3:
                            break
                        text_lc = (hit.get("text") or "").lower()
                        matched_in_one_chunk = sum(
                            1 for t in q_tokens if t in text_lc
                        )
                        if matched_in_one_chunk >= threshold:
                            return True
                        scanned += 1
                    if scanned >= 3:
                        break
                return False
            except Exception:
                return False

        if support_state == "unsupported":
            if grounding.score < 0.15 and not _retrieval_bypass_unsupported():
                log.warning(
                    "Grounding UNSUPPORTED (score=%.3f, semantic=%s) — refusing instead of "
                    "showing an ungrounded answer",
                    grounding.score, grounding.semantic_support or "n/a"
                )
                doc_names_seen = []
                if refs:
                    for r in refs:
                        dn = (r.get("document") or "").strip()
                        if dn and dn not in doc_names_seen:
                            doc_names_seen.append(dn)
                        if len(doc_names_seen) >= 3:
                            break

                if doc_names_seen:
                    refusal_answer = (
                        "I couldn't find information in the retrieved PERA documents that "
                        "directly answers this question. Closely related content was found "
                        f"in: **{', '.join(doc_names_seen)}** — you can review those "
                        "references below, or please rephrase your question with a specific "
                        "role, schedule, or section name."
                    )
                else:
                    refusal_answer = (
                        "I couldn't find information in the retrieved PERA documents that "
                        "directly answers this question. Please rephrase your question or "
                        "ask about a specific role, schedule, or section."
                    )

                return {
                    "answer": refusal_answer,
                    "references": refs,
                    "decision": "refuse",
                    "support_state": "unsupported",
                    "grounding": {
                        "confidence": grounding.confidence,
                        "score": grounding.score,
                        "semantic_support": grounding.semantic_support,
                        "support_state": "unsupported",
                    },
                }
            else:
                # Weak-but-not-zero grounding — show with visible caution.
                # The caution is appended at the END of the answer (combined
                # with the partial-support note) so the user reads the
                # substantive answer first, then sees the caveats together
                # as one trailing note.
                log.info(
                    "Grounding unsupported but score=%.3f >= 0.15 — showing with caution banner",
                    grounding.score
                )
                support_state = "partially_supported"
                answer_text = answer_text + (
                    "\n\n---\n\n"
                    "⚠️ **Caution:** The retrieved PERA documents do not directly address "
                    "this question. The response above is inferred from related content — "
                    "please verify against the referenced documents."
                )

        final_answer = _apply_support_state_wording(
            answer_text, support_state,
            answer_source_mode=answer_source_mode or "both",
        )

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
        if _numeric_validation_post is not None:
            result["numeric_validation"] = _numeric_validation_post
        if _numeric_replaced:
            result["deterministic"] = True
            result["evidence_type"] = _ev_type_post

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
