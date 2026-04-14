from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple

from extractors import ExtractedUnit


# -----------------------------
# Chunk structure
# -----------------------------
@dataclass
class Chunk:
    doc_name: str
    doc_rank: int
    source_type: str         # "pdf" | "docx"
    loc_kind: str            # "page" | "section" | "paragraphs"
    loc_start: Any
    loc_end: Any
    chunk_text: str
    path: Optional[str] = None
    doc_authority: int = 2   # 1=low (working papers), 2=medium, 3=high (official)


# -----------------------------
# Config / Debug
# -----------------------------
from log_config import get_logger
_chunker_log = get_logger("pera.chunker")


# -----------------------------
# Utilities
# -----------------------------
_WS_RE = re.compile(r"[ \t]+")
_NUL_RE = re.compile(r"\x00+")

# Page marker regex — matches <<PAGE:N>> inserted by the PDF extractor
_PAGE_MARKER_RE = re.compile(r"<<PAGE:(\d+)>>")

def _clean_text(s: str) -> str:
    s = s or ""
    s = _NUL_RE.sub(" ", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = _WS_RE.sub(" ", s)
    s = re.sub(r"\n{5,}", "\n\n\n\n", s)
    s = "\n".join([ln.strip() for ln in s.split("\n")])
    return s.strip()


def _parse_book_rank(filename: str) -> int:
    base = os.path.splitext(os.path.basename(filename))[0]
    m = re.search(r"\bbook\s*([0-9]+)\b", base, flags=re.IGNORECASE)
    if not m:
        return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0


# -----------------------------
# Structural heuristics: tables/lists/headings
# -----------------------------
_BULLET_RE = re.compile(r"^\s*(?:[•\-\u2022]|\d+[\)\.]|[a-zA-Z][\)\.])\s+")
_PIPE_TABLE_RE = re.compile(r"\s\|\s")
_TAB_TABLE_RE = re.compile(r"\t+")

def _looks_like_table_line(line: str) -> bool:
    if not line:
        return False
    s = line.strip()
    if len(s) < 18:
        return False
    if _PIPE_TABLE_RE.search(s):
        return s.count("|") >= 2
    if _TAB_TABLE_RE.search(s):
        return True
    return False

def _looks_like_list_line(line: str) -> bool:
    if not line:
        return False
    return _BULLET_RE.search(line) is not None

def _looks_like_table_or_list(line: str) -> bool:
    return _looks_like_list_line(line) or _looks_like_table_line(line)


# -----------------------------
# Heading detection
# -----------------------------
_HEADING_RE = re.compile(
    r"^\s*(?:"
    r"(schedule|annex(?:ure)?|appendix|chapter|section|rule|regulation|part|table)\s*"
    r"([\-–—]?\s*[A-Za-z0-9IVXLC]+)?"
    r")\b"
    r".*$",
    re.IGNORECASE
)

def _is_all_caps_heading(s: str) -> bool:
    s = (s or "").strip()
    if len(s) < 4 or len(s) > 60:
        return False
    letters = re.sub(r"[^A-Za-z]+", "", s)
    if not (4 <= len(letters) <= 40):
        return False
    return letters.isupper()

def _is_heading(line: str) -> bool:
    if not line:
        return False
    s = line.strip()
    if _HEADING_RE.match(s):
        return True
    if _is_all_caps_heading(s):
        return True
    return False


# ─────────────────────────────────────────────────────────────
# Role heading detection (EXPANDED — pattern-based, not just list)
# ─────────────────────────────────────────────────────────────
_MAX_ROLE_LEN = 120  # increased from 80 for titles with wing/dept qualifiers

_ROLE_EXCLUDE_RE = re.compile(
    r"^\s*(report\s*to|reporting\s*to|reports\s*to|department|location|grade|scale|pay|"
    r"job\s*summary|summary|objective|purpose\s+of\s+the\s+position|education|qualification|experience|"
    r"responsibilit|duties|skills|competenc|note|remarks|wing|section)\s*[:\-]",
    re.IGNORECASE
)

# "Position Title:" "Job Title:" "Role:" "Designation:" prefix
_ROLE_PREFIX_RE = re.compile(
    r"^\s*(position\s*title|job\s*title|role|designation)\s*[:\-–]+\s*(.*)$",
    re.IGNORECASE
)

# Pattern-based role detection: matches common title structures
# Rather than enumerating every title, we match common patterns:
#   - X Director (General)
#   - X Officer
#   - X Manager (Y)
#   - Secretary to the Authority
#   - Chief X Officer
_ROLE_PATTERN_RE = re.compile(
    r"^\s*(?:"
    # "Position Title: - X" (already stripped by prefix handler)
    # Director variants
    r"(?:Additional\s+|Deputy\s+|Assistant\s+|Joint\s+|Regional\s+)?"
    r"Director(?:\s+General)?(?:\s*\([^)]+\))?"
    r"|"
    # Chief X Officer
    r"Chief\s+(?:\w+\s+){1,2}Officer"
    r"|"
    # X Officer variants
    r"(?:Senior\s+|Sub[\s\-]?Divisional\s+|Divisional\s+|Provisional\s+|Zonal\s+)?"
    r"(?:Enforcement|Investigation|Inspection|System\s+Support|Welfare|Research|Security|Intelligence|Liaison)\s+Officer"
    r"|"
    # Manager (X) or X Manager
    r"(?:Manager\s*\([^)]+\)|[\w\s&]+\s+Manager)"
    r"|"
    # Secretary variants
    r"(?:Secretary|Private\s+Secretary|Personal\s+Secretary)(?:\s+(?:to\s+the\s+)?(?:Authority|Board|DG|Director\s+General))?"
    r"|"
    # Developer / Administrator / Engineer / Specialist / Coordinator / Analyst
    r"(?:[\w\s&]+\s+(?:Developer|Administrator|Engineer|Specialist|Coordinator|Analyst|Supervisor|Technician|Trainer|Consultant))"
    r"|"
    # Chairman / Chairperson
    r"Chair(?:man|person)"
    r"|"
    # Competent Authority
    r"Competent\s+Authority"
    r")\s*$",
    re.IGNORECASE
)


def _is_role_heading(line: str) -> Optional[str]:
    """
    Detect if a line is a role heading.
    Returns the role title if detected, None otherwise.
    Uses pattern-based detection (not just hardcoded list).
    """
    if not line:
        return None
    s = line.strip()

    if not s or len(s) > _MAX_ROLE_LEN:
        return None

    # Exclude common metadata / section labels
    if _ROLE_EXCLUDE_RE.match(s):
        return None

    # Handle "Position Title: - Role Name" or "Designation: X"
    m = _ROLE_PREFIX_RE.match(s)
    if m:
        role_part = (m.group(2) or "").strip()
        role_part = re.sub(r"^[-•\:\s]+", "", role_part).strip()
        if not role_part:
            return None
        # The extracted role is the heading
        return role_part

    # Ensure it looks like a title (not a sentence)
    if sum(1 for ch in s if ch in ".;,") >= 2:
        return None

    # Pattern-based matching
    if _ROLE_PATTERN_RE.match(s):
        return s

    return None


# -----------------------------
# Block splitting with role context
# -----------------------------
def _split_into_blocks_with_context(text: str) -> List[Tuple[Optional[str], str]]:
    """
    Splits into blocks while tracking role headings.
    Returns list of (role_context, block_text) tuples.
    """
    t = _clean_text(text)
    if not t:
        return []

    lines = t.split("\n")
    blocks: List[Tuple[Optional[str], str]] = []
    buf: List[str] = []
    current_role: Optional[str] = None

    def flush():
        nonlocal buf
        if not buf:
            return
        b = _clean_text("\n".join(buf))
        if b:
            blocks.append((current_role, b))
        buf = []

    def last_is_structured() -> bool:
        if not buf:
            return False
        # Skip page markers when checking last structured status —
        # they shouldn't affect block-split decisions.
        for item in reversed(buf):
            if not _PAGE_MARKER_RE.match((item or "").strip()):
                return _looks_like_table_or_list(item)
        return False

    for ln in lines:
        raw = (ln or "").strip()

        if not raw:
            flush()
            continue

        # <<PAGE:N>> markers: keep in buffer but don't trigger any
        # block-splitting logic (heading, role, structured transitions).
        # They'll be parsed later for per-chunk page assignment.
        if _PAGE_MARKER_RE.match(raw):
            buf.append(raw)
            continue

        role_match = _is_role_heading(raw)
        if role_match:
            _chunker_log.debug("Role heading detected: %s", role_match)
            flush()
            current_role = role_match
            continue

        if _is_heading(raw):
            flush()
            buf.append(raw)
            continue

        structured = _looks_like_table_or_list(raw)
        if buf:
            prev_structured = last_is_structured()
            if structured != prev_structured:
                flush()

        buf.append(raw)

    flush()
    return blocks


def _trim_overlap_to_boundary(tail: str) -> str:
    s = (tail or "").strip()
    if not s:
        return ""
    m = re.search(r"[\s\.,;:\)\]\}!\?]", s)
    if m and m.start() < 20:
        s = s[m.start():].lstrip()
    return s.strip()


def _chunk_by_char_budget(blocks: List[str], max_chars: int, overlap_chars: int) -> List[str]:
    if not blocks:
        return []

    max_chars = max(500, int(max_chars or 0))
    overlap_chars = max(0, int(overlap_chars or 0))
    if overlap_chars >= max_chars:
        overlap_chars = max(0, max_chars // 5)

    chunks: List[str] = []
    buf: List[str] = []
    size = 0

    def flush():
        nonlocal buf, size
        if not buf:
            return
        chunk = _clean_text("\n\n".join(buf))
        if chunk:
            chunks.append(chunk)
        buf = []
        size = 0

    GLOBAL_MAX_CHUNKS = 20000

    for b in blocks:
        b = (b or "").strip()
        if not b:
            continue

        if len(b) > max_chars:
            flush()
            step = max(200, max_chars - overlap_chars)
            start = 0
            parts_made = 0
            max_parts = max(80, min(3000, int(len(b) / max_chars) + 10))

            while start < len(b) and parts_made < max_parts:
                end = min(len(b), start + max_chars)
                part = _clean_text(b[start:end])
                if part:
                    chunks.append(part)
                    if len(chunks) >= GLOBAL_MAX_CHUNKS:
                        return chunks
                parts_made += 1
                if end >= len(b):
                    break
                start += step
            continue

        if size + len(b) + 2 > max_chars and buf:
            flush()

        buf.append(b)
        size += len(b) + 2

        if len(chunks) >= GLOBAL_MAX_CHUNKS:
            flush()
            return chunks

    flush()

    # Overlap stitching
    if overlap_chars > 0 and len(chunks) > 1:
        out: List[str] = []
        cap = max(80, min(overlap_chars, 500))

        for i, c in enumerate(chunks):
            if i == 0:
                out.append(c)
                continue
            prev = chunks[i - 1]
            tail = _trim_overlap_to_boundary(prev[-cap:])
            if tail:
                out.append(_clean_text(tail + "\n\n" + c))
            else:
                out.append(_clean_text(c))
        return out

    return chunks


def _force_keep_chunk(ctext: str) -> bool:
    t = (ctext or "").lower()
    if "schedule" in t or "annex" in t or "annexure" in t or "appendix" in t:
        return True
    if "punjab enforcement and regulatory authority" in t or re.search(r"\bpera\b", t):
        if len(t) < 500:
            return True
    if "chief technology officer" in t or re.search(r"\bcto\b", t):
        return True
    if "terms of reference" in t or re.search(r"\btor\b", t):
        return True
    if "position title" in t or "designation" in t:
        return True
    return False


# Sub-document page numbers that survived extraction (e.g., from org-chart images)
_SUBDOC_PAGE_RE = re.compile(r"^\s*Page\s+\d+\s+of\s+\d+\s*$", re.IGNORECASE)


def _is_junk_chunk(text: str) -> bool:
    """
    Detect chunks that are mostly junk content — e.g., page number
    headers from embedded sub-documents ("Page 7 of 83", "Page 12 of 75").

    Returns True if >40% of non-empty lines are sub-document page numbers.
    """
    lines = [ln.strip() for ln in (text or "").split("\n") if ln.strip()]
    if not lines:
        return True
    junk = sum(1 for ln in lines if _SUBDOC_PAGE_RE.match(ln))
    return junk / len(lines) > 0.40


# ─────────────────────────────────────────────────────────────
# Page-marker resolution: assign accurate per-chunk page numbers
# ─────────────────────────────────────────────────────────────
def _resolve_chunk_page_markers(chunks: List["Chunk"]) -> List["Chunk"]:
    """
    Post-process PDF chunks to extract actual page numbers from
    embedded <<PAGE:N>> markers.

    The PDF extractor inserts <<PAGE:N>> markers into the text stream
    at every page boundary.  When a section spans many pages (e.g.
    Annex H pages 2-62), the chunker initially gives every chunk the
    section-level loc_start/loc_end.  This function fixes that by
    scanning each chunk's text for the markers it actually contains.

    Rules:
    - If a chunk contains markers → loc_start = first marker page,
      loc_end = last marker page.
    - If a chunk has NO markers → inherit the last page seen from the
      previous chunk in the *same document* (the content is a
      continuation of that page).
    - After resolving, all <<PAGE:N>> markers are stripped from the
      chunk text.
    """
    # Track last-seen page per document, so chunks from different
    # documents don't pollute each other.
    last_page_by_doc: Dict[str, int] = {}

    for chunk in chunks:
        # Only process PDF chunks with page-based location
        if chunk.source_type != "pdf" or chunk.loc_kind != "page":
            continue

        # Match both full <<PAGE:N>> and partial <<PAGE:N (without >>)
        markers = re.findall(r"<<PAGE:(\d+)>{0,2}", chunk.chunk_text)

        if markers:
            pages = [int(p) for p in markers]
            chunk.loc_start = pages[0]
            chunk.loc_end = pages[-1]
            last_page_by_doc[chunk.doc_name] = pages[-1]
        else:
            # No markers — this chunk is a continuation of the
            # previous page (e.g. a mid-page split)
            last_page = last_page_by_doc.get(chunk.doc_name)
            if last_page is not None:
                chunk.loc_start = last_page
                chunk.loc_end = last_page
            # else: keep original unit-level loc_start/loc_end

        # Strip all <<PAGE:N>> markers from the text.
        # Also strip partial markers (e.g. "<<PAGE:71" without ">>")
        # caused by character-budget splitting cutting through a marker.
        cleaned = _PAGE_MARKER_RE.sub("", chunk.chunk_text)
        cleaned = re.sub(r"<<PAGE:\d+(?:>{0,2})", "", cleaned)   # partials
        cleaned = re.sub(r">{1,2}(?=\s|\n|$)", "", cleaned)      # orphaned >>
        # Collapse runs of blank lines left by marker removal
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        if cleaned:
            chunk.chunk_text = cleaned

    return chunks


# -----------------------------
# Main chunking API
# -----------------------------
def chunk_units(
    units: List[ExtractedUnit],
    max_chars: int = 4500,
    overlap_chars: int = 500,    # increased from 350 for better cross-boundary context
    min_chunk_chars: int = 150   # lowered from 200 to preserve short but important sections
) -> List[Chunk]:
    """
    Converts extracted units into chunks while preserving traceability.

    Units can now span multiple pages (from stream-based extraction).
    Chunks inherit page ranges from their parent unit.

    Guarantees:
      - Each chunk stays within one ExtractedUnit (never mixes units)
      - Multi-page provenance is preserved via loc_start/loc_end
      - Role context is attached to relevant chunks
      - Short but high-value chunks are kept
    """
    out: List[Chunk] = []

    for u in units:
        txt = _clean_text(getattr(u, "text", "") or "")
        if not txt:
            continue

        rank = getattr(u, "doc_rank", 0) or _parse_book_rank(getattr(u, "doc_name", ""))
        authority = getattr(u, "doc_authority", 2)

        # If unit itself is short, keep it as one chunk
        if len(txt) < min_chunk_chars:
            if not _force_keep_chunk(txt) and len(txt) < 80:
                continue  # too short and not important
            out.append(
                Chunk(
                    doc_name=u.doc_name,
                    doc_rank=rank,
                    source_type=u.source_type,
                    loc_kind=u.loc_kind,
                    loc_start=u.loc_start,
                    loc_end=u.loc_end,
                    chunk_text=txt,
                    path=getattr(u, "path", None),
                    doc_authority=authority,
                )
            )
            continue

        # Split into (role_context, block_text)
        blocks_with_ctx = _split_into_blocks_with_context(txt)
        if not blocks_with_ctx:
            continue

        # Group blocks by role context to prevent role bleeding
        grouped: List[Tuple[Optional[str], List[str]]] = []
        current_ctx = blocks_with_ctx[0][0]
        current_texts: List[str] = []

        for ctx, btxt in blocks_with_ctx:
            if ctx != current_ctx:
                if current_texts:
                    grouped.append((current_ctx, current_texts))
                current_ctx = ctx
                current_texts = []
            current_texts.append(btxt)

        if current_texts:
            grouped.append((current_ctx, current_texts))

        # Chunk each group independently
        for ctx, texts in grouped:
            chunk_texts = _chunk_by_char_budget(texts, max_chars=max_chars, overlap_chars=overlap_chars)
            if not chunk_texts and len(texts) == 1:
                chunk_texts = texts

            for i, ctext in enumerate(chunk_texts):
                ctext = _clean_text(ctext)
                if not ctext:
                    continue

                # short chunk handling
                if len(ctext) < min_chunk_chars:
                    if len(chunk_texts) == 1:
                        pass
                    elif _force_keep_chunk(ctext):
                        pass
                    elif i == len(chunk_texts) - 1:
                        if len(re.findall(r"[A-Za-z\u0600-\u06FF]{3,}", ctext)) >= 8:
                            pass
                        else:
                            continue
                    else:
                        continue

                # Role context injection
                final_text = ctext
                if ctx and f"[role:" not in ctext.lower():
                    final_text = f"[Role: {ctx}]\n{ctext}"

                out.append(
                    Chunk(
                        doc_name=u.doc_name,
                        doc_rank=rank,
                        source_type=u.source_type,
                        loc_kind=u.loc_kind,
                        loc_start=u.loc_start,
                        loc_end=u.loc_end,
                        chunk_text=final_text,
                        path=getattr(u, "path", None),
                        doc_authority=authority,
                    )
                )

    # ── Resolve per-chunk page numbers from <<PAGE:N>> markers ──
    out = _resolve_chunk_page_markers(out)

    # ── Drop junk chunks (mostly sub-document page numbers) ──
    pre_junk = len(out)
    out = [c for c in out if not _is_junk_chunk(c.chunk_text)]
    junk_dropped = pre_junk - len(out)
    if junk_dropped:
        _chunker_log.info("Dropped %d junk chunks (sub-document page numbers)", junk_dropped)

    _chunker_log.info(
        "Chunking complete: %d units -> %d chunks (role-tagged: %d, multi-page: %d)",
        len(units),
        len(out),
        sum(1 for c in out if "[Role:" in c.chunk_text),
        sum(1 for c in out if c.loc_start != c.loc_end),
    )
    return out
