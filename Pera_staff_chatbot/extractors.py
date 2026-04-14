from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple

# -----------------------------
# Data structures
# -----------------------------
@dataclass
class ExtractedUnit:
    """
    A traceable extraction unit that can later be chunked.
    - PDF  => one unit per *logical section* (may span multiple pages)
    - DOCX => one unit per section (heading) or paragraph-range block
    """
    doc_name: str
    source_type: str              # "pdf" | "docx"
    loc_kind: str                 # "page" | "section" | "paragraphs"
    loc_start: Any                # int page number, or str anchor
    loc_end: Any                  # int page number, or str anchor
    text: str

    # optional metadata
    path: Optional[str] = None
    doc_rank: int = 0
    doc_authority: int = 2        # 1=low (working papers), 2=medium, 3=high (official acts)


# -----------------------------
# Helpers
# -----------------------------
SUPPORTED_EXTS = (".pdf", ".docx")

from log_config import get_logger
log = get_logger("pera.extractors")

_NUL_RE = re.compile(r"\x00+")
_PAGE_NUM_RE = re.compile(r"^\s*(?:page\s*)?\d+\s*(?:of\s*\d+)?\s*$", re.I)
_MULTI_NEWLINES_RE = re.compile(r"\n{5,}")

# Bullet patterns
_BULLET_RE = re.compile(r"^\s*(?:[•\-\u2022]|\d+[\)\.]|[A-Za-z][\)\.])\s+")

# Detect tabular alignment (raw, before whitespace collapsing)
_TABLE_LIKE_RE = re.compile(r"(\t+|\s{3,})")

# Hyphenation: join "regula-" + "tory" (very common in PDFs)
_HYPHEN_END_RE = re.compile(r"[\w\u0600-\u06FF]-$", re.UNICODE)

# Token density helpers
_WORD_RE = re.compile(r"[A-Za-z\u0600-\u06FF]{2,}", re.UNICODE)

# ── Section boundary detection patterns ──────────────────────
# Headings: chapter, schedule, annex, part headings
# NOTE: These patterns use \s*$ to match standalone heading lines.
_SECTION_HEADING_RE = re.compile(
    r"^\s*(?:"
    r"(?:CHAPTER|PART|SCHEDULE|ANNEX|FLAG|SECTION)\s*[-–—:\s]*[A-Z0-9IVXLC]+"
    r"|SCHEDULE\s*[-–—:]\s*"
    r"|CONTENTS\b"
    r"|TABLE\s+OF\s+CONTENTS"
    r"|GOVERNMENT\s+OF\s+THE\s+PUNJAB"
    r")\s*$",
    re.IGNORECASE | re.MULTILINE,
)

# PREFIX-based section heading: matches lines that START with structural
# headings even when they have trailing content on the same line.
# Real PDFs often have "Schedule-II Terms and Reference for..." on one line.
_SECTION_HEADING_PREFIX_RE = re.compile(
    r"^\s*(?:"
    r"(?:CHAPTER|PART|SCHEDULE|ANNEX|FLAG|SECTION)\s*[-–—:\s]*[A-Z0-9IVXLC]+"
    r")\s",
    re.IGNORECASE,
)

# Position / Role title block start (robust pattern-based)
# NOTE: \s*$ requires the ENTIRE line to match — standalone role headings only.
_ROLE_TITLE_RE = re.compile(
    r"^\s*(?:"
    r"Position\s+Title\s*[:\-–]"
    r"|Designation\s*[:\-–]"
    r"|(?:Director|Additional\s+Director|Deputy\s+Director|Assistant\s+Director)"
    r"\s+(?:General\b)?"
    r"|(?:Chief\s+(?:Technology|Executive|Operating|Financial|Security)\s+Officer)"
    r"|(?:(?:Senior\s+|Sub[\s\-]?Divisional\s+)?Enforcement\s+Officer)"
    r"|(?:(?:System\s+Support|Investigation|Welfare|Research)\s+Officer)"
    r"|(?:(?:HR|IT|Project|Operations|Finance|Training|Communication)\s+Manager)"
    r"|(?:Manager\s*\([^)]+\))"
    r"|(?:Secretary\s+(?:to\s+the\s+)?(?:Authority|Board|DG))"
    r")\s*$",
    re.IGNORECASE | re.MULTILINE,
)

# PREFIX-based role boundary: matches "Position Title:" even when the role
# name and "Report To:" are on the same line (common in PERA PDFs).
_ROLE_TITLE_PREFIX_RE = re.compile(
    r"^\s*(?:"
    r"Position\s+Title\s*[:\-–]"
    r"|Designation\s*[:\-–]"
    r")",
    re.IGNORECASE,
)

# Sub-document page numbers embedded in text (from org-chart images, etc.)
# e.g., "Page 7 of 83", "Page 12 of 75"
_SUBDOC_PAGE_NUM_RE = re.compile(
    r"^\s*Page\s+\d+\s+of\s+\d+\s*$",
    re.IGNORECASE,
)

# Numbered regulation / rule patterns
_REGULATION_NUM_RE = re.compile(
    r"^\s*(\d{1,3})\.\s+[A-Z]",  # e.g. "12. Retirement. -"
)

# Major structural break: new document section
_MAJOR_BREAK_RE = re.compile(
    r"(?:GOVERNMENT\s+OF\s+THE\s+PUNJAB.*?(?:ENFORCEMENT|REGULATORY).*?AUTHORITY"
    r"|NOTIFICATION\b"
    r"|^\s*SCHEDULE\s*[-–—:]\s*(?:I|II|III|IV|V|VI)"
    r")",
    re.IGNORECASE | re.DOTALL,
)


def _clean_text_general(s: str) -> str:
    """
    General cleaning after structure decisions are already made.
    IMPORTANT: we do NOT collapse multiple spaces here globally
    because tables may have been converted into " | " already.
    """
    s = s or ""
    s = _NUL_RE.sub(" ", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = _MULTI_NEWLINES_RE.sub("\n\n\n\n", s)
    # Trim each line but preserve line boundaries
    s = "\n".join([ln.strip() for ln in s.split("\n")])
    return s.strip()


def _is_heading_style(style_name: str) -> bool:
    if not style_name:
        return False
    sn = style_name.strip().lower()
    return sn.startswith("heading")


def discover_documents(data_dir: str = "assets/data") -> List[str]:
    data_dir = data_dir.replace("\\", "/")
    if not os.path.isdir(data_dir):
        return []
    out: List[str] = []
    for name in os.listdir(data_dir):
        p = os.path.join(data_dir, name).replace("\\", "/")
        if not os.path.isfile(p):
            continue
        low = name.lower()
        if low.endswith(SUPPORTED_EXTS):
            out.append(p)
    return sorted(out)


# -----------------------------
# PDF extraction quality helpers
# -----------------------------
def _pdf_lines_raw(text: str) -> List[str]:
    """
    Split PDF raw text into lines WITHOUT collapsing multiple spaces.
    This is critical for table detection.
    """
    t = text or ""
    t = _NUL_RE.sub(" ", t)
    t = t.replace("\r\n", "\n").replace("\r", "\n")

    lines = [ln.strip("\n") for ln in t.split("\n")]
    lines = [ln.strip() for ln in lines if ln and ln.strip()]
    return lines


def _normalize_line_for_header_footer(line: str) -> str:
    l = (line or "").strip().lower()
    l = re.sub(r"\d+", "0", l)
    l = re.sub(r"\s+", " ", l).strip()
    return l


def _word_count(s: str) -> int:
    return len(_WORD_RE.findall(s or ""))


def _is_header_footer_candidate(line: str) -> bool:
    s = (line or "").strip()
    if not s:
        return False
    if len(s) > 130:
        return False
    if _PAGE_NUM_RE.match(s):
        return True
    if _word_count(s) <= 3:
        return True
    sl = s.lower()
    if ("punjab" in sl and ("authority" in sl or "regulatory" in sl or "enforcement" in sl)) and len(s) <= 90:
        return True
    return False


def _detect_repeated_header_footer(page_lines: List[List[str]], min_pages: int = 3) -> Dict[str, set]:
    if len(page_lines) < min_pages:
        return {"header": set(), "footer": set()}

    first_counts: Dict[str, int] = {}
    last_counts: Dict[str, int] = {}

    eligible_pages = 0
    for lines in page_lines:
        if not lines:
            continue
        eligible_pages += 1
        for ln in lines[:2]:
            if not _is_header_footer_candidate(ln):
                continue
            k = _normalize_line_for_header_footer(ln)
            if k:
                first_counts[k] = first_counts.get(k, 0) + 1
        for ln in lines[-2:]:
            if not _is_header_footer_candidate(ln):
                continue
            k = _normalize_line_for_header_footer(ln)
            if k:
                last_counts[k] = last_counts.get(k, 0) + 1

    if eligible_pages < min_pages:
        return {"header": set(), "footer": set()}

    threshold = max(2, int(0.70 * eligible_pages))
    header = {k for k, c in first_counts.items() if c >= threshold}
    footer = {k for k, c in last_counts.items() if c >= threshold}
    return {"header": header, "footer": footer}


def _strip_headers_footers(lines: List[str], hf: Dict[str, set]) -> List[str]:
    if not lines:
        return lines
    out: List[str] = []
    for ln in lines:
        s = (ln or "").strip()
        if not s:
            continue
        if _PAGE_NUM_RE.match(s):
            continue
        norm = _normalize_line_for_header_footer(s)
        if norm in hf.get("header", set()) or norm in hf.get("footer", set()):
            continue
        out.append(s)
    return out


def _looks_like_table_row(raw_line: str) -> bool:
    s = (raw_line or "").rstrip()
    if len(s) < 20:
        return False
    if _TABLE_LIKE_RE.search(s) is None:
        return False
    col_gaps = len(re.findall(r"\s{3,}", s)) + (1 if "\t" in s else 0)
    if col_gaps < 1:
        return False
    tokens = re.findall(r"[A-Za-z\u0600-\u06FF0-9]{2,}", s)
    if len(tokens) < 3:
        return False
    if len(s) > 180 and col_gaps == 1:
        return False
    return True


def _normalize_table_row(raw_line: str) -> str:
    s = (raw_line or "").strip()
    s = re.sub(r"\t+", "  ", s)
    s = re.sub(r"\s{3,}", " | ", s).strip()
    s = re.sub(r"(?:\s\|\s){2,}", " | ", s)
    return s


def _join_pdf_lines(lines: List[str]) -> str:
    """
    Join PDF-extracted lines into cleaner text:
    - keep bullets / table-like rows as new lines
    - merge narrative lines into paragraphs
    - fix hyphenated line breaks
    """
    if not lines:
        return ""

    merged: List[str] = []
    buf: str = ""  # current paragraph buffer

    def flush_paragraph() -> None:
        nonlocal buf
        if buf.strip():
            merged.append(buf.strip())
        buf = ""

    for ln in lines:
        raw = (ln or "").strip()
        if not raw:
            flush_paragraph()
            continue

        # Filter sub-document page numbers (e.g., "Page 7 of 83" from
        # org-chart images). These are junk, not real content.
        if _SUBDOC_PAGE_NUM_RE.match(raw):
            continue

        if _BULLET_RE.search(raw):
            flush_paragraph()
            merged.append(raw)
            continue

        if _looks_like_table_row(raw):
            flush_paragraph()
            merged.append(_normalize_table_row(raw))
            continue

        if buf and _HYPHEN_END_RE.search(buf):
            buf = buf[:-1] + raw
            continue

        if buf.endswith(":"):
            flush_paragraph()
            buf = raw
            continue

        if not buf:
            buf = raw
        else:
            buf = buf + " " + raw

    flush_paragraph()
    text = "\n".join(merged)
    return _clean_text_general(text)


# ─────────────────────────────────────────────────────────────
# Table-aware extraction with pdfplumber (selective)
# ─────────────────────────────────────────────────────────────
_PDFPLUMBER_AVAILABLE = False
try:
    import pdfplumber
    _PDFPLUMBER_AVAILABLE = True
except ImportError:
    pass


def _page_has_tables(pdf_path: str, page_idx: int) -> bool:
    """Quick check if a specific page has extractable tables via pdfplumber."""
    if not _PDFPLUMBER_AVAILABLE:
        return False
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_idx < len(pdf.pages):
                tables = pdf.pages[page_idx].find_tables()
                return len(tables) > 0
    except Exception:
        pass
    return False


def _extract_tables_pdfplumber(pdf_path: str, page_idx: int) -> str:
    """
    Extract tables from a specific page using pdfplumber.
    Returns formatted table text with | delimiters preserving row/column meaning.
    """
    if not _PDFPLUMBER_AVAILABLE:
        return ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_idx >= len(pdf.pages):
                return ""
            page = pdf.pages[page_idx]
            tables = page.extract_tables()
            if not tables:
                return ""

            parts = []
            for table in tables:
                for row in table:
                    if row:
                        cells = [str(c or "").strip().replace("\n", " ") for c in row]
                        if any(cells):
                            parts.append(" | ".join(cells))
            return "\n".join(parts)
    except Exception as e:
        log.debug("pdfplumber table extraction failed for page %d: %s", page_idx + 1, e)
        return ""


def _is_table_heavy_page(lines: List[str]) -> bool:
    """Heuristic: a page is table-heavy if >40% of its lines look like table rows."""
    if len(lines) < 3:
        return False
    table_lines = sum(1 for ln in lines if _looks_like_table_row(ln))
    return table_lines / len(lines) > 0.40


# ─────────────────────────────────────────────────────────────
# Section-boundary detection for stream segmentation
# ─────────────────────────────────────────────────────────────

_PAGE_MARKER_RE = re.compile(r"<<PAGE:(\d+)>>")

def _is_section_boundary(line: str) -> bool:
    """
    Returns True if this line represents a hard section boundary
    where we should split into a new ExtractedUnit.

    Uses both strict (full-line) and prefix-based patterns to handle
    real PDF headings where trailing content appears on the same line
    (e.g., "Position Title: - CTO Report To: - DG Wing: - IT").
    """
    s = (line or "").strip()
    if not s:
        return False

    # Major structural headings (standalone)
    if _SECTION_HEADING_RE.match(s):
        return True

    # Major structural headings (prefix — trailing content on same line)
    # e.g., "Schedule-II Terms and Reference for Contractual Employment"
    if _SECTION_HEADING_PREFIX_RE.match(s) and len(s) > 12:
        return True

    # Role/position title blocks (standalone)
    if _ROLE_TITLE_RE.match(s):
        return True

    # Position Title / Designation prefix (with trailing content)
    # e.g., "Position Title: - Chief Technology Officer Report To: ..."
    if _ROLE_TITLE_PREFIX_RE.match(s) and len(s) > 16:
        return True

    # Numbered regulation start (e.g. "12. Retirement. -")
    if _REGULATION_NUM_RE.match(s) and len(s) > 10:
        return True

    # "GOVERNMENT OF THE PUNJAB" block = new section
    if re.match(r"^\s*GOVERNMENT\s+OF\s+THE\s+PUNJAB\b", s, re.IGNORECASE):
        return True

    return False


# ─────────────────────────────────────────────────────────────
# NEW: Stream-based PDF extraction (replaces page-isolated extraction)
# ─────────────────────────────────────────────────────────────
def extract_pdf_units(pdf_path: str) -> List[ExtractedUnit]:
    """
    Extract PDF as a continuous document stream, then segment by logical
    section boundaries. Units can span multiple pages.

    Strategy:
    1. Extract all pages' text, clean each page, concatenate with <<PAGE:N>> markers
    2. Detect section boundaries (headings, role titles, regulation numbers, etc.)
    3. Split stream at section boundaries
    4. Each unit carries page_start/page_end range from the markers it spans
    """
    units: List[ExtractedUnit] = []
    pdf_path = (pdf_path or "").replace("\\", "/")
    doc_name = os.path.basename(pdf_path)

    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
    except Exception as e:
        log.error("Cannot read PDF %s: %s", pdf_path, e)
        return units

    n_pages = len(reader.pages)
    if n_pages == 0:
        return units

    # Step 1: Extract per-page lines with header/footer detection
    raw_lines_by_page: List[List[str]] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        raw_lines_by_page.append(_pdf_lines_raw(text))

    hf = _detect_repeated_header_footer(raw_lines_by_page)

    # Step 2: Build stream with page markers
    stream_lines: List[str] = []
    for i, lines in enumerate(raw_lines_by_page):
        page_no = i + 1
        lines2 = _strip_headers_footers(lines, hf)
        if len(lines2) < max(4, int(0.30 * len(lines))):
            lines2 = lines

        # For table-heavy pages, try pdfplumber extraction
        table_text = ""
        if _is_table_heavy_page(lines2) and _PDFPLUMBER_AVAILABLE:
            table_text = _extract_tables_pdfplumber(pdf_path, i)

        # Insert page marker
        stream_lines.append(f"<<PAGE:{page_no}>>")

        if table_text:
            # Use pdfplumber table output, plus any non-table narrative from pypdf
            narrative_lines = [ln for ln in lines2 if not _looks_like_table_row(ln)]
            if narrative_lines:
                joined_narrative = _join_pdf_lines(narrative_lines)
                if joined_narrative.strip():
                    stream_lines.append(joined_narrative)
            stream_lines.append(table_text)
        else:
            # Standard pypdf extraction with line joining
            joined = _join_pdf_lines(lines2)
            if joined.strip():
                stream_lines.append(joined)

    # Step 3: Join into a single stream and segment by section boundaries
    full_stream = "\n".join(stream_lines)
    full_stream = _clean_text_general(full_stream)

    # Split stream into lines for section detection
    all_lines = full_stream.split("\n")

    # Step 3b: Split lines that contain "Position Title:" mid-line.
    # PDF text extraction often joins heading + body into one line, e.g.:
    # "Schedule-II Terms and Reference for Contractual Employment Position Title: - CTO Report To: - DG"
    # We split these so "Position Title:" starts its own line and triggers
    # section boundary detection.
    _PT_MID_LINE = re.compile(
        r"(?<=\S)\s+(Position\s+Title\s*[:\-–])", re.IGNORECASE
    )
    expanded_lines: List[str] = []
    for line in all_lines:
        m = _PT_MID_LINE.search(line)
        if m and m.start() > 15:
            before = line[:m.start()].strip()
            after = line[m.start():].strip()
            if before:
                expanded_lines.append(before)
            expanded_lines.append(after)
        else:
            expanded_lines.append(line)
    all_lines = expanded_lines

    # Step 4: Segment by section boundaries
    sections: List[Tuple[int, int, str]] = []  # (page_start, page_end, text)
    current_section_lines: List[str] = []
    current_page_start: int = 1
    current_page_end: int = 1
    last_seen_page: int = 1

    def flush_section() -> None:
        nonlocal current_section_lines, current_page_start, current_page_end
        if not current_section_lines:
            return
        # KEEP <<PAGE:N>> markers in text so the chunker can assign
        # accurate per-chunk page numbers (instead of inheriting the
        # section-level loc_start for every chunk).
        text = "\n".join(current_section_lines).strip()
        text = _clean_text_general(text)
        # Check minimum content EXCLUDING markers
        content_only = _PAGE_MARKER_RE.sub("", text).strip()
        if content_only and len(content_only) > 50:  # minimum viable content
            sections.append((current_page_start, current_page_end, text))
        current_section_lines = []

    # Max pages per section — prevents mega-sections that span dozens
    # of pages (e.g., Schedule sections with embedded org-chart images).
    # When exceeded, the section is force-flushed at the page boundary.
    MAX_SECTION_PAGES = 8

    for line in all_lines:
        # Track page markers
        pm = _PAGE_MARKER_RE.match(line)
        if pm:
            last_seen_page = int(pm.group(1))
            if not current_section_lines:
                current_page_start = last_seen_page
            current_page_end = last_seen_page

            # Force-flush if section has grown beyond MAX_SECTION_PAGES.
            # Use a lower text threshold (30 chars) than normal section splits
            # because image-heavy pages (org charts) may produce very little text.
            page_span = current_page_end - current_page_start
            if page_span >= MAX_SECTION_PAGES and current_section_lines:
                text_so_far = "\n".join(
                    ln for ln in current_section_lines
                    if not _PAGE_MARKER_RE.match(ln)
                ).strip()
                if len(text_so_far) > 30:
                    flush_section()
                    current_page_start = last_seen_page
                    current_page_end = last_seen_page

            current_section_lines.append(line)
            continue

        # Check for section boundary
        if _is_section_boundary(line) and current_section_lines:
            # Only split if current section has meaningful content
            text_so_far = "\n".join(
                ln for ln in current_section_lines if not _PAGE_MARKER_RE.match(ln)
            ).strip()
            # "Position Title:" is always a hard boundary (each TOR is its
            # own section). Use a lower content threshold so that even a
            # short preceding section header (like "Schedule-II Terms and
            # Reference") gets flushed before the new TOR starts.
            s_stripped = (line or "").strip()
            is_hard_boundary = bool(
                _ROLE_TITLE_PREFIX_RE.match(s_stripped)
            )
            min_content = 20 if is_hard_boundary else 100
            if len(text_so_far) > min_content:
                flush_section()
                current_page_start = last_seen_page
                current_page_end = last_seen_page

        current_section_lines.append(line)
        current_page_end = last_seen_page

    flush_section()

    # Step 5: Build ExtractedUnits from sections
    for page_start, page_end, text in sections:
        units.append(
            ExtractedUnit(
                doc_name=doc_name,
                source_type="pdf",
                loc_kind="page",
                loc_start=page_start,
                loc_end=page_end,
                text=text,
                path=pdf_path,
            )
        )

    log.info(
        "PDF stream extraction: %s -> %d sections from %d pages (multi-page: %d)",
        doc_name,
        len(units),
        n_pages,
        sum(1 for u in units if u.loc_start != u.loc_end),
    )
    return units


# -----------------------------
# DOCX Extraction (preserved)
# -----------------------------
def extract_docx_units(
    docx_path: str,
    min_chars_per_unit: int = 800,
    max_chars_per_unit: int = 6000
) -> List[ExtractedUnit]:
    units: List[ExtractedUnit] = []
    docx_path = (docx_path or "").replace("\\", "/")
    doc_name = os.path.basename(docx_path)

    try:
        from docx import Document
        doc = Document(docx_path)
    except Exception:
        return units

    paras: List[Dict[str, Any]] = []
    para_idx = 0
    current_heading = ""

    for p in doc.paragraphs:
        txt = (p.text or "").strip()
        if not txt:
            continue

        style_name = ""
        try:
            style_name = p.style.name if p.style else ""
        except Exception:
            style_name = ""

        if _is_heading_style(style_name):
            current_heading = txt
            continue

        para_idx += 1
        paras.append({
            "i": para_idx,
            "heading": current_heading,
            "text": txt
        })

    if not paras:
        return units

    has_any_heading = any(p["heading"] for p in paras)

    if has_any_heading:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for p in paras:
            h = p["heading"] or "Untitled"
            groups.setdefault(h, []).append(p)

        for heading, items in groups.items():
            _emit_docx_group_as_units(
                units=units,
                doc_name=doc_name,
                docx_path=docx_path,
                heading=heading,
                items=items,
                min_chars=min_chars_per_unit,
                max_chars=max_chars_per_unit
            )
        return units

    _emit_docx_paragraph_blocks(
        units=units,
        doc_name=doc_name,
        docx_path=docx_path,
        items=paras,
        min_chars=min_chars_per_unit,
        max_chars=max_chars_per_unit
    )
    return units


def _emit_docx_group_as_units(
    units: List[ExtractedUnit],
    doc_name: str,
    docx_path: str,
    heading: str,
    items: List[Dict[str, Any]],
    min_chars: int,
    max_chars: int
) -> None:
    buffer: List[str] = []
    start_i: Optional[int] = None
    end_i: Optional[int] = None
    char_count = 0

    def flush() -> None:
        nonlocal buffer, start_i, end_i, char_count
        if not buffer:
            return
        text = _clean_text_general("\n".join(buffer))
        if text and len(text) >= min_chars:
            anchor = f'Section: "{heading}" (Paragraphs {start_i}–{end_i})'
            units.append(
                ExtractedUnit(
                    doc_name=doc_name,
                    source_type="docx",
                    loc_kind="section",
                    loc_start=anchor,
                    loc_end=anchor,
                    text=text,
                    path=docx_path,
                )
            )
        buffer = []
        start_i = None
        end_i = None
        char_count = 0

    for p in items:
        txt = p["text"]
        i = p["i"]
        if start_i is None:
            start_i = i
        end_i = i
        buffer.append(txt)
        char_count += len(txt) + 1
        if char_count >= max_chars:
            flush()

    flush()


def _emit_docx_paragraph_blocks(
    units: List[ExtractedUnit],
    doc_name: str,
    docx_path: str,
    items: List[Dict[str, Any]],
    min_chars: int,
    max_chars: int
) -> None:
    buffer: List[str] = []
    start_i: Optional[int] = None
    end_i: Optional[int] = None
    char_count = 0

    def flush() -> None:
        nonlocal buffer, start_i, end_i, char_count
        if not buffer:
            return
        text = _clean_text_general("\n".join(buffer))
        if text and len(text) >= min_chars:
            anchor = f"Paragraphs {start_i}–{end_i}"
            units.append(
                ExtractedUnit(
                    doc_name=doc_name,
                    source_type="docx",
                    loc_kind="paragraphs",
                    loc_start=anchor,
                    loc_end=anchor,
                    text=text,
                    path=docx_path,
                )
            )
        buffer = []
        start_i = None
        end_i = None
        char_count = 0

    for p in items:
        txt = p["text"]
        i = p["i"]
        if start_i is None:
            start_i = i
        end_i = i
        buffer.append(txt)
        char_count += len(txt) + 1
        if char_count >= max_chars:
            flush()

    flush()


# -----------------------------
# Unified interface (PDF + DOCX)
# -----------------------------
def extract_units_from_file(path: str) -> List[ExtractedUnit]:
    p = (path or "").replace("\\", "/")
    low = p.lower()
    if low.endswith(".pdf"):
        return extract_pdf_units(p)
    if low.endswith(".docx"):
        return extract_docx_units(p)
    return []


def extract_units_from_files(paths: List[str]) -> List[ExtractedUnit]:
    all_units: List[ExtractedUnit] = []
    for p in paths:
        all_units.extend(extract_units_from_file(p))
    return all_units
