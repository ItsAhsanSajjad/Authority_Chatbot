import os
import uuid
from urllib.parse import quote
from typing import List, Dict, Any, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware

from settings import get_settings
from auth import require_auth, get_auth_identity
from retriever import retrieve, rewrite_contextual_query
from answerer import answer_question
from log_config import setup_logging, get_logger, request_id_var
from openai_clients import has_api_key
from session_store import get_session_store, SessionTurn
from audit_trail import log_audit_entry
from context_state import anchor_query, extract_subject, extract_evidence_metadata
from smalltalk_intent import decide_smalltalk

# Auto-indexer (safe blue/green builds + atomic pointer switch)
from index_manager import SafeAutoIndexer, IndexManagerConfig

# Rate limiting
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

log = get_logger("pera.api")
_settings = get_settings()


# ============================================================
# FastAPI app
# ============================================================
app = FastAPI(title="PERA AI Backend", version="3.0.0")

# ── Rate Limiting ─────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address, enabled=_settings.RATE_LIMIT_ENABLED)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS (permissive — accepts all origins) ───────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Allow iframes (fix for browser blocking)
@app.middleware("http")
async def add_iframe_headers(request, call_next):
    response = await call_next(request)
    # X-Frame-Options is obsolete and ALLOWALL is invalid. 
    # Use CSP frame-ancestors instead.
    if "X-Frame-Options" in response.headers:
        del response.headers["X-Frame-Options"]
    response.headers["Content-Security-Policy"] = "frame-ancestors *"
    return response


# ============================================================
# Static PDFs setup (serves real files at /assets/data/<filename>)
# ============================================================
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = _settings.DATA_DIR if os.path.isabs(_settings.DATA_DIR) else os.path.join(_SCRIPT_DIR, _settings.DATA_DIR)
app.mount("/assets/data", StaticFiles(directory=DATA_DIR), name="data")


# ============================================================
# Auto-indexer setup (from centralized settings)
# ============================================================
indexer = SafeAutoIndexer(
    IndexManagerConfig(
        data_dir=DATA_DIR,
        indexes_root=_settings.INDEXES_ROOT.replace("\\", "/"),
        active_pointer_path=_settings.INDEX_POINTER_PATH.replace("\\", "/"),
        poll_seconds=_settings.INDEX_POLL_SECONDS,
        keep_last_n=_settings.INDEX_KEEP_LAST_N,
        chunk_max_chars=_settings.CHUNK_MAX_CHARS,
        chunk_overlap_chars=_settings.CHUNK_OVERLAP_CHARS,
    )
)

# ============================================================
# Request ID Middleware
# ============================================================
class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Accept forwarded request ID or generate a new one
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        request.state.request_id = rid
        request_id_var.set(rid)
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response

app.add_middleware(RequestIDMiddleware)


@app.on_event("startup")
def _startup():
    """Initialize logging, validate config, and start background indexer."""
    setup_logging()
    log.info("PERA AI Backend v3.0.0 starting up")
    try:
        _settings.validate_required_secrets()
    except RuntimeError as e:
        log.error("Startup validation failed: %s", e)
        raise
    log.info("Auth: %s | CORS origins: %s | Rate limiting: %s",
             "enabled" if _settings.AUTH_ENABLED else "DISABLED",
             _settings.CORS_ORIGINS[:80],
             "enabled" if _settings.RATE_LIMIT_ENABLED else "disabled")
    indexer.start_background()

    # ── API Ingestion Bootstrap ──
    if not _settings.API_INGESTION_ENABLED:
        log.warning(
            "API_INGESTION_ENABLED=False — API data pipeline is OFF. "
            "Stored API and 'both' modes will have no API data. "
            "Set API_INGESTION_ENABLED=1 and API_SYNC_ENABLED=1 in .env to activate."
        )
    if _settings.API_INGESTION_ENABLED:
        try:
            from api_db import ApiDatabase
            from api_sync_manager import ApiSyncManager

            api_db = ApiDatabase(_settings.API_DB_URL)
            api_db.migrate()
            log.info("API ingestion DB migrations applied")

            if _settings.API_SYNC_ENABLED:
                idx_dir = indexer.get_active_index_dir()
                sync_mgr = ApiSyncManager(api_db, index_dir=idx_dir)
                sync_result = sync_mgr.run_once()
                log.info("API ingestion bootstrap: %s", sync_result)

                # Phase 5: Start background scheduler
                try:
                    from api_scheduler import ApiScheduler
                    _api_scheduler = ApiScheduler(index_dir=idx_dir)
                    _api_scheduler.start()
                except Exception as e:
                    log.error("API scheduler start failed (non-fatal): %s", e)
        except Exception as e:
            log.error("API ingestion bootstrap failed (non-fatal): %s", e)

    # Phase 5: Include admin routes
    try:
        from api_admin_routes import router as admin_router
        if _settings.API_INGESTION_ENABLED:
            app.include_router(admin_router)
            log.info("API admin routes registered")
    except Exception as e:
        log.warning("API admin routes not loaded: %s", e)

    # Phase 6: Start Challan continuous sync (PostgreSQL)
    # Two-tier: fast=5s for totals, full=60s for all other APIs
    try:
        from challan_sync import ChallanScheduler
        fast = int(os.environ.get("CHALLAN_FAST_INTERVAL", "5"))
        full = int(os.environ.get("CHALLAN_FULL_INTERVAL", "60"))
        _challan_scheduler = ChallanScheduler(fast_interval=fast, full_interval=full)
        if _challan_scheduler.start():
            log.info("Challan scheduler started (fast=%ds, full=%ds)", fast, full)
        else:
            log.warning("Challan scheduler did not start (PostgreSQL may be unavailable)")
    except Exception as e:
        log.error("Challan scheduler start failed (non-fatal): %s", e)


# ── Health / Ready Endpoints (Phase 5) ────────────────────────

@app.get("/health")
def health_check():
    """System health check including API ingestion status."""
    result = {"status": "ok", "service": "pera-ai"}
    try:
        if _settings.API_INGESTION_ENABLED:
            from api_health import get_api_health_status
            result["api_ingestion"] = get_api_health_status()
    except Exception:
        pass
    return result


@app.get("/ready")
def readiness_check():
    """Readiness probe — checks index availability."""
    ready = True
    details = {}
    try:
        from index_cache import get_cached_index
        idx, rows, id_map, _, _ = get_cached_index()
        details["index_loaded"] = idx is not None
        details["chunks_count"] = len(id_map) if id_map else 0
        if idx is None:
            ready = False
    except Exception as e:
        details["index_error"] = str(e)
        ready = False

    if _settings.API_INGESTION_ENABLED:
        try:
            from api_health import get_api_health_status
            details["api_ingestion"] = get_api_health_status()
        except Exception:
            pass

    return {"ready": ready, "details": details}


# ============================================================
# API models
# ============================================================
class QueryRequest(BaseModel):
    user_id: str
    message: str


class QueryResponse(BaseModel):
    user_id: str
    answer: str


class QueryResponseJSON(BaseModel):
    user_id: str
    answer: str
    references: List[Dict[str, Any]]


# ============================================================
# Helpers
# ============================================================
def _base_url() -> str:
    return _settings.BASE_URL.rstrip("/")


def _is_safe_under_assets_data(abs_path: str) -> bool:
    """Prevent path traversal to ensure the file is under the assets/data directory."""
    try:
        data_abs = os.path.abspath(DATA_DIR)
        file_abs = os.path.abspath(abs_path)
        return os.path.commonpath([data_abs, file_abs]) == data_abs
    except Exception:
        return False


def _extract_filename(ref_path: str, doc_name: str) -> str:
    """
    Extract filename from reference path/doc name.
    Ensures we only ever return a filename (no directories).
    """
    p = (ref_path or "").strip().replace("\\", "/")
    if p.startswith("/assets/data/") or p.startswith("assets/data/"):
        return os.path.basename(p)
    if p.lower().endswith(".pdf") or p.lower().endswith(".docx"):
        return os.path.basename(p)
    return os.path.basename((doc_name or "").strip())


def _safe_join_base_and_path(base_url: str, path: str) -> str:
    if not path:
        return ""
    p = path.strip().replace("\\", "/")
    if p.startswith("http://") or p.startswith("https://"):
        return p
    if not p.startswith("/"):
        p = "/" + p
    return f"{base_url}{p}"


def _build_assets_data_url(baseurl: str, filename: str) -> str:
    """Real static file path: https://<host>/assets/data/<filename>"""
    safe = quote(filename)
    return f"{baseurl}/assets/data/{safe}"


def _build_forced_download_url(baseurl: str, filename: str) -> str:
    """Optional forced-download endpoint (Content-Disposition: attachment)."""
    safe = quote(filename)
    return f"{baseurl}/download/{safe}"


def _compress_int_ranges(nums: List[int]) -> str:
    if not nums:
        return ""
    nums = sorted(set(int(x) for x in nums if isinstance(x, int) or str(x).isdigit()))
    if not nums:
        return ""
    out: List[str] = []
    start = prev = nums[0]
    for n in nums[1:]:
        if n == prev + 1:
            prev = n
        else:
            out.append(str(start) if start == prev else f"{start}–{prev}")
            start = prev = n
    out.append(str(start) if start == prev else f"{start}–{prev}")
    return ", ".join(out)


def _extract_pages(ref: Dict[str, Any]) -> List[int]:
    pages: List[int] = []
    ps = ref.get("page_start")
    pe = ref.get("page_end")
    try:
        if ps is not None:
            a = int(ps)
            b = int(pe) if pe is not None else a
            for p in range(min(a, b), max(a, b) + 1):
                pages.append(p)
    except Exception:
        pass
    return pages


def _extract_locs(ref: Dict[str, Any]) -> List[str]:
    locs: List[str] = []
    # include loc_start (and loc if present)
    for k in ("loc", "loc_start"):
        v = ref.get(k)
        if v is not None and str(v).strip():
            locs.append(str(v).strip())
    return sorted(set(locs))


def _apply_page_anchor_if_missing(open_url: str, pages: List[int]) -> str:
    if not open_url:
        return open_url
    if "#" in open_url:
        return open_url
    if pages:
        p = min(int(x) for x in pages if isinstance(x, int) or str(x).isdigit())
        return f"{open_url}#page={p}"
    return open_url


def _build_open_url_like_streamlit(baseurl: str, ref: Dict[str, Any]) -> str:
    """
    Match Streamlit behavior:
    - Prefer answerer-produced open_url (already includes #page= when available)
    - Else build base + ref.path + ref.url_hint
    - Else fallback to /assets/data/<filename>
    - Ensure #page= is applied if we have pages and no anchor
    """
    # 1) Prefer answerer open_url
    u = (ref.get("open_url") or "").strip()
    if u:
        pages = _extract_pages(ref)
        return _apply_page_anchor_if_missing(u, pages)

    # 2) base + path + url_hint
    path = (ref.get("path") or "").strip()
    url_hint = (ref.get("url_hint") or "").strip()
    if path:
        built = _safe_join_base_and_path(baseurl, path)
        u2 = f"{built}{url_hint}"
        pages = _extract_pages(ref)
        return _apply_page_anchor_if_missing(u2, pages)

    # 3) fallback to /assets/data/<filename>
    doc = (ref.get("document") or "Unknown document").strip()
    filename = _extract_filename("", doc)
    u3 = _build_assets_data_url(baseurl, filename)
    pages = _extract_pages(ref)
    return _apply_page_anchor_if_missing(u3, pages)


def _group_references_like_streamlit(refs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Group by (document, path) and aggregate pages/locs so output matches Streamlit style:
      Document
      Pages: ...
      Sections / Paragraphs: ...
    """
    grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for r in refs or []:
        if not isinstance(r, dict):
            continue

        doc = (r.get("document") or r.get("doc_name") or "Unknown document").strip()
        path = (r.get("path") or r.get("public_path") or "").strip()

        # if no path provided, synthesize /assets/data/<filename>
        if not path:
            filename = _extract_filename(r.get("path", ""), doc)
            path = f"/assets/data/{filename}"

        key = (doc, path)
        g = grouped.get(key)
        if not g:
            g = {
                "document": doc,
                "path": path,
                "open_url": (r.get("open_url") or "").strip(),
                "url_hint": (r.get("url_hint") or "").strip(),
                "pages": set(),
                "locs": set(),
                "snippet": (r.get("snippet") or "").strip(),
            }
            grouped[key] = g

        # keep first snippet if already set; otherwise fill
        if not g.get("snippet") and (r.get("snippet") or "").strip():
            g["snippet"] = (r.get("snippet") or "").strip()

        # accumulate pages
        for p in _extract_pages(r):
            g["pages"].add(int(p))

        # accumulate locs
        for loc in _extract_locs(r):
            g["locs"].add(loc)

        # keep open_url/url_hint if missing
        if not g.get("open_url") and (r.get("open_url") or "").strip():
            g["open_url"] = (r.get("open_url") or "").strip()
        if not g.get("url_hint") and (r.get("url_hint") or "").strip():
            g["url_hint"] = (r.get("url_hint") or "").strip()

    out: List[Dict[str, Any]] = []
    for g in grouped.values():
        out.append(
            {
                "document": g["document"],
                "path": g["path"],
                "open_url": g.get("open_url", ""),
                "url_hint": g.get("url_hint", ""),
                "pages": sorted(list(g["pages"])),
                "locs": sorted(list(g["locs"])),
                "snippet": g.get("snippet", ""),
            }
        )

    out.sort(key=lambda x: x.get("document", ""))
    return out


# ── Debug: officer detection test ────────────────────────────

# ============================================================
# Download endpoint (kept) - forces download
# ============================================================
@app.get("/download/{filename:path}", dependencies=[Depends(require_auth)])
def download_pdf(filename: str):
    """
    Forced download from assets/data with Content-Disposition: attachment.
    NOTE: References default to open_url (/assets/data...) for page anchors.
    """
    filename = (filename or "").strip()
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required.")

    abs_path = os.path.join(DATA_DIR, filename).replace("\\", "/")

    if not _is_safe_under_assets_data(abs_path):
        raise HTTPException(status_code=400, detail="Invalid path.")

    if not os.path.exists(abs_path) or not os.path.isfile(abs_path):
        raise HTTPException(status_code=404, detail="File not found.")

    return FileResponse(
        abs_path,
        media_type="application/pdf",
        filename=os.path.basename(abs_path),
        headers={"Content-Disposition": f'attachment; filename="{os.path.basename(abs_path)}"'},
    )


# ============================================================
# Main endpoint (HTML)
# FIX: Open links should jump to the correct page (Streamlit-like)
# ============================================================
@app.post("/ask", response_model=QueryResponse, dependencies=[Depends(require_auth)])
@limiter.limit(_settings.RATE_LIMIT_ASK)
def ask_question(request: Request, query: QueryRequest):
    retrieval = retrieve(query.message)
    result = answer_question(query.message, retrieval)

    baseurl = _base_url()
    answer_text = result.get("answer", "") or ""

    # Group refs like Streamlit (aggregate pages/locs)
    raw_refs = result.get("references", []) or []
    grouped = _group_references_like_streamlit(raw_refs)

    parts: List[str] = []
    parts.append('<div style="font-family: Arial, sans-serif; line-height: 1.6; margin:0; padding:0;">')
    parts.append(f'<p style="margin:0; padding:0;">{answer_text}</p>')

    if grouped:
        parts.append('<hr style="margin:8px 0;" />')
        parts.append('<h3 style="margin:4px 0;">References</h3>')
        parts.append('<ol style="margin:0; padding-left:18px;">')

        for g in grouped:
            doc = (g.get("document") or "Unknown document").strip()
            snippet = (g.get("snippet") or "").strip()
            pages: List[int] = g.get("pages") or []
            locs: List[str] = g.get("locs") or []

            # Build Streamlit-like open URL: prefer open_url/path/url_hint and ensure #page=...
            open_url = _build_open_url_like_streamlit(
                baseurl,
                {
                    "document": doc,
                    "path": g.get("path"),
                    "open_url": g.get("open_url"),
                    "url_hint": g.get("url_hint"),
                    # Provide page_start/page_end from aggregated pages (for anchor)
                    "page_start": min(pages) if pages else None,
                    "page_end": max(pages) if pages else None,
                    "loc_start": (locs[0] if locs else None),
                },
            )

            meta_lines: List[str] = []
            if pages:
                meta_lines.append(f"Pages: {_compress_int_ranges(pages)}")
            if locs:
                joined = "; ".join(locs[:6])
                if len(locs) > 6:
                    joined += f"; +{len(locs)-6} more"
                meta_lines.append(f"Sections / Paragraphs: {joined}")

            meta_html = ""
            if meta_lines:
                meta_html = "<br/>".join(meta_lines)

            parts.append(
                f"""
                <li style="margin-bottom:10px;">
                  <a href="{open_url}" target="_blank" rel="noopener noreferrer">{doc}</a>
                  {("<div style='margin-top:3px; color:#6b7280; font-size:13px;'>" + meta_html + "</div>") if meta_html else ""}
                  {("<p style='margin:4px 0 0 0;'>" + snippet + "</p>") if snippet else ""}
                </li>
                """.strip()
            )

        parts.append("</ol>")

    parts.append("</div>")
    return QueryResponse(user_id=query.user_id, answer="\n".join(parts).strip())


# ============================================================
# JSON endpoint (for mobile / frontend)
# FIX: Provide Streamlit-like reference object (open_url with #page)
# ============================================================
@app.post("/ask_json", response_model=QueryResponseJSON, dependencies=[Depends(require_auth)])
@limiter.limit(_settings.RATE_LIMIT_ASK)
def ask_question_json(request: Request, query: QueryRequest):
    retrieval = retrieve(query.message)
    result = answer_question(query.message, retrieval)

    baseurl = _base_url()

    raw_refs = result.get("references", []) or []
    grouped = _group_references_like_streamlit(raw_refs)

    refs_out: List[Dict[str, Any]] = []
    for g in grouped:
        doc = (g.get("document") or "Unknown document").strip()
        pages: List[int] = g.get("pages") or []
        locs: List[str] = g.get("locs") or []

        # Use the same open_url logic as Streamlit (page anchor)
        open_url = _build_open_url_like_streamlit(
            baseurl,
            {
                "document": doc,
                "path": g.get("path"),
                "open_url": g.get("open_url"),
                "url_hint": g.get("url_hint"),
                "page_start": min(pages) if pages else None,
                "page_end": max(pages) if pages else None,
                "loc_start": (locs[0] if locs else None),
            },
        )

        # Optional forced-download URL (use filename)
        filename = _extract_filename(g.get("path", ""), doc)
        download_url = _build_forced_download_url(baseurl, filename)

        refs_out.append(
            {
                "document": doc,
                "open_url": open_url,
                "download_url": download_url,
                "path": g.get("path"),
                "pages": pages,     # aggregated
                "locs": locs,       # aggregated
                "snippet": (g.get("snippet") or "").strip(),
            }
        )

    return QueryResponseJSON(
        user_id=query.user_id,
        answer=result.get("answer", "") or "",
        references=refs_out,
    )


# ============================================================
# Simple Chat API for Next.js frontend
# ============================================================

# Valid answer source modes
VALID_SOURCE_MODES = {"documents", "stored_api", "both", "live_api"}

# Display labels and provenance text for each mode
SOURCE_MODE_LABELS = {
    "documents": "Documents Only",
    "stored_api": "Stored API Data",
    "both": "Documents + Stored API Data",
    "live_api": "Live API Data",
}
SOURCE_MODE_PROVENANCE = {
    "documents": "This answer was generated from stored indexed regulatory documents.",
    "stored_api": "This answer was generated from stored indexed API snapshots.",
    "both": "This answer combines stored indexed documents and stored indexed API snapshots.",
    "live_api": "This answer was generated from live API responses.",
}


class SimpleChatRequest(BaseModel):
    question: str
    conversation_history: Optional[List[Dict[str, Any]]] = None
    session_id: Optional[str] = None  # Part 3: server-side session
    answer_source_mode: Optional[str] = "both"  # documents | stored_api | both | live_api


class SimpleChatResponse(BaseModel):
    answer: str
    decision: str
    references: List[Dict[str, Any]]
    session_id: Optional[str] = None  # returned so client can persist
    grounding: Optional[Dict[str, Any]] = None  # Part 2 additive
    source_mode: Optional[str] = None  # which mode was used
    source_mode_label: Optional[str] = None  # human-readable label
    provenance: Optional[str] = None  # provenance statement


import re as _re_ru

# ── Common typo / misspelling corrections ────────────────────────────────────
# Runs on ALL queries before any routing or intent detection.
# Maps common misspellings to correct English so intent keywords match.
_TYPO_CORRECTIONS = [
    # inspection typos
    (_re_ru.compile(r"\binpections?\b", _re_ru.I), "inspections"),
    (_re_ru.compile(r"\binspections?\b", _re_ru.I), lambda m: m.group(0)),  # no-op, already correct
    (_re_ru.compile(r"\binsepctions?\b", _re_ru.I), "inspections"),
    (_re_ru.compile(r"\binspectons?\b", _re_ru.I), "inspections"),
    (_re_ru.compile(r"\binspcetions?\b", _re_ru.I), "inspections"),
    (_re_ru.compile(r"\binspetions?\b", _re_ru.I), "inspections"),
    # summary typos
    (_re_ru.compile(r"\bsammary\b", _re_ru.I), "summary"),
    (_re_ru.compile(r"\bsummry\b", _re_ru.I), "summary"),
    (_re_ru.compile(r"\bsumary\b", _re_ru.I), "summary"),
    (_re_ru.compile(r"\bsummari\b", _re_ru.I), "summary"),
    (_re_ru.compile(r"\bsumery\b", _re_ru.I), "summary"),
    (_re_ru.compile(r"\bsummey\b", _re_ru.I), "summary"),
    # challan typos
    (_re_ru.compile(r"\bchallns?\b", _re_ru.I), "challans"),
    (_re_ru.compile(r"\bchalans?\b", _re_ru.I), "challans"),
    (_re_ru.compile(r"\bchallanz?\b", _re_ru.I), "challans"),
    # division typos
    (_re_ru.compile(r"\bdivisons?\b", _re_ru.I), "divisions"),
    (_re_ru.compile(r"\bdevisions?\b", _re_ru.I), "divisions"),
]

def _fix_typos(text: str) -> str:
    """Fix common misspellings of domain keywords."""
    result = text
    for pattern, replacement in _TYPO_CORRECTIONS:
        if callable(replacement):
            result = pattern.sub(replacement, result)
        else:
            result = pattern.sub(replacement, result)
    return result

# ── Roman Urdu → English preprocessing ───────────────────────────────────────
# Translates key Roman Urdu words/phrases to English equivalents so the LLM
# and retriever can correctly parse date ranges, quantities, locations, etc.
_RU_WORD_MAP = [
    # Preserve "sab sy/se ziada" (= most) BEFORE translating sy/se → from
    (_re_ru.compile(r"\bsab\s+s[ey]\s+(?:ziada|zyada|ziyada)\b", _re_ru.IGNORECASE), "most"),
    # Date-range markers: sy/se → from, tk/tak → to
    (_re_ru.compile(r"\bsy\b",   _re_ru.IGNORECASE), "from"),
    (_re_ru.compile(r"\bse\b",   _re_ru.IGNORECASE), "from"),
    (_re_ru.compile(r"\btk\b",   _re_ru.IGNORECASE), "to"),
    (_re_ru.compile(r"\btak\b",  _re_ru.IGNORECASE), "to"),
    # Conjunctions (Roman Urdu "or" = "and" NOT English "or")
    (_re_ru.compile(r"\bor\b",   _re_ru.IGNORECASE), "and"),
    (_re_ru.compile(r"\baur\b",  _re_ru.IGNORECASE), "and"),
    # Location marker
    (_re_ru.compile(r"\bmein\b", _re_ru.IGNORECASE), "in"),
    (_re_ru.compile(r"\bmae\b",  _re_ru.IGNORECASE), "in"),
    # Quantity questions
    (_re_ru.compile(r"\bkitni\b", _re_ru.IGNORECASE), "how many"),
    (_re_ru.compile(r"\bkitne\b", _re_ru.IGNORECASE), "how many"),
    (_re_ru.compile(r"\bkitna\b", _re_ru.IGNORECASE), "how many"),
    (_re_ru.compile(r"\bkitny\b", _re_ru.IGNORECASE), "how many"),
    # Common verbs / question words (drop or translate)
    (_re_ru.compile(r"\bhoi\s+hain\b", _re_ru.IGNORECASE), "were"),
    (_re_ru.compile(r"\bhoey\s+hain\b", _re_ru.IGNORECASE), "were"),
    (_re_ru.compile(r"\bhuey\s+hain\b", _re_ru.IGNORECASE), "were"),
    (_re_ru.compile(r"\bhain\b",  _re_ru.IGNORECASE), "are"),
    (_re_ru.compile(r"\bhai\b",   _re_ru.IGNORECASE), "is"),
    (_re_ru.compile(r"\bkia\b",   _re_ru.IGNORECASE), "what"),
    (_re_ru.compile(r"\bkya\b",   _re_ru.IGNORECASE), "what"),
    (_re_ru.compile(r"\bkaun\b",  _re_ru.IGNORECASE), "who"),
    (_re_ru.compile(r"\bkahan\b", _re_ru.IGNORECASE), "where"),
    (_re_ru.compile(r"\bkab\b",   _re_ru.IGNORECASE), "when"),
    (_re_ru.compile(r"\bkyun\b",  _re_ru.IGNORECASE), "why"),
    (_re_ru.compile(r"\bkaise\b", _re_ru.IGNORECASE), "how"),
    # Common nouns
    (_re_ru.compile(r"\bjama\b",  _re_ru.IGNORECASE), "total"),
    (_re_ru.compile(r"\btafseeli\b", _re_ru.IGNORECASE), "detailed"),
    (_re_ru.compile(r"\bkul\b",   _re_ru.IGNORECASE), "total"),
    (_re_ru.compile(r"\bafsaran\b", _re_ru.IGNORECASE), "officers"),
    (_re_ru.compile(r"\bafsar\b", _re_ru.IGNORECASE), "officer"),
    (_re_ru.compile(r"\bmulazimat\b", _re_ru.IGNORECASE), "employees"),
    (_re_ru.compile(r"\bmulazmeen\b", _re_ru.IGNORECASE), "employees"),
    # Clean up leftover particles
    (_re_ru.compile(r"\b(ki|ka|ke|ko|ne|bhi|hi|woh|yeh|ye|tha|thi|the)\b", _re_ru.IGNORECASE), ""),
]

def _preprocess_roman_urdu(text: str) -> str:
    """
    Convert Roman Urdu words/phrases to English equivalents before retrieval/LLM.
    Handles word-order rewriting (e.g. "10 march sy 14 march tk" → "from 10 march to 14 march").
    Only runs when the text contains typical Roman Urdu patterns.
    """
    # Quick check: does it contain any Roman Urdu markers?
    _RU_INDICATORS = {"sy", "se", "tk", "tak", "mein", "mae", "kitni", "kitne",
                      "kitna", "kitny", "aur", "hain", "hai", "kia", "kya",
                      "kaun", "kahan", "kab"}
    words_lower = set(text.lower().split())
    if not words_lower.intersection(_RU_INDICATORS):
        return text  # Pure English — skip processing

    result = text

    # ── Phase 1: Reorder date-range phrases BEFORE word substitution ──
    # Pattern: "<date_expr1> sy/se <date_expr2> tk/tak"
    # → "from <date_expr1> to <date_expr2>"
    # date_expr = digits, month names, year numbers (e.g. "10 march", "14 march 2026")
    _MONTH = r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    _DATE_EXPR = r"(\d{1,2}\s+" + _MONTH + r"(?:\s+\d{4})?)"
    _DATE_RANGE_RU = _re_ru.compile(
        _DATE_EXPR + r"\s+(?:sy|se)\s+" + _DATE_EXPR + r"\s+(?:tk|tak)",
        _re_ru.IGNORECASE
    )
    result = _DATE_RANGE_RU.sub(lambda m: f"from {m.group(1)} to {m.group(2)}", result)

    # ── Phase 2: Word-level substitutions ─────────────────────────────
    for pattern, replacement in _RU_WORD_MAP:
        result = pattern.sub(replacement, result)

    # Collapse multiple spaces left by empty replacements
    result = _re_ru.sub(r"\s{2,}", " ", result).strip()
    log.info("Roman Urdu preprocessed: '%s' → '%s'", text[:120], result[:120])
    return result


@app.post("/api/ask", response_model=SimpleChatResponse, dependencies=[Depends(require_auth)])
@limiter.limit(_settings.RATE_LIMIT_ASK)
def simple_ask(request: Request, body: SimpleChatRequest):
    """Chat endpoint with session tracking, smalltalk bypass, entity anchoring, and audit trail."""
    # Translate Roman Urdu to English before any processing
    question = _fix_typos(_preprocess_roman_urdu(body.question.strip()))
    rid = getattr(request.state, "request_id", "")

    # ── 0a. Validate and normalize source mode ─────────────────
    source_mode = (body.answer_source_mode or "both").strip().lower()
    if source_mode not in VALID_SOURCE_MODES:
        source_mode = "both"
    log.info("Source mode: %s", source_mode)

    # ── 0. Session ────────────────────────────────────────────
    store = get_session_store()
    session = store.get_or_create(body.session_id)
    sid = session.session_id

    # ── 1. Smalltalk bypass ───────────────────────────────────
    st = decide_smalltalk(question)
    if st and st.is_greeting_only:
        log.info("Smalltalk bypass: '%s' -> deterministic response", question[:40])
        log_audit_entry(
            request_id=rid, session_id=sid, question=question,
            decision="smalltalk", answer_text=st.response,
            is_smalltalk=True,
        )
        return SimpleChatResponse(
            answer=st.response,
            decision="smalltalk",
            references=[],
            session_id=sid,
        )

    # If greeting + real question, use remainingquestion with ack prefix
    ack_prefix = ""
    if st and not st.is_greeting_only and st.remaining_question:
        ack_prefix = st.ack
        question = st.remaining_question
        log.info("Greeting stripped: ack='%s', question='%s'", ack_prefix, question[:60])

    # ── 2. Extract last Q/A from client history FIRST ───────────
    # (needed for entity anchoring when session_id is not sent)
    last_question_client = None
    last_answer_client = None
    if body.conversation_history:
        for msg in reversed(body.conversation_history):
            role = msg.get("role", "")
            content = (msg.get("content") or "").strip()
            if role == "assistant" and last_answer_client is None:
                last_answer_client = content
            elif role == "user" and last_question_client is None:
                last_question_client = content
            if last_question_client and last_answer_client:
                break

    # Build fallback subject from client history if session is empty
    fallback_subject = ""
    if not session.last_subject and last_answer_client:
        fallback_subject = extract_subject(last_answer_client)
        if fallback_subject:
            log.info("Using client-history subject fallback: '%s'", fallback_subject[:60])

    # ── 3. Entity anchoring (session + client fallback) ────────
    anchored_q, subject_used, was_anchored = anchor_query(
        question,
        last_subject=session.last_subject or fallback_subject,
        last_question=session.last_question or last_question_client or "",
        last_answer=session.last_answer or last_answer_client or "",
    )

    # Prefer server-side session for last Q/A if available
    lq = session.last_question or last_question_client or ""
    la = session.last_answer or last_answer_client or ""

    # ── 4. Rewrite query (handles Urdu, abbreviations, follow-ups) ──
    query_for_retrieval = rewrite_contextual_query(anchored_q, lq, la)
    rewrite_diff = ""
    if query_for_retrieval != question:
        rewrite_diff = f"{question} -> {query_for_retrieval}"
    log.info("Query: '%s' -> Anchored: '%s' -> Rewritten: '%s'",
             question[:60], anchored_q[:60], query_for_retrieval[:60])

    # ── 5. Retrieve + Answer ──────────────────────────────────
    # Live API mode — bypass normal RAG pipeline entirely
    if source_mode == "live_api":
        from live_api_handler import query_live_api
        live_result = query_live_api(query_for_retrieval)
        if live_result["success"]:
            live_answer = live_result["answer"]
            live_refs = [{
                "document": f"Live: {live_result['endpoint_name']}",
                "source_type": "live_api",
                "endpoint_key": live_result["endpoint_key"],
                "timestamp": live_result["timestamp"],
            }]
        else:
            live_answer = (
                f"⚠️ Live API could not answer this question.\n\n"
                f"{live_result.get('error', 'Unknown error')}\n\n"
                f"Please try switching to **Documents** or **Stored API** mode."
            )
            live_refs = []

        log_audit_entry(
            request_id=rid, session_id=sid, question=question,
            normalized_query=query_for_retrieval,
            decision="live_api", answer_text=live_answer,
            evidence_ids=[], doc_names=[],
            answer_source_mode="live_api",
            live_api_used=True,
            live_api_endpoint=live_result.get("endpoint_key", ""),
        )
        final_live = (ack_prefix + live_answer) if ack_prefix else live_answer
        return SimpleChatResponse(
            answer=final_live,
            decision="live_api",
            references=live_refs,
            session_id=sid,
            source_mode="live_api",
            source_mode_label=SOURCE_MODE_LABELS["live_api"],
            provenance=SOURCE_MODE_PROVENANCE["live_api"],
        )

    # Normal RAG path with source_type_filter
    _filter_map = {
        "documents": "document",
        "stored_api": "api",
        "both": None,
    }

    # ── 5a. Direct stored API lookup for known reference queries ──
    # For known structured patterns (divisions, strength, finance),
    # query the SQLite api_records table directly instead of FAISS.
    from stored_api_lookup import detect_lookup_intent, execute_lookup, \
        build_lookup_retrieval, merge_lookup_with_rag
    from query_router import classify_query, QueryType

    query_class = classify_query(query_for_retrieval)
    log.info("Query classification: %s", query_class.value)

    lookup_type = None
    lookup_result = None
    _is_followup = False
    if source_mode in ("stored_api", "both"):
        # ── Follow-up detection FIRST ──────────────────────────────
        # When there's a previous lookup context (e.g. oa_tehsil:Shalimar),
        # follow-up questions like "in k against kitny challans?" should
        # carry forward that context. If we run direct intent detection
        # first, it loses the location and returns oa_summary.

        # Fallback: if session has no last_lookup_type (e.g. fresh session
        # after restart or incognito), try to infer it from the previous
        # user question in conversation_history.
        _prev_lookup = session.last_lookup_type
        if not _prev_lookup and last_question_client:
            _prev_lookup = detect_lookup_intent(last_question_client)
            if _prev_lookup:
                log.info("Inferred prev lookup from client history: '%s' -> %s",
                         last_question_client[:60], _prev_lookup)

        if _prev_lookup:
            # 1. OA follow-up (including cross-domain OA→Challan)
            try:
                from operational_activity_lookup import detect_oa_followup
                followup_intent = detect_oa_followup(question, _prev_lookup)
                if followup_intent:
                    lookup_type = followup_intent
                    _is_followup = True
                    log.info("OA follow-up detected: '%s' -> %s (prev=%s)",
                             question[:60], followup_intent, _prev_lookup)
            except ImportError:
                pass

            # 2. Inspection follow-up
            if not lookup_type:
                try:
                    from inspection_lookup import detect_inspection_followup
                    followup_intent = detect_inspection_followup(question, _prev_lookup)
                    if followup_intent:
                        lookup_type = followup_intent
                        _is_followup = True
                        log.info("Inspection follow-up detected: '%s' -> %s (prev=%s)",
                                 question[:60], followup_intent, _prev_lookup)
                except ImportError:
                    pass

            # 3. Challan follow-up (only if OA/Inspection didn't match)
            if not lookup_type:
                from challan_lookup import detect_challan_followup
                followup_intent = detect_challan_followup(question, _prev_lookup)
                if followup_intent:
                    lookup_type = followup_intent
                    _is_followup = True
                    log.info("Challan follow-up detected: '%s' -> %s (prev=%s)",
                             question[:60], followup_intent, _prev_lookup)

        # ── Direct intent detection (if no follow-up matched) ─────
        if not lookup_type:
            # Check original question FIRST — the LLM rewrite can inject
            # keywords (e.g. adding "challans" to a pure requisition
            # question) which would cause false cross-domain routing.
            lookup_type = detect_lookup_intent(question)
            log.debug("Direct intent (original): '%s' -> %s", question[:80], lookup_type)
            if not lookup_type:
                # Fallback: check rewritten query (may have resolved dates/context)
                lookup_type = detect_lookup_intent(query_for_retrieval)
                log.debug("Direct intent (rewritten): '%s' -> %s",
                         (query_for_retrieval or "")[:80], lookup_type)
            # For structured queries in "both" mode, also try lookup detection
            if not lookup_type and query_class == QueryType.STRUCTURED:
                lookup_type = detect_lookup_intent(question)

    # _is_followup stays True only when the follow-up detectors (OA/Insp/Challan)
    # set the lookup_type. Direct intent detection does NOT count as a follow-up.
    # (The flag was already initialised to False before the follow-up block;
    #  we must NOT reset it here or we lose the follow-up signal.)

    if lookup_type:
        # ALWAYS use the ORIGINAL user question for lookup execution (date
        # extraction, location parsing etc.).  The LLM-rewritten query can
        # fabricate dates from conversation history (e.g. "last week" gets
        # rewritten to "March 10 to March 20" from a previous turn).
        _lookup_q = question
        # For follow-ups, only prepend previous question when the current
        # question has NO dates of its own.  Otherwise the old dates
        # override whatever the user actually asked for.
        if _is_followup and lq:
            # Check if current question already has dates
            _cur_has_dates = False
            try:
                from challan_lookup import _extract_date_range as _edr
                _cur_dates = _edr(_lookup_q)
                if _cur_dates and _cur_dates[0]:
                    _cur_has_dates = True
            except Exception:
                pass
            if not _cur_has_dates:
                _lookup_q = f"{lq} {_lookup_q}"
                log.info("Follow-up (no new dates): combined question for context: '%s'", _lookup_q[:120])
            else:
                log.info("Follow-up (has own dates): using current question only: '%s'", _lookup_q[:120])
        lookup_result = execute_lookup(lookup_type, question=_lookup_q)
        log.info("API lookup detected (type=%s, mode=%s, found=%s)",
                 lookup_type, source_mode, bool(lookup_result))

    if lookup_result and source_mode == "stored_api":
        # Pure stored API mode: use only the direct lookup data
        retrieval = build_lookup_retrieval(lookup_result, lookup_type)
    elif lookup_result and source_mode == "both":
        # Both mode: merge lookup data with document RAG
        lookup_retrieval = build_lookup_retrieval(lookup_result, lookup_type)
        doc_retrieval = retrieve(
            query_for_retrieval,
            source_type_filter="document",
        )
        retrieval = merge_lookup_with_rag(lookup_retrieval, doc_retrieval)
    else:
        # Default: normal FAISS retrieval path
        retrieval = retrieve(
            query_for_retrieval,
            source_type_filter=_filter_map.get(source_mode),
        )

    # ── 5b. Live API fallback for structured queries with no evidence ──
    # When in "both" mode and FAISS/stored API returned nothing for a
    # structured query, try the live API as a last resort.
    if (source_mode == "both"
            and not retrieval.get("has_evidence")
            and query_class in (QueryType.STRUCTURED, QueryType.HYBRID)):
        try:
            from live_api_handler import query_live_api
            live_fallback = query_live_api(query_for_retrieval)
            if live_fallback.get("success"):
                log.info("Live API fallback succeeded for structured query: %s",
                         live_fallback.get("endpoint_key"))
                # Wrap live response as retrieval evidence
                retrieval = {
                    "question": query_for_retrieval,
                    "has_evidence": True,
                    "evidence": [{
                        "doc_name": f"Live: {live_fallback['endpoint_name']}",
                        "max_score": 0.90,
                        "hits": [{
                            "text": live_fallback["answer"],
                            "score": 0.90,
                            "_blend": 0.90,
                            "page_start": "?",
                            "page_end": "?",
                            "public_path": "",
                            "doc_authority": 2,
                            "search_text": "",
                            "source_type": "api",
                            "api_source_id": live_fallback.get("endpoint_key", ""),
                            "record_id": "",
                            "record_type": "live_api_response",
                            "evidence_id": f"live_{live_fallback.get('endpoint_key', 'unknown')}",
                        }],
                    }],
                }
        except Exception as e:
            log.warning("Live API fallback failed (non-fatal): %s", e)

    # Only pass conversation_history for genuine follow-ups to prevent
    # prior wrong answers from contaminating standalone questions
    hist_for_llm = body.conversation_history if was_anchored else None
    # Use anchored_q (with resolved pronouns) for the LLM so position
    # filtering sees the actual role name (e.g. "salary of manager development"
    # instead of "salary of this?")
    question_for_llm = anchored_q if was_anchored else question
    # For vague follow-ups ("tell me in detail"), use the rewritten query which
    # has the full context resolved from conversation history.
    # Combine with original question so user's intent keywords ("in amount",
    # "in detail") survive the rewrite and guide the LLM answer.
    if lookup_type and query_for_retrieval and query_for_retrieval != question:
        _q_words = len(question.split())
        if _q_words <= 8:  # short/vague follow-up
            question_for_llm = f"{question} — {query_for_retrieval}"
            log.info("Using combined query for LLM (vague follow-up): '%s'",
                     question_for_llm[:120])
    result = answer_question(
        question_for_llm,
        retrieval,
        conversation_history=hist_for_llm,
        answer_source_mode=source_mode,
    )

    # ── If LLM call failed internally, raise HTTP 500 so frontend shows retry button ──
    if result.get("decision") == "error":
        log.error("answer_question returned decision=error for question: %s", question_for_llm[:120])
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=result.get("answer", "LLM call failed. Please try again."))

    # ── 6. Extract metadata for session + audit ───────────────
    evidence_ids, doc_names = extract_evidence_metadata(retrieval)
    answer_text = result.get("answer", "")
    decision = result.get("decision", "answer")
    grounding = result.get("grounding")
    support_state = result.get("support_state", "")

    # Extract subject from the answer if we didn't anchor
    subject_from_answer = extract_subject(answer_text) if not subject_used else subject_used
    final_subject = subject_used or subject_from_answer

    # ── 7. Update session state ───────────────────────────────
    if lookup_type:
        session.last_lookup_type = lookup_type
    session.add_turn(SessionTurn(
        question=question,
        normalized_query=query_for_retrieval,
        answer_preview=answer_text[:300],
        decision=decision,
        evidence_ids=evidence_ids,
        doc_names=doc_names,
        subject_entity=final_subject,
    ))
    store.put(sid, session)

    # ── 8. Audit trail ────────────────────────────────────────
    # Build evidence text preview (the actual context sent to LLM)
    evidence_text = ""
    try:
        from answerer import format_evidence_for_llm
        evidence_text = format_evidence_for_llm(retrieval, question)
    except Exception:
        pass

    # Build grounding details dict for audit
    grounding_details = None
    if grounding:
        grounding_details = {
            "score": grounding.get("score"),
            "confidence": grounding.get("confidence"),
            "semantic_support": grounding.get("semantic_support"),
            "support_state": grounding.get("support_state", support_state),
            "unsupported_claims": grounding.get("unsupported_claims", [])[:3],
        }

    log_audit_entry(
        request_id=rid,
        session_id=sid,
        question=question,
        normalized_query=query_for_retrieval,
        decision=decision,
        answer_text=answer_text,
        evidence_ids=evidence_ids,
        doc_names=doc_names,
        references_count=len(result.get("references", [])),
        grounding_score=grounding.get("score") if grounding else None,
        grounding_confidence=grounding.get("confidence", "") if grounding else "",
        grounding_details=grounding_details,
        subject_entity=final_subject,
        evidence_text_preview=evidence_text,
        rewrite_diff=rewrite_diff,
        support_state=support_state,
        auth_identity=get_auth_identity(request),
        prompt_version=_settings.PROMPT_VERSION,
        answer_source_mode=source_mode,
    )

    # ── 9. Build response ─────────────────────────────────────
    final_answer = (ack_prefix + answer_text) if ack_prefix else answer_text

    return SimpleChatResponse(
        answer=final_answer,
        decision=decision,
        references=result.get("references", []),
        session_id=sid,
        grounding=grounding,
        source_mode=source_mode,
        source_mode_label=SOURCE_MODE_LABELS.get(source_mode, "Documents + Stored API Data"),
        provenance=SOURCE_MODE_PROVENANCE.get(source_mode, ""),
    )


# ============================================================
# NEW: Voice Transcription Endpoint
# ============================================================
from fastapi import File, UploadFile
from speech import transcribe_audio


class TranscribeResponse(BaseModel):
    text: str
    success: bool


@app.post("/transcribe", response_model=TranscribeResponse, dependencies=[Depends(require_auth)])
@limiter.limit(_settings.RATE_LIMIT_TRANSCRIBE)
async def transcribe(request: Request, audio: UploadFile = File(...)):
    """Transcribe audio to text using Whisper."""
    try:
        audio_bytes = await audio.read()
        text = transcribe_audio(audio_bytes)
        
        # Check if it's an error message
        is_error = text.startswith("⚠️")
        
        return TranscribeResponse(
            text=text if not is_error else "",
            success=not is_error
        )
    except Exception as e:
        log.error("Transcription endpoint error: %s", e, exc_info=True)
        return TranscribeResponse(text="", success=False)


# ============================================================
# NEW: PDF Serving Endpoint (for Next.js)
# ============================================================
from urllib.parse import unquote
import re

def _normalize_for_match(s: str) -> str:
    """Normalize filename for fuzzy matching - strips all special chars."""
    # Replace all types of dashes with hyphen
    s = s.replace("–", "-").replace("—", "-").replace("−", "-")
    # Replace smart quotes
    s = s.replace("'", "'").replace("'", "'").replace('"', '"').replace('"', '"')
    # Remove extra spaces
    s = re.sub(r'\s+', ' ', s).strip()
    # Lowercase for comparison
    return s.lower()

@app.get("/pdf/{filename:path}", dependencies=[Depends(require_auth)])
def serve_pdf(filename: str):
    """Serve PDF files for the Next.js frontend."""
    try:
        # 1. First attempt: EXACT match (but URL decoded)
        # This handles 'PERA – FAQs.pdf' correctly if the fs has an en-dash
        decoded_name = unquote(filename).strip()
        filepath = os.path.join(DATA_DIR, decoded_name)
        
        found = False
        
        # Check exact existence
        if os.path.exists(filepath) and os.path.isfile(filepath):
            found = True
        
        # 2. Second attempt: Normalize dashes (en-dash/em-dash -> hyphen)
        if not found:
            normalized_name = decoded_name.replace("–", "-").replace("—", "-")
            filepath_norm = os.path.join(DATA_DIR, normalized_name)
            if os.path.exists(filepath_norm) and os.path.isfile(filepath_norm):
                filepath = filepath_norm
                found = True
        
        # 3. Third attempt: Try adding .pdf extension if missing
        if not found:
             # Try exact + .pdf
            filepath_ext = os.path.join(DATA_DIR, decoded_name + ".pdf")
            if os.path.exists(filepath_ext):
                filepath = filepath_ext
                found = True
            else:
                 # Try normalized + .pdf
                filepath_ext_norm = os.path.join(DATA_DIR, normalized_name + ".pdf")
                if os.path.exists(filepath_ext_norm):
                    filepath = filepath_ext_norm
                    found = True
        
        # 4. Fourth attempt: Smart fuzzy scan using normalized comparison
        if not found:
            try:
                # Normalize the target name (remove .pdf, normalize all special chars)
                target_norm = _normalize_for_match(decoded_name)
                if target_norm.endswith(".pdf"):
                    target_norm = target_norm[:-4]
                
                # Also try without any dashes/special chars at all
                target_stripped = re.sub(r'[^a-z0-9 ]', '', target_norm)
                    
                for f in os.listdir(DATA_DIR):
                    f_norm = _normalize_for_match(f)
                    if f_norm.endswith(".pdf"):
                        f_norm = f_norm[:-4]
                    f_stripped = re.sub(r'[^a-z0-9 ]', '', f_norm)
                    
                    # Match if normalized versions match OR stripped versions match
                    if f_norm == target_norm or f_stripped == target_stripped:
                        filepath = os.path.join(DATA_DIR, f)
                        found = True
                        log.debug("Fuzzy matched: %s -> %s", decoded_name, f)
                        break
            except Exception as e:
                log.warning("Error during fuzzy scan: %s", e)
        
        if not found:
             log.info("PDF Not Found: %s (decoded: %s)", filename, decoded_name)
             raise HTTPException(status_code=404, detail=f"PDF not found: {filename}")

        # Security check
        if not _is_safe_under_assets_data(filepath):
            log.warning("Access Denied for path: %s", filepath)
            raise HTTPException(status_code=403, detail="Access denied")
        
        return FileResponse(
            filepath,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="{os.path.basename(filepath)}"',
                "Content-Security-Policy": "frame-ancestors *",
                "X-Frame-Options": "ALLOWALL",
                "Access-Control-Allow-Origin": "*"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error("Internal server error serving PDF '%s': %s", filename, e, exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while serving file")


# ============================================================
# Health & Readiness Endpoints
# ============================================================
@app.get("/health")
def health_check():
    """Lightweight liveness probe — confirms the process is alive."""
    return {"status": "ok"}


@app.get("/ready")
def readiness_check():
    """Readiness probe — confirms the service can serve traffic."""
    checks: Dict[str, Any] = {}

    # 1. OpenAI API key present
    checks["openai_api_key"] = has_api_key()

    # 2. Active index pointer exists and is valid
    try:
        active_dir = indexer.pointer.read()
        checks["active_index"] = active_dir is not None
        checks["active_index_dir"] = active_dir or "none"
    except Exception as e:
        checks["active_index"] = False
        checks["active_index_error"] = str(e)

    # 3. FAISS index loadable (lightweight — just checks file exists)
    if active_dir:
        import os as _os
        faiss_path = _os.path.join(active_dir, "faiss.index").replace("\\", "/")
        chunks_path = _os.path.join(active_dir, "chunks.jsonl").replace("\\", "/")
        checks["faiss_index_exists"] = _os.path.exists(faiss_path)
        checks["chunks_file_exists"] = _os.path.exists(chunks_path)
    else:
        checks["faiss_index_exists"] = False
        checks["chunks_file_exists"] = False

    ready = all([
        checks.get("openai_api_key", False),
        checks.get("active_index", False),
        checks.get("faiss_index_exists", False),
        checks.get("chunks_file_exists", False),
    ])

    return JSONResponse(
        status_code=200 if ready else 503,
        content={"ready": ready, "checks": checks}
    )
