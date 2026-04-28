"""
Admin API routes for the PERA AI dashboard.

All endpoints live under ``/api/admin/*`` and require a valid Bearer token
issued by ``POST /api/admin/login``. See ``admin_auth.py`` for the token
format and credential storage.

Endpoints:

* ``POST   /api/admin/login``              — exchange email + password for a JWT.
* ``GET    /api/admin/documents``          — list current PDFs in assets/data with indexed status.
* ``POST   /api/admin/documents``          — upload a new PDF; triggers an index rebuild.
* ``DELETE /api/admin/documents/{name}``   — remove a PDF; triggers an index rebuild.
* ``GET    /api/admin/index-status``       — current active index build metadata.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from pydantic import BaseModel

from admin_auth import (
    check_rate_limit,
    issue_admin_token,
    require_admin,
    verify_credentials,
)
from doc_registry import scan_assets_data
from log_config import get_logger

log = get_logger("pera.admin_routes")

admin_router = APIRouter(prefix="/api/admin", tags=["admin"])

_MAX_UPLOAD_BYTES = 100 * 1024 * 1024  # 100 MB


# ---------------------------------------------------------------------------
# Lazy accessors — avoid circular imports with fastapi_app
# ---------------------------------------------------------------------------
def _get_indexer():
    import fastapi_app as _app
    return _app.indexer


def _get_data_dir() -> str:
    import fastapi_app as _app
    return _app.DATA_DIR


def _get_active_pointer_path() -> str:
    import fastapi_app as _app
    return _app.ACTIVE_POINTER_PATH


def _is_safe_path(abs_path: str) -> bool:
    import fastapi_app as _app
    return _app._is_safe_under_assets_data(abs_path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9 ._()\-]")


def _sanitize_filename(name: str) -> str:
    """Strip path components and unsafe characters, keep extension."""
    base = os.path.basename(name or "").strip()
    base = _SAFE_NAME_RE.sub("_", base)
    # Prevent leading dots (hidden file) and empty after sanitization.
    base = base.lstrip(". ")
    return base


def _file_sha256(path: str, chunk_bytes: int = 1024 * 1024) -> str:
    """Streamed SHA-256 of a file on disk. Returns hex digest."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while True:
                buf = f.read(chunk_bytes)
                if not buf:
                    break
                h.update(buf)
    except OSError:
        return ""
    return h.hexdigest()


def _index_existing_pdfs(data_dir: str) -> Dict[str, str]:
    """Return {sha256: filename} for every PDF currently in data_dir.
    Used for content-hash duplicate detection during upload."""
    out: Dict[str, str] = {}
    try:
        for name in os.listdir(data_dir):
            if not name.lower().endswith(".pdf"):
                continue
            path = os.path.join(data_dir, name)
            if not os.path.isfile(path):
                continue
            digest = _file_sha256(path)
            if digest:
                out[digest] = name
    except OSError:
        pass
    return out


def _load_active_manifest() -> Dict[str, Any]:
    """Read the manifest of the currently-active index build."""
    pointer_path = _get_active_pointer_path()
    try:
        with open(pointer_path, "r", encoding="utf-8") as f:
            pointer = json.load(f)
        build_dir = (pointer.get("active_index_dir") or "").replace("\\", "/")
        manifest_path = os.path.join(build_dir, "manifest.json")
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        manifest["_build_dir"] = build_dir
        return manifest
    except Exception:
        return {}


def _indexed_filenames_from_manifest(manifest: Dict[str, Any]) -> set:
    """Extract the set of filenames that are currently indexed.

    The manifest stores ``files`` as a dict keyed by filename with a
    descriptor value. Older builds may store it as a list of dicts — we
    handle both.
    """
    files = manifest.get("files") or {}
    names: set = set()
    if isinstance(files, dict):
        for key, entry in files.items():
            if isinstance(entry, dict):
                fn = entry.get("filename") or key or os.path.basename(entry.get("path", ""))
            else:
                fn = key
            if fn:
                names.add(fn)
    elif isinstance(files, list):
        for entry in files:
            if isinstance(entry, dict):
                fn = entry.get("filename") or os.path.basename(entry.get("path", ""))
                if fn:
                    names.add(fn)
    return names


def _manifest_doc_count(manifest: Dict[str, Any]) -> int:
    files = manifest.get("files") or {}
    if isinstance(files, (dict, list)):
        return len(files)
    return 0


def _manifest_built_at(manifest: Dict[str, Any]) -> int:
    """Return a best-effort unix timestamp for when the active build was made.

    Order: explicit top-level keys, then manifest file mtime.
    """
    for k in ("built_at", "created_at", "timestamp"):
        v = manifest.get(k)
        if isinstance(v, (int, float)) and v > 0:
            return int(v)
    build_dir = manifest.get("_build_dir") or ""
    mf = os.path.join(build_dir, "manifest.json")
    try:
        return int(os.path.getmtime(mf))
    except OSError:
        return 0


def _count_chunks_in_active_build(manifest: Dict[str, Any]) -> int:
    build_dir = manifest.get("_build_dir") or ""
    chunks_path = os.path.join(build_dir, "chunks.jsonl")
    if not os.path.exists(chunks_path):
        return 0
    try:
        # Count active rows only (rows may have active=False flag).
        active = 0
        with open(chunks_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if row.get("active", True):
                    active += 1
        return active
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------
class LoginBody(BaseModel):
    email: str
    password: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@admin_router.post("/login")
def admin_login(body: LoginBody, request: Request) -> Dict[str, Any]:
    client_ip = (
        request.client.host
        if request.client else request.headers.get("x-forwarded-for", "unknown")
    )
    if not check_rate_limit(client_ip):
        raise HTTPException(
            status_code=429,
            detail={"error": "rate_limited", "retry_after_seconds": 60},
        )
    if not verify_credentials(body.email, body.password):
        # Intentionally opaque to prevent user-enumeration.
        log.info("Admin login rejected for ip=%s", client_ip)
        raise HTTPException(
            status_code=401, detail={"error": "invalid_credentials"}
        )
    token, exp = issue_admin_token(body.email.strip())
    log.info("Admin login granted for ip=%s", client_ip)
    return {"token": token, "expires_at": exp}


# Tracks whether a background re-index is already in flight so
# concurrent GET /documents calls don't spawn multiple workers for
# the same pending set.
_reindex_in_flight: Dict[str, Any] = {"running": False, "started_at": 0.0, "last_error": ""}


def _kick_background_reindex() -> None:
    """Fire off the indexer in a daemon thread so the HTTP response
    returns immediately. Deduplicates concurrent kicks by checking an
    in-flight flag. Errors are captured into _reindex_in_flight so the
    next GET /documents can surface them."""
    import threading

    if _reindex_in_flight.get("running"):
        return
    _reindex_in_flight["running"] = True
    _reindex_in_flight["started_at"] = time.time()
    _reindex_in_flight["last_error"] = ""

    def _worker() -> None:
        try:
            indexer = _get_indexer()
            result = indexer.ensure_index_once() or {}
            err = result.get("error")
            if err:
                _reindex_in_flight["last_error"] = str(err)
                log.warning("Auto re-index for pending files failed: %s", err)
            else:
                # Clear any prior sticky error now that the worker completed
                # cleanly — previously this only cleared at the *start* of
                # the next kick, so transient validation failures stayed
                # surfaced on the admin dashboard forever.
                _reindex_in_flight["last_error"] = ""
                log.info(
                    "Auto re-index complete: changed=%s active_dir=%s",
                    result.get("changed"), result.get("active_index_dir"),
                )
        except Exception as exc:
            _reindex_in_flight["last_error"] = str(exc)
            log.error("Auto re-index worker crashed: %s", exc)
        finally:
            _reindex_in_flight["running"] = False

    threading.Thread(target=_worker, name="auto-reindex", daemon=True).start()


@admin_router.get("/documents")
def list_documents(claims: Dict[str, Any] = Depends(require_admin)) -> List[Dict[str, Any]]:
    data_dir = _get_data_dir()
    try:
        entries = scan_assets_data(data_dir)
    except Exception as exc:
        log.error("scan_assets_data failed: %s", exc)
        entries = []
    manifest = _load_active_manifest()
    indexed = _indexed_filenames_from_manifest(manifest)
    out: List[Dict[str, Any]] = []
    pending_count = 0
    for e in entries:
        is_indexed = e.get("filename") in indexed
        if not is_indexed:
            pending_count += 1
        out.append({
            "filename": e.get("filename"),
            "size": int(e.get("size", 0)),
            "mtime": int(e.get("mtime", 0)),
            "authority": int(e.get("authority", 0)),
            "rank": int(e.get("rank", 0)),
            "indexed": is_indexed,
        })
    # Auto-recovery — if any document is pending, kick off a background
    # re-index so a subsequent refresh sees it indexed (assuming the
    # underlying issue, e.g. a transient embedding-API failure, has
    # resolved). Response is returned immediately; the rebuild runs
    # asynchronously so this endpoint stays fast.
    if pending_count > 0:
        log.info(
            "Pending files detected (%d); kicking background re-index",
            pending_count,
        )
        _kick_background_reindex()
    return out


@admin_router.post("/documents")
async def upload_document(
    file: UploadFile = File(...),
    claims: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    # Basic content-type check (trust-but-verify with magic bytes below).
    if file.content_type and file.content_type not in (
        "application/pdf",
        "application/octet-stream",  # some browsers omit the MIME
    ):
        raise HTTPException(
            status_code=400,
            detail={"error": "unsupported_content_type", "got": file.content_type},
        )
    raw_name = file.filename or ""
    if not raw_name.lower().endswith(".pdf"):
        raise HTTPException(
            status_code=400, detail={"error": "only_pdf_allowed"}
        )
    safe = _sanitize_filename(raw_name)
    if not safe.lower().endswith(".pdf"):
        raise HTTPException(
            status_code=400, detail={"error": "only_pdf_allowed"}
        )

    data_dir = _get_data_dir()
    os.makedirs(data_dir, exist_ok=True)

    # Duplicate detection — Stage 1: reject on exact filename match.
    # If an admin uploads a file whose name already exists in the data
    # directory, we refuse instead of silently writing foo-1.pdf. The
    # front-end surfaces this to the user as a clear error.
    exact_path = os.path.join(data_dir, safe)
    if os.path.exists(exact_path):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "duplicate_filename",
                "message": (
                    f"A document named '{safe}' already exists. "
                    "Delete the existing copy first, or rename the new file."
                ),
                "existing_filename": safe,
            },
        )

    # Stage 2: write the file to a temporary sidecar path so we can hash
    # its content. If the content matches an existing PDF we delete the
    # sidecar and reject the upload — catches re-uploads of the same
    # PDF under a different filename.
    target_abs = os.path.abspath(exact_path)
    if not _is_safe_path(target_abs):
        raise HTTPException(status_code=400, detail={"error": "unsafe_path"})

    # Stream to disk with size cap + magic-byte check on first read.
    total = 0
    first_chunk = True
    sha = hashlib.sha256()
    try:
        with open(target_abs, "wb") as out_f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                if first_chunk:
                    if not chunk.startswith(b"%PDF-"):
                        out_f.close()
                        try:
                            os.remove(target_abs)
                        except OSError:
                            pass
                        raise HTTPException(
                            status_code=400,
                            detail={"error": "not_a_pdf"},
                        )
                    first_chunk = False
                total += len(chunk)
                if total > _MAX_UPLOAD_BYTES:
                    out_f.close()
                    try:
                        os.remove(target_abs)
                    except OSError:
                        pass
                    raise HTTPException(
                        status_code=413,
                        detail={"error": "file_too_large", "limit_bytes": _MAX_UPLOAD_BYTES},
                    )
                sha.update(chunk)
                out_f.write(chunk)
    except HTTPException:
        raise
    except Exception as exc:
        log.error("Upload failed for %s: %s", safe, exc)
        try:
            os.remove(target_abs)
        except OSError:
            pass
        raise HTTPException(status_code=500, detail={"error": "upload_failed"})

    uploaded_digest = sha.hexdigest()

    # Stage 2: compare content hash against all OTHER PDFs in the data
    # directory. If we find an identical file under a different name,
    # delete the new one and refuse the upload.
    existing = _index_existing_pdfs(data_dir)
    existing.pop(uploaded_digest, None)  # ignore ourselves — we just wrote
    # Rescan the dir to find any OTHER file with the same hash.
    other_match: Optional[str] = None
    for other_name in os.listdir(data_dir):
        if other_name == safe or not other_name.lower().endswith(".pdf"):
            continue
        other_path = os.path.join(data_dir, other_name)
        if not os.path.isfile(other_path):
            continue
        if _file_sha256(other_path) == uploaded_digest:
            other_match = other_name
            break
    if other_match:
        try:
            os.remove(target_abs)
        except OSError:
            pass
        raise HTTPException(
            status_code=409,
            detail={
                "error": "duplicate_content",
                "message": (
                    f"This document is identical to an existing file "
                    f"('{other_match}'). Duplicate uploads are not allowed."
                ),
                "existing_filename": other_match,
            },
        )

    log.info("Admin uploaded PDF: %s (%d bytes, sha256=%s)", safe, total, uploaded_digest[:12])

    # Fire-and-forget index rebuild — blocking the HTTP response on the
    # full OpenAI embedding round-trip used to hang the admin UI for ~2
    # minutes per upload. Now: kick a background daemon thread, return
    # immediately, and let the frontend poll /api/admin/index-status to
    # know when the rebuild has finished (reindex_running flips false).
    _kick_background_reindex()
    return {
        "filename": os.path.basename(target_abs),
        "size": total,
        "duration_ms": 0,
        "index": {
            "changed": True,
            "background": True,
            "status": "indexing",
            "message": "Document saved. Indexing started in the background — "
                       "poll /api/admin/index-status to track progress.",
        },
    }


@admin_router.delete("/documents/{filename:path}")
def delete_document(
    filename: str,
    claims: Dict[str, Any] = Depends(require_admin),
) -> Dict[str, Any]:
    name = unquote(filename or "").strip()
    if not name or "/" in name or "\\" in name or name.startswith("."):
        raise HTTPException(status_code=400, detail={"error": "bad_filename"})
    data_dir = _get_data_dir()
    target_abs = os.path.abspath(os.path.join(data_dir, name))
    if not _is_safe_path(target_abs):
        raise HTTPException(status_code=400, detail={"error": "unsafe_path"})
    if not os.path.exists(target_abs):
        raise HTTPException(status_code=404, detail={"error": "not_found"})
    try:
        os.remove(target_abs)
    except OSError as exc:
        log.error("Delete failed for %s: %s", name, exc)
        raise HTTPException(status_code=500, detail={"error": "delete_failed"})

    log.info("Admin deleted PDF: %s", name)

    # Fire-and-forget index rebuild — see upload_document for rationale.
    _kick_background_reindex()
    return {
        "filename": name,
        "duration_ms": 0,
        "index": {
            "changed": True,
            "background": True,
            "status": "indexing",
            "message": "Document removed. Re-indexing started in the "
                       "background — poll /api/admin/index-status to track.",
        },
    }


@admin_router.get("/index-status")
def index_status(claims: Dict[str, Any] = Depends(require_admin)) -> Dict[str, Any]:
    manifest = _load_active_manifest()
    build_dir = manifest.get("_build_dir") or ""
    # Include the most recent background re-index error (if any) so the
    # admin UI can show the actual reason a file is stuck pending
    # rather than silently looping forever.
    last_err = _reindex_in_flight.get("last_error") or ""

    # Defensive: if the active index is healthy (all scanned PDFs are in
    # the manifest and no re-index is currently running), an old "last
    # error" from a prior failed build is stale and would only confuse
    # the admin. Clear and hide it.
    try:
        indexed_names = _indexed_filenames_from_manifest(manifest)
        scanned_entries = scan_assets_data(_get_data_dir())
        pending_now = sum(
            1 for e in scanned_entries if e.get("filename") not in indexed_names
        )
    except Exception:
        pending_now = 0
    if (
        last_err
        and pending_now == 0
        and not _reindex_in_flight.get("running")
    ):
        _reindex_in_flight["last_error"] = ""
        last_err = ""
    # Truncate/clean the error for UI display — keep the important part
    # (e.g. "insufficient_quota") and drop noise.
    if last_err:
        if "insufficient_quota" in last_err:
            last_err_msg = (
                "OpenAI quota exceeded. Add billing at "
                "https://platform.openai.com/account/billing and refresh."
            )
        elif "rate_limit" in last_err.lower():
            last_err_msg = (
                "OpenAI rate-limit hit. Will retry automatically; "
                "refresh in a minute."
            )
        elif len(last_err) > 220:
            last_err_msg = last_err[:220] + "…"
        else:
            last_err_msg = last_err
    else:
        last_err_msg = ""

    return {
        "active_index_dir": build_dir,
        "built_at": _manifest_built_at(manifest),
        "doc_count": _manifest_doc_count(manifest),
        "chunk_count": _count_chunks_in_active_build(manifest),
        "reindex_running": bool(_reindex_in_flight.get("running")),
        "last_error": last_err_msg,
    }
