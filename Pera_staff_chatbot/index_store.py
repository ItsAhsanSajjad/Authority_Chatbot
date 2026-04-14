from __future__ import annotations

import os
import json
import time
import hashlib
import re
import random
from typing import List, Dict, Any, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import faiss  # type: ignore

from doc_registry import scan_assets_data, compare_with_manifest
from extractors import extract_units_from_files
from chunker import chunk_units, Chunk
from openai_clients import get_chat_client, require_api_key, OPENAI_BASE_URL, EMBEDDING_MODEL
from log_config import get_logger

log = get_logger("pera.index_store")


# -----------------------------
# Config (env-tunable)
# -----------------------------
# Note: OPENAI_API_KEY, OPENAI_BASE_URL, EMBEDDING_MODEL are now in openai_clients.py

# Versioning: bump when you change embed/search text construction logic
EMBED_TEXT_VERSION = int(os.getenv("EMBED_TEXT_VERSION", "4"))  # bumped: slim embed header
SEARCH_TEXT_VERSION = int(os.getenv("SEARCH_TEXT_VERSION", "2"))

# Rebuild if model changes or behavior changes
EMBED_MODEL_VERSION = int(os.getenv("EMBED_MODEL_VERSION", "1"))

MAX_EMBED_CHARS_PER_TEXT = int(os.getenv("MAX_EMBED_CHARS_PER_TEXT", "7000"))
MAX_EMBED_CHARS_PER_BATCH = int(os.getenv("MAX_EMBED_CHARS_PER_BATCH", "120000"))

DEFAULT_CHUNK_MAX_CHARS = int(os.getenv("CHUNK_MAX_CHARS", "4500"))
DEFAULT_CHUNK_OVERLAP_CHARS = int(os.getenv("CHUNK_OVERLAP_CHARS", "500"))  # increased from 350

FORCE_REBUILD_IF_INDEX_MISSING = os.getenv("FORCE_REBUILD_IF_INDEX_MISSING", "1").strip() != "0"

EMBED_RETRIES = int(os.getenv("EMBED_RETRIES", "4"))
EMBED_RETRY_BASE_SLEEP = float(os.getenv("EMBED_RETRY_BASE_SLEEP", "0.8"))

# Purge inactive vectors from FAISS (critical for recall correctness)
PURGE_INACTIVE_FROM_FAISS = os.getenv("PURGE_INACTIVE_FROM_FAISS", "1").strip() != "0"

# Auditability controls
STORE_EMBED_TEXT_PREVIEW = os.getenv("STORE_EMBED_TEXT_PREVIEW", "1").strip() != "0"
EMBED_TEXT_PREVIEW_CHARS = int(os.getenv("EMBED_TEXT_PREVIEW_CHARS", "1200"))

# If enabled, embed_texts() returns normalized vectors (recommended for IP/cosine search correctness)
NORMALIZE_EMBEDDINGS = os.getenv("NORMALIZE_EMBEDDINGS", "1").strip() != "0"


# -----------------------------
# Helpers: filesystem
# -----------------------------
def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _p(index_dir: str, name: str) -> str:
    return os.path.join(index_dir, name).replace("\\", "/")

def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out

def _rewrite_jsonl(path: str, rows: List[Dict[str, Any]]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, path)

def _sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8", errors="ignore")).hexdigest()

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _safe_default_fs_path(doc_name: str) -> str:
    doc_name = (doc_name or "").strip()
    if not doc_name:
        return ""
    return os.path.join("assets", "data", doc_name).replace("\\", "/")

def _canonical_public_path(doc_name: str) -> str:
    dn = (doc_name or "").strip()
    return f"/assets/data/{dn}".replace("\\", "/") if dn else "/assets/data"

def _basename(p: str) -> str:
    try:
        return os.path.basename((p or "").replace("\\", "/"))
    except Exception:
        return (p or "").split("/")[-1]

def _now() -> int:
    return int(time.time())


# -----------------------------
# FAISS helpers
# -----------------------------
def _new_idmap_index(dim: int) -> faiss.Index:
    base = faiss.IndexFlatIP(dim)
    return faiss.IndexIDMap2(base)

def _load_or_create_faiss(faiss_path: str, dim: int) -> faiss.Index:
    if os.path.exists(faiss_path):
        return faiss.read_index(faiss_path)
    return _new_idmap_index(dim)

def _save_faiss(index: faiss.Index, faiss_path: str) -> None:
    faiss.write_index(index, faiss_path)

def _normalize_vectors(v: np.ndarray) -> np.ndarray:
    if v.size == 0:
        return v
    norms = np.linalg.norm(v, axis=1, keepdims=True) + 1e-12
    return v / norms

def _faiss_supports_remove(idx: faiss.Index) -> bool:
    return hasattr(idx, "remove_ids")

def _remove_ids_from_faiss(idx: faiss.Index, ids: List[int]) -> Tuple[bool, str]:
    if not ids:
        return True, "no-ids"
    if not _faiss_supports_remove(idx):
        return False, "remove_ids not supported"
    try:
        arr = np.array(sorted(set(int(x) for x in ids)), dtype=np.int64)
        sel = faiss.IDSelectorBatch(arr.size, faiss.swig_ptr(arr))
        idx.remove_ids(sel)
        return True, f"removed={arr.size}"
    except Exception as e:
        return False, f"remove_ids failed: {e}"


# -----------------------------
# OpenAI embeddings (memory-safe batching + retries)
# -----------------------------
def _require_base_url() -> str:
    return OPENAI_BASE_URL or "https://api.openai.com/v1"

def _truncate_text_for_embedding(t: str) -> str:
    t = (t or "").strip()
    if len(t) <= MAX_EMBED_CHARS_PER_TEXT:
        return t
    return t[:MAX_EMBED_CHARS_PER_TEXT]

def embed_texts(texts: List[str]) -> np.ndarray:
    """
    Embeds texts using OpenAI client (or gateway via OPENAI_BASE_URL).
    Returns float32 numpy array [n, dim].
    By default, outputs are L2-normalized (NORMALIZE_EMBEDDINGS=1).
    """
    if not texts:
        return np.zeros((0, 1), dtype=np.float32)

    from openai import OpenAI

    client = get_chat_client()

    safe_texts = [_truncate_text_for_embedding(t) for t in texts]
    n_total = len(safe_texts)

    dim: Optional[int] = None
    out: Optional[np.ndarray] = None
    cursor = 0

    # Use env-controlled batch char budget
    max_batch_chars = max(5000, int(MAX_EMBED_CHARS_PER_BATCH or 0))

    def _sleep(attempt: int) -> None:
        # exponential backoff + jitter
        base = EMBED_RETRY_BASE_SLEEP * (2 ** attempt)
        time.sleep(base + random.uniform(0.0, 0.25))

    def _call_embeddings(inp: List[str]) -> List[List[float]]:
        last_err: Optional[Exception] = None
        for attempt in range(max(1, EMBED_RETRIES)):
            try:
                resp = client.embeddings.create(model=EMBEDDING_MODEL, input=inp)
                return [d.embedding for d in resp.data]
            except Exception as e:
                last_err = e
                log.debug("embed_texts attempt=%d/%d error=%s", attempt + 1, EMBED_RETRIES, e)
                _sleep(attempt)
        raise RuntimeError(f"Embedding call failed after retries: {last_err}")

    batch: List[str] = []
    batch_chars = 0

    def flush() -> None:
        nonlocal batch, batch_chars, dim, out, cursor
        if not batch:
            return
        embs = _call_embeddings(batch)
        if not embs:
            batch = []
            batch_chars = 0
            return

        if dim is None:
            dim = len(embs[0])
            out = np.empty((n_total, dim), dtype=np.float32)

        arr = np.asarray(embs, dtype=np.float32)
        if out is not None:
            out[cursor:cursor + arr.shape[0], :] = arr
        cursor += arr.shape[0]

        batch = []
        batch_chars = 0

    for t in safe_texts:
        tlen = len(t)

        # If a single text exceeds batch budget, embed alone
        if tlen >= max_batch_chars:
            flush()
            embs = _call_embeddings([t])
            if embs:
                if dim is None:
                    dim = len(embs[0])
                    out = np.empty((n_total, dim), dtype=np.float32)
                arr = np.asarray(embs, dtype=np.float32)
                if out is not None:
                    out[cursor:cursor + 1, :] = arr
                cursor += 1
            continue

        if batch and (batch_chars + tlen > max_batch_chars):
            flush()

        batch.append(t)
        batch_chars += tlen

    flush()

    if out is None or dim is None:
        return np.zeros((0, 1), dtype=np.float32)

    if cursor != n_total:
        out = out[:cursor, :]

    # IMPORTANT: normalize here to make inner-product == cosine similarity
    if NORMALIZE_EMBEDDINGS:
        out = _normalize_vectors(out)

    return out


# -----------------------------
# Evidence quality: skip junk chunks at ingestion (but keep key short signals)
# -----------------------------
_PAGE_GARBAGE_RE = re.compile(r"^\s*page\s*\d+\s*(of\s*\d+)?\s*$", re.I)
_ONLY_NUM_PUNCT_RE = re.compile(r"^[\s0-9\-–—_.,:;|/\\()]+$")

def _count_letters(s: str) -> int:
    return len(re.findall(r"[A-Za-z\u0600-\u06FF]", s or ""))

def _count_words(s: str) -> int:
    return len(re.findall(r"[A-Za-z\u0600-\u06FF]{2,}", s or ""))

def _force_keep_short_signal(t: str) -> bool:
    tl = (t or "").lower()
    if "schedule" in tl or "annex" in tl or "appendix" in tl:
        return True
    if "punjab enforcement and regulatory authority" in tl or re.search(r"\bpera\b", tl):
        return True
    if "terms of reference" in tl or re.search(r"\btor\b", tl):
        return True
    if "chief technology officer" in tl or re.search(r"\bcto\b", tl):
        return True
    return False

def _is_low_signal_chunk(txt: str) -> bool:
    t = (txt or "").strip()
    if not t:
        return True
    if _force_keep_short_signal(t):
        return False
    if _PAGE_GARBAGE_RE.match(t):
        return True
    if len(t) <= 16 and _ONLY_NUM_PUNCT_RE.match(t):
        return True
    if len(t) < 60 and _count_words(t) < 6 and _count_letters(t) < 25:
        return True
    return False


# -----------------------------
# Embedding/search text builders
# -----------------------------
def _loc_label(loc_kind: Any, loc_start: Any, loc_end: Any) -> str:
    lk = (loc_kind or "").strip()
    if lk == "page" and loc_start is not None:
        try:
            ps = int(loc_start)
            pe = int(loc_end) if loc_end is not None else ps
            return f"Page {ps}" if ps == pe else f"Pages {ps}-{pe}"
        except Exception:
            return f"Page {loc_start}"
    if loc_start is not None:
        return str(loc_start)
    return ""

_PERA_IDENTITY_LINE = (
    "ENTITY: PERA (Punjab Enforcement and Regulatory Authority, Punjab). "
    "ALIASES: pira, perra, peera, peraa, pehra. "
    "TOPICS: enforcement, regulation, scheduled laws, complaints, hearings, recruitment, HR, discipline, contracts."
)

_TAG_PATTERNS: List[Tuple[str, str]] = [
    (r"\bschedule\b", "schedule"),
    (r"\bscheduled\b", "scheduled"),
    (r"\bannex(ure)?\b", "annexure"),
    (r"\bappendix\b", "appendix"),
    (r"\btable\b", "table"),

    (r"\bshall\s+consist\b", "composition"),
    (r"\bconsist\s+of\b", "composition"),
    (r"\bcomposition\b", "composition"),
    (r"\bconstitution\b", "constitution"),
    (r"\bmember(s)?\b", "members"),
    (r"\bchairperson\b", "chairperson"),
    (r"\bvice\s+chairperson\b", "vice chairperson"),
    (r"\bsecretary\b", "secretary"),
    (r"\bdirector\s+general\b", "director general"),

    (r"\bcomplaint(s)?\b", "complaint"),
    (r"\bpublic\s+complaint(s)?\b", "public complaint"),
    (r"\bhearing(s)?\b", "hearing"),
    (r"\bhearing\s+officer\b", "hearing officer"),
    (r"\bgrievance\b", "grievance"),
    (r"\bappeal\b", "appeal"),
    (r"\bprocedure\b", "procedure"),
    (r"\bprocess\b", "process"),
    (r"\bhow\s+to\b", "how to"),

    (r"\bpurpose\b", "purpose"),
    (r"\bobjective(s)?\b", "objectives"),
    (r"\bfunction(s)?\b", "functions"),
    (r"\bmandate\b", "mandate"),
    (r"\bestablished\s+to\b", "established to"),
    (r"\bvision\b", "vision"),
    (r"\bmission\b", "mission"),

    (r"\brecruitment\b", "recruitment"),
    (r"\beligibil", "eligibility"),
    (r"\bqualification(s)?\b", "qualification"),
    (r"\bexperience\b", "experience"),
    (r"\bcontract(ual)?\b", "contract"),
    (r"\bprobation\b", "probation"),
    (r"\btermination\b", "termination"),
    (r"\bdisciplin", "discipline"),
    (r"\bmisconduct\b", "misconduct"),

    (r"\bfaq\b", "faq"),
    (r"\bfrequently\s+asked\b", "faq"),
    (r"\bquestion(s)?\b", "questions"),
    (r"\banswer(s)?\b", "answers"),
]

def _derive_search_tags(raw_text: str) -> List[str]:
    t = (raw_text or "")
    tl = t.lower()
    tags: List[str] = []

    if "pera" in tl or "punjab enforcement" in tl or "enforcement and regulatory" in tl:
        tags.append("pera")

    for pat, tag in _TAG_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            tags.append(tag)

    seen = set()
    out: List[str] = []
    for x in tags:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def _build_embed_text_from_parts(
    doc_name: str,
    doc_rank: Any,
    source_type: str,
    loc_kind: Any,
    loc_start: Any,
    loc_end: Any,
    raw_text: str,
) -> str:
    dn = (doc_name or "Unknown document").strip()
    stype = (source_type or "").strip()
    loc = _loc_label(loc_kind, loc_start, loc_end)
    rank = str(doc_rank) if doc_rank is not None else "0"

    body = (raw_text or "").strip()
    tags = _derive_search_tags(body)
    tags_line = f"TAGS: {', '.join(tags)}" if tags else "TAGS:"

    header = (
        f"DOCUMENT: {dn} | {loc}\n"
        f"{tags_line}\n"
    )
    return (header + "\n" + body).strip()

def _build_embed_text_for_chunk(ch: Chunk) -> str:
    return _build_embed_text_from_parts(
        getattr(ch, "doc_name", "Unknown document"),
        getattr(ch, "doc_rank", 0),
        getattr(ch, "source_type", ""),
        getattr(ch, "loc_kind", ""),
        getattr(ch, "loc_start", None),
        getattr(ch, "loc_end", None),
        getattr(ch, "chunk_text", "") or "",
    )

def _build_embed_text_for_row(r: Dict[str, Any]) -> str:
    return _build_embed_text_from_parts(
        r.get("doc_name", "Unknown document"),
        r.get("doc_rank", 0),
        r.get("source_type", ""),
        r.get("loc_kind", ""),
        r.get("loc_start"),
        r.get("loc_end"),
        r.get("text", "") or "",
    )

def _build_search_text_from_parts(
    doc_name: str,
    source_type: str,
    loc_kind: Any,
    loc_start: Any,
    loc_end: Any,
    raw_text: str,
) -> str:
    dn = (doc_name or "Unknown document").strip()
    stype = (source_type or "").strip()
    loc = _loc_label(loc_kind, loc_start, loc_end)
    body = (raw_text or "").strip()

    tags = _derive_search_tags(body)
    tags_line = f"TAGS: {', '.join(tags)}" if tags else "TAGS:"

    header = f"DOC: {dn}\nTYPE: {stype}\nLOC: {loc}\n{tags_line}\n"
    return (header + "\n" + body).strip()

def _build_search_text_for_chunk(ch: Chunk) -> str:
    return _build_search_text_from_parts(
        getattr(ch, "doc_name", "Unknown document"),
        getattr(ch, "source_type", ""),
        getattr(ch, "loc_kind", ""),
        getattr(ch, "loc_start", None),
        getattr(ch, "loc_end", None),
        getattr(ch, "chunk_text", "") or "",
    )

def _build_search_text_for_row(r: Dict[str, Any]) -> str:
    return _build_search_text_from_parts(
        r.get("doc_name", "Unknown document"),
        r.get("source_type", ""),
        r.get("loc_kind", ""),
        r.get("loc_start"),
        r.get("loc_end"),
        r.get("text", "") or "",
    )


def _needs_embed_version_rebuild(rows: List[Dict[str, Any]]) -> bool:
    for r in rows:
        if not r.get("active", True):
            continue
        v = r.get("embed_text_version")
        if v is None or int(v) != int(EMBED_TEXT_VERSION):
            return True
    return False

def _needs_search_version_rebuild(rows: List[Dict[str, Any]]) -> bool:
    for r in rows:
        if not r.get("active", True):
            continue
        v = r.get("search_text_version")
        if v is None or int(v) != int(SEARCH_TEXT_VERSION):
            return True
    return False

def _needs_model_rebuild(rows: List[Dict[str, Any]]) -> bool:
    for r in rows:
        if not r.get("active", True):
            continue
        if (r.get("embedding_model") or "").strip() != EMBEDDING_MODEL:
            return True
        try:
            if int(r.get("embed_model_version", 0) or 0) != int(EMBED_MODEL_VERSION):
                return True
        except Exception:
            return True
    return False


# -----------------------------
# Chunk store / id assignment
# -----------------------------
def _next_chunk_id(existing_rows: List[Dict[str, Any]]) -> int:
    if not existing_rows:
        return 1
    best = 0
    for r in existing_rows:
        try:
            best = max(best, int(r.get("id", 0) or 0))
        except Exception:
            continue
    return best + 1

def _mark_inactive_for_doc(rows: List[Dict[str, Any]], doc_name: str) -> List[int]:
    ids: List[int] = []
    now = _now()
    for r in rows:
        if r.get("doc_name") == doc_name and r.get("active", True):
            r["active"] = False
            r["deactivated_at"] = now
            try:
                ids.append(int(r.get("id")))
            except Exception:
                pass
    return ids


# -----------------------------
# Manifest builder for cold-start rebuild
# -----------------------------
def _build_manifest_from_scanned(scanned: List[Dict[str, Any]]) -> Dict[str, Any]:
    files_map: Dict[str, Dict[str, Any]] = {}
    for e in scanned:
        try:
            sha = _sha256_file(e["path"])
        except Exception:
            sha = ""
        files_map[e["filename"]] = {
            "filename": e["filename"],
            "path": e["path"],
            "ext": e.get("ext", ""),
            "mtime": int(e.get("mtime", 0) or 0),
            "size": int(e.get("size", 0) or 0),
            "rank": int(e.get("rank", 0) or 0),
            "sha256": sha,
        }

    return {
        "version": 2,
        "build": {
            "embedding_model": EMBEDDING_MODEL,
            "embed_model_version": int(EMBED_MODEL_VERSION),
            "embed_text_version": int(EMBED_TEXT_VERSION),
            "search_text_version": int(SEARCH_TEXT_VERSION),
            "chunk_max_chars": int(DEFAULT_CHUNK_MAX_CHARS),
            "chunk_overlap_chars": int(DEFAULT_CHUNK_OVERLAP_CHARS),
        },
        "files": files_map,
    }


# -----------------------------
# Main: scan + incremental ingest
# -----------------------------
def scan_and_ingest_if_needed(
    data_dir: str = "assets/data",
    index_dir: str = "assets/index",
    manifest_name: str = "manifest.json",
    chunk_max_chars: int = DEFAULT_CHUNK_MAX_CHARS,
    chunk_overlap_chars: int = DEFAULT_CHUNK_OVERLAP_CHARS,
) -> Dict[str, Any]:
    _safe_mkdir(index_dir)

    faiss_path = _p(index_dir, "faiss.index")
    chunks_path = _p(index_dir, "chunks.jsonl")
    manifest_path = _p(index_dir, "manifest.json")

    scanned = scan_assets_data(data_dir=data_dir)
    rows = _read_jsonl(chunks_path)

    chunks_missing_or_empty = (not os.path.exists(chunks_path)) or (os.path.getsize(chunks_path) == 0)
    faiss_missing = not os.path.exists(faiss_path)
    cold_start = FORCE_REBUILD_IF_INDEX_MISSING and (faiss_missing or chunks_missing_or_empty)

    idx: Optional[faiss.Index] = None
    if os.path.exists(faiss_path):
        try:
            idx = faiss.read_index(faiss_path)
        except Exception as e:
            log.warning("Failed to read existing FAISS index at %s: %s", faiss_path, e)
            idx = None

    unchanged: List[Dict[str, Any]] = []
    removed: List[Dict[str, Any]] = []
    new_or_changed: List[Dict[str, Any]] = []

    chunks_deactivated = 0
    removed_ids_total: List[int] = []

    if cold_start:
        new_or_changed = list(scanned)
        updated_manifest = _build_manifest_from_scanned(scanned)
        rows = []
        start_id = 1
    else:
        new_or_changed, unchanged, removed, updated_manifest = compare_with_manifest(
            scanned=scanned,
            index_dir=index_dir,
            manifest_name=manifest_name,
            compute_hash=True
        )

        # Rebuild if embed/search/model changed (prevents silent mismatch)
        if os.path.exists(faiss_path) and rows and (
            _needs_embed_version_rebuild(rows) or _needs_search_version_rebuild(rows) or _needs_model_rebuild(rows)
        ):
            rebuilt = rebuild_index_from_chunks(index_dir=index_dir)
            idx = rebuilt["index"]
            rows = _read_jsonl(chunks_path)

        start_id = _next_chunk_id(rows)

        # Deactivate removed docs (collect IDs for purge)
        for r in removed:
            docname = r.get("filename") or r.get("name")
            if docname:
                ids = _mark_inactive_for_doc(rows, docname)
                removed_ids_total.extend(ids)
                chunks_deactivated += len(ids)

    # Deactivate changed docs (old versions) too
    changed_files = [e["path"] for e in new_or_changed]
    changed_names = [e["filename"] for e in new_or_changed]

    for doc in changed_names:
        ids = _mark_inactive_for_doc(rows, doc)
        removed_ids_total.extend(ids)
        chunks_deactivated += len(ids)

    # Purge inactive IDs from FAISS
    purge_note = ""
    if PURGE_INACTIVE_FROM_FAISS and removed_ids_total:
        if idx is None and os.path.exists(faiss_path):
            try:
                idx = faiss.read_index(faiss_path)
            except Exception as e:
                log.warning("Failed to re-read FAISS index for purge at %s: %s", faiss_path, e)
                idx = None

        if idx is not None:
            ok, msg = _remove_ids_from_faiss(idx, removed_ids_total)
            purge_note = msg
            if not ok:
                rebuilt = rebuild_index_from_chunks(index_dir=index_dir)
                idx = rebuilt["index"]
                rows = _read_jsonl(chunks_path)
                start_id = _next_chunk_id(rows)
                purge_note = "rebuild_due_to_purge_failure"

            try:
                _save_faiss(idx, faiss_path)
            except Exception as e:
                log.error("Failed to save FAISS index after purge at %s: %s", faiss_path, e, exc_info=True)

    # Early return if nothing changed
    if (not cold_start) and (not new_or_changed) and os.path.exists(faiss_path):
        _rewrite_jsonl(chunks_path, rows)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(updated_manifest, f, ensure_ascii=False, indent=2)

        out = {
            "found": len(scanned),
            "new_or_changed": 0,
            "unchanged": len(unchanged),
            "removed": len(removed),
            "chunks_added": 0,
            "chunks_deactivated": chunks_deactivated,
            "faiss_vectors_added": 0,
            "cold_start_rebuild": False,
        }
        if purge_note:
            out["purge_note"] = purge_note
        return out

    # Extract + chunk only changed docs
    units = extract_units_from_files(changed_files)

    rank_map = {e["filename"]: int(e.get("rank", 0) or 0) for e in new_or_changed}
    authority_map = {e["filename"]: int(e.get("authority", 2) or 2) for e in new_or_changed}
    for u in units:
        try:
            u.doc_rank = rank_map.get(u.doc_name, 0)
            u.doc_authority = authority_map.get(u.doc_name, 2)
        except Exception:
            pass

    chunks: List[Chunk] = chunk_units(
        units,
        max_chars=chunk_max_chars,
        overlap_chars=chunk_overlap_chars
    )

    embed_text_list: List[str] = []
    kept_chunks: List[Chunk] = []

    for c in chunks:
        raw = (c.chunk_text or "").strip()
        if not raw:
            continue
        if _is_low_signal_chunk(raw):
            continue
        kept_chunks.append(c)
        embed_text_list.append(_build_embed_text_for_chunk(c))

    # If no usable chunks, still persist manifest + rows
    if not embed_text_list:
        _rewrite_jsonl(chunks_path, rows)
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(updated_manifest, f, ensure_ascii=False, indent=2)

        out = {
            "found": len(scanned),
            "new_or_changed": len(new_or_changed),
            "unchanged": 0 if cold_start else len(unchanged),
            "removed": 0 if cold_start else len(removed),
            "chunks_added": 0,
            "chunks_deactivated": chunks_deactivated,
            "faiss_vectors_added": 0,
            "cold_start_rebuild": bool(cold_start),
            "note": "No usable chunks extracted (scanned PDFs or extractor returned empty text).",
        }
        if purge_note:
            out["purge_note"] = purge_note
        return out

    # Embed (embed_texts already normalizes if NORMALIZE_EMBEDDINGS=1)
    vectors = embed_texts(embed_text_list)
    dim = int(vectors.shape[1]) if vectors.ndim == 2 else 1

    # Load/create FAISS
    if idx is None:
        idx = _load_or_create_faiss(faiss_path, dim)

    # Dimension mismatch safety
    if getattr(idx, "d", None) != dim:
        rebuilt = rebuild_index_from_chunks(index_dir=index_dir)
        idx = rebuilt["index"]
        rows = _read_jsonl(chunks_path)
        start_id = _next_chunk_id(rows)

    # Add new vectors with stable IDs
    ids = np.arange(start_id, start_id + len(kept_chunks), dtype=np.int64)
    idx.add_with_ids(vectors, ids)

    now = _now()
    new_rows: List[Dict[str, Any]] = []

    for cid, ch, et in zip(ids.tolist(), kept_chunks, embed_text_list):
        t = (ch.chunk_text or "").strip()
        if not t or _is_low_signal_chunk(t):
            continue

        doc_name = getattr(ch, "doc_name", "Unknown document")
        public_path = _canonical_public_path(doc_name)

        raw_fs_path = (getattr(ch, "path", "") or "").strip()
        if not raw_fs_path:
            raw_fs_path = _safe_default_fs_path(doc_name)
        raw_fs_path = raw_fs_path.replace("\\", "/")

        loc_kind = getattr(ch, "loc_kind", "") or ""
        loc_start = getattr(ch, "loc_start", None)
        loc_end = getattr(ch, "loc_end", None)
        if loc_kind == "page":
            try:
                if loc_start is not None:
                    loc_start = int(loc_start)
                if loc_end is not None:
                    loc_end = int(loc_end)
            except Exception:
                pass

        search_text = _build_search_text_for_chunk(ch)

        row: Dict[str, Any] = {
            "id": int(cid),
            "active": True,
            "created_at": now,

            "doc_name": doc_name,
            "doc_rank": int(getattr(ch, "doc_rank", 0) or 0),
            "doc_authority": int(getattr(ch, "doc_authority", 2) or 2),
            "source_type": getattr(ch, "source_type", ""),

            "loc_kind": loc_kind,
            "loc_start": loc_start,
            "loc_end": loc_end,

            "public_path": public_path,
            "path": raw_fs_path,

            "text": t,
            "text_sha256": _sha256_text(t),
            "text_clean_sha256": _sha256_text(re.sub(r"\s+", " ", t).strip()),

            "embedding_model": EMBEDDING_MODEL,
            "embed_model_version": int(EMBED_MODEL_VERSION),

            "embed_text_version": int(EMBED_TEXT_VERSION),
            "embed_text_sha256": _sha256_text(et),

            "search_text_version": int(SEARCH_TEXT_VERSION),
            "search_text_sha256": _sha256_text(search_text),
            "search_text": search_text,
        }

        if STORE_EMBED_TEXT_PREVIEW:
            row["embed_text_preview"] = et[:max(100, EMBED_TEXT_PREVIEW_CHARS)]

        new_rows.append(row)

    rows.extend(new_rows)
    _rewrite_jsonl(chunks_path, rows)
    _save_faiss(idx, faiss_path)

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(updated_manifest, f, ensure_ascii=False, indent=2)

    out = {
        "found": len(scanned),
        "new_or_changed": len(new_or_changed),
        "unchanged": 0 if cold_start else len(unchanged),
        "removed": 0 if cold_start else len(removed),
        "chunks_added": len(new_rows),
        "chunks_deactivated": chunks_deactivated,
        "faiss_vectors_added": len(new_rows),
        "cold_start_rebuild": bool(cold_start),
    }
    if purge_note:
        out["purge_note"] = purge_note
    return out


def rebuild_index_from_chunks(index_dir: str = "assets/index") -> Dict[str, Any]:
    """
    Full rebuild from chunks.jsonl active rows.
    Safety hatch when purge fails or dim mismatch occurs.
    """
    faiss_path = _p(index_dir, "faiss.index")
    chunks_path = _p(index_dir, "chunks.jsonl")

    rows = _read_jsonl(chunks_path)
    active = [r for r in rows if r.get("active", True)]

    if not active:
        try:
            if os.path.exists(faiss_path):
                os.remove(faiss_path)
        except Exception:
            pass
        empty_idx = _new_idmap_index(1)
        _save_faiss(empty_idx, faiss_path)
        return {"index": empty_idx, "rebuilt": True, "count": 0}

    embed_texts_list = [_build_embed_text_for_row(r) for r in active]
    vectors = embed_texts(embed_texts_list)
    dim = int(vectors.shape[1])

    idx = _new_idmap_index(dim)
    ids = np.array([int(r["id"]) for r in active], dtype=np.int64)
    idx.add_with_ids(vectors, ids)
    _save_faiss(idx, faiss_path)

    changed = False
    for r, et in zip(active, embed_texts_list):
        if (r.get("embedding_model") or "").strip() != EMBEDDING_MODEL:
            r["embedding_model"] = EMBEDDING_MODEL
            changed = True
        if int(r.get("embed_model_version", 0) or 0) != int(EMBED_MODEL_VERSION):
            r["embed_model_version"] = int(EMBED_MODEL_VERSION)
            changed = True

        new_et_sha = _sha256_text(et)
        if int(r.get("embed_text_version", -1)) != int(EMBED_TEXT_VERSION) or (r.get("embed_text_sha256") or "") != new_et_sha:
            r["embed_text_version"] = int(EMBED_TEXT_VERSION)
            r["embed_text_sha256"] = new_et_sha
            if STORE_EMBED_TEXT_PREVIEW:
                r["embed_text_preview"] = et[:max(100, EMBED_TEXT_PREVIEW_CHARS)]
            changed = True

        stxt = _build_search_text_for_row(r)
        new_st_sha = _sha256_text(stxt)
        if int(r.get("search_text_version", -1)) != int(SEARCH_TEXT_VERSION) or (r.get("search_text_sha256") or "") != new_st_sha:
            r["search_text_version"] = int(SEARCH_TEXT_VERSION)
            r["search_text_sha256"] = new_st_sha
            r["search_text"] = stxt
            changed = True

        dn = (r.get("doc_name") or "").strip()
        if dn and not (r.get("public_path") or "").startswith("/assets/data/"):
            r["public_path"] = _canonical_public_path(dn)
            changed = True

    if changed:
        _rewrite_jsonl(chunks_path, rows)

    return {"index": idx, "rebuilt": True, "count": len(active)}


def load_index_and_chunks(index_dir: str = "assets/index") -> Tuple[Optional[faiss.Index], List[Dict[str, Any]]]:
    faiss_path = _p(index_dir, "faiss.index")
    chunks_path = _p(index_dir, "chunks.jsonl")

    idx = None
    if os.path.exists(faiss_path):
        try:
            idx = faiss.read_index(faiss_path)
        except Exception:
            idx = None

    rows = _read_jsonl(chunks_path)
    return idx, rows


# ---------------------------------------------------------------------------
# API Chunk Integration (Phase 3)
# Inserts/removes API-sourced chunks into the shared JSONL+FAISS index
# ---------------------------------------------------------------------------

def _build_api_embed_text(chunk_row: Dict[str, Any]) -> str:
    """Build embed text for an API chunk, compatible with doc embed format."""
    display_name = chunk_row.get("doc_name", "API Source")
    record_type = chunk_row.get("record_type", "")
    record_id = chunk_row.get("record_id", "")
    text = (chunk_row.get("text", "") or "").strip()

    tags = _derive_search_tags(text)
    api_tags = chunk_row.get("api_tags", [])
    for t in api_tags:
        if t not in tags:
            tags.append(t)
    tags_line = f"TAGS: {', '.join(tags)}" if tags else "TAGS:"

    loc_label = f"Record: {record_id}" if record_id else "API Record"
    if record_type:
        loc_label = f"{record_type.title()}: {record_id}" if record_id else record_type.title()

    header = (
        f"DOCUMENT: {display_name} | {loc_label}\n"
        f"{tags_line}\n"
    )
    return (header + "\n" + text).strip()


def _build_api_search_text(chunk_row: Dict[str, Any]) -> str:
    """Build search text for an API chunk."""
    display_name = chunk_row.get("doc_name", "API Source")
    record_type = chunk_row.get("record_type", "")
    record_id = chunk_row.get("record_id", "")
    text = (chunk_row.get("text", "") or "").strip()

    tags = _derive_search_tags(text)
    api_tags = chunk_row.get("api_tags", [])
    for t in api_tags:
        if t not in tags:
            tags.append(t)
    tags_line = f"TAGS: {', '.join(tags)}" if tags else "TAGS:"

    loc_label = f"Record: {record_id}" if record_id else "API Record"

    header = f"DOC: {display_name}\nTYPE: api\nLOC: {loc_label}\n{tags_line}\n"
    return (header + "\n" + text).strip()


def upsert_api_chunks(
    index_dir: str,
    api_chunks: list,
    source_id: str = "",
) -> Dict[str, Any]:
    """
    Embed and insert API chunks into the shared JSONL+FAISS index.

    Args:
        index_dir: Active index directory containing faiss.index + chunks.jsonl
        api_chunks: List of ApiChunk objects from api_chunker.py
        source_id: Source ID for logging

    Returns:
        Dict with counts: chunks_added, chunks_deactivated, vectors_added
    """
    _safe_mkdir(index_dir)
    faiss_path = _p(index_dir, "faiss.index")
    chunks_path = _p(index_dir, "chunks.jsonl")

    rows = _read_jsonl(chunks_path)
    idx = _load_or_create_faiss(faiss_path, 1)  # dim updated on first embed

    # Deactivate old chunks for the same source+records being upserted
    record_ids_inserting = {c.record_id for c in api_chunks}
    deactivated_ids: List[int] = []

    for r in rows:
        if not r.get("active", True):
            continue
        if r.get("api_source_id") == source_id and r.get("record_id") in record_ids_inserting:
            r["active"] = False
            r["deactivated_at"] = _now()
            try:
                deactivated_ids.append(int(r["id"]))
            except (KeyError, ValueError):
                pass

    # Purge deactivated from FAISS
    if deactivated_ids and PURGE_INACTIVE_FROM_FAISS:
        _remove_ids_from_faiss(idx, deactivated_ids)

    if not api_chunks:
        _rewrite_jsonl(chunks_path, rows)
        _save_faiss(idx, faiss_path)
        return {"chunks_added": 0, "chunks_deactivated": len(deactivated_ids), "vectors_added": 0}

    # Build rows and embed texts
    start_id = _next_chunk_id(rows)
    now = _now()
    new_rows: List[Dict[str, Any]] = []
    embed_texts_list: List[str] = []

    for i, chunk in enumerate(api_chunks):
        cid = start_id + i

        chunk_row: Dict[str, Any] = {
            "id": cid,
            "active": True,
            "created_at": now,

            "doc_name": chunk.display_name or chunk.display_title or f"API: {chunk.source_id}",
            "doc_rank": 0,
            "doc_authority": chunk.authority,
            "source_type": "api",

            "loc_kind": "record",
            "loc_start": chunk.record_id,
            "loc_end": chunk.record_id,

            "public_path": "",
            "path": "",

            "text": chunk.chunk_text,
            "text_sha256": _sha256_text(chunk.chunk_text),
            "text_clean_sha256": _sha256_text(re.sub(r"\s+", " ", chunk.chunk_text).strip()),

            "embedding_model": EMBEDDING_MODEL,
            "embed_model_version": int(EMBED_MODEL_VERSION),
            "embed_text_version": int(EMBED_TEXT_VERSION),
            "search_text_version": int(SEARCH_TEXT_VERSION),

            # API-specific metadata
            "api_source_id": chunk.source_id,
            "record_id": chunk.record_id,
            "record_type": chunk.record_type,
            "chunk_index": chunk.chunk_index,
            "api_chunk_id": chunk.chunk_id,
            "api_tags": list(chunk.tags),
        }

        et = _build_api_embed_text(chunk_row)
        st = _build_api_search_text(chunk_row)

        chunk_row["embed_text_sha256"] = _sha256_text(et)
        chunk_row["search_text_sha256"] = _sha256_text(st)
        chunk_row["search_text"] = st

        if STORE_EMBED_TEXT_PREVIEW:
            chunk_row["embed_text_preview"] = et[:max(100, EMBED_TEXT_PREVIEW_CHARS)]

        new_rows.append(chunk_row)
        embed_texts_list.append(et)

    # Embed all chunk texts
    try:
        vectors = embed_texts(embed_texts_list)
    except Exception as e:
        log.error("API chunk embedding failed for %s: %s", source_id, e)
        # Don't corrupt index — just save deactivation and return
        _rewrite_jsonl(chunks_path, rows)
        _save_faiss(idx, faiss_path)
        return {
            "chunks_added": 0,
            "chunks_deactivated": len(deactivated_ids),
            "vectors_added": 0,
            "error": str(e),
        }

    dim = int(vectors.shape[1]) if vectors.ndim == 2 else 1

    # Rebuild FAISS if dimension mismatch
    if getattr(idx, "d", None) != dim:
        idx = _new_idmap_index(dim)
        # Re-add all active rows
        active_existing = [r for r in rows if r.get("active", True)]
        if active_existing:
            existing_texts = [_build_embed_text_for_row(r) for r in active_existing]
            existing_vecs = embed_texts(existing_texts)
            existing_ids = np.array([int(r["id"]) for r in active_existing], dtype=np.int64)
            idx.add_with_ids(existing_vecs, existing_ids)

    # Add API chunk vectors
    ids = np.arange(start_id, start_id + len(new_rows), dtype=np.int64)
    idx.add_with_ids(vectors, ids)

    rows.extend(new_rows)
    _rewrite_jsonl(chunks_path, rows)
    _save_faiss(idx, faiss_path)

    log.info("Upserted %d API chunks for source %s", len(new_rows), source_id)
    return {
        "chunks_added": len(new_rows),
        "chunks_deactivated": len(deactivated_ids),
        "vectors_added": len(new_rows),
    }


def delete_chunks_by_source(
    index_dir: str,
    source_id: str,
    source_type: str = "api",
) -> Dict[str, Any]:
    """Remove all chunks for a given API source from JSONL+FAISS."""
    faiss_path = _p(index_dir, "faiss.index")
    chunks_path = _p(index_dir, "chunks.jsonl")

    rows = _read_jsonl(chunks_path)
    deactivated_ids: List[int] = []

    for r in rows:
        if not r.get("active", True):
            continue
        if r.get("api_source_id") == source_id and r.get("source_type", "") == source_type:
            r["active"] = False
            r["deactivated_at"] = _now()
            try:
                deactivated_ids.append(int(r["id"]))
            except (KeyError, ValueError):
                pass

    if deactivated_ids and PURGE_INACTIVE_FROM_FAISS:
        idx = _load_or_create_faiss(faiss_path, 1)
        _remove_ids_from_faiss(idx, deactivated_ids)
        _save_faiss(idx, faiss_path)

    _rewrite_jsonl(chunks_path, rows)
    log.info("Deactivated %d chunks for source %s", len(deactivated_ids), source_id)
    return {"chunks_deactivated": len(deactivated_ids)}


def delete_chunks_by_record_ids(
    index_dir: str,
    source_id: str,
    record_ids: List[str],
) -> Dict[str, Any]:
    """Remove chunks for specific record IDs from JSONL+FAISS."""
    if not record_ids:
        return {"chunks_deactivated": 0}

    faiss_path = _p(index_dir, "faiss.index")
    chunks_path = _p(index_dir, "chunks.jsonl")

    record_id_set = set(record_ids)
    rows = _read_jsonl(chunks_path)
    deactivated_ids: List[int] = []

    for r in rows:
        if not r.get("active", True):
            continue
        if r.get("api_source_id") == source_id and r.get("record_id") in record_id_set:
            r["active"] = False
            r["deactivated_at"] = _now()
            try:
                deactivated_ids.append(int(r["id"]))
            except (KeyError, ValueError):
                pass

    if deactivated_ids and PURGE_INACTIVE_FROM_FAISS:
        idx = _load_or_create_faiss(faiss_path, 1)
        _remove_ids_from_faiss(idx, deactivated_ids)
        _save_faiss(idx, faiss_path)

    _rewrite_jsonl(chunks_path, rows)
    log.info("Deactivated %d chunks for %d records in %s", len(deactivated_ids), len(record_ids), source_id)
    return {"chunks_deactivated": len(deactivated_ids)}


def get_chunks_by_source(
    index_dir: str,
    source_id: str,
    source_type: Optional[str] = None,
    active_only: bool = True,
) -> List[Dict[str, Any]]:
    """Get all chunk rows for a given source."""
    chunks_path = _p(index_dir, "chunks.jsonl")
    rows = _read_jsonl(chunks_path)

    results: List[Dict[str, Any]] = []
    for r in rows:
        if active_only and not r.get("active", True):
            continue
        if r.get("api_source_id") == source_id:
            if source_type is not None and r.get("source_type", "") != source_type:
                continue
            results.append(r)
    return results

