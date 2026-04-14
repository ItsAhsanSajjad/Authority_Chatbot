"""
PERA AI — In-Memory Index Cache

Caches the active FAISS index + parsed chunks in memory.
Invalidates and reloads when the active index pointer changes.
Thread-safe for concurrent FastAPI requests.
"""
from __future__ import annotations

import os
import json
import re
import threading
from typing import Optional, Dict, Any, List, Tuple, Set

import faiss  # type: ignore
import numpy as np

from log_config import get_logger

log = get_logger("pera.index_cache")


class IndexCache:
    """
    Thread-safe in-memory cache for FAISS index + chunks.
    
    Loads from disk once, then serves from memory until the
    active index pointer changes (blue/green switch).
    """

    def __init__(self, pointer_path: str = "assets/indexes/ACTIVE.json"):
        self._pointer_path = (pointer_path or "").replace("\\", "/")
        self._lock = threading.Lock()

        # Cached state
        self._cached_dir: Optional[str] = None
        self._cached_pointer_mtime: float = 0.0
        self._index: Optional[faiss.Index] = None
        self._rows: List[Dict[str, Any]] = []
        self._id_map: Dict[int, Dict[str, Any]] = {}
        self._token_index: Dict[str, Set[int]] = {}  # token -> {chunk_ids}

    # ──────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────
    def get(self) -> Tuple[Optional["faiss.Index"], List[Dict[str, Any]], Dict[int, Dict[str, Any]], Dict[str, Set[int]], str]:
        """
        Returns (index, rows, id_map, token_index, resolved_dir).
        Serves from memory if cache is valid; reloads if pointer changed.
        """
        with self._lock:
            if self._needs_reload():
                self._reload()
            return self._index, self._rows, self._id_map, self._token_index, self._cached_dir or ""

    def invalidate(self) -> None:
        """Force cache to reload on next access."""
        with self._lock:
            log.info("Index cache explicitly invalidated")
            self._cached_dir = None
            self._cached_pointer_mtime = 0.0
            self._index = None
            self._rows = []
            self._id_map = {}
            self._token_index = {}

    # ──────────────────────────────────────────────────────────
    # Internal
    # ──────────────────────────────────────────────────────────
    def _needs_reload(self) -> bool:
        """Check if the active pointer file changed since last load."""
        if self._index is None or self._cached_dir is None:
            return True  # never loaded

        try:
            if not os.path.exists(self._pointer_path):
                return self._cached_dir is None
            current_mtime = os.path.getmtime(self._pointer_path)
            if current_mtime != self._cached_pointer_mtime:
                # Pointer file was modified — check if dir actually changed
                new_dir = self._read_pointer_dir()
                if new_dir != self._cached_dir:
                    return True
                # Same dir — just update mtime to avoid re-checking
                self._cached_pointer_mtime = current_mtime
            return False
        except Exception:
            return True  # when in doubt, reload

    def _read_pointer_dir(self) -> Optional[str]:
        """Read the active index directory from the pointer file."""
        try:
            with open(self._pointer_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            p = (data.get("active_index_dir") or "").strip().replace("\\", "/")
            return p if p and os.path.isdir(p) else None
        except Exception:
            return None

    def _reload(self) -> None:
        """Load FAISS index + chunks from disk into memory."""
        active_dir = self._read_pointer_dir()
        if not active_dir:
            # Fallback to legacy single-index path
            if os.path.isdir("assets/index"):
                active_dir = "assets/index"
            else:
                log.warning("No active index found — cache empty")
                self._index = None
                self._rows = []
                self._id_map = {}
                self._cached_dir = None
                return

        faiss_path = os.path.join(active_dir, "faiss.index").replace("\\", "/")
        chunks_path = os.path.join(active_dir, "chunks.jsonl").replace("\\", "/")

        # Load FAISS
        idx = None
        if os.path.exists(faiss_path):
            try:
                idx = faiss.read_index(faiss_path)
            except Exception as e:
                log.error("Failed to load FAISS index from %s: %s", faiss_path, e)

        # Load chunks
        rows: List[Dict[str, Any]] = []
        if os.path.exists(chunks_path):
            try:
                with open(chunks_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                rows.append(json.loads(line))
                            except json.JSONDecodeError:
                                continue
            except Exception as e:
                log.error("Failed to load chunks from %s: %s", chunks_path, e)

        # Build ID map (active rows only)
        id_map: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            if r.get("active", True):
                try:
                    id_map[int(r["id"])] = r
                except (KeyError, ValueError):
                    continue

        # Build inverted token index for O(1) keyword lookup
        _token_re = re.compile(r"[a-z0-9\u0600-\u06FF]+", re.IGNORECASE)
        token_index: Dict[str, Set[int]] = {}
        for cid, row in id_map.items():
            txt = (row.get("text") or "").lower()
            for tok in _token_re.findall(txt):
                if len(tok) > 1:
                    if tok not in token_index:
                        token_index[tok] = set()
                    token_index[tok].add(cid)

        old_dir = self._cached_dir
        self._index = idx
        self._rows = rows
        self._id_map = id_map
        self._token_index = token_index
        self._cached_dir = active_dir

        # Update pointer mtime
        try:
            if os.path.exists(self._pointer_path):
                self._cached_pointer_mtime = os.path.getmtime(self._pointer_path)
        except Exception:
            pass

        n_vectors = idx.ntotal if idx else 0
        n_active = len(id_map)
        n_tokens = len(token_index)

        if old_dir and old_dir != active_dir:
            log.info("Index cache reloaded (pointer changed): %s -> %s (%d vectors, %d chunks, %d tokens)",
                     old_dir, active_dir, n_vectors, n_active, n_tokens)
        else:
            log.info("Index cache loaded: %s (%d vectors, %d chunks, %d tokens)",
                     active_dir, n_vectors, n_active, n_tokens)


# ─── Singleton ────────────────────────────────────────────────────────────────
_cache = IndexCache(os.getenv("INDEX_POINTER_PATH", "assets/indexes/ACTIVE.json"))


def get_cached_index():
    """Module-level accessor for the index cache singleton."""
    return _cache.get()


def invalidate_cache():
    """Module-level invalidation."""
    _cache.invalidate()
