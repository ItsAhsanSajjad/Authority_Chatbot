# index_manager.py
from __future__ import annotations

import os
import time
import json
import shutil
import threading
from dataclasses import dataclass
from typing import Optional, Dict, Any

from doc_registry import scan_assets_data, compare_with_manifest
from index_store import scan_and_ingest_if_needed, load_index_and_chunks
from log_config import get_logger

log = get_logger("pera.index_manager")


@dataclass
class IndexManagerConfig:
    data_dir: str = "assets/data"

    # Where we store multiple built indexes
    indexes_root: str = "assets/indexes"

    # Pointer file tells retriever which index dir to use
    active_pointer_path: str = "assets/indexes/ACTIVE.json"

    # How often to check for new/changed PDFs
    poll_seconds: int = 30

    # Optional: keep a few older builds for rollback/debug
    keep_last_n: int = 3

    # Chunking options forwarded to scan_and_ingest_if_needed
    chunk_max_chars: int = 4500
    chunk_overlap_chars: int = 350


class ActiveIndexPointer:
    """
    Reads/writes the active index directory pointer atomically.
    """

    def __init__(self, pointer_path: str):
        self.pointer_path = (pointer_path or "").replace("\\", "/")
        if self.pointer_path:
            os.makedirs(os.path.dirname(self.pointer_path), exist_ok=True)

    def read(self) -> Optional[str]:
        """
        Returns active index dir ONLY if it exists.
        """
        if not self.pointer_path or not os.path.exists(self.pointer_path):
            return None
        try:
            with open(self.pointer_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            p = (data.get("active_index_dir") or "").strip()
            if not p:
                return None
            p = p.replace("\\", "/")
            return p if os.path.isdir(p) else None
        except Exception:
            return None

    def write_atomic(self, new_active_dir: str) -> None:
        new_active_dir = (new_active_dir or "").replace("\\", "/").strip()
        if not new_active_dir:
            raise ValueError("new_active_dir is empty")
        tmp = self.pointer_path + ".tmp"
        payload = {
            "active_index_dir": new_active_dir,
            "updated_at": int(time.time()),
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.pointer_path)


class SafeAutoIndexer:
    """
    Production-safe auto-indexer:
    - Polls assets/data for new/changed PDFs
    - Builds a new index in a fresh directory (green build)
    - Validates build exists + has chunks
    - Atomically switches ACTIVE pointer to the new index directory
    - Never edits the currently active index in-place
    """

    def __init__(self, cfg: IndexManagerConfig):
        self.cfg = cfg
        self.cfg.data_dir = self.cfg.data_dir.replace("\\", "/")
        self.cfg.indexes_root = self.cfg.indexes_root.replace("\\", "/")
        self.cfg.active_pointer_path = self.cfg.active_pointer_path.replace("\\", "/")

        os.makedirs(self.cfg.indexes_root, exist_ok=True)
        self.pointer = ActiveIndexPointer(self.cfg.active_pointer_path)

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ------------------------
    # Public API
    # ------------------------
    def start_background(self) -> None:
        """
        Starts a background poller thread.
        Call this once at FastAPI startup.
        """
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        log.info("Background indexer started (poll_seconds=%d)", self.cfg.poll_seconds)

    def stop_background(self) -> None:
        self._stop_event.set()

    def ensure_index_once(self) -> Dict[str, Any]:
        """
        Run a single check+build if needed (no background).
        Useful for cron/manual admin triggers.
        """
        with self._lock:
            return self._check_and_rebuild_if_needed()

    def get_active_index_dir(self) -> str:
        """
        Returns currently active index dir.
        If pointer missing, bootstraps to a default build.
        """
        cur = self.pointer.read()
        if cur:
            return cur

        result = self._bootstrap_initial_index()
        return result["active_index_dir"]

    # ------------------------
    # Internal
    # ------------------------
    def _run_loop(self) -> None:
        # Bootstrap once at startup
        with self._lock:
            self._bootstrap_initial_index()

        while not self._stop_event.is_set():
            try:
                with self._lock:
                    self._check_and_rebuild_if_needed()
            except Exception as e:
                # Never crash production due to indexing errors — but DO log them
                log.error("Background index rebuild failed: %s", e, exc_info=True)
            self._stop_event.wait(self.cfg.poll_seconds)

    def _bootstrap_initial_index(self) -> Dict[str, Any]:
        active = self.pointer.read()
        if active:
            return {"changed": False, "active_index_dir": active}

        build_dir = self._new_build_dir()
        build_res = scan_and_ingest_if_needed(
            data_dir=self.cfg.data_dir,
            index_dir=build_dir,
            chunk_max_chars=self.cfg.chunk_max_chars,
            chunk_overlap_chars=self.cfg.chunk_overlap_chars,
        )

        if not self._validate_index_dir(build_dir):
            raise RuntimeError("Initial index build failed validation (no faiss/chunks).")

        self.pointer.write_atomic(build_dir)
        log.info("Initial index bootstrapped at %s", build_dir)
        self._cleanup_old_builds()
        return {"changed": True, "active_index_dir": build_dir, "build": build_res}

    def _check_and_rebuild_if_needed(self) -> Dict[str, Any]:
        """
        - Compare current assets/data scan against the manifest in the ACTIVE index dir
        - If new/changed/removed exists => build fresh index and switch pointer
        """
        active_dir = self.pointer.read()
        if not active_dir:
            return self._bootstrap_initial_index()

        scanned = scan_assets_data(data_dir=self.cfg.data_dir)

        # doc_registry.scan_assets_data already filters by SUPPORTED_EXTS

        new_or_changed, unchanged, removed, _updated_manifest = compare_with_manifest(
            scanned=scanned,
            index_dir=active_dir,
            manifest_name="manifest.json",
            compute_hash=True
        )

        if not new_or_changed and not removed:
            return {
                "changed": False,
                "active_index_dir": active_dir,
                "found": len(scanned),
                "unchanged": len(unchanged),
            }

        build_dir = self._new_build_dir()
        build_res = scan_and_ingest_if_needed(
            data_dir=self.cfg.data_dir,
            index_dir=build_dir,
            chunk_max_chars=self.cfg.chunk_max_chars,
            chunk_overlap_chars=self.cfg.chunk_overlap_chars,
        )

        if not self._validate_index_dir(build_dir):
            return {
                "changed": False,
                "active_index_dir": active_dir,
                "error": "New index build failed validation; keeping current index.",
                "build_dir": build_dir,
                "build": build_res,
            }

        self.pointer.write_atomic(build_dir)
        log.info("Index rebuild complete. Switched pointer: %s -> %s", active_dir, build_dir)
        self._cleanup_old_builds()

        return {
            "changed": True,
            "previous_index_dir": active_dir,
            "active_index_dir": build_dir,
            "build": build_res,
        }

    def _new_build_dir(self) -> str:
        ts = time.strftime("%Y%m%d_%H%M%S")
        build_dir = os.path.join(self.cfg.indexes_root, f"build_{ts}").replace("\\", "/")
        os.makedirs(build_dir, exist_ok=True)
        return build_dir

    def _validate_index_dir(self, index_dir: str) -> bool:
        """
        Minimal sanity checks:
        - faiss.index exists
        - chunks.jsonl exists and has at least one active row
        """
        faiss_path = os.path.join(index_dir, "faiss.index").replace("\\", "/")
        chunks_path = os.path.join(index_dir, "chunks.jsonl").replace("\\", "/")
        if not os.path.exists(faiss_path):
            return False
        if not os.path.exists(chunks_path):
            return False

        try:
            idx, rows = load_index_and_chunks(index_dir=index_dir)
            if idx is None:
                return False
            active_rows = [r for r in rows if r.get("active", True) and (r.get("text") or "").strip()]
            return len(active_rows) > 0
        except Exception:
            return False

    def _cleanup_old_builds(self) -> None:
        keep_n = max(1, int(self.cfg.keep_last_n))
        active = self.pointer.read()

        try:
            entries = []
            for name in os.listdir(self.cfg.indexes_root):
                full = os.path.join(self.cfg.indexes_root, name).replace("\\", "/")
                if os.path.isdir(full) and name.startswith("build_"):
                    entries.append(full)
            entries.sort()
        except Exception:
            return

        keep_set = set(entries[-keep_n:])
        if active:
            keep_set.add(active)

        for d in entries:
            if d in keep_set:
                continue
            try:
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass
