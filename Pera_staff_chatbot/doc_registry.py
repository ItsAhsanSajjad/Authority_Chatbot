from __future__ import annotations

import os
import json
import hashlib
import re
from typing import List, Dict, Tuple


# Supported document types
SUPPORTED_EXTS = (".pdf", ".docx")


# ─── Document authority classification ───────────────────────
# authority=3: Official acts, formal notifications, approved regulations/annexes
# authority=2: Policy documents, HR manuals, operational guidelines
# authority=1: Compiled working papers, meeting minutes, draft materials
#
# The Compiled Working Paper (~54.7% of old index) gets authority=1
# so it can no longer dominate retrieval over authoritative sources.

_AUTHORITY_HIGH_PATTERNS = [
    r"\(\d+\s+of\s+\d{4}\)",           # "(10 of 2024)" — official act
    r"\bnotifi(?:cation|ed)\b",         # formal notifications
    r"\bannex\s*[a-z]\b",              # Annex G, H, I, L, M, N, P
    r"\bflag[-\s]*[a-z]\b",            # Flag-C, Flag-F, etc.
]
_AUTHORITY_HIGH_RE = re.compile("|".join(_AUTHORITY_HIGH_PATTERNS), re.IGNORECASE)

_AUTHORITY_LOW_PATTERNS = [
    r"compiled[\s_]+working[\s_]+paper",
    r"meeting[\s_]+(?:minutes|notes|mom)",
    r"working[\s_]+paper",
    r"\bdraft\b",
]
_AUTHORITY_LOW_RE = re.compile("|".join(_AUTHORITY_LOW_PATTERNS), re.IGNORECASE)


def classify_doc_authority(filename: str) -> int:
    """
    Classify a document's authority level based on its filename.
    Returns 3 (high), 2 (medium), or 1 (low).
    """
    name = (filename or "").strip()
    if _AUTHORITY_LOW_RE.search(name):
        return 1
    if _AUTHORITY_HIGH_RE.search(name):
        return 3
    # HR Manual, O&Ps Code, etc. = medium
    return 2


def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_book_rank(filename: str) -> int:
    """
    Priority rule you requested:
    book1, book2, ... bookN => higher number = newer/higher priority.
    If no match => rank 0.
    """
    base = os.path.splitext(os.path.basename(filename))[0]
    m = re.search(r"\bbook\s*([0-9]+)\b", base, flags=re.IGNORECASE)
    if not m:
        return 0
    try:
        return int(m.group(1))
    except Exception:
        return 0


def scan_assets_data(data_dir: str = "assets/data") -> List[Dict]:
    """
    Scans assets/data for PDFs.
    Returns list of dict entries with basic file metadata.
    """
    data_dir = data_dir.replace("\\", "/")
    if not os.path.isdir(data_dir):
        return []

    out: List[Dict] = []
    for name in os.listdir(data_dir):
        full = os.path.join(data_dir, name).replace("\\", "/")
        if not os.path.isfile(full):
            continue

        low = name.lower()
        if not low.endswith(SUPPORTED_EXTS):
            continue

        out.append({
            "path": full,
            "filename": name,
            "ext": os.path.splitext(name)[1].lower(),
            "mtime": int(os.path.getmtime(full)),
            "size": int(os.path.getsize(full)),
            "rank": _parse_book_rank(name),
            "authority": classify_doc_authority(name),
        })

    # Sort by rank desc (newest first), then by filename
    out.sort(key=lambda x: (-x["rank"], x["filename"].lower()))
    return out


def _load_manifest(manifest_path: str) -> Dict:
    if not os.path.exists(manifest_path):
        return {"version": 1, "files": {}}
    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"version": 1, "files": {}}


def _save_manifest(manifest_path: str, manifest: Dict) -> None:
    tmp = manifest_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    os.replace(tmp, manifest_path)


def compare_with_manifest(
    scanned: List[Dict],
    index_dir: str = "assets/index",
    manifest_name: str = "manifest.json",
    compute_hash: bool = True
) -> Tuple[List[Dict], List[Dict], List[Dict], Dict]:
    """
    Returns (new_or_changed, unchanged, removed, updated_manifest)

    - new_or_changed: files not in manifest OR hash changed OR size changed OR mtime changed
    - unchanged: files same as manifest
    - removed: files present in manifest but no longer present in assets/data
    """
    index_dir = index_dir.replace("\\", "/")
    _safe_mkdir(index_dir)

    manifest_path = os.path.join(index_dir, manifest_name).replace("\\", "/")
    manifest = _load_manifest(manifest_path)
    mf_files: Dict[str, Dict] = manifest.get("files", {})

    current_keys = set()
    new_or_changed: List[Dict] = []
    unchanged: List[Dict] = []

    for entry in scanned:
        key = entry["filename"]
        current_keys.add(key)

        prev = mf_files.get(key)

        if compute_hash:
            entry_hash = _sha256_file(entry["path"])
        else:
            entry_hash = None

        entry_out = dict(entry)
        if entry_hash:
            entry_out["sha256"] = entry_hash

        if prev is None:
            new_or_changed.append(entry_out)
            continue

        changed = False
        if compute_hash and prev.get("sha256") != entry_hash:
            changed = True
        elif prev.get("size") != entry["size"]:
            changed = True
        elif prev.get("mtime") != entry["mtime"]:
            changed = True

        if changed:
            new_or_changed.append(entry_out)
        else:
            unchanged.append(entry_out)

    removed: List[Dict] = []
    for key, prev in mf_files.items():
        if key not in current_keys:
            removed.append(prev)

    # Build updated manifest (only current scanned files)
    new_files_map: Dict[str, Dict] = {}
    for e in scanned:
        sha = _sha256_file(e["path"]) if compute_hash else mf_files.get(e["filename"], {}).get("sha256")
        new_files_map[e["filename"]] = {
            "filename": e["filename"],
            "path": e["path"],
            "ext": e["ext"],
            "mtime": e["mtime"],
            "size": e["size"],
            "rank": e["rank"],
            "sha256": sha,
        }

    updated_manifest = {"version": 1, "files": new_files_map}
    return new_or_changed, unchanged, removed, updated_manifest


def scan_and_update_manifest(
    data_dir: str = "assets/data",
    index_dir: str = "assets/index",
    manifest_name: str = "manifest.json",
) -> Dict:
    scanned = scan_assets_data(data_dir=data_dir)
    new_or_changed, unchanged, removed, updated_manifest = compare_with_manifest(
        scanned=scanned,
        index_dir=index_dir,
        manifest_name=manifest_name,
        compute_hash=True
    )

    manifest_path = os.path.join(index_dir, manifest_name).replace("\\", "/")
    _save_manifest(manifest_path, updated_manifest)

    return {
        "found": len(scanned),
        "new_or_changed": len(new_or_changed),
        "unchanged": len(unchanged),
        "removed": len(removed),
        "new_or_changed_files": new_or_changed,
        "removed_files": removed,
        "data_dir": data_dir,
        "manifest_path": manifest_path,
    }
