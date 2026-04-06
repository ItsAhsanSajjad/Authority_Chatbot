"""
PERA AI — API Source Utilities

Helpers for source ID normalization, URL normalization,
stable content hashing, YAML scanning, and config file discovery.
"""
from __future__ import annotations

import hashlib
import os
import re
from typing import Dict, List, Optional

import yaml

from log_config import get_logger

log = get_logger("pera.api.utils")


def normalize_source_id(raw: str) -> str:
    """Normalize a source_id to lowercase alphanumeric + underscores."""
    s = raw.strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_url(url: str) -> str:
    """Normalize a URL: strip whitespace, remove trailing slash."""
    return url.strip().rstrip("/")


def stable_config_hash(content: str) -> str:
    """
    Compute a stable SHA-256 hash of config content.
    Normalizes line endings and strips trailing whitespace for consistency
    across platforms (Windows → Unix).
    """
    normalized = content.replace("\r\n", "\n").replace("\r", "\n").strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def stable_file_hash(path: str) -> str:
    """Read a file and return its stable content hash."""
    with open(path, "r", encoding="utf-8") as f:
        return stable_config_hash(f.read())


def safe_parse_yaml(path: str) -> Optional[Dict]:
    """
    Safely parse a YAML file. Returns None on any error.
    Logs warnings for parse failures.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        if isinstance(data, dict):
            return data
        log.warning("YAML file is not a mapping: %s", path)
        return None
    except yaml.YAMLError as e:
        log.warning("YAML parse error in %s: %s", path, e)
        return None
    except OSError as e:
        log.warning("Cannot read file %s: %s", path, e)
        return None


def scan_api_config_dir(source_dir: str) -> List[str]:
    """
    Scan a directory for YAML config files.
    Returns sorted list of absolute paths to .yaml/.yml files.
    Skips files starting with '.' or '_'.
    """
    if not os.path.isdir(source_dir):
        log.info("API source directory does not exist: %s", source_dir)
        return []

    paths: List[str] = []
    for fname in sorted(os.listdir(source_dir)):
        if fname.startswith((".", "_")):
            continue
        if not fname.lower().endswith((".yaml", ".yml")):
            continue
        full = os.path.join(source_dir, fname)
        if os.path.isfile(full):
            paths.append(os.path.abspath(full))

    log.info("Found %d API config file(s) in %s", len(paths), source_dir)
    return paths


def extract_source_id_from_file(path: str) -> Optional[str]:
    """Try to extract source_id from a YAML config file without full validation."""
    data = safe_parse_yaml(path)
    if data and "source_id" in data:
        return normalize_source_id(str(data["source_id"]))
    return None
