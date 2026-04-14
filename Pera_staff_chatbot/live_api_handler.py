"""
PERA AI — Live API Handler (Config-Driven)

Isolated handler for live API queries.  Only hits approved,
read-only endpoints.  Never pollutes normal RAG pipeline.

**Now powered by api_lookup_registry** — endpoint discovery,
query matching, and response formatting are all driven by
the YAML configs in ``assets/apis/``.  Adding a new live
endpoint requires ONLY a YAML file with a ``lookup:`` section.

Phase 5 UX module.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import requests

from log_config import get_logger

log = get_logger("pera.live_api")

TIMEOUT_SECONDS = 15
MAX_RESPONSE_SIZE = 1024 * 1024  # 1 MB


# ── Helpers ──────────────────────────────────────────────────

def _get_endpoints() -> Dict[str, Dict[str, Any]]:
    """Lazily load live endpoints from the YAML-driven registry."""
    from api_lookup_registry import get_live_endpoints
    return get_live_endpoints()


# ── Public API ───────────────────────────────────────────────

def get_approved_endpoints() -> List[Dict[str, str]]:
    """Return the list of approved live endpoints (for frontend display)."""
    endpoints = _get_endpoints()
    return [
        {
            "key": k,
            "display_name": v["display_name"],
            "description": v["description"],
        }
        for k, v in endpoints.items()
    ]


def _match_endpoint(question: str) -> Optional[str]:
    """
    Match a question to a live API endpoint.
    Returns the ``source_id`` key or ``None``.
    """
    from api_lookup_registry import match_endpoint
    return match_endpoint(question)


def query_live_api(question: str) -> Dict[str, Any]:
    """
    Execute a live API query for the given question.

    Returns a dict with keys:
        success, answer, endpoint_key, endpoint_name,
        timestamp, raw_data, error
    """
    result: Dict[str, Any] = {
        "success": False,
        "answer": "",
        "endpoint_key": "",
        "endpoint_name": "",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "raw_data": None,
        "error": None,
    }

    # 1. Match endpoint
    endpoint_key = _match_endpoint(question)
    endpoints = _get_endpoints()

    if not endpoint_key or endpoint_key not in endpoints:
        supported = ", ".join(
            ep["display_name"] for ep in endpoints.values()
        )
        result["error"] = (
            "No approved live API endpoint matches this question. "
            f"Live mode currently supports: {supported}. "
            "Please try a different question or switch to "
            "Documents or Stored API mode."
        )
        return result

    endpoint = endpoints[endpoint_key]
    result["endpoint_key"] = endpoint_key
    result["endpoint_name"] = endpoint["display_name"]

    # 2. Fetch (read-only GET only)
    try:
        log.info("Live API query: %s -> %s", endpoint_key, endpoint["url"])
        resp = requests.get(
            endpoint["url"],
            headers={"Accept": "application/json"},
            timeout=TIMEOUT_SECONDS,
        )
        resp.raise_for_status()

        if len(resp.content) > MAX_RESPONSE_SIZE:
            result["error"] = "Response too large for live processing."
            return result

        data = resp.json()
        result["raw_data"] = data
        result["timestamp"] = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
        )

    except requests.Timeout:
        result["error"] = (
            f"Live API request timed out after {TIMEOUT_SECONDS}s."
        )
        log.warning("Live API timeout: %s", endpoint["url"])
        return result
    except requests.RequestException as exc:
        result["error"] = f"Live API request failed: {exc}"
        log.error("Live API error: %s — %s", endpoint["url"], exc)
        return result

    # 3. Format response (config-driven)
    try:
        from api_lookup_registry import format_live_response
        formatted = format_live_response(endpoint_key, data)
        result["answer"] = formatted or f"**Live Data**\n\n{str(data)[:2000]}"
        result["success"] = True
    except Exception as exc:
        result["error"] = f"Failed to format live response: {exc}"
        log.error("Live API format error: %s", exc, exc_info=True)

    return result
