"""
PERA AI — API Fetcher

HTTP fetch layer for API ingestion. Uses httpx with retries,
backoff, timeout enforcement, and response size limits.
Ingestion-time only — not wired to query endpoints.

Phase 2 module.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx

from api_auth import ApiAuthResolver, ApiAuthError
from api_config_models import ApiSourceConfig
from log_config import get_logger
from settings import get_settings

log = get_logger("pera.api.fetcher")


@dataclass
class ApiFetchResult:
    """Result of an API fetch operation."""
    source_id: str = ""
    success: bool = False
    http_status: int = 0
    payload: Any = None  # parsed JSON
    raw_bytes: int = 0
    snapshot_hash: str = ""
    etag: str = ""
    last_modified: str = ""
    fetched_at: float = 0.0
    duration_seconds: float = 0.0
    error_message: str = ""
    pages_fetched: int = 1
    retry_count: int = 0


class ApiFetcher:
    """
    Fetches JSON data from external APIs for ingestion.
    Supports single-request and paginated modes.
    """

    def __init__(self, auth_resolver: Optional[ApiAuthResolver] = None):
        self._auth = auth_resolver or ApiAuthResolver()
        self._settings = get_settings()

    def fetch_source(self, config: ApiSourceConfig) -> ApiFetchResult:
        """
        Fetch data from an API source based on its config.
        Dispatches to single or paginated fetch as needed.
        """
        started = time.time()
        result = ApiFetchResult(
            source_id=config.source_id,
            fetched_at=started,
        )

        # Build headers
        try:
            headers = dict(config.fetch.headers)
            auth_headers = self._auth.build_auth_headers(config.auth)
            headers.update(auth_headers)
        except ApiAuthError as e:
            result.error_message = f"Auth error: {e}"
            result.duration_seconds = time.time() - started
            log.error("Auth failed for %s: %s", config.source_id, e)
            return result

        # Security: reject HTTP unless explicitly allowed
        if not self._settings.API_ALLOW_HTTP and config.fetch.url.startswith("http://"):
            result.error_message = "HTTP URLs are not allowed (API_ALLOW_HTTP=False)"
            result.duration_seconds = time.time() - started
            return result

        try:
            if config.fetch.pagination.type == "none":
                result = self._fetch_single_request(config, headers, result)
            else:
                result = self._fetch_paginated(config, headers, result)
        except Exception as e:
            result.error_message = f"Fetch error: {e}"
            log.error("Fetch failed for %s: %s", config.source_id, e)

        result.duration_seconds = time.time() - started
        return result

    def _fetch_single_request(
        self,
        config: ApiSourceConfig,
        headers: Dict[str, str],
        result: ApiFetchResult,
    ) -> ApiFetchResult:
        """Fetch a single non-paginated request."""
        timeout = config.fetch.timeout_seconds or self._settings.API_DEFAULT_TIMEOUT_SECONDS
        max_retries = config.fetch.retry_count
        backoff = config.fetch.retry_backoff_seconds

        last_error = ""
        for attempt in range(max_retries + 1):
            try:
                with httpx.Client(timeout=timeout) as client:
                    if config.fetch.method == "POST":
                        resp = client.post(config.fetch.url, headers=headers)
                    else:
                        resp = client.get(config.fetch.url, headers=headers)

                result.http_status = resp.status_code
                result.raw_bytes = len(resp.content)
                result.etag = resp.headers.get("etag", "")
                result.last_modified = resp.headers.get("last-modified", "")

                # Size check
                max_bytes = self._settings.API_MAX_RESPONSE_BYTES
                if result.raw_bytes > max_bytes:
                    result.error_message = (
                        f"Response too large: {result.raw_bytes} bytes > {max_bytes} limit"
                    )
                    return result

                # Status check
                if resp.status_code >= 400:
                    result.error_message = f"HTTP {resp.status_code}"
                    result.retry_count = attempt
                    if attempt < max_retries:
                        time.sleep(backoff * (attempt + 1))
                        last_error = result.error_message
                        continue
                    return result

                # Content-type check
                content_type = resp.headers.get("content-type", "")
                if not self._settings.API_ALLOW_NON_JSON and "json" not in content_type.lower():
                    result.error_message = (
                        f"Non-JSON response: {content_type}"
                    )
                    return result

                # Parse JSON
                try:
                    result.payload = resp.json()
                except (json.JSONDecodeError, ValueError) as e:
                    result.error_message = f"JSON parse error: {e}"
                    return result

                result.snapshot_hash = self._compute_snapshot_hash(resp.content)
                result.success = True
                result.retry_count = attempt
                return result

            except httpx.TimeoutException:
                last_error = "Request timed out"
                result.retry_count = attempt
                if attempt < max_retries:
                    time.sleep(backoff * (attempt + 1))
                    continue
                result.error_message = last_error
                return result

            except httpx.RequestError as e:
                last_error = f"Request error: {e}"
                result.retry_count = attempt
                if attempt < max_retries:
                    time.sleep(backoff * (attempt + 1))
                    continue
                result.error_message = last_error
                return result

        result.error_message = last_error or "Max retries exceeded"
        return result

    def _fetch_paginated(
        self,
        config: ApiSourceConfig,
        headers: Dict[str, str],
        result: ApiFetchResult,
    ) -> ApiFetchResult:
        """Fetch paginated data. Collects all pages into a combined payload."""
        pag = config.fetch.pagination
        timeout = config.fetch.timeout_seconds or self._settings.API_DEFAULT_TIMEOUT_SECONDS
        all_records: List[Any] = []
        page = 0
        offset = 0

        while page < pag.max_pages:
            # Build URL with pagination params
            params: Dict[str, Any] = {}
            if pag.type == "offset":
                params[pag.page_param] = offset
                params[pag.size_param] = pag.page_size
            elif pag.type == "page":
                params[pag.page_param] = page + 1  # 1-indexed
                params[pag.size_param] = pag.page_size

            try:
                with httpx.Client(timeout=timeout) as client:
                    resp = client.get(
                        config.fetch.url, headers=headers, params=params
                    )

                if resp.status_code >= 400:
                    result.error_message = f"HTTP {resp.status_code} on page {page}"
                    result.http_status = resp.status_code
                    return result

                content_type = resp.headers.get("content-type", "")
                if not self._settings.API_ALLOW_NON_JSON and "json" not in content_type.lower():
                    result.error_message = f"Non-JSON on page {page}: {content_type}"
                    return result

                page_data = resp.json()

                # Extract records using root_selector
                records = self._extract_by_selector(
                    page_data, config.normalization.root_selector
                )
                if records is None:
                    records = []
                if isinstance(records, list):
                    all_records.extend(records)
                else:
                    all_records.append(records)

                page += 1
                result.pages_fetched = page
                result.http_status = resp.status_code

                # Check if we've fetched all records
                if len(records) < pag.page_size:
                    break  # Last page

                offset += pag.page_size

            except (httpx.TimeoutException, httpx.RequestError) as e:
                result.error_message = f"Pagination error on page {page}: {e}"
                return result

        # Reconstruct combined payload
        if config.normalization.root_selector:
            result.payload = self._rebuild_payload(
                config.normalization.root_selector, all_records
            )
        else:
            result.payload = all_records

        result.raw_bytes = len(json.dumps(result.payload).encode())
        result.snapshot_hash = self._compute_snapshot_hash(
            json.dumps(result.payload, sort_keys=True).encode()
        )
        result.success = True
        return result

    def _compute_snapshot_hash(self, content: bytes) -> str:
        """Compute SHA-256 hash of raw response content."""
        return hashlib.sha256(content).hexdigest()

    def _extract_by_selector(self, data: Any, selector: str) -> Any:
        """Navigate a dot-separated path in JSON data."""
        if not selector:
            return data
        parts = selector.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

    def _rebuild_payload(self, selector: str, records: List) -> Dict:
        """Rebuild a nested dict placing records at the selector path."""
        parts = selector.split(".")
        result: Dict[str, Any] = {}
        current = result
        for i, part in enumerate(parts):
            if i == len(parts) - 1:
                current[part] = records
            else:
                current[part] = {}
                current = current[part]
        return result
