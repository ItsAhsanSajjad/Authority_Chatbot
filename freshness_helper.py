"""
PERA AI — Shared freshness + numeric formatting helpers.

Single source of truth for:
  • compute_tier(seconds_old, sync_interval_s)
        Classifies an age into "live" / "near-live" / "hourly" /
        "daily" / "stale" — used by every stored-API lookup so the
        chatbot can surface a uniform freshness signal.
  • format_freshness_footer(table, snapshot_date, sync_interval_s, source_label)
        Returns a stable single-line footer like
            "[FRESHNESS — challan_totals: live · as of 2026-04-29 10:35]"
        or
            "[FRESHNESS — inspection_performance: stale · snapshot from 2026-04-20]"
        Safe with `None` snapshot_date — never raises.
  • format_int_indian(value)
        1,00,000 / 10,00,000 / 1,23,45,678 (Indian numbering).
  • format_money_pkr(value)
        "Rs. 12,34,567" — uses Indian grouping with a Rs. prefix.

No external dependencies. Tested standalone (see tests/test_freshness_helper.py).
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional, Union

# ── Tier thresholds (seconds) ────────────────────────────────────────────
# These are the upper bounds for each tier. A snapshot older than DAILY
# (and older than 3× its own sync_interval if known) is "stale".
_TIER_LIVE_MAX_S      = 60         # ≤ 1 min
_TIER_NEAR_LIVE_MAX_S = 60 * 60    # ≤ 1 hour
_TIER_HOURLY_MAX_S    = 4 * 60 * 60     # ≤ 4 hours
_TIER_DAILY_MAX_S     = 2 * 24 * 60 * 60  # ≤ 2 days
# anything older falls into "stale" or, if 3× its sync_interval, also "stale"


def compute_tier(
    seconds_old: Optional[Union[int, float]],
    sync_interval_s: Optional[Union[int, float]] = None,
) -> str:
    """Classify an age (in seconds) into a freshness tier.

    Tiers (in increasing staleness):
        "live"      — fresh enough for live decisions
        "near-live" — under an hour
        "hourly"    — within a few hours
        "daily"     — within ~2 days
        "stale"     — anything older, OR more than 3× the table's own
                      sync interval (i.e., the scheduler missed cycles)
        "unknown"   — when seconds_old is None / negative / unparseable

    `sync_interval_s` is optional; if provided, it sharpens the "stale"
    boundary so a table that's normally synced every 30 min and is now
    2 hours behind will be flagged stale even though 2 h < daily.
    """
    if seconds_old is None:
        return "unknown"
    try:
        secs = float(seconds_old)
    except (TypeError, ValueError):
        return "unknown"
    if secs < 0:
        # Future-dated snapshot is suspicious; treat as unknown rather
        # than misleadingly "live".
        return "unknown"

    # Per-table stale check (if a sync interval is known)
    if sync_interval_s is not None:
        try:
            interval = float(sync_interval_s)
            if interval > 0 and secs > 3 * interval:
                # If we missed 3+ expected cycles, it's stale regardless
                # of the absolute thresholds below.
                if secs > _TIER_HOURLY_MAX_S:
                    return "stale"
        except (TypeError, ValueError):
            pass

    if secs <= _TIER_LIVE_MAX_S:
        return "live"
    if secs <= _TIER_NEAR_LIVE_MAX_S:
        return "near-live"
    if secs <= _TIER_HOURLY_MAX_S:
        return "hourly"
    if secs <= _TIER_DAILY_MAX_S:
        return "daily"
    return "stale"


# Human-readable phrasing per tier — used by the footer.
_TIER_LABELS = {
    "live":      "live",
    "near-live": "near-live",
    "hourly":    "current",
    "daily":     "current (synced daily)",
    "stale":     "stale",
    "unknown":   "freshness unknown",
}


def _coerce_to_datetime(snapshot: Optional[Union[datetime, date, str, int, float]]) -> Optional[datetime]:
    """Normalize many possible shapes of "snapshot date" into a datetime.

    Accepts:
      - datetime / date
      - ISO-format string ("2026-04-29", "2026-04-29T10:35:00", with or without TZ)
      - unix timestamp (int / float seconds since epoch)
      - None → returns None

    Never raises — returns None on any parse failure so the caller can
    still produce a sensible footer ("freshness unknown").
    """
    if snapshot is None:
        return None
    if isinstance(snapshot, datetime):
        return snapshot
    if isinstance(snapshot, date):
        return datetime(snapshot.year, snapshot.month, snapshot.day)
    if isinstance(snapshot, (int, float)):
        try:
            return datetime.fromtimestamp(float(snapshot))
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(snapshot, str):
        s = snapshot.strip()
        if not s:
            return None
        # Try ISO 8601, then a couple of common formats
        for parse_attempt in (
            lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),
            lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
            lambda x: datetime.strptime(x, "%Y-%m-%d"),
        ):
            try:
                return parse_attempt(s)
            except (ValueError, TypeError):
                continue
        return None
    return None


def format_freshness_footer(
    table_name: str,
    snapshot_date: Optional[Union[datetime, date, str, int, float]] = None,
    sync_interval_s: Optional[Union[int, float]] = None,
    source_label: Optional[str] = None,
) -> str:
    """Render a single-line freshness stamp for inclusion in an answer's
    formatted_context. Safe with ``snapshot_date=None`` — falls back to a
    "freshness unknown" stamp instead of raising.

    Examples:
        >>> format_freshness_footer("challan_totals", datetime.now(), 5)
        '[FRESHNESS — challan_totals: live · as of 2026-04-29 10:35]'

        >>> from datetime import date
        >>> format_freshness_footer("inspection_performance", date(2026, 4, 20))
        '[FRESHNESS — inspection_performance: stale · snapshot from 2026-04-20]'

        >>> format_freshness_footer("requisition_detail", None)
        '[FRESHNESS — requisition_detail: freshness unknown]'

    The footer is wrapped in square brackets and prefixed with the literal
    string "[FRESHNESS —" so the LLM (and downstream tooling) can reliably
    detect and surface it in the final answer.
    """
    label = (source_label or table_name or "stored data").strip()
    snapshot_dt = _coerce_to_datetime(snapshot_date)

    if snapshot_dt is None:
        return f"[FRESHNESS — {label}: freshness unknown]"

    # Compute age. If snapshot is timezone-aware and "now" isn't, normalise
    # both to naive local for the subtraction.
    now = datetime.now()
    try:
        if snapshot_dt.tzinfo is not None:
            # Convert to naive local-equivalent for stable subtraction
            snapshot_dt = snapshot_dt.astimezone().replace(tzinfo=None)
        seconds_old = max(0.0, (now - snapshot_dt).total_seconds())
    except Exception:
        return f"[FRESHNESS — {label}: freshness unknown]"

    tier = compute_tier(seconds_old, sync_interval_s)
    tier_label = _TIER_LABELS.get(tier, tier)

    # Date phrasing varies slightly by tier so it reads naturally
    if tier in ("live", "near-live"):
        date_str = snapshot_dt.strftime("%Y-%m-%d %H:%M")
        return f"[FRESHNESS — {label}: {tier_label} · as of {date_str}]"
    if tier == "stale":
        date_str = snapshot_dt.strftime("%Y-%m-%d")
        return f"[FRESHNESS — {label}: {tier_label} · snapshot from {date_str}]"
    # hourly / daily / unknown
    date_str = snapshot_dt.strftime("%Y-%m-%d %H:%M") if tier == "hourly" else snapshot_dt.strftime("%Y-%m-%d")
    return f"[FRESHNESS — {label}: {tier_label} · as of {date_str}]"


# ── Numeric formatting (Indian-style grouping) ───────────────────────────
def _indian_grouping(digits: str) -> str:
    """Apply Indian comma grouping to a string of digits (no sign, no dot).

    Rule: last three digits, then commas every two digits.
    Example: "1234567" → "12,34,567"
    """
    if len(digits) <= 3:
        return digits
    last3 = digits[-3:]
    rest = digits[:-3]
    # group `rest` into chunks of 2 from the right
    chunks = []
    while len(rest) > 2:
        chunks.append(rest[-2:])
        rest = rest[:-2]
    if rest:
        chunks.append(rest)
    chunks.reverse()
    return ",".join(chunks) + "," + last3


def format_int_indian(value: Optional[Union[int, float, str]]) -> str:
    """Format an integer with Indian-style comma grouping.

    Returns "—" for None / blank / unparseable input.

    >>> format_int_indian(0)
    '0'
    >>> format_int_indian(1234)
    '1,234'
    >>> format_int_indian(1234567)
    '12,34,567'
    >>> format_int_indian(-1234567)
    '-12,34,567'
    >>> format_int_indian(None)
    '—'
    """
    if value is None:
        return "—"
    try:
        if isinstance(value, str):
            v = value.strip().replace(",", "")
            if not v:
                return "—"
            n = int(float(v))
        else:
            n = int(value)
    except (TypeError, ValueError):
        return "—"
    sign = "-" if n < 0 else ""
    return sign + _indian_grouping(str(abs(n)))


def format_money_pkr(value: Optional[Union[int, float, str]]) -> str:
    """Format a money value as Pakistani Rupees with Indian-style grouping.

    >>> format_money_pkr(1234567)
    'Rs. 12,34,567'
    >>> format_money_pkr(None)
    'Rs. —'
    >>> format_money_pkr(0)
    'Rs. 0'
    """
    return "Rs. " + format_int_indian(value)


__all__ = [
    "compute_tier",
    "format_freshness_footer",
    "format_int_indian",
    "format_money_pkr",
]
