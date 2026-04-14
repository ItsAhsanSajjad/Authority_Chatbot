"""
PERA AI — Structured Logging Configuration

Call `setup_logging()` once at app startup.
Use `get_logger(name)` in each module, e.g. get_logger("pera.retriever").
"""
from __future__ import annotations

import logging
import sys
from contextvars import ContextVar
from typing import Optional

# Context variable for per-request ID (set by middleware)
request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


class PERAFormatter(logging.Formatter):
    """
    Structured log formatter: [timestamp] [level] [module] [request_id] message
    Plain-text for readability in local dev and production stdout.
    """

    FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-22s | %(rid)s | %(message)s"
    DATE_FMT = "%Y-%m-%d %H:%M:%S"

    def __init__(self) -> None:
        super().__init__(fmt=self.FORMAT, datefmt=self.DATE_FMT)

    def format(self, record: logging.LogRecord) -> str:
        rid = request_id_var.get()
        record.rid = rid or "-"  # type: ignore[attr-defined]
        return super().format(record)


def setup_logging(level: str = "INFO") -> None:
    """
    Configure root logging for the PERA application.
    Safe to call multiple times (idempotent).
    """
    root = logging.getLogger()

    # Avoid adding duplicate handlers on repeat calls
    if any(isinstance(h, logging.StreamHandler) and getattr(h, "_pera_handler", False) for h in root.handlers):
        return

    handler = logging.StreamHandler(sys.stdout)
    handler._pera_handler = True  # type: ignore[attr-defined]
    handler.setFormatter(PERAFormatter())

    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.addHandler(handler)

    # Quiet noisy third-party loggers
    for noisy in ("httpcore", "httpx", "openai", "faiss", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Convention: 'pera.<module>'."""
    return logging.getLogger(name)
