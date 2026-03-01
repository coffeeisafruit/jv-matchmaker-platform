"""
Logging filters for structured log output.

Provides correlation ID tracking across requests and batch pipeline runs.
"""
import logging
import threading

_local = threading.local()


def get_correlation_id() -> str:
    """Get the current correlation ID (empty string if not set)."""
    return getattr(_local, "correlation_id", "")


def set_correlation_id(cid: str) -> None:
    """Set the correlation ID for the current thread."""
    _local.correlation_id = cid


class CorrelationIdFilter(logging.Filter):
    """Attach correlation_id to every log record."""

    def filter(self, record):
        record.correlation_id = get_correlation_id()
        return True
