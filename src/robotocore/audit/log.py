"""Ring-buffer audit log for API requests.

Configuration:
    AUDIT_LOG_SIZE=1000   Maximum number of entries to keep (default: 1000)
"""

import os
import threading
import time
from collections import deque


class AuditLog:
    """Thread-safe ring buffer of API request audit entries."""

    def __init__(self, max_size: int | None = None):
        if max_size is not None:
            size = max_size
        else:
            raw = os.environ.get("AUDIT_LOG_SIZE", "1000")
            try:
                size = int(raw)
            except ValueError:
                raise ValueError(
                    f"AUDIT_LOG_SIZE environment variable must be a valid integer, got: {raw!r}"
                ) from None
        self._entries: deque[dict] = deque(maxlen=size)
        self._lock = threading.Lock()

    def record(
        self,
        *,
        service: str,
        operation: str | None = None,
        method: str = "POST",
        path: str = "/",
        status_code: int = 200,
        duration_ms: float = 0.0,
        account_id: str = "",
        region: str = "",
        error: str | None = None,
    ) -> None:
        """Record an API request."""
        entry = {
            "timestamp": time.time(),
            "service": service,
            "operation": operation,
            "method": method,
            "path": path,
            "status_code": status_code,
            "duration_ms": round(duration_ms, 3),
            "account_id": account_id,
            "region": region,
            "error": error if error else None,
        }
        with self._lock:
            self._entries.append(entry)

    def recent(
        self,
        limit: int = 100,
        service: str | None = None,
        operation: str | None = None,
        since: float | None = None,
        start_time: float | None = None,
    ) -> list[dict]:
        """Return the most recent entries (newest first), with optional filters.

        Args:
            limit: Maximum number of entries to return.
            service: Filter by service name.
            operation: Filter by operation name.
            since: Only return entries with timestamp >= this value.
            start_time: Alias for since.
        """
        effective_since = since if since is not None else start_time
        with self._lock:
            entries = list(self._entries)
        entries.reverse()
        if service is not None:
            entries = [e for e in entries if e.get("service") == service]
        if operation is not None:
            entries = [e for e in entries if e.get("operation") == operation]
        if effective_since is not None:
            entries = [e for e in entries if e.get("timestamp", 0) >= effective_since]
        return entries[:limit]

    def clear(self) -> int:
        """Clear all entries. Returns count cleared."""
        with self._lock:
            count = len(self._entries)
            self._entries.clear()
            return count


# Singleton
_log: AuditLog | None = None


def get_audit_log() -> AuditLog:
    global _log
    if _log is None:
        _log = AuditLog()
    return _log
