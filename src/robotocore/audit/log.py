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
        size = max_size if max_size is not None else int(os.environ.get("AUDIT_LOG_SIZE", "1000"))
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
            "duration_ms": round(duration_ms, 2),
            "account_id": account_id,
            "region": region,
        }
        if error:
            entry["error"] = error
        with self._lock:
            self._entries.append(entry)

    def recent(self, limit: int = 100) -> list[dict]:
        """Return the most recent entries (newest first)."""
        with self._lock:
            entries = list(self._entries)
        entries.reverse()
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
