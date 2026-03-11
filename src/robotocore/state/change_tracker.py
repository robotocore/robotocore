"""Track whether emulator state has changed since the last save.

Used by snapshot strategies to avoid unnecessary saves (e.g. scheduled strategy
skips a tick when nothing has changed).
"""

import threading

# HTTP methods that are read-only and don't mutate state
_READ_ONLY_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# Path prefixes for management endpoints that don't affect AWS state
_MANAGEMENT_PREFIXES = ("/_robotocore/",)


class ChangeTracker:
    """Thread-safe tracker for state mutations."""

    def __init__(self) -> None:
        self._dirty = False
        self._lock = threading.Lock()

    @property
    def is_dirty(self) -> bool:
        return self._dirty

    def mark_dirty(self) -> None:
        """Mark state as modified (a mutation occurred)."""
        with self._lock:
            self._dirty = True

    def mark_clean(self) -> None:
        """Mark state as clean (a save was performed)."""
        with self._lock:
            self._dirty = False

    def on_request(self, method: str, path: str) -> None:
        """Called after an AWS request completes. Sets dirty if mutating.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            path: Request path.
        """
        if method.upper() in _READ_ONLY_METHODS:
            return
        # Management endpoints don't affect AWS state
        for prefix in _MANAGEMENT_PREFIXES:
            if path.startswith(prefix):
                return
        self.mark_dirty()
