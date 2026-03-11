"""State consistency guarantees for concurrent request handling.

Wraps StateManager to prevent mutations during snapshot saves, ensuring
consistent state images. Uses a read-write lock pattern where:

- Normal AWS requests acquire a read lock (many can proceed concurrently)
- Save operations acquire a write lock (exclusive, pauses mutations briefly)

Also provides snapshot versioning: a monotonic counter increments on every
mutation, and saves record the version at start/end to detect any
inconsistency that slips through.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any

from robotocore.state.rwlock import ReadWriteLock

logger = logging.getLogger(__name__)

# HTTP methods that are read-only (never mutate state)
_READ_ONLY_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# Default timeout for requests waiting during a save (seconds)
_DEFAULT_MUTATION_PAUSE_TIMEOUT = 5.0


@dataclass
class SaveResult:
    """Result of a consistent save operation."""

    path: str
    version_start: int
    version_end: int
    consistent: bool
    duration_ms: float
    used_cow: bool = False

    @property
    def potentially_inconsistent(self) -> bool:
        """True if mutations occurred during the save."""
        return not self.consistent


@dataclass
class ConsistencyStatus:
    """Current consistency state for the status endpoint."""

    mutation_version: int
    last_save_version_start: int | None
    last_save_version_end: int | None
    last_save_consistent: bool | None
    mutations_paused: bool
    active_readers: int
    writer_active: bool
    saves_total: int
    inconsistent_saves_total: int


class MutationVersionTracker:
    """Thread-safe monotonic counter that increments on every state mutation."""

    def __init__(self) -> None:
        self._version: int = 0
        self._lock = threading.Lock()

    @property
    def version(self) -> int:
        """Current mutation version."""
        return self._version

    def increment(self) -> int:
        """Increment and return the new version."""
        with self._lock:
            self._version += 1
            return self._version

    def reset(self) -> None:
        """Reset version to 0 (for testing)."""
        with self._lock:
            self._version = 0


class CopyOnWriteSnapshot:
    """Take shallow copies of native state dicts for serialization.

    Allows mutations to continue on live state while the save serializes
    from the frozen copy. Only effective for native providers -- Moto
    backends are complex object graphs that need the pause approach.
    """

    def __init__(self) -> None:
        self._snapshot: dict[str, Any] | None = None

    def capture(self, native_handlers: dict[str, tuple]) -> dict[str, Any]:
        """Take a shallow copy of all native provider state.

        Args:
            native_handlers: Dict of service -> (save_fn, load_fn) tuples.

        Returns:
            Dict of service -> state_copy.
        """
        snapshot: dict[str, Any] = {}
        for service, (save_fn, _) in native_handlers.items():
            try:
                state = save_fn()
                # Shallow copy to isolate from further mutations
                snapshot[service] = copy.copy(state) if isinstance(state, dict) else state
            except Exception:
                logger.debug("Could not snapshot native state for %s", service, exc_info=True)
        self._snapshot = snapshot
        return snapshot

    @property
    def data(self) -> dict[str, Any] | None:
        """The captured snapshot data, or None if not captured."""
        return self._snapshot

    def clear(self) -> None:
        """Release the snapshot data."""
        self._snapshot = None


class ConsistentStateManager:
    """Wraps StateManager with consistency guarantees.

    Coordinates between incoming requests and save operations using a
    read-write lock. Normal requests acquire read access (concurrent),
    while saves acquire write access (exclusive).

    Also tracks a mutation version counter and supports copy-on-write
    snapshots for native provider state.
    """

    def __init__(self, manager: Any) -> None:
        from robotocore.state.manager import StateManager

        if not isinstance(manager, StateManager):
            raise TypeError(f"Expected StateManager, got {type(manager).__name__}")

        self._manager: StateManager = manager
        self._rw_lock = ReadWriteLock()
        self._version_tracker = MutationVersionTracker()
        self._mutations_paused = False
        self._resume_event = asyncio.Event()
        self._resume_event.set()  # not paused initially
        self._pause_timeout = _DEFAULT_MUTATION_PAUSE_TIMEOUT

        # Save statistics
        self._saves_total: int = 0
        self._inconsistent_saves_total: int = 0
        self._last_save_version_start: int | None = None
        self._last_save_version_end: int | None = None
        self._last_save_consistent: bool | None = None

    @property
    def manager(self) -> Any:
        """The underlying StateManager."""
        return self._manager

    @property
    def mutations_paused(self) -> bool:
        """Whether mutations are currently paused for a save."""
        return self._mutations_paused

    @property
    def mutation_version(self) -> int:
        """Current mutation version counter."""
        return self._version_tracker.version

    @property
    def rw_lock(self) -> ReadWriteLock:
        """The read-write lock (exposed for status endpoint)."""
        return self._rw_lock

    def record_mutation(self, method: str = "POST", path: str = "/") -> int:
        """Record a mutation and increment the version counter.

        Args:
            method: HTTP method. Read-only methods are ignored.
            path: Request path. Management endpoints are ignored.

        Returns:
            The new version number, or current version if not a mutation.
        """
        if method.upper() in _READ_ONLY_METHODS:
            return self._version_tracker.version
        if path.startswith("/_robotocore/"):
            return self._version_tracker.version
        return self._version_tracker.increment()

    async def save(self, **kwargs: Any) -> SaveResult:
        """Perform a consistent save with mutation pausing.

        Acquires the write lock, pauses mutations, saves state, then resumes.
        Records version at start and end to detect inconsistency.
        """
        start_time = time.monotonic()

        async with self._rw_lock.write():
            self._pause_mutations()
            version_start = self._version_tracker.version
            try:
                path = self._manager.save(**kwargs)
            finally:
                version_end = self._version_tracker.version
                self._resume_mutations()

        duration_ms = (time.monotonic() - start_time) * 1000
        consistent = version_start == version_end

        self._saves_total += 1
        self._last_save_version_start = version_start
        self._last_save_version_end = version_end
        self._last_save_consistent = consistent
        if not consistent:
            self._inconsistent_saves_total += 1

        return SaveResult(
            path=path,
            version_start=version_start,
            version_end=version_end,
            consistent=consistent,
            duration_ms=duration_ms,
        )

    async def save_with_cow(self, **kwargs: Any) -> SaveResult:
        """Save using copy-on-write for native state.

        Takes a shallow copy of native state dicts before saving,
        allowing mutations to continue on live state during serialization.
        Moto backends still use the pause approach.
        """
        start_time = time.monotonic()
        cow = CopyOnWriteSnapshot()

        async with self._rw_lock.write():
            self._pause_mutations()
            version_start = self._version_tracker.version
            try:
                # Capture native state snapshot
                cow.capture(self._manager._native_handlers)
                path = self._manager.save(**kwargs)
            finally:
                version_end = self._version_tracker.version
                self._resume_mutations()
                cow.clear()

        duration_ms = (time.monotonic() - start_time) * 1000
        consistent = version_start == version_end

        self._saves_total += 1
        self._last_save_version_start = version_start
        self._last_save_version_end = version_end
        self._last_save_consistent = consistent
        if not consistent:
            self._inconsistent_saves_total += 1

        return SaveResult(
            path=path,
            version_start=version_start,
            version_end=version_end,
            consistent=consistent,
            duration_ms=duration_ms,
            used_cow=True,
        )

    async def on_request(self, method: str, path: str) -> None:
        """Called before processing an AWS request.

        If mutations are paused (save in progress), read-only requests
        pass through immediately. Write requests wait for the save to
        complete, with a timeout to prevent indefinite blocking.

        Args:
            method: HTTP method.
            path: Request path.
        """
        # Read-only requests always pass through
        if method.upper() in _READ_ONLY_METHODS:
            return

        # Management endpoints pass through
        if path.startswith("/_robotocore/"):
            return

        # If mutations are paused, wait for resume
        if self._mutations_paused:
            try:
                await asyncio.wait_for(
                    self._wait_for_resume(),
                    timeout=self._pause_timeout,
                )
            except TimeoutError:
                logger.warning(
                    "Request waited %.1fs for save to complete, proceeding anyway",
                    self._pause_timeout,
                )

    async def _wait_for_resume(self) -> None:
        """Wait until mutations are resumed."""
        await self._resume_event.wait()

    def _pause_mutations(self) -> None:
        """Pause incoming mutations (called under write lock)."""
        self._mutations_paused = True
        self._resume_event.clear()

    def _resume_mutations(self) -> None:
        """Resume incoming mutations."""
        self._mutations_paused = False
        self._resume_event.set()

    def status(self) -> ConsistencyStatus:
        """Return current consistency status."""
        return ConsistencyStatus(
            mutation_version=self._version_tracker.version,
            last_save_version_start=self._last_save_version_start,
            last_save_version_end=self._last_save_version_end,
            last_save_consistent=self._last_save_consistent,
            mutations_paused=self._mutations_paused,
            active_readers=self._rw_lock.readers,
            writer_active=self._rw_lock.writer_active,
            saves_total=self._saves_total,
            inconsistent_saves_total=self._inconsistent_saves_total,
        )

    def status_dict(self) -> dict[str, Any]:
        """Return consistency status as a JSON-serializable dict."""
        s = self.status()
        return {
            "mutation_version": s.mutation_version,
            "last_save": {
                "version_start": s.last_save_version_start,
                "version_end": s.last_save_version_end,
                "consistent": s.last_save_consistent,
            },
            "locks": {
                "mutations_paused": s.mutations_paused,
                "active_readers": s.active_readers,
                "writer_active": s.writer_active,
            },
            "stats": {
                "saves_total": s.saves_total,
                "inconsistent_saves_total": s.inconsistent_saves_total,
            },
        }


# Singleton
_consistent_manager: ConsistentStateManager | None = None


def get_consistent_state_manager() -> ConsistentStateManager:
    """Get the global ConsistentStateManager instance."""
    global _consistent_manager
    if _consistent_manager is None:
        from robotocore.state.manager import get_state_manager

        _consistent_manager = ConsistentStateManager(get_state_manager())
    return _consistent_manager


def reset_consistent_state_manager() -> None:
    """Reset the singleton (for testing)."""
    global _consistent_manager
    _consistent_manager = None
