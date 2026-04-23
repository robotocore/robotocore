"""SQS behavioral fidelity: PurgeInProgress, QueueDeletedRecently, message retention.

These features match real AWS SQS behavior that Moto does not enforce:
- PurgeQueue returns PurgeQueueInProgress if called within 60s on the same queue
- CreateQueue returns QueueDeletedRecently if the queue was deleted within 60s
- Messages older than MessageRetentionPeriod are automatically removed
"""

import logging
import os
import threading
import time
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from robotocore.services.sqs.models import SqsStore


PURGE_COOLDOWN_SECONDS = 60
DELETE_COOLDOWN_SECONDS = 60
DEFAULT_RETENTION_PERIOD = 345600  # 4 days in seconds
DEFAULT_SCAN_INTERVAL = 60  # seconds


class PurgeQueueInProgressError(Exception):
    """Raised when PurgeQueue is called too soon after a previous purge."""

    def __init__(self):
        super().__init__(
            "PurgeQueueInProgress: Only one PurgeQueue operation on "
            "the same queue is allowed every 60 seconds."
        )


class QueueDeletedRecentlyError(Exception):
    """Raised when CreateQueue is called for a recently-deleted queue name."""

    def __init__(self, name: str):
        super().__init__(
            f"QueueDeletedRecently: You must wait 60 seconds after deleting "
            f"a queue before you can create another with the same name. "
            f"Queue: {name}"
        )


class PurgeTracker:
    """Tracks last purge time per queue name. Thread-safe."""

    def __init__(self):
        self._purge_times: dict[str, float] = {}
        self._lock = threading.Lock()

    def _is_enabled(self) -> bool:
        return os.environ.get("SQS_DELAY_PURGE_RETRY", "true").lower() != "false"

    def check_and_record(self, queue_name: str) -> None:
        """Check if purge is allowed; if so, record the time. Raises on cooldown."""
        if not self._is_enabled():
            return
        with self._lock:
            now = time.time()
            last = self._purge_times.get(queue_name)
            if last is not None and (now - last) < PURGE_COOLDOWN_SECONDS:
                raise PurgeQueueInProgressError()
            self._purge_times[queue_name] = now

    def remove(self, queue_name: str) -> None:
        """Remove tracking for a deleted queue."""
        with self._lock:
            self._purge_times.pop(queue_name, None)

    def snapshot_state(self) -> dict[str, float]:
        """Return a serializable copy of purge cooldown state."""
        with self._lock:
            return dict(self._purge_times)

    def restore_state(self, state: dict[str, float] | None) -> None:
        """Replace purge cooldown state from a snapshot."""
        with self._lock:
            self._purge_times.clear()
            self._purge_times.update(state or {})


class QueueDeletedTracker:
    """Tracks recently-deleted queue names with timestamps. Thread-safe."""

    def __init__(self):
        self._deletion_times: dict[str, float] = {}
        self._lock = threading.Lock()

    def _is_enabled(self) -> bool:
        return os.environ.get("SQS_DELAY_RECENTLY_DELETED", "true").lower() != "false"

    def record_deletion(self, queue_name: str) -> None:
        """Record that a queue was deleted."""
        with self._lock:
            self._deletion_times[queue_name] = time.time()

    def check_create(self, queue_name: str) -> None:
        """Check if creating this queue name is allowed. Raises if recently deleted."""
        if not self._is_enabled():
            return
        with self._lock:
            deleted_at = self._deletion_times.get(queue_name)
            if deleted_at is not None and (time.time() - deleted_at) < DELETE_COOLDOWN_SECONDS:
                raise QueueDeletedRecentlyError(queue_name)
            # Clean up expired entry
            self._deletion_times.pop(queue_name, None)

    def snapshot_state(self) -> dict[str, float]:
        """Return a serializable copy of recent deletion state."""
        with self._lock:
            return dict(self._deletion_times)

    def restore_state(self, state: dict[str, float] | None) -> None:
        """Replace recent deletion state from a snapshot."""
        with self._lock:
            self._deletion_times.clear()
            self._deletion_times.update(state or {})


class RetentionScanner:
    """Background scanner that removes messages past their retention period."""

    def __init__(self, scan_interval: int = DEFAULT_SCAN_INTERVAL):
        self.scan_interval = scan_interval
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def _is_enabled(self) -> bool:
        return os.environ.get("SQS_ENABLE_MESSAGE_RETENTION_PERIOD", "true").lower() != "false"

    def scan_store(self, store: "SqsStore") -> None:
        """Scan all queues in a store and remove expired messages."""
        if not self._is_enabled():
            return
        now = time.time()
        for queue in list(store.queues.values()):
            retention = int(
                queue.attributes.get("MessageRetentionPeriod", DEFAULT_RETENTION_PERIOD)
            )
            expired_ids = []
            with queue.mutex:
                for msg_id, msg in list(queue._all_messages.items()):
                    if (now - msg.created) > retention:
                        expired_ids.append(msg_id)
                for msg_id in expired_ids:
                    msg = queue._all_messages.pop(msg_id, None)
                    if msg:
                        msg.deleted = True
                        queue._inflight.pop(msg_id, None)
                        queue._delayed.pop(msg_id, None)

    def start(self, stores: dict) -> None:
        """Start the background scanning thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, args=(stores,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal the background thread to stop."""
        self._stop_event.set()

    def _run(self, stores: dict) -> None:
        """Background loop: scan all stores periodically."""
        while not self._stop_event.wait(timeout=self.scan_interval):
            for store in list(stores.values()):
                try:
                    self.scan_store(store)
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "Failed to scan store for expired messages",
                        exc_info=True,
                    )
