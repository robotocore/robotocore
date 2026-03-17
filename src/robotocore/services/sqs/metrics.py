"""SQS CloudWatch metrics — publishes queue metrics to the AWS/SQS namespace.

Periodically computes per-queue metrics (message counts, ages, throughput counters)
and publishes them directly to Moto's CloudWatch backend. Matches AWS behavior:
namespace AWS/SQS, dimension QueueName, 1-minute resolution.

Counter metrics (NumberOfMessagesSent, etc.) are tracked via thread-safe increment
functions called from the SQS provider, then reset each publish cycle.
"""

import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-queue counters (reset each publish cycle)
# ---------------------------------------------------------------------------

_counter_lock = threading.Lock()
# {queue_name: {"sent": N, "received": N, "deleted": N, "empty_receives": N, "sent_bytes": N}}
_counters: dict[str, dict[str, int]] = {}


def _get_counters(queue_name: str) -> dict[str, int]:
    """Return counter dict for a queue, creating if needed. Caller must hold _counter_lock."""
    if queue_name not in _counters:
        _counters[queue_name] = {
            "sent": 0,
            "received": 0,
            "deleted": 0,
            "empty_receives": 0,
            "sent_bytes": 0,
        }
    return _counters[queue_name]


def increment_sent(queue_name: str, message_size: int = 0) -> None:
    """Record a SendMessage call."""
    with _counter_lock:
        c = _get_counters(queue_name)
        c["sent"] += 1
        c["sent_bytes"] += message_size


def increment_received(queue_name: str, count: int = 1) -> None:
    """Record messages returned by ReceiveMessage."""
    with _counter_lock:
        c = _get_counters(queue_name)
        c["received"] += count


def increment_deleted(queue_name: str) -> None:
    """Record a DeleteMessage call."""
    with _counter_lock:
        c = _get_counters(queue_name)
        c["deleted"] += 1


def increment_empty_receives(queue_name: str) -> None:
    """Record a ReceiveMessage that returned 0 messages."""
    with _counter_lock:
        c = _get_counters(queue_name)
        c["empty_receives"] += 1


def snapshot_and_reset_counters() -> dict[str, dict[str, int]]:
    """Atomically snapshot all counters and reset them to zero.

    Returns a dict of {queue_name: {metric: value}}.
    """
    with _counter_lock:
        snapshot = {}
        for qname, c in _counters.items():
            snapshot[qname] = dict(c)
            for k in c:
                c[k] = 0
        return snapshot


# ---------------------------------------------------------------------------
# Metric computation from queue state
# ---------------------------------------------------------------------------


def compute_queue_metrics(queue) -> dict[str, float]:
    """Compute approximate metrics from a StandardQueue instance.

    Returns dict of metric_name -> value.
    """
    now = time.time()

    with queue.mutex:
        visible = queue._visible.qsize()
        inflight = len(queue._inflight)
        delayed = len(queue._delayed)

        # Age of oldest message: check all non-deleted messages
        oldest_age = 0.0
        for msg in queue._all_messages.values():
            if not msg.deleted:
                age = now - msg.created
                if age > oldest_age:
                    oldest_age = age

    return {
        "ApproximateNumberOfMessagesVisible": float(visible),
        "ApproximateNumberOfMessagesNotVisible": float(inflight),
        "ApproximateNumberOfMessagesDelayed": float(delayed),
        "ApproximateAgeOfOldestMessage": oldest_age,
    }


# ---------------------------------------------------------------------------
# CloudWatch publisher
# ---------------------------------------------------------------------------


def publish_metrics(
    account_id: str = "123456789012",
    region: str = "us-east-1",
) -> None:
    """Compute and publish SQS metrics to CloudWatch for all queues.

    Uses Moto's CloudWatch backend directly for efficiency (no HTTP round-trip).
    """
    from robotocore.services.sqs.provider import _get_store

    store = _get_store(region, account_id)
    counter_snapshot = snapshot_and_reset_counters()

    try:
        from moto.backends import get_backend

        cw_backend = get_backend("cloudwatch")[account_id][region]
    except (KeyError, TypeError):
        logger.debug("CloudWatch backend not available, skipping SQS metrics publish")
        return

    queues = store.list_queues()
    for queue in queues:
        qname = queue.name
        dimensions = [{"Name": "QueueName", "Value": qname}]

        # Gauge metrics from queue state
        gauge_metrics = compute_queue_metrics(queue)
        for metric_name, value in gauge_metrics.items():
            _put_metric(cw_backend, metric_name, value, dimensions)

        # Counter metrics from the snapshot
        counters = counter_snapshot.get(qname, {})
        counter_metric_map = {
            "NumberOfMessagesSent": counters.get("sent", 0),
            "NumberOfMessagesReceived": counters.get("received", 0),
            "NumberOfMessagesDeleted": counters.get("deleted", 0),
            "NumberOfEmptyReceives": counters.get("empty_receives", 0),
            "SentMessageSize": counters.get("sent_bytes", 0),
        }
        for metric_name, value in counter_metric_map.items():
            _put_metric(cw_backend, metric_name, float(value), dimensions)


def _put_metric(
    cw_backend,
    metric_name: str,
    value: float,
    dimensions: list[dict],
) -> None:
    """Write a single metric datum to the CloudWatch backend."""
    # Moto's put_metric_data expects a list of MetricDatum-like dicts.
    # We use the internal method that the Moto responses layer calls.
    try:
        cw_backend.put_metric_data(
            namespace="AWS/SQS",
            metric_data=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": "Count" if "Size" not in metric_name else "Bytes",
                    "Dimensions": [{"Name": d["Name"], "Value": d["Value"]} for d in dimensions],
                }
            ],
        )
    except Exception:  # noqa: BLE001
        logger.debug("Failed to put metric %s", metric_name, exc_info=True)


# ---------------------------------------------------------------------------
# Background publisher thread
# ---------------------------------------------------------------------------

_publisher: "SqsMetricsPublisher | None" = None
_publisher_lock = threading.Lock()


class SqsMetricsPublisher:
    """Background daemon that periodically publishes SQS metrics to CloudWatch."""

    def __init__(self, interval: int | None = None):
        self.interval = interval or int(os.environ.get("SQS_METRICS_INTERVAL", "60"))
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the background publisher thread."""
        if not self._is_enabled():
            logger.info("SQS CloudWatch metrics disabled (SQS_CLOUDWATCH_METRICS=false)")
            return
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="sqs-metrics")
        self._thread.start()
        logger.info("SQS CloudWatch metrics publisher started (interval=%ds)", self.interval)

    def stop(self) -> None:
        """Signal the background thread to stop."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(self.interval)
            if self._stop_event.is_set():
                break
            try:
                publish_metrics()
            except Exception:  # noqa: BLE001
                logger.debug("SQS metrics publish error", exc_info=True)

    @staticmethod
    def _is_enabled() -> bool:
        val = os.environ.get("SQS_CLOUDWATCH_METRICS", "true").lower()
        return val in ("true", "1", "yes")


def get_sqs_metrics_publisher() -> SqsMetricsPublisher:
    """Return the global SqsMetricsPublisher singleton."""
    global _publisher
    with _publisher_lock:
        if _publisher is None:
            _publisher = SqsMetricsPublisher()
        return _publisher
