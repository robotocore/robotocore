"""Usage analytics engine for API request tracking.

Tracks per-service and per-operation statistics including request counts,
error rates, latency, and client patterns. Uses in-memory counters with
a rolling time-series window.

Configuration:
    USAGE_ANALYTICS=1          Enable/disable analytics (default: enabled)
    USAGE_ANALYTICS_WINDOW=60  Rolling window in minutes (default: 60)
"""

import os
import threading
import time
from collections import defaultdict


class UsageAnalytics:
    """Thread-safe usage analytics with per-service/operation counters and time series."""

    def __init__(self, window_minutes: int | None = None):
        self._enabled = os.environ.get("USAGE_ANALYTICS", "1") != "0"
        self._window_minutes = window_minutes or int(os.environ.get("USAGE_ANALYTICS_WINDOW", "60"))
        self._lock = threading.Lock()

        # Per-service counters: service -> {total, success, error, latency_sum}
        self._service_stats: dict[str, dict] = defaultdict(
            lambda: {"total": 0, "success": 0, "error": 0, "latency_sum": 0.0}
        )

        # Per-operation counters: (service, operation) -> {total, success, error, latency_sum}
        self._operation_stats: dict[tuple[str, str], dict] = defaultdict(
            lambda: {"total": 0, "success": 0, "error": 0, "latency_sum": 0.0}
        )

        # Time series: minute_bucket (int, epoch // 60) -> count
        self._timeline: dict[int, int] = defaultdict(int)

        # Error breakdown
        self._errors_by_status: dict[str, int] = defaultdict(int)
        self._errors_by_type: dict[str, int] = defaultdict(int)

        # Recent errors for display
        self._recent_errors: list[dict] = []
        self._max_recent_errors = 100

        # Client tracking: access_key_id -> request count
        self._client_counts: dict[str, int] = defaultdict(int)

        # Global counters
        self._total_requests = 0
        self._total_errors = 0
        self._total_latency = 0.0

    def record_request(
        self,
        *,
        service: str,
        operation: str | None = None,
        status_code: int = 200,
        duration_ms: float = 0.0,
        error_type: str | None = None,
        access_key_id: str | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Record a single API request."""
        if not self._enabled:
            return

        ts = timestamp or time.time()
        is_error = status_code >= 400
        op = operation or "Unknown"

        with self._lock:
            # Global
            self._total_requests += 1
            self._total_latency += duration_ms
            if is_error:
                self._total_errors += 1

            # Service stats
            svc = self._service_stats[service]
            svc["total"] += 1
            svc["latency_sum"] += duration_ms
            if is_error:
                svc["error"] += 1
            else:
                svc["success"] += 1

            # Operation stats
            op_key = (service, op)
            ops = self._operation_stats[op_key]
            ops["total"] += 1
            ops["latency_sum"] += duration_ms
            if is_error:
                ops["error"] += 1
            else:
                ops["success"] += 1

            # Timeline (per-minute bucket)
            minute_bucket = int(ts) // 60
            self._timeline[minute_bucket] += 1

            # Prune old timeline entries
            cutoff = int(ts) // 60 - self._window_minutes
            old_keys = [k for k in self._timeline if k < cutoff]
            for k in old_keys:
                del self._timeline[k]

            # Error tracking
            if is_error:
                self._errors_by_status[str(status_code)] += 1
                if error_type:
                    self._errors_by_type[error_type] += 1
                self._recent_errors.append(
                    {
                        "timestamp": ts,
                        "service": service,
                        "operation": op,
                        "status_code": status_code,
                        "error_type": error_type,
                    }
                )
                if len(self._recent_errors) > self._max_recent_errors:
                    self._recent_errors = self._recent_errors[-self._max_recent_errors :]

            # Client tracking
            if access_key_id:
                self._client_counts[access_key_id] += 1

    def get_usage_summary(self) -> dict:
        """Return overall usage summary."""
        with self._lock:
            avg_latency = (
                self._total_latency / self._total_requests if self._total_requests > 0 else 0.0
            )
            # Top clients sorted by request count
            top_clients = sorted(
                [{"access_key_id": k, "request_count": v} for k, v in self._client_counts.items()],
                key=lambda x: x["request_count"],
                reverse=True,
            )[:10]

            return {
                "total_requests": self._total_requests,
                "total_errors": self._total_errors,
                "services_used": len(self._service_stats),
                "avg_latency_ms": round(avg_latency, 2),
                "unique_clients": len(self._client_counts),
                "top_clients": top_clients,
            }

    def get_service_stats(self, service: str) -> dict:
        """Return stats for a specific service including operation breakdown."""
        with self._lock:
            svc = self._service_stats.get(service)
            if not svc:
                return {
                    "total_requests": 0,
                    "success_count": 0,
                    "error_count": 0,
                    "avg_latency_ms": 0.0,
                    "operations": {},
                }

            avg = svc["latency_sum"] / svc["total"] if svc["total"] > 0 else 0.0

            # Collect operations for this service
            operations = {}
            for (s, op), op_stats in self._operation_stats.items():
                if s == service:
                    op_avg = (
                        op_stats["latency_sum"] / op_stats["total"]
                        if op_stats["total"] > 0
                        else 0.0
                    )
                    operations[op] = {
                        "total_requests": op_stats["total"],
                        "success_count": op_stats["success"],
                        "error_count": op_stats["error"],
                        "avg_latency_ms": round(op_avg, 2),
                    }

            return {
                "total_requests": svc["total"],
                "success_count": svc["success"],
                "error_count": svc["error"],
                "avg_latency_ms": round(avg, 2),
                "operations": operations,
            }

    def get_all_service_stats(self) -> dict[str, dict]:
        """Return stats for all services."""
        with self._lock:
            services = {}
            for service, svc in self._service_stats.items():
                avg = svc["latency_sum"] / svc["total"] if svc["total"] > 0 else 0.0
                services[service] = {
                    "total_requests": svc["total"],
                    "success_count": svc["success"],
                    "error_count": svc["error"],
                    "avg_latency_ms": round(avg, 2),
                }
            return services

    def get_error_summary(self) -> dict:
        """Return error breakdown."""
        with self._lock:
            return {
                "total_errors": self._total_errors,
                "by_status_code": dict(self._errors_by_status),
                "by_error_type": dict(self._errors_by_type),
                "recent_errors": list(self._recent_errors[-20:]),
            }

    def get_timeline(self) -> list[dict]:
        """Return per-minute request counts for the rolling window."""
        with self._lock:
            now = int(time.time()) // 60
            cutoff = now - self._window_minutes
            entries = []
            for minute_bucket, count in sorted(self._timeline.items()):
                if minute_bucket >= cutoff:
                    entries.append({"minute": minute_bucket * 60, "count": count})
            return entries


# Singleton
_analytics: UsageAnalytics | None = None


def get_usage_analytics() -> UsageAnalytics:
    """Get or create the global UsageAnalytics singleton."""
    global _analytics
    if _analytics is None:
        _analytics = UsageAnalytics()
    return _analytics


def _reset_singleton() -> None:
    """Reset the singleton (for testing)."""
    global _analytics
    _analytics = None
