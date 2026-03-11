"""Unified observability hub connecting chaos, audit, IAM, and diagnostics.

Provides a central event bus with request correlation, timeline queries,
and enhanced diagnostics integration.
"""

from __future__ import annotations

import statistics
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum


class EventType(StrEnum):
    """Types of observability events."""

    CHAOS = "chaos"
    IAM = "iam"
    AUDIT = "audit"
    ERROR = "error"


@dataclass
class RequestTrace:
    """Complete trace of a single request through all systems."""

    request_id: str
    service: str
    operation: str | None
    timestamp: float
    chaos_rule_matched: dict | None = None
    chaos_action_taken: str | None = None  # "error_injected", "latency_added", None
    iam_decision: str | None = None  # "ALLOW", "DENY", "IMPLICIT_DENY", None
    iam_matched_policy: str | None = None
    iam_principal: str | None = None
    audit_entry: dict | None = None
    response_status: int | None = None
    duration_ms: float | None = None


@dataclass
class TimelineEntry:
    """A single event in the unified timeline."""

    timestamp: float
    event_type: str  # EventType value
    request_id: str
    service: str
    operation: str | None = None
    summary: str = ""
    details: dict = field(default_factory=dict)


class ObservabilityHub:
    """Central event bus connecting chaos, audit, IAM, and diagnostics.

    Thread-safe. Records events and correlates them by request_id for
    end-to-end request tracing.
    """

    def __init__(self, max_events: int = 5000):
        self._lock = threading.Lock()
        self._events: deque[TimelineEntry] = deque(maxlen=max_events)
        self._traces: dict[str, RequestTrace] = {}
        self._trace_order: deque[str] = deque(maxlen=max_events)
        self._max_traces = max_events

    @staticmethod
    def generate_request_id() -> str:
        """Generate a unique request ID."""
        return uuid.uuid4().hex[:16]

    def record_event(self, event: TimelineEntry) -> None:
        """Record an event in the timeline."""
        with self._lock:
            self._events.append(event)

    def record_chaos_event(
        self,
        *,
        request_id: str,
        service: str,
        operation: str | None,
        rule: dict,
        action_taken: str,
    ) -> None:
        """Record a chaos fault injection event."""
        entry = TimelineEntry(
            timestamp=time.time(),
            event_type=EventType.CHAOS,
            request_id=request_id,
            service=service,
            operation=operation,
            summary=f"Chaos: {action_taken} via rule {rule.get('rule_id', '?')}",
            details={"rule": rule, "action_taken": action_taken},
        )
        with self._lock:
            self._events.append(entry)
            trace = self._get_or_create_trace(request_id, service, operation)
            trace.chaos_rule_matched = rule
            trace.chaos_action_taken = action_taken

    def record_iam_event(
        self,
        *,
        request_id: str,
        service: str,
        operation: str | None,
        decision: str,
        principal: str | None = None,
        matched_policy: str | None = None,
        action: str | None = None,
        resource: str | None = None,
    ) -> None:
        """Record an IAM policy evaluation event."""
        entry = TimelineEntry(
            timestamp=time.time(),
            event_type=EventType.IAM,
            request_id=request_id,
            service=service,
            operation=operation,
            summary=f"IAM: {decision} for {principal or 'anonymous'}",
            details={
                "decision": decision,
                "principal": principal,
                "matched_policy": matched_policy,
                "action": action,
                "resource": resource,
            },
        )
        with self._lock:
            self._events.append(entry)
            trace = self._get_or_create_trace(request_id, service, operation)
            trace.iam_decision = decision
            trace.iam_matched_policy = matched_policy
            trace.iam_principal = principal

    def record_audit_event(
        self,
        *,
        request_id: str,
        service: str,
        operation: str | None,
        status_code: int,
        duration_ms: float,
        error: str | None = None,
    ) -> None:
        """Record an audit/completion event for a request."""
        entry = TimelineEntry(
            timestamp=time.time(),
            event_type=EventType.AUDIT if not error else EventType.ERROR,
            request_id=request_id,
            service=service,
            operation=operation,
            summary=f"{service}:{operation or '?'} -> {status_code} ({duration_ms:.1f}ms)",
            details={
                "status_code": status_code,
                "duration_ms": duration_ms,
                "error": error,
            },
        )
        with self._lock:
            self._events.append(entry)
            trace = self._get_or_create_trace(request_id, service, operation)
            trace.response_status = status_code
            trace.duration_ms = duration_ms
            trace.audit_entry = {
                "status_code": status_code,
                "duration_ms": duration_ms,
                "error": error,
            }

    def get_request_trace(self, request_id: str) -> RequestTrace | None:
        """Get the complete trace for a single request."""
        with self._lock:
            return self._traces.get(request_id)

    def get_timeline(
        self,
        *,
        limit: int = 100,
        service: str | None = None,
        event_types: list[str] | None = None,
        request_id: str | None = None,
        min_duration: float | None = None,
    ) -> list[dict]:
        """Get a filtered, chronological timeline of events.

        Args:
            limit: Max entries to return.
            service: Filter by service name.
            event_types: Filter by event type (e.g. ["chaos", "iam"]).
            request_id: Get all events for one request.
            min_duration: Filter to requests slower than this (ms).
        """
        with self._lock:
            entries = list(self._events)

        # Newest first
        entries.reverse()

        if service:
            entries = [e for e in entries if e.service == service]
        if event_types:
            entries = [e for e in entries if e.event_type in event_types]
        if request_id:
            entries = [e for e in entries if e.request_id == request_id]
        if min_duration is not None:
            # Filter by duration from details
            entries = [e for e in entries if e.details.get("duration_ms", 0) >= min_duration]

        result = []
        for e in entries[:limit]:
            result.append(
                {
                    "timestamp": e.timestamp,
                    "event_type": e.event_type,
                    "request_id": e.request_id,
                    "service": e.service,
                    "operation": e.operation,
                    "summary": e.summary,
                    "details": e.details,
                }
            )
        return result

    def get_diagnostics_summary(self) -> dict:
        """Get enhanced diagnostics data for the diagnose endpoint.

        Returns:
            Dict with chaos stats, IAM denial summary, latency percentiles,
            and per-service error rates.
        """
        with self._lock:
            events = list(self._events)
            traces = dict(self._traces)

        # Active chaos rules with match counts
        chaos_events = [e for e in events if e.event_type == EventType.CHAOS]
        chaos_summary = {
            "total_faults_injected": len(chaos_events),
            "recent_faults": [
                {
                    "request_id": e.request_id,
                    "service": e.service,
                    "operation": e.operation,
                    "rule_id": e.details.get("rule", {}).get("rule_id"),
                    "action": e.details.get("action_taken"),
                    "timestamp": e.timestamp,
                }
                for e in chaos_events[-10:]
            ],
        }

        # Recent IAM denials
        iam_denials = [
            e
            for e in events
            if e.event_type == EventType.IAM
            and e.details.get("decision") in ("Deny", "ImplicitDeny", "DENY", "IMPLICIT_DENY")
        ]
        iam_summary = {
            "total_denials": len(iam_denials),
            "recent_denials": [
                {
                    "request_id": e.request_id,
                    "service": e.service,
                    "operation": e.operation,
                    "principal": e.details.get("principal"),
                    "action": e.details.get("action"),
                    "matched_policy": e.details.get("matched_policy"),
                    "timestamp": e.timestamp,
                }
                for e in iam_denials[-10:]
            ],
        }

        # Latency percentiles from completed requests
        durations = []
        error_by_service: dict[str, dict[str, int]] = {}
        for trace in traces.values():
            if trace.duration_ms is not None:
                durations.append(trace.duration_ms)
            if trace.response_status is not None:
                svc = trace.service
                if svc not in error_by_service:
                    error_by_service[svc] = {"total": 0, "errors": 0}
                error_by_service[svc]["total"] += 1
                if trace.response_status >= 400:
                    error_by_service[svc]["errors"] += 1

        latency_stats: dict[str, float] = {}
        if durations:
            durations_sorted = sorted(durations)
            latency_stats["p50"] = round(durations_sorted[int(len(durations_sorted) * 0.50)], 2)
            latency_stats["p95"] = round(
                durations_sorted[min(int(len(durations_sorted) * 0.95), len(durations_sorted) - 1)],
                2,
            )
            latency_stats["p99"] = round(
                durations_sorted[min(int(len(durations_sorted) * 0.99), len(durations_sorted) - 1)],
                2,
            )
            latency_stats["mean"] = round(statistics.mean(durations), 2)

        # Error rates by service
        error_rates = {}
        for svc, counts in error_by_service.items():
            rate = counts["errors"] / counts["total"] if counts["total"] > 0 else 0
            error_rates[svc] = {
                "total_requests": counts["total"],
                "error_count": counts["errors"],
                "error_rate": round(rate, 4),
            }

        return {
            "chaos": chaos_summary,
            "iam": iam_summary,
            "latency": latency_stats,
            "error_rates": error_rates,
        }

    def clear(self) -> None:
        """Clear all events and traces."""
        with self._lock:
            self._events.clear()
            self._traces.clear()
            self._trace_order.clear()

    def _get_or_create_trace(
        self, request_id: str, service: str, operation: str | None
    ) -> RequestTrace:
        """Get or create a RequestTrace. Must be called with lock held."""
        if request_id not in self._traces:
            # Evict oldest if at capacity
            if len(self._trace_order) >= self._max_traces:
                oldest = self._trace_order.popleft()
                self._traces.pop(oldest, None)
            self._traces[request_id] = RequestTrace(
                request_id=request_id,
                service=service,
                operation=operation,
                timestamp=time.time(),
            )
            self._trace_order.append(request_id)
        return self._traces[request_id]


# Singleton
_hub: ObservabilityHub | None = None
_hub_lock = threading.Lock()


def get_observability_hub() -> ObservabilityHub:
    """Get the singleton ObservabilityHub instance."""
    global _hub
    if _hub is None:
        with _hub_lock:
            if _hub is None:
                _hub = ObservabilityHub()
    return _hub


def reset_hub() -> None:
    """Reset the singleton hub (for testing)."""
    global _hub
    _hub = None
