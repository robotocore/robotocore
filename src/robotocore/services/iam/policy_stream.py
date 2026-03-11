"""Real-time IAM policy evaluation stream.

Records every IAM policy evaluation for debugging and least-privilege analysis.

Configuration:
    IAM_POLICY_STREAM=1       Enable the stream (default: true when ENFORCE_IAM=1)
    IAM_POLICY_STREAM_SIZE=1000  Maximum entries to keep (default: 1000)
"""

from __future__ import annotations

import os
import re
import threading
import time
from collections import Counter, deque
from typing import Any


def is_stream_enabled() -> bool:
    """Check whether the policy stream is enabled."""
    explicit = os.environ.get("IAM_POLICY_STREAM")
    if explicit is not None:
        return explicit == "1"
    # Default: enabled when ENFORCE_IAM=1
    return os.environ.get("ENFORCE_IAM", "0") == "1"


class PolicyStream:
    """Thread-safe ring buffer of IAM policy evaluation entries."""

    def __init__(self, max_size: int | None = None):
        size = (
            max_size
            if max_size is not None
            else int(os.environ.get("IAM_POLICY_STREAM_SIZE", "1000"))
        )
        self._entries: deque[dict] = deque(maxlen=size)
        self._lock = threading.Lock()

    def record(
        self,
        *,
        principal: str,
        action: str,
        resource: str,
        decision: str,
        matched_policies: list[str] | None = None,
        matched_statement: dict | None = None,
        request_id: str = "",
        evaluation_duration_ms: float = 0.0,
    ) -> None:
        """Record an IAM policy evaluation."""
        entry = {
            "timestamp": time.time(),
            "principal": principal,
            "action": action,
            "resource": resource,
            "decision": decision,
            "matched_policies": matched_policies or [],
            "matched_statement": matched_statement,
            "request_id": request_id,
            "evaluation_duration_ms": round(evaluation_duration_ms, 3),
        }
        with self._lock:
            self._entries.append(entry)

    def recent(
        self,
        limit: int = 100,
        *,
        principal: str | None = None,
        action: str | None = None,
        decision: str | None = None,
    ) -> list[dict]:
        """Return recent entries (newest first), optionally filtered."""
        with self._lock:
            entries = list(self._entries)
        entries.reverse()

        if principal is not None:
            entries = [e for e in entries if e["principal"] == principal]
        if action is not None:
            entries = [e for e in entries if _action_matches(e["action"], action)]
        if decision is not None:
            entries = [e for e in entries if e["decision"] == decision]

        return entries[:limit]

    def clear(self) -> int:
        """Clear all entries. Returns count cleared."""
        with self._lock:
            count = len(self._entries)
            self._entries.clear()
            return count

    def summary(self) -> dict[str, Any]:
        """Compute aggregate summary of evaluations."""
        with self._lock:
            entries = list(self._entries)

        total = len(entries)
        allow_count = sum(1 for e in entries if e["decision"] == "Allow")
        deny_count = total - allow_count

        denied = [e for e in entries if e["decision"] == "Deny"]
        action_counts = Counter(e["action"] for e in denied)
        principal_counts = Counter(e["principal"] for e in denied)

        top_denied_actions = [{"action": a, "count": c} for a, c in action_counts.most_common(10)]
        top_denied_principals = [
            {"principal": p, "count": c} for p, c in principal_counts.most_common(10)
        ]

        return {
            "total_evaluations": total,
            "allow_count": allow_count,
            "deny_count": deny_count,
            "top_denied_actions": top_denied_actions,
            "top_denied_principals": top_denied_principals,
        }

    def suggest_policy(self, principal: str) -> dict[str, Any]:
        """Generate a minimal IAM policy covering all allowed actions for a principal."""
        with self._lock:
            entries = list(self._entries)

        # Collect allowed action -> set of resources
        resource_actions: dict[str, set[str]] = {}
        for e in entries:
            if e["principal"] == principal and e["decision"] == "Allow":
                res = e["resource"]
                if res not in resource_actions:
                    resource_actions[res] = set()
                resource_actions[res].add(e["action"])

        statements = []
        for resource, actions in sorted(resource_actions.items()):
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": sorted(actions) if len(actions) > 1 else list(actions)[0],
                    "Resource": resource,
                }
            )

        return {
            "Version": "2012-10-17",
            "Statement": statements,
        }


def format_stream_response(entries: list[dict]) -> dict[str, Any]:
    """Format entries for JSON API response."""
    return {
        "entries": entries,
        "count": len(entries),
    }


def _action_matches(value: str, pattern: str) -> bool:
    """Match action against a pattern supporting * wildcards."""
    if pattern == value:
        return True
    # Convert wildcard pattern to regex
    regex_parts = []
    for part in re.split(r"(\*)", pattern):
        if part == "*":
            regex_parts.append(".*")
        else:
            regex_parts.append(re.escape(part))
    regex = "^" + "".join(regex_parts) + "$"
    return bool(re.match(regex, value, re.IGNORECASE))


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_stream: PolicyStream | None = None


def get_policy_stream() -> PolicyStream:
    """Get or create the global policy stream singleton."""
    global _stream
    if _stream is None:
        _stream = PolicyStream()
    return _stream
