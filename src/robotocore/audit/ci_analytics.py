"""CI analytics: track test reliability and service usage across CI builds.

Activates automatically when CI env vars are detected (CI=true, GITHUB_ACTIONS,
GITLAB_CI, JENKINS_URL, CIRCLECI) or when ROBOTOCORE_CI_SESSION is set.

Session summaries are persisted as JSON files in ROBOTOCORE_STATE_DIR/ci_analytics/.
"""

import json
import os
import threading
import time
import uuid
from collections import defaultdict
from pathlib import Path


def detect_ci_provider() -> tuple[str | None, str | None]:
    """Detect CI provider from environment variables.

    Returns (provider_name, build_id) or (None, None) if not in CI.
    """
    if os.environ.get("GITHUB_ACTIONS"):
        return "github_actions", os.environ.get("GITHUB_RUN_ID")
    if os.environ.get("GITLAB_CI"):
        return "gitlab_ci", os.environ.get("CI_JOB_ID")
    if os.environ.get("JENKINS_URL"):
        return "jenkins", os.environ.get("BUILD_NUMBER")
    if os.environ.get("CIRCLECI"):
        return "circleci", os.environ.get("CIRCLE_BUILD_NUM")
    if os.environ.get("CI"):
        return "generic_ci", None
    return None, None


class CISession:
    """In-memory representation of a single CI session."""

    def __init__(
        self,
        session_id: str,
        ci_provider: str | None = None,
        build_id: str | None = None,
    ):
        self.session_id = session_id
        self.ci_provider = ci_provider
        self.build_id = build_id
        self.start_time: float = time.time()
        self.end_time: float = 0.0
        self.total_requests: int = 0
        self.error_count: int = 0
        self.services_used: set[str] = set()
        self.service_counts: dict[str, int] = defaultdict(int)
        self.operation_stats: dict[str, dict[str, int]] = defaultdict(
            lambda: {"success": 0, "failure": 0}
        )

    @property
    def duration(self) -> float:
        end = self.end_time if self.end_time > 0 else time.time()
        return end - self.start_time

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": round(self.duration, 3),
            "total_requests": self.total_requests,
            "error_count": self.error_count,
            "services_used": sorted(self.services_used),
            "service_counts": dict(self.service_counts),
            "operation_stats": dict(self.operation_stats),
            "ci_provider": self.ci_provider,
            "build_id": self.build_id,
        }


class CIAnalytics:
    """Thread-safe CI session tracker."""

    def __init__(self, session: CISession):
        self.session = session
        self._lock = threading.Lock()

    def record_request(
        self,
        *,
        service: str,
        operation: str | None = None,
        success: bool = True,
    ) -> None:
        """Record a request within the current CI session."""
        with self._lock:
            self.session.total_requests += 1
            if not success:
                self.session.error_count += 1
            self.session.services_used.add(service)
            self.session.service_counts[service] += 1
            if operation:
                key = f"{service}:{operation}"
                if success:
                    self.session.operation_stats[key]["success"] += 1
                else:
                    self.session.operation_stats[key]["failure"] += 1

    def end_session(self) -> None:
        """Mark the session as ended."""
        self.session.end_time = time.time()

    def save_session(self, state_dir: Path) -> Path:
        """Persist session summary to a JSON file."""
        state_dir.mkdir(parents=True, exist_ok=True)
        filename = f"session-{self.session.session_id}.json"
        path = state_dir / filename
        path.write_text(json.dumps(self.session.to_dict(), indent=2))
        return path


# ---------------------------------------------------------------------------
# File-based session queries
# ---------------------------------------------------------------------------


def list_sessions(state_dir: Path) -> list[dict]:
    """List all session summaries from the state directory."""
    if not state_dir.exists():
        return []
    sessions = []
    for f in sorted(state_dir.glob("session-*.json")):
        try:
            data = json.loads(f.read_text())
            sessions.append(data)
        except (json.JSONDecodeError, OSError):
            continue
    return sessions


def get_session_detail(state_dir: Path, session_id: str) -> dict | None:
    """Get a specific session by ID."""
    path = state_dir / f"session-{session_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return None


def clear_sessions(state_dir: Path) -> int:
    """Delete all session files. Returns count deleted."""
    if not state_dir.exists():
        return 0
    count = 0
    for f in state_dir.glob("session-*.json"):
        f.unlink()
        count += 1
    return count


def compute_aggregate_summary(state_dir: Path) -> dict:
    """Compute aggregate analytics across all saved sessions."""
    sessions = list_sessions(state_dir)
    if not sessions:
        return {
            "total_sessions": 0,
            "avg_duration": 0,
            "most_used_services": [],
            "zero_error_session_rate": 0,
            "service_reliability": {},
            "most_failing_operations": [],
        }

    total_sessions = len(sessions)
    total_duration = sum(s.get("duration", 0) for s in sessions)
    avg_duration = total_duration / total_sessions

    # Service usage counts across all sessions
    service_totals: dict[str, int] = defaultdict(int)
    for s in sessions:
        for svc, count in s.get("service_counts", {}).items():
            service_totals[svc] += count

    most_used = sorted(service_totals.keys(), key=lambda k: service_totals[k], reverse=True)

    # Zero-error session rate
    zero_error = sum(1 for s in sessions if s.get("error_count", 0) == 0)
    zero_error_rate = zero_error / total_sessions

    # Service reliability: success rate per service
    service_success: dict[str, int] = defaultdict(int)
    service_total_ops: dict[str, int] = defaultdict(int)
    # Operation failure tracking
    op_failures: dict[str, int] = defaultdict(int)
    op_successes: dict[str, int] = defaultdict(int)

    for s in sessions:
        for op_key, stats in s.get("operation_stats", {}).items():
            svc = op_key.split(":")[0] if ":" in op_key else op_key
            succ = stats.get("success", 0)
            fail = stats.get("failure", 0)
            service_success[svc] += succ
            service_total_ops[svc] += succ + fail
            op_successes[op_key] += succ
            op_failures[op_key] += fail

    service_reliability = {}
    for svc in service_total_ops:
        total = service_total_ops[svc]
        if total > 0:
            service_reliability[svc] = service_success[svc] / total

    # Most failing operations (sorted by failure count desc)
    most_failing = sorted(
        [
            {"operation": op, "failures": count, "successes": op_successes.get(op, 0)}
            for op, count in op_failures.items()
            if count > 0
        ],
        key=lambda x: x["failures"],
        reverse=True,
    )

    return {
        "total_sessions": total_sessions,
        "avg_duration": round(avg_duration, 3),
        "most_used_services": most_used[:10],
        "zero_error_session_rate": zero_error_rate,
        "service_reliability": service_reliability,
        "most_failing_operations": most_failing[:20],
    }


# ---------------------------------------------------------------------------
# Singleton management
# ---------------------------------------------------------------------------

_UNCHECKED = object()  # sentinel: CI detection has not run yet
_analytics: CIAnalytics | None | object = _UNCHECKED


def get_ci_analytics(force_enable: bool = False) -> CIAnalytics | None:
    """Get or create the CI analytics singleton.

    Returns None if not in a CI environment and force_enable is False.
    """
    global _analytics

    if isinstance(_analytics, CIAnalytics):
        return _analytics

    if _analytics is None and not force_enable:
        # Already checked; not in a CI environment.
        return None

    # Session ID from explicit env var
    session_id = os.environ.get("ROBOTOCORE_CI_SESSION", "").strip()
    ci_provider, build_id = detect_ci_provider()

    if not session_id and not ci_provider and not force_enable:
        _analytics = None
        return None

    if not session_id:
        session_id = str(uuid.uuid4())

    _analytics = CIAnalytics(
        session=CISession(
            session_id=session_id,
            ci_provider=ci_provider,
            build_id=build_id,
        )
    )
    return _analytics


def reset_ci_analytics() -> None:
    """Reset the singleton (for testing)."""
    global _analytics
    _analytics = _UNCHECKED
