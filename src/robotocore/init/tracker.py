"""Init script execution tracker.

Tracks the execution status of init scripts across lifecycle stages
(boot, start, ready, shutdown). Provides data for the /_robotocore/init endpoints.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ScriptStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ScriptRecord:
    filename: str
    stage: str
    status: ScriptStatus = ScriptStatus.PENDING
    duration: float | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        d: dict = {
            "filename": self.filename,
            "stage": self.stage,
            "status": self.status.value,
        }
        if self.duration is not None:
            d["duration"] = self.duration
        if self.error is not None:
            d["error"] = self.error
        return d


class InitTracker:
    """Tracks init script execution across lifecycle stages."""

    def __init__(self) -> None:
        # stage -> list of ScriptRecord
        self._scripts: dict[str, list[ScriptRecord]] = {}

    def _find_or_create(self, filename: str, stage: str) -> ScriptRecord:
        """Find existing record or create a new one."""
        scripts = self._scripts.setdefault(stage, [])
        for s in scripts:
            if s.filename == filename:
                return s
        record = ScriptRecord(filename=filename, stage=stage)
        scripts.append(record)
        return record

    def record_pending(self, filename: str, stage: str) -> None:
        """Record a script as pending execution."""
        record = self._find_or_create(filename, stage)
        record.status = ScriptStatus.PENDING

    def record_start(self, filename: str, stage: str) -> None:
        """Record a script starting execution."""
        record = self._find_or_create(filename, stage)
        record.status = ScriptStatus.RUNNING

    def record_complete(self, filename: str, stage: str, duration: float = 0.0) -> None:
        """Record a script completing successfully."""
        record = self._find_or_create(filename, stage)
        record.status = ScriptStatus.COMPLETED
        record.duration = duration

    def record_failure(
        self, filename: str, stage: str, error: str = "", duration: float = 0.0
    ) -> None:
        """Record a script failing."""
        record = self._find_or_create(filename, stage)
        record.status = ScriptStatus.FAILED
        record.duration = duration
        record.error = error

    def get_scripts(self, stage: str) -> list[dict]:
        """Get all scripts for a given stage."""
        scripts = self._scripts.get(stage, [])
        return [s.to_dict() for s in scripts]

    def get_summary(self) -> dict:
        """Get summary of all stages."""
        stages = {}
        for stage, scripts in self._scripts.items():
            counts = {
                "total": len(scripts),
                "pending": 0,
                "running": 0,
                "completed": 0,
                "failed": 0,
            }
            for s in scripts:
                counts[s.status.value] += 1
            stages[stage] = counts
        return {"stages": stages}


# Global singleton
_tracker: InitTracker | None = None


def get_init_tracker() -> InitTracker:
    """Get or create the global init tracker."""
    global _tracker
    if _tracker is None:
        _tracker = InitTracker()
    return _tracker
