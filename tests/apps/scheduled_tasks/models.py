"""
Data models for the scheduled task system.

Plain dataclasses -- no AWS or robotocore imports.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class ExecutionStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    SKIPPED = "SKIPPED"


class AlertType(StrEnum):
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"
    TIMEOUT = "TIMEOUT"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"


class DependencyCondition(StrEnum):
    SUCCESS = "SUCCESS"
    COMPLETED = "COMPLETED"  # any terminal state


@dataclass
class TaskDefinition:
    task_id: str = field(default_factory=lambda: f"task-{uuid.uuid4().hex[:8]}")
    name: str = ""
    group: str = "default"
    schedule_expression: str = "rate(1 hour)"
    target_arn: str = ""
    input_payload: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    max_retries: int = 3
    timeout_seconds: int = 300
    max_concurrent: int = 1
    description: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


@dataclass
class TaskExecution:
    execution_id: str = field(default_factory=lambda: f"exec-{uuid.uuid4().hex[:8]}")
    task_id: str = ""
    status: ExecutionStatus = ExecutionStatus.PENDING
    started_at: str = ""
    finished_at: str = ""
    attempt: int = 1
    output_key: str = ""
    error_message: str = ""
    duration_seconds: float = 0.0


@dataclass
class TaskGroup:
    group_name: str = ""
    tasks: list[str] = field(default_factory=list)
    description: str = ""


@dataclass
class TaskDependency:
    task_id: str = ""
    depends_on: str = ""
    condition: DependencyCondition = DependencyCondition.SUCCESS


@dataclass
class TaskMetrics:
    task_id: str = ""
    total_executions: int = 0
    successes: int = 0
    failures: int = 0
    timeouts: int = 0
    avg_duration_seconds: float = 0.0
    last_run: str = ""


@dataclass
class ExecutionAlert:
    execution_id: str = ""
    task_id: str = ""
    alert_type: AlertType = AlertType.FAILURE
    message: str = ""
    sent_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
