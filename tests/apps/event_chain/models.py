"""Data models for the event chain application."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ChainStage:
    """A single stage in an event processing chain."""

    name: str
    trigger_type: str  # "s3", "dynamodb_stream", "sqs_esm", "eventbridge", "sns"
    resource_arn: str
    handler_arn: str | None = None
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChainResult:
    """Result of running an event chain end-to-end."""

    stages_completed: int
    total_stages: int
    final_output: dict[str, Any] | None = None
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.stages_completed == self.total_stages and self.error is None
