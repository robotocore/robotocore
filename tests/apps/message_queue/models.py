"""
Data models for the message queue application.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class QueueConfig:
    """Configuration for creating a queue."""

    name: str
    fifo: bool = False
    visibility_timeout: int = 30
    delay_seconds: int = 0
    max_receive_count: int = 0
    dlq_arn: str | None = None

    @property
    def full_name(self) -> str:
        if self.fifo and not self.name.endswith(".fifo"):
            return f"{self.name}.fifo"
        return self.name


@dataclass
class Message:
    """A message to be sent to a queue."""

    body: str
    attributes: dict[str, dict[str, str]] = field(default_factory=dict)
    group_id: str | None = None
    dedup_id: str | None = None
    delay_seconds: int | None = None


@dataclass
class ReceivedMessage:
    """A message received from a queue."""

    message_id: str
    receipt_handle: str
    body: str
    attributes: dict[str, dict[str, str]]
    receive_count: int
    sent_timestamp: int

    @property
    def age_seconds(self) -> float:
        return time.time() - (self.sent_timestamp / 1000.0)


@dataclass
class QueueStats:
    """Approximate statistics for a queue."""

    approximate_messages: int
    in_flight: int
    delayed: int
    oldest_message_age: int | None = None


@dataclass
class DeliveryResult:
    """Result of a batch send operation."""

    successful: list[str] = field(default_factory=list)
    failed: list[dict] = field(default_factory=list)

    @property
    def message_ids(self) -> list[str]:
        return list(self.successful)

    @property
    def all_succeeded(self) -> bool:
        return len(self.failed) == 0
