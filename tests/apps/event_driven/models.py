"""
Data models for the event-driven architecture application.

These dataclasses represent the core domain objects used by the EventRouter
to manage event buses, rules, targets, schemas, and statistics.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Event:
    """An event to be published to an EventBridge bus."""

    source: str
    detail_type: str
    detail: dict[str, Any]
    resources: list[str] = field(default_factory=list)
    time: float = field(default_factory=time.time)

    def to_entry(self, bus_name: str) -> dict[str, Any]:
        """Convert to a PutEvents entry dict."""
        import json

        entry: dict[str, Any] = {
            "Source": self.source,
            "DetailType": self.detail_type,
            "Detail": json.dumps(self.detail),
            "EventBusName": bus_name,
        }
        if self.resources:
            entry["Resources"] = self.resources
        return entry


@dataclass
class EventTarget:
    """A target for an EventBridge rule."""

    id: str
    arn: str
    input_transformer: dict[str, Any] | None = None
    dlq_arn: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a PutTargets target dict."""
        target: dict[str, Any] = {"Id": self.id, "Arn": self.arn}
        if self.input_transformer:
            target["InputTransformer"] = self.input_transformer
        if self.dlq_arn:
            target["DeadLetterConfig"] = {"Arn": self.dlq_arn}
        return target


@dataclass
class EventRule:
    """An EventBridge rule that matches events and routes them to targets."""

    name: str
    bus_name: str
    pattern: dict[str, Any]
    targets: list[EventTarget] = field(default_factory=list)
    state: str = "ENABLED"
    description: str = ""

    @property
    def pattern_json(self) -> str:
        import json

        return json.dumps(self.pattern)


@dataclass
class EventSchema:
    """A schema describing the structure of an event's detail payload."""

    name: str
    source: str
    detail_type: str
    json_schema: dict[str, Any]
    version: int = 1

    def to_dynamodb_item(self) -> dict[str, Any]:
        """Convert to a DynamoDB item dict."""
        import json

        return {
            "pk": {"S": f"SCHEMA#{self.source}#{self.name}"},
            "sk": {"S": f"v{self.version}"},
            "name": {"S": self.name},
            "source": {"S": self.source},
            "detail_type": {"S": self.detail_type},
            "json_schema": {"S": json.dumps(self.json_schema)},
            "version": {"N": str(self.version)},
        }

    @classmethod
    def from_dynamodb_item(cls, item: dict[str, Any]) -> EventSchema:
        import json

        return cls(
            name=item["name"]["S"],
            source=item["source"]["S"],
            detail_type=item["detail_type"]["S"],
            json_schema=json.loads(item["json_schema"]["S"]),
            version=int(item["version"]["N"]),
        )


@dataclass
class FanOutConfig:
    """Configuration for SNS topic fan-out to multiple SQS queues."""

    topic_arn: str
    subscriptions: list[str] = field(default_factory=list)  # queue ARNs


@dataclass
class EventStats:
    """Statistics for events by source and detail type."""

    source: str
    detail_type: str
    count: int = 0
    last_seen: float = 0.0

    def record(self) -> None:
        self.count += 1
        self.last_seen = time.time()
