"""
Data models for the notification dispatch system.

Pure dataclasses — no AWS dependencies.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import StrEnum


class Channel(StrEnum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    WEBHOOK = "webhook"
    IN_APP = "in_app"


class Priority(StrEnum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"


class DeliveryStatus(StrEnum):
    PENDING = "PENDING"
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    BOUNCED = "BOUNCED"


@dataclass
class Template:
    template_id: str
    name: str
    channel: Channel
    subject: str
    body: str
    variables: list[str] = field(default_factory=list)

    def render(self, values: dict[str, str]) -> tuple[str, str]:
        """Render subject and body with variable substitution.

        Returns (rendered_subject, rendered_body).
        Raises ValueError if a required variable is missing.
        """
        missing = [v for v in self.variables if v not in values]
        if missing:
            raise ValueError(f"Missing template variables: {', '.join(missing)}")
        subject = self.subject
        body = self.body
        for var, val in values.items():
            subject = subject.replace(f"{{{{{var}}}}}", val)
            body = body.replace(f"{{{{{var}}}}}", val)
        return subject, body


@dataclass
class Notification:
    notification_id: str
    user_id: str
    channel: Channel
    template_id: str
    variables: dict[str, str]
    priority: Priority = Priority.NORMAL
    status: DeliveryStatus = DeliveryStatus.PENDING
    created_at: str = ""
    sent_at: str = ""

    def __post_init__(self):
        if not self.notification_id:
            self.notification_id = f"NOTIF-{uuid.uuid4().hex[:12]}"


@dataclass
class UserPreferences:
    user_id: str
    channels: dict[str, bool] = field(default_factory=dict)
    quiet_hours_start: str = ""  # HH:MM format
    quiet_hours_end: str = ""

    def is_channel_enabled(self, channel: Channel) -> bool:
        return self.channels.get(channel.value, True)

    def in_quiet_hours(self, current_hour: int, current_minute: int) -> bool:
        if not self.quiet_hours_start or not self.quiet_hours_end:
            return False
        start_h, start_m = map(int, self.quiet_hours_start.split(":"))
        end_h, end_m = map(int, self.quiet_hours_end.split(":"))
        start = start_h * 60 + start_m
        end = end_h * 60 + end_m
        now = current_hour * 60 + current_minute
        if start <= end:
            return start <= now < end
        # Wraps midnight
        return now >= start or now < end


@dataclass
class DeliveryRecord:
    notification_id: str
    user_id: str
    channel: str
    status: DeliveryStatus
    attempt: int = 1
    sent_at: str = ""
    error_message: str = ""


@dataclass
class NotificationStats:
    channel: str
    total_sent: int = 0
    delivered: int = 0
    failed: int = 0
    bounced: int = 0
    avg_delivery_time_ms: float = 0.0


@dataclass
class BulkSendResult:
    total: int = 0
    sent: int = 0
    failed: int = 0
    notification_ids: list[str] = field(default_factory=list)


@dataclass
class ScheduledNotification:
    schedule_id: str
    user_id: str
    template_id: str
    channel: Channel
    variables: dict[str, str]
    priority: Priority
    scheduled_for: str  # ISO 8601
    cancelled: bool = False

    def __post_init__(self):
        if not self.schedule_id:
            self.schedule_id = f"SCHED-{uuid.uuid4().hex[:12]}"
