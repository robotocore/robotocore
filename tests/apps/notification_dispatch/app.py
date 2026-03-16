"""
NotificationService — multi-channel notification dispatch platform.

Orchestrates SNS topics (channels), SQS queues (consumers), S3 (templates),
DynamoDB (delivery tracking + user preferences), and CloudWatch (metrics + logs).

Only depends on boto3 — no robotocore/moto imports.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from .models import (
    BulkSendResult,
    Channel,
    DeliveryRecord,
    DeliveryStatus,
    Notification,
    NotificationStats,
    Priority,
    ScheduledNotification,
    Template,
    UserPreferences,
)


class NotificationService:
    """Multi-channel notification dispatch service.

    Manages channels (SNS topics), consumer queues (SQS), templates (S3),
    delivery tracking (DynamoDB), user preferences (DynamoDB), metrics
    (CloudWatch), and audit logs (CloudWatch Logs).
    """

    def __init__(
        self,
        *,
        sns,
        sqs,
        s3,
        dynamodb,
        cloudwatch,
        logs,
        template_bucket: str,
        delivery_table: str,
        preferences_table: str,
        schedule_table: str,
        metrics_namespace: str,
        log_group: str,
        log_stream: str,
        rate_limit_per_second: int = 100,
    ):
        self.sns = sns
        self.sqs = sqs
        self.s3 = s3
        self.dynamodb = dynamodb
        self.cloudwatch = cloudwatch
        self.logs = logs

        self.template_bucket = template_bucket
        self.delivery_table = delivery_table
        self.preferences_table = preferences_table
        self.schedule_table = schedule_table
        self.metrics_namespace = metrics_namespace
        self.log_group = log_group
        self.log_stream = log_stream
        self.rate_limit_per_second = rate_limit_per_second

        # In-memory state for channels and rate tracking
        self._channels: dict[str, dict[str, str]] = {}  # channel_name -> {topic_arn, ...}
        self._send_timestamps: list[float] = []

    # -----------------------------------------------------------------------
    # Channel management
    # -----------------------------------------------------------------------

    def create_channel(self, channel: Channel, unique_suffix: str) -> dict[str, str]:
        """Create an SNS topic and SQS subscriber queue for a channel.

        Returns dict with topic_arn, queue_url, queue_arn, subscription_arn.
        """
        topic_name = f"notif-{channel.value}-{unique_suffix}"
        topic_resp = self.sns.create_topic(Name=topic_name)
        topic_arn = topic_resp["TopicArn"]

        queue_name = f"notif-consumer-{channel.value}-{unique_suffix}"
        queue_resp = self.sqs.create_queue(QueueName=queue_name)
        queue_url = queue_resp["QueueUrl"]
        queue_attrs = self.sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = queue_attrs["Attributes"]["QueueArn"]

        sub_resp = self.sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
        subscription_arn = sub_resp["SubscriptionArn"]

        info = {
            "channel": channel.value,
            "topic_arn": topic_arn,
            "queue_url": queue_url,
            "queue_arn": queue_arn,
            "subscription_arn": subscription_arn,
        }
        self._channels[channel.value] = info
        return info

    def get_channel(self, channel: Channel) -> dict[str, str] | None:
        """Return channel info or None if not created."""
        return self._channels.get(channel.value)

    def list_channels(self) -> list[dict[str, str]]:
        """Return all registered channels."""
        return list(self._channels.values())

    def delete_channel(self, channel: Channel) -> None:
        """Delete SNS topic and SQS queue for a channel."""
        info = self._channels.pop(channel.value, None)
        if not info:
            return
        self.sns.unsubscribe(SubscriptionArn=info["subscription_arn"])
        self.sqs.delete_queue(QueueUrl=info["queue_url"])
        self.sns.delete_topic(TopicArn=info["topic_arn"])

    def receive_from_channel(
        self, channel: Channel, max_messages: int = 10, wait_seconds: int = 1
    ) -> list[dict[str, Any]]:
        """Receive messages from a channel's SQS queue.

        Returns parsed notification payloads (unwrapping the SNS envelope).
        """
        info = self._channels.get(channel.value)
        if not info:
            return []
        resp = self.sqs.receive_message(
            QueueUrl=info["queue_url"],
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,
        )
        results = []
        for msg in resp.get("Messages", []):
            body = json.loads(msg["Body"])
            # SNS wraps in an envelope with a "Message" key
            inner = json.loads(body["Message"]) if "Message" in body else body
            results.append(inner)
            # Delete after processing
            self.sqs.delete_message(
                QueueUrl=info["queue_url"],
                ReceiptHandle=msg["ReceiptHandle"],
            )
        return results

    # -----------------------------------------------------------------------
    # Template management (S3)
    # -----------------------------------------------------------------------

    def upload_template(self, template: Template) -> str:
        """Store a notification template in S3.

        Key: templates/{channel}/{template_id}.json
        Returns the S3 key.
        """
        key = f"templates/{template.channel.value}/{template.template_id}.json"
        payload = json.dumps(
            {
                "template_id": template.template_id,
                "name": template.name,
                "channel": template.channel.value,
                "subject": template.subject,
                "body": template.body,
                "variables": template.variables,
            }
        )
        self.s3.put_object(
            Bucket=self.template_bucket,
            Key=key,
            Body=payload.encode(),
            ContentType="application/json",
            Metadata={"template-name": template.name, "channel": template.channel.value},
        )
        return key

    def get_template(self, channel: Channel, template_id: str) -> Template:
        """Load a template from S3."""
        key = f"templates/{channel.value}/{template_id}.json"
        resp = self.s3.get_object(Bucket=self.template_bucket, Key=key)
        data = json.loads(resp["Body"].read().decode())
        return Template(
            template_id=data["template_id"],
            name=data["name"],
            channel=Channel(data["channel"]),
            subject=data["subject"],
            body=data["body"],
            variables=data.get("variables", []),
        )

    def update_template(self, template: Template) -> str:
        """Update an existing template (overwrites in S3)."""
        return self.upload_template(template)

    def list_templates(self, channel: Channel | None = None) -> list[str]:
        """List template IDs. Optionally filter by channel."""
        prefix = "templates/"
        if channel:
            prefix = f"templates/{channel.value}/"
        resp = self.s3.list_objects_v2(Bucket=self.template_bucket, Prefix=prefix)
        keys = []
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".json"):
                keys.append(k)
        return keys

    def render_template(
        self, channel: Channel, template_id: str, variables: dict[str, str]
    ) -> tuple[str, str]:
        """Load a template and render with variables.

        Returns (rendered_subject, rendered_body).
        Raises ValueError on missing variables.
        """
        template = self.get_template(channel, template_id)
        return template.render(variables)

    # -----------------------------------------------------------------------
    # User preferences (DynamoDB)
    # -----------------------------------------------------------------------

    def set_user_preferences(self, prefs: UserPreferences) -> None:
        """Store or update user channel preferences."""
        item: dict[str, Any] = {
            "user_id": {"S": prefs.user_id},
            "channels": {"S": json.dumps(prefs.channels)},
        }
        if prefs.quiet_hours_start:
            item["quiet_hours_start"] = {"S": prefs.quiet_hours_start}
        if prefs.quiet_hours_end:
            item["quiet_hours_end"] = {"S": prefs.quiet_hours_end}
        self.dynamodb.put_item(TableName=self.preferences_table, Item=item)

    def get_user_preferences(self, user_id: str) -> UserPreferences:
        """Load user preferences. Returns defaults if not found."""
        resp = self.dynamodb.get_item(
            TableName=self.preferences_table,
            Key={"user_id": {"S": user_id}},
        )
        item = resp.get("Item")
        if not item:
            return UserPreferences(user_id=user_id)
        channels = json.loads(item["channels"]["S"]) if "channels" in item else {}
        return UserPreferences(
            user_id=user_id,
            channels=channels,
            quiet_hours_start=item.get("quiet_hours_start", {}).get("S", ""),
            quiet_hours_end=item.get("quiet_hours_end", {}).get("S", ""),
        )

    def unsubscribe_channel(self, user_id: str, channel: Channel) -> None:
        """Opt a user out of a specific channel."""
        prefs = self.get_user_preferences(user_id)
        prefs.channels[channel.value] = False
        self.set_user_preferences(prefs)

    def subscribe_channel(self, user_id: str, channel: Channel) -> None:
        """Opt a user into a specific channel."""
        prefs = self.get_user_preferences(user_id)
        prefs.channels[channel.value] = True
        self.set_user_preferences(prefs)

    # -----------------------------------------------------------------------
    # Rate limiting
    # -----------------------------------------------------------------------

    def _check_rate_limit(self) -> bool:
        """Check if we are within rate limits. Returns True if OK to send."""
        now = time.time()
        cutoff = now - 1.0
        self._send_timestamps = [t for t in self._send_timestamps if t > cutoff]
        return len(self._send_timestamps) < self.rate_limit_per_second

    def _record_send(self) -> None:
        """Record a send timestamp for rate limiting."""
        self._send_timestamps.append(time.time())

    # -----------------------------------------------------------------------
    # Delivery tracking (DynamoDB)
    # -----------------------------------------------------------------------

    def _record_delivery(self, record: DeliveryRecord) -> None:
        """Write a delivery record to DynamoDB."""
        item: dict[str, Any] = {
            "notification_id": {"S": record.notification_id},
            "channel": {"S": record.channel},
            "user_id": {"S": record.user_id},
            "delivery_status": {"S": record.status.value},
            "attempt": {"N": str(record.attempt)},
            "sent_at": {"S": record.sent_at or datetime.now(UTC).isoformat()},
        }
        if record.error_message:
            item["error_message"] = {"S": record.error_message}
        self.dynamodb.put_item(TableName=self.delivery_table, Item=item)

    def get_delivery_record(self, notification_id: str, channel: str) -> DeliveryRecord | None:
        """Fetch a single delivery record."""
        resp = self.dynamodb.get_item(
            TableName=self.delivery_table,
            Key={
                "notification_id": {"S": notification_id},
                "channel": {"S": channel},
            },
        )
        item = resp.get("Item")
        if not item:
            return None
        return DeliveryRecord(
            notification_id=item["notification_id"]["S"],
            user_id=item["user_id"]["S"],
            channel=item["channel"]["S"],
            status=DeliveryStatus(item["delivery_status"]["S"]),
            attempt=int(item.get("attempt", {}).get("N", "1")),
            sent_at=item.get("sent_at", {}).get("S", ""),
            error_message=item.get("error_message", {}).get("S", ""),
        )

    def update_delivery_status(
        self,
        notification_id: str,
        channel: str,
        status: DeliveryStatus,
        error_message: str = "",
    ) -> None:
        """Update the status of an existing delivery record."""
        update_expr = "SET delivery_status = :s"
        expr_vals: dict[str, Any] = {":s": {"S": status.value}}
        if status == DeliveryStatus.DELIVERED:
            update_expr += ", delivered_at = :d"
            expr_vals[":d"] = {"S": datetime.now(UTC).isoformat()}
        if error_message:
            update_expr += ", error_message = :e"
            expr_vals[":e"] = {"S": error_message}
        self.dynamodb.update_item(
            TableName=self.delivery_table,
            Key={
                "notification_id": {"S": notification_id},
                "channel": {"S": channel},
            },
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_vals,
        )

    def query_deliveries_by_user(self, user_id: str, limit: int = 50) -> list[DeliveryRecord]:
        """Query delivery history by user_id using GSI."""
        resp = self.dynamodb.query(
            TableName=self.delivery_table,
            IndexName="by-user",
            KeyConditionExpression="user_id = :u",
            ExpressionAttributeValues={":u": {"S": user_id}},
            Limit=limit,
        )
        return [
            DeliveryRecord(
                notification_id=item["notification_id"]["S"],
                user_id=item["user_id"]["S"],
                channel=item["channel"]["S"],
                status=DeliveryStatus(item["delivery_status"]["S"]),
                attempt=int(item.get("attempt", {}).get("N", "1")),
                sent_at=item.get("sent_at", {}).get("S", ""),
                error_message=item.get("error_message", {}).get("S", ""),
            )
            for item in resp.get("Items", [])
        ]

    def query_deliveries_by_channel(self, channel: str, limit: int = 50) -> list[DeliveryRecord]:
        """Query delivery history by channel using GSI."""
        resp = self.dynamodb.query(
            TableName=self.delivery_table,
            IndexName="by-channel",
            KeyConditionExpression="channel = :c",
            ExpressionAttributeValues={":c": {"S": channel}},
            Limit=limit,
        )
        return [
            DeliveryRecord(
                notification_id=item["notification_id"]["S"],
                user_id=item["user_id"]["S"],
                channel=item["channel"]["S"],
                status=DeliveryStatus(item["delivery_status"]["S"]),
                attempt=int(item.get("attempt", {}).get("N", "1")),
                sent_at=item.get("sent_at", {}).get("S", ""),
                error_message=item.get("error_message", {}).get("S", ""),
            )
            for item in resp.get("Items", [])
        ]

    def query_deliveries_by_date_range(
        self, user_id: str, start_time: str, end_time: str
    ) -> list[DeliveryRecord]:
        """Query delivery history for a user within a date range."""
        resp = self.dynamodb.query(
            TableName=self.delivery_table,
            IndexName="by-user",
            KeyConditionExpression="user_id = :u AND sent_at BETWEEN :s AND :e",
            ExpressionAttributeValues={
                ":u": {"S": user_id},
                ":s": {"S": start_time},
                ":e": {"S": end_time},
            },
        )
        return [
            DeliveryRecord(
                notification_id=item["notification_id"]["S"],
                user_id=item["user_id"]["S"],
                channel=item["channel"]["S"],
                status=DeliveryStatus(item["delivery_status"]["S"]),
                attempt=int(item.get("attempt", {}).get("N", "1")),
                sent_at=item.get("sent_at", {}).get("S", ""),
                error_message=item.get("error_message", {}).get("S", ""),
            )
            for item in resp.get("Items", [])
        ]

    def get_delivery_stats(self, channel: str) -> NotificationStats:
        """Calculate delivery stats for a channel by scanning delivery records."""
        resp = self.dynamodb.query(
            TableName=self.delivery_table,
            IndexName="by-channel",
            KeyConditionExpression="channel = :c",
            ExpressionAttributeValues={":c": {"S": channel}},
        )
        stats = NotificationStats(channel=channel)
        for item in resp.get("Items", []):
            status = item["delivery_status"]["S"]
            stats.total_sent += 1
            if status == DeliveryStatus.DELIVERED.value:
                stats.delivered += 1
            elif status == DeliveryStatus.FAILED.value:
                stats.failed += 1
            elif status == DeliveryStatus.BOUNCED.value:
                stats.bounced += 1
        return stats

    # -----------------------------------------------------------------------
    # Send notification
    # -----------------------------------------------------------------------

    def send_notification(
        self,
        user_id: str,
        channel: Channel,
        template_id: str,
        variables: dict[str, str],
        priority: Priority = Priority.NORMAL,
        force: bool = False,
    ) -> Notification | None:
        """Send a notification to a user on a specific channel.

        Checks user preferences (unless priority is CRITICAL or force=True).
        Records delivery in DynamoDB, publishes to SNS, logs audit event.

        Returns the Notification object, or None if suppressed by preferences/rate limit.
        """
        # Check rate limit
        if not self._check_rate_limit():
            return None

        # Check user preferences (CRITICAL bypasses opt-out)
        if priority != Priority.CRITICAL and not force:
            prefs = self.get_user_preferences(user_id)
            if not prefs.is_channel_enabled(channel):
                return None

        # Check channel exists
        channel_info = self._channels.get(channel.value)
        if not channel_info:
            return None

        # Render template
        rendered_subject, rendered_body = self.render_template(channel, template_id, variables)

        # Create notification
        notification = Notification(
            notification_id=f"NOTIF-{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            channel=channel,
            template_id=template_id,
            variables=variables,
            priority=priority,
            status=DeliveryStatus.PENDING,
            created_at=datetime.now(UTC).isoformat(),
        )

        # Record pending delivery
        self._record_delivery(
            DeliveryRecord(
                notification_id=notification.notification_id,
                user_id=user_id,
                channel=channel.value,
                status=DeliveryStatus.PENDING,
                attempt=1,
                sent_at=datetime.now(UTC).isoformat(),
            )
        )

        # Publish to SNS
        message_payload = {
            "notification_id": notification.notification_id,
            "user_id": user_id,
            "channel": channel.value,
            "subject": rendered_subject,
            "body": rendered_body,
            "priority": priority.value,
            "template_id": template_id,
        }
        self.sns.publish(
            TopicArn=channel_info["topic_arn"],
            Message=json.dumps(message_payload),
            MessageAttributes={
                "priority": {"DataType": "String", "StringValue": priority.value},
                "channel": {"DataType": "String", "StringValue": channel.value},
            },
        )

        # Update status to SENT
        self.update_delivery_status(
            notification.notification_id, channel.value, DeliveryStatus.SENT
        )
        notification.status = DeliveryStatus.SENT
        notification.sent_at = datetime.now(UTC).isoformat()

        # Record send for rate limiting
        self._record_send()

        # Publish CloudWatch metric
        self._publish_metric("NotificationSent", channel.value, 1)

        # Log audit event
        self._log_event(
            {
                "event": "notification_sent",
                "notification_id": notification.notification_id,
                "user_id": user_id,
                "channel": channel.value,
                "priority": priority.value,
                "template_id": template_id,
            }
        )

        return notification

    def send_notification_quiet_hours_aware(
        self,
        user_id: str,
        channel: Channel,
        template_id: str,
        variables: dict[str, str],
        priority: Priority = Priority.NORMAL,
        current_hour: int = 0,
        current_minute: int = 0,
    ) -> Notification | None:
        """Send with quiet hours check. Returns None if in quiet hours (unless CRITICAL)."""
        if priority != Priority.CRITICAL:
            prefs = self.get_user_preferences(user_id)
            if prefs.in_quiet_hours(current_hour, current_minute):
                return None
        return self.send_notification(user_id, channel, template_id, variables, priority)

    # -----------------------------------------------------------------------
    # Bulk send
    # -----------------------------------------------------------------------

    def bulk_send(
        self,
        user_ids: list[str],
        channel: Channel,
        template_id: str,
        variables: dict[str, str],
        priority: Priority = Priority.NORMAL,
    ) -> BulkSendResult:
        """Send the same notification to multiple users.

        Returns a summary of results.
        """
        result = BulkSendResult(total=len(user_ids))
        for user_id in user_ids:
            notif = self.send_notification(user_id, channel, template_id, variables, priority)
            if notif:
                result.sent += 1
                result.notification_ids.append(notif.notification_id)
            else:
                result.failed += 1
        return result

    # -----------------------------------------------------------------------
    # Retry
    # -----------------------------------------------------------------------

    def retry_failed_delivery(self, notification_id: str, channel: str) -> DeliveryRecord | None:
        """Retry a failed delivery.

        Increments attempt count, re-publishes to SNS, updates status.
        Returns updated record or None if original not found.
        """
        record = self.get_delivery_record(notification_id, channel)
        if not record:
            return None

        new_attempt = record.attempt + 1
        channel_info = self._channels.get(channel)
        if not channel_info:
            return None

        # Re-publish to SNS with retry metadata
        retry_payload = {
            "notification_id": notification_id,
            "user_id": record.user_id,
            "channel": channel,
            "retry_attempt": new_attempt,
            "original_sent_at": record.sent_at,
        }
        self.sns.publish(
            TopicArn=channel_info["topic_arn"],
            Message=json.dumps(retry_payload),
            MessageAttributes={
                "retry": {"DataType": "String", "StringValue": str(new_attempt)},
            },
        )

        # Update record
        self.dynamodb.update_item(
            TableName=self.delivery_table,
            Key={
                "notification_id": {"S": notification_id},
                "channel": {"S": channel},
            },
            UpdateExpression="SET delivery_status = :s, attempt = :a",
            ExpressionAttributeValues={
                ":s": {"S": DeliveryStatus.SENT.value},
                ":a": {"N": str(new_attempt)},
            },
        )

        self._publish_metric("NotificationRetried", channel, 1)
        self._log_event(
            {
                "event": "notification_retried",
                "notification_id": notification_id,
                "channel": channel,
                "attempt": new_attempt,
            }
        )

        return DeliveryRecord(
            notification_id=notification_id,
            user_id=record.user_id,
            channel=channel,
            status=DeliveryStatus.SENT,
            attempt=new_attempt,
            sent_at=datetime.now(UTC).isoformat(),
        )

    # -----------------------------------------------------------------------
    # Scheduled notifications
    # -----------------------------------------------------------------------

    def schedule_notification(
        self,
        user_id: str,
        channel: Channel,
        template_id: str,
        variables: dict[str, str],
        scheduled_for: str,
        priority: Priority = Priority.NORMAL,
    ) -> ScheduledNotification:
        """Schedule a notification for future delivery."""
        sched = ScheduledNotification(
            schedule_id=f"SCHED-{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            template_id=template_id,
            channel=channel,
            variables=variables,
            priority=priority,
            scheduled_for=scheduled_for,
        )
        self.dynamodb.put_item(
            TableName=self.schedule_table,
            Item={
                "schedule_id": {"S": sched.schedule_id},
                "user_id": {"S": sched.user_id},
                "template_id": {"S": sched.template_id},
                "channel": {"S": sched.channel.value},
                "variables": {"S": json.dumps(sched.variables)},
                "priority": {"S": sched.priority.value},
                "scheduled_for": {"S": sched.scheduled_for},
                "cancelled": {"BOOL": False},
            },
        )
        self._log_event(
            {
                "event": "notification_scheduled",
                "schedule_id": sched.schedule_id,
                "user_id": user_id,
                "channel": channel.value,
                "scheduled_for": scheduled_for,
            }
        )
        return sched

    def list_scheduled_notifications(self, user_id: str) -> list[ScheduledNotification]:
        """List scheduled notifications for a user."""
        resp = self.dynamodb.query(
            TableName=self.schedule_table,
            IndexName="by-user",
            KeyConditionExpression="user_id = :u",
            ExpressionAttributeValues={":u": {"S": user_id}},
        )
        results = []
        for item in resp.get("Items", []):
            if item.get("cancelled", {}).get("BOOL", False):
                continue
            results.append(
                ScheduledNotification(
                    schedule_id=item["schedule_id"]["S"],
                    user_id=item["user_id"]["S"],
                    template_id=item["template_id"]["S"],
                    channel=Channel(item["channel"]["S"]),
                    variables=json.loads(item["variables"]["S"]),
                    priority=Priority(item["priority"]["S"]),
                    scheduled_for=item["scheduled_for"]["S"],
                    cancelled=item.get("cancelled", {}).get("BOOL", False),
                )
            )
        return results

    def cancel_scheduled_notification(self, schedule_id: str) -> bool:
        """Cancel a scheduled notification. Returns True if found."""
        # Scan for the schedule_id (it's the partition key)
        resp = self.dynamodb.get_item(
            TableName=self.schedule_table,
            Key={"schedule_id": {"S": schedule_id}},
        )
        if not resp.get("Item"):
            return False
        self.dynamodb.update_item(
            TableName=self.schedule_table,
            Key={"schedule_id": {"S": schedule_id}},
            UpdateExpression="SET cancelled = :c",
            ExpressionAttributeValues={":c": {"BOOL": True}},
        )
        self._log_event(
            {
                "event": "notification_cancelled",
                "schedule_id": schedule_id,
            }
        )
        return True

    def bulk_schedule(
        self,
        user_ids: list[str],
        channel: Channel,
        template_id: str,
        variables: dict[str, str],
        scheduled_for: str,
        priority: Priority = Priority.NORMAL,
    ) -> list[ScheduledNotification]:
        """Schedule the same notification for multiple users."""
        results = []
        for uid in user_ids:
            sched = self.schedule_notification(
                uid, channel, template_id, variables, scheduled_for, priority
            )
            results.append(sched)
        return results

    # -----------------------------------------------------------------------
    # CloudWatch metrics
    # -----------------------------------------------------------------------

    def _publish_metric(self, metric_name: str, channel: str, value: float) -> None:
        """Publish a metric data point to CloudWatch."""
        self.cloudwatch.put_metric_data(
            Namespace=self.metrics_namespace,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Channel", "Value": channel}],
                }
            ],
        )

    def publish_delivery_metrics(self, channel: str) -> NotificationStats:
        """Calculate and publish delivery metrics for a channel."""
        stats = self.get_delivery_stats(channel)
        metrics = [
            ("TotalSent", stats.total_sent),
            ("Delivered", stats.delivered),
            ("Failed", stats.failed),
            ("Bounced", stats.bounced),
        ]
        for name, value in metrics:
            if value > 0:
                self._publish_metric(name, channel, value)
        return stats

    def get_cloudwatch_metric(
        self, metric_name: str, channel: str, hours_back: int = 1
    ) -> list[dict]:
        """Retrieve metric statistics from CloudWatch."""
        now = datetime.now(UTC)
        resp = self.cloudwatch.get_metric_statistics(
            Namespace=self.metrics_namespace,
            MetricName=metric_name,
            StartTime=(now - timedelta(hours=hours_back)).isoformat(),
            EndTime=(now + timedelta(hours=1)).isoformat(),
            Period=3600,
            Statistics=["Sum"],
            Dimensions=[{"Name": "Channel", "Value": channel}],
        )
        return resp.get("Datapoints", [])

    # -----------------------------------------------------------------------
    # CloudWatch Logs audit
    # -----------------------------------------------------------------------

    def _log_event(self, event_data: dict) -> None:
        """Write an audit log event to CloudWatch Logs."""
        event_data["timestamp_iso"] = datetime.now(UTC).isoformat()
        try:
            self.logs.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                logEvents=[
                    {
                        "timestamp": int(time.time() * 1000),
                        "message": json.dumps(event_data),
                    }
                ],
            )
        except Exception:
            pass  # Don't fail the operation if logging fails

    def get_audit_logs(self, start_from_head: bool = True) -> list[dict]:
        """Retrieve audit log events."""
        resp = self.logs.get_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            startFromHead=start_from_head,
        )
        results = []
        for evt in resp.get("events", []):
            try:
                results.append(json.loads(evt["message"]))
            except (json.JSONDecodeError, KeyError):
                pass  # intentionally ignored
        return results

    def filter_audit_logs(self, filter_pattern: str) -> list[dict]:
        """Filter audit logs with a CloudWatch Logs filter pattern."""
        resp = self.logs.filter_log_events(
            logGroupName=self.log_group,
            filterPattern=filter_pattern,
        )
        results = []
        for evt in resp.get("events", []):
            try:
                results.append(json.loads(evt["message"]))
            except (json.JSONDecodeError, KeyError):
                pass  # intentionally ignored
        return results

    # -----------------------------------------------------------------------
    # Cleanup
    # -----------------------------------------------------------------------

    def cleanup(self) -> None:
        """Delete all AWS resources created by this service."""
        for channel_name in list(self._channels.keys()):
            self.delete_channel(Channel(channel_name))

        # Clean up S3 bucket contents
        try:
            resp = self.s3.list_objects_v2(Bucket=self.template_bucket)
            for obj in resp.get("Contents", []):
                self.s3.delete_object(Bucket=self.template_bucket, Key=obj["Key"])
            self.s3.delete_bucket(Bucket=self.template_bucket)
        except Exception:
            pass  # best-effort cleanup

        # Clean up DynamoDB tables
        for table in [self.delivery_table, self.preferences_table, self.schedule_table]:
            try:
                self.dynamodb.delete_table(TableName=table)
            except Exception:
                pass  # best-effort cleanup

        # Clean up CloudWatch Logs
        try:
            self.logs.delete_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)
            self.logs.delete_log_group(logGroupName=self.log_group)
        except Exception:
            pass  # best-effort cleanup
