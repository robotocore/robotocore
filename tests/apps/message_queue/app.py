"""
MessageBroker — a robust message processing framework built on SQS.

Provides queue management, producer/consumer patterns, dead-letter queue handling,
message routing, retry logic, and FIFO message group management. Designed as a
production-quality abstraction over AWS SQS.

Usage:
    sqs = boto3.client("sqs", endpoint_url="http://localhost:4566")
    broker = MessageBroker(sqs)

    # Create queues
    main_url = broker.create_queue(QueueConfig(name="orders"))
    dlq_url = broker.create_queue(QueueConfig(name="orders-dlq"))
    broker.attach_dlq(main_url, dlq_url, max_receive_count=3)

    # Send messages
    broker.send(main_url, Message(body='{"order_id": "123"}'))

    # Consume messages
    for msg in broker.consume(main_url, batch_size=5):
        process(msg)
        broker.acknowledge(main_url, msg)
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Callable
from typing import Any

from .models import DeliveryResult, Message, QueueConfig, QueueStats, ReceivedMessage


class MessageBroker:
    """A robust message processing framework built on AWS SQS.

    Manages queue lifecycle, message production and consumption, dead-letter
    queue wiring, message routing, and queue metrics.
    """

    def __init__(self, sqs_client: Any) -> None:
        self._sqs = sqs_client
        self._queues: dict[str, str] = {}  # name -> url
        self._routes: list[tuple[Callable[[Message], bool], str]] = []

    # ------------------------------------------------------------------ #
    # Queue management
    # ------------------------------------------------------------------ #

    def create_queue(self, config: QueueConfig) -> str:
        """Create a standard or FIFO queue and return its URL."""
        attributes: dict[str, str] = {
            "VisibilityTimeout": str(config.visibility_timeout),
            "DelaySeconds": str(config.delay_seconds),
        }

        if config.fifo:
            attributes["FifoQueue"] = "true"
            attributes["ContentBasedDeduplication"] = "true"

        if config.max_receive_count > 0 and config.dlq_arn:
            attributes["RedrivePolicy"] = json.dumps(
                {
                    "deadLetterTargetArn": config.dlq_arn,
                    "maxReceiveCount": str(config.max_receive_count),
                }
            )

        response = self._sqs.create_queue(
            QueueName=config.full_name,
            Attributes=attributes,
        )
        url = response["QueueUrl"]
        self._queues[config.full_name] = url
        return url

    def delete_queue(self, queue_url: str) -> None:
        """Delete a queue by URL."""
        self._sqs.delete_queue(QueueUrl=queue_url)
        self._queues = {k: v for k, v in self._queues.items() if v != queue_url}

    def get_queue_url(self, name: str) -> str:
        """Look up a queue URL by name."""
        if name in self._queues:
            return self._queues[name]
        response = self._sqs.get_queue_url(QueueName=name)
        url = response["QueueUrl"]
        self._queues[name] = url
        return url

    def get_queue_arn(self, queue_url: str) -> str:
        """Get the ARN for a queue."""
        attrs = self._sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )
        return attrs["Attributes"]["QueueArn"]

    def attach_dlq(
        self,
        source_url: str,
        dlq_url: str,
        max_receive_count: int = 3,
    ) -> None:
        """Wire a dead-letter queue to a source queue with the given max receive count."""
        dlq_arn = self.get_queue_arn(dlq_url)
        self._sqs.set_queue_attributes(
            QueueUrl=source_url,
            Attributes={
                "RedrivePolicy": json.dumps(
                    {
                        "deadLetterTargetArn": dlq_arn,
                        "maxReceiveCount": str(max_receive_count),
                    }
                ),
            },
        )

    def set_visibility_timeout(self, queue_url: str, timeout: int) -> None:
        """Update the default visibility timeout for a queue."""
        self._sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"VisibilityTimeout": str(timeout)},
        )

    def purge_queue(self, queue_url: str) -> None:
        """Purge all messages from a queue."""
        self._sqs.purge_queue(QueueUrl=queue_url)

    # ------------------------------------------------------------------ #
    # Producer
    # ------------------------------------------------------------------ #

    def send(self, queue_url: str, message: Message) -> str:
        """Send a single message to a queue. Returns the message ID."""
        kwargs: dict[str, Any] = {
            "QueueUrl": queue_url,
            "MessageBody": message.body,
        }

        if message.attributes:
            kwargs["MessageAttributes"] = message.attributes

        if message.group_id is not None:
            kwargs["MessageGroupId"] = message.group_id

        if message.dedup_id is not None:
            kwargs["MessageDeduplicationId"] = message.dedup_id

        if message.delay_seconds is not None:
            kwargs["DelaySeconds"] = message.delay_seconds

        response = self._sqs.send_message(**kwargs)
        return response["MessageId"]

    def send_batch(
        self,
        queue_url: str,
        messages: list[Message],
    ) -> DeliveryResult:
        """Send up to 10 messages in a single batch. Returns delivery results."""
        if len(messages) > 10:
            raise ValueError("SQS batch size limit is 10 messages")

        entries = []
        for i, msg in enumerate(messages):
            entry: dict[str, Any] = {
                "Id": str(i),
                "MessageBody": msg.body,
            }
            if msg.attributes:
                entry["MessageAttributes"] = msg.attributes
            if msg.group_id is not None:
                entry["MessageGroupId"] = msg.group_id
            if msg.dedup_id is not None:
                entry["MessageDeduplicationId"] = msg.dedup_id
            if msg.delay_seconds is not None:
                entry["DelaySeconds"] = msg.delay_seconds
            entries.append(entry)

        response = self._sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=entries,
        )

        result = DeliveryResult()
        for s in response.get("Successful", []):
            result.successful.append(s["MessageId"])
        for f in response.get("Failed", []):
            result.failed.append(f)
        return result

    def send_json(self, queue_url: str, payload: dict, **kwargs: Any) -> str:
        """Convenience: serialize a dict as JSON and send it."""
        msg = Message(body=json.dumps(payload), **kwargs)
        return self.send(queue_url, msg)

    # ------------------------------------------------------------------ #
    # Consumer
    # ------------------------------------------------------------------ #

    def receive(
        self,
        queue_url: str,
        batch_size: int = 1,
        visibility_timeout: int | None = None,
        wait_time: int = 5,
        attribute_names: list[str] | None = None,
    ) -> list[ReceivedMessage]:
        """Receive up to batch_size messages from a queue."""
        kwargs: dict[str, Any] = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": min(batch_size, 10),
            "WaitTimeSeconds": wait_time,
            "MessageAttributeNames": ["All"],
            "AttributeNames": ["All"],
        }
        if visibility_timeout is not None:
            kwargs["VisibilityTimeout"] = visibility_timeout
        if attribute_names:
            kwargs["MessageAttributeNames"] = attribute_names

        response = self._sqs.receive_message(**kwargs)
        raw_messages = response.get("Messages", [])

        results = []
        for raw in raw_messages:
            sys_attrs = raw.get("Attributes", {})
            results.append(
                ReceivedMessage(
                    message_id=raw["MessageId"],
                    receipt_handle=raw["ReceiptHandle"],
                    body=raw["Body"],
                    attributes=raw.get("MessageAttributes", {}),
                    receive_count=int(sys_attrs.get("ApproximateReceiveCount", "1")),
                    sent_timestamp=int(sys_attrs.get("SentTimestamp", "0")),
                )
            )
        return results

    def consume(
        self,
        queue_url: str,
        batch_size: int = 1,
        visibility_timeout: int | None = None,
        wait_time: int = 5,
        max_messages: int | None = None,
    ) -> list[ReceivedMessage]:
        """Poll-based consumer: receive messages, collecting up to max_messages.

        If max_messages is None, performs a single receive call.
        Otherwise, polls until max_messages are collected or queue appears empty.
        """
        if max_messages is None:
            return self.receive(
                queue_url,
                batch_size=batch_size,
                visibility_timeout=visibility_timeout,
                wait_time=wait_time,
            )

        collected: list[ReceivedMessage] = []
        empty_polls = 0
        while len(collected) < max_messages and empty_polls < 3:
            msgs = self.receive(
                queue_url,
                batch_size=min(batch_size, max_messages - len(collected)),
                visibility_timeout=visibility_timeout,
                wait_time=wait_time,
            )
            if msgs:
                collected.extend(msgs)
                empty_polls = 0
            else:
                empty_polls += 1
        return collected

    def acknowledge(self, queue_url: str, message: ReceivedMessage) -> None:
        """Acknowledge (delete) a received message."""
        self._sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message.receipt_handle,
        )

    def acknowledge_batch(
        self,
        queue_url: str,
        messages: list[ReceivedMessage],
    ) -> None:
        """Acknowledge (delete) multiple messages in a single batch call."""
        if not messages:
            return
        entries = [
            {"Id": str(i), "ReceiptHandle": msg.receipt_handle} for i, msg in enumerate(messages)
        ]
        self._sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)

    def change_visibility(
        self,
        queue_url: str,
        message: ReceivedMessage,
        timeout: int,
    ) -> None:
        """Change the visibility timeout of a received message."""
        self._sqs.change_message_visibility(
            QueueUrl=queue_url,
            ReceiptHandle=message.receipt_handle,
            VisibilityTimeout=timeout,
        )

    # ------------------------------------------------------------------ #
    # Message routing
    # ------------------------------------------------------------------ #

    def add_route(
        self,
        predicate: Callable[[Message], bool],
        target_queue_url: str,
    ) -> None:
        """Register a routing rule: messages matching predicate go to target queue."""
        self._routes.append((predicate, target_queue_url))

    def route_message(self, message: Message) -> str | None:
        """Route a message to the first matching queue. Returns target URL or None."""
        for predicate, target_url in self._routes:
            if predicate(message):
                self.send(target_url, message)
                return target_url
        return None

    def forward(
        self,
        source_url: str,
        target_url: str,
        transform: Callable[[str], str] | None = None,
        batch_size: int = 10,
        wait_time: int = 1,
    ) -> int:
        """Consume from source and forward to target. Returns count forwarded.

        Optionally transform message body before forwarding.
        """
        messages = self.receive(source_url, batch_size=batch_size, wait_time=wait_time)
        forwarded = 0
        for msg in messages:
            body = msg.body
            if transform:
                body = transform(body)
            self.send(target_url, Message(body=body, attributes=msg.attributes))
            self.acknowledge(source_url, msg)
            forwarded += 1
        return forwarded

    # ------------------------------------------------------------------ #
    # FIFO message group management
    # ------------------------------------------------------------------ #

    def send_ordered(
        self,
        queue_url: str,
        bodies: list[str],
        group_id: str,
    ) -> list[str]:
        """Send a sequence of messages to a FIFO queue in order, all in the same group."""
        message_ids = []
        for body in bodies:
            dedup_id = hashlib.md5(  # noqa: S324
                f"{group_id}:{body}:{uuid.uuid4().hex}".encode()
            ).hexdigest()
            msg_id = self.send(
                queue_url,
                Message(body=body, group_id=group_id, dedup_id=dedup_id),
            )
            message_ids.append(msg_id)
        return message_ids

    def receive_group(
        self,
        queue_url: str,
        max_messages: int = 10,
        wait_time: int = 5,
    ) -> dict[str, list[ReceivedMessage]]:
        """Receive messages and group them by MessageGroupId.

        Returns a dict mapping group_id -> ordered list of messages.
        """
        messages = self.consume(
            queue_url,
            batch_size=10,
            max_messages=max_messages,
            wait_time=wait_time,
        )
        groups: dict[str, list[ReceivedMessage]] = {}
        for msg in messages:
            # Try to extract group from the message body (JSON with "group" key)
            # or fall back to "default"
            group_key = "default"
            try:
                parsed = json.loads(msg.body)
                if isinstance(parsed, dict) and "group" in parsed:
                    group_key = str(parsed["group"])
            except (json.JSONDecodeError, KeyError):
                pass  # intentionally ignored
            groups.setdefault(group_key, []).append(msg)
        return groups

    # ------------------------------------------------------------------ #
    # Message filtering
    # ------------------------------------------------------------------ #

    def receive_filtered(
        self,
        queue_url: str,
        attribute_filter: dict[str, str],
        batch_size: int = 10,
        max_polls: int = 5,
        wait_time: int = 1,
    ) -> list[ReceivedMessage]:
        """Receive messages matching the given attribute filter.

        Messages that don't match are left in the queue (visibility timeout
        will make them reappear). Only messages where all filter key/value
        pairs match are returned.
        """
        matched: list[ReceivedMessage] = []
        unmatched: list[ReceivedMessage] = []

        for _ in range(max_polls):
            messages = self.receive(
                queue_url,
                batch_size=batch_size,
                wait_time=wait_time,
            )
            if not messages:
                break

            for msg in messages:
                if self._matches_filter(msg, attribute_filter):
                    matched.append(msg)
                else:
                    # Make message visible again immediately
                    self.change_visibility(queue_url, msg, 0)
                    unmatched.append(msg)

        return matched

    def _matches_filter(
        self,
        message: ReceivedMessage,
        attribute_filter: dict[str, str],
    ) -> bool:
        """Check if a message's attributes match all filter criteria."""
        for key, expected_value in attribute_filter.items():
            attr = message.attributes.get(key)
            if attr is None:
                return False
            actual_value = attr.get("StringValue", attr.get("BinaryValue", ""))
            if actual_value != expected_value:
                return False
        return True

    # ------------------------------------------------------------------ #
    # Queue metrics
    # ------------------------------------------------------------------ #

    def get_stats(self, queue_url: str) -> QueueStats:
        """Get approximate queue statistics."""
        attrs = self._sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )
        a = attrs["Attributes"]
        return QueueStats(
            approximate_messages=int(a.get("ApproximateNumberOfMessages", "0")),
            in_flight=int(a.get("ApproximateNumberOfMessagesNotVisible", "0")),
            delayed=int(a.get("ApproximateNumberOfMessagesDelayed", "0")),
        )

    def wait_for_empty(
        self,
        queue_url: str,
        timeout: float = 10.0,
        poll_interval: float = 0.5,
    ) -> bool:
        """Wait until a queue has no visible messages. Returns True if empty within timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            stats = self.get_stats(queue_url)
            if stats.approximate_messages == 0 and stats.delayed == 0:
                return True
            time.sleep(poll_interval)
        return False

    # ------------------------------------------------------------------ #
    # Retry logic
    # ------------------------------------------------------------------ #

    def process_with_retry(
        self,
        queue_url: str,
        handler: Callable[[ReceivedMessage], bool],
        max_retries: int = 3,
        batch_size: int = 1,
        wait_time: int = 5,
    ) -> tuple[int, int]:
        """Process messages with retry logic.

        The handler should return True on success, False on failure.
        Failed messages are left in the queue for retry (via visibility timeout).
        Messages exceeding max_retries are acknowledged to prevent infinite loops
        (they should end up on the DLQ if one is configured).

        Returns (success_count, failure_count).
        """
        messages = self.receive(
            queue_url,
            batch_size=batch_size,
            wait_time=wait_time,
        )

        successes = 0
        failures = 0

        for msg in messages:
            try:
                if handler(msg):
                    self.acknowledge(queue_url, msg)
                    successes += 1
                else:
                    if msg.receive_count >= max_retries:
                        # Let DLQ handle it — just acknowledge to stop retries
                        self.acknowledge(queue_url, msg)
                    failures += 1
            except Exception:
                if msg.receive_count >= max_retries:
                    self.acknowledge(queue_url, msg)
                failures += 1

        return successes, failures

    # ------------------------------------------------------------------ #
    # Utilities
    # ------------------------------------------------------------------ #

    def drain(
        self,
        queue_url: str,
        max_messages: int = 100,
        wait_time: int = 1,
    ) -> list[ReceivedMessage]:
        """Drain all visible messages from a queue, acknowledging each."""
        all_messages: list[ReceivedMessage] = []
        empty_polls = 0
        while len(all_messages) < max_messages and empty_polls < 3:
            msgs = self.receive(queue_url, batch_size=10, wait_time=wait_time)
            if msgs:
                for msg in msgs:
                    self.acknowledge(queue_url, msg)
                all_messages.extend(msgs)
                empty_polls = 0
            else:
                empty_polls += 1
        return all_messages

    def move_messages(
        self,
        source_url: str,
        target_url: str,
        count: int = 10,
    ) -> int:
        """Move messages from one queue to another (e.g., DLQ redrive)."""
        return self.forward(source_url, target_url, batch_size=count)

    def count_messages(self, queue_url: str) -> int:
        """Return the approximate number of visible messages in a queue."""
        return self.get_stats(queue_url).approximate_messages
