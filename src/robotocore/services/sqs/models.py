"""In-memory SQS data models: messages, queues, and lifecycle management."""

import base64
import hashlib
import heapq
import json
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from queue import Empty, Queue


def _new_id() -> str:
    return str(uuid.uuid4())


@dataclass
class SqsMessage:
    message_id: str
    body: str
    md5_of_body: str
    attributes: dict = field(default_factory=dict)
    message_attributes: dict = field(default_factory=dict)
    system_attributes: dict = field(default_factory=dict)
    created: float = field(default_factory=time.time)
    delay_seconds: int = 0
    receive_count: int = 0
    first_received: float | None = None
    last_received: float | None = None
    visibility_deadline: float | None = None
    deleted: bool = False
    receipt_handles: set = field(default_factory=set)
    # FIFO fields
    message_group_id: str | None = None
    message_deduplication_id: str | None = None
    sequence_number: str | None = None

    @property
    def is_visible(self) -> bool:
        if self.deleted:
            return False
        if self.visibility_deadline is not None:
            return time.time() >= self.visibility_deadline
        return True

    @property
    def is_delayed(self) -> bool:
        if self.delay_seconds <= 0:
            return False
        return time.time() < self.created + self.delay_seconds

    @property
    def priority(self) -> float:
        return self.created

    def update_visibility_timeout(self, timeout: int) -> None:
        self.visibility_deadline = time.time() + timeout

    def __eq__(self, other):
        if isinstance(other, SqsMessage):
            return self.message_id == other.message_id
        return NotImplemented

    def __hash__(self):
        return hash(self.message_id)

    def __lt__(self, other):
        return self.priority < other.priority


@dataclass
class MessageMoveTask:
    """Represents a message move task (DLQ redrive)."""

    task_handle: str
    source_arn: str
    destination_arn: str | None
    max_number_of_messages_per_second: int
    status: str = "RUNNING"  # RUNNING, COMPLETED, CANCELLING, CANCELLED, FAILED
    approximate_number_of_messages_moved: int = 0
    approximate_number_of_messages_to_move: int = 0
    started_timestamp: float = field(default_factory=time.time)
    failure_reason: str | None = None


class StandardQueue:
    """Standard SQS queue with visibility timeout and long polling."""

    def __init__(self, name: str, region: str, account_id: str, attributes: dict | None = None):
        self.name = name
        self.region = region
        self.account_id = account_id
        self.created = time.time()
        self.attributes = attributes or {}
        self.mutex = threading.RLock()
        self.tags: dict[str, str] = {}

        # Message storage
        self._visible: Queue = Queue()
        self._inflight: OrderedDict[str, SqsMessage] = OrderedDict()
        self._delayed: dict[str, SqsMessage] = {}
        self._all_messages: dict[str, SqsMessage] = {}
        self._receipts: dict[str, SqsMessage] = {}

    @property
    def arn(self) -> str:
        return f"arn:aws:sqs:{self.region}:{self.account_id}:{self.name}"

    @property
    def url(self) -> str:
        from robotocore.services.sqs.endpoint_strategy import sqs_queue_url

        return sqs_queue_url(self.name, self.region, self.account_id)

    @property
    def is_fifo(self) -> bool:
        return self.name.endswith(".fifo")

    @property
    def default_visibility_timeout(self) -> int:
        return int(self.attributes.get("VisibilityTimeout", "30"))

    @property
    def wait_time_seconds(self) -> int:
        return int(self.attributes.get("ReceiveMessageWaitTimeSeconds", "0"))

    @property
    def delay_seconds(self) -> int:
        return int(self.attributes.get("DelaySeconds", "0"))

    @property
    def max_receive_count(self) -> int | None:
        policy = self.redrive_policy
        if policy:
            return int(policy.get("maxReceiveCount", 0)) or None
        return None

    @property
    def redrive_policy(self) -> dict | None:
        raw = self.attributes.get("RedrivePolicy")
        if raw:
            return json.loads(raw) if isinstance(raw, str) else raw
        return None

    @property
    def redrive_allow_policy(self) -> dict | None:
        raw = self.attributes.get("RedriveAllowPolicy")
        if raw:
            return json.loads(raw) if isinstance(raw, str) else raw
        return None

    def is_redrive_allowed(self, source_queue_arn: str) -> bool:
        """Check if a given source queue is allowed to use this queue as DLQ."""
        policy = self.redrive_allow_policy
        if not policy:
            return True  # No policy = allow all (default)
        permission = policy.get("redrivePermission", "allowAll")
        if permission == "allowAll":
            return True
        if permission == "denyAll":
            return False
        if permission == "byQueue":
            allowed = policy.get("sourceQueueArns", [])
            return source_queue_arn in allowed
        return True

    def put(self, message: SqsMessage) -> None:
        with self.mutex:
            delay = message.delay_seconds or self.delay_seconds
            if delay > 0:
                message.delay_seconds = delay
                self._delayed[message.message_id] = message
            else:
                self._visible.put(message)
            self._all_messages[message.message_id] = message

    def receive(
        self,
        max_messages: int = 1,
        visibility_timeout: int | None = None,
        wait_time_seconds: int | None = None,
    ) -> list[tuple[SqsMessage, str]]:
        """Receive messages. Returns list of (message, receipt_handle) tuples."""
        if visibility_timeout is None:
            visibility_timeout = self.default_visibility_timeout
        if wait_time_seconds is None:
            wait_time_seconds = self.wait_time_seconds

        results = []
        block = wait_time_seconds > 0
        timeout = wait_time_seconds if block else 0.05  # Small timeout for non-blocking

        start = time.time()
        while len(results) < max_messages:
            try:
                remaining = max(0, timeout - (time.time() - start)) if block else 0.01
                if remaining <= 0 and len(results) > 0:
                    break
                message = self._visible.get(block=block, timeout=max(remaining, 0.01))
            except Empty:
                break

            if message.deleted:
                continue

            with self.mutex:
                message.receive_count += 1
                now = time.time()
                if message.first_received is None:
                    message.first_received = now
                message.last_received = now
                message.update_visibility_timeout(visibility_timeout)

                receipt_handle = self._make_receipt_handle(message)
                message.receipt_handles.add(receipt_handle)
                self._inflight[message.message_id] = message
                self._receipts[receipt_handle] = message

            results.append((message, receipt_handle))

            # After first message, don't block for remaining
            if block:
                block = False

        return results

    def delete_message(self, receipt_handle: str) -> bool:
        with self.mutex:
            message = self._receipts.pop(receipt_handle, None)
            if message is None:
                return False
            message.deleted = True
            self._inflight.pop(message.message_id, None)
            self._all_messages.pop(message.message_id, None)
            # Clean up all receipt handles for this message
            for rh in message.receipt_handles:
                self._receipts.pop(rh, None)
            return True

    def change_visibility(self, receipt_handle: str, timeout: int) -> bool:
        with self.mutex:
            message = self._receipts.get(receipt_handle)
            if message is None:
                return False
            if timeout == 0:
                # Make immediately visible
                message.visibility_deadline = None
                self._inflight.pop(message.message_id, None)
                self._visible.put(message)
            else:
                message.update_visibility_timeout(timeout)
            return True

    def requeue_inflight_messages(self) -> None:
        """Move expired inflight messages back to visible queue."""
        with self.mutex:
            expired = [msg for msg in self._inflight.values() if msg.is_visible and not msg.deleted]
            for msg in expired:
                self._inflight.pop(msg.message_id, None)
                msg.visibility_deadline = None
                self._visible.put(msg)

    def enqueue_delayed_messages(self) -> None:
        """Move delayed messages that are ready to the visible queue."""
        with self.mutex:
            ready = [
                msg for msg in self._delayed.values() if not msg.is_delayed and not msg.deleted
            ]
            for msg in ready:
                self._delayed.pop(msg.message_id, None)
                self._visible.put(msg)

    def purge(self) -> None:
        with self.mutex:
            while not self._visible.empty():
                try:
                    self._visible.get_nowait()
                except Empty:
                    break
            self._inflight.clear()
            self._delayed.clear()
            self._all_messages.clear()
            self._receipts.clear()

    def get_all_messages(self) -> list[SqsMessage]:
        """Return all messages in queue (visible + inflight + delayed). For move tasks."""
        with self.mutex:
            return list(self._all_messages.values())

    def get_attributes(self) -> dict:
        with self.mutex:
            visible_count = self._visible.qsize()
            inflight_count = len(self._inflight)
            delayed_count = len(self._delayed)

        attrs = {
            "QueueArn": self.arn,
            "CreatedTimestamp": str(int(self.created)),
            "LastModifiedTimestamp": str(int(self.created)),
            "ApproximateNumberOfMessages": str(visible_count),
            "ApproximateNumberOfMessagesNotVisible": str(inflight_count),
            "ApproximateNumberOfMessagesDelayed": str(delayed_count),
            "VisibilityTimeout": str(self.default_visibility_timeout),
            "ReceiveMessageWaitTimeSeconds": str(self.wait_time_seconds),
            "DelaySeconds": str(self.delay_seconds),
            "MaximumMessageSize": self.attributes.get("MaximumMessageSize", "262144"),
            "MessageRetentionPeriod": self.attributes.get("MessageRetentionPeriod", "345600"),
        }
        if "RedrivePolicy" in self.attributes:
            attrs["RedrivePolicy"] = self.attributes["RedrivePolicy"]
        if "RedriveAllowPolicy" in self.attributes:
            attrs["RedriveAllowPolicy"] = self.attributes["RedriveAllowPolicy"]
        if "Policy" in self.attributes:
            attrs["Policy"] = self.attributes["Policy"]
        # SSE attributes (simulated)
        if "SqsManagedSseEnabled" in self.attributes:
            attrs["SqsManagedSseEnabled"] = self.attributes["SqsManagedSseEnabled"]
        if "KmsMasterKeyId" in self.attributes:
            attrs["KmsMasterKeyId"] = self.attributes["KmsMasterKeyId"]
        if "KmsDataKeyReusePeriodSeconds" in self.attributes:
            attrs["KmsDataKeyReusePeriodSeconds"] = self.attributes["KmsDataKeyReusePeriodSeconds"]
        if self.is_fifo:
            attrs["FifoQueue"] = "true"
            attrs["ContentBasedDeduplication"] = self.attributes.get(
                "ContentBasedDeduplication", "false"
            )
        return attrs

    def _make_receipt_handle(self, message: SqsMessage) -> str:
        raw = f"{_new_id()} {self.arn} {message.message_id} {message.last_received}"
        return base64.b64encode(raw.encode()).decode()


class FifoQueue(StandardQueue):
    """FIFO SQS queue with message group ordering and deduplication."""

    DEDUP_INTERVAL = 300  # 5 minutes

    def __init__(self, name: str, region: str, account_id: str, attributes: dict | None = None):
        super().__init__(name, region, account_id, attributes)
        self._dedup_cache: dict[str, tuple[SqsMessage, float]] = {}
        self._message_groups: dict[str, list[SqsMessage]] = {}
        self._inflight_groups: set[str] = set()
        self._queued_groups: set[str] = set()  # Groups currently in _group_queue
        self._group_queue: Queue = Queue()
        self._sequence_counter = 0

    @property
    def content_based_dedup(self) -> bool:
        return self.attributes.get("ContentBasedDeduplication", "false").lower() == "true"

    def put(self, message: SqsMessage) -> SqsMessage:
        with self.mutex:
            # Deduplication
            dedup_id = message.message_deduplication_id
            if not dedup_id and self.content_based_dedup:
                dedup_id = hashlib.sha256(message.body.encode()).hexdigest()
                message.message_deduplication_id = dedup_id

            if dedup_id:
                self._clean_dedup_cache()
                if dedup_id in self._dedup_cache:
                    existing, _ = self._dedup_cache[dedup_id]
                    return existing  # Return original (dedup)
                self._dedup_cache[dedup_id] = (message, time.time())

            # Sequence number
            self._sequence_counter += 1
            message.sequence_number = str(self._sequence_counter)

            # Add to message group
            group_id = message.message_group_id or "__default__"
            if group_id not in self._message_groups:
                self._message_groups[group_id] = []
            heapq.heappush(self._message_groups[group_id], message)
            self._all_messages[message.message_id] = message

            # Enqueue group if not already in-flight or queued
            if group_id not in self._inflight_groups and group_id not in self._queued_groups:
                self._group_queue.put(group_id)
                self._queued_groups.add(group_id)

        return message

    def receive(
        self,
        max_messages: int = 1,
        visibility_timeout: int | None = None,
        wait_time_seconds: int | None = None,
    ) -> list[tuple[SqsMessage, str]]:
        if visibility_timeout is None:
            visibility_timeout = self.default_visibility_timeout
        if wait_time_seconds is None:
            wait_time_seconds = self.wait_time_seconds

        results = []
        block = wait_time_seconds > 0
        timeout = wait_time_seconds if block else 0.05
        start = time.time()

        while len(results) < max_messages:
            try:
                remaining = max(0, timeout - (time.time() - start)) if block else 0.01
                if remaining <= 0 and len(results) > 0:
                    break
                group_id = self._group_queue.get(block=block, timeout=max(remaining, 0.01))
            except Empty:
                break

            with self.mutex:
                self._queued_groups.discard(group_id)
                group_msgs = self._message_groups.get(group_id, [])
                if not group_msgs:
                    continue

                self._inflight_groups.add(group_id)

                while len(results) < max_messages and group_msgs:
                    message = heapq.heappop(group_msgs)
                    if message.deleted:
                        continue

                    message.receive_count += 1
                    now = time.time()
                    if message.first_received is None:
                        message.first_received = now
                    message.last_received = now
                    message.update_visibility_timeout(visibility_timeout)

                    receipt_handle = self._make_receipt_handle(message)
                    message.receipt_handles.add(receipt_handle)
                    self._inflight[message.message_id] = message
                    self._receipts[receipt_handle] = message
                    results.append((message, receipt_handle))

            if block:
                block = False

        return results

    def delete_message(self, receipt_handle: str) -> bool:
        with self.mutex:
            message = self._receipts.pop(receipt_handle, None)
            if message is None:
                return False
            message.deleted = True
            self._inflight.pop(message.message_id, None)
            self._all_messages.pop(message.message_id, None)
            for rh in message.receipt_handles:
                self._receipts.pop(rh, None)
            # Check if group should become available again
            group_id = message.message_group_id or "__default__"
            group_msgs = self._message_groups.get(group_id, [])
            # Check if no more inflight messages for this group
            group_still_inflight = any(
                m.message_group_id == message.message_group_id for m in self._inflight.values()
            )
            if not group_still_inflight:
                self._inflight_groups.discard(group_id)
                if group_msgs and group_id not in self._queued_groups:
                    self._group_queue.put(group_id)
                    self._queued_groups.add(group_id)
            return True

    def requeue_inflight_messages(self) -> None:
        with self.mutex:
            expired = [msg for msg in self._inflight.values() if msg.is_visible and not msg.deleted]
            for msg in expired:
                self._inflight.pop(msg.message_id, None)
                msg.visibility_deadline = None
                group_id = msg.message_group_id or "__default__"
                if group_id not in self._message_groups:
                    self._message_groups[group_id] = []
                heapq.heappush(self._message_groups[group_id], msg)
                # Make group available again
                group_still_inflight = any(
                    m.message_group_id == msg.message_group_id for m in self._inflight.values()
                )
                if not group_still_inflight:
                    self._inflight_groups.discard(group_id)
                    if group_id not in self._queued_groups:
                        self._group_queue.put(group_id)
                        self._queued_groups.add(group_id)

    def _clean_dedup_cache(self) -> None:
        now = time.time()
        expired = [k for k, (_, t) in self._dedup_cache.items() if now - t > self.DEDUP_INTERVAL]
        for k in expired:
            del self._dedup_cache[k]


class SqsStore:
    """Per-region SQS store managing all queues."""

    def __init__(self):
        self.queues: dict[str, StandardQueue] = {}
        self.mutex = threading.RLock()
        self._move_tasks: dict[str, MessageMoveTask] = {}

    def create_queue(
        self, name: str, region: str, account_id: str, attributes: dict | None = None
    ) -> StandardQueue:
        with self.mutex:
            if name in self.queues:
                return self.queues[name]
            if name.endswith(".fifo"):
                queue = FifoQueue(name, region, account_id, attributes)
            else:
                queue = StandardQueue(name, region, account_id, attributes)
            self.queues[name] = queue
            return queue

    def get_queue(self, name: str) -> StandardQueue | None:
        return self.queues.get(name)

    def get_queue_by_url(self, url: str) -> StandardQueue | None:
        # URL format: http://host:port/account_id/queue_name
        parts = url.rstrip("/").rsplit("/", 1)
        if len(parts) >= 2:
            name = parts[-1]
            return self.queues.get(name)
        return None

    def get_queue_by_arn(self, arn: str) -> StandardQueue | None:
        name = arn.rsplit(":", 1)[-1]
        return self.queues.get(name)

    def delete_queue(self, name: str) -> bool:
        with self.mutex:
            return self.queues.pop(name, None) is not None

    def list_queues(self, prefix: str | None = None) -> list[StandardQueue]:
        queues = list(self.queues.values())
        if prefix:
            queues = [q for q in queues if q.name.startswith(prefix)]
        return queues

    def requeue_all(self) -> None:
        """Background task: requeue expired inflight and delayed messages."""
        for queue in list(self.queues.values()):
            queue.requeue_inflight_messages()
            queue.enqueue_delayed_messages()

    # --- Message Move Tasks ---

    def start_message_move_task(
        self,
        source_arn: str,
        destination_arn: str | None = None,
        max_number_per_second: int = 500,
    ) -> MessageMoveTask:
        """Start moving messages from source (DLQ) to destination queue."""
        source_queue = self.get_queue_by_arn(source_arn)
        if not source_queue:
            raise ValueError(f"Source queue not found: {source_arn}")

        # If no destination, find the original queue that uses this as DLQ
        if not destination_arn:
            # Actually for DLQ redrive, the source IS the DLQ, and we need
            # to find the original queue. We'll check all queues for one
            # that has this DLQ as its target.
            for q in self.queues.values():
                rp = q.redrive_policy
                if rp and rp.get("deadLetterTargetArn") == source_arn:
                    destination_arn = q.arn
                    break

        dest_queue = self.get_queue_by_arn(destination_arn) if destination_arn else None

        task_handle = _new_id()
        messages = source_queue.get_all_messages()
        task = MessageMoveTask(
            task_handle=task_handle,
            source_arn=source_arn,
            destination_arn=destination_arn,
            max_number_of_messages_per_second=max_number_per_second,
            approximate_number_of_messages_to_move=len(messages),
        )
        self._move_tasks[task_handle] = task

        # Actually move messages
        if dest_queue:
            for msg in messages:
                if msg.deleted:
                    continue
                new_msg = SqsMessage(
                    message_id=_new_id(),
                    body=msg.body,
                    md5_of_body=msg.md5_of_body,
                    message_attributes=dict(msg.message_attributes),
                )
                dest_queue.put(new_msg)
                msg.deleted = True
                task.approximate_number_of_messages_moved += 1

        # Purge source of moved messages
        if dest_queue:
            source_queue.purge()

        task.status = "COMPLETED"
        return task

    def get_message_move_task(self, task_handle: str) -> MessageMoveTask | None:
        return self._move_tasks.get(task_handle)

    def list_message_move_tasks(self, source_arn: str) -> list[MessageMoveTask]:
        return [t for t in self._move_tasks.values() if t.source_arn == source_arn]

    def cancel_message_move_task(self, task_handle: str) -> MessageMoveTask | None:
        task = self._move_tasks.get(task_handle)
        if task and task.status == "RUNNING":
            task.status = "CANCELLED"
        return task
