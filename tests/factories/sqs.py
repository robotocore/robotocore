"""SQS test data factories with automatic cleanup.

Provides context managers for creating SQS queues and messages that are
automatically cleaned up after the test.

Usage:
    from tests.factories.sqs import queue, fifo_queue, queue_with_messages

    def test_send_receive(sqs):
        with queue(sqs) as queue_url:
            sqs.send_message(QueueUrl=queue_url, MessageBody="hello")
            response = sqs.receive_message(QueueUrl=queue_url)
            assert len(response["Messages"]) == 1

    def test_fifo(sqs):
        with fifo_queue(sqs) as queue_url:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody="hello",
                MessageGroupId="group1",
                MessageDeduplicationId="dedup1"
            )

    def test_batch_receive(sqs):
        with queue_with_messages(sqs, count=10) as (queue_url, message_ids):
            response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=5)
            assert len(response["Messages"]) == 5
"""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from botocore.exceptions import ClientError

from . import unique_name

__all__ = ["queue", "fifo_queue", "queue_with_messages"]


@contextmanager
def queue(client: Any, name: str | None = None, **attributes: Any) -> Generator[str, None, None]:
    """Create an SQS queue with automatic cleanup.

    Args:
        client: boto3 SQS client
        name: Optional queue name (auto-generated if not provided)
        **attributes: Optional queue attributes (VisibilityTimeout, etc.)

    Yields:
        Queue URL

    Example:
        with queue(sqs) as queue_url:
            sqs.send_message(QueueUrl=queue_url, MessageBody="test")

        with queue(sqs, VisibilityTimeout="30") as queue_url:
            ...
    """
    queue_name = name or unique_name("test-queue")

    create_kwargs: dict[str, Any] = {"QueueName": queue_name}
    if attributes:
        create_kwargs["Attributes"] = {str(k): str(v) for k, v in attributes.items()}

    response = client.create_queue(**create_kwargs)
    queue_url = response["QueueUrl"]

    try:
        yield queue_url
    finally:
        try:
            client.delete_queue(QueueUrl=queue_url)
        except ClientError:
            pass  # Best effort cleanup


@contextmanager
def fifo_queue(
    client: Any, name: str | None = None, **attributes: Any
) -> Generator[str, None, None]:
    """Create a FIFO SQS queue with automatic cleanup.

    Args:
        client: boto3 SQS client
        name: Optional queue name base (auto-generated if not provided)
               .fifo suffix is added automatically
        **attributes: Optional queue attributes

    Yields:
        Queue URL

    Example:
        with fifo_queue(sqs) as queue_url:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody="test",
                MessageGroupId="group1",
                MessageDeduplicationId="dedup1"
            )
    """
    base_name = name or unique_name("test-fifo")
    queue_name = f"{base_name}.fifo"

    attrs = {
        "FifoQueue": "true",
        "ContentBasedDeduplication": "true",
        **{str(k): str(v) for k, v in attributes.items()},
    }

    response = client.create_queue(QueueName=queue_name, Attributes=attrs)
    queue_url = response["QueueUrl"]

    try:
        yield queue_url
    finally:
        try:
            client.delete_queue(QueueUrl=queue_url)
        except ClientError:
            pass  # Best effort cleanup


@contextmanager
def queue_with_messages(
    client: Any, count: int = 5, name: str | None = None, **attributes: Any
) -> Generator[tuple[str, list[str]], None, None]:
    """Create an SQS queue pre-populated with messages.

    Args:
        client: boto3 SQS client
        count: Number of messages to send (default 5)
        name: Optional queue name (auto-generated if not provided)
        **attributes: Optional queue attributes

    Yields:
        Tuple of (queue_url, list of message_ids)

    Example:
        with queue_with_messages(sqs, count=10) as (queue_url, message_ids):
            response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=5)
            assert len(response["Messages"]) == 5
    """
    with queue(client, name=name, **attributes) as queue_url:
        message_ids = []
        for i in range(count):
            response = client.send_message(QueueUrl=queue_url, MessageBody=f"test-message-{i}")
            message_ids.append(response["MessageId"])
        yield queue_url, message_ids
