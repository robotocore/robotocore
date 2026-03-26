"""Test data factories with automatic cleanup.

Provides context managers for creating AWS resources that are automatically
cleaned up after the test, even if the test fails.

Usage:
    from tests.factories.sqs import queue, queue_with_messages

    def test_send_receive(sqs):
        with queue(sqs) as queue_url:
            sqs.send_message(QueueUrl=queue_url, MessageBody="hello")
            response = sqs.receive_message(QueueUrl=queue_url)
            assert len(response["Messages"]) == 1

    def test_batch_receive(sqs):
        with queue_with_messages(sqs, count=10) as (queue_url, message_ids):
            response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=5)
            assert len(response["Messages"]) == 5
"""

from collections.abc import Generator
from contextlib import contextmanager
from uuid import uuid4

__all__ = ["unique_name", "cleanup_errors"]


def unique_name(prefix: str = "test") -> str:
    """Generate a unique name for test resources.

    Format: {prefix}-{8 char hex}

    Examples:
        unique_name() -> "test-a1b2c3d4"
        unique_name("my-queue") -> "my-queue-a1b2c3d4"
    """
    return f"{prefix}-{uuid4().hex[:8]}"


@contextmanager
def cleanup_errors() -> Generator[list[Exception], None, None]:
    """Context manager that collects cleanup errors instead of raising them.

    Usage:
        with cleanup_errors() as errors:
            try:
                client.delete_resource(id=resource_id)
            except Exception as e:
                errors.append(e)
        # errors list contains any cleanup failures

    This prevents cleanup failures from masking test failures while still
    recording what went wrong.
    """
    errors: list[Exception] = []
    yield errors
    # Could log errors here if needed
