"""
Fixtures for message queue application tests.
"""

import pytest

from .app import MessageBroker
from .models import QueueConfig


@pytest.fixture
def broker(sqs):
    """Create a MessageBroker instance with the shared SQS client."""
    return MessageBroker(sqs)


@pytest.fixture
def standard_queue(broker, unique_name):
    """Create a standard queue with cleanup."""
    config = QueueConfig(
        name=f"standard-{unique_name}",
        visibility_timeout=5,
    )
    url = broker.create_queue(config)
    yield url
    broker.delete_queue(url)


@pytest.fixture
def fifo_queue(broker, unique_name):
    """Create a FIFO queue with content-based deduplication."""
    config = QueueConfig(
        name=f"fifo-{unique_name}",
        fifo=True,
        visibility_timeout=30,
    )
    url = broker.create_queue(config)
    yield url
    broker.delete_queue(url)


@pytest.fixture
def dlq_pair(broker, sqs, unique_name):
    """Create a main queue + DLQ pair wired together with redrive policy."""
    # Create DLQ first
    dlq_config = QueueConfig(name=f"dlq-{unique_name}", visibility_timeout=5)
    dlq_url = broker.create_queue(dlq_config)
    dlq_arn = broker.get_queue_arn(dlq_url)

    # Create main queue with redrive policy
    main_config = QueueConfig(
        name=f"main-{unique_name}",
        visibility_timeout=1,
        max_receive_count=1,
        dlq_arn=dlq_arn,
    )
    main_url = broker.create_queue(main_config)

    yield main_url, dlq_url

    broker.delete_queue(main_url)
    broker.delete_queue(dlq_url)


@pytest.fixture
def delay_queue(broker, unique_name):
    """Create a queue with 5-second delivery delay."""
    config = QueueConfig(
        name=f"delay-{unique_name}",
        delay_seconds=5,
        visibility_timeout=10,
    )
    url = broker.create_queue(config)
    yield url
    broker.delete_queue(url)
