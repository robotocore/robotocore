"""
Fixtures for the event-driven architecture test suite.
"""

import uuid

import pytest

from .app import EventRouter


@pytest.fixture
def event_router(events, sns, sqs, dynamodb):
    """Create an EventRouter instance with all required clients."""
    router = EventRouter(
        events_client=events,
        sns_client=sns,
        sqs_client=sqs,
        dynamodb_client=dynamodb,
    )
    yield router
    router.cleanup()


@pytest.fixture
def event_bus(event_router, unique_name):
    """Create a custom EventBridge event bus with automatic cleanup."""
    bus_name = f"evt-bus-{unique_name}"
    event_router.create_bus(bus_name)
    return bus_name


@pytest.fixture
def fan_out(event_router, unique_name):
    """Create an SNS topic with 3 SQS subscriber queues wired together.

    Returns (FanOutConfig, [queue_url_1, queue_url_2, queue_url_3]).
    """
    topic_name = f"fanout-topic-{unique_name}"
    queue_names = [
        f"fanout-q1-{unique_name}",
        f"fanout-q2-{unique_name}",
        f"fanout-q3-{unique_name}",
    ]
    config = event_router.create_fan_out(topic_name, queue_names)
    queue_urls = [event_router.get_queue_url_by_name(qn) for qn in queue_names]
    return config, queue_urls


@pytest.fixture
def schema_table(event_router, unique_name):
    """Create a DynamoDB table for the event schema registry."""
    table_name = f"schemas-{unique_name}"
    event_router.init_schema_table(table_name)
    return table_name


@pytest.fixture
def unique_name():
    """Generate a unique resource name to avoid test collisions."""
    return f"ed-{uuid.uuid4().hex[:8]}"
