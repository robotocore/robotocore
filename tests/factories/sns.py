"""SNS test data factories with automatic cleanup.

Provides context managers for creating SNS topics and subscriptions that are
automatically cleaned up after the test.

Usage:
    from tests.factories.sns import topic, topic_with_subscriptions

    def test_publish(sns):
        with topic(sns) as topic_arn:
            sns.publish(TopicArn=topic_arn, Message="hello")

    def test_subscriptions(sns, sqs):
        with topic_with_subscriptions(sns, protocol="sqs", endpoints=[queue_arn]) as topic_arn:
            subscriptions = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
            assert len(subscriptions["Subscriptions"]) == 1
"""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from botocore.exceptions import ClientError

from . import unique_name

__all__ = ["topic", "topic_with_subscriptions"]


@contextmanager
def topic(client: Any, name: str | None = None) -> Generator[str, None, None]:
    """Create an SNS topic with automatic cleanup.

    Args:
        client: boto3 SNS client
        name: Optional topic name (auto-generated if not provided)

    Yields:
        Topic ARN

    Example:
        with topic(sns) as topic_arn:
            sns.publish(TopicArn=topic_arn, Message="hello")
    """
    topic_name = name or unique_name("test-topic")

    response = client.create_topic(Name=topic_name)
    topic_arn = response["TopicArn"]

    try:
        yield topic_arn
    finally:
        try:
            # Delete all subscriptions first
            paginator = client.get_paginator("list_subscriptions_by_topic")
            for page in paginator.paginate(TopicArn=topic_arn):
                for sub in page.get("Subscriptions", []):
                    if sub["SubscriptionArn"] != "PendingConfirmation":
                        try:
                            client.unsubscribe(SubscriptionArn=sub["SubscriptionArn"])
                        except ClientError:
                            pass  # best-effort cleanup

            client.delete_topic(TopicArn=topic_arn)
        except ClientError:
            pass  # Best effort cleanup


@contextmanager
def topic_with_subscriptions(
    client: Any,
    protocol: str,
    endpoints: list[str],
    name: str | None = None,
) -> Generator[str, None, None]:
    """Create an SNS topic with subscriptions.

    Args:
        client: boto3 SNS client
        protocol: Subscription protocol ("sqs", "http", "https", "email", etc.)
        endpoints: List of endpoints (queue ARNs, URLs, emails, etc.)
        name: Optional topic name (auto-generated if not provided)

    Yields:
        Topic ARN

    Example:
        with topic_with_subscriptions(sns, protocol="sqs", endpoints=[queue_arn]) as topic_arn:
            subscriptions = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
            assert len(subscriptions["Subscriptions"]) == 1
    """
    with topic(client, name=name) as topic_arn:
        for endpoint in endpoints:
            client.subscribe(TopicArn=topic_arn, Protocol=protocol, Endpoint=endpoint)
        yield topic_arn


@contextmanager
def fifo_topic(client: Any, name: str | None = None) -> Generator[str, None, None]:
    """Create a FIFO SNS topic with automatic cleanup.

    Args:
        client: boto3 SNS client
        name: Optional topic name base (auto-generated if not provided)
              .fifo suffix is added automatically

    Yields:
        Topic ARN

    Example:
        with fifo_topic(sns) as topic_arn:
            sns.publish(
                TopicArn=topic_arn,
                Message="hello",
                MessageGroupId="group1",
                MessageDeduplicationId="dedup1"
            )
    """
    base_name = name or unique_name("test-fifo")
    topic_name = f"{base_name}.fifo"

    response = client.create_topic(
        Name=topic_name,
        Attributes={
            "FifoTopic": "true",
            "ContentBasedDeduplication": "true",
        },
    )
    topic_arn = response["TopicArn"]

    try:
        yield topic_arn
    finally:
        try:
            client.delete_topic(TopicArn=topic_arn)
        except ClientError:
            pass  # Best effort cleanup
