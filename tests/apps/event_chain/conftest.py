"""Fixtures for the event chain test suite."""

import os
import uuid

import pytest

from .app import EventChainOrchestrator

# The AGENTS.md portability rule: suites follow ENDPOINT_URL (the compat
# lane runs on non-default ports) or AWS_ENDPOINT_URL, in that order;
# localhost:4566 is only the no-env default.
ENDPOINT_URL = (
    os.environ.get("ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL") or "http://localhost:4566"
)


@pytest.fixture
def unique_name():
    return f"ec-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def chain(s3, lambda_client, dynamodb, sqs, sns, events, cloudwatch, iam):
    """Create an EventChainOrchestrator with all clients."""
    orch = EventChainOrchestrator(
        s3_client=s3,
        lambda_client=lambda_client,
        dynamodb_client=dynamodb,
        sqs_client=sqs,
        sns_client=sns,
        events_client=events,
        cloudwatch_client=cloudwatch,
        iam_client=iam,
        endpoint_url=ENDPOINT_URL,
    )
    yield orch
    orch.cleanup()
