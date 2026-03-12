"""Fixtures for the event chain test suite."""

import uuid

import pytest

from .app import EventChainOrchestrator

ENDPOINT_URL = "http://localhost:4566"


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
