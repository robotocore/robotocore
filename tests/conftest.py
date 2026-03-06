"""Shared test fixtures for robotocore."""

import pytest
from starlette.testclient import TestClient

from robotocore.gateway.app import app


@pytest.fixture
def client():
    """Starlette test client for unit/integration tests."""
    return TestClient(app)


@pytest.fixture
def aws_client():
    """boto3-compatible client factory pointing at robotocore."""
    import boto3

    def _make_client(service_name: str, region_name: str = "us-east-1"):
        return boto3.client(
            service_name,
            endpoint_url="http://localhost:4566",
            region_name=region_name,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

    return _make_client


@pytest.fixture
def live_aws_client():
    """boto3 client factory for integration tests against a running container.

    Only use in tests/integration/ — requires `docker compose up` first.
    """
    import boto3

    def _make_client(service_name: str, region_name: str = "us-east-1"):
        return boto3.client(
            service_name,
            endpoint_url="http://localhost:4566",
            region_name=region_name,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

    return _make_client
