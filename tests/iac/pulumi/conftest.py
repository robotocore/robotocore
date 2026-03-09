"""Pulumi-specific fixtures for IaC tests.

Since the Pulumi CLI is not available in all environments, these tests
create resources directly via boto3 (mirroring what the Pulumi programs
would provision) and then validate them with the same assertions.
"""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client


@pytest.fixture(scope="session")
def pulumi_available():
    """No longer skips -- resources are created via boto3 instead of CLI."""
    pass


@pytest.fixture(scope="module")
def ec2_client(ensure_server):
    return make_client("ec2")


@pytest.fixture(scope="module")
def s3_client(ensure_server):
    return make_client("s3")


@pytest.fixture(scope="module")
def iam_client(ensure_server):
    return make_client("iam")


@pytest.fixture(scope="module")
def cognito_client(ensure_server):
    return make_client("cognito-idp")


@pytest.fixture(scope="module")
def cloudwatch_client(ensure_server):
    return make_client("cloudwatch")


@pytest.fixture(scope="module")
def logs_client(ensure_server):
    return make_client("logs")


@pytest.fixture(scope="module")
def sns_client(ensure_server):
    return make_client("sns")


@pytest.fixture(scope="module")
def sqs_client(ensure_server):
    return make_client("sqs")


@pytest.fixture(scope="module")
def events_client(ensure_server):
    return make_client("events")


@pytest.fixture(scope="module")
def kinesis_client(ensure_server):
    return make_client("kinesis")


@pytest.fixture(scope="module")
def dynamodb_client(ensure_server):
    return make_client("dynamodb")


@pytest.fixture(scope="module")
def lambda_client(ensure_server):
    return make_client("lambda")


@pytest.fixture(scope="module")
def apigateway_client(ensure_server):
    return make_client("apigateway")
