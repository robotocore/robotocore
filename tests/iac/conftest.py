"""Shared fixtures for IaC integration tests."""

import os
import uuid

import boto3
import pytest
import urllib3
from botocore.config import Config

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")
REGION = "us-east-1"
ACCOUNT_ID = "123456789012"


def make_client(service_name: str, **kwargs):
    """Create a boto3 client pointed at the robotocore endpoint."""
    config_kwargs = {}
    if service_name == "s3":
        config_kwargs["s3"] = {"addressing_style": "path"}

    return boto3.client(
        service_name,
        endpoint_url=ENDPOINT_URL,
        region_name=kwargs.pop("region_name", REGION),
        aws_access_key_id=kwargs.pop("aws_access_key_id", "testing"),
        aws_secret_access_key=kwargs.pop("aws_secret_access_key", "testing"),
        config=Config(**config_kwargs),
        **kwargs,
    )


@pytest.fixture(scope="session")
def ensure_server():
    """Skip all IaC tests if the robotocore server is not reachable."""
    http = urllib3.PoolManager()
    try:
        resp = http.request("GET", f"{ENDPOINT_URL}/_robotocore/health", timeout=5.0)
        if resp.status != 200:
            pytest.skip(f"Robotocore server unhealthy (status {resp.status})")
    except Exception as exc:
        pytest.skip(f"Robotocore server unreachable at {ENDPOINT_URL}: {exc}")


@pytest.fixture(scope="session")
def test_run_id():
    """Unique prefix for resources created during this test run."""
    return f"iac-{uuid.uuid4().hex[:8]}"


# ── boto3 client fixtures ──────────────────────────────────────────────────


@pytest.fixture(scope="session")
def s3(ensure_server):
    return make_client("s3")


@pytest.fixture(scope="session")
def dynamodb(ensure_server):
    return make_client("dynamodb")


@pytest.fixture(scope="session")
def lambda_client(ensure_server):
    return make_client("lambda")


@pytest.fixture(scope="session")
def sqs(ensure_server):
    return make_client("sqs")


@pytest.fixture(scope="session")
def sns(ensure_server):
    return make_client("sns")


@pytest.fixture(scope="session")
def iam(ensure_server):
    return make_client("iam")


@pytest.fixture(scope="session")
def apigateway(ensure_server):
    return make_client("apigateway")


@pytest.fixture(scope="session")
def cloudwatch(ensure_server):
    return make_client("cloudwatch")


@pytest.fixture(scope="session")
def logs(ensure_server):
    return make_client("logs")


@pytest.fixture(scope="session")
def ec2(ensure_server):
    return make_client("ec2")


@pytest.fixture(scope="session")
def kinesis(ensure_server):
    return make_client("kinesis")


@pytest.fixture(scope="session")
def route53(ensure_server):
    return make_client("route53")


@pytest.fixture(scope="session")
def cognito(ensure_server):
    return make_client("cognito-idp")


@pytest.fixture(scope="session")
def ssm(ensure_server):
    return make_client("ssm")


@pytest.fixture(scope="session")
def secretsmanager(ensure_server):
    return make_client("secretsmanager")


@pytest.fixture(scope="session")
def stepfunctions(ensure_server):
    return make_client("stepfunctions")


@pytest.fixture(scope="session")
def cloudformation(ensure_server):
    return make_client("cloudformation")
