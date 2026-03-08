"""
Shared fixtures for application integration tests.

These tests verify real-world application patterns against an AWS-compatible
endpoint. They work with LocalStack, robotocore, or real AWS.

Usage:
    AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/ -v
"""

import io
import os
import uuid
import zipfile

import boto3
import pytest
from botocore.config import Config

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def boto_session():
    return boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "testing"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "testing"),
        region_name=REGION,
    )


@pytest.fixture
def s3(boto_session):
    return boto_session.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        config=Config(s3={"addressing_style": "path"}),
    )


@pytest.fixture
def sqs(boto_session):
    return boto_session.client("sqs", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def dynamodb(boto_session):
    return boto_session.client("dynamodb", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def lambda_client(boto_session):
    return boto_session.client("lambda", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def events(boto_session):
    return boto_session.client("events", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def secretsmanager(boto_session):
    return boto_session.client("secretsmanager", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def sns(boto_session):
    return boto_session.client("sns", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def iam(boto_session):
    return boto_session.client("iam", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def apigateway(boto_session):
    return boto_session.client("apigateway", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def stepfunctions(boto_session):
    return boto_session.client("stepfunctions", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def kinesis(boto_session):
    return boto_session.client("kinesis", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def cloudwatch(boto_session):
    return boto_session.client("cloudwatch", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def logs(boto_session):
    return boto_session.client("logs", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def ssm(boto_session):
    return boto_session.client("ssm", endpoint_url=ENDPOINT_URL)


def make_lambda_zip(code: str) -> bytes:
    """Create a Lambda deployment package from inline Python code."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


@pytest.fixture
def unique_name():
    """Generate a unique resource name to avoid collisions."""
    return f"app-test-{uuid.uuid4().hex[:8]}"
