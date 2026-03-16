"""
Shared fixtures for application integration tests.

These tests verify real-world application patterns against an AWS-compatible
endpoint. They work with robotocore or real AWS.

Usage:
    AWS_ENDPOINT_URL=http://localhost:4566 pytest tests/apps/ -v
"""

import io
import json
import os
import time
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


@pytest.fixture
def lambda_role(iam):
    """Create an IAM role for Lambda execution. Yields ARN, cleans up."""
    role_name = f"lambda-role-{uuid.uuid4().hex[:8]}"
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy,
        Description="Lambda execution role for app tests",
    )
    role_arn = resp["Role"]["Arn"]
    yield role_arn
    try:
        iam.delete_role(RoleName=role_name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def deploy_lambda(lambda_client, lambda_role):
    """Factory fixture to deploy Lambda functions from inline code.

    Usage:
        fn_arn = deploy_lambda("my-func", handler_code, env={"KEY": "val"})

    Returns the function ARN. Handles cleanup automatically.
    """
    created: list[str] = []

    def _deploy(name: str, code: str, env: dict[str, str] | None = None) -> str:
        zip_bytes = make_lambda_zip(code)
        kwargs: dict = {
            "FunctionName": name,
            "Runtime": "python3.12",
            "Role": lambda_role,
            "Handler": "index.handler",
            "Code": {"ZipFile": zip_bytes},
            "Timeout": 30,
        }
        if env:
            kwargs["Environment"] = {"Variables": env}
        resp = lambda_client.create_function(**kwargs)
        created.append(name)
        return resp["FunctionArn"]

    yield _deploy

    for fn_name in created:
        try:
            lambda_client.delete_function(FunctionName=fn_name)
        except Exception:
            pass  # best-effort cleanup


def wait_for_messages(sqs_client, queue_url: str, timeout: int = 10, expected: int = 1):
    """Poll SQS with retry/timeout. Returns list of parsed message bodies."""
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        for msg in resp.get("Messages", []):
            messages.append(msg)
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
    return messages
