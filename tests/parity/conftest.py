"""Shared fixtures for AWS parity tests.

These tests are designed to run against any AWS-compatible endpoint:
robotocore or real AWS. The endpoint is configured via the
AWS_ENDPOINT_URL environment variable (default: http://localhost:4566).
"""

import json
import os

import boto3
import pytest
from botocore.config import Config

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")

LAMBDA_ASSUME_ROLE_POLICY = json.dumps(
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


class ClientFactory:
    """Factory that creates boto3 clients pointed at the configured endpoint."""

    def __getattr__(self, service_name):
        service = service_name
        if service_name == "lambda_":
            service = "lambda"
        else:
            service = service_name.replace("_", "-")

        config_kwargs = {}
        if service == "s3":
            config_kwargs["s3"] = {"addressing_style": "path"}

        return boto3.client(
            service,
            endpoint_url=ENDPOINT_URL,
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            config=Config(**config_kwargs),
        )


@pytest.fixture
def aws_client():
    """Factory for boto3 clients pointed at the configured endpoint."""
    return ClientFactory()


@pytest.fixture(scope="session")
def lambda_role_arn():
    """Create an IAM role that Lambda can assume. Shared across all tests."""
    iam = boto3.client(
        "iam",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    role_name = "parity-test-lambda-role"
    try:
        resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=LAMBDA_ASSUME_ROLE_POLICY,
        )
        role_arn = resp["Role"]["Arn"]
    except iam.exceptions.EntityAlreadyExistsException:
        resp = iam.get_role(RoleName=role_name)
        role_arn = resp["Role"]["Arn"]

    # Attach basic execution policy (may not exist in emulators)
    try:
        iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=("arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"),
        )
    except Exception:
        pass  # best-effort cleanup
    return role_arn
