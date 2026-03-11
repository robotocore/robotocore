#!/usr/bin/env python3
"""pulumilocal — Pulumi CLI wrapper that auto-configures Pulumi to use robotocore endpoints.

Usage:
    pulumilocal [pulumi args...]

Environment variables:
    ROBOTOCORE_HOST       Custom hostname (default: localhost)
    ROBOTOCORE_PORT       Custom port (default: 4566)
    ROBOTOCORE_ENDPOINT   Full endpoint URL (overrides host/port)
    PULUMI_BACKEND_URL    Pulumi state backend (default: file://~)
    PULUMI_CONFIG_PASSPHRASE  Encryption passphrase (default: test)
"""

import json
import os
import shutil
import subprocess
import sys

DEFAULT_HOST = "localhost"
DEFAULT_PORT = "4566"
DEFAULT_ENDPOINT_TEMPLATE = "http://{host}:{port}"
DEFAULT_CREDENTIALS = {
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_DEFAULT_REGION": "us-east-1",
}
DEFAULT_PASSPHRASE = "test"
DEFAULT_BACKEND_URL = "file://~"

# AWS services supported by the Pulumi AWS provider that should be routed to robotocore
PULUMI_AWS_SERVICES = [
    "accessanalyzer",
    "acm",
    "apigateway",
    "apigatewayv2",
    "appconfig",
    "applicationautoscaling",
    "appsync",
    "athena",
    "autoscaling",
    "batch",
    "cloudformation",
    "cloudfront",
    "cloudwatch",
    "cloudwatchlogs",
    "codecommit",
    "codedeploy",
    "cognitoidp",
    "configservice",
    "dynamodb",
    "ec2",
    "ecr",
    "ecs",
    "efs",
    "eks",
    "elasticache",
    "elasticsearch",
    "elb",
    "elbv2",
    "emr",
    "events",
    "firehose",
    "glacier",
    "glue",
    "iam",
    "kinesis",
    "kms",
    "lambda",
    "mediastore",
    "mq",
    "neptune",
    "opensearch",
    "organizations",
    "qldb",
    "rds",
    "redshift",
    "resourcegroups",
    "resourcegroupstaggingapi",
    "route53",
    "route53resolver",
    "s3",
    "sagemaker",
    "secretsmanager",
    "ses",
    "sesv2",
    "sfn",
    "sns",
    "sqs",
    "ssm",
    "sts",
    "swf",
    "timestreamwrite",
    "transcribe",
    "waf",
    "wafv2",
    "xray",
]


def get_endpoint(base_env: dict[str, str]) -> str:
    """Determine the robotocore endpoint URL from environment variables."""
    if endpoint := base_env.get("ROBOTOCORE_ENDPOINT"):
        return endpoint
    host = base_env.get("ROBOTOCORE_HOST", DEFAULT_HOST)
    port = base_env.get("ROBOTOCORE_PORT", DEFAULT_PORT)
    return DEFAULT_ENDPOINT_TEMPLATE.format(host=host, port=port)


def build_env(base_env: dict[str, str]) -> dict[str, str]:
    """Build the full environment dict for running pulumi.

    Sets defaults for AWS credentials, endpoint, passphrase, and backend URL
    without overriding values already set by the user.

    Args:
        base_env: The current environment (or a subset for testing).

    Returns:
        A new dict with all required variables set.
    """
    env = dict(base_env)

    # Endpoint
    env["AWS_ENDPOINT_URL"] = get_endpoint(base_env)

    # Credentials — don't override user-set values
    for key, default in DEFAULT_CREDENTIALS.items():
        if key not in env:
            env[key] = default

    # Pulumi config passphrase
    if "PULUMI_CONFIG_PASSPHRASE" not in env:
        env["PULUMI_CONFIG_PASSPHRASE"] = DEFAULT_PASSPHRASE

    # Pulumi backend URL
    if "PULUMI_BACKEND_URL" not in env:
        env["PULUMI_BACKEND_URL"] = DEFAULT_BACKEND_URL

    return env


def build_command(args: list[str]) -> list[str]:
    """Build the pulumi command with all arguments passed through.

    Args:
        args: Arguments to pass to pulumi.

    Returns:
        The full command list starting with 'pulumi'.
    """
    return ["pulumi"] + args


def build_pulumi_config(endpoint: str) -> dict[str, str]:
    """Build Pulumi provider configuration for AWS.

    Args:
        endpoint: The robotocore endpoint URL.

    Returns:
        A dict of Pulumi config keys to values.
    """
    # Build endpoints array — Pulumi AWS provider expects JSON array of objects
    endpoints = [{svc: endpoint for svc in PULUMI_AWS_SERVICES}]

    return {
        "aws:skipCredentialsValidation": "true",
        "aws:skipMetadataApiCheck": "true",
        "aws:s3UsePathStyle": "true",
        "aws:endpoints": json.dumps(endpoints),
    }


def check_pulumi_installed() -> None:
    """Check that pulumi is installed and on PATH.

    Raises:
        SystemExit: If pulumi is not found, exits with a helpful message.
    """
    if shutil.which("pulumi") is None:
        print(
            "Error: 'pulumi' not found on PATH.\n"
            "Install Pulumi: https://www.pulumi.com/docs/install/",
            file=sys.stderr,
        )
        sys.exit(1)


def main() -> int:
    """Entry point for pulumilocal CLI."""
    check_pulumi_installed()

    env = build_env(dict(os.environ))
    cmd = build_command(sys.argv[1:])

    try:
        result = subprocess.run(cmd, env=env)
        return result.returncode
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
