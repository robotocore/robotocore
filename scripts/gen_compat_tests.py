#!/usr/bin/env python3
"""Generate compatibility test skeletons from botocore service specs.

Creates pytest test files that exercise the major operations of each AWS service.
Tests are endpoint-agnostic (controlled by ENDPOINT_URL env var) so they can run
against both robotocore and real AWS.

Enhanced: generates smart default parameters for required fields based on shape analysis.

Usage:
    uv run python scripts/gen_compat_tests.py lambda
    uv run python scripts/gen_compat_tests.py stepfunctions --output tests/compatibility/
    uv run python scripts/gen_compat_tests.py --batch sqs,sns,kinesis
    uv run python scripts/gen_compat_tests.py ec2 --smart-defaults --force
"""

import re
import sys
from pathlib import Path


def find_service_model(service_name: str) -> dict | None:
    """Find and load botocore service model."""
    try:
        import botocore.loaders

        loader = botocore.loaders.Loader()
        name_map = {
            "lambda": "lambda",
            "stepfunctions": "stepfunctions",
            "events": "events",
            "eventbridge": "events",
            "logs": "logs",
            "cloudwatch": "monitoring",
        }
        botocore_name = name_map.get(service_name, service_name)
        return loader.load_service_model(botocore_name, "service-2")
    except Exception as e:
        print(f"Warning: Could not load botocore model: {e}", file=sys.stderr)
        return None


def _to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


PRIORITY_PATTERNS = [
    r"^Create",
    r"^Delete",
    r"^Get",
    r"^List",
    r"^Describe",
    r"^Put",
    r"^Update",
    r"^Send",
    r"^Receive",
    r"^Publish",
    r"^Tag",
    r"^Untag",
    r"^Invoke",
]

# Smart default values for common parameter types
SMART_DEFAULTS = {
    # By parameter name (case-insensitive match)
    "FunctionName": 'f"test-func-{uuid.uuid4().hex[:8]}"',
    "QueueName": 'f"test-queue-{uuid.uuid4().hex[:8]}"',
    "TopicName": 'f"test-topic-{uuid.uuid4().hex[:8]}"',
    "TopicArn": "topic_arn  # from fixture",
    "QueueUrl": "queue_url  # from fixture",
    "TableName": 'f"test-table-{uuid.uuid4().hex[:8]}"',
    "BucketName": 'f"test-bucket-{uuid.uuid4().hex[:8]}"',
    "Bucket": 'f"test-bucket-{uuid.uuid4().hex[:8]}"',
    "Key": '"test-key.txt"',
    "Body": 'b"hello world"',
    "StreamName": 'f"test-stream-{uuid.uuid4().hex[:8]}"',
    "RoleName": 'f"test-role-{uuid.uuid4().hex[:8]}"',
    "RoleArn": 'f"arn:aws:iam::123456789012:role/test-role"',
    "PolicyName": 'f"test-policy-{uuid.uuid4().hex[:8]}"',
    "PolicyDocument": "json.dumps(BASIC_POLICY)",
    "AssumeRolePolicyDocument": "json.dumps(ASSUME_ROLE_POLICY)",
    "StateMachineName": 'f"test-sfn-{uuid.uuid4().hex[:8]}"',
    "DefinitionString": "json.dumps(BASIC_SFN_DEFINITION)",
    "Name": 'f"test-{uuid.uuid4().hex[:8]}"',
    "Runtime": '"python3.12"',
    "Handler": '"index.handler"',
    "Code": '{"ZipFile": _make_lambda_zip("def handler(event, context): return event")}',
    "Role": 'f"arn:aws:iam::123456789012:role/test-role"',
    "Description": '"test description"',
    "Timeout": "30",
    "MemorySize": "128",
    "KeySchema": '[{"AttributeName": "id", "KeyType": "HASH"}]',
    "AttributeDefinitions": '[{"AttributeName": "id", "AttributeType": "S"}]',
    "BillingMode": '"PAY_PER_REQUEST"',
    "ShardCount": "1",
    "Protocol": '"sqs"',
    "Endpoint": "queue_arn  # from fixture",
    "MessageBody": '"test message body"',
    "Message": '"test message"',
    "Subject": '"test subject"',
    "Detail": 'json.dumps({"key": "value"})',
    "DetailType": '"TestEvent"',
    "Source": '"test.source"',
    "EventBusName": '"default"',
    "LogGroupName": 'f"/test/logs/{uuid.uuid4().hex[:8]}"',
    "LogStreamName": 'f"test-stream-{uuid.uuid4().hex[:8]}"',
    "SecretId": "secret_arn  # from fixture",
    "SecretString": '"supersecret"',
    "ParameterName": 'f"/test/param/{uuid.uuid4().hex[:8]}"',
    "Value": '"test-value"',
    "Type": '"String"',
    "Entries": '[{"Source": "test.source", "DetailType": "Test", "Detail": "{}"}]',
}

# Shape type -> default value
SHAPE_TYPE_DEFAULTS = {
    "string": '"test-string"',
    "integer": "1",
    "long": "1",
    "boolean": "True",
    "timestamp": '"2024-01-01T00:00:00Z"',
    "blob": 'b"test-data"',
    "map": "{}",
    "list": "[]",
    "structure": "{}",
}

# Common fixture/helper templates per service
SERVICE_FIXTURES: dict[str, str] = {
    "sqs": """
@pytest.fixture
def queue_url(client):
    name = f"test-queue-{uuid.uuid4().hex[:8]}"
    resp = client.create_queue(QueueName=name)
    url = resp["QueueUrl"]
    yield url
    try:
        client.delete_queue(QueueUrl=url)
    except Exception:
        pass
""",
    "sns": """
@pytest.fixture
def topic_arn(client):
    name = f"test-topic-{uuid.uuid4().hex[:8]}"
    resp = client.create_topic(Name=name)
    arn = resp["TopicArn"]
    yield arn
    try:
        client.delete_topic(TopicArn=arn)
    except Exception:
        pass
""",
    "s3": """
@pytest.fixture
def bucket_name(client):
    name = f"test-bucket-{uuid.uuid4().hex[:8]}"
    client.create_bucket(Bucket=name)
    yield name
    try:
        # Delete all objects first
        objs = client.list_objects_v2(Bucket=name).get("Contents", [])
        for obj in objs:
            client.delete_object(Bucket=name, Key=obj["Key"])
        client.delete_bucket(Bucket=name)
    except Exception:
        pass
""",
    "dynamodb": """
@pytest.fixture
def table_name(client):
    name = f"test-table-{uuid.uuid4().hex[:8]}"
    client.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    client.get_waiter("table_exists").wait(TableName=name)
    yield name
    try:
        client.delete_table(TableName=name)
    except Exception:
        pass
""",
    "kinesis": """
@pytest.fixture
def stream_name(client):
    name = f"test-stream-{uuid.uuid4().hex[:8]}"
    client.create_stream(StreamName=name, ShardCount=1)
    client.get_waiter("stream_exists").wait(StreamName=name)
    yield name
    try:
        client.delete_stream(StreamName=name, EnforceConsumerDeletion=True)
    except Exception:
        pass
""",
    "logs": """
@pytest.fixture
def log_group_name(client):
    name = f"/test/logs/{uuid.uuid4().hex[:8]}"
    client.create_log_group(logGroupName=name)
    yield name
    try:
        client.delete_log_group(logGroupName=name)
    except Exception:
        pass
""",
    "secretsmanager": """
@pytest.fixture
def secret_arn(client):
    name = f"test-secret-{uuid.uuid4().hex[:8]}"
    resp = client.create_secret(Name=name, SecretString="supersecret")
    arn = resp["ARN"]
    yield arn
    try:
        client.delete_secret(SecretId=arn, ForceDeleteWithoutRecovery=True)
    except Exception:
        pass
""",
    "events": """
@pytest.fixture
def event_bus_name(client):
    name = f"test-bus-{uuid.uuid4().hex[:8]}"
    client.create_event_bus(Name=name)
    yield name
    try:
        client.delete_event_bus(Name=name)
    except Exception:
        pass
""",
    "lambda": """
import base64
import io
import zipfile


def _make_lambda_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


@pytest.fixture
def function_name(client):
    name = f"test-func-{uuid.uuid4().hex[:8]}"
    client.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role="arn:aws:iam::123456789012:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _make_lambda_zip("def handler(event, context): return event")},
    )
    yield name
    try:
        client.delete_function(FunctionName=name)
    except Exception:
        pass
""",
}

# Common policy documents used by tests
POLICY_HELPERS = """
BASIC_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}],
}

ASSUME_ROLE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

BASIC_SFN_DEFINITION = {
    "StartAt": "Pass",
    "States": {"Pass": {"Type": "Pass", "End": True}},
}
"""


def get_required_params(operation_name: str, model: dict) -> list[dict]:
    """Extract required parameters with their shapes from a botocore operation."""
    op_spec = model.get("operations", {}).get(operation_name, {})
    input_shape_name = op_spec.get("input", {}).get("shape")
    if not input_shape_name:
        return []

    shapes = model.get("shapes", {})
    input_shape = shapes.get(input_shape_name, {})
    required = input_shape.get("required", [])
    members = input_shape.get("members", {})

    params = []
    for param_name in required:
        member = members.get(param_name, {})
        shape_ref = member.get("shape", "")
        shape_def = shapes.get(shape_ref, {})
        params.append(
            {
                "name": param_name,
                "shape": shape_ref,
                "type": shape_def.get("type", "string"),
                "enum": shape_def.get("enum"),
                "min": shape_def.get("min"),
                "max": shape_def.get("max"),
                "pattern": shape_def.get("pattern"),
            }
        )
    return params


def get_smart_default(param: dict) -> str:
    """Get a smart default value for a required parameter."""
    name = param["name"]

    # Check exact match first
    if name in SMART_DEFAULTS:
        return SMART_DEFAULTS[name]

    # Check common suffixes
    for suffix in ["Arn", "Name", "Id", "Url"]:
        if name.endswith(suffix):
            return f'"test-{_to_snake_case(name)}"'

    # Check enum
    if param.get("enum"):
        return f'"{param["enum"][0]}"'

    # Fall back to shape type
    return SHAPE_TYPE_DEFAULTS.get(param["type"], '"test"')


def select_test_operations(operations: list[dict], max_ops: int = 20) -> list[dict]:
    priority = []
    secondary = []

    for op in operations:
        is_priority = any(re.match(p, op["name"]) for p in PRIORITY_PATTERNS)
        if is_priority:
            priority.append(op)
        else:
            secondary.append(op)

    result = priority[:max_ops]
    remaining = max_ops - len(result)
    if remaining > 0:
        result.extend(secondary[:remaining])
    return result


def generate_test_file(
    service_name: str,
    boto3_service: str,
    model: dict,
    operations: list[dict],
    smart_defaults: bool = True,
) -> str:
    """Generate a pytest compatibility test file with smart defaults."""
    service_full = model.get("metadata", {}).get("serviceFullName", service_name)

    lines = [
        f'"""Compatibility tests for {service_full}.',
        "",
        "These tests run against robotocore to verify AWS API compatibility.",
        '"""',
        "",
        "import json",
        "import uuid",
        "",
        "import pytest",
        "",
        "from conftest import make_client",
        "",
    ]

    # Add policy helpers if IAM-related
    if service_name in ("iam", "lambda", "stepfunctions"):
        lines.append(POLICY_HELPERS)

    lines.extend(
        [
            "",
            "@pytest.fixture",
            "def client():",
            f'    return make_client("{boto3_service}")',
            "",
        ]
    )

    # Add service-specific fixtures
    fixture = SERVICE_FIXTURES.get(service_name, "")
    if fixture:
        lines.append(fixture)

    lines.append("")

    selected = select_test_operations(operations)

    for op in selected:
        test_name = _to_snake_case(op["name"])
        params = get_required_params(op["name"], model) if smart_defaults else []

        lines.append(f"def test_{test_name}(client):")
        lines.append(f'    """Test {op["name"]} operation."""')

        if smart_defaults and params:
            # Generate actual API call with smart defaults
            call_args = []
            for p in params:
                default = get_smart_default(p)
                call_args.append(f"        {p['name']}={default},")

            lines.append(f"    response = client.{_to_snake_case(op['name'])}(")
            lines.extend(call_args)
            lines.append("    )")
            lines.append("    assert response is not None")
        else:
            lines.append(f"    # {op['method']} {op['uri']}")
            lines.append("    pytest.skip('Not implemented yet')")

        lines.append("")
        lines.append("")

    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate compatibility tests")
    parser.add_argument("service", nargs="?", help="AWS service name")
    parser.add_argument("--output", default="tests/compatibility/", help="Output directory")
    parser.add_argument("--max-ops", type=int, default=20, help="Max operations to test")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    parser.add_argument(
        "--smart-defaults",
        action="store_true",
        default=True,
        help="Generate smart default parameters (default: True)",
    )
    parser.add_argument(
        "--no-smart-defaults",
        action="store_true",
        help="Disable smart defaults",
    )
    parser.add_argument(
        "--batch",
        help="Comma-separated list of services to generate",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print to stdout instead of writing",
    )
    args = parser.parse_args()

    services = []
    if args.batch:
        services = [s.strip() for s in args.batch.split(",")]
    elif args.service:
        services = [args.service]
    else:
        parser.print_help()
        sys.exit(1)

    smart_defaults = not args.no_smart_defaults

    boto3_map = {
        "lambda": "lambda",
        "stepfunctions": "stepfunctions",
        "events": "events",
        "logs": "logs",
        "cloudwatch": "cloudwatch",
        "kinesis": "kinesis",
        "firehose": "firehose",
        "kms": "kms",
        "secretsmanager": "secretsmanager",
        "ssm": "ssm",
        "apigateway": "apigateway",
        "ec2": "ec2",
        "iam": "iam",
        "sts": "sts",
        "s3": "s3",
        "dynamodb": "dynamodb",
        "sqs": "sqs",
        "sns": "sns",
        "route53": "route53",
        "acm": "acm",
        "ecs": "ecs",
        "cognito-idp": "cognito-idp",
    }

    for service_name in services:
        boto3_service = boto3_map.get(service_name, service_name)

        model = find_service_model(service_name)
        if not model:
            print(f"Could not find botocore model for '{service_name}'", file=sys.stderr)
            continue

        operations = []
        for name, spec in model.get("operations", {}).items():
            http = spec.get("http", {})
            operations.append(
                {
                    "name": name,
                    "method": http.get("method", "POST"),
                    "uri": http.get("requestUri", "/"),
                }
            )

        code = generate_test_file(
            service_name, boto3_service, model, operations, smart_defaults=smart_defaults
        )

        if args.dry_run:
            print(f"# === {service_name} ===")
            print(code)
            print()
            continue

        out_dir = Path(args.output)
        out_file = out_dir / f"test_{service_name}_compat.py"

        if out_file.exists() and not args.force:
            print(
                f"File already exists: {out_file} — skipping (use --force to overwrite)",
                file=sys.stderr,
            )
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        out_file.write_text(code)
        selected = select_test_operations(operations, args.max_ops)
        print(f"Generated {out_file} ({len(operations)} operations, {len(selected)} tests)")


if __name__ == "__main__":
    main()
