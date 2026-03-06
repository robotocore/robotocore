#!/usr/bin/env python3
"""Generate compatibility test skeletons from botocore service specs.

Creates pytest test files that exercise the major operations of each AWS service.
Tests are endpoint-agnostic (controlled by ENDPOINT_URL env var) so they can run
against both robotocore and LocalStack/real AWS.

Usage:
    uv run python scripts/gen_compat_tests.py lambda
    uv run python scripts/gen_compat_tests.py stepfunctions --output tests/compatibility/
"""

import json
import os
import re
import sys
from pathlib import Path


def find_service_model(service_name: str) -> dict | None:
    """Find and load botocore service model."""
    try:
        import botocore.loaders
        loader = botocore.loaders.Loader()
        # Map names
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
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


# Operations that are good candidates for compatibility tests
# (common CRUD operations that most implementations should support)
PRIORITY_PATTERNS = [
    r"^Create", r"^Delete", r"^Get", r"^List", r"^Describe",
    r"^Put", r"^Update", r"^Send", r"^Receive", r"^Publish",
    r"^Tag", r"^Untag", r"^Invoke",
]


def select_test_operations(operations: list[dict], max_ops: int = 20) -> list[dict]:
    """Select the most important operations to test."""
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


def generate_test_file(service_name: str, boto3_service: str, model: dict, operations: list[dict]) -> str:
    """Generate a pytest compatibility test file."""
    service_full = model.get("metadata", {}).get("serviceFullName", service_name)

    lines = [
        f'"""Compatibility tests for {service_full}.',
        "",
        "These tests run against both robotocore and LocalStack to verify parity.",
        '"""',
        "",
        "import uuid",
        "",
        "import pytest",
        "",
        "from conftest import make_client",
        "",
        "",
        "@pytest.fixture",
        f"def client():",
        f'    return make_client("{boto3_service}")',
        "",
        "",
    ]

    selected = select_test_operations(operations)

    for op in selected:
        test_name = _to_snake_case(op["name"])
        lines.extend([
            f"def test_{test_name}(client):",
            f'    """Test {op["name"]} operation."""',
            f"    # TODO: Implement test for {op['name']}",
            f"    # {op['method']} {op['uri']}",
            f"    pytest.skip('Not implemented yet')",
            "",
            "",
        ])

    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate compatibility tests")
    parser.add_argument("service", help="AWS service name")
    parser.add_argument("--output", default="tests/compatibility/", help="Output directory")
    parser.add_argument("--max-ops", type=int, default=20, help="Max operations to test")
    args = parser.parse_args()

    # Map service names to boto3 client names
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
    }
    boto3_service = boto3_map.get(args.service, args.service)

    model = find_service_model(args.service)
    if not model:
        print(f"Could not find botocore model for '{args.service}'", file=sys.stderr)
        sys.exit(1)

    operations = []
    for name, spec in model.get("operations", {}).items():
        http = spec.get("http", {})
        operations.append({
            "name": name,
            "method": http.get("method", "POST"),
            "uri": http.get("requestUri", "/"),
        })

    code = generate_test_file(args.service, boto3_service, model, operations)
    out_dir = Path(args.output)
    out_file = out_dir / f"test_{args.service}_compat.py"

    if out_file.exists():
        print(f"File already exists: {out_file} — skipping (use --force to overwrite)", file=sys.stderr)
        sys.exit(0)

    out_file.write_text(code)
    print(f"Generated {out_file} ({len(operations)} operations, {len(select_test_operations(operations, args.max_ops))} tests)")


if __name__ == "__main__":
    main()
