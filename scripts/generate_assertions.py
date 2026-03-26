#!/usr/bin/env python3
"""Generate meaningful assertions for AWS operations based on botocore specs.

This tool analyzes botocore service definitions to understand what fields
an operation returns, then generates appropriate assertions.

Usage:
    # Generate assertions for a specific operation
    uv run python scripts/generate_assertions.py --service sqs --operation CreateQueue

    # Generate for all operations in a test
    uv run python scripts/generate_assertions.py --test-file tests/compatibility/test_sqs_compat.py
"""

import argparse
import ast
from pathlib import Path

import botocore.loaders
import botocore.session


def get_operation_output_shape(service: str, operation: str) -> dict:
    """Get the output shape for an operation from botocore specs."""
    loader = botocore.loaders.Loader()
    try:
        api = loader.load_service_model(service, "service-2")
    except Exception:
        return {}

    op_spec = api.get("operations", {}).get(operation, {})
    if "output" not in op_spec:
        return {}

    output_shape_name = op_spec["output"]["shape"]
    shapes = api.get("shapes", {})
    return shapes.get(output_shape_name, {})


def generate_assertions(service: str, operation: str, var_name: str = "response") -> list[str]:
    """Generate assertion statements for an operation's response.

    Returns a list of assertion strings to add to a test.
    """
    shape = get_operation_output_shape(service, operation)
    if not shape:
        return []

    assertions = []
    members = shape.get("members", {})

    # Always check HTTP status
    assertions.append(f'assert {var_name}["ResponseMetadata"]["HTTPStatusCode"] == 200')

    # Generate assertions for required fields
    required = set(shape.get("required", []))
    for member_name, member_spec in members.items():
        if member_name == "ResponseMetadata":
            continue

        # Key presence
        if member_name in required:
            assertions.append(f'assert "{member_name}" in {var_name}')

        # Type-specific checks
        member_type = member_spec.get("type")

        if member_type == "string":
            # Non-empty string
            assertions.append(f'assert isinstance({var_name}["{member_name}"], str)')
            assertions.append(f'assert len({var_name}["{member_name}"]) > 0')

            # ARN format validation
            if "arn" in member_name.lower():
                assertions.append(f'assert {var_name}["{member_name}"].startswith("arn:aws:")')

            # URL format validation
            if "url" in member_name.lower():
                assertions.append(f'assert {var_name}["{member_name}"].startswith("http")')

        elif member_type == "integer" or member_type == "long":
            assertions.append(f'assert isinstance({var_name}["{member_name}"], int)')
            # Non-negative for counts
            if any(x in member_name.lower() for x in ["count", "number", "size"]):
                assertions.append(f'assert {var_name}["{member_name}"] >= 0')

        elif member_type == "boolean":
            assertions.append(f'assert isinstance({var_name}["{member_name}"], bool)')

        elif member_type == "list":
            assertions.append(f'assert isinstance({var_name}["{member_name}"], list)')

        elif member_type == "structure":
            assertions.append(f'assert isinstance({var_name}["{member_name}"], dict)')

    return assertions


def generate_for_test_file(test_file: Path, service: str) -> dict[str, list[str]]:
    """Analyze a test file and generate assertions for each test.

    Returns: {test_name: [assertion_list]}
    """
    if not test_file.exists():
        return {}

    source = test_file.read_text()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return {}

    # Find all test methods that call operations
    test_assertions = {}

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        if not node.name.startswith("test_"):
            continue

        # Find boto3 client calls in this test
        for child in ast.walk(node):
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                # Extract operation name
                method_name = child.func.attr
                # Convert snake_case to PascalCase
                operation = "".join(word.capitalize() for word in method_name.split("_"))

                # Generate assertions
                assertions = generate_assertions(service, operation, "response")
                if assertions:
                    test_assertions[node.name] = assertions
                    break  # Only generate for first operation in test

    return test_assertions


def main():
    parser = argparse.ArgumentParser(description="Generate assertions for AWS operations")
    parser.add_argument("--service", help="AWS service name")
    parser.add_argument("--operation", help="Operation name (PascalCase)")
    parser.add_argument("--test-file", type=Path, help="Test file to analyze")
    parser.add_argument("--var-name", default="response", help="Variable name for response")
    args = parser.parse_args()

    if args.test_file and args.service:
        # Generate for all tests in file
        assertions_map = generate_for_test_file(args.test_file, args.service)
        for test_name, assertions in assertions_map.items():
            print(f"\n# {test_name}")
            for assertion in assertions[:5]:  # Limit to top 5
                print(f"    {assertion}")
    elif args.service and args.operation:
        # Generate for specific operation
        assertions = generate_assertions(args.service, args.operation, args.var_name)
        for assertion in assertions:
            print(assertion)
    else:
        parser.error("Provide either --test-file + --service OR --service + --operation")


if __name__ == "__main__":
    main()
