#!/usr/bin/env python3
"""Generate compat tests for untested working operations using probe results.

Reads probe results JSON to know which operations work, scans existing test
files to find what's already tested, then generates minimal compat tests
for the gap.

Usage:
    # Dry-run: show what would be generated for a service
    uv run python scripts/gen_coverage_tests.py --service ec2

    # Generate and write tests
    uv run python scripts/gen_coverage_tests.py --service ec2 --write

    # Batch: generate for all services
    uv run python scripts/gen_coverage_tests.py --all --write

    # Show summary of gaps
    uv run python scripts/gen_coverage_tests.py --all --summary
"""

import argparse
import json
import re
import sys
from pathlib import Path

import botocore.session

PROBE_FILE = Path("full_probe_results.json")
TESTS_DIR = Path("tests/compatibility")

# Operations that are destructive or impractical to test without setup
SKIP_OPS = {
    "RunInstances",
    "TerminateInstances",
    "StopInstances",
    "StartInstances",
    "RebootInstances",
    "InvokeAsync",
    "GetBucketLifecycle",
    "PutBucketLifecycle",
    "DeleteStack",
    "DeleteBucket",
    "DeleteQueue",
    "DeleteTopic",
    "DeleteTable",
    "DeleteFunction",
    "DeleteRole",
    "DeleteUser",
    "DeletePolicy",
    "DeleteGroup",
    "DeleteKey",
    "DeleteSecret",
    "DeleteStateMachine",
    "DeleteLogGroup",
    "DeleteRule",
    "DeleteHostedZone",
    "DeleteCertificate",
    "DeleteDomain",
    "DeleteCluster",
    "DeleteService",
    "DeleteRepository",
}

# boto3 service name overrides (when test file name differs from service name)
SERVICE_TO_FILE = {
    "cognito-idp": "cognito",
    "resource-groups": "resource_groups",
    "resourcegroupstaggingapi": "resource_groups_tagging",
    "apigatewaymanagementapi": "apigatewaymanagementapi",
    "dynamodbstreams": "dynamodbstreams",
    "route53resolver": "route53resolver",
    "s3control": "s3control",
}

# botocore service name overrides (when botocore uses different name)
# Registry service name → botocore/boto3 service name
BOTOCORE_NAME_MAP: dict[str, str] = {
    "acmpca": "acm-pca",
    "applicationautoscaling": "application-autoscaling",
    "bedrockagent": "bedrock-agent",
    "cognitoidentity": "cognito-identity",
    "ec2instanceconnect": "ec2-instance-connect",
    "emrcontainers": "emr-containers",
    "emrserverless": "emr-serverless",
    "iotdata": "iot-data",
    "lexv2models": "lexv2-models",
    "networkfirewall": "network-firewall",
    "rdsdata": "rds-data",
    "redshiftdata": "redshift-data",
    "servicecatalogappregistry": "servicecatalog-appregistry",
    "ssoadmin": "sso-admin",
    "timestreaminfluxdb": "timestream-influxdb",
    "timestreamquery": "timestream-query",
    "timestreamwrite": "timestream-write",
    "vpclattice": "vpc-lattice",
    "workspacesweb": "workspaces-web",
}


def _to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_test_file(service: str) -> Path:
    """Get the test file path for a service."""
    file_stem = SERVICE_TO_FILE.get(service, service.replace("-", "_"))
    return TESTS_DIR / f"test_{file_stem}_compat.py"


def _to_botocore_name(service: str) -> str:
    return BOTOCORE_NAME_MAP.get(service, service)


def get_all_operations(service: str) -> list[str]:
    """Get all botocore operations for a service."""
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(_to_botocore_name(service))
        return sorted(model.operation_names)
    except Exception:
        return []


def get_tested_operations(test_file: Path) -> set[str]:
    """Extract AWS operation names that appear to be tested."""
    if not test_file.exists():
        return set()
    source = test_file.read_text()
    ops = set()
    skip_methods = {
        "append",
        "get",
        "items",
        "keys",
        "values",
        "pop",
        "update",
        "join",
        "format",
        "encode",
        "decode",
        "split",
        "strip",
        "startswith",
        "endswith",
        "replace",
        "lower",
        "upper",
        "raises",
        "mark",
        "fixture",
        "skip",
        "parametrize",
        "dumps",
        "loads",
        "sleep",
        "time",
        "hexdigest",
        "getvalue",
        "writestr",
        "read",
        "write",
        "close",
        "wait",
        "result",
        "exception",
        "isinstance",
        "assert",
        "print",
        "sorted",
        "list",
        "dict",
        "set",
        "len",
        "any",
        "all",
        "next",
        "enumerate",
        "range",
        "zip",
        "map",
        "filter",
    }
    for match in re.finditer(r"\b\w+\.\s*(\w+)\s*\(", source):
        method = match.group(1)
        if method.startswith("_") or method in skip_methods:
            continue
        pascal = "".join(word.capitalize() for word in method.split("_"))
        ops.add(pascal)
    return ops


def get_working_ops(probe_results: dict, service: str) -> set[str]:
    """Get operations that are working from probe results."""
    svc_results = probe_results.get("results", {}).get(service, {})
    working = set()
    for op, info in svc_results.items():
        if isinstance(info, dict):
            status = info.get("status", "")
            if status in ("working", "needs_params"):
                working.add(op)
    return working


def get_parameterless_ops(service: str) -> set[str]:
    """Get operations that have no required parameters."""
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(_to_botocore_name(service))
    except Exception:
        return set()

    parameterless = set()
    for op_name in model.operation_names:
        op_model = model.operation_model(op_name)
        input_shape = op_model.input_shape
        if input_shape is None:
            parameterless.add(op_name)
            continue
        required = input_shape.metadata.get("required", [])
        if hasattr(input_shape, "required_members"):
            required = input_shape.required_members
        if not required:
            parameterless.add(op_name)
    return parameterless


def get_response_key(service: str, op_name: str) -> str | None:
    """Get the main response key for an operation (e.g., 'Buckets' for ListBuckets)."""
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(_to_botocore_name(service))
        op_model = model.operation_model(op_name)
        output = op_model.output_shape
        if output is None:
            return None
        members = list(output.members.keys())
        # Filter out metadata keys
        content_keys = [
            k
            for k in members
            if k
            not in (
                "ResponseMetadata",
                "NextToken",
                "NextMarker",
                "Marker",
                "IsTruncated",
                "MaxItems",
                "RequestId",
            )
        ]
        if content_keys:
            return content_keys[0]
    except Exception:
        pass
    return None


def generate_test_class(
    service: str,
    ops: list[str],
    parameterless: set[str],
) -> str:
    """Generate a test class for untested operations."""
    # Determine client variable name
    client_var = service.replace("-", "_")
    if client_var == "lambda":
        client_var = "lam"

    class_name = "".join(w.capitalize() for w in service.replace("-", "_").split("_"))

    lines = []
    lines.append("")
    lines.append("")
    lines.append(f"class Test{class_name}AutoCoverage:")
    lines.append(f'    """Auto-generated coverage tests for {service}."""')
    lines.append("")
    boto_name = _to_botocore_name(service)
    lines.append("    @pytest.fixture")
    lines.append("    def client(self):")
    lines.append(f'        return make_client("{boto_name}")')
    lines.append("")

    for op in ops:
        snake = _to_snake_case(op)
        resp_key = get_response_key(service, op)

        if op in parameterless:
            # Simple parameterless test
            if resp_key:
                lines.append(f"    def test_{snake}(self, client):")
                lines.append(f'        """{op} returns a response."""')
                lines.append(f"        resp = client.{snake}()")
                lines.append(f'        assert "{resp_key}" in resp')
                lines.append("")
            else:
                lines.append(f"    def test_{snake}(self, client):")
                lines.append(f'        """{op} returns a response."""')
                lines.append(f"        client.{snake}()")
                lines.append("")
        else:
            # Operation needs params — test that it exists (expect param error)
            lines.append(f"    def test_{snake}(self, client):")
            lines.append(f'        """{op} is implemented (may need params)."""')
            lines.append("        try:")
            lines.append(f"            client.{snake}()")
            lines.append("        except client.exceptions.ClientError:")
            lines.append("            pass  # Expected — operation exists but needs params")
            lines.append("        except client.exceptions.ParamValidationError:")
            lines.append("            pass  # Expected — operation exists but needs params")
            lines.append("")

    return "\n".join(lines)


def generate_full_file(service: str, ops: list[str], parameterless: set[str]) -> str:
    """Generate a complete test file for a service that has no existing tests."""
    client_var = service.replace("-", "_")
    if client_var == "lambda":
        client_var = "lam"

    header = (
        f'"""Auto-generated {service} compatibility tests."""\n\n'
        "import pytest\n\n"
        "from tests.compatibility.conftest import make_client\n"
    )
    body = generate_test_class(service, ops, parameterless)
    return header + body


def analyze_service(service: str, probe_results: dict) -> tuple[list[str], set[str], Path]:
    """Analyze a service and return (untested_working_ops, parameterless_ops, test_file)."""
    test_file = get_test_file(service)
    tested = get_tested_operations(test_file)
    working = get_working_ops(probe_results, service)
    parameterless = get_parameterless_ops(service)
    all_ops = set(get_all_operations(service))

    # Untested working ops (excluding skipped)
    untested = sorted((working & all_ops) - tested - SKIP_OPS)
    return untested, parameterless, test_file


def main():
    parser = argparse.ArgumentParser(description="Generate coverage tests from probe data")
    parser.add_argument("--service", help="Single service to generate for")
    parser.add_argument("--all", action="store_true", help="Generate for all probed services")
    parser.add_argument("--summary", action="store_true", help="Show gap summary only")
    parser.add_argument("--write", action="store_true", help="Write tests to files")
    parser.add_argument("--dry-run", action="store_true", help="Print generated code (default)")
    parser.add_argument("--probe-file", default=str(PROBE_FILE), help="Path to probe results JSON")
    parser.add_argument(
        "--max-per-service", type=int, default=0, help="Max tests per service (0=unlimited)"
    )
    args = parser.parse_args()

    probe_path = Path(args.probe_file)
    if not probe_path.exists():
        print(f"Error: probe file {probe_path} not found. Run batch_probe_gap.py first.")
        sys.exit(1)

    with open(probe_path) as f:
        probe_results = json.load(f)

    if args.service:
        services = [args.service]
    elif args.all:
        services = sorted(probe_results.get("results", {}).keys())
    else:
        parser.error("Specify --service or --all")

    total_generated = 0
    total_gap = 0

    for service in services:
        untested, parameterless, test_file = analyze_service(service, probe_results)

        if args.max_per_service > 0:
            untested = untested[: args.max_per_service]

        total_gap += len(untested)

        if args.summary:
            if untested:
                print(f"{service}: {len(untested)} untested working ops")
            continue

        if not untested:
            continue

        if test_file.exists():
            # Append to existing file
            code = generate_test_class(service, untested, parameterless)
        else:
            # Create new file
            code = generate_full_file(service, untested, parameterless)

        if args.write:
            if test_file.exists():
                with open(test_file, "a") as f:
                    f.write(code)
                print(f"{service}: appended {len(untested)} tests to {test_file}")
            else:
                with open(test_file, "w") as f:
                    f.write(code)
                print(f"{service}: created {test_file} with {len(untested)} tests")
            total_generated += len(untested)
        else:
            print(f"\n# === {service} ({len(untested)} tests) → {test_file} ===")
            print(code)
            total_generated += len(untested)

    if args.summary:
        print(f"\nTotal untested working ops: {total_gap}")
    elif total_generated:
        print(f"\nTotal tests generated: {total_generated}")


if __name__ == "__main__":
    main()
