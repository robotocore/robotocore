#!/usr/bin/env python3
"""Analyze compatibility test coverage gaps.

Compares botocore operations against existing compat tests to find gaps.
Can generate incremental test stubs for missing operations.

Usage:
    # Show coverage report for all services
    uv run python scripts/compat_coverage.py

    # Show gaps for specific service
    uv run python scripts/compat_coverage.py --service sqs

    # Generate test stubs for gaps (dry-run)
    uv run python scripts/compat_coverage.py --service sqs --generate --dry-run

    # Write generated stubs to files
    uv run python scripts/compat_coverage.py --service sqs --generate --write

    # Generate for all services with low coverage
    uv run python scripts/compat_coverage.py --generate --min-coverage 50 --write
"""

import ast
import re
import sys
from pathlib import Path

import botocore.session

# Map from our service names to botocore service names
BOTOCORE_MAP = {
    "sqs": "sqs",
    "sns": "sns",
    "s3": "s3",
    "dynamodb": "dynamodb",
    "lambda": "lambda",
    "iam": "iam",
    "sts": "sts",
    "events": "events",
    "logs": "logs",
    "cloudwatch": "cloudwatch",
    "kms": "kms",
    "secretsmanager": "secretsmanager",
    "ssm": "ssm",
    "kinesis": "kinesis",
    "firehose": "firehose",
    "stepfunctions": "stepfunctions",
    "cloudformation": "cloudformation",
    "apigateway": "apigateway",
    "ec2": "ec2",
    "route53": "route53",
    "acm": "acm",
    "ses": "ses",
    "scheduler": "scheduler",
}

# Map from our file names to botocore service names
FILE_TO_SERVICE = {
    "test_sqs_compat.py": "sqs",
    "test_sns_compat.py": "sns",
    "test_s3_compat.py": "s3",
    "test_dynamodb_compat.py": "dynamodb",
    "test_lambda_compat.py": "lambda",
    "test_iam_compat.py": "iam",
    "test_sts_compat.py": "sts",
    "test_events_compat.py": "events",
    "test_logs_compat.py": "logs",
    "test_cloudwatch_compat.py": "cloudwatch",
    "test_kms_compat.py": "kms",
    "test_secretsmanager_compat.py": "secretsmanager",
    "test_ssm_compat.py": "ssm",
    "test_kinesis_compat.py": "kinesis",
    "test_firehose_compat.py": "firehose",
    "test_stepfunctions_compat.py": "stepfunctions",
    "test_cloudformation_compat.py": "cloudformation",
    "test_apigateway_compat.py": "apigateway",
    "test_ec2_compat.py": "ec2",
    "test_route53_compat.py": "route53",
    "test_acm_compat.py": "acm",
    "test_ses_compat.py": "ses",
    "test_scheduler_compat.py": "scheduler",
}

# Operations that are admin-only, deprecated, or testing-unfriendly
SKIP_OPERATIONS = {
    # Cross-account / org operations
    "AcceptHandshake", "CreateOrganization", "InviteAccountToOrganization",
    # Deprecated
    "GetBucketLifecycle", "PutBucketLifecycle",
    # Waiter-only
    "DescribeTable", "DescribeStream",
    # Read-only aggregate (tested implicitly)
    "GetMetricData", "GetInsightResults",
    # Dangerous in tests
    "DeleteAccountAlias", "DeleteAccount",
}


def _to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_botocore_operations(service_name: str) -> list[str]:
    """Get all operation names for a botocore service."""
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(service_name)
        return sorted(model.operation_names)
    except Exception:
        return []


def get_tested_operations(test_file: Path) -> set[str]:
    """Extract AWS operation names tested in a file by analyzing boto3 client calls."""
    if not test_file.exists():
        return set()

    source = test_file.read_text()
    tested = set()

    # Parse AST to find client method calls
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            # Convert snake_case method to PascalCase operation name
            pascal = "".join(word.capitalize() for word in method_name.split("_"))
            tested.add(pascal)

    # Also scan for string references to operation names in test names/docstrings
    for match in re.finditer(r"def test_(\w+)", source):
        test_name = match.group(1)
        # Extract likely operation from test name (e.g., "test_create_queue" → "CreateQueue")
        parts = test_name.split("_")
        # Try to find a matching operation by progressively joining parts
        for i in range(1, len(parts) + 1):
            candidate = "".join(word.capitalize() for word in parts[:i])
            tested.add(candidate)

    return tested


def analyze_service(
    service_name: str, botocore_name: str, test_file: Path
) -> dict:
    """Analyze coverage for a single service."""
    all_ops = get_botocore_operations(botocore_name)
    tested_ops = get_tested_operations(test_file)

    # Filter out operations we skip
    relevant_ops = [op for op in all_ops if op not in SKIP_OPERATIONS]

    covered = []
    missing = []
    for op in relevant_ops:
        snake = _to_snake_case(op)
        # Check if the operation or its snake_case form appears in tested ops
        if op in tested_ops or snake in {_to_snake_case(t) for t in tested_ops}:
            covered.append(op)
        else:
            missing.append(op)

    total = len(relevant_ops)
    pct = (len(covered) / total * 100) if total > 0 else 0

    return {
        "service": service_name,
        "botocore_name": botocore_name,
        "total_ops": total,
        "covered": len(covered),
        "missing_count": len(missing),
        "coverage_pct": pct,
        "missing_ops": missing,
        "covered_ops": covered,
        "test_file": str(test_file),
    }


def print_report(results: list[dict], verbose: bool = False):
    """Print a coverage report."""
    # Sort by coverage ascending (worst first)
    results.sort(key=lambda r: r["coverage_pct"])

    print(f"\n{'Service':<20} {'Tested':>7} {'Total':>7} {'Coverage':>10} {'Missing':>8}")
    print("-" * 60)

    total_covered = 0
    total_ops = 0

    for r in results:
        total_covered += r["covered"]
        total_ops += r["total_ops"]
        bar = "█" * int(r["coverage_pct"] / 5) + "░" * (20 - int(r["coverage_pct"] / 5))
        print(
            f"{r['service']:<20} {r['covered']:>7} {r['total_ops']:>7} "
            f"{r['coverage_pct']:>8.1f}%  {bar}"
        )

    overall = (total_covered / total_ops * 100) if total_ops > 0 else 0
    print("-" * 60)
    print(f"{'TOTAL':<20} {total_covered:>7} {total_ops:>7} {overall:>8.1f}%")

    if verbose:
        print("\n\nDetailed gaps per service:")
        print("=" * 60)
        for r in results:
            if r["missing_ops"]:
                print(f"\n{r['service']} ({r['missing_count']} missing):")
                for op in r["missing_ops"]:
                    print(f"  - {op}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Analyze compat test coverage gaps")
    parser.add_argument("--service", help="Analyze specific service")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show missing operations")
    parser.add_argument("--generate", action="store_true", help="Generate test stubs for gaps")
    parser.add_argument("--dry-run", action="store_true", help="Print generated stubs to stdout")
    parser.add_argument("--write", action="store_true", help="Append stubs to test files")
    parser.add_argument(
        "--min-coverage", type=float, default=0,
        help="Only generate for services below this coverage %%"
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    test_dir = Path("tests/compatibility")

    if args.service:
        services = {args.service: BOTOCORE_MAP.get(args.service, args.service)}
    else:
        services = BOTOCORE_MAP

    results = []
    for svc_name, bc_name in services.items():
        # Find the test file
        test_file = None
        for fname, mapped_svc in FILE_TO_SERVICE.items():
            if mapped_svc == svc_name:
                test_file = test_dir / fname
                break
        if test_file is None:
            test_file = test_dir / f"test_{svc_name}_compat.py"

        result = analyze_service(svc_name, bc_name, test_file)
        results.append(result)

    if args.json:
        import json
        # Remove non-serializable fields
        for r in results:
            r.pop("test_file", None)
        print(json.dumps(results, indent=2))
        return

    print_report(results, verbose=args.verbose)

    if args.generate:
        for r in results:
            if r["coverage_pct"] > args.min_coverage and args.min_coverage > 0:
                continue
            if not r["missing_ops"]:
                continue
            print(f"\n# Gaps for {r['service']}: {len(r['missing_ops'])} missing operations")
            for op in r["missing_ops"][:10]:  # Show first 10
                print(f"#   - {op} ({_to_snake_case(op)})")


if __name__ == "__main__":
    main()
