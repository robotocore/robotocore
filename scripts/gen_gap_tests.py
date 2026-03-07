#!/usr/bin/env python3
"""Generate test stubs for untested operations and validate they pass.

Unlike gen_compat_tests.py which generates whole files, this tool:
1. Analyzes existing test files to find coverage gaps
2. Generates incremental test stubs for specific missing operations
3. Optionally validates tests pass against a running server

Usage:
    # Show gaps for a service
    uv run python scripts/gen_gap_tests.py sqs

    # Generate tests for top N gaps (dry-run)
    uv run python scripts/gen_gap_tests.py sqs --gen 5

    # Generate and validate against running server
    uv run python scripts/gen_gap_tests.py sqs --gen 5 --validate

    # Generate and append to test file
    uv run python scripts/gen_gap_tests.py sqs --gen 5 --write

    # Batch mode: generate for multiple services
    uv run python scripts/gen_gap_tests.py --batch sqs,sns,s3 --gen 10 --write
"""

import re
import subprocess
import sys
import tempfile
from pathlib import Path

import botocore.session

# Operations that require complex setup or are impractical to test
SKIP_OPS = {
    # EC2 operations that require running instances, VPCs, etc.
    "RunInstances", "TerminateInstances", "StopInstances", "StartInstances",
    "RebootInstances",
    # IAM operations that are org-level
    "CreateServiceLinkedRole", "DeleteServiceLinkedRole",
    "GenerateCredentialReport", "GetCredentialReport",
    "CreateAccountAlias", "DeleteAccountAlias",
    # Operations requiring real infrastructure
    "InvokeAsync",
    # Deprecated
    "GetBucketLifecycle", "PutBucketLifecycle",
}

# Map service → test file
TEST_FILES = {
    "sqs": "tests/compatibility/test_sqs_compat.py",
    "sns": "tests/compatibility/test_sns_compat.py",
    "s3": "tests/compatibility/test_s3_compat.py",
    "dynamodb": "tests/compatibility/test_dynamodb_compat.py",
    "lambda": "tests/compatibility/test_lambda_compat.py",
    "iam": "tests/compatibility/test_iam_compat.py",
    "sts": "tests/compatibility/test_sts_compat.py",
    "events": "tests/compatibility/test_events_compat.py",
    "logs": "tests/compatibility/test_logs_compat.py",
    "cloudwatch": "tests/compatibility/test_cloudwatch_compat.py",
    "kms": "tests/compatibility/test_kms_compat.py",
    "secretsmanager": "tests/compatibility/test_secretsmanager_compat.py",
    "ssm": "tests/compatibility/test_ssm_compat.py",
    "kinesis": "tests/compatibility/test_kinesis_compat.py",
    "firehose": "tests/compatibility/test_firehose_compat.py",
    "stepfunctions": "tests/compatibility/test_stepfunctions_compat.py",
    "cloudformation": "tests/compatibility/test_cloudformation_compat.py",
    "apigateway": "tests/compatibility/test_apigateway_compat.py",
    "ec2": "tests/compatibility/test_ec2_compat.py",
    "route53": "tests/compatibility/test_route53_compat.py",
    "acm": "tests/compatibility/test_acm_compat.py",
    "ses": "tests/compatibility/test_ses_compat.py",
    "scheduler": "tests/compatibility/test_scheduler_compat.py",
}

# Priority operations per service (these are most commonly used)
HIGH_PRIORITY_OPS = {
    "sqs": [
        "PurgeQueue", "SetQueueAttributes", "GetQueueAttributes",
        "ChangeMessageVisibility", "ListDeadLetterSourceQueues",
        "AddPermission", "RemovePermission",
    ],
    "sns": [
        "SetSubscriptionAttributes", "GetSubscriptionAttributes",
        "SetTopicAttributes", "GetTopicAttributes",
        "ConfirmSubscription", "ListSubscriptionsByTopic",
        "SetSMSAttributes", "GetSMSAttributes",
        "PublishBatch",
    ],
    "s3": [
        "CopyObject", "GetObjectAcl", "PutObjectAcl",
        "GetBucketAcl", "PutBucketAcl",
        "GetBucketCors", "PutBucketCors", "DeleteBucketCors",
        "CreateMultipartUpload", "AbortMultipartUpload",
        "GetBucketVersioning", "PutBucketVersioning",
        "GetBucketPolicy", "PutBucketPolicy", "DeleteBucketPolicy",
        "GetBucketWebsite", "PutBucketWebsite", "DeleteBucketWebsite",
        "GetBucketEncryption", "PutBucketEncryption",
        "PutBucketLifecycleConfiguration", "GetBucketLifecycleConfiguration",
        "ListObjectVersions",
    ],
    "dynamodb": [
        "BatchWriteItem", "BatchGetItem", "TransactWriteItems",
        "TransactGetItems", "DescribeTimeToLive", "UpdateTimeToLive",
        "DescribeContinuousBackups", "CreateBackup", "DeleteBackup",
        "ListBackups", "RestoreTableFromBackup",
        "DescribeGlobalTableSettings",
    ],
    "lambda": [
        "GetFunctionConfiguration", "UpdateFunctionConfiguration",
        "UpdateFunctionCode", "ListVersionsByFunction",
        "CreateAlias", "GetAlias", "ListAliases", "DeleteAlias",
        "GetPolicy", "AddPermission", "RemovePermission",
        "PutFunctionConcurrency", "GetFunctionConcurrency",
        "DeleteFunctionConcurrency",
    ],
    "iam": [
        "CreateGroup", "DeleteGroup", "GetGroup", "ListGroups",
        "AddUserToGroup", "RemoveUserFromGroup", "ListGroupsForUser",
        "AttachRolePolicy", "DetachRolePolicy", "ListAttachedRolePolicies",
        "AttachUserPolicy", "DetachUserPolicy", "ListAttachedUserPolicies",
        "CreateInstanceProfile", "DeleteInstanceProfile",
        "AddRoleToInstanceProfile", "RemoveRoleFromInstanceProfile",
        "ListInstanceProfiles",
        "PutRolePolicy", "GetRolePolicy", "DeleteRolePolicy",
        "ListRolePolicies",
        "CreateAccessKey", "DeleteAccessKey", "ListAccessKeys",
        "CreateLoginProfile", "DeleteLoginProfile", "GetLoginProfile",
    ],
    "events": [
        "ListRules", "DescribeEventBus", "PutTargets", "RemoveTargets",
        "ListTargetsByRule", "EnableRule", "DisableRule",
        "CreateEventBus", "DeleteEventBus", "ListEventBuses",
        "CreateArchive", "DeleteArchive", "DescribeArchive",
    ],
    "logs": [
        "PutLogEvents", "GetLogEvents", "FilterLogEvents",
        "DescribeLogStreams", "PutRetentionPolicy", "DeleteRetentionPolicy",
        "PutSubscriptionFilter", "DescribeSubscriptionFilters",
        "DeleteSubscriptionFilter",
        "PutMetricFilter", "DescribeMetricFilters", "DeleteMetricFilter",
    ],
    "kms": [
        "GenerateDataKey", "GenerateDataKeyWithoutPlaintext",
        "ReEncrypt", "GetKeyPolicy", "PutKeyPolicy",
        "GetKeyRotationStatus", "EnableKeyRotation", "DisableKeyRotation",
        "CreateGrant", "ListGrants", "RevokeGrant",
    ],
    "secretsmanager": [
        "UpdateSecret", "PutSecretValue", "GetSecretValue",
        "RotateSecret", "RestoreSecret",
        "UpdateSecretVersionStage",
    ],
}


def _to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_existing_operations(test_file: Path) -> set[str]:
    """Extract boto3 method calls from existing test file."""
    if not test_file.exists():
        return set()
    source = test_file.read_text()
    ops = set()
    # Match any identifier.method_name( — catches all client variable names
    # Filter out common non-boto3 patterns
    skip_methods = {
        "append", "get", "items", "keys", "values", "pop", "update",
        "join", "format", "encode", "decode", "split", "strip",
        "startswith", "endswith", "replace", "lower", "upper",
        "raises", "mark", "fixture", "skip", "parametrize",
        "dumps", "loads", "sleep", "time", "hexdigest",
        "getvalue", "writestr", "read", "write", "close",
        "wait", "result", "exception",
    }
    for match in re.finditer(r"\b\w+\.\s*(\w+)\s*\(", source):
        method = match.group(1)
        if method.startswith("_") or method in skip_methods:
            continue
        # Convert snake_case to PascalCase
        pascal = "".join(word.capitalize() for word in method.split("_"))
        ops.add(pascal)
    return ops


def get_all_operations(service_name: str) -> list[str]:
    """Get botocore operations for a service."""
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(service_name)
        return sorted(model.operation_names)
    except Exception:
        return []


def find_gaps(service: str) -> list[str]:
    """Find untested operations for a service."""
    test_file = Path(TEST_FILES.get(service, f"tests/compatibility/test_{service}_compat.py"))
    existing = get_existing_operations(test_file)
    all_ops = get_all_operations(service)

    gaps = []
    for op in all_ops:
        if op in SKIP_OPS:
            continue
        if op not in existing:
            gaps.append(op)

    # Sort by priority
    priority = HIGH_PRIORITY_OPS.get(service, [])
    priority_set = set(priority)
    high = [op for op in priority if op in gaps]
    rest = [op for op in gaps if op not in priority_set]
    return high + rest


def validate_test(service: str, test_code: str) -> tuple[bool, str]:
    """Write test to temp file and run it against the server."""
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".py", prefix=f"test_{service}_gap_",
        dir="tests/compatibility", delete=False
    ) as f:
        f.write(test_code)
        tmp_path = f.name

    try:
        result = subprocess.run(
            ["uv", "run", "pytest", tmp_path, "-x", "--tb=short", "-q"],
            capture_output=True, text=True, timeout=60,
        )
        return result.returncode == 0, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return False, "Test timed out"
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate gap tests")
    parser.add_argument("service", nargs="?", help="Service name")
    parser.add_argument("--batch", help="Comma-separated services")
    parser.add_argument("--gen", type=int, default=0, help="Generate N test stubs")
    parser.add_argument("--validate", action="store_true", help="Validate generated tests pass")
    parser.add_argument("--write", action="store_true", help="Append to test files")
    parser.add_argument("--dry-run", action="store_true", help="Print to stdout (default)")
    args = parser.parse_args()

    services = []
    if args.batch:
        services = [s.strip() for s in args.batch.split(",")]
    elif args.service:
        services = [args.service]
    else:
        # Show all services
        for svc in sorted(TEST_FILES.keys()):
            gaps = find_gaps(svc)
            total = len(get_all_operations(svc))
            tested = total - len(gaps)
            pct = (tested / total * 100) if total > 0 else 0
            status = "✓" if not gaps else f"{len(gaps)} gaps"
            print(f"  {svc:<20} {tested:>4}/{total:<4} ({pct:5.1f}%)  {status}")
        return

    for service in services:
        gaps = find_gaps(service)
        print(f"\n{service}: {len(gaps)} untested operations")

        if args.gen > 0:
            to_gen = gaps[:args.gen]
            print(f"  Generating tests for: {', '.join(to_gen)}")
            for op in to_gen:
                snake = _to_snake_case(op)
                print(f"    - {op} ({snake})")
        else:
            # Just list gaps
            priority = HIGH_PRIORITY_OPS.get(service, [])
            priority_set = set(priority)
            high = [op for op in gaps if op in priority_set]
            rest = [op for op in gaps if op not in priority_set]
            if high:
                print(f"  HIGH PRIORITY ({len(high)}):")
                for op in high:
                    print(f"    - {op}")
            if rest:
                print(f"  Other ({len(rest)}):")
                for op in rest[:20]:
                    print(f"    - {op}")
                if len(rest) > 20:
                    print(f"    ... and {len(rest) - 20} more")


if __name__ == "__main__":
    main()
