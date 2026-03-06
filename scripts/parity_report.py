#!/usr/bin/env python3
"""Generate parity report showing test coverage per service.

Usage:
    uv run python scripts/parity_report.py
"""

import os
import re
import sys

# All LocalStack community services
LOCALSTACK_SERVICES = sorted([
    "acm", "apigateway", "cloudformation", "cloudwatch", "config",
    "dynamodb", "dynamodbstreams", "ec2", "es", "events", "firehose",
    "iam", "kinesis", "kms", "lambda", "logs", "opensearch", "redshift",
    "resource-groups", "resourcegroupstaggingapi", "route53", "route53resolver",
    "s3", "s3control", "scheduler", "secretsmanager", "ses", "sns", "sqs",
    "ssm", "stepfunctions", "sts", "support", "swf", "transcribe",
])

# Map from test file names to service names
FILE_TO_SERVICE = {
    "test_acm_compat.py": "acm",
    "test_apigateway_compat.py": "apigateway",
    "test_cloudformation_compat.py": "cloudformation",
    "test_cloudwatch_compat.py": "cloudwatch",
    "test_config_compat.py": "config",
    "test_dynamodb_compat.py": "dynamodb",
    "test_ec2_compat.py": "ec2",
    "test_events_compat.py": "events",
    "test_firehose_compat.py": "firehose",
    "test_iam_compat.py": "iam",
    "test_kinesis_compat.py": "kinesis",
    "test_kms_compat.py": "kms",
    "test_lambda_compat.py": "lambda",
    "test_logs_compat.py": "logs",
    "test_opensearch_compat.py": "opensearch",
    "test_redshift_compat.py": "redshift",
    "test_resource_groups_compat.py": "resource-groups",
    "test_resource_groups_tagging_compat.py": "resourcegroupstaggingapi",
    "test_route53_compat.py": "route53",
    "test_s3_compat.py": "s3",
    "test_scheduler_compat.py": "scheduler",
    "test_secretsmanager_compat.py": "secretsmanager",
    "test_ses_compat.py": "ses",
    "test_sns_compat.py": "sns",
    "test_sqs_compat.py": "sqs",
    "test_ssm_compat.py": "ssm",
    "test_stepfunctions_compat.py": "stepfunctions",
    "test_sts_compat.py": "sts",
    "test_support_compat.py": "support",
    "test_swf_compat.py": "swf",
    "test_transcribe_compat.py": "transcribe",
    "test_route53resolver_compat.py": "route53resolver",
    "test_s3control_compat.py": "s3control",
    "test_dynamodbstreams_compat.py": "dynamodbstreams",
    "test_es_compat.py": "es",
}


def count_tests(filepath: str) -> int:
    with open(filepath) as f:
        content = f.read()
    return len(re.findall(r"def test_", content))


def main():
    compat_dir = os.path.join(os.path.dirname(__file__), "..", "tests", "compatibility")
    compat_dir = os.path.abspath(compat_dir)

    unit_dir = os.path.join(os.path.dirname(__file__), "..", "tests", "unit")
    unit_dir = os.path.abspath(unit_dir)

    # Count unit tests
    unit_count = 0
    for root, dirs, files in os.walk(unit_dir):
        for f in files:
            if f.startswith("test_") and f.endswith(".py"):
                unit_count += count_tests(os.path.join(root, f))

    # Count compat tests per service
    service_tests = {}
    total_compat = 0
    for fname, service in FILE_TO_SERVICE.items():
        fpath = os.path.join(compat_dir, fname)
        if os.path.exists(fpath):
            n = count_tests(fpath)
            service_tests[service] = n
            total_compat += n

    # Native providers
    native = {"sqs", "sns", "s3", "firehose", "cloudformation"}

    print("=" * 70)
    print("  ROBOTOCORE PARITY REPORT")
    print("=" * 70)
    print()

    # Services table
    tested = 0
    not_tested = []
    print(f"  {'Service':<30} {'Tests':>6}  {'Provider':>12}")
    print(f"  {'-'*30} {'-'*6}  {'-'*12}")
    for svc in LOCALSTACK_SERVICES:
        n = service_tests.get(svc, 0)
        provider = "native" if svc in native else "moto"
        if n > 0:
            tested += 1
            print(f"  {svc:<30} {n:>6}  {provider:>12}")
        else:
            not_tested.append(svc)
            print(f"  {svc:<30} {'---':>6}  {provider:>12}")

    print()
    print(f"  {'-'*50}")
    print(f"  Services tested:        {tested}/{len(LOCALSTACK_SERVICES)}")
    print(f"  Compatibility tests:    {total_compat}")
    print(f"  Unit tests:             {unit_count}")
    print(f"  Total tests:            {unit_count + total_compat}")
    print(f"  Native providers:       {len(native)} ({', '.join(sorted(native))})")
    print(f"  CloudFormation types:   12 (SQS, SNS, S3, IAM Role/Policy, DynamoDB, Events, KMS, SSM, Logs, Lambda)")

    if not_tested:
        print(f"\n  Services without tests: {', '.join(not_tested)}")

    coverage_pct = tested / len(LOCALSTACK_SERVICES) * 100
    print(f"\n  Parity coverage: {coverage_pct:.0f}%")
    print("=" * 70)


if __name__ == "__main__":
    main()
