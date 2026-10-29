#!/usr/bin/env python3
"""
sweep_service.py - Batch fix a service by comparing Python vs Rust responses.

For each failing test in a service:
1. Identify the operation(s) the test exercises
2. Reproduce the test's calls against both Python and Rust
3. Compare responses and identify missing fields
4. Generate a summary of all fixes needed

Usage:
    python sweep_service.py --service ssm
    python sweep_service.py --service ssm --max-tests 10
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
import gzip
from pathlib import Path

import botocore

PYV = os.environ.get("PYV", "/Users/jackdanger/www/robotocore/.venv/bin/python")
ROBOTOCORE_DIR = os.environ.get("ROBOTOCORE_DIR", "/Users/jackdanger/www/robotocore")
RUST_DIR = os.environ.get("RUST_DIR", "/Users/jackdanger/www/robotocore-rust")
PY_PORT = os.environ.get("PY_PORT", "4566")
RUST_PORT = os.environ.get("RUST_PORT", "4567")

TEST_FILES = {
    "ssm": "test_ssm_compat.py",
    "iam": "test_iam_compat.py",
    "lambda": "test_lambda_compat.py",
    "dynamodb": "test_dynamodb_compat.py",
    "s3": "test_s3_compat.py",
    "logs": "test_logs_compat.py",
    "sns": "test_sns_compat.py",
    "events": "test_events_compat.py",
    "kinesis": "test_kinesis_compat.py",
    "ecr": "test_ecr_compat.py",
    "ecs": "test_ecs_compat.py",
    "cloudwatch": "test_cloudwatch_compat.py",
    "kms": "test_kms_compat.py",
    "sqs": "test_sqs_compat.py",
    "secretsmanager": "test_secretsmanager_compat.py",
    "sts": "test_sts_compat.py",
    "stepfunctions": "test_stepfunctions_compat.py",
    "firehose": "test_firehose_compat.py",
}

def get_failing_tests(service):
    """Get list of failing test IDs for a service."""
    test_file = TEST_FILES[service]
    cmd = [
        PYV, "-m", "pytest",
        f"tests/compatibility/{test_file}",
        "-q", "--tb=no", "--no-header",
    ]
    env = os.environ.copy()
    env["ENDPOINT_URL"] = f"http://127.0.0.1:{RUST_PORT}"
    result = subprocess.run(
        cmd, cwd=ROBOTOCORE_DIR,
        capture_output=True, text=True, timeout=300, env=env
    )
    output = result.stdout + result.stderr
    failed = []
    for line in output.split("\n"):
        if line.startswith("FAILED"):
            parts = line.split(" ")
            if len(parts) > 1:
                failed.append(parts[1])
    return failed

def extract_ops_from_test_file(test_file_path, test_name):
    """Extract operations from a specific test method."""
    content = open(test_file_path).read()
    # Find the test method
    # Test name format: TestClass::test_name or TestClass::test_name[param]
    test_method = test_name.split("::")[-1].split("[")[0]

    # Find the method body
    pattern = r'def %s\(.*?:(.*?)(?=\n    def |\n    @|\nclass |\Z)' % re.escape(test_method)
    m = re.search(pattern, content, re.DOTALL)
    if not m:
        return []

    body = m.group(1)
    ops = []
    # Find boto3 method calls
    for match in re.finditer(r'\.(put_|get_|create_|delete_|list_|describe_|update_|tag_|untag_|send_|invoke_|start_|stop_|add_|remove_|label_|unlabel_|put_|set_|register_|deregister_)\w+', body):
        method = match.group(0).lstrip(".")
        # Convert to operation name
        op = ""
        parts = method.split("_")
        for p in parts:
            if p in ("and", "or", "the", "a", "an"):
                continue
            op += p[0].upper() + p[1:]
        if op and op not in ops:
            ops.append(op)
    return ops

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--max-tests", type=int, default=20)
    args = parser.parse_args()

    test_file = TEST_FILES[args.service]
    test_file_path = f"{ROBOTOCORE_DIR}/tests/compatibility/{test_file}"

    print(f"Service: {args.service}")
    print(f"Test file: {test_file}")

    # Get failing tests
    failing = get_failing_tests(args.service)
    print(f"Failing tests: {len(failing)}")

    if not failing:
        print("No failing tests!")
        return

    # Extract ops for each failing test
    op_counts = {}
    test_ops = {}
    for test_id in failing[:args.max_tests]:
        ops = extract_ops_from_test_file(test_file_path, test_id)
        test_ops[test_id] = ops
        for op in ops:
            op_counts[op] = op_counts.get(op, 0) + 1

    print(f"\nOperations by frequency (top 20):")
    for op, count in sorted(op_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"  {op:40} {count}")

    # Save results
    output = {
        "service": args.service,
        "total_failing": len(failing),
        "tests_analyzed": len(failing[:args.max_tests]),
        "test_ops": test_ops,
        "op_counts": op_counts,
    }
    out_path = f"{RUST_DIR}/triage/{args.service}_sweep.json"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    open(out_path, "w").write(json.dumps(output, indent=2))
    print(f"\nSaved to {out_path}")

if __name__ == "__main__":
    main()
