#!/usr/bin/env python3
"""
triage.py - Auto-diagnose failing compat tests.

For each failing test, determines:
- Which AWS operation(s) the test exercises
- What the Python server returns (gold)
- What the Rust server returns (actual)
- The failure class: missing_op | missing_field | value_mismatch | error_mismatch

Output: triage/<service>.json with work orders grouped by operation.

Usage:
    python triage.py --service ssm
    python triage.py --service lambda --test test_function_tags
    python triage.py --all
"""

import argparse
import json
import re
import subprocess
import sys
import os
from pathlib import Path
from typing import Optional

PYV = os.environ.get("PYV", "/Users/jackdanger/www/robotocore/.venv/bin/python")
ROBOTOCORE_DIR = os.environ.get("ROBOTOCORE_DIR", "/Users/jackdanger/www/robotocore")
RUST_DIR = os.environ.get("RUST_DIR", "/Users/jackdanger/www/robotocore-rust")
PY_PORT = os.environ.get("PY_PORT", "4566")
RUST_PORT = os.environ.get("RUST_PORT", "4567")

# Mapping of boto3 client method names to AWS operation names
# These are the common patterns; we extract from test source.
BOTO3_METHOD_TO_OP = {
    # SSM
    "put_parameter": "PutParameter",
    "get_parameter": "GetParameter",
    "delete_parameter": "DeleteParameter",
    "list_parameters": "ListParameters",
    "describe_parameters": "DescribeParameters",
    "add_tags_to_resource": "AddTagsToResource",
    "remove_tags_from_resource": "RemoveTagsFromResource",
    "list_tags_for_resource": "ListTagsForResource",
    "label_parameter_version": "LabelParameterVersion",
    "unlabel_parameter_version": "UnlabelParameterVersion",
    "get_parameters": "GetParameters",
    "get_parameter_history": "GetParameterHistory",
    "create_document": "CreateDocument",
    "get_document": "GetDocument",
    "describe_documents": "DescribeDocuments",
    "delete_document": "DeleteDocument",
    "send_command": "SendCommand",
    "list_commands": "ListCommands",
    "describe_instance_information": "DescribeInstanceInformation",
    "create_maintenance_window": "CreateMaintenanceWindow",
    "get_maintenance_window": "GetMaintenanceWindow",
    "describe_maintenance_windows": "DescribeMaintenanceWindows",
    "delete_maintenance_window": "DeleteMaintenanceWindow",
    "put_compliance_settings": "PutComplianceSettings",
    "get_compliance_settings": "GetComplianceSettings",
    "create_association": "CreateAssociation",
    "describe_associations": "DescribeAssociations",
    "disassociate": "Disassociate",
    "create_opitem": "CreateOPItem",
    "list_opitems": "ListOPItems",
    "get_opitem": "GetOPItem",
    "update_opitem": "UpdateOPItem",
    "create_automation_execution": "CreateAutomationExecution",
    "start_automation_execution": "StartAutomationExecution",
    "describe_automation_executions": "DescribeAutomationExecutions",
    "get_automation_execution": "GetAutomationExecution",
    "describe_maintenance_window_tasks": "DescribeMaintenanceWindowTasks",
    "describe_maintenance_window_registrations": "DescribeMaintenanceWindowRegistrations",
    "describe_maintenance_window_schedules": "DescribeMaintenanceWindowSchedules",
    "describe_activations": "DescribeActivations",
    "deregister_managed_instance": "DeregisterManagedInstance",
    "register_managed_instance": "RegisterManagedInstance",
    "get_default_patch_baseline": "GetDefaultPatchBaseline",
    "get_patch_baseline": "GetPatchBaseline",
    "create_patch_baseline": "CreatePatchBaseline",
    "update_patch_baseline": "UpdatePatchBaseline",
    "delete_patch_baseline": "DeletePatchBaseline",
    "list_patch_baselines": "ListPatchBaselines",
    "describe_instance_patches": "DescribeInstancePatches",
    "get_inventory": "GetInventory",
    "put_inventory": "PutInventory",
    "describe_instance_associations": "DescribeInstanceAssociations",
    "describe_document": "DescribeDocument",
    "get_parameter": "GetParameter",
    "get_parameters_by_path": "GetParametersByPath",
    "list_op_item_events_for_resource": "ListOPItemEventsForResource",
    "update_document_metadata": "UpdateDocumentMetadata",
    "create_document": "CreateDocument",
    "create_quick_setup": "CreateQuickSetup",
    "get_quick_setup": "GetQuickSetup",
    "delete_quick_setup": "DeleteQuickSetup",
    "list_quick_setups": "ListQuickSetups",
    "update_quick_setup": "UpdateQuickSetup",
    "create_default_hybrid_activation": "CreateDefaultHybridActivation",
    "get_default_hybrid_activation": "GetDefaultHybridActivation",
    "delete_default_hybrid_activation": "DeleteDefaultHybridActivation",
    "put_tier_one_recommendations": "PutTierOneRecommendations",
    "get_tier_one_recommendations": "GetTierOneRecommendations",
    "describe_tier_one_recommendations": "DescribeTierOneRecommendations",
}

def extract_ops_from_test(test_code: str) -> list[str]:
    """Extract AWS operation names from test source code."""
    ops = []
    # Find all boto3 client method calls
    # Pattern: client.method_name( or client.method_name(
    for match in re.finditer(r'(?:client|\w+)\.([a-z_]+)\(', test_code):
        method = match.group(1)
        if method in BOTO3_METHOD_TO_OP:
            op = BOTO3_METHOD_TO_OP[method]
            if op not in ops:
                ops.append(op)
        elif method.startswith("get_") or method.startswith("put_") or \
             method.startswith("create_") or method.startswith("delete_") or \
             method.startswith("list_") or method.startswith("describe_") or \
             method.startswith("update_") or method.startswith("tag_") or \
             method.startswith("untag_") or method.startswith("send_") or \
             method.startswith("invoke_") or method.startswith("start_") or \
             method.startswith("stop_") or method.startswith("add_") or \
             method.startswith("remove_"):
            # Convert snake_case to CamelCase
            op = method.replace("_", " ").title().replace(" ", "")
            # Capitalize first letter
            op = op[0].upper() + op[1:]
            if op not in ops:
                ops.append(op)
    return ops


def run_test(test_file: str, test_name: str) -> dict:
    """Run a single test and capture the result."""
    cmd = [
        PYV, "-m", "pytest",
        f"tests/compatibility/{test_file}::{test_name}",
        "-q", "--tb=short", "--no-header",
    ]
    env = os.environ.copy()
    env["ENDPOINT_URL"] = f"http://127.0.0.1:{RUST_PORT}"
    result = subprocess.run(
        cmd, cwd=ROBOTOCORE_DIR,
        capture_output=True, text=True, timeout=60, env=env
    )
    output = (result.stdout + result.stderr).strip()
    last_line = output.split("\n")[-1] if output else ""

    passed = "1 passed" in last_line or output.count("passed") > 0 and "failed" not in last_line
    failed = "1 failed" in last_line or "failed" in last_line

    # Extract the error
    error = ""
    for line in output.split("\n"):
        if "Error" in line or "assert" in line.lower():
            error = line.strip()
            break

    return {
        "test": test_name,
        "file": test_file,
        "passed": passed,
        "failed": failed,
        "error": error,
        "output": output[-500:],
    }


def get_test_files_for_service(service: str) -> list[str]:
    """Get test file(s) for a service."""
    mapping = {
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
    return [mapping.get(service, f"test_{service}_compat.py")]


def get_failing_tests(test_file: str) -> list[str]:
    """Get list of failing test names for a test file."""
    cmd = [
        PYV, "-m", "pytest",
        f"tests/compatibility/{test_file}",
        "-q", "--tb=no", "--no-header", "-x",
    ]
    # Actually, we need to get ALL failing tests, not stop at first
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
    # Extract FAILED test names
    failed_tests = []
    for line in output.split("\n"):
        if line.startswith("FAILED"):
            # Format: FAILED tests/compatibility/test_x.py::TestClass::test_name
            test_id = line.split(" ")[1] if len(line.split(" ")) > 1 else line[7:]
            # Extract test name (after ::)
            parts = test_id.split("::")
            if len(parts) >= 2:
                test_name = "::".join(parts[1:])
                failed_tests.append(test_name)
    return failed_tests


def triage_service(service: str, max_tests: Optional[int] = None) -> dict:
    """Triage all failing tests for a service."""
    test_files = get_test_files_for_service(service)
    all_failing = []

    for tf in test_files:
        tf_path = Path(ROBOTOCORE_DIR) / "tests" / "compatibility" / tf
        if not tf_path.exists():
            continue
        tf_content = tf_path.read_text()

        failing_tests = get_failing_tests(tf)
        if max_tests:
            failing_tests = failing_tests[:max_tests]

        for test_name in failing_tests:
            # Extract ops from test source
            # Find the test method in the source
            test_code = ""
            # Simple extraction: find the method
            for cls_match in re.finditer(r'class\s+\w+.*?:', tf_content):
                pass
            # For now, just extract all ops from the file
            ops = extract_ops_from_test(tf_content)

            all_failing.append({
                "test": test_name,
                "file": tf,
                "ops": ops,
            })

    return {
        "service": service,
        "total_failing": len(all_failing),
        "tests": all_failing,
    }


def main():
    parser = argparse.ArgumentParser(description="Triage failing compat tests")
    parser.add_argument("--service", help="Service to triage")
    parser.add_argument("--all", action="store_true", help="Triage all services")
    parser.add_argument("--max-tests", type=int, default=20, help="Max tests per service")
    parser.add_argument("--output-dir", default=RUST_DIR + "/triage", help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    services = ["ssm", "iam", "lambda", "dynamodb", "s3", "logs", "sns", "events",
                "kinesis", "ecr", "ecs", "cloudwatch", "kms", "sqs", "secretsmanager",
                "sts", "stepfunctions", "firehose"]

    if args.service:
        services = [args.service]

    for svc in services:
        print(f"Triaing {svc}...", file=sys.stderr)
        result = triage_service(svc, max_tests=args.max_tests)
        out_path = Path(args.output_dir) / f"{svc}.json"
        out_path.write_text(json.dumps(result, indent=2))
        print(f"  {result['total_failing']} failing tests -> {out_path}", file=sys.stderr)

    print("Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
