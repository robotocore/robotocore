#!/usr/bin/env python3
"""Auto-response validator: compare Python vs Rust responses field-by-field.

Usage:
    python scripts/harness/auto_validate.py --service ssm --op PutParameter
    python scripts/harness/auto_validate.py --service iam --op ListUsers
    python scripts/harness/auto_validate.py --service ssm --op PutParameter --params '{"Name": "/test", "Value": "v1", "Type": "String"}'
"""

import argparse
import json
import os
import sys
import uuid

import boto3
import botocore
from botocore.config import Config

PY_ENDPOINT = os.environ.get("PY_ENDPOINT", "http://127.0.0.1:4566")
RUST_ENDPOINT = os.environ.get("RUST_ENDPOINT", "http://127.0.0.1:4567")

SERVICE_NAMES = {
    "cloudwatch-logs": "logs",
    "cloudwatch": "cloudwatch",
    "stepfunctions": "stepfunctions",
    "secretsmanager": "secretsmanager",
    "dynamodb": "dynamodb",
}

def op_to_method(op):
    """Convert AWS operation name to boto3 method name."""
    import re
    # PascalCase to snake_case: PutParameter -> put_parameter
    s = re.sub(r'(?<!^)(?=[A-Z])', '_', op)
    return s.lower()

def make_client(service, endpoint):
    svc = SERVICE_NAMES.get(service, service)
    return boto3.client(
        svc,
        endpoint_url=endpoint,
        aws_access_key_id="123456789012",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(retries={"max_attempts": 0}),
    )

def strip_volatile(data):
    """Remove volatile fields from a response."""
    if isinstance(data, dict):
        return {
            k: strip_volatile(v)
            for k, v in data.items()
            if k not in ("ResponseMetadata", "RequestId", "requestId",
                         "CreatedDate", "CreateDate", "LastModifiedDate",
                         "LastModifiedBy", "LastUpdatedDate", "Date")
            and v is not None
        }
    elif isinstance(data, list):
        return [strip_volatile(item) for item in data]
    return data

def diff_responses(py_resp, rust_resp, path=""):
    """Recursively diff two responses."""
    diffs = []

    if type(py_resp) != type(rust_resp):
        diffs.append({
            "path": path or "(root)",
            "type": "type_mismatch",
            "py": type(py_resp).__name__,
            "rust": type(rust_resp).__name__,
            "py_value": repr(py_resp)[:100],
            "rust_value": repr(rust_resp)[:100],
        })
        return diffs

    if isinstance(py_resp, dict):
        py_keys = set(py_resp.keys())
        rust_keys = set(rust_resp.keys())

        missing_in_rust = py_keys - rust_keys
        missing_in_py = rust_keys - py_keys

        for key in sorted(missing_in_rust):
            diffs.append({
                "path": f"{path}.{key}" if path else key,
                "type": "missing_in_rust",
                "py_value": repr(py_resp[key])[:100],
            })

        for key in sorted(missing_in_py):
            diffs.append({
                "path": f"{path}.{key}" if path else key,
                "type": "extra_in_rust",
                "rust_value": repr(rust_resp[key])[:100],
            })

        for key in py_keys & rust_keys:
            diffs.extend(diff_responses(py_resp[key], rust_resp[key],
                                       f"{path}.{key}" if path else key))

    elif isinstance(py_resp, list):
        if len(py_resp) != len(rust_resp):
            diffs.append({
                "path": f"{path} (length)",
                "type": "list_length_mismatch",
                "py": len(py_resp),
                "rust": len(rust_resp),
            })
        for i, (py_item, rust_item) in enumerate(zip(py_resp, rust_resp)):
            diffs.extend(diff_responses(py_item, rust_item, f"{path}[{i}]"))

    else:
        if py_resp != rust_resp:
            diffs.append({
                "path": path or "(root)",
                "type": "value_mismatch",
                "py_value": repr(py_resp)[:100],
                "rust_value": repr(rust_resp)[:100],
            })

    return diffs

def main():
    parser = argparse.ArgumentParser(description="Auto-response validator")
    parser.add_argument("--service", required=True)
    parser.add_argument("--op", required=True)
    parser.add_argument("--params", default="{}", help="JSON params for the call")
    parser.add_argument("--endpoint-py", default=PY_ENDPOINT)
    parser.add_argument("--endpoint-rust", default=RUST_ENDPOINT)
    args = parser.parse_args()

    params = json.loads(args.params)

    py_client = make_client(args.service, args.endpoint_py)
    rust_client = make_client(args.service, args.endpoint_rust)

    # Call the operation on both
    try:
        py_resp = getattr(py_client, op_to_method(args.op))(**params)
        py_status = "ok"
    except Exception as e:
        py_resp = {"error": str(e)}
        py_status = f"error: {e.__class__.__name__}"

    try:
        rust_resp = getattr(rust_client, op_to_method(args.op))(**params)
        rust_status = "ok"
    except Exception as e:
        rust_resp = {"error": str(e)}
        rust_status = f"error: {e.__class__.__name__}"

    print(f"== {args.service}.{args.op} ==")
    print(f"Python: {py_status}")
    print(f"Rust:   {rust_status}")
    print()

    if "error" in py_resp or "error" in rust_resp:
        print("One or both calls failed. Showing raw responses:")
        print(f"  Python: {json.dumps(py_resp, indent=2)[:500]}")
        print(f"  Rust:   {json.dumps(rust_resp, indent=2)[:500]}")
        return

    # Strip volatile fields and diff
    py_clean = strip_volatile(py_resp)
    rust_clean = strip_volatile(rust_resp)

    diffs = diff_responses(py_clean, rust_clean)

    if not diffs:
        print("✓ Responses match (after stripping volatile fields)")
        return

    print(f"✗ {len(diffs)} differences found:")
    print()
    for d in diffs:
        path = d.get("path", "")
        dtype = d.get("type", "")
        if dtype == "missing_in_rust":
            print(f"  MISSING IN RUST: {path}")
            print(f"    Python has: {d.get('py_value', '')}")
        elif dtype == "extra_in_rust":
            print(f"  EXTRA IN RUST: {path}")
            print(f"    Rust has: {d.get('rust_value', '')}")
        elif dtype == "value_mismatch":
            print(f"  VALUE MISMATCH: {path}")
            print(f"    Python: {d.get('py_value', '')}")
            print(f"    Rust:   {d.get('rust_value', '')}")
        elif dtype == "type_mismatch":
            print(f"  TYPE MISMATCH: {path}")
            print(f"    Python: {d.get('py_value', '')} ({d.get('py', '')})")
            print(f"    Rust:   {d.get('rust_value', '')} ({d.get('rust', '')})")
        else:
            print(f"  {dtype}: {path}")
        print()

if __name__ == "__main__":
    main()
