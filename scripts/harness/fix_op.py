#!/usr/bin/env python3
"""
fix_op.py - Fix a single operation's response by comparing
Python vs Rust responses.

Usage:
    python fix_op.py --service ssm --op GetParameter --test test_label_and_unlabel
    python fix_op.py --service ssm --op GetParameter
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.request
import gzip

import botocore

PYV = os.environ.get("PYV", "/Users/jackdanger/www/robotocore/.venv/bin/python")
ROBOTOCORE_DIR = os.environ.get("ROBOTOCORE_DIR", "/Users/jackdanger/www/robotocore")
RUST_DIR = os.environ.get("RUST_DIR", "/Users/jackdanger/www/robotocore-rust")
PY_PORT = os.environ.get("PY_PORT", "4566")
RUST_PORT = os.environ.get("RUST_PORT", "4567")

def call_service(endpoint, service, op, params, protocol="json"):
    """Call a service operation and return the response."""
    if protocol == "json":
        url = f"http://127.0.0.1:{endpoint}/"
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "x-amz-target": f"{service.capitalize()}.{op}",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260830/us-east-1/%s/aws4_request" % service,
            "AWS-Access-Key-Id": "123456789012",
        }
        body = json.dumps(params).encode()
    elif protocol == "query":
        # Query protocol: form-encoded
        from urllib.parse import urlencode
        params["Action"] = op
        params["Version"] = "2010-03-31"
        url = f"http://127.0.0.1:{endpoint}/"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260830/us-east-1/%s/aws4_request" % service,
            "AWS-Access-Key-Id": "123456789012",
        }
        body = urlencode(params).encode()
    else:
        # REST protocol: use the path
        url = f"http://127.0.0.1:{endpoint}/"
        headers = {
            "Content-Type": "application/json",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260830/us-east-1/%s/aws4_request" % service,
            "AWS-Access-Key-Id": "123456789012",
        }
        body = json.dumps(params).encode()

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

def get_spec(service):
    data_dir = os.path.join(os.path.dirname(botocore.__file__), "data")
    svc_dir = os.path.join(data_dir, service)
    if not os.path.exists(svc_dir):
        return None
    versions = os.listdir(svc_dir)
    version = sorted(versions)[-1]
    spec_path = os.path.join(svc_dir, version, "service-2.json.gz")
    with gzip.open(spec_path, "rt") as f:
        return json.load(f)

def find_handler_method(service, op):
    """Find the handler method name for an operation."""
    handler_path = f"{RUST_DIR}/crates/{service}/src/handler.rs"
    if not os.path.exists(handler_path):
        return None
    content = open(handler_path).read()
    # Find the match arm for this operation
    pattern = r'"%s"\s*=>\s*self\.(\w+)\(' % op
    m = re.search(pattern, content)
    if m:
        return m.group(1)
    return None

def find_method_body(service, op, method_name):
    """Find the method body in the handler."""
    handler_path = f"{RUST_DIR}/crates/{service}/src/handler.rs"
    content = open(handler_path).read()
    # Find the method
    pattern = r'fn %s\(&self, req: &AwsRequest\) -> AwsResponse \{' % method_name
    m = re.search(pattern, content)
    if not m:
        return None, -1, -1
    start = m.end()
    # Find the matching closing brace
    depth = 1
    i = start
    while i < len(content) and depth > 0:
        if content[i] == '{':
            depth += 1
        elif content[i] == '}':
            depth -= 1
        i += 1
    end = i - 1
    return content[start:end], start, end

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--op", required=True)
    parser.add_argument("--params", default="{}", help="JSON params")
    parser.add_argument("--show", action="store_true", help="Just show the diff")
    parser.add_argument("--fix", action="store_true", help="Apply the fix")
    args = parser.parse_args()

    spec = get_spec(args.service)
    protocol = spec.get("metadata", {}).get("protocol", "json") if spec else "json"

    params = json.loads(args.params)

    # Call both servers
    py_status, py_body = call_service(PY_PORT, args.service, args.op, params, protocol)
    rust_status, rust_body = call_service(RUST_PORT, args.service, args.op, params, protocol)

    print(f"Python: {py_status}")
    print(f"  {py_body[:200]}")
    print(f"Rust: {rust_status}")
    print(f"  {rust_body[:200]}")

    if py_status != rust_status:
        print(f"\nSTATUS MISMATCH: Python={py_status} Rust={rust_status}")
        return

    # Parse both as JSON (if possible)
    try:
        py_data = json.loads(py_body)
        rust_data = json.loads(rust_body)
    except:
        print("\nNot JSON responses - skipping auto-fix")
        return

    # Find missing fields
    missing = {}
    for key, val in py_data.items():
        if key not in rust_data:
            missing[key] = val

    if missing:
        print(f"\nMissing fields in Rust response: {list(missing.keys())}")
        for k, v in missing.items():
            print(f"  {k}: {json.dumps(v)[:100]}")

    # Find value mismatches
    mismatches = {}
    for key in py_data:
        if key in rust_data:
            if py_data[key] != rust_data[key]:
                mismatches[key] = {"py": py_data[key], "rust": rust_data[key]}

    if mismatches:
        print(f"\nValue mismatches: {list(mismatches.keys())}")
        for k, v in mismatches.items():
            print(f"  {k}: Python={json.dumps(v['py'])[:50]} Rust={json.dumps(v['rust'])[:50]}")

    if args.fix:
        # Find and fix the handler method
        method_name = find_handler_method(args.service, args.op)
        if not method_name:
            print(f"\nCould not find handler method for {args.op}")
            return

        body, start, end = find_method_body(args.service, args.op, method_name)
        if body is None:
            print(f"\nCould not find method body for {method_name}")
            return

        # For now, just print what we'd fix
        print(f"\nHandler method: {method_name}")
        print(f"  Lines: {start}-{end}")
        print(f"  Body length: {len(body)}")
        # TODO: auto-patch the response to include missing fields

if __name__ == "__main__":
    main()
