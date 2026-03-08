#!/usr/bin/env python3
"""Runtime test validator — runs tests and checks they actually contact the server.

Uses the /_robotocore/audit endpoint to verify each test generates at least one
server request. This catches tests that only trigger client-side validation.

Usage:
    # Validate a single test file
    uv run python scripts/validate_tests_runtime.py tests/compatibility/test_iam_compat.py

    # Validate all compat tests (slow — runs each test individually)
    uv run python scripts/validate_tests_runtime.py --all --sample 50

    # Check specific class
    uv run python scripts/validate_tests_runtime.py tests/compatibility/test_iam_compat.py \
        -k TestIamAutoCoverage --sample 20
"""

import argparse
import json
import subprocess
import sys
import urllib.request
from pathlib import Path

ENDPOINT_URL = "http://localhost:4566"
AUDIT_URL = f"{ENDPOINT_URL}/_robotocore/audit"


def get_audit_count() -> int:
    """Get current audit entry count."""
    try:
        with urllib.request.urlopen(AUDIT_URL, timeout=2) as resp:
            data = json.loads(resp.read())
            return len(data.get("entries", []))
    except Exception:
        return -1


def get_recent_audit(n: int = 5) -> list[dict]:
    """Get the N most recent audit entries."""
    try:
        with urllib.request.urlopen(AUDIT_URL, timeout=2) as resp:
            data = json.loads(resp.read())
            return data.get("entries", [])[:n]
    except Exception:
        return []


def collect_tests(filepath: str, filter_expr: str | None = None) -> list[str]:
    """Collect test node IDs from a file."""
    cmd = ["uv", "run", "pytest", filepath, "--collect-only", "-q"]
    if filter_expr:
        cmd.extend(["-k", filter_expr])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    tests = []
    for line in result.stdout.split("\n"):
        line = line.strip()
        if "::" in line and not line.startswith("="):
            tests.append(line)
    return tests


def run_single_test(test_id: str) -> tuple[bool, bool, str]:
    """Run a single test and check if it contacted the server.

    Returns: (passed, contacted_server, details)
    """
    before = get_recent_audit(1)
    before_ts = before[0]["timestamp"] if before else 0

    result = subprocess.run(
        ["uv", "run", "pytest", test_id, "-x", "-q", "--tb=no", "--no-header"],
        capture_output=True,
        text=True,
        timeout=30,
        env={
            **__import__("os").environ,
            "ENDPOINT_URL": ENDPOINT_URL,
        },
    )

    passed = result.returncode == 0

    after = get_recent_audit(5)
    new_requests = [e for e in after if e["timestamp"] > before_ts]
    contacted = len(new_requests) > 0

    if new_requests:
        services = {e.get("service", "?") for e in new_requests}
        details = f"{len(new_requests)} requests to {', '.join(services)}"
    else:
        details = "no server requests"

    return passed, contacted, details


def main():
    parser = argparse.ArgumentParser(description="Runtime test quality validator")
    parser.add_argument("file", nargs="?", help="Test file to validate")
    parser.add_argument("--all", action="store_true", help="Validate all compat test files")
    parser.add_argument("-k", dest="filter", help="pytest -k filter expression")
    parser.add_argument(
        "--sample",
        type=int,
        default=0,
        help="Random sample N tests per file (0=all)",
    )
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    # Check server is running
    if get_audit_count() < 0:
        print("Error: server not running or audit endpoint not available")
        sys.exit(1)

    if args.all:
        files = sorted(Path("tests/compatibility").glob("test_*_compat.py"))
    elif args.file:
        files = [Path(args.file)]
    else:
        parser.error("Specify a test file or --all")

    results = []
    total_pass = 0
    total_contact = 0
    total_no_contact = 0
    total_tests = 0

    for filepath in files:
        tests = collect_tests(str(filepath), args.filter)

        if args.sample and len(tests) > args.sample:
            import random

            tests = random.sample(tests, args.sample)

        for test_id in tests:
            passed, contacted, details = run_single_test(test_id)
            total_tests += 1
            if passed:
                total_pass += 1
            if contacted:
                total_contact += 1
            else:
                total_no_contact += 1

            status = "PASS" if passed else "FAIL"
            contact = "SERVER" if contacted else "NO-SERVER"

            entry = {
                "test": test_id,
                "passed": passed,
                "contacted_server": contacted,
                "details": details,
            }
            results.append(entry)

            if not args.json:
                short_id = test_id.split("/")[-1]
                if not contacted:
                    print(f"  [{status}] [{contact}] {short_id}  — {details}")
                elif not passed:
                    print(f"  [{status}] [{contact}] {short_id}")

        if not args.json and tests:
            file_contact = sum(1 for r in results[-len(tests) :] if r["contacted_server"])
            file_total = len(tests)
            fname = filepath.name
            pct = file_contact / file_total * 100 if file_total else 0
            print(f"{fname}: {file_contact}/{file_total} tests contact server ({pct:.0f}%)")
            print()

    if args.json:
        print(
            json.dumps(
                {
                    "total": total_tests,
                    "passed": total_pass,
                    "contacted_server": total_contact,
                    "no_server_contact": total_no_contact,
                    "no_contact_pct": round(
                        total_no_contact / total_tests * 100 if total_tests else 0, 1
                    ),
                    "results": results,
                },
                indent=2,
            )
        )
    else:
        print(f"{'=' * 50}")
        print(f"Total: {total_tests} tests")
        print(f"  Passed: {total_pass}")
        print(f"  Contact server: {total_contact}")
        print(f"  No server contact: {total_no_contact}")
        if total_tests:
            pct = total_no_contact / total_tests * 100
            print(f"  No-contact rate: {pct:.1f}%")

    if total_no_contact > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
