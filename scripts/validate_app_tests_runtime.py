#!/usr/bin/env python3
"""Runtime validator for app integration tests.

Runs each test against the live robotocore server and checks:
1. The test actually sends requests (audit log count increases)
2. Which AWS services each test exercises
3. How many API calls each test makes
4. No tests silently skip due to missing fixtures

Requires robotocore running on port 4566.

Usage:
    # Validate all app tests
    uv run python scripts/validate_app_tests_runtime.py

    # Single file
    uv run python scripts/validate_app_tests_runtime.py --file tests/apps/test_data_pipeline_app.py

    # Show per-test service breakdown
    uv run python scripts/validate_app_tests_runtime.py --verbose

    # JSON output
    uv run python scripts/validate_app_tests_runtime.py --json
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ENDPOINT_URL = "http://localhost:4566"
AUDIT_URL = f"{ENDPOINT_URL}/_robotocore/audit"
TESTS_DIR = Path("tests/apps")


def server_running() -> bool:
    """Check if robotocore is running."""
    try:
        urllib.request.urlopen(f"{ENDPOINT_URL}/_robotocore/health", timeout=2)
        return True
    except Exception:
        return False


def clear_audit() -> bool:
    """Clear the audit log to get a clean baseline."""
    try:
        req = urllib.request.Request(
            f"{AUDIT_URL}/clear",
            method="POST",
            data=b"",
        )
        urllib.request.urlopen(req, timeout=2)
        return True
    except Exception:
        # Clear endpoint may not exist — fall back to counting
        return False


def get_audit_entries() -> list[dict]:
    """Get all audit log entries."""
    try:
        with urllib.request.urlopen(AUDIT_URL, timeout=5) as resp:
            data = json.loads(resp.read())
            return data.get("entries", [])
    except Exception:
        return []


def get_audit_count() -> int:
    """Get current audit entry count."""
    return len(get_audit_entries())


def collect_test_ids(filepath: str) -> list[str]:
    """Collect all test IDs from a file using pytest --collect-only."""
    result = subprocess.run(
        [
            "uv", "run", "pytest", filepath,
            "--collect-only", "-q", "--no-header",
        ],
        capture_output=True,
        text=True,
        env={
            **dict(__import__("os").environ),
            "AWS_ENDPOINT_URL": ENDPOINT_URL,
        },
    )
    tests = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if "::" in line and not line.startswith("="):
            tests.append(line)
    return tests


def run_single_test(test_id: str) -> dict:
    """Run a single test and measure its server impact."""
    before_count = get_audit_count()
    before_entries = get_audit_entries()
    before_ids = {e.get("id", e.get("timestamp", "")) for e in before_entries}

    result = subprocess.run(
        [
            "uv", "run", "pytest", test_id, "-x", "--no-header", "-q",
            "--tb=short",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        env={
            **dict(__import__("os").environ),
            "AWS_ENDPOINT_URL": ENDPOINT_URL,
        },
    )

    # Small delay for audit log to catch up
    time.sleep(0.1)

    after_entries = get_audit_entries()
    # Find new entries
    new_entries = [
        e for e in after_entries
        if e.get("id", e.get("timestamp", "")) not in before_ids
    ]

    # If we can't track by ID, fall back to count delta
    after_count = len(after_entries)
    api_calls = max(len(new_entries), after_count - before_count)

    # Extract services hit
    services = set()
    operations = []
    for entry in new_entries:
        svc = entry.get("service", "unknown")
        op = entry.get("operation", "unknown")
        services.add(svc)
        operations.append(f"{svc}.{op}")

    passed = "passed" in result.stdout or "1 passed" in result.stdout
    failed = "failed" in result.stdout or "FAILED" in result.stdout
    errored = "error" in result.stdout.lower() and not passed
    status = "passed" if passed else "failed" if failed else "error" if errored else "skipped"

    return {
        "test_id": test_id,
        "status": status,
        "api_calls": api_calls,
        "services": sorted(services),
        "operations": operations,
        "contacted_server": api_calls > 0,
        "stdout": result.stdout[-500:] if result.stdout else "",
        "stderr": result.stderr[-500:] if result.stderr else "",
    }


def analyze_file(filepath: Path, verbose: bool = False) -> dict:
    """Run all tests in a file and analyze server contact."""
    test_ids = collect_test_ids(str(filepath))

    if not test_ids:
        return {
            "file": str(filepath),
            "total_tests": 0,
            "error": "no tests collected",
            "tests": [],
        }

    results = []
    for tid in test_ids:
        if verbose:
            print(f"  Running {tid.split('::')[-1]}...", end=" ", flush=True)
        r = run_single_test(tid)
        results.append(r)
        if verbose:
            emoji = {
                "passed": "OK",
                "failed": "FAIL",
                "error": "ERR",
                "skipped": "SKIP",
            }.get(r["status"], "?")
            svcs = ",".join(r["services"]) if r["services"] else "NONE"
            print(f"{emoji} ({r['api_calls']} calls: {svcs})")

    contacted = [r for r in results if r["contacted_server"]]
    no_contact = [r for r in results if not r["contacted_server"]]
    all_services = set()
    for r in results:
        all_services.update(r["services"])

    return {
        "file": str(filepath),
        "total_tests": len(results),
        "passed": len([r for r in results if r["status"] == "passed"]),
        "failed": len([r for r in results if r["status"] == "failed"]),
        "errored": len([r for r in results if r["status"] == "error"]),
        "skipped": len([r for r in results if r["status"] == "skipped"]),
        "contacted_server": len(contacted),
        "no_contact": len(no_contact),
        "no_contact_tests": [r["test_id"] for r in no_contact],
        "services_exercised": sorted(all_services),
        "total_api_calls": sum(r["api_calls"] for r in results),
        "tests": results,
    }


def print_report(results: list[dict]) -> int:
    """Print human-readable report."""
    total_tests = 0
    total_contacted = 0
    total_no_contact = 0
    all_services = set()
    total_calls = 0

    for r in results:
        total_tests += r["total_tests"]
        total_contacted += r.get("contacted_server", 0)
        total_no_contact += r.get("no_contact", 0)
        all_services.update(r.get("services_exercised", []))
        total_calls += r.get("total_api_calls", 0)

        print(f"\n{'='*60}")
        print(f"{r['file']}")
        print(f"{'='*60}")
        print(
            f"  Tests: {r['total_tests']} | "
            f"Passed: {r.get('passed', 0)} | "
            f"Failed: {r.get('failed', 0)} | "
            f"Errors: {r.get('errored', 0)}"
        )
        print(
            f"  Server contact: {r.get('contacted_server', 0)}/{r['total_tests']} "
            f"({r.get('contacted_server', 0)*100//max(r['total_tests'],1)}%)"
        )
        print(f"  API calls: {r.get('total_api_calls', 0)}")
        print(f"  Services: {', '.join(r.get('services_exercised', []))}")

        if r.get("no_contact_tests"):
            print("\n  NO SERVER CONTACT:")
            for tid in r["no_contact_tests"]:
                print(f"    - {tid}")

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"  Total tests: {total_tests}")
    print(f"  Server contact: {total_contacted}/{total_tests}")
    print(f"  No contact: {total_no_contact}")
    print(f"  Total API calls: {total_calls}")
    print(f"  Services exercised: {', '.join(sorted(all_services))}")

    if total_no_contact > 0:
        print(f"\n  WARNING: {total_no_contact} tests did not contact the server!")
        return 1
    else:
        print("\n  All tests exercise robotocore!")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Runtime validate app integration tests")
    parser.add_argument("--file", help="Check a single file")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--verbose", "-v", action="store_true", help="Per-test details")
    parser.add_argument("--dir", default=str(TESTS_DIR), help="Test directory")
    args = parser.parse_args()

    if not server_running():
        print("ERROR: robotocore is not running on port 4566")
        print("Start it with: make start")
        return 1

    if args.file:
        files = [Path(args.file)]
    else:
        test_dir = Path(args.dir)
        files = sorted(test_dir.glob("test_*.py"))

    if not files:
        print("No test files found.")
        return 0

    results = []
    for f in files:
        print(f"\nValidating {f.name}...")
        r = analyze_file(f, verbose=args.verbose)
        results.append(r)

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    return print_report(results)


if __name__ == "__main__":
    sys.exit(main())
