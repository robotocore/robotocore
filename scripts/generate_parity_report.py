#!/usr/bin/env python3
"""Generate a parity report showing test results per service per operation.

Can run the same test suite against robotocore and/or LocalStack to produce
a comparison matrix.

Usage:
    uv run python scripts/generate_parity_report.py
    uv run python scripts/generate_parity_report.py --output parity-report.json
    uv run python scripts/generate_parity_report.py --endpoint http://localhost:4566
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def collect_test_results(endpoint_url: str | None = None) -> dict:
    """Run compatibility tests and collect results per test."""
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/compatibility/",
        "--tb=no", "--no-header",
        "-v",
    ]

    import os
    full_env = dict(os.environ)
    if endpoint_url:
        full_env["ENDPOINT_URL"] = endpoint_url

    result = subprocess.run(cmd, capture_output=True, text=True, env=full_env)

    results = {}
    for line in result.stdout.splitlines():
        if " PASSED" in line or " FAILED" in line or " ERROR" in line:
            parts = line.strip().split(" ")
            test_id = parts[0]
            status = "PASSED" if "PASSED" in line else "FAILED" if "FAILED" in line else "ERROR"

            # Extract service from test file name
            # tests/compatibility/test_s3_compat.py::TestClass::test_method
            if "::" in test_id:
                file_part = test_id.split("::")[0]
                service = file_part.split("test_")[1].split("_compat")[0] if "compat" in file_part else "unknown"
                test_name = test_id.split("::")[-1]
                if service not in results:
                    results[service] = {}
                results[service][test_name] = status

    return results


def generate_report(results: dict, target_name: str = "robotocore") -> dict:
    """Generate a structured parity report."""
    report = {
        "target": target_name,
        "services": {},
        "summary": {
            "total_tests": 0,
            "passed": 0,
            "failed": 0,
            "services_tested": 0,
            "services_fully_passing": 0,
        },
    }

    for service, tests in sorted(results.items()):
        total = len(tests)
        passed = sum(1 for s in tests.values() if s == "PASSED")
        failed = total - passed
        pct = (passed / total * 100) if total else 0

        report["services"][service] = {
            "total": total,
            "passed": passed,
            "failed": failed,
            "percentage": round(pct, 1),
            "tests": tests,
        }

        report["summary"]["total_tests"] += total
        report["summary"]["passed"] += passed
        report["summary"]["failed"] += failed
        report["summary"]["services_tested"] += 1
        if failed == 0:
            report["summary"]["services_fully_passing"] += 1

    total = report["summary"]["total_tests"]
    if total:
        report["summary"]["overall_percentage"] = round(
            report["summary"]["passed"] / total * 100, 1
        )
    else:
        report["summary"]["overall_percentage"] = 0

    return report


def print_report(report: dict) -> None:
    """Print a human-readable parity report."""
    summary = report["summary"]
    print("=" * 70)
    print(f"  Parity Report: {report['target']}")
    print("=" * 70)
    print(f"  Overall: {summary['passed']}/{summary['total_tests']} tests passing ({summary['overall_percentage']}%)")
    print(f"  Services tested: {summary['services_tested']}")
    print(f"  Services 100%: {summary['services_fully_passing']}")
    print()
    print(f"  {'Service':<25} {'Passed':>8} {'Total':>8} {'%':>8}")
    print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*8}")

    for service, data in sorted(report["services"].items()):
        marker = " *" if data["failed"] > 0 else ""
        print(f"  {service:<25} {data['passed']:>8} {data['total']:>8} {data['percentage']:>7.1f}%{marker}")

    if summary["failed"] > 0:
        print()
        print("  Failed tests:")
        for service, data in sorted(report["services"].items()):
            for test, status in sorted(data["tests"].items()):
                if status != "PASSED":
                    print(f"    {service}: {test} [{status}]")
    print()


def main():
    parser = argparse.ArgumentParser(description="Generate parity report")
    parser.add_argument("--endpoint", help="Endpoint URL to test against")
    parser.add_argument("--output", help="Output JSON file")
    parser.add_argument("--name", default="robotocore", help="Target name for report")
    args = parser.parse_args()

    print(f"Running compatibility tests against {args.endpoint or 'default endpoint'}...")
    results = collect_test_results(args.endpoint)

    if not results:
        print("No test results collected. Are the compatibility tests available?")
        sys.exit(1)

    report = generate_report(results, args.name)
    print_report(report)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2)
        print(f"Report written to {args.output}")


if __name__ == "__main__":
    main()
