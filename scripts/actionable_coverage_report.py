#!/usr/bin/env python3
"""Generate actionable coverage report combining coverage, probe, and quality data.

This script identifies:
1. Implemented but untested operations (ready to test)
2. Tested but weak assertions (needs strengthening)
3. Not implemented operations (needs provider work first)
4. Well-tested operations (no action needed)

Usage:
    # Full report for all services
    uv run python scripts/actionable_coverage_report.py

    # Single service with details
    uv run python scripts/actionable_coverage_report.py --service ec2 -v

    # JSON output
    uv run python scripts/actionable_coverage_report.py --service sqs --json

    # Generate work items
    uv run python scripts/actionable_coverage_report.py --service ec2 --work-items
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str]) -> dict | str:
    """Run a command and return parsed output."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return {} if "--json" in cmd else ""

    if "--json" in cmd:
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {}
    return result.stdout


def get_coverage_data(service: str) -> dict:
    """Get coverage data for a service."""
    result = run_command(
        ["uv", "run", "python", "scripts/compat_coverage.py", "--service", service, "--json"]
    )
    if isinstance(result, list) and result:
        return result[0]
    return {}


def get_probe_data(service: str) -> dict:
    """Get probe data for a service (what's actually implemented)."""
    # Check if server is running
    import urllib.request

    try:
        urllib.request.urlopen("http://localhost:4566/_robotocore/health", timeout=2)
    except Exception:
        print("Warning: Server not running, probe data unavailable", file=sys.stderr)
        return {}

    result = run_command(
        ["uv", "run", "python", "scripts/probe_service.py", "--service", service, "--all", "--json"]
    )
    return result if isinstance(result, dict) else {}


def get_quality_data(test_file: str) -> dict:
    """Get test quality data for a test file."""
    if not Path(test_file).exists():
        return {}

    result = run_command(
        ["uv", "run", "python", "scripts/validate_test_quality.py", "--file", test_file, "--json"]
    )
    return result if isinstance(result, dict) else {}


def analyze_service(service: str, with_probe: bool = False) -> dict:
    """Analyze a service and categorize operations."""
    coverage = get_coverage_data(service)
    if not coverage:
        return {}

    quality = get_quality_data(coverage.get("test_file", ""))
    probe = get_probe_data(service) if with_probe else {}

    # Build operation status map
    missing_ops = set(coverage.get("missing_ops", []))

    # Probe status by operation
    probe_status = {}
    if probe and "operations" in probe:
        for op_info in probe["operations"]:
            probe_status[op_info["operation"]] = op_info["status"]

    # Categorize operations
    analysis = {
        "service": service,
        "total_ops": coverage.get("total_ops", 0),
        "coverage_pct": coverage.get("coverage_pct", 0),
        "test_quality_pct": quality.get("effective_pct", 0) if quality else 0,
        "categories": {},
    }

    # Category 1: Implemented but not tested (HIGH PRIORITY)
    implemented_not_tested = []
    for op in missing_ops:
        status = probe_status.get(op, "unknown")
        if status == "working":
            implemented_not_tested.append({"operation": op, "probe_status": status})

    # Category 2: Tested but weak assertions (MEDIUM PRIORITY)
    tested_weak = []
    if quality and quality.get("total_tests", 0) > 0:
        # We'd need to map tests to operations to get this precisely
        # For now, estimate based on quality percentage
        weak_count = quality.get("no_assertion", 0)
        if weak_count > 0:
            tested_weak.append(
                {"count": weak_count, "note": f"{weak_count} tests have no meaningful assertions"}
            )

    # Category 3: Not implemented (NEEDS PROVIDER WORK)
    not_implemented = []
    for op in missing_ops:
        status = probe_status.get(op, "unknown")
        if status in ("not_implemented", "500_error"):
            not_implemented.append({"operation": op, "probe_status": status})

    # Category 4: Well tested (NO ACTION)
    well_tested = []
    if quality:
        effective_count = quality.get("server_contact_with_assertions", 0)
        well_tested.append(
            {"count": effective_count, "note": f"{effective_count} tests are effective"}
        )

    # Category 5: Unknown (needs probing)
    unknown = []
    if not probe:
        unknown = list(missing_ops)
    else:
        for op in missing_ops:
            if op not in probe_status:
                unknown.append({"operation": op, "probe_status": "not_probed"})

    analysis["categories"] = {
        "implemented_not_tested": implemented_not_tested,
        "tested_weak_assertions": tested_weak,
        "not_implemented": not_implemented,
        "well_tested": well_tested,
        "unknown": unknown,
    }

    return analysis


def print_analysis(analysis: dict, verbose: bool = False):
    """Print analysis in human-readable format."""
    svc = analysis["service"]
    print(f"\n{'=' * 70}")
    print(f"Service: {svc}")
    print(f"{'=' * 70}")
    print(f"Total operations: {analysis['total_ops']}")
    print(f"Coverage: {analysis['coverage_pct']:.1f}%")
    print(f"Test quality: {analysis['test_quality_pct']:.1f}%")
    print()

    cats = analysis["categories"]

    # Priority 1
    impl_not_tested = cats.get("implemented_not_tested", [])
    if impl_not_tested:
        print(f"🔴 HIGH PRIORITY: {len(impl_not_tested)} implemented but untested")
        if verbose:
            for item in impl_not_tested[:10]:
                print(f"   - {item['operation']}")
            if len(impl_not_tested) > 10:
                print(f"   ... and {len(impl_not_tested) - 10} more")
        print()

    # Priority 2
    weak = cats.get("tested_weak_assertions", [])
    if weak and weak[0].get("count", 0) > 0:
        print(f"🟡 MEDIUM PRIORITY: {weak[0]['note']}")
        print()

    # Priority 3
    not_impl = cats.get("not_implemented", [])
    if not_impl:
        print(f"🔵 NEEDS PROVIDER: {len(not_impl)} not implemented")
        if verbose:
            for item in not_impl[:10]:
                print(f"   - {item['operation']} ({item['probe_status']})")
            if len(not_impl) > 10:
                print(f"   ... and {len(not_impl) - 10} more")
        print()

    # Unknown
    unknown = cats.get("unknown", [])
    if unknown:
        if isinstance(unknown[0], dict):
            count = len(unknown)
        else:
            count = len(unknown)
        print(f"⚪ UNKNOWN: {count} operations (run with server to probe)")
        print()

    # Well tested
    well = cats.get("well_tested", [])
    if well and well[0].get("count", 0) > 0:
        print(f"✅ WELL TESTED: {well[0]['note']}")
        print()


def generate_work_items(analysis: dict):
    """Generate actionable work items."""
    svc = analysis["service"]
    cats = analysis["categories"]

    print(f"\n## Work Items for {svc}\n")

    # Item 1: Test implemented operations
    impl_not_tested = cats.get("implemented_not_tested", [])
    if impl_not_tested:
        print(f"### 1. Add tests for {len(impl_not_tested)} implemented operations\n")
        print("```bash")
        print("# Generate tests for operations that work on the server")
        for item in impl_not_tested[:5]:
            print(f"# - {item['operation']}")
        if len(impl_not_tested) > 5:
            print(f"# ... and {len(impl_not_tested) - 5} more")
        print("```\n")

    # Item 2: Strengthen weak tests
    weak = cats.get("tested_weak_assertions", [])
    if weak and weak[0].get("count", 0) > 0:
        print(f"### 2. Add assertions to {weak[0]['count']} weak tests\n")
        print("```bash")
        svc_file = f"tests/compatibility/test_{svc.replace('-', '_')}_compat.py"
        print(f"uv run python scripts/validate_test_quality.py --file {svc_file} --problems-only")
        print("```\n")

    # Item 3: Implement missing operations
    not_impl = cats.get("not_implemented", [])
    if not_impl:
        print(f"### 3. Implement {len(not_impl)} missing operations\n")
        print("```bash")
        print("# These operations return 501 or 500 - need provider work")
        for item in not_impl[:5]:
            print(f"# - {item['operation']}")
        if len(not_impl) > 5:
            print(f"# ... and {len(not_impl) - 5} more")
        print("```\n")


def main():
    parser = argparse.ArgumentParser(description="Generate actionable coverage report")
    parser.add_argument("--service", help="Analyze specific service")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show details")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--work-items", action="store_true", help="Generate work items")
    parser.add_argument(
        "--with-probe",
        action="store_true",
        help="Include probe data (requires server running)",
    )
    args = parser.parse_args()

    if args.service:
        services = [args.service]
    else:
        # Get all services with gaps
        result = run_command(["uv", "run", "python", "scripts/compat_coverage.py", "--json"])
        if isinstance(result, list):
            services = [s["service"] for s in result if s.get("coverage_pct", 100) < 100][
                :10
            ]  # Top 10 with gaps
        else:
            print("Error: Could not get service list", file=sys.stderr)
            sys.exit(1)

    analyses = []
    for svc in services:
        analysis = analyze_service(svc, with_probe=args.with_probe)
        if analysis:
            analyses.append(analysis)

    if args.json:
        print(json.dumps(analyses, indent=2))
    elif args.work_items:
        for analysis in analyses:
            generate_work_items(analysis)
    else:
        for analysis in analyses:
            print_analysis(analysis, verbose=args.verbose)


if __name__ == "__main__":
    main()
