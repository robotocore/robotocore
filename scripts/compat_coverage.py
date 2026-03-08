#!/usr/bin/env python3
"""Analyze compatibility test coverage gaps.

Dynamically discovers all registered services and their compat test files.
Compares botocore operations against existing compat tests to find gaps.

Usage:
    # Show coverage report for all services with gaps
    uv run python scripts/compat_coverage.py

    # Show all services including 100% covered
    uv run python scripts/compat_coverage.py --all

    # Show gaps for specific service
    uv run python scripts/compat_coverage.py --service sqs -v

    # Output as JSON
    uv run python scripts/compat_coverage.py --json
"""

import ast
import importlib
import re
import sys
from pathlib import Path

import botocore.session

# Service name mapping for names that differ between our filenames
# and botocore service names. Most match directly.
SERVICE_NAME_OVERRIDES = {
    "lambda_": "lambda",
    "lambda_event_source": None,  # skip, not a real service
    "apigateway_lambda": None,  # skip, integration test
    "state_persistence": None,  # skip, infra test
}

# Operations that are admin-only, deprecated, or testing-unfriendly
SKIP_OPERATIONS = {
    # Cross-account / org operations
    "AcceptHandshake",
    "CreateOrganization",
    "InviteAccountToOrganization",
    # Deprecated
    "GetBucketLifecycle",
    "PutBucketLifecycle",
    # Dangerous in tests
    "DeleteAccountAlias",
    "DeleteAccount",
}


def _to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def discover_services() -> dict[str, str]:
    """Discover all registered services and map to botocore names.

    Returns {our_name: botocore_name}.
    """
    # Try to import the registry
    try:
        sys.path.insert(0, str(Path("src")))
        mod = importlib.import_module("robotocore.providers.registry")
        registry = getattr(mod, "SERVICE_REGISTRY", {})
        services = {}
        for svc_name, info in registry.items():
            # The registry key is the botocore-compatible service name
            services[svc_name] = svc_name
        return services
    except Exception:
        # Fallback: discover from test files
        return _discover_from_test_files()


def _discover_from_test_files() -> dict[str, str]:
    """Fallback: discover services from test file names."""
    test_dir = Path("tests/compatibility")
    services = {}
    for f in sorted(test_dir.glob("test_*_compat.py")):
        name = f.stem  # test_sqs_compat -> test_sqs_compat
        name = name.removeprefix("test_").removesuffix("_compat")
        if name in SERVICE_NAME_OVERRIDES:
            bc = SERVICE_NAME_OVERRIDES[name]
            if bc is None:
                continue
            services[name] = bc
        else:
            services[name] = name
    return services


def get_botocore_operations(service_name: str) -> list[str]:
    """Get all operation names for a botocore service."""
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(service_name)
        return sorted(model.operation_names)
    except Exception:
        return []


def find_test_file(service_name: str) -> Path:
    """Find the compat test file for a service."""
    test_dir = Path("tests/compatibility")
    # Try direct match first
    direct = test_dir / f"test_{service_name}_compat.py"
    if direct.exists():
        return direct
    # Try with hyphens replaced
    hyphen = test_dir / f"test_{service_name.replace('-', '_')}_compat.py"
    if hyphen.exists():
        return hyphen
    # Try lambda special case
    if service_name == "lambda":
        lam = test_dir / "test_lambda_compat.py"
        if lam.exists():
            return lam
    return direct  # return expected path even if missing


def get_tested_operations(test_file: Path) -> set[str]:
    """Extract AWS operation names tested in a file."""
    if not test_file.exists():
        return set()

    source = test_file.read_text()
    tested = set()

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()

    # Find all method calls on client objects (e.g., s3.list_buckets())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            pascal = "".join(word.capitalize() for word in method_name.split("_"))
            tested.add(pascal)

    # Also scan test names for operation hints
    for match in re.finditer(r"def test_(\w+)", source):
        test_name = match.group(1)
        parts = test_name.split("_")
        for i in range(1, len(parts) + 1):
            candidate = "".join(word.capitalize() for word in parts[:i])
            tested.add(candidate)

    return tested


def analyze_service(service_name: str, botocore_name: str, test_file: Path) -> dict:
    """Analyze coverage for a single service."""
    all_ops = get_botocore_operations(botocore_name)
    if not all_ops:
        return {
            "service": service_name,
            "botocore_name": botocore_name,
            "total_ops": 0,
            "covered": 0,
            "missing_count": 0,
            "coverage_pct": 0,
            "missing_ops": [],
            "covered_ops": [],
            "test_file": str(test_file),
            "has_test_file": test_file.exists(),
        }

    tested_ops = get_tested_operations(test_file)
    relevant_ops = [op for op in all_ops if op not in SKIP_OPERATIONS]

    covered = []
    missing = []
    for op in relevant_ops:
        snake = _to_snake_case(op)
        if op in tested_ops or snake in {_to_snake_case(t) for t in tested_ops}:
            covered.append(op)
        else:
            missing.append(op)

    total = len(relevant_ops)
    pct = (len(covered) / total * 100) if total > 0 else 0

    return {
        "service": service_name,
        "botocore_name": botocore_name,
        "total_ops": total,
        "covered": len(covered),
        "missing_count": len(missing),
        "coverage_pct": pct,
        "missing_ops": missing,
        "covered_ops": covered,
        "test_file": str(test_file),
        "has_test_file": test_file.exists(),
    }


def print_report(results: list[dict], verbose: bool = False, show_all: bool = False):
    """Print a coverage report."""
    results.sort(key=lambda r: r["coverage_pct"])

    # Filter to services with gaps unless --all
    if not show_all:
        results = [r for r in results if r["coverage_pct"] < 100]

    if not results:
        print("All services at 100% coverage!")
        return

    header = f"{'Service':<30} {'Tested':>7} {'Total':>7} {'Coverage':>10}"
    print(f"\n{header}")
    print("-" * 60)

    total_covered = 0
    total_ops = 0

    for r in results:
        total_covered += r["covered"]
        total_ops += r["total_ops"]
        fill = int(r["coverage_pct"] / 5)
        bar = "█" * fill + "░" * (20 - fill)
        print(
            f"{r['service']:<30} {r['covered']:>7} "
            f"{r['total_ops']:>7} {r['coverage_pct']:>8.1f}%  {bar}"
        )

    overall = (total_covered / total_ops * 100) if total_ops > 0 else 0
    print("-" * 60)
    print(f"{'TOTAL':<30} {total_covered:>7} {total_ops:>7} {overall:>8.1f}%")

    if verbose:
        print("\n\nDetailed gaps per service:")
        print("=" * 60)
        for r in results:
            if r["missing_ops"]:
                print(f"\n{r['service']} ({r['missing_count']} missing):")
                for op in r["missing_ops"]:
                    print(f"  - {op}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Analyze compat test coverage gaps")
    parser.add_argument("--service", help="Analyze specific service")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show missing ops")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Show all services including 100%% covered",
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    if args.service:
        services = {args.service: args.service}
    else:
        services = discover_services()

    results = []
    for svc_name, bc_name in sorted(services.items()):
        # Skip non-service test files
        if svc_name in SERVICE_NAME_OVERRIDES:
            override = SERVICE_NAME_OVERRIDES[svc_name]
            if override is None:
                continue
            bc_name = override

        test_file = find_test_file(svc_name)
        result = analyze_service(svc_name, bc_name, test_file)
        # Skip services with 0 botocore ops (invalid service names)
        if result["total_ops"] == 0:
            continue
        results.append(result)

    if args.json:
        import json

        print(json.dumps(results, indent=2))
        return

    print_report(results, verbose=args.verbose, show_all=args.all)


if __name__ == "__main__":
    main()
