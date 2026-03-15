#!/usr/bin/env python3
"""Audit native provider fallthrough coverage.

For each native provider with an _ACTION_MAP, identifies which AWS operations
fall through to forward_to_moto() (i.e., not natively implemented) and checks
whether those operations have compat test coverage.

Usage:
    uv run python scripts/audit_fallthrough.py              # all providers
    uv run python scripts/audit_fallthrough.py --service stepfunctions
    uv run python scripts/audit_fallthrough.py --all --json
    uv run python scripts/audit_fallthrough.py --max-uncovered 100  # CI gate
"""

import argparse
import ast
import json
import re
import sys
from pathlib import Path

SRC = Path("src/robotocore/services")
TESTS = Path("tests/compatibility")


def get_action_map_keys(provider_path: Path) -> set[str] | None:
    """Extract keys from _ACTION_MAP in a provider.py. Returns None if no _ACTION_MAP found."""
    source = provider_path.read_text()
    if "_ACTION_MAP" not in source:
        return None

    try:
        tree = ast.parse(source, filename=str(provider_path))
    except SyntaxError:
        return None

    for node in ast.walk(tree):
        # Look for _ACTION_MAP = { ... }
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_ACTION_MAP":
                    if isinstance(node.value, ast.Dict):
                        keys = set()
                        for key in node.value.keys:
                            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                                keys.add(key.value)
                        return keys
    return None


def get_service_name_from_provider(provider_path: Path) -> str | None:
    """Extract the AWS service name used in forward_to_moto calls."""
    source = provider_path.read_text()
    # Look for: forward_to_moto(request, "service_name", ...)
    match = re.search(r'forward_to_moto\s*\(\s*request\s*,\s*"([^"]+)"', source)
    if match:
        return match.group(1)
    # Fallback: use directory name
    return provider_path.parent.name


def get_botocore_operations(service: str) -> list[str]:
    """Get all operation names from botocore for a service."""
    try:
        import botocore.session

        session = botocore.session.get_session()
        # Handle service name variations
        model = session.get_service_model(service)
        return list(model.operation_names)
    except Exception:
        return []


def pascal_to_snake(name: str) -> str:
    """Convert PascalCase to snake_case."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_tested_operations(service: str) -> set[str]:
    """Get set of operation names (PascalCase) that have compat tests for a service."""
    # Map service name to test file
    svc_clean = service.replace("-", "_")
    test_file = TESTS / f"test_{svc_clean}_compat.py"
    if not test_file.exists():
        return set()

    source = test_file.read_text()

    # Find all boto3 method calls: client.method_name(
    # These are snake_case; convert to PascalCase
    tested = set()

    # Get all attribute calls: .<method_name>(
    for match in re.finditer(r"\.([a-z][a-z0-9_]+)\s*\(", source):
        method = match.group(1)
        # Convert snake_case to PascalCase
        pascal = "".join(word.capitalize() for word in method.split("_"))
        tested.add(pascal)

    return tested


def audit_provider(provider_path: Path) -> dict | None:
    """Audit a single provider. Returns audit result dict or None if not applicable."""
    action_map_keys = get_action_map_keys(provider_path)
    if action_map_keys is None:
        return None  # No _ACTION_MAP — not a dispatched provider

    service = get_service_name_from_provider(provider_path)
    if not service:
        return None

    botocore_ops = get_botocore_operations(service)
    if not botocore_ops:
        return None

    # Fallthrough ops = botocore ops not in _ACTION_MAP
    fallthrough_ops = [op for op in botocore_ops if op not in action_map_keys]
    native_ops = [op for op in botocore_ops if op in action_map_keys]

    tested_ops = get_tested_operations(service)

    covered = []
    uncovered = []
    for op in sorted(fallthrough_ops):
        if op in tested_ops:
            covered.append(op)
        else:
            uncovered.append(op)

    return {
        "service": service,
        "provider": str(provider_path),
        "total_botocore_ops": len(botocore_ops),
        "native_ops": len(native_ops),
        "fallthrough_ops": len(fallthrough_ops),
        "covered_fallthroughs": len(covered),
        "uncovered_fallthroughs": len(uncovered),
        "uncovered": uncovered,
        "covered": covered,
    }


def main():
    parser = argparse.ArgumentParser(description="Audit native provider fallthrough coverage")
    parser.add_argument("--service", help="Audit a single service by name")
    parser.add_argument("--all", action="store_true", help="Audit all providers (default)")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument(
        "--max-uncovered",
        type=int,
        default=0,
        help="Fail if uncovered fallthrough count exceeds this (0=no check)",
    )
    parser.add_argument(
        "--uncovered-only", action="store_true", help="Show only uncovered operations"
    )
    args = parser.parse_args()

    # Find providers
    if args.service:
        svc_dir = SRC / args.service.replace("-", "_")
        if not svc_dir.exists():
            svc_dir = SRC / args.service
        provider = svc_dir / "provider.py"
        if not provider.exists():
            print(f"No provider found for service: {args.service}", file=sys.stderr)
            sys.exit(1)
        providers = [provider]
    else:
        providers = sorted(SRC.glob("*/provider.py"))

    results = []
    for provider_path in providers:
        result = audit_provider(provider_path)
        if result:
            results.append(result)

    total_uncovered = sum(r["uncovered_fallthroughs"] for r in results)
    total_covered = sum(r["covered_fallthroughs"] for r in results)
    total_fallthrough = sum(r["fallthrough_ops"] for r in results)

    if args.json:
        output = {
            "total_providers": len(results),
            "total_fallthrough_ops": total_fallthrough,
            "covered_fallthroughs": total_covered,
            "uncovered_fallthroughs": total_uncovered,
            "coverage_pct": (
                round(total_covered / total_fallthrough * 100, 1) if total_fallthrough else 100.0
            ),
            "services": results,
        }
        print(json.dumps(output, indent=2))
    else:
        print("Native Provider Fallthrough Coverage Audit")
        print("=" * 60)
        print(f"Providers audited:    {len(results)}")
        print(f"Total fallthrough ops: {total_fallthrough}")
        print(f"  Covered:            {total_covered}")
        print(f"  Uncovered:          {total_uncovered}")
        if total_fallthrough:
            pct = total_covered / total_fallthrough * 100
            print(f"Fallthrough coverage: {pct:.1f}%")
        print()

        for r in sorted(results, key=lambda x: x["uncovered_fallthroughs"], reverse=True):
            if args.uncovered_only and r["uncovered_fallthroughs"] == 0:
                continue
            c = r["covered_fallthroughs"]
            t = r["fallthrough_ops"]
            if t == 0:
                continue
            pct = c / t * 100 if t else 100
            print(f"  {r['service']:<30s}  {c:>3}/{t:<3} covered  ({pct:.0f}%)")
            if args.uncovered_only and r["uncovered"]:
                for op in r["uncovered"][:10]:
                    print(f"    - {op}")
                if len(r["uncovered"]) > 10:
                    print(f"    ... and {len(r['uncovered']) - 10} more")

    # CI gate
    if args.max_uncovered > 0 and total_uncovered > args.max_uncovered:
        print(
            f"\nFAIL: {total_uncovered} uncovered fallthrough operations "
            f"(threshold: {args.max_uncovered})"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
