#!/usr/bin/env python3
"""Generate parity report showing test coverage per service.

Usage:
    uv run python scripts/parity_report.py
"""

import os
import re
import sys

# Add project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def get_registry():
    """Import and return service registry data."""
    from robotocore.services.registry import SERVICE_REGISTRY
    return SERVICE_REGISTRY


def count_tests(filepath: str) -> int:
    with open(filepath) as f:
        content = f.read()
    return len(re.findall(r"def test_", content))


def main():
    registry = get_registry()

    compat_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tests", "compatibility"))
    unit_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "tests", "unit"))

    # Count unit tests
    unit_count = 0
    for root, dirs, files in os.walk(unit_dir):
        for f in files:
            if f.startswith("test_") and f.endswith(".py"):
                unit_count += count_tests(os.path.join(root, f))

    # Count compat tests per service + extra test files
    service_tests = {}
    extra_tests = 0
    total_compat = 0

    for fname in sorted(os.listdir(compat_dir)):
        if not fname.startswith("test_") or not fname.endswith(".py"):
            continue
        fpath = os.path.join(compat_dir, fname)
        n = count_tests(fpath)
        total_compat += n

        # Map filename to service
        # Pattern: test_{service}_compat.py or test_{service}_{extra}_compat.py
        base = fname.replace("test_", "").replace("_compat.py", "").replace(".py", "")

        # Special filename-to-service mappings
        FILENAME_MAP = {
            "resource_groups_tagging": "resourcegroupstaggingapi",
            "resource_groups": "resource-groups",
            "es": "es",
            "opensearch": "opensearch",
            "lambda_event_source": "lambda",
            "apigateway_lambda": "apigateway",
            "cross_service": "_cross_service",
            "cognito": "cognito-idp",
            "state_persistence": "_state_persistence",
        }

        # Try to match to registry service
        matched = False
        if base in FILENAME_MAP:
            svc = FILENAME_MAP[base]
            if svc.startswith("_"):
                extra_tests += n
            else:
                service_tests[svc] = service_tests.get(svc, 0) + n
            matched = True
        else:
            for svc_name in registry:
                clean = svc_name.replace("-", "_")
                if base == clean or base.startswith(clean + "_"):
                    service_tests[svc_name] = service_tests.get(svc_name, 0) + n
                    matched = True
                    break

        if not matched:
            extra_tests += n

    # Collect native services
    native = {k for k, v in registry.items() if v.status.value == "native"}

    print("=" * 72)
    print("  ROBOTOCORE PARITY REPORT")
    print("=" * 72)
    print()

    print(f"  {'Service':<28} {'Status':>12} {'Tests':>6}  {'Protocol':>10}")
    print(f"  {'-'*28} {'-'*12} {'-'*6}  {'-'*10}")

    tested = 0
    not_tested = []
    for svc_name in sorted(registry.keys()):
        info = registry[svc_name]
        n = service_tests.get(svc_name, 0)
        status = info.status.value.replace("_", " ")
        if n > 0:
            tested += 1
            marker = "✓"
        else:
            not_tested.append(svc_name)
            marker = " "
        print(f"  {marker} {svc_name:<26} {status:>11} {n:>6}  {info.protocol:>10}")

    print()
    print(f"  {'-'*60}")
    print(f"  Services with tests:     {tested}/{len(registry)}")
    print(f"  Compatibility tests:     {total_compat}")
    if extra_tests:
        print(f"    (incl. cross-service:  {extra_tests})")
    print(f"  Unit tests:              {unit_count}")
    print(f"  Total tests:             {unit_count + total_compat}")
    print(f"  Native providers:        {len(native)}")

    # List native providers
    print()
    print("  Native providers:")
    for svc in sorted(native):
        desc = registry[svc].description
        n = service_tests.get(svc, 0)
        print(f"    • {svc}: {desc} ({n} tests)")

    if not_tested:
        print(f"\n  Services without tests:  {', '.join(not_tested)}")

    coverage_pct = tested / len(registry) * 100 if registry else 0
    print(f"\n  Service coverage: {coverage_pct:.0f}%")
    print("=" * 72)


if __name__ == "__main__":
    main()
