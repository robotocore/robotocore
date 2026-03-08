#!/usr/bin/env python3
"""Project-level static analysis for robotocore.

Catches structural problems that ruff/mypy can't: stale mappings, test quality,
registration mismatches, parallel-unsafe patterns, and protocol drift.

Usage:
    uv run python scripts/lint_project.py          # All checks
    uv run python scripts/lint_project.py --check registry   # Single check
    uv run python scripts/lint_project.py --json   # Machine-readable output
    uv run python scripts/lint_project.py --fail   # Exit 1 if any warnings

Checks:
    1.  test-server-contact    Tests that never contact the server
    2.  test-no-assertion      Tests with no meaningful assertions
    3.  test-hardcoded-names   Hardcoded resource names (parallel-unsafe)
    4.  test-client-mismatch   Test file service != client service
    5.  registry-native-sync   NATIVE registry entries vs NATIVE_PROVIDERS dict
    6.  registry-router-sync   Registered services missing from router
    7.  registry-protocol      Registry protocol vs botocore spec mismatch
    8.  provider-import-sync   Provider files vs app.py imports
    9.  router-stale-ops       Hardcoded operation sets that may be stale
    10. test-duplicate-names   Duplicate test method names within a class
"""

import argparse
import ast
import json
import re
import sys
from pathlib import Path

SRC = Path("src/robotocore")
TESTS = Path("tests/compatibility")
SCRIPTS = Path("scripts")


# ─── Check infrastructure ────────────────────────────────────────────────────


class Finding:
    def __init__(self, check: str, severity: str, file: str, line: int, message: str):
        self.check = check
        self.severity = severity  # "error", "warning", "info"
        self.file = file
        self.line = line
        self.message = message

    def to_dict(self):
        return {
            "check": self.check,
            "severity": self.severity,
            "file": self.file,
            "line": self.line,
            "message": self.message,
        }

    def __str__(self):
        sev = {"error": "ERR ", "warning": "WARN", "info": "INFO"}[self.severity]
        return f"  [{sev}] {self.file}:{self.line}: {self.message}"


def _parse_file(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(), filename=str(path))
    except SyntaxError:
        return None


# ─── Check 1 & 2: Test server contact and assertions ─────────────────────────


def check_test_quality(findings: list[Finding]):
    """Checks 1 & 2: Tests that never contact server or lack assertions."""
    for fpath in sorted(TESTS.glob("test_*_compat.py")):
        tree = _parse_file(fpath)
        if not tree:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
                continue
            _check_single_test(fpath, node, findings)


def _check_single_test(fpath: Path, node: ast.FunctionDef, findings: list[Finding]):
    has_assert = False
    catches_param_validation = False
    all_excepts_pass = True
    has_any_call = False

    for child in ast.walk(node):
        if isinstance(child, (ast.Assert,)):
            has_assert = True
        if isinstance(child, ast.Attribute) and child.attr == "raises":
            has_assert = True
        if isinstance(child, ast.Call):
            has_any_call = True
        if isinstance(child, ast.Try):
            for handler in child.handlers:
                etype = handler.type
                if isinstance(etype, ast.Name) and etype.id == "ParamValidationError":
                    catches_param_validation = True
                elif isinstance(etype, ast.Attribute) and etype.attr == "ParamValidationError":
                    catches_param_validation = True
                for stmt in handler.body:
                    if not isinstance(stmt, (ast.Pass, ast.Expr)):
                        all_excepts_pass = False

    if catches_param_validation and all_excepts_pass and not has_assert:
        findings.append(
            Finding(
                "test-server-contact",
                "error",
                str(fpath),
                node.lineno,
                f"{node.name}: catches ParamValidationError (client-side) — never hits server",
            )
        )
    elif not has_assert and has_any_call:
        findings.append(
            Finding(
                "test-no-assertion",
                "warning",
                str(fpath),
                node.lineno,
                f"{node.name}: no assertions — call without verification",
            )
        )


# ─── Check 3: Hardcoded resource names ───────────────────────────────────────

_CREATE_PATTERN = re.compile(
    r"""(?:create_bucket|create_queue|create_table|create_function|"""
    r"""create_topic|create_stream|create_role|create_user|create_group|"""
    r"""create_stack|create_secret|create_state_machine|"""
    r"""create_log_group|create_repository|create_cluster|create_collection)\s*\("""
)

_HARDCODED_NAME = re.compile(
    r"""(?:(?:Bucket|QueueName|TableName|FunctionName|TopicName|StreamName|"""
    r"""RoleName|UserName|GroupName|StackName|Name|LogGroupName|"""
    r"""RepositoryName|ClusterName|CollectionName)\s*=\s*"[^"]*")"""
)

_DYNAMIC_INDICATORS = re.compile(r"uuid|random|time|unique|f\"|{")


def check_hardcoded_names(findings: list[Finding]):
    """Check 3: Hardcoded resource names in tests (parallel-unsafe)."""
    for fpath in sorted(TESTS.glob("test_*_compat.py")):
        source = fpath.read_text()
        lines = source.split("\n")
        for i, line in enumerate(lines, 1):
            # Skip comments
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # Look for resource creation with hardcoded names
            for match in _HARDCODED_NAME.finditer(line):
                name_str = match.group(0)
                # Check if in a fixture (acceptable if scoped)
                # Check if dynamic
                if _DYNAMIC_INDICATORS.search(line):
                    continue
                # Check if it's in a finally/cleanup block
                if "delete_" in line:
                    continue
                findings.append(
                    Finding(
                        "test-hardcoded-names",
                        "warning",
                        str(fpath),
                        i,
                        f"Hardcoded resource name: {name_str} — may collide in parallel runs",
                    )
                )


# ─── Check 4: Test file service mismatch ─────────────────────────────────────


def check_client_mismatch(findings: list[Finding]):
    """Check 4: Test file implies service X but client is for service Y."""
    for fpath in sorted(TESTS.glob("test_*_compat.py")):
        # Extract expected service from filename
        fname = fpath.stem  # test_iam_compat
        svc_from_file = fname.replace("test_", "").replace("_compat", "")

        source = fpath.read_text()
        # Find all make_client("service") calls
        for match in re.finditer(r'make_client\("([^"]+)"\)', source):
            client_svc = match.group(1)
            # Normalize for comparison
            norm_file = svc_from_file.replace("_", "")
            norm_client = client_svc.replace("-", "").replace("_", "")
            # Allow known mappings
            known_ok = {
                "cognitoidentity": "cognitoidentity",
                "cognito": "cognitoidp",
                "resourcegroups": "resourcegroups",
                "resourcegroupstagging": "resourcegroupstaggingapi",
            }
            if norm_file in known_ok:
                expected = known_ok[norm_file]
                if norm_client == expected:
                    continue
            elif norm_file == norm_client:
                continue
            # Different enough to flag
            line_no = source[: match.start()].count("\n") + 1
            findings.append(
                Finding(
                    "test-client-mismatch",
                    "warning",
                    str(fpath),
                    line_no,
                    f"File implies '{svc_from_file}' but client is '{client_svc}'",
                )
            )


# ─── Check 5: Registry NATIVE vs NATIVE_PROVIDERS ────────────────────────────


def check_registry_native_sync(findings: list[Finding]):
    """Check 5: Services marked NATIVE in registry must be in NATIVE_PROVIDERS."""
    registry_path = SRC / "services" / "registry.py"
    app_path = SRC / "gateway" / "app.py"

    registry_src = registry_path.read_text()
    app_src = app_path.read_text()

    # Extract NATIVE services from registry
    native_in_registry = set()
    for match in re.finditer(r'"([^"]+)":\s*ServiceInfo\([^)]*ServiceStatus\.NATIVE', registry_src):
        native_in_registry.add(match.group(1))

    # Extract NATIVE_PROVIDERS keys from app.py
    native_in_app = set()
    in_dict = False
    for line in app_src.split("\n"):
        if "NATIVE_PROVIDERS" in line and "{" in line:
            in_dict = True
        if in_dict:
            m = re.match(r'\s+"([^"]+)":', line)
            if m:
                native_in_app.add(m.group(1))
            if "}" in line and in_dict and "NATIVE_PROVIDERS" not in line:
                in_dict = False

    # Compare
    missing_from_app = native_in_registry - native_in_app
    extra_in_app = native_in_app - native_in_registry

    for svc in sorted(missing_from_app):
        findings.append(
            Finding(
                "registry-native-sync",
                "error",
                str(app_path),
                0,
                f"'{svc}' is NATIVE in registry but missing from NATIVE_PROVIDERS — "
                f"requests will silently fall back to Moto",
            )
        )
    for svc in sorted(extra_in_app):
        findings.append(
            Finding(
                "registry-native-sync",
                "warning",
                str(registry_path),
                0,
                f"'{svc}' is in NATIVE_PROVIDERS but not marked NATIVE in registry",
            )
        )


# ─── Check 6: Registered services missing from router ────────────────────────


def check_registry_router_sync(findings: list[Finding]):
    """Check 6: Services in registry that have no route path."""
    registry_path = SRC / "services" / "registry.py"
    router_path = SRC / "gateway" / "router.py"

    registry_src = registry_path.read_text()
    router_src = router_path.read_text()

    # All registered service names
    registered = set()
    for match in re.finditer(r'"([^"]+)":\s*ServiceInfo\(', registry_src):
        registered.add(match.group(1))

    # All services mentioned in router (TARGET_PREFIX_MAP values,
    # PATH_PATTERNS values, SERVICE_NAME_ALIASES values, and aliases keys)
    routed = set()
    # TARGET_PREFIX_MAP values
    for match in re.finditer(r'":\s*"([^"]+)"', router_src):
        routed.add(match.group(1))
    # SERVICE_NAME_ALIASES: both keys (credential scope names) map to values
    # The values are what matters — they're the service names requests get routed to
    # PATH_PATTERNS service names
    for match in re.finditer(r'r"[^"]*"\)\s*,\s*"([^"]+)"', router_src):
        routed.add(match.group(1))

    # Services that auth header would match directly (name == credential scope)
    # These don't need explicit router entries
    # Most services use their own name as credential scope, so they route fine

    # Only flag services that use a DIFFERENT credential scope name
    # and aren't covered by SERVICE_NAME_ALIASES
    alias_targets = set()
    for match in re.finditer(r'"([^"]+)":\s*"([^"]+)"', router_src):
        alias_targets.add(match.group(2))

    all_routed = routed | alias_targets

    # Services not explicitly routed — these rely on auth header matching
    # This is fine for most services, but we should flag the info
    unrouted = registered - all_routed
    if unrouted:
        # Only flag as info — auth header fallback works for most
        findings.append(
            Finding(
                "registry-router-sync",
                "info",
                str(router_path),
                0,
                f"{len(unrouted)} services rely on auth-header fallback routing "
                f"(not in TARGET_PREFIX_MAP, PATH_PATTERNS, or aliases): "
                f"{', '.join(sorted(list(unrouted)[:10]))}{'...' if len(unrouted) > 10 else ''}",
            )
        )


# ─── Check 7: Registry protocol vs botocore ──────────────────────────────────

# Map of botocore service name to our registry name (for lookup)
_BOTOCORE_MAP = {
    "acmpca": "acm-pca",
    "applicationautoscaling": "application-autoscaling",
    "cognitoidentity": "cognito-identity",
    "ec2instanceconnect": "ec2-instance-connect",
    "emrcontainers": "emr-containers",
    "emrserverless": "emr-serverless",
    "iotdata": "iot-data",
    "kinesisanalyticsv2": "kinesis-analytics-v2",
    "networkfirewall": "network-firewall",
    "rdsdata": "rds-data",
    "redshiftdata": "redshift-data",
    "ssoadmin": "sso-admin",
    "timestreaminfluxdb": "timestream-influxdb",
    "timestreamquery": "timestream-query",
    "timestreamwrite": "timestream-write",
    "vpclattice": "vpc-lattice",
    "workspacesweb": "workspaces-web",
}


def check_registry_protocol(findings: list[Finding]):
    """Check 7: Registry protocol field vs botocore service JSON spec."""
    try:
        import botocore.session
    except ImportError:
        return

    registry_path = SRC / "services" / "registry.py"
    registry_src = registry_path.read_text()

    session = botocore.session.get_session()

    for match in re.finditer(
        r'"([^"]+)":\s*ServiceInfo\(\s*"[^"]*"\s*,\s*ServiceStatus\.\w+\s*,\s*"([^"]+)"',
        registry_src,
    ):
        svc_name = match.group(1)
        our_protocol = match.group(2)

        # Map to botocore name
        boto_name = _BOTOCORE_MAP.get(svc_name, svc_name)

        try:
            model = session.get_service_model(boto_name)
            actual_protocol = model.protocol
        except Exception:
            continue

        if our_protocol != actual_protocol:
            line_no = registry_src[: match.start()].count("\n") + 1
            findings.append(
                Finding(
                    "registry-protocol",
                    "warning",
                    str(registry_path),
                    line_no,
                    f"'{svc_name}': registry says '{our_protocol}' "
                    f"but botocore says '{actual_protocol}'",
                )
            )


# ─── Check 8: Provider files vs app.py imports ───────────────────────────────


def check_provider_import_sync(findings: list[Finding]):
    """Check 8: Provider .py files that exist but aren't imported in app.py."""
    app_path = SRC / "gateway" / "app.py"
    app_src = app_path.read_text()

    # Find all provider.py files
    provider_files = set()
    for p in SRC.glob("services/*/provider.py"):
        svc_dir = p.parent.name
        provider_files.add(svc_dir)

    # Also check *_provider.py
    for p in SRC.glob("services/*/**/*provider*.py"):
        provider_files.add(p.parent.name if p.parent.name != "services" else p.stem)

    # Find what's imported in app.py
    imported_paths = set()
    for match in re.finditer(r"from robotocore\.services\.([^.]+)\.", app_src):
        imported_paths.add(match.group(1))

    unimported = provider_files - imported_paths
    # Filter out __pycache__ and __init__
    unimported = {p for p in unimported if not p.startswith("_")}

    for svc_dir in sorted(unimported):
        findings.append(
            Finding(
                "provider-import-sync",
                "warning",
                str(app_path),
                0,
                f"Provider exists at services/{svc_dir}/provider.py but not imported in app.py",
            )
        )


# ─── Check 9: Hardcoded operation sets ───────────────────────────────────────


def check_stale_operation_sets(findings: list[Finding]):
    """Check 9: Hardcoded operation sets that may drift from botocore."""
    router_path = SRC / "gateway" / "router.py"
    router_src = router_path.read_text()

    # Find frozenset definitions with operation names
    for match in re.finditer(r"(\w+)\s*=\s*frozenset\(\s*\{([^}]+)\}\s*\)", router_src, re.DOTALL):
        var_name = match.group(1)
        ops_str = match.group(2)
        ops = [s.strip().strip('"').strip("'") for s in ops_str.split(",") if s.strip()]
        ops = [o for o in ops if o]

        line_no = router_src[: match.start()].count("\n") + 1
        findings.append(
            Finding(
                "router-stale-ops",
                "info",
                str(router_path),
                line_no,
                f"{var_name}: {len(ops)} hardcoded operations — "
                f"verify against botocore if AWS adds new ops",
            )
        )


# ─── Check 10: Duplicate test method names ───────────────────────────────────


def check_duplicate_test_names(findings: list[Finding]):
    """Check 10: Duplicate test method names within a class."""
    for fpath in sorted(TESTS.glob("test_*_compat.py")):
        tree = _parse_file(fpath)
        if not tree:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            seen: dict[str, int] = {}
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name.startswith("test_"):
                    if item.name in seen:
                        findings.append(
                            Finding(
                                "test-duplicate-names",
                                "error",
                                str(fpath),
                                item.lineno,
                                f"Duplicate method '{item.name}' in {node.name} "
                                f"(first at line {seen[item.name]})",
                            )
                        )
                    else:
                        seen[item.name] = item.lineno


# ─── Main ────────────────────────────────────────────────────────────────────

ALL_CHECKS = {
    "test-server-contact": check_test_quality,
    "test-no-assertion": check_test_quality,  # same function handles both
    "test-hardcoded-names": check_hardcoded_names,
    "test-client-mismatch": check_client_mismatch,
    "registry-native-sync": check_registry_native_sync,
    "registry-router-sync": check_registry_router_sync,
    "registry-protocol": check_registry_protocol,
    "provider-import-sync": check_provider_import_sync,
    "router-stale-ops": check_stale_operation_sets,
    "test-duplicate-names": check_duplicate_test_names,
}

# Deduplicate functions (test_quality handles two check names)
_UNIQUE_CHECKS = list(dict.fromkeys(ALL_CHECKS.values()))


def main():
    parser = argparse.ArgumentParser(description="Project-level static analysis")
    parser.add_argument("--check", help="Run only this check (by name prefix)")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--fail", action="store_true", help="Exit 1 if any errors found")
    parser.add_argument(
        "--severity",
        choices=["error", "warning", "info"],
        default="warning",
        help="Minimum severity to show (default: warning)",
    )
    args = parser.parse_args()

    findings: list[Finding] = []

    # Run checks
    ran_functions = set()
    for name, func in ALL_CHECKS.items():
        if args.check and not name.startswith(args.check):
            continue
        if func in ran_functions:
            continue
        ran_functions.add(func)
        func(findings)

    # Filter by severity
    sev_order = {"error": 0, "warning": 1, "info": 2}
    min_sev = sev_order[args.severity]
    findings = [f for f in findings if sev_order[f.severity] <= min_sev]

    if args.json:
        by_check: dict[str, list] = {}
        for f in findings:
            by_check.setdefault(f.check, []).append(f.to_dict())
        summary = {
            "total_findings": len(findings),
            "errors": sum(1 for f in findings if f.severity == "error"),
            "warnings": sum(1 for f in findings if f.severity == "warning"),
            "info": sum(1 for f in findings if f.severity == "info"),
            "by_check": {k: len(v) for k, v in by_check.items()},
        }
        print(json.dumps(summary, indent=2))
    else:
        # Group by check
        by_check: dict[str, list[Finding]] = {}
        for f in findings:
            by_check.setdefault(f.check, []).append(f)

        errors = sum(1 for f in findings if f.severity == "error")
        warnings = sum(1 for f in findings if f.severity == "warning")
        infos = sum(1 for f in findings if f.severity == "info")

        print("Robotocore Project Lint")
        print(f"{'=' * 60}")
        print(f"  {errors} errors, {warnings} warnings, {infos} info")
        print()

        for check_name, check_findings in sorted(by_check.items()):
            count = len(check_findings)
            # Show abbreviated output for large groups
            print(f"[{check_name}] ({count} findings)")
            show = check_findings[:5]
            for f in show:
                print(f)
            if count > 5:
                print(f"  ... and {count - 5} more")
            print()

    if args.fail:
        errors = sum(1 for f in findings if f.severity == "error")
        if errors > 0:
            sys.exit(1)


if __name__ == "__main__":
    main()
