#!/usr/bin/env python3
"""Generate the parity report the coverage page consumes — rust era.

The python product is gone, so this is data-driven now; there is no provider
code to parse. Sources of truth, in order:
  - probes/<service>.json artifacts (scripts/probe_service.py): per-operation
    working / needs_params / not_implemented / 500_error
  - the live twin's /_robotocore/services (registration, protocol, description)
  - botocore service models (required params per operation)
  - tests/compatibility/*.py (operations exercised + test-method counts)

Usage:
    uv run python scripts/generate_parity_report.py
    uv run python scripts/generate_parity_report.py --output site/parity-report.json
    uv run python scripts/generate_parity_report.py --service sqs
    uv run python scripts/generate_parity_report.py --no-color
"""

import argparse
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

COMPAT_TEST_DIR = PROJECT_ROOT / "tests" / "compatibility"
DEFAULT_ENDPOINT = "http://localhost:4566"

# --- Color helpers (kept from the python-era report) ---

_USE_COLOR = True


def _supports_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if not hasattr(sys.stdout, "isatty"):
        return False
    return sys.stdout.isatty()


def _c(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _USE_COLOR else text


def _green(text: str) -> str:
    return _c(text, "32")


def _yellow(text: str) -> str:
    return _c(text, "33")


def _red(text: str) -> str:
    return _c(text, "31")


def _bold(text: str) -> str:
    return _c(text, "1")


def _dim(text: str) -> str:
    return _c(text, "2")


def _pct_color(pct: float) -> str:
    s = f"{pct:.1f}%"
    if pct >= 80:
        return _green(s)
    if pct >= 40:
        return _yellow(s)
    return _red(s)


# --- Live twin ---


def fetch_table(endpoint: str) -> dict[str, dict]:
    """Read /_robotocore/services. Empty table if the twin is unreachable."""
    try:
        with urllib.request.urlopen(
            f"{endpoint.rstrip('/')}/_robotocore/services", timeout=30
        ) as r:
            data = json.load(r)["services"]
        return {entry["name"]: entry for entry in data}
    except Exception:
        return {}


# --- Botocore models ---


def get_botocore_data(service_name: str) -> tuple[list[str], dict[str, list[str]]]:
    """Operation names and required params per operation from botocore."""
    try:
        import botocore.loaders

        model = botocore.loaders.Loader().load_service_model(service_name, "service-2")
    except Exception:
        return [], {}

    op_params: dict[str, list[str]] = {}
    shapes = model.get("shapes", {})
    for op_name, op_data in model.get("operations", {}).items():
        input_ref = op_data.get("input") or {}
        shape = shapes.get(input_ref.get("shape", ""), {})
        op_params[op_name] = shape.get("required", []) or []
    return sorted(model.get("operations", {})), op_params


# --- Compat tests ---


def _extract_boto3_operations(filepath: Path) -> set[str]:
    """Distinct AWS operations called via boto3 in a test file."""
    try:
        content = filepath.read_text()
    except Exception:
        return set()

    filtered = {
        "get", "set", "items", "keys", "values", "append", "extend", "update",
        "format", "join", "split", "strip", "encode", "decode", "read", "write",
        "close", "sleep", "client", "resource", "lower", "upper", "pop", "add",
        "remove", "clear", "sort", "count", "index", "find", "match", "search",
        "sub", "group", "dump", "dumps", "load", "loads", "open", "seek", "tell",
        "flush", "environ", "fixture", "mark", "param", "raises", "skip", "fail",
        "endswith", "startswith", "replace",
    }

    operations = set()
    for m in re.finditer(r"\b\w+\.([a-z][a-z_]+)\(", content):
        method = m.group(1)
        if method in filtered or method.startswith(("assert", "pytest")):
            continue
        pascal = "".join(word.capitalize() for word in method.split("_"))
        if len(pascal) > 3 and pascal[0].isupper():
            operations.add(pascal)
    return operations


def count_test_methods(filepath: Path) -> int:
    try:
        return len(re.findall(r"\bdef test_", filepath.read_text()))
    except Exception:
        return 0


def get_compat_data(service_names: set[str]) -> tuple[dict[str, set[str]], dict[str, int], int]:
    """Map scraped compat-test data onto twin service names.

    Returns (tested_ops, test_method_counts, unmapped_test_methods). Filenames
    are matched by stripping separators (test_cognito_idp_compat.py matches
    "cognito-idp"); ops are attributed via boto3 call scraping, the same
    heuristic the python-era report used.
    """
    if not COMPAT_TEST_DIR.exists():
        return {}, {}, 0

    # Filenames that need a manual target; None = never attribute to a service.
    compat_file_map = {
        "test_apigateway_lambda_compat.py": "apigateway",
        "test_cross_service_compat.py": None,
        "test_cfn_e2e_compat.py": None,
        "test_concurrent_requests_compat.py": None,
    }

    def _norm(name: str) -> str:
        return name.replace("_", "").replace("-", "").lower()

    tested: dict[str, set[str]] = {}
    counts: dict[str, int] = {}
    unmapped_methods = 0

    for fname in sorted(COMPAT_TEST_DIR.glob("test_*.py")):
        n_methods = count_test_methods(fname)
        if fname.name in compat_file_map:
            svc = compat_file_map[fname.name]
        else:
            base = fname.name.replace("test_", "").replace("_compat.py", "")
            svc = next(
                (c for c in service_names if _norm(base) == _norm(c)),
                None,
            )
        if svc is None:
            unmapped_methods += n_methods
            continue
        counts[svc] = counts.get(svc, 0) + n_methods
        ops = _extract_boto3_operations(fname)
        tested.setdefault(svc, set()).update(ops)

    return tested, counts, unmapped_methods


# --- Report ---


def read_probe(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def build_report(probes_dir: Path, endpoint: str) -> dict:
    """Build parity-report data from probe artifacts + the live twin."""
    live = fetch_table(endpoint)
    if not live:
        # Twin may be down; fall back to whatever probe artifacts exist.
        live = {
            p.stem: {}
            for p in sorted(probes_dir.glob("*.json"))
            if read_probe(p) is not None
        }

    tested_ops, test_counts, unmapped_methods = get_compat_data(set(live))

    services: dict[str, dict] = {}
    for svc_name in sorted(live):
        info = live.get(svc_name, {})
        status = info.get("status", "absent")
        protocol = info.get("protocol", "")
        description = info.get("description", "")

        counts = {"working": 0, "needs_params": 0, "not_implemented": 0, "500_error": 0}
        all_ops: list[str] = []
        implemented_ops: list[str] = []
        probe = read_probe(probes_dir / f"{svc_name}.json")
        if probe is not None:
            counts = {
                "working": probe["counts"].get("working", 0),
                "needs_params": probe["counts"].get("needs_params", 0),
                "not_implemented": probe["counts"].get("not_implemented", 0),
                "500_error": probe["counts"].get("500_error", 0),
            }
            all_ops = [op["operation"] for op in probe["operations"]]
            implemented_ops = [
                op["operation"] for op in probe["operations"] if op["status"] == "working"
            ]

        botocore_ops, op_params = get_botocore_data(svc_name)
        botocore_set = set(botocore_ops)
        valid_params = {op: op_params[op] for op in all_ops if op in op_params}

        svc_tested = tested_ops.get(svc_name, set())
        probed_set = set(all_ops)
        # Tested ops only count against probed ops — same denominator as impl_pct.
        valid_tested = sorted(svc_tested & botocore_set & probed_set)

        total = len(all_ops)
        impl = len(implemented_ops)
        tested_count = len(valid_tested)
        # Rust era: implemented ops on a native service are native; there is no
        # moto tier anymore.
        native_ops = sorted(implemented_ops) if status == "native" else []

        services[svc_name] = {
            "status": status,
            "protocol": protocol,
            "description": description,
            "total_aws_ops": total,
            "all_ops": all_ops,
            "op_params": valid_params,
            "implemented_ops": sorted(implemented_ops),
            "implemented_count": impl,
            "native_ops": native_ops,
            "native_count": len(native_ops),
            "moto_ops": [],
            "moto_count": 0,
            "delegates_to_moto": False,
            "tested_ops": valid_tested,
            "tested_count": tested_count,
            "test_method_count": test_counts.get(svc_name, 0),
            "impl_pct": (impl / total * 100 if total else 0),
            "test_pct": (tested_count / total * 100 if total else 0),
            "needs_params_count": counts["needs_params"],
            "not_implemented_count": counts["not_implemented"],
            "error_count": counts["500_error"],
            "probed": probe is not None,
        }

    probed = [s for s in services.values() if s["probed"]]
    total_aws = sum(s["total_aws_ops"] for s in services.values())
    total_impl = sum(s["implemented_count"] for s in services.values())
    total_tested = sum(s["tested_count"] for s in services.values())
    total_tests = sum(s["test_method_count"] for s in services.values())
    native_count = sum(1 for s in services.values() if s["status"] == "native")

    return {
        "services": services,
        "summary": {
            "total_services": len(services),
            "native_services": native_count,
            "absent_services": len(services) - native_count,
            "total_aws_operations": total_aws,
            "total_implemented": total_impl,
            "total_tested": total_tested,
            "total_test_methods": total_tests,
            "unmapped_test_methods": unmapped_methods,
            "impl_pct": total_impl / total_aws * 100 if total_aws else 0,
            "test_pct": total_tested / total_aws * 100 if total_aws else 0,
            "probed_services": len(probed),
            "needs_params_operations": sum(
                s["needs_params_count"] for s in services.values()
            ),
            "not_implemented_operations": sum(
                s["not_implemented_count"] for s in services.values()
            ),
            "error_operations": sum(s["error_count"] for s in services.values()),
        },
        "source": {
            "endpoint": endpoint,
            "probes_dir": str(probes_dir),
            "generated_by": "scripts/generate_parity_report.py",
            "note": (
                "Destructive operations (TerminateInstances, DeleteBucket, DeleteVpc, …) "
                "are excluded from probes — see SKIP_OPERATIONS in scripts/probe_service.py"
            ),
        },
    }


def print_report(report: dict) -> None:
    services = report["services"]
    summary = report["summary"]

    print()
    print(_bold(f"  {'=' * 88}"))
    print(_bold("  ROBOTOCORE AWS PARITY REPORT".center(88)))
    print(_bold(f"  {'=' * 88}"))
    print()
    hdr = (
        f"  {'Service':<26}{'Status':>10}{'AWS Ops':>9}{'Impl':>7}"
        f"{'Impl%':>9}{'NeedsParams':>13}{'501':>6}{'500':>6}{'Tests':>8}"
    )
    print(_bold(hdr))
    sep = (
        f"  {'-' * 26} {'-' * 9} {'-' * 8} {'-' * 6} {'-' * 8}"
        f" {'-' * 12} {'-' * 5} {'-' * 5} {'-' * 7}"
    )
    print(_dim(sep))

    for name, data in services.items():
        status = data["status"]
        status_str = _green(f"{status:>10}") if status == "native" else _red(f"{status:>10}")
        if not data["probed"]:
            print(f"  {name:<26}{status_str}{'--- not probed ---':>30}")
            continue
        print(
            f"  {name:<26}{status_str}{data['total_aws_ops']:>9}"
            f"{data['implemented_count']:>7}"
            f"{_pct_color(data['impl_pct']):>13}"
            f"{data['needs_params_count']:>10}"
            f"{data['not_implemented_count']:>6}"
            f"{data['error_count']:>6}"
            f"{data['test_method_count']:>8}"
        )

    print()
    print(_bold(f"  {'=' * 88}"))
    print(_bold("  SUMMARY"))
    print(_bold(f"  {'=' * 88}"))
    print()
    print(f"  Services registered:     {_bold(str(summary['total_services']))}")
    print(f"    Native providers:      {_green(str(summary['native_services']))}")
    print(f"    Absent:                {_red(str(summary['absent_services']))}")
    print(f"    Probed:                {summary['probed_services']}")
    print()
    print(f"  Operations probed total: {summary['total_aws_operations']}")
    impl = _pct_color(summary["impl_pct"])
    print(f"  Operations working:      {summary['total_implemented']}  ({impl})")
    print(f"  Needs params (unknown):  {summary['needs_params_operations']}")
    print(f"  Not implemented (501):   {summary['not_implemented_operations']}")
    print(f"  500 errors (bugs):       {summary['error_operations']}")
    print(f"  Compat test methods:     {summary['total_test_methods']}")
    print()


def print_service_detail(report: dict, service_name: str) -> None:
    data = report["services"].get(service_name)
    if data is None:
        print(f"  Service '{service_name}' not registered on the twin.")
        return
    print()
    print(_bold(f"  Service: {service_name}"))
    print(f"  Status: {data['status']}  |  Protocol: {data['protocol']}  |  {data['description']}")
    if not data["probed"]:
        print("  No probe artifact — run scripts/probe_service.py first.")
        return
    print(
        f"  {data['implemented_count']}/{data['total_aws_ops']} working "
        f"({_pct_color(data['impl_pct'])}), {data['not_implemented_count']} not implemented, "
        f"{data['needs_params_count']} needs params, {data['error_count']} 500-errors"
    )
    print()
    op_status = _classify_rows(report, service_name)
    for op in data["all_ops"]:
        st = op_status.get(op, "?")
        marker = {
            "working": _green("  ✓ working"),
            "needs_params": _yellow("  ? needs_params"),
            "not_implemented": _red("  · not_impl (501)"),
            "500_error": _red("  ! 500_error"),
        }.get(st, f"  ? {st}")
        params = ", ".join(data["op_params"].get(op, []) or []) or "—"
        print(f"  {op:<46}{marker}  [{params}]")
    print()


def _classify_rows(report: dict, service_name: str) -> dict[str, str]:
    probe = read_probe(PROJECT_ROOT / "probes" / f"{service_name}.json") or {}
    return {op["operation"]: op["status"] for op in probe.get("operations", [])}


def main():
    parser = argparse.ArgumentParser(description="Generate the robotocore parity report")
    parser.add_argument("--json", action="store_true", help="Output as JSON to stdout")
    parser.add_argument("--output", help="Write JSON output to this file path")
    parser.add_argument("--service", help="Show detailed report for a single service")
    parser.add_argument("--no-color", action="store_true", help="Disable color output")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="Twin endpoint URL")
    parser.add_argument(
        "--probes-dir",
        default=str(PROJECT_ROOT / "probes"),
        help="Directory containing probes/<service>.json artifacts",
    )
    args = parser.parse_args()

    global _USE_COLOR
    if args.no_color or not _supports_color():
        _USE_COLOR = False

    if args.service:
        report = build_report(Path(args.probes_dir), args.endpoint)
        print_service_detail(report, args.service)
        return

    report = build_report(Path(args.probes_dir), args.endpoint)

    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, default=str))
        print(f"Parity report written to {args.output}")
    elif args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print_report(report)


if __name__ == "__main__":
    main()
