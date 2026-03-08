#!/usr/bin/env python3
"""Generate a parity report showing AWS operation coverage per service.

Analyzes three dimensions for each registered service:
1. Total AWS operations (from botocore service specs)
2. Operations implemented (native provider ACTION_MAPs + Moto-backed operations)
3. Operations tested in compatibility tests (by parsing boto3 calls)

Usage:
    uv run python scripts/generate_parity_report.py
    uv run python scripts/generate_parity_report.py --json
    uv run python scripts/generate_parity_report.py --service sqs
    uv run python scripts/generate_parity_report.py --no-color
"""

import ast
import json
import os
import re
import sys
from pathlib import Path

# Add project root so we can import robotocore
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

SRC_SERVICES_DIR = PROJECT_ROOT / "src" / "robotocore" / "services"
COMPAT_TEST_DIR = PROJECT_ROOT / "tests" / "compatibility"

# --- Color helpers ---

_USE_COLOR = True


def _supports_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if not hasattr(sys.stdout, "isatty"):
        return False
    return sys.stdout.isatty()


def _c(text: str, code: str) -> str:
    if not _USE_COLOR:
        return text
    return f"\033[{code}m{text}\033[0m"


def _green(text: str) -> str:
    return _c(text, "32")


def _yellow(text: str) -> str:
    return _c(text, "33")


def _red(text: str) -> str:
    return _c(text, "31")


def _cyan(text: str) -> str:
    return _c(text, "36")


def _bold(text: str) -> str:
    return _c(text, "1")


def _dim(text: str) -> str:
    return _c(text, "2")


def _pct_color(pct: float) -> str:
    """Color a percentage string based on its value."""
    s = f"{pct:.0f}%"
    if pct >= 80:
        return _green(s)
    elif pct >= 40:
        return _yellow(s)
    else:
        return _red(s)


# --- Registry ---


def get_registry() -> dict:
    from robotocore.services.registry import SERVICE_REGISTRY

    return SERVICE_REGISTRY


# --- Botocore operations ---

# Map registry service names to botocore service names where they differ
_BOTOCORE_NAME_MAP = {
    "lambda": "lambda",
    "events": "events",
    "logs": "logs",
    "s3control": "s3control",
    "cognito-idp": "cognito-idp",
    "resource-groups": "resource-groups",
    "resourcegroupstaggingapi": "resourcegroupstaggingapi",
    "es": "es",
    "opensearch": "opensearch",
}


def get_botocore_operations(service_name: str) -> list[str]:
    """Get all operation names from botocore for a service."""
    botocore_name = _BOTOCORE_NAME_MAP.get(service_name, service_name)
    try:
        import botocore.loaders

        loader = botocore.loaders.Loader()
        model = loader.load_service_model(botocore_name, "service-2")
        return sorted(model.get("operations", {}).keys())
    except Exception:
        return []


# --- Native provider operations ---

# Map registry service names to their provider directories
_SERVICE_DIR_MAP = {
    "lambda": "lambda_",
    "cognito-idp": "cognito",
    "resource-groups": None,  # no native provider
    "resourcegroupstaggingapi": None,
}

# Some native providers live in a different directory or use non-standard filenames
_SERVICE_PROVIDER_FILES = {
    "logs": SRC_SERVICES_DIR / "cloudwatch" / "logs_provider.py",
}


def _extract_action_map_keys(filepath: Path) -> list[str]:
    """Extract operation names from _ACTION_MAP or ACTION_MAP dicts in a provider file."""
    try:
        content = filepath.read_text()
    except Exception:
        return []

    operations = []
    # Parse _ACTION_MAP = { "OpName": _handler, ... } using regex
    # This is more robust than AST for dict literals that reference local functions
    in_map = False
    brace_depth = 0
    for line in content.splitlines():
        stripped = line.strip()
        if re.match(r"_?ACTION_MAP\b.*=\s*\{", stripped):
            in_map = True
            brace_depth = stripped.count("{") - stripped.count("}")
            # Check for keys on the same line as the opening
            for m in re.finditer(r'"([A-Z][a-zA-Z]+)"', stripped):
                operations.append(m.group(1))
            continue
        if in_map:
            brace_depth += stripped.count("{") - stripped.count("}")
            for m in re.finditer(r'"([A-Z][a-zA-Z]+)"', stripped):
                operations.append(m.group(1))
            if brace_depth <= 0:
                in_map = False

    return operations


def _extract_rest_route_operations(filepath: Path, service_name: str) -> list[str]:
    """Extract operations from REST-based providers by analyzing route comments and patterns.

    REST providers (S3, Lambda, Scheduler, API Gateway V2, AppSync, Batch) route
    by HTTP method + path rather than using an ACTION_MAP. We look for comments like
    '# GET /functions -- ListFunctions' and method-based routing patterns.
    """
    try:
        content = filepath.read_text()
    except Exception:
        return []

    operations = set()

    # Pattern 1: Comments that reference operation names
    # e.g. "# ListFunctions", "# GET /functions -- ListFunctions"
    for m in re.finditer(
        r"#\s*(?:GET|PUT|POST|DELETE|PATCH|HEAD)?\s*(?:/\S+)?\s*[-—]+\s*([A-Z][a-zA-Z]+)",
        content,
    ):
        operations.add(m.group(1))

    # Pattern 2: Comments with just an operation name on a line
    # e.g. "# CreateFunction" or "#  ListVersionsByFunction"
    for m in re.finditer(r"^\s*#\s+([A-Z][a-z]+[A-Z][a-zA-Z]+)\b", content, re.MULTILINE):
        op = m.group(1)
        # Filter out class/type names that aren't AWS operations
        if (
            not op.endswith("Error")
            and not op.endswith("Exception")
            and not op.endswith("Response")
        ):
            operations.add(op)

    # Pattern 3: Moto backend method calls that map to AWS operations
    # e.g. backend.create_function, backend.list_functions
    for m in re.finditer(r"backend\.([a-z_]+)\(", content):
        method = m.group(1)
        # Convert snake_case to PascalCase
        pascal = "".join(word.capitalize() for word in method.split("_"))
        if len(pascal) > 4 and pascal[0].isupper():
            operations.add(pascal)

    # Pattern 4: forward_to_moto calls (wraps all Moto operations)
    if "forward_to_moto" in content:
        # This service delegates to Moto -- we'll count Moto's operations separately
        pass

    # Pattern 5: Handler function names (def _create_api, def _get_routes, etc.)
    # These are the implementation functions in REST-based providers
    aws_verbs = {
        "create",
        "get",
        "put",
        "delete",
        "update",
        "list",
        "describe",
        "register",
        "deregister",
        "submit",
        "cancel",
        "terminate",
        "start",
        "stop",
        "run",
        "tag",
        "untag",
        "publish",
        "invoke",
        "send",
        "receive",
        "admin",
        "initiate",
        "respond",
        "confirm",
        "forgot",
        "change",
        "sign",
        "set",
        "add",
        "remove",
        "batch",
        "associate",
        "disassociate",
        "enable",
        "disable",
        "schedule",
        "query",
        "scan",
        "execute",
    }
    for m in re.finditer(r"^def _([a-z][a-z_]+)\(", content, re.MULTILINE):
        func_name = m.group(1)
        parts = func_name.split("_")
        # Only include if starts with an AWS-like verb
        if parts[0] in aws_verbs and len(parts) >= 2:
            pascal = "".join(word.capitalize() for word in parts)
            operations.add(pascal)

    return sorted(operations)


def get_native_operations(service_name: str) -> tuple[list[str], bool]:
    """Get implemented operations from a native provider.

    Returns (operations_list, delegates_to_moto).
    """
    # Check for non-standard provider file locations first
    if service_name in _SERVICE_PROVIDER_FILES:
        provider_path = _SERVICE_PROVIDER_FILES[service_name]
        if not provider_path.exists():
            return [], False
    else:
        dir_name = _SERVICE_DIR_MAP.get(service_name, service_name)
        if dir_name is None:
            return [], False
        provider_path = SRC_SERVICES_DIR / dir_name / "provider.py"
        if not provider_path.exists():
            return [], False

    # Check if this provider delegates to Moto
    try:
        content = provider_path.read_text()
    except Exception:
        return [], False

    delegates_to_moto = "forward_to_moto" in content

    # Try ACTION_MAP first (most providers)
    ops = _extract_action_map_keys(provider_path)
    if ops:
        return ops, delegates_to_moto

    # Try REST route extraction
    ops = _extract_rest_route_operations(provider_path, service_name)
    if ops:
        return ops, delegates_to_moto

    return [], delegates_to_moto


def get_moto_operations(service_name: str) -> list[str]:
    """Get operations that Moto implements for a service by checking its responses module."""
    # Map to Moto's internal service name
    moto_name_map = {
        "lambda": "awslambda",
        "logs": "logs",
        "events": "events",
        "cognito-idp": "cognitoidp",
        "resource-groups": "resourcegroups",
        "resourcegroupstaggingapi": "resourcegroupstaggingapi",
        "s3control": "s3control",
        "es": "es",
        "opensearch": "opensearch",
        "apigatewayv2": "apigatewayv2",
    }
    moto_name = moto_name_map.get(service_name, service_name)

    # Try to find dispatch methods in Moto's responses.py
    vendor_responses = PROJECT_ROOT / "vendor" / "moto" / "moto" / moto_name / "responses.py"
    if not vendor_responses.exists():
        # Some services use different directory names
        for alt in [moto_name.replace("-", ""), moto_name.replace("-", "_")]:
            alt_path = PROJECT_ROOT / "vendor" / "moto" / "moto" / alt / "responses.py"
            if alt_path.exists():
                vendor_responses = alt_path
                break

    # Collect all response files to scan
    response_files = []
    if vendor_responses.exists():
        response_files.append(vendor_responses)
    else:
        # Check for responses/ directory (e.g., EC2 splits across many files)
        responses_dir = vendor_responses.parent / "responses"
        if responses_dir.is_dir():
            response_files.extend(responses_dir.glob("*.py"))

    if not response_files:
        # Fallback: scan models.py for public methods on Backend classes
        moto_dir = PROJECT_ROOT / "vendor" / "moto" / "moto" / moto_name
        models_path = moto_dir / "models.py"
        if models_path.exists():
            response_files.append(models_path)

    if not response_files:
        return []

    operations = set()

    for resp_file in response_files:
        try:
            content = resp_file.read_text()
        except Exception:
            continue

        try:
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    for item in node.body:
                        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            name = item.name
                            if name.startswith("_") or name in (
                                "setup_class",
                                "call_action",
                                "dispatch",
                                "tags",
                            ):
                                continue
                            # Convert snake_case to PascalCase for AWS operation name
                            pascal = "".join(word.capitalize() for word in name.split("_"))
                            if len(pascal) > 3:
                                operations.add(pascal)
        except SyntaxError:
            pass

    return sorted(operations)


# --- Compat test operations ---

# Map compat test filenames to service names
_COMPAT_FILE_MAP = {
    "test_resource_groups_tagging_compat.py": "resourcegroupstaggingapi",
    "test_resource_groups_compat.py": "resource-groups",
    "test_lambda_event_source_compat.py": "lambda",
    "test_apigateway_lambda_compat.py": "apigateway",
    "test_cross_service_compat.py": None,  # skip
    "test_state_persistence_compat.py": None,  # skip
    "test_cognito_compat.py": "cognito-idp",
    "test_es_compat.py": "es",
    "test_opensearch_compat.py": "opensearch",
}


def _extract_boto3_operations(filepath: Path) -> set[str]:
    """Extract distinct AWS operations called via boto3 in a test file.

    Looks for patterns like:
        client.create_queue(...)
        sqs.send_message(...)
        s3.put_object(...)
    and converts snake_case method names to PascalCase operation names.
    """
    try:
        content = filepath.read_text()
    except Exception:
        return set()

    operations = set()

    # Find all boto3 client method calls
    # Matches: <var>.<method_name>( where method_name is snake_case
    for m in re.finditer(r"\b\w+\.([a-z][a-z_]+)\(", content):
        method = m.group(1)
        # Filter out common non-AWS methods
        if method in (
            "get",
            "set",
            "items",
            "keys",
            "values",
            "append",
            "extend",
            "update",
            "format",
            "join",
            "split",
            "strip",
            "encode",
            "decode",
            "read",
            "write",
            "close",
            "sleep",
            "client",
            "resource",
            "assert_called",
            "assert_called_once",
            "assert_not_called",
            "assert_called_with",
            "assert_called_once_with",
            "start_with",
            "startswith",
            "endswith",
            "replace",
            "lower",
            "upper",
            "pop",
            "add",
            "remove",
            "clear",
            "copy",
            "sort",
            "reverse",
            "count",
            "index",
            "find",
            "match",
            "search",
            "sub",
            "group",
            "dump",
            "dumps",
            "load",
            "loads",
            "open",
            "seek",
            "tell",
            "flush",
        ):
            continue
        # Filter out pytest/test framework methods
        if method.startswith("assert") or method.startswith("pytest"):
            continue
        # Filter common fixture/helper patterns
        if method in ("fixture", "mark", "param", "raises", "skip", "fail", "environ"):
            continue

        # Convert to PascalCase
        pascal = "".join(word.capitalize() for word in method.split("_"))
        if len(pascal) > 3 and pascal[0].isupper():
            operations.add(pascal)

    return operations


def get_tested_operations() -> dict[str, set[str]]:
    """Get operations tested in compat tests, keyed by service name."""
    if not COMPAT_TEST_DIR.exists():
        return {}

    result: dict[str, set[str]] = {}

    for fname in sorted(os.listdir(COMPAT_TEST_DIR)):
        if not fname.startswith("test_") or not fname.endswith(".py"):
            continue

        fpath = COMPAT_TEST_DIR / fname

        # Determine service name
        if fname in _COMPAT_FILE_MAP:
            svc = _COMPAT_FILE_MAP[fname]
            if svc is None:
                continue
        else:
            # Derive from filename: test_{service}_compat.py
            base = fname.replace("test_", "").replace("_compat.py", "").replace(".py", "")
            svc = base

        ops = _extract_boto3_operations(fpath)
        if svc in result:
            result[svc] |= ops
        else:
            result[svc] = ops

    return result


def count_test_methods(filepath: Path) -> int:
    """Count test methods in a file."""
    try:
        content = filepath.read_text()
    except Exception:
        return 0
    return len(re.findall(r"\bdef test_", content))


def get_compat_test_counts() -> dict[str, int]:
    """Count test methods per service in compat tests."""
    if not COMPAT_TEST_DIR.exists():
        return {}

    result: dict[str, int] = {}
    registry = get_registry()

    for fname in sorted(os.listdir(COMPAT_TEST_DIR)):
        if not fname.startswith("test_") or not fname.endswith(".py"):
            continue

        fpath = COMPAT_TEST_DIR / fname
        n = count_test_methods(fpath)

        if fname in _COMPAT_FILE_MAP:
            svc = _COMPAT_FILE_MAP[fname]
            if svc is None:
                continue
        else:
            base = fname.replace("test_", "").replace("_compat.py", "").replace(".py", "")
            # Try to match to registry
            svc = None
            for svc_name in registry:
                clean = svc_name.replace("-", "_")
                if base == clean or base.startswith(clean + "_"):
                    svc = svc_name
                    break
            if svc is None:
                svc = base

        result[svc] = result.get(svc, 0) + n

    return result


# --- Report generation ---


def build_report(filter_service: str | None = None) -> dict:
    """Build the full parity report data structure."""
    registry = get_registry()
    tested_ops = get_tested_operations()
    test_counts = get_compat_test_counts()

    services = {}

    for svc_name, svc_info in sorted(registry.items()):
        if filter_service and svc_name != filter_service:
            continue

        # 1. Total AWS operations from botocore
        botocore_ops = get_botocore_operations(svc_name)

        # 2. Native provider operations
        native_ops, delegates_to_moto = get_native_operations(svc_name)

        # 3. Moto operations (for moto-backed or moto-delegating services)
        moto_ops = []
        if svc_info.status.value in ("moto_backed",) or delegates_to_moto:
            moto_ops = get_moto_operations(svc_name)

        # Combine implemented operations (native + moto, deduplicated)
        all_implemented = set(native_ops) | set(moto_ops)

        # Intersect with botocore operations to get valid ones
        botocore_set = set(botocore_ops)
        valid_implemented = all_implemented & botocore_set if botocore_set else all_implemented

        # 4. Tested operations from compat tests
        svc_tested = tested_ops.get(svc_name, set())
        valid_tested = svc_tested & botocore_set if botocore_set else svc_tested

        services[svc_name] = {
            "status": svc_info.status.value,
            "protocol": svc_info.protocol,
            "description": svc_info.description,
            "total_aws_ops": len(botocore_ops),
            "implemented_ops": sorted(valid_implemented),
            "implemented_count": len(valid_implemented),
            "native_ops": sorted(set(native_ops) & botocore_set) if botocore_set else native_ops,
            "native_count": (
                len(set(native_ops) & botocore_set) if botocore_set else len(native_ops)
            ),
            "moto_ops": sorted(set(moto_ops) & botocore_set) if botocore_set else moto_ops,
            "moto_count": len(set(moto_ops) & botocore_set) if botocore_set else len(moto_ops),
            "tested_ops": sorted(valid_tested),
            "tested_count": len(valid_tested),
            "test_method_count": test_counts.get(svc_name, 0),
            "impl_pct": (len(valid_implemented) / len(botocore_ops) * 100 if botocore_ops else 0),
            "test_pct": (len(valid_tested) / len(botocore_ops) * 100 if botocore_ops else 0),
            "delegates_to_moto": delegates_to_moto,
        }

    # Summary
    total_aws = sum(s["total_aws_ops"] for s in services.values())
    total_impl = sum(s["implemented_count"] for s in services.values())
    total_tested = sum(s["tested_count"] for s in services.values())
    total_tests = sum(s["test_method_count"] for s in services.values())
    native_count = sum(1 for s in services.values() if s["status"] == "native")
    moto_count = sum(1 for s in services.values() if s["status"] == "moto_backed")

    return {
        "services": services,
        "summary": {
            "total_services": len(services),
            "native_services": native_count,
            "moto_backed_services": moto_count,
            "total_aws_operations": total_aws,
            "total_implemented": total_impl,
            "total_tested": total_tested,
            "total_test_methods": total_tests,
            "impl_pct": total_impl / total_aws * 100 if total_aws else 0,
            "test_pct": total_tested / total_aws * 100 if total_aws else 0,
        },
    }


def print_report(report: dict) -> None:
    """Print a formatted, terminal-friendly parity report."""
    services = report["services"]
    summary = report["summary"]

    header = "ROBOTOCORE AWS PARITY REPORT"
    print()
    print(_bold(f"  {'=' * 88}"))
    print(_bold(f"  {header:^88}"))
    print(_bold(f"  {'=' * 88}"))
    print()

    # Column headers
    hdr = (
        f"  {'Service':<26}"
        f"{'Status':>11}"
        f"{'AWS Ops':>9}"
        f"{'Impl':>7}"
        f"{'Impl%':>8}"
        f"{'Tested':>8}"
        f"{'Test%':>8}"
        f"{'Tests':>8}"
    )
    print(_bold(hdr))
    sep = f"  {'-' * 26} {'-' * 10} {'-' * 8} {'-' * 6} {'-' * 7} {'-' * 7} {'-' * 7} {'-' * 7}"
    print(_dim(sep))

    # Group by phase
    phase1 = [
        "s3",
        "sqs",
        "sns",
        "dynamodb",
        "dynamodbstreams",
        "lambda",
        "iam",
        "sts",
        "cloudformation",
        "cloudwatch",
        "logs",
        "kms",
    ]
    phase2 = [
        "events",
        "kinesis",
        "firehose",
        "stepfunctions",
        "scheduler",
        "apigateway",
        "apigatewayv2",
        "secretsmanager",
        "ssm",
        "s3control",
    ]

    def print_service_row(svc_name: str, data: dict) -> None:
        status = data["status"].replace("_", " ")
        if data["status"] == "native":
            status_str = _green(f"{status:>10}")
        else:
            status_str = _cyan(f"{status:>10}")

        impl_pct_str = _pct_color(data["impl_pct"]) if data["total_aws_ops"] else _dim("  n/a")
        test_pct_str = _pct_color(data["test_pct"]) if data["total_aws_ops"] else _dim("  n/a")

        print(
            f"  {svc_name:<26}"
            f"{status_str}"
            f"{data['total_aws_ops']:>9}"
            f"{data['implemented_count']:>7}"
            f"{impl_pct_str:>16}"
            f"{data['tested_count']:>8}"
            f"{test_pct_str:>16}"
            f"{data['test_method_count']:>8}"
        )

    phase3 = [k for k in services if k not in phase1 and k not in phase2]

    if any(s in services for s in phase1):
        print(_dim(f"  {'Phase 1 - Core':}"))
        for svc in phase1:
            if svc in services:
                print_service_row(svc, services[svc])

    if any(s in services for s in phase2):
        print(_dim(f"  {'Phase 2 - Integration':}"))
        for svc in phase2:
            if svc in services:
                print_service_row(svc, services[svc])

    if phase3:
        print(_dim(f"  {'Phase 3 - Remaining':}"))
        for svc in sorted(phase3):
            if svc in services:
                print_service_row(svc, services[svc])

    # Summary
    print()
    print(_bold(f"  {'=' * 88}"))
    print(_bold("  SUMMARY"))
    print(_bold(f"  {'=' * 88}"))
    print()
    print(f"  Services registered:     {_bold(str(summary['total_services']))}")
    print(f"    Native providers:      {_green(str(summary['native_services']))}")
    print(f"    Moto-backed:           {_cyan(str(summary['moto_backed_services']))}")
    print()
    print(f"  AWS operations (total):  {summary['total_aws_operations']}")
    print(
        f"  Operations implemented:  {summary['total_implemented']}"
        f"  ({_pct_color(summary['impl_pct'])})"
    )
    print(
        f"  Operations tested:       {summary['total_tested']}  ({_pct_color(summary['test_pct'])})"
    )
    print(f"  Compat test methods:     {summary['total_test_methods']}")
    print()

    # Top gaps (services with lowest implementation coverage)
    gaps = [
        (name, data)
        for name, data in services.items()
        if data["total_aws_ops"] > 0 and data["impl_pct"] < 100
    ]
    gaps.sort(key=lambda x: x[1]["impl_pct"])

    if gaps:
        print(_bold("  BIGGEST IMPLEMENTATION GAPS (lowest coverage):"))
        for name, data in gaps[:10]:
            missing = data["total_aws_ops"] - data["implemented_count"]
            bar_width = 30
            filled = int(data["impl_pct"] / 100 * bar_width)
            bar = _green("*" * filled) + _dim("-" * (bar_width - filled))
            print(f"    {name:<24} [{bar}] {data['impl_pct']:>5.1f}%  ({missing} ops missing)")
        print()

    # Untested operations (services with tests but operations not covered)
    untested_gaps = [
        (name, data)
        for name, data in services.items()
        if data["total_aws_ops"] > 0 and data["implemented_count"] > data["tested_count"]
    ]
    untested_gaps.sort(key=lambda x: x[1]["implemented_count"] - x[1]["tested_count"], reverse=True)

    if untested_gaps:
        print(_bold("  IMPLEMENTED BUT UNTESTED (potential test targets):"))
        for name, data in untested_gaps[:10]:
            untested = data["implemented_count"] - data["tested_count"]
            if untested > 0:
                print(f"    {name:<24} {untested} implemented ops not covered by compat tests")
        print()

    print(_bold(f"  {'=' * 88}"))
    print()


def print_service_detail(report: dict, service_name: str) -> None:
    """Print detailed operation-level information for a single service."""
    if service_name not in report["services"]:
        print(f"  Service '{service_name}' not found in registry.")
        return

    data = report["services"][service_name]
    print()
    print(_bold(f"  Service: {service_name}"))
    print(f"  Status: {data['status']}  |  Protocol: {data['protocol']}  |  {data['description']}")
    print(f"  Delegates to Moto: {'yes' if data['delegates_to_moto'] else 'no'}")
    print()

    botocore_ops = get_botocore_operations(service_name)
    impl_set = set(data["implemented_ops"])
    tested_set = set(data["tested_ops"])
    native_set = set(data["native_ops"])
    moto_set = set(data["moto_ops"])

    print(f"  {'Operation':<45} {'Impl':>6} {'Source':>8} {'Tested':>7}")
    print(f"  {'-' * 45} {'-' * 6} {'-' * 8} {'-' * 7}")

    for op in botocore_ops:
        is_impl = op in impl_set
        is_tested = op in tested_set
        source = ""
        if op in native_set and op in moto_set:
            source = "both"
        elif op in native_set:
            source = "native"
        elif op in moto_set:
            source = "moto"

        impl_mark = _green("  yes") if is_impl else _red("   no")
        test_mark = _green("  yes") if is_tested else (_dim("   no") if is_impl else _dim("    -"))
        source_str = _green(f"{source:>8}") if source else _dim(f"{'':>8}")

        print(f"  {op:<45} {impl_mark} {source_str} {test_mark}")

    print()
    print(
        f"  Total: {len(botocore_ops)} operations, "
        f"{data['implemented_count']} implemented ({data['impl_pct']:.0f}%), "
        f"{data['tested_count']} tested ({data['test_pct']:.0f}%)"
    )
    print()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate AWS parity report for robotocore")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--service", help="Show detailed report for a single service")
    parser.add_argument("--no-color", action="store_true", help="Disable color output")
    parser.add_argument("--output", help="Write JSON output to this file path")
    args = parser.parse_args()

    global _USE_COLOR
    if args.no_color or not _supports_color():
        _USE_COLOR = False

    report = build_report(filter_service=args.service if args.service else None)

    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, default=str))
        print(f"Parity report written to {args.output}")
    elif args.json:
        # Convert sets to lists for JSON serialization
        print(json.dumps(report, indent=2, default=str))
    elif args.service:
        # Rebuild full report to get botocore ops for detail view
        full_report = build_report()
        print_service_detail(full_report, args.service)
    else:
        print_report(report)


if __name__ == "__main__":
    main()
