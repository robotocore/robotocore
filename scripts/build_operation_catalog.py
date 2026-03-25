#!/usr/bin/env python3
"""Build a per-operation truth table for every AWS service in robotocore.

Combines evidence from botocore specs, native providers, Moto backends,
compat tests, and (optionally) live probe results into a single JSON catalog.
Each record captures implementation confidence, test status, and a MECE
work classification.

Usage:
    uv run python scripts/build_operation_catalog.py --json
    uv run python scripts/build_operation_catalog.py --service sqs -v
    uv run python scripts/build_operation_catalog.py --mece --md
    uv run python scripts/build_operation_catalog.py --summary
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.lib.service_names import (  # noqa: E402
    MOTO_BASE,
    SRC_SERVICES,
    ServiceNames,
    resolve_all_services,
)

# ── Skip lists ──

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

# ── CRUD prefix detection ──

_CRUD_PREFIXES = [
    "Create", "Delete", "Remove", "Get", "Describe", "List", "Update",
    "Put", "Tag", "Untag", "Start", "Stop", "Enable", "Disable",
    "Register", "Deregister", "Add", "Batch",
]


def _derive_crud(operation: str) -> tuple[str, str]:
    """Derive CRUD group and role from operation name."""
    for prefix in _CRUD_PREFIXES:
        if operation.startswith(prefix):
            remainder = operation[len(prefix):]
            if remainder:
                return remainder, prefix.lower()
    return operation, "other"


# ── Botocore operations ──

def get_botocore_operations(botocore_name: str) -> list[str]:
    """Get all operation names from botocore for a service."""
    import botocore.session
    try:
        session = botocore.session.get_session()
        model = session.get_service_model(botocore_name)
        return sorted(model.operation_names)
    except Exception:
        return []


# ── Native provider operations ──

def _extract_action_map_keys(filepath: Path) -> list[str]:
    """Extract operation names from _ACTION_MAP dicts in a provider file."""
    try:
        content = filepath.read_text()
    except Exception:
        return []

    operations = []
    in_map = False
    brace_depth = 0
    for line in content.splitlines():
        stripped = line.strip()
        if re.match(r"_?ACTION_MAP\b.*=\s*\{", stripped):
            in_map = True
            brace_depth = stripped.count("{") - stripped.count("}")
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


def _extract_rest_route_operations(filepath: Path) -> list[str]:
    """Extract operations from REST-based providers."""
    try:
        content = filepath.read_text()
    except Exception:
        return []

    operations = set()

    # Comments referencing operations: # GET /functions -- ListFunctions
    for m in re.finditer(
        r"#\s*(?:GET|PUT|POST|DELETE|PATCH|HEAD)?\s*(?:/\S+)?\s*[-—]+\s*([A-Z][a-zA-Z]+)",
        content,
    ):
        operations.add(m.group(1))

    # Comments with just an operation name
    for m in re.finditer(r"^\s*#\s+([A-Z][a-z]+[A-Z][a-zA-Z]+)\b", content, re.MULTILINE):
        op = m.group(1)
        if not op.endswith(("Error", "Exception", "Response")):
            operations.add(op)

    # Backend method calls: backend.create_function(...)
    for m in re.finditer(r"backend\.([a-z_]+)\(", content):
        pascal = "".join(w.capitalize() for w in m.group(1).split("_"))
        if len(pascal) > 4:
            operations.add(pascal)

    return sorted(operations)


def get_native_operations(names: ServiceNames) -> tuple[set[str], bool]:
    """Get operations from native provider. Returns (ops, delegates_to_moto)."""
    if not names.provider_dir:
        return set(), False

    provider_path = SRC_SERVICES / names.provider_dir / "provider.py"
    if not provider_path.exists():
        # Check for service-specific provider files
        alt = SRC_SERVICES / names.provider_dir / f"{names.registry.replace('-', '_')}_provider.py"
        if alt.exists():
            provider_path = alt
        elif names.registry == "logs":
            provider_path = SRC_SERVICES / "cloudwatch" / "logs_provider.py"
        else:
            return set(), False

    try:
        content = provider_path.read_text()
    except Exception:
        return set(), False

    delegates = "forward_to_moto" in content

    ops = _extract_action_map_keys(provider_path)
    if not ops:
        ops = _extract_rest_route_operations(provider_path)

    return set(ops), delegates


# ── Moto operations + stub detection ──

def _to_snake(name: str) -> str:
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _is_stub_method(func_node: ast.FunctionDef) -> bool:
    """Check if a method body is trivial (stub)."""
    real_stmts = []
    for n in func_node.body:
        if isinstance(n, ast.Pass):
            continue
        if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant):
            continue  # docstring
        real_stmts.append(n)
    if not real_stmts:
        return True
    if len(real_stmts) == 1 and isinstance(real_stmts[0], ast.Raise):
        return True
    return False


def get_moto_operations(names: ServiceNames) -> tuple[set[str], set[str]]:
    """Get Moto operations. Returns (implemented_ops, stub_ops)."""
    if not names.moto_dir:
        return set(), set()

    # Build snake_case → PascalCase lookup from botocore
    botocore_ops = get_botocore_operations(names.botocore)
    snake_to_pascal = {_to_snake(op): op for op in botocore_ops}

    moto_dir = MOTO_BASE / names.moto_dir
    implemented = set()
    stubs = set()

    # Scan models.py or models/__init__.py for Backend class methods
    models_files = []
    if (moto_dir / "models.py").exists():
        models_files.append(moto_dir / "models.py")
    elif (moto_dir / "models" / "__init__.py").exists():
        models_files.append(moto_dir / "models" / "__init__.py")

    for src_file in models_files:
        try:
            tree = ast.parse(src_file.read_text())
        except Exception:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef) or not node.name.endswith("Backend"):
                continue
            for item in node.body:
                if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if item.name.startswith("_"):
                    continue
                pascal = snake_to_pascal.get(item.name)
                if pascal:
                    if _is_stub_method(item):
                        stubs.add(pascal)
                    else:
                        implemented.add(pascal)

    # Also scan responses.py
    resp_files = []
    if (moto_dir / "responses.py").exists():
        resp_files.append(moto_dir / "responses.py")
    elif (moto_dir / "responses" / "__init__.py").exists():
        resp_files.append(moto_dir / "responses" / "__init__.py")

    for resp_file in resp_files:
        try:
            tree = ast.parse(resp_file.read_text())
        except Exception:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            for item in node.body:
                if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if item.name.startswith("_"):
                    continue
                pascal = snake_to_pascal.get(item.name)
                if pascal and pascal not in implemented and pascal not in stubs:
                    implemented.add(pascal)

    return implemented, stubs


# ── Moto @aws_verified test scanning ──

def get_aws_verified_operations(names: ServiceNames) -> set[str]:
    """Find operations covered by Moto's @aws_verified tests."""
    if not names.moto_dir:
        return set()

    test_dir = MOTO_BASE.parent / "tests" / f"test_{names.moto_dir}"
    if not test_dir.is_dir():
        return set()

    # Build snake→pascal lookup
    botocore_ops = get_botocore_operations(names.botocore)
    snake_to_pascal = {_to_snake(op): op for op in botocore_ops}

    verified_ops = set()

    for py_file in test_dir.glob("*.py"):
        try:
            tree = ast.parse(py_file.read_text())
        except Exception:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue

            # Check for @aws_verified decorator
            has_aws_verified = False
            for dec in node.decorator_list:
                if isinstance(dec, ast.Name) and dec.id == "aws_verified":
                    has_aws_verified = True
                elif isinstance(dec, ast.Call):
                    if isinstance(dec.func, ast.Name) and dec.func.id == "aws_verified":
                        has_aws_verified = True
                    elif isinstance(dec.func, ast.Attribute) and dec.func.attr == "aws_verified":
                        has_aws_verified = True
            if not has_aws_verified:
                continue

            # Find boto3 client method calls within this function
            for child in ast.walk(node):
                if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                    method = child.func.attr
                    pascal = snake_to_pascal.get(method)
                    if pascal:
                        verified_ops.add(pascal)

    return verified_ops


# ── Compat test operations + quality ──

# Non-AWS method names to exclude
_SKIP_METHODS = frozenset({
    "get", "set", "items", "keys", "values", "append", "extend", "update",
    "format", "join", "split", "strip", "encode", "decode", "read", "write",
    "close", "sleep", "client", "resource", "startswith", "endswith",
    "replace", "lower", "upper", "pop", "add", "remove", "clear", "copy",
    "sort", "reverse", "count", "index", "find", "match", "search", "sub",
    "group", "dump", "dumps", "load", "loads", "open", "seek", "tell",
    "flush", "isinstance", "len", "str", "int", "float", "bool", "list",
    "dict", "tuple", "type", "print", "range", "enumerate", "zip", "map",
    "filter", "sorted", "any", "all", "min", "max", "sum", "abs",
})


def _is_key_presence_assert(node: ast.Assert) -> bool:
    """Return True if the assert is just key-presence: assert 'X' in resp."""
    test = node.test
    if not isinstance(test, ast.Compare):
        return False
    if len(test.ops) != 1 or not isinstance(test.ops[0], ast.In):
        return False
    if not isinstance(test.left, ast.Constant) or not isinstance(test.left.value, str):
        return False
    return True


def _classify_test_quality(func_node: ast.FunctionDef) -> str:
    """Classify a test function's quality."""
    has_assert = False
    has_try_except = False
    catches_param_validation = False
    catches_client_error = False
    all_excepts_pass = True
    has_method_call = False
    assert_nodes: list[ast.Assert] = []

    for child in ast.walk(func_node):
        if isinstance(child, ast.Assert):
            has_assert = True
            assert_nodes.append(child)
        if isinstance(child, ast.Attribute) and child.attr == "raises":
            has_assert = True
        if isinstance(child, ast.Try):
            has_try_except = True
            for handler in child.handlers:
                if handler.type is None:
                    catches_client_error = True
                elif isinstance(handler.type, (ast.Attribute, ast.Name)):
                    ht = handler.type
                    name = ht.attr if isinstance(ht, ast.Attribute) else ht.id
                    if name == "ParamValidationError":
                        catches_param_validation = True
                    elif name == "ClientError":
                        catches_client_error = True
                if handler.body:
                    for stmt in handler.body:
                        if not isinstance(stmt, (ast.Pass, ast.Expr)):
                            all_excepts_pass = False
                        elif isinstance(stmt, ast.Expr) and not isinstance(
                            stmt.value, (ast.Constant, ast.JoinedStr)
                        ):
                            all_excepts_pass = False
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
            has_method_call = True

    if catches_param_validation and has_try_except and all_excepts_pass and not has_assert:
        return "no_server_contact"
    if has_try_except and all_excepts_pass and catches_client_error and not has_assert:
        return "no_assertion"
    if not has_assert and not has_try_except and has_method_call:
        return "no_assertion"
    if assert_nodes and all(_is_key_presence_assert(a) for a in assert_nodes):
        return "weak_assertion"
    if has_assert or (has_try_except and not all_excepts_pass):
        return "ok"
    return "unknown"


def get_test_evidence(names: ServiceNames) -> dict[str, list[dict]]:
    """Get per-operation test evidence from compat tests.

    Returns {PascalCaseOp: [{"function": ..., "quality": ...}, ...]}.
    """
    if not names.test_stem:
        return {}

    test_file = PROJECT_ROOT / "tests" / "compatibility" / f"test_{names.test_stem}_compat.py"
    if not test_file.exists():
        return {}

    botocore_ops = get_botocore_operations(names.botocore)
    snake_to_pascal = {_to_snake(op): op for op in botocore_ops}

    try:
        tree = ast.parse(test_file.read_text())
    except Exception:
        return {}

    result: dict[str, list[dict]] = defaultdict(list)
    current_class = None

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            current_class = node.name
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if item.name.startswith("test_"):
                        _process_test_func(item, current_class, snake_to_pascal, result)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("test_") and current_class is None:
                _process_test_func(node, None, snake_to_pascal, result)

    return dict(result)


def _process_test_func(
    func_node: ast.FunctionDef,
    class_name: str | None,
    snake_to_pascal: dict[str, str],
    result: dict[str, list[dict]],
) -> None:
    """Extract operations called and quality for a single test function."""
    quality = _classify_test_quality(func_node)
    full_name = f"{class_name}::{func_node.name}" if class_name else func_node.name

    # Find all boto3-like method calls
    ops_called = set()
    for child in ast.walk(func_node):
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
            method = child.func.attr
            if method in _SKIP_METHODS or method.startswith(("assert", "pytest")):
                continue
            pascal = snake_to_pascal.get(method)
            if pascal:
                ops_called.add(pascal)

    for op in ops_called:
        result[op].append({"function": full_name, "quality": quality})


# ── Confidence lattice ──

def compute_confidence(evidence: dict) -> str:
    """Compute implementation confidence level from evidence."""
    if evidence.get("moto_aws_verified"):
        return "behaviorally_verified"
    if evidence.get("shape_conformant"):
        return "shape_conformant"
    if evidence.get("probe_status") == "working":
        return "probe_working"
    me = evidence.get("method_exists", {})
    if (me.get("native") or me.get("moto_backend") or me.get("moto_response")) \
            and not evidence.get("moto_is_stub"):
        return "method_exists"
    if evidence.get("moto_is_stub"):
        return "method_is_stub"
    return "unimplemented"


# ── MECE classification ──

def classify(record: dict) -> str:
    """Total function: every operation gets exactly one category.

    Priority-ordered decision tree. Every path returns. Catch-all at end.
    """
    op = record["operation"]
    confidence = record["impl_confidence"]
    probe = record.get("impl_evidence", {}).get("probe_status")
    test_info = record.get("test_evidence", {})
    has_test = test_info.get("has_compat_test", False)
    quality = test_info.get("test_quality")

    if op in SKIP_OPERATIONS:
        return "skip"
    if confidence in ("unimplemented", "method_is_stub"):
        return "implement"
    if probe == "500_error":
        return "fix_impl"
    if confidence == "method_exists" and probe == "needs_params":
        return "verify"
    if not has_test:
        return "test"
    if quality in ("no_assertion", "no_server_contact"):
        return "fix_test"
    if quality == "weak_assertion":
        return "strengthen_test"
    return "done"


# ── Main catalog builder ──

def build_catalog(
    service_filter: str | None = None,
    with_probe: bool = False,
) -> list[dict]:
    """Build the complete operation catalog."""
    all_services = resolve_all_services()

    if service_filter:
        if service_filter not in all_services:
            print(f"Unknown service: {service_filter}", file=sys.stderr)
            sys.exit(1)
        all_services = {service_filter: all_services[service_filter]}

    catalog = []

    for svc_name, names in sorted(all_services.items()):
        botocore_ops = get_botocore_operations(names.botocore)
        if not botocore_ops:
            continue

        # Gather evidence for this service
        native_ops, delegates = get_native_operations(names)
        moto_impl, moto_stubs = get_moto_operations(names)
        aws_verified = get_aws_verified_operations(names)
        test_evidence = get_test_evidence(names)

        # Load probe cache if requested
        probe_data: dict[str, str] = {}
        if with_probe:
            cache_file = PROJECT_ROOT / "data" / "probe_cache" / f"{svc_name}.json"
            if cache_file.exists():
                try:
                    probe_data = {
                        r["operation"]: r["status"]
                        for r in json.loads(cache_file.read_text()).get("operations", [])
                    }
                except Exception:
                    pass

        for op in botocore_ops:
            # Implementation evidence
            in_native = op in native_ops
            in_moto_backend = op in moto_impl
            in_moto_response = op in moto_impl  # simplified: same set from backend+responses
            is_stub = op in moto_stubs and op not in moto_impl
            is_verified = op in aws_verified

            impl_evidence = {
                "method_exists": {
                    "native": in_native,
                    "moto_backend": in_moto_backend,
                    "moto_response": in_moto_response,
                },
                "moto_is_stub": is_stub,
                "moto_aws_verified": is_verified,
                "probe_status": probe_data.get(op),
                "shape_conformant": None,
                "delegates_to_moto": delegates,
            }

            confidence = compute_confidence(impl_evidence)

            # Test evidence
            op_tests = test_evidence.get(op, [])
            has_test = len(op_tests) > 0
            # Best quality among all tests for this operation
            quality_order = ["ok", "weak_assertion", "no_assertion", "no_server_contact", "unknown"]
            best_quality = None
            if op_tests:
                qualities = [t["quality"] for t in op_tests]
                for q in quality_order:
                    if q in qualities:
                        best_quality = q
                        break
                if best_quality is None:
                    best_quality = qualities[0]

            test_ev = {
                "has_compat_test": has_test,
                "test_quality": best_quality,
                "test_functions": [t["function"] for t in op_tests],
            }

            # CRUD group
            crud_group, crud_role = _derive_crud(op)

            record = {
                "service": svc_name,
                "operation": op,
                "impl_confidence": confidence,
                "impl_evidence": impl_evidence,
                "test_evidence": test_ev,
                "crud_group": crud_group,
                "crud_role": crud_role,
            }
            record["mece_category"] = classify(record)
            catalog.append(record)

    # MECE self-test
    all_ops = [(r["service"], r["operation"]) for r in catalog]
    assert len(all_ops) == len(set(all_ops)), "MECE violation: duplicate (service, operation)"

    return catalog


# ── Output formatting ──

def print_summary(catalog: list[dict]) -> None:
    """Print summary counts."""
    from collections import Counter

    total = len(catalog)
    by_category = Counter(r["mece_category"] for r in catalog)
    by_confidence = Counter(r["impl_confidence"] for r in catalog)

    svc_count = len({r["service"] for r in catalog})

    print(f"Operation Catalog: {total} operations across {svc_count} services\n")

    print("MECE Categories:")
    cats = [
        "done", "test", "implement", "strengthen_test",
        "fix_test", "fix_impl", "verify", "skip",
    ]
    for cat in cats:
        count = by_category.get(cat, 0)
        pct = count / total * 100 if total else 0
        bar = "█" * int(pct / 2) + "░" * (50 - int(pct / 2))
        print(f"  {cat:<20} {count:>6}  ({pct:5.1f}%)  {bar}")

    print("\nImplementation Confidence:")
    for level in [
        "behaviorally_verified", "shape_conformant", "probe_working",
        "method_exists", "method_is_stub", "unimplemented",
    ]:
        count = by_confidence.get(level, 0)
        pct = count / total * 100 if total else 0
        print(f"  {level:<25} {count:>6}  ({pct:5.1f}%)")


def print_mece_table(catalog: list[dict], as_md: bool = False) -> None:
    """Print MECE task list grouped by service."""
    by_service: dict[str, list[dict]] = defaultdict(list)
    for r in catalog:
        if r["mece_category"] not in ("done", "skip"):
            by_service[r["service"]].append(r)

    if not by_service:
        print("All operations are done or skipped!")
        return

    if as_md:
        print("# MECE Task List\n")
        print(f"Generated from {len(catalog)} total operations.\n")
        from collections import Counter
        cats = Counter(r["mece_category"] for r in catalog)
        print("| Category | Count |")
        print("|----------|-------|")
        mece_cats = [
            "done", "test", "implement", "strengthen_test",
            "fix_test", "fix_impl", "verify", "skip",
        ]
        for c in mece_cats:
            print(f"| {c} | {cats.get(c, 0)} |")
        print()

        for svc, tasks in sorted(by_service.items()):
            print(f"## {svc} ({len(tasks)} tasks)\n")
            for t in sorted(tasks, key=lambda x: x["mece_category"]):
                cat = t["mece_category"]
                op = t["operation"]
                conf = t["impl_confidence"]
                print(f"- **{cat}**: `{op}` (confidence: {conf})")
            print()
    else:
        for svc, tasks in sorted(by_service.items()):
            print(f"\n{svc} ({len(tasks)} remaining):")
            for t in sorted(tasks, key=lambda x: x["mece_category"]):
                print(f"  [{t['mece_category']:<17}] {t['operation']:<40} ({t['impl_confidence']})")


def print_service_detail(catalog: list[dict], service: str) -> None:
    """Print detailed view for one service."""
    records = [r for r in catalog if r["service"] == service]
    if not records:
        print(f"No records for service: {service}")
        return

    from collections import Counter
    cats = Counter(r["mece_category"] for r in records)
    confs = Counter(r["impl_confidence"] for r in records)

    print(f"\n{service}: {len(records)} operations\n")
    print("MECE breakdown:")
    for c, n in cats.most_common():
        print(f"  {c:<20} {n}")
    print("\nConfidence breakdown:")
    for c, n in confs.most_common():
        print(f"  {c:<25} {n}")
    print("\nOperations:")

    for r in sorted(records, key=lambda x: (x["mece_category"], x["operation"])):
        test_str = "tested" if r["test_evidence"]["has_compat_test"] else "no test"
        quality = r["test_evidence"]["test_quality"] or "-"
        print(
            f"  [{r['mece_category']:<17}] {r['operation']:<40} "
            f"impl={r['impl_confidence']:<22} {test_str} quality={quality}"
        )


def main():
    parser = argparse.ArgumentParser(description="Build operation catalog")
    parser.add_argument("--service", help="Analyze a single service")
    parser.add_argument("--json", action="store_true", help="Output full catalog as JSON")
    parser.add_argument("--mece", action="store_true", help="Show MECE task list (non-done items)")
    parser.add_argument("--md", action="store_true", help="Output as markdown (use with --mece)")
    parser.add_argument("--summary", action="store_true", help="Show summary counts")
    parser.add_argument("--with-probe", action="store_true", help="Include probe cache data")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--output", help="Write JSON to file instead of stdout")
    args = parser.parse_args()

    catalog = build_catalog(
        service_filter=args.service,
        with_probe=args.with_probe,
    )

    if args.json:
        output = json.dumps(catalog, indent=2)
        if args.output:
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(output)
            print(f"Wrote {len(catalog)} records to {args.output}", file=sys.stderr)
        else:
            print(output)
    elif args.mece:
        if args.md:
            print_mece_table(catalog, as_md=True)
        else:
            print_mece_table(catalog, as_md=False)
    elif args.summary:
        print_summary(catalog)
    elif args.service and args.verbose:
        print_service_detail(catalog, args.service)
    elif args.service:
        print_service_detail(catalog, args.service)
    else:
        print_summary(catalog)


if __name__ == "__main__":
    main()
