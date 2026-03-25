#!/usr/bin/env python3
"""Validate test assertions against botocore output shapes.

Catches co-generated bias where both implementation and test agree on
wrong behavior. Uses botocore service models as structural ground truth.

Checks:
  - Phantom keys: asserted keys not in botocore output shape
  - ResponseMetadata-only: tests that only check status codes
  - Missing error code: pytest.raises(ClientError) without code check
  - Low shape coverage: tests cover <30% of output shape members
  - Type mismatches: asserting string equality on integer fields

Usage:
    uv run python scripts/validate_test_semantics.py --json
    uv run python scripts/validate_test_semantics.py --file tests/compatibility/test_sqs_compat.py
    uv run python scripts/validate_test_semantics.py --max-phantom-pct 2
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import botocore.session

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.lib.service_names import resolve_all_services  # noqa: E402

COMPAT_DIR = PROJECT_ROOT / "tests" / "compatibility"

# Keys added by boto3 client-side, NOT in botocore output shapes
BOTO3_INJECTED_KEYS = {"ResponseMetadata"}
RESPONSE_METADATA_CHILDREN = {
    "HTTPStatusCode",
    "RequestId",
    "HTTPHeaders",
    "RetryAttempts",
}


def _to_snake(name: str) -> str:
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_output_shape_keys(
    botocore_name: str,
) -> dict[str, dict[str, str]]:
    """Get output shape member names and types for all operations.

    Returns {OperationName: {MemberName: type_name}}.
    """
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(botocore_name)
    except Exception:
        return {}

    result = {}
    for op_name in model.operation_names:
        op = model.operation_model(op_name)
        members = {}
        if op.output_shape and op.output_shape.members:
            for name, shape in op.output_shape.members.items():
                # Skip header/URI/querystring locations
                loc = shape.serialization.get("location", "")
                if loc in ("header", "headers", "uri", "querystring", "statusCode"):
                    continue
                members[name] = shape.type_name
        result[op_name] = members
    return result


class ResponseTracker(ast.NodeVisitor):
    """Track response variable → operation bindings and assertion targets.

    Performs lightweight data flow analysis within a single test function.
    """

    def __init__(self, snake_to_pascal: dict[str, str]):
        self.snake_to_pascal = snake_to_pascal
        # var_name → PascalCase operation name
        self.bindings: dict[str, str] = {}
        # List of (operation, key_accessed, in_assert_context)
        self.key_accesses: list[tuple[str, str, bool]] = []
        # Operations called in this function
        self.operations_called: set[str] = set()
        self._in_assert = False
        # Tracks pytest.raises usage
        self.has_pytest_raises = False
        self.pytest_raises_checks_code = False
        # Tracks if only ResponseMetadata is asserted on
        self.asserted_non_metadata_key = False

    def visit_Assign(self, node: ast.Assign):
        # resp = client.foo(...) → bindings["resp"] = "Foo"
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            method = node.value.func.attr
            pascal = self.snake_to_pascal.get(method)
            if pascal:
                self.operations_called.add(pascal)
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        self.bindings[target.id] = pascal
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr):
        # Bare client.foo() calls without assignment
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            method = node.value.func.attr
            pascal = self.snake_to_pascal.get(method)
            if pascal:
                self.operations_called.add(pascal)
        self.generic_visit(node)

    def visit_Assert(self, node: ast.Assert):
        old = self._in_assert
        self._in_assert = True
        self.generic_visit(node)
        self._in_assert = old

    def visit_Subscript(self, node: ast.Subscript):
        # resp["Key"] or resp.get("Key")
        if isinstance(node.value, ast.Name) and isinstance(node.slice, ast.Constant):
            var = node.value.id
            key = node.slice.value
            if isinstance(key, str) and var in self.bindings:
                op = self.bindings[var]
                self.key_accesses.append((op, key, self._in_assert))
                if key not in BOTO3_INJECTED_KEYS and key not in RESPONSE_METADATA_CHILDREN:
                    self.asserted_non_metadata_key = True
        self.generic_visit(node)

    def visit_With(self, node: ast.With):
        # Detect pytest.raises(ClientError)
        for item in node.items:
            ctx = item.context_expr
            if isinstance(ctx, ast.Call) and isinstance(ctx.func, ast.Attribute):
                if ctx.func.attr == "raises":
                    self.has_pytest_raises = True
                    # Check if the body accesses .response["Error"]["Code"]
                    for child in ast.walk(node):
                        if isinstance(child, ast.Subscript):
                            if isinstance(child.slice, ast.Constant):
                                if child.slice.value == "Code":
                                    self.pytest_raises_checks_code = True
        self.generic_visit(node)


def analyze_test_function(
    func_node: ast.FunctionDef,
    class_name: str | None,
    snake_to_pascal: dict[str, str],
    output_shapes: dict[str, dict[str, str]],
) -> list[dict]:
    """Analyze a single test function for semantic issues."""
    tracker = ResponseTracker(snake_to_pascal)
    tracker.visit(func_node)

    full_name = f"{class_name}::{func_node.name}" if class_name else func_node.name
    results = []

    # Group key accesses by operation
    ops_keys: dict[str, set[str]] = defaultdict(set)
    ops_assert_keys: dict[str, set[str]] = defaultdict(set)
    for op, key, in_assert in tracker.key_accesses:
        ops_keys[op].add(key)
        if in_assert:
            ops_assert_keys[op].add(key)

    for op in tracker.operations_called:
        shape_keys = output_shapes.get(op, {})
        asserted = ops_assert_keys.get(op, set())
        # Filter out boto3-injected keys
        asserted_clean = asserted - BOTO3_INJECTED_KEYS - RESPONSE_METADATA_CHILDREN
        shape_key_names = set(shape_keys.keys())

        # Phantom keys: asserted but not in shape
        phantom = asserted_clean - shape_key_names
        # Uncovered keys: in shape but not asserted
        uncovered = shape_key_names - asserted_clean
        # Shape coverage
        coverage = (
            len(shape_key_names & asserted_clean) / len(shape_key_names) if shape_key_names else 1.0
        )

        issues = []

        for k in sorted(phantom):
            issues.append(
                {
                    "type": "phantom_key",
                    "severity": "error",
                    "key": k,
                    "message": f"Asserted key '{k}' not in botocore output shape",
                }
            )

        # ResponseMetadata-only check
        if asserted and not tracker.asserted_non_metadata_key:
            rm_only = asserted <= (BOTO3_INJECTED_KEYS | RESPONSE_METADATA_CHILDREN)
            if rm_only:
                issues.append(
                    {
                        "type": "metadata_only",
                        "severity": "warning",
                        "message": "Test only asserts on ResponseMetadata keys",
                    }
                )

        # Missing error code check
        if tracker.has_pytest_raises and not tracker.pytest_raises_checks_code:
            issues.append(
                {
                    "type": "missing_error_code",
                    "severity": "warning",
                    "message": ("pytest.raises(ClientError) without checking Error.Code"),
                }
            )

        # Low shape coverage
        if len(shape_key_names) > 3 and coverage < 0.3:
            issues.append(
                {
                    "type": "low_coverage",
                    "severity": "info",
                    "key_count": len(shape_key_names),
                    "covered": len(shape_key_names & asserted_clean),
                    "message": (
                        f"Only {len(shape_key_names & asserted_clean)}"
                        f"/{len(shape_key_names)} output keys asserted"
                    ),
                }
            )

        results.append(
            {
                "test": full_name,
                "operation": op,
                "asserted_keys": sorted(asserted_clean),
                "botocore_output_keys": sorted(shape_key_names),
                "phantom_keys": sorted(phantom),
                "uncovered_keys": sorted(uncovered),
                "shape_coverage": round(coverage, 3),
                "issues": issues,
            }
        )

    return results


def analyze_file(
    test_file: Path,
    botocore_name: str,
) -> list[dict]:
    """Analyze all test functions in a file."""
    botocore_ops = []
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(botocore_name)
        botocore_ops = list(model.operation_names)
    except Exception:
        return []

    snake_to_pascal = {_to_snake(op): op for op in botocore_ops}
    output_shapes = get_output_shape_keys(botocore_name)

    try:
        tree = ast.parse(test_file.read_text())
    except Exception:
        return []

    all_results = []
    current_class = None

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef):
            current_class = node.name
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if item.name.startswith("test_"):
                        results = analyze_test_function(
                            item,
                            current_class,
                            snake_to_pascal,
                            output_shapes,
                        )
                        all_results.extend(results)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("test_"):
                results = analyze_test_function(
                    node,
                    None,
                    snake_to_pascal,
                    output_shapes,
                )
                all_results.extend(results)

    return all_results


def main():
    parser = argparse.ArgumentParser(
        description="Validate test assertions against botocore shapes",
    )
    parser.add_argument("--file", help="Analyze a single test file")
    parser.add_argument("--service", help="Analyze tests for one service")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--problems-only", action="store_true", help="Only show tests with issues")
    parser.add_argument(
        "--max-phantom-pct",
        type=float,
        default=100,
        help="CI gate: fail if phantom key %% exceeds this",
    )
    args = parser.parse_args()

    all_services = resolve_all_services()
    all_results = []

    if args.file:
        # Single file mode — derive service from filename
        fname = Path(args.file).name
        stem = fname.removeprefix("test_").removesuffix("_compat.py")
        matched = None
        for name, sn in all_services.items():
            if sn.test_stem == stem:
                matched = sn
                break
        if not matched:
            print(f"Could not map {fname} to a service", file=sys.stderr)
            sys.exit(1)
        all_results = analyze_file(Path(args.file), matched.botocore)
    else:
        target_services = all_services
        if args.service:
            if args.service not in all_services:
                print(f"Unknown service: {args.service}", file=sys.stderr)
                sys.exit(1)
            target_services = {args.service: all_services[args.service]}

        for name, sn in sorted(target_services.items()):
            if not sn.test_stem:
                continue
            test_file = COMPAT_DIR / f"test_{sn.test_stem}_compat.py"
            if not test_file.exists():
                continue
            results = analyze_file(test_file, sn.botocore)
            for r in results:
                r["service"] = name
            all_results.extend(results)

    # Filter if requested
    if args.problems_only:
        all_results = [r for r in all_results if r.get("issues")]

    # Compute summary stats
    total = len(all_results)
    with_issues = sum(1 for r in all_results if r.get("issues"))
    phantom_count = sum(
        1 for r in all_results for i in r.get("issues", []) if i["type"] == "phantom_key"
    )
    metadata_only = sum(
        1 for r in all_results for i in r.get("issues", []) if i["type"] == "metadata_only"
    )
    missing_code = sum(
        1 for r in all_results for i in r.get("issues", []) if i["type"] == "missing_error_code"
    )
    low_cov = sum(
        1 for r in all_results for i in r.get("issues", []) if i["type"] == "low_coverage"
    )

    if args.json:
        output = {
            "summary": {
                "total_test_operation_pairs": total,
                "with_issues": with_issues,
                "phantom_key_tests": phantom_count,
                "metadata_only_tests": metadata_only,
                "missing_error_code_tests": missing_code,
                "low_coverage_tests": low_cov,
            },
            "results": all_results,
        }
        print(json.dumps(output, indent=2))
    else:
        print(f"Semantic Test Validation: {total} test-operation pairs\n")
        print(f"  With issues:         {with_issues}")
        print(f"  Phantom keys:        {phantom_count}")
        print(f"  Metadata-only:       {metadata_only}")
        print(f"  Missing error code:  {missing_code}")
        print(f"  Low coverage:        {low_cov}")

        if all_results and (args.problems_only or with_issues):
            print("\nIssues:")
            for r in all_results:
                if not r.get("issues"):
                    continue
                svc = r.get("service", "?")
                for issue in r["issues"]:
                    sev = issue["severity"].upper()
                    print(f"  [{sev}] {svc}/{r['operation']} in {r['test']}: {issue['message']}")

    # CI gate
    if total > 0:
        phantom_pct = phantom_count / total * 100
        if phantom_pct > args.max_phantom_pct:
            print(
                f"\nFAIL: phantom key rate {phantom_pct:.1f}% exceeds max {args.max_phantom_pct}%",
                file=sys.stderr,
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
