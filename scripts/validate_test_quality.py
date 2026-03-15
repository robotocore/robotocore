#!/usr/bin/env python3
"""Validate that compat tests actually test server behavior.

Catches these classes of bad tests:
1. Tests that never contact the server (client-side ParamValidationError)
2. Tests with no assertions (catch-all exception handlers)
3. Tests that would pass against a stopped server

Usage:
    # Full report
    uv run python scripts/validate_test_quality.py

    # Check a single file
    uv run python scripts/validate_test_quality.py --file tests/compatibility/test_iam_compat.py

    # JSON output for CI
    uv run python scripts/validate_test_quality.py --json

    # Fail CI if too many bad tests
    uv run python scripts/validate_test_quality.py --max-no-contact-pct 5

    # Show only problems
    uv run python scripts/validate_test_quality.py --problems-only
"""

import argparse
import ast
import json
import sys
from pathlib import Path

TESTS_DIR = Path("tests/compatibility")


def _is_key_presence_assert(node: ast.Assert) -> bool:
    """Return True if the assert is a key-presence check: assert "X" in <name>.

    These only verify a key exists in a dict, not that it has a meaningful value.
    Patterns matched:
        assert "Key" in response
        assert "Key" in result
        assert "Key" in resp
    Does NOT match:
        assert response["Key"] == value   (value check — good)
        assert len(response["X"]) > 0     (value check — good)
        assert "Key" not in response      (negative check — ok, deliberate)
    """
    test = node.test
    if not isinstance(test, ast.Compare):
        return False
    if len(test.ops) != 1 or not isinstance(test.ops[0], ast.In):
        return False
    # Left side must be a string constant
    if not isinstance(test.left, ast.Constant) or not isinstance(test.left.value, str):
        return False
    return True


class TestQualityVisitor(ast.NodeVisitor):
    """AST visitor that classifies test methods by quality."""

    def __init__(self):
        self.tests: list[dict] = []
        self._current_class: str | None = None

    def visit_ClassDef(self, node: ast.ClassDef):
        old_class = self._current_class
        self._current_class = node.name
        self.generic_visit(node)
        self._current_class = old_class

    def visit_FunctionDef(self, node: ast.FunctionDef):
        if not node.name.startswith("test_"):
            return
        # Skip fixtures
        if any(
            isinstance(d, ast.Name)
            and d.id == "fixture"
            or isinstance(d, ast.Attribute)
            and d.attr == "fixture"
            for d in node.decorator_list
        ):
            return

        info = self._analyze_test(node)
        info["name"] = node.name
        info["class"] = self._current_class
        info["line"] = node.lineno
        self.tests.append(info)

    visit_AsyncFunctionDef = visit_FunctionDef  # noqa: N815

    def _analyze_test(self, node: ast.FunctionDef) -> dict:
        """Analyze a test method and classify its quality."""

        has_assert = False
        has_try_except = False
        catches_param_validation = False
        catches_client_error = False
        all_excepts_pass = True
        has_method_call = False
        has_response_capture = False
        assert_nodes: list[ast.Assert] = []

        for child in ast.walk(node):
            # Check for assert statements
            if isinstance(child, ast.Assert):
                has_assert = True
                assert_nodes.append(child)

            # Check for pytest.raises
            if isinstance(child, ast.Attribute) and child.attr == "raises":
                has_assert = True

            # Check for try/except
            if isinstance(child, ast.Try):
                has_try_except = True
                for handler in child.handlers:
                    if handler.type is None:
                        catches_client_error = True
                    elif isinstance(handler.type, ast.Attribute):
                        if handler.type.attr == "ParamValidationError":
                            catches_param_validation = True
                        elif handler.type.attr == "ClientError":
                            catches_client_error = True
                    elif isinstance(handler.type, ast.Name):
                        if handler.type.id == "ParamValidationError":
                            catches_param_validation = True
                        elif handler.type.id == "ClientError":
                            catches_client_error = True

                    # Check if handler body is just 'pass'
                    if handler.body:
                        for stmt in handler.body:
                            if not isinstance(stmt, (ast.Pass, ast.Expr)):
                                all_excepts_pass = False
                            elif isinstance(stmt, ast.Expr) and not isinstance(
                                stmt.value, (ast.Constant, ast.JoinedStr)
                            ):
                                all_excepts_pass = False

            # Check for method calls on client (indicates server contact attempt)
            if isinstance(child, ast.Call):
                if isinstance(child.func, ast.Attribute):
                    has_method_call = True

            # Check for response capture (resp = client.foo())
            if isinstance(child, ast.Assign):
                for target in child.targets:
                    if isinstance(target, ast.Name) and target.id in (
                        "resp",
                        "response",
                        "result",
                    ):
                        has_response_capture = True

        # Detect weak assertions: all asserts are key-presence checks
        # e.g. `assert "X" in response` but never `assert response["X"] == value`
        is_weak = False
        if assert_nodes:
            all_weak = all(_is_key_presence_assert(a) for a in assert_nodes)
            if all_weak:
                is_weak = True

        # Classify
        if catches_param_validation and has_try_except and all_excepts_pass and not has_assert:
            quality = "no_server_contact"
            reason = "catches ParamValidationError (client-side) with pass — never hits server"
        elif has_try_except and all_excepts_pass and catches_client_error and not has_assert:
            quality = "no_assertion"
            reason = "catches ClientError with pass — no assertion on error type or message"
        elif not has_assert and not has_try_except and has_method_call:
            quality = "no_assertion"
            reason = "calls server but has no assertions"
        elif is_weak:
            quality = "weak_assertion"
            reason = "all assertions are key-presence only (assert 'X' in resp) — no value checks"
        elif has_assert or (has_try_except and not all_excepts_pass):
            quality = "ok"
            reason = ""
        else:
            quality = "unknown"
            reason = "could not classify"

        return {
            "quality": quality,
            "reason": reason,
            "has_assert": has_assert,
            "has_try_except": has_try_except,
            "catches_param_validation": catches_param_validation,
            "catches_client_error": catches_client_error,
            "all_excepts_pass": all_excepts_pass,
            "has_response_capture": has_response_capture,
        }


def analyze_file(filepath: Path) -> list[dict]:
    """Analyze all tests in a file."""
    source = filepath.read_text()
    try:
        tree = ast.parse(source, filename=str(filepath))
    except SyntaxError:
        return []

    visitor = TestQualityVisitor()
    visitor.visit(tree)
    for t in visitor.tests:
        t["file"] = str(filepath)
    return visitor.tests


def main():
    parser = argparse.ArgumentParser(description="Validate compat test quality")
    parser.add_argument("--file", help="Single file to check")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--problems-only", action="store_true", help="Show only bad tests")
    parser.add_argument(
        "--max-no-contact-pct",
        type=float,
        default=0,
        help="Fail if no-server-contact tests exceed this percentage (0=no check)",
    )
    parser.add_argument(
        "--max-no-assertion-pct",
        type=float,
        default=0,
        help="Fail if no-assertion tests exceed this percentage (0=no check)",
    )
    parser.add_argument(
        "--max-weak-assertion-pct",
        type=float,
        default=0,
        help="Fail if weak-assertion tests exceed this percentage (0=no check)",
    )
    args = parser.parse_args()

    if args.file:
        files = [Path(args.file)]
    else:
        files = sorted(TESTS_DIR.glob("test_*_compat.py"))

    all_tests = []
    for f in files:
        all_tests.extend(analyze_file(f))

    # Aggregate
    total = len(all_tests)
    by_quality = {}
    for t in all_tests:
        q = t["quality"]
        by_quality.setdefault(q, []).append(t)

    ok_count = len(by_quality.get("ok", []))
    no_contact = len(by_quality.get("no_server_contact", []))
    no_assert = len(by_quality.get("no_assertion", []))
    weak_assert = len(by_quality.get("weak_assertion", []))
    unknown = len(by_quality.get("unknown", []))

    if args.json:
        summary = {
            "total_tests": total,
            "server_contact_with_assertions": ok_count,
            "no_server_contact": no_contact,
            "no_assertion": no_assert,
            "weak_assertion": weak_assert,
            "unknown": unknown,
            "no_contact_pct": round(no_contact / total * 100, 1) if total else 0,
            "no_assertion_pct": round(no_assert / total * 100, 1) if total else 0,
            "weak_assertion_pct": round(weak_assert / total * 100, 1) if total else 0,
            "effective_tests": ok_count,
            "effective_pct": round(ok_count / total * 100, 1) if total else 0,
        }
        if args.problems_only:
            problems = (
                by_quality.get("no_server_contact", [])
                + by_quality.get("no_assertion", [])
                + by_quality.get("weak_assertion", [])
            )
            summary["problems"] = [
                {"file": t["file"], "line": t["line"], "name": t["name"], "reason": t["reason"]}
                for t in problems
            ]
        print(json.dumps(summary, indent=2))
    else:
        print("Compat Test Quality Report")
        print(f"{'=' * 60}")
        print(f"Total test methods:              {total:,}")
        print(f"  Server contact + assertions:   {ok_count:,}  (effective)")
        print(f"  No server contact:             {no_contact:,}  (client-side validation only)")
        print(f"  No meaningful assertion:        {no_assert:,}  (call without verification)")
        print(f"  Weak assertions only:           {weak_assert:,}  (key-presence checks only)")
        print(f"  Unknown:                       {unknown:,}")
        print()
        pct_effective = ok_count / total * 100 if total else 0
        pct_no_contact = no_contact / total * 100 if total else 0
        pct_weak = weak_assert / total * 100 if total else 0
        print(f"Effective test rate:             {pct_effective:.1f}%")
        print(f"No-server-contact rate:          {pct_no_contact:.1f}%")
        print(f"Weak-assertion rate:             {pct_weak:.1f}%")
        print()

        if args.problems_only or not args.json:
            # Group problems by file
            problems = (
                by_quality.get("no_server_contact", [])
                + by_quality.get("no_assertion", [])
                + by_quality.get("weak_assertion", [])
            )
            if problems and not args.problems_only:
                print("Top files with non-effective tests:")
                file_counts: dict[str, dict[str, int]] = {}
                for t in problems:
                    fc = file_counts.setdefault(
                        t["file"], {"no_contact": 0, "no_assert": 0, "weak_assert": 0}
                    )
                    if t["quality"] == "no_server_contact":
                        fc["no_contact"] += 1
                    elif t["quality"] == "weak_assertion":
                        fc["weak_assert"] += 1
                    else:
                        fc["no_assert"] += 1
                for f, counts in sorted(
                    file_counts.items(), key=lambda x: sum(x[1].values()), reverse=True
                )[:15]:
                    total_bad = sum(counts.values())
                    fname = Path(f).name
                    print(f"  {fname:<50s} {total_bad:>4d} non-effective")

    # CI gate
    exit_code = 0
    if args.max_no_contact_pct > 0 and total > 0:
        pct = no_contact / total * 100
        if pct > args.max_no_contact_pct:
            print(
                f"\nFAIL: {pct:.1f}% of tests never contact server "
                f"(threshold: {args.max_no_contact_pct}%)"
            )
            exit_code = 1

    if args.max_no_assertion_pct > 0 and total > 0:
        pct = no_assert / total * 100
        if pct > args.max_no_assertion_pct:
            print(
                f"\nFAIL: {pct:.1f}% of tests have no assertions "
                f"(threshold: {args.max_no_assertion_pct}%)"
            )
            exit_code = 1

    if args.max_weak_assertion_pct > 0 and total > 0:
        pct = weak_assert / total * 100
        if pct > args.max_weak_assertion_pct:
            print(
                f"\nFAIL: {pct:.1f}% of tests have only weak (key-presence) assertions "
                f"(threshold: {args.max_weak_assertion_pct}%)"
            )
            exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
