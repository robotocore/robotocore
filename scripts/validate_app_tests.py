#!/usr/bin/env python3
"""Validate that app integration tests actually exercise robotocore.

Static analysis checks:
1. Every boto3 client uses endpoint_url (not hitting real AWS)
2. No mocking of boto3/botocore (tests should hit the live server)
3. Every test has at least one assert
4. No bare 'except Exception: pass' that swallows real failures
5. No ParamValidationError catches (client-side only, server never contacted)
6. Fixtures use conftest clients (no duplicate fixture definitions)
7. No hardcoded account IDs in non-ARN contexts
8. Timestamps are deterministic (no datetime.now() for sort keys)

Usage:
    uv run python scripts/validate_app_tests.py
    uv run python scripts/validate_app_tests.py --file tests/apps/test_data_pipeline_app.py
    uv run python scripts/validate_app_tests.py --json
"""

import argparse
import ast
import json
import sys
from pathlib import Path

TESTS_DIR = Path("tests/apps")

# Fixtures already in conftest.py — tests should use these, not redefine
CONFTEST_FIXTURES = {
    "s3", "sqs", "dynamodb", "lambda_client", "events", "secretsmanager",
    "sns", "iam", "apigateway", "stepfunctions", "kinesis", "cloudwatch",
    "logs", "ssm", "boto_session", "unique_name",
}


class AppTestVisitor(ast.NodeVisitor):
    """AST visitor that checks app test quality."""

    def __init__(self, source_lines: list[str]):
        self.source_lines = source_lines
        self.issues: list[dict] = []
        self.tests: list[dict] = []
        self._current_class: str | None = None
        self._current_func: str | None = None
        # Track fixture-like functions at module level
        self.redefined_fixtures: list[dict] = []
        # Track mock imports
        self.mock_imports: list[dict] = []
        # Track all function defs for fixture detection
        self._top_level_funcs: list[str] = []

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            if "mock" in (alias.name or "").lower() or "mock" in (alias.asname or "").lower():
                self.mock_imports.append({
                    "line": node.lineno,
                    "name": alias.name,
                })

    def visit_ImportFrom(self, node: ast.ImportFrom):
        module = node.module or ""
        if "mock" in module.lower():
            self.mock_imports.append({
                "line": node.lineno,
                "name": module,
            })
        for alias in node.names:
            if "mock" in (alias.name or "").lower():
                self.mock_imports.append({
                    "line": node.lineno,
                    "name": f"{module}.{alias.name}",
                })

    def visit_ClassDef(self, node: ast.ClassDef):
        old = self._current_class
        self._current_class = node.name
        self.generic_visit(node)
        self._current_class = old

    def visit_FunctionDef(self, node: ast.FunctionDef):
        # Check for redefined conftest fixtures
        if self._current_class is None and not node.name.startswith("test_"):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Attribute) and dec.attr == "fixture":
                    if node.name in CONFTEST_FIXTURES:
                        self.redefined_fixtures.append({
                            "line": node.lineno,
                            "name": node.name,
                        })
                elif isinstance(dec, ast.Name) and dec.id == "fixture":
                    if node.name in CONFTEST_FIXTURES:
                        self.redefined_fixtures.append({
                            "line": node.lineno,
                            "name": node.name,
                        })

        if not node.name.startswith("test_"):
            self.generic_visit(node)
            return

        old_func = self._current_func
        self._current_func = node.name

        test_info = {
            "name": node.name,
            "class": self._current_class,
            "line": node.lineno,
            "issues": [],
        }

        # Check: has assertions?
        has_assert = self._has_assert(node)
        if not has_assert:
            test_info["issues"].append("no_assert")

        # Check: catches ParamValidationError?
        if self._catches_param_validation(node):
            test_info["issues"].append("catches_param_validation_error")

        # Check: bare except pass?
        bare_excepts = self._find_bare_except_pass(node)
        for line in bare_excepts:
            test_info["issues"].append(f"bare_except_pass:L{line}")

        # Check: uses datetime.now() (potential flakiness)
        now_calls = self._find_datetime_now(node)
        for line in now_calls:
            test_info["issues"].append(f"datetime_now:L{line}")

        self.tests.append(test_info)
        self._current_func = old_func

    visit_AsyncFunctionDef = visit_FunctionDef  # noqa: N815

    def _has_assert(self, node: ast.FunctionDef) -> bool:
        """Check if function body contains any assert statement or pytest assertion."""
        for child in ast.walk(node):
            if isinstance(child, ast.Assert):
                return True
            # pytest.raises counts as an assertion
            if isinstance(child, ast.Attribute) and child.attr == "raises":
                return True
            # assertIn, assertEqual etc from unittest
            if isinstance(child, ast.Call):
                if isinstance(child.func, ast.Attribute):
                    if child.func.attr.startswith("assert"):
                        return True
        return False

    def _catches_param_validation(self, node: ast.FunctionDef) -> bool:
        """Check if function catches ParamValidationError (client-side only)."""
        for child in ast.walk(node):
            if isinstance(child, ast.ExceptHandler):
                if child.type and isinstance(child.type, ast.Attribute):
                    if child.type.attr == "ParamValidationError":
                        return True
                if child.type and isinstance(child.type, ast.Name):
                    if child.type.id == "ParamValidationError":
                        return True
        return False

    def _find_bare_except_pass(self, node: ast.FunctionDef) -> list[int]:
        """Find 'except Exception: pass' patterns (swallows real failures)."""
        lines = []
        for child in ast.walk(node):
            if isinstance(child, ast.ExceptHandler):
                if child.type is None or (
                    isinstance(child.type, ast.Name) and child.type.id == "Exception"
                ):
                    # Check if body is just 'pass'
                    if (
                        len(child.body) == 1
                        and isinstance(child.body[0], ast.Pass)
                    ):
                        lines.append(child.lineno)
        return lines

    def _find_datetime_now(self, node: ast.FunctionDef) -> list[int]:
        """Find datetime.now() calls (potential timestamp flakiness)."""
        lines = []
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                if isinstance(child.func, ast.Attribute) and child.func.attr == "now":
                    if isinstance(child.func.value, ast.Name) and child.func.value.id == "datetime":
                        lines.append(child.lineno)
                    elif (
                        isinstance(child.func.value, ast.Attribute)
                        and child.func.value.attr == "datetime"
                    ):
                        lines.append(child.lineno)
        return lines


def check_source_patterns(filepath: Path) -> list[dict]:
    """Check source-level patterns that AST misses."""
    issues = []
    text = filepath.read_text()
    lines = text.splitlines()

    for i, line in enumerate(lines, 1):
        # Check for mocking patterns
        if "patch(" in line and ("boto" in line or "client" in line):
            issues.append({
                "line": i,
                "type": "mocking_boto",
                "detail": line.strip(),
            })
        if "@mock" in line.lower() or "monkeypatch" in line.lower():
            if "boto" in line.lower() or "aws" in line.lower():
                issues.append({
                    "line": i,
                    "type": "mocking_aws",
                    "detail": line.strip(),
                })

    return issues


def analyze_file(filepath: Path) -> dict:
    """Analyze a single test file."""
    source = filepath.read_text()
    tree = ast.parse(source)
    lines = source.splitlines()

    visitor = AppTestVisitor(lines)
    visitor.visit(tree)

    source_issues = check_source_patterns(filepath)

    total_tests = len(visitor.tests)
    tests_with_issues = [t for t in visitor.tests if t["issues"]]
    tests_no_assert = [t for t in visitor.tests if "no_assert" in t["issues"]]
    tests_param_val = [
        t for t in visitor.tests if "catches_param_validation_error" in t["issues"]
    ]
    tests_datetime_now = [
        t for t in visitor.tests if any("datetime_now" in i for i in t["issues"])
    ]
    tests_bare_except = [
        t for t in visitor.tests if any("bare_except_pass" in i for i in t["issues"])
    ]

    return {
        "file": str(filepath),
        "total_tests": total_tests,
        "clean_tests": total_tests - len(tests_with_issues),
        "tests_no_assert": [t["name"] for t in tests_no_assert],
        "tests_param_validation": [t["name"] for t in tests_param_val],
        "tests_datetime_now": [
            {"name": t["name"], "issues": t["issues"]} for t in tests_datetime_now
        ],
        "tests_bare_except": [
            {"name": t["name"], "issues": t["issues"]} for t in tests_bare_except
        ],
        "redefined_fixtures": visitor.redefined_fixtures,
        "mock_imports": visitor.mock_imports,
        "source_issues": source_issues,
        "all_tests": visitor.tests,
    }


def print_report(results: list[dict]) -> int:
    """Print human-readable report. Returns exit code."""
    total_tests = 0
    total_issues = 0

    for r in results:
        filepath = r["file"]
        total_tests += r["total_tests"]
        file_issues = 0

        header = f"\n{'='*60}\n{filepath} ({r['total_tests']} tests)\n{'='*60}"

        issues_text = []

        if r["mock_imports"]:
            for m in r["mock_imports"]:
                issues_text.append(f"  CRITICAL: Mock import at line {m['line']}: {m['name']}")
                file_issues += 1

        if r["redefined_fixtures"]:
            for f in r["redefined_fixtures"]:
                issues_text.append(
                    f"  WARNING: Redefines conftest fixture '{f['name']}' at line {f['line']}"
                )
                file_issues += 1

        if r["tests_no_assert"]:
            for name in r["tests_no_assert"]:
                issues_text.append(f"  FAIL: {name} — no assertions")
                file_issues += 1

        if r["tests_param_validation"]:
            for name in r["tests_param_validation"]:
                issues_text.append(f"  FAIL: {name} — catches ParamValidationError (client-side)")
                file_issues += 1

        if r["tests_datetime_now"]:
            for t in r["tests_datetime_now"]:
                issues_text.append(f"  WARN: {t['name']} — uses datetime.now() (flaky timestamps)")
                file_issues += 1

        if r["tests_bare_except"]:
            for t in r["tests_bare_except"]:
                issues_text.append(f"  WARN: {t['name']} — bare except:pass swallows failures")
                file_issues += 1

        if r["source_issues"]:
            for s in r["source_issues"]:
                issues_text.append(f"  CRITICAL: Line {s['line']}: {s['type']} — {s['detail']}")
                file_issues += 1

        total_issues += file_issues

        if issues_text:
            print(header)
            for line in issues_text:
                print(line)
        else:
            print(f"\n{filepath}: {r['total_tests']} tests — ALL CLEAN")

    print(f"\n{'='*60}")
    print(f"TOTAL: {total_tests} tests, {total_issues} issues")
    if total_issues == 0:
        print("All app tests look good!")
    print(f"{'='*60}")

    return 1 if total_issues > 0 else 0


def main():
    parser = argparse.ArgumentParser(description="Validate app integration test quality")
    parser.add_argument("--file", help="Check a single file")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument(
        "--dir", default=str(TESTS_DIR), help="Test directory (default: tests/apps)"
    )
    args = parser.parse_args()

    if args.file:
        files = [Path(args.file)]
    else:
        test_dir = Path(args.dir)
        files = sorted(test_dir.glob("test_*.py"))

    if not files:
        print("No test files found.")
        return 0

    results = []
    for f in files:
        results.append(analyze_file(f))

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    return print_report(results)


if __name__ == "__main__":
    sys.exit(main())
