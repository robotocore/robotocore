#!/usr/bin/env python3
"""Validate that compat tests actually test server behavior.

Catches these classes of bad tests:
1. Tests that never contact the server (client-side ParamValidationError)
2. Tests with no assertions (catch-all exception handlers)
3. Tests that would pass against a stopped server

Also provides behavioral coverage tracking to detect CRUD patterns:
- CREATE: Calls Create*/Put* operations
- RETRIEVE: Calls Get*/Describe* with ID
- LIST: Calls List*/Describe* (plural)
- UPDATE: Calls Update*/Modify*/Set*
- DELETE: Calls Delete*/Remove*
- ERROR: Uses pytest.raises or catches ClientError

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

    # Behavioral coverage report
    uv run python scripts/validate_test_quality.py --behavioral

    # Behavioral coverage for a specific service
    uv run python scripts/validate_test_quality.py --behavioral --file tests/compatibility/test_sqs_compat.py
"""

import argparse
import ast
import json
import re
import sys
from pathlib import Path

TESTS_DIR = Path("tests/compatibility")

# Behavioral pattern detection - method prefixes for each pattern
BEHAVIORAL_PATTERNS = {
    "CREATE": ["Create", "Put", "Add", "Register", "Start", "Run", "Send", "Publish"],
    "RETRIEVE": ["Get", "Describe"],
    "LIST": ["List"],
    "UPDATE": ["Update", "Modify", "Set", "Change", "Enable", "Disable", "Tag", "Untag"],
    "DELETE": ["Delete", "Remove", "Terminate", "Stop", "Deregister", "Purge"],
    "ERROR": [],  # Detected via pytest.raises or ClientError handling
}


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


def _snake_to_pascal(name: str) -> str:
    """Convert snake_case to PascalCase.

    Examples:
        create_queue -> CreateQueue
        send_message_batch -> SendMessageBatch
        get_queue_url -> GetQueueUrl
    """
    return "".join(word.capitalize() for word in name.split("_"))


def _detect_behavioral_patterns(node: ast.FunctionDef) -> dict[str, bool]:
    """Detect which behavioral patterns a test covers.

    Returns a dict with keys: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR
    Each value is True if the test covers that pattern.
    """
    patterns = {
        "CREATE": False,
        "RETRIEVE": False,
        "LIST": False,
        "UPDATE": False,
        "DELETE": False,
        "ERROR": False,
    }

    for child in ast.walk(node):
        # Check for method calls that match pattern prefixes
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
            method_name = child.func.attr
            # Convert snake_case (boto3) to PascalCase (AWS operation name)
            pascal_name = _snake_to_pascal(method_name)

            for pattern, prefixes in BEHAVIORAL_PATTERNS.items():
                if pattern == "ERROR":
                    continue  # Handled separately
                for prefix in prefixes:
                    if pascal_name.startswith(prefix):
                        # Special case: Describe* plural is LIST, singular is RETRIEVE
                        if prefix == "Describe":
                            # Check if it's plural (ends in 's' or 'es')
                            noun = pascal_name[len(prefix) :]
                            if noun.endswith("s") or noun.endswith("es"):
                                patterns["LIST"] = True
                            else:
                                patterns["RETRIEVE"] = True
                        else:
                            patterns[pattern] = True
                        break

        # Check for pytest.raises (error handling)
        if isinstance(child, ast.Attribute) and child.attr == "raises":
            patterns["ERROR"] = True

        # Check for try/except ClientError
        if isinstance(child, ast.Try):
            for handler in child.handlers:
                if handler.type is not None:
                    if (
                        isinstance(handler.type, ast.Attribute)
                        and handler.type.attr == "ClientError"
                    ):
                        patterns["ERROR"] = True
                    elif isinstance(handler.type, ast.Name) and handler.type.id == "ClientError":
                        patterns["ERROR"] = True

    return patterns


def _calculate_behavioral_score(patterns: dict[str, bool]) -> tuple[int, int]:
    """Calculate behavioral coverage score.

    Returns (covered_count, total_count).
    """
    covered = sum(1 for v in patterns.values() if v)
    total = len(patterns)
    return covered, total


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

        # Detect behavioral patterns
        behavioral_patterns = _detect_behavioral_patterns(node)
        covered, total = _calculate_behavioral_score(behavioral_patterns)

        return {
            "quality": quality,
            "reason": reason,
            "has_assert": has_assert,
            "has_try_except": has_try_except,
            "catches_param_validation": catches_param_validation,
            "catches_client_error": catches_client_error,
            "all_excepts_pass": all_excepts_pass,
            "has_response_capture": has_response_capture,
            "behavioral_patterns": behavioral_patterns,
            "behavioral_score": covered,
            "behavioral_total": total,
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


def _get_behavioral_report(all_tests: list[dict], single_file: str | None) -> dict:
    """Generate behavioral coverage report data.

    Returns dict with behavioral coverage info.
    """
    # Extract service name from file path
    if single_file:
        match = re.search(r"test_(\w+)_compat\.py", single_file)
        service = match.group(1) if match else "unknown"
    else:
        service = "all services"

    # Calculate pattern coverage across all tests
    pattern_coverage = {p: 0 for p in BEHAVIORAL_PATTERNS}
    total_tests = len(all_tests)
    total_score = 0
    max_score = 0

    for test in all_tests:
        patterns = test.get("behavioral_patterns", {})
        for pattern, covered in patterns.items():
            if covered:
                pattern_coverage[pattern] += 1
        total_score += test.get("behavioral_score", 0)
        max_score += test.get("behavioral_total", 6)

    return {
        "behavioral_service": service,
        "overall_behavioral_coverage_pct": round(total_score / max_score * 100, 1)
        if max_score
        else 0,
        "pattern_coverage": {
            p: {"count": c, "pct": round(c / total_tests * 100, 1) if total_tests else 0}
            for p, c in pattern_coverage.items()
        },
        "behavioral_tests": [
            {
                "name": t["name"],
                "patterns": t.get("behavioral_patterns", {}),
                "score": t.get("behavioral_score", 0),
                "score_pct": round(
                    t.get("behavioral_score", 0) / t.get("behavioral_total", 6) * 100
                )
                if t.get("behavioral_total", 0)
                else 0,
            }
            for t in all_tests
        ],
    }


def _print_behavioral_report(all_tests: list[dict], single_file: str | None):
    """Print behavioral coverage report to stdout.

    Shows which CRUD patterns each test covers and calculates overall coverage.
    """
    report = _get_behavioral_report(all_tests, single_file)
    service = report["behavioral_service"]
    total_tests = len(all_tests)
    pattern_coverage = {p: d["count"] for p, d in report["pattern_coverage"].items()}

    print()
    print(f"Behavioral Coverage Report: {service}")
    print("=" * 70)
    print()

    # Overall summary
    overall_pct = report["overall_behavioral_coverage_pct"]
    total_score = sum(t.get("behavioral_score", 0) for t in all_tests)
    max_score = sum(t.get("behavioral_total", 6) for t in all_tests)
    print(f"Overall behavioral coverage: {overall_pct}% ({total_score}/{max_score} patterns)")
    print()

    # Pattern coverage summary
    print("Pattern coverage across all tests:")
    for pattern, count in pattern_coverage.items():
        pct = round(count / total_tests * 100, 1) if total_tests else 0
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        emoji = "✅" if pct >= 50 else "⚠️ " if pct >= 25 else "❌"
        print(f"  {pattern:<8s} {bar} {pct:>5.1f}% ({count}/{total_tests} tests) {emoji}")
    print()

    # Per-test breakdown (top 20 by score, then bottom 10)
    sorted_tests = sorted(all_tests, key=lambda t: t.get("behavioral_score", 0), reverse=True)

    print("Top tests by behavioral coverage:")
    for test in sorted_tests[:10]:
        patterns = test.get("behavioral_patterns", {})
        score = test.get("behavioral_score", 0)
        total = test.get("behavioral_total", 6)
        pct = round(score / total * 100) if total else 0
        pattern_str = "".join(f"{'✅' if patterns.get(p) else '❌'}" for p in BEHAVIORAL_PATTERNS)
        print(f"  {test['name']:<55s} {pattern_str} {pct:>3d}% ({score}/{total})")
    print()

    # Tests needing improvement (lowest scores)
    low_score_tests = [t for t in sorted_tests if t.get("behavioral_score", 0) <= 2]
    if low_score_tests:
        print(f"Tests needing improvement ({len(low_score_tests)} with <=2 patterns):")
        for test in sorted(low_score_tests, key=lambda t: t.get("behavioral_score", 0))[:15]:
            patterns = test.get("behavioral_patterns", {})
            score = test.get("behavioral_score", 0)
            total = test.get("behavioral_total", 6)
            pct = round(score / total * 100) if total else 0
            pattern_str = "".join(
                f"{'✅' if patterns.get(p) else '❌'}" for p in BEHAVIORAL_PATTERNS
            )
            print(f"  {test['name']:<55s} {pattern_str} {pct:>3d}% ({score}/{total})")
        print()

    # Pattern legend
    print("Pattern legend: C=CREATE R=RETRIEVE L=LIST U=UPDATE D=DELETE E=ERROR")


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
    parser.add_argument(
        "--behavioral",
        action="store_true",
        help="Show behavioral coverage report (CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR patterns)",
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
        # Add behavioral data if requested
        if args.behavioral:
            behavioral_data = _get_behavioral_report(all_tests, args.file)
            summary.update(behavioral_data)
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

    # Behavioral coverage report (non-JSON output)
    if args.behavioral and not args.json:
        _print_behavioral_report(all_tests, args.file)

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
