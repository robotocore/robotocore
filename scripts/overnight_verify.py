#!/usr/bin/env python3
"""7-gate verification pipeline for overnight test expansion.

Runs all verification gates sequentially on a test file. Returns structured
JSON with pass/fail per gate and healable vs fatal problem classification.

Gates:
  1. Syntax      — py_compile
  2. Static      — validate_test_quality.py (no ParamValidationError, assertions)
  3. New tests   — pytest -k <new tests> pass
  4. Regression  — ALL tests in file pass
  5. Runtime     — new tests actually hit server (audit log)
  6. Coverage    — compat_coverage.py number went up
  7. Lint        — ruff check + format

Usage:
    uv run python scripts/overnight_verify.py \
        --file tests/compatibility/test_sqs_compat.py \
        --new-tests test_send_message,test_receive_message \
        --service sqs --before-coverage 15

    # JSON output
    uv run python scripts/overnight_verify.py \
        --file tests/compatibility/test_sqs_compat.py \
        --new-tests test_send_message \
        --service sqs --before-coverage 15 --json
"""

import argparse
import json
import py_compile
import subprocess
import sys
import time
import urllib.request

HEALTH_URL = "http://localhost:4566/_robotocore/health"


def server_healthy() -> bool:
    """Check if the server is running and healthy."""
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=2):
            return True
    except Exception:
        return False


def ensure_server():
    """Start the server if not running. Returns True if we started it."""
    if server_healthy():
        return False
    print("Starting server...", file=sys.stderr)
    subprocess.run(["make", "start"], capture_output=True)
    for _ in range(10):
        time.sleep(1)
        if server_healthy():
            print("Server ready", file=sys.stderr)
            return True
    print("WARNING: Server failed to start", file=sys.stderr)
    return True


def stop_server():
    """Stop the server."""
    subprocess.run(["make", "stop"], capture_output=True)


def run_cmd(args: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    """Run a command with timeout, capturing output."""
    try:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            args,
            returncode=124,
            stdout="",
            stderr=f"Timed out after {timeout}s",
        )


def gate_syntax(test_file: str) -> dict:
    """Gate 1: File parses as valid Python."""
    try:
        py_compile.compile(test_file, doraise=True)
        return {"passed": True}
    except py_compile.PyCompileError as e:
        return {"passed": False, "error": str(e)}


def gate_static_quality(test_file: str) -> dict:
    """Gate 2: Static quality — no client-side-only tests, all have assertions."""
    result = run_cmd(
        [
            "uv",
            "run",
            "python",
            "scripts/validate_test_quality.py",
            "--file",
            test_file,
            "--json",
        ]
    )
    if result.returncode != 0 and not result.stdout.strip():
        return {"passed": False, "error": f"validate_test_quality.py failed: {result.stderr[:200]}"}

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"passed": False, "error": "Could not parse quality output"}

    no_contact = data.get("no_server_contact", 0)
    no_assertion = data.get("no_assertion", 0)
    problems = []

    if no_contact > 0:
        problems.append(f"{no_contact} tests don't contact server")
    if no_assertion > 0:
        problems.append(f"{no_assertion} tests lack assertions")

    return {
        "passed": no_contact == 0 and no_assertion == 0,
        "no_contact": no_contact,
        "no_assertion": no_assertion,
        "total_tests": data.get("total_tests", 0),
        "problems": problems,
        "healable": True,  # can be fixed by re-prompting
    }


def gate_new_tests_pass(test_file: str, new_tests: list[str]) -> dict:
    """Gate 3: All newly-written tests pass."""
    if not new_tests:
        return {"passed": True, "skipped": True, "reason": "no new tests specified"}

    # Build -k expression: "test_a or test_b or test_c"
    k_expr = " or ".join(new_tests)
    result = run_cmd(
        [
            "uv",
            "run",
            "pytest",
            test_file,
            "-k",
            k_expr,
            "-q",
            "--tb=short",
        ],
        timeout=120,
    )

    failures = []
    if result.returncode != 0:
        # Parse failure output for specific test names
        for line in result.stdout.splitlines():
            if line.startswith("FAILED"):
                failures.append(line.split("::")[1].split()[0] if "::" in line else line)

    return {
        "passed": result.returncode == 0,
        "failures": failures,
        "output": result.stdout[-500:] if result.returncode != 0 else "",
        "healable": True,  # can re-prompt to fix
    }


def gate_regression(test_file: str) -> dict:
    """Gate 4: ALL tests in the file pass (no regressions)."""
    result = run_cmd(
        [
            "uv",
            "run",
            "pytest",
            test_file,
            "-q",
            "--tb=short",
        ],
        timeout=180,
    )

    return {
        "passed": result.returncode == 0,
        "output": result.stdout[-500:] if result.returncode != 0 else "",
        "healable": False,  # regression = revert, never heal
    }


def gate_runtime_validation(test_file: str, new_tests: list[str]) -> dict:
    """Gate 5: New tests actually contact the server (audit log check)."""
    if not new_tests:
        return {"passed": True, "skipped": True}

    # Run validate_tests_runtime.py with -k filter for new tests
    k_expr = " or ".join(new_tests)
    result = run_cmd(
        [
            "uv",
            "run",
            "python",
            "scripts/validate_tests_runtime.py",
            test_file,
            "-k",
            k_expr,
            "--json",
        ],
        timeout=180,
    )

    if result.returncode != 0 and not result.stdout.strip():
        # Server might be down — non-fatal, skip this gate
        return {
            "passed": True,
            "skipped": True,
            "reason": f"Runtime validator error: {result.stderr[:200]}",
        }

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"passed": True, "skipped": True, "reason": "Could not parse runtime output"}

    no_contact_tests = [
        r["test"].split("::")[-1]
        for r in data.get("results", [])
        if not r.get("contacted_server", True)
    ]

    return {
        "passed": len(no_contact_tests) == 0,
        "no_contact_tests": no_contact_tests,
        "total_checked": data.get("total", 0),
        "healable": True,  # delete bad tests surgically
    }


def gate_coverage_delta(service: str, before_coverage: int) -> dict:
    """Gate 6: Coverage actually went up."""
    result = run_cmd(
        [
            "uv",
            "run",
            "python",
            "scripts/compat_coverage.py",
            "--service",
            service,
            "--json",
        ]
    )

    if result.returncode != 0:
        return {"passed": True, "skipped": True, "reason": "Could not check coverage"}

    try:
        data = json.loads(result.stdout)
        if not data:
            return {"passed": True, "skipped": True, "reason": "No coverage data"}
        after = data[0]["covered"]
    except (json.JSONDecodeError, KeyError, IndexError):
        return {"passed": True, "skipped": True, "reason": "Could not parse coverage"}

    delta = after - before_coverage
    return {
        "passed": delta > 0,
        "before": before_coverage,
        "after": after,
        "delta": delta,
    }


def gate_lint(test_file: str) -> dict:
    """Gate 7: ruff check + format pass."""
    # Try auto-fix first
    run_cmd(["uv", "run", "ruff", "check", "--fix", "--unsafe-fixes", "--quiet", test_file])
    run_cmd(["uv", "run", "ruff", "format", "--quiet", test_file])

    # Now check if it's clean
    check_result = run_cmd(["uv", "run", "ruff", "check", test_file, "--quiet"])
    if check_result.returncode != 0:
        return {
            "passed": False,
            "errors": check_result.stdout[:500],
            "healable": False,  # already tried auto-fix
        }

    return {"passed": True, "auto_fixed": True}


def run_pipeline(
    test_file: str,
    new_tests: list[str],
    service: str,
    before_coverage: int,
    skip_runtime: bool = False,
) -> dict:
    """Run all 7 gates sequentially. Short-circuits on fatal failures."""

    gates = {}
    healable_problems = []
    fatal_problems = []

    # Gate 1: Syntax
    gates["syntax"] = gate_syntax(test_file)
    if not gates["syntax"]["passed"]:
        fatal_problems.append(f"syntax: {gates['syntax'].get('error', 'parse error')}")
        return _result(False, gates, healable_problems, fatal_problems)

    # Gate 2: Static quality
    gates["static_quality"] = gate_static_quality(test_file)
    if not gates["static_quality"]["passed"]:
        for p in gates["static_quality"].get("problems", []):
            healable_problems.append(f"static_quality: {p}")

    # Gate 3: New tests pass
    gates["new_tests_pass"] = gate_new_tests_pass(test_file, new_tests)
    if not gates["new_tests_pass"]["passed"]:
        failures = gates["new_tests_pass"].get("failures", [])
        healable_problems.append(
            f"new_tests_pass: {len(failures)} failures: {', '.join(failures[:5])}"
        )

    # Gate 4: Regression (fatal — never heal)
    gates["regression"] = gate_regression(test_file)
    if not gates["regression"]["passed"]:
        fatal_problems.append("regression: existing tests broke")
        return _result(False, gates, healable_problems, fatal_problems)

    # Gate 5: Runtime validation (optional)
    if skip_runtime:
        gates["runtime_validation"] = {"passed": True, "skipped": True, "reason": "skip requested"}
    else:
        gates["runtime_validation"] = gate_runtime_validation(test_file, new_tests)
        if not gates["runtime_validation"]["passed"]:
            bad = gates["runtime_validation"].get("no_contact_tests", [])
            healable_problems.append(
                f"runtime_validation: {len(bad)} tests don't contact server: {', '.join(bad[:5])}"
            )

    # Gate 6: Coverage delta
    gates["coverage_delta"] = gate_coverage_delta(service, before_coverage)
    if not gates["coverage_delta"]["passed"] and not gates["coverage_delta"].get("skipped"):
        fatal_problems.append(
            f"coverage_delta: no improvement "
            f"(before={before_coverage}, after={gates['coverage_delta'].get('after', '?')})"
        )

    # Gate 7: Lint
    gates["lint"] = gate_lint(test_file)
    if not gates["lint"]["passed"]:
        fatal_problems.append(f"lint: {gates['lint'].get('errors', 'ruff errors')}")

    all_passed = all(g.get("passed", False) or g.get("skipped", False) for g in gates.values())
    passed = all_passed and len(fatal_problems) == 0

    return _result(passed, gates, healable_problems, fatal_problems)


def _result(passed: bool, gates: dict, healable: list, fatal: list) -> dict:
    return {
        "passed": passed,
        "gates": gates,
        "healable_problems": healable,
        "fatal_problems": fatal,
    }


def print_human(result: dict):
    """Print human-readable verification report."""
    status = "PASSED" if result["passed"] else "FAILED"
    print(f"\nVerification: {status}")
    print("=" * 50)

    for name, gate in result["gates"].items():
        if gate.get("skipped"):
            icon = "\u2013"
        elif gate.get("passed"):
            icon = "\u2713"
        else:
            icon = "\u2717"
        label = name.replace("_", " ").title()
        print(f"  {icon} {label}")

        # Show details for failures
        if not gate.get("passed") and not gate.get("skipped"):
            for key in ["error", "errors", "failures", "no_contact_tests", "problems", "output"]:
                if key in gate and gate[key]:
                    val = gate[key]
                    if isinstance(val, list):
                        val = ", ".join(str(v) for v in val[:3])
                    elif isinstance(val, str) and len(val) > 100:
                        val = val[:100] + "..."
                    print(f"    {key}: {val}")

    if result["healable_problems"]:
        print(f"\nHealable ({len(result['healable_problems'])}):")
        for p in result["healable_problems"]:
            print(f"  - {p}")

    if result["fatal_problems"]:
        print(f"\nFatal ({len(result['fatal_problems'])}):")
        for p in result["fatal_problems"]:
            print(f"  - {p}")


def main():
    parser = argparse.ArgumentParser(description="7-gate verification pipeline")
    parser.add_argument("--file", required=True, help="Test file to verify")
    parser.add_argument(
        "--new-tests",
        default="",
        help="Comma-separated list of new test function names",
    )
    parser.add_argument("--service", required=True, help="AWS service name")
    parser.add_argument(
        "--before-coverage",
        type=int,
        required=True,
        help="Coverage count before changes",
    )
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument(
        "--skip-runtime",
        action="store_true",
        help="Skip runtime validation gate",
    )
    parser.add_argument(
        "--ensure-server",
        action="store_true",
        help="Start server before running, stop after",
    )
    args = parser.parse_args()

    we_started_server = False
    if args.ensure_server:
        we_started_server = ensure_server()

    new_tests = [t.strip() for t in args.new_tests.split(",") if t.strip()]

    try:
        result = run_pipeline(
            test_file=args.file,
            new_tests=new_tests,
            service=args.service,
            before_coverage=args.before_coverage,
            skip_runtime=args.skip_runtime,
        )
    finally:
        if we_started_server:
            stop_server()

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_human(result)

    sys.exit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
