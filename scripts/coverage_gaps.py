#!/usr/bin/env python3
"""Analyze test coverage gaps by comparing source modules to test modules.

Identifies:
1. Source files with no corresponding test file
2. Public functions/classes in source with no test coverage
3. Lines-of-code per source file vs test file (ratio analysis)

Usage:
    uv run python scripts/coverage_gaps.py [--json] [--verbose]
"""

import ast
import os
import re
import sys
import json as json_mod

SRC_DIR = os.path.join(os.path.dirname(__file__), "..", "src", "robotocore")
TEST_DIR = os.path.join(os.path.dirname(__file__), "..", "tests")
UNIT_DIR = os.path.join(TEST_DIR, "unit")


def get_source_modules() -> dict[str, str]:
    """Return {module_path: absolute_file_path} for all source files."""
    modules = {}
    for root, dirs, files in os.walk(SRC_DIR):
        for f in files:
            if f.endswith(".py") and f != "__init__.py":
                abspath = os.path.join(root, f)
                relpath = os.path.relpath(abspath, SRC_DIR)
                modules[relpath] = abspath
    return modules


def get_test_modules() -> dict[str, str]:
    """Return {test_path: absolute_file_path} for all unit test files."""
    modules = {}
    for root, dirs, files in os.walk(UNIT_DIR):
        for f in files:
            if f.startswith("test_") and f.endswith(".py"):
                abspath = os.path.join(root, f)
                relpath = os.path.relpath(abspath, UNIT_DIR)
                modules[relpath] = abspath
    return modules


def extract_public_symbols(filepath: str) -> dict:
    """Extract public functions and classes from a Python file."""
    try:
        with open(filepath) as f:
            tree = ast.parse(f.read(), filename=filepath)
    except SyntaxError:
        return {"functions": [], "classes": [], "lines": 0}

    functions = []
    classes = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
            functions.append(node.name)
        elif isinstance(node, ast.AsyncFunctionDef) and not node.name.startswith("_"):
            functions.append(node.name)
        elif isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
            classes.append(node.name)

    with open(filepath) as f:
        lines = sum(1 for line in f if line.strip() and not line.strip().startswith("#"))

    return {"functions": functions, "classes": classes, "lines": lines}


def count_test_functions(filepath: str) -> int:
    """Count test functions in a file."""
    with open(filepath) as f:
        content = f.read()
    return len(re.findall(r"def test_", content))


def count_lines(filepath: str) -> int:
    """Count non-empty, non-comment lines."""
    with open(filepath) as f:
        return sum(1 for line in f if line.strip() and not line.strip().startswith("#"))


def find_tested_symbols(test_filepath: str) -> set[str]:
    """Extract symbol names referenced in test file."""
    with open(test_filepath) as f:
        content = f.read()
    # Look for imports from source and direct references
    symbols = set()
    # from robotocore.foo import bar, baz
    for m in re.finditer(r"from robotocore\S* import (.+)", content):
        for name in m.group(1).split(","):
            name = name.strip().split(" as ")[0].strip()
            symbols.add(name)
    return symbols


def source_to_test_path(src_rel: str) -> str:
    """Map a source path like 'gateway/router.py' to expected test path 'gateway/test_router.py'."""
    parts = src_rel.split(os.sep)
    filename = parts[-1]
    dirname = os.sep.join(parts[:-1])
    test_filename = f"test_{filename}"
    if dirname:
        return os.path.join(dirname, test_filename)
    return test_filename


def analyze():
    """Run the full coverage gap analysis."""
    src_modules = get_source_modules()
    test_modules = get_test_modules()

    # Also check for tests in services/ subdirectory
    test_lookup = {}
    for tpath, tabspath in test_modules.items():
        test_lookup[tpath] = tabspath
        # Also index by just the filename for fuzzy matching
        test_lookup[os.path.basename(tpath)] = tabspath

    results = []

    for src_rel, src_abs in sorted(src_modules.items()):
        expected_test = source_to_test_path(src_rel)
        src_info = extract_public_symbols(src_abs)

        # Try multiple test path patterns
        test_abs = None
        test_path_found = None
        candidates = [
            expected_test,
            f"test_{os.path.basename(src_rel)}",  # flat
            os.path.join("services", f"test_{os.path.basename(src_rel)}"),  # services/
        ]
        # For services/foo/provider.py -> services/test_foo_provider.py
        parts = src_rel.replace(os.sep, "/").split("/")
        if len(parts) >= 3 and parts[0] == "services":
            svc_name = parts[1]
            fname = parts[2]
            candidates.append(os.path.join("services", f"test_{svc_name}_{fname}"))
            candidates.append(f"test_{svc_name}_{fname}")

        for candidate in candidates:
            if candidate in test_lookup:
                test_abs = test_lookup[candidate]
                test_path_found = candidate
                break

        tested_symbols = set()
        test_count = 0
        test_lines = 0
        if test_abs:
            tested_symbols = find_tested_symbols(test_abs)
            test_count = count_test_functions(test_abs)
            test_lines = count_lines(test_abs)

        untested_fns = [fn for fn in src_info["functions"] if fn not in tested_symbols]
        untested_cls = [cls for cls in src_info["classes"] if cls not in tested_symbols]

        entry = {
            "source": src_rel,
            "source_lines": src_info["lines"],
            "test_file": test_path_found,
            "test_count": test_count,
            "test_lines": test_lines,
            "public_functions": len(src_info["functions"]),
            "public_classes": len(src_info["classes"]),
            "untested_functions": untested_fns,
            "untested_classes": untested_cls,
            "coverage_ratio": test_lines / src_info["lines"] if src_info["lines"] > 0 else 0,
            "has_tests": test_abs is not None,
        }
        results.append(entry)

    return results


def print_report(results, verbose=False):
    """Print a human-readable coverage gap report."""
    no_tests = [r for r in results if not r["has_tests"]]
    low_coverage = [r for r in results if r["has_tests"] and r["coverage_ratio"] < 0.5]
    good = [r for r in results if r["has_tests"] and r["coverage_ratio"] >= 0.5]

    print("=" * 72)
    print("  UNIT TEST COVERAGE GAP ANALYSIS")
    print("=" * 72)

    if no_tests:
        print(f"\n  FILES WITH NO UNIT TESTS ({len(no_tests)}):")
        print(f"  {'Source File':<45} {'Lines':>6}  {'Public Fns':>10}")
        print(f"  {'-'*45} {'-'*6}  {'-'*10}")
        for r in sorted(no_tests, key=lambda x: -x["source_lines"]):
            print(f"  {r['source']:<45} {r['source_lines']:>6}  {r['public_functions']:>10}")

    if low_coverage:
        print(f"\n  LOW COVERAGE (<50% test:source ratio) ({len(low_coverage)}):")
        print(f"  {'Source File':<45} {'Src':>5} {'Test':>5} {'Ratio':>6} {'#Tests':>6}")
        print(f"  {'-'*45} {'-'*5} {'-'*5} {'-'*6} {'-'*6}")
        for r in sorted(low_coverage, key=lambda x: x["coverage_ratio"]):
            print(f"  {r['source']:<45} {r['source_lines']:>5} {r['test_lines']:>5} {r['coverage_ratio']:>5.0%} {r['test_count']:>6}")

    if verbose:
        print(f"\n  GOOD COVERAGE ({len(good)}):")
        for r in sorted(good, key=lambda x: -x["coverage_ratio"]):
            print(f"  {r['source']:<45} {r['source_lines']:>5} {r['test_lines']:>5} {r['coverage_ratio']:>5.0%} {r['test_count']:>6}")

    # Untested public symbols
    all_untested_fns = []
    for r in results:
        for fn in r["untested_functions"]:
            all_untested_fns.append((r["source"], fn))

    if all_untested_fns and verbose:
        print(f"\n  UNTESTED PUBLIC FUNCTIONS ({len(all_untested_fns)}):")
        for src, fn in sorted(all_untested_fns):
            print(f"    {src}: {fn}()")

    # Summary
    total_src = sum(r["source_lines"] for r in results)
    total_test = sum(r["test_lines"] for r in results)
    tested_files = sum(1 for r in results if r["has_tests"])
    total_files = len(results)

    print(f"\n  {'='*60}")
    print(f"  Source files:        {total_files}")
    print(f"  With unit tests:     {tested_files}/{total_files} ({tested_files/total_files:.0%})")
    print(f"  Without unit tests:  {len(no_tests)}")
    print(f"  Source lines:        {total_src}")
    print(f"  Test lines:          {total_test}")
    print(f"  Overall ratio:       {total_test/total_src:.0%}" if total_src else "  Overall ratio:       N/A")
    print("=" * 72)

    return no_tests, low_coverage


def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    use_json = "--json" in sys.argv

    results = analyze()

    if use_json:
        print(json_mod.dumps(results, indent=2))
    else:
        no_tests, low_coverage = print_report(results, verbose=verbose)

    # Exit 1 if there are gaps (useful for CI)
    if no_tests:
        sys.exit(1)


if __name__ == "__main__":
    main()
