#!/usr/bin/env python3
"""Generate unit test stubs for source files that lack test coverage.

Reads source files, extracts public functions/classes, and generates
pytest test stubs. Won't overwrite existing test files.

Usage:
    uv run python scripts/gen_unit_tests.py                    # Preview what would be generated
    uv run python scripts/gen_unit_tests.py --write            # Actually write test files
    uv run python scripts/gen_unit_tests.py --file gateway/router.py  # Generate for specific file
"""

import ast
import os
import sys
import textwrap

SRC_DIR = os.path.join(os.path.dirname(__file__), "..", "src", "robotocore")
UNIT_DIR = os.path.join(os.path.dirname(__file__), "..", "tests", "unit")


def extract_signatures(filepath: str) -> list[dict]:
    """Extract function/class signatures from a Python source file."""
    with open(filepath) as f:
        source = f.read()

    try:
        tree = ast.parse(source, filename=filepath)
    except SyntaxError:
        return []

    signatures = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("_"):
                continue
            args = [a.arg for a in node.args.args if a.arg != "self"]
            is_async = isinstance(node, ast.AsyncFunctionDef)
            # Get return annotation if present
            returns = ast.unparse(node.returns) if node.returns else None
            signatures.append({
                "type": "function",
                "name": node.name,
                "args": args,
                "is_async": is_async,
                "returns": returns,
                "lineno": node.lineno,
            })
        elif isinstance(node, ast.ClassDef):
            if node.name.startswith("_"):
                continue
            methods = []
            for item in ast.iter_child_nodes(node):
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if item.name.startswith("_") and item.name != "__init__":
                        continue
                    args = [a.arg for a in item.args.args if a.arg != "self"]
                    methods.append({
                        "name": item.name,
                        "args": args,
                        "is_async": isinstance(item, ast.AsyncFunctionDef),
                    })
            signatures.append({
                "type": "class",
                "name": node.name,
                "methods": methods,
                "lineno": node.lineno,
            })

    return signatures


def src_rel_to_import(src_rel: str) -> str:
    """Convert 'gateway/router.py' to 'robotocore.gateway.router'."""
    module = src_rel.replace(os.sep, ".").replace("/", ".").removesuffix(".py")
    return f"robotocore.{module}"


def src_rel_to_test_path(src_rel: str) -> str:
    """Convert 'gateway/router.py' to 'tests/unit/gateway/test_router.py'.
    For services/foo/bar.py -> tests/unit/services/test_foo_bar.py
    """
    parts = src_rel.replace(os.sep, "/").split("/")
    if len(parts) >= 3 and parts[0] == "services":
        # services/sqs/models.py -> services/test_sqs_models.py
        svc = parts[1]
        fname = parts[2]
        return os.path.join(UNIT_DIR, "services", f"test_{svc}_{fname}")
    elif len(parts) >= 2:
        dirname = os.path.join(*parts[:-1])
        return os.path.join(UNIT_DIR, dirname, f"test_{parts[-1]}")
    else:
        return os.path.join(UNIT_DIR, f"test_{parts[0]}")


def generate_test_content(src_rel: str, signatures: list[dict]) -> str:
    """Generate a test file's content for the given source module."""
    module_import = src_rel_to_import(src_rel)
    module_name = module_import.split(".")[-1]

    lines = [
        f'"""Unit tests for {module_import}."""\n',
        "",
        "import pytest",
        "from unittest.mock import MagicMock, patch",
        "",
    ]

    # Build import line
    importable = []
    for sig in signatures:
        if sig["type"] == "function":
            importable.append(sig["name"])
        elif sig["type"] == "class":
            importable.append(sig["name"])

    if importable:
        names = ", ".join(importable)
        lines.append(f"from {module_import} import {names}")
        lines.append("")
        lines.append("")

    for sig in signatures:
        if sig["type"] == "function":
            fn_name = sig["name"]
            test_class = f"Test{_to_pascal(fn_name)}"
            lines.append(f"class {test_class}:")
            lines.append(f'    """Tests for {fn_name}()."""')
            lines.append("")

            # Basic test: function exists and is callable
            lines.append(f"    def test_{fn_name}_exists(self):")
            lines.append(f"        assert callable({fn_name})")
            lines.append("")

            # Test with basic args
            if sig["args"]:
                lines.append(f"    def test_{fn_name}_basic(self):")
                lines.append(f'        """Test {fn_name} with basic arguments."""')
                args_str = ", ".join(f"{a}=None" for a in sig["args"])
                if sig["is_async"]:
                    lines.append(f"        # TODO: async test")
                    lines.append(f"        pass")
                else:
                    lines.append(f"        # TODO: implement with real arguments")
                    lines.append(f"        pass")
                lines.append("")

            lines.append("")

        elif sig["type"] == "class":
            cls_name = sig["name"]
            lines.append(f"class Test{cls_name}:")
            lines.append(f'    """Tests for {cls_name}."""')
            lines.append("")

            if not sig["methods"]:
                lines.append(f"    def test_{_to_snake(cls_name)}_instantiates(self):")
                lines.append(f"        # TODO: test instantiation")
                lines.append(f"        pass")
                lines.append("")
            else:
                for method in sig["methods"]:
                    mname = method["name"]
                    if mname == "__init__":
                        lines.append(f"    def test_init(self):")
                        lines.append(f'        """Test {cls_name} construction."""')
                        lines.append(f"        # TODO: test with real arguments")
                        lines.append(f"        pass")
                    else:
                        lines.append(f"    def test_{mname}(self):")
                        lines.append(f'        """Test {cls_name}.{mname}()."""')
                        lines.append(f"        # TODO: implement")
                        lines.append(f"        pass")
                    lines.append("")

            lines.append("")

    return "\n".join(lines)


def _to_pascal(name: str) -> str:
    """Convert snake_case to PascalCase."""
    return "".join(word.capitalize() for word in name.split("_"))


def _to_snake(name: str) -> str:
    """Convert PascalCase to snake_case."""
    import re
    s = re.sub(r"([A-Z])", r"_\1", name).lower().lstrip("_")
    return s


def find_gaps() -> list[dict]:
    """Find source files without corresponding unit tests."""
    gaps = []
    for root, dirs, files in os.walk(SRC_DIR):
        for f in files:
            if f.endswith(".py") and f != "__init__.py":
                src_abs = os.path.join(root, f)
                src_rel = os.path.relpath(src_abs, SRC_DIR)
                test_path = src_rel_to_test_path(src_rel)

                if not os.path.exists(test_path):
                    sigs = extract_signatures(src_abs)
                    if sigs:  # Only if there are public symbols to test
                        gaps.append({
                            "source": src_rel,
                            "source_abs": src_abs,
                            "test_path": test_path,
                            "signatures": sigs,
                        })
    return gaps


def main():
    write = "--write" in sys.argv
    target_file = None
    for i, arg in enumerate(sys.argv):
        if arg == "--file" and i + 1 < len(sys.argv):
            target_file = sys.argv[i + 1]

    gaps = find_gaps()

    if target_file:
        gaps = [g for g in gaps if target_file in g["source"]]

    if not gaps:
        print("No coverage gaps found — all source files have unit tests!")
        return

    print(f"Found {len(gaps)} source files without unit tests:\n")

    for gap in sorted(gaps, key=lambda g: g["source"]):
        src = gap["source"]
        test_path = gap["test_path"]
        sigs = gap["signatures"]
        fn_count = sum(1 for s in sigs if s["type"] == "function")
        cls_count = sum(1 for s in sigs if s["type"] == "class")

        print(f"  {src}")
        print(f"    -> {os.path.relpath(test_path)}")
        print(f"    {fn_count} functions, {cls_count} classes")

        if write:
            content = generate_test_content(src, sigs)
            os.makedirs(os.path.dirname(test_path), exist_ok=True)
            with open(test_path, "w") as f:
                f.write(content)
            print(f"    WRITTEN: {test_path}")
        print()

    if not write:
        print(f"Run with --write to generate {len(gaps)} test files.")


if __name__ == "__main__":
    main()
