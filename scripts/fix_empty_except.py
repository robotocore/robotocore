#!/usr/bin/env python3
"""Lint and auto-fix empty ``except: pass`` blocks.

Detects ``except ...: pass`` without an explanatory comment and applies
context-aware fixes:

- **src/robotocore/**: Replaces ``pass`` with ``logger.debug(...)`` so that
  swallowed exceptions are visible in CI/debug logs.  Ensures the file has
  ``import logging`` and a module-level ``logger``.
- **tests/**: Adds an explanatory comment after ``pass`` (cleanup is intentional
  in test fixtures and teardown).

Usage:
    uv run python scripts/fix_empty_except.py                  # dry-run, all files
    uv run python scripts/fix_empty_except.py --write           # apply fixes
    uv run python scripts/fix_empty_except.py --file tests/     # specific path
    uv run python scripts/fix_empty_except.py --check           # exit 1 if any found (CI gate)
"""

from __future__ import annotations

import argparse
import ast
import glob
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Classification: which comment/log message to attach
# ---------------------------------------------------------------------------

_CLEANUP_VERBS = frozenset(
    {
        "delete",
        "remove",
        "destroy",
        "deregister",
        "detach",
        "release",
        "stop",
        "terminate",
        "purge",
        "clean",
        "close",
        "cancel",
        "unsubscribe",
        "unbind",
        "shutdown",
        "kill",
        "drain",
        "abort",
        "disassociate",
        "revoke",
        "drop",
    }
)


def _calls_in_try(try_node: ast.Try) -> list[str]:
    """Return lowercased function/method names called in the try body."""
    names: list[str] = []
    for node in ast.walk(try_node):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                names.append(node.func.attr.lower())
            elif isinstance(node.func, ast.Name):
                names.append(node.func.id.lower())
    return names


def _is_cleanup_try(try_node: ast.Try) -> bool:
    """True if the try body looks like resource cleanup."""
    for name in _calls_in_try(try_node):
        for verb in _CLEANUP_VERBS:
            if verb in name:
                return True
    return False


def _enclosing_function(node: ast.AST) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    parent = getattr(node, "_parent", None)
    while parent:
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return parent
        parent = getattr(parent, "_parent", None)
    return None


def _is_fixture(func: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for d in func.decorator_list:
        if isinstance(d, ast.Name) and d.id == "fixture":
            return True
        if isinstance(d, ast.Attribute) and d.attr == "fixture":
            return True
        if isinstance(d, ast.Call):
            f = d.func
            if isinstance(f, ast.Attribute) and f.attr == "fixture":
                return True
            if isinstance(f, ast.Name) and f.id == "fixture":
                return True
    return False


def _function_has_yield(func: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for node in ast.walk(func):
        if isinstance(node, (ast.Yield, ast.YieldFrom)):
            return True
    return False


def _exc_type_name(handler: ast.ExceptHandler) -> str | None:
    if handler.type is None:
        return None
    if isinstance(handler.type, ast.Name):
        return handler.type.id
    if isinstance(handler.type, ast.Attribute):
        return handler.type.attr
    if isinstance(handler.type, ast.Tuple):
        parts = []
        for elt in handler.type.elts:
            if isinstance(elt, ast.Name):
                parts.append(elt.id)
            elif isinstance(elt, ast.Attribute):
                parts.append(elt.attr)
        return ", ".join(parts) if parts else None
    return None


def _classify_comment(handler: ast.ExceptHandler, try_node: ast.Try) -> str:
    """Return the comment string for test files."""
    func = _enclosing_function(handler)
    exc_name = _exc_type_name(handler)

    if func and (_is_fixture(func) or _function_has_yield(func)):
        return "best-effort cleanup"
    if _is_cleanup_try(try_node):
        return "best-effort cleanup"
    if exc_name in ("ClientError", "BotoCoreError"):
        return "resource may already be cleaned up"
    if exc_name in (
        "NoSuchEntity",
        "NoSuchEntityException",
        "NotFoundException",
        "ResourceNotFoundException",
        "NoSuchBucket",
        "ResourceAlreadyExistsException",
    ):
        return "resource may not exist"
    if exc_name in ("FileNotFoundError", "OSError", "IOError"):
        return "file may not exist"
    if exc_name in ("KeyError", "IndexError", "AttributeError"):
        return "key/index may be absent"
    if exc_name in ("ValueError", "TypeError"):
        return "conversion may fail; not critical"
    if exc_name in ("TimeoutError", "ConnectionError"):
        return "timeout/connection failure; non-fatal"
    if exc_name == "JSONDecodeError":
        return "malformed JSON; non-fatal"
    if func and func.name.startswith("test_"):
        return "best-effort cleanup"
    return "intentionally ignored"


def _classify_log_msg(handler: ast.ExceptHandler, try_node: ast.Try) -> str:
    """Return the log message for src/ files."""
    func = _enclosing_function(handler)
    func_name = func.name if func else "<module>"
    calls = _calls_in_try(try_node)
    if calls:
        action = calls[0]
    elif _is_cleanup_try(try_node):
        action = "cleanup"
    else:
        action = "operation"
    return f"{func_name}: {action} failed (non-fatal): %s"


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------


def _set_parents(tree: ast.AST) -> None:
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child._parent = node  # type: ignore[attr-defined]


def _find_try_for_handler(tree: ast.AST, handler: ast.ExceptHandler) -> ast.Try | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Try):
            if handler in node.handlers:
                return node
    return None


class Finding:
    __slots__ = (
        "path",
        "line",
        "col",
        "comment",
        "log_msg",
        "pass_lineno",
        "except_lineno",
        "has_exc_name",
        "is_src",
    )

    def __init__(
        self,
        path: str,
        line: int,
        col: int,
        comment: str,
        log_msg: str,
        pass_lineno: int,
        except_lineno: int,
        has_exc_name: bool,
        is_src: bool,
    ):
        self.path = path
        self.line = line
        self.col = col
        self.comment = comment
        self.log_msg = log_msg
        self.pass_lineno = pass_lineno
        self.except_lineno = except_lineno
        self.has_exc_name = has_exc_name
        self.is_src = is_src


def _is_src_file(filepath: str) -> bool:
    return filepath.startswith("src/")


def scan_file(filepath: str) -> list[Finding]:
    source = Path(filepath).read_text()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    _set_parents(tree)
    lines = source.splitlines()
    findings: list[Finding] = []
    is_src = _is_src_file(filepath)

    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        body = node.body
        if len(body) != 1 or not isinstance(body[0], ast.Pass):
            continue
        pass_node = body[0]
        pass_line = lines[pass_node.lineno - 1]
        if "#" in pass_line:
            continue  # already has a comment

        # For src files, also skip if there's already a logger.debug call
        # (shouldn't happen with pass, but be safe)

        try_node = _find_try_for_handler(tree, node)
        if not try_node:
            continue

        comment = _classify_comment(node, try_node)
        log_msg = _classify_log_msg(node, try_node)
        has_exc_name = node.name is not None  # `except Foo as exc:`

        findings.append(
            Finding(
                path=filepath,
                line=node.lineno,
                col=node.col_offset,
                comment=comment,
                log_msg=log_msg,
                pass_lineno=pass_node.lineno,
                except_lineno=node.lineno,
                has_exc_name=has_exc_name,
                is_src=is_src,
            )
        )

    return findings


# ---------------------------------------------------------------------------
# Fixing
# ---------------------------------------------------------------------------


def _ensure_logger(lines: list[str]) -> list[str]:
    """Ensure the file has `import logging` and `logger = logging.getLogger(__name__)`."""
    has_import = False
    has_logger = False
    last_import_idx = 0

    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "import logging" or stripped.startswith("from logging"):
            has_import = True
        if re.match(r"^logger\s*=\s*logging\.getLogger", stripped):
            has_logger = True
        if stripped.startswith("import ") or stripped.startswith("from "):
            last_import_idx = i

    additions: list[str] = []
    insert_idx = last_import_idx + 1

    if not has_import:
        additions.append("import logging\n")
    if not has_logger:
        additions.append("\nlogger = logging.getLogger(__name__)\n")

    if additions:
        for j, add_line in enumerate(additions):
            lines.insert(insert_idx + j, add_line)

    return lines


def fix_file(filepath: str, findings: list[Finding]) -> str:
    """Return the fixed source."""
    source = Path(filepath).read_text()
    lines = source.splitlines(keepends=True)
    is_src = any(f.is_src for f in findings)
    needs_logger = is_src and any(not f.has_exc_name or True for f in findings)

    if is_src and needs_logger:
        lines = _ensure_logger(lines)
        # Recalculate line offsets — the logger insertion may shift lines
        # Re-parse to get accurate positions
        new_source = "".join(lines)
        try:
            ast.parse(new_source)
        except SyntaxError:
            # If we broke the file, fall back to comment-only mode
            lines = Path(filepath).read_text().splitlines(keepends=True)
            is_src = False
            needs_logger = False

    if is_src and needs_logger:
        # Re-scan to get updated line numbers after logger insertion
        new_source = "".join(lines)
        new_findings = scan_file_from_source(filepath, new_source)
        if new_findings:
            findings = new_findings

    # Process in reverse line order so line numbers stay valid
    for f in sorted(findings, key=lambda f: f.pass_lineno, reverse=True):
        idx = f.pass_lineno - 1
        if idx >= len(lines):
            continue
        line = lines[idx]
        rstripped = line.rstrip("\n\r")

        if not rstripped.rstrip().endswith("pass"):
            continue

        if f.is_src:
            # Replace pass with logger.debug(...)
            indent = line[: len(line) - len(line.lstrip())]
            log_line = f'{indent}logger.debug("{f.log_msg}", exc)\n'
            lines[idx] = log_line

            # Ensure the except line binds the exception
            exc_idx = f.except_lineno - 1
            if exc_idx < len(lines):
                exc_line = lines[exc_idx]
                # If no `as exc:` binding, add one
                if " as " not in exc_line and exc_line.rstrip().endswith(":"):
                    # `except Exception:` → `except Exception as exc:`
                    lines[exc_idx] = exc_line.rstrip().rstrip(":") + " as exc:\n"
                elif " as " not in exc_line and "pass" not in exc_line:
                    # Multi-line or unusual format — try the simple pattern
                    exc_stripped = exc_line.rstrip("\n\r")
                    if exc_stripped.endswith(":"):
                        lines[exc_idx] = exc_stripped[:-1] + " as exc:\n"
        else:
            # Add comment after pass
            lines[idx] = rstripped.rstrip() + f"  # {f.comment}\n"

    return "".join(lines)


def scan_file_from_source(filepath: str, source: str) -> list[Finding]:
    """Scan from already-modified source (for re-scanning after logger insertion)."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    _set_parents(tree)
    src_lines = source.splitlines()
    findings: list[Finding] = []
    is_src = _is_src_file(filepath)

    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        body = node.body
        if len(body) != 1 or not isinstance(body[0], ast.Pass):
            continue
        pass_node = body[0]
        pass_line = src_lines[pass_node.lineno - 1]
        if "#" in pass_line:
            continue

        try_node = _find_try_for_handler(tree, node)
        if not try_node:
            continue

        comment = _classify_comment(node, try_node)
        log_msg = _classify_log_msg(node, try_node)

        findings.append(
            Finding(
                path=filepath,
                line=node.lineno,
                col=node.col_offset,
                comment=comment,
                log_msg=log_msg,
                pass_lineno=pass_node.lineno,
                except_lineno=node.lineno,
                has_exc_name=node.name is not None,
                is_src=is_src,
            )
        )

    return findings


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def collect_files(paths: list[str]) -> list[str]:
    result: list[str] = []
    for p in paths:
        pp = Path(p)
        if pp.is_file() and pp.suffix == ".py":
            result.append(str(pp))
        elif pp.is_dir():
            result.extend(sorted(glob.glob(str(pp / "**/*.py"), recursive=True)))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Fix empty except:pass blocks")
    parser.add_argument("--write", action="store_true", help="Apply fixes (default: dry-run)")
    parser.add_argument("--check", action="store_true", help="Exit 1 if any findings (CI gate)")
    parser.add_argument(
        "--file", nargs="+", default=["src/", "tests/"], help="Files or directories to scan"
    )
    args = parser.parse_args()

    files = collect_files(args.file)
    total_findings: list[Finding] = []

    for filepath in files:
        findings = scan_file(filepath)
        if not findings:
            continue
        total_findings.extend(findings)
        if not args.check:
            for f in findings:
                mode = "logger.debug" if f.is_src else f"# {f.comment}"
                print(f"  {f.path}:{f.pass_lineno}: except:pass → {mode}")

    if not total_findings:
        print("No empty except:pass blocks found.")
        return 0

    src_count = sum(1 for f in total_findings if f.is_src)
    test_count = len(total_findings) - src_count
    print(
        f"\n{len(total_findings)} empty except:pass blocks "
        f"({src_count} src → logger.debug, {test_count} tests → comment) "
        f"in {len(files)} files scanned."
    )

    if args.write:
        by_file: dict[str, list[Finding]] = {}
        for f in total_findings:
            by_file.setdefault(f.path, []).append(f)

        fixed = 0
        for filepath, findings in sorted(by_file.items()):
            new_source = fix_file(filepath, findings)
            Path(filepath).write_text(new_source)
            fixed += len(findings)
        print(f"Fixed {fixed} blocks in {len(by_file)} files.")

    if args.check:
        print(f"FAIL: {len(total_findings)} empty except:pass blocks found.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
