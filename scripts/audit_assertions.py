"""Assertion-strength audit (inverse mutation): for a sampled set of
compat tests, perturb one expected literal per test and re-run the test
from a temporary copy inside the same package (so conftest fixtures
resolve). A test that STILL PASSES with its expectation flipped is
vacuous — it cannot detect the semantic error it is written to detect.
This is the mirror of mutation-testing the implementation: it audits
the TEST side.

Usage (server must be running):
    uv run python scripts/audit_assertions.py --sample 150 [--seed 7]
Exit 1 when any perturbed test survives (a vacuous assertion).
"""

import argparse
import ast
import random
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).parent.parent
COMPAT = ROOT / "tests" / "compatibility"


def collect_tests(match: str) -> list[tuple[Path, str, str]]:
    out = subprocess.run(
        [
            "uv",
            "run",
            "pytest",
            "tests/compatibility/",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    tests = []
    for line in out.stdout.splitlines():
        line = line.strip()
        if "::" not in line or line.startswith("="):
            continue
        node_id = line.split()[-1]
        if "[" in node_id:  # skip parametrized copies
            continue
        path_part, _, qualified = node_id.partition("::")
        if match and match not in node_id:
            continue
        # Class-qualified ids ("TestClass::test_x") carry the bare method
        # name last — the function name the AST can match on.
        bare = qualified.rpartition("::")[2]
        tests.append((COMPAT / Path(path_part).relative_to("tests/compatibility"), bare, qualified))
    return tests


def perturb(module_path: Path, qualified: str) -> Path | None:
    """Write a copy of the module whose target test's FIRST comparable
    literal is flipped. `qualified` is the full pytest tail
    ("Class::method" or "method") — duplicate method names across classes
    are common here, and editing the wrong same-named function once
    flagged honest tests as vacuous. None when the test has no
    perturbable literal."""
    source = module_path.read_text()
    tree = ast.parse(source)
    parts = qualified.split("::")
    target_class, target_method = parts[0], parts[-1]

    class Flip(ast.NodeTransformer):
        def __init__(self):
            self.done = False

        def flip_compare(self, node):
            target = node.test if isinstance(node, ast.Assert) else node
            if not isinstance(target, ast.Compare):
                return None
            # Only flip EQUALITY comparisons: weakening a `<=` bound (1 → 2)
            # makes the assert looser but not vacuous — flagged hits there
            # are artifacts. An `==` flip genuinely inverts the expectation.
            if not any(isinstance(op, ast.Eq) for op in target.ops):
                return None
            self.generic_visit(target)
            if not self.done:
                for i, (op, comparator) in enumerate(zip(target.ops, target.comparators)):
                    if not isinstance(op, ast.Eq):
                        continue
                    new = self.flip_constant(comparator)
                    if new is not None:
                        target.comparators[i] = new
                        self.done = True
                        break
            return node

        @staticmethod
        def flip_constant(node):
            if not isinstance(node, ast.Constant):
                if isinstance(node, (ast.List, ast.Tuple, ast.Set)) and node.elts:
                    first = node.elts[0]
                    flipped = Flip.flip_constant(first)
                    if flipped is not None:
                        node.elts = [flipped] + node.elts[1:]
                        return node
                return None
            v = node.value
            if isinstance(v, bool):
                return ast.Constant(value=not v)
            if isinstance(v, int):
                return ast.Constant(value=v + 1)
            if isinstance(v, float):
                return ast.Constant(value=v + 1.0)
            if isinstance(v, str) and v:
                return ast.Constant(value=("X" if v[0] != "X" else "Y") + v[1:])
            if isinstance(v, bytes) and v:
                return ast.Constant(value=b"X" + v[1:])
            return None

    flipped_any = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != target_class:
            continue
        for item in node.body:
            if isinstance(item, ast.FunctionDef) and item.name == target_method:
                f = Flip()
                for child in ast.walk(item):
                    if isinstance(child, ast.Assert):
                        f.flip_compare(child)
                flipped_any = f.done
                break
        break
    if not flipped_any:
        return None

    out = COMPAT / f"zz_audit_{uuid.uuid4().hex[:8]}_{module_path.name}"
    out.write_text(ast.unparse(tree))
    return out


def run_single(temp_file: Path, qualified: str, out_dir: Path) -> tuple[str, str]:
    """Classify the perturbed run: "vacuous" (PASSED with a flipped
    expectation), "skipped", or "caught" (failed as it should)."""
    rel = temp_file.relative_to(ROOT)
    junit = out_dir / "audit_result.xml"
    out = subprocess.run(
        [
            "uv",
            "run",
            "pytest",
            f"{rel}::{qualified}",
            "-q",
            "-p",
            "no:cacheprovider",
            "--no-header",
            "--tb=no",
            f"--junitxml={junit}",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    verdict = "caught"
    try:
        import xml.etree.ElementTree as ET

        tree = ET.parse(junit)
        for case in tree.iter("testcase"):
            if case.find("failure") is not None or case.find("error") is not None:
                verdict = "caught"
            elif case.find("skipped") is not None:
                verdict = "skipped"
            else:
                verdict = "vacuous"
    except Exception:
        verdict = "caught" if out.returncode != 0 else "vacuous"
    junit.unlink(missing_ok=True)
    return verdict, (out.stdout + out.stderr)[-400:]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=int, default=150)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--match", default="", help="substring filter on node ids")
    args = parser.parse_args()

    tests = collect_tests(args.match)
    rng = random.Random(args.seed)
    sample = rng.sample(tests, min(args.sample, len(tests)))
    print(f"auditing {len(sample)} of {len(tests)} tests (seed {args.seed})")

    work_dir = Path(tempfile.mkdtemp(prefix="assert-audit-"))
    vacuous = []
    skipped = 0
    for i, (path, bare, qualified) in enumerate(sample):
        try:
            temp_file = perturb(path, bare)
        except SyntaxError:
            continue
        if temp_file is None:
            continue
        try:
            verdict, _tail = run_single(temp_file, qualified, work_dir)
            if verdict == "vacuous":
                vacuous.append((path.name, qualified))
                print(f"  VACUOUS {path.name}::{qualified} — passes with a flipped expectation")
            elif verdict == "skipped":
                skipped += 1
        finally:
            temp_file.unlink(missing_ok=True)
        if (i + 1) % 25 == 0:
            print(f"  … {i + 1}/{len(sample)} audited")

    import shutil as _shutil

    _shutil.rmtree(work_dir, ignore_errors=True)
    print(
        f"\n{len(vacuous)} vacuous assertions found in {len(sample)} audited tests"
        f" ({skipped} skipped for unavailable runtimes)"
    )
    return 1 if vacuous else 0


if __name__ == "__main__":
    sys.exit(main())
