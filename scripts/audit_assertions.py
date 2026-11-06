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
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).parent.parent
COMPAT = ROOT / "tests" / "compatibility"


def collect_tests(match: str) -> list[tuple[Path, str]]:
    out = subprocess.run(
        [
            "uv", "run", "pytest", "tests/compatibility/",
            "--collect-only", "-q", "-p", "no:cacheprovider",
        ],
        cwd=ROOT, capture_output=True, text=True,
    )
    tests = []
    for line in out.stdout.splitlines():
        line = line.strip()
        if "::" not in line or line.startswith("="):
            continue
        node_id = line.split()[-1]
        if "[" in node_id:  # skip parametrized copies
            continue
        path_part, _, name = node_id.partition("::")
        if match and match not in node_id:
            continue
        tests.append((COMPAT / Path(path_part).relative_to("tests/compatibility"), name))
    return tests


def perturb(module_path: Path, test_name: str) -> Path | None:
    """Write a copy of the module whose target test's FIRST comparable
    literal is flipped. None when the test has no perturbable literal."""
    source = module_path.read_text()
    tree = ast.parse(source)

    class Flip(ast.NodeTransformer):
        def __init__(self):
            self.done = False

        def flip_compare(self, node):
            target = node.test if isinstance(node, ast.Assert) else node
            if not isinstance(target, ast.Compare):
                return None
            self.generic_visit(target)
            if not self.done:
                for i, comparator in enumerate(target.comparators):
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
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == test_name:
            f = Flip()
            for child in ast.walk(node):
                if isinstance(child, ast.Assert):
                    f.flip_compare(child)
            flipped_any = f.done
            break
    if not flipped_any:
        return None

    out = COMPAT / f"zz_audit_{uuid.uuid4().hex[:8]}_{module_path.name}"
    out.write_text(ast.unparse(tree))
    return out


def run_single(temp_file: Path, test_name: str) -> bool:
    """True when the perturbed test PASSES (the vacuous case)."""
    rel = temp_file.relative_to(ROOT)
    out = subprocess.run(
        [
            "uv", "run", "pytest", f"{rel}::{test_name}",
            "-q", "-p", "no:cacheprovider", "--no-header", "--tb=no",
        ],
        cwd=ROOT, capture_output=True, text=True, timeout=300,
    )
    return out.returncode == 0


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

    vacuous = []
    for i, (path, name) in enumerate(sample):
        try:
            temp_file = perturb(path, name)
        except SyntaxError:
            continue
        if temp_file is None:
            continue
        try:
            if run_single(temp_file, name):
                vacuous.append((path.name, name))
                print(f"  VACUOUS {path.name}::{name} — passes with a flipped expectation")
        finally:
            shutil.rmtree(temp_file, ignore_errors=True)
        if (i + 1) % 25 == 0:
            print(f"  … {i + 1}/{len(sample)} audited")

    print(f"\n{len(vacuous)} vacuous assertions found in {len(sample)} audited tests")
    return 1 if vacuous else 0


if __name__ == "__main__":
    sys.exit(main())
