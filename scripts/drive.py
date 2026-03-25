#!/usr/bin/env python3
"""Catalog-aware progress driver. data/operation_catalog.json IS the state.

Reads the MECE operation catalog and produces agent work items in priority order:
  fix_test → test → strengthen_test → implement

Running it repeatedly always picks up exactly what remains — no external state file.

Usage:
    uv run python scripts/drive.py                     # show full work queue
    uv run python scripts/drive.py --summary           # counts per category
    uv run python scripts/drive.py --category test     # filter by category
    uv run python scripts/drive.py --service cognito-idp  # one service
    uv run python scripts/drive.py --batch 5           # next 5 work items
    uv run python scripts/drive.py --json              # machine-readable (used by overnight.sh)
    uv run python scripts/drive.py --run               # execute headlessly via claude CLI
    uv run python scripts/drive.py --run --category test   # run one category
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

CATALOG = Path("data/operation_catalog.json")
CATEGORY_PRIORITY = ["fix_test", "test", "strengthen_test", "implement"]
DEFAULT_CHUNK = {"fix_test": 20, "test": 15, "strengthen_test": 20, "implement": 8}

_service_names_cache: dict | None = None


def _svc_names() -> dict:
    global _service_names_cache
    if _service_names_cache is None:
        sys.path.insert(0, str(Path(__file__).parent))
        from lib.service_names import resolve_all_services  # type: ignore[import]

        with _SuppressWarnings():
            _service_names_cache = resolve_all_services()
    return _service_names_cache


class _SuppressWarnings:  # noqa: N801
    def __enter__(self):
        import warnings

        self._filters = warnings.filters[:]
        warnings.filterwarnings("ignore")
        return self

    def __exit__(self, *_):
        import warnings

        warnings.filters[:] = self._filters


def load_catalog() -> dict[str, dict[str, list[dict]]]:
    """Returns {category: {service: [records]}}"""
    records = json.loads(CATALOG.read_text())
    grouped: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for r in records:
        cat = r["mece_category"]
        if cat in CATEGORY_PRIORITY:
            grouped[cat][r["service"]].append(r)
    return grouped


def build_queue(
    grouped: dict,
    category: str | None = None,
    service: str | None = None,
    max_ops: int | None = None,
) -> list[dict]:
    """Build an ordered list of work items.

    Each item: {category, service, ops, chunk_idx, total_chunks}
    Large services are split into chunks of max_ops (default from DEFAULT_CHUNK).
    """
    queue = []
    for cat in CATEGORY_PRIORITY:
        if category and cat != category:
            continue
        services = grouped.get(cat, {})
        chunk_size = max_ops or DEFAULT_CHUNK[cat]
        # Sort by total ops ASC so small services get done quickly first
        for svc in sorted(services, key=lambda s: len(services[s])):
            if service and svc != service:
                continue
            records = services[svc]
            # Split into chunks
            chunks = [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]
            for idx, chunk in enumerate(chunks):
                queue.append(
                    {
                        "category": cat,
                        "service": svc,
                        "ops": [r["operation"] for r in chunk],
                        "records": chunk,
                        "chunk_idx": idx,
                        "total_chunks": len(chunks),
                    }
                )
    return queue


def make_prompt(item: dict) -> str:
    """Generate the complete agent prompt for a work item."""
    cat = item["category"]
    svc = item["service"]
    ops = item["ops"]
    records = item["records"]

    names = _svc_names()
    sn = names.get(svc)
    botocore_name = sn.botocore if sn else svc
    moto_dir = sn.moto_dir if sn else svc.replace("-", "")
    test_stem = sn.test_stem if sn else svc.replace("-", "_")
    test_file = (
        f"tests/compatibility/test_{test_stem}_compat.py"
        if test_stem
        else f"tests/compatibility/test_{svc.replace('-', '_')}_compat.py"
    )
    chunk_label = (
        f" (chunk {item['chunk_idx'] + 1}/{item['total_chunks']})"
        if item["total_chunks"] > 1
        else ""
    )

    if cat == "test":
        return _prompt_test(svc, botocore_name, test_file, ops, chunk_label)
    if cat == "strengthen_test":
        fn_map = {r["operation"]: r["test_evidence"].get("test_functions", []) for r in records}
        return _prompt_strengthen(svc, botocore_name, test_file, ops, fn_map, chunk_label)
    if cat == "implement":
        return _prompt_implement(svc, botocore_name, moto_dir or "???", test_file, ops, chunk_label)
    if cat == "fix_test":
        fn_map = {r["operation"]: r["test_evidence"].get("test_functions", []) for r in records}
        return _prompt_fix_test(svc, test_file, ops, fn_map)
    return f"Work on {svc}: {ops}"


def _prompt_test(
    svc: str, botocore_name: str, test_file: str, ops: list[str], chunk_label: str
) -> str:
    ops_list = "\n".join(f"  - {op}" for op in ops)
    return f"""\
Write compat tests for **{svc}**{chunk_label} (botocore: `{botocore_name}`).
The catalog says Moto implements these but they have no compat tests yet.
Server is running on port 4566.

Operations ({len(ops)} total):
{ops_list}

## Steps

1. Probe the server to confirm which ops actually work:
   ```
   uv run python scripts/probe_service.py --service {svc} --all --json
   ```
   Ops returning 501/not_implemented → skip entirely. Test only confirmed-working ops.

2. Find or create the test file: `{test_file}`
   If it doesn't exist, create it following other test_*_compat.py files as a template.

3. For each working operation, write ONE test using the simplest pattern that applies:

   **List/describe (no setup needed):**
   ```python
   def test_list_things(self, client):
       result = client.list_things()
       assert "Things" in result
   ```

   **Non-existent resource (proves implementation):**
   ```python
   def test_describe_nonexistent(self, client):
       with pytest.raises(ClientError) as exc:
           client.describe_thing(ThingId="does-not-exist")
       assert exc.value.response["Error"]["Code"] in (
           "ResourceNotFoundException", "NotFoundException", "NoSuchEntity")
   ```

   **CRUD (only when the above don't apply):**
   ```python
   def test_describe_thing(self, client):
       resp = client.create_thing(Name="test-drive")
       thing_id = resp["ThingId"]
       try:
           result = client.describe_thing(ThingId=thing_id)
           assert "ThingId" in result
       finally:
           client.delete_thing(ThingId=thing_id)
   ```

4. Run each test IMMEDIATELY after writing it:
   `uv run pytest {test_file} -k "test_<name>" -q --tb=short`
   - Passes → keep
   - 501/NotImplemented → DELETE the test
   - Bad params → fix once; if still broken, skip

5. After all ops, run the full file and quality check:
   `uv run pytest {test_file} -q --tb=short`
   `uv run python scripts/validate_test_quality.py --file {test_file}`
   Delete any test the quality checker flags as not contacting the server.

6. Format, then commit:
   `uv run ruff format {test_file}`
   `uv run ruff check --fix {test_file}`
   `git add {test_file}`
   `git commit -m "test: add compat tests for {svc}{chunk_label} (N operations)"`

## Rules
- NEVER catch ParamValidationError — that's client-side, proves nothing
- NEVER write a test without asserting on a real response key
- If stuck on params for >2 min, skip the operation
- If 501, DELETE the test and move on
- Print exactly this when done: CHUNK_RESULT: added=N failed=M skipped=K
"""


def _prompt_strengthen(
    svc: str,
    botocore_name: str,
    test_file: str,
    ops: list[str],
    fn_map: dict[str, list[str]],
    chunk_label: str,
) -> str:
    fn_lines = []
    for op in ops:
        fns = fn_map.get(op, [])
        if fns:
            fn_lines.append(f"  - {op}: {', '.join(fns)}")
        else:
            fn_lines.append(f"  - {op}: (search in {test_file} for {op.lower()})")
    fn_list = "\n".join(fn_lines)

    return f"""\
Strengthen compat test assertions for **{svc}**{chunk_label} (botocore: `{botocore_name}`).
These tests pass but only assert on `ResponseMetadata` — not on any real response field.
Server is running on port 4566.

Tests to strengthen ({len(ops)} total):
{fn_list}

## Steps

1. For each operation, find its test function in `{test_file}`.

2. Look up the real response shape to see what keys are returned:
   ```
   python -c "
   import botocore.session
   s = botocore.session.get_session()
   m = s.get_service_model('{botocore_name}')
   op = m.operation_model('OPERATION_NAME')
   shape = op.output_shape
   print(list(shape.members.keys()) if shape and shape.members else 'empty body')
   "
   ```
   Replace OPERATION_NAME with the actual AWS operation name (PascalCase).

3. Add ONE meaningful assertion per test:
   - `assert "SomeKey" in resp` for the first non-metadata key
   - `assert isinstance(resp["Count"], int)` for numeric fields
   - `assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200` ONLY if the operation
     truly returns an empty body (confirm with botocore shape check above)

4. Run each modified test immediately:
   `uv run pytest {test_file} -k "TEST_FUNCTION_NAME" -q --tb=short`

5. Format and commit:
   `uv run ruff format {test_file}`
   `git add {test_file}`
   `git commit -m "test: strengthen assertions for {svc}{chunk_label}"`

## Rules
- ONE new assertion per test is enough — don't over-engineer
- If the test already has a real assertion (was fixed since catalog was built), skip it
- If running the test fails (operation returns 501), note it — don't add assertion
- Print exactly this when done: CHUNK_RESULT: added=N failed=M skipped=K
"""


def _prompt_implement(
    svc: str,
    botocore_name: str,
    moto_dir: str,
    test_file: str,
    ops: list[str],
    chunk_label: str,
) -> str:
    ops_list = "\n".join(f"  - {op}" for op in ops)
    return f"""\
Implement missing operations for **{svc}**{chunk_label} (botocore: `{botocore_name}`).
The catalog classifies these as unimplemented. Server is running on port 4566.

Operations ({len(ops)} total):
{ops_list}

## Steps

1. Probe to confirm current status (some may have been fixed since the catalog was built):
   ```
   uv run python scripts/probe_service.py --service {svc} --all 2>/dev/null
   ```
   Ops that now return `working` → write a test and move on (no impl needed).
   Ops that return `not_implemented` or `500_error` → implement.

2. For each not-implemented operation, check for a Moto stub:
   ```
   grep -n "def OPERATION_NAME\\|op_name" vendor/moto/moto/{moto_dir}/responses.py | head -20
   ```

3a. **Moto stub exists** (raises NotImplementedError or returns placeholder):
    - Implement in `vendor/moto/moto/{moto_dir}/models.py`
    - Wire the response in `vendor/moto/moto/{moto_dir}/responses.py`
    - Follow the patterns of the nearest implemented operation in the same file

3b. **No Moto stub**:
    - Add URL routing in `vendor/moto/moto/{moto_dir}/urls.py`
    - Add dispatch method in `responses.py`
    - Implement backend method in `models.py`

4. After each 2-3 operations: commit to vendor/moto and update the lockfile:
   ```
   cd vendor/moto
   git add moto/{moto_dir}/
   git commit -m "feat: implement {svc} OperationName"
   git push jackdanger HEAD:master
   cd ../..
   uv lock
   git add uv.lock
   ```

5. Write a compat test proving the operation works:
   - Add to `{test_file}`
   - Run it: `uv run pytest {test_file} -k "test_<name>" -q --tb=short`
   - Passes → keep and commit

6. After all ops: run the full test file:
   `uv run pytest {test_file} -q --tb=short`

## Rules
- Always read models.py before implementing — understand the data model first
- Never accumulate >200 lines between commits
- If an operation requires understanding a complex AWS resource model, implement
  just the happy path first — partial coverage is better than nothing
- Print exactly this when done: CHUNK_RESULT: added=N failed=M skipped=K
"""


def _prompt_fix_test(svc: str, test_file: str, ops: list[str], fn_map: dict) -> str:
    fn_lines = []
    for op in ops:
        fns = fn_map.get(op, [])
        if fns:
            fn_lines.append(f"  - {op}: {', '.join(fns)}")
        else:
            fn_lines.append(f"  - {op}: (search in {test_file})")
    fn_list = "\n".join(fn_lines)

    return f"""\
Fix broken compat tests for **{svc}**.
These tests catch ParamValidationError (never reach the server) or have no assertion.
Server is running on port 4566.

Tests to fix ({len(ops)} total):
{fn_list}

## Steps

1. For each test, run it to see what's happening:
   `uv run pytest {test_file} -k "TEST_FUNCTION_NAME" -xvs`

2. Diagnose and fix:
   - **Catches ParamValidationError**: The params are wrong client-side.
     Find valid params (check AWS docs or botocore shape) and provide them.
   - **No assertion**: Add `assert "SomeKey" in resp` or `assert status == 200`
   - **Returns 501**: DELETE the test — the operation isn't implemented

3. Quality check: `uv run python scripts/validate_test_quality.py --file {test_file}`
   All tests must show "contacts_server: true"

4. Format and commit:
   `uv run ruff format {test_file}`
   `git add {test_file}`
   `git commit -m "fix: repair broken compat tests for {svc}"`

## Rules
- Never use pytest.raises(ParamValidationError) — that's a client-side check
- If you can't find valid params in 2 minutes, delete the test
- Print exactly this when done: CHUNK_RESULT: added=N failed=M skipped=K
"""


def print_summary(grouped: dict) -> None:
    total_ops = sum(
        len(records) for cat in CATEGORY_PRIORITY for records in grouped.get(cat, {}).values()
    )
    print(f"\n{'Category':<20} {'Services':>10} {'Ops':>8}")
    print("-" * 42)
    for cat in CATEGORY_PRIORITY:
        services = grouped.get(cat, {})
        ops = sum(len(r) for r in services.values())
        print(f"{cat:<20} {len(services):>10} {ops:>8}")
    print("-" * 42)
    total_svcs = sum(len(grouped.get(c, {})) for c in CATEGORY_PRIORITY)
    print(f"{'REMAINING':<20} {total_svcs:>10} {total_ops:>8}")
    print()


def rebuild_catalog() -> None:
    with open("data/operation_catalog.json", "w") as f:
        subprocess.run(
            ["uv", "run", "python", "scripts/build_operation_catalog.py", "--json"],
            stdout=f,
            check=False,
        )


def run_claude(prompt: str, log_file: str | None = None) -> int:
    cmd = [
        "claude",
        "--output-format",
        "stream-json",
        "--verbose",
        "--permission-mode",
        "bypassPermissions",
        "-p",
        prompt,
    ]
    if log_file:
        with open(log_file, "w") as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
    else:
        result = subprocess.run(cmd)
    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(description="Catalog-aware progress driver")
    parser.add_argument("--category", choices=CATEGORY_PRIORITY, help="Filter by category")
    parser.add_argument("--service", help="Filter by service name")
    parser.add_argument("--batch", type=int, default=0, help="Limit to N work items")
    parser.add_argument("--max-ops", type=int, help="Max ops per work item (overrides defaults)")
    parser.add_argument("--json", action="store_true", dest="json_out", help="JSON output")
    parser.add_argument("--run", action="store_true", help="Execute via claude CLI")
    parser.add_argument("--summary", action="store_true", help="Show counts per category")
    parser.add_argument(
        "--log-dir", default="logs/overnight", help="Directory for claude session logs"
    )
    args = parser.parse_args()

    if not CATALOG.exists():
        print("ERROR: data/operation_catalog.json not found. Run: make catalog", file=sys.stderr)
        sys.exit(1)

    grouped = load_catalog()

    if args.summary:
        print_summary(grouped)
        return

    queue = build_queue(grouped, args.category, args.service, args.max_ops)

    if args.batch:
        queue = queue[: args.batch]

    if not queue:
        print("Nothing to do — all operations are done or skipped.", file=sys.stderr)
        return

    if args.json_out:
        output = []
        for item in queue:
            output.append(
                {
                    "category": item["category"],
                    "service": item["service"],
                    "ops": item["ops"],
                    "chunk_idx": item["chunk_idx"],
                    "total_chunks": item["total_chunks"],
                    "prompt": make_prompt(item),
                }
            )
        print(json.dumps(output, indent=2))
        return

    if not args.run:
        # Dry-run: show table
        print(f"\n{'CATEGORY':<20} {'SERVICE':<30} {'CHUNK':<10} {'OPS':>5}")
        print("-" * 68)
        for item in queue:
            chunk = (
                f"{item['chunk_idx'] + 1}/{item['total_chunks']}"
                if item["total_chunks"] > 1
                else "-"
            )
            print(f"{item['category']:<20} {item['service']:<30} {chunk:<10} {len(item['ops']):>5}")
        total_ops = sum(len(i["ops"]) for i in queue)
        print(f"\n{len(queue)} work items, {total_ops} operations\n")
        return

    # --run: execute headlessly via claude CLI
    import os
    import time

    os.makedirs(args.log_dir, exist_ok=True)

    for i, item in enumerate(queue):
        cat = item["category"]
        svc = item["service"]
        chunk_label = f"-chunk{item['chunk_idx'] + 1}" if item["total_chunks"] > 1 else ""
        ts = time.strftime("%Y%m%d-%H%M%S")
        log_file = f"{args.log_dir}/{ts}-{cat}-{svc}{chunk_label}.log"
        ln = f"{args.log_dir}/latest.log"
        if os.path.exists(ln) or os.path.islink(ln):
            os.remove(ln)
        os.symlink(os.path.basename(log_file), ln)

        print(f"\n[{i + 1}/{len(queue)}] {cat} / {svc}{chunk_label} ({len(item['ops'])} ops)")

        prompt = make_prompt(item)
        run_claude(prompt, log_file)

        # Rebuild catalog after each item so the queue reflects current reality
        print("  Rebuilding catalog...", end="", flush=True)
        rebuild_catalog()
        print(" done")

        # Re-check if this item is still needed (it may be done now)
        # (Next iteration will naturally skip done items since we reload the catalog)

    print("\nAll work items processed. Run `make catalog` for final summary.")


if __name__ == "__main__":
    main()
