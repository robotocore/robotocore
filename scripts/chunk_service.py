#!/usr/bin/env python3
"""Break a service into resource-group chunks for incremental test writing.

Groups operations by resource noun (e.g., Vpc, SecurityGroup, Instance)
and outputs chunks of 3-8 operations each — the right size for one
focused test-writing session.

Usage:
    uv run python scripts/chunk_service.py --service ec2
    uv run python scripts/chunk_service.py --service ec2 --json
    uv run python scripts/chunk_service.py --service ec2 --untested-only
    uv run python scripts/chunk_service.py --service sagemaker --with-probe
"""

import argparse
import json
import re
import sys
from pathlib import Path

import botocore.session

# Verbs to strip when extracting resource nouns
VERB_PREFIXES = (
    "Accept",
    "Activate",
    "Add",
    "Advertise",
    "Allocate",
    "Apply",
    "Assign",
    "Associate",
    "Attach",
    "Authorize",
    "Batch",
    "Bundle",
    "Cancel",
    "Confirm",
    "Copy",
    "Create",
    "Deactivate",
    "Delete",
    "Deregister",
    "Describe",
    "Detach",
    "Disable",
    "Disassociate",
    "Enable",
    "Execute",
    "Export",
    "Get",
    "Import",
    "Invoke",
    "List",
    "Modify",
    "Monitor",
    "Move",
    "Publish",
    "Put",
    "Reboot",
    "Register",
    "Reject",
    "Release",
    "Remove",
    "Replace",
    "Request",
    "Reset",
    "Restore",
    "Revoke",
    "Rotate",
    "Run",
    "Schedule",
    "Send",
    "Set",
    "Start",
    "Stop",
    "Subscribe",
    "Tag",
    "Terminate",
    "Unassign",
    "Unmonitor",
    "Unsubscribe",
    "Untag",
    "Update",
    "Verify",
    "Withdraw",
)


def _to_snake_case(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def extract_noun(op_name: str) -> str:
    """Extract the resource noun from an operation name."""
    for prefix in sorted(VERB_PREFIXES, key=len, reverse=True):
        if op_name.startswith(prefix):
            noun = op_name[len(prefix) :]
            if noun:
                return noun
    return op_name


def get_tested_operations(service_name: str) -> set[str]:
    """Get operations already tested in the compat file."""
    import ast

    test_dir = Path("tests/compatibility")
    # Try variations
    for name in [
        service_name,
        service_name.replace("-", "_"),
    ]:
        path = test_dir / f"test_{name}_compat.py"
        if path.exists():
            break
    else:
        return set()

    source = path.read_text()
    tested = set()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            method = node.func.attr
            pascal = "".join(w.capitalize() for w in method.split("_"))
            tested.add(pascal)
    return tested


def chunk_service(
    service_name: str,
    untested_only: bool = False,
    probe_data: dict | None = None,
) -> list[dict]:
    """Break a service into resource-group chunks.

    Returns list of chunks, each with:
      - noun: resource noun (e.g., "Vpc", "SecurityGroup")
      - operations: list of operation names
      - tested: list already tested
      - untested: list not yet tested
      - probe_status: dict of {op: status} if probe data provided
    """
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(service_name)
    except Exception:
        return []

    all_ops = sorted(model.operation_names)
    tested = get_tested_operations(service_name)

    # Build probe lookup
    probe_lookup = {}
    if probe_data:
        for op_info in probe_data.get("operations", []):
            probe_lookup[op_info["operation"]] = op_info["status"]

    # Group by resource noun
    groups: dict[str, list[str]] = {}
    for op in all_ops:
        noun = extract_noun(op)
        groups.setdefault(noun, []).append(op)

    # Build chunks
    chunks = []
    for noun, ops in sorted(groups.items()):
        tested_ops = [o for o in ops if o in tested]
        untested_ops = [o for o in ops if o not in tested]

        if untested_only and not untested_ops:
            continue

        chunk = {
            "noun": noun,
            "operations": ops,
            "tested": tested_ops,
            "untested": untested_ops,
            "size": len(ops),
            "untested_count": len(untested_ops),
        }

        if probe_lookup:
            chunk["probe_status"] = {o: probe_lookup.get(o, "unknown") for o in ops}
            chunk["working_untested"] = [
                o for o in untested_ops if probe_lookup.get(o) == "working"
            ]
            chunk["working_untested_count"] = len(chunk["working_untested"])

        chunks.append(chunk)

    # Sort: most working-untested first (most impactful)
    if probe_lookup:
        chunks.sort(key=lambda c: -c.get("working_untested_count", 0))
    else:
        chunks.sort(key=lambda c: -c["untested_count"])

    return chunks


def main():
    parser = argparse.ArgumentParser(description="Break service into resource-group chunks")
    parser.add_argument("--service", required=True, help="AWS service name")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument(
        "--untested-only",
        action="store_true",
        help="Only show chunks with untested ops",
    )
    parser.add_argument(
        "--with-probe",
        action="store_true",
        help="Include probe data (requires running server)",
    )
    parser.add_argument(
        "--probe-file",
        help="Path to pre-computed probe JSON file",
    )
    args = parser.parse_args()

    probe_data = None
    if args.probe_file:
        probe_data = json.loads(Path(args.probe_file).read_text())
    elif args.with_probe:
        import subprocess

        result = subprocess.run(
            [
                "uv",
                "run",
                "python",
                "scripts/probe_service.py",
                "--service",
                args.service,
                "--all",
                "--json",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            try:
                probe_data = json.loads(result.stdout)
            except json.JSONDecodeError:
                pass

    chunks = chunk_service(
        args.service,
        untested_only=args.untested_only,
        probe_data=probe_data,
    )

    if not chunks:
        print(f"No chunks found for {args.service}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(json.dumps(chunks, indent=2))
    else:
        total_untested = sum(c["untested_count"] for c in chunks)
        total_ops = sum(c["size"] for c in chunks)
        print(f"\n{args.service}: {len(chunks)} chunks, ", end="")
        print(f"{total_untested}/{total_ops} untested")
        print()

        for c in chunks:
            tested_mark = f"{len(c['tested'])}/{c['size']} tested"
            line = f"  {c['noun']:40s} {tested_mark:>15s}"
            if "working_untested_count" in c:
                line += f"  ({c['working_untested_count']} ready)"
            print(line)
            if c["untested"]:
                for op in c["untested"][:5]:
                    status = ""
                    if "probe_status" in c:
                        status = f" [{c['probe_status'].get(op, '?')}]"
                    print(f"    - {op}{status}")
                if len(c["untested"]) > 5:
                    print(f"    ... and {len(c['untested']) - 5} more")


if __name__ == "__main__":
    main()
