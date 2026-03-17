#!/usr/bin/env python3
"""Record and compare AWS response snapshots.

Record mode: Call operations against real AWS and capture response structure
(keys + types, not values). Store as golden files in contracts/.

Compare mode: Load golden files, run same ops against robotocore, diff structure.

Usage:
    # Record from real AWS (needs AWS_ACCESS_KEY_ID)
    uv run python scripts/record_aws_snapshots.py record --service s3 --service dynamodb

    # Compare robotocore against golden files (no creds needed)
    uv run python scripts/record_aws_snapshots.py compare --service s3
    uv run python scripts/record_aws_snapshots.py compare --all

    # Show what's recorded
    uv run python scripts/record_aws_snapshots.py list
"""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import botocore.exceptions
import botocore.loaders

from scripts.lib.param_filler import get_params_for_operation, to_snake_case

CONTRACTS_DIR = Path("contracts")

# Safe operations to record (non-destructive)
SAFE_PREFIXES = ("List", "Describe", "Get")


def _extract_shape_tree(value: Any, max_depth: int = 5) -> dict:
    """Extract the structural shape of a value (keys + types, not values).

    Returns a dict describing the shape tree:
      {"type": "structure", "members": {"Key": {"type": "string"}, ...}}
    """
    if max_depth <= 0:
        return {"type": type(value).__name__}

    if isinstance(value, dict):
        members = {}
        for k, v in value.items():
            if k == "ResponseMetadata":
                continue
            members[k] = _extract_shape_tree(v, max_depth - 1)
        return {"type": "structure", "members": members}
    elif isinstance(value, list):
        if value:
            return {"type": "list", "member": _extract_shape_tree(value[0], max_depth - 1)}
        return {"type": "list", "member": {"type": "unknown"}}
    elif isinstance(value, str):
        return {"type": "string"}
    elif isinstance(value, bool):
        return {"type": "boolean"}
    elif isinstance(value, int):
        return {"type": "integer"}
    elif isinstance(value, float):
        return {"type": "float"}
    elif isinstance(value, bytes):
        return {"type": "blob"}
    elif value is None:
        return {"type": "null"}
    else:
        # datetime, etc.
        return {"type": type(value).__name__}


def _get_safe_operations(service_name: str) -> list[str]:
    """Get safe operations for a service."""
    loader = botocore.loaders.Loader()
    try:
        api = loader.load_service_model(service_name, "service-2")
    except Exception:
        return []
    operations = api.get("operations", {})
    return sorted(n for n in operations if n.startswith(SAFE_PREFIXES))


def _diff_shapes(expected: dict, actual: dict, path: str = "") -> list[str]:
    """Diff two shape trees. Returns list of mismatch descriptions."""
    diffs: list[str] = []

    exp_type = expected.get("type", "unknown")
    act_type = actual.get("type", "unknown")

    if exp_type != act_type:
        # Allow int/float interchange
        if not ({exp_type, act_type} <= {"integer", "float"}):
            diffs.append(f"{path}: type mismatch (AWS={exp_type}, local={act_type})")
            return diffs

    if exp_type == "structure":
        exp_members = expected.get("members", {})
        act_members = actual.get("members", {})

        for key in exp_members:
            if key not in act_members:
                diffs.append(f"{path}.{key}: missing in local response")
            else:
                diffs.extend(_diff_shapes(exp_members[key], act_members[key], f"{path}.{key}"))

        for key in act_members:
            if key not in exp_members:
                diffs.append(f"{path}.{key}: extra key not in AWS response")

    elif exp_type == "list":
        exp_member = expected.get("member", {})
        act_member = actual.get("member", {})
        if exp_member and act_member:
            diffs.extend(_diff_shapes(exp_member, act_member, f"{path}[]"))

    return diffs


def record_service(service_name: str, endpoint: str | None = None) -> dict:
    """Record response shapes from AWS (or a local endpoint).

    Returns a contract dict suitable for saving.
    """
    kwargs: dict[str, Any] = {
        "service_name": service_name,
        "region_name": "us-east-1",
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        kwargs["aws_access_key_id"] = "testing"
        kwargs["aws_secret_access_key"] = "testing"

    client = boto3.client(**kwargs)
    operations = _get_safe_operations(service_name)

    recorded: list[dict] = []
    for op_name in operations:
        params = get_params_for_operation(service_name, op_name)
        if params is None:
            params = {}

        try:
            method = getattr(client, to_snake_case(op_name))
            response = method(**params)
            shape_tree = _extract_shape_tree(response)
            recorded.append(
                {
                    "operation": op_name,
                    "status": "success",
                    "shape": shape_tree,
                }
            )
        except botocore.exceptions.ParamValidationError:
            recorded.append(
                {
                    "operation": op_name,
                    "status": "param_error",
                    "shape": None,
                }
            )
        except Exception as e:
            error_code = ""
            if hasattr(e, "response"):
                error_code = e.response.get("Error", {}).get("Code", "")
            recorded.append(
                {
                    "operation": op_name,
                    "status": "error",
                    "error_code": error_code,
                    "shape": None,
                }
            )

    return {
        "service": service_name,
        "recorded_at": datetime.now(UTC).isoformat(),
        "source": "aws" if not endpoint else endpoint,
        "operations": recorded,
    }


def save_contract(service_name: str, contract: dict) -> Path:
    """Save a contract to the contracts directory."""
    CONTRACTS_DIR.mkdir(parents=True, exist_ok=True)
    path = CONTRACTS_DIR / f"{service_name}.json"
    with open(path, "w") as f:
        json.dump(contract, f, indent=2)
        f.write("\n")
    return path


def compare_service(service_name: str, endpoint: str = "http://localhost:4566") -> dict:
    """Compare robotocore responses against golden contract files."""
    contract_path = CONTRACTS_DIR / f"{service_name}.json"
    if not contract_path.exists():
        return {
            "service": service_name,
            "status": "no_contract",
            "message": f"No golden file at {contract_path}",
        }

    with open(contract_path) as f:
        contract = json.load(f)

    # Record from robotocore
    local = record_service(service_name, endpoint=endpoint)

    # Build lookup of AWS shapes
    aws_shapes = {}
    for op in contract.get("operations", []):
        if op.get("shape"):
            aws_shapes[op["operation"]] = op["shape"]

    local_shapes = {}
    for op in local.get("operations", []):
        if op.get("shape"):
            local_shapes[op["operation"]] = op["shape"]

    # Diff
    results: list[dict] = []
    for op_name, aws_shape in aws_shapes.items():
        if op_name not in local_shapes:
            results.append(
                {
                    "operation": op_name,
                    "status": "missing",
                    "diffs": [f"{op_name}: no local response (error or skipped)"],
                }
            )
            continue

        diffs = _diff_shapes(aws_shape, local_shapes[op_name], op_name)
        results.append(
            {
                "operation": op_name,
                "status": "fail" if diffs else "pass",
                "diffs": diffs,
            }
        )

    passed = sum(1 for r in results if r["status"] == "pass")
    failed = sum(1 for r in results if r["status"] == "fail")
    missing = sum(1 for r in results if r["status"] == "missing")

    return {
        "service": service_name,
        "status": "pass" if failed == 0 and missing == 0 else "fail",
        "summary": {"pass": passed, "fail": failed, "missing": missing},
        "operations": results,
    }


def cmd_record(args):
    """Record command handler."""
    for service in args.service:
        print(f"Recording {service}...")
        contract = record_service(service, endpoint=args.endpoint)
        n_success = sum(1 for o in contract["operations"] if o["status"] == "success")
        n_total = len(contract["operations"])
        path = save_contract(service, contract)
        print(f"  {n_success}/{n_total} operations recorded → {path}")
    return 0


def cmd_compare(args):
    """Compare command handler."""
    if args.all:
        if not CONTRACTS_DIR.exists():
            print("No contracts directory found. Run 'record' first.")
            return 1
        services = sorted(p.stem for p in CONTRACTS_DIR.glob("*.json"))
    else:
        services = args.service or []

    if not services:
        print("Specify --service or --all")
        return 1

    any_failed = False
    for service in services:
        result = compare_service(service, endpoint=args.endpoint)

        if args.json:
            print(json.dumps(result, indent=2))
        else:
            if result["status"] == "no_contract":
                print(f"{service}: {result['message']}")
                continue

            s = result["summary"]
            status = "PASS" if result["status"] == "pass" else "FAIL"
            print(
                f"{service}: {s['pass']} pass, {s['fail']} fail, {s['missing']} missing — {status}"
            )

            for op in result["operations"]:
                if op["status"] != "pass":
                    for diff in op["diffs"]:
                        print(f"  {diff}")

        if result.get("status") == "fail":
            any_failed = True

    return 1 if any_failed else 0


def cmd_list(args):
    """List command handler."""
    if not CONTRACTS_DIR.exists():
        print("No contracts directory. Run 'record' first.")
        return 0

    for path in sorted(CONTRACTS_DIR.glob("*.json")):
        with open(path) as f:
            data = json.load(f)
        n_ops = len(data.get("operations", []))
        n_success = sum(1 for o in data.get("operations", []) if o["status"] == "success")
        recorded = data.get("recorded_at", "unknown")
        source = data.get("source", "unknown")
        print(
            f"  {path.stem:25s} {n_success}/{n_ops} ops  recorded={recorded[:10]}  source={source}"
        )

    return 0


def main():
    parser = argparse.ArgumentParser(description="Record and compare AWS response snapshots")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Record subcommand
    record_parser = subparsers.add_parser("record", help="Record from AWS")
    record_parser.add_argument("--service", action="append", required=True)
    record_parser.add_argument(
        "--endpoint",
        default=None,
        help="Endpoint URL (omit to use real AWS)",
    )

    # Compare subcommand
    compare_parser = subparsers.add_parser("compare", help="Compare against golden files")
    compare_parser.add_argument("--service", action="append")
    compare_parser.add_argument("--all", action="store_true")
    compare_parser.add_argument(
        "--endpoint",
        default="http://localhost:4566",
        help="Endpoint URL for robotocore",
    )
    compare_parser.add_argument("--json", action="store_true")

    # List subcommand
    subparsers.add_parser("list", help="List recorded contracts")

    args = parser.parse_args()

    if args.command == "record":
        return cmd_record(args)
    elif args.command == "compare":
        return cmd_compare(args)
    elif args.command == "list":
        return cmd_list(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
