#!/usr/bin/env python3
"""Check and fix Moto responses.py _get_param() calls against botocore input wire names.

For each handler method in a Moto responses.py file, determines the correct wire key
that botocore sends in the JSON body and checks whether _get_param("X") uses it.

The "wire name" is NOT the botocore shape name — it's the serialization key that actually
appears in the HTTP request body. For REST-JSON services this is camelCase via locationName;
for plain JSON services it's the shape name directly (which may also be camelCase).

Usage:
    uv run python scripts/fix_moto_param_names.py --service logs
    uv run python scripts/fix_moto_param_names.py --service logs --write
    uv run python scripts/fix_moto_param_names.py --all
    uv run python scripts/fix_moto_param_names.py --all --write
    uv run python scripts/fix_moto_param_names.py --services athena config ecr iot
"""

import argparse
import re
import sys
from pathlib import Path

import botocore.session

VENDOR_MOTO = Path(__file__).resolve().parent.parent / "vendor" / "moto" / "moto"

# Botocore service name for directory names that differ
SERVICE_NAME_MAP = {
    "ses": "ses",
    "opensearchserverless": "opensearchserverless",
    "databrew": "databrew",
}

# Body locations — URI, header, querystring params are NOT read via _get_param
SKIP_LOCATIONS = {"header", "headers", "uri", "querystring", "statusCode"}

# Methods that are not AWS operations (internal, routing, etc.)
SKIP_METHODS = {
    "call_action",
    "dispatch",
    "setup_class",
    "teardown",
    "_get_param",
    "_get_querystring",
    "_get_body",
    "region",
    "account_id",
}

# Batch 4-7 services modified in vendor/moto
BATCH_SERVICES = [
    "athena",
    "batch",
    "config",
    "databrew",
    "datasync",
    "ecr",
    "identitystore",
    "iot",
    "ivs",
    "logs",
    "mediapackage",
    "opensearchserverless",
    "rekognition",
    "route53resolver",
    "ses",
]


def get_botocore_session() -> botocore.session.Session:
    return botocore.session.get_session()


def snake_to_pascal(name: str) -> str:
    return "".join(word.capitalize() for word in name.split("_"))


def get_all_wire_names(
    service: str, operation: str, session
) -> tuple[dict[str, str], dict[str, str]] | tuple[None, None]:
    """Return (body_wire_names, all_wire_names) for an operation's input params.

    body_wire_names: {wire_name: shape_name} for body-only params
    all_wire_names: {wire_name: shape_name} for ALL params (body + URI + querystring)
      — Moto's _get_param reads from all three sources

    Returns (None, None) if the operation isn't found in botocore.
    """
    try:
        model = session.get_service_model(service)
        op = model.operation_model(operation)
    except Exception:
        return None, None

    if not op.input_shape or not op.input_shape.members:
        return {}, {}

    body_result: dict[str, str] = {}
    all_result: dict[str, str] = {}
    for shape_name, shape in op.input_shape.members.items():
        loc = shape.serialization.get("location", "")
        wire_name = shape.serialization.get("name", shape_name)
        all_result[wire_name] = shape_name
        if loc not in SKIP_LOCATIONS:
            body_result[wire_name] = shape_name
    return body_result, all_result


def extract_method_get_params(source: str) -> list[tuple[str, list[str], int, int]]:
    """Parse responses.py and return [(method_name, [param_key, ...], start_line, end_line)].

    Uses a simple approach: split on `def ` at class-method indentation level.
    """
    results = []
    # Match instance methods at 4-space indent
    method_re = re.compile(r"^    def (\w+)\(self\)", re.MULTILINE)
    param_re = re.compile(r'_get_param\(\s*"([^"]+)"')

    starts = [(m.start(), m.group(1)) for m in method_re.finditer(source)]
    for i, (start, name) in enumerate(starts):
        if name.startswith("_") or name in SKIP_METHODS:
            continue
        end = starts[i + 1][0] if i + 1 < len(starts) else len(source)
        body = source[start:end]
        # Strip comment lines so commented-out _get_param calls are ignored
        body_no_comments = "\n".join(
            line for line in body.splitlines() if not line.lstrip().startswith("#")
        )
        params = param_re.findall(body_no_comments)
        if params:
            results.append((name, params))
    return results


def find_mismatches(service: str, session) -> list[dict]:
    """Check all _get_param calls in a service's responses.py against botocore."""
    botocore_name = SERVICE_NAME_MAP.get(service, service)
    responses_path = VENDOR_MOTO / service / "responses.py"
    if not responses_path.exists():
        return []

    source = responses_path.read_text()
    methods = extract_method_get_params(source)

    issues = []
    for method_name, params in methods:
        op_name = snake_to_pascal(method_name)
        body_wire_names, all_wire_names = get_all_wire_names(botocore_name, op_name, session)
        if all_wire_names is None:
            continue  # Operation not in botocore — skip

        # Use all_wire_names (body + URI + querystring) for validity check —
        # Moto's _get_param reads from all three sources, so URI params like
        # "thingName" are valid even though they're not in the body.
        all_valid = set(all_wire_names.keys())
        if not all_valid:
            continue  # No params for this operation
        lower_all_valid = {k.lower(): k for k in all_valid}

        for param in params:
            if param in all_valid:
                continue  # Correct

            # Case-insensitive match = almost certainly a casing bug
            suggestion = lower_all_valid.get(param.lower())

            if suggestion and suggestion != param:
                shape_name = all_wire_names[suggestion]
                issues.append(
                    {
                        "service": service,
                        "method": method_name,
                        "operation": op_name,
                        "wrong": param,
                        "correct": suggestion,
                        "shape_name": shape_name,
                        "kind": "casing",
                        "all_valid": sorted(all_valid),
                    }
                )
            else:
                # Param is not in botocore at all — report as unknown
                # (may be a completely wrong name or Moto-internal)
                issues.append(
                    {
                        "service": service,
                        "method": method_name,
                        "operation": op_name,
                        "wrong": param,
                        "correct": None,
                        "shape_name": None,
                        "kind": "unknown",
                        "all_valid": sorted(all_valid),
                    }
                )

    return issues


def apply_fixes(service: str, issues: list[dict]) -> int:
    """Apply _get_param key replacements. Returns number of replacements made."""
    if not issues:
        return 0

    responses_path = VENDOR_MOTO / service / "responses.py"
    source = responses_path.read_text()
    original = source

    # Group fixes by (wrong, correct) to deduplicate
    replacements = {(i["wrong"], i["correct"]) for i in issues}

    for wrong, correct in sorted(replacements):
        # Replace _get_param("wrong" → _get_param("correct"
        # Use word-boundary-ish matching to avoid partial replacements
        old = f'_get_param("{wrong}"'
        new = f'_get_param("{correct}"'
        source = source.replace(old, new)

    if source != original:
        responses_path.write_text(source)
        return len(replacements)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Check/fix Moto _get_param wire names")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--service", help="Check one service")
    group.add_argument("--services", nargs="+", help="Check specific services")
    group.add_argument(
        "--all", action="store_true", help=f"Check batch 4-7 services: {BATCH_SERVICES}"
    )
    parser.add_argument("--write", action="store_true", help="Apply fixes (default: dry run)")
    args = parser.parse_args()

    if args.service:
        services = [args.service]
    elif args.services:
        services = args.services
    else:
        services = BATCH_SERVICES

    session = get_botocore_session()
    all_issues: list[dict] = []

    for service in services:
        issues = find_mismatches(service, session)
        all_issues.extend(issues)

    # Split into casing bugs (fixable) and unknown params (need review)
    casing = [i for i in all_issues if i["kind"] == "casing"]
    unknown = [i for i in all_issues if i["kind"] == "unknown"]

    if not all_issues:
        print(f"✓ No _get_param mismatches found in {len(services)} service(s).")
        return 0

    # Group by service for display
    by_service: dict[str, list[dict]] = {}
    for issue in all_issues:
        by_service.setdefault(issue["service"], []).append(issue)

    if casing:
        print(f"CASING BUGS ({len(casing)}) — auto-fixable with --write:\n")
        for issue in casing:
            print(
                f"  {issue['service']}.{issue['method']}(): "
                f'_get_param("{issue["wrong"]}") → "{issue["correct"]}"'
            )
        print()

    if unknown:
        print(f"UNKNOWN PARAMS ({len(unknown)}) — not in botocore, needs manual review:\n")
        for issue in unknown:
            print(
                f"  {issue['service']}.{issue['method']}() ({issue['operation']}): "
                f'_get_param("{issue["wrong"]}") — valid keys: {issue["all_valid"][:5]}...'
            )
        print()

    if args.write:
        if not casing:
            print("No casing bugs to fix.")
            return 0
        total_fixed = 0
        fixable = {
            s: [i for i in issues if i["kind"] == "casing"] for s, issues in by_service.items()
        }
        for svc, issues in fixable.items():
            if issues:
                n = apply_fixes(svc, issues)
                if n:
                    print(f"Fixed {n} replacement(s) in {svc}/responses.py")
                    total_fixed += n
        print(f"\nApplied {total_fixed} fix(es). Run `uv run ruff format vendor/moto` to reformat.")
        return 0
    else:
        if casing:
            print("Dry run — use --write to fix casing bugs automatically.")
        return 1 if casing else 0


if __name__ == "__main__":
    sys.exit(main())
