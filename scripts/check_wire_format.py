#!/usr/bin/env python3
"""Check native provider wire format against botocore service model expectations.

For each native provider, inspects the botocore model to determine what wire format
the service expects (PascalCase for json/query, camelCase via locationName for rest-json/rest-xml),
then checks the provider code for potential mismatches.

Usage:
    uv run python scripts/check_wire_format.py              # Check all native providers
    uv run python scripts/check_wire_format.py --service sqs # Check one service
    uv run python scripts/check_wire_format.py --json        # Machine-readable output
"""

import argparse
import json
import re
import sys
from pathlib import Path

import botocore.session

SRC = Path(__file__).resolve().parent.parent / "src" / "robotocore" / "services"

# Map of service directory name to botocore service name (when they differ)
SERVICE_NAME_MAP = {
    "lambda_": "lambda",
    "cognito": "cognito-idp",
}

# Services that have known camelCase conversion logic
SERVICES_WITH_CAMEL_CONVERSION = {"apigatewayv2"}

# Services that delegate serialization to Moto (wire format handled by Moto's responses.py)
MOTO_DELEGATED_SERVICES = {"s3", "lambda_", "dynamodb", "cloudwatch"}


def get_native_providers() -> dict[str, Path]:
    """Find all native provider files."""
    providers = {}
    for provider_file in sorted(SRC.glob("*/provider.py")):
        service_dir = provider_file.parent.name
        providers[service_dir] = provider_file
    return providers


def get_wire_format_info(botocore_name: str) -> dict:
    """Get wire format expectations from botocore model."""
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(botocore_name)
    except botocore.exceptions.UnknownServiceError:
        return {"error": f"Unknown botocore service: {botocore_name}"}

    protocol = model.protocol
    operations = {}

    for op_name in model.operation_names:
        op = model.operation_model(op_name)
        output_keys = {}
        if op.output_shape and op.output_shape.members:
            for name, shape in op.output_shape.members.items():
                loc = shape.serialization.get("name", "")
                # Skip header/URI/querystring locations — only check body keys
                location = shape.serialization.get("location", "")
                if location in ("header", "headers", "uri", "querystring", "statusCode"):
                    continue
                wire_name = loc if loc else name
                output_keys[name] = {
                    "wire_name": wire_name,
                    "location_name": loc,
                    "needs_camel": loc != "" and loc != name,
                }

        input_keys = {}
        if op.input_shape and op.input_shape.members:
            for name, shape in op.input_shape.members.items():
                loc = shape.serialization.get("name", "")
                location = shape.serialization.get("location", "")
                if location in ("header", "headers", "uri", "querystring", "statusCode"):
                    continue
                wire_name = loc if loc else name
                input_keys[name] = {
                    "wire_name": wire_name,
                    "location_name": loc,
                    "needs_camel": loc != "" and loc != name,
                }

        operations[op_name] = {
            "output_keys": output_keys,
            "input_keys": input_keys,
        }

    # Count how many keys actually need case conversion (locationName differs from shape name)
    total_keys_needing_conversion = 0
    for op_info in operations.values():
        for key_info in op_info["output_keys"].values():
            if key_info["needs_camel"]:
                total_keys_needing_conversion += 1

    return {
        "protocol": protocol,
        "needs_camel_conversion": protocol in ("rest-json", "rest-xml")
        and total_keys_needing_conversion > 0,
        "keys_needing_conversion": total_keys_needing_conversion,
        "operation_count": len(operations),
        "operations": operations,
    }


def check_provider_has_conversion(provider_path: Path) -> dict:
    """Check if a provider file has camelCase conversion logic."""
    code = provider_path.read_text()

    patterns = {
        "has_camel_function": bool(re.search(r"def _to_camel|def _camel_keys|camelCase", code)),
        "has_pascal_function": bool(re.search(r"def _to_pascal|def _pascal_keys|PascalCase", code)),
        "has_location_name_handling": bool(re.search(r"locationName|location_name", code)),
        "returns_pascal_keys": False,
    }

    # Look for dict literals with PascalCase keys being returned
    # This is a heuristic — look for return statements or response dicts with PascalCase keys
    pascal_key_pattern = re.compile(r'"([A-Z][a-zA-Z]+)":\s')
    matches = pascal_key_pattern.findall(code)
    if matches:
        patterns["pascal_keys_in_code"] = len(matches)
        # Sample a few
        patterns["sample_pascal_keys"] = sorted(set(matches))[:10]

    return patterns


def analyze_service(service_dir: str, provider_path: Path) -> dict:
    """Analyze a single service for wire format issues."""
    botocore_name = SERVICE_NAME_MAP.get(service_dir, service_dir)
    wire_info = get_wire_format_info(botocore_name)

    if "error" in wire_info:
        return {"service": service_dir, "status": "skip", "reason": wire_info["error"]}

    protocol = wire_info["protocol"]
    needs_camel = wire_info["needs_camel_conversion"]
    provider_checks = check_provider_has_conversion(provider_path)

    issues = []
    status = "ok"

    if needs_camel:
        has_conversion = (
            provider_checks["has_camel_function"] or service_dir in SERVICES_WITH_CAMEL_CONVERSION
        )
        delegates_to_moto = service_dir in MOTO_DELEGATED_SERVICES
        if delegates_to_moto:
            # Moto handles serialization — no conversion needed in our provider
            return {
                "service": service_dir,
                "botocore_name": botocore_name,
                "protocol": protocol,
                "needs_camel_conversion": needs_camel,
                "has_conversion_logic": has_conversion,
                "delegates_to_moto": True,
                "operation_count": wire_info["operation_count"],
                "status": "ok",
                "issues": [],
            }
        if not has_conversion:
            issues.append(
                "REST-JSON service but no camelCase conversion found. "
                "boto3 expects camelCase keys on the wire (e.g., locationName mappings)."
            )
            status = "warn"

        # Count operations that need camelCase output
        ops_needing_camel = 0
        for op_name, op_info in wire_info["operations"].items():
            for key, key_info in op_info["output_keys"].items():
                if key_info["needs_camel"]:
                    ops_needing_camel += 1
                    break

        if ops_needing_camel > 0 and not has_conversion:
            status = "error"
            issues.append(
                f"{ops_needing_camel} operations have output keys that need "
                f"camelCase conversion (locationName differs from shape name)."
            )
    else:
        # JSON/query protocol — PascalCase on wire is correct
        if provider_checks.get("has_camel_function"):
            issues.append(
                f"{protocol} protocol service has camelCase conversion — "
                f"this may be incorrect (PascalCase expected on wire)."
            )
            status = "warn"

    return {
        "service": service_dir,
        "botocore_name": botocore_name,
        "protocol": protocol,
        "needs_camel_conversion": needs_camel,
        "has_conversion_logic": provider_checks.get("has_camel_function", False),
        "pascal_keys_in_code": provider_checks.get("pascal_keys_in_code", 0),
        "operation_count": wire_info["operation_count"],
        "status": status,
        "issues": issues,
    }


def main():
    parser = argparse.ArgumentParser(description="Check native provider wire format")
    parser.add_argument("--service", help="Check a single service")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show per-operation details")
    args = parser.parse_args()

    providers = get_native_providers()

    if args.service:
        if args.service not in providers:
            print(f"No native provider found for '{args.service}'")
            print(f"Available: {', '.join(sorted(providers))}")
            sys.exit(1)
        providers = {args.service: providers[args.service]}

    results = []
    for service_dir, provider_path in sorted(providers.items()):
        result = analyze_service(service_dir, provider_path)
        results.append(result)

    if args.json:
        print(json.dumps(results, indent=2))
        return

    # Pretty print
    errors = [r for r in results if r["status"] == "error"]
    warnings = [r for r in results if r["status"] == "warn"]
    ok = [r for r in results if r["status"] == "ok"]
    skipped = [r for r in results if r["status"] == "skip"]

    print(f"Wire Format Check: {len(providers)} native providers\n")

    if errors:
        print("ERRORS (likely broken wire format):")
        for r in errors:
            print(f"  {r['service']} ({r['protocol']})")
            for issue in r["issues"]:
                print(f"    - {issue}")
        print()

    if warnings:
        print("WARNINGS (review recommended):")
        print("  These REST-JSON services need camelCase conversion like apigatewayv2.")
        print("  See src/robotocore/services/apigatewayv2/provider.py for the pattern.")
        print()
        for r in warnings:
            print(f"  {r['service']} ({r['protocol']})")
            for issue in r["issues"]:
                print(f"    - {issue}")
        print()

    if ok:
        print("OK:")
        for r in ok:
            camel = " (has camelCase conversion)" if r.get("has_conversion_logic") else ""
            print(f"  {r['service']} ({r['protocol']}){camel}")
        print()

    if skipped:
        print("SKIPPED:")
        for r in skipped:
            print(f"  {r['service']}: {r.get('reason', 'unknown')}")
        print()

    # Summary
    parts = [f"{len(ok)} ok", f"{len(warnings)} warnings", f"{len(errors)} errors"]
    if skipped:
        parts.append(f"{len(skipped)} skipped")
    print(f"Summary: {', '.join(parts)}")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
