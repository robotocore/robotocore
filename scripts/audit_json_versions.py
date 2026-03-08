#!/usr/bin/env python3
"""Audit JSON protocol versions across all native providers and error handlers.

Reads each service's botocore model to extract `metadata.jsonVersion`, then
checks the robotocore source for Content-Type headers to flag mismatches.

Usage:
    uv run python scripts/audit_json_versions.py
"""

import re
import sys
from pathlib import Path

import botocore.session


def get_expected_json_versions() -> dict[str, str]:
    """Get expected JSON version for each service from botocore metadata."""
    session = botocore.session.get_session()
    versions = {}
    for service_name in session.get_available_services():
        try:
            model = session.get_service_model(service_name)
            jv = model.metadata.get("jsonVersion")
            if jv:
                versions[service_name] = jv
        except Exception:
            pass
    return versions


def find_hardcoded_json_versions(src_dir: Path) -> list[dict]:
    """Find all hardcoded x-amz-json-* Content-Type strings in source."""
    results = []
    pattern = re.compile(r"application/x-amz-json-(\d+\.\d+)")

    for py_file in src_dir.rglob("*.py"):
        content = py_file.read_text()
        for i, line in enumerate(content.splitlines(), 1):
            for match in pattern.finditer(line):
                results.append(
                    {
                        "file": str(py_file.relative_to(src_dir.parent.parent)),
                        "line": i,
                        "version": match.group(1),
                        "text": line.strip(),
                    }
                )
    return results


def main():
    src_dir = Path(__file__).parent.parent / "src" / "robotocore"
    expected = get_expected_json_versions()

    # Native providers that use JSON protocol
    json_services = {name: ver for name, ver in expected.items() if ver in ("1.0", "1.1")}

    # Find all hardcoded versions in source
    hardcoded = find_hardcoded_json_versions(src_dir)

    # Map service names to their provider files
    # Check for mismatches
    mismatches = []
    for entry in hardcoded:
        file_path = entry["file"]
        # Try to infer service from file path
        parts = file_path.split("/")
        service_name = None
        for i, part in enumerate(parts):
            if part == "services" and i + 1 < len(parts):
                service_name = parts[i + 1]
                break

        if service_name:
            # Normalize service name (lambda_ -> lambda, etc.)
            normalized = service_name.rstrip("_").replace("_", "-")
            # Special case: cloudwatch/logs_provider.py handles "logs" service
            filename = Path(entry["file"]).name
            if normalized == "cloudwatch" and "logs" in filename:
                normalized = "logs"
            expected_ver = expected.get(normalized, expected.get(service_name))
            if expected_ver and entry["version"] != expected_ver:
                mismatches.append(
                    {
                        **entry,
                        "service": normalized,
                        "expected": expected_ver,
                        "actual": entry["version"],
                    }
                )

    # Report
    print("=== JSON Protocol Version Audit ===\n")

    print(f"Services with JSON protocol: {len(json_services)}")
    print(f"Hardcoded JSON versions found in source: {len(hardcoded)}")
    print(f"Mismatches: {len(mismatches)}\n")

    if mismatches:
        print("--- MISMATCHES ---")
        for m in mismatches:
            print(f"  {m['file']}:{m['line']}")
            print(f"    Service: {m['service']}")
            print(f"    Expected: {m['expected']}, Found: {m['actual']}")
            print(f"    Line: {m['text']}")
            print()
        sys.exit(1)
    else:
        print("All JSON versions match botocore metadata.")

    # Also print a reference table
    print("\n--- Reference: Expected JSON versions for native providers ---")
    native_services = [
        "dynamodb",
        "dynamodbstreams",
        "kinesis",
        "logs",
        "cognito-idp",
        "ecs",
        "events",
        "stepfunctions",
        "firehose",
        "secretsmanager",
        "cloudwatch",
        "ecr",
        "batch",
        "rekognition",
        "xray",
        "support",
        "appsync",
        "scheduler",
    ]
    for svc in sorted(native_services):
        ver = expected.get(svc, "N/A")
        print(f"  {svc}: {ver}")


if __name__ == "__main__":
    main()
