#!/usr/bin/env python3
"""Generate a health matrix table for all registered services.

Shows service name, status, protocol, test coverage, and Moto operation count.

Usage:
    uv run python scripts/service_health_matrix.py
    uv run python scripts/service_health_matrix.py --format csv
    uv run python scripts/service_health_matrix.py --format json --output health.json
"""

import argparse
import ast
import csv
import io
import json
import sys
from pathlib import Path

# Paths
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src" / "robotocore"
TESTS = ROOT / "tests"
MOTO = ROOT / "vendor" / "moto" / "moto"

# Registry key -> Moto directory name (only where they differ)
MOTO_DIR_MAP = {
    "lambda": "awslambda",
    "cognito-idp": "cognitoidp",
    "resource-groups": "resourcegroups",
}

# Registry key -> test file prefix (only where they differ from simple normalization)
TEST_NAME_MAP = {
    "cognito-idp": "cognito",
    "resourcegroupstaggingapi": "resource_groups_tagging",
}


def load_registry() -> dict:
    """Import SERVICE_REGISTRY from robotocore."""
    sys.path.insert(0, str(SRC.parent))
    from robotocore.services.registry import SERVICE_REGISTRY

    return SERVICE_REGISTRY


def has_compat_tests(service_name: str) -> bool:
    """Check if compatibility tests exist for a service."""
    normalized = TEST_NAME_MAP.get(service_name, service_name.replace("-", "_"))
    compat_dir = TESTS / "compatibility"
    if not compat_dir.exists():
        return False
    for f in compat_dir.iterdir():
        if f.name.startswith(f"test_{normalized}") and f.name.endswith("_compat.py"):
            return True
    return False


def has_unit_tests(service_name: str) -> bool:
    """Check if unit tests exist for a service."""
    normalized = TEST_NAME_MAP.get(service_name, service_name.replace("-", "_"))
    unit_dir = TESTS / "unit" / "services"
    if not unit_dir.exists():
        return False
    for f in unit_dir.iterdir():
        if f.name.startswith(f"test_{normalized}") and f.name.endswith(".py"):
            return True
    return False


def count_moto_ops(service_name: str) -> int | None:
    """Count public methods in Moto's models.py that look like AWS operations.

    Returns None if no models.py found.
    """
    moto_dir = MOTO_DIR_MAP.get(service_name, service_name)
    models_path = MOTO / moto_dir / "models.py"
    if not models_path.exists():
        return None

    try:
        source = models_path.read_text()
        tree = ast.parse(source)
    except (SyntaxError, UnicodeDecodeError):
        return None

    # Find the main backend class (largest class, or one ending in Backend)
    ops = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        # Only look at Backend classes
        if not node.name.endswith("Backend"):
            continue
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                name = item.name
                # Skip private/dunder methods
                if name.startswith("_"):
                    continue
                # Skip common non-API helper methods
                if name in (
                    "reset",
                    "default_vpc_endpoint_service",
                    "default_vpc_endpoint_service_factory",
                ):
                    continue
                ops.add(name)

    return len(ops) if ops else None


def gather_data(registry: dict) -> list[dict]:
    """Gather health data for all services."""
    rows = []
    for name in sorted(registry.keys()):
        info = registry[name]
        moto_ops = count_moto_ops(name)
        rows.append(
            {
                "service": name,
                "status": info.status.value,
                "protocol": info.protocol,
                "compat_tests": has_compat_tests(name),
                "unit_tests": has_unit_tests(name),
                "moto_ops": moto_ops,
            }
        )
    return rows


def format_markdown(rows: list[dict]) -> str:
    """Format rows as a markdown table."""
    lines = []
    header = "| Service | Status | Protocol | Compat Tests | Unit Tests | Moto Ops |"
    sep = "|---------|--------|----------|:------------:|:----------:|---------:|"
    lines.append(header)
    lines.append(sep)

    for r in rows:
        compat = "yes" if r["compat_tests"] else "no"
        unit = "yes" if r["unit_tests"] else "no"
        moto = str(r["moto_ops"]) if r["moto_ops"] is not None else "-"
        lines.append(
            f"| {r['service']} | {r['status']} | {r['protocol']} | {compat} | {unit} | {moto} |"
        )

    # Summary
    total = len(rows)
    native = sum(1 for r in rows if r["status"] == "native")
    moto_backed = sum(1 for r in rows if r["status"] == "moto_backed")
    with_compat = sum(1 for r in rows if r["compat_tests"])
    with_unit = sum(1 for r in rows if r["unit_tests"])
    total_ops = sum(r["moto_ops"] for r in rows if r["moto_ops"] is not None)

    lines.append("")
    lines.append(
        f"**Total: {total} services** | "
        f"{native} native, {moto_backed} moto-backed | "
        f"{with_compat} with compat tests, {with_unit} with unit tests | "
        f"{total_ops} total Moto ops"
    )
    lines.append("")
    return "\n".join(lines)


def format_csv(rows: list[dict]) -> str:
    """Format rows as CSV."""
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=["service", "status", "protocol", "compat_tests", "unit_tests", "moto_ops"]
    )
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def format_json(rows: list[dict]) -> str:
    """Format rows as JSON."""
    return json.dumps(rows, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Service health matrix for robotocore")
    parser.add_argument("--output", "-o", help="Write output to file instead of stdout")
    parser.add_argument(
        "--format",
        "-f",
        choices=["markdown", "csv", "json"],
        default="markdown",
        help="Output format (default: markdown)",
    )
    args = parser.parse_args()

    registry = load_registry()
    rows = gather_data(registry)

    formatters = {
        "markdown": format_markdown,
        "csv": format_csv,
        "json": format_json,
    }
    output = formatters[args.format](rows)

    if args.output:
        Path(args.output).write_text(output)
        print(f"Wrote {len(rows)} services to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
