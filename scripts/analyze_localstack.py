#!/usr/bin/env python3
"""Analyze LocalStack Enterprise features to identify what robotocore needs.

Scans the LocalStack vendor directory to enumerate:
- Every service provider and its implemented operations
- Cross-service integrations (imports between services)
- Pro/Enterprise-only features
- Handler decorators and API specs

Usage:
    uv run python scripts/analyze_localstack.py [--service SERVICE] [--output json|table]
"""

import ast
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

VENDOR_DIR = Path("vendor/localstack/localstack-core/localstack/services")
PRO_DIR = Path("vendor/localstack/localstack-core/localstack/pro/core/services") if Path("vendor/localstack/localstack-core/localstack/pro").exists() else None


def find_provider_files(base_dir: Path) -> dict[str, Path]:
    """Find all provider.py files, keyed by service name."""
    providers = {}
    if not base_dir.exists():
        return providers
    for provider_file in base_dir.rglob("provider.py"):
        # Service name is the parent directory
        service_name = provider_file.parent.name
        # Skip if it's a nested subdir
        rel = provider_file.relative_to(base_dir)
        if len(rel.parts) == 2:  # service/provider.py
            providers[service_name] = provider_file
    return providers


def extract_operations(filepath: Path) -> dict:
    """Extract implemented operations from a provider.py file using AST."""
    result = {
        "operations": [],
        "api_spec": None,
        "cross_service_imports": [],
        "has_pro_features": False,
    }

    try:
        source = filepath.read_text()
    except Exception:
        return result

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return result

    # Find class definitions that inherit from provider base classes
    for node in ast.walk(tree):
        # Find @handler decorators
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Name) and decorator.id == "handler":
                    result["operations"].append(node.name)
                elif isinstance(decorator, ast.Call):
                    func = decorator.func
                    if isinstance(func, ast.Name) and func.id == "handler":
                        # Extract operation name from first arg
                        if decorator.args:
                            arg = decorator.args[0]
                            if isinstance(arg, ast.Constant):
                                result["operations"].append(arg.value)
                            elif isinstance(arg, ast.Attribute):
                                result["operations"].append(arg.attr)
                        else:
                            result["operations"].append(node.name)

        # Find `api` class variable for API spec
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "api":
                    if isinstance(node.value, ast.Constant):
                        result["api_spec"] = node.value.value

    # Find cross-service imports
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and "localstack.services." in node.module:
                # Extract the service being imported from
                parts = node.module.split(".")
                try:
                    idx = parts.index("services")
                    if idx + 1 < len(parts):
                        imported_service = parts[idx + 1]
                        if imported_service not in result["cross_service_imports"]:
                            result["cross_service_imports"].append(imported_service)
                except ValueError:
                    pass

    # Check for pro feature indicators
    pro_patterns = [
        r"pro\b",
        r"enterprise",
        r"@requires_pro",
        r"is_pro",
    ]
    for pattern in pro_patterns:
        if re.search(pattern, source, re.IGNORECASE):
            result["has_pro_features"] = True
            break

    return result


def find_handler_methods(filepath: Path) -> list[str]:
    """Find all methods decorated with @handler in a file using regex (faster than AST for scanning)."""
    try:
        source = filepath.read_text()
    except Exception:
        return []

    methods = []
    # Match def method_name lines that follow @handler decorators
    for match in re.finditer(r'def\s+(\w+)\s*\(', source):
        methods.append(match.group(1))
    return methods


def analyze_service(service_name: str, provider_path: Path) -> dict:
    """Full analysis of a single service."""
    info = extract_operations(provider_path)
    info["name"] = service_name
    info["path"] = str(provider_path)

    # Count lines
    try:
        info["lines"] = len(provider_path.read_text().splitlines())
    except Exception:
        info["lines"] = 0

    # Check for models.py
    models_path = provider_path.parent / "models.py"
    info["has_models"] = models_path.exists()

    return info


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Analyze LocalStack services")
    parser.add_argument("--service", help="Analyze a specific service")
    parser.add_argument("--output", choices=["json", "table"], default="table")
    parser.add_argument("--cross-service", action="store_true", help="Show cross-service dependency graph")
    args = parser.parse_args()

    providers = find_provider_files(VENDOR_DIR)

    if args.service:
        if args.service not in providers:
            print(f"Service '{args.service}' not found. Available: {sorted(providers.keys())}")
            sys.exit(1)
        info = analyze_service(args.service, providers[args.service])
        if args.output == "json":
            print(json.dumps(info, indent=2))
        else:
            print(f"\n{'='*60}")
            print(f"Service: {info['name']}")
            print(f"Path: {info['path']}")
            print(f"Lines: {info['lines']}")
            print(f"API Spec: {info['api_spec'] or 'N/A'}")
            print(f"Operations ({len(info['operations'])}):")
            for op in sorted(info["operations"]):
                print(f"  - {op}")
            if info["cross_service_imports"]:
                print(f"Cross-service imports: {', '.join(info['cross_service_imports'])}")
            print(f"Has Pro features: {info['has_pro_features']}")
        return

    # Analyze all services
    all_services = {}
    for name, path in sorted(providers.items()):
        all_services[name] = analyze_service(name, path)

    if args.cross_service:
        print("\nCross-Service Dependency Graph:")
        print("=" * 60)
        for name, info in sorted(all_services.items()):
            if info["cross_service_imports"]:
                deps = ", ".join(info["cross_service_imports"])
                print(f"  {name} -> {deps}")
        return

    if args.output == "json":
        print(json.dumps(all_services, indent=2))
    else:
        total_ops = 0
        print(f"\n{'Service':<30} {'Ops':>5} {'Lines':>6} {'CrossSvc':>10} {'Pro':>5}")
        print("-" * 65)
        for name, info in sorted(all_services.items()):
            ops = len(info["operations"])
            total_ops += ops
            cross = len(info["cross_service_imports"])
            pro = "YES" if info["has_pro_features"] else ""
            print(f"{name:<30} {ops:>5} {info['lines']:>6} {cross:>10} {pro:>5}")
        print("-" * 65)
        print(f"{'TOTAL':<30} {total_ops:>5} {'':<6} {len(all_services):>10} services")
        print(f"\nTotal services found: {len(all_services)}")
        print(f"Total operations: {total_ops}")


if __name__ == "__main__":
    main()
