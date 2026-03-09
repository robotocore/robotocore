#!/usr/bin/env python3
"""Analyze LocalStack Enterprise features to identify what robotocore needs.

Scans the LocalStack vendor directory to enumerate:
- Every service provider and its implemented operations
- Cross-service integrations (imports between services)
- Pro/Enterprise-only features
- Handler decorators and API specs
- Diff between Community and Enterprise per service

Usage:
    uv run python scripts/analyze_localstack.py [--service SERVICE] [--output json|table]
    uv run python scripts/analyze_localstack.py --enterprise-diff
    uv run python scripts/analyze_localstack.py --cross-service
    uv run python scripts/analyze_localstack.py --robotocore-gap
"""

import ast
import json
import re
import sys
from pathlib import Path

VENDOR_DIR = Path("vendor/localstack/localstack-core/localstack/services")
PRO_DIRS = [
    Path("vendor/localstack/localstack-core/localstack/pro/core/services"),
    Path("vendor/localstack/localstack-ext/localstack_ext/services"),
]
ROBOTOCORE_DIR = Path("src/robotocore/services")

# LocalStack tier availability — hardcoded from public docs (March 2026).
# Source: https://www.localstack.cloud/pricing-comparison
# Tiers: "free" (all plans), "base" (Base+), "ultimate" (Ultimate/Pro only)
#
# Keys use robotocore/botocore service identifiers (not LocalStack's internal names).
_LS_TIER: dict[str, str] = {
    # ── Free (available in all plans) ─────────────────────────────────────────
    # Note: keys use both botocore names AND robotocore/vendor dir names where they differ
    "acm": "free",
    "apigateway": "free",
    "cloudformation": "free",
    "cloudwatch": "free",
    "config": "free",
    "configservice": "free",  # LocalStack vendor dir name
    "dynamodb": "free",
    "dynamodbstreams": "free",
    "ec2": "free",
    "events": "free",
    "firehose": "free",
    "iam": "free",
    "kinesis": "free",
    "kms": "free",
    "lambda_": "free",  # robotocore dir name (Python reserved keyword; botocore uses "lambda")
    "logs": "free",
    "opensearch": "free",
    "es": "free",
    "redshift": "free",
    "resource_groups": "free",  # both vendor dir and robotocore dir name
    "resourcegroupstaggingapi": "free",
    "route53": "free",
    "route53resolver": "free",
    "s3": "free",
    "s3control": "free",
    "scheduler": "free",
    "secretsmanager": "free",
    "ses": "free",
    "sns": "free",
    "sqs": "free",
    "ssm": "free",
    "stepfunctions": "free",
    "sts": "free",
    "support": "free",
    "swf": "free",
    "transcribe": "free",
    # ── Base (Base plan and above) ─────────────────────────────────────────────
    "apigatewaymanagementapi": "base",
    "apigatewayv2": "base",
    "appconfig": "base",
    "applicationautoscaling": "base",
    "autoscaling": "base",
    "codebuild": "base",
    "codecommit": "base",
    "cloudfront": "base",
    "cognito": "base",  # robotocore dir name for cognito-idp (User Pools)
    "cognitoidentity": "base",  # Identity Pools
    "ecr": "base",
    "ecs": "base",
    "elasticache": "base",
    "elb": "base",
    "elbv2": "base",
    "iot": "base",
    "mq": "base",
    "rds": "base",
    "rdsdata": "base",
    "sesv2": "base",
    # ── Ultimate (paid Pro/Enterprise tier only) ───────────────────────────────
    "acmpca": "ultimate",
    "amp": "ultimate",
    "appsync": "ultimate",
    "athena": "ultimate",
    "backup": "ultimate",
    "batch": "ultimate",
    "bedrock": "ultimate",
    "ce": "ultimate",
    "cloudtrail": "ultimate",
    "codedeploy": "ultimate",
    "codepipeline": "ultimate",
    "dms": "ultimate",
    "docdb": "ultimate",
    "efs": "ultimate",
    "eks": "ultimate",
    "elasticbeanstalk": "ultimate",
    "emr": "ultimate",
    "emrcontainers": "ultimate",
    "emrserverless": "ultimate",
    "glacier": "ultimate",
    "glue": "ultimate",
    "identitystore": "ultimate",
    "kafka": "ultimate",
    "kinesisanalyticsv2": "ultimate",
    "lakeformation": "ultimate",
    "managedblockchain": "ultimate",
    "mediaconvert": "ultimate",
    "memorydb": "ultimate",
    "neptune": "ultimate",
    "organizations": "ultimate",
    "pinpoint": "ultimate",
    "pipes": "ultimate",
    "ram": "ultimate",
    "sagemaker": "ultimate",
    "shield": "ultimate",
    "ssoadmin": "ultimate",
    "textract": "ultimate",
    "timestreamwrite": "ultimate",
    "timestreamquery": "ultimate",
    "transfer": "ultimate",
    "verifiedpermissions": "ultimate",
    "wafv2": "ultimate",
    "xray": "ultimate",
}


def find_provider_files(base_dir: Path) -> dict[str, Path]:
    """Find all provider.py files, keyed by service name."""
    providers = {}
    if not base_dir.exists():
        return providers
    for provider_file in base_dir.rglob("provider.py"):
        service_name = provider_file.parent.name
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
        "decorators": [],
        "classes": [],
    }

    try:
        source = filepath.read_text()
    except Exception:
        return result

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return result

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                if isinstance(decorator, ast.Name) and decorator.id == "handler":
                    result["operations"].append(node.name)
                elif isinstance(decorator, ast.Call):
                    func = decorator.func
                    if isinstance(func, ast.Name) and func.id == "handler":
                        if decorator.args:
                            arg = decorator.args[0]
                            if isinstance(arg, ast.Constant):
                                result["operations"].append(arg.value)
                            elif isinstance(arg, ast.Attribute):
                                result["operations"].append(arg.attr)
                        else:
                            result["operations"].append(node.name)

        # Track class definitions (provider classes)
        if isinstance(node, ast.ClassDef):
            result["classes"].append(node.name)
            # Extract methods
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and not item.name.startswith("_"):
                    result["operations"].append(item.name)

        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "api":
                    if isinstance(node.value, ast.Constant):
                        result["api_spec"] = node.value.value

    # Find cross-service imports
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and "localstack.services." in node.module:
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
        r"localstack_ext",
    ]
    for pattern in pro_patterns:
        if re.search(pattern, source, re.IGNORECASE):
            result["has_pro_features"] = True
            break

    return result


def analyze_service(service_name: str, provider_path: Path) -> dict:
    """Full analysis of a single service."""
    info = extract_operations(provider_path)
    info["name"] = service_name
    info["path"] = str(provider_path)

    try:
        info["lines"] = len(provider_path.read_text().splitlines())
    except Exception:
        info["lines"] = 0

    models_path = provider_path.parent / "models.py"
    info["has_models"] = models_path.exists()

    return info


def find_enterprise_features() -> dict[str, dict]:
    """Scan for Enterprise/Pro features per service."""
    enterprise = {}

    for pro_dir in PRO_DIRS:
        if not pro_dir.exists():
            continue
        for provider_file in pro_dir.rglob("provider.py"):
            service_name = provider_file.parent.name
            info = extract_operations(provider_file)
            info["path"] = str(provider_file)
            try:
                info["lines"] = len(provider_file.read_text().splitlines())
            except Exception:
                info["lines"] = 0
            enterprise[service_name] = info

    return enterprise


def _pascal_to_snake(name: str) -> str:
    """Convert PascalCase to snake_case (e.g. 'CreateQueue' -> 'create_queue')."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def extract_robotocore_operations(filepath: Path) -> list[str]:
    """Extract implemented operations from a robotocore provider.py using AST.

    Robotocore providers use two dispatch patterns:

    1. Module-level ``_ACTION_MAP`` dict (most providers)::

        _ACTION_MAP: dict[str, Callable] = {
            "CreateQueue": _create_queue,
            ...
        }

    2. ``if action == "X":`` / ``elif action == "X":`` chains (some providers).

    Returns operation names normalised to snake_case so they can be compared
    against LocalStack's snake_case method names.
    """
    try:
        source = filepath.read_text()
        tree = ast.parse(source)
    except Exception:
        return []

    ops: list[str] = []

    for node in ast.walk(tree):
        # Pattern 1: _ACTION_MAP dict (plain Assign or annotated AnnAssign)
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            value = node.value
            if value is None or not isinstance(value, ast.Dict):
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id == "_ACTION_MAP":
                    for key in value.keys:
                        if isinstance(key, ast.Constant) and isinstance(key.value, str):
                            ops.append(_pascal_to_snake(key.value))
                    return ops  # found the map, done

        # Pattern 2: if/elif action == "OperationName"
        if isinstance(node, (ast.If,)):
            test = node.test
            if (
                isinstance(test, ast.Compare)
                and len(test.ops) == 1
                and isinstance(test.ops[0], ast.Eq)
                and len(test.comparators) == 1
                and isinstance(test.comparators[0], ast.Constant)
                and isinstance(test.comparators[0].value, str)
            ):
                val = test.comparators[0].value
                # Only capture PascalCase strings (AWS operation names)
                if val and val[0].isupper():
                    ops.append(_pascal_to_snake(val))

    return list(dict.fromkeys(ops))  # dedupe, preserve order


def analyze_robotocore_gap(community: dict, enterprise: dict) -> dict[str, dict]:
    """Compare robotocore implementation against LocalStack Community.

    Uses the parity report's implementation detection (which checks both native
    providers and Moto backends, validated against botocore) instead of ad-hoc
    provider scanning. This ensures the numbers match across reports.
    """
    # Import the parity report's implementation detection
    scripts_dir = str(Path(__file__).resolve().parent)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    src_dir = str(Path(__file__).resolve().parent.parent / "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    from generate_parity_report import build_report

    parity = build_report()
    parity_services = parity["services"]

    # Build mapping: robotocore dir name / LS vendor name → registry service name
    # The community dict uses LS vendor dir names (lambda_, resource_groups, etc.)
    # The _LS_TIER dict uses botocore names (resource-groups, resourcegroupstaggingapi, etc.)
    # The parity report uses registry names (resource-groups, resourcegroupstaggingapi, etc.)
    _name_to_registry = {}
    for reg_name in parity_services:
        _name_to_registry[reg_name] = reg_name
        # Also map common variants
        _name_to_registry[reg_name.replace("-", "_")] = reg_name
        _name_to_registry[reg_name.replace("-", "")] = reg_name

    # Extra manual mappings for LS vendor dir names that don't match
    _name_to_registry["lambda_"] = "lambda"
    _name_to_registry["configservice"] = "config"
    _name_to_registry["stepfunctions"] = "stepfunctions"
    _name_to_registry["resource_groups"] = "resource-groups"

    robotocore_providers = find_provider_files(ROBOTOCORE_DIR)
    gaps = {}

    # Include all services known to either community vendor OR tier map
    all_services = set(community.keys()) | set(_LS_TIER.keys())

    for service in sorted(all_services):
        community_ops = set(community.get(service, {}).get("operations", []))

        # Find the registry name for this service
        reg_name = _name_to_registry.get(service, service)
        parity_data = parity_services.get(reg_name)

        if parity_data:
            # Use the parity report's implementation count (validated against botocore)
            # Convert parity ops (PascalCase) to snake_case to compare with LS community ops
            robotocore_ops_pascal = set(parity_data["implemented_ops"])
            robotocore_ops = set()
            for op in robotocore_ops_pascal:
                snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", op)
                snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", snake).lower()
                robotocore_ops.add(snake)
            has_provider = True
        else:
            robotocore_ops = set()
            if service in robotocore_providers:
                robotocore_ops = set(extract_robotocore_operations(robotocore_providers[service]))
            has_provider = service in robotocore_providers

        missing_ops = community_ops - robotocore_ops
        ls_tier = _LS_TIER.get(service, "unknown")

        if missing_ops or not has_provider:
            gaps[service] = {
                "has_provider": has_provider,
                "community_ops": len(community_ops),
                "robotocore_ops": len(robotocore_ops),
                "missing_ops": sorted(missing_ops),
                "ls_tier": ls_tier,
                "coverage_pct": (
                    min(100, round(len(robotocore_ops) / len(community_ops) * 100))
                    if community_ops
                    else None
                ),
            }

    return gaps


def enterprise_diff(community: dict, enterprise: dict):
    """Print per-service diff between Community and Enterprise."""
    print("\nEnterprise vs Community Feature Diff")
    print("=" * 80)

    all_services = sorted(set(community.keys()) | set(enterprise.keys()))

    enterprise_only_services = []
    enhanced_services = []

    for service in all_services:
        com_ops = set(community.get(service, {}).get("operations", []))
        ent_ops = set(enterprise.get(service, {}).get("operations", []))

        if service not in community:
            enterprise_only_services.append((service, ent_ops))
        elif ent_ops - com_ops:
            enhanced_services.append((service, com_ops, ent_ops))

    if enterprise_only_services:
        print(f"\n--- Enterprise-Only Services ({len(enterprise_only_services)}) ---")
        for service, ops in enterprise_only_services:
            print(f"\n  {service} ({len(ops)} operations):")
            for op in sorted(ops)[:10]:
                print(f"    + {op}")
            if len(ops) > 10:
                print(f"    ... and {len(ops) - 10} more")

    if enhanced_services:
        print(f"\n--- Enterprise-Enhanced Services ({len(enhanced_services)}) ---")
        for service, com_ops, ent_ops in enhanced_services:
            extra = ent_ops - com_ops
            print(f"\n  {service}: +{len(extra)} enterprise operations (total: {len(ent_ops)})")
            for op in sorted(extra)[:10]:
                print(f"    + {op}")
            if len(extra) > 10:
                print(f"    ... and {len(extra) - 10} more")

    print(f"\n{'=' * 80}")
    print(f"Enterprise-only services: {len(enterprise_only_services)}")
    print(f"Enterprise-enhanced services: {len(enhanced_services)}")
    total_enterprise_ops = sum(
        len(ent_ops - com_ops) for _, com_ops, ent_ops in enhanced_services
    ) + sum(len(ops) for _, ops in enterprise_only_services)
    print(f"Total enterprise-only operations: {total_enterprise_ops}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Analyze LocalStack services")
    parser.add_argument("--service", help="Analyze a specific service")
    parser.add_argument("--output", choices=["json", "table"], default="table")
    parser.add_argument(
        "--cross-service", action="store_true", help="Show cross-service dependency graph"
    )
    parser.add_argument(
        "--enterprise-diff",
        action="store_true",
        help="Diff Enterprise vs Community features",
    )
    parser.add_argument(
        "--robotocore-gap",
        action="store_true",
        help="Show what robotocore is missing vs LocalStack",
    )
    args = parser.parse_args()

    providers = find_provider_files(VENDOR_DIR)

    if args.enterprise_diff:
        community = {}
        for name, path in sorted(providers.items()):
            community[name] = analyze_service(name, path)
        enterprise = find_enterprise_features()
        enterprise_diff(community, enterprise)
        return

    if args.robotocore_gap:
        community = {}
        for name, path in sorted(providers.items()):
            community[name] = analyze_service(name, path)
        enterprise = find_enterprise_features()
        gaps = analyze_robotocore_gap(community, enterprise)

        if args.output == "json":
            print(json.dumps(gaps, indent=2))
        else:
            tier_order = {"free": 0, "base": 1, "ultimate": 2, "unknown": 3}
            tier_label = {"free": "Free", "base": "Base+", "ultimate": "Ultimate", "unknown": "?"}
            sorted_gaps = sorted(
                gaps.items(), key=lambda kv: (tier_order.get(kv[1]["ls_tier"], 3), kv[0])
            )
            print("\nRobotocore vs LocalStack — Coverage Gaps")
            print("=" * 86)
            print(
                f"{'Service':<25} {'LS Tier':>9} {'Provider':>8} {'Community':>10} "
                f"{'Robotocore':>11} {'Coverage':>9}"
            )
            print("-" * 86)
            current_tier = None
            for service, gap in sorted_gaps:
                tier = gap["ls_tier"]
                if tier != current_tier:
                    current_tier = tier
                    print(f"  [{tier_label[tier]}]")
                prov = "YES" if gap["has_provider"] else "NO"
                comm = str(gap["community_ops"]) if gap["community_ops"] else "—"
                cov = f"{gap['coverage_pct']}%" if gap["coverage_pct"] is not None else "—"
                print(
                    f"{service:<25} {tier_label[tier]:>9} {prov:>8} {comm:>10} "
                    f"{gap['robotocore_ops']:>11} {cov:>9}"
                )
            print("-" * 86)
            total_missing = sum(len(g["missing_ops"]) for g in gaps.values())
            tier_counts = {}
            for g in gaps.values():
                tier_counts[g["ls_tier"]] = tier_counts.get(g["ls_tier"], 0) + 1
            print(f"Total services with gaps: {len(gaps)}")
            free_n = tier_counts.get("free", 0)
            base_n = tier_counts.get("base", 0)
            ult_n = tier_counts.get("ultimate", 0)
            print(f"  Free: {free_n}  Base+: {base_n}  Ultimate: {ult_n}")
            print(f"Total missing community operations: {total_missing}")
            print("(Tier data: https://www.localstack.cloud/pricing-comparison, March 2026)")
        return

    if args.service:
        if args.service not in providers:
            print(f"Service '{args.service}' not found. Available: {sorted(providers.keys())}")
            sys.exit(1)
        info = analyze_service(args.service, providers[args.service])
        if args.output == "json":
            print(json.dumps(info, indent=2))
        else:
            print(f"\n{'=' * 60}")
            print(f"Service: {info['name']}")
            print(f"Path: {info['path']}")
            print(f"Lines: {info['lines']}")
            print(f"API Spec: {info['api_spec'] or 'N/A'}")
            print(f"Classes: {', '.join(info['classes'])}")
            print(f"Operations ({len(info['operations'])}):")
            for op in sorted(info["operations"]):
                print(f"  - {op}")
            if info["cross_service_imports"]:
                print(f"Cross-service imports: {', '.join(info['cross_service_imports'])}")
            print(f"Has Pro features: {info['has_pro_features']}")
        return

    if args.cross_service:
        print("\nCross-Service Dependency Graph:")
        print("=" * 60)
        all_services = {}
        for name, path in sorted(providers.items()):
            all_services[name] = analyze_service(name, path)

        for name, info in sorted(all_services.items()):
            if info["cross_service_imports"]:
                deps = ", ".join(info["cross_service_imports"])
                print(f"  {name} -> {deps}")
        return

    # Default: analyze all services
    all_services = {}
    for name, path in sorted(providers.items()):
        all_services[name] = analyze_service(name, path)

    if args.output == "json":
        print(json.dumps(all_services, indent=2))
    else:
        total_ops = 0
        print(
            f"\n{'Service':<30} {'Ops':>5} {'Lines':>6} {'CrossSvc':>10} {'Classes':>10} {'Pro':>5}"
        )
        print("-" * 75)
        for name, info in sorted(all_services.items()):
            ops = len(info["operations"])
            total_ops += ops
            cross = len(info["cross_service_imports"])
            classes = len(info["classes"])
            pro = "YES" if info["has_pro_features"] else ""
            print(f"{name:<30} {ops:>5} {info['lines']:>6} {cross:>10} {classes:>10} {pro:>5}")
        print("-" * 75)
        print(f"{'TOTAL':<30} {total_ops:>5} {'':<6} {len(all_services):>10} services")
        print(f"\nTotal services found: {len(all_services)}")
        print(f"Total operations: {total_ops}")


if __name__ == "__main__":
    main()
