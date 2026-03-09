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

# LocalStack tier availability — from https://www.localstack.cloud/pricing-comparison
# (March 2026). Tiers: "community" (free), "base" ($30+/mo), "ultimate" ($70+/mo).
#
# Keys are botocore service names (matching robotocore's registry).  Services that
# robotocore implements but LocalStack doesn't offer at any tier are "not_offered".
_LS_TIER: dict[str, str] = {
    # ── Community (free) ──────────────────────────────────────────────────────
    "acm": "community",
    "apigateway": "community",
    "cloudformation": "community",
    "cloudwatch": "community",
    "config": "community",
    "dynamodb": "community",
    "dynamodbstreams": "community",
    "ec2": "community",
    "es": "community",
    "events": "community",
    "firehose": "community",
    "iam": "community",
    "kinesis": "community",
    "kms": "community",
    "lambda": "community",
    "logs": "community",
    "opensearch": "community",
    "redshift": "community",
    "resource-groups": "community",
    "resourcegroupstaggingapi": "community",
    "route53": "community",
    "route53resolver": "community",
    "s3": "community",
    "s3control": "community",
    "scheduler": "community",
    "secretsmanager": "community",
    "ses": "community",
    "sns": "community",
    "sqs": "community",
    "ssm": "community",
    "stepfunctions": "community",
    "sts": "community",
    "support": "community",
    "swf": "community",
    "transcribe": "community",
    # ── Base ($30+/mo) ────────────────────────────────────────────────────────
    "athena": "base",
    "codebuild": "base",
    "codecommit": "base",
    "cognito-idp": "base",
    "cognitoidentity": "base",
    "ecr": "base",
    "ecs": "base",
    "elasticache": "base",
    "emr": "base",
    "glue": "base",
    "mq": "base",
    "rds": "base",
    "rdsdata": "base",
    # ── Ultimate ($70+/mo) ────────────────────────────────────────────────────
    "account": "ultimate",
    "acmpca": "ultimate",
    "amp": "ultimate",
    "apigatewaymanagementapi": "ultimate",
    "apigatewayv2": "ultimate",
    "appconfig": "ultimate",
    "applicationautoscaling": "ultimate",
    "appsync": "ultimate",
    "autoscaling": "ultimate",
    "backup": "ultimate",
    "batch": "ultimate",
    "bedrock": "ultimate",
    "ce": "ultimate",
    "cloudfront": "ultimate",
    "cloudtrail": "ultimate",
    "codedeploy": "ultimate",
    "codepipeline": "ultimate",
    "dms": "ultimate",
    "efs": "ultimate",
    "eks": "ultimate",
    "elasticbeanstalk": "ultimate",
    "elb": "ultimate",
    "elbv2": "ultimate",
    "emrcontainers": "ultimate",
    "emrserverless": "ultimate",
    "glacier": "ultimate",
    "identitystore": "ultimate",
    "iot": "ultimate",
    "kafka": "ultimate",
    "kinesisanalyticsv2": "ultimate",
    "lakeformation": "ultimate",
    "managedblockchain": "ultimate",
    "mediaconvert": "ultimate",
    "mediastore": "ultimate",
    "memorydb": "ultimate",
    "organizations": "ultimate",
    "pinpoint": "ultimate",
    "pipes": "ultimate",
    "ram": "ultimate",
    "rekognition": "ultimate",
    "sagemaker": "ultimate",
    "sesv2": "ultimate",
    "servicediscovery": "ultimate",
    "shield": "ultimate",
    "ssoadmin": "ultimate",
    "textract": "ultimate",
    "timestreamquery": "ultimate",
    "timestreamwrite": "ultimate",
    "transfer": "ultimate",
    "wafv2": "ultimate",
    "xray": "ultimate",
    # ── Not offered by LocalStack at any tier ─────────────────────────────────
    # (Robotocore implements these via Moto; LocalStack doesn't offer them at all)
    "appmesh": "not_offered",
    "bedrockagent": "not_offered",
    "budgets": "not_offered",
    "clouddirectory": "not_offered",
    "cloudhsmv2": "not_offered",
    "comprehend": "not_offered",
    "connect": "not_offered",
    "connectcampaigns": "not_offered",
    "databrew": "not_offered",
    "datapipeline": "not_offered",
    "datasync": "not_offered",
    "dax": "not_offered",
    "ds": "not_offered",
    "dsql": "not_offered",
    "ec2instanceconnect": "not_offered",
    "fsx": "not_offered",
    "greengrass": "not_offered",
    "guardduty": "not_offered",
    "inspector2": "not_offered",
    "iotdata": "not_offered",
    "ivs": "not_offered",
    "kinesisvideo": "not_offered",
    "lexv2models": "not_offered",
    "macie2": "not_offered",
    "mediaconnect": "not_offered",
    "medialive": "not_offered",
    "mediapackage": "not_offered",
    "mediapackagev2": "not_offered",
    "networkfirewall": "not_offered",
    "networkmanager": "not_offered",
    "opensearchserverless": "not_offered",
    "osis": "not_offered",
    "panorama": "not_offered",
    "polly": "not_offered",
    "quicksight": "not_offered",
    "redshiftdata": "not_offered",
    "resiliencehub": "not_offered",
    "route53domains": "not_offered",
    "s3tables": "not_offered",
    "s3vectors": "not_offered",
    "securityhub": "not_offered",
    "servicecatalog": "not_offered",
    "servicecatalogappregistry": "not_offered",
    "signer": "not_offered",
    "synthetics": "not_offered",
    "timestreaminfluxdb": "not_offered",
    "vpclattice": "not_offered",
    "workspaces": "not_offered",
    "workspacesweb": "not_offered",
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

    # Extra manual mappings for LS vendor dir names that don't match registry
    _name_to_registry["lambda_"] = "lambda"
    _name_to_registry["configservice"] = "config"
    _name_to_registry["resource_groups"] = "resource-groups"
    _name_to_registry["cognito"] = "cognito-idp"

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
        ls_tier = _LS_TIER.get(reg_name, _LS_TIER.get(service, "unknown"))

        if missing_ops or not has_provider:
            gaps[reg_name] = {
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


def tier_analysis():
    """Show what robotocore gives away free vs what LocalStack charges for."""
    scripts_dir = str(Path(__file__).resolve().parent)
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    src_dir = str(Path(__file__).resolve().parent.parent / "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    from generate_parity_report import build_report

    parity = build_report()
    parity_services = parity["services"]

    # Group services by LS tier
    tiers: dict[str, list[dict]] = {
        "community": [],
        "base": [],
        "ultimate": [],
        "not_offered": [],
    }

    for svc_name, svc_data in sorted(parity_services.items()):
        ls_tier = _LS_TIER.get(svc_name, "unknown")
        if ls_tier == "unknown":
            continue  # skip unmapped (shouldn't happen now)
        impl = svc_data["implemented_count"]
        total = svc_data["total_aws_ops"]
        tested = svc_data["tested_count"]
        status = svc_data["status"]
        pct = round(impl / total * 100) if total else 0
        tiers[ls_tier].append(
            {
                "name": svc_name,
                "status": status,
                "total": total,
                "impl": impl,
                "pct": pct,
                "tested": tested,
            }
        )

    w = 90

    print()
    print("=" * w)
    print("  ROBOTOCORE vs LOCALSTACK — TIER COMPARISON".center(w))
    print("=" * w)
    print()

    def _tier_header(title: str, svcs: list, impl: int, total: int):
        print("-" * w)
        print(f"  {title}")
        pct = _pct(impl, total)
        print(f"  {len(svcs)} services, {impl}/{total} ops ({pct})")
        print("-" * w)

    # ── Community tier ────────────────────────────────────────────────────
    comm = tiers["community"]
    comm_impl = sum(s["impl"] for s in comm)
    comm_total = sum(s["total"] for s in comm)
    _tier_header(
        "COMMUNITY (free tier in LocalStack)",
        comm,
        comm_impl,
        comm_total,
    )
    _print_svc_table(comm)

    # ── Base tier ─────────────────────────────────────────────────────────
    base = tiers["base"]
    base_impl = sum(s["impl"] for s in base)
    base_total = sum(s["total"] for s in base)
    print()
    _tier_header(
        "BASE ($30+/mo in LocalStack)",
        base,
        base_impl,
        base_total,
    )
    _print_svc_table(base)

    # ── Ultimate tier ─────────────────────────────────────────────────────
    ult = tiers["ultimate"]
    ult_impl = sum(s["impl"] for s in ult)
    ult_total = sum(s["total"] for s in ult)
    print()
    _tier_header(
        "ULTIMATE ($70+/mo in LocalStack)",
        ult,
        ult_impl,
        ult_total,
    )
    _print_svc_table(ult)

    # ── Not offered ───────────────────────────────────────────────────────
    extra = tiers["not_offered"]
    extra_impl = sum(s["impl"] for s in extra)
    extra_total = sum(s["total"] for s in extra)
    print()
    _tier_header(
        "NOT IN LOCALSTACK (Robotocore-only, via Moto)",
        extra,
        extra_impl,
        extra_total,
    )
    _print_svc_table(extra)

    # ── Summary ───────────────────────────────────────────────────────────
    total_svcs = len(comm) + len(base) + len(ult) + len(extra)
    total_impl = comm_impl + base_impl + ult_impl + extra_impl
    total_ops = comm_total + base_total + ult_total + extra_total
    print()
    print("=" * w)
    print("  SUMMARY".center(w))
    print("=" * w)
    print(f"  Total services:          {total_svcs}")
    pct_str = _pct(total_impl, total_ops)
    print(f"  Total ops implemented:   {total_impl}/{total_ops} ({pct_str})")
    print()
    print(f"  LS Community overlap:    {len(comm)} services")
    print(f"  LS Base overlap:         {len(base)} services, {base_impl} ops")
    print(f"  LS Ultimate overlap:     {len(ult)} services, {ult_impl} ops")
    print(f"  Robotocore-only:         {len(extra)} services, {extra_impl} ops")
    print()
    print("  Source: localstack.cloud/pricing-comparison (March 2026)")
    print("=" * w)


def _pct(n: int, d: int) -> str:
    return f"{round(n / d * 100)}%" if d else "n/a"


def _print_svc_table(services: list[dict]):
    """Print a compact table of services with implementation status."""
    hdr = f"  {'Service':<28} {'Type':<12} {'Ops':>5} {'Impl':>5} {'%':>5} {'Tested':>6}"
    print(hdr)
    print(f"  {'-' * 28} {'-' * 12} {'-' * 5} {'-' * 5} {'-' * 5} {'-' * 6}")
    for s in services:
        status = s["status"].replace("_", " ")
        pct = f"{s['pct']}%" if s["total"] else "n/a"
        print(
            f"  {s['name']:<28} {status:<12}"
            f" {s['total']:>5} {s['impl']:>5} {pct:>5} {s['tested']:>6}"
        )


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Analyze LocalStack services")
    parser.add_argument("--service", help="Analyze a specific service")
    parser.add_argument("--output", choices=["json", "table"], default="table")
    parser.add_argument(
        "--cross-service", action="store_true", help="Show cross-service dependency graph"
    )
    parser.add_argument(
        "--robotocore-gap",
        action="store_true",
        help="Show what robotocore is missing vs LocalStack",
    )
    parser.add_argument(
        "--tier-analysis",
        action="store_true",
        help="Show robotocore vs LocalStack tier-by-tier comparison",
    )
    args = parser.parse_args()

    if args.tier_analysis:
        tier_analysis()
        return

    providers = find_provider_files(VENDOR_DIR)

    if args.robotocore_gap:
        community = {}
        for name, path in sorted(providers.items()):
            community[name] = analyze_service(name, path)
        enterprise = find_enterprise_features()
        gaps = analyze_robotocore_gap(community, enterprise)

        if args.output == "json":
            print(json.dumps(gaps, indent=2))
        else:
            tier_order = {"community": 0, "base": 1, "ultimate": 2, "not_offered": 3, "unknown": 4}
            tier_heading = {
                "community": "Community (free)",
                "base": "Base ($30+/mo)",
                "ultimate": "Ultimate ($70+/mo)",
                "not_offered": "Not in LocalStack",
                "unknown": "Unknown",
            }
            tier_short = {
                "community": "Free",
                "base": "Base",
                "ultimate": "Ult.",
                "not_offered": "N/A",
                "unknown": "?",
            }
            sorted_gaps = sorted(
                gaps.items(), key=lambda kv: (tier_order.get(kv[1]["ls_tier"], 3), kv[0])
            )
            print("\nRobotocore vs LocalStack — Coverage Gaps")
            print("=" * 86)
            print(
                f"  {'Service':<25} {'LS Tier':>7} {'Prov':>5} {'LS Ops':>7} "
                f"{'RC Ops':>7} {'Coverage':>9}"
            )
            print("  " + "-" * 84)
            current_tier = None
            for service, gap in sorted_gaps:
                tier = gap["ls_tier"]
                if tier != current_tier:
                    current_tier = tier
                    print(f"\n  [{tier_heading[tier]}]")
                prov = "yes" if gap["has_provider"] else "NO"
                comm = str(gap["community_ops"]) if gap["community_ops"] else "-"
                cov = f"{gap['coverage_pct']}%" if gap["coverage_pct"] is not None else "-"
                print(
                    f"  {service:<25} {tier_short[tier]:>7} {prov:>5} {comm:>7} "
                    f"{gap['robotocore_ops']:>7} {cov:>9}"
                )
            print("  " + "-" * 84)
            total_missing = sum(len(g["missing_ops"]) for g in gaps.values())
            tier_counts = {}
            for g in gaps.values():
                tier_counts[g["ls_tier"]] = tier_counts.get(g["ls_tier"], 0) + 1
            print(f"Total services with gaps: {len(gaps)}")
            comm_n = tier_counts.get("community", 0)
            base_n = tier_counts.get("base", 0)
            ult_n = tier_counts.get("ultimate", 0)
            not_n = tier_counts.get("not_offered", 0)
            print(f"  Community: {comm_n}  Base: {base_n}  Ultimate: {ult_n}  Not in LS: {not_n}")
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
