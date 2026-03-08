#!/usr/bin/env python3
"""Batch-register Moto-backed services in Robotocore's registry and router.

Reads botocore service model metadata to auto-generate the correct
ServiceInfo and routing entries for any Moto backend.

Usage:
    # Dry-run all Moto services not yet registered
    uv run python scripts/batch_register_services.py --all-moto

    # Dry-run a specific service
    uv run python scripts/batch_register_services.py --service rds

    # Actually write changes
    uv run python scripts/batch_register_services.py --all-moto --write
"""

import argparse
import gzip
import json
import os
import re
import sys

BOTOCORE_DATA = None  # set at runtime
MOTO_BASE = os.path.join(os.path.dirname(__file__), "..", "vendor", "moto", "moto")
REGISTRY_PATH = os.path.join(
    os.path.dirname(__file__), "..", "src", "robotocore", "services", "registry.py"
)
ROUTER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "src", "robotocore", "gateway", "router.py"
)

# Moto dir name -> botocore service name (when they differ)
MOTO_TO_BOTOCORE = {
    "awslambda": "lambda",
    "cognitoidentity": "cognito-identity",
    "cognitoidp": "cognito-idp",
    "applicationautoscaling": "application-autoscaling",
    "acmpca": "acm-pca",
    "bedrockagent": "bedrock-agent",
    # cloudhsmv2 matches botocore directly, no mapping needed
    "ec2instanceconnect": "ec2-instance-connect",
    "emrcontainers": "emr-containers",
    "emrserverless": "emr-serverless",
    "iotdata": "iot-data",
    "kinesisvideoarchivedmedia": "kinesis-video-archived-media",
    "lexv2models": "lexv2-models",
    "mediastoredata": "mediastore-data",
    "networkfirewall": "network-firewall",
    "rdsdata": "rds-data",
    "redshiftdata": "redshift-data",
    "resourcegroups": "resource-groups",
    "sagemakermetrics": "sagemaker-metrics",
    "sagemakerruntime": "sagemaker-runtime",
    "servicecatalogappregistry": "servicecatalog-appregistry",
    "servicequotas": "service-quotas",
    "ssoadmin": "sso-admin",
    "timestreaminfluxdb": "timestream-influxdb",
    "timestreamquery": "timestream-query",
    "timestreamwrite": "timestream-write",
    "vpclattice": "vpc-lattice",
    "workspacesweb": "workspaces-web",
    "mediapackagev2": "mediapackagev2",
}

# Skip these Moto dirs entirely
SKIP_DIRS = {
    "core",
    "instance_metadata",
    "awslambda_simple",
    "batch_simple",
    "dynamodb_v20111205",
    "s3bucket_path",
    "moto_api",
    "moto_proxy",
    "moto_server",
}

# Moto dirs that are already registered under a different registry key
# (moto_dir -> registry_key that already exists)
ALREADY_REGISTERED_AS = {
    "awslambda": "lambda",
    "cognitoidp": "cognito-idp",
    "cognitoidentity": None,  # not registered yet, but cognito-idp is native
    "resourcegroups": "resource-groups",
    "applicationautoscaling": None,  # register as applicationautoscaling
    "dynamodbstreams": "dynamodbstreams",  # already native
    "s3control": "s3control",  # already registered
}

# Signing names that map to multiple Moto services. We can only have one alias
# per signing name, so we pick the primary service and skip the rest.
# The secondary services are routed via TARGET_PREFIX_MAP or PATH_PATTERNS instead.
AMBIGUOUS_SIGNING_NAMES = {
    "elasticloadbalancing": "elbv2",  # elbv2 is the modern one; elb uses same signing name
    "sagemaker": "sagemaker",  # sagemakermetrics/sagemakerruntime routed by target prefix
    "timestream": "timestreamwrite",  # timestreamquery routed by target prefix
    "kinesisvideo": "kinesisvideo",  # kinesisvideoarchivedmedia routed differently
    "bedrock": "bedrock",  # bedrockagent routed by target prefix
    "mediastore": "mediastore",  # mediastoredata has different path patterns
    "servicecatalog": "servicecatalog",  # servicecatalogappregistry has different paths
}

# Signing name aliases that would conflict with existing native providers
# These signing names are already handled by native providers in the router
SKIP_ALIASES = {
    "lambda",  # native lambda provider
    "cognito-idp",  # native cognito-idp provider
    "cognito-identity",  # will route to cognitoidentity via alias
    "dynamodb",  # native dynamodb provider
    "es",  # native es provider
    "monitoring",  # already aliased to cloudwatch
    "email",  # already aliased to ses
    "states",  # already aliased to stepfunctions
    "tagging",  # already aliased to resourcegroupstaggingapi
    "resource-groups",  # native resource-groups provider
    "execute-api",  # handled by special execute-api routes
    "s3",  # native s3 provider
    "apigateway",  # native apigateway provider
    "sso",  # ambiguous - would need to distinguish sso vs ssoadmin
}

# Human-readable descriptions for common services
SERVICE_DESCRIPTIONS = {
    "rds": "Relational Database Service",
    "elbv2": "Application/Network Load Balancer",
    "elb": "Classic Load Balancer",
    "cloudfront": "CloudFront CDN",
    "autoscaling": "Auto Scaling Groups",
    "eks": "Elastic Kubernetes Service",
    "elasticache": "ElastiCache (Redis/Memcached)",
    "athena": "Athena SQL Analytics",
    "glue": "Glue ETL",
    "emr": "Elastic MapReduce",
    "emrcontainers": "EMR on EKS",
    "emrserverless": "EMR Serverless",
    "organizations": "AWS Organizations",
    "cloudtrail": "CloudTrail Audit Logging",
    "codebuild": "CodeBuild CI",
    "codecommit": "CodeCommit Git Repos",
    "codepipeline": "CodePipeline CI/CD",
    "codedeploy": "CodeDeploy Deployment",
    "wafv2": "WAF v2",
    "guardduty": "GuardDuty Threat Detection",
    "securityhub": "Security Hub",
    "inspector2": "Inspector Vulnerability Scanning",
    "acmpca": "ACM Private CA",
    "shield": "Shield DDoS Protection",
    "sagemaker": "SageMaker ML Platform",
    "backup": "AWS Backup",
    "xray": "X-Ray Distributed Tracing",
    "efs": "Elastic File System",
    "glacier": "Glacier Archive Storage",
    "dax": "DynamoDB Accelerator",
    "neptune": "Neptune Graph DB",
    "kafka": "Managed Streaming for Kafka",
    "mq": "Amazon MQ Message Brokers",
    "servicediscovery": "Cloud Map Service Discovery",
    "servicequotas": "Service Quotas",
    "ram": "Resource Access Manager",
    "ssoadmin": "SSO Admin",
    "rdsdata": "RDS Data API",
    "applicationautoscaling": "Application Auto Scaling",
    "elasticbeanstalk": "Elastic Beanstalk",
    "account": "AWS Account Management",
    "amp": "Managed Prometheus",
    "appconfig": "AppConfig",
    "appmesh": "App Mesh",
    "bedrock": "Bedrock AI",
    "bedrockagent": "Bedrock Agent",
    "budgets": "AWS Budgets",
    "ce": "Cost Explorer",
    "cognitoidentity": "Cognito Identity Pools",
    "comprehend": "Comprehend NLP",
    "connect": "Amazon Connect",
    "connectcampaigns": "Connect Campaigns",
    "cloudhsmv2": "CloudHSM v2",
    "clouddirectory": "Cloud Directory",
    "datapipeline": "Data Pipeline",
    "datasync": "DataSync",
    "databrew": "DataBrew",
    "directconnect": "Direct Connect",
    "dms": "Database Migration Service",
    "ds": "Directory Service",
    "dsql": "Aurora DSQL",
    "ebs": "EBS Direct APIs",
    "ec2instanceconnect": "EC2 Instance Connect",
    "forecast": "Amazon Forecast",
    "fsx": "FSx File Systems",
    "greengrass": "IoT Greengrass",
    "identitystore": "Identity Store",
    "iot": "IoT Core",
    "iotdata": "IoT Data Plane",
    "ivs": "Interactive Video Service",
    "kinesisvideo": "Kinesis Video Streams",
    "kinesisvideoarchivedmedia": "Kinesis Video Archived Media",
    "kinesisanalyticsv2": "Kinesis Analytics v2",
    "lakeformation": "Lake Formation",
    "lexv2models": "Lex v2 Models",
    "macie2": "Macie v2",
    "managedblockchain": "Managed Blockchain",
    "mediaconnect": "MediaConnect",
    "medialive": "MediaLive",
    "mediapackage": "MediaPackage",
    "mediapackagev2": "MediaPackage v2",
    "mediastore": "MediaStore",
    "mediastoredata": "MediaStore Data",
    "memorydb": "MemoryDB for Redis",
    "meteringmarketplace": "Marketplace Metering",
    "networkfirewall": "Network Firewall",
    "networkmanager": "Network Manager",
    "opensearchserverless": "OpenSearch Serverless",
    "osis": "OpenSearch Ingestion",
    "panorama": "Panorama",
    "personalize": "Amazon Personalize",
    "pinpoint": "Pinpoint",
    "pipes": "EventBridge Pipes",
    "polly": "Polly Text-to-Speech",
    "quicksight": "QuickSight",
    "redshiftdata": "Redshift Data API",
    "rekognition": "Rekognition",
    "resiliencehub": "Resilience Hub",
    "route53domains": "Route 53 Domains",
    "s3tables": "S3 Tables",
    "s3vectors": "S3 Vectors",
    "sagemakermetrics": "SageMaker Metrics",
    "sagemakerruntime": "SageMaker Runtime",
    "sdb": "SimpleDB",
    "servicecatalog": "Service Catalog",
    "servicecatalogappregistry": "Service Catalog App Registry",
    "signer": "Signer",
    "synthetics": "CloudWatch Synthetics",
    "textract": "Textract",
    "timestreaminfluxdb": "Timestream InfluxDB",
    "timestreamquery": "Timestream Query",
    "timestreamwrite": "Timestream Write",
    "transfer": "Transfer Family",
    "vpclattice": "VPC Lattice",
    "workspaces": "WorkSpaces",
    "workspacesweb": "WorkSpaces Web",
    "apigatewaymanagementapi": "API Gateway Management API",
    "cloudformation": "CloudFormation",
}


def find_botocore_data():
    """Find botocore data directory."""
    try:
        import botocore

        return os.path.join(os.path.dirname(botocore.__file__), "data")
    except ImportError:
        # Try common locations
        for p in [
            "/opt/homebrew/lib/python3.14/site-packages/botocore/data",
            "/opt/homebrew/lib/python3.12/site-packages/botocore/data",
        ]:
            if os.path.isdir(p):
                return p
    print("ERROR: Could not find botocore data directory", file=sys.stderr)
    sys.exit(1)


def load_botocore_metadata(botocore_name):
    """Load metadata from botocore service-2.json for a service."""
    svc_dir = os.path.join(BOTOCORE_DATA, botocore_name)
    if not os.path.isdir(svc_dir):
        return None
    versions = [v for v in sorted(os.listdir(svc_dir)) if os.path.isdir(os.path.join(svc_dir, v))]
    if not versions:
        return None
    latest = versions[-1]
    for ext in ["service-2.json.gz", "service-2.json"]:
        p = os.path.join(svc_dir, latest, ext)
        if os.path.exists(p):
            if ext.endswith(".gz"):
                with gzip.open(p, "rt") as f:
                    return json.load(f)["metadata"]
            else:
                with open(p) as f:
                    return json.load(f)["metadata"]
    return None


def get_all_moto_services():
    """Get all Moto service directories that have a models.py."""
    services = []
    for d in sorted(os.listdir(MOTO_BASE)):
        if d in SKIP_DIRS or d.startswith("__") or d.startswith("."):
            continue
        full = os.path.join(MOTO_BASE, d)
        if os.path.isdir(full) and os.path.exists(os.path.join(full, "models.py")):
            services.append(d)
    return services


def get_registered_services():
    """Parse registry.py to find already-registered service names."""
    with open(REGISTRY_PATH) as f:
        content = f.read()
    # Match keys in SERVICE_REGISTRY dict
    return set(re.findall(r'^\s+"([^"]+)":\s+ServiceInfo\(', content, re.MULTILINE))


def get_existing_target_prefixes():
    """Parse router.py to find already-registered TARGET_PREFIX_MAP entries."""
    with open(ROUTER_PATH) as f:
        content = f.read()
    return set(re.findall(r'^\s+"([^"]+)":\s+"[^"]+",', content, re.MULTILINE))


def get_existing_aliases():
    """Parse router.py to find already-registered SERVICE_NAME_ALIASES."""
    with open(ROUTER_PATH) as f:
        content = f.read()
    # Find SERVICE_NAME_ALIASES section
    match = re.search(r"SERVICE_NAME_ALIASES.*?{([^}]+)}", content, re.DOTALL)
    if match:
        return set(re.findall(r'"([^"]+)":', match.group(1)))
    return set()


def compute_registration(moto_dir):
    """Compute what needs to be added for a Moto service."""
    botocore_name = MOTO_TO_BOTOCORE.get(moto_dir, moto_dir)
    meta = load_botocore_metadata(botocore_name)
    if meta is None:
        return None

    protocol = meta.get("protocol", "json")
    # Normalize smithy-rpc-v2-cbor to json (cloudwatch uses this in newer botocore)
    if protocol == "smithy-rpc-v2-cbor":
        protocol = "json"

    target_prefix = meta.get("targetPrefix")
    signing_name = meta.get("signingName") or meta.get("endpointPrefix") or botocore_name
    endpoint_prefix = meta.get("endpointPrefix") or botocore_name
    description = SERVICE_DESCRIPTIONS.get(moto_dir, botocore_name.replace("-", " ").title())

    # The registry key is the moto_dir name (what forward_to_moto uses)
    registry_key = moto_dir

    result = {
        "moto_dir": moto_dir,
        "botocore_name": botocore_name,
        "registry_key": registry_key,
        "protocol": protocol,
        "description": description,
        "target_prefix": target_prefix,
        "signing_name": signing_name,
        "endpoint_prefix": endpoint_prefix,
        "registry_entry": None,
        "target_prefix_entry": None,
        "alias_entry": None,
        "path_pattern_entries": [],
    }

    # Registry entry
    result["registry_entry"] = (
        f'    "{registry_key}": ServiceInfo(\n'
        f'        "{registry_key}", ServiceStatus.MOTO_BACKED, "{protocol}", "{description}"\n'
        f"    ),"
    )

    # Routing: JSON protocol services use TARGET_PREFIX_MAP
    if protocol in ("json",) and target_prefix:
        result["target_prefix_entry"] = (target_prefix, registry_key)

    # Routing: signing name alias if signing_name != moto_dir
    if signing_name != moto_dir and signing_name not in SKIP_ALIASES:
        # Handle ambiguous signing names
        if signing_name in AMBIGUOUS_SIGNING_NAMES:
            preferred = AMBIGUOUS_SIGNING_NAMES[signing_name]
            if preferred == moto_dir:
                result["alias_entry"] = (signing_name, registry_key)
            # else: skip this alias, the secondary service needs target prefix routing
        else:
            result["alias_entry"] = (signing_name, registry_key)

    return result


def parse_current_files():
    """Parse registry.py and router.py to find insertion points."""
    with open(REGISTRY_PATH) as f:
        registry_content = f.read()
    with open(ROUTER_PATH) as f:
        router_content = f.read()
    return registry_content, router_content


def apply_changes(registrations, dry_run=True):
    """Apply the computed registrations to registry.py and router.py."""
    registry_content, router_content = parse_current_files()
    registered = get_registered_services()
    existing_prefixes = get_existing_target_prefixes()
    existing_aliases = get_existing_aliases()

    # Collect new entries
    new_registry_entries = []
    new_target_prefixes = []
    new_aliases = []
    seen_prefixes = set()  # track within this batch to avoid duplicates

    for reg in registrations:
        if reg is None:
            continue
        if reg["registry_key"] in registered:
            continue

        new_registry_entries.append(reg["registry_entry"])

        if reg["target_prefix_entry"]:
            prefix, svc = reg["target_prefix_entry"]
            if prefix not in existing_prefixes and prefix not in seen_prefixes:
                new_target_prefixes.append((prefix, svc))
                seen_prefixes.add(prefix)

        if reg["alias_entry"]:
            alias, svc = reg["alias_entry"]
            if alias not in existing_aliases:
                new_aliases.append((alias, svc))

    if not new_registry_entries:
        print("No new services to register.")
        return

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Changes to apply:")
    print(f"  Registry entries: {len(new_registry_entries)}")
    print(f"  Target prefix entries: {len(new_target_prefixes)}")
    print(f"  Alias entries: {len(new_aliases)}")

    if dry_run:
        print("\n--- registry.py additions ---")
        for entry in new_registry_entries:
            print(entry)
        if new_target_prefixes:
            print("\n--- router.py TARGET_PREFIX_MAP additions ---")
            for prefix, svc in sorted(new_target_prefixes):
                print(f'    "{prefix}": "{svc}",')
        if new_aliases:
            print("\n--- router.py SERVICE_NAME_ALIASES additions ---")
            for alias, svc in sorted(new_aliases):
                print(f'    "{alias}": "{svc}",')
        return

    # Write registry.py
    # Insert before the closing "}" of SERVICE_REGISTRY
    lines = registry_content.split("\n")
    # Find the line with the closing brace of SERVICE_REGISTRY
    insert_idx = None
    brace_depth = 0
    in_registry = False
    for i, line in enumerate(lines):
        if "SERVICE_REGISTRY" in line and "{" in line:
            in_registry = True
            brace_depth = 1
            continue
        if in_registry:
            brace_depth += line.count("{") - line.count("}")
            if brace_depth == 0:
                insert_idx = i
                break

    if insert_idx is None:
        print("ERROR: Could not find SERVICE_REGISTRY closing brace", file=sys.stderr)
        return

    # Build the new entries block
    new_block = "    # Auto-registered Moto-backed services\n"
    new_block += "\n".join(new_registry_entries)
    new_block += "\n"

    lines.insert(insert_idx, new_block)
    with open(REGISTRY_PATH, "w") as f:
        f.write("\n".join(lines))
    print(f"  Wrote {len(new_registry_entries)} entries to {REGISTRY_PATH}")

    # Write router.py - TARGET_PREFIX_MAP additions
    if new_target_prefixes:
        lines = router_content.split("\n")
        # Find the closing brace of TARGET_PREFIX_MAP
        in_map = False
        map_close_idx = None
        for i, line in enumerate(lines):
            if "TARGET_PREFIX_MAP" in line:
                in_map = True
            if in_map and line.strip() == "}":
                map_close_idx = i
                break

        if map_close_idx is not None:
            new_entries = []
            for prefix, svc in sorted(new_target_prefixes):
                new_entries.append(f'    "{prefix}": "{svc}",')
            lines.insert(map_close_idx, "\n".join(new_entries))
            router_content = "\n".join(lines)
            print(f"  Added {len(new_target_prefixes)} TARGET_PREFIX_MAP entries")

    # Write router.py - SERVICE_NAME_ALIASES additions
    if new_aliases:
        lines = router_content.split("\n")
        # Find the closing brace of SERVICE_NAME_ALIASES
        in_aliases = False
        alias_close_idx = None
        for i, line in enumerate(lines):
            if "SERVICE_NAME_ALIASES" in line:
                in_aliases = True
            if in_aliases and line.strip() == "}":
                alias_close_idx = i
                break

        if alias_close_idx is not None:
            new_entries = []
            for alias, svc in sorted(new_aliases):
                new_entries.append(f'    "{alias}": "{svc}",')
            lines.insert(alias_close_idx, "\n".join(new_entries))
            router_content = "\n".join(lines)
            print(f"  Added {len(new_aliases)} SERVICE_NAME_ALIASES entries")

    with open(ROUTER_PATH, "w") as f:
        f.write(router_content)
    print(f"  Wrote router changes to {ROUTER_PATH}")


def main():
    global BOTOCORE_DATA

    parser = argparse.ArgumentParser(description="Batch-register Moto-backed services")
    parser.add_argument("--all-moto", action="store_true", help="Register all Moto services")
    parser.add_argument("--service", type=str, help="Register a specific service (moto dir name)")
    parser.add_argument("--write", action="store_true", help="Write changes (default: dry-run)")
    parser.add_argument("--list", action="store_true", help="List unregistered Moto services")
    args = parser.parse_args()

    BOTOCORE_DATA = find_botocore_data()
    print(f"Using botocore data: {BOTOCORE_DATA}")

    registered = get_registered_services()
    all_moto = get_all_moto_services()

    if args.list:
        unregistered = [s for s in all_moto if s not in registered]
        print(f"\nUnregistered Moto services ({len(unregistered)}):")
        for s in unregistered:
            botocore_name = MOTO_TO_BOTOCORE.get(s, s)
            meta = load_botocore_metadata(botocore_name)
            proto = meta.get("protocol", "?") if meta else "?"
            print(f"  {s:35s} protocol={proto}")
        return

    if args.service:
        services = [args.service]
    elif args.all_moto:
        services = [s for s in all_moto if s not in registered]
    else:
        parser.print_help()
        return

    print(f"Processing {len(services)} service(s)...")
    registrations = []
    skipped = []
    for svc in services:
        if svc in registered:
            print(f"  SKIP {svc} (already registered)")
            continue
        # Check if registered under a different name
        if svc in ALREADY_REGISTERED_AS:
            alt = ALREADY_REGISTERED_AS[svc]
            if alt and alt in registered:
                print(f"  SKIP {svc} (already registered as '{alt}')")
                continue
        reg = compute_registration(svc)
        if reg is None:
            skipped.append(svc)
            print(f"  SKIP {svc} (no botocore metadata found)")
            continue
        registrations.append(reg)
        print(f"  OK   {svc} -> protocol={reg['protocol']}, signing={reg['signing_name']}")

    if skipped:
        print(f"\nSkipped {len(skipped)} services (no botocore metadata): {', '.join(skipped)}")

    apply_changes(registrations, dry_run=not args.write)


if __name__ == "__main__":
    main()
