"""Resource browser: introspect Moto backends to enumerate resources."""

import logging

from moto.core.base_backend import BackendDict

logger = logging.getLogger(__name__)

DEFAULT_ACCOUNT_ID = "123456789012"

# Map of (service, attribute) -> resource type name
# Each entry tells us which attribute on the Moto backend holds resources
RESOURCE_ATTRS = {
    "s3": [("buckets", "Buckets")],
    "sqs": [("queues", "Queues")],
    "sns": [("topics", "Topics"), ("subscriptions", "Subscriptions")],
    "dynamodb": [("tables", "Tables")],
    "lambda": [("_lambdas", "Functions")],
    "iam": [("roles", "Roles"), ("users", "Users"), ("policies", "Policies")],
    "ec2": [("instances", "Instances"), ("vpcs", "VPCs")],
    "ecs": [("clusters", "Clusters"), ("task_definitions", "TaskDefinitions")],
    "kinesis": [("streams", "Streams")],
    "events": [("rules", "Rules"), ("event_buses", "EventBuses")],
    "logs": [("groups", "LogGroups")],
    "secretsmanager": [("secrets", "Secrets")],
    "ssm": [("_parameters", "Parameters")],
    "stepfunctions": [("state_machines", "StateMachines")],
    "kms": [("keys", "Keys")],
    "cloudformation": [("stacks", "Stacks")],
    "route53": [("hosted_zones", "HostedZones")],
    "cloudwatch": [("alarms", "Alarms")],
    "rds": [("databases", "DBInstances"), ("clusters", "DBClusters")],
    "elbv2": [("load_balancers", "LoadBalancers"), ("target_groups", "TargetGroups")],
    "eks": [("clusters", "Clusters")],
    "cloudfront": [("distributions", "Distributions")],
}


def _get_backend(service_name: str, account_id: str = DEFAULT_ACCOUNT_ID):
    """Safely get a Moto backend instance."""
    try:
        from moto.backends import get_backend

        backend_dict = get_backend(service_name)
        if isinstance(backend_dict, BackendDict):
            if account_id in backend_dict:
                regions = backend_dict[account_id]
                if "us-east-1" in regions:
                    return regions["us-east-1"]
                if "global" in regions:
                    return regions["global"]
                # Return first available region
                for region in regions:
                    return regions[region]
        return None
    except Exception:
        return None


def get_resource_counts(account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, int]:
    """Get resource counts for all services."""
    from robotocore.services.registry import SERVICE_REGISTRY

    counts = {}
    for service_name in sorted(SERVICE_REGISTRY.keys()):
        backend = _get_backend(service_name, account_id=account_id)
        if backend is None:
            continue

        total = 0
        if service_name in RESOURCE_ATTRS:
            for attr, _ in RESOURCE_ATTRS[service_name]:
                obj = getattr(backend, attr, None)
                if obj is not None:
                    try:
                        total += len(obj)
                    except TypeError as exc:
                        logger.debug("get_resource_counts: len failed (non-fatal): %s", exc)
        if total > 0:
            counts[service_name] = total

    return counts


def get_service_resources(service_name: str, account_id: str = DEFAULT_ACCOUNT_ID) -> list[dict]:
    """Get detailed resource list for a specific service."""
    backend = _get_backend(service_name, account_id=account_id)
    if backend is None:
        return []

    resources = []

    if service_name in RESOURCE_ATTRS:
        for attr, type_name in RESOURCE_ATTRS[service_name]:
            obj = getattr(backend, attr, None)
            if obj is None:
                continue
            try:
                items = obj.values() if isinstance(obj, dict) else obj
                for item in items:
                    entry = {"type": type_name}
                    # Try to extract ARN and name
                    for arn_attr in ("arn", "physical_resource_id", "resource_arn"):
                        val = getattr(item, arn_attr, None)
                        if val:
                            entry["arn"] = str(val)
                            break
                    for name_attr in ("name", "Name", "id", "table_name", "function_name"):
                        val = getattr(item, name_attr, None)
                        if val:
                            entry["name"] = str(val)
                            break
                    resources.append(entry)
            except Exception:
                logger.debug("Error browsing %s.%s", service_name, attr, exc_info=True)
    else:
        # Generic: list all dict-like attributes that might hold resources
        for attr in dir(backend):
            if attr.startswith("_"):
                continue
            obj = getattr(backend, attr, None)
            if isinstance(obj, dict) and len(obj) > 0:
                # Heuristic: dicts with string keys might be resource stores
                first_key = next(iter(obj))
                if isinstance(first_key, str):
                    resources.append(
                        {
                            "type": attr,
                            "count": len(obj),
                        }
                    )

    return resources
