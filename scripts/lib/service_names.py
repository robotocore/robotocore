"""Canonical service name resolution.

Every registered service has up to 5 different names depending on context:
  registry_name:  key in SERVICE_REGISTRY (e.g. "cognito-idp")
  botocore_name:  botocore service id (e.g. "cognito-idp")
  provider_dir:   directory under src/robotocore/services/ (e.g. "cognito")
  moto_dir:       directory under vendor/moto/moto/ (e.g. "cognitoidp")
  test_stem:      compat test file stem (e.g. "cognito" from test_cognito_compat.py)

This module consolidates the 10+ fragmented name-mapping tables scattered
across the codebase into one source of truth.
"""

from __future__ import annotations

import sys
import warnings
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_SERVICES = PROJECT_ROOT / "src" / "robotocore" / "services"
MOTO_BASE = PROJECT_ROOT / "vendor" / "moto" / "moto"
COMPAT_TESTS = PROJECT_ROOT / "tests" / "compatibility"

# ── Explicit overrides for names that can't be derived by convention ──

# registry name → botocore name (only where they differ)
_BOTOCORE_OVERRIDES: dict[str, str] = {
    "acmpca": "acm-pca",
    "applicationautoscaling": "application-autoscaling",
    "bedrockagent": "bedrock-agent",
    "cognitoidentity": "cognito-identity",
    "ec2instanceconnect": "ec2-instance-connect",
    "emrcontainers": "emr-containers",
    "emrserverless": "emr-serverless",
    "lexv2models": "lexv2-models",
    "networkfirewall": "network-firewall",
    "redshiftdata": "redshift-data",
    "sagemakermetrics": "sagemaker-metrics",
    "servicecatalogappregistry": "servicecatalog-appregistry",
    "timestreaminfluxdb": "timestream-influxdb",
    "timestreamquery": "timestream-query",
    "timestreamwrite": "timestream-write",
    "workspacesweb": "workspaces-web",
    "iotdata": "iot-data",
    "rdsdata": "rds-data",
    "ssoadmin": "sso-admin",
    "vpclattice": "vpc-lattice",
}

# registry name → native provider directory name (only where they differ)
_PROVIDER_DIR_OVERRIDES: dict[str, str] = {
    "lambda": "lambda_",
    "cognito-idp": "cognito",
    "resource-groups": "resource_groups",
    "resourcegroupstaggingapi": "tagging",
    "es": "opensearch",  # shared provider
    "sesv2": "ses",  # shared provider
    "logs": "cloudwatch",  # logs_provider.py lives in cloudwatch/
    "iotdata": "iot",  # shared provider
    "rdsdata": "rds",  # shared provider
}

# registry name → Moto vendor directory name (only where they differ)
_MOTO_DIR_OVERRIDES: dict[str, str] = {
    "lambda": "awslambda",
    "cognito-idp": "cognitoidp",
    "cognitoidentity": "cognitoidentity",
    "resource-groups": "resourcegroups",
    "resourcegroupstaggingapi": "resourcegroupstaggingapi",
    "ec2instanceconnect": "ec2instanceconnect",
    "kinesis-video-archived-media": "kinesisvideoarchivedmedia",
    "mediastore-data": "mediastoredata",
    "sagemaker-runtime": "sagemakerruntime",
    "sagemaker-metrics": "sagemakermetrics",
    "bedrock-runtime": "bedrockruntime",
    "service-quotas": "servicequotas",
    "application-autoscaling": "applicationautoscaling",
    "applicationautoscaling": "applicationautoscaling",
    "s3tables": "s3tables",
    "s3vectors": "s3vectors",
}

# registry name → compat test file stem (only where they differ from convention)
_TEST_STEM_OVERRIDES: dict[str, str] = {
    "cognito-idp": "cognito_idp",
    "resourcegroupstaggingapi": "resource_groups_tagging",
    "dynamodbstreams": "dynamodbstreams",
}

# Test file stems that are NOT real services (integration tests, infra tests)
_TEST_STEM_SKIP = {
    "apigateway_lambda",
    "cross_service",
    "state_persistence",
    "lambda_event_source",
    "cfn_e2e",
    "concurrent_requests",
}


@dataclass(frozen=True)
class ServiceNames:
    registry: str
    botocore: str
    provider_dir: str | None  # None if no native provider
    moto_dir: str | None  # None if Moto doesn't have this service
    test_stem: str | None  # None if no compat test file exists


def _derive_botocore_name(registry_name: str) -> str:
    """Derive botocore name from registry name."""
    if registry_name in _BOTOCORE_OVERRIDES:
        return _BOTOCORE_OVERRIDES[registry_name]
    return registry_name


def _derive_provider_dir(registry_name: str) -> str | None:
    """Find the native provider directory, if it exists."""
    if registry_name in _PROVIDER_DIR_OVERRIDES:
        dirname = _PROVIDER_DIR_OVERRIDES[registry_name]
        provider_path = SRC_SERVICES / dirname / "provider.py"
        if not provider_path.exists():
            # Check for service-specific provider files (e.g., logs_provider.py)
            alt = SRC_SERVICES / dirname / f"{registry_name.replace('-', '_')}_provider.py"
            if alt.exists():
                return dirname
            # cloudwatch/logs_provider.py special case
            if registry_name == "logs":
                logs_provider = SRC_SERVICES / "cloudwatch" / "logs_provider.py"
                if logs_provider.exists():
                    return "cloudwatch"
            return None
        return dirname

    # Convention: registry name with hyphens → underscores
    candidates = [
        registry_name,
        registry_name.replace("-", "_"),
    ]
    for c in candidates:
        if (SRC_SERVICES / c / "provider.py").exists():
            return c
    return None


def _has_moto_models(dirname: str) -> bool:
    """Check if a Moto directory has models (either models.py or models/__init__.py)."""
    d = MOTO_BASE / dirname
    return (d / "models.py").exists() or (d / "models" / "__init__.py").exists()


def _derive_moto_dir(registry_name: str) -> str | None:
    """Find the Moto vendor directory, if it exists."""
    if registry_name in _MOTO_DIR_OVERRIDES:
        dirname = _MOTO_DIR_OVERRIDES[registry_name]
        if _has_moto_models(dirname):
            return dirname
        return None

    # Convention: strip hyphens, try as-is
    candidates = [
        registry_name,
        registry_name.replace("-", ""),
        registry_name.replace("-", "_"),
    ]
    for c in candidates:
        if _has_moto_models(c):
            return c
    return None


def _derive_test_stem(registry_name: str) -> str | None:
    """Find the compat test file stem, if it exists."""
    if registry_name in _TEST_STEM_OVERRIDES:
        stem = _TEST_STEM_OVERRIDES[registry_name]
        if (COMPAT_TESTS / f"test_{stem}_compat.py").exists():
            return stem
        return None

    # Convention: hyphens → underscores
    candidates = [
        registry_name.replace("-", "_"),
        registry_name,
    ]
    for c in candidates:
        if (COMPAT_TESTS / f"test_{c}_compat.py").exists():
            return c
    return None


def resolve_all_services() -> dict[str, ServiceNames]:
    """Build canonical name mapping for every registered service.

    Returns {registry_name: ServiceNames}.
    """
    sys.path.insert(0, str(PROJECT_ROOT / "src"))
    import botocore.session as _botocore_session

    from robotocore.services.registry import SERVICE_REGISTRY

    _session = _botocore_session.get_session()
    result: dict[str, ServiceNames] = {}
    unresolved: list[str] = []

    for name in sorted(SERVICE_REGISTRY):
        bc_name = _derive_botocore_name(name)
        provider_dir = _derive_provider_dir(name)
        moto_dir = _derive_moto_dir(name)
        test_stem = _derive_test_stem(name)

        # Validate botocore name resolves
        try:
            _session.get_service_model(bc_name)
        except Exception:
            unresolved.append(f"{name}: botocore name '{bc_name}' does not resolve")

        result[name] = ServiceNames(
            registry=name,
            botocore=bc_name,
            provider_dir=provider_dir,
            moto_dir=moto_dir,
            test_stem=test_stem,
        )

    if unresolved:
        for msg in unresolved:
            warnings.warn(f"Service name resolution: {msg}", stacklevel=2)

    return result


def resolve_service(registry_name: str) -> ServiceNames:
    """Resolve names for a single service."""
    all_services = resolve_all_services()
    if registry_name not in all_services:
        raise KeyError(f"Unknown service: {registry_name}")
    return all_services[registry_name]


if __name__ == "__main__":
    services = resolve_all_services()
    print(f"Resolved {len(services)} services:\n")
    print(f"{'Registry':<35} {'Botocore':<30} {'Provider':<18} {'Moto':<25} {'Test'}")
    print("-" * 140)
    missing_botocore = 0
    missing_moto = 0
    missing_test = 0
    for name, sn in sorted(services.items()):
        prov = sn.provider_dir or "-"
        moto = sn.moto_dir or "-"
        test = sn.test_stem or "-"
        if sn.moto_dir is None:
            missing_moto += 1
        if sn.test_stem is None:
            missing_test += 1
        print(f"{sn.registry:<35} {sn.botocore:<30} {prov:<18} {moto:<25} {test}")
    print(f"\nTotal: {len(services)}")
    print(f"  Missing moto_dir: {missing_moto}")
    print(f"  Missing test_stem: {missing_test}")
