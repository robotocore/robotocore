"""Service metadata from botocore specs — protocol types and operation names."""

from functools import lru_cache

import botocore.session


@lru_cache
def _get_session() -> botocore.session.Session:
    return botocore.session.get_session()


@lru_cache
def get_service_protocol(service_name: str) -> str | None:
    """Return the AWS protocol type for a service (query, json, rest-json, rest-xml, ec2)."""
    try:
        model = _get_session().get_service_model(service_name)
        return model.protocol
    except Exception:  # noqa: BLE001
        return None


@lru_cache
def get_service_json_version(service_name: str) -> str | None:
    """Return the JSON version for a service (e.g. '1.0', '1.1'), or None."""
    try:
        model = _get_session().get_service_model(service_name)
        return model.metadata.get("jsonVersion")
    except Exception:  # noqa: BLE001
        return None


@lru_cache
def get_service_operations(service_name: str) -> frozenset[str]:
    """Return the set of operation names for a service."""
    try:
        model = _get_session().get_service_model(service_name)
        return frozenset(model.operation_names)
    except Exception:  # noqa: BLE001
        return frozenset()
