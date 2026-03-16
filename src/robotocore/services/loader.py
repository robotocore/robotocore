"""Service loading controls and runtime provider overrides.

Supports:
- SERVICES env var: comma-separated list of services to enable
- STRICT_SERVICE_LOADING: only load listed services into memory
- EAGER_SERVICE_LOADING: initialize all backends at startup
- PROVIDER_OVERRIDE_<SERVICE>: swap provider implementations at runtime
"""

import importlib
import logging
import os
from typing import Any

from robotocore.services.registry import SERVICE_REGISTRY, ServiceStatus

log = logging.getLogger(__name__)

# Module-level state
_allowed_services: set[str] | None = None
_provider_overrides: dict[str, str] = {}
_initialized_services: set[str] = set()
_initialized = False


logger = logging.getLogger(__name__)


def _normalize_service_name(name: str) -> str:
    """Strip whitespace and lowercase a service name."""
    return name.strip().lower()


def parse_services_env() -> set[str] | None:
    """Parse the SERVICES env var into a set of service names.

    Returns None if SERVICES is not set or empty (meaning all services).
    """
    raw = os.environ.get("SERVICES", "").strip()
    if not raw:
        return None

    result: set[str] = set()
    for name in raw.split(","):
        normalized = _normalize_service_name(name)
        if not normalized:
            continue
        if normalized not in SERVICE_REGISTRY:
            log.warning("SERVICES: unknown service '%s', skipping", normalized)
            continue
        result.add(normalized)

    return result if result else None


def parse_provider_overrides() -> dict[str, str]:
    """Parse PROVIDER_OVERRIDE_<SERVICE> env vars.

    Returns a dict mapping service name -> override value.
    Valid values: 'native', 'moto', or a Python dotted path.
    """
    overrides: dict[str, str] = {}
    prefix = "PROVIDER_OVERRIDE_"

    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        service_name = _normalize_service_name(key[len(prefix) :].replace("_", "-"))
        # Handle services with underscores in env var names (e.g., COGNITO_IDP -> cognito-idp)
        # Also try without dash replacement for single-word services
        if service_name not in SERVICE_REGISTRY:
            # Try with the raw lowercase (no dash replacement)
            alt_name = _normalize_service_name(key[len(prefix) :].lower())
            if alt_name in SERVICE_REGISTRY:
                service_name = alt_name
            else:
                log.warning(
                    "PROVIDER_OVERRIDE: unknown service '%s' (from %s), ignoring",
                    service_name,
                    key,
                )
                continue

        value = value.strip()
        if not value:
            continue
        overrides[service_name] = value

    return overrides


def resolve_provider_override(service_name: str, override_value: str) -> Any | None:
    """Resolve a provider override value to a callable or None.

    Args:
        service_name: The service name.
        override_value: 'native', 'moto', or a Python dotted path.

    Returns:
        A handler callable for 'native' or dotted path, None for 'moto'.
    """
    if override_value == "moto":
        return None  # Signals: use Moto bridge

    if override_value == "native":
        # Import the native provider from the standard location
        from robotocore.gateway.app import NATIVE_PROVIDERS

        handler = NATIVE_PROVIDERS.get(service_name)
        if handler is None:
            log.warning(
                "PROVIDER_OVERRIDE: no native provider for '%s', using default",
                service_name,
            )
        return handler

    # Dotted path: import the class/function
    try:
        module_path, attr_name = override_value.rsplit(".", 1)
        module = importlib.import_module(module_path)
        return getattr(module, attr_name)
    except (ValueError, ImportError, AttributeError) as exc:
        log.warning(
            "PROVIDER_OVERRIDE: failed to load '%s' for service '%s': %s",
            override_value,
            service_name,
            exc,
        )
        return None


def is_service_allowed(service_name: str) -> bool:
    """Check if a service is in the allowed list.

    If SERVICES env var is not set, all registered services are allowed.
    """
    global _allowed_services
    if _allowed_services is None:
        return service_name in SERVICE_REGISTRY
    return service_name in _allowed_services


def get_allowed_services() -> set[str] | None:
    """Return the set of explicitly allowed services, or None if all are allowed."""
    return _allowed_services


def get_provider_overrides() -> dict[str, str]:
    """Return the current provider overrides."""
    return dict(_provider_overrides)


def get_effective_provider(service_name: str, native_providers: dict[str, Any]) -> Any | None:
    """Get the effective provider for a service, considering overrides.

    Returns:
        A handler callable if native/custom, None if should use Moto bridge.
    """
    override = _provider_overrides.get(service_name)
    if override is not None:
        if override == "moto":
            return None
        if override == "native":
            return native_providers.get(service_name)
        # Dotted path — resolve lazily
        return resolve_provider_override(service_name, override)

    # No override — use default
    return native_providers.get(service_name)


def initialize_service(service_name: str) -> None:
    """Initialize a service backend (trigger Moto backend load).

    This is used for eager loading — calling get_backend forces Moto to
    initialize the backend for this service.
    """
    if service_name in _initialized_services:
        return

    log.debug("Loading service %s...", service_name)
    try:
        from moto.backends import get_backend

        get_backend(service_name)
    except Exception as exc:
        # Some services may not have Moto backends (native-only)
        logger.debug("initialize_service: get_backend failed (non-fatal): %s", exc)
    _initialized_services.add(service_name)


def eager_load_services() -> None:
    """Initialize all enabled service backends at startup.

    Only called when EAGER_SERVICE_LOADING=1.
    """
    services = _allowed_services if _allowed_services is not None else set(SERVICE_REGISTRY.keys())
    for name in sorted(services):
        initialize_service(name)
    log.info("Eager-loaded %d service backends", len(services))


def init_loader() -> None:
    """Initialize the service loader from environment variables.

    Call this once at startup. Sets up:
    - Allowed service list from SERVICES env var
    - Provider overrides from PROVIDER_OVERRIDE_* env vars
    - Eager loading if EAGER_SERVICE_LOADING=1
    """
    global _allowed_services, _provider_overrides, _initialized

    _allowed_services = parse_services_env()
    _provider_overrides = parse_provider_overrides()

    if _allowed_services is not None:
        log.info(
            "Service filter active: %d of %d services enabled",
            len(_allowed_services),
            len(SERVICE_REGISTRY),
        )
    if _provider_overrides:
        log.info("Provider overrides: %s", _provider_overrides)

    if os.environ.get("EAGER_SERVICE_LOADING", "0") == "1":
        eager_load_services()

    _initialized = True


def reset_loader() -> None:
    """Reset loader state — for testing only."""
    global _allowed_services, _provider_overrides, _initialized_services, _initialized
    _allowed_services = None
    _provider_overrides = {}
    _initialized_services = set()
    _initialized = False


def get_service_info_with_status(
    service_name: str,
) -> dict[str, Any]:
    """Get service info including enabled/disabled status.

    Used by the /_robotocore/services endpoint.
    """
    info = SERVICE_REGISTRY.get(service_name)
    if info is None:
        return {"name": service_name, "enabled": False, "registered": False}

    enabled = is_service_allowed(service_name)
    override = _provider_overrides.get(service_name)
    stype = "native" if info.status == ServiceStatus.NATIVE else "moto"

    result: dict[str, Any] = {
        "name": service_name,
        "status": stype,
        "protocol": info.protocol,
        "description": info.description,
        "enabled": enabled,
    }
    if override:
        result["provider_override"] = override

    return result
