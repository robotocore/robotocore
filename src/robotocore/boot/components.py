"""Predefined service components for boot orchestration.

Each component wraps one of robotocore's background services with start/stop
callables and a health check so the BootOrchestrator can manage them.
"""

from __future__ import annotations

import logging
import os
import socket

from robotocore.boot.orchestrator import ServiceComponent

logger = logging.getLogger(__name__)


def _check_port(host: str, port: int) -> bool:
    """Return True if a TCP connection to host:port succeeds."""
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Component factories
# ---------------------------------------------------------------------------


def core_component() -> ServiceComponent:
    """Moto backends initialization -- the foundation for all services."""

    _ready = False

    def start() -> None:
        nonlocal _ready
        from robotocore.services.loader import init_loader

        init_loader()
        _ready = True

    def health_check() -> bool:
        return _ready

    return ServiceComponent(
        name="core",
        depends_on=[],
        start=start,
        health_check=health_check,
        stop=None,
        required=True,
        timeout=30.0,
    )


def state_component() -> ServiceComponent:
    """State manager -- auto-loads/saves emulator state."""

    _ready = False

    def start() -> None:
        nonlocal _ready
        if os.environ.get("ROBOTOCORE_STATE_DIR"):
            from robotocore.state.manager import get_state_manager

            manager = get_state_manager()
            if not manager.restore_on_startup():
                manager.load()
        _ready = True

    def health_check() -> bool:
        return _ready

    def stop() -> None:
        if os.environ.get("ROBOTOCORE_PERSIST", "0") == "1":
            from robotocore.state.manager import get_state_manager

            manager = get_state_manager()
            if manager.state_dir:
                manager.save()

    return ServiceComponent(
        name="state",
        depends_on=["core"],
        start=start,
        health_check=health_check,
        stop=stop,
        required=True,
        timeout=30.0,
    )


def plugins_component() -> ServiceComponent:
    """Extension registry discovery."""

    _ready = False

    def start() -> None:
        nonlocal _ready
        from robotocore.extensions import discover_extensions

        discover_extensions()
        _ready = True

    def health_check() -> bool:
        return _ready

    return ServiceComponent(
        name="plugins",
        depends_on=["core"],
        start=start,
        health_check=health_check,
        stop=None,
        required=True,
        timeout=10.0,
    )


def dns_component() -> ServiceComponent:
    """DNS server on port 53 (resolves *.amazonaws.com locally)."""

    _ready = False

    def start() -> None:
        nonlocal _ready
        from robotocore.dns.server import start_dns_server

        server = start_dns_server()
        if server is None:
            # Disabled or failed to bind -- still considered "ready" since optional
            _ready = True
            return
        _ready = True

    def health_check() -> bool:
        return _ready

    def stop() -> None:
        from robotocore.dns.server import stop_dns_server

        stop_dns_server()

    return ServiceComponent(
        name="dns",
        depends_on=["core"],
        start=start,
        health_check=health_check,
        stop=stop,
        required=False,
        timeout=5.0,
    )


def smtp_component() -> ServiceComponent:
    """SMTP server on port 1025 for SES email capture."""

    _ready = False

    def start() -> None:
        nonlocal _ready
        from robotocore.services.ses.smtp_server import start_smtp_server

        start_smtp_server()
        _ready = True

    def health_check() -> bool:
        return _ready

    def stop() -> None:
        from robotocore.services.ses.smtp_server import stop_smtp_server

        stop_smtp_server()

    return ServiceComponent(
        name="smtp",
        depends_on=["core"],
        start=start,
        health_check=health_check,
        stop=stop,
        required=False,
        timeout=5.0,
    )


def https_component() -> ServiceComponent:
    """HTTPS server on port 443 (if TLS is enabled)."""

    _ready = False

    def start() -> None:
        nonlocal _ready
        from robotocore.gateway.tls import TLSConfig

        config = TLSConfig.from_env()
        if not config.enabled:
            logger.info("HTTPS component disabled (HTTPS_DISABLED=1)")
            _ready = True
            return

        # The actual HTTPS uvicorn is started via main.py's _start_https_server
        # We just mark readiness here -- the main.py integration calls it.
        _ready = True

    def health_check() -> bool:
        return _ready

    return ServiceComponent(
        name="https",
        depends_on=["core"],
        start=start,
        health_check=health_check,
        stop=None,
        required=False,
        timeout=10.0,
    )


def background_engines_component() -> ServiceComponent:
    """Background engines: Lambda ESM, CloudWatch alarms, Synthetics, SQS metrics, DDB TTL."""

    _ready = False

    def start() -> None:
        nonlocal _ready
        from robotocore.services.cloudwatch.alarm_scheduler import get_alarm_scheduler
        from robotocore.services.dynamodb.ttl import get_ttl_scanner
        from robotocore.services.lambda_.event_source import get_engine
        from robotocore.services.sqs.metrics import get_sqs_metrics_publisher
        from robotocore.services.synthetics.scheduler import get_canary_scheduler

        get_engine().start()
        get_alarm_scheduler().start()
        get_canary_scheduler().start()
        get_sqs_metrics_publisher().start()
        get_ttl_scanner().start()
        _ready = True

    def health_check() -> bool:
        return _ready

    def stop() -> None:
        from robotocore.services.dynamodb.ttl import get_ttl_scanner

        get_ttl_scanner().stop()

    return ServiceComponent(
        name="engines",
        depends_on=["core", "state"],
        start=start,
        health_check=health_check,
        stop=stop,
        required=True,
        timeout=10.0,
    )


def gateway_component() -> ServiceComponent:
    """Main HTTP gateway -- placeholder for boot status tracking.

    The actual uvicorn server is started by main.py; this component just
    represents gateway readiness in the boot status.
    """

    _ready = False

    def start() -> None:
        nonlocal _ready
        from robotocore.observability.hooks import run_init_hooks

        run_init_hooks("ready")
        _ready = True

    def health_check() -> bool:
        return _ready

    return ServiceComponent(
        name="gateway",
        depends_on=["core", "state", "plugins"],
        start=start,
        health_check=health_check,
        stop=None,
        required=True,
        timeout=5.0,
    )


def register_all_components() -> None:
    """Register all standard components with the global orchestrator."""
    from robotocore.boot.orchestrator import get_orchestrator

    orch = get_orchestrator()
    orch.register(core_component())
    orch.register(state_component())
    orch.register(plugins_component())
    orch.register(dns_component())
    orch.register(smtp_component())
    orch.register(https_component())
    orch.register(background_engines_component())
    orch.register(gateway_component())
