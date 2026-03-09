"""Startup banner for Robotocore."""

import logging

from robotocore.services.registry import SERVICE_REGISTRY, ServiceStatus

logger = logging.getLogger(__name__)

BANNER = r"""
 ____       _           _
|  _ \ ___ | |__   ___ | |_ ___   ___ ___  _ __ ___
| |_) / _ \| '_ \ / _ \| __/ _ \ / __/ _ \| '__/ _ \
|  _ < (_) | |_) | (_) | || (_) | (_| (_) | | |  __/
|_| \_\___/|_.__/ \___/ \__\___/ \___\___/|_|  \___|
"""


def print_banner(*, host: str, port: int) -> None:
    """Print the startup banner with service info."""
    total_services = len(SERVICE_REGISTRY)
    native_count = sum(1 for s in SERVICE_REGISTRY.values() if s.status == ServiceStatus.NATIVE)

    lines = [
        BANNER,
        "Robotocore v1.0.0 -- Free AWS Emulator",
        f"Port: {port} | Services: {total_services} | Native providers: {native_count}",
        "",
        f"Listening on http://{host}:{port}",
        f"DNS: use AWS_ENDPOINT_URL=http://localhost:{port}",
        "",
        "Ready.",
        "",
    ]
    for line in lines:
        # Print directly (not via logging) so it appears before log config
        print(line)
