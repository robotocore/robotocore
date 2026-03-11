"""OpenSearch endpoint strategies matching LocalStack's OPENSEARCH_ENDPOINT_STRATEGY.

Supports 3 strategies controlled by the OPENSEARCH_ENDPOINT_STRATEGY env var:
- domain (default): http://{domain_name}.{region}.opensearch.localhost.localstack.cloud:4566
- path: http://localhost:4566/opensearch/{region}/{domain_name}
- port: Allocate a unique port per domain from range 4510-4559
"""

import os
import re
import threading
from enum import StrEnum

GATEWAY_PORT = 4566
GATEWAY_HOST = "localhost"
LOCALSTACK_HOST = "localhost.localstack.cloud"

# Port-strategy allocation range
PORT_RANGE_START = 4510
PORT_RANGE_END = 4559  # inclusive, 50 ports


class OpenSearchEndpointStrategy(StrEnum):
    DOMAIN = "domain"
    PATH = "path"
    PORT = "port"


def get_opensearch_endpoint_strategy() -> OpenSearchEndpointStrategy:
    """Read the current OpenSearch endpoint strategy from the environment."""
    raw = os.environ.get("OPENSEARCH_ENDPOINT_STRATEGY", "domain").lower().strip()
    try:
        return OpenSearchEndpointStrategy(raw)
    except ValueError:
        return OpenSearchEndpointStrategy.DOMAIN


# ---------------------------------------------------------------------------
# Port allocation for the "port" strategy
# ---------------------------------------------------------------------------

_port_lock = threading.Lock()
_domain_ports: dict[str, int] = {}  # domain_name -> port
_next_port = PORT_RANGE_START


def _allocate_port(domain_name: str) -> int:
    """Allocate (or retrieve) a port for the given domain name."""
    global _next_port
    with _port_lock:
        if domain_name in _domain_ports:
            return _domain_ports[domain_name]
        if _next_port > PORT_RANGE_END:
            raise RuntimeError(
                f"OpenSearch port range exhausted ({PORT_RANGE_START}-{PORT_RANGE_END}). "
                f"Cannot allocate port for domain '{domain_name}'."
            )
        port = _next_port
        _domain_ports[domain_name] = port
        _next_port += 1
        return port


def reset_port_allocations() -> None:
    """Reset all port allocations (for testing)."""
    global _next_port
    with _port_lock:
        _domain_ports.clear()
        _next_port = PORT_RANGE_START


# ---------------------------------------------------------------------------
# Endpoint generation
# ---------------------------------------------------------------------------


def opensearch_endpoint(
    domain_name: str,
    region: str,
    strategy: OpenSearchEndpointStrategy | None = None,
) -> str:
    """Generate an OpenSearch domain endpoint according to the active strategy."""
    if strategy is None:
        strategy = get_opensearch_endpoint_strategy()

    port = int(os.environ.get("GATEWAY_PORT", str(GATEWAY_PORT)))

    if strategy == OpenSearchEndpointStrategy.DOMAIN:
        return f"http://{domain_name}.{region}.opensearch.{LOCALSTACK_HOST}:{port}"
    elif strategy == OpenSearchEndpointStrategy.PATH:
        return f"http://{GATEWAY_HOST}:{port}/opensearch/{region}/{domain_name}"
    elif strategy == OpenSearchEndpointStrategy.PORT:
        allocated = _allocate_port(domain_name)
        return f"http://{GATEWAY_HOST}:{allocated}"
    # Fallback
    return f"http://{GATEWAY_HOST}:{port}"


# ---------------------------------------------------------------------------
# Incoming-request URL parsing helpers
# ---------------------------------------------------------------------------

# Matches: /opensearch/{region}/{domain_name}[/optional-path]
PATH_STYLE_RE = re.compile(
    r"^/opensearch/(?P<region>[a-z0-9-]+)/(?P<domain_name>[A-Za-z0-9-]+)(?P<rest>/.*)?$"
)

# Matches Host: {domain_name}.{region}.opensearch.localhost.localstack.cloud
DOMAIN_HOST_RE = re.compile(
    r"^(?P<domain_name>[A-Za-z0-9-]+)\.(?P<region>[a-z0-9-]+)\.opensearch\."
    + re.escape(LOCALSTACK_HOST)
)


def parse_opensearch_url(path: str, host: str) -> dict | None:
    """Try to parse an incoming request URL as an OpenSearch domain request.

    Returns a dict with keys {region, domain_name} or None.
    """
    # 1. Path-style
    m = PATH_STYLE_RE.match(path)
    if m:
        return {"region": m.group("region"), "domain_name": m.group("domain_name")}

    # 2. Domain host
    m = DOMAIN_HOST_RE.match(host)
    if m:
        return {"region": m.group("region"), "domain_name": m.group("domain_name")}

    return None
