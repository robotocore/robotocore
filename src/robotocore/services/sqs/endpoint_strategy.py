"""SQS endpoint URL strategies matching LocalStack's SQS_ENDPOINT_STRATEGY.

Supports 4 strategies controlled by the SQS_ENDPOINT_STRATEGY env var:
- standard (default): http://sqs.{region}.localhost.localstack.cloud:4566/{account_id}/{queue_name}
- domain: http://{region}.queue.localhost.localstack.cloud:4566/{account_id}/{queue_name}
- path: http://localhost:4566/queue/{region}/{account_id}/{queue_name}
- dynamic: Returns path-style URLs but accepts all formats for incoming requests
"""

import os
import re
from enum import StrEnum

# Default gateway port
GATEWAY_PORT = 4566
GATEWAY_HOST = "localhost"
LOCALSTACK_HOST = "localhost.localstack.cloud"


class SqsEndpointStrategy(StrEnum):
    STANDARD = "standard"
    DOMAIN = "domain"
    PATH = "path"
    DYNAMIC = "dynamic"


def get_sqs_endpoint_strategy() -> SqsEndpointStrategy:
    """Read the current SQS endpoint strategy from the environment."""
    raw = os.environ.get("SQS_ENDPOINT_STRATEGY", "standard").lower().strip()
    try:
        return SqsEndpointStrategy(raw)
    except ValueError:
        return SqsEndpointStrategy.STANDARD


def sqs_queue_url(
    queue_name: str,
    region: str,
    account_id: str,
    strategy: SqsEndpointStrategy | None = None,
) -> str:
    """Generate a queue URL according to the active strategy."""
    if strategy is None:
        strategy = get_sqs_endpoint_strategy()

    port = int(os.environ.get("GATEWAY_PORT", str(GATEWAY_PORT)))

    if strategy == SqsEndpointStrategy.STANDARD:
        return f"http://sqs.{region}.{LOCALSTACK_HOST}:{port}/{account_id}/{queue_name}"
    elif strategy == SqsEndpointStrategy.DOMAIN:
        return f"http://{region}.queue.{LOCALSTACK_HOST}:{port}/{account_id}/{queue_name}"
    elif strategy in (SqsEndpointStrategy.PATH, SqsEndpointStrategy.DYNAMIC):
        return f"http://{GATEWAY_HOST}:{port}/queue/{region}/{account_id}/{queue_name}"
    # Fallback (should not be reached)
    return f"http://{GATEWAY_HOST}:{port}/{account_id}/{queue_name}"


# ---------------------------------------------------------------------------
# Incoming-request URL parsing helpers
# ---------------------------------------------------------------------------

# Matches: /queue/{region}/{account_id}/{queue_name}
PATH_STYLE_RE = re.compile(
    r"^/queue/(?P<region>[a-z0-9-]+)/(?P<account_id>\d+)/(?P<queue_name>[A-Za-z0-9_.-]+)$"
)

# Matches Host: sqs.{region}.localhost.localstack.cloud
STANDARD_HOST_RE = re.compile(r"^sqs\.(?P<region>[a-z0-9-]+)\." + re.escape(LOCALSTACK_HOST))

# Matches Host: {region}.queue.localhost.localstack.cloud
DOMAIN_HOST_RE = re.compile(r"^(?P<region>[a-z0-9-]+)\.queue\." + re.escape(LOCALSTACK_HOST))


def parse_sqs_url(path: str, host: str) -> dict | None:
    """Try to parse an incoming request URL as an SQS request.

    Returns a dict with keys {region, account_id, queue_name} or None.
    """
    # 1. Path-style: /queue/{region}/{account_id}/{queue_name}
    m = PATH_STYLE_RE.match(path)
    if m:
        return m.groupdict()

    # 2. Standard host: sqs.{region}.localhost.localstack.cloud + /{account_id}/{queue_name}
    m = STANDARD_HOST_RE.match(host)
    if m:
        parts = path.strip("/").split("/")
        if len(parts) >= 2:
            return {
                "region": m.group("region"),
                "account_id": parts[0],
                "queue_name": parts[1],
            }

    # 3. Domain host: {region}.queue.localhost.localstack.cloud + /{account_id}/{queue_name}
    m = DOMAIN_HOST_RE.match(host)
    if m:
        parts = path.strip("/").split("/")
        if len(parts) >= 2:
            return {
                "region": m.group("region"),
                "account_id": parts[0],
                "queue_name": parts[1],
            }

    return None
