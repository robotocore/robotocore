"""DNS resolution logic for routing AWS hostnames to the local container.

Pure logic module -- no I/O or network calls. Decides whether a given hostname
should be resolved locally (to the configured IP) or forwarded upstream.
"""

import ipaddress
import os
import re

# Default patterns that match AWS service hostnames
_DEFAULT_LOCAL_PATTERNS: list[str] = [
    r".*\.amazonaws\.com$",
    r".*\.aws\.amazon\.com$",
    r"^amazonaws\.com$",
    r"^aws\.amazon\.com$",
]

# Default TTL for local DNS responses (seconds)
DEFAULT_TTL = 300


def get_config() -> dict:
    """Read DNS configuration from environment variables and return as a dict."""
    resolve_ip = os.environ.get("DNS_RESOLVE_IP", "127.0.0.1")
    port = int(os.environ.get("DNS_PORT", "53"))
    address = os.environ.get("DNS_ADDRESS", "0.0.0.0")
    disabled = os.environ.get("DNS_DISABLED", "0") == "1"
    upstream = os.environ.get("DNS_SERVER", "")

    # Parse upstream bypass patterns
    upstream_patterns_raw = os.environ.get("DNS_NAME_PATTERNS_TO_RESOLVE_UPSTREAM", "")
    upstream_patterns = [p.strip() for p in upstream_patterns_raw.split(",") if p.strip()]

    # Parse additional local patterns
    local_patterns_raw = os.environ.get("DNS_LOCAL_NAME_PATTERNS", "")
    extra_local_patterns = [p.strip() for p in local_patterns_raw.split(",") if p.strip()]

    return {
        "resolve_ip": resolve_ip,
        "port": port,
        "address": address,
        "disabled": disabled,
        "upstream_server": upstream,
        "upstream_patterns": upstream_patterns,
        "local_patterns": _DEFAULT_LOCAL_PATTERNS + extra_local_patterns,
        "ttl": DEFAULT_TTL,
    }


def should_resolve_locally(hostname: str, config: dict | None = None) -> bool:
    """Return True if the hostname should be resolved to the local IP.

    Resolution order:
    1. If hostname matches an upstream bypass pattern -> forward upstream
    2. If hostname matches a local pattern -> resolve locally
    3. Otherwise -> forward upstream
    """
    if config is None:
        config = get_config()

    name = hostname.rstrip(".").lower()

    # Check upstream bypass patterns first (higher priority)
    for pattern in config.get("upstream_patterns", []):
        if re.search(pattern, name):
            return False

    # Check local patterns
    for pattern in config.get("local_patterns", _DEFAULT_LOCAL_PATTERNS):
        if re.search(pattern, name):
            return True

    return False


def resolve_a_record(hostname: str, config: dict | None = None) -> str | None:
    """Resolve an A record query. Returns the IP string or None to forward upstream."""
    if config is None:
        config = get_config()

    if should_resolve_locally(hostname, config):
        return config.get("resolve_ip", "127.0.0.1")
    return None


def resolve_aaaa_record(hostname: str, config: dict | None = None) -> str | None:
    """Resolve an AAAA record query. Returns an IPv6 string or None.

    If the configured resolve_ip is IPv6, return it directly.
    If it's IPv4, return the IPv4-mapped IPv6 address (::ffff:x.x.x.x).
    If the hostname should not be resolved locally, return None.
    """
    if config is None:
        config = get_config()

    if not should_resolve_locally(hostname, config):
        return None

    ip_str = config.get("resolve_ip", "127.0.0.1")
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return None

    if isinstance(addr, ipaddress.IPv6Address):
        return str(addr)

    # Map IPv4 to IPv6 -- convert to pure hex notation for dnslib compatibility
    mapped = ipaddress.IPv6Address(f"::ffff:{ip_str}")
    # packed is 16 bytes; format as 8 groups of 4 hex digits
    packed = mapped.packed
    groups = [f"{packed[i] << 8 | packed[i + 1]:04x}" for i in range(0, 16, 2)]
    return ":".join(groups)


def resolve_cname_record(hostname: str, config: dict | None = None) -> str | None:
    """Resolve a CNAME record query. Returns the target hostname or None.

    For AWS hostnames resolved locally, we return the original hostname
    (since we handle A/AAAA directly). Returns None for upstream forwarding.
    """
    if config is None:
        config = get_config()

    if should_resolve_locally(hostname, config):
        # No CNAME indirection needed -- A/AAAA records handle it directly
        return hostname.rstrip(".") + "."
    return None
