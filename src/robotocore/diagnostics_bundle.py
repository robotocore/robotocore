"""Diagnostic bundle endpoint -- full system diagnostics for debugging.

Returns a comprehensive JSON bundle with system, server, config, services,
state, background engines, memory, audit, and extension information.

Only available when DEBUG=1 or ROBOTOCORE_DIAG=1 for security.

Usage:
    GET /_robotocore/diagnose              — full bundle
    GET /_robotocore/diagnose?section=config,services  — specific sections only
"""

import logging
import os
import platform
import resource
import sys
import threading
import time

from starlette.requests import Request
from starlette.responses import JSONResponse

from robotocore import __version__

logger = logging.getLogger(__name__)

# Known env var prefixes/names to include in config section
_CONFIG_PREFIXES = ("ROBOTOCORE_", "LAMBDA_", "SQS_", "DYNAMODB_", "DNS_", "SMTP_", "SNAPSHOT_")
_CONFIG_EXACT_KEYS = {
    "SERVICES",
    "ENFORCE_IAM",
    "PERSISTENCE",
    "DEBUG",
    "LOG_LEVEL",
    "LOG_FORMAT",
}
# Substrings that indicate a sensitive value to mask
_SENSITIVE_SUBSTRINGS = ("SECRET", "KEY", "PASSWORD", "TOKEN")

ALL_SECTIONS = [
    "system",
    "server",
    "config",
    "services",
    "state",
    "background_engines",
    "memory",
    "audit",
    "extensions",
]


def _is_sensitive(key: str) -> bool:
    """Check if an env var key looks like it holds a secret."""
    upper = key.upper()
    return any(s in upper for s in _SENSITIVE_SUBSTRINGS)


def _collect_system() -> dict:
    """System information: Python, platform, PID, cwd."""
    return {
        "python_version": sys.version,
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "pid": os.getpid(),
        "working_directory": os.getcwd(),
    }


def _collect_server() -> dict:
    """Server information: version, uptime, port, host."""
    from robotocore.gateway.app import _server_start_time

    uptime = time.monotonic() - _server_start_time if _server_start_time else 0
    return {
        "version": __version__,
        "uptime_seconds": round(uptime, 2),
        "port": int(os.environ.get("GATEWAY_LISTEN", os.environ.get("PORT", "4566"))),
        "host": os.environ.get("ROBOTOCORE_HOST", "0.0.0.0"),
        "start_time": _server_start_time,
    }


def _collect_config() -> dict:
    """Configuration: relevant env vars with sensitive values masked."""
    result = {}
    for key, value in sorted(os.environ.items()):
        include = False
        if any(key.startswith(prefix) for prefix in _CONFIG_PREFIXES):
            include = True
        if key in _CONFIG_EXACT_KEYS:
            include = True
        if include:
            result[key] = "***MASKED***" if _is_sensitive(key) else value
    return result


def _collect_services() -> dict:
    """Service counts and lists."""
    from robotocore.services.registry import SERVICE_REGISTRY, ServiceStatus

    native = []
    moto = []
    disabled = []
    for name, info in sorted(SERVICE_REGISTRY.items()):
        if info.status == ServiceStatus.NATIVE:
            native.append(name)
        elif info.status == ServiceStatus.MOTO_BACKED:
            moto.append(name)
        else:
            disabled.append(name)

    return {
        "total_count": len(SERVICE_REGISTRY),
        "native_count": len(native),
        "moto_count": len(moto),
        "disabled_count": len(disabled),
        "native_providers": native,
        "disabled_services": disabled,
    }


def _collect_state() -> dict:
    """State/snapshot information."""
    from robotocore.state.manager import get_state_manager

    manager = get_state_manager()
    snapshots = manager.list_snapshots()
    state_dir = str(manager.state_dir) if manager.state_dir else None

    # Collect state file sizes if state_dir exists
    file_sizes = {}
    if manager.state_dir and manager.state_dir.exists():
        for f in manager.state_dir.iterdir():
            if f.is_file():
                file_sizes[f.name] = f.stat().st_size

    return {
        "snapshot_count": len(snapshots),
        "persistence_enabled": os.environ.get("PERSISTENCE", "0") == "1",
        "state_directory": state_dir,
        "state_file_sizes": file_sizes,
    }


def _collect_background_engines() -> dict:
    """List background threads with alive/dead status."""
    # Known engine thread name patterns
    known_patterns = [
        "lambda",
        "esm",
        "alarm",
        "cloudwatch",
        "ttl",
        "scanner",
        "smtp",
        "sqs",
        "metrics",
        "canary",
        "synthetics",
        "dns",
    ]

    engines = []
    for t in threading.enumerate():
        name = t.name.lower()
        # Include daemon threads and threads matching known patterns
        is_known = any(p in name for p in known_patterns)
        if is_known or t.daemon:
            engines.append(
                {
                    "name": t.name,
                    "alive": t.is_alive(),
                    "daemon": t.daemon,
                }
            )

    return engines


def _collect_memory() -> dict:
    """Memory usage via resource module."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # maxrss is in bytes on Linux, kilobytes on macOS
    rss_bytes = usage.ru_maxrss
    if sys.platform == "darwin":
        # macOS reports in bytes already
        pass
    else:
        # Linux reports in KB
        rss_bytes = rss_bytes * 1024

    # Try to get more accurate current RSS from /proc if available
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_bytes = int(line.split()[1]) * 1024
                    break
    except (FileNotFoundError, PermissionError):
        logger.debug("Could not read /proc/self/status for RSS; falling back to resource.getrusage")

    # VMS from /proc or fallback
    vms_bytes = 0
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmSize:"):
                    vms_bytes = int(line.split()[1]) * 1024
                    break
    except (FileNotFoundError, PermissionError):
        # Fallback: use maxrss as approximation
        vms_bytes = rss_bytes

    return {
        "rss_bytes": rss_bytes,
        "vms_bytes": vms_bytes,
        "max_rss_bytes": usage.ru_maxrss if sys.platform != "darwin" else usage.ru_maxrss,
    }


def _collect_audit() -> dict:
    """Audit log summary: total requests, error count, last 5 errors."""
    from robotocore.audit.log import get_audit_log

    log = get_audit_log()
    all_entries = log.recent(10000)  # Get all recent entries
    total = len(all_entries)
    errors = [e for e in all_entries if e.get("status_code", 200) >= 400]
    return {
        "total_requests": total,
        "error_count": len(errors),
        "last_errors": errors[:5],
    }


def _collect_extensions() -> dict:
    """Loaded plugins with names and status."""
    from robotocore.extensions.registry import get_extension_registry

    registry = get_extension_registry()
    return {
        "plugins": registry.list_plugins(),
    }


# Section name -> collector function
_COLLECTORS = {
    "system": _collect_system,
    "server": _collect_server,
    "config": _collect_config,
    "services": _collect_services,
    "state": _collect_state,
    "background_engines": _collect_background_engines,
    "memory": _collect_memory,
    "audit": _collect_audit,
    "extensions": _collect_extensions,
}


def collect_diagnostics(sections: list[str] | None = None) -> dict:
    """Collect diagnostic information for the requested sections.

    Args:
        sections: List of section names to include. None means all sections.

    Returns:
        Dict with section names as keys and section data as values.
    """
    if sections is None:
        sections = ALL_SECTIONS

    result = {}
    for section in sections:
        collector = _COLLECTORS.get(section)
        if collector:
            result[section] = collector()
    return result


async def diagnose_endpoint(request: Request) -> JSONResponse:
    """GET /_robotocore/diagnose -- returns diagnostic bundle.

    Only available when DEBUG=1 or ROBOTOCORE_DIAG=1.
    Accepts ?section=config,services to return specific sections.
    """
    # Security gate
    debug = os.environ.get("DEBUG", "0")
    diag = os.environ.get("ROBOTOCORE_DIAG", "")
    if debug != "1" and not diag:
        return JSONResponse(
            {"error": "Diagnostic endpoint requires DEBUG=1 or ROBOTOCORE_DIAG=1"},
            status_code=403,
        )

    # Parse section filter
    section_param = request.query_params.get("section")
    sections = None
    if section_param:
        sections = [s.strip() for s in section_param.split(",") if s.strip()]

    bundle = collect_diagnostics(sections=sections)
    return JSONResponse(bundle)
