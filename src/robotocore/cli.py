"""Robotocore CLI — manage the robotocore Docker container."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

DEFAULT_CONTAINER_NAME = "robotocore-main"
DEFAULT_IMAGE = "robotocore/robotocore:latest"
DEFAULT_PORT = 4566
DEFAULT_WAIT_TIMEOUT = 30

# Env vars that get passed through to the container automatically
PASSTHROUGH_ENV_VARS = [
    "ENFORCE_IAM",
    "ROBOTOCORE_LOG_LEVEL",
    "AUDIT_LOG_SIZE",
    "SERVICES",
]


def _get_container_name(args: argparse.Namespace) -> str:
    return getattr(args, "name", DEFAULT_CONTAINER_NAME) or DEFAULT_CONTAINER_NAME


def _get_image(args: argparse.Namespace) -> str:
    return getattr(args, "image", None) or os.environ.get("ROBOTOCORE_IMAGE") or DEFAULT_IMAGE


def _get_port(args: argparse.Namespace) -> int:
    port = getattr(args, "port", None)
    if port is not None:
        return int(port)
    env_port = os.environ.get("ROBOTOCORE_PORT")
    if env_port:
        return int(env_port)
    return DEFAULT_PORT


def _get_base_url(args: argparse.Namespace) -> str:
    port = _get_port(args)
    return f"http://localhost:{port}"


def _run_docker(cmd_args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run a docker command, returning the CompletedProcess."""
    full_cmd = ["docker"] + cmd_args
    return subprocess.run(full_cmd, capture_output=True, text=True, check=check)


def _api_request(
    url: str,
    *,
    method: str = "GET",
    data: dict | None = None,
) -> dict | list:
    """Make an HTTP request to the robotocore management API."""
    body = None
    if data is not None:
        body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def _print_json(data: dict | list) -> None:
    """Pretty-print a JSON response."""
    print(json.dumps(data, indent=2))


def _print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a simple column-aligned table."""
    if not rows:
        print("(no data)")
        return
    # Compute column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))
    # Print header
    header_line = "  ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("  ".join("-" * w for w in col_widths))
    # Print rows
    for row in rows:
        line = "  ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row))
        print(line)


def _get_format(args: argparse.Namespace) -> str:
    """Get the output format from args, defaulting to 'table'."""
    return getattr(args, "format", "table") or "table"


def _api_cmd(
    args: argparse.Namespace,
    path: str,
    *,
    method: str = "GET",
    data: dict | None = None,
    label: str = "Request",
) -> dict | list | None:
    """Execute an API request with standard error handling. Returns response or None on error."""
    base_url = _get_base_url(args)
    try:
        return _api_request(f"{base_url}{path}", method=method, data=data)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"{label} failed: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Container lifecycle commands
# ---------------------------------------------------------------------------


def cmd_start(args: argparse.Namespace) -> int:
    """Start the robotocore container in the background."""
    name = _get_container_name(args)
    image = _get_image(args)
    port = _get_port(args)

    # Check if container already running
    result = _run_docker(
        ["ps", "-q", "-f", f"name=^/{name}$"],
        check=False,
    )
    if result.stdout.strip():
        print(f"Container '{name}' is already running.", file=sys.stderr)
        return 1

    # Remove stopped container with same name if it exists
    _run_docker(["rm", "-f", name], check=False)

    # Build docker run command
    docker_args = [
        "run",
        "-d",
        "--name",
        name,
        "-p",
        f"{port}:4566",
    ]

    # Pass through known env vars
    for var in PASSTHROUGH_ENV_VARS:
        val = os.environ.get(var)
        if val is not None:
            docker_args.extend(["-e", f"{var}={val}"])

    # Pass through arbitrary --env flags
    for env_pair in getattr(args, "env", None) or []:
        docker_args.extend(["-e", env_pair])

    docker_args.append(image)

    result = _run_docker(docker_args, check=False)
    if result.returncode != 0:
        print(f"Failed to start container: {result.stderr.strip()}", file=sys.stderr)
        return 1

    container_id = result.stdout.strip()[:12]
    print(f"Robotocore container '{name}' started (id: {container_id})")
    print(f"Endpoint: http://localhost:{port}")

    # --wait: block until healthy
    if getattr(args, "wait", False):
        timeout = getattr(args, "timeout", DEFAULT_WAIT_TIMEOUT) or DEFAULT_WAIT_TIMEOUT
        # Reuse cmd_wait logic
        wait_args = argparse.Namespace(port=port, timeout=timeout, name=name)
        return cmd_wait(wait_args)

    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    """Stop the robotocore container."""
    name = _get_container_name(args)
    result = _run_docker(["stop", name], check=False)
    if result.returncode != 0:
        print(f"Failed to stop container '{name}': {result.stderr.strip()}", file=sys.stderr)
        return 1
    _run_docker(["rm", name], check=False)
    print(f"Container '{name}' stopped.")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show container status."""
    name = _get_container_name(args)
    result = _run_docker(
        [
            "ps",
            "-a",
            "--filter",
            f"name=^/{name}$",
            "--format",
            "{{.ID}}\t{{.Status}}\t{{.Ports}}",
        ],
        check=False,
    )
    output = result.stdout.strip()
    if not output:
        print(f"Container '{name}' is not running.")
        return 1

    parts = output.split("\t")
    container_id = parts[0] if len(parts) > 0 else "?"
    status = parts[1] if len(parts) > 1 else "?"
    ports = parts[2] if len(parts) > 2 else "?"

    print(f"Container:  {name}")
    print(f"ID:         {container_id}")
    print(f"Status:     {status}")
    print(f"Ports:      {ports}")

    # If running, also try to hit the health endpoint
    if "Up" in status:
        base_url = _get_base_url(args)
        try:
            data = _api_request(f"{base_url}/_robotocore/health")
            svc_count = data.get("services", "?")
            version = data.get("version", "?")
            print(f"Version:    {version}")
            print(f"Services:   {svc_count}")
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
            pass  # Container running but not yet healthy

    return 0


def cmd_logs(args: argparse.Namespace) -> int:
    """Tail container logs."""
    name = _get_container_name(args)
    docker_args = ["logs"]
    tail = getattr(args, "tail", None)
    if tail is not None:
        docker_args.extend(["--tail", str(tail)])
    follow = getattr(args, "follow", True)
    if follow:
        docker_args.append("-f")
    docker_args.append(name)
    try:
        subprocess.run(["docker"] + docker_args, check=True)
    except subprocess.CalledProcessError:
        print(f"Failed to get logs for container '{name}'.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        pass  # User pressed Ctrl+C to stop following logs
    return 0


def cmd_restart(args: argparse.Namespace) -> int:
    """Restart the container (stop + start)."""
    rc = cmd_stop(args)
    # Ignore stop failure (container might not be running)
    if rc != 0:
        # Still try to start even if stop failed
        pass
    return cmd_start(args)


def cmd_health(args: argparse.Namespace) -> int:
    """Check the health endpoint."""
    base_url = _get_base_url(args)
    try:
        data = _api_request(f"{base_url}/_robotocore/health")
        _print_json(data)
        return 0
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"Health check failed: {e}", file=sys.stderr)
        return 1


def cmd_wait(args: argparse.Namespace) -> int:
    """Wait for the container to become healthy."""
    base_url = _get_base_url(args)
    timeout = getattr(args, "timeout", DEFAULT_WAIT_TIMEOUT) or DEFAULT_WAIT_TIMEOUT
    deadline = time.monotonic() + timeout
    url = f"{base_url}/_robotocore/health"

    while time.monotonic() < deadline:
        try:
            data = _api_request(url)
            if data.get("status") == "ok":
                print("Robotocore is ready.")
                return 0
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
            pass  # Server not ready yet, retry after sleep
        time.sleep(1)

    print(f"Timed out after {timeout}s waiting for robotocore to become healthy.", file=sys.stderr)
    return 1


def cmd_version(args: argparse.Namespace) -> int:
    """Show robotocore version from the health endpoint."""
    base_url = _get_base_url(args)
    try:
        data = _api_request(f"{base_url}/_robotocore/health")
        version = data.get("version", "unknown")
        print(f"robotocore {version}")
        return 0
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"Could not reach server: {e}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# State commands
# ---------------------------------------------------------------------------


def cmd_state_save(args: argparse.Namespace) -> int:
    """Save a state snapshot."""
    resp = _api_cmd(
        args,
        "/_robotocore/state/save",
        method="POST",
        data={"name": args.snapshot_name},
        label="State save",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def cmd_state_load(args: argparse.Namespace) -> int:
    """Load a state snapshot."""
    resp = _api_cmd(
        args,
        "/_robotocore/state/load",
        method="POST",
        data={"name": args.snapshot_name},
        label="State load",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def cmd_state_list(args: argparse.Namespace) -> int:
    """List state snapshots."""
    resp = _api_cmd(args, "/_robotocore/state/snapshots", label="State list")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def cmd_state_reset(args: argparse.Namespace) -> int:
    """Reset all state."""
    resp = _api_cmd(args, "/_robotocore/state/reset", method="POST", label="State reset")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


# ---------------------------------------------------------------------------
# Services command
# ---------------------------------------------------------------------------


def cmd_services(args: argparse.Namespace) -> int:
    """List registered services."""
    resp = _api_cmd(args, "/_robotocore/services", label="Services list")
    if resp is None:
        return 1
    fmt = _get_format(args)
    status_filter = getattr(args, "status", "all") or "all"

    # Normalize response — could be list or dict with "services" key
    services = resp if isinstance(resp, list) else resp.get("services", [])

    # Apply status filter
    if status_filter != "all":
        filter_map = {"native": "NATIVE", "moto": "MOTO_BACKED"}
        filter_val = filter_map.get(status_filter, status_filter.upper())
        services = [s for s in services if s.get("status", "").upper() == filter_val]

    if fmt == "json":
        _print_json(services)
    else:
        headers = ["SERVICE", "STATUS", "PROTOCOL", "ENABLED"]
        rows = []
        for svc in services:
            rows.append(
                [
                    svc.get("name", "?"),
                    svc.get("status", "?"),
                    svc.get("protocol", "?"),
                    str(svc.get("enabled", "?")),
                ]
            )
        rows.sort(key=lambda r: r[0])
        _print_table(headers, rows)
    return 0


# ---------------------------------------------------------------------------
# Config commands
# ---------------------------------------------------------------------------


def cmd_config(args: argparse.Namespace) -> int:
    """Runtime configuration management."""
    sub = getattr(args, "config_command", None)
    if sub == "get":
        return _cmd_config_get(args)
    elif sub == "set":
        return _cmd_config_set(args)
    elif sub == "reset":
        return _cmd_config_reset(args)
    else:
        print("Usage: robotocore config {get,set,reset}", file=sys.stderr)
        return 1


def _cmd_config_get(args: argparse.Namespace) -> int:
    resp = _api_cmd(args, "/_robotocore/config", label="Config get")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def _cmd_config_set(args: argparse.Namespace) -> int:
    key = args.key
    value = args.value
    # Try to parse value as JSON (for booleans, numbers)
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, ValueError):
        parsed = value
    resp = _api_cmd(
        args,
        "/_robotocore/config",
        method="POST",
        data={key: parsed},
        label="Config set",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def _cmd_config_reset(args: argparse.Namespace) -> int:
    key = args.key
    resp = _api_cmd(args, f"/_robotocore/config/{key}", method="DELETE", label="Config reset")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


# ---------------------------------------------------------------------------
# Chaos commands
# ---------------------------------------------------------------------------


def cmd_chaos(args: argparse.Namespace) -> int:
    """Chaos engineering rules management."""
    sub = getattr(args, "chaos_command", None)
    if sub == "list":
        return _cmd_chaos_list(args)
    elif sub == "add":
        return _cmd_chaos_add(args)
    elif sub == "remove":
        return _cmd_chaos_remove(args)
    elif sub == "clear":
        return _cmd_chaos_clear(args)
    else:
        print("Usage: robotocore chaos {list,add,remove,clear}", file=sys.stderr)
        return 1


def _cmd_chaos_list(args: argparse.Namespace) -> int:
    resp = _api_cmd(args, "/_robotocore/chaos/rules", label="Chaos list")
    if resp is None:
        return 1
    fmt = _get_format(args)
    rules = resp if isinstance(resp, list) else resp.get("rules", [])
    if fmt == "json":
        _print_json(rules)
    else:
        if not rules:
            print("No chaos rules configured.")
            return 0
        headers = ["ID", "SERVICE", "ERROR", "STATUS", "OPERATION", "PROB", "LATENCY"]
        rows = []
        for r in rules:
            rows.append(
                [
                    str(r.get("rule_id", "?")),
                    r.get("service", "*"),
                    r.get("error_code", "?"),
                    str(r.get("status_code", "?")),
                    r.get("operation", "*"),
                    str(r.get("probability", 1.0)),
                    str(r.get("latency_ms", 0)),
                ]
            )
        _print_table(headers, rows)
    return 0


def _cmd_chaos_add(args: argparse.Namespace) -> int:
    rule: dict = {
        "service": args.service,
        "error_code": args.error,
        "status_code": args.status_code,
    }
    if getattr(args, "operation", None):
        rule["operation"] = args.operation
    if getattr(args, "latency", None) is not None:
        rule["latency_ms"] = args.latency
    if getattr(args, "rate", None) is not None:
        rule["probability"] = args.rate
    resp = _api_cmd(
        args,
        "/_robotocore/chaos/rules",
        method="POST",
        data=rule,
        label="Chaos add",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def _cmd_chaos_remove(args: argparse.Namespace) -> int:
    rule_id = args.rule_id
    resp = _api_cmd(
        args,
        f"/_robotocore/chaos/rules/{rule_id}",
        method="DELETE",
        label="Chaos remove",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def _cmd_chaos_clear(args: argparse.Namespace) -> int:
    resp = _api_cmd(
        args,
        "/_robotocore/chaos/rules/clear",
        method="POST",
        label="Chaos clear",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


# ---------------------------------------------------------------------------
# Resources command
# ---------------------------------------------------------------------------


def cmd_resources(args: argparse.Namespace) -> int:
    """Resource browser."""
    service = getattr(args, "service", None)
    if service:
        path = f"/_robotocore/resources/{service}"
    else:
        path = "/_robotocore/resources"
    resp = _api_cmd(args, path, label="Resources")
    if resp is None:
        return 1
    fmt = _get_format(args)
    if fmt == "json" or service:
        _print_json(resp)
    else:
        # Summary mode: show counts per service
        resources = resp.get("resources", resp) if isinstance(resp, dict) else {}
        headers = ["SERVICE", "COUNT"]
        rows = []
        for svc_name, svc_data in sorted(resources.items()):
            if isinstance(svc_data, int):
                count = svc_data
            elif isinstance(svc_data, list):
                count = len(svc_data)
            else:
                count = "?"
            rows.append([svc_name, str(count)])
        _print_table(headers, rows)
    return 0


# ---------------------------------------------------------------------------
# Audit command
# ---------------------------------------------------------------------------


def cmd_audit(args: argparse.Namespace) -> int:
    """API call history."""
    limit = getattr(args, "limit", 20) or 20
    resp = _api_cmd(args, f"/_robotocore/audit?limit={limit}", label="Audit")
    if resp is None:
        return 1
    fmt = _get_format(args)
    entries = resp if isinstance(resp, list) else resp.get("entries", [])
    if fmt == "json":
        _print_json(entries)
    else:
        headers = ["TIME", "SERVICE", "OPERATION", "STATUS", "DURATION"]
        rows = []
        for e in entries:
            rows.append(
                [
                    e.get("timestamp", "?"),
                    e.get("service", "?"),
                    e.get("operation", "?"),
                    str(e.get("status_code", "?")),
                    str(e.get("duration_ms", "?")) + "ms",
                ]
            )
        _print_table(headers, rows)
    return 0


# ---------------------------------------------------------------------------
# Usage commands
# ---------------------------------------------------------------------------


def cmd_usage(args: argparse.Namespace) -> int:
    """Usage analytics."""
    sub = getattr(args, "usage_command", None)
    if sub == "services":
        return _cmd_usage_services(args)
    elif sub == "errors":
        return _cmd_usage_errors(args)
    else:
        # Default: summary
        return _cmd_usage_summary(args)


def _cmd_usage_summary(args: argparse.Namespace) -> int:
    resp = _api_cmd(args, "/_robotocore/usage", label="Usage")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def _cmd_usage_services(args: argparse.Namespace) -> int:
    resp = _api_cmd(args, "/_robotocore/usage/services", label="Usage services")
    if resp is None:
        return 1
    fmt = _get_format(args)
    services = resp if isinstance(resp, list) else resp.get("services", [])
    if fmt == "json":
        _print_json(services)
    else:
        headers = ["SERVICE", "REQUESTS", "ERRORS"]
        rows = []
        for s in services:
            rows.append(
                [
                    s.get("service", "?"),
                    str(s.get("request_count", 0)),
                    str(s.get("error_count", 0)),
                ]
            )
        rows.sort(key=lambda r: int(r[1]) if r[1].isdigit() else 0, reverse=True)
        _print_table(headers, rows)
    return 0


def _cmd_usage_errors(args: argparse.Namespace) -> int:
    resp = _api_cmd(args, "/_robotocore/usage/errors", label="Usage errors")
    if resp is None:
        return 1
    fmt = _get_format(args)
    errors = resp if isinstance(resp, list) else resp.get("errors", [])
    if fmt == "json":
        _print_json(errors)
    else:
        headers = ["SERVICE", "OPERATION", "ERROR", "COUNT"]
        rows = []
        for e in errors:
            rows.append(
                [
                    e.get("service", "?"),
                    e.get("operation", "?"),
                    e.get("error", "?"),
                    str(e.get("count", 0)),
                ]
            )
        _print_table(headers, rows)
    return 0


# ---------------------------------------------------------------------------
# Pods commands
# ---------------------------------------------------------------------------


def cmd_pods(args: argparse.Namespace) -> int:
    """Cloud Pods management."""
    sub = getattr(args, "pods_command", None)
    if sub == "list":
        return _cmd_pods_list(args)
    elif sub == "save":
        return _cmd_pods_save(args)
    elif sub == "load":
        return _cmd_pods_load(args)
    elif sub == "info":
        return _cmd_pods_info(args)
    elif sub == "delete":
        return _cmd_pods_delete(args)
    else:
        print("Usage: robotocore pods {list,save,load,info,delete}", file=sys.stderr)
        return 1


def _cmd_pods_list(args: argparse.Namespace) -> int:
    resp = _api_cmd(args, "/_robotocore/pods", label="Pods list")
    if resp is None:
        return 1
    fmt = _get_format(args)
    pods = resp if isinstance(resp, list) else resp.get("pods", [])
    if fmt == "json":
        _print_json(pods)
    else:
        if not pods:
            print("No pods found.")
            return 0
        headers = ["NAME", "CREATED", "SIZE", "VERSIONS"]
        rows = []
        for p in pods:
            rows.append(
                [
                    p.get("name", "?"),
                    p.get("created_at", "?"),
                    str(p.get("size_bytes", "?")),
                    str(p.get("version_count", "?")),
                ]
            )
        _print_table(headers, rows)
    return 0


def _cmd_pods_save(args: argparse.Namespace) -> int:
    resp = _api_cmd(
        args,
        "/_robotocore/pods/save",
        method="POST",
        data={"name": args.pod_name},
        label="Pod save",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def _cmd_pods_load(args: argparse.Namespace) -> int:
    resp = _api_cmd(
        args,
        "/_robotocore/pods/load",
        method="POST",
        data={"name": args.pod_name},
        label="Pod load",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def _cmd_pods_info(args: argparse.Namespace) -> int:
    name = args.pod_name
    resp = _api_cmd(args, f"/_robotocore/pods/{name}", label="Pod info")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


def _cmd_pods_delete(args: argparse.Namespace) -> int:
    name = args.pod_name
    resp = _api_cmd(args, f"/_robotocore/pods/{name}", method="DELETE", label="Pod delete")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


# ---------------------------------------------------------------------------
# SES commands
# ---------------------------------------------------------------------------


def cmd_ses(args: argparse.Namespace) -> int:
    """Email inspection."""
    sub = getattr(args, "ses_command", None)
    if sub == "messages":
        return _cmd_ses_messages(args)
    elif sub == "clear":
        return _cmd_ses_clear(args)
    else:
        print("Usage: robotocore ses {messages,clear}", file=sys.stderr)
        return 1


def _cmd_ses_messages(args: argparse.Namespace) -> int:
    limit = getattr(args, "limit", None)
    path = "/_robotocore/ses/messages"
    if limit is not None:
        path += f"?limit={limit}"
    resp = _api_cmd(args, path, label="SES messages")
    if resp is None:
        return 1
    fmt = _get_format(args)
    messages = resp if isinstance(resp, list) else resp.get("messages", [])
    if fmt == "json":
        _print_json(messages)
    else:
        if not messages:
            print("No email messages.")
            return 0
        headers = ["TIME", "FROM", "TO", "SUBJECT"]
        rows = []
        for m in messages:
            to_addrs = m.get("to", [])
            if isinstance(to_addrs, list):
                to_str = ", ".join(to_addrs[:3])
                if len(to_addrs) > 3:
                    to_str += f" (+{len(to_addrs) - 3})"
            else:
                to_str = str(to_addrs)
            rows.append(
                [
                    m.get("timestamp", "?"),
                    m.get("from", "?"),
                    to_str,
                    m.get("subject", "(no subject)"),
                ]
            )
        _print_table(headers, rows)
    return 0


def _cmd_ses_clear(args: argparse.Namespace) -> int:
    resp = _api_cmd(
        args,
        "/_robotocore/ses/messages",
        method="DELETE",
        label="SES clear",
    )
    if resp is None:
        return 1
    _print_json(resp)
    return 0


# ---------------------------------------------------------------------------
# IAM commands
# ---------------------------------------------------------------------------


def cmd_iam(args: argparse.Namespace) -> int:
    """IAM policy stream."""
    sub = getattr(args, "iam_command", None)
    if sub == "stream":
        return _cmd_iam_stream(args)
    elif sub == "suggest":
        return _cmd_iam_suggest(args)
    else:
        print("Usage: robotocore iam {stream,suggest}", file=sys.stderr)
        return 1


def _cmd_iam_stream(args: argparse.Namespace) -> int:
    params: list[str] = []
    limit = getattr(args, "limit", None)
    decision = getattr(args, "decision", None)
    if limit is not None:
        params.append(f"limit={limit}")
    if decision is not None:
        params.append(f"decision={decision}")
    path = "/_robotocore/iam/policy-stream"
    if params:
        path += "?" + "&".join(params)
    resp = _api_cmd(args, path, label="IAM stream")
    if resp is None:
        return 1
    fmt = _get_format(args)
    entries = resp if isinstance(resp, list) else resp.get("entries", [])
    if fmt == "json":
        _print_json(entries)
    else:
        headers = ["TIME", "PRINCIPAL", "ACTION", "RESOURCE", "DECISION"]
        rows = []
        for e in entries:
            rows.append(
                [
                    e.get("timestamp", "?"),
                    e.get("principal", "?"),
                    e.get("action", "?"),
                    e.get("resource", "?"),
                    e.get("decision", "?"),
                ]
            )
        _print_table(headers, rows)
    return 0


def _cmd_iam_suggest(args: argparse.Namespace) -> int:
    principal = args.principal
    params = [f"principal={principal}"]
    limit = getattr(args, "limit", None)
    if limit is not None:
        params.append(f"limit={limit}")
    path = "/_robotocore/iam/policy-stream/suggest-policy?" + "&".join(params)
    resp = _api_cmd(args, path, label="IAM suggest")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


# ---------------------------------------------------------------------------
# Diagnose command
# ---------------------------------------------------------------------------


def cmd_diagnose(args: argparse.Namespace) -> int:
    """Collect diagnostic bundle."""
    resp = _api_cmd(args, "/_robotocore/diagnose", label="Diagnose")
    if resp is None:
        return 1
    _print_json(resp)
    return 0


# ---------------------------------------------------------------------------
# Parser construction
# ---------------------------------------------------------------------------


def build_url(base_url: str, path: str) -> str:
    """Build a full URL from base and path. Exposed for testing."""
    return f"{base_url}{path}"


def _add_format_flag(parser: argparse.ArgumentParser, default: str = "table") -> None:
    """Add --format flag to a parser."""
    parser.add_argument(
        "--format",
        choices=["table", "json"],
        default=default,
        help=f"Output format (default: {default})",
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser. Exposed for testing."""
    parser = argparse.ArgumentParser(
        prog="robotocore",
        description="Manage the robotocore AWS emulator container.",
    )
    parser.add_argument(
        "--name",
        default=DEFAULT_CONTAINER_NAME,
        help=f"Container name (default: {DEFAULT_CONTAINER_NAME})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"Gateway port (default: {DEFAULT_PORT}, or ROBOTOCORE_PORT env var)",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # start
    start_parser = subparsers.add_parser("start", help="Start robotocore container")
    start_parser.add_argument(
        "--image",
        default=None,
        help=f"Docker image (default: {DEFAULT_IMAGE}, or ROBOTOCORE_IMAGE env var)",
    )
    start_parser.add_argument(
        "--env",
        "-e",
        action="append",
        help="Pass environment variable to container (KEY=VAL)",
    )
    start_parser.add_argument(
        "--wait",
        action="store_true",
        default=False,
        help="Wait for container to become healthy after starting",
    )
    start_parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_WAIT_TIMEOUT,
        help=f"Timeout for --wait in seconds (default: {DEFAULT_WAIT_TIMEOUT})",
    )

    # stop
    subparsers.add_parser("stop", help="Stop robotocore container")

    # status
    subparsers.add_parser("status", help="Show container status")

    # logs
    logs_parser = subparsers.add_parser("logs", help="Tail container logs")
    logs_parser.add_argument(
        "--tail",
        type=int,
        default=None,
        help="Number of lines to show from end of logs",
    )
    logs_parser.add_argument(
        "--no-follow",
        dest="follow",
        action="store_false",
        default=True,
        help="Don't follow logs (just print and exit)",
    )

    # restart
    restart_parser = subparsers.add_parser("restart", help="Restart container")
    restart_parser.add_argument(
        "--image",
        default=None,
        help=f"Docker image (default: {DEFAULT_IMAGE}, or ROBOTOCORE_IMAGE env var)",
    )
    restart_parser.add_argument(
        "--env",
        "-e",
        action="append",
        help="Pass environment variable to container (KEY=VAL)",
    )

    # health
    subparsers.add_parser("health", help="Check health endpoint")

    # wait
    wait_parser = subparsers.add_parser("wait", help="Wait for container to be healthy")
    wait_parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_WAIT_TIMEOUT,
        help=f"Timeout in seconds (default: {DEFAULT_WAIT_TIMEOUT})",
    )

    # version
    subparsers.add_parser("version", help="Show robotocore version")

    # state subcommands
    state_parser = subparsers.add_parser("state", help="Manage state snapshots")
    state_sub = state_parser.add_subparsers(dest="state_command", help="State command")

    save_parser = state_sub.add_parser("save", help="Save a state snapshot")
    save_parser.add_argument("snapshot_name", help="Snapshot name")

    load_parser = state_sub.add_parser("load", help="Load a state snapshot")
    load_parser.add_argument("snapshot_name", help="Snapshot name")

    state_sub.add_parser("list", help="List snapshots")
    state_sub.add_parser("reset", help="Reset all state")

    # services
    services_parser = subparsers.add_parser("services", help="List registered services")
    _add_format_flag(services_parser)
    services_parser.add_argument(
        "--status",
        choices=["native", "moto", "all"],
        default="all",
        help="Filter by status (default: all)",
    )

    # config
    config_parser = subparsers.add_parser("config", help="Runtime configuration")
    config_sub = config_parser.add_subparsers(dest="config_command", help="Config command")

    config_sub.add_parser("get", help="Show all configuration")

    config_set_parser = config_sub.add_parser("set", help="Set a config value")
    config_set_parser.add_argument("key", help="Config key")
    config_set_parser.add_argument("value", help="Config value")

    config_reset_parser = config_sub.add_parser("reset", help="Reset a config key")
    config_reset_parser.add_argument("key", help="Config key to reset")

    # chaos
    chaos_parser = subparsers.add_parser("chaos", help="Chaos engineering rules")
    _add_format_flag(chaos_parser)
    chaos_sub = chaos_parser.add_subparsers(dest="chaos_command", help="Chaos command")

    chaos_sub.add_parser("list", help="List chaos rules")

    chaos_add_parser = chaos_sub.add_parser("add", help="Add a chaos rule")
    chaos_add_parser.add_argument("--service", required=True, help="Target service")
    chaos_add_parser.add_argument("--error", required=True, help="Error code to inject")
    chaos_add_parser.add_argument(
        "--status-code", type=int, required=True, dest="status_code", help="HTTP status code"
    )
    chaos_add_parser.add_argument("--operation", default=None, help="Target operation (optional)")
    chaos_add_parser.add_argument(
        "--latency", type=int, default=None, help="Latency to add in ms (optional)"
    )
    chaos_add_parser.add_argument(
        "--rate", type=float, default=None, help="Error rate 0.0-1.0 (optional)"
    )

    chaos_remove_parser = chaos_sub.add_parser("remove", help="Remove a chaos rule")
    chaos_remove_parser.add_argument("rule_id", help="Rule ID to remove")

    chaos_sub.add_parser("clear", help="Clear all chaos rules")

    # resources
    resources_parser = subparsers.add_parser("resources", help="Browse resources")
    _add_format_flag(resources_parser)
    resources_parser.add_argument(
        "service", nargs="?", default=None, help="Service to inspect (optional)"
    )

    # audit
    audit_parser = subparsers.add_parser("audit", help="API call history")
    _add_format_flag(audit_parser)
    audit_parser.add_argument(
        "--limit", type=int, default=20, help="Number of entries to show (default: 20)"
    )

    # usage
    usage_parser = subparsers.add_parser("usage", help="Usage analytics")
    _add_format_flag(usage_parser)
    usage_sub = usage_parser.add_subparsers(dest="usage_command", help="Usage command")
    usage_sub.add_parser("services", help="Per-service usage")
    usage_sub.add_parser("errors", help="Error breakdown")

    # pods
    pods_parser = subparsers.add_parser("pods", help="Cloud Pods management")
    _add_format_flag(pods_parser)
    pods_sub = pods_parser.add_subparsers(dest="pods_command", help="Pods command")

    pods_sub.add_parser("list", help="List pods")

    pods_save_parser = pods_sub.add_parser("save", help="Save a pod")
    pods_save_parser.add_argument("pod_name", help="Pod name")

    pods_load_parser = pods_sub.add_parser("load", help="Load a pod")
    pods_load_parser.add_argument("pod_name", help="Pod name")

    pods_info_parser = pods_sub.add_parser("info", help="Show pod details")
    pods_info_parser.add_argument("pod_name", help="Pod name")

    pods_delete_parser = pods_sub.add_parser("delete", help="Delete a pod")
    pods_delete_parser.add_argument("pod_name", help="Pod name")

    # ses
    ses_parser = subparsers.add_parser("ses", help="Email inspection")
    _add_format_flag(ses_parser)
    ses_sub = ses_parser.add_subparsers(dest="ses_command", help="SES command")

    ses_messages_parser = ses_sub.add_parser("messages", help="List email messages")
    ses_messages_parser.add_argument(
        "--limit", type=int, default=None, help="Number of messages to show"
    )

    ses_sub.add_parser("clear", help="Clear all messages")

    # iam
    iam_parser = subparsers.add_parser("iam", help="IAM policy stream")
    _add_format_flag(iam_parser)
    iam_sub = iam_parser.add_subparsers(dest="iam_command", help="IAM command")

    iam_stream_parser = iam_sub.add_parser("stream", help="View policy evaluation stream")
    iam_stream_parser.add_argument("--limit", type=int, default=None, help="Number of entries")
    iam_stream_parser.add_argument(
        "--decision", choices=["ALLOW", "DENY"], default=None, help="Filter by decision"
    )

    iam_suggest_parser = iam_sub.add_parser("suggest", help="Suggest IAM policy for principal")
    iam_suggest_parser.add_argument("principal", help="Principal ARN or ID")
    iam_suggest_parser.add_argument("--limit", type=int, default=None, help="Number of entries")

    # diagnose
    subparsers.add_parser("diagnose", help="Collect diagnostic bundle")

    return parser


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_STATE_DISPATCH = {
    "save": cmd_state_save,
    "load": cmd_state_load,
    "list": cmd_state_list,
    "reset": cmd_state_reset,
}


def _run(argv: list[str] | None = None) -> int:
    """Run the CLI, returning an exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 1

    dispatch = {
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "logs": cmd_logs,
        "restart": cmd_restart,
        "health": cmd_health,
        "wait": cmd_wait,
        "version": cmd_version,
        "services": cmd_services,
        "config": cmd_config,
        "chaos": cmd_chaos,
        "resources": cmd_resources,
        "audit": cmd_audit,
        "usage": cmd_usage,
        "pods": cmd_pods,
        "ses": cmd_ses,
        "iam": cmd_iam,
        "diagnose": cmd_diagnose,
    }

    if args.command == "state":
        state_cmd = getattr(args, "state_command", None)
        handler = _STATE_DISPATCH.get(state_cmd)
        if handler is None:
            print("Usage: robotocore state {save,load,list,reset}", file=sys.stderr)
            return 1
        return handler(args)

    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        return 1

    return handler(args)


def main(argv: list[str] | None = None) -> None:
    """Console script entrypoint."""
    sys.exit(_run(argv))


if __name__ == "__main__":
    main()
