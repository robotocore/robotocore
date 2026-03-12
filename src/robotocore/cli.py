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
) -> dict:
    """Make an HTTP request to the robotocore management API."""
    body = None
    if data is not None:
        body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def _print_json(data: dict) -> None:
    """Pretty-print a JSON response."""
    print(json.dumps(data, indent=2))


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
    return 0


def cmd_logs(args: argparse.Namespace) -> int:
    """Tail container logs."""
    name = _get_container_name(args)
    try:
        subprocess.run(["docker", "logs", "-f", name], check=True)
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


def cmd_state_save(args: argparse.Namespace) -> int:
    """Save a state snapshot."""
    base_url = _get_base_url(args)
    try:
        data = _api_request(
            f"{base_url}/_robotocore/state/save",
            method="POST",
            data={"name": args.snapshot_name},
        )
        _print_json(data)
        return 0
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"State save failed: {e}", file=sys.stderr)
        return 1


def cmd_state_load(args: argparse.Namespace) -> int:
    """Load a state snapshot."""
    base_url = _get_base_url(args)
    try:
        data = _api_request(
            f"{base_url}/_robotocore/state/load",
            method="POST",
            data={"name": args.snapshot_name},
        )
        _print_json(data)
        return 0
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"State load failed: {e}", file=sys.stderr)
        return 1


def cmd_state_list(args: argparse.Namespace) -> int:
    """List state snapshots."""
    base_url = _get_base_url(args)
    try:
        data = _api_request(f"{base_url}/_robotocore/state/snapshots")
        _print_json(data)
        return 0
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"State list failed: {e}", file=sys.stderr)
        return 1


def cmd_state_reset(args: argparse.Namespace) -> int:
    """Reset all state."""
    base_url = _get_base_url(args)
    try:
        data = _api_request(
            f"{base_url}/_robotocore/state/reset",
            method="POST",
        )
        _print_json(data)
        return 0
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"State reset failed: {e}", file=sys.stderr)
        return 1


def build_url(base_url: str, path: str) -> str:
    """Build a full URL from base and path. Exposed for testing."""
    return f"{base_url}{path}"


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

    # stop
    subparsers.add_parser("stop", help="Stop robotocore container")

    # status
    subparsers.add_parser("status", help="Show container status")

    # logs
    subparsers.add_parser("logs", help="Tail container logs")

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

    # state subcommands
    state_parser = subparsers.add_parser("state", help="Manage state snapshots")
    state_sub = state_parser.add_subparsers(dest="state_command", help="State command")

    save_parser = state_sub.add_parser("save", help="Save a state snapshot")
    save_parser.add_argument("snapshot_name", help="Snapshot name")

    load_parser = state_sub.add_parser("load", help="Load a state snapshot")
    load_parser.add_argument("snapshot_name", help="Snapshot name")

    state_sub.add_parser("list", help="List snapshots")
    state_sub.add_parser("reset", help="Reset all state")

    return parser


_STATE_DISPATCH: dict[str | None, type[...] | None] = {
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

    dispatch: dict[str, ...] = {
        "start": cmd_start,
        "stop": cmd_stop,
        "status": cmd_status,
        "logs": cmd_logs,
        "restart": cmd_restart,
        "health": cmd_health,
        "wait": cmd_wait,
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
