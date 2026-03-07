#!/usr/bin/env python3
"""Development server lifecycle and test runner.

Manages starting/stopping the robotocore server and running tests against it.

Usage:
    # Run unit tests (parallel, no server needed)
    uv run python scripts/dev.py test-unit

    # Run compat tests (starts server automatically, parallel)
    uv run python scripts/dev.py test-compat

    # Run all tests
    uv run python scripts/dev.py test-all

    # Start server in background (for manual testing)
    uv run python scripts/dev.py server-start

    # Stop background server
    uv run python scripts/dev.py server-stop

    # Check if server is running
    uv run python scripts/dev.py server-status

    # Run smoke tests against running server
    uv run python scripts/dev.py smoke
"""

import argparse
import os
import signal
import subprocess
import sys
import time
import urllib.request

PIDFILE = os.path.join(os.path.dirname(__file__), "..", ".robotocore.pid")
PORT = int(os.environ.get("ROBOTOCORE_PORT", "4566"))
HEALTH_URL = f"http://localhost:{PORT}/_robotocore/health"
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _is_server_running():
    """Check if the server is responding to health checks."""
    try:
        resp = urllib.request.urlopen(HEALTH_URL, timeout=2)
        return resp.status == 200
    except Exception:
        return False


def _read_pid():
    """Read PID from pidfile."""
    try:
        with open(PIDFILE) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None


def _wait_for_server(timeout=30):
    """Wait for server to become healthy."""
    start = time.time()
    while time.time() - start < timeout:
        if _is_server_running():
            return True
        time.sleep(0.3)
    return False


def server_start(port=None):
    """Start the robotocore server in the background."""
    if _is_server_running():
        print(f"Server already running on port {PORT}")
        return True

    port = port or PORT
    env = os.environ.copy()
    env["ROBOTOCORE_PORT"] = str(port)
    env["ROBOTOCORE_HOST"] = "0.0.0.0"

    log_path = os.path.join(PROJECT_ROOT, ".robotocore.log")
    log_file = open(log_path, "w")

    proc = subprocess.Popen(
        [sys.executable, "-m", "robotocore.main"],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=log_file,
        stderr=log_file,
        start_new_session=True,
    )

    with open(PIDFILE, "w") as f:
        f.write(str(proc.pid))

    print(f"Starting server (PID {proc.pid}) on port {port}...")
    if _wait_for_server():
        print(f"Server ready on port {port}")
        return True
    else:
        print(f"Server failed to start. Check {log_path}")
        # Try to show last few lines of log
        try:
            with open(log_path) as f:
                lines = f.readlines()
                for line in lines[-10:]:
                    print(f"  {line.rstrip()}")
        except Exception:
            pass
        return False


def server_stop():
    """Stop the background server."""
    pid = _read_pid()
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
            # Wait for process to die
            for _ in range(20):
                try:
                    os.kill(pid, 0)
                    time.sleep(0.1)
                except ProcessLookupError:
                    break
            print(f"Server (PID {pid}) stopped")
        except ProcessLookupError:
            print(f"Server (PID {pid}) was not running")
        try:
            os.unlink(PIDFILE)
        except FileNotFoundError:
            pass
    else:
        print("No PID file found")
        # Try to kill by port
        if _is_server_running():
            print(f"Server is running on port {PORT} but no PID file. Kill manually.")


def server_status():
    """Check server status."""
    running = _is_server_running()
    pid = _read_pid()
    if running:
        print(f"Server is running on port {PORT}" + (f" (PID {pid})" if pid else ""))
    else:
        print(f"Server is not running on port {PORT}")
    return running


def _cpu_count():
    """Get number of CPUs for parallel testing."""
    try:
        return os.cpu_count() or 4
    except Exception:
        return 4


def run_unit_tests(extra_args=None):
    """Run unit tests in parallel."""
    n = min(_cpu_count(), 12)  # cap at 12 workers
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/unit/",
        f"-n{n}",
        "-q",
        "--tb=short",
    ]
    if extra_args:
        cmd.extend(extra_args)
    print(f"Running unit tests with {n} workers...")
    return subprocess.call(cmd, cwd=PROJECT_ROOT)


def run_compat_tests(extra_args=None, manage_server=True):
    """Run compatibility tests, starting server if needed."""
    server_was_running = _is_server_running()

    if not server_was_running and manage_server:
        if not server_start():
            print("Cannot run compat tests: server failed to start")
            return 1

    n = min(_cpu_count(), 8)  # slightly fewer workers for compat (network I/O)
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/compatibility/",
        f"-n{n}",
        "--dist=loadfile",  # tests in same file run serially to avoid state collisions
        "-q",
        "--tb=short",
    ]
    if extra_args:
        cmd.extend(extra_args)

    print(f"Running compat tests with {n} workers...")
    result = subprocess.call(cmd, cwd=PROJECT_ROOT)

    if not server_was_running and manage_server:
        server_stop()

    return result


def run_integration_tests(extra_args=None, manage_server=True):
    """Run integration tests, starting server if needed."""
    server_was_running = _is_server_running()

    if not server_was_running and manage_server:
        if not server_start():
            print("Cannot run integration tests: server failed to start")
            return 1

    cmd = [
        sys.executable, "-m", "pytest",
        "tests/integration/",
        "-q",
        "--tb=short",
    ]
    if extra_args:
        cmd.extend(extra_args)

    print("Running integration tests...")
    result = subprocess.call(cmd, cwd=PROJECT_ROOT)

    if not server_was_running and manage_server:
        server_stop()

    return result


def run_all_tests(extra_args=None):
    """Run all tests: unit (parallel), then compat+integration (with server)."""
    print("=== Unit Tests ===")
    rc = run_unit_tests(extra_args)
    if rc != 0:
        print("Unit tests failed, stopping.")
        return rc

    print("\n=== Starting server for compat + integration tests ===")
    server_was_running = _is_server_running()
    if not server_was_running:
        if not server_start():
            return 1

    print("\n=== Compatibility Tests ===")
    rc = run_compat_tests(extra_args, manage_server=False)

    print("\n=== Integration Tests ===")
    rc2 = run_integration_tests(extra_args, manage_server=False)

    if not server_was_running:
        server_stop()

    return max(rc, rc2)


def run_smoke():
    """Run smoke tests against running server."""
    if not _is_server_running():
        print("Server not running. Start it first or use 'test-compat'.")
        return 1
    cmd = [sys.executable, "scripts/smoke_test.py"]
    return subprocess.call(cmd, cwd=PROJECT_ROOT)


def main():
    parser = argparse.ArgumentParser(description="Robotocore dev server & test runner")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("server-start", help="Start dev server in background")
    sub.add_parser("server-stop", help="Stop dev server")
    sub.add_parser("server-status", help="Check server status")

    p = sub.add_parser("test-unit", help="Run unit tests (parallel)")
    p.add_argument("pytest_args", nargs="*", help="Extra pytest args")

    p = sub.add_parser("test-compat", help="Run compat tests (auto-starts server)")
    p.add_argument("pytest_args", nargs="*", help="Extra pytest args")

    p = sub.add_parser("test-integration", help="Run integration tests")
    p.add_argument("pytest_args", nargs="*", help="Extra pytest args")

    p = sub.add_parser("test-all", help="Run all tests")
    p.add_argument("pytest_args", nargs="*", help="Extra pytest args")

    sub.add_parser("smoke", help="Run smoke tests")

    args = parser.parse_args()

    if args.command == "server-start":
        sys.exit(0 if server_start() else 1)
    elif args.command == "server-stop":
        server_stop()
    elif args.command == "server-status":
        sys.exit(0 if server_status() else 1)
    elif args.command == "test-unit":
        sys.exit(run_unit_tests(args.pytest_args))
    elif args.command == "test-compat":
        sys.exit(run_compat_tests(args.pytest_args))
    elif args.command == "test-integration":
        sys.exit(run_integration_tests(args.pytest_args))
    elif args.command == "test-all":
        sys.exit(run_all_tests(args.pytest_args))
    elif args.command == "smoke":
        sys.exit(run_smoke())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
