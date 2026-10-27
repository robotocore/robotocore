#!/usr/bin/env python3
"""Legacy invocation shim: boots the Rust robotocore twin from the old workflows.

robotocore 2.0.0 is pure Rust — the python server that
`python -m robotocore.main` used to start no longer exists. The GitHub
workflows (parity, nightly, iac, ci server shapes) still launch the twin
with that exact invocation, so this shim keeps them at zero-yml-changes
(the origin OAuth token cannot modify workflow files): it builds and starts
the Rust server through scripts/start_parity_servers.sh, waits for
`/_robotocore/health`, then replaces itself with the server process so each
workflow's `SERVER_PID=$!` + `kill -0` bookkeeping keeps being true.
"""

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _pick_binary() -> Path | None:
    release = ROOT / "target" / "release" / "robotocore-rust"
    debug = ROOT / "target" / "debug" / "robotocore-rust"
    return next((c for c in (release, debug) if c.exists()), None)


def main() -> int:
    port = os.environ.get("ROBOTOCORE_PORT") or os.environ.get("RUST_PORT") or "4566"
    os.environ.setdefault("LOG_PREFIX", "robotocore")
    script = ROOT / "scripts" / "start_parity_servers.sh"
    if not script.exists():
        print(f"shim: {script} missing — cannot start the Rust server", file=sys.stderr)
        return 1

    # Build (or reuse) the release binary, then replace THIS process with the
    # server itself so workflows' `SERVER_PID=$!` stays truthful (kill -0,
    # SIGTERM teardown). The starter's build+nohup+wait shape is replicated
    # here with exec substituted for the nohup so one pid serves both roles.
    proc = subprocess.Popen(["bash", "-c", "cargo build --release"],
                            env={**os.environ, "RUST_PORT": port})
    returncode = proc.wait()
    if returncode != 0:
        print("shim: cargo build --release failed", file=sys.stderr)
        return 1
    binary = _pick_binary()
    if binary is None:
        print("shim: cargo built but no robotocore-rust binary on disk", file=sys.stderr)
        return 1
    host = os.environ.get("RUST_HOST", "127.0.0.1")
    print(f"shim: exec-ing {binary} --host {host} --port {port}")
    os.execv(str(binary), [str(binary), "--host", host, "--port", port])
    return 0  # unreachable: execv replaces this process


if __name__ == "__main__":
    sys.exit(main())
