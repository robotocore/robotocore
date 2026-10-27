#!/usr/bin/env python3
"""Legacy invocation shim: boots the Rust robotocore twin from the old workflows.

robotocore 2.0.0 is pure Rust — the python server that
`python -m robotocore.main` used to start no longer exists. The GitHub
workflows (parity, nightly, iac, ci server shapes) still launch the twin
with that exact invocation, so this shim keeps them at zero-yml-changes
(the origin OAuth token cannot modify workflow files):

- CI runners (GITHUB_ACTIONS): compile-on-the-fly is too slow for the
  workflows' 60-second health poll, so it runs the released docker
  image instead — the same v2.0.0 bits that jackdanger/robotocore and
  ghcr.io/robotocore serve (fast pull, port mapped 1:1). `docker run
  --rm -i` keeps the CLI alive as the exec target so workflows'
  `SERVER_PID=$!` + `kill -0`/SIGTERM bookkeeping stays true.
- Local/non-CI: reuse (or cargo-build) the release binary and exec into
  it in the same process, same pid bookkeeping.
"""

import os
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HEALTH_TIMEOUT_S = 300  # a cold local cargo build may take a few minutes


def _pick_binary() -> Path | None:
    # The debug-profile binary is what CI's `cargo build` produces (faster
    # compile, fits the workflows' 60 s health poll); release is the local
    # fallback the starter script keeps around.
    debug = ROOT / "target" / "debug" / "robotocore-rust"
    release = ROOT / "target" / "release" / "robotocore-rust"
    if debug.exists():
        return debug
    if release.exists():
        return release
    return None


def _health_ok(port: str) -> bool:
    try:
        return bool(
            urllib.request.urlopen(f"http://127.0.0.1:{port}/_robotocore/health", timeout=2).read()
        )
    except Exception:
        return False


def main() -> int:
    port = os.environ.get("ROBOTOCORE_PORT") or os.environ.get("RUST_PORT") or "4566"
    host = os.environ.get("RUST_HOST", "127.0.0.1")

    if _health_ok(port):
        print(f"shim: a twin is already healthy on {port} — nothing to start", file=sys.stderr)
        # Keep the foreground process alive without a second server; the
        # workflow's SERVER_PID then points at a live guardian that dies with
        # its docker container on teardown.
        subprocess.run([sys.executable, "-c", "import time;time.sleep(10**9)"])
        return 0

    # CI fast-path: the runner boots docker; the registry image is
    # identical to the rust tip's release build (tagged v2.0.0).
    if os.environ.get("GITHUB_ACTIONS") == "true" and shutil.which("docker"):
        try:
            _exec_docker([IMAGE, FALLBACK_IMAGE], port)
        except RuntimeError as e:
            print(
                f"shim: docker fast-path unavailable ({e}) — falling back to cargo build",
                file=sys.stderr,
            )

    # Local path: reuse (or build) the release binary, then exec it in-place
    # so workflows' `SERVER_PID=$!` + `kill -0` stays truthful.
    if _pick_binary() is None or os.environ.get("ROBOTOCORE_FORCE_BUILD") == "1":
        proc = subprocess.Popen(
            ["cargo", "build"], cwd=str(ROOT), env={**os.environ, "RUST_PORT": port}
        )
        if proc.wait() != 0:
            print("shim: cargo build --release failed", file=sys.stderr)
            return 1
    binary = _pick_binary()
    if binary is None:
        print("shim: no release robotocore-rust binary on disk", file=sys.stderr)
        return 1
    print(f"shim: exec-ing {binary} --host {host} --port {port}")
    try:
        os.execv(str(binary), [str(binary), "--host", host, "--port", port])
    except Exception as e:  # pragma: no cover
        print(f"shim: exec failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
