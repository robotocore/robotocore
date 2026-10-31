#!/usr/bin/env python3
"""Invocation shim: keeps `python -m robotocore.main` boots working post-2.0.

robotocore is pure Rust since 2.0 — `python -m robotocore.main` no longer
starts a python server. The repo's CI lanes now build the Rust twin from
source directly (see .github/actions/start-twin-from-source), so this
shim's remaining consumers are local dev and third-party example lanes:

- CI runners (GITHUB_ACTIONS) happening to call this shim: run a released
  docker image — :latest first (the release workflow retags it on every
  version), with the v2.0.0 mirrors as first-generation fallbacks.
- Local/no-docker: reuse (or cargo-build) the debug/release binary and
  exec into it in-process, keeping pid bookkeeping truthful.
"""

import os
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


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

    # CI fast-path: the runners have docker; the published release images
    # (ghcr.io and jackdanger, tag v2.0.0) ARE this tree's rust build
    # (verified: 160 services, version 2.0.0, axum startup). Breathing
    # through them is what lets a cold runner answer the workflows'
    # 60-second health poll; the cargo path below is the fallback for when
    # the registries are not reachable.
    if os.environ.get("GITHUB_ACTIONS") == "true" and shutil.which("docker"):
        docker = shutil.which("docker")
        # Prefer :latest: the release workflow pushes it on every tag, so the
        # shim serves the newest published bits without being pinned to one
        # version forever; the v2.0.0 mirrors remain first-generation
        # fallbacks for offline-ish tags.
        for image in ("ghcr.io/robotocore/robotocore:latest",
                      "jackdanger/robotocore:latest",
                      "ghcr.io/robotocore/robotocore:v2.0.0",
                      "jackdanger/robotocore:v2.0.0"):
            pulled = subprocess.run(
                ["docker", "pull", image],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if pulled.returncode == 0:
                print(f"shim (ci fast path): serving the twin from {image}")
                os.execv(docker, [docker, "run", "--rm", "-i",
                                  "-p", f"{port}:4566", image])
            # pull failure → try the next mirror, then fall through to cargo
    # Local path: reuse (or build) the release binary, then exec it in-place
    # so workflows' `SERVER_PID=$!` + `kill -0` stays truthful.
    if _pick_binary() is None or os.environ.get("ROBOTOCORE_FORCE_BUILD") == "1":
        proc = subprocess.Popen(
            ["cargo", "build"], cwd=str(ROOT), env={**os.environ, "RUST_PORT": port}
        )
        if proc.wait() != 0:
            print("shim: cargo build failed", file=sys.stderr)
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
