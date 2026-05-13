"""Fault-in installer for Lambda Ruby runtimes (ruby3.2/3.3/3.4).

Unlike Java/Node/.NET, there's no official portable-binary distribution
for Ruby. We pull from Docker Hub: the official ``ruby:3.X-slim`` images
ship a complete Ruby install under ``/usr/local/`` that we already use
in the (baked-in) Dockerfile pattern. The same trick works at runtime
via the Docker Registry HTTP API — no docker daemon, no extra binary
dependencies, stdlib-only.

Flow:
  1. Get an anonymous bearer token from auth.docker.io.
  2. Fetch the manifest list, pick the matching platform variant.
  3. For each layer (gzipped tar), stream and extract members whose
     paths start with ``usr/local/`` into ``prefix/``.
  4. Write a wrapper at ``/usr/local/bin/rubyX.Y`` that sets
     LD_LIBRARY_PATH + RUBYLIB + GEM_PATH and exec's the right binary.

The same wrapper shape as the Dockerfile's baked-in installs (see
``Dockerfile`` standard stage), so behavior matches whichever path the
runtime is provided through.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import platform
import tarfile
from dataclasses import dataclass
from urllib.request import Request, urlopen

from robotocore.services.lambda_.runtimes.install import (
    CACHE_DIR,
    DOWNLOAD_TIMEOUT_S,
    InstallPlan,
    _register,
)

logger = logging.getLogger(__name__)

_REGISTRY = "https://registry-1.docker.io"
_AUTH = "https://auth.docker.io/token"
_MANIFEST_ACCEPT = ",".join(
    [
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.index.v1+json",
    ]
)


def _docker_arch() -> str:
    m = platform.machine().lower()
    if m in ("x86_64", "amd64"):
        return "amd64"
    if m in ("aarch64", "arm64"):
        return "arm64"
    return m


def _get_token(repo: str) -> str:
    url = f"{_AUTH}?service=registry.docker.io&scope=repository:{repo}:pull"
    with urlopen(Request(url), timeout=DOWNLOAD_TIMEOUT_S) as resp:
        return json.loads(resp.read())["token"]


def _fetch_manifest(repo: str, ref: str, token: str) -> dict:
    url = f"{_REGISTRY}/v2/{repo}/manifests/{ref}"
    req = Request(url, headers={"Authorization": f"Bearer {token}", "Accept": _MANIFEST_ACCEPT})
    with urlopen(req, timeout=DOWNLOAD_TIMEOUT_S) as resp:
        return json.loads(resp.read())


def _pick_platform_manifest(index: dict, repo: str, token: str) -> dict:
    """Resolve a multi-arch index to the manifest for our platform."""
    wanted = _docker_arch()
    for m in index.get("manifests", []):
        p = m.get("platform", {})
        if p.get("os") == "linux" and p.get("architecture") == wanted:
            return _fetch_manifest(repo, m["digest"], token)
    raise RuntimeError(f"No linux/{wanted} manifest in image index")


def _stream_layer_extract(repo: str, digest: str, token: str, target: str) -> None:
    """Download a layer blob, extract any ``usr/local/`` files into ``target``."""
    url = f"{_REGISTRY}/v2/{repo}/blobs/{digest}"
    req = Request(url, headers={"Authorization": f"Bearer {token}"})
    with urlopen(req, timeout=DOWNLOAD_TIMEOUT_S) as resp:
        # Layer blobs are gzipped tar; some registries return Content-Type
        # application/vnd.docker.image.rootfs.diff.tar.gzip — stream-decompress.
        # We need to buffer fully because Python's tarfile in streaming gz
        # mode is reliable, but seeks into nested directories must be linear.
        raw = resp.read()
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
        with tarfile.open(fileobj=gz, mode="r|") as tar:
            for member in tar:
                name = member.name.lstrip("./")
                # We only want files installed under /usr/local; rewrite to
                # be relative to target (so /usr/local/bin/ruby → bin/ruby).
                if not name.startswith("usr/local/"):
                    continue
                member.name = name[len("usr/local/") :]
                if not member.name:
                    continue
                try:
                    tar.extract(member, path=target, set_attrs=False)
                except (PermissionError, OSError) as exc:
                    logger.debug("ruby layer extract skipped %r: %s", member.name, exc)


def _pull_image_usr_local(repo: str, tag: str, target: str) -> None:
    token = _get_token(repo)
    manifest = _fetch_manifest(repo, tag, token)
    if manifest.get("manifests"):
        manifest = _pick_platform_manifest(manifest, repo, token)
    for layer in manifest.get("layers", []):
        _stream_layer_extract(repo, layer["digest"], token, target)


@dataclass
class RubyInstallPlan(InstallPlan):
    minor: str = ""  # "3.2", "3.3", "3.4"

    def install(self) -> None:
        self.prefix.mkdir(parents=True, exist_ok=True)
        _pull_image_usr_local("library/ruby", f"{self.minor}-slim", str(self.prefix))
        # Matching wrapper shape from the (baked-in) Dockerfile install:
        # RUBYLIB covers the relocated stdlib so json/rubygems/etc. load.
        wrapper = (
            f"#!/bin/sh\n"
            f'PREFIX="{self.prefix}"\n'
            f'VER="{self.minor}.0"\n'
            f'export LD_LIBRARY_PATH="$PREFIX/lib${{LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}}"\n'
            f'RUBYLIB_ADD="$PREFIX/lib/ruby/$VER:$PREFIX/lib/ruby/site_ruby/$VER:'
            f'$PREFIX/lib/ruby/vendor_ruby/$VER"\n'
            f'for arch_dir in "$PREFIX/lib/ruby/$VER"/*-linux*; do\n'
            f'  [ -d "$arch_dir" ] && RUBYLIB_ADD="$RUBYLIB_ADD:$arch_dir"\n'
            f"done\n"
            f'export RUBYLIB="$RUBYLIB_ADD${{RUBYLIB:+:$RUBYLIB}}"\n'
            f'export GEM_PATH="$PREFIX/lib/ruby/gems/$VER${{GEM_PATH:+:$GEM_PATH}}"\n'
            f'exec "$PREFIX/bin/ruby" "$@"\n'
        )
        self._write_wrapper(wrapper)
        self._mark_installed()


for _minor in ("3.2", "3.3", "3.4"):
    _register(
        RubyInstallPlan(
            runtime=f"ruby{_minor}",
            family="ruby",
            prefix=CACHE_DIR / f"ruby-{_minor}",
            binary_name=f"ruby{_minor}",
            minor=_minor,
        )
    )
