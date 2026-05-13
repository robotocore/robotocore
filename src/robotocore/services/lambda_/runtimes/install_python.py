"""Fault-in installer for Lambda Python runtimes (python3.8 ... python3.13).

Source: `python-build-standalone <https://github.com/astral-sh/python-build-standalone>`_,
maintained by Astral (same group as ``uv`` and ``ruff``). They publish
statically-linked portable CPython tarballs for every active 3.x version
on Linux x86_64 and aarch64.

Release tag format: ``YYYYMMDD`` (dated). We resolve the latest tag per
major.minor by hitting the GitHub releases API and matching
``cpython-{X.Y}.*-{arch}-unknown-linux-gnu-install_only.tar.gz``.

Tarballs extract to a top-level ``python/`` dir; we strip it.
"""

from __future__ import annotations

import json
import logging
import platform
import re
from dataclasses import dataclass
from urllib.request import Request, urlopen

from robotocore.services.lambda_.runtimes.install import (
    CACHE_DIR,
    DOWNLOAD_TIMEOUT_S,
    InstallPlan,
    _register,
)
from robotocore.services.lambda_.runtimes.install_util import (
    download_and_extract_tarball,
)

logger = logging.getLogger(__name__)


def _pbs_arch() -> str:
    m = platform.machine().lower()
    if m in ("x86_64", "amd64"):
        return "x86_64"
    if m in ("aarch64", "arm64"):
        return "aarch64"
    return m


def _latest_pbs_asset(minor: int) -> str:
    """Find the URL for the latest python-build-standalone tarball for 3.<minor>."""
    arch = _pbs_arch()
    needle = re.compile(
        rf"cpython-3\.{minor}\.\d+\+\d{{8}}-{arch}-unknown-linux-gnu-install_only\.tar\.gz$"
    )
    api = "https://api.github.com/repos/astral-sh/python-build-standalone/releases/latest"
    req = Request(api, headers={"Accept": "application/vnd.github+json"})
    with urlopen(req, timeout=DOWNLOAD_TIMEOUT_S) as resp:
        body = json.loads(resp.read())
    for asset in body.get("assets", []):
        if needle.search(asset["name"]):
            return asset["browser_download_url"]
    raise RuntimeError(f"No python-build-standalone asset for 3.{minor} on {arch}")


@dataclass
class PythonInstallPlan(InstallPlan):
    minor: int = 0  # e.g. 10 for python3.10

    def install(self) -> None:
        url = _latest_pbs_asset(self.minor)
        # The PBS tarball contains a top-level ``python/`` directory.
        download_and_extract_tarball(url, self.prefix, strip_components=1)
        self._write_wrapper(
            f'#!/bin/sh\nexec "{self.prefix}/bin/python3.{self.minor}" "$@"\n',
        )
        self._mark_installed()


for _minor in (8, 9, 10, 11, 12, 13):
    _register(
        PythonInstallPlan(
            runtime=f"python3.{_minor}",
            family="python",
            prefix=CACHE_DIR / f"python-3.{_minor}",
            binary_name=f"python3.{_minor}",
            minor=_minor,
        )
    )
