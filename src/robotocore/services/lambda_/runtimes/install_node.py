"""Fault-in installer for Lambda Node.js runtimes (nodejs18.x/20.x/22.x).

Source: the official Node.js distribution at https://nodejs.org/dist/.
Each major version has a "latest-v{major}.x" symlink that we follow to
the current minor.patch. Tarballs are plain tar.xz with a top-level
``node-vX.Y.Z-linux-{arch}/`` directory.
"""

from __future__ import annotations

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


def _node_arch() -> str:
    m = platform.machine().lower()
    if m in ("x86_64", "amd64"):
        return "x64"
    if m in ("aarch64", "arm64"):
        return "arm64"
    return m


def _resolve_latest(major: int) -> str:
    """Return the current minor.patch for a Node major (e.g. '20.18.0').

    Hits https://nodejs.org/dist/latest-vXX.x/ and parses the redirect or
    a small directory listing.
    """
    url = f"https://nodejs.org/dist/latest-v{major}.x/SHASUMS256.txt"
    with urlopen(Request(url), timeout=DOWNLOAD_TIMEOUT_S) as resp:
        # Each line is "<sha>  <filename>" — first filename has the version.
        text = resp.read().decode()
    m = re.search(rf"node-v({major}\.\d+\.\d+)-linux", text)
    if not m:
        raise RuntimeError(f"Could not parse Node {major}.x version from {url}")
    return m.group(1)


@dataclass
class NodeInstallPlan(InstallPlan):
    major: int = 0

    def install(self) -> None:
        version = _resolve_latest(self.major)
        arch = _node_arch()
        url = f"https://nodejs.org/dist/v{version}/node-v{version}-linux-{arch}.tar.xz"
        download_and_extract_tarball(url, self.prefix, strip_components=1)
        self._write_wrapper(
            f'#!/bin/sh\nexec "{self.prefix}/bin/node" "$@"\n',
        )
        self._mark_installed()


for _major in (18, 20, 22):
    _register(
        NodeInstallPlan(
            runtime=f"nodejs{_major}.x",
            family="nodejs",
            prefix=CACHE_DIR / f"node-{_major}",
            binary_name=f"node{_major}",
            major=_major,
        )
    )
