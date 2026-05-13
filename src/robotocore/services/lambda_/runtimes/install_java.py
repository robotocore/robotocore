"""Fault-in installer for Lambda Java runtimes (java8/8.al2/11/17/21).

Source: the Adoptium API (https://api.adoptium.net/v3/binary/...), which
publishes Eclipse Temurin builds for every JDK major version we care
about. We fetch the JRE (not JDK) since ``Bootstrap.java`` is
pre-compiled at image build time — runtime invocations only need
``java``, not ``javac``.

URL pattern:
    https://api.adoptium.net/v3/binary/latest/{major}/ga/linux/{arch}/jre/hotspot/normal/eclipse

The response is a tar.gz with a single top-level directory containing
``bin/java``, ``lib/``, etc.
"""

from __future__ import annotations

import logging
import platform
from dataclasses import dataclass

from robotocore.services.lambda_.runtimes.install import (
    CACHE_DIR,
    InstallPlan,
    _register,
)
from robotocore.services.lambda_.runtimes.install_util import (
    download_and_extract_tarball,
)

logger = logging.getLogger(__name__)


def _adoptium_arch() -> str:
    """Map ``platform.machine()`` to Adoptium's URL arch token."""
    m = platform.machine().lower()
    if m in ("x86_64", "amd64"):
        return "x64"
    if m in ("aarch64", "arm64"):
        return "aarch64"
    return m  # let Adoptium reject it explicitly if unknown


@dataclass
class JavaInstallPlan(InstallPlan):
    major: int = 0  # 8, 11, 17, 21

    def install(self) -> None:
        url = (
            f"https://api.adoptium.net/v3/binary/latest/{self.major}/ga/"
            f"linux/{_adoptium_arch()}/jre/hotspot/normal/eclipse"
        )
        # Adoptium redirects to a versioned tarball; strip the single
        # top-level dir (jdk-21.0.5+11-jre) so files land directly under prefix.
        download_and_extract_tarball(url, self.prefix, strip_components=1)
        self._write_wrapper(
            f'#!/bin/sh\nexec "{self.prefix}/bin/java" "$@"\n',
        )
        self._mark_installed()


# Register one plan per Lambda runtime ID. java8.al2 shares the major-8 install.
for _major, _ids in {
    8: ("java8", "java8.al2"),
    11: ("java11",),
    17: ("java17",),
    21: ("java21",),
}.items():
    for _rt in _ids:
        _register(
            JavaInstallPlan(
                runtime=_rt,
                family="java",
                prefix=CACHE_DIR / f"java-{_major}",
                # _RUNTIME_BINARY["java8.al2"] = "java8" — the wrapper name
                # follows the binary mapping, not the runtime ID.
                binary_name=f"java{_major}",
                major=_major,
            )
        )
