"""Fault-in installer for Lambda .NET runtimes (dotnet6/8/9).

Source: Microsoft's official ``dotnet-install.sh`` (https://dot.net/v1/dotnet-install.sh),
already used by the Dockerfile for the baked-in default SDK. We invoke
the same script per-channel with ``--install-dir`` pointing at the
fault-in cache prefix, so each version gets its own dotnet host root.

Unlike the other languages, .NET runtimes share a single ``dotnet``
binary multiplexed via ``runtimeconfig.json`` and reference packs. Our
``_detect_tfm()`` already routes per-runtime to ``netX.0`` once the
matching SDK is present anywhere on $PATH — but with fault-in we
install each into its own prefix and expose a single ``dotnet`` host
script that points $DOTNET_ROOT at the union prefix. The simplest
working pattern: install every channel into the SAME prefix (Microsoft
designed dotnet-install.sh to handle this), and let ``dotnet --info``
report all installed SDKs. So the wrapper here is a no-op; the install
puts new SDK files under ``/var/lib/robotocore/runtimes/dotnet/`` and
the existing ``dotnet`` binary picks them up via DOTNET_ROOT.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from urllib.request import Request, urlopen

from robotocore.services.lambda_.runtimes.install import (
    CACHE_DIR,
    DOWNLOAD_TIMEOUT_S,
    InstallPlan,
    _register,
)

logger = logging.getLogger(__name__)

# All .NET SDKs share a single install root so the dotnet host can find
# every SDK + runtime at once. Individual ``InstallPlan.prefix``es are
# subdirs under this for the .installed marker only.
_DOTNET_ROOT = CACHE_DIR / "dotnet"
_INSTALL_SCRIPT_URL = "https://dot.net/v1/dotnet-install.sh"


def _ensure_install_script() -> str:
    """Fetch dotnet-install.sh once into a temp file and return its path.

    The file is not chmod +x — we invoke it via ``sh <script>`` rather
    than executing it directly, which avoids needing the permission
    bit and a bandit B103 nosec comment.
    """
    fd, path = tempfile.mkstemp(suffix="-dotnet-install.sh")
    try:
        with urlopen(Request(_INSTALL_SCRIPT_URL), timeout=DOWNLOAD_TIMEOUT_S) as resp:
            os.write(fd, resp.read())
    finally:
        os.close(fd)
    return path


@dataclass
class DotnetInstallPlan(InstallPlan):
    channel: str = ""  # "6.0", "8.0", "9.0"

    def install(self) -> None:
        script = _ensure_install_script()
        try:
            _DOTNET_ROOT.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ["sh", script, "--channel", self.channel, "--install-dir", str(_DOTNET_ROOT)],
                check=True,
                timeout=DOWNLOAD_TIMEOUT_S,
            )
        finally:
            try:
                os.unlink(script)
            except OSError as exc:
                logger.debug("dotnet-install script cleanup: %s", exc)

        # Expose dotnet on $PATH if not already there. The dotnet host
        # picks up every SDK/runtime under _DOTNET_ROOT automatically.
        if not shutil.which("dotnet"):
            self._write_wrapper(
                f"#!/bin/sh\n"
                f'export DOTNET_ROOT="{_DOTNET_ROOT}"\n'
                f'exec "{_DOTNET_ROOT}/dotnet" "$@"\n'
            )
        self._mark_installed()


for _channel, _rt in (("6.0", "dotnet6"), ("8.0", "dotnet8"), ("9.0", "dotnet9")):
    _register(
        DotnetInstallPlan(
            runtime=_rt,
            family="dotnet",
            # Per-plan prefix exists only to host the .installed marker;
            # actual files all land under the shared _DOTNET_ROOT.
            prefix=CACHE_DIR / f"dotnet-{_channel.replace('.', '_')}",
            binary_name="dotnet",
            channel=_channel,
        )
    )
