"""Fault-in installer for Lambda .NET runtimes (dotnet6/8/9).

Source: Microsoft's official ``dotnet-install.sh`` (https://dot.net/v1/dotnet-install.sh),
already used by the Dockerfile for the baked-in default SDK. We invoke
the same script per-channel with ``--install-dir`` pointing at the
SAME location the baked SDK uses.

Unified DOTNET_ROOT is the key correctness property:

* The dotnet host (``dotnet`` binary) walks one root for SDKs and
  runtimes. Splitting baked into ``/usr/share/dotnet`` and fault-in
  into ``/var/lib/robotocore/runtimes/dotnet`` would make every
  faulted-in SDK invisible to the existing ``/usr/local/bin/dotnet``.
* The Dockerfile installs the baked SDK 9.0 directly into
  ``/var/lib/robotocore/runtimes/dotnet`` (matching ``_DOTNET_ROOT``
  below) and sets ``ENV DOTNET_ROOT=...`` so children inherit it.
* Fault-in then layers additional SDKs (6.0, 8.0) under the same root.
* No wrapper script is needed at ``WRAPPER_BIN_DIR`` because the
  baked ``/usr/local/bin/dotnet`` symlink already resolves to the
  unified root.

After installing a new SDK we must invalidate the cached probe in
``dotnet.py`` — otherwise ``_list_installed_majors()`` keeps returning
the pre-install set and ``_detect_tfm("dotnet6")`` keeps falling back
to host max.
"""

from __future__ import annotations

import logging
import os
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

# Must match the Dockerfile's ``ENV DOTNET_ROOT=`` so the baked SDK and
# any fault-in SDKs share a single dotnet host root.
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

        # Invalidate the cached runtime/TFM probes in dotnet.py so the new SDK
        # is visible to _list_installed_majors() and _detect_tfm() on the
        # next call. Without this, an endpoint refresh or another invocation
        # would still see only the pre-install set.
        from robotocore.services.lambda_.runtimes import dotnet as _dotnet_mod

        _dotnet_mod.invalidate_caches()

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
