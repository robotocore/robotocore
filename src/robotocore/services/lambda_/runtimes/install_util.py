"""Shared helpers for fault-in installers.

Stdlib-only — these helpers run inside the same Python that hosts
robotocore, so we can't depend on requests/httpx without expanding the
image's dependency surface.
"""

from __future__ import annotations

import gzip
import logging
import lzma
import os
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import IO
from urllib.request import Request, urlopen

from robotocore.services.lambda_.runtimes.install import DOWNLOAD_TIMEOUT_S

logger = logging.getLogger(__name__)


def _open_compressed(stream: IO[bytes], url: str) -> IO[bytes]:
    """Wrap ``stream`` in a gzip/xz decompressor based on the URL suffix."""
    if url.endswith(".tar.gz") or url.endswith(".tgz"):
        return gzip.GzipFile(fileobj=stream)
    if url.endswith(".tar.xz"):
        return lzma.LZMAFile(stream)
    if url.endswith(".tar"):
        return stream
    raise ValueError(f"Unsupported tarball compression for URL: {url}")


def download_and_extract_tarball(
    url: str,
    target_dir: Path,
    *,
    strip_components: int = 0,
) -> None:
    """Stream a tarball from ``url`` and extract into ``target_dir``.

    ``strip_components`` mirrors GNU tar's flag: drop the first N path
    segments from each member. Most distribution tarballs have a single
    top-level directory; ``strip_components=1`` lands files directly
    under ``target_dir``.

    Streaming via tarfile mode "r|" avoids buffering the whole archive
    in memory — important for the ~100MB Python and ~200MB JDK
    downloads.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s into %s", url, target_dir)

    # Some servers (Adoptium) reject the default urllib User-Agent.
    req = Request(url, headers={"User-Agent": "robotocore-fault-in/1.0"})

    with urlopen(req, timeout=DOWNLOAD_TIMEOUT_S) as resp:
        # tarfile streaming requires the underlying stream to be a true
        # blocking, seek-less stream. urllib responses fit; just wrap in
        # the right decompressor based on URL.
        decompressed = _open_compressed(resp, url)
        try:
            with tarfile.open(fileobj=decompressed, mode="r|") as tar:
                for member in tar:
                    if strip_components > 0:
                        parts = member.name.split("/", strip_components)
                        if len(parts) <= strip_components:
                            continue  # member is at or above the strip level
                        member.name = parts[strip_components]
                    if not member.name or member.name.startswith(("/", "..")):
                        continue
                    try:
                        tar.extract(member, path=str(target_dir), set_attrs=False)
                    except (PermissionError, OSError) as exc:
                        logger.debug("tar extract skipped %r: %s", member.name, exc)
        finally:
            decompressed.close()


def download_to_file(url: str, target_path: Path) -> None:
    """Stream ``url`` straight to disk. Useful for non-tarball assets."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    req = Request(url, headers={"User-Agent": "robotocore-fault-in/1.0"})
    fd, tmp = tempfile.mkstemp(dir=str(target_path.parent), prefix=".tmp-")
    try:
        with urlopen(req, timeout=DOWNLOAD_TIMEOUT_S) as resp, os.fdopen(fd, "wb") as out:
            shutil.copyfileobj(resp, out)
        os.replace(tmp, target_path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError as exc:
            logger.debug("download_to_file cleanup: %s", exc)
        raise
