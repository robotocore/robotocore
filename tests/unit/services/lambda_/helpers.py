"""Shared test helpers for Lambda runtime tests."""

import io
import zipfile


def make_zip(files: dict[str, str | bytes]) -> bytes:
    """Create a zip archive in memory. files maps filename -> content."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            if isinstance(content, str):
                zf.writestr(name, content)
            else:
                zf.writestr(name, content)
    return buf.getvalue()
