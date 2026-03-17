"""Robotocore: Free, open-source AWS emulator built on Moto."""

from importlib.metadata import version as _pkg_version


def _get_version() -> str:
    """Derive version from package metadata (set by git tag at install time)."""
    try:
        return _pkg_version("robotocore")
    except Exception:  # noqa: BLE001
        return "dev"


__version__ = _get_version()
