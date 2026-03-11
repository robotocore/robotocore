"""Configuration profile loading for Robotocore."""

from robotocore.config.profiles import load_profiles

__all__ = ["load_config"]


def load_config() -> None:
    """Load configuration from profile files.

    Call this early in startup, before any other configuration is read.
    """
    load_profiles()
