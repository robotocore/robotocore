"""Configuration profile loading from dotenv files.

Profiles are stored at ``~/.robotocore/<name>.env`` (or the directory specified
by ``ROBOTOCORE_CONFIG_DIR``).  The ``CONFIG_PROFILE`` environment variable
selects which profile(s) to load (comma-separated for multiple).

Loading order (later overrides earlier):
1. ``default.env`` (if it exists)
2. Named profiles from ``CONFIG_PROFILE`` in order
3. Real environment variables (always highest priority)
"""

import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# Module-level state tracking which profiles were loaded
_active_profiles: list[str] = []
_resolved_values: dict[str, str] = {}

# Regex for ${VAR} expansion
_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def get_config_dir() -> Path:
    """Return the configuration directory, creating it if needed."""
    env_dir = os.environ.get("ROBOTOCORE_CONFIG_DIR")
    if env_dir:
        config_dir = Path(env_dir)
    else:
        config_dir = Path.home() / ".robotocore"

    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def parse_dotenv(content: str, env: dict[str, str] | None = None) -> dict[str, str]:
    """Parse dotenv content into a dict.

    Supports:
    - KEY=value
    - # comments
    - Empty lines
    - Quoted values (single and double)
    - Variable expansion: ${OTHER_KEY}
    """
    result: dict[str, str] = {}
    # Merge current env + already-parsed keys for variable expansion
    lookup: dict[str, str] = dict(os.environ)
    if env:
        lookup.update(env)

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        if "=" not in line:
            continue

        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()

        # Strip quotes
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]

        # Variable expansion
        def _expand(match: re.Match[str]) -> str:
            var_name = match.group(1)
            # Check already-parsed values first, then lookup
            return result.get(var_name, lookup.get(var_name, match.group(0)))

        value = _VAR_RE.sub(_expand, value)

        result[key] = value

    return result


def _load_profile_file(profile_path: Path, env_snapshot: dict[str, str]) -> dict[str, str]:
    """Load and parse a single profile file, returning its values."""
    if not profile_path.is_file():
        return {}
    content = profile_path.read_text()
    return parse_dotenv(content, env=env_snapshot)


def load_profiles() -> None:
    """Load configuration profiles into environment variables.

    Respects the priority order: default.env < named profiles < real env vars.
    """
    global _active_profiles, _resolved_values  # noqa: PLW0603
    _active_profiles = []
    _resolved_values = {}

    config_dir = get_config_dir()

    # Snapshot env vars that were set BEFORE profile loading -- these always win
    pre_env = dict(os.environ)

    # Accumulated values from all profiles
    accumulated: dict[str, str] = {}

    # 1. Load default.env if it exists
    default_path = config_dir / "default.env"
    if default_path.is_file():
        vals = _load_profile_file(default_path, accumulated)
        accumulated.update(vals)
        _active_profiles.append("default")
        logger.info("Loaded default configuration profile")

    # 2. Load named profiles from CONFIG_PROFILE
    profile_str = os.environ.get("CONFIG_PROFILE", "")
    if profile_str:
        profile_names = [p.strip() for p in profile_str.split(",") if p.strip()]
        for name in profile_names:
            profile_path = config_dir / f"{name}.env"
            if not profile_path.is_file():
                logger.warning("Configuration profile not found: %s (%s)", name, profile_path)
                continue
            vals = _load_profile_file(profile_path, accumulated)
            accumulated.update(vals)
            _active_profiles.append(name)
            logger.info("Loaded configuration profile: %s", name)

    # 3. Apply accumulated values to env, but env vars always win
    for key, value in accumulated.items():
        if key not in pre_env:
            os.environ[key] = value

    _resolved_values = dict(accumulated)

    if _active_profiles:
        logger.info("Active configuration profiles: %s", ", ".join(_active_profiles))


def list_available_profiles() -> list[str]:
    """Return names of all available profile files in the config directory."""
    config_dir = get_config_dir()
    return sorted(p.stem for p in config_dir.glob("*.env"))


def get_active_profiles() -> list[str]:
    """Return the list of profiles that were loaded (in order)."""
    return list(_active_profiles)


def get_resolved_config() -> dict[str, str]:
    """Return all config values resolved from profiles (before env overrides)."""
    return dict(_resolved_values)
