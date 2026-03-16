"""Runtime configuration management.

Allows updating a whitelisted set of configuration settings at runtime
via the ``/_robotocore/config`` management endpoint, without restarting
the server.

Gated behind ``ENABLE_CONFIG_UPDATES=1`` for safety.
"""

import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Whitelist of settings that can be changed at runtime.
# Maps setting name -> description for documentation.
# ---------------------------------------------------------------------------
MUTABLE_SETTINGS: dict[str, str] = {
    "LOG_LEVEL": "Root log verbosity (DEBUG, INFO, WARNING, ERROR)",
    "DEBUG": "Enable/disable debug mode (0 or 1)",
    "ENFORCE_IAM": "Toggle IAM policy enforcement (0 or 1)",
    "AUDIT_LOG_SIZE": "Maximum entries in the audit ring buffer",
    "SQS_DELAY_PURGE_RETRY": "Toggle SQS purge delay retry behavior (0 or 1)",
    "SQS_DELAY_RECENTLY_DELETED": "Toggle SQS recently-deleted delay (0 or 1)",
    "DYNAMODB_REMOVE_EXPIRED_ITEMS": "Toggle DynamoDB TTL item removal (0 or 1)",
    "USAGE_ANALYTICS": "Toggle usage analytics (0 or 1)",
}


def _apply_setting(key: str, value: str) -> None:
    """Propagate a setting change to the relevant subsystem."""
    if key == "LOG_LEVEL":
        level = getattr(logging, value.upper(), None)
        if level is not None:
            logging.getLogger().setLevel(level)
    elif key == "DEBUG":
        if value == "1":
            logging.getLogger().setLevel(logging.DEBUG)
    elif key == "AUDIT_LOG_SIZE":
        try:
            from robotocore.audit.log import get_audit_log

            get_audit_log().resize(int(value))
        except (ImportError, ValueError) as exc:
            logger.debug("_apply_setting: resize failed (non-fatal): %s", exc)


class RuntimeConfig:
    """Manages runtime configuration overrides.

    Overrides sit above environment variables and profile values.
    Only settings listed in ``MUTABLE_SETTINGS`` can be changed.
    """

    def __init__(self) -> None:
        self._overrides: dict[str, str] = {}
        self._history: list[dict[str, Any]] = {}  # type: ignore[assignment]
        self._history = []
        self.updates_enabled: bool = os.environ.get("ENABLE_CONFIG_UPDATES", "0") == "1"

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def get(self, key: str, default: str | None = None) -> str | None:
        """Return the effective value for *key*.

        Priority: runtime override > env var > *default*.
        """
        if key in self._overrides:
            return self._overrides[key]
        return os.environ.get(key, default)

    def set(self, key: str, value: str) -> str | None:
        """Set a runtime override for *key*, returning the old value.

        Raises ``PermissionError`` if updates are disabled.
        Raises ``ValueError`` if *key* is not in the whitelist.
        """
        if not self.updates_enabled:
            raise PermissionError("Runtime config updates are disabled")
        if key not in MUTABLE_SETTINGS:
            raise ValueError(f"Setting {key} cannot be changed at runtime")

        old_value = self.get(key)
        self._overrides[key] = value

        # Record history
        self._history.append(
            {
                "key": key,
                "old_value": old_value,
                "new_value": value,
                "timestamp": time.time(),
            }
        )

        # Propagate the change
        _apply_setting(key, value)
        logger.info("Config updated: %s = %s (was %s)", key, value, old_value)
        return old_value

    def delete(self, key: str) -> str | None:
        """Remove a runtime override for *key*, returning the old override value.

        Returns ``None`` if there was no override.
        """
        old = self._overrides.pop(key, None)
        if old is not None:
            # Re-apply the env/default value so subsystems revert
            env_val = os.environ.get(key)
            if env_val is not None:
                _apply_setting(key, env_val)
            self._history.append(
                {
                    "key": key,
                    "old_value": old,
                    "new_value": None,
                    "timestamp": time.time(),
                    "action": "reset",
                }
            )
            logger.info("Config reset: %s (was %s)", key, old)
        return old

    # ------------------------------------------------------------------
    # Inspection API
    # ------------------------------------------------------------------

    def get_history(self) -> list[dict[str, Any]]:
        """Return the full change history."""
        return list(self._history)

    def list_all(self) -> list[dict[str, Any]]:
        """Return all mutable settings with current value, source, and mutability."""
        result: list[dict[str, Any]] = []
        for key, description in MUTABLE_SETTINGS.items():
            if key in self._overrides:
                value = self._overrides[key]
                source = "runtime"
            elif key in os.environ:
                value = os.environ[key]
                source = "env"
            else:
                value = None
                source = "default"
            result.append(
                {
                    "key": key,
                    "value": value,
                    "source": source,
                    "mutable": True,
                    "description": description,
                }
            )
        return result


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
_runtime_config: RuntimeConfig | None = None


def get_runtime_config() -> RuntimeConfig:
    """Return the global RuntimeConfig singleton."""
    global _runtime_config  # noqa: PLW0603
    if _runtime_config is None:
        _runtime_config = RuntimeConfig()
    return _runtime_config
