"""State lifecycle hooks for save/load/reset operations.

Provides a registry for callbacks that fire before and after state operations.
Hooks can be registered by plugins or programmatically for testing/integrations.

Before hooks:
    - Run synchronously in registration order
    - If a before hook raises, the operation is aborted (exception propagates)

After hooks:
    - Run synchronously in registration order
    - If an after hook raises, a warning is logged but the operation is not failed
"""

import enum
import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)


class HookType(enum.Enum):
    """Types of state lifecycle hooks."""

    BEFORE_SAVE = "before_save"
    AFTER_SAVE = "after_save"
    BEFORE_LOAD = "before_load"
    AFTER_LOAD = "after_load"
    BEFORE_RESET = "before_reset"
    AFTER_RESET = "after_reset"


# Which hook types are "after" hooks (errors are logged, not propagated)
_AFTER_HOOKS = {
    HookType.AFTER_SAVE,
    HookType.AFTER_LOAD,
    HookType.AFTER_RESET,
}


class StateHookRegistry:
    """Registry for state lifecycle hook callbacks.

    Usage::

        from robotocore.state.hooks import state_hooks, HookType

        def my_hook(context: dict) -> None:
            print(f"State saved to {context.get('snapshot_path')}")

        state_hooks.register(HookType.AFTER_SAVE, my_hook)
    """

    def __init__(self) -> None:
        self._hooks: dict[HookType, list[Callable[[dict], None]]] = {ht: [] for ht in HookType}

    def register(self, hook_type: HookType, callback: Callable[[dict], None]) -> None:
        """Register a callback for the given hook type."""
        self._hooks[hook_type].append(callback)

    def unregister(self, hook_type: HookType, callback: Callable[[dict], None]) -> None:
        """Remove a previously registered callback."""
        try:
            self._hooks[hook_type].remove(callback)
        except ValueError as exc:
            logger.debug("unregister: remove failed (non-fatal): %s", exc)

    def fire(self, hook_type: HookType, context: dict) -> None:
        """Execute all callbacks registered for the given hook type.

        For 'before' hooks: exceptions propagate (aborting the operation).
        For 'after' hooks: exceptions are logged as warnings but don't propagate.

        Args:
            hook_type: Which hook to fire.
            context: Dict with keys like services, snapshot_name, snapshot_path.
        """
        is_after = hook_type in _AFTER_HOOKS
        for callback in self._hooks[hook_type]:
            if is_after:
                try:
                    callback(context)
                except Exception:
                    logger.warning(
                        "After-hook %s raised an exception",
                        _callback_name(callback),
                        exc_info=True,
                    )
            else:
                # Before hooks: let exceptions propagate to abort the operation
                callback(context)

    def list_hooks(self) -> dict[str, list[str]]:
        """Return a summary of all registered hooks for introspection.

        Returns:
            Dict mapping hook type value to list of callback descriptions.
        """
        result: dict[str, list[str]] = {}
        for hook_type, callbacks in self._hooks.items():
            if callbacks:
                result[hook_type.value] = [_callback_name(cb) for cb in callbacks]
        return result


def _callback_name(callback: Callable) -> str:
    """Get a human-readable name for a callback."""
    if hasattr(callback, "__qualname__"):
        return callback.__qualname__
    if hasattr(callback, "__name__"):
        return callback.__name__
    return repr(callback)


# Global singleton
state_hooks = StateHookRegistry()
