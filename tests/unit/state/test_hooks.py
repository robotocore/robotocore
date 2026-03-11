"""Tests for state lifecycle hooks."""

import logging

import pytest

from robotocore.state.hooks import (
    HookType,
    StateHookRegistry,
    state_hooks,
)


class TestHookRegistration:
    def test_register_hook(self):
        """Register a callback and verify it's stored."""
        registry = StateHookRegistry()
        calls = []
        registry.register(HookType.BEFORE_SAVE, lambda ctx: calls.append("called"))
        # Trigger to verify
        registry.fire(HookType.BEFORE_SAVE, {})
        assert calls == ["called"]

    def test_unregister_hook(self):
        """Unregister a callback and verify it's no longer called."""
        registry = StateHookRegistry()
        calls = []

        def my_hook(ctx):
            calls.append("called")

        registry.register(HookType.BEFORE_SAVE, my_hook)
        registry.unregister(HookType.BEFORE_SAVE, my_hook)
        registry.fire(HookType.BEFORE_SAVE, {})
        assert calls == []

    def test_unregistered_hook_not_called(self):
        """A hook that was never registered should not fire."""
        registry = StateHookRegistry()
        calls = []
        registry.fire(HookType.BEFORE_SAVE, {})
        assert calls == []

    def test_registry_independent_per_hook_type(self):
        """Hooks registered for one type don't fire for another."""
        registry = StateHookRegistry()
        calls = []

        registry.register(HookType.BEFORE_SAVE, lambda ctx: calls.append("save"))
        registry.fire(HookType.BEFORE_LOAD, {})
        assert calls == []
        registry.fire(HookType.BEFORE_SAVE, {})
        assert calls == ["save"]

    def test_global_singleton_works(self):
        """The global state_hooks singleton should be a StateHookRegistry."""
        assert isinstance(state_hooks, StateHookRegistry)


class TestHookExecution:
    def test_multiple_hooks_called_in_order(self):
        """Multiple hooks for same type fire in registration order."""
        registry = StateHookRegistry()
        calls = []

        registry.register(HookType.BEFORE_SAVE, lambda ctx: calls.append("first"))
        registry.register(HookType.BEFORE_SAVE, lambda ctx: calls.append("second"))
        registry.register(HookType.BEFORE_SAVE, lambda ctx: calls.append("third"))
        registry.fire(HookType.BEFORE_SAVE, {})
        assert calls == ["first", "second", "third"]

    def test_before_hook_exception_aborts(self):
        """If a before hook raises, fire() should propagate the exception."""
        registry = StateHookRegistry()

        def bad_hook(ctx):
            raise ValueError("abort!")

        registry.register(HookType.BEFORE_SAVE, bad_hook)
        with pytest.raises(ValueError, match="abort!"):
            registry.fire(HookType.BEFORE_SAVE, {})

    def test_after_hook_exception_logs_warning(self, caplog):
        """If an after hook raises, it should log a warning but not propagate."""
        registry = StateHookRegistry()

        def bad_hook(ctx):
            raise RuntimeError("oops")

        registry.register(HookType.AFTER_SAVE, bad_hook)
        with caplog.at_level(logging.WARNING):
            registry.fire(HookType.AFTER_SAVE, {})
        assert "oops" in caplog.text

    def test_hook_receives_context(self):
        """Hooks should receive the context dict passed to fire()."""
        registry = StateHookRegistry()
        received = []

        registry.register(HookType.BEFORE_SAVE, lambda ctx: received.append(ctx))
        context = {"services": ["s3"], "snapshot_name": "test"}
        registry.fire(HookType.BEFORE_SAVE, context)
        assert received == [context]

    def test_hook_with_services_filter(self):
        """Hook receives services list from context."""
        registry = StateHookRegistry()
        received = []

        registry.register(HookType.BEFORE_SAVE, lambda ctx: received.append(ctx))
        registry.fire(HookType.BEFORE_SAVE, {"services": ["s3", "dynamodb"]})
        assert received[0]["services"] == ["s3", "dynamodb"]


class TestHookTypeCoverage:
    """Verify all six hook types fire correctly."""

    def test_before_save_fires(self):
        registry = StateHookRegistry()
        calls = []
        registry.register(HookType.BEFORE_SAVE, lambda ctx: calls.append("before_save"))
        registry.fire(HookType.BEFORE_SAVE, {})
        assert calls == ["before_save"]

    def test_after_save_fires(self):
        registry = StateHookRegistry()
        calls = []
        registry.register(HookType.AFTER_SAVE, lambda ctx: calls.append("after_save"))
        registry.fire(HookType.AFTER_SAVE, {})
        assert calls == ["after_save"]

    def test_before_load_fires(self):
        registry = StateHookRegistry()
        calls = []
        registry.register(HookType.BEFORE_LOAD, lambda ctx: calls.append("before_load"))
        registry.fire(HookType.BEFORE_LOAD, {})
        assert calls == ["before_load"]

    def test_after_load_fires(self):
        registry = StateHookRegistry()
        calls = []
        registry.register(HookType.AFTER_LOAD, lambda ctx: calls.append("after_load"))
        registry.fire(HookType.AFTER_LOAD, {})
        assert calls == ["after_load"]

    def test_before_reset_fires(self):
        registry = StateHookRegistry()
        calls = []
        registry.register(HookType.BEFORE_RESET, lambda ctx: calls.append("before_reset"))
        registry.fire(HookType.BEFORE_RESET, {})
        assert calls == ["before_reset"]

    def test_after_reset_fires(self):
        registry = StateHookRegistry()
        calls = []
        registry.register(HookType.AFTER_RESET, lambda ctx: calls.append("after_reset"))
        registry.fire(HookType.AFTER_RESET, {})
        assert calls == ["after_reset"]


class TestListHooks:
    def test_list_hooks_empty(self):
        registry = StateHookRegistry()
        result = registry.list_hooks()
        assert result == {}

    def test_list_hooks_with_registrations(self):
        registry = StateHookRegistry()

        def my_hook(ctx):
            pass

        registry.register(HookType.BEFORE_SAVE, my_hook)
        result = registry.list_hooks()
        assert HookType.BEFORE_SAVE.value in result
        assert len(result[HookType.BEFORE_SAVE.value]) == 1
        assert "my_hook" in result[HookType.BEFORE_SAVE.value][0]
