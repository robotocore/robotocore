"""Integration tests for state lifecycle hooks wired into StateManager."""

import logging
import tempfile

import pytest

from robotocore.state.hooks import HookType, StateHookRegistry
from robotocore.state.manager import StateManager


class TestStateManagerHooksEndToEnd:
    def test_save_fires_before_and_after_hooks(self):
        """Register hooks, save state, verify both before and after fired with path."""
        registry = StateHookRegistry()
        calls = []

        registry.register(HookType.BEFORE_SAVE, lambda ctx: calls.append(("before", ctx)))
        registry.register(HookType.AFTER_SAVE, lambda ctx: calls.append(("after", ctx)))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir, hook_registry=registry)
            saved_path = manager.save(name="hook-test")

        assert len(calls) == 2
        assert calls[0][0] == "before"
        assert calls[1][0] == "after"
        assert calls[1][1]["snapshot_path"] == saved_path

    def test_before_save_exception_aborts_save(self):
        """If before_save hook raises, save should not proceed."""
        registry = StateHookRegistry()

        def abort_hook(ctx):
            raise ValueError("no saving allowed")

        registry.register(HookType.BEFORE_SAVE, abort_hook)

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir, hook_registry=registry)
            with pytest.raises(ValueError, match="no saving allowed"):
                manager.save(name="should-not-exist")

    def test_load_fires_before_and_after_hooks(self):
        """Register hooks, load state, verify both before and after fired."""
        registry = StateHookRegistry()
        calls = []

        registry.register(HookType.BEFORE_LOAD, lambda ctx: calls.append(("before", ctx)))
        registry.register(HookType.AFTER_LOAD, lambda ctx: calls.append(("after", ctx)))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir, hook_registry=registry)
            # Save first so there's something to load
            manager.save(name="load-test")
            calls.clear()
            manager.load(name="load-test")

        assert len(calls) == 2
        assert calls[0][0] == "before"
        assert calls[1][0] == "after"

    def test_reset_fires_before_and_after_hooks(self):
        """Register hooks, reset state, verify hooks fire."""
        registry = StateHookRegistry()
        calls = []

        registry.register(HookType.BEFORE_RESET, lambda ctx: calls.append("before"))
        registry.register(HookType.AFTER_RESET, lambda ctx: calls.append("after"))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir, hook_registry=registry)
            manager.reset()

        assert calls == ["before", "after"]

    def test_selective_save_passes_services_to_hooks(self):
        """When saving with services filter, hooks receive the service list."""
        registry = StateHookRegistry()
        received = []

        registry.register(HookType.BEFORE_SAVE, lambda ctx: received.append(ctx))

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir, hook_registry=registry)
            manager.save(services=["s3", "dynamodb"])

        assert received[0]["services"] == ["s3", "dynamodb"]

    def test_plugin_like_hook_registration(self):
        """Simulate plugin registering a hook and verify it fires."""
        registry = StateHookRegistry()
        plugin_calls = []

        # Simulating what a plugin would do
        class MyPlugin:
            def on_before_state_save(self, ctx):
                plugin_calls.append(f"plugin saw save for {ctx.get('services')}")

        plugin = MyPlugin()
        registry.register(HookType.BEFORE_SAVE, plugin.on_before_state_save)

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir, hook_registry=registry)
            manager.save(services=["sqs"])

        assert len(plugin_calls) == 1
        assert "sqs" in plugin_calls[0]

    def test_after_hook_failure_does_not_fail_save(self, caplog):
        """After-save hook failure should log but not raise."""
        registry = StateHookRegistry()

        def bad_after_hook(ctx):
            raise RuntimeError("after hook boom")

        registry.register(HookType.AFTER_SAVE, bad_after_hook)

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = StateManager(state_dir=tmpdir, hook_registry=registry)
            with caplog.at_level(logging.WARNING):
                saved_path = manager.save(name="still-saves")
            # Save should have succeeded despite the hook error
            assert "still-saves" in saved_path
            assert "after hook boom" in caplog.text


class TestHooksManagementEndpoint:
    def test_list_hooks_endpoint(self):
        """The management endpoint should return registered hooks."""
        from robotocore.state.hooks import state_hooks

        def temp_hook(ctx):
            pass

        state_hooks.register(HookType.BEFORE_SAVE, temp_hook)
        try:
            result = state_hooks.list_hooks()
            assert HookType.BEFORE_SAVE.value in result
            hook_names = result[HookType.BEFORE_SAVE.value]
            assert any("temp_hook" in name for name in hook_names)
        finally:
            state_hooks.unregister(HookType.BEFORE_SAVE, temp_hook)
