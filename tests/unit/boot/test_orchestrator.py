"""Tests for the boot orchestrator -- dependency-aware startup with health checks."""

from __future__ import annotations

import asyncio

import pytest

from robotocore.boot.orchestrator import (
    BootOrchestrator,
    BootResult,
    CircularDependencyError,
    ComponentState,
    ComponentStatus,
    ServiceComponent,
    get_orchestrator,
    reset_orchestrator,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_component(
    name: str,
    depends_on: list[str] | None = None,
    required: bool = True,
    timeout: float = 2.0,
    start_fn=None,
    health_fn=None,
    stop_fn=None,
    fail_start: bool = False,
    health_delay: float = 0.0,
    never_healthy: bool = False,
) -> ServiceComponent:
    """Build a ServiceComponent with sensible test defaults."""
    _started = False
    _stopped = False

    def default_start():
        nonlocal _started
        if fail_start:
            raise RuntimeError(f"{name} failed to start")
        _started = True

    def default_health():
        if never_healthy:
            return False
        return _started

    def default_stop():
        nonlocal _stopped
        _stopped = True

    return ServiceComponent(
        name=name,
        depends_on=depends_on or [],
        start=start_fn or default_start,
        health_check=health_fn or default_health,
        stop=stop_fn or default_stop,
        required=required,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Dependency resolution
# ---------------------------------------------------------------------------


class TestDependencyResolution:
    def test_no_components(self):
        orch = BootOrchestrator()
        order = orch._resolve_order()
        assert order == []

    def test_single_component(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        assert orch._resolve_order() == ["a"]

    def test_linear_chain(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        orch.register(_make_component("b", depends_on=["a"]))
        orch.register(_make_component("c", depends_on=["b"]))
        order = orch._resolve_order()
        assert order == ["a", "b", "c"]

    def test_diamond_dependency(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        orch.register(_make_component("b", depends_on=["a"]))
        orch.register(_make_component("c", depends_on=["a"]))
        orch.register(_make_component("d", depends_on=["b", "c"]))
        order = orch._resolve_order()
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_independent_components_sorted_alphabetically(self):
        orch = BootOrchestrator()
        orch.register(_make_component("z"))
        orch.register(_make_component("a"))
        orch.register(_make_component("m"))
        order = orch._resolve_order()
        assert order == ["a", "m", "z"]

    def test_circular_dependency_raises(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", depends_on=["b"]))
        orch.register(_make_component("b", depends_on=["a"]))
        with pytest.raises(CircularDependencyError) as exc_info:
            orch._resolve_order()
        assert len(exc_info.value.cycle) >= 2

    def test_self_dependency_raises(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", depends_on=["a"]))
        with pytest.raises(CircularDependencyError):
            orch._resolve_order()

    def test_three_way_cycle(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", depends_on=["c"]))
        orch.register(_make_component("b", depends_on=["a"]))
        orch.register(_make_component("c", depends_on=["b"]))
        with pytest.raises(CircularDependencyError):
            orch._resolve_order()

    def test_unknown_dependency_raises(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", depends_on=["nonexistent"]))
        with pytest.raises(ValueError, match="unknown component 'nonexistent'"):
            orch._resolve_order()

    def test_multiple_roots(self):
        orch = BootOrchestrator()
        orch.register(_make_component("root1"))
        orch.register(_make_component("root2"))
        orch.register(_make_component("child", depends_on=["root1", "root2"]))
        order = orch._resolve_order()
        assert order.index("root1") < order.index("child")
        assert order.index("root2") < order.index("child")


# ---------------------------------------------------------------------------
# Boot sequence
# ---------------------------------------------------------------------------


class TestBoot:
    async def test_simple_boot_succeeds(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        result = await orch.boot()
        assert result.success is True
        assert result.failed == []
        assert orch._status["a"].state == ComponentState.HEALTHY

    async def test_boot_with_dependencies(self):
        order = []

        def make_start(name):
            def start():
                order.append(name)

            return start

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(
                name="a", start=make_start("a"), health_check=lambda: True, timeout=2.0
            )
        )
        orch.register(
            ServiceComponent(
                name="b",
                depends_on=["a"],
                start=make_start("b"),
                health_check=lambda: True,
                timeout=2.0,
            )
        )
        result = await orch.boot()
        assert result.success is True
        assert order == ["a", "b"]

    async def test_required_failure_aborts_dependents(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", fail_start=True, required=True))
        orch.register(_make_component("b", depends_on=["a"]))
        result = await orch.boot()
        assert result.success is False
        assert "a" in result.failed
        assert "b" in result.skipped
        assert orch._status["b"].state == ComponentState.SKIPPED

    async def test_optional_failure_does_not_abort_boot(self):
        orch = BootOrchestrator()
        orch.register(_make_component("core"))
        orch.register(
            _make_component("optional", depends_on=["core"], required=False, fail_start=True)
        )
        result = await orch.boot()
        # Boot still succeeds because the failed component is optional
        assert result.success is True
        assert "optional" in result.failed

    async def test_optional_failure_skips_dependents(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        orch.register(_make_component("opt", depends_on=["a"], required=False, fail_start=True))
        orch.register(_make_component("c", depends_on=["opt"]))
        result = await orch.boot()
        assert result.success is False  # "c" is required and was skipped
        assert "opt" in result.failed
        assert "c" in result.skipped

    async def test_boot_records_duration(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        result = await orch.boot()
        assert result.total_duration_ms >= 0
        assert orch._status["a"].boot_duration_ms is not None

    async def test_boot_only_once(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        await orch.boot()
        with pytest.raises(RuntimeError, match="already been called"):
            await orch.boot()

    async def test_boot_circular_dependency_returns_failure(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", depends_on=["b"]))
        orch.register(_make_component("b", depends_on=["a"]))
        result = await orch.boot()
        assert result.success is False

    async def test_async_start_function(self):
        started = False

        async def async_start():
            nonlocal started
            await asyncio.sleep(0)
            started = True

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(
                name="async_comp",
                start=async_start,
                health_check=lambda: started,
                timeout=2.0,
            )
        )
        result = await orch.boot()
        assert result.success is True
        assert started is True

    async def test_no_start_function(self):
        orch = BootOrchestrator()
        orch.register(ServiceComponent(name="noop", timeout=2.0))
        result = await orch.boot()
        assert result.success is True
        assert orch._status["noop"].state == ComponentState.HEALTHY


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------


class TestHealthChecks:
    async def test_no_health_check_means_instant_ready(self):
        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(name="a", start=lambda: None, health_check=None, timeout=2.0)
        )
        result = await orch.boot()
        assert result.success is True
        assert orch._status["a"].state == ComponentState.HEALTHY

    async def test_health_check_polled_until_true(self):
        call_count = 0

        def health():
            nonlocal call_count
            call_count += 1
            return call_count >= 3

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(name="a", start=lambda: None, health_check=health, timeout=5.0)
        )
        result = await orch.boot()
        assert result.success is True
        assert call_count >= 3

    async def test_health_check_timeout(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", never_healthy=True, timeout=0.1, required=True))
        result = await orch.boot()
        assert result.success is False
        assert "a" in result.failed
        assert "timed out" in (orch._status["a"].error or "")

    async def test_health_check_exception_retries(self):
        call_count = 0

        def health():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionRefusedError("not ready")
            return True

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(name="a", start=lambda: None, health_check=health, timeout=5.0)
        )
        result = await orch.boot()
        assert result.success is True
        assert call_count >= 3

    async def test_async_health_check(self):
        call_count = 0

        async def health():
            nonlocal call_count
            call_count += 1
            return call_count >= 2

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(name="a", start=lambda: None, health_check=health, timeout=5.0)
        )
        result = await orch.boot()
        assert result.success is True

    async def test_optional_health_timeout_doesnt_fail_boot(self):
        orch = BootOrchestrator()
        orch.register(_make_component("opt", never_healthy=True, timeout=0.1, required=False))
        result = await orch.boot()
        assert result.success is True
        assert "opt" in result.failed


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


class TestShutdown:
    async def test_shutdown_reverse_order(self):
        stop_order = []

        def make_stop(name):
            def stop():
                stop_order.append(name)

            return stop

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(
                name="a",
                start=lambda: None,
                health_check=lambda: True,
                stop=make_stop("a"),
                timeout=2.0,
            )
        )
        orch.register(
            ServiceComponent(
                name="b",
                depends_on=["a"],
                start=lambda: None,
                health_check=lambda: True,
                stop=make_stop("b"),
                timeout=2.0,
            )
        )
        orch.register(
            ServiceComponent(
                name="c",
                depends_on=["b"],
                start=lambda: None,
                health_check=lambda: True,
                stop=make_stop("c"),
                timeout=2.0,
            )
        )
        await orch.boot()
        await orch.shutdown()
        # c depends on b depends on a, so shutdown is c, b, a
        assert stop_order == ["c", "b", "a"]

    async def test_shutdown_only_healthy_components(self):
        stop_order = []

        def make_stop(name):
            def stop():
                stop_order.append(name)

            return stop

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(
                name="a",
                start=lambda: None,
                health_check=lambda: True,
                stop=make_stop("a"),
                timeout=2.0,
            )
        )
        orch.register(_make_component("b", depends_on=["a"], fail_start=True, required=False))
        await orch.boot()
        await orch.shutdown()
        # b failed, only a should be stopped
        assert stop_order == ["a"]

    async def test_shutdown_error_doesnt_halt(self):
        stop_order = []

        def bad_stop():
            raise RuntimeError("stop failed")

        def good_stop():
            stop_order.append("a")

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(
                name="a",
                start=lambda: None,
                health_check=lambda: True,
                stop=good_stop,
                timeout=2.0,
            )
        )
        orch.register(
            ServiceComponent(
                name="b",
                depends_on=["a"],
                start=lambda: None,
                health_check=lambda: True,
                stop=bad_stop,
                timeout=2.0,
            )
        )
        await orch.boot()
        await orch.shutdown()
        # b's stop failed, but a's stop should still run
        assert stop_order == ["a"]

    async def test_shutdown_async_stop(self):
        stopped = False

        async def async_stop():
            nonlocal stopped
            await asyncio.sleep(0)
            stopped = True

        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(
                name="a",
                start=lambda: None,
                health_check=lambda: True,
                stop=async_stop,
                timeout=2.0,
            )
        )
        await orch.boot()
        await orch.shutdown()
        assert stopped is True

    async def test_shutdown_before_boot_is_noop(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        await orch.shutdown()  # Should not raise

    async def test_shutdown_sets_stopped_state(self):
        orch = BootOrchestrator()
        orch.register(
            ServiceComponent(
                name="a",
                start=lambda: None,
                health_check=lambda: True,
                stop=lambda: None,
                timeout=2.0,
            )
        )
        await orch.boot()
        assert orch._status["a"].state == ComponentState.HEALTHY
        await orch.shutdown()
        assert orch._status["a"].state == ComponentState.STOPPED


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration:
    def test_duplicate_name_raises(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        with pytest.raises(ValueError, match="already registered"):
            orch.register(_make_component("a"))

    def test_register_many_components(self):
        orch = BootOrchestrator()
        for i in range(20):
            orch.register(_make_component(f"comp_{i}"))
        assert len(orch._components) == 20


# ---------------------------------------------------------------------------
# Status / get_status
# ---------------------------------------------------------------------------


class TestStatus:
    async def test_status_before_boot(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        status = orch.get_status()
        assert status["booted"] is False
        assert status["boot_result"] is None
        assert status["components"]["a"]["state"] == "pending"

    async def test_status_after_boot(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        await orch.boot()
        status = orch.get_status()
        assert status["booted"] is True
        assert status["boot_result"]["success"] is True
        assert status["components"]["a"]["state"] == "healthy"

    async def test_status_shows_failed(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", fail_start=True, required=False))
        await orch.boot()
        status = orch.get_status()
        assert status["components"]["a"]["state"] == "failed"
        assert "error" in status["components"]["a"]

    async def test_status_shows_skipped(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a", fail_start=True))
        orch.register(_make_component("b", depends_on=["a"]))
        await orch.boot()
        status = orch.get_status()
        assert status["components"]["b"]["state"] == "skipped"

    async def test_boot_duration_in_status(self):
        orch = BootOrchestrator()
        orch.register(_make_component("a"))
        await orch.boot()
        status = orch.get_status()
        assert "boot_duration_ms" in status["components"]["a"]


# ---------------------------------------------------------------------------
# BootResult
# ---------------------------------------------------------------------------


class TestBootResult:
    def test_to_dict(self):
        result = BootResult(
            success=True,
            total_duration_ms=42.5,
            failed=["x"],
            skipped=["y"],
        )
        d = result.to_dict()
        assert d["success"] is True
        assert d["total_duration_ms"] == 42.5
        assert d["failed"] == ["x"]
        assert d["skipped"] == ["y"]


# ---------------------------------------------------------------------------
# ComponentStatus
# ---------------------------------------------------------------------------


class TestComponentStatus:
    def test_boot_duration_ms(self):
        s = ComponentStatus(name="a", started_at=1.0, ready_at=1.05)
        assert s.boot_duration_ms == 50.0

    def test_boot_duration_ms_none(self):
        s = ComponentStatus(name="a")
        assert s.boot_duration_ms is None

    def test_to_dict(self):
        s = ComponentStatus(
            name="a",
            state=ComponentState.HEALTHY,
            started_at=1.0,
            ready_at=1.1,
            required=True,
        )
        d = s.to_dict()
        assert d["name"] == "a"
        assert d["state"] == "healthy"
        assert d["boot_duration_ms"] == 100.0


# ---------------------------------------------------------------------------
# Global orchestrator helpers
# ---------------------------------------------------------------------------


class TestGlobalOrchestrator:
    def test_get_orchestrator_singleton(self):
        reset_orchestrator()
        o1 = get_orchestrator()
        o2 = get_orchestrator()
        assert o1 is o2
        reset_orchestrator()

    def test_reset_orchestrator(self):
        reset_orchestrator()
        o1 = get_orchestrator()
        reset_orchestrator()
        o2 = get_orchestrator()
        assert o1 is not o2
        reset_orchestrator()
