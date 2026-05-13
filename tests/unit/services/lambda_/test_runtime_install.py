"""Tests for the on-demand runtime fault-in framework."""

from __future__ import annotations

import threading

import pytest

from robotocore.services.lambda_.runtimes import install as install_mod
from robotocore.services.lambda_.runtimes.install import (
    _PLANS,
    InstallPlan,
    ensure_installed,
    get_plan,
    is_installed,
    list_plans,
)


@pytest.fixture
def isolated_cache(monkeypatch, tmp_path):
    """Redirect CACHE_DIR + WRAPPER_BIN_DIR to a per-test tmp tree."""
    cache = tmp_path / "runtimes"
    wrappers = tmp_path / "bin"
    cache.mkdir()
    wrappers.mkdir()
    monkeypatch.setattr(install_mod, "CACHE_DIR", cache)
    monkeypatch.setattr(install_mod, "WRAPPER_BIN_DIR", wrappers)
    monkeypatch.setattr(install_mod, "FAULTIN_DISABLED", False)
    yield cache, wrappers


class _RecordingPlan(InstallPlan):
    """Test double — counts install calls and writes a sentinel file."""

    def __init__(self, runtime, prefix):
        super().__init__(runtime=runtime, family="test", prefix=prefix, binary_name=runtime)
        self.installs = 0

    def install(self) -> None:
        self.installs += 1
        self._write_wrapper(f"#!/bin/sh\necho {self.runtime}\n")
        self._mark_installed()


class TestPlanRegistry:
    def test_real_plans_are_registered(self):
        plans = list_plans()
        assert "java17" in plans
        assert "ruby3.3" in plans
        assert "python3.10" in plans
        assert "nodejs20.x" in plans
        assert "dotnet8" in plans

    def test_get_plan_returns_none_for_unknown(self):
        assert get_plan("cobol42") is None

    def test_java8_al2_shares_install_with_java8(self):
        # Two runtime IDs, one underlying prefix → same install satisfies both.
        java8 = get_plan("java8")
        java8_al2 = get_plan("java8.al2")
        assert java8.prefix == java8_al2.prefix
        assert java8.binary_name == java8_al2.binary_name


class TestEnsureInstalled:
    def test_idempotent_returns_true_when_marker_present(self, isolated_cache, monkeypatch):
        cache, _ = isolated_cache
        plan = _RecordingPlan("test1", cache / "test1")
        monkeypatch.setitem(_PLANS, "test1", plan)
        plan._mark_installed()
        assert ensure_installed("test1") is True
        assert plan.installs == 0  # didn't re-install

    def test_install_runs_when_marker_missing(self, isolated_cache, monkeypatch):
        cache, wrappers = isolated_cache
        plan = _RecordingPlan("test2", cache / "test2")
        monkeypatch.setitem(_PLANS, "test2", plan)
        assert ensure_installed("test2") is True
        assert plan.installs == 1
        assert (cache / "test2" / ".installed").is_file()
        assert (wrappers / "test2").is_file()

    def test_unknown_runtime_returns_false(self, isolated_cache):
        assert ensure_installed("cobol42") is False

    def test_disabled_returns_false_without_running_install(self, isolated_cache, monkeypatch):
        cache, _ = isolated_cache
        plan = _RecordingPlan("test3", cache / "test3")
        monkeypatch.setitem(_PLANS, "test3", plan)
        monkeypatch.setattr(install_mod, "FAULTIN_DISABLED", True)
        assert ensure_installed("test3") is False
        assert plan.installs == 0

    def test_failed_install_returns_false_no_marker(self, isolated_cache, monkeypatch):
        cache, _ = isolated_cache

        class FailingPlan(InstallPlan):
            def install(self) -> None:
                raise RuntimeError("simulated fetch failure")

        plan = FailingPlan(
            runtime="test4",
            family="test",
            prefix=cache / "test4",
            binary_name="test4",
        )
        monkeypatch.setitem(_PLANS, "test4", plan)
        assert ensure_installed("test4") is False
        assert not (cache / "test4" / ".installed").exists()


class TestConcurrency:
    def test_concurrent_callers_install_once(self, isolated_cache, monkeypatch):
        """Two threads racing on the same runtime install should result in one fetch."""
        cache, _ = isolated_cache

        import time as _time

        class SlowPlan(InstallPlan):
            installs = 0

            def install(self) -> None:
                # Simulate a slow download. flock should serialize the two threads
                # so the second sees the .installed marker and skips.
                _time.sleep(0.05)
                type(self).installs += 1
                self._mark_installed()

        plan = SlowPlan(
            runtime="test_race",
            family="test",
            prefix=cache / "test_race",
            binary_name="test_race",
        )
        monkeypatch.setitem(_PLANS, "test_race", plan)

        results = []

        def _run():
            results.append(ensure_installed("test_race"))

        threads = [threading.Thread(target=_run) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(results)
        assert plan.installs == 1  # not 3


class TestIsInstalled:
    def test_false_for_fresh_plan(self, isolated_cache, monkeypatch):
        cache, _ = isolated_cache
        plan = _RecordingPlan("test5", cache / "test5")
        monkeypatch.setitem(_PLANS, "test5", plan)
        assert is_installed("test5") is False

    def test_true_after_marker_written(self, isolated_cache, monkeypatch):
        cache, _ = isolated_cache
        plan = _RecordingPlan("test6", cache / "test6")
        monkeypatch.setitem(_PLANS, "test6", plan)
        plan._mark_installed()
        assert is_installed("test6") is True

    def test_unknown_runtime_is_not_installed(self):
        assert is_installed("cobol42") is False
