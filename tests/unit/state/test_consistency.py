"""Tests for state consistency guarantees.

Covers:
- ReadWriteLock: concurrent readers, exclusive writer, fair scheduling
- MutationVersionTracker: increment, reset, thread safety
- ConsistentStateManager: save, mutation pause, timeout, COW, status
- CopyOnWriteSnapshot: capture, isolation
"""

from __future__ import annotations

import asyncio
import threading
import time
from unittest.mock import MagicMock

import pytest

from robotocore.state.consistency import (
    ConsistencyStatus,
    ConsistentStateManager,
    CopyOnWriteSnapshot,
    MutationVersionTracker,
    SaveResult,
)
from robotocore.state.rwlock import ReadWriteLock

# -----------------------------------------------------------------------
# ReadWriteLock tests
# -----------------------------------------------------------------------


class TestReadWriteLock:
    """Tests for the async read-write lock."""

    async def test_single_reader(self):
        lock = ReadWriteLock()
        async with lock.read():
            assert lock.readers == 1
        assert lock.readers == 0

    async def test_multiple_concurrent_readers(self):
        lock = ReadWriteLock()
        max_readers = 0

        async def read_task():
            nonlocal max_readers
            async with lock.read():
                if lock.readers > max_readers:
                    max_readers = lock.readers
                await asyncio.sleep(0.05)

        tasks = [asyncio.create_task(read_task()) for _ in range(5)]
        await asyncio.gather(*tasks)
        assert max_readers >= 2, f"Expected concurrent readers, got max {max_readers}"
        assert lock.readers == 0

    async def test_writer_exclusive(self):
        lock = ReadWriteLock()
        async with lock.write():
            assert lock.writer_active
            assert lock.readers == 0
        assert not lock.writer_active

    async def test_writer_waits_for_readers(self):
        lock = ReadWriteLock()
        events: list[str] = []

        async def reader():
            async with lock.read():
                events.append("reader_acquired")
                await asyncio.sleep(0.1)
                events.append("reader_released")

        async def writer():
            await asyncio.sleep(0.02)  # Let reader start
            async with lock.write():
                events.append("writer_acquired")
                events.append("writer_released")

        await asyncio.gather(reader(), writer())
        # Writer must acquire after reader releases
        assert events.index("writer_acquired") > events.index("reader_released")

    async def test_readers_wait_for_writer(self):
        lock = ReadWriteLock()
        events: list[str] = []

        async def writer():
            async with lock.write():
                events.append("writer_acquired")
                await asyncio.sleep(0.1)
                events.append("writer_released")

        async def reader():
            await asyncio.sleep(0.02)  # Let writer start
            async with lock.read():
                events.append("reader_acquired")

        await asyncio.gather(writer(), reader())
        assert events.index("reader_acquired") > events.index("writer_released")

    async def test_fair_scheduling_writer_not_starved(self):
        """Writers should not starve: once a writer is waiting, new readers queue behind it."""
        lock = ReadWriteLock()
        events: list[str] = []

        async def initial_reader():
            async with lock.read():
                events.append("initial_reader_start")
                await asyncio.sleep(0.1)
                events.append("initial_reader_done")

        async def writer():
            await asyncio.sleep(0.02)  # Let initial reader start
            events.append("writer_waiting")
            async with lock.write():
                events.append("writer_acquired")
                await asyncio.sleep(0.02)
                events.append("writer_done")

        async def late_reader():
            await asyncio.sleep(0.04)  # Start after writer is waiting
            events.append("late_reader_waiting")
            async with lock.read():
                events.append("late_reader_acquired")

        await asyncio.gather(initial_reader(), writer(), late_reader())
        # Writer should acquire before the late reader
        assert events.index("writer_acquired") < events.index("late_reader_acquired")

    async def test_properties_initial_state(self):
        lock = ReadWriteLock()
        assert lock.readers == 0
        assert not lock.writer_active

    async def test_nested_reads_allowed(self):
        """Multiple reads from the same coroutine should not deadlock."""
        lock = ReadWriteLock()
        async with lock.read():
            assert lock.readers == 1

            # Second read from a different task
            async def inner_read():
                async with lock.read():
                    assert lock.readers == 2

            await inner_read()
        assert lock.readers == 0


# -----------------------------------------------------------------------
# MutationVersionTracker tests
# -----------------------------------------------------------------------


class TestMutationVersionTracker:
    def test_initial_version_is_zero(self):
        t = MutationVersionTracker()
        assert t.version == 0

    def test_increment(self):
        t = MutationVersionTracker()
        v = t.increment()
        assert v == 1
        assert t.version == 1

    def test_increment_multiple(self):
        t = MutationVersionTracker()
        for i in range(10):
            assert t.increment() == i + 1
        assert t.version == 10

    def test_reset(self):
        t = MutationVersionTracker()
        t.increment()
        t.increment()
        t.reset()
        assert t.version == 0

    def test_thread_safety(self):
        t = MutationVersionTracker()
        n_threads = 10
        n_increments = 100

        def worker():
            for _ in range(n_increments):
                t.increment()

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert t.version == n_threads * n_increments


# -----------------------------------------------------------------------
# CopyOnWriteSnapshot tests
# -----------------------------------------------------------------------


class TestCopyOnWriteSnapshot:
    def test_capture_empty(self):
        cow = CopyOnWriteSnapshot()
        result = cow.capture({})
        assert result == {}
        assert cow.data == {}

    def test_capture_native_state(self):
        state = {"key": "value", "count": 42}
        save_fn = MagicMock(return_value=state)
        handlers = {"my_service": (save_fn, MagicMock())}

        cow = CopyOnWriteSnapshot()
        result = cow.capture(handlers)
        assert "my_service" in result
        assert result["my_service"]["key"] == "value"
        save_fn.assert_called_once()

    def test_shallow_copy_isolation(self):
        """Mutations to the original dict should not affect the snapshot."""
        original = {"key": "value"}
        save_fn = MagicMock(return_value=original)
        handlers = {"svc": (save_fn, MagicMock())}

        cow = CopyOnWriteSnapshot()
        cow.capture(handlers)
        snapshot_data = cow.data

        # Mutate original
        original["key"] = "mutated"
        original["new_key"] = "new"

        # Shallow copy: top-level keys are independent
        assert snapshot_data["svc"]["key"] == "value"
        # But new_key won't be in the copy since it was added after
        assert "new_key" not in snapshot_data["svc"]

    def test_clear(self):
        cow = CopyOnWriteSnapshot()
        cow.capture({"svc": (lambda: {"a": 1}, MagicMock())})
        assert cow.data is not None
        cow.clear()
        assert cow.data is None

    def test_capture_handles_errors(self):
        """Errors in save_fn should be handled gracefully."""

        def bad_save():
            raise RuntimeError("save failed")

        handlers = {"bad_svc": (bad_save, MagicMock())}
        cow = CopyOnWriteSnapshot()
        result = cow.capture(handlers)
        assert "bad_svc" not in result

    def test_non_dict_state(self):
        """Non-dict state should be captured as-is."""
        save_fn = MagicMock(return_value=[1, 2, 3])
        handlers = {"list_svc": (save_fn, MagicMock())}

        cow = CopyOnWriteSnapshot()
        result = cow.capture(handlers)
        assert result["list_svc"] == [1, 2, 3]


# -----------------------------------------------------------------------
# ConsistentStateManager tests
# -----------------------------------------------------------------------


def _make_mock_manager(state_dir="/tmp/test-state"):
    """Create a mock StateManager for testing."""
    from robotocore.state.hooks import StateHookRegistry
    from robotocore.state.manager import StateManager

    mgr = StateManager(state_dir=state_dir, hook_registry=StateHookRegistry())
    return mgr


class TestConsistentStateManager:
    def test_init_requires_state_manager(self):
        with pytest.raises(TypeError, match="Expected StateManager"):
            ConsistentStateManager("not a manager")

    def test_record_mutation_increments_version(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        assert csm.mutation_version == 0
        csm.record_mutation("POST", "/")
        assert csm.mutation_version == 1

    def test_record_mutation_ignores_readonly(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        csm.record_mutation("GET", "/")
        csm.record_mutation("HEAD", "/")
        csm.record_mutation("OPTIONS", "/")
        assert csm.mutation_version == 0

    def test_record_mutation_ignores_management_endpoints(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        csm.record_mutation("POST", "/_robotocore/chaos/rules")
        assert csm.mutation_version == 0

    def test_record_mutation_returns_version(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        v = csm.record_mutation("PUT", "/some-bucket")
        assert v == 1

    async def test_save_acquires_write_lock(self, tmp_path):
        mgr = _make_mock_manager(state_dir=str(tmp_path))
        csm = ConsistentStateManager(mgr)

        result = await csm.save()
        assert isinstance(result, SaveResult)
        assert result.consistent
        assert result.version_start == 0
        assert result.version_end == 0
        assert result.duration_ms >= 0

    async def test_save_records_stats(self, tmp_path):
        mgr = _make_mock_manager(state_dir=str(tmp_path))
        csm = ConsistentStateManager(mgr)

        await csm.save()
        status = csm.status()
        assert status.saves_total == 1
        assert status.inconsistent_saves_total == 0
        assert status.last_save_consistent is True

    async def test_mutations_paused_during_save(self, tmp_path):
        mgr = _make_mock_manager(state_dir=str(tmp_path))
        csm = ConsistentStateManager(mgr)
        paused_during_save = []

        original_save = mgr.save

        def patched_save(**kwargs):
            paused_during_save.append(csm.mutations_paused)
            return original_save(**kwargs)

        mgr.save = patched_save
        await csm.save()
        assert paused_during_save == [True]
        assert not csm.mutations_paused

    async def test_on_request_readonly_passes_through(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        csm._mutations_paused = True
        csm._resume_event.clear()

        # Read-only should not block
        await asyncio.wait_for(csm.on_request("GET", "/"), timeout=0.5)

    async def test_on_request_management_passes_through(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        csm._mutations_paused = True
        csm._resume_event.clear()

        await asyncio.wait_for(csm.on_request("POST", "/_robotocore/state/save"), timeout=0.5)

    async def test_on_request_waits_when_paused(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        csm._mutations_paused = True
        csm._resume_event.clear()
        csm._pause_timeout = 0.2

        start = time.monotonic()
        await csm.on_request("POST", "/")
        elapsed = time.monotonic() - start
        # Should have waited ~0.2s (the timeout)
        assert elapsed >= 0.15

    async def test_on_request_resumes_when_unpaused(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        csm._mutations_paused = True
        csm._resume_event.clear()

        async def unpause_soon():
            await asyncio.sleep(0.05)
            csm._resume_mutations()

        task = asyncio.create_task(unpause_soon())
        start = time.monotonic()
        await csm.on_request("POST", "/")
        elapsed = time.monotonic() - start
        assert elapsed < 0.5  # Should have resumed quickly
        await task

    async def test_save_with_cow(self, tmp_path):
        mgr = _make_mock_manager(state_dir=str(tmp_path))
        csm = ConsistentStateManager(mgr)

        result = await csm.save_with_cow()
        assert isinstance(result, SaveResult)
        assert result.used_cow
        assert result.consistent

    async def test_status_dict(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)

        d = csm.status_dict()
        assert "mutation_version" in d
        assert "last_save" in d
        assert "locks" in d
        assert "stats" in d
        assert d["mutation_version"] == 0
        assert d["locks"]["mutations_paused"] is False
        assert d["stats"]["saves_total"] == 0

    async def test_concurrent_saves_serialized(self, tmp_path):
        """Two concurrent saves should be serialized by the write lock."""
        mgr = _make_mock_manager(state_dir=str(tmp_path))
        csm = ConsistentStateManager(mgr)
        save_times: list[tuple[float, float]] = []

        original_save = mgr.save

        def slow_save(**kwargs):
            start = time.monotonic()
            result = original_save(**kwargs)
            end = time.monotonic()
            save_times.append((start, end))
            return result

        mgr.save = slow_save

        await asyncio.gather(csm.save(), csm.save())
        assert len(save_times) == 2
        # Saves should not overlap
        times_sorted = sorted(save_times, key=lambda x: x[0])
        assert times_sorted[0][1] <= times_sorted[1][0] + 0.01  # small tolerance

    async def test_version_increments_tracked_across_saves(self, tmp_path):
        mgr = _make_mock_manager(state_dir=str(tmp_path))
        csm = ConsistentStateManager(mgr)

        csm.record_mutation("POST", "/")
        csm.record_mutation("PUT", "/bucket")
        assert csm.mutation_version == 2

        result = await csm.save()
        assert result.version_start == 2
        assert result.version_end == 2
        assert result.consistent

    def test_status_returns_correct_type(self):
        mgr = _make_mock_manager()
        csm = ConsistentStateManager(mgr)
        s = csm.status()
        assert isinstance(s, ConsistencyStatus)


# -----------------------------------------------------------------------
# SaveResult tests
# -----------------------------------------------------------------------


class TestSaveResult:
    def test_consistent(self):
        r = SaveResult(
            path="/tmp/x",
            version_start=5,
            version_end=5,
            consistent=True,
            duration_ms=10.0,
        )
        assert not r.potentially_inconsistent

    def test_inconsistent(self):
        r = SaveResult(
            path="/tmp/x",
            version_start=5,
            version_end=7,
            consistent=False,
            duration_ms=10.0,
        )
        assert r.potentially_inconsistent

    def test_default_cow(self):
        r = SaveResult(
            path="/tmp/x",
            version_start=0,
            version_end=0,
            consistent=True,
            duration_ms=0.0,
        )
        assert not r.used_cow


# -----------------------------------------------------------------------
# Integration-style tests
# -----------------------------------------------------------------------


class TestConsistencyIntegration:
    async def test_save_during_concurrent_mutations(self, tmp_path):
        """Simulate concurrent mutations during a save.

        The save should complete, and mutations that happen after the write lock
        is released should be reflected in the version counter.
        """
        mgr = _make_mock_manager(state_dir=str(tmp_path))
        csm = ConsistentStateManager(mgr)

        # Record some mutations before save
        for _ in range(5):
            csm.record_mutation("POST", "/")
        assert csm.mutation_version == 5

        result = await csm.save()
        assert result.consistent
        assert result.version_start == 5
        assert result.version_end == 5

        # Mutations after save
        csm.record_mutation("POST", "/")
        assert csm.mutation_version == 6

    async def test_multiple_saves_track_independent_versions(self, tmp_path):
        mgr = _make_mock_manager(state_dir=str(tmp_path))
        csm = ConsistentStateManager(mgr)

        r1 = await csm.save()
        csm.record_mutation("POST", "/")
        r2 = await csm.save()

        assert r1.version_start == 0
        assert r2.version_start == 1
        assert csm.status().saves_total == 2

    async def test_readers_and_writer_coexist(self):
        """Read locks and write locks coordinate correctly."""
        lock = ReadWriteLock()
        log: list[str] = []

        async def reader(name: str):
            async with lock.read():
                log.append(f"{name}_start")
                await asyncio.sleep(0.05)
                log.append(f"{name}_end")

        async def writer():
            await asyncio.sleep(0.02)
            async with lock.write():
                log.append("writer_start")
                await asyncio.sleep(0.05)
                log.append("writer_end")

        await asyncio.gather(reader("r1"), writer(), reader("r2"))
        # Writer should complete exclusively
        writer_start = log.index("writer_start")
        writer_end = log.index("writer_end")
        # No reader events between writer_start and writer_end
        between = log[writer_start : writer_end + 1]
        assert between == ["writer_start", "writer_end"]
