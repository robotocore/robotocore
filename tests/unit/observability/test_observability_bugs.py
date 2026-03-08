"""Failing tests that expose correctness bugs in the observability stack.

Each test documents a specific bug and fails against the current implementation.
"""

import stat
import threading
import time
from unittest.mock import patch

from robotocore.observability.hooks import run_init_hooks
from robotocore.observability.metrics import RequestCounter


class TestMetricsUptimeRaceCondition:
    """Bug: uptime_seconds reads _start_time without holding the lock.

    The reset() method writes _start_time under lock, but the uptime_seconds
    property reads it without the lock. This is a data race: a concurrent
    reset() could update _start_time between the read of _start_time and the
    subtraction in uptime_seconds, potentially yielding a negative value.
    """

    def test_uptime_not_negative_during_concurrent_reset(self):
        """uptime_seconds should never be negative, even during concurrent resets."""
        counter = RequestCounter()
        negative_seen = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                val = counter.uptime_seconds
                if val < 0:
                    negative_seen.append(val)

        def resetter():
            while not stop.is_set():
                counter.reset()

        threads = [threading.Thread(target=reader) for _ in range(4)]
        threads += [threading.Thread(target=resetter) for _ in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.2)
        stop.set()
        for t in threads:
            t.join()

        # The property should be protected by the lock so negative values
        # are impossible. If this assertion fails, it means uptime_seconds
        # can go negative due to the race.
        #
        # NOTE: On CPython with the GIL, the race is hard to trigger in
        # practice, but the code is still incorrect. We test the structural
        # issue instead: uptime_seconds MUST acquire the lock.
        import inspect

        source = inspect.getsource(RequestCounter.uptime_seconds.fget)
        assert "_lock" in source, (
            "uptime_seconds reads _start_time without acquiring _lock. "
            "This is a data race with reset() which writes _start_time under lock."
        )


class TestMetricsUnboundedGrowth:
    """Bug: _counts dict grows without bound as new service names are added.

    There is no cap on the number of unique keys in _counts. If malformed
    requests or bad routing produce unique service strings (e.g., from user
    input in URL paths), the dict grows unboundedly, leaking memory.

    A correct implementation would either cap the number of tracked services
    or provide a mechanism to evict stale entries.
    """

    def test_counts_dict_has_size_limit(self):
        """The counter should cap the number of tracked services to prevent memory leaks."""
        counter = RequestCounter()

        # Simulate 10,000 unique service names (e.g., from bad routing)
        for i in range(10_000):
            counter.increment(f"bogus-service-{i}")

        all_counts = counter.get_all()
        # A well-designed counter should cap at some reasonable limit
        # (e.g., 1000 services max) to prevent unbounded memory growth.
        assert len(all_counts) <= 1000, (
            f"_counts dict grew to {len(all_counts)} entries with no cap. "
            "Unique service names from bad routing will cause unbounded memory growth."
        )


class TestTracerInitRaceCondition:
    """Bug: _get_tracer() has a TOCTOU race on the global _tracer variable.

    Two threads can both see _tracer is None, both enter the initialization
    block, and both call trace.set_tracer_provider(). The second call either
    raises an error or silently overwrites the first provider, potentially
    losing spans.

    The initialization should use a lock or threading.once pattern.
    """

    def test_get_tracer_is_thread_safe(self):
        """_get_tracer() should use a lock to prevent double initialization."""
        import inspect

        from robotocore.observability import tracing

        source = inspect.getsource(tracing._get_tracer)
        # The function should use some form of synchronization
        has_lock = "Lock" in source or "_lock" in source or "threading" in source
        has_once = "once" in source.lower()
        assert has_lock or has_once, (
            "_get_tracer() checks and sets the global _tracer without any "
            "synchronization. Two threads can race past the 'if _tracer is not None' "
            "check and both initialize the tracer, causing duplicate TracerProviders."
        )


class TestHooksFailedScriptDoesNotStopChain:
    """Bug: A failed boot hook does not prevent subsequent hooks from running.

    In standard init.d semantics, if a critical boot script exits non-zero,
    subsequent scripts should NOT execute. The current implementation logs a
    warning but continues executing all remaining scripts.

    This means a hook that checks prerequisites (e.g., "is the database
    reachable?") can fail, but later hooks that depend on the database will
    still run and fail in unpredictable ways.
    """

    def test_failed_boot_hook_stops_subsequent_hooks(self, tmp_path):
        """If a boot hook exits non-zero, subsequent hooks should not run."""
        boot_dir = tmp_path / "boot.d"
        boot_dir.mkdir()

        # First hook fails
        script1 = boot_dir / "01_check_prereqs.sh"
        script1.write_text("#!/bin/bash\necho 'prereq check failed' >&2\nexit 1")
        script1.chmod(script1.stat().st_mode | stat.S_IEXEC)

        # Second hook depends on prereqs
        script2 = boot_dir / "02_setup.sh"
        script2.write_text("#!/bin/bash\necho 'setup running'")
        script2.chmod(script2.stat().st_mode | stat.S_IEXEC)

        with patch(
            "robotocore.observability.hooks.get_hook_base",
            return_value=str(tmp_path),
        ):
            results = run_init_hooks("boot")

        # The first hook failed, so the second should NOT have run
        assert len(results) == 1, (
            f"Expected 1 result (failed hook stops chain), got {len(results)}. "
            "A failed boot hook should halt execution of subsequent hooks, "
            "but the current implementation continues running all hooks."
        )


class TestHooksExceptionInOneDoesNotReportOthers:
    """Bug: When a hook raises an unexpected exception, the error result
    does not include the stage name, making it hard to diagnose which
    lifecycle stage had the failure.
    """

    def test_hook_error_result_includes_stage(self, tmp_path):
        """Error results from hooks should include the stage name for debugging."""
        boot_dir = tmp_path / "boot.d"
        boot_dir.mkdir()

        script = boot_dir / "01_explode.sh"
        script.write_text("#!/bin/bash\nexit 1")
        script.chmod(script.stat().st_mode | stat.S_IEXEC)

        with patch(
            "robotocore.observability.hooks.get_hook_base",
            return_value=str(tmp_path),
        ):
            results = run_init_hooks("boot")

        assert len(results) == 1
        # The result should include the stage for debugging
        assert "stage" in results[0], (
            "Hook results do not include the 'stage' field. "
            "When a hook fails, operators need to know which lifecycle stage "
            "(boot/ready/shutdown) it was in. Current results only have: "
            f"{list(results[0].keys())}"
        )
