"""Tests for build log streaming via CloudWatch Logs."""

import time

from .models import BuildLog


class TestBuildLogging:
    """CloudWatch Logs-based build log streaming."""

    def test_stream_build_logs(self, pipeline):
        build = pipeline.queue_build(repo="org/log-app", branch="main", commit_sha="log111")
        now = int(time.time() * 1000)
        logs = [
            BuildLog(build.build_id, now, "INFO", "Compiling source"),
            BuildLog(build.build_id, now + 1000, "INFO", "Running tests"),
            BuildLog(build.build_id, now + 2000, "INFO", "Build complete"),
        ]
        pipeline.write_build_logs("org/log-app", build.build_id, logs)

        events = pipeline.get_build_logs("org/log-app", build.build_id)
        assert len(events) >= 3
        messages = [e["message"] for e in events]
        assert any("Compiling source" in m for m in messages)
        assert any("Build complete" in m for m in messages)

        pipeline.cleanup_log_group("org/log-app")

    def test_retrieve_logs_for_specific_build(self, pipeline):
        b1 = pipeline.queue_build(repo="org/multi-log", branch="main", commit_sha="ml1")
        b2 = pipeline.queue_build(repo="org/multi-log", branch="main", commit_sha="ml2")
        now = int(time.time() * 1000)
        pipeline.write_build_logs(
            "org/multi-log",
            b1.build_id,
            [BuildLog(b1.build_id, now, "INFO", "Build 1 log")],
        )
        pipeline.write_build_logs(
            "org/multi-log",
            b2.build_id,
            [BuildLog(b2.build_id, now + 100, "INFO", "Build 2 log")],
        )

        events_b1 = pipeline.get_build_logs("org/multi-log", b1.build_id)
        events_b2 = pipeline.get_build_logs("org/multi-log", b2.build_id)

        b1_messages = [e["message"] for e in events_b1]
        b2_messages = [e["message"] for e in events_b2]
        assert any("Build 1 log" in m for m in b1_messages)
        assert any("Build 2 log" in m for m in b2_messages)
        # Each stream should only have its own logs
        assert not any("Build 2 log" in m for m in b1_messages)
        assert not any("Build 1 log" in m for m in b2_messages)

        pipeline.cleanup_log_group("org/multi-log")

    def test_separate_log_streams_per_build(self, pipeline):
        builds = []
        for i in range(3):
            b = pipeline.queue_build(repo="org/stream-app", branch="main", commit_sha=f"str{i}")
            builds.append(b)

        now = int(time.time() * 1000)
        for i, b in enumerate(builds):
            pipeline.write_build_logs(
                "org/stream-app",
                b.build_id,
                [BuildLog(b.build_id, now + i * 100, "INFO", f"Stream {i}")],
            )

        for i, b in enumerate(builds):
            events = pipeline.get_build_logs("org/stream-app", b.build_id)
            msgs = [e["message"] for e in events]
            assert any(f"Stream {i}" in m for m in msgs)

        pipeline.cleanup_log_group("org/stream-app")

    def test_filter_error_logs(self, pipeline):
        build = pipeline.queue_build(repo="org/filter-app", branch="main", commit_sha="flt")
        now = int(time.time() * 1000)
        logs = [
            BuildLog(build.build_id, now, "INFO", "Starting build"),
            BuildLog(build.build_id, now + 1000, "ERROR", "Compilation failed"),
            BuildLog(build.build_id, now + 2000, "INFO", "Retrying"),
            BuildLog(build.build_id, now + 3000, "ERROR", "Still failing"),
        ]
        pipeline.write_build_logs("org/filter-app", build.build_id, logs)

        error_events = pipeline.filter_build_logs("org/filter-app", filter_pattern="ERROR")
        assert len(error_events) >= 2
        assert all("ERROR" in e["message"] for e in error_events)

        pipeline.cleanup_log_group("org/filter-app")

    def test_build_queue_writes_initial_log(self, pipeline):
        """Queuing a build should automatically write an initial log entry."""
        build = pipeline.queue_build(repo="org/auto-log", branch="main", commit_sha="auto")
        events = pipeline.get_build_logs("org/auto-log", build.build_id)
        assert len(events) >= 1
        messages = [e["message"] for e in events]
        assert any("queued" in m.lower() for m in messages)

        pipeline.cleanup_log_group("org/auto-log")
