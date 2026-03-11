"""Unit tests for CI analytics session tracking."""

import json
import os
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

from robotocore.audit.ci_analytics import (
    CIAnalytics,
    CISession,
    clear_sessions,
    compute_aggregate_summary,
    detect_ci_provider,
    get_ci_analytics,
    get_session_detail,
    list_sessions,
    reset_ci_analytics,
)


@pytest.fixture(autouse=True)
def _reset_analytics():
    """Reset the CI analytics singleton between tests."""
    reset_ci_analytics()
    yield
    reset_ci_analytics()


# ---------------------------------------------------------------------------
# CI provider detection
# ---------------------------------------------------------------------------


class TestDetectCIProvider:
    def test_github_actions(self):
        with mock.patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}, clear=False):
            name, build_id = detect_ci_provider()
            assert name == "github_actions"

    def test_gitlab_ci(self):
        env = {"GITLAB_CI": "true", "CI_JOB_ID": "12345"}
        with mock.patch.dict(os.environ, env, clear=False):
            name, build_id = detect_ci_provider()
            assert name == "gitlab_ci"
            assert build_id == "12345"

    def test_jenkins(self):
        env = {"JENKINS_URL": "http://ci.example.com", "BUILD_NUMBER": "42"}
        with mock.patch.dict(os.environ, env, clear=False):
            name, build_id = detect_ci_provider()
            assert name == "jenkins"
            assert build_id == "42"

    def test_circleci(self):
        env = {"CIRCLECI": "true", "CIRCLE_BUILD_NUM": "99"}
        with mock.patch.dict(os.environ, env, clear=False):
            name, build_id = detect_ci_provider()
            assert name == "circleci"
            assert build_id == "99"

    def test_generic_ci(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            name, build_id = detect_ci_provider()
            assert name == "generic_ci"

    def test_no_ci(self):
        env_clear = {
            "CI": "",
            "GITHUB_ACTIONS": "",
            "GITLAB_CI": "",
            "JENKINS_URL": "",
            "CIRCLECI": "",
            "ROBOTOCORE_CI_SESSION": "",
        }
        with mock.patch.dict(os.environ, env_clear, clear=False):
            name, build_id = detect_ci_provider()
            assert name is None


# ---------------------------------------------------------------------------
# Session ID handling
# ---------------------------------------------------------------------------


class TestSessionID:
    def test_session_id_from_env(self):
        with mock.patch.dict(os.environ, {"ROBOTOCORE_CI_SESSION": "my-session-123"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            assert analytics.session.session_id == "my-session-123"

    def test_auto_generated_session_id(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            assert analytics.session.session_id  # non-empty
            assert len(analytics.session.session_id) > 8

    def test_disabled_when_no_ci_and_not_forced(self):
        env_clear = {
            "CI": "",
            "GITHUB_ACTIONS": "",
            "GITLAB_CI": "",
            "JENKINS_URL": "",
            "CIRCLECI": "",
            "ROBOTOCORE_CI_SESSION": "",
        }
        with mock.patch.dict(os.environ, env_clear, clear=False):
            analytics = get_ci_analytics(force_enable=False)
            assert analytics is None


# ---------------------------------------------------------------------------
# Request recording
# ---------------------------------------------------------------------------


class TestRecordRequest:
    def test_record_increments_total_requests(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="s3", operation="PutObject", success=True)
            analytics.record_request(service="s3", operation="GetObject", success=True)
            assert analytics.session.total_requests == 2

    def test_record_error_increments_error_count(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="s3", operation="PutObject", success=False)
            assert analytics.session.error_count == 1
            assert analytics.session.total_requests == 1

    def test_per_service_request_counting(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="s3", operation="PutObject", success=True)
            analytics.record_request(service="s3", operation="GetObject", success=True)
            analytics.record_request(service="dynamodb", operation="PutItem", success=True)
            assert analytics.session.service_counts["s3"] == 2
            assert analytics.session.service_counts["dynamodb"] == 1

    def test_per_operation_success_failure(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="s3", operation="PutObject", success=True)
            analytics.record_request(service="s3", operation="PutObject", success=True)
            analytics.record_request(service="s3", operation="PutObject", success=False)
            op_stats = analytics.session.operation_stats["s3:PutObject"]
            assert op_stats["success"] == 2
            assert op_stats["failure"] == 1

    def test_services_used_tracking(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="s3", operation="PutObject", success=True)
            analytics.record_request(service="dynamodb", operation="PutItem", success=True)
            analytics.record_request(service="sqs", operation="SendMessage", success=True)
            assert analytics.session.services_used == {"s3", "dynamodb", "sqs"}


# ---------------------------------------------------------------------------
# Session timing
# ---------------------------------------------------------------------------


class TestSessionTiming:
    def test_session_has_start_time(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            assert analytics.session.start_time > 0

    def test_end_session_sets_end_time(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.end_session()
            assert analytics.session.end_time > 0
            assert analytics.session.end_time >= analytics.session.start_time

    def test_session_duration(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.session.start_time = 1000.0
            analytics.session.end_time = 1005.5
            assert analytics.session.duration == 5.5

    def test_session_duration_none_when_not_ended(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            # Duration uses current time if not ended
            assert analytics.session.duration >= 0


# ---------------------------------------------------------------------------
# Session summary serialization
# ---------------------------------------------------------------------------


class TestSessionSummary:
    def test_to_dict(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="s3", operation="PutObject", success=True)
            analytics.end_session()
            d = analytics.session.to_dict()
            assert "session_id" in d
            assert "start_time" in d
            assert "end_time" in d
            assert "duration" in d
            assert "total_requests" in d
            assert "error_count" in d
            assert "services_used" in d
            assert "service_counts" in d
            assert "operation_stats" in d
            assert d["total_requests"] == 1
            assert "s3" in d["services_used"]

    def test_json_serializable(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="sqs", operation="SendMessage", success=True)
            analytics.end_session()
            d = analytics.session.to_dict()
            result = json.dumps(d)
            assert isinstance(result, str)
            parsed = json.loads(result)
            assert parsed["total_requests"] == 1


# ---------------------------------------------------------------------------
# File persistence
# ---------------------------------------------------------------------------


class TestSessionPersistence:
    def test_save_session_creates_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
                analytics = get_ci_analytics(force_enable=True)
                analytics.record_request(service="s3", operation="PutObject", success=True)
                analytics.end_session()
                analytics.save_session(Path(tmpdir))
                files = list(Path(tmpdir).glob("*.json"))
                assert len(files) == 1
                data = json.loads(files[0].read_text())
                assert data["total_requests"] == 1

    def test_list_sessions_reads_from_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            # Write two session files
            for i in range(2):
                session_data = {
                    "session_id": f"sess-{i}",
                    "start_time": time.time() - 100 + i,
                    "end_time": time.time() - 50 + i,
                    "duration": 50.0,
                    "total_requests": i + 1,
                    "error_count": 0,
                    "services_used": ["s3"],
                    "service_counts": {"s3": i + 1},
                    "operation_stats": {},
                    "ci_provider": "generic_ci",
                    "build_id": None,
                }
                (state_dir / f"session-{i}.json").write_text(json.dumps(session_data))

            from robotocore.audit.ci_analytics import list_sessions

            sessions = list_sessions(state_dir)
            assert len(sessions) == 2

    def test_get_session_detail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            session_data = {
                "session_id": "test-sess-1",
                "start_time": time.time(),
                "end_time": time.time(),
                "duration": 10.0,
                "total_requests": 5,
                "error_count": 1,
                "services_used": ["s3", "dynamodb"],
                "service_counts": {"s3": 3, "dynamodb": 2},
                "operation_stats": {},
                "ci_provider": "github_actions",
                "build_id": "123",
            }
            (state_dir / "session-test-sess-1.json").write_text(json.dumps(session_data))

            from robotocore.audit.ci_analytics import get_session_detail

            detail = get_session_detail(state_dir, "test-sess-1")
            assert detail is not None
            assert detail["session_id"] == "test-sess-1"
            assert detail["total_requests"] == 5

    def test_get_session_detail_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from robotocore.audit.ci_analytics import get_session_detail

            detail = get_session_detail(Path(tmpdir), "nonexistent")
            assert detail is None

    def test_clear_sessions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            (state_dir / "session-a.json").write_text("{}")
            (state_dir / "session-b.json").write_text("{}")

            from robotocore.audit.ci_analytics import clear_sessions

            count = clear_sessions(state_dir)
            assert count == 2
            assert len(list(state_dir.glob("*.json"))) == 0


# ---------------------------------------------------------------------------
# Aggregate summary
# ---------------------------------------------------------------------------


class TestAggregateSummary:
    def test_aggregate_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            for i in range(3):
                session_data = {
                    "session_id": f"sess-{i}",
                    "start_time": time.time() - 100,
                    "end_time": time.time() - 50,
                    "duration": 50.0,
                    "total_requests": 10,
                    "error_count": i,  # 0, 1, 2 errors
                    "services_used": ["s3", "dynamodb"] if i < 2 else ["sqs"],
                    "service_counts": {"s3": 5, "dynamodb": 5} if i < 2 else {"sqs": 10},
                    "operation_stats": {"s3:PutObject": {"success": 5, "failure": i}}
                    if i < 2
                    else {"sqs:SendMessage": {"success": 10, "failure": 0}},
                    "ci_provider": "github_actions",
                    "build_id": str(i),
                }
                (state_dir / f"session-{i}.json").write_text(json.dumps(session_data))

            from robotocore.audit.ci_analytics import compute_aggregate_summary

            summary = compute_aggregate_summary(state_dir)
            assert summary["total_sessions"] == 3
            assert summary["avg_duration"] == 50.0
            assert "s3" in summary["most_used_services"]
            assert summary["zero_error_session_rate"] == pytest.approx(1 / 3)

    def test_aggregate_summary_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            from robotocore.audit.ci_analytics import compute_aggregate_summary

            summary = compute_aggregate_summary(Path(tmpdir))
            assert summary["total_sessions"] == 0


# ---------------------------------------------------------------------------
# Reliability tracking
# ---------------------------------------------------------------------------


class TestReliabilityTracking:
    def test_operation_failure_tracking(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            session_data = {
                "session_id": "sess-rel",
                "start_time": time.time(),
                "end_time": time.time(),
                "duration": 10.0,
                "total_requests": 10,
                "error_count": 3,
                "services_used": ["s3"],
                "service_counts": {"s3": 10},
                "operation_stats": {
                    "s3:PutObject": {"success": 7, "failure": 3},
                    "s3:GetObject": {"success": 10, "failure": 0},
                },
                "ci_provider": "generic_ci",
                "build_id": None,
            }
            (state_dir / "session-rel.json").write_text(json.dumps(session_data))

            from robotocore.audit.ci_analytics import compute_aggregate_summary

            summary = compute_aggregate_summary(state_dir)
            reliability = summary["service_reliability"]
            assert "s3" in reliability
            # s3 had 17 successes, 3 failures -> 17/20 = 85%
            assert reliability["s3"] == pytest.approx(17 / 20)

    def test_most_failing_operations(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            session_data = {
                "session_id": "sess-fail",
                "start_time": time.time(),
                "end_time": time.time(),
                "duration": 10.0,
                "total_requests": 10,
                "error_count": 5,
                "services_used": ["s3", "dynamodb"],
                "service_counts": {"s3": 7, "dynamodb": 3},
                "operation_stats": {
                    "s3:PutObject": {"success": 2, "failure": 5},
                    "dynamodb:PutItem": {"success": 2, "failure": 1},
                },
                "ci_provider": "generic_ci",
                "build_id": None,
            }
            (state_dir / "session-fail.json").write_text(json.dumps(session_data))

            from robotocore.audit.ci_analytics import compute_aggregate_summary

            summary = compute_aggregate_summary(state_dir)
            failing = summary["most_failing_operations"]
            # s3:PutObject should be first (most failures)
            assert len(failing) > 0
            assert failing[0]["operation"] == "s3:PutObject"
            assert failing[0]["failures"] == 5


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_record_requests(self):
        """Multiple threads recording concurrently should not lose counts."""
        session = CISession(session_id="thread-test")
        analytics = CIAnalytics(session=session)

        def record_n(n: int):
            for _ in range(n):
                analytics.record_request(service="s3", operation="PutObject", success=True)

        threads = [threading.Thread(target=record_n, args=(100,)) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert analytics.session.total_requests == 1000
        assert analytics.session.service_counts["s3"] == 1000
        assert analytics.session.operation_stats["s3:PutObject"]["success"] == 1000

    def test_concurrent_mixed_success_failure(self):
        """Concurrent success and failure recording keeps accurate counts."""
        session = CISession(session_id="mixed-thread")
        analytics = CIAnalytics(session=session)

        def record_successes():
            for _ in range(50):
                analytics.record_request(service="sqs", operation="Send", success=True)

        def record_failures():
            for _ in range(30):
                analytics.record_request(service="sqs", operation="Send", success=False)

        t1 = threading.Thread(target=record_successes)
        t2 = threading.Thread(target=record_failures)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert analytics.session.total_requests == 80
        assert analytics.session.error_count == 30
        assert analytics.session.operation_stats["sqs:Send"]["success"] == 50
        assert analytics.session.operation_stats["sqs:Send"]["failure"] == 30


# ---------------------------------------------------------------------------
# CISession direct tests
# ---------------------------------------------------------------------------


class TestCISessionDirect:
    def test_duration_uses_current_time_when_not_ended(self):
        session = CISession(session_id="dur-test")
        session.start_time = time.time() - 5.0
        # end_time is 0.0 (not ended), so duration should use current time
        assert session.duration >= 4.9  # at least ~5 seconds

    def test_duration_uses_end_time_when_ended(self):
        session = CISession(session_id="dur-ended")
        session.start_time = 1000.0
        session.end_time = 1042.5
        assert session.duration == 42.5

    def test_to_dict_services_used_sorted(self):
        session = CISession(session_id="sort-test")
        session.services_used = {"zebra", "alpha", "middle"}
        d = session.to_dict()
        assert d["services_used"] == ["alpha", "middle", "zebra"]

    def test_to_dict_includes_ci_provider_and_build_id(self):
        session = CISession(session_id="meta-test", ci_provider="github_actions", build_id="run-99")
        d = session.to_dict()
        assert d["ci_provider"] == "github_actions"
        assert d["build_id"] == "run-99"

    def test_to_dict_defaults(self):
        session = CISession(session_id="defaults")
        d = session.to_dict()
        assert d["total_requests"] == 0
        assert d["error_count"] == 0
        assert d["services_used"] == []
        assert d["service_counts"] == {}
        assert d["operation_stats"] == {}
        assert d["ci_provider"] is None
        assert d["build_id"] is None

    def test_to_dict_operation_stats_serializable(self):
        """operation_stats uses defaultdict internally; to_dict must produce plain dict."""
        session = CISession(session_id="serial")
        # Access via defaultdict to create entries
        session.operation_stats["s3:Get"]["success"] += 1
        d = session.to_dict()
        result = json.dumps(d)
        parsed = json.loads(result)
        assert parsed["operation_stats"]["s3:Get"]["success"] == 1


# ---------------------------------------------------------------------------
# record_request edge cases
# ---------------------------------------------------------------------------


class TestRecordRequestEdgeCases:
    def test_record_without_operation(self):
        """When operation is None, no operation_stats entry is created."""
        session = CISession(session_id="no-op")
        analytics = CIAnalytics(session=session)
        analytics.record_request(service="s3", operation=None, success=True)
        assert analytics.session.total_requests == 1
        assert analytics.session.services_used == {"s3"}
        assert analytics.session.service_counts["s3"] == 1
        assert len(analytics.session.operation_stats) == 0

    def test_record_multiple_services(self):
        session = CISession(session_id="multi-svc")
        analytics = CIAnalytics(session=session)
        for svc in ["s3", "dynamodb", "sqs", "lambda", "iam"]:
            analytics.record_request(service=svc, operation="Op", success=True)
        assert analytics.session.services_used == {"s3", "dynamodb", "sqs", "lambda", "iam"}
        assert analytics.session.total_requests == 5

    def test_success_does_not_increment_error_count(self):
        session = CISession(session_id="no-err")
        analytics = CIAnalytics(session=session)
        for _ in range(10):
            analytics.record_request(service="s3", operation="Put", success=True)
        assert analytics.session.error_count == 0
        assert analytics.session.total_requests == 10


# ---------------------------------------------------------------------------
# Singleton behavior
# ---------------------------------------------------------------------------


class TestSingletonBehavior:
    def test_get_ci_analytics_returns_same_instance(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            a1 = get_ci_analytics(force_enable=True)
            a2 = get_ci_analytics(force_enable=True)
            assert a1 is a2

    def test_reset_allows_new_instance(self):
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            a1 = get_ci_analytics(force_enable=True)
            reset_ci_analytics()
            a2 = get_ci_analytics(force_enable=True)
            assert a1 is not a2

    def test_none_cached_after_no_ci_detected(self):
        """Once None is returned for non-CI, it stays None without force_enable."""
        env_clear = {
            "CI": "",
            "GITHUB_ACTIONS": "",
            "GITLAB_CI": "",
            "JENKINS_URL": "",
            "CIRCLECI": "",
            "ROBOTOCORE_CI_SESSION": "",
        }
        with mock.patch.dict(os.environ, env_clear, clear=False):
            assert get_ci_analytics(force_enable=False) is None
            # Second call should also be None (cached)
            assert get_ci_analytics(force_enable=False) is None

    def test_force_enable_overrides_no_ci(self):
        env_clear = {
            "CI": "",
            "GITHUB_ACTIONS": "",
            "GITLAB_CI": "",
            "JENKINS_URL": "",
            "CIRCLECI": "",
            "ROBOTOCORE_CI_SESSION": "",
        }
        with mock.patch.dict(os.environ, env_clear, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            assert analytics is not None


# ---------------------------------------------------------------------------
# CI provider detection edge cases
# ---------------------------------------------------------------------------


class TestDetectCIProviderEdgeCases:
    def test_github_actions_takes_precedence_over_generic_ci(self):
        """GITHUB_ACTIONS is checked before CI=true."""
        env = {"GITHUB_ACTIONS": "true", "CI": "true", "GITHUB_RUN_ID": "42"}
        with mock.patch.dict(os.environ, env, clear=False):
            name, build_id = detect_ci_provider()
            assert name == "github_actions"
            assert build_id == "42"

    def test_github_actions_without_run_id(self):
        env = {"GITHUB_ACTIONS": "true"}
        with mock.patch.dict(os.environ, env, clear=False):
            name, build_id = detect_ci_provider()
            assert name == "github_actions"
            assert build_id is None

    def test_generic_ci_has_no_build_id(self):
        # Clear all specific providers first
        env = {
            "CI": "true",
            "GITHUB_ACTIONS": "",
            "GITLAB_CI": "",
            "JENKINS_URL": "",
            "CIRCLECI": "",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            name, build_id = detect_ci_provider()
            assert name == "generic_ci"
            assert build_id is None


# ---------------------------------------------------------------------------
# File persistence edge cases
# ---------------------------------------------------------------------------


class TestFilePersistenceEdgeCases:
    def test_save_session_creates_nested_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nested = Path(tmpdir) / "deep" / "nested" / "dir"
            session = CISession(session_id="nested-test")
            analytics = CIAnalytics(session=session)
            analytics.record_request(service="s3", operation="Put", success=True)
            analytics.end_session()
            path = analytics.save_session(nested)
            assert path.exists()
            data = json.loads(path.read_text())
            assert data["session_id"] == "nested-test"
            assert data["total_requests"] == 1

    def test_list_sessions_skips_corrupt_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            # Valid session
            valid = {"session_id": "good", "total_requests": 1}
            (state_dir / "session-good.json").write_text(json.dumps(valid))
            # Corrupt JSON
            (state_dir / "session-bad.json").write_text("{not valid json!!!")
            sessions = list_sessions(state_dir)
            assert len(sessions) == 1
            assert sessions[0]["session_id"] == "good"

    def test_list_sessions_nonexistent_dir(self):
        sessions = list_sessions(Path("/tmp/nonexistent_ci_analytics_dir_12345"))
        assert sessions == []

    def test_get_session_detail_corrupt_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            (state_dir / "session-corrupt.json").write_text("{{bad}}")
            detail = get_session_detail(state_dir, "corrupt")
            assert detail is None

    def test_clear_sessions_nonexistent_dir(self):
        count = clear_sessions(Path("/tmp/nonexistent_ci_analytics_dir_12345"))
        assert count == 0

    def test_clear_sessions_preserves_non_session_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            (state_dir / "session-a.json").write_text("{}")
            (state_dir / "other-file.txt").write_text("keep me")
            count = clear_sessions(state_dir)
            assert count == 1
            assert (state_dir / "other-file.txt").exists()

    def test_list_sessions_returns_sorted_by_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            for name in ["session-c.json", "session-a.json", "session-b.json"]:
                (state_dir / name).write_text(json.dumps({"session_id": name}))
            sessions = list_sessions(state_dir)
            assert len(sessions) == 3
            # sorted() on glob means alphabetical by filename
            assert sessions[0]["session_id"] == "session-a.json"
            assert sessions[1]["session_id"] == "session-b.json"
            assert sessions[2]["session_id"] == "session-c.json"

    def test_save_and_retrieve_roundtrip(self):
        """Full roundtrip: create session, record, save, retrieve, verify all fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            session = CISession(session_id="roundtrip", ci_provider="jenkins", build_id="77")
            analytics = CIAnalytics(session=session)
            analytics.record_request(service="s3", operation="Put", success=True)
            analytics.record_request(service="s3", operation="Put", success=False)
            analytics.record_request(service="dynamodb", operation="Get", success=True)
            analytics.end_session()
            analytics.save_session(state_dir)

            detail = get_session_detail(state_dir, "roundtrip")
            assert detail["session_id"] == "roundtrip"
            assert detail["ci_provider"] == "jenkins"
            assert detail["build_id"] == "77"
            assert detail["total_requests"] == 3
            assert detail["error_count"] == 1
            assert set(detail["services_used"]) == {"dynamodb", "s3"}
            assert detail["service_counts"]["s3"] == 2
            assert detail["service_counts"]["dynamodb"] == 1
            assert detail["operation_stats"]["s3:Put"]["success"] == 1
            assert detail["operation_stats"]["s3:Put"]["failure"] == 1
            assert detail["operation_stats"]["dynamodb:Get"]["success"] == 1


# ---------------------------------------------------------------------------
# Aggregate summary edge cases
# ---------------------------------------------------------------------------


class TestAggregateSummaryEdgeCases:
    def test_aggregate_nonexistent_dir(self):
        summary = compute_aggregate_summary(Path("/tmp/nonexistent_ci_agg_12345"))
        assert summary["total_sessions"] == 0
        assert summary["avg_duration"] == 0
        assert summary["most_used_services"] == []
        assert summary["zero_error_session_rate"] == 0
        assert summary["service_reliability"] == {}
        assert summary["most_failing_operations"] == []

    def test_most_used_services_capped_at_10(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            # Create a session with 15 different services
            svc_counts = {f"service-{i:02d}": 100 - i for i in range(15)}
            session_data = {
                "session_id": "many-svcs",
                "start_time": 1000.0,
                "end_time": 1010.0,
                "duration": 10.0,
                "total_requests": sum(svc_counts.values()),
                "error_count": 0,
                "services_used": list(svc_counts.keys()),
                "service_counts": svc_counts,
                "operation_stats": {},
                "ci_provider": "generic_ci",
                "build_id": None,
            }
            (state_dir / "session-many.json").write_text(json.dumps(session_data))
            summary = compute_aggregate_summary(state_dir)
            assert len(summary["most_used_services"]) == 10

    def test_all_sessions_zero_errors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            for i in range(5):
                data = {
                    "session_id": f"clean-{i}",
                    "start_time": 1000.0,
                    "end_time": 1010.0,
                    "duration": 10.0,
                    "total_requests": 5,
                    "error_count": 0,
                    "services_used": ["s3"],
                    "service_counts": {"s3": 5},
                    "operation_stats": {"s3:Put": {"success": 5, "failure": 0}},
                    "ci_provider": None,
                    "build_id": None,
                }
                (state_dir / f"session-clean-{i}.json").write_text(json.dumps(data))
            summary = compute_aggregate_summary(state_dir)
            assert summary["zero_error_session_rate"] == 1.0
            assert summary["most_failing_operations"] == []

    def test_service_reliability_100_percent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            data = {
                "session_id": "perfect",
                "start_time": 1000.0,
                "end_time": 1010.0,
                "duration": 10.0,
                "total_requests": 10,
                "error_count": 0,
                "services_used": ["s3"],
                "service_counts": {"s3": 10},
                "operation_stats": {
                    "s3:PutObject": {"success": 10, "failure": 0},
                },
                "ci_provider": None,
                "build_id": None,
            }
            (state_dir / "session-perf.json").write_text(json.dumps(data))
            summary = compute_aggregate_summary(state_dir)
            assert summary["service_reliability"]["s3"] == 1.0

    def test_service_reliability_multiple_sessions(self):
        """Reliability aggregates across sessions correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            # Session 1: s3 has 8 success, 2 failure
            data1 = {
                "session_id": "rel-1",
                "start_time": 1000.0,
                "end_time": 1010.0,
                "duration": 10.0,
                "total_requests": 10,
                "error_count": 2,
                "services_used": ["s3"],
                "service_counts": {"s3": 10},
                "operation_stats": {"s3:Put": {"success": 8, "failure": 2}},
                "ci_provider": None,
                "build_id": None,
            }
            # Session 2: s3 has 10 success, 0 failure
            data2 = {
                "session_id": "rel-2",
                "start_time": 1020.0,
                "end_time": 1030.0,
                "duration": 10.0,
                "total_requests": 10,
                "error_count": 0,
                "services_used": ["s3"],
                "service_counts": {"s3": 10},
                "operation_stats": {"s3:Put": {"success": 10, "failure": 0}},
                "ci_provider": None,
                "build_id": None,
            }
            (state_dir / "session-rel-1.json").write_text(json.dumps(data1))
            (state_dir / "session-rel-2.json").write_text(json.dumps(data2))
            summary = compute_aggregate_summary(state_dir)
            # s3: 18 success / 20 total = 0.9
            assert summary["service_reliability"]["s3"] == pytest.approx(0.9)

    def test_most_failing_ops_sorted_by_failure_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            data = {
                "session_id": "fail-sort",
                "start_time": 1000.0,
                "end_time": 1010.0,
                "duration": 10.0,
                "total_requests": 30,
                "error_count": 15,
                "services_used": ["s3", "dynamodb"],
                "service_counts": {"s3": 20, "dynamodb": 10},
                "operation_stats": {
                    "s3:Put": {"success": 5, "failure": 10},
                    "s3:Get": {"success": 3, "failure": 2},
                    "dynamodb:PutItem": {"success": 7, "failure": 3},
                },
                "ci_provider": None,
                "build_id": None,
            }
            (state_dir / "session-fs.json").write_text(json.dumps(data))
            summary = compute_aggregate_summary(state_dir)
            failing = summary["most_failing_operations"]
            assert len(failing) == 3
            assert failing[0]["operation"] == "s3:Put"
            assert failing[0]["failures"] == 10
            assert failing[0]["successes"] == 5
            assert failing[1]["operation"] == "dynamodb:PutItem"
            assert failing[1]["failures"] == 3
            assert failing[2]["operation"] == "s3:Get"
            assert failing[2]["failures"] == 2

    def test_aggregate_skips_sessions_without_operation_stats(self):
        """Sessions with empty operation_stats don't break aggregation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            data = {
                "session_id": "no-ops",
                "start_time": 1000.0,
                "end_time": 1010.0,
                "duration": 10.0,
                "total_requests": 5,
                "error_count": 0,
                "services_used": ["s3"],
                "service_counts": {"s3": 5},
                "operation_stats": {},
                "ci_provider": None,
                "build_id": None,
            }
            (state_dir / "session-noop.json").write_text(json.dumps(data))
            summary = compute_aggregate_summary(state_dir)
            assert summary["total_sessions"] == 1
            assert summary["service_reliability"] == {}
            assert summary["most_failing_operations"] == []
