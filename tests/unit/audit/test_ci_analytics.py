"""Unit tests for CI analytics session tracking."""

import json
import os
import tempfile
import time
from pathlib import Path
from unittest import mock

import pytest

from robotocore.audit.ci_analytics import (
    detect_ci_provider,
    get_ci_analytics,
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
