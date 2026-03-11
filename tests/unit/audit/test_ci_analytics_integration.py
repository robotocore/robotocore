"""End-to-end semantic tests for CI analytics."""

import json
import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from robotocore.audit.ci_analytics import (
    clear_sessions,
    compute_aggregate_summary,
    get_ci_analytics,
    get_session_detail,
    list_sessions,
    reset_ci_analytics,
)


@pytest.fixture(autouse=True)
def _reset_analytics():
    reset_ci_analytics()
    yield
    reset_ci_analytics()


@pytest.fixture
def state_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestEndToEndSessionLifecycle:
    def test_start_record_end_verify(self, state_dir: Path):
        """Full lifecycle: start session, record requests, end, verify summary."""
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            session_id = analytics.session.session_id

            # Record a mix of successes and failures
            analytics.record_request(service="s3", operation="PutObject", success=True)
            analytics.record_request(service="s3", operation="GetObject", success=True)
            analytics.record_request(service="s3", operation="DeleteObject", success=False)
            analytics.record_request(service="dynamodb", operation="PutItem", success=True)

            analytics.end_session()
            analytics.save_session(state_dir)

            # Verify persisted summary
            detail = get_session_detail(state_dir, session_id)
            assert detail is not None
            assert detail["total_requests"] == 4
            assert detail["error_count"] == 1
            assert set(detail["services_used"]) == {"s3", "dynamodb"}
            assert detail["service_counts"]["s3"] == 3
            assert detail["service_counts"]["dynamodb"] == 1
            assert detail["duration"] >= 0

    def test_multiple_sessions_aggregate(self, state_dir: Path):
        """Multiple sessions produce correct aggregate stats."""
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            for i in range(3):
                reset_ci_analytics()
                analytics = get_ci_analytics(force_enable=True)
                for _ in range(5):
                    analytics.record_request(service="s3", operation="PutObject", success=(i != 2))
                analytics.end_session()
                analytics.save_session(state_dir)

            summary = compute_aggregate_summary(state_dir)
            assert summary["total_sessions"] == 3
            assert summary["avg_duration"] >= 0
            assert "s3" in summary["most_used_services"]
            # Sessions 0 and 1 have 0 errors, session 2 has 5 errors
            # zero_error_session_rate = 2/3
            assert summary["zero_error_session_rate"] == pytest.approx(2 / 3)

    def test_management_endpoints_json(self, state_dir: Path):
        """Session list and detail return valid JSON-serializable dicts."""
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="sqs", operation="SendMessage", success=True)
            analytics.end_session()
            analytics.save_session(state_dir)

            sessions = list_sessions(state_dir)
            result = json.dumps(sessions)
            assert isinstance(json.loads(result), list)

            detail = get_session_detail(state_dir, analytics.session.session_id)
            result = json.dumps(detail)
            assert isinstance(json.loads(result), dict)

            summary = compute_aggregate_summary(state_dir)
            result = json.dumps(summary)
            assert isinstance(json.loads(result), dict)

    def test_persistence_survives_reset(self, state_dir: Path):
        """Saved sessions persist after analytics reset."""
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            session_id = analytics.session.session_id
            analytics.record_request(service="lambda", operation="Invoke", success=True)
            analytics.end_session()
            analytics.save_session(state_dir)

        # Reset singleton (simulates restart)
        reset_ci_analytics()

        # Data still on disk
        detail = get_session_detail(state_dir, session_id)
        assert detail is not None
        assert detail["total_requests"] == 1

    def test_clear_then_list_empty(self, state_dir: Path):
        """After clearing sessions, list returns empty."""
        with mock.patch.dict(os.environ, {"CI": "true"}, clear=False):
            analytics = get_ci_analytics(force_enable=True)
            analytics.record_request(service="s3", operation="PutObject", success=True)
            analytics.end_session()
            analytics.save_session(state_dir)

        count = clear_sessions(state_dir)
        assert count == 1
        assert list_sessions(state_dir) == []
