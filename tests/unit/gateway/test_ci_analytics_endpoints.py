"""Tests for CI analytics, boot status, endpoints config, and S3 routing management endpoints."""

import json
import os
from unittest.mock import patch


class TestCISessionsList:
    """GET /_robotocore/ci/sessions"""

    def test_returns_200_without_state_dir(self, client):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROBOTOCORE_STATE_DIR", None)
            response = client.get("/_robotocore/ci/sessions")
        assert response.status_code == 200
        data = response.json()
        assert data["sessions"] == []
        assert "error" in data

    def test_returns_empty_sessions_with_empty_dir(self, client, tmp_path):
        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.get("/_robotocore/ci/sessions")
        assert response.status_code == 200
        data = response.json()
        assert data["sessions"] == []
        assert data["count"] == 0

    def test_lists_saved_sessions(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        session_data = {
            "session_id": "test-sess-1",
            "start_time": 1000.0,
            "end_time": 1060.0,
            "duration": 60.0,
            "total_requests": 5,
            "error_count": 0,
            "services_used": ["s3"],
            "service_counts": {"s3": 5},
            "operation_stats": {},
            "ci_provider": "github_actions",
            "build_id": "123",
        }
        (ci_dir / "session-test-sess-1.json").write_text(json.dumps(session_data))

        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.get("/_robotocore/ci/sessions")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["sessions"][0]["session_id"] == "test-sess-1"
        assert data["sessions"][0]["total_requests"] == 5

    def test_lists_multiple_sessions_sorted(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        for i in range(3):
            sess = {"session_id": f"sess-{i}", "total_requests": i}
            (ci_dir / f"session-sess-{i}.json").write_text(json.dumps(sess))

        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.get("/_robotocore/ci/sessions")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3


class TestCISessionsClear:
    """DELETE /_robotocore/ci/sessions"""

    def test_clear_without_state_dir_returns_400(self, client):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROBOTOCORE_STATE_DIR", None)
            response = client.delete("/_robotocore/ci/sessions")
        assert response.status_code == 400
        assert "error" in response.json()

    def test_clear_empty_dir(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.delete("/_robotocore/ci/sessions")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "cleared"
        assert data["count"] == 0

    def test_clear_removes_sessions(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        for i in range(3):
            (ci_dir / f"session-s{i}.json").write_text(json.dumps({"session_id": f"s{i}"}))

        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.delete("/_robotocore/ci/sessions")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "cleared"
        assert data["count"] == 3
        # Verify files are gone
        assert list(ci_dir.glob("session-*.json")) == []


class TestCISessionDetail:
    """GET /_robotocore/ci/sessions/{session_id}"""

    def test_without_state_dir_returns_400(self, client):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROBOTOCORE_STATE_DIR", None)
            response = client.get("/_robotocore/ci/sessions/some-id")
        assert response.status_code == 400
        assert "error" in response.json()

    def test_nonexistent_session_returns_404(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.get("/_robotocore/ci/sessions/nonexistent")
        assert response.status_code == 404
        assert response.json()["error"] == "Session not found"

    def test_get_existing_session(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        session_data = {
            "session_id": "abc-123",
            "start_time": 1000.0,
            "end_time": 1120.0,
            "duration": 120.0,
            "total_requests": 42,
            "error_count": 2,
            "services_used": ["s3", "sqs"],
            "service_counts": {"s3": 30, "sqs": 12},
            "operation_stats": {
                "s3:PutObject": {"success": 28, "failure": 2},
                "sqs:SendMessage": {"success": 12, "failure": 0},
            },
            "ci_provider": "github_actions",
            "build_id": "456",
        }
        (ci_dir / "session-abc-123.json").write_text(json.dumps(session_data))

        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.get("/_robotocore/ci/sessions/abc-123")
        assert response.status_code == 200
        data = response.json()
        assert data["session_id"] == "abc-123"
        assert data["total_requests"] == 42
        assert data["error_count"] == 2
        assert "s3" in data["services_used"]
        assert data["operation_stats"]["s3:PutObject"]["failure"] == 2


class TestCISummary:
    """GET /_robotocore/ci/summary"""

    def test_without_state_dir_returns_400(self, client):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ROBOTOCORE_STATE_DIR", None)
            response = client.get("/_robotocore/ci/summary")
        assert response.status_code == 400
        assert "error" in response.json()

    def test_empty_summary(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.get("/_robotocore/ci/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["total_sessions"] == 0
        assert data["avg_duration"] == 0
        assert data["most_used_services"] == []
        assert data["zero_error_session_rate"] == 0
        assert data["service_reliability"] == {}
        assert data["most_failing_operations"] == []

    def test_summary_with_sessions(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        sessions = [
            {
                "session_id": "s1",
                "duration": 60.0,
                "error_count": 0,
                "service_counts": {"s3": 10, "sqs": 5},
                "operation_stats": {
                    "s3:PutObject": {"success": 10, "failure": 0},
                    "sqs:SendMessage": {"success": 5, "failure": 0},
                },
            },
            {
                "session_id": "s2",
                "duration": 120.0,
                "error_count": 3,
                "service_counts": {"s3": 20, "lambda": 8},
                "operation_stats": {
                    "s3:PutObject": {"success": 17, "failure": 3},
                    "lambda:Invoke": {"success": 8, "failure": 0},
                },
            },
        ]
        for s in sessions:
            (ci_dir / f"session-{s['session_id']}.json").write_text(json.dumps(s))

        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.get("/_robotocore/ci/summary")
        assert response.status_code == 200
        data = response.json()
        assert data["total_sessions"] == 2
        assert data["avg_duration"] == 90.0
        # One session had zero errors out of two total
        assert data["zero_error_session_rate"] == 0.5
        # s3 had 30 total requests, most used
        assert data["most_used_services"][0] == "s3"
        # s3:PutObject had 3 failures
        assert len(data["most_failing_operations"]) >= 1
        failing_op = data["most_failing_operations"][0]
        assert failing_op["operation"] == "s3:PutObject"
        assert failing_op["failures"] == 3
        assert failing_op["successes"] == 27

    def test_summary_service_reliability(self, client, tmp_path):
        ci_dir = tmp_path / "ci_analytics"
        ci_dir.mkdir()
        session = {
            "session_id": "r1",
            "duration": 10.0,
            "error_count": 1,
            "service_counts": {"dynamodb": 4},
            "operation_stats": {
                "dynamodb:PutItem": {"success": 3, "failure": 1},
            },
        }
        (ci_dir / "session-r1.json").write_text(json.dumps(session))

        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            response = client.get("/_robotocore/ci/summary")
        data = response.json()
        assert "dynamodb" in data["service_reliability"]
        # 3 success out of 4 total = 0.75
        assert data["service_reliability"]["dynamodb"] == 0.75


class TestCISessionCRUDFlow:
    """End-to-end: create session via CIAnalytics, list, get, clear."""

    def test_full_lifecycle(self, client, tmp_path):
        from robotocore.audit.ci_analytics import CIAnalytics, CISession

        ci_dir = tmp_path / "ci_analytics"
        session = CISession(
            session_id="lifecycle-test",
            ci_provider="test",
            build_id="99",
        )
        analytics = CIAnalytics(session=session)
        analytics.record_request(service="s3", operation="PutObject", success=True)
        analytics.record_request(service="s3", operation="GetObject", success=False)
        analytics.end_session()
        analytics.save_session(ci_dir)

        with patch.dict(os.environ, {"ROBOTOCORE_STATE_DIR": str(tmp_path)}):
            # List
            resp = client.get("/_robotocore/ci/sessions")
            assert resp.status_code == 200
            assert resp.json()["count"] == 1

            # Detail
            resp = client.get("/_robotocore/ci/sessions/lifecycle-test")
            assert resp.status_code == 200
            detail = resp.json()
            assert detail["session_id"] == "lifecycle-test"
            assert detail["total_requests"] == 2
            assert detail["error_count"] == 1
            assert detail["ci_provider"] == "test"
            assert "s3" in detail["services_used"]

            # Summary
            resp = client.get("/_robotocore/ci/summary")
            assert resp.status_code == 200
            assert resp.json()["total_sessions"] == 1

            # Clear
            resp = client.delete("/_robotocore/ci/sessions")
            assert resp.status_code == 200
            assert resp.json()["count"] == 1

            # Verify cleared
            resp = client.get("/_robotocore/ci/sessions")
            assert resp.json()["count"] == 0


class TestBootStatusEndpoint:
    """GET /_robotocore/boot/status"""

    def test_returns_200(self, client):
        response = client.get("/_robotocore/boot/status")
        assert response.status_code == 200

    def test_has_booted_field(self, client):
        response = client.get("/_robotocore/boot/status")
        data = response.json()
        assert "booted" in data
        assert isinstance(data["booted"], bool)

    def test_has_components_dict(self, client):
        response = client.get("/_robotocore/boot/status")
        data = response.json()
        assert "components" in data
        assert isinstance(data["components"], dict)

    def test_has_boot_result_field(self, client):
        response = client.get("/_robotocore/boot/status")
        data = response.json()
        assert "boot_result" in data


class TestEndpointsConfigEndpoint:
    """GET /_robotocore/endpoints/config"""

    def test_returns_200(self, client):
        response = client.get("/_robotocore/endpoints/config")
        assert response.status_code == 200

    def test_has_sqs_strategy(self, client):
        response = client.get("/_robotocore/endpoints/config")
        data = response.json()
        assert "sqs_endpoint_strategy" in data
        assert isinstance(data["sqs_endpoint_strategy"], str)

    def test_has_opensearch_strategy(self, client):
        response = client.get("/_robotocore/endpoints/config")
        data = response.json()
        assert "opensearch_endpoint_strategy" in data
        assert isinstance(data["opensearch_endpoint_strategy"], str)

    def test_default_strategies(self, client):
        response = client.get("/_robotocore/endpoints/config")
        data = response.json()
        assert data["sqs_endpoint_strategy"] == "standard"
        assert data["opensearch_endpoint_strategy"] == "domain"


class TestS3RoutingConfigEndpoint:
    """GET /_robotocore/s3/routing"""

    def test_returns_200(self, client):
        response = client.get("/_robotocore/s3/routing")
        assert response.status_code == 200

    def test_has_required_fields(self, client):
        response = client.get("/_robotocore/s3/routing")
        data = response.json()
        assert "s3_hostname" in data
        assert "virtual_hosted_style" in data
        assert "website_hostname" in data
        assert "supported_patterns" in data

    def test_virtual_hosted_style_enabled(self, client):
        response = client.get("/_robotocore/s3/routing")
        data = response.json()
        assert data["virtual_hosted_style"] is True

    def test_supported_patterns_is_list(self, client):
        response = client.get("/_robotocore/s3/routing")
        data = response.json()
        assert isinstance(data["supported_patterns"], list)
        assert len(data["supported_patterns"]) > 0

    def test_hostname_in_website_hostname(self, client):
        response = client.get("/_robotocore/s3/routing")
        data = response.json()
        hostname = data["s3_hostname"]
        assert hostname in data["website_hostname"]
