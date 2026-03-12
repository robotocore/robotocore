"""Tests for IAM policy stream management API endpoints.

Covers:
- GET  /_robotocore/iam/policy-stream          (list/filter evaluations)
- DELETE /_robotocore/iam/policy-stream         (clear stream)
- GET  /_robotocore/iam/policy-stream/summary   (aggregate summary)
- GET  /_robotocore/iam/policy-stream/suggest-policy?principal=ARN  (least-privilege policy)
"""

import pytest
from starlette.testclient import TestClient

from robotocore.gateway.app import app
from robotocore.services.iam.policy_stream import PolicyStream, get_policy_stream


@pytest.fixture
def client(monkeypatch):
    # Ensure the stream is enabled for all tests
    monkeypatch.setenv("IAM_POLICY_STREAM", "1")
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def _reset_stream():
    """Clear the policy stream singleton before each test."""
    stream = get_policy_stream()
    stream.clear()
    yield
    stream.clear()


def _seed_entries(stream: PolicyStream) -> None:
    """Populate the stream with a known set of entries for filtering tests."""
    stream.record(
        principal="arn:aws:iam::123456789012:user/alice",
        action="s3:GetObject",
        resource="arn:aws:s3:::my-bucket/*",
        decision="Allow",
        matched_policies=["policy-1"],
        request_id="req-1",
    )
    stream.record(
        principal="arn:aws:iam::123456789012:user/alice",
        action="s3:PutObject",
        resource="arn:aws:s3:::my-bucket/*",
        decision="Allow",
        matched_policies=["policy-1"],
        request_id="req-2",
    )
    stream.record(
        principal="arn:aws:iam::123456789012:user/bob",
        action="ec2:DescribeInstances",
        resource="*",
        decision="Deny",
        request_id="req-3",
    )
    stream.record(
        principal="arn:aws:iam::123456789012:user/bob",
        action="sqs:SendMessage",
        resource="arn:aws:sqs:us-east-1:123456789012:my-queue",
        decision="Deny",
        request_id="req-4",
    )
    stream.record(
        principal="arn:aws:iam::123456789012:user/alice",
        action="dynamodb:GetItem",
        resource="arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
        decision="Allow",
        request_id="req-5",
    )


# ---------------------------------------------------------------------------
# GET /_robotocore/iam/policy-stream
# ---------------------------------------------------------------------------


class TestPolicyStreamList:
    def test_empty_stream_returns_zero_entries(self, client):
        resp = client.get("/_robotocore/iam/policy-stream")
        assert resp.status_code == 200
        data = resp.json()
        assert data["entries"] == []
        assert data["count"] == 0

    def test_returns_recorded_entries(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 5
        assert len(data["entries"]) == 5

    def test_entries_are_newest_first(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream")
        entries = resp.json()["entries"]
        # Last recorded entry (req-5) should come first
        assert entries[0]["request_id"] == "req-5"
        assert entries[-1]["request_id"] == "req-1"

    def test_limit_param(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        assert len(data["entries"]) == 2

    def test_filter_by_principal(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get(
            "/_robotocore/iam/policy-stream?principal=arn:aws:iam::123456789012:user/bob"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        for entry in data["entries"]:
            assert entry["principal"] == "arn:aws:iam::123456789012:user/bob"

    def test_filter_by_decision_allow(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream?decision=Allow")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 3
        for entry in data["entries"]:
            assert entry["decision"] == "Allow"

    def test_filter_by_decision_deny(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream?decision=Deny")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        for entry in data["entries"]:
            assert entry["decision"] == "Deny"

    def test_filter_by_action(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream?action=s3:GetObject")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["entries"][0]["action"] == "s3:GetObject"

    def test_filter_by_action_wildcard(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream?action=s3:*")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        for entry in data["entries"]:
            assert entry["action"].startswith("s3:")

    def test_combined_filters(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get(
            "/_robotocore/iam/policy-stream"
            "?principal=arn:aws:iam::123456789012:user/alice&decision=Allow"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 3
        for entry in data["entries"]:
            assert entry["principal"] == "arn:aws:iam::123456789012:user/alice"
            assert entry["decision"] == "Allow"

    def test_entry_has_expected_fields(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream?limit=1")
        entry = resp.json()["entries"][0]
        expected_keys = {
            "timestamp",
            "principal",
            "action",
            "resource",
            "decision",
            "matched_policies",
            "matched_statement",
            "request_id",
            "evaluation_duration_ms",
        }
        assert set(entry.keys()) == expected_keys

    def test_disabled_stream_returns_400(self, client, monkeypatch):
        monkeypatch.setenv("IAM_POLICY_STREAM", "0")
        resp = client.get("/_robotocore/iam/policy-stream")
        assert resp.status_code == 400
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# DELETE /_robotocore/iam/policy-stream
# ---------------------------------------------------------------------------


class TestPolicyStreamClear:
    def test_clear_empty_stream(self, client):
        resp = client.delete("/_robotocore/iam/policy-stream")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cleared"
        assert data["count"] == 0

    def test_clear_returns_count(self, client):
        _seed_entries(get_policy_stream())
        resp = client.delete("/_robotocore/iam/policy-stream")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cleared"
        assert data["count"] == 5

    def test_clear_then_list_is_empty(self, client):
        _seed_entries(get_policy_stream())
        client.delete("/_robotocore/iam/policy-stream")
        resp = client.get("/_robotocore/iam/policy-stream")
        assert resp.json()["count"] == 0


# ---------------------------------------------------------------------------
# GET /_robotocore/iam/policy-stream/summary
# ---------------------------------------------------------------------------


class TestPolicyStreamSummary:
    def test_summary_empty_stream(self, client):
        resp = client.get("/_robotocore/iam/policy-stream/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_evaluations"] == 0
        assert data["allow_count"] == 0
        assert data["deny_count"] == 0
        assert data["top_denied_actions"] == []
        assert data["top_denied_principals"] == []

    def test_summary_with_entries(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_evaluations"] == 5
        assert data["allow_count"] == 3
        assert data["deny_count"] == 2

    def test_summary_top_denied_actions(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream/summary")
        data = resp.json()
        denied_actions = {a["action"] for a in data["top_denied_actions"]}
        assert "ec2:DescribeInstances" in denied_actions
        assert "sqs:SendMessage" in denied_actions

    def test_summary_top_denied_principals(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get("/_robotocore/iam/policy-stream/summary")
        data = resp.json()
        denied_principals = {p["principal"] for p in data["top_denied_principals"]}
        assert "arn:aws:iam::123456789012:user/bob" in denied_principals

    def test_summary_has_all_required_keys(self, client):
        resp = client.get("/_robotocore/iam/policy-stream/summary")
        data = resp.json()
        expected_keys = {
            "total_evaluations",
            "allow_count",
            "deny_count",
            "top_denied_actions",
            "top_denied_principals",
        }
        assert set(data.keys()) == expected_keys


# ---------------------------------------------------------------------------
# GET /_robotocore/iam/policy-stream/suggest-policy
# ---------------------------------------------------------------------------


class TestPolicyStreamSuggestPolicy:
    def test_suggest_policy_missing_principal_returns_400(self, client):
        resp = client.get("/_robotocore/iam/policy-stream/suggest-policy")
        assert resp.status_code == 400
        assert "error" in resp.json()
        assert "principal" in resp.json()["error"]

    def test_suggest_policy_no_matching_entries(self, client):
        resp = client.get(
            "/_robotocore/iam/policy-stream/suggest-policy"
            "?principal=arn:aws:iam::123456789012:user/nobody"
        )
        assert resp.status_code == 200
        policy = resp.json()
        assert policy["Version"] == "2012-10-17"
        assert policy["Statement"] == []

    def test_suggest_policy_returns_valid_iam_structure(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get(
            "/_robotocore/iam/policy-stream/suggest-policy"
            "?principal=arn:aws:iam::123456789012:user/alice"
        )
        assert resp.status_code == 200
        policy = resp.json()
        assert policy["Version"] == "2012-10-17"
        assert isinstance(policy["Statement"], list)
        assert len(policy["Statement"]) > 0

    def test_suggest_policy_statements_have_required_keys(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get(
            "/_robotocore/iam/policy-stream/suggest-policy"
            "?principal=arn:aws:iam::123456789012:user/alice"
        )
        policy = resp.json()
        for stmt in policy["Statement"]:
            assert stmt["Effect"] == "Allow"
            assert "Action" in stmt
            assert "Resource" in stmt

    def test_suggest_policy_only_includes_allowed_actions(self, client):
        _seed_entries(get_policy_stream())
        # Bob only has Deny entries, so suggest-policy should produce empty statements
        resp = client.get(
            "/_robotocore/iam/policy-stream/suggest-policy"
            "?principal=arn:aws:iam::123456789012:user/bob"
        )
        policy = resp.json()
        assert policy["Statement"] == []

    def test_suggest_policy_groups_by_resource(self, client):
        _seed_entries(get_policy_stream())
        # Alice has s3:GetObject and s3:PutObject on the same resource
        resp = client.get(
            "/_robotocore/iam/policy-stream/suggest-policy"
            "?principal=arn:aws:iam::123456789012:user/alice"
        )
        policy = resp.json()
        # Find the s3 bucket statement (has 2 actions on same resource)
        s3_stmts = [s for s in policy["Statement"] if s["Resource"] == "arn:aws:s3:::my-bucket/*"]
        assert len(s3_stmts) == 1
        s3_stmt = s3_stmts[0]
        # Multiple actions on same resource -> list
        assert isinstance(s3_stmt["Action"], list)
        assert "s3:GetObject" in s3_stmt["Action"]
        assert "s3:PutObject" in s3_stmt["Action"]

    def test_suggest_policy_single_action_is_string(self, client):
        _seed_entries(get_policy_stream())
        resp = client.get(
            "/_robotocore/iam/policy-stream/suggest-policy"
            "?principal=arn:aws:iam::123456789012:user/alice"
        )
        policy = resp.json()
        # The dynamodb statement has a single action
        dynamo_stmts = [
            s
            for s in policy["Statement"]
            if "dynamodb" in (s["Resource"] if isinstance(s["Resource"], str) else "")
        ]
        assert len(dynamo_stmts) == 1
        # Single action -> string, not list
        assert dynamo_stmts[0]["Action"] == "dynamodb:GetItem"
