"""Semantic integration tests for IAM policy stream + management endpoints."""

import pytest

from robotocore.services.iam import policy_stream as policy_stream_mod
from robotocore.services.iam.policy_stream import format_stream_response, get_policy_stream


@pytest.fixture(autouse=True)
def _reset_singleton(monkeypatch):
    """Reset the global stream singleton before each test."""
    old = policy_stream_mod._stream
    policy_stream_mod._stream = None
    monkeypatch.setenv("IAM_POLICY_STREAM", "1")
    yield
    policy_stream_mod._stream = old


def _make_entry(**overrides):
    base = {
        "principal": "arn:aws:iam::123456789012:user/alice",
        "action": "s3:GetObject",
        "resource": "arn:aws:s3:::my-bucket/key.txt",
        "decision": "Allow",
        "matched_policies": ["policy1"],
        "matched_statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
        "request_id": "req-001",
    }
    base.update(overrides)
    return base


class TestEndToEndStream:
    def test_record_then_get_stream(self):
        stream = get_policy_stream()
        stream.record(**_make_entry(request_id="req-e2e-1"))
        stream.record(**_make_entry(request_id="req-e2e-2", decision="Deny"))

        entries = stream.recent(limit=100)
        assert len(entries) == 2
        assert entries[0]["request_id"] == "req-e2e-2"
        assert entries[1]["request_id"] == "req-e2e-1"

    def test_record_deny_then_summary(self):
        stream = get_policy_stream()
        stream.record(**_make_entry(decision="Allow"))
        stream.record(**_make_entry(decision="Deny", action="ec2:RunInstances"))
        stream.record(**_make_entry(decision="Deny", action="ec2:RunInstances"))

        summary = stream.summary()
        assert summary["deny_count"] == 2
        assert summary["allow_count"] == 1
        assert summary["total_evaluations"] == 3

    def test_record_multiple_then_suggest_policy(self):
        stream = get_policy_stream()
        stream.record(
            **_make_entry(
                action="s3:GetObject",
                resource="arn:aws:s3:::bucket/*",
                decision="Allow",
            )
        )
        stream.record(
            **_make_entry(
                action="dynamodb:GetItem",
                resource="arn:aws:dynamodb:us-east-1:123456789012:table/T",
                decision="Allow",
            )
        )

        policy = stream.suggest_policy("arn:aws:iam::123456789012:user/alice")
        assert policy["Version"] == "2012-10-17"
        assert len(policy["Statement"]) == 2

        all_actions = []
        for stmt in policy["Statement"]:
            a = stmt["Action"]
            if isinstance(a, list):
                all_actions.extend(a)
            else:
                all_actions.append(a)
        assert "s3:GetObject" in all_actions
        assert "dynamodb:GetItem" in all_actions

    def test_clear_then_get_empty(self):
        stream = get_policy_stream()
        stream.record(**_make_entry())
        assert len(stream.recent(limit=100)) == 1
        stream.clear()
        assert stream.recent(limit=100) == []


class TestManagementEndpointJSON:
    """Test that the management endpoint handlers produce correct JSON structures."""

    def test_stream_endpoint_json_structure(self):
        stream = get_policy_stream()
        stream.record(**_make_entry())
        result = format_stream_response(stream.recent(limit=100))
        assert "entries" in result
        assert "count" in result
        assert result["count"] == 1
        entry = result["entries"][0]
        assert "timestamp" in entry
        assert "principal" in entry
        assert "action" in entry
        assert "decision" in entry

    def test_summary_endpoint_json_structure(self):
        stream = get_policy_stream()
        stream.record(**_make_entry(decision="Deny", action="ec2:RunInstances"))
        summary = stream.summary()
        assert "total_evaluations" in summary
        assert "allow_count" in summary
        assert "deny_count" in summary
        assert "top_denied_actions" in summary
        assert "top_denied_principals" in summary

    def test_suggest_policy_json_structure(self):
        stream = get_policy_stream()
        stream.record(
            **_make_entry(
                action="s3:GetObject",
                resource="arn:aws:s3:::bucket/*",
                decision="Allow",
            )
        )
        policy = stream.suggest_policy("arn:aws:iam::123456789012:user/alice")
        # Must be a valid IAM policy document
        assert policy["Version"] == "2012-10-17"
        for stmt in policy["Statement"]:
            assert stmt["Effect"] == "Allow"
            assert "Action" in stmt
            assert "Resource" in stmt
