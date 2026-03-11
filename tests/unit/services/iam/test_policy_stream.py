"""Unit tests for IAM policy evaluation stream."""

import json
import threading

import pytest

from robotocore.services.iam.policy_stream import (
    PolicyStream,
    is_stream_enabled,
)


@pytest.fixture
def stream():
    """Create a fresh PolicyStream for each test."""
    return PolicyStream(max_size=100)


@pytest.fixture
def small_stream():
    """Stream with small buffer for eviction tests."""
    return PolicyStream(max_size=3)


def _make_entry(**overrides):
    """Helper to build a policy evaluation entry."""
    base = {
        "principal": "arn:aws:iam::123456789012:user/alice",
        "action": "s3:GetObject",
        "resource": "arn:aws:s3:::my-bucket/key.txt",
        "decision": "Allow",
        "matched_policies": ["arn:aws:iam::123456789012:policy/ReadOnly"],
        "matched_statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
        "request_id": "req-001",
    }
    base.update(overrides)
    return base


class TestRecordEvaluation:
    def test_record_with_all_fields(self, stream):
        stream.record(**_make_entry())
        entries = stream.recent(limit=10)
        assert len(entries) == 1
        e = entries[0]
        assert e["principal"] == "arn:aws:iam::123456789012:user/alice"
        assert e["action"] == "s3:GetObject"
        assert e["resource"] == "arn:aws:s3:::my-bucket/key.txt"
        assert e["decision"] == "Allow"
        assert e["matched_policies"] == ["arn:aws:iam::123456789012:policy/ReadOnly"]
        assert e["matched_statement"] == {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*",
        }
        assert e["request_id"] == "req-001"
        assert "timestamp" in e
        assert "evaluation_duration_ms" in e

    def test_record_with_evaluation_duration(self, stream):
        stream.record(**_make_entry(), evaluation_duration_ms=1.5)
        entries = stream.recent(limit=10)
        assert entries[0]["evaluation_duration_ms"] == 1.5


class TestRingBuffer:
    def test_oldest_entries_evicted_when_full(self, small_stream):
        for i in range(5):
            small_stream.record(**_make_entry(request_id=f"req-{i}"))
        entries = small_stream.recent(limit=10)
        assert len(entries) == 3
        # Newest first
        assert entries[0]["request_id"] == "req-4"
        assert entries[1]["request_id"] == "req-3"
        assert entries[2]["request_id"] == "req-2"

    def test_configurable_buffer_size(self):
        stream = PolicyStream(max_size=5)
        for i in range(10):
            stream.record(**_make_entry(request_id=f"req-{i}"))
        entries = stream.recent(limit=100)
        assert len(entries) == 5


class TestThreadSafety:
    def test_concurrent_recording(self, stream):
        errors = []

        def writer(start, count):
            try:
                for i in range(count):
                    stream.record(**_make_entry(request_id=f"req-{start + i}"))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(i * 50, 50)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        entries = stream.recent(limit=200)
        assert len(entries) == 100  # max_size=100


class TestGetRecent:
    def test_default_limit(self, stream):
        for i in range(150):
            stream.record(**_make_entry(request_id=f"req-{i}"))
        entries = stream.recent()
        assert len(entries) == 100  # default limit

    def test_custom_limit(self, stream):
        for i in range(50):
            stream.record(**_make_entry(request_id=f"req-{i}"))
        entries = stream.recent(limit=10)
        assert len(entries) == 10


class TestFilters:
    @pytest.fixture(autouse=True)
    def _populate(self, stream):
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="s3:GetObject",
                decision="Allow",
                request_id="req-1",
            )
        )
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/bob",
                action="s3:PutObject",
                decision="Deny",
                request_id="req-2",
            )
        )
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="dynamodb:GetItem",
                decision="Deny",
                request_id="req-3",
            )
        )
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/bob",
                action="sqs:SendMessage",
                decision="Allow",
                request_id="req-4",
            )
        )

    def test_filter_by_principal(self, stream):
        entries = stream.recent(limit=100, principal="arn:aws:iam::123456789012:user/alice")
        assert len(entries) == 2
        assert all(e["principal"] == "arn:aws:iam::123456789012:user/alice" for e in entries)

    def test_filter_by_action_exact(self, stream):
        entries = stream.recent(limit=100, action="s3:GetObject")
        assert len(entries) == 1
        assert entries[0]["action"] == "s3:GetObject"

    def test_filter_by_action_wildcard(self, stream):
        entries = stream.recent(limit=100, action="s3:*")
        assert len(entries) == 2
        assert all(e["action"].startswith("s3:") for e in entries)

    def test_filter_by_decision(self, stream):
        entries = stream.recent(limit=100, decision="Deny")
        assert len(entries) == 2
        assert all(e["decision"] == "Deny" for e in entries)

    def test_combined_filters(self, stream):
        entries = stream.recent(
            limit=100,
            principal="arn:aws:iam::123456789012:user/alice",
            decision="Deny",
        )
        assert len(entries) == 1
        assert entries[0]["action"] == "dynamodb:GetItem"


class TestClear:
    def test_clear_stream(self, stream):
        stream.record(**_make_entry())
        stream.record(**_make_entry())
        count = stream.clear()
        assert count == 2
        assert stream.recent(limit=100) == []


class TestSummary:
    def test_summary_computation(self, stream):
        stream.record(**_make_entry(decision="Allow", action="s3:GetObject"))
        stream.record(**_make_entry(decision="Allow", action="s3:PutObject"))
        stream.record(
            **_make_entry(
                decision="Deny",
                action="ec2:RunInstances",
                principal="arn:aws:iam::123456789012:user/bob",
            )
        )
        stream.record(
            **_make_entry(
                decision="Deny",
                action="ec2:RunInstances",
                principal="arn:aws:iam::123456789012:user/bob",
            )
        )
        stream.record(
            **_make_entry(
                decision="Deny",
                action="lambda:InvokeFunction",
                principal="arn:aws:iam::123456789012:user/alice",
            )
        )

        summary = stream.summary()
        assert summary["total_evaluations"] == 5
        assert summary["allow_count"] == 2
        assert summary["deny_count"] == 3
        # top_denied_actions should have ec2:RunInstances first (2 denies)
        assert summary["top_denied_actions"][0]["action"] == "ec2:RunInstances"
        assert summary["top_denied_actions"][0]["count"] == 2
        # top_denied_principals should have bob first (2 denies)
        assert (
            summary["top_denied_principals"][0]["principal"] == "arn:aws:iam::123456789012:user/bob"
        )
        assert summary["top_denied_principals"][0]["count"] == 2


class TestSuggestPolicy:
    def test_suggest_policy_generates_valid_document(self, stream):
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="s3:GetObject",
                resource="arn:aws:s3:::bucket1/*",
                decision="Allow",
            )
        )
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="s3:PutObject",
                resource="arn:aws:s3:::bucket1/*",
                decision="Allow",
            )
        )

        policy = stream.suggest_policy("arn:aws:iam::123456789012:user/alice")
        assert policy["Version"] == "2012-10-17"
        assert len(policy["Statement"]) >= 1
        # Validate it's valid JSON
        json.dumps(policy)

    def test_suggest_policy_groups_by_resource(self, stream):
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="s3:GetObject",
                resource="arn:aws:s3:::bucket1/*",
                decision="Allow",
            )
        )
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="s3:PutObject",
                resource="arn:aws:s3:::bucket1/*",
                decision="Allow",
            )
        )
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="dynamodb:GetItem",
                resource="arn:aws:dynamodb:us-east-1:123456789012:table/MyTable",
                decision="Allow",
            )
        )

        policy = stream.suggest_policy("arn:aws:iam::123456789012:user/alice")
        # Should have 2 statements: one for s3 resource, one for dynamodb resource
        assert len(policy["Statement"]) == 2
        resources = {s["Resource"] for s in policy["Statement"]}
        assert "arn:aws:s3:::bucket1/*" in resources
        assert "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable" in resources

    def test_suggest_policy_unknown_principal(self, stream):
        policy = stream.suggest_policy("arn:aws:iam::123456789012:user/nobody")
        assert policy["Version"] == "2012-10-17"
        assert policy["Statement"] == []

    def test_suggest_policy_excludes_denied(self, stream):
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="s3:GetObject",
                resource="arn:aws:s3:::bucket1/*",
                decision="Allow",
            )
        )
        stream.record(
            **_make_entry(
                principal="arn:aws:iam::123456789012:user/alice",
                action="s3:DeleteBucket",
                resource="arn:aws:s3:::bucket1",
                decision="Deny",
            )
        )

        policy = stream.suggest_policy("arn:aws:iam::123456789012:user/alice")
        all_actions = []
        for stmt in policy["Statement"]:
            actions = stmt["Action"]
            if isinstance(actions, list):
                all_actions.extend(actions)
            else:
                all_actions.append(actions)
        assert "s3:GetObject" in all_actions
        assert "s3:DeleteBucket" not in all_actions


class TestStreamEnabled:
    def test_stream_enabled_when_enforce_iam_on(self, monkeypatch):
        monkeypatch.setenv("ENFORCE_IAM", "1")
        monkeypatch.delenv("IAM_POLICY_STREAM", raising=False)
        assert is_stream_enabled() is True

    def test_stream_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("ENFORCE_IAM", raising=False)
        monkeypatch.delenv("IAM_POLICY_STREAM", raising=False)
        assert is_stream_enabled() is False

    def test_stream_explicitly_disabled(self, monkeypatch):
        monkeypatch.setenv("ENFORCE_IAM", "1")
        monkeypatch.setenv("IAM_POLICY_STREAM", "0")
        assert is_stream_enabled() is False

    def test_stream_explicitly_enabled(self, monkeypatch):
        monkeypatch.delenv("ENFORCE_IAM", raising=False)
        monkeypatch.setenv("IAM_POLICY_STREAM", "1")
        assert is_stream_enabled() is True
