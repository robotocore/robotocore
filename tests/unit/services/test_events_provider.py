"""Unit tests for the EventBridge (events) provider."""

import json

import pytest

from robotocore.services.events.provider import (
    EventsError,
    _api_destinations,
    _connections,
    _error,
    _json,
    _stores,
    clear_invocation_log,
    get_invocation_log,
    handle_events_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(operation: str, body: dict):
    """Build a fake Starlette Request."""
    from starlette.requests import Request

    target = f"AWSEvents.{operation}"
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"x-amz-target", target.encode())],
    }
    body_bytes = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


@pytest.fixture(autouse=True)
def _clear_stores():
    """Reset global stores between tests."""
    _stores.clear()
    _connections.clear()
    _api_destinations.clear()
    clear_invocation_log()
    yield
    _stores.clear()
    _connections.clear()
    _api_destinations.clear()
    clear_invocation_log()


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json(200, {"key": "val"})
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"key": "val"}

    def test_json_response_none_body(self):
        resp = _json(200, None)
        assert resp.status_code == 200
        assert resp.body == b""

    def test_error_response(self):
        resp = _error("SomeError", "bad thing", 400)
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "SomeError"
        assert data["message"] == "bad thing"


# ---------------------------------------------------------------------------
# EventsError
# ---------------------------------------------------------------------------


class TestEventsError:
    def test_default_status(self):
        e = EventsError("Code", "msg")
        assert e.status == 400

    def test_custom_status(self):
        e = EventsError("Code", "msg", 500)
        assert e.status == 500


# ---------------------------------------------------------------------------
# handle_events_request — routing
# ---------------------------------------------------------------------------


class TestHandleEventsRequest:
    @pytest.mark.asyncio
    async def test_put_rule_creates_rule(self):
        req = _make_request(
            "PutRule",
            {"Name": "test-rule", "EventPattern": '{"source": ["my.app"]}'},
        )
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "RuleArn" in data

    @pytest.mark.asyncio
    async def test_describe_rule_not_found(self):
        req = _make_request("DescribeRule", {"Name": "no-such-rule"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceNotFoundException"

    @pytest.mark.asyncio
    async def test_put_and_describe_rule(self):
        req1 = _make_request("PutRule", {"Name": "r1", "State": "ENABLED", "Description": "desc"})
        await handle_events_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("DescribeRule", {"Name": "r1"})
        resp = await handle_events_request(req2, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert data["Name"] == "r1"
        assert data["State"] == "ENABLED"

    @pytest.mark.asyncio
    async def test_delete_rule(self):
        req1 = _make_request("PutRule", {"Name": "r1"})
        await handle_events_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("DeleteRule", {"Name": "r1"})
        resp = await handle_events_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_enable_and_disable_rule(self):
        req1 = _make_request("PutRule", {"Name": "r1", "State": "ENABLED"})
        await handle_events_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("DisableRule", {"Name": "r1"})
        await handle_events_request(req2, "us-east-1", "123456789012")

        req3 = _make_request("DescribeRule", {"Name": "r1"})
        resp = await handle_events_request(req3, "us-east-1", "123456789012")
        assert json.loads(resp.body)["State"] == "DISABLED"

        req4 = _make_request("EnableRule", {"Name": "r1"})
        await handle_events_request(req4, "us-east-1", "123456789012")

        resp2 = await handle_events_request(req3, "us-east-1", "123456789012")
        assert json.loads(resp2.body)["State"] == "ENABLED"

    @pytest.mark.asyncio
    async def test_list_rules(self):
        for name in ("rule-a", "rule-b"):
            req = _make_request("PutRule", {"Name": name})
            await handle_events_request(req, "us-east-1", "123456789012")

        req = _make_request("ListRules", {})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        names = [r["Name"] for r in data["Rules"]]
        assert "rule-a" in names
        assert "rule-b" in names

    @pytest.mark.asyncio
    async def test_put_and_list_targets(self):
        req1 = _make_request("PutRule", {"Name": "r1"})
        await handle_events_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "PutTargets",
            {
                "Rule": "r1",
                "Targets": [
                    {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123:q1"},
                ],
            },
        )
        resp = await handle_events_request(req2, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert data["FailedEntryCount"] == 0

        req3 = _make_request("ListTargetsByRule", {"Rule": "r1"})
        resp3 = await handle_events_request(req3, "us-east-1", "123456789012")
        targets = json.loads(resp3.body)["Targets"]
        assert len(targets) == 1
        assert targets[0]["Id"] == "t1"

    @pytest.mark.asyncio
    async def test_put_events_returns_event_ids(self):
        req = _make_request(
            "PutEvents",
            {
                "Entries": [
                    {
                        "Source": "my.app",
                        "DetailType": "TestEvent",
                        "Detail": '{"key": "val"}',
                    }
                ]
            },
        )
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert data["FailedEntryCount"] == 0
        assert len(data["Entries"]) == 1
        assert "EventId" in data["Entries"][0]

    @pytest.mark.asyncio
    async def test_create_and_describe_event_bus(self):
        req1 = _make_request("CreateEventBus", {"Name": "mybus"})
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        assert "EventBusArn" in json.loads(resp1.body)

        req2 = _make_request("DescribeEventBus", {"Name": "mybus"})
        resp2 = await handle_events_request(req2, "us-east-1", "123456789012")
        assert json.loads(resp2.body)["Name"] == "mybus"

    @pytest.mark.asyncio
    async def test_describe_nonexistent_bus(self):
        req = _make_request("DescribeEventBus", {"Name": "nope"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_list_event_buses_includes_default(self):
        req = _make_request("ListEventBuses", {})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        names = [b["Name"] for b in data["EventBuses"]]
        assert "default" in names

    @pytest.mark.asyncio
    async def test_internal_error_returns_500(self):
        """If a handler raises an unexpected exception, we get 500."""
        from robotocore.services.events.provider import _ACTION_MAP

        original = _ACTION_MAP["PutRule"]
        _ACTION_MAP["PutRule"] = lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("unexpected"))
        try:
            req = _make_request("PutRule", {"Name": "r"})
            resp = await handle_events_request(req, "us-east-1", "123456789012")
            assert resp.status_code == 500
        finally:
            _ACTION_MAP["PutRule"] = original


# ---------------------------------------------------------------------------
# Invocation log
# ---------------------------------------------------------------------------


class TestTagOperations:
    @pytest.mark.asyncio
    async def test_tag_and_list_tags(self):
        # Create a rule to get an ARN
        req1 = _make_request("PutRule", {"Name": "tag-rule", "ScheduleExpression": "rate(1 hour)"})
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        rule_arn = json.loads(resp1.body)["RuleArn"]

        # Tag it
        req2 = _make_request(
            "TagResource",
            {
                "ResourceARN": rule_arn,
                "Tags": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            },
        )
        resp2 = await handle_events_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 200

        # List tags
        req3 = _make_request("ListTagsForResource", {"ResourceARN": rule_arn})
        resp3 = await handle_events_request(req3, "us-east-1", "123456789012")
        data = json.loads(resp3.body)
        tags = {t["Key"]: t["Value"] for t in data["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

    @pytest.mark.asyncio
    async def test_untag_resource(self):
        req1 = _make_request(
            "PutRule", {"Name": "untag-rule", "ScheduleExpression": "rate(1 hour)"}
        )
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        rule_arn = json.loads(resp1.body)["RuleArn"]

        # Tag it
        req2 = _make_request(
            "TagResource",
            {
                "ResourceARN": rule_arn,
                "Tags": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            },
        )
        await handle_events_request(req2, "us-east-1", "123456789012")

        # Untag 'env'
        req3 = _make_request(
            "UntagResource",
            {"ResourceARN": rule_arn, "TagKeys": ["env"]},
        )
        resp3 = await handle_events_request(req3, "us-east-1", "123456789012")
        assert resp3.status_code == 200

        # Verify only 'team' remains
        req4 = _make_request("ListTagsForResource", {"ResourceARN": rule_arn})
        resp4 = await handle_events_request(req4, "us-east-1", "123456789012")
        data = json.loads(resp4.body)
        keys = [t["Key"] for t in data["Tags"]]
        assert "env" not in keys
        assert "team" in keys

    @pytest.mark.asyncio
    async def test_list_tags_empty(self):
        req = _make_request(
            "ListTagsForResource",
            {"ResourceARN": "arn:aws:events:us-east-1:123456789012:rule/no-such-rule"},
        )
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert data["Tags"] == []

    @pytest.mark.asyncio
    async def test_tag_overwrite(self):
        req1 = _make_request("PutRule", {"Name": "ow-rule", "ScheduleExpression": "rate(1 hour)"})
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        rule_arn = json.loads(resp1.body)["RuleArn"]

        # Tag with env=test
        req2 = _make_request(
            "TagResource",
            {"ResourceARN": rule_arn, "Tags": [{"Key": "env", "Value": "test"}]},
        )
        await handle_events_request(req2, "us-east-1", "123456789012")

        # Overwrite env=prod
        req3 = _make_request(
            "TagResource",
            {"ResourceARN": rule_arn, "Tags": [{"Key": "env", "Value": "prod"}]},
        )
        await handle_events_request(req3, "us-east-1", "123456789012")

        req4 = _make_request("ListTagsForResource", {"ResourceARN": rule_arn})
        resp4 = await handle_events_request(req4, "us-east-1", "123456789012")
        data = json.loads(resp4.body)
        tags = {t["Key"]: t["Value"] for t in data["Tags"]}
        assert tags["env"] == "prod"
        assert len(data["Tags"]) == 1


class TestInvocationLog:
    def test_get_and_clear(self):
        assert get_invocation_log() == []
        from robotocore.services.events.provider import _log_invocation

        _log_invocation("lambda", "arn:test", "payload")
        assert len(get_invocation_log()) == 1
        clear_invocation_log()
        assert get_invocation_log() == []


# ---------------------------------------------------------------------------
# Categorical bug: Store isolation by account_id
# ---------------------------------------------------------------------------


class TestStoreIsolationByAccount:
    """_get_store() must key on (region, account_id), not just region.
    Two AWS accounts in the same region must not share state."""

    @pytest.mark.asyncio
    async def test_different_accounts_have_separate_buses(self):
        req1 = _make_request("CreateEventBus", {"Name": "acct-bus"})
        await handle_events_request(req1, "us-east-1", "111111111111")

        req2 = _make_request("DescribeEventBus", {"Name": "acct-bus"})
        resp2 = await handle_events_request(req2, "us-east-1", "222222222222")
        # Account 222 should NOT see account 111's bus
        assert resp2.status_code == 400
        data = json.loads(resp2.body)
        assert data["__type"] == "ResourceNotFoundException"

    @pytest.mark.asyncio
    async def test_different_accounts_have_separate_rules(self):
        req1 = _make_request("PutRule", {"Name": "acct-rule"})
        await handle_events_request(req1, "us-east-1", "111111111111")

        req2 = _make_request("DescribeRule", {"Name": "acct-rule"})
        resp2 = await handle_events_request(req2, "us-east-1", "222222222222")
        assert resp2.status_code == 400


# ---------------------------------------------------------------------------
# Categorical bug: Parent-child cascade on deletion
# ---------------------------------------------------------------------------


class TestCascadeDeletion:
    """Deleting a bus should clean up rules, targets, archives, and tags."""

    @pytest.mark.asyncio
    async def test_delete_bus_cleans_up_rules(self):
        # Create bus with a rule
        req1 = _make_request("CreateEventBus", {"Name": "cascade-bus"})
        await handle_events_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("PutRule", {"Name": "cascade-rule", "EventBusName": "cascade-bus"})
        await handle_events_request(req2, "us-east-1", "123456789012")

        # Delete bus
        req3 = _make_request("DeleteEventBus", {"Name": "cascade-bus"})
        await handle_events_request(req3, "us-east-1", "123456789012")

        # Rule should not be findable after bus deletion
        req4 = _make_request(
            "DescribeRule", {"Name": "cascade-rule", "EventBusName": "cascade-bus"}
        )
        resp4 = await handle_events_request(req4, "us-east-1", "123456789012")
        assert resp4.status_code == 400

    @pytest.mark.asyncio
    async def test_delete_bus_cleans_up_archives(self):
        """Archives sourced from a deleted bus should also be cleaned up."""
        req1 = _make_request("CreateEventBus", {"Name": "arch-bus"})
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        bus_arn = json.loads(resp1.body)["EventBusArn"]

        req2 = _make_request(
            "CreateArchive",
            {"ArchiveName": "bus-archive", "EventSourceArn": bus_arn},
        )
        await handle_events_request(req2, "us-east-1", "123456789012")

        # Delete bus
        req3 = _make_request("DeleteEventBus", {"Name": "arch-bus"})
        await handle_events_request(req3, "us-east-1", "123456789012")

        # Archive sourced from deleted bus should be gone
        req4 = _make_request("DescribeArchive", {"ArchiveName": "bus-archive"})
        resp4 = await handle_events_request(req4, "us-east-1", "123456789012")
        assert resp4.status_code == 400

    @pytest.mark.asyncio
    async def test_delete_bus_cleans_up_tags(self):
        """Tags on a deleted bus ARN should be cleaned up."""
        req1 = _make_request("CreateEventBus", {"Name": "tag-bus"})
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        bus_arn = json.loads(resp1.body)["EventBusArn"]

        # Tag the bus
        req2 = _make_request(
            "TagResource",
            {"ResourceARN": bus_arn, "Tags": [{"Key": "env", "Value": "test"}]},
        )
        await handle_events_request(req2, "us-east-1", "123456789012")

        # Delete bus
        req3 = _make_request("DeleteEventBus", {"Name": "tag-bus"})
        await handle_events_request(req3, "us-east-1", "123456789012")

        # Tags should be gone
        req4 = _make_request("ListTagsForResource", {"ResourceARN": bus_arn})
        resp4 = await handle_events_request(req4, "us-east-1", "123456789012")
        data = json.loads(resp4.body)
        assert data["Tags"] == []


# ---------------------------------------------------------------------------
# Categorical bug: Tags not cleaned up on resource deletion
# ---------------------------------------------------------------------------


class TestTagCleanupOnDeletion:
    """Tags must be removed when the tagged resource is deleted."""

    @pytest.mark.asyncio
    async def test_delete_rule_cleans_up_tags(self):
        req1 = _make_request("PutRule", {"Name": "tagged-rule"})
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        rule_arn = json.loads(resp1.body)["RuleArn"]

        req2 = _make_request(
            "TagResource",
            {"ResourceARN": rule_arn, "Tags": [{"Key": "k", "Value": "v"}]},
        )
        await handle_events_request(req2, "us-east-1", "123456789012")

        # Delete rule
        req3 = _make_request("DeleteRule", {"Name": "tagged-rule"})
        await handle_events_request(req3, "us-east-1", "123456789012")

        # Tags should be gone
        req4 = _make_request("ListTagsForResource", {"ResourceARN": rule_arn})
        resp4 = await handle_events_request(req4, "us-east-1", "123456789012")
        data = json.loads(resp4.body)
        assert data["Tags"] == []

    @pytest.mark.asyncio
    async def test_delete_archive_cleans_up_tags(self):
        bus_arn = "arn:aws:events:us-east-1:123456789012:event-bus/default"
        req1 = _make_request(
            "CreateArchive",
            {"ArchiveName": "tagged-archive", "EventSourceArn": bus_arn},
        )
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        archive_arn = json.loads(resp1.body)["ArchiveArn"]

        req2 = _make_request(
            "TagResource",
            {"ResourceARN": archive_arn, "Tags": [{"Key": "k", "Value": "v"}]},
        )
        await handle_events_request(req2, "us-east-1", "123456789012")

        # Delete archive
        req3 = _make_request("DeleteArchive", {"ArchiveName": "tagged-archive"})
        await handle_events_request(req3, "us-east-1", "123456789012")

        # Tags should be gone
        req4 = _make_request("ListTagsForResource", {"ResourceARN": archive_arn})
        resp4 = await handle_events_request(req4, "us-east-1", "123456789012")
        data = json.loads(resp4.body)
        assert data["Tags"] == []


# ---------------------------------------------------------------------------
# Categorical bug: Enable/disable nonexistent rule silently succeeds
# ---------------------------------------------------------------------------


class TestEnableDisableNonexistentRule:
    """EnableRule and DisableRule should raise ResourceNotFoundException for missing rules."""

    @pytest.mark.asyncio
    async def test_enable_nonexistent_rule_returns_error(self):
        req = _make_request("EnableRule", {"Name": "ghost-rule"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceNotFoundException"

    @pytest.mark.asyncio
    async def test_disable_nonexistent_rule_returns_error(self):
        req = _make_request("DisableRule", {"Name": "ghost-rule"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Categorical bug: Connections/API destinations not cleared between tests
# ---------------------------------------------------------------------------


class TestConnectionsApiDestinationsIsolation:
    """Connections and API destinations are module-level globals. They must be
    cleared between tests (the fixture handles this) and the operations must work."""

    @pytest.mark.asyncio
    async def test_connection_lifecycle(self):
        req1 = _make_request(
            "CreateConnection",
            {"Name": "my-conn", "AuthorizationType": "API_KEY", "AuthParameters": {}},
        )
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        assert resp1.status_code == 200

        req2 = _make_request("DescribeConnection", {"Name": "my-conn"})
        resp2 = await handle_events_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 200
        assert json.loads(resp2.body)["Name"] == "my-conn"

        req3 = _make_request("DeleteConnection", {"Name": "my-conn"})
        resp3 = await handle_events_request(req3, "us-east-1", "123456789012")
        assert resp3.status_code == 200

        # Should be gone now
        req4 = _make_request("DescribeConnection", {"Name": "my-conn"})
        resp4 = await handle_events_request(req4, "us-east-1", "123456789012")
        assert resp4.status_code == 400  # AWS uses 400 for ResourceNotFoundException

    @pytest.mark.asyncio
    async def test_api_destination_lifecycle(self):
        req1 = _make_request(
            "CreateApiDestination",
            {
                "Name": "my-dest",
                "ConnectionArn": "arn:aws:events:us-east-1:123:connection/c",
                "InvocationEndpoint": "https://example.com",
                "HttpMethod": "POST",
            },
        )
        resp1 = await handle_events_request(req1, "us-east-1", "123456789012")
        assert resp1.status_code == 200

        req2 = _make_request("DescribeApiDestination", {"Name": "my-dest"})
        resp2 = await handle_events_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 200

    @pytest.mark.asyncio
    async def test_connections_isolated_between_tests(self):
        """This test verifies the fixture clears _connections.
        If run after test_connection_lifecycle, 'my-conn' should not exist."""
        req = _make_request("DescribeConnection", {"Name": "my-conn"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400  # Should be gone (fixture clears)


# ---------------------------------------------------------------------------
# Categorical bug: Inconsistent error status codes
# ---------------------------------------------------------------------------


class TestPublishEventToBus:
    """Unit tests for the publish_event_to_bus() helper (called by S3, etc.)."""

    def test_no_bus_returns_silently(self):
        """If the named bus does not exist, publish_event_to_bus returns without error."""
        from robotocore.services.events.provider import publish_event_to_bus

        # "nonexistent-bus" has never been created — should be a no-op
        publish_event_to_bus({"source": "aws.s3"}, "us-east-1", "123456789012", "nonexistent-bus")

    def test_default_bus_exists_is_noop_with_no_rules(self):
        """Publishing to the default bus with no matching rules completes without error."""
        from robotocore.services.events.provider import _get_store, publish_event_to_bus

        store = _get_store("us-east-1", "123456789012")
        bus = store.get_bus("default")
        assert bus is not None  # default bus always exists
        event = {"source": "aws.s3", "detail-type": "Object Created", "detail": {}}
        # Should not raise; no rules means no dispatch
        publish_event_to_bus(event, "us-east-1", "123456789012")

    @pytest.mark.asyncio
    async def test_matching_rule_dispatches_target(self):
        """A rule whose pattern matches the event should attempt dispatch to its targets."""
        from unittest.mock import patch

        from robotocore.services.events.provider import (
            handle_events_request,
            publish_event_to_bus,
        )

        # Create a rule that matches source aws.s3
        req = _make_request(
            "PutRule",
            {
                "Name": "s3-rule",
                "EventPattern": json.dumps({"source": ["aws.s3"]}),
                "State": "ENABLED",
            },
        )
        await handle_events_request(req, "us-east-1", "123456789012")

        # Add an SQS target
        target_req = _make_request(
            "PutTargets",
            {
                "Rule": "s3-rule",
                "Targets": [{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q"}],
            },
        )
        await handle_events_request(target_req, "us-east-1", "123456789012")

        event = {
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {},
            "account": "123456789012",
            "region": "us-east-1",
        }
        # Patch the SQS dispatch so we can verify it was called (rule matched)
        with patch("robotocore.services.events.provider._invoke_sqs_target") as mock_sqs:
            publish_event_to_bus(event, "us-east-1", "123456789012")
        mock_sqs.assert_called_once()

    def test_non_matching_rule_does_not_dispatch(self):
        """An event that does not match any rule pattern should not dispatch targets."""
        from robotocore.services.events.provider import (
            clear_invocation_log,
            get_invocation_log,
            publish_event_to_bus,
        )

        clear_invocation_log()
        # Publish an event with a source that no rule matches
        event = {
            "source": "aws.ec2",
            "detail-type": "EC2 Instance State-change Notification",
            "detail": {},
        }
        publish_event_to_bus(event, "us-east-1", "123456789012")
        log = get_invocation_log()
        # No targets dispatched (we haven't created any ec2 rules)
        assert log == []


class TestConsistentErrorCodes:
    """AWS EventBridge uses HTTP 400 for ResourceNotFoundException (not 404).
    All operations must be consistent."""

    @pytest.mark.asyncio
    async def test_describe_connection_not_found_is_400(self):
        req = _make_request("DescribeConnection", {"Name": "nope"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        # AWS EventBridge JSON protocol returns 400 for ResourceNotFoundException
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_delete_connection_not_found_is_400(self):
        req = _make_request("DeleteConnection", {"Name": "nope"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_describe_api_destination_not_found_is_400(self):
        req = _make_request("DescribeApiDestination", {"Name": "nope"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_update_archive_not_found_is_400(self):
        req = _make_request("UpdateArchive", {"ArchiveName": "nope"})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
