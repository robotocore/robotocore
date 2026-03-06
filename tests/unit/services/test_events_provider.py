"""Unit tests for the EventBridge (events) provider."""

import json

import pytest

from robotocore.services.events.provider import (
    EventsError,
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
    clear_invocation_log()
    yield
    _stores.clear()
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
    async def test_unknown_operation_returns_400(self):
        req = _make_request("NonExistentOp", {})
        resp = await handle_events_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "UnknownOperation"

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


class TestInvocationLog:
    def test_get_and_clear(self):
        assert get_invocation_log() == []
        from robotocore.services.events.provider import _log_invocation

        _log_invocation("lambda", "arn:test", "payload")
        assert len(get_invocation_log()) == 1
        clear_invocation_log()
        assert get_invocation_log() == []
