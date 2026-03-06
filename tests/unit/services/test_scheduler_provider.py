"""Unit tests for the EventBridge Scheduler provider."""

import json

import pytest
from starlette.requests import Request

from robotocore.services.scheduler.provider import (
    SchedulerError,
    _error,
    _get_groups,
    _groups,
    _json_response,
    _schedules,
    handle_scheduler_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(method: str, path: str, body: dict | None = None, query: str = ""):
    scope = {
        "type": "http",
        "method": method.upper(),
        "path": path,
        "query_string": query.encode(),
        "headers": [],
    }
    body_bytes = json.dumps(body).encode() if body else b""

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


@pytest.fixture(autouse=True)
def _clear_state():
    _schedules.clear()
    _groups.clear()
    yield
    _schedules.clear()
    _groups.clear()


# ---------------------------------------------------------------------------
# SchedulerError
# ---------------------------------------------------------------------------


class TestSchedulerError:
    def test_default_status(self):
        e = SchedulerError("Code", "msg")
        assert e.status == 400

    def test_custom_status(self):
        e = SchedulerError("Code", "msg", 409)
        assert e.status == 409


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json_response({"key": "val"})
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"key": "val"}

    def test_error_response(self):
        resp = _error("TestCode", "test msg", 404)
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "TestCode"
        assert data["Message"] == "test msg"


# ---------------------------------------------------------------------------
# Schedule CRUD via handle_scheduler_request
# ---------------------------------------------------------------------------


class TestScheduleCRUD:
    @pytest.mark.asyncio
    async def test_create_schedule(self):
        req = _make_request(
            "POST",
            "/schedules/my-schedule",
            {
                "ScheduleExpression": "rate(1 hour)",
                "Target": {"Arn": "arn:aws:lambda:us-east-1:123:function:fn"},
                "FlexibleTimeWindow": {"Mode": "OFF"},
            },
        )
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "ScheduleArn" in data

    @pytest.mark.asyncio
    async def test_get_schedule(self):
        # Create first
        req1 = _make_request(
            "POST",
            "/schedules/sched1",
            {
                "ScheduleExpression": "rate(5 minutes)",
                "Target": {"Arn": "arn:aws:sqs:us-east-1:123:queue"},
                "FlexibleTimeWindow": {"Mode": "OFF"},
            },
        )
        await handle_scheduler_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("GET", "/schedules/sched1")
        resp = await handle_scheduler_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Name"] == "sched1"
        assert data["ScheduleExpression"] == "rate(5 minutes)"

    @pytest.mark.asyncio
    async def test_get_nonexistent_schedule(self):
        req = _make_request("GET", "/schedules/nope")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_update_schedule(self):
        req1 = _make_request(
            "POST",
            "/schedules/sched1",
            {
                "ScheduleExpression": "rate(1 hour)",
                "Target": {"Arn": "arn:test"},
                "FlexibleTimeWindow": {"Mode": "OFF"},
            },
        )
        await handle_scheduler_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "PUT",
            "/schedules/sched1",
            {"ScheduleExpression": "rate(2 hours)", "State": "DISABLED"},
        )
        resp = await handle_scheduler_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200

        req3 = _make_request("GET", "/schedules/sched1")
        resp3 = await handle_scheduler_request(req3, "us-east-1", "123456789012")
        data = json.loads(resp3.body)
        assert data["ScheduleExpression"] == "rate(2 hours)"
        assert data["State"] == "DISABLED"

    @pytest.mark.asyncio
    async def test_update_nonexistent_schedule(self):
        req = _make_request("PUT", "/schedules/nope", {"ScheduleExpression": "rate(1 hour)"})
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_schedule(self):
        req1 = _make_request(
            "POST",
            "/schedules/sched1",
            {
                "ScheduleExpression": "rate(1 hour)",
                "Target": {"Arn": "arn:test"},
                "FlexibleTimeWindow": {"Mode": "OFF"},
            },
        )
        await handle_scheduler_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("DELETE", "/schedules/sched1")
        resp = await handle_scheduler_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200

        req3 = _make_request("GET", "/schedules/sched1")
        resp3 = await handle_scheduler_request(req3, "us-east-1", "123456789012")
        assert resp3.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_nonexistent_schedule(self):
        req = _make_request("DELETE", "/schedules/nope")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_list_schedules(self):
        for name in ("s1", "s2"):
            req = _make_request(
                "POST",
                f"/schedules/{name}",
                {
                    "ScheduleExpression": "rate(1 hour)",
                    "Target": {"Arn": "arn:test"},
                    "FlexibleTimeWindow": {"Mode": "OFF"},
                },
            )
            await handle_scheduler_request(req, "us-east-1", "123456789012")

        req = _make_request("GET", "/schedules/", query="")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        names = [s["Name"] for s in data["Schedules"]]
        assert "s1" in names
        assert "s2" in names

    @pytest.mark.asyncio
    async def test_list_schedules_with_group_filter(self):
        req1 = _make_request(
            "POST",
            "/schedules/s1",
            {
                "ScheduleExpression": "rate(1 hour)",
                "Target": {"Arn": "arn:test"},
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "GroupName": "mygroup",
            },
        )
        await handle_scheduler_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "POST",
            "/schedules/s2",
            {
                "ScheduleExpression": "rate(1 hour)",
                "Target": {"Arn": "arn:test"},
                "FlexibleTimeWindow": {"Mode": "OFF"},
                "GroupName": "other",
            },
        )
        await handle_scheduler_request(req2, "us-east-1", "123456789012")

        req = _make_request("GET", "/schedules/", query="GroupName=mygroup")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        names = [s["Name"] for s in data["Schedules"]]
        assert names == ["s1"]


# ---------------------------------------------------------------------------
# Schedule Group CRUD
# ---------------------------------------------------------------------------


class TestScheduleGroupCRUD:
    @pytest.mark.asyncio
    async def test_create_group(self):
        req = _make_request("POST", "/schedule-groups/mygroup", {})
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert "ScheduleGroupArn" in json.loads(resp.body)

    @pytest.mark.asyncio
    async def test_create_duplicate_group(self):
        req = _make_request("POST", "/schedule-groups/mygroup", {})
        await handle_scheduler_request(req, "us-east-1", "123456789012")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 409

    @pytest.mark.asyncio
    async def test_get_group(self):
        req1 = _make_request("POST", "/schedule-groups/mygroup", {})
        await handle_scheduler_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("GET", "/schedule-groups/mygroup")
        resp = await handle_scheduler_request(req2, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert data["Name"] == "mygroup"

    @pytest.mark.asyncio
    async def test_get_nonexistent_group(self):
        req = _make_request("GET", "/schedule-groups/nope")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_group(self):
        req1 = _make_request("POST", "/schedule-groups/mygroup", {})
        await handle_scheduler_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("DELETE", "/schedule-groups/mygroup")
        resp = await handle_scheduler_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_delete_default_group_fails(self):
        # Ensure default group exists by accessing it
        _get_groups("us-east-1")
        req = _make_request("DELETE", "/schedule-groups/default")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_list_groups_includes_default(self):
        req = _make_request("GET", "/schedule-groups/")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        names = [g["Name"] for g in data["ScheduleGroups"]]
        assert "default" in names


# ---------------------------------------------------------------------------
# Unknown path
# ---------------------------------------------------------------------------


class TestUnknownPath:
    @pytest.mark.asyncio
    async def test_unknown_path_returns_400(self):
        req = _make_request("GET", "/unknown/path")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_tags_get(self):
        req = _make_request("GET", "/tags/arn:aws:scheduler:us-east-1:123:schedule/x")
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"Tags": []}

    @pytest.mark.asyncio
    async def test_tags_post(self):
        req = _make_request(
            "POST",
            "/tags/arn:aws:scheduler:us-east-1:123:schedule/x",
            {"Tags": [{"Key": "env", "Value": "test"}]},
        )
        resp = await handle_scheduler_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
