"""Categorical tests: tag operations across all REST-path native providers.

Every provider with REST tag endpoints (/tags/{arn}) must:
1. Return tags set during resource creation via ListTagsForResource
2. Store tags set via TagResource and return them via ListTagsForResource
3. Remove tags via UntagResource

This catches the category of bug where tag endpoints are stubbed as no-ops.
"""

import json
from urllib.parse import quote

import pytest
from starlette.requests import Request

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REGION = "us-east-1"
ACCOUNT = "123456789012"


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


# ---------------------------------------------------------------------------
# AppSync tag tests
# ---------------------------------------------------------------------------


class TestAppSyncTags:
    """Tags for AppSync GraphQL APIs and Event APIs."""

    @pytest.fixture(autouse=True)
    def _clear(self):
        from robotocore.services.appsync.provider import _stores

        _stores.clear()
        yield
        _stores.clear()

    async def _handle(self, method, path, body=None, query=""):
        from robotocore.services.appsync.provider import handle_appsync_request

        req = _make_request(method, path, body, query)
        return await handle_appsync_request(req, REGION, ACCOUNT)

    async def _create_graphql_api(self, tags=None):
        params = {"name": "TestAPI", "authenticationType": "API_KEY"}
        if tags:
            params["tags"] = tags
        resp = await self._handle("POST", "/v1/apis", params)
        data = json.loads(resp.body)
        return data["graphqlApi"]

    async def _create_event_api(self, tags=None):
        params = {"name": "TestEventAPI"}
        if tags:
            params["tags"] = tags
        resp = await self._handle("POST", "/v2/apis", params)
        data = json.loads(resp.body)
        return data["api"]

    @pytest.mark.asyncio
    async def test_graphql_api_creation_tags_returned_by_list(self):
        """Tags set during CreateGraphqlApi must be returned by ListTagsForResource."""
        api = await self._create_graphql_api(tags={"env": "test", "team": "infra"})
        arn = api["arn"]
        resp = await self._handle("GET", f"/v1/tags/{quote(arn, safe='')}")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["tags"]["env"] == "test"
        assert data["tags"]["team"] == "infra"

    @pytest.mark.asyncio
    async def test_graphql_api_tag_resource_roundtrip(self):
        """TagResource + ListTagsForResource must round-trip."""
        api = await self._create_graphql_api()
        arn = api["arn"]
        # Tag the resource
        resp = await self._handle(
            "POST", f"/v1/tags/{quote(arn, safe='')}", {"tags": {"color": "blue"}}
        )
        assert resp.status_code == 200
        # List tags
        resp = await self._handle("GET", f"/v1/tags/{quote(arn, safe='')}")
        data = json.loads(resp.body)
        assert data["tags"]["color"] == "blue"

    @pytest.mark.asyncio
    async def test_graphql_api_untag_resource(self):
        """UntagResource must remove tags."""
        api = await self._create_graphql_api(tags={"env": "test", "team": "infra"})
        arn = api["arn"]
        # Untag
        resp = await self._handle("DELETE", f"/v1/tags/{quote(arn, safe='')}", query="tagKeys=env")
        assert resp.status_code == 200
        # Verify
        resp = await self._handle("GET", f"/v1/tags/{quote(arn, safe='')}")
        data = json.loads(resp.body)
        assert "env" not in data["tags"]
        assert data["tags"]["team"] == "infra"

    @pytest.mark.asyncio
    async def test_event_api_creation_tags_returned_by_list(self):
        """Tags set during CreateApi (Event API) must be returned by ListTagsForResource."""
        api = await self._create_event_api(tags={"env": "staging"})
        arn = api["apiArn"]
        resp = await self._handle("GET", f"/v1/tags/{quote(arn, safe='')}")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["tags"]["env"] == "staging"

    @pytest.mark.asyncio
    async def test_event_api_tag_resource_roundtrip(self):
        """TagResource on Event API must round-trip."""
        api = await self._create_event_api()
        arn = api["apiArn"]
        await self._handle("POST", f"/v1/tags/{quote(arn, safe='')}", {"tags": {"owner": "jack"}})
        resp = await self._handle("GET", f"/v1/tags/{quote(arn, safe='')}")
        data = json.loads(resp.body)
        assert data["tags"]["owner"] == "jack"

    @pytest.mark.asyncio
    async def test_tag_resource_merges_not_replaces(self):
        """TagResource should merge with existing tags, not replace them."""
        api = await self._create_graphql_api(tags={"env": "test"})
        arn = api["arn"]
        await self._handle("POST", f"/v1/tags/{quote(arn, safe='')}", {"tags": {"team": "infra"}})
        resp = await self._handle("GET", f"/v1/tags/{quote(arn, safe='')}")
        data = json.loads(resp.body)
        assert data["tags"]["env"] == "test"
        assert data["tags"]["team"] == "infra"

    @pytest.mark.asyncio
    async def test_tag_nonexistent_resource(self):
        """TagResource on nonexistent ARN should return 404."""
        fake_arn = f"arn:aws:appsync:{REGION}:{ACCOUNT}:apis/nonexistent"
        resp = await self._handle(
            "POST",
            f"/v1/tags/{quote(fake_arn, safe='')}",
            {"tags": {"x": "y"}},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# API Gateway V2 tag tests
# ---------------------------------------------------------------------------


class TestApiGatewayV2Tags:
    """Tags for API Gateway V2 APIs."""

    @pytest.fixture(autouse=True)
    def _clear(self):
        from robotocore.services.apigatewayv2.provider import (
            _api_mappings,
            _apis,
            _authorizers,
            _connections,
            _deployments,
            _domain_names,
            _integrations,
            _models,
            _routes,
            _stages,
            _vpc_links,
        )

        stores = (
            _apis,
            _routes,
            _integrations,
            _stages,
            _authorizers,
            _deployments,
            _connections,
            _vpc_links,
            _domain_names,
            _api_mappings,
            _models,
        )
        for s in stores:
            s.clear()
        yield
        for s in stores:
            s.clear()

    async def _handle(self, method, path, body=None, query=""):
        from robotocore.services.apigatewayv2.provider import handle_apigatewayv2_request

        req = _make_request(method, path, body, query)
        return await handle_apigatewayv2_request(req, REGION, ACCOUNT)

    async def _create_api(self, tags=None):
        params = {"name": "TestAPI", "protocolType": "HTTP"}
        if tags:
            params["tags"] = tags
        resp = await self._handle("POST", "/v2/apis", params)
        data = json.loads(resp.body)
        return data

    @pytest.mark.asyncio
    async def test_api_creation_tags_returned_by_list(self):
        """Tags set during CreateApi must be returned by GetTags."""
        api = await self._create_api(tags={"env": "test", "team": "infra"})
        api_id = api["apiId"]
        arn = f"arn:aws:apigateway:{REGION}::/apis/{api_id}"
        resp = await self._handle("GET", f"/v2/tags/{quote(arn, safe='')}")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["tags"]["env"] == "test"
        assert data["tags"]["team"] == "infra"

    @pytest.mark.asyncio
    async def test_tag_resource_roundtrip(self):
        """TagResource + GetTags must round-trip."""
        api = await self._create_api()
        api_id = api["apiId"]
        arn = f"arn:aws:apigateway:{REGION}::/apis/{api_id}"
        await self._handle("POST", f"/v2/tags/{quote(arn, safe='')}", {"tags": {"color": "blue"}})
        resp = await self._handle("GET", f"/v2/tags/{quote(arn, safe='')}")
        data = json.loads(resp.body)
        assert data["tags"]["color"] == "blue"

    @pytest.mark.asyncio
    async def test_untag_resource(self):
        """UntagResource must remove tags."""
        api = await self._create_api(tags={"env": "test", "team": "infra"})
        api_id = api["apiId"]
        arn = f"arn:aws:apigateway:{REGION}::/apis/{api_id}"
        resp = await self._handle("DELETE", f"/v2/tags/{quote(arn, safe='')}", query="tagKeys=env")
        assert resp.status_code in (200, 204)
        resp = await self._handle("GET", f"/v2/tags/{quote(arn, safe='')}")
        data = json.loads(resp.body)
        assert "env" not in data["tags"]
        assert data["tags"]["team"] == "infra"


# ---------------------------------------------------------------------------
# Scheduler tag tests
# ---------------------------------------------------------------------------


class TestSchedulerTags:
    """Tags for EventBridge Scheduler schedules."""

    @pytest.fixture(autouse=True)
    def _clear(self):
        from robotocore.services.scheduler.provider import _groups, _schedules

        _schedules.clear()
        _groups.clear()
        yield
        _schedules.clear()
        _groups.clear()

    async def _handle(self, method, path, body=None, query=""):
        from robotocore.services.scheduler.provider import handle_scheduler_request

        req = _make_request(method, path, body, query)
        return await handle_scheduler_request(req, REGION, ACCOUNT)

    async def _create_schedule(self, tags=None):
        params = {
            "Name": "test-schedule",
            "ScheduleExpression": "rate(1 hour)",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {
                "Arn": f"arn:aws:sqs:{REGION}:{ACCOUNT}:test-queue",
                "RoleArn": f"arn:aws:iam::{ACCOUNT}:role/test-role",
            },
        }
        if tags:
            params["Tags"] = tags
        resp = await self._handle("POST", "/schedules/test-schedule", params)
        data = json.loads(resp.body)
        return data

    @pytest.mark.asyncio
    async def test_schedule_creation_tags_returned_by_list(self):
        """Tags set during CreateSchedule must be returned by ListTagsForResource."""
        schedule = await self._create_schedule(tags=[{"Key": "env", "Value": "test"}])
        arn = schedule["ScheduleArn"]
        resp = await self._handle("GET", f"/tags/{quote(arn, safe='')}")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        tag_map = {t["Key"]: t["Value"] for t in data["Tags"]}
        assert tag_map["env"] == "test"

    @pytest.mark.asyncio
    async def test_tag_resource_roundtrip(self):
        """TagResource + ListTagsForResource must round-trip."""
        schedule = await self._create_schedule()
        arn = schedule["ScheduleArn"]
        await self._handle(
            "POST",
            f"/tags/{quote(arn, safe='')}",
            {"Tags": [{"Key": "color", "Value": "blue"}]},
        )
        resp = await self._handle("GET", f"/tags/{quote(arn, safe='')}")
        data = json.loads(resp.body)
        tag_map = {t["Key"]: t["Value"] for t in data["Tags"]}
        assert tag_map["color"] == "blue"

    @pytest.mark.asyncio
    async def test_untag_resource(self):
        """UntagResource must remove tags."""
        schedule = await self._create_schedule(
            tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "infra"}]
        )
        arn = schedule["ScheduleArn"]
        resp = await self._handle("DELETE", f"/tags/{quote(arn, safe='')}", query="TagKeys=env")
        assert resp.status_code == 200
        resp = await self._handle("GET", f"/tags/{quote(arn, safe='')}")
        data = json.loads(resp.body)
        tag_map = {t["Key"]: t["Value"] for t in data["Tags"]}
        assert "env" not in tag_map
        assert tag_map["team"] == "infra"
