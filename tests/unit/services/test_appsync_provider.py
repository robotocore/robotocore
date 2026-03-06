"""Unit tests for the AppSync provider."""

import json

import pytest
from starlette.requests import Request

from robotocore.services.appsync.provider import (
    AppSyncError,
    _error,
    _json_response,
    _stores,
    handle_appsync_request,
)

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


@pytest.fixture(autouse=True)
def _clear_stores():
    _stores.clear()
    yield
    _stores.clear()


async def _create_api(name: str = "TestAPI") -> str:
    """Create an API and return its apiId."""
    req = _make_request("POST", "/v1/apis", {"name": name})
    resp = await handle_appsync_request(req, REGION, ACCOUNT)
    data = json.loads(resp.body)
    return data["graphqlApi"]["apiId"]


# ---------------------------------------------------------------------------
# Error / response helpers
# ---------------------------------------------------------------------------


class TestAppSyncError:
    def test_default_status(self):
        e = AppSyncError("Code", "msg")
        assert e.status == 400

    def test_custom_status(self):
        e = AppSyncError("Code", "msg", 404)
        assert e.status == 404


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json_response({"key": "val"})
        assert resp.status_code == 200

    def test_error_response(self):
        resp = _error("Code", "msg", 400)
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "Code"


# ---------------------------------------------------------------------------
# GraphQL API CRUD
# ---------------------------------------------------------------------------


class TestGraphqlApiCrud:
    @pytest.mark.asyncio
    async def test_create_api(self):
        req = _make_request("POST", "/v1/apis", {"name": "MyAPI"})
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["graphqlApi"]["name"] == "MyAPI"
        assert "apiId" in data["graphqlApi"]

    @pytest.mark.asyncio
    async def test_create_api_missing_name(self):
        req = _make_request("POST", "/v1/apis", {})
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_get_api(self):
        api_id = await _create_api()
        req = _make_request("GET", f"/v1/apis/{api_id}")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["graphqlApi"]["apiId"] == api_id

    @pytest.mark.asyncio
    async def test_get_nonexistent_api(self):
        req = _make_request("GET", "/v1/apis/nope")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_list_apis(self):
        await _create_api("API1")
        await _create_api("API2")
        req = _make_request("GET", "/v1/apis")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["graphqlApis"]) == 2

    @pytest.mark.asyncio
    async def test_update_api(self):
        api_id = await _create_api()
        req = _make_request("POST", f"/v1/apis/{api_id}", {"name": "Updated"})
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["graphqlApi"]["name"] == "Updated"

    @pytest.mark.asyncio
    async def test_delete_api(self):
        api_id = await _create_api()
        req = _make_request("DELETE", f"/v1/apis/{api_id}")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

        req2 = _make_request("GET", f"/v1/apis/{api_id}")
        resp2 = await handle_appsync_request(req2, REGION, ACCOUNT)
        assert resp2.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_nonexistent_api(self):
        req = _make_request("DELETE", "/v1/apis/nope")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------


class TestApiKeys:
    @pytest.mark.asyncio
    async def test_create_api_key(self):
        api_id = await _create_api()
        req = _make_request("POST", f"/v1/apis/{api_id}/apikeys", {})
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "id" in data["apiKey"]

    @pytest.mark.asyncio
    async def test_list_api_keys(self):
        api_id = await _create_api()
        await handle_appsync_request(
            _make_request("POST", f"/v1/apis/{api_id}/apikeys", {}), REGION, ACCOUNT
        )
        req = _make_request("GET", f"/v1/apis/{api_id}/apikeys")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["apiKeys"]) == 1

    @pytest.mark.asyncio
    async def test_delete_api_key(self):
        api_id = await _create_api()
        create_resp = await handle_appsync_request(
            _make_request("POST", f"/v1/apis/{api_id}/apikeys", {}), REGION, ACCOUNT
        )
        key_id = json.loads(create_resp.body)["apiKey"]["id"]

        req = _make_request("DELETE", f"/v1/apis/{api_id}/apikeys/{key_id}")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class TestSchema:
    @pytest.mark.asyncio
    async def test_start_schema_creation(self):
        api_id = await _create_api()
        req = _make_request(
            "POST", f"/v1/apis/{api_id}/schemacreation",
            {"definition": "type Query { hello: String }"},
        )
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["status"] == "SUCCESS"

    @pytest.mark.asyncio
    async def test_get_schema_creation_status(self):
        api_id = await _create_api()
        await handle_appsync_request(
            _make_request(
                "POST", f"/v1/apis/{api_id}/schemacreation",
                {"definition": "type Query { hello: String }"},
            ), REGION, ACCOUNT
        )
        req = _make_request("GET", f"/v1/apis/{api_id}/schemacreation")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["status"] == "SUCCESS"

    @pytest.mark.asyncio
    async def test_get_schema_not_created(self):
        api_id = await _create_api()
        req = _make_request("GET", f"/v1/apis/{api_id}/schemacreation")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Resolvers
# ---------------------------------------------------------------------------


class TestResolvers:
    @pytest.mark.asyncio
    async def test_create_resolver(self):
        api_id = await _create_api()
        req = _make_request(
            "POST", f"/v1/apis/{api_id}/types/Query/resolvers",
            {"fieldName": "hello", "dataSourceName": "myds"},
        )
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["resolver"]["fieldName"] == "hello"

    @pytest.mark.asyncio
    async def test_get_resolver(self):
        api_id = await _create_api()
        await handle_appsync_request(
            _make_request(
                "POST", f"/v1/apis/{api_id}/types/Query/resolvers",
                {"fieldName": "hello"},
            ), REGION, ACCOUNT
        )
        req = _make_request("GET", f"/v1/apis/{api_id}/types/Query/resolvers/hello")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_resolvers(self):
        api_id = await _create_api()
        for fn in ("hello", "world"):
            await handle_appsync_request(
                _make_request(
                    "POST", f"/v1/apis/{api_id}/types/Query/resolvers",
                    {"fieldName": fn},
                ), REGION, ACCOUNT
            )
        req = _make_request("GET", f"/v1/apis/{api_id}/types/Query/resolvers")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["resolvers"]) == 2

    @pytest.mark.asyncio
    async def test_delete_resolver(self):
        api_id = await _create_api()
        await handle_appsync_request(
            _make_request(
                "POST", f"/v1/apis/{api_id}/types/Query/resolvers",
                {"fieldName": "hello"},
            ), REGION, ACCOUNT
        )
        req = _make_request("DELETE", f"/v1/apis/{api_id}/types/Query/resolvers/hello")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Data Sources
# ---------------------------------------------------------------------------


class TestDataSources:
    @pytest.mark.asyncio
    async def test_create_data_source(self):
        api_id = await _create_api()
        req = _make_request(
            "POST", f"/v1/apis/{api_id}/datasources",
            {"name": "myds", "type": "AMAZON_DYNAMODB"},
        )
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["dataSource"]["name"] == "myds"

    @pytest.mark.asyncio
    async def test_create_duplicate_data_source(self):
        api_id = await _create_api()
        params = {"name": "myds", "type": "NONE"}
        await handle_appsync_request(
            _make_request("POST", f"/v1/apis/{api_id}/datasources", params),
            REGION, ACCOUNT
        )
        resp = await handle_appsync_request(
            _make_request("POST", f"/v1/apis/{api_id}/datasources", params),
            REGION, ACCOUNT
        )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_get_data_source(self):
        api_id = await _create_api()
        await handle_appsync_request(
            _make_request(
                "POST", f"/v1/apis/{api_id}/datasources", {"name": "myds"}
            ), REGION, ACCOUNT
        )
        req = _make_request("GET", f"/v1/apis/{api_id}/datasources/myds")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_data_sources(self):
        api_id = await _create_api()
        for n in ("ds1", "ds2"):
            await handle_appsync_request(
                _make_request(
                    "POST", f"/v1/apis/{api_id}/datasources", {"name": n}
                ), REGION, ACCOUNT
            )
        req = _make_request("GET", f"/v1/apis/{api_id}/datasources")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["dataSources"]) == 2

    @pytest.mark.asyncio
    async def test_delete_data_source(self):
        api_id = await _create_api()
        await handle_appsync_request(
            _make_request(
                "POST", f"/v1/apis/{api_id}/datasources", {"name": "myds"}
            ), REGION, ACCOUNT
        )
        req = _make_request("DELETE", f"/v1/apis/{api_id}/datasources/myds")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


class TestTypes:
    @pytest.mark.asyncio
    async def test_create_type(self):
        api_id = await _create_api()
        req = _make_request(
            "POST", f"/v1/apis/{api_id}/types",
            {"definition": "type Query { hello: String }", "format": "SDL"},
        )
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["type"]["name"] == "Query"

    @pytest.mark.asyncio
    async def test_get_type(self):
        api_id = await _create_api()
        await handle_appsync_request(
            _make_request(
                "POST", f"/v1/apis/{api_id}/types",
                {"definition": "type Mutation { create: String }"},
            ), REGION, ACCOUNT
        )
        req = _make_request("GET", f"/v1/apis/{api_id}/types/Mutation")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_types(self):
        api_id = await _create_api()
        for defn in ("type Query { a: String }", "type Mutation { b: String }"):
            await handle_appsync_request(
                _make_request(
                    "POST", f"/v1/apis/{api_id}/types", {"definition": defn}
                ), REGION, ACCOUNT
            )
        req = _make_request("GET", f"/v1/apis/{api_id}/types")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["types"]) == 2


# ---------------------------------------------------------------------------
# Tags and unknown path
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_tags_get(self):
        req = _make_request("GET", "/v1/tags/arn:aws:appsync:us-east-1:123:apis/test")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_unknown_path(self):
        req = _make_request("GET", "/v1/unknown")
        resp = await handle_appsync_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400
