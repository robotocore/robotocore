"""Failing tests exposing correctness bugs in the DynamoDB native provider.

Each test documents a specific bug and is expected to FAIL against the current code.
Do NOT fix the provider -- only add tests here.
"""

import json
from unittest.mock import AsyncMock, patch

import pytest
from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.dynamodb.provider import (
    _global_tables,
    handle_dynamodb_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(target: str, body: dict) -> Request:
    """Build a fake Starlette Request with the given X-Amz-Target and JSON body."""
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


def _mock_moto_response(status_code: int = 200, body: bytes = b"{}") -> Response:
    return Response(content=body, status_code=status_code)


# ---------------------------------------------------------------------------
# Bug 1: PutItem stream event is always INSERT, never MODIFY
# ---------------------------------------------------------------------------


class TestPutItemStreamEventType:
    """Bug: PutItem always fires INSERT stream event even when overwriting an
    existing item. Real AWS fires MODIFY when the item already exists and
    INSERT only for new items. The comment on line 77 of provider.py
    acknowledges this: 'Could be MODIFY if item existed, but INSERT is close
    enough'. It is not close enough -- consumers that distinguish INSERT from
    MODIFY (e.g., replication pipelines) will behave incorrectly.
    """

    @pytest.mark.asyncio
    async def test_put_item_overwrite_should_fire_modify(self):
        """When PutItem overwrites an existing item the stream event must be MODIFY,
        not INSERT."""
        body = {"TableName": "mytable", "Item": {"id": {"S": "1"}, "val": {"S": "new"}}}
        req = _make_request("DynamoDB_20120810.PutItem", body)
        # Simulate Moto returning a successful PutItem with Attributes (indicating overwrite)
        moto_resp_body = json.dumps(
            {"Attributes": {"id": {"S": "1"}, "val": {"S": "old"}}}
        ).encode()
        mock_resp = _mock_moto_response(200, moto_resp_body)

        with (
            patch(
                "robotocore.services.dynamodb.provider.forward_to_moto",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ),
            patch("robotocore.services.dynamodbstreams.hooks.notify_table_change") as mock_notify,
        ):
            await handle_dynamodb_request(req, "us-east-1", "123456789012")

        # The provider currently always sends INSERT. For an overwrite it should be MODIFY.
        call_kw = mock_notify.call_args[1]
        assert call_kw["event_name"] == "MODIFY", (
            f"Expected MODIFY for overwrite, got {call_kw['event_name']}"
        )


# ---------------------------------------------------------------------------
# Bug 2: _list_global_tables ignores Limit parameter (no pagination)
# ---------------------------------------------------------------------------


class TestListGlobalTablesPagination:
    """Bug: ListGlobalTables ignores the Limit and ExclusiveStartGlobalTableName
    parameters. Real AWS supports pagination via these parameters. The provider
    returns all tables regardless of Limit.
    """

    @pytest.fixture(autouse=True)
    def _clear_global_tables(self):
        _global_tables.clear()
        yield
        _global_tables.clear()

    @pytest.mark.asyncio
    async def test_list_global_tables_respects_limit(self):
        """ListGlobalTables with Limit=1 should return at most 1 table."""
        # Create 3 global tables
        for name in ["gt-alpha", "gt-beta", "gt-gamma"]:
            req = _make_request(
                "DynamoDB_20120810.CreateGlobalTable",
                {"GlobalTableName": name, "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            )
            resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
            assert resp.status_code == 200

        # List with Limit=1
        req = _make_request(
            "DynamoDB_20120810.ListGlobalTables",
            {"Limit": 1},
        )
        resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)

        assert len(body["GlobalTables"]) <= 1, (
            f"Expected at most 1 table with Limit=1, got {len(body['GlobalTables'])}"
        )

    @pytest.mark.asyncio
    async def test_list_global_tables_exclusive_start(self):
        """ListGlobalTables with ExclusiveStartGlobalTableName should paginate."""
        # Create 2 global tables
        for name in ["gt-first", "gt-second"]:
            req = _make_request(
                "DynamoDB_20120810.CreateGlobalTable",
                {"GlobalTableName": name, "ReplicationGroup": [{"RegionName": "us-east-1"}]},
            )
            await handle_dynamodb_request(req, "us-east-1", "123456789012")

        # List starting after "gt-first"
        req = _make_request(
            "DynamoDB_20120810.ListGlobalTables",
            {"ExclusiveStartGlobalTableName": "gt-first"},
        )
        resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        table_names = [t["GlobalTableName"] for t in body["GlobalTables"]]

        assert "gt-first" not in table_names, (
            "ExclusiveStartGlobalTableName should exclude the start key itself"
        )


# ---------------------------------------------------------------------------
# Bug 3: Global tables state not isolated by account_id
# ---------------------------------------------------------------------------


class TestGlobalTablesAccountIsolation:
    """Bug: The _global_tables dict is a plain module-level dict keyed only by
    table name. Two different account IDs creating a global table with the same
    name will collide -- the second CreateGlobalTable gets
    GlobalTableAlreadyExistsException even though it's a different account.
    Real AWS isolates global tables per account.
    """

    @pytest.fixture(autouse=True)
    def _clear_global_tables(self):
        _global_tables.clear()
        yield
        _global_tables.clear()

    @pytest.mark.asyncio
    async def test_different_accounts_can_create_same_global_table_name(self):
        """Two different accounts should be able to create global tables with the
        same name independently."""
        req1 = _make_request(
            "DynamoDB_20120810.CreateGlobalTable",
            {"GlobalTableName": "shared-name", "ReplicationGroup": []},
        )
        resp1 = await handle_dynamodb_request(req1, "us-east-1", "111111111111")
        assert resp1.status_code == 200

        req2 = _make_request(
            "DynamoDB_20120810.CreateGlobalTable",
            {"GlobalTableName": "shared-name", "ReplicationGroup": []},
        )
        resp2 = await handle_dynamodb_request(req2, "us-east-1", "222222222222")
        # Bug: this returns 400 GlobalTableAlreadyExistsException
        assert resp2.status_code == 200, (
            f"Different account should be able to create same-named global table, "
            f"got status {resp2.status_code}: {resp2.body.decode()}"
        )


# ---------------------------------------------------------------------------
# Bug 4: DescribeTableReplicaAutoScaling doesn't validate table exists
# ---------------------------------------------------------------------------


class TestDescribeTableReplicaAutoScalingValidation:
    """Bug: DescribeTableReplicaAutoScaling returns 200 with a stub response
    even when the table doesn't exist. Real AWS returns ResourceNotFoundException
    (status 400) if the table is not found.
    """

    @pytest.mark.asyncio
    async def test_nonexistent_table_returns_error(self):
        """DescribeTableReplicaAutoScaling for a nonexistent table should return
        ResourceNotFoundException, not a 200 with stub data."""
        req = _make_request(
            "DynamoDB_20120810.DescribeTableReplicaAutoScaling",
            {"TableName": "this-table-does-not-exist-anywhere"},
        )
        resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        # Bug: provider returns 200 with stub data instead of 400
        assert resp.status_code == 400, (
            f"Expected 400 for nonexistent table, got {resp.status_code}"
        )
        body = json.loads(resp.body)
        assert "ResourceNotFoundException" in body.get("__type", ""), (
            f"Expected ResourceNotFoundException, got {body}"
        )


# ---------------------------------------------------------------------------
# Bug 5: DynamoDB error response missing __type namespace prefix
# ---------------------------------------------------------------------------


class TestErrorResponseFormat:
    """Bug: The intercepted operations return error __type without the namespace
    prefix. Real AWS DynamoDB returns __type values like
    'com.amazonaws.dynamodb.v20120810#GlobalTableNotFoundException'.
    The provider returns just 'GlobalTableNotFoundException'.
    Clients that parse the full __type string (including splitting on #) may
    misbehave.
    """

    @pytest.fixture(autouse=True)
    def _clear_global_tables(self):
        _global_tables.clear()
        yield
        _global_tables.clear()

    @pytest.mark.asyncio
    async def test_error_type_has_namespace_prefix(self):
        """Error __type should include the com.amazonaws.dynamodb.v20120810# prefix."""
        req = _make_request(
            "DynamoDB_20120810.DescribeGlobalTable",
            {"GlobalTableName": "nope"},
        )
        resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        # Real AWS returns fully qualified type
        assert "#" in body["__type"], (
            f"Expected namespaced __type like 'com.amazonaws...#ErrorName', got '{body['__type']}'"
        )


# ---------------------------------------------------------------------------
# Bug 6: ListGlobalTables ignores RegionName filter
# ---------------------------------------------------------------------------


class TestListGlobalTablesRegionFilter:
    """Bug: ListGlobalTables accepts an optional RegionName parameter that should
    filter results to global tables with replicas in that region. The provider
    ignores it entirely.
    """

    @pytest.fixture(autouse=True)
    def _clear_global_tables(self):
        _global_tables.clear()
        yield
        _global_tables.clear()

    @pytest.mark.asyncio
    async def test_region_filter(self):
        """ListGlobalTables with RegionName should only return tables with replicas
        in that region."""
        # Create a table replicated to us-east-1
        req1 = _make_request(
            "DynamoDB_20120810.CreateGlobalTable",
            {
                "GlobalTableName": "east-table",
                "ReplicationGroup": [{"RegionName": "us-east-1"}],
            },
        )
        await handle_dynamodb_request(req1, "us-east-1", "123456789012")

        # Create a table replicated to eu-west-1
        req2 = _make_request(
            "DynamoDB_20120810.CreateGlobalTable",
            {
                "GlobalTableName": "west-table",
                "ReplicationGroup": [{"RegionName": "eu-west-1"}],
            },
        )
        await handle_dynamodb_request(req2, "us-east-1", "123456789012")

        # List only tables with replicas in eu-west-1
        req3 = _make_request(
            "DynamoDB_20120810.ListGlobalTables",
            {"RegionName": "eu-west-1"},
        )
        resp = await handle_dynamodb_request(req3, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        table_names = [t["GlobalTableName"] for t in body["GlobalTables"]]

        assert "east-table" not in table_names, (
            f"Table without eu-west-1 replica should be filtered out, got {table_names}"
        )
        assert "west-table" in table_names


# ---------------------------------------------------------------------------
# Bug 7: CreateGlobalTable response missing ReplicationGroup region details
# ---------------------------------------------------------------------------


class TestCreateGlobalTableResponseFormat:
    """Bug: CreateGlobalTable stores and returns ReplicationGroup exactly as
    provided by the caller. Real AWS enriches each replica entry with
    additional fields: ReplicaStatus, ReplicaStatusDescription, etc.
    A minimal real AWS response includes at least ReplicaStatus='CREATING' or
    'ACTIVE' on each replica entry.
    """

    @pytest.fixture(autouse=True)
    def _clear_global_tables(self):
        _global_tables.clear()
        yield
        _global_tables.clear()

    @pytest.mark.asyncio
    async def test_replication_group_entries_have_replica_status(self):
        """Each ReplicationGroup entry should have a ReplicaStatus field."""
        req = _make_request(
            "DynamoDB_20120810.CreateGlobalTable",
            {
                "GlobalTableName": "my-global",
                "ReplicationGroup": [{"RegionName": "us-east-1"}],
            },
        )
        resp = await handle_dynamodb_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        replicas = body["GlobalTableDescription"]["ReplicationGroup"]
        for replica in replicas:
            assert "ReplicaStatus" in replica, f"Replica entry missing ReplicaStatus: {replica}"
