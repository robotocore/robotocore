"""Unit tests for the Kinesis provider."""

import base64
import json

import pytest
from starlette.requests import Request

from robotocore.services.kinesis.models import _stores
from robotocore.services.kinesis.provider import (
    KinesisError,
    _decode_iterator,
    _encode_iterator,
    _error,
    handle_kinesis_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(action: str, body: dict):
    target = f"Kinesis_20131202.{action}"
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


def _make_request_no_target(body: dict):
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [],
    }
    body_bytes = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


@pytest.fixture(autouse=True)
def _clear_stores():
    _stores.clear()
    yield
    _stores.clear()


# ---------------------------------------------------------------------------
# Iterator encode/decode
# ---------------------------------------------------------------------------


class TestIteratorEncoding:
    def test_round_trip(self):
        token = _encode_iterator("stream1", "shard-0", "TRIM_HORIZON", "000", "us-east-1")
        decoded = _decode_iterator(token)
        assert decoded["stream"] == "stream1"
        assert decoded["shard"] == "shard-0"
        assert decoded["type"] == "TRIM_HORIZON"
        assert decoded["seq"] == "000"

    def test_invalid_token_raises(self):
        with pytest.raises(KinesisError) as exc:
            _decode_iterator("not-valid-base64!!!")
        assert exc.value.code == "InvalidArgumentException"


# ---------------------------------------------------------------------------
# KinesisError
# ---------------------------------------------------------------------------


class TestKinesisError:
    def test_default_status(self):
        e = KinesisError("Code", "msg")
        assert e.status == 400

    def test_custom_status(self):
        e = KinesisError("Code", "msg", 500)
        assert e.status == 500


# ---------------------------------------------------------------------------
# handle_kinesis_request — routing
# ---------------------------------------------------------------------------


class TestHandleKinesisRequest:
    @pytest.mark.asyncio
    async def test_missing_target_returns_400(self):
        req = _make_request_no_target({})
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_unknown_action_returns_400(self):
        req = _make_request("BogusAction", {})
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_create_stream(self):
        req = _make_request("CreateStream", {"StreamName": "mystream", "ShardCount": 1})
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_create_duplicate_stream(self):
        req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req, "us-east-1", "123456789012")
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        assert "ResourceInUseException" in resp.body.decode()

    @pytest.mark.asyncio
    async def test_describe_stream(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 2})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("DescribeStream", {"StreamName": "s1"})
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        desc = data["StreamDescription"]
        assert desc["StreamName"] == "s1"
        assert len(desc["Shards"]) == 2

    @pytest.mark.asyncio
    async def test_describe_nonexistent_stream(self):
        req = _make_request("DescribeStream", {"StreamName": "nope"})
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_delete_stream(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("DeleteStream", {"StreamName": "s1"})
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_delete_nonexistent_stream(self):
        req = _make_request("DeleteStream", {"StreamName": "nope"})
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_list_streams(self):
        for name in ("a-stream", "b-stream"):
            req = _make_request("CreateStream", {"StreamName": name, "ShardCount": 1})
            await handle_kinesis_request(req, "us-east-1", "123456789012")

        req = _make_request("ListStreams", {})
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert "a-stream" in data["StreamNames"]
        assert "b-stream" in data["StreamNames"]

    @pytest.mark.asyncio
    async def test_put_and_get_records(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        data_b64 = base64.b64encode(b"hello").decode()
        req2 = _make_request(
            "PutRecord",
            {"StreamName": "s1", "PartitionKey": "pk1", "Data": data_b64},
        )
        resp2 = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 200
        put_result = json.loads(resp2.body)
        assert "SequenceNumber" in put_result

        # Get shard iterator
        req3 = _make_request(
            "GetShardIterator",
            {
                "StreamName": "s1",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
        )
        resp3 = await handle_kinesis_request(req3, "us-east-1", "123456789012")
        iterator = json.loads(resp3.body)["ShardIterator"]

        # Get records
        req4 = _make_request("GetRecords", {"ShardIterator": iterator})
        resp4 = await handle_kinesis_request(req4, "us-east-1", "123456789012")
        data = json.loads(resp4.body)
        assert len(data["Records"]) == 1
        assert base64.b64decode(data["Records"][0]["Data"]) == b"hello"

    @pytest.mark.asyncio
    async def test_put_records_batch(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        data_b64 = base64.b64encode(b"rec").decode()
        req2 = _make_request(
            "PutRecords",
            {
                "StreamName": "s1",
                "Records": [
                    {"PartitionKey": "pk1", "Data": data_b64},
                    {"PartitionKey": "pk2", "Data": data_b64},
                ],
            },
        )
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert data["FailedRecordCount"] == 0
        assert len(data["Records"]) == 2

    @pytest.mark.asyncio
    async def test_list_shards(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 3})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("ListShards", {"StreamName": "s1"})
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        data = json.loads(resp.body)
        assert len(data["Shards"]) == 3

    @pytest.mark.asyncio
    async def test_add_and_list_tags(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "AddTagsToStream",
            {"StreamName": "s1", "Tags": {"env": "test"}},
        )
        resp2 = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 200

        req3 = _make_request("ListTagsForStream", {"StreamName": "s1"})
        resp3 = await handle_kinesis_request(req3, "us-east-1", "123456789012")
        data = json.loads(resp3.body)
        tags = {t["Key"]: t["Value"] for t in data["Tags"]}
        assert tags["env"] == "test"

    @pytest.mark.asyncio
    async def test_increase_retention(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "IncreaseStreamRetentionPeriod",
            {"StreamName": "s1", "RetentionPeriodHours": 48},
        )
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_decrease_retention_below_minimum_fails(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        # First increase to 48
        req2 = _make_request(
            "IncreaseStreamRetentionPeriod",
            {"StreamName": "s1", "RetentionPeriodHours": 48},
        )
        await handle_kinesis_request(req2, "us-east-1", "123456789012")

        # Try to decrease below 24
        req3 = _make_request(
            "DecreaseStreamRetentionPeriod",
            {"StreamName": "s1", "RetentionPeriodHours": 12},
        )
        resp = await handle_kinesis_request(req3, "us-east-1", "123456789012")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_internal_error_returns_500(self):
        """Unexpected exception becomes 500 InternalError."""
        from robotocore.services.kinesis.provider import _ACTION_MAP

        original = _ACTION_MAP["CreateStream"]
        _ACTION_MAP["CreateStream"] = lambda *a, **kw: (_ for _ in ()).throw(
            RuntimeError("unexpected")
        )
        try:
            req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
            resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
            assert resp.status_code == 500
        finally:
            _ACTION_MAP["CreateStream"] = original


# ---------------------------------------------------------------------------
# _error helper
# ---------------------------------------------------------------------------


class TestErrorHelper:
    def test_error_response(self):
        resp = _error("TestCode", "test msg", 404)
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "TestCode"
