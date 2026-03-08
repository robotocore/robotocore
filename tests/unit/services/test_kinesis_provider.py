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
    async def test_describe_stream_summary(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 3})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request("DescribeStreamSummary", {"StreamName": "s1"})
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        summary = data["StreamDescriptionSummary"]
        assert summary["StreamName"] == "s1"
        assert summary["StreamStatus"] == "ACTIVE"
        assert summary["OpenShardCount"] == 3
        assert "StreamARN" in summary
        assert "RetentionPeriodHours" in summary
        assert summary["ConsumerCount"] == 0

    @pytest.mark.asyncio
    async def test_describe_stream_summary_nonexistent(self):
        req = _make_request("DescribeStreamSummary", {"StreamName": "nope"})
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        assert "ResourceNotFoundException" in resp.body.decode()

    @pytest.mark.asyncio
    async def test_update_shard_count(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "UpdateShardCount",
            {"StreamName": "s1", "TargetShardCount": 4, "ScalingType": "UNIFORM_SCALING"},
        )
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["CurrentShardCount"] == 1
        assert data["TargetShardCount"] == 4

        # Verify shards were actually updated
        req3 = _make_request("ListShards", {"StreamName": "s1"})
        resp3 = await handle_kinesis_request(req3, "us-east-1", "123456789012")
        shards = json.loads(resp3.body)["Shards"]
        assert len(shards) == 4

    @pytest.mark.asyncio
    async def test_update_shard_count_nonexistent_stream(self):
        req = _make_request(
            "UpdateShardCount",
            {"StreamName": "nope", "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING"},
        )
        resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        assert "ResourceNotFoundException" in resp.body.decode()

    @pytest.mark.asyncio
    async def test_update_shard_count_missing_target(self):
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "UpdateShardCount",
            {"StreamName": "s1", "ScalingType": "UNIFORM_SCALING"},
        )
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
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


# ---------------------------------------------------------------------------
# Bug: UpdateShardCount drops all existing records
#
# When _update_shard_count is called, it rebuilds the shard list from scratch
# with brand new Shard objects. All records stored in the old shards are lost.
# Real AWS keeps parent shards (with their records) readable until retention
# expires; only new data goes to the child shards.
# ---------------------------------------------------------------------------


class TestUpdateShardCountDropsRecords:
    @pytest.mark.asyncio
    async def test_records_survive_reshard(self):
        """Records written before UpdateShardCount should still be readable."""
        # Create stream with 2 shards
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 2})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        # Put a record
        data_b64 = base64.b64encode(b"before-reshard").decode()
        req2 = _make_request(
            "PutRecord",
            {"StreamName": "s1", "PartitionKey": "pk1", "Data": data_b64},
        )
        put_resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        put_data = json.loads(put_resp.body)
        shard_id = put_data["ShardId"]

        # Get iterator for that shard (TRIM_HORIZON)
        req3 = _make_request(
            "GetShardIterator",
            {
                "StreamName": "s1",
                "ShardId": shard_id,
                "ShardIteratorType": "TRIM_HORIZON",
            },
        )
        iter_resp = await handle_kinesis_request(req3, "us-east-1", "123456789012")
        iterator = json.loads(iter_resp.body)["ShardIterator"]

        # Reshard from 2 -> 4
        req4 = _make_request(
            "UpdateShardCount",
            {"StreamName": "s1", "TargetShardCount": 4, "ScalingType": "UNIFORM_SCALING"},
        )
        await handle_kinesis_request(req4, "us-east-1", "123456789012")

        # Try to read the record using the old iterator
        req5 = _make_request("GetRecords", {"ShardIterator": iterator})
        get_resp = await handle_kinesis_request(req5, "us-east-1", "123456789012")
        assert get_resp.status_code == 200, (
            f"Expected 200 but got {get_resp.status_code}: {get_resp.body.decode()}"
        )
        get_data = json.loads(get_resp.body)
        assert len(get_data["Records"]) > 0, (
            "Records written before UpdateShardCount should still be readable"
        )


# ---------------------------------------------------------------------------
# Bug: SplitShard drops all records from the split shard
#
# _split_shard removes the target shard from the list and creates two new
# empty shards. All records in the original shard are lost.
# ---------------------------------------------------------------------------


class TestSplitShardDropsRecords:
    @pytest.mark.asyncio
    async def test_records_survive_split(self):
        """Records in a shard should be readable after that shard is split."""
        # Create stream with 1 shard
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        # Put 3 records
        data_b64 = base64.b64encode(b"before-split").decode()
        for i in range(3):
            req = _make_request(
                "PutRecord",
                {"StreamName": "s1", "PartitionKey": f"pk-{i}", "Data": data_b64},
            )
            await handle_kinesis_request(req, "us-east-1", "123456789012")

        # Get iterator for shard-0
        req3 = _make_request(
            "GetShardIterator",
            {
                "StreamName": "s1",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
        )
        iter_resp = await handle_kinesis_request(req3, "us-east-1", "123456789012")
        iterator = json.loads(iter_resp.body)["ShardIterator"]

        # Split the shard
        req4 = _make_request(
            "SplitShard",
            {
                "StreamName": "s1",
                "ShardToSplit": "shardId-000000000000",
                "NewStartingHashKey": str(2**127),
            },
        )
        split_resp = await handle_kinesis_request(req4, "us-east-1", "123456789012")
        assert split_resp.status_code == 200

        # The old shard's records should still be accessible
        req5 = _make_request("GetRecords", {"ShardIterator": iterator})
        get_resp = await handle_kinesis_request(req5, "us-east-1", "123456789012")
        assert get_resp.status_code == 200, (
            f"Expected 200 but got {get_resp.status_code}: {get_resp.body.decode()}"
        )
        get_data = json.loads(get_resp.body)
        assert len(get_data["Records"]) == 3, (
            f"Expected 3 records from original shard after split, got {len(get_data['Records'])}"
        )


# ---------------------------------------------------------------------------
# Bug: MergeShards drops all records from both source shards
#
# _merge_shards removes both source shards and creates a new empty merged
# shard. All records in the source shards are lost.
# ---------------------------------------------------------------------------


class TestMergeShardsDropsRecords:
    @pytest.mark.asyncio
    async def test_records_survive_merge(self):
        """Records in merged shards should be readable after merge."""
        # Create stream with 2 shards
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 2})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        # Put a record into each shard using explicit hash keys
        data_b64 = base64.b64encode(b"before-merge").decode()
        req2 = _make_request(
            "PutRecord",
            {
                "StreamName": "s1",
                "PartitionKey": "pk1",
                "Data": data_b64,
                "ExplicitHashKey": "0",  # Goes to shard 0
            },
        )
        await handle_kinesis_request(req2, "us-east-1", "123456789012")

        req3 = _make_request(
            "PutRecord",
            {
                "StreamName": "s1",
                "PartitionKey": "pk2",
                "Data": data_b64,
                "ExplicitHashKey": str(2**128 - 1),  # Goes to shard 1
            },
        )
        await handle_kinesis_request(req3, "us-east-1", "123456789012")

        # Get iterators for both shards before merge
        iterators = []
        for shard_id in ("shardId-000000000000", "shardId-000000000001"):
            req = _make_request(
                "GetShardIterator",
                {
                    "StreamName": "s1",
                    "ShardId": shard_id,
                    "ShardIteratorType": "TRIM_HORIZON",
                },
            )
            resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
            iterators.append(json.loads(resp.body)["ShardIterator"])

        # Merge the two shards
        req4 = _make_request(
            "MergeShards",
            {
                "StreamName": "s1",
                "ShardToMerge": "shardId-000000000000",
                "AdjacentShardToMerge": "shardId-000000000001",
            },
        )
        merge_resp = await handle_kinesis_request(req4, "us-east-1", "123456789012")
        assert merge_resp.status_code == 200

        # The old shards' records should still be readable
        total_records = 0
        for it in iterators:
            req = _make_request("GetRecords", {"ShardIterator": it})
            get_resp = await handle_kinesis_request(req, "us-east-1", "123456789012")
            if get_resp.status_code == 200:
                get_data = json.loads(get_resp.body)
                total_records += len(get_data["Records"])

        assert total_records == 2, (
            f"Expected 2 records total from the original shards after merge, got {total_records}"
        )


# ---------------------------------------------------------------------------
# Bug: GetShardIterator AT_TIMESTAMP type not supported
#
# AWS supports ShardIteratorType=AT_TIMESTAMP which returns an iterator
# pointing to records at or after a given timestamp. The provider raises
# InvalidArgumentException for this valid iterator type.
# ---------------------------------------------------------------------------


class TestGetShardIteratorAtTimestamp:
    @pytest.mark.asyncio
    async def test_at_timestamp_type_supported(self):
        """GetShardIterator should support AT_TIMESTAMP iterator type."""
        import time

        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "GetShardIterator",
            {
                "StreamName": "s1",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": time.time(),
            },
        )
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        assert resp.status_code == 200, (
            f"AT_TIMESTAMP is a valid AWS ShardIteratorType but got status {resp.status_code}: "
            f"{resp.body.decode()}"
        )
        data = json.loads(resp.body)
        assert "ShardIterator" in data


# ---------------------------------------------------------------------------
# Bug: AFTER_SEQUENCE_NUMBER with non-numeric sequence gives 500 instead
# of InvalidArgumentException
#
# The provider does int(starting_seq) which raises ValueError for non-numeric
# strings. This is caught by the generic Exception handler and returned as
# a 500 InternalError instead of a 400 InvalidArgumentException.
# ---------------------------------------------------------------------------


class TestAfterSequenceNumberNonNumeric:
    @pytest.mark.asyncio
    async def test_non_numeric_sequence_gives_400(self):
        """AFTER_SEQUENCE_NUMBER with non-numeric sequence should return 400, not 500."""
        req1 = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req1, "us-east-1", "123456789012")

        req2 = _make_request(
            "GetShardIterator",
            {
                "StreamName": "s1",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": "not-a-number",
            },
        )
        resp = await handle_kinesis_request(req2, "us-east-1", "123456789012")
        # Should be 400 with InvalidArgumentException, not 500 InternalError
        assert resp.status_code == 400, (
            f"Expected 400 for invalid sequence number but got {resp.status_code}"
        )
        data = json.loads(resp.body)
        assert data["__type"] == "InvalidArgumentException", (
            f"Expected InvalidArgumentException but got {data['__type']}"
        )
