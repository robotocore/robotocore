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


# ---------------------------------------------------------------------------
# Categorical Bug: Delete stream does not clean up resource policies
#
# When a stream is deleted, any resource policies stored at the store level
# (keyed by the stream ARN) are orphaned. This is a parent-child cascade bug
# that likely exists across all providers with resource policies.
# ---------------------------------------------------------------------------


class TestDeleteStreamCleansUpResourcePolicies:
    @pytest.mark.asyncio
    async def test_resource_policy_cleaned_up_on_stream_delete(self):
        """Deleting a stream should remove its resource policy from the store."""
        region = "us-east-1"
        account = "123456789012"

        # Create stream
        req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req, region, account)

        # Put a resource policy on it
        stream_arn = f"arn:aws:kinesis:{region}:{account}:stream/s1"
        req = _make_request(
            "PutResourcePolicy",
            {"ResourceARN": stream_arn, "Policy": '{"Version":"2012-10-17"}'},
        )
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200

        # Verify policy exists
        req = _make_request("GetResourcePolicy", {"ResourceARN": stream_arn})
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Policy"] == '{"Version":"2012-10-17"}'

        # Delete the stream
        req = _make_request("DeleteStream", {"StreamName": "s1"})
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200

        # The resource policy should be gone — GetResourcePolicy on a deleted
        # stream should return ResourceNotFoundException, not stale data
        req = _make_request("GetResourcePolicy", {"ResourceARN": stream_arn})
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 400
        assert "ResourceNotFoundException" in resp.body.decode()


# ---------------------------------------------------------------------------
# Categorical Bug: DescribeStreamSummary hardcodes ConsumerCount to 0
#
# The provider returns ConsumerCount: 0 instead of counting actual registered
# consumers. This is a stale-hardcoded-value bug pattern.
# ---------------------------------------------------------------------------


class TestDescribeStreamSummaryConsumerCount:
    @pytest.mark.asyncio
    async def test_consumer_count_reflects_registered_consumers(self):
        """DescribeStreamSummary ConsumerCount should reflect actual consumers."""
        region = "us-east-1"
        account = "123456789012"

        req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req, region, account)

        stream_arn = f"arn:aws:kinesis:{region}:{account}:stream/s1"

        # Register two consumers
        for name in ("consumer-1", "consumer-2"):
            req = _make_request(
                "RegisterStreamConsumer",
                {"StreamARN": stream_arn, "ConsumerName": name},
            )
            resp = await handle_kinesis_request(req, region, account)
            assert resp.status_code == 200

        # DescribeStreamSummary should show ConsumerCount == 2
        req = _make_request("DescribeStreamSummary", {"StreamName": "s1"})
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["StreamDescriptionSummary"]["ConsumerCount"] == 2


# ---------------------------------------------------------------------------
# Categorical Bug: DeregisterStreamConsumer silently succeeds for non-existent
# consumer
#
# Uses dict.pop(key, None) which silently succeeds. AWS returns
# ResourceNotFoundException when the consumer doesn't exist.
# ---------------------------------------------------------------------------


class TestDeregisterNonexistentConsumer:
    @pytest.mark.asyncio
    async def test_deregister_nonexistent_consumer_returns_error(self):
        """Deregistering a non-existent consumer should return ResourceNotFoundException."""
        region = "us-east-1"
        account = "123456789012"

        req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req, region, account)

        stream_arn = f"arn:aws:kinesis:{region}:{account}:stream/s1"
        req = _make_request(
            "DeregisterStreamConsumer",
            {"StreamARN": stream_arn, "ConsumerName": "does-not-exist"},
        )
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 400, (
            f"Expected 400 ResourceNotFoundException but got {resp.status_code}"
        )
        assert "ResourceNotFoundException" in resp.body.decode()


# ---------------------------------------------------------------------------
# Categorical Bug: Resource policy operations parse stream name from ARN
# incorrectly for consumer ARNs
#
# Consumer ARNs look like:
#   arn:aws:kinesis:us-east-1:123:stream/mystream/consumer/myconsumer:12345
# Using split("/")[-1] returns "myconsumer:12345" instead of "mystream".
# Resource policies can apply to both streams and consumers.
# ---------------------------------------------------------------------------


class TestResourcePolicyConsumerArn:
    @pytest.mark.asyncio
    async def test_put_resource_policy_with_consumer_arn(self):
        """PutResourcePolicy should work with a consumer ARN (not just stream ARN)."""
        region = "us-east-1"
        account = "123456789012"

        req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req, region, account)

        stream_arn = f"arn:aws:kinesis:{region}:{account}:stream/s1"
        # Register a consumer
        req = _make_request(
            "RegisterStreamConsumer",
            {"StreamARN": stream_arn, "ConsumerName": "my-consumer"},
        )
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200
        consumer_arn = json.loads(resp.body)["Consumer"]["ConsumerARN"]

        # Put a resource policy on the consumer ARN
        req = _make_request(
            "PutResourcePolicy",
            {"ResourceARN": consumer_arn, "Policy": '{"Version":"2012-10-17"}'},
        )
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200, (
            f"PutResourcePolicy with consumer ARN should succeed but got "
            f"{resp.status_code}: {resp.body.decode()}"
        )

        # Get it back
        req = _make_request("GetResourcePolicy", {"ResourceARN": consumer_arn})
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["Policy"] == '{"Version":"2012-10-17"}'


# ---------------------------------------------------------------------------
# Categorical Bug: ListTagsForStream ignores ExclusiveStartTagKey/Limit
#
# AWS's ListTagsForStream supports pagination via ExclusiveStartTagKey and
# Limit parameters. The implementation ignores them entirely.
# ---------------------------------------------------------------------------


class TestListTagsPagination:
    @pytest.mark.asyncio
    async def test_list_tags_with_limit(self):
        """ListTagsForStream should respect Limit parameter."""
        region = "us-east-1"
        account = "123456789012"

        req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req, region, account)

        # Add several tags
        tags = {f"key{i}": f"val{i}" for i in range(5)}
        req = _make_request("AddTagsToStream", {"StreamName": "s1", "Tags": tags})
        await handle_kinesis_request(req, region, account)

        # List with limit of 2
        req = _make_request("ListTagsForStream", {"StreamName": "s1", "Limit": 2})
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert len(data["Tags"]) == 2
        assert data["HasMoreTags"] is True

    @pytest.mark.asyncio
    async def test_list_tags_with_exclusive_start(self):
        """ListTagsForStream should respect ExclusiveStartTagKey."""
        region = "us-east-1"
        account = "123456789012"

        req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req, region, account)

        tags = {"aaa": "1", "bbb": "2", "ccc": "3"}
        req = _make_request("AddTagsToStream", {"StreamName": "s1", "Tags": tags})
        await handle_kinesis_request(req, region, account)

        # List starting after "aaa"
        req = _make_request(
            "ListTagsForStream",
            {"StreamName": "s1", "ExclusiveStartTagKey": "aaa"},
        )
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        tag_keys = [t["Key"] for t in data["Tags"]]
        assert "aaa" not in tag_keys
        assert "bbb" in tag_keys
        assert "ccc" in tag_keys


# ---------------------------------------------------------------------------
# Categorical Bug: RemoveTagsFromStream with nonexistent tag keys silently
# succeeds — which is actually correct AWS behavior. But verify it.
# ---------------------------------------------------------------------------


class TestRemoveNonexistentTags:
    @pytest.mark.asyncio
    async def test_remove_nonexistent_tags_succeeds(self):
        """RemoveTagsFromStream with non-existent keys should succeed (AWS behavior)."""
        region = "us-east-1"
        account = "123456789012"

        req = _make_request("CreateStream", {"StreamName": "s1", "ShardCount": 1})
        await handle_kinesis_request(req, region, account)

        req = _make_request(
            "RemoveTagsFromStream",
            {"StreamName": "s1", "TagKeys": ["nonexistent"]},
        )
        resp = await handle_kinesis_request(req, region, account)
        assert resp.status_code == 200
