"""Unit tests for the Firehose provider."""

import base64
import json
from unittest.mock import MagicMock, patch

import pytest
from starlette.requests import Request

from robotocore.services.firehose.provider import (
    FirehoseError,
    _create_delivery_stream,
    _delete_delivery_stream,
    _delivery_streams,
    _describe_delivery_stream,
    _error,
    _flush_buffer,
    _key,
    _list_delivery_streams,
    _list_tags_for_delivery_stream,
    _put_record,
    _put_record_batch,
    _start_delivery_stream_encryption,
    _stop_delivery_stream_encryption,
    _stream_buffers,
    _tag_delivery_stream,
    _untag_delivery_stream,
    _update_destination,
    _write_to_s3,
    handle_firehose_request,
)

# Default test account/region
_ACCT = "123456789012"
_REGION = "us-east-1"


def _k(name: str, region: str = _REGION, account_id: str = _ACCT) -> tuple[str, str, str]:
    """Shorthand for building a scoped key in tests."""
    return _key(name, region, account_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(action: str, body: dict):
    target = f"Firehose_20150804.{action}"
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
def _clear_streams():
    """Reset global state between tests."""
    _delivery_streams.clear()
    _stream_buffers.clear()
    yield
    _delivery_streams.clear()
    _stream_buffers.clear()


# ---------------------------------------------------------------------------
# FirehoseError
# ---------------------------------------------------------------------------


class TestFirehoseError:
    def test_default_status(self):
        e = FirehoseError("Code", "msg")
        assert e.status == 400

    def test_custom_status(self):
        e = FirehoseError("Code", "msg", 500)
        assert e.status == 500


# ---------------------------------------------------------------------------
# handle_firehose_request — routing
# ---------------------------------------------------------------------------


class TestHandleFirehoseRequest:
    @pytest.mark.asyncio
    async def test_missing_target_returns_400(self):
        req = _make_request_no_target({})
        with patch("robotocore.services.firehose.provider._ensure_worker"):
            resp = await handle_firehose_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "InvalidAction"

    @pytest.mark.asyncio
    async def test_create_delivery_stream(self):
        req = _make_request(
            "CreateDeliveryStream",
            {"DeliveryStreamName": "my-stream"},
        )
        with patch("robotocore.services.firehose.provider._ensure_worker"):
            resp = await handle_firehose_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "DeliveryStreamARN" in data

    @pytest.mark.asyncio
    async def test_firehose_error_returns_proper_status(self):
        """Creating a duplicate stream returns ResourceInUseException."""
        _create_delivery_stream({"DeliveryStreamName": "dup"}, "us-east-1", "123456789012")
        req = _make_request("CreateDeliveryStream", {"DeliveryStreamName": "dup"})
        with patch("robotocore.services.firehose.provider._ensure_worker"):
            resp = await handle_firehose_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceInUseException"


# ---------------------------------------------------------------------------
# _create_delivery_stream
# ---------------------------------------------------------------------------


class TestCreateDeliveryStream:
    def test_creates_stream(self):
        result = _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        assert "DeliveryStreamARN" in result
        assert _k("s1") in _delivery_streams

    def test_empty_name_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _create_delivery_stream({}, "us-east-1", "123456789012")
        assert exc.value.code == "ValidationException"

    def test_duplicate_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        with pytest.raises(FirehoseError) as exc:
            _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        assert exc.value.code == "ResourceInUseException"

    def test_stores_s3_config(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "logs/",
                },
            },
            "us-east-1",
            "123456789012",
        )
        assert _delivery_streams[_k("s1")]["s3_config"]["BucketARN"] == ("arn:aws:s3:::mybucket")


# ---------------------------------------------------------------------------
# _delete_delivery_stream
# ---------------------------------------------------------------------------


class TestDeleteDeliveryStream:
    def test_deletes_existing(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        result = _delete_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        assert result == {}
        assert _k("s1") not in _delivery_streams

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _delete_delivery_stream({"DeliveryStreamName": "nope"}, "us-east-1", "123456789012")
        assert exc.value.code == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# _describe_delivery_stream
# ---------------------------------------------------------------------------


class TestDescribeDeliveryStream:
    def test_describes_existing(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        result = _describe_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        desc = result["DeliveryStreamDescription"]
        assert desc["DeliveryStreamName"] == "s1"
        assert desc["DeliveryStreamStatus"] == "ACTIVE"

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError):
            _describe_delivery_stream({"DeliveryStreamName": "nope"}, "us-east-1", "123456789012")


# ---------------------------------------------------------------------------
# _list_delivery_streams
# ---------------------------------------------------------------------------


class TestListDeliveryStreams:
    def test_empty(self):
        result = _list_delivery_streams({}, "us-east-1", "123456789012")
        assert result["DeliveryStreamNames"] == []

    def test_lists_created_streams(self):
        for name in ("a-stream", "b-stream"):
            _create_delivery_stream({"DeliveryStreamName": name}, "us-east-1", "123456789012")
        result = _list_delivery_streams({}, "us-east-1", "123456789012")
        assert result["DeliveryStreamNames"] == ["a-stream", "b-stream"]

    def test_pagination_with_start(self):
        for name in ("a", "b", "c"):
            _create_delivery_stream({"DeliveryStreamName": name}, "us-east-1", "123456789012")
        result = _list_delivery_streams(
            {"ExclusiveStartDeliveryStreamName": "a"}, "us-east-1", "123456789012"
        )
        assert result["DeliveryStreamNames"] == ["b", "c"]


# ---------------------------------------------------------------------------
# _put_record / _put_record_batch
# ---------------------------------------------------------------------------


class TestPutRecord:
    def test_puts_record(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        data = base64.b64encode(b"hello").decode()
        result = _put_record(
            {"DeliveryStreamName": "s1", "Record": {"Data": data}},
            "us-east-1",
            "123456789012",
        )
        assert "RecordId" in result
        assert len(_stream_buffers[_k("s1")]) == 1

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError):
            _put_record(
                {"DeliveryStreamName": "nope", "Record": {"Data": ""}},
                "us-east-1",
                "123456789012",
            )


class TestPutRecordBatch:
    def test_puts_batch(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        data = base64.b64encode(b"hello").decode()
        result = _put_record_batch(
            {
                "DeliveryStreamName": "s1",
                "Records": [{"Data": data}, {"Data": data}],
            },
            "us-east-1",
            "123456789012",
        )
        assert result["FailedPutCount"] == 0
        assert len(result["RequestResponses"]) == 2
        assert len(_stream_buffers[_k("s1")]) == 2


# ---------------------------------------------------------------------------
# _flush_buffer / _write_to_s3
# ---------------------------------------------------------------------------


class TestFlushBuffer:
    def test_flush_empty_buffer_is_noop(self):
        _flush_buffer(_k("nonexistent"))

    def test_flush_writes_to_s3(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "logs/",
                },
            },
            "us-east-1",
            "123456789012",
        )
        _stream_buffers[_k("s1")] = [b"data1", b"data2"]

        with patch("robotocore.services.firehose.provider._write_to_s3") as mock_write:
            _flush_buffer(_k("s1"))
        mock_write.assert_called_once()
        assert _stream_buffers[_k("s1")] == []


class TestWriteToS3:
    def test_writes_via_moto_backend(self):
        mock_s3 = MagicMock()
        mock_get = MagicMock()
        mock_get.return_value.__getitem__ = MagicMock(
            return_value=MagicMock(__getitem__=MagicMock(return_value=mock_s3))
        )
        with patch("moto.backends.get_backend", mock_get):
            _write_to_s3("bucket", "key", b"data", "us-east-1")
        mock_s3.put_object.assert_called_once_with("bucket", "key", b"data")

    def test_exception_is_silenced(self):
        with patch("moto.backends.get_backend", side_effect=Exception("fail")):
            _write_to_s3("bucket", "key", b"data", "us-east-1")


# ---------------------------------------------------------------------------
# _update_destination
# ---------------------------------------------------------------------------


class TestUpdateDestination:
    def test_updates_prefix(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "original/",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                },
            },
            "us-east-1",
            "123456789012",
        )
        result = _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
                "ExtendedS3DestinationUpdate": {"Prefix": "updated/"},
            },
            "us-east-1",
            "123456789012",
        )
        assert result == {}
        assert _delivery_streams[_k("s1")]["s3_config"]["Prefix"] == "updated/"
        # Original fields preserved
        assert _delivery_streams[_k("s1")]["s3_config"]["BucketARN"] == "arn:aws:s3:::mybucket"

    def test_updates_buffering_hints(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                },
            },
            "us-east-1",
            "123456789012",
        )
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
                "ExtendedS3DestinationUpdate": {
                    "BufferingHints": {"SizeInMBs": 10, "IntervalInSeconds": 300},
                },
            },
            "us-east-1",
            "123456789012",
        )
        hints = _delivery_streams[_k("s1")]["s3_config"]["BufferingHints"]
        assert hints == {"SizeInMBs": 10, "IntervalInSeconds": 300}

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _update_destination(
                {
                    "DeliveryStreamName": "nope",
                    "DestinationId": "dest-1",
                    "CurrentDeliveryStreamVersionId": "1",
                },
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_missing_destination_id_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        with pytest.raises(FirehoseError) as exc:
            _update_destination(
                {
                    "DeliveryStreamName": "s1",
                    "CurrentDeliveryStreamVersionId": "1",
                },
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "ValidationException"

    def test_describe_shows_updated_prefix(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "old/",
                },
            },
            "us-east-1",
            "123456789012",
        )
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
                "ExtendedS3DestinationUpdate": {"Prefix": "new/"},
            },
            "us-east-1",
            "123456789012",
        )
        result = _describe_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        dest = result["DeliveryStreamDescription"]["Destinations"][0]
        assert dest["ExtendedS3DestinationDescription"]["Prefix"] == "new/"


# ---------------------------------------------------------------------------
# _start/_stop_delivery_stream_encryption
# ---------------------------------------------------------------------------


class TestDeliveryStreamEncryption:
    def test_start_encryption(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        result = _start_delivery_stream_encryption(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamEncryptionInput": {"KeyType": "AWS_OWNED_CMK"},
            },
            "us-east-1",
            "123456789012",
        )
        assert result == {}
        assert _delivery_streams[_k("s1")]["encryption"]["Status"] == "ENABLED"
        assert _delivery_streams[_k("s1")]["encryption"]["KeyType"] == "AWS_OWNED_CMK"

    def test_stop_encryption(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        _start_delivery_stream_encryption(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamEncryptionInput": {"KeyType": "AWS_OWNED_CMK"},
            },
            "us-east-1",
            "123456789012",
        )
        result = _stop_delivery_stream_encryption(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        assert result == {}
        assert _delivery_streams[_k("s1")]["encryption"]["Status"] == "DISABLED"

    def test_start_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _start_delivery_stream_encryption(
                {"DeliveryStreamName": "nope"}, "us-east-1", "123456789012"
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_stop_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _stop_delivery_stream_encryption(
                {"DeliveryStreamName": "nope"}, "us-east-1", "123456789012"
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_describe_shows_encryption(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        _start_delivery_stream_encryption(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamEncryptionInput": {"KeyType": "AWS_OWNED_CMK"},
            },
            "us-east-1",
            "123456789012",
        )
        result = _describe_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        enc = result["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]
        assert enc["Status"] == "ENABLED"
        assert enc["KeyType"] == "AWS_OWNED_CMK"

    def test_describe_no_encryption_field(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        result = _describe_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        assert "DeliveryStreamEncryptionConfiguration" not in result["DeliveryStreamDescription"]


# ---------------------------------------------------------------------------
# _error helper
# ---------------------------------------------------------------------------


class TestErrorHelper:
    def test_error_response_format(self):
        resp = _error("TestCode", "test msg", 404)
        assert resp.status_code == 404
        data = json.loads(resp.body)
        assert data["__type"] == "TestCode"
        assert data["message"] == "test msg"


# ---------------------------------------------------------------------------
# Bug: ListDeliveryStreams always returns HasMoreDeliveryStreams=False
#
# Even when the result is truncated by Limit, the response always says
# HasMoreDeliveryStreams: False. This means clients cannot paginate correctly.
# See provider.py line 231: HasMoreDeliveryStreams is hardcoded to False.
# ---------------------------------------------------------------------------


class TestListDeliveryStreamsHasMore:
    def test_has_more_when_truncated(self):
        """ListDeliveryStreams should return HasMoreDeliveryStreams=True when truncated."""
        # Create 5 streams
        for i in range(5):
            _create_delivery_stream(
                {"DeliveryStreamName": f"stream-{i:02d}"},
                "us-east-1",
                "123456789012",
            )

        # List with Limit=2
        result = _list_delivery_streams(
            {"Limit": 2},
            "us-east-1",
            "123456789012",
        )

        assert len(result["DeliveryStreamNames"]) == 2
        assert result["HasMoreDeliveryStreams"] is True, (
            "HasMoreDeliveryStreams should be True when there are more streams beyond Limit"
        )


# ---------------------------------------------------------------------------
# Bug: UpdateDestination version_id mismatch when version is passed as int
#
# The version comparison does `current_version != expected` where expected
# is `str(stream.get("version_id", 1))`. But if the caller passes an int
# (which is valid JSON), the comparison fails because "1" != 1. AWS accepts
# both string and int for CurrentDeliveryStreamVersionId.
# ---------------------------------------------------------------------------


class TestUpdateDestinationVersionMismatch:
    def test_version_id_as_int_accepted(self):
        """UpdateDestination should accept version_id as int, not just string."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "original/",
                },
            },
            "us-east-1",
            "123456789012",
        )

        # Pass version as int (not string) — this is valid per AWS API
        try:
            _update_destination(
                {
                    "DeliveryStreamName": "s1",
                    "DestinationId": "dest-1",
                    "CurrentDeliveryStreamVersionId": 1,  # int, not "1"
                    "ExtendedS3DestinationUpdate": {"Prefix": "updated/"},
                },
                "us-east-1",
                "123456789012",
            )
        except FirehoseError as e:
            if "Version mismatch" in e.message:
                pytest.fail(
                    f"UpdateDestination rejected int version_id with: {e.message}. "
                    "AWS accepts both int and string for CurrentDeliveryStreamVersionId."
                )
            raise
        assert _delivery_streams[_k("s1")]["s3_config"]["Prefix"] == "updated/"


# ---------------------------------------------------------------------------
# Bug: DescribeDeliveryStream reads stream data outside the lock
#
# In _describe_delivery_stream, the `with _lock:` block (lines 175-178)
# only covers the lookup. All the field accesses on the stream dict
# (lines 180+) happen OUTSIDE the lock, creating a race condition where
# another thread could modify or delete the stream concurrently.
#
# This test verifies the describe response has the correct VersionId after
# an update, which can fail under race conditions when the lock isn't held.
# ---------------------------------------------------------------------------


class TestDescribeDeliveryStreamVersionId:
    def test_version_id_increments(self):
        """DescribeDeliveryStream should show incremented VersionId after update."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "original/",
                },
            },
            "us-east-1",
            "123456789012",
        )

        # Update destination (increments version_id)
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
                "ExtendedS3DestinationUpdate": {"Prefix": "updated/"},
            },
            "us-east-1",
            "123456789012",
        )

        # Second update
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "2",
                "ExtendedS3DestinationUpdate": {"Prefix": "updated2/"},
            },
            "us-east-1",
            "123456789012",
        )

        # Describe should show version 3
        result = _describe_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        desc = result["DeliveryStreamDescription"]
        assert desc["VersionId"] == "3", (
            f"Expected VersionId '3' after two updates, got '{desc['VersionId']}'"
        )


# ---------------------------------------------------------------------------
# CATEGORICAL BUG: Tag round-trip — create with tags, list tags, tag, untag
#
# Tags set during CreateDeliveryStream must be visible via
# ListTagsForDeliveryStream. TagDeliveryStream and UntagDeliveryStream must
# modify the tag set correctly. This pattern applies to ALL native providers
# that support tagging.
# ---------------------------------------------------------------------------


class TestTagRoundTrip:
    def test_create_with_tags_visible_in_list_tags(self):
        """Tags provided at creation time must appear in ListTagsForDeliveryStream."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "tagged",
                "Tags": [
                    {"Key": "env", "Value": "prod"},
                    {"Key": "team", "Value": "infra"},
                ],
            },
            "us-east-1",
            "123456789012",
        )
        result = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "tagged"}, "us-east-1", "123456789012"
        )
        tags = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags == {"env": "prod", "team": "infra"}

    def test_tag_delivery_stream_adds_tags(self):
        """TagDeliveryStream must add tags visible in ListTags."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        _tag_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [{"Key": "color", "Value": "blue"}],
            },
            "us-east-1",
            "123456789012",
        )
        result = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        tags = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags == {"color": "blue"}

    def test_tag_delivery_stream_overwrites_existing_key(self):
        """TagDeliveryStream with an existing key should overwrite the value."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [{"Key": "env", "Value": "dev"}],
            },
            "us-east-1",
            "123456789012",
        )
        _tag_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [{"Key": "env", "Value": "prod"}],
            },
            "us-east-1",
            "123456789012",
        )
        result = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        tags = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags["env"] == "prod"

    def test_untag_delivery_stream_removes_tags(self):
        """UntagDeliveryStream must remove specified keys."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                    {"Key": "c", "Value": "3"},
                ],
            },
            "us-east-1",
            "123456789012",
        )
        _untag_delivery_stream(
            {"DeliveryStreamName": "s1", "TagKeys": ["a", "c"]},
            "us-east-1",
            "123456789012",
        )
        result = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        tags = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags == {"b": "2"}

    def test_untag_nonexistent_key_is_noop(self):
        """Removing a key that doesn't exist should not raise."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        _untag_delivery_stream(
            {"DeliveryStreamName": "s1", "TagKeys": ["nonexistent"]},
            "us-east-1",
            "123456789012",
        )
        result = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        assert result["Tags"] == []

    def test_tag_nonexistent_stream_raises(self):
        """TagDeliveryStream on a nonexistent stream must raise ResourceNotFoundException."""
        with pytest.raises(FirehoseError) as exc:
            _tag_delivery_stream(
                {
                    "DeliveryStreamName": "nope",
                    "Tags": [{"Key": "k", "Value": "v"}],
                },
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_untag_nonexistent_stream_raises(self):
        """UntagDeliveryStream on a nonexistent stream must raise ResourceNotFoundException."""
        with pytest.raises(FirehoseError) as exc:
            _untag_delivery_stream(
                {"DeliveryStreamName": "nope", "TagKeys": ["k"]},
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_list_tags_nonexistent_stream_raises(self):
        """ListTagsForDeliveryStream on nonexistent stream must raise."""
        with pytest.raises(FirehoseError) as exc:
            _list_tags_for_delivery_stream(
                {"DeliveryStreamName": "nope"}, "us-east-1", "123456789012"
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_list_tags_pagination(self):
        """ListTagsForDeliveryStream should paginate with ExclusiveStartTagKey."""
        tags = [{"Key": f"key-{i:02d}", "Value": f"val-{i}"} for i in range(10)]
        _create_delivery_stream(
            {"DeliveryStreamName": "s1", "Tags": tags},
            "us-east-1",
            "123456789012",
        )
        result = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "s1", "Limit": 3},
            "us-east-1",
            "123456789012",
        )
        assert len(result["Tags"]) == 3
        assert result["HasMoreTags"] is True

        # Page 2 starting after the third key
        last_key = result["Tags"][-1]["Key"]
        result2 = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "s1", "Limit": 3, "ExclusiveStartTagKey": last_key},
            "us-east-1",
            "123456789012",
        )
        assert len(result2["Tags"]) == 3
        # Keys should not overlap
        page1_keys = {t["Key"] for t in result["Tags"]}
        page2_keys = {t["Key"] for t in result2["Tags"]}
        assert page1_keys.isdisjoint(page2_keys)


# ---------------------------------------------------------------------------
# CATEGORICAL BUG: Cross-account/cross-region isolation
#
# Native providers using a single global dict (like _delivery_streams) must
# scope by (account_id, region). Otherwise streams from different accounts
# or regions collide. This is a common pattern across many providers.
# ---------------------------------------------------------------------------


class TestCrossAccountRegionIsolation:
    def test_same_name_different_accounts(self):
        """Two accounts should each be able to create a stream with the same name."""
        _create_delivery_stream(
            {"DeliveryStreamName": "shared-name"},
            "us-east-1",
            "111111111111",
        )
        # This should NOT raise ResourceInUseException
        _create_delivery_stream(
            {"DeliveryStreamName": "shared-name"},
            "us-east-1",
            "222222222222",
        )
        # Both should be describable
        r1 = _describe_delivery_stream(
            {"DeliveryStreamName": "shared-name"}, "us-east-1", "111111111111"
        )
        r2 = _describe_delivery_stream(
            {"DeliveryStreamName": "shared-name"}, "us-east-1", "222222222222"
        )
        assert (
            r1["DeliveryStreamDescription"]["DeliveryStreamARN"]
            != (r2["DeliveryStreamDescription"]["DeliveryStreamARN"])
        )

    def test_same_name_different_regions(self):
        """Two regions should each be able to create a stream with the same name."""
        _create_delivery_stream(
            {"DeliveryStreamName": "shared-name"},
            "us-east-1",
            "123456789012",
        )
        # This should NOT raise ResourceInUseException
        _create_delivery_stream(
            {"DeliveryStreamName": "shared-name"},
            "eu-west-1",
            "123456789012",
        )

    def test_list_streams_scoped_to_account_and_region(self):
        """ListDeliveryStreams should only return streams for the given account/region."""
        _create_delivery_stream(
            {"DeliveryStreamName": "stream-a"},
            "us-east-1",
            "111111111111",
        )
        _create_delivery_stream(
            {"DeliveryStreamName": "stream-b"},
            "us-east-1",
            "222222222222",
        )
        _create_delivery_stream(
            {"DeliveryStreamName": "stream-c"},
            "eu-west-1",
            "111111111111",
        )

        result = _list_delivery_streams({}, "us-east-1", "111111111111")
        assert result["DeliveryStreamNames"] == ["stream-a"]

    def test_delete_does_not_affect_other_accounts(self):
        """Deleting a stream in one account should not affect the same-named stream in another."""
        _create_delivery_stream(
            {"DeliveryStreamName": "shared"},
            "us-east-1",
            "111111111111",
        )
        _create_delivery_stream(
            {"DeliveryStreamName": "shared"},
            "us-east-1",
            "222222222222",
        )
        _delete_delivery_stream({"DeliveryStreamName": "shared"}, "us-east-1", "111111111111")
        # Other account's stream should still exist
        result = _describe_delivery_stream(
            {"DeliveryStreamName": "shared"}, "us-east-1", "222222222222"
        )
        assert result["DeliveryStreamDescription"]["DeliveryStreamName"] == "shared"


# ---------------------------------------------------------------------------
# CATEGORICAL BUG: Describe reads stream dict outside the lock
#
# _describe_delivery_stream acquires _lock only to look up the stream,
# then reads all fields outside the lock. This is a TOCTOU race. The fix
# is to do all reads inside the lock (or copy the dict under the lock).
# This test verifies correctness by checking all expected fields are present.
# ---------------------------------------------------------------------------


class TestDescribeFieldsUnderLock:
    def test_describe_returns_all_expected_fields(self):
        """DescribeDeliveryStream response must contain all required fields."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamType": "DirectPut",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "logs/",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                },
            },
            "us-east-1",
            "123456789012",
        )
        result = _describe_delivery_stream(
            {"DeliveryStreamName": "s1"}, "us-east-1", "123456789012"
        )
        desc = result["DeliveryStreamDescription"]
        # All these fields must be present
        assert "DeliveryStreamName" in desc
        assert "DeliveryStreamARN" in desc
        assert "DeliveryStreamStatus" in desc
        assert "DeliveryStreamType" in desc
        assert "VersionId" in desc
        assert "Destinations" in desc
        assert "HasMoreDestinations" in desc
        assert "CreateTimestamp" in desc


# ---------------------------------------------------------------------------
# CATEGORICAL BUG: Delete does not clean up all child state
#
# When a delivery stream is deleted, the buffer is cleaned but we should
# verify that put_record on the deleted stream raises ResourceNotFoundException.
# ---------------------------------------------------------------------------


class TestDeleteCascade:
    def test_put_record_after_delete_raises(self):
        """PutRecord on a deleted stream must raise ResourceNotFoundException."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        with pytest.raises(FirehoseError) as exc:
            _put_record(
                {
                    "DeliveryStreamName": "s1",
                    "Record": {"Data": base64.b64encode(b"hello").decode()},
                },
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_describe_after_delete_raises(self):
        """DescribeDeliveryStream on deleted stream must raise ResourceNotFoundException."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        with pytest.raises(FirehoseError) as exc:
            _describe_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        assert exc.value.code == "ResourceNotFoundException"

    def test_tag_after_delete_raises(self):
        """TagDeliveryStream on a deleted stream must raise ResourceNotFoundException."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        with pytest.raises(FirehoseError) as exc:
            _tag_delivery_stream(
                {
                    "DeliveryStreamName": "s1",
                    "Tags": [{"Key": "k", "Value": "v"}],
                },
                "us-east-1",
                "123456789012",
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_buffers_cleaned_after_delete(self):
        """Stream buffers must be cleaned up after deletion."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        _put_record(
            {
                "DeliveryStreamName": "s1",
                "Record": {"Data": base64.b64encode(b"data").decode()},
            },
            "us-east-1",
            "123456789012",
        )
        assert len(_stream_buffers.get(_k("s1"), [])) == 1
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        assert _k("s1") not in _stream_buffers
