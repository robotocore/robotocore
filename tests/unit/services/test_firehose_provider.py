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
    _list_delivery_streams,
    _put_record,
    _put_record_batch,
    _start_delivery_stream_encryption,
    _stop_delivery_stream_encryption,
    _stream_buffers,
    _update_destination,
    _write_to_s3,
    handle_firehose_request,
)

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
    async def test_unknown_action_returns_400(self):
        req = _make_request("NonExistentAction", {})
        with patch("robotocore.services.firehose.provider._ensure_worker"):
            resp = await handle_firehose_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

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
        assert "s1" in _delivery_streams

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
        assert _delivery_streams["s1"]["s3_config"]["BucketARN"] == ("arn:aws:s3:::mybucket")


# ---------------------------------------------------------------------------
# _delete_delivery_stream
# ---------------------------------------------------------------------------


class TestDeleteDeliveryStream:
    def test_deletes_existing(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        result = _delete_delivery_stream({"DeliveryStreamName": "s1"}, "us-east-1", "123456789012")
        assert result == {}
        assert "s1" not in _delivery_streams

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
        assert len(_stream_buffers["s1"]) == 1

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
        assert len(_stream_buffers["s1"]) == 2


# ---------------------------------------------------------------------------
# _flush_buffer / _write_to_s3
# ---------------------------------------------------------------------------


class TestFlushBuffer:
    def test_flush_empty_buffer_is_noop(self):
        _flush_buffer("nonexistent")

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
        _stream_buffers["s1"] = [b"data1", b"data2"]

        with patch("robotocore.services.firehose.provider._write_to_s3") as mock_write:
            _flush_buffer("s1")
        mock_write.assert_called_once()
        assert _stream_buffers["s1"] == []


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
        assert _delivery_streams["s1"]["s3_config"]["Prefix"] == "updated/"
        # Original fields preserved
        assert _delivery_streams["s1"]["s3_config"]["BucketARN"] == "arn:aws:s3:::mybucket"

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
        hints = _delivery_streams["s1"]["s3_config"]["BufferingHints"]
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
        assert _delivery_streams["s1"]["encryption"]["Status"] == "ENABLED"
        assert _delivery_streams["s1"]["encryption"]["KeyType"] == "AWS_OWNED_CMK"

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
        assert _delivery_streams["s1"]["encryption"]["Status"] == "DISABLED"

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
