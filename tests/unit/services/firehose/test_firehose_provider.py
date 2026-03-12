"""Comprehensive unit tests for the Firehose native provider.

Tests cover all 12 actions in the provider's _ACTION_MAP, edge cases for
buffer flushing, cross-account/region isolation, tag pagination, encryption
round-trips, version conflict handling, and the async request handler routing.
"""

import asyncio
import base64
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.requests import Request

from robotocore.services.firehose.provider import (
    BUFFER_SIZE,
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

_ACCT = "123456789012"
_REGION = "us-east-1"


def _k(name: str, region: str = _REGION, account_id: str = _ACCT) -> tuple[str, str, str]:
    return _key(name, region, account_id)


def _make_request(action: str, body: dict) -> Request:
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


def _make_request_no_target(body: dict) -> Request:
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
def _clear_state():
    """Reset global state between tests."""
    _delivery_streams.clear()
    _stream_buffers.clear()
    yield
    _delivery_streams.clear()
    _stream_buffers.clear()


# ---------------------------------------------------------------------------
# _key helper
# ---------------------------------------------------------------------------


class TestKey:
    def test_returns_tuple(self):
        result = _key("my-stream", "us-east-1", "111111111111")
        assert result == ("111111111111", "us-east-1", "my-stream")

    def test_different_names_produce_different_keys(self):
        assert _key("a", "us-east-1", "111") != _key("b", "us-east-1", "111")

    def test_different_regions_produce_different_keys(self):
        assert _key("a", "us-east-1", "111") != _key("a", "eu-west-1", "111")

    def test_different_accounts_produce_different_keys(self):
        assert _key("a", "us-east-1", "111") != _key("a", "us-east-1", "222")


# ---------------------------------------------------------------------------
# FirehoseError
# ---------------------------------------------------------------------------


class TestFirehoseError:
    def test_default_status_is_400(self):
        err = FirehoseError("ValidationException", "bad input")
        assert err.status == 400
        assert err.code == "ValidationException"
        assert err.message == "bad input"

    def test_custom_status(self):
        err = FirehoseError("InternalError", "oops", 500)
        assert err.status == 500

    def test_inherits_from_exception(self):
        err = FirehoseError("Code", "msg")
        assert isinstance(err, Exception)


# ---------------------------------------------------------------------------
# _error response helper
# ---------------------------------------------------------------------------


class TestErrorHelper:
    def test_builds_json_response(self):
        resp = _error("TestCode", "test message", 404)
        assert resp.status_code == 404
        assert resp.media_type == "application/x-amz-json-1.1"
        data = json.loads(resp.body)
        assert data["__type"] == "TestCode"
        assert data["message"] == "test message"

    def test_400_error(self):
        resp = _error("ValidationException", "invalid", 400)
        assert resp.status_code == 400

    def test_500_error(self):
        resp = _error("InternalError", "crash", 500)
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# handle_firehose_request routing
# ---------------------------------------------------------------------------


class TestHandleFirehoseRequest:
    def test_missing_target_returns_400(self):
        req = _make_request_no_target({})
        with patch("robotocore.services.firehose.provider._ensure_worker"):
            resp = asyncio.run(handle_firehose_request(req, _REGION, _ACCT))
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "InvalidAction"

    def test_known_action_returns_200(self):
        req = _make_request("CreateDeliveryStream", {"DeliveryStreamName": "test-stream"})
        with patch("robotocore.services.firehose.provider._ensure_worker"):
            resp = asyncio.run(handle_firehose_request(req, _REGION, _ACCT))
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "DeliveryStreamARN" in data

    def test_firehose_error_returns_error_status(self):
        _create_delivery_stream({"DeliveryStreamName": "dup"}, _REGION, _ACCT)
        req = _make_request("CreateDeliveryStream", {"DeliveryStreamName": "dup"})
        with patch("robotocore.services.firehose.provider._ensure_worker"):
            resp = asyncio.run(handle_firehose_request(req, _REGION, _ACCT))
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceInUseException"

    def test_unknown_action_forwards_to_moto(self):
        req = _make_request("SomeUnknownAction", {})
        mock_resp = MagicMock()
        with (
            patch("robotocore.services.firehose.provider._ensure_worker"),
            patch(
                "robotocore.providers.moto_bridge.forward_to_moto",
                new_callable=AsyncMock,
                return_value=mock_resp,
            ) as mock_fwd,
        ):
            resp = asyncio.run(handle_firehose_request(req, _REGION, _ACCT))
        mock_fwd.assert_called_once()
        assert resp is mock_resp

    def test_internal_exception_returns_500(self):
        """If a handler raises a non-FirehoseError exception, return 500."""
        req = _make_request("ListDeliveryStreams", {})
        with (
            patch("robotocore.services.firehose.provider._ensure_worker"),
            patch(
                "robotocore.services.firehose.provider._ACTION_MAP",
                {"ListDeliveryStreams": MagicMock(side_effect=RuntimeError("boom"))},
            ),
        ):
            resp = asyncio.run(handle_firehose_request(req, _REGION, _ACCT))
        assert resp.status_code == 500
        data = json.loads(resp.body)
        assert data["__type"] == "InternalError"

    def test_response_media_type(self):
        req = _make_request("ListDeliveryStreams", {})
        with patch("robotocore.services.firehose.provider._ensure_worker"):
            resp = asyncio.run(handle_firehose_request(req, _REGION, _ACCT))
        assert resp.media_type == "application/x-amz-json-1.1"


# ---------------------------------------------------------------------------
# CreateDeliveryStream
# ---------------------------------------------------------------------------


class TestCreateDeliveryStream:
    def test_basic_create(self):
        result = _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert "DeliveryStreamARN" in result
        assert _k("s1") in _delivery_streams

    def test_arn_format(self):
        result = _create_delivery_stream({"DeliveryStreamName": "my-stream"}, _REGION, _ACCT)
        arn = result["DeliveryStreamARN"]
        assert arn == f"arn:aws:firehose:{_REGION}:{_ACCT}:deliverystream/my-stream"

    def test_missing_name_raises_validation(self):
        with pytest.raises(FirehoseError) as exc:
            _create_delivery_stream({}, _REGION, _ACCT)
        assert exc.value.code == "ValidationException"

    def test_empty_name_raises_validation(self):
        with pytest.raises(FirehoseError) as exc:
            _create_delivery_stream({"DeliveryStreamName": ""}, _REGION, _ACCT)
        assert exc.value.code == "ValidationException"

    def test_duplicate_name_raises_resource_in_use(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        with pytest.raises(FirehoseError) as exc:
            _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert exc.value.code == "ResourceInUseException"

    def test_extended_s3_config_stored(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "logs/",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                },
            },
            _REGION,
            _ACCT,
        )
        s3_cfg = _delivery_streams[_k("s1")]["s3_config"]
        assert s3_cfg["BucketARN"] == "arn:aws:s3:::mybucket"
        assert s3_cfg["Prefix"] == "logs/"

    def test_s3_config_fallback(self):
        """S3DestinationConfiguration is used when ExtendedS3 is not provided."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "S3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::fallback-bucket",
                    "Prefix": "fb/",
                },
            },
            _REGION,
            _ACCT,
        )
        s3_cfg = _delivery_streams[_k("s1")]["s3_config"]
        assert s3_cfg["BucketARN"] == "arn:aws:s3:::fallback-bucket"

    def test_default_stream_type_is_direct_put(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert _delivery_streams[_k("s1")]["type"] == "DirectPut"

    def test_custom_stream_type(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamType": "KinesisStreamAsSource",
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["type"] == "KinesisStreamAsSource"

    def test_initial_version_id_is_1(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert _delivery_streams[_k("s1")]["version_id"] == 1

    def test_initial_tags_stored(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [
                    {"Key": "env", "Value": "prod"},
                    {"Key": "team", "Value": "infra"},
                ],
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["tags"] == {"env": "prod", "team": "infra"}

    def test_tag_without_value_defaults_to_empty(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [{"Key": "flag"}],
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["tags"]["flag"] == ""

    def test_creates_empty_buffer(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert _stream_buffers[_k("s1")] == []

    def test_no_s3_config_stores_empty_dict(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert _delivery_streams[_k("s1")]["s3_config"] == {}


# ---------------------------------------------------------------------------
# DeleteDeliveryStream
# ---------------------------------------------------------------------------


class TestDeleteDeliveryStream:
    def test_deletes_existing_stream(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert result == {}
        assert _k("s1") not in _delivery_streams

    def test_cleans_up_buffers(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _stream_buffers[_k("s1")] = [b"data"]
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert _k("s1") not in _stream_buffers

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _delete_delivery_stream({"DeliveryStreamName": "nope"}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_double_delete_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        with pytest.raises(FirehoseError) as exc:
            _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# DescribeDeliveryStream
# ---------------------------------------------------------------------------


class TestDescribeDeliveryStream:
    def test_describes_stream(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        desc = result["DeliveryStreamDescription"]
        assert desc["DeliveryStreamName"] == "s1"
        assert desc["DeliveryStreamStatus"] == "ACTIVE"
        assert desc["DeliveryStreamType"] == "DirectPut"
        assert desc["VersionId"] == "1"
        assert desc["HasMoreDestinations"] is False

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _describe_delivery_stream({"DeliveryStreamName": "nope"}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_arn_in_description(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert result["DeliveryStreamDescription"]["DeliveryStreamARN"].endswith("/s1")

    def test_create_timestamp_present(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert isinstance(result["DeliveryStreamDescription"]["CreateTimestamp"], float)

    def test_destinations_with_s3_config(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "logs/",
                    "RoleARN": "arn:aws:iam::123456789012:role/test",
                    "CompressionFormat": "GZIP",
                    "ErrorOutputPrefix": "errors/",
                    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300},
                },
            },
            _REGION,
            _ACCT,
        )
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        dests = result["DeliveryStreamDescription"]["Destinations"]
        assert len(dests) == 1
        s3_desc = dests[0]["ExtendedS3DestinationDescription"]
        assert s3_desc["BucketARN"] == "arn:aws:s3:::mybucket"
        assert s3_desc["Prefix"] == "logs/"
        assert s3_desc["RoleARN"] == "arn:aws:iam::123456789012:role/test"
        assert s3_desc["CompressionFormat"] == "GZIP"
        assert s3_desc["ErrorOutputPrefix"] == "errors/"
        assert s3_desc["BufferingHints"] == {"SizeInMBs": 5, "IntervalInSeconds": 300}

    def test_destinations_empty_without_s3_config(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert result["DeliveryStreamDescription"]["Destinations"] == []

    def test_destination_id_is_dest_1(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                },
            },
            _REGION,
            _ACCT,
        )
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert result["DeliveryStreamDescription"]["Destinations"][0]["DestinationId"] == "dest-1"

    def test_default_compression_format_uncompressed(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                },
            },
            _REGION,
            _ACCT,
        )
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        s3_desc = result["DeliveryStreamDescription"]["Destinations"][0][
            "ExtendedS3DestinationDescription"
        ]
        assert s3_desc["CompressionFormat"] == "UNCOMPRESSED"

    def test_encryption_shown_when_enabled(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _start_delivery_stream_encryption(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamEncryptionConfigurationInput": {
                    "KeyType": "CUSTOMER_MANAGED_CMK",
                    "KeyARN": "arn:aws:kms:us-east-1:123456789012:key/abc",
                },
            },
            _REGION,
            _ACCT,
        )
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        enc = result["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]
        assert enc["Status"] == "ENABLED"
        assert enc["KeyType"] == "CUSTOMER_MANAGED_CMK"
        assert enc["KeyARN"] == "arn:aws:kms:us-east-1:123456789012:key/abc"

    def test_encryption_not_shown_when_never_set(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _describe_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert "DeliveryStreamEncryptionConfiguration" not in result["DeliveryStreamDescription"]


# ---------------------------------------------------------------------------
# ListDeliveryStreams
# ---------------------------------------------------------------------------


class TestListDeliveryStreams:
    def test_empty_list(self):
        result = _list_delivery_streams({}, _REGION, _ACCT)
        assert result["DeliveryStreamNames"] == []
        assert result["HasMoreDeliveryStreams"] is False

    def test_returns_sorted_names(self):
        for name in ("charlie", "alpha", "bravo"):
            _create_delivery_stream({"DeliveryStreamName": name}, _REGION, _ACCT)
        result = _list_delivery_streams({}, _REGION, _ACCT)
        assert result["DeliveryStreamNames"] == ["alpha", "bravo", "charlie"]

    def test_limit_truncates(self):
        for i in range(5):
            _create_delivery_stream({"DeliveryStreamName": f"s{i:02d}"}, _REGION, _ACCT)
        result = _list_delivery_streams({"Limit": 2}, _REGION, _ACCT)
        assert len(result["DeliveryStreamNames"]) == 2
        assert result["HasMoreDeliveryStreams"] is True

    def test_has_more_false_when_not_truncated(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _list_delivery_streams({"Limit": 10}, _REGION, _ACCT)
        assert result["HasMoreDeliveryStreams"] is False

    def test_pagination_with_exclusive_start(self):
        for name in ("a", "b", "c", "d"):
            _create_delivery_stream({"DeliveryStreamName": name}, _REGION, _ACCT)
        result = _list_delivery_streams({"ExclusiveStartDeliveryStreamName": "b"}, _REGION, _ACCT)
        assert result["DeliveryStreamNames"] == ["c", "d"]

    def test_pagination_with_start_and_limit(self):
        for name in ("a", "b", "c", "d", "e"):
            _create_delivery_stream({"DeliveryStreamName": name}, _REGION, _ACCT)
        result = _list_delivery_streams(
            {"ExclusiveStartDeliveryStreamName": "a", "Limit": 2}, _REGION, _ACCT
        )
        assert result["DeliveryStreamNames"] == ["b", "c"]
        assert result["HasMoreDeliveryStreams"] is True

    def test_nonexistent_start_name_returns_all(self):
        """If ExclusiveStartDeliveryStreamName doesn't exist, return all."""
        for name in ("a", "b"):
            _create_delivery_stream({"DeliveryStreamName": name}, _REGION, _ACCT)
        result = _list_delivery_streams(
            {"ExclusiveStartDeliveryStreamName": "nonexistent"}, _REGION, _ACCT
        )
        assert result["DeliveryStreamNames"] == ["a", "b"]

    def test_scoped_to_account_and_region(self):
        _create_delivery_stream({"DeliveryStreamName": "mine"}, _REGION, _ACCT)
        _create_delivery_stream({"DeliveryStreamName": "other"}, _REGION, "999999999999")
        _create_delivery_stream({"DeliveryStreamName": "other-region"}, "eu-west-1", _ACCT)
        result = _list_delivery_streams({}, _REGION, _ACCT)
        assert result["DeliveryStreamNames"] == ["mine"]

    def test_default_limit_is_100(self):
        """With no Limit param, up to 100 streams are returned."""
        # Create just 3 and verify they all come back
        for i in range(3):
            _create_delivery_stream({"DeliveryStreamName": f"s{i:02d}"}, _REGION, _ACCT)
        result = _list_delivery_streams({}, _REGION, _ACCT)
        assert len(result["DeliveryStreamNames"]) == 3


# ---------------------------------------------------------------------------
# PutRecord
# ---------------------------------------------------------------------------


class TestPutRecord:
    def test_puts_record_and_returns_record_id(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        data = base64.b64encode(b"hello world").decode()
        result = _put_record({"DeliveryStreamName": "s1", "Record": {"Data": data}}, _REGION, _ACCT)
        assert "RecordId" in result
        assert len(result["RecordId"]) == 32  # hex uuid without dashes
        assert result["Encrypted"] is False

    def test_data_buffered(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        data = base64.b64encode(b"payload").decode()
        _put_record({"DeliveryStreamName": "s1", "Record": {"Data": data}}, _REGION, _ACCT)
        assert len(_stream_buffers[_k("s1")]) == 1
        assert _stream_buffers[_k("s1")][0] == b"payload"

    def test_empty_data_buffered_as_empty_bytes(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _put_record({"DeliveryStreamName": "s1", "Record": {"Data": ""}}, _REGION, _ACCT)
        assert _stream_buffers[_k("s1")][0] == b""

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _put_record({"DeliveryStreamName": "nope", "Record": {"Data": ""}}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_multiple_puts_accumulate(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        for i in range(5):
            data = base64.b64encode(f"rec-{i}".encode()).decode()
            _put_record({"DeliveryStreamName": "s1", "Record": {"Data": data}}, _REGION, _ACCT)
        assert len(_stream_buffers[_k("s1")]) == 5

    def test_buffer_flush_at_threshold(self):
        """Buffer should flush when total size exceeds BUFFER_SIZE."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "logs/",
                },
            },
            _REGION,
            _ACCT,
        )
        # Create a record large enough to trigger flush
        big_data = base64.b64encode(b"x" * (BUFFER_SIZE + 1)).decode()
        with patch("robotocore.services.firehose.provider._write_to_s3"):
            _put_record({"DeliveryStreamName": "s1", "Record": {"Data": big_data}}, _REGION, _ACCT)
        # Buffer should be flushed (emptied)
        assert _stream_buffers[_k("s1")] == []


# ---------------------------------------------------------------------------
# PutRecordBatch
# ---------------------------------------------------------------------------


class TestPutRecordBatch:
    def test_puts_batch(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        records = [{"Data": base64.b64encode(f"rec-{i}".encode()).decode()} for i in range(3)]
        result = _put_record_batch({"DeliveryStreamName": "s1", "Records": records}, _REGION, _ACCT)
        assert result["FailedPutCount"] == 0
        assert result["Encrypted"] is False
        assert len(result["RequestResponses"]) == 3
        assert len(_stream_buffers[_k("s1")]) == 3

    def test_each_response_has_record_id(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        records = [{"Data": base64.b64encode(b"data").decode()}]
        result = _put_record_batch({"DeliveryStreamName": "s1", "Records": records}, _REGION, _ACCT)
        assert "RecordId" in result["RequestResponses"][0]

    def test_empty_records_list(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _put_record_batch({"DeliveryStreamName": "s1", "Records": []}, _REGION, _ACCT)
        assert result["FailedPutCount"] == 0
        assert result["RequestResponses"] == []

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _put_record_batch({"DeliveryStreamName": "nope", "Records": []}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_batch_flush_at_threshold(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                },
            },
            _REGION,
            _ACCT,
        )
        big_data = base64.b64encode(b"x" * (BUFFER_SIZE + 1)).decode()
        with patch("robotocore.services.firehose.provider._write_to_s3"):
            _put_record_batch(
                {"DeliveryStreamName": "s1", "Records": [{"Data": big_data}]}, _REGION, _ACCT
            )
        assert _stream_buffers[_k("s1")] == []


# ---------------------------------------------------------------------------
# _flush_buffer
# ---------------------------------------------------------------------------


class TestFlushBuffer:
    def test_noop_for_nonexistent_stream(self):
        _flush_buffer(_k("nonexistent"))  # Should not raise

    def test_noop_for_empty_buffer(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                },
            },
            _REGION,
            _ACCT,
        )
        with patch("robotocore.services.firehose.provider._write_to_s3") as mock_write:
            _flush_buffer(_k("s1"))
        mock_write.assert_not_called()

    def test_flush_writes_concatenated_data(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "logs/",
                },
            },
            _REGION,
            _ACCT,
        )
        _stream_buffers[_k("s1")] = [b"aaa", b"bbb"]
        with patch("robotocore.services.firehose.provider._write_to_s3") as mock_write:
            _flush_buffer(_k("s1"))
        mock_write.assert_called_once()
        args = mock_write.call_args
        assert args[0][2] == b"aaabbb"  # data argument is concatenated

    def test_flush_clears_buffer(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                },
            },
            _REGION,
            _ACCT,
        )
        _stream_buffers[_k("s1")] = [b"data"]
        with patch("robotocore.services.firehose.provider._write_to_s3"):
            _flush_buffer(_k("s1"))
        assert _stream_buffers[_k("s1")] == []

    def test_flush_noop_without_s3_config(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _stream_buffers[_k("s1")] = [b"data"]
        with patch("robotocore.services.firehose.provider._write_to_s3") as mock_write:
            _flush_buffer(_k("s1"))
        mock_write.assert_not_called()

    def test_flush_noop_without_bucket(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "Prefix": "logs/",  # No BucketARN
                },
            },
            _REGION,
            _ACCT,
        )
        _stream_buffers[_k("s1")] = [b"data"]
        with patch("robotocore.services.firehose.provider._write_to_s3") as mock_write:
            _flush_buffer(_k("s1"))
        mock_write.assert_not_called()

    def test_flush_uses_correct_bucket(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::my-bucket",
                    "Prefix": "prefix/",
                },
            },
            _REGION,
            _ACCT,
        )
        _stream_buffers[_k("s1")] = [b"data"]
        with patch("robotocore.services.firehose.provider._write_to_s3") as mock_write:
            _flush_buffer(_k("s1"))
        args = mock_write.call_args[0]
        assert args[0] == "my-bucket"  # bucket parsed from ARN


# ---------------------------------------------------------------------------
# _write_to_s3
# ---------------------------------------------------------------------------


class TestWriteToS3:
    def test_calls_moto_backend(self):
        mock_s3 = MagicMock()
        mock_get = MagicMock()
        mock_get.return_value.__getitem__ = MagicMock(
            return_value=MagicMock(__getitem__=MagicMock(return_value=mock_s3))
        )
        with patch("moto.backends.get_backend", mock_get):
            _write_to_s3("bucket", "key", b"data", "us-east-1")
        mock_s3.put_object.assert_called_once_with("bucket", "key", b"data")

    def test_silences_exceptions(self):
        """_write_to_s3 should not raise even if the backend fails."""
        with patch("moto.backends.get_backend", side_effect=Exception("fail")):
            _write_to_s3("bucket", "key", b"data", "us-east-1")  # Should not raise


# ---------------------------------------------------------------------------
# UpdateDestination
# ---------------------------------------------------------------------------


class TestUpdateDestination:
    def test_updates_s3_prefix(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "old/",
                },
            },
            _REGION,
            _ACCT,
        )
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
                "ExtendedS3DestinationUpdate": {"Prefix": "new/"},
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["s3_config"]["Prefix"] == "new/"
        # Original BucketARN preserved
        assert _delivery_streams[_k("s1")]["s3_config"]["BucketARN"] == "arn:aws:s3:::mybucket"

    def test_s3_destination_update_fallback(self):
        """S3DestinationUpdate is used when ExtendedS3DestinationUpdate is not provided."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "S3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "Prefix": "old/",
                },
            },
            _REGION,
            _ACCT,
        )
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
                "S3DestinationUpdate": {"Prefix": "new/"},
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["s3_config"]["Prefix"] == "new/"

    def test_increments_version_id(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["version_id"] == 2

    def test_version_mismatch_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        with pytest.raises(FirehoseError) as exc:
            _update_destination(
                {
                    "DeliveryStreamName": "s1",
                    "DestinationId": "dest-1",
                    "CurrentDeliveryStreamVersionId": "99",
                },
                _REGION,
                _ACCT,
            )
        assert exc.value.code == "InvalidArgumentException"

    def test_version_id_as_int_accepted(self):
        """AWS accepts version ID as both string and int."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        # Should not raise
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": 1,
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["version_id"] == 2

    def test_no_version_check_when_none(self):
        """If CurrentDeliveryStreamVersionId is not provided, skip version check."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["version_id"] == 2

    def test_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _update_destination(
                {"DeliveryStreamName": "nope", "DestinationId": "dest-1"}, _REGION, _ACCT
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_missing_destination_id_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        with pytest.raises(FirehoseError) as exc:
            _update_destination(
                {"DeliveryStreamName": "s1", "CurrentDeliveryStreamVersionId": "1"},
                _REGION,
                _ACCT,
            )
        assert exc.value.code == "ValidationException"

    def test_deep_merge_buffering_hints(self):
        """When BufferingHints is a dict in both old and new, it should be merged."""
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "ExtendedS3DestinationConfiguration": {
                    "BucketARN": "arn:aws:s3:::mybucket",
                    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300},
                },
            },
            _REGION,
            _ACCT,
        )
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
                "ExtendedS3DestinationUpdate": {
                    "BufferingHints": {"SizeInMBs": 10},
                },
            },
            _REGION,
            _ACCT,
        )
        hints = _delivery_streams[_k("s1")]["s3_config"]["BufferingHints"]
        assert hints["SizeInMBs"] == 10
        assert hints["IntervalInSeconds"] == 300  # Preserved from original

    def test_sequential_updates_increment_version(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "1",
            },
            _REGION,
            _ACCT,
        )
        _update_destination(
            {
                "DeliveryStreamName": "s1",
                "DestinationId": "dest-1",
                "CurrentDeliveryStreamVersionId": "2",
            },
            _REGION,
            _ACCT,
        )
        assert _delivery_streams[_k("s1")]["version_id"] == 3


# ---------------------------------------------------------------------------
# StartDeliveryStreamEncryption / StopDeliveryStreamEncryption
# ---------------------------------------------------------------------------


class TestEncryption:
    def test_start_encryption_aws_owned(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _start_delivery_stream_encryption(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamEncryptionInput": {"KeyType": "AWS_OWNED_CMK"},
            },
            _REGION,
            _ACCT,
        )
        assert result == {}
        enc = _delivery_streams[_k("s1")]["encryption"]
        assert enc["Status"] == "ENABLED"
        assert enc["KeyType"] == "AWS_OWNED_CMK"

    def test_start_encryption_customer_managed(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _start_delivery_stream_encryption(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamEncryptionConfigurationInput": {
                    "KeyType": "CUSTOMER_MANAGED_CMK",
                    "KeyARN": "arn:aws:kms:us-east-1:123456789012:key/abc-123",
                },
            },
            _REGION,
            _ACCT,
        )
        enc = _delivery_streams[_k("s1")]["encryption"]
        assert enc["KeyType"] == "CUSTOMER_MANAGED_CMK"
        assert enc["KeyARN"] == "arn:aws:kms:us-east-1:123456789012:key/abc-123"

    def test_start_encryption_default_key_type(self):
        """Default KeyType is AWS_OWNED_CMK when not specified."""
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _start_delivery_stream_encryption({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert _delivery_streams[_k("s1")]["encryption"]["KeyType"] == "AWS_OWNED_CMK"

    def test_start_encryption_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _start_delivery_stream_encryption({"DeliveryStreamName": "nope"}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_stop_encryption(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _start_delivery_stream_encryption(
            {
                "DeliveryStreamName": "s1",
                "DeliveryStreamEncryptionInput": {"KeyType": "AWS_OWNED_CMK"},
            },
            _REGION,
            _ACCT,
        )
        result = _stop_delivery_stream_encryption({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert result == {}
        assert _delivery_streams[_k("s1")]["encryption"]["Status"] == "DISABLED"

    def test_stop_encryption_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _stop_delivery_stream_encryption({"DeliveryStreamName": "nope"}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# TagDeliveryStream / UntagDeliveryStream / ListTagsForDeliveryStream
# ---------------------------------------------------------------------------


class TestTagOperations:
    def test_tag_adds_tags(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _tag_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [{"Key": "env", "Value": "prod"}],
            },
            _REGION,
            _ACCT,
        )
        result = _list_tags_for_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        tags = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags == {"env": "prod"}

    def test_tag_overwrites_existing(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [{"Key": "env", "Value": "dev"}],
            },
            _REGION,
            _ACCT,
        )
        _tag_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [{"Key": "env", "Value": "prod"}],
            },
            _REGION,
            _ACCT,
        )
        result = _list_tags_for_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert result["Tags"][0]["Value"] == "prod"

    def test_tag_multiple_at_once(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _tag_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                    {"Key": "c", "Value": "3"},
                ],
            },
            _REGION,
            _ACCT,
        )
        result = _list_tags_for_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert len(result["Tags"]) == 3

    def test_tag_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _tag_delivery_stream(
                {"DeliveryStreamName": "nope", "Tags": [{"Key": "k", "Value": "v"}]},
                _REGION,
                _ACCT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_untag_removes_keys(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                ],
            },
            _REGION,
            _ACCT,
        )
        _untag_delivery_stream({"DeliveryStreamName": "s1", "TagKeys": ["a"]}, _REGION, _ACCT)
        result = _list_tags_for_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        tags = {t["Key"]: t["Value"] for t in result["Tags"]}
        assert tags == {"b": "2"}

    def test_untag_nonexistent_key_is_noop(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _untag_delivery_stream(
            {"DeliveryStreamName": "s1", "TagKeys": ["nonexistent"]}, _REGION, _ACCT
        )
        result = _list_tags_for_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert result["Tags"] == []

    def test_untag_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _untag_delivery_stream({"DeliveryStreamName": "nope", "TagKeys": ["k"]}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_list_tags_not_found_raises(self):
        with pytest.raises(FirehoseError) as exc:
            _list_tags_for_delivery_stream({"DeliveryStreamName": "nope"}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_list_tags_sorted(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [
                    {"Key": "c", "Value": "3"},
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                ],
            },
            _REGION,
            _ACCT,
        )
        result = _list_tags_for_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        keys = [t["Key"] for t in result["Tags"]]
        assert keys == ["a", "b", "c"]

    def test_list_tags_pagination_limit(self):
        tags = [{"Key": f"key-{i:02d}", "Value": f"val-{i}"} for i in range(10)]
        _create_delivery_stream({"DeliveryStreamName": "s1", "Tags": tags}, _REGION, _ACCT)
        result = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "s1", "Limit": 3}, _REGION, _ACCT
        )
        assert len(result["Tags"]) == 3
        assert result["HasMoreTags"] is True

    def test_list_tags_pagination_exclusive_start(self):
        tags = [{"Key": f"key-{i:02d}", "Value": f"val-{i}"} for i in range(5)]
        _create_delivery_stream({"DeliveryStreamName": "s1", "Tags": tags}, _REGION, _ACCT)
        result = _list_tags_for_delivery_stream(
            {"DeliveryStreamName": "s1", "ExclusiveStartTagKey": "key-02"}, _REGION, _ACCT
        )
        keys = [t["Key"] for t in result["Tags"]]
        assert "key-00" not in keys
        assert "key-01" not in keys
        assert "key-02" not in keys
        assert "key-03" in keys
        assert "key-04" in keys

    def test_list_tags_has_more_false_at_end(self):
        _create_delivery_stream(
            {
                "DeliveryStreamName": "s1",
                "Tags": [{"Key": "only", "Value": "one"}],
            },
            _REGION,
            _ACCT,
        )
        result = _list_tags_for_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert result["HasMoreTags"] is False

    def test_list_tags_default_limit_50(self):
        """Default limit for ListTagsForDeliveryStream is 50."""
        tags = [{"Key": f"key-{i:03d}", "Value": f"val-{i}"} for i in range(55)]
        _create_delivery_stream({"DeliveryStreamName": "s1", "Tags": tags}, _REGION, _ACCT)
        result = _list_tags_for_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert len(result["Tags"]) == 50
        assert result["HasMoreTags"] is True


# ---------------------------------------------------------------------------
# Cross-account / cross-region isolation
# ---------------------------------------------------------------------------


class TestIsolation:
    def test_same_name_different_accounts(self):
        _create_delivery_stream({"DeliveryStreamName": "shared"}, _REGION, "111111111111")
        _create_delivery_stream({"DeliveryStreamName": "shared"}, _REGION, "222222222222")
        r1 = _describe_delivery_stream({"DeliveryStreamName": "shared"}, _REGION, "111111111111")
        r2 = _describe_delivery_stream({"DeliveryStreamName": "shared"}, _REGION, "222222222222")
        assert (
            r1["DeliveryStreamDescription"]["DeliveryStreamARN"]
            != r2["DeliveryStreamDescription"]["DeliveryStreamARN"]
        )

    def test_same_name_different_regions(self):
        _create_delivery_stream({"DeliveryStreamName": "shared"}, "us-east-1", _ACCT)
        _create_delivery_stream({"DeliveryStreamName": "shared"}, "eu-west-1", _ACCT)
        # Both should be independently describable
        r1 = _describe_delivery_stream({"DeliveryStreamName": "shared"}, "us-east-1", _ACCT)
        r2 = _describe_delivery_stream({"DeliveryStreamName": "shared"}, "eu-west-1", _ACCT)
        assert "us-east-1" in r1["DeliveryStreamDescription"]["DeliveryStreamARN"]
        assert "eu-west-1" in r2["DeliveryStreamDescription"]["DeliveryStreamARN"]

    def test_delete_in_one_account_does_not_affect_other(self):
        _create_delivery_stream({"DeliveryStreamName": "shared"}, _REGION, "111111111111")
        _create_delivery_stream({"DeliveryStreamName": "shared"}, _REGION, "222222222222")
        _delete_delivery_stream({"DeliveryStreamName": "shared"}, _REGION, "111111111111")
        # Other account's stream should still exist
        r = _describe_delivery_stream({"DeliveryStreamName": "shared"}, _REGION, "222222222222")
        assert r["DeliveryStreamDescription"]["DeliveryStreamName"] == "shared"

    def test_put_record_scoped_to_account(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, "111111111111")
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, "222222222222")
        data = base64.b64encode(b"hello").decode()
        _put_record(
            {"DeliveryStreamName": "s1", "Record": {"Data": data}},
            _REGION,
            "111111111111",
        )
        assert len(_stream_buffers[_k("s1", account_id="111111111111")]) == 1
        assert len(_stream_buffers[_k("s1", account_id="222222222222")]) == 0


# ---------------------------------------------------------------------------
# Post-delete operations
# ---------------------------------------------------------------------------


class TestPostDeleteBehavior:
    def test_put_record_after_delete_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        with pytest.raises(FirehoseError) as exc:
            _put_record(
                {
                    "DeliveryStreamName": "s1",
                    "Record": {"Data": base64.b64encode(b"x").decode()},
                },
                _REGION,
                _ACCT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_put_record_batch_after_delete_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        with pytest.raises(FirehoseError) as exc:
            _put_record_batch({"DeliveryStreamName": "s1", "Records": []}, _REGION, _ACCT)
        assert exc.value.code == "ResourceNotFoundException"

    def test_update_destination_after_delete_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        with pytest.raises(FirehoseError) as exc:
            _update_destination(
                {"DeliveryStreamName": "s1", "DestinationId": "dest-1"}, _REGION, _ACCT
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_tag_after_delete_raises(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        with pytest.raises(FirehoseError) as exc:
            _tag_delivery_stream(
                {"DeliveryStreamName": "s1", "Tags": [{"Key": "k", "Value": "v"}]},
                _REGION,
                _ACCT,
            )
        assert exc.value.code == "ResourceNotFoundException"

    def test_can_recreate_after_delete(self):
        _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        _delete_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        result = _create_delivery_stream({"DeliveryStreamName": "s1"}, _REGION, _ACCT)
        assert "DeliveryStreamARN" in result
