"""Tests for robotocore.services.dynamodbstreams.provider."""

import base64
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.dynamodbstreams.provider import (
    _ACTION_MAP,
    StreamsError,
    _describe_stream,
    _error,
    _get_records,
    _get_shard_iterator,
    _json,
    _list_streams,
    _shard_iterators,
    handle_dynamodbstreams_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_shard(shard_id="shardId-000", starting_seq=0, items=None, created_on=None):
    from datetime import UTC, datetime

    return SimpleNamespace(
        id=shard_id,
        starting_sequence_number=starting_seq,
        items=items or [],
        created_on=created_on or datetime.now(tz=UTC),
        to_json=lambda: {
            "ShardId": shard_id,
            "SequenceNumberRange": {
                "StartingSequenceNumber": str(starting_seq),
            },
        },
    )


def _make_table(
    name="test-table",
    stream_label="2024-01-01T00:00:00",
    stream_shard=None,
    stream_view_type="NEW_AND_OLD_IMAGES",
    schema=None,
):
    shard = stream_shard or _make_shard()
    tbl = SimpleNamespace(
        name=name,
        latest_stream_label=stream_label,
        stream_shard=shard,
        stream_specification={"StreamViewType": stream_view_type},
        schema=schema or [{"AttributeName": "id", "KeyType": "HASH"}],
    )
    tbl.describe = lambda base_key="Table": {
        "Table": {
            "TableName": name,
            "LatestStreamArn": (
                f"arn:aws:dynamodb:us-east-1:123456789012:table/{name}/stream/{stream_label}"
            ),
            "LatestStreamLabel": stream_label,
        }
    }
    return tbl


def _make_backend(tables=None):
    backend = MagicMock()
    backend.tables = {t.name: t for t in (tables or [])}

    def get_table(name):
        if name in backend.tables:
            return backend.tables[name]
        raise Exception(f"Table {name} not found")

    backend.get_table = get_table
    return backend


STREAM_ARN = "arn:aws:dynamodb:us-east-1:123456789012:table/test-table/stream/2024-01-01T00:00:00"
REGION = "us-east-1"
ACCOUNT = "123456789012"


# ---------------------------------------------------------------------------
# StreamsError
# ---------------------------------------------------------------------------


class TestStreamsError:
    def test_default_status(self):
        err = StreamsError("ValidationException", "bad input")
        assert err.code == "ValidationException"
        assert err.message == "bad input"
        assert err.status == 400

    def test_custom_status(self):
        err = StreamsError("InternalServerError", "oops", 500)
        assert err.status == 500


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json(200, {"key": "value"})
        assert resp.status_code == 200
        assert resp.media_type == "application/x-amz-json-1.0"
        body = json.loads(resp.body)
        assert body == {"key": "value"}

    def test_error_response(self):
        resp = _error("SomeException", "something broke", 400)
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "SomeException"
        assert body["Message"] == "something broke"

    def test_json_with_non_serializable_uses_str(self):
        from datetime import datetime

        resp = _json(200, {"ts": datetime(2024, 1, 1)})
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "2024" in body["ts"]


# ---------------------------------------------------------------------------
# _list_streams
# ---------------------------------------------------------------------------


class TestListStreams:
    @patch("robotocore.services.dynamodbstreams.provider._get_dynamodb_backend")
    def test_lists_all_streams(self, mock_get_backend):
        table = _make_table()
        backend = _make_backend([table])
        mock_get_backend.return_value = backend

        result = _list_streams({}, REGION, ACCOUNT)
        assert len(result["Streams"]) == 1
        assert result["Streams"][0]["TableName"] == "test-table"

    @patch("robotocore.services.dynamodbstreams.provider._get_dynamodb_backend")
    def test_filters_by_table_name(self, mock_get_backend):
        t1 = _make_table(name="table1")
        t2 = _make_table(name="table2")
        backend = _make_backend([t1, t2])
        mock_get_backend.return_value = backend

        result = _list_streams({"TableName": "table1"}, REGION, ACCOUNT)
        assert len(result["Streams"]) == 1
        assert result["Streams"][0]["TableName"] == "table1"

    @patch("robotocore.services.dynamodbstreams.provider._get_dynamodb_backend")
    def test_skips_tables_without_streams(self, mock_get_backend):
        table = _make_table(stream_label=None)
        backend = _make_backend([table])
        mock_get_backend.return_value = backend

        result = _list_streams({}, REGION, ACCOUNT)
        assert result["Streams"] == []

    @patch("robotocore.services.dynamodbstreams.provider._get_dynamodb_backend")
    def test_empty_tables(self, mock_get_backend):
        backend = _make_backend([])
        mock_get_backend.return_value = backend

        result = _list_streams({}, REGION, ACCOUNT)
        assert result["Streams"] == []


# ---------------------------------------------------------------------------
# _describe_stream
# ---------------------------------------------------------------------------


class TestDescribeStream:
    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_describe_valid_stream(self, mock_get_table):
        table = _make_table()
        mock_get_table.return_value = table

        result = _describe_stream({"StreamArn": STREAM_ARN}, REGION, ACCOUNT)
        desc = result["StreamDescription"]
        assert desc["StreamArn"] == STREAM_ARN
        assert desc["TableName"] == "test-table"
        assert desc["StreamStatus"] == "ENABLED"
        assert desc["StreamViewType"] == "NEW_AND_OLD_IMAGES"
        assert len(desc["Shards"]) == 1

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_describe_missing_stream_arn(self, mock_get_table):
        with pytest.raises(StreamsError) as exc_info:
            _describe_stream({}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationException"

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_describe_nonexistent_stream(self, mock_get_table):
        mock_get_table.side_effect = Exception("not found")

        with pytest.raises(StreamsError) as exc_info:
            _describe_stream({"StreamArn": "arn:bad"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ResourceNotFoundException"

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_describe_table_without_stream_label(self, mock_get_table):
        table = _make_table(stream_label=None)
        mock_get_table.return_value = table

        with pytest.raises(StreamsError) as exc_info:
            _describe_stream({"StreamArn": STREAM_ARN}, REGION, ACCOUNT)
        assert exc_info.value.code == "ResourceNotFoundException"

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_describe_stream_no_shard(self, mock_get_table):
        table = _make_table()
        table.stream_shard = None
        mock_get_table.return_value = table

        result = _describe_stream({"StreamArn": STREAM_ARN}, REGION, ACCOUNT)
        assert result["StreamDescription"]["Shards"] == []


# ---------------------------------------------------------------------------
# _get_shard_iterator
# ---------------------------------------------------------------------------


class TestGetShardIterator:
    def setup_method(self):
        _shard_iterators.clear()

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_trim_horizon(self, mock_get_table):
        shard = _make_shard(starting_seq=100, items=[1, 2, 3])
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        result = _get_shard_iterator(
            {
                "StreamArn": STREAM_ARN,
                "ShardId": "shardId-000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
            REGION,
            ACCOUNT,
        )
        iterator_id = result["ShardIterator"]
        state = json.loads(base64.b64decode(iterator_id))
        assert state["sequence_number"] == 100

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_latest(self, mock_get_table):
        shard = _make_shard(starting_seq=100, items=[1, 2, 3])
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        result = _get_shard_iterator(
            {
                "StreamArn": STREAM_ARN,
                "ShardId": "shardId-000",
                "ShardIteratorType": "LATEST",
            },
            REGION,
            ACCOUNT,
        )
        state = json.loads(base64.b64decode(result["ShardIterator"]))
        assert state["sequence_number"] == 103  # 100 + 3 items

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_at_sequence_number(self, mock_get_table):
        shard = _make_shard(starting_seq=100)
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        result = _get_shard_iterator(
            {
                "StreamArn": STREAM_ARN,
                "ShardId": "shardId-000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "SequenceNumber": "150",
            },
            REGION,
            ACCOUNT,
        )
        state = json.loads(base64.b64decode(result["ShardIterator"]))
        assert state["sequence_number"] == 150

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_after_sequence_number(self, mock_get_table):
        shard = _make_shard(starting_seq=100)
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        result = _get_shard_iterator(
            {
                "StreamArn": STREAM_ARN,
                "ShardId": "shardId-000",
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "SequenceNumber": "150",
            },
            REGION,
            ACCOUNT,
        )
        state = json.loads(base64.b64decode(result["ShardIterator"]))
        assert state["sequence_number"] == 151

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_invalid_iterator_type(self, mock_get_table):
        shard = _make_shard()
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        with pytest.raises(StreamsError) as exc_info:
            _get_shard_iterator(
                {
                    "StreamArn": STREAM_ARN,
                    "ShardId": "shardId-000",
                    "ShardIteratorType": "BOGUS",
                },
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ValidationException"

    def test_missing_stream_arn(self):
        with pytest.raises(StreamsError) as exc_info:
            _get_shard_iterator({"ShardId": "s"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationException"

    def test_missing_shard_id(self):
        with pytest.raises(StreamsError) as exc_info:
            _get_shard_iterator({"StreamArn": STREAM_ARN}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationException"

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_nonexistent_stream(self, mock_get_table):
        mock_get_table.side_effect = Exception("not found")
        with pytest.raises(StreamsError) as exc_info:
            _get_shard_iterator(
                {"StreamArn": "arn:bad", "ShardId": "s"},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ResourceNotFoundException"

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_wrong_shard_id(self, mock_get_table):
        shard = _make_shard(shard_id="shardId-000")
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        with pytest.raises(StreamsError) as exc_info:
            _get_shard_iterator(
                {
                    "StreamArn": STREAM_ARN,
                    "ShardId": "shardId-999",
                },
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ResourceNotFoundException"

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_iterator_stored_in_global_dict(self, mock_get_table):
        shard = _make_shard()
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        result = _get_shard_iterator(
            {
                "StreamArn": STREAM_ARN,
                "ShardId": "shardId-000",
                "ShardIteratorType": "TRIM_HORIZON",
            },
            REGION,
            ACCOUNT,
        )
        assert result["ShardIterator"] in _shard_iterators


# ---------------------------------------------------------------------------
# _get_records
# ---------------------------------------------------------------------------


class TestGetRecords:
    def setup_method(self):
        _shard_iterators.clear()

    def test_missing_iterator_raises(self):
        with pytest.raises(StreamsError) as exc_info:
            _get_records({}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationException"

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_get_records_with_valid_iterator(self, mock_get_table):
        records = [
            {"dynamodb": {"SequenceNumber": "5"}},
            {"dynamodb": {"SequenceNumber": "6"}},
        ]
        shard = _make_shard(shard_id="shardId-000")
        shard.get = MagicMock(return_value=records)
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        # Create a valid iterator via base64-encoded state
        state = {
            "stream_arn": STREAM_ARN,
            "shard_id": "shardId-000",
            "sequence_number": 0,
            "region": REGION,
            "account_id": ACCOUNT,
        }
        iterator_id = base64.b64encode(json.dumps(state).encode()).decode()
        _shard_iterators[iterator_id] = state

        result = _get_records({"ShardIterator": iterator_id}, REGION, ACCOUNT)
        assert len(result["Records"]) == 2
        assert "NextShardIterator" in result

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_get_records_empty(self, mock_get_table):
        shard = _make_shard(shard_id="shardId-000")
        shard.get = MagicMock(return_value=[])
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        state = {
            "stream_arn": STREAM_ARN,
            "shard_id": "shardId-000",
            "sequence_number": 0,
            "region": REGION,
            "account_id": ACCOUNT,
        }
        iterator_id = base64.b64encode(json.dumps(state).encode()).decode()
        _shard_iterators[iterator_id] = state

        result = _get_records({"ShardIterator": iterator_id}, REGION, ACCOUNT)
        assert result["Records"] == []
        assert "NextShardIterator" in result

    def test_invalid_iterator_base64_raises_expired(self):
        with pytest.raises(StreamsError) as exc_info:
            _get_records(
                {"ShardIterator": "not-valid-base64!!!"},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ExpiredIteratorException"

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_get_records_cleans_up_old_iterator(self, mock_get_table):
        records = [{"dynamodb": {"SequenceNumber": "5"}}]
        shard = _make_shard(shard_id="shardId-000")
        shard.get = MagicMock(return_value=records)
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        state = {
            "stream_arn": STREAM_ARN,
            "shard_id": "shardId-000",
            "sequence_number": 0,
            "region": REGION,
            "account_id": ACCOUNT,
        }
        iterator_id = base64.b64encode(json.dumps(state).encode()).decode()
        _shard_iterators[iterator_id] = state

        result = _get_records({"ShardIterator": iterator_id}, REGION, ACCOUNT)
        # Old iterator cleaned up (next_seq=6 != 0, so different key)
        assert iterator_id not in _shard_iterators
        # New iterator stored
        assert result["NextShardIterator"] in _shard_iterators

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_get_records_wrong_shard_returns_empty(self, mock_get_table):
        shard = _make_shard(shard_id="shardId-OTHER")
        shard.get = MagicMock(return_value=[])
        table = _make_table(stream_shard=shard)
        mock_get_table.return_value = table

        state = {
            "stream_arn": STREAM_ARN,
            "shard_id": "shardId-000",
            "sequence_number": 0,
            "region": REGION,
            "account_id": ACCOUNT,
        }
        iterator_id = base64.b64encode(json.dumps(state).encode()).decode()
        _shard_iterators[iterator_id] = state

        result = _get_records({"ShardIterator": iterator_id}, REGION, ACCOUNT)
        assert result["Records"] == []

    @patch("robotocore.services.dynamodbstreams.provider._get_table_from_stream_arn")
    def test_get_records_resource_not_found(self, mock_get_table):
        mock_get_table.side_effect = Exception("gone")

        state = {
            "stream_arn": STREAM_ARN,
            "shard_id": "shardId-000",
            "sequence_number": 0,
            "region": REGION,
            "account_id": ACCOUNT,
        }
        iterator_id = base64.b64encode(json.dumps(state).encode()).decode()
        _shard_iterators[iterator_id] = state

        with pytest.raises(StreamsError) as exc_info:
            _get_records({"ShardIterator": iterator_id}, REGION, ACCOUNT)
        assert exc_info.value.code == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# handle_dynamodbstreams_request (async handler)
# ---------------------------------------------------------------------------


def _make_request(target: str, body: bytes = b"{}"):
    """Create a mock Starlette-like request with async body()."""
    request = MagicMock()
    request.headers = {"x-amz-target": target}

    async def async_body():
        return body

    request.body = async_body
    return request


class TestHandleRequest:
    async def test_dispatches_to_list_streams(self):
        mock_list = MagicMock(return_value={"Streams": []})
        request = _make_request("DynamoDBStreams_20120810.ListStreams")

        with patch.dict(_ACTION_MAP, {"ListStreams": mock_list}):
            resp = await handle_dynamodbstreams_request(request, REGION, ACCOUNT)
        assert resp.status_code == 200
        mock_list.assert_called_once_with({}, REGION, ACCOUNT)

    async def test_unknown_operation(self):
        request = _make_request("DynamoDBStreams_20120810.BogusOp")

        resp = await handle_dynamodbstreams_request(request, REGION, ACCOUNT)
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "UnknownOperationException"

    async def test_streams_error_returns_proper_response(self):
        mock_desc = MagicMock(
            side_effect=StreamsError("ResourceNotFoundException", "not found", 400)
        )
        request = _make_request(
            "DynamoDBStreams_20120810.DescribeStream",
            json.dumps({"StreamArn": "x"}).encode(),
        )

        with patch.dict(_ACTION_MAP, {"DescribeStream": mock_desc}):
            resp = await handle_dynamodbstreams_request(request, REGION, ACCOUNT)
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "ResourceNotFoundException"

    async def test_unexpected_exception_returns_500(self):
        mock_desc = MagicMock(side_effect=RuntimeError("kaboom"))
        request = _make_request(
            "DynamoDBStreams_20120810.DescribeStream",
            json.dumps({"StreamArn": "x"}).encode(),
        )

        with patch.dict(_ACTION_MAP, {"DescribeStream": mock_desc}):
            resp = await handle_dynamodbstreams_request(request, REGION, ACCOUNT)
        assert resp.status_code == 500
        body = json.loads(resp.body)
        assert body["__type"] == "InternalServerError"

    async def test_empty_body(self):
        mock_list = MagicMock(return_value={"Streams": []})
        request = _make_request("DynamoDBStreams_20120810.ListStreams", b"")

        with patch.dict(_ACTION_MAP, {"ListStreams": mock_list}):
            resp = await handle_dynamodbstreams_request(request, REGION, ACCOUNT)
        assert resp.status_code == 200
        mock_list.assert_called_once_with({}, REGION, ACCOUNT)

    async def test_target_without_dot(self):
        """When x-amz-target has no dot, operation is the full string."""
        request = _make_request("NoDotHere")

        resp = await handle_dynamodbstreams_request(request, REGION, ACCOUNT)
        # "NoDotHere" not in ACTION_MAP -> unknown operation
        assert resp.status_code == 400
