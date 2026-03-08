"""Compatibility tests for Kinesis Video Streams service."""

import uuid

import pytest

from .conftest import make_client


@pytest.fixture
def kinesisvideo_client():
    return make_client("kinesisvideo")


@pytest.fixture
def stream_name():
    return f"test-stream-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def created_stream(kinesisvideo_client, stream_name):
    """Create a stream and clean it up after the test."""
    resp = kinesisvideo_client.create_stream(StreamName=stream_name, DataRetentionInHours=24)
    arn = resp["StreamARN"]
    yield {"StreamName": stream_name, "StreamARN": arn}
    try:
        kinesisvideo_client.delete_stream(StreamARN=arn)
    except Exception:
        pass


class TestKinesisVideoCompat:
    def test_create_stream(self, kinesisvideo_client):
        name = f"test-stream-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_stream(StreamName=name, DataRetentionInHours=24)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "StreamARN" in resp
        assert f"stream/{name}/" in resp["StreamARN"]
        # cleanup
        kinesisvideo_client.delete_stream(StreamARN=resp["StreamARN"])

    def test_describe_stream(self, kinesisvideo_client, created_stream):
        resp = kinesisvideo_client.describe_stream(StreamName=created_stream["StreamName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        info = resp["StreamInfo"]
        assert info["StreamName"] == created_stream["StreamName"]
        assert info["StreamARN"] == created_stream["StreamARN"]
        assert info["Status"] == "ACTIVE"
        assert info["DataRetentionInHours"] == 24

    def test_describe_stream_by_arn(self, kinesisvideo_client, created_stream):
        resp = kinesisvideo_client.describe_stream(StreamARN=created_stream["StreamARN"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["StreamInfo"]["StreamName"] == created_stream["StreamName"]

    def test_list_streams(self, kinesisvideo_client, created_stream):
        resp = kinesisvideo_client.list_streams()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "StreamInfoList" in resp
        names = [s["StreamName"] for s in resp["StreamInfoList"]]
        assert created_stream["StreamName"] in names

    def test_list_streams_returns_stream_info_fields(self, kinesisvideo_client, created_stream):
        resp = kinesisvideo_client.list_streams()
        match = [
            s for s in resp["StreamInfoList"] if s["StreamName"] == created_stream["StreamName"]
        ]
        assert len(match) == 1
        info = match[0]
        assert info["StreamARN"] == created_stream["StreamARN"]
        assert info["Status"] == "ACTIVE"
        assert "CreationTime" in info
        assert info["DataRetentionInHours"] == 24

    def test_delete_stream(self, kinesisvideo_client):
        name = f"test-stream-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_stream(StreamName=name, DataRetentionInHours=24)
        arn = resp["StreamARN"]

        del_resp = kinesisvideo_client.delete_stream(StreamARN=arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify stream is gone from list
        list_resp = kinesisvideo_client.list_streams()
        names = [s["StreamName"] for s in list_resp["StreamInfoList"]]
        assert name not in names

    def test_create_stream_without_retention(self, kinesisvideo_client):
        name = f"test-stream-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_stream(StreamName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        arn = resp["StreamARN"]

        desc = kinesisvideo_client.describe_stream(StreamName=name)
        # When no retention is specified, the field is either 0 or absent
        assert desc["StreamInfo"].get("DataRetentionInHours", 0) == 0

        kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_create_duplicate_stream_fails(self, kinesisvideo_client, created_stream):
        with pytest.raises(Exception):
            kinesisvideo_client.create_stream(
                StreamName=created_stream["StreamName"], DataRetentionInHours=24
            )

    def test_get_data_endpoint(self, kinesisvideo_client, created_stream):
        resp = kinesisvideo_client.get_data_endpoint(
            StreamName=created_stream["StreamName"],
            APIName="GET_MEDIA",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DataEndpoint" in resp
        assert resp["DataEndpoint"].startswith("http")
