"""Compatibility tests for Kinesis Video Streams service."""

import uuid

import pytest
from botocore.exceptions import ClientError

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


class TestKinesisVideoSignalingChannel:
    """Tests for signaling channel CRUD operations."""

    def test_create_signaling_channel(self, kinesisvideo_client):
        name = f"test-sig-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_signaling_channel(ChannelName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ChannelARN" in resp
        assert f"channel/{name}/" in resp["ChannelARN"]
        # cleanup
        kinesisvideo_client.delete_signaling_channel(ChannelARN=resp["ChannelARN"])

    def test_describe_signaling_channel(self, kinesisvideo_client):
        name = f"test-sig-{uuid.uuid4().hex[:8]}"
        create_resp = kinesisvideo_client.create_signaling_channel(ChannelName=name)
        sig_arn = create_resp["ChannelARN"]
        try:
            resp = kinesisvideo_client.describe_signaling_channel(ChannelARN=sig_arn)
            info = resp["ChannelInfo"]
            assert info["ChannelName"] == name
            assert info["ChannelARN"] == sig_arn
            assert info["ChannelStatus"] == "ACTIVE"
            assert "ChannelType" in info
            assert "CreationTime" in info
            assert "Version" in info
        finally:
            kinesisvideo_client.delete_signaling_channel(ChannelARN=sig_arn)

    def test_list_signaling_channels(self, kinesisvideo_client):
        name = f"test-sig-{uuid.uuid4().hex[:8]}"
        create_resp = kinesisvideo_client.create_signaling_channel(ChannelName=name)
        sig_arn = create_resp["ChannelARN"]
        try:
            resp = kinesisvideo_client.list_signaling_channels()
            assert "ChannelInfoList" in resp
            arns = [ch["ChannelARN"] for ch in resp["ChannelInfoList"]]
            assert sig_arn in arns
        finally:
            kinesisvideo_client.delete_signaling_channel(ChannelARN=sig_arn)

    def test_delete_signaling_channel(self, kinesisvideo_client):
        name = f"test-sig-{uuid.uuid4().hex[:8]}"
        create_resp = kinesisvideo_client.create_signaling_channel(ChannelName=name)
        sig_arn = create_resp["ChannelARN"]
        del_resp = kinesisvideo_client.delete_signaling_channel(ChannelARN=sig_arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        listed = kinesisvideo_client.list_signaling_channels()
        arns = [ch["ChannelARN"] for ch in listed["ChannelInfoList"]]
        assert sig_arn not in arns

    def test_describe_signaling_channel_not_found(self, kinesisvideo_client):
        fake_arn = "arn:aws:kinesisvideo:us-east-1:123456789012:channel/nonexistent/0"
        with pytest.raises(ClientError) as exc_info:
            kinesisvideo_client.describe_signaling_channel(ChannelARN=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestKinesisVideoTagging:
    """Tests for tagging operations on KinesisVideo resources."""

    def test_tag_resource(self, kinesisvideo_client, created_stream):
        resp = kinesisvideo_client.tag_resource(
            ResourceARN=created_stream["StreamARN"],
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "dev"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_tags_for_resource(self, kinesisvideo_client, created_stream):
        kinesisvideo_client.tag_resource(
            ResourceARN=created_stream["StreamARN"],
            Tags=[{"Key": "env", "Value": "staging"}],
        )
        resp = kinesisvideo_client.list_tags_for_resource(ResourceARN=created_stream["StreamARN"])
        assert "Tags" in resp
        assert resp["Tags"]["env"] == "staging"

    def test_untag_resource(self, kinesisvideo_client, created_stream):
        kinesisvideo_client.tag_resource(
            ResourceARN=created_stream["StreamARN"],
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "dev"}],
        )
        kinesisvideo_client.untag_resource(
            ResourceARN=created_stream["StreamARN"],
            TagKeyList=["env"],
        )
        resp = kinesisvideo_client.list_tags_for_resource(ResourceARN=created_stream["StreamARN"])
        assert "env" not in resp.get("Tags", {})
        assert resp["Tags"]["team"] == "dev"

    def test_list_tags_for_resource_empty(self, kinesisvideo_client, created_stream):
        resp = kinesisvideo_client.list_tags_for_resource(ResourceARN=created_stream["StreamARN"])
        assert "Tags" in resp
        assert isinstance(resp["Tags"], dict)


class TestKinesisVideoUpdates:
    """Tests for KinesisVideo update and tag_stream operations."""

    def test_tag_stream(self, kinesisvideo_client):
        name = f"test-stream-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_stream(StreamName=name, DataRetentionInHours=24)
        arn = resp["StreamARN"]
        try:
            tag_resp = kinesisvideo_client.tag_stream(
                StreamName=name, Tags={"env": "test", "team": "dev"}
            )
            assert tag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_untag_stream(self, kinesisvideo_client):
        name = f"test-stream-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_stream(StreamName=name, DataRetentionInHours=24)
        arn = resp["StreamARN"]
        try:
            kinesisvideo_client.tag_stream(StreamName=name, Tags={"env": "test", "team": "dev"})
            untag_resp = kinesisvideo_client.untag_stream(StreamName=name, TagKeyList=["env"])
            assert untag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify the tag was removed
            tags = kinesisvideo_client.list_tags_for_resource(ResourceARN=arn)
            assert "env" not in tags.get("Tags", {})
            assert tags["Tags"]["team"] == "dev"
        finally:
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_update_stream(self, kinesisvideo_client):
        name = f"test-stream-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_stream(StreamName=name, DataRetentionInHours=24)
        arn = resp["StreamARN"]
        try:
            # Get current version
            desc = kinesisvideo_client.describe_stream(StreamARN=arn)
            version = desc["StreamInfo"]["Version"]
            upd_resp = kinesisvideo_client.update_stream(
                StreamARN=arn,
                CurrentVersion=version,
                MediaType="video/h264",
            )
            assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify updated
            desc2 = kinesisvideo_client.describe_stream(StreamARN=arn)
            assert desc2["StreamInfo"]["MediaType"] == "video/h264"
        finally:
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_update_signaling_channel(self, kinesisvideo_client):
        name = f"test-sig-{uuid.uuid4().hex[:8]}"
        create_resp = kinesisvideo_client.create_signaling_channel(ChannelName=name)
        sig_arn = create_resp["ChannelARN"]
        try:
            # Get current version
            desc = kinesisvideo_client.describe_signaling_channel(ChannelARN=sig_arn)
            version = desc["ChannelInfo"]["Version"]
            upd_resp = kinesisvideo_client.update_signaling_channel(
                ChannelARN=sig_arn,
                CurrentVersion=version,
            )
            assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            kinesisvideo_client.delete_signaling_channel(ChannelARN=sig_arn)
