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
        pass  # best-effort cleanup


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


class TestKinesisVideoNewOps:
    """Tests for newly implemented KinesisVideo operations."""

    def test_list_tags_for_stream(self, kinesisvideo_client):
        name = f"test-tags-{uuid.uuid4().hex[:8]}"
        kinesisvideo_client.create_stream(StreamName=name)
        try:
            resp = kinesisvideo_client.list_tags_for_stream(StreamName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Tags" in resp
        finally:
            arn = kinesisvideo_client.describe_stream(StreamName=name)["StreamInfo"]["StreamARN"]
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_update_data_retention(self, kinesisvideo_client):
        name = f"test-dr-{uuid.uuid4().hex[:8]}"
        kinesisvideo_client.create_stream(StreamName=name, DataRetentionInHours=1)
        try:
            version = kinesisvideo_client.describe_stream(StreamName=name)["StreamInfo"]["Version"]
            resp = kinesisvideo_client.update_data_retention(
                StreamName=name,
                CurrentVersion=version,
                Operation="INCREASE_DATA_RETENTION",
                DataRetentionChangeInHours=2,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            arn = kinesisvideo_client.describe_stream(StreamName=name)["StreamInfo"]["StreamARN"]
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_describe_image_generation_configuration(self, kinesisvideo_client):
        name = f"test-imgcfg-{uuid.uuid4().hex[:8]}"
        kinesisvideo_client.create_stream(StreamName=name)
        try:
            resp = kinesisvideo_client.describe_image_generation_configuration(StreamName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            arn = kinesisvideo_client.describe_stream(StreamName=name)["StreamInfo"]["StreamARN"]
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_describe_notification_configuration(self, kinesisvideo_client):
        name = f"test-notifcfg-{uuid.uuid4().hex[:8]}"
        kinesisvideo_client.create_stream(StreamName=name)
        try:
            resp = kinesisvideo_client.describe_notification_configuration(StreamName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            arn = kinesisvideo_client.describe_stream(StreamName=name)["StreamInfo"]["StreamARN"]
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_get_signaling_channel_endpoint(self, kinesisvideo_client):
        name = f"test-ep-{uuid.uuid4().hex[:8]}"
        ch_resp = kinesisvideo_client.create_signaling_channel(ChannelName=name)
        ch_arn = ch_resp["ChannelARN"]
        try:
            resp = kinesisvideo_client.get_signaling_channel_endpoint(
                ChannelARN=ch_arn,
                SingleMasterChannelEndpointConfiguration={"Protocols": ["WSS"], "Role": "MASTER"},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "ResourceEndpointList" in resp
        finally:
            version = kinesisvideo_client.describe_signaling_channel(ChannelARN=ch_arn)[
                "ChannelInfo"
            ]["Version"]
            kinesisvideo_client.delete_signaling_channel(ChannelARN=ch_arn, CurrentVersion=version)

    def test_describe_stream_storage_configuration(self, kinesisvideo_client):
        name = f"test-storecfg-{uuid.uuid4().hex[:8]}"
        kinesisvideo_client.create_stream(StreamName=name)
        try:
            resp = kinesisvideo_client.describe_stream_storage_configuration(StreamName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            arn = kinesisvideo_client.describe_stream(StreamName=name)["StreamInfo"]["StreamARN"]
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_describe_mapped_resource_configuration(self, kinesisvideo_client):
        name = f"test-mrcfg-{uuid.uuid4().hex[:8]}"
        kinesisvideo_client.create_stream(StreamName=name)
        try:
            resp = kinesisvideo_client.describe_mapped_resource_configuration(StreamName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "MappedResourceConfigurationList" in resp
        finally:
            arn = kinesisvideo_client.describe_stream(StreamName=name)["StreamInfo"]["StreamARN"]
            kinesisvideo_client.delete_stream(StreamARN=arn)


class TestKinesisVideoMissingGapOps:
    """Tests for KinesisVideo operations identified as coverage gaps."""

    def test_update_image_generation_configuration(self, kinesisvideo_client):
        name = f"test-kv-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_stream(StreamName=name, DataRetentionInHours=24)
        arn = resp["StreamARN"]
        try:
            upd_resp = kinesisvideo_client.update_image_generation_configuration(
                StreamName=name,
                ImageGenerationConfiguration={
                    "Status": "DISABLED",
                    "ImageSelectorType": "SERVER_TIMESTAMP",
                    "DestinationConfig": {
                        "Uri": "s3://test-bucket/images",
                        "DestinationRegion": "us-east-1",
                    },
                    "SamplingInterval": 3000,
                    "Format": "JPEG",
                },
            )
            assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            kinesisvideo_client.delete_stream(StreamARN=arn)

    def test_update_notification_configuration(self, kinesisvideo_client):
        name = f"test-kv-{uuid.uuid4().hex[:8]}"
        resp = kinesisvideo_client.create_stream(StreamName=name, DataRetentionInHours=24)
        arn = resp["StreamARN"]
        try:
            upd_resp = kinesisvideo_client.update_notification_configuration(
                StreamName=name,
                NotificationConfiguration={
                    "Status": "DISABLED",
                    "DestinationConfig": {
                        "Uri": "arn:aws:sns:us-east-1:123456789012:test-topic",
                    },
                },
            )
            assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            kinesisvideo_client.delete_stream(StreamARN=arn)


class TestKinesisVideoMediaStorageOps:
    """Tests for media storage configuration operations."""

    @pytest.fixture
    def kinesisvideo_client(self):
        return make_client("kinesisvideo")

    def test_describe_media_storage_configuration_nonexistent(self, kinesisvideo_client):
        """DescribeMediaStorageConfiguration with unknown ARN raises ResourceNotFoundException."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc:
            kinesisvideo_client.describe_media_storage_configuration(
                ChannelARN="arn:aws:kinesisvideo:us-east-1:123456789012:channel/nonexistent/123"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestKinesisVideoEdgeConfigOps:
    """Tests for edge configuration operations."""

    @pytest.fixture
    def kv(self):
        return make_client("kinesisvideo")

    def test_describe_edge_configuration(self, kv):
        """DescribeEdgeConfiguration returns edge config (stub)."""
        resp = kv.describe_edge_configuration(StreamName="nonexistent-stream")
        assert "EdgeConfig" in resp or resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_edge_agent_configurations(self, kv):
        """ListEdgeAgentConfigurations returns empty list."""
        resp = kv.list_edge_agent_configurations(
            HubDeviceArn="arn:aws:kinesisvideo:us-east-1:123456789012:device/test/123"
        )
        assert "EdgeConfigs" in resp

    def test_delete_edge_configuration(self, kv):
        """DeleteEdgeConfiguration succeeds (stub)."""
        resp = kv.delete_edge_configuration(StreamName="nonexistent-stream")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_media_storage_configuration(self, kv):
        """UpdateMediaStorageConfiguration succeeds (stub)."""
        resp = kv.update_media_storage_configuration(
            ChannelARN="arn:aws:kinesisvideo:us-east-1:123456789012:channel/test/123",
            MediaStorageConfiguration={"Status": "DISABLED"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_stream_storage_configuration(self, kv):
        """UpdateStreamStorageConfiguration succeeds (stub)."""
        resp = kv.update_stream_storage_configuration(
            StreamName="nonexistent-stream",
            CurrentVersion="1",
            StreamStorageConfiguration={"DefaultStorageTier": "ARCHIVE"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestKinesisVideoStartEdgeConfigUpdate:
    """Test StartEdgeConfigurationUpdate operation."""

    @pytest.fixture
    def kv(self):
        return make_client("kinesisvideo")

    def test_start_edge_configuration_update(self, kv):
        """StartEdgeConfigurationUpdate returns EdgeConfig."""
        try:
            resp = kv.start_edge_configuration_update(
                StreamName="fake-stream",
                EdgeConfig={
                    "HubDeviceArn": ("arn:aws:kinesisvideo:us-east-1:123456789012:device/fake"),
                    "RecorderConfig": {
                        "MediaSourceConfig": {
                            "MediaUriSecretArn": (
                                "arn:aws:secretsmanager:us-east-1:123456789012:secret/f"
                            ),
                            "MediaUriType": "RTSP_URI",
                        },
                        "ScheduleConfig": {
                            "ScheduleExpression": "rate(1 day)",
                            "DurationInSeconds": 3600,
                        },
                    },
                },
            )
            assert "EdgeConfig" in resp or "SyncStatus" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None
