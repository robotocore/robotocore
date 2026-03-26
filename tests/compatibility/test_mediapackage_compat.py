"""MediaPackage compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def mediapackage():
    return make_client("mediapackage")


def _unique_id(prefix="mp"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestMediaPackageChannels:
    def test_create_channel(self, mediapackage):
        channel_id = _unique_id("ch")
        response = mediapackage.create_channel(Id=channel_id)
        assert response["Id"] == channel_id
        assert "Arn" in response

    def test_create_channel_with_description(self, mediapackage):
        channel_id = _unique_id("ch")
        response = mediapackage.create_channel(Id=channel_id, Description="test channel")
        assert response["Id"] == channel_id

    def test_describe_channel(self, mediapackage):
        channel_id = _unique_id("ch")
        mediapackage.create_channel(Id=channel_id)
        response = mediapackage.describe_channel(Id=channel_id)
        assert response["Id"] == channel_id
        assert "Arn" in response

    def test_list_channels(self, mediapackage):
        channel_id = _unique_id("ch")
        mediapackage.create_channel(Id=channel_id)
        response = mediapackage.list_channels()
        assert "Channels" in response
        channel_ids = [ch["Id"] for ch in response["Channels"]]
        assert channel_id in channel_ids

    def test_delete_channel(self, mediapackage):
        channel_id = _unique_id("ch")
        mediapackage.create_channel(Id=channel_id)
        mediapackage.delete_channel(Id=channel_id)
        response = mediapackage.list_channels()
        channel_ids = [ch["Id"] for ch in response["Channels"]]
        assert channel_id not in channel_ids

    def test_create_and_describe_channel_arn_format(self, mediapackage):
        channel_id = _unique_id("ch")
        create_resp = mediapackage.create_channel(Id=channel_id)
        describe_resp = mediapackage.describe_channel(Id=channel_id)
        assert create_resp["Arn"] == describe_resp["Arn"]

    def test_describe_channel_nonexistent(self, mediapackage):
        """DescribeChannel for nonexistent channel raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            mediapackage.describe_channel(Id="no-such-channel")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaPackageHarvestJobs:
    def test_list_harvest_jobs(self, mediapackage):
        response = mediapackage.list_harvest_jobs()
        assert "HarvestJobs" in response
        assert isinstance(response["HarvestJobs"], list)

    def test_describe_harvest_job_nonexistent(self, mediapackage):
        """DescribeHarvestJob for nonexistent job raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            mediapackage.describe_harvest_job(Id="no-such-harvest-job")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaPackageOriginEndpoints:
    def test_list_origin_endpoints(self, mediapackage):
        response = mediapackage.list_origin_endpoints()
        assert "OriginEndpoints" in response

    def test_create_origin_endpoint(self, mediapackage):
        channel_id = _unique_id("ch")
        endpoint_id = _unique_id("ep")
        mediapackage.create_channel(Id=channel_id)
        response = mediapackage.create_origin_endpoint(
            ChannelId=channel_id,
            Id=endpoint_id,
            HlsPackage={},
        )
        assert response["Id"] == endpoint_id
        assert response["ChannelId"] == channel_id

    def test_describe_origin_endpoint(self, mediapackage):
        channel_id = _unique_id("ch")
        endpoint_id = _unique_id("ep")
        mediapackage.create_channel(Id=channel_id)
        mediapackage.create_origin_endpoint(
            ChannelId=channel_id,
            Id=endpoint_id,
            HlsPackage={},
        )
        response = mediapackage.describe_origin_endpoint(Id=endpoint_id)
        assert response["Id"] == endpoint_id
        assert response["ChannelId"] == channel_id

    def test_delete_origin_endpoint(self, mediapackage):
        channel_id = _unique_id("ch")
        endpoint_id = _unique_id("ep")
        mediapackage.create_channel(Id=channel_id)
        mediapackage.create_origin_endpoint(
            ChannelId=channel_id,
            Id=endpoint_id,
            HlsPackage={},
        )
        mediapackage.delete_origin_endpoint(Id=endpoint_id)
        response = mediapackage.list_origin_endpoints()
        endpoint_ids = [ep["Id"] for ep in response["OriginEndpoints"]]
        assert endpoint_id not in endpoint_ids

    def test_update_origin_endpoint(self, mediapackage):
        channel_id = _unique_id("ch")
        endpoint_id = _unique_id("ep")
        mediapackage.create_channel(Id=channel_id)
        mediapackage.create_origin_endpoint(
            ChannelId=channel_id,
            Id=endpoint_id,
            Description="original",
            HlsPackage={},
        )
        response = mediapackage.update_origin_endpoint(
            Id=endpoint_id,
            Description="updated",
            HlsPackage={},
        )
        assert response["Id"] == endpoint_id
        assert response["Description"] == "updated"
        # Verify via describe
        desc = mediapackage.describe_origin_endpoint(Id=endpoint_id)
        assert desc["Description"] == "updated"

    def test_describe_origin_endpoint_nonexistent(self, mediapackage):
        """DescribeOriginEndpoint for nonexistent endpoint raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            mediapackage.describe_origin_endpoint(Id="no-such-endpoint")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_list_origin_endpoints_by_channel(self, mediapackage):
        channel_id = _unique_id("ch")
        ep1 = _unique_id("ep")
        ep2 = _unique_id("ep")
        mediapackage.create_channel(Id=channel_id)
        mediapackage.create_origin_endpoint(ChannelId=channel_id, Id=ep1, HlsPackage={})
        mediapackage.create_origin_endpoint(ChannelId=channel_id, Id=ep2, HlsPackage={})
        response = mediapackage.list_origin_endpoints(ChannelId=channel_id)
        endpoint_ids = [ep["Id"] for ep in response["OriginEndpoints"]]
        assert ep1 in endpoint_ids
        assert ep2 in endpoint_ids


class TestMediaPackageTagOps:
    """Tests for tag/untag/list_tags and update_channel operations."""

    @pytest.fixture
    def client(self):
        return make_client("mediapackage")

    @pytest.fixture
    def channel(self, client):
        channel_id = _unique_id("ch")
        resp = client.create_channel(Id=channel_id, Tags={"env": "test"})
        yield {"id": channel_id, "arn": resp["Arn"]}
        try:
            client.delete_channel(Id=channel_id)
        except Exception:
            pass  # best-effort cleanup

    def test_list_tags_for_resource(self, client, channel):
        """ListTagsForResource returns channel tags."""
        resp = client.list_tags_for_resource(ResourceArn=channel["arn"])
        assert "Tags" in resp
        assert resp["Tags"].get("env") == "test"

    def test_tag_resource(self, client, channel):
        """TagResource adds tags to a channel."""
        client.tag_resource(ResourceArn=channel["arn"], Tags={"stage": "prod"})
        resp = client.list_tags_for_resource(ResourceArn=channel["arn"])
        assert resp["Tags"].get("stage") == "prod"

    def test_untag_resource(self, client, channel):
        """UntagResource removes tags from a channel."""
        client.untag_resource(ResourceArn=channel["arn"], TagKeys=["env"])
        resp = client.list_tags_for_resource(ResourceArn=channel["arn"])
        assert "env" not in resp["Tags"]

    def test_update_channel(self, client, channel):
        """UpdateChannel updates the channel description."""
        resp = client.update_channel(Id=channel["id"], Description="updated description")
        assert resp["Description"] == "updated description"
        assert resp["Id"] == channel["id"]


class TestMediaPackageNewOps:
    """Tests for newly-implemented mediapackage gap operations."""

    @pytest.fixture
    def mp(self):
        return make_client("mediapackage")

    def test_configure_logs(self, mp):
        """ConfigureLogs returns updated channel."""
        channel_id = _unique_id("ch")
        mp.create_channel(Id=channel_id)
        try:
            resp = mp.configure_logs(Id=channel_id)
            assert "Id" in resp
            assert resp["Id"] == channel_id
        finally:
            mp.delete_channel(Id=channel_id)

    def test_create_harvest_job(self, mp):
        """CreateHarvestJob returns HarvestJob with id."""
        channel_id = _unique_id("ch")
        ep_id = _unique_id("ep")
        mp.create_channel(Id=channel_id)
        mp.create_origin_endpoint(ChannelId=channel_id, Id=ep_id)
        try:
            resp = mp.create_harvest_job(
                Id=_unique_id("hj"),
                OriginEndpointId=ep_id,
                StartTime="2023-01-01T00:00:00Z",
                EndTime="2023-01-02T00:00:00Z",
                S3Destination={
                    "BucketName": "my-bucket",
                    "ManifestKey": "harvest/output.m3u8",
                    "RoleArn": "arn:aws:iam::123456789012:role/MediaPackage",
                },
            )
            assert "Id" in resp
            assert "OriginEndpointId" in resp
        finally:
            mp.delete_origin_endpoint(Id=ep_id)
            mp.delete_channel(Id=channel_id)

    def test_rotate_channel_credentials(self, mp):
        """RotateChannelCredentials returns the channel."""
        channel_id = _unique_id("ch")
        mp.create_channel(Id=channel_id)
        try:
            resp = mp.rotate_channel_credentials(Id=channel_id)
            assert "Id" in resp
            assert resp["Id"] == channel_id
        finally:
            mp.delete_channel(Id=channel_id)


class TestMediaPackageGapOps:
    """Tests for MediaPackage operations that weren't previously covered."""

    @pytest.fixture
    def client(self):
        return make_client("mediapackage")

    def test_rotate_ingest_endpoint_credentials_not_found(self, client):
        """RotateIngestEndpointCredentials raises NotFoundException for nonexistent channel."""
        from botocore.exceptions import ClientError  # noqa: PLC0415

        with pytest.raises(ClientError) as exc:
            client.rotate_ingest_endpoint_credentials(
                Id="nonexistent-channel-xyz",
                IngestEndpointId="nonexistent-endpoint-xyz",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"
